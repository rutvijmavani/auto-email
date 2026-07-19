"""
workers/email_processor.py — Gmail email classification and status tracking daemon.

Reads from queue:email:push (written by api.py /email-push webhook). For each
Pub/Sub notification, calls history.list to find new message IDs, then for each:

  1. Fetches subject + sender metadata (no body, cheap API call)
  2. Runs Qwen3-8B gate  (subject+sender → yes/no/not_sure, ~5-6s warm)
  3. Fetches full body only if gate passes (yes or not_sure)
  4. Runs Qwen3-8B extraction  (body → {company, title, status} JSON, ~25-30s)
  5. 4-layer matching funnel: ATS domain filter → fuzzy match → Qwen3-8B disambiguation
  6. Updates applications.email_status + ats_company + ats_title, or writes to
     unmatched_emails when no application can be identified.

Model: Qwen3-8B-Q4_K_M.gguf via llama-cpp-python. Loaded once at startup (~2 min),
stays in memory forever. Single worker — email volume (~5-10/day) does not justify
parallelism, and concurrent Qwen inference on a 2-core VM is slower than sequential.

At-least-once delivery (same pattern as detail_worker):
  LMOVE queue:email:push → queue:email:push:inflight:{worker_id}   (atomic)
  LREM inflight on success; requeue on transient failure (up to 3 attempts); DLQ after.

Recovery: _recover_stuck_jobs() on startup scans inflight keys from dead workers
(no heartbeat) and pushes their items back to the source queue.

Usage:
  python -m workers.email_processor          # run forever
  python -m workers.email_processor --once   # process one notification then exit
"""

import base64
import json
import multiprocessing
import os
import re
import socket
import sys
import time
from datetime import datetime, timezone

import google.auth.exceptions
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from llama_cpp import Llama
from rapidfuzz import fuzz

from config import REDIS_EMAIL_PUSH, REDIS_EMAIL_DLQ
from db.connection import get_conn
from db.gmail_tokens import get_token_by_email, update_history_id
from logger import get_logger, init_logging
from workers.redis_client import get_redis

logger = get_logger(__name__)

WORKER_ID = f"{socket.gethostname()}:{os.getpid()}"

# set in run_worker() after forking so WORKER_ID uses the real child PID
_INFLIGHT_KEY: str = ""

_CLIENT_ID     = os.environ.get("GMAIL_CLIENT_ID", "")
_CLIENT_SECRET = os.environ.get("GMAIL_CLIENT_SECRET", "")
_MODEL_PATH    = os.environ.get("EMAIL_PROCESSOR_MODEL_PATH", "")

_MAX_ATTEMPTS     = 3           # total attempts before DLQ (1 original + 2 retries)
_RETRY_DELAYS     = [30, 300]   # seconds to wait before attempt 2 and 3
_DLQ_MAX_ENTRIES  = 200
_INFERENCE_TIMEOUT = 120        # seconds before treating an inference call as hung

# ATS email sender domain → platform name used for Layer 1 filtering
_SENDER_TO_PLATFORM = {
    "greenhouse.io":       "greenhouse",
    "hire.lever.co":       "lever",
    "lever.co":            "lever",
    "ashbyhq.com":         "ashby",
    "myworkday.com":       "workday",
    "workday.com":         "workday",
    "smartrecruiters.com": "smartrecruiters",
    "successfactors.com":  "successfactors",
    "sap.com":             "successfactors",
    "icims.com":           "icims",
    "jobvite.com":         "jobvite",
}

# Platform → substrings to match in applications.job_url for Layer 1 DB filter
_PLATFORM_URL_HINTS: dict[str, list[str]] = {
    "greenhouse":      ["greenhouse.io"],
    "lever":           ["lever.co"],
    "ashby":           ["ashbyhq.com"],
    "workday":         ["myworkdayjobs.com", "workday.com"],
    "smartrecruiters": ["smartrecruiters.com"],
    "successfactors":  ["successfactors.com", "sap.com"],
    "icims":           ["icims.com"],
    "jobvite":         ["jobvite.com"],
}

# Atomic: remove from inflight, push to source queue (retry without losing the item)
_ATOMIC_REQUEUE_LUA = """
local removed = redis.call('LREM', KEYS[1], 1, ARGV[1])
if removed > 0 then redis.call('RPUSH', KEYS[2], ARGV[1]) end
return removed
"""

llm: "Llama | None" = None


# ── Model ─────────────────────────────────────────────────────────────────────

def _load_model() -> None:
    global llm
    if llm is not None:
        return
    if not _MODEL_PATH:
        raise RuntimeError(
            "EMAIL_PROCESSOR_MODEL_PATH is not set — "
            "point it at the Qwen3-8B-Q4_K_M.gguf file"
        )
    logger.info("loading Qwen3-8B from %s ...", _MODEL_PATH)
    llm = Llama(
        model_path=_MODEL_PATH,
        n_ctx=4096,
        n_threads=2,
        verbose=False,
    )
    logger.info("model ready")


def _run_with_timeout(fn, timeout: int = _INFERENCE_TIMEOUT):
    """Run fn() in a forked child process; terminate it if timeout expires."""
    def _worker(q, fn):
        try:
            q.put(("ok", fn()))
        except Exception as exc:
            try:
                q.put(("err", exc))
            except Exception:
                q.put(("err", RuntimeError(str(exc))))

    q: multiprocessing.Queue = multiprocessing.Queue()
    p = multiprocessing.Process(target=_worker, args=(q, fn))
    p.start()
    try:
        p.join(timeout)
        if p.is_alive():
            p.terminate()
            p.join()
            raise RuntimeError(f"inference timed out after {timeout}s")
        if not q.empty():
            kind, val = q.get_nowait()
            if kind == "ok":
                return val
            raise val
        raise RuntimeError("inference process exited without result")
    finally:
        q.close()
        q.join_thread()
        p.close()


def _strip_think(text: str) -> str:
    """Remove Qwen3 <think>…</think> block so only the final answer remains."""
    return re.sub(r"<think>.*?</think>", "", text, flags=re.DOTALL).strip()


# ── Gmail helpers ──────────────────────────────────────────────────────────────

def _build_gmail_service(token_row: dict):
    """Build an authenticated Gmail API client from a stored refresh token."""
    creds = Credentials(
        token=None,
        refresh_token=token_row["refresh_token"],
        token_uri="https://oauth2.googleapis.com/token",
        client_id=_CLIENT_ID,
        client_secret=_CLIENT_SECRET,
    )
    creds.refresh(Request())   # exchange refresh token for a fresh access token
    return build("gmail", "v1", credentials=creds, cache_discovery=False)


def _extract_body(payload: dict) -> str:
    """
    Extract plain text from a Gmail message payload.

    Preference order:
      1. text/plain part
      2. text/html part (tags stripped)
      3. Single-part body.data
    """
    def _decode(data: str) -> str:
        return base64.urlsafe_b64decode(data + "==").decode("utf-8", errors="replace")

    parts = payload.get("parts", [])
    for part in parts:
        if part.get("mimeType") == "text/plain" and part.get("body", {}).get("data"):
            return _decode(part["body"]["data"])
    for part in parts:
        if part.get("mimeType") == "text/html" and part.get("body", {}).get("data"):
            html = _decode(part["body"]["data"])
            return re.sub(r"<[^>]+>", " ", html)
    body_data = payload.get("body", {}).get("data", "")
    if body_data:
        return _decode(body_data)
    return ""


def _truncate_body(text: str, max_words: int = 2000) -> str:
    """Trim body so llama-cpp's context window isn't silently truncated."""
    words = text.split()
    return " ".join(words[:max_words]) if len(words) > max_words else text


# ── Qwen inference ─────────────────────────────────────────────────────────────

_GATE_PROMPT = """/no_think
Classify this email.

Sender: {sender}
Subject: {subject}

Is this email a status update about a job application that was submitted? Examples: rejection notice, interview invitation, phone screen request, online assessment invitation, offer letter.

Output exactly one word — yes, no, or not_sure.
Never output "no" unless you are completely certain this is not about a submitted job application.
Output:"""

_EXTRACT_PROMPT = """/no_think
Extract structured information from this job application email. Output JSON only, no other text.

{body}

Output:
{{
  "company": "<company name exactly as written in the email>",
  "title": "<job title exactly as written in the email, or null if not mentioned>",
  "status": "<one of: rejected, interview, phone_screen, assessment, offer — or null if unclear>"
}}"""

_DISAMBIGUATE_PROMPT = """/think
A job application status email has arrived. I need to identify which application it belongs to.

Email:
  Company: {company}
  Job title: {title}
  Status update: {status}

Candidate applications (company names may contain typos or informal abbreviations):
{candidates}

Which application ID does this email refer to? Consider that the email company name and the stored company name may not match exactly.
Output JSON: {{"application_id": <one of the IDs listed above>}}"""


def _gate(subject: str, sender: str) -> str:
    """Call 1: classify subject+sender. Returns 'yes', 'no', or 'not_sure'."""
    prompt = _GATE_PROMPT.format(sender=sender, subject=subject)
    resp = _run_with_timeout(lambda: llm.create_chat_completion(  # type: ignore[union-attr]
        messages=[{"role": "user", "content": prompt}],
        max_tokens=10,
        temperature=0,
    ))
    raw = _strip_think(resp["choices"][0]["message"]["content"]).lower()
    for token in ("yes", "not_sure", "no"):
        if token in raw:
            return token
    return "not_sure"  # safe default — a false not_sure costs one extra call; a false no loses a real update


def _extract(body: str) -> dict:
    """Call 2: extract company, title, status from full email body."""
    prompt = _EXTRACT_PROMPT.format(body=_truncate_body(body))
    resp = _run_with_timeout(lambda: llm.create_chat_completion(  # type: ignore[union-attr]
        messages=[{"role": "user", "content": prompt}],
        max_tokens=200,
        temperature=0,
    ))
    text = _strip_think(resp["choices"][0]["message"]["content"])
    m = re.search(r"\{[^}]+\}", text, re.DOTALL)
    if not m:
        return {}
    try:
        return json.loads(m.group())
    except json.JSONDecodeError:
        return {}


def _disambiguate(extracted: dict, candidates: list) -> "int | None":
    """
    Layer 4: given 2-3 matching candidates, use /think mode to pick one.
    Returns the chosen application ID, or None if model cannot decide.
    """
    lines = "\n".join(
        f"  ID {c['id']}: {c['company']}"
        + (f" — {c['job_title']}" if c.get("job_title") else "")
        + (f" (applied {c['applied_date']})" if c.get("applied_date") else "")
        for c in candidates
    )
    prompt = _DISAMBIGUATE_PROMPT.format(
        company=extracted.get("company", "unknown"),
        title=extracted.get("title") or "not mentioned",
        status=extracted.get("status") or "unknown",
        candidates=lines,
    )
    resp = _run_with_timeout(lambda: llm.create_chat_completion(  # type: ignore[union-attr]
        messages=[{"role": "user", "content": prompt}],
        max_tokens=400,
        temperature=0,
    ))
    text = _strip_think(resp["choices"][0]["message"]["content"])
    m = re.search(r"\{[^}]+\}", text, re.DOTALL)
    if not m:
        return None
    try:
        chosen_id = int(json.loads(m.group()).get("application_id", 0))
        if any(c["id"] == chosen_id for c in candidates):
            return chosen_id
    except (json.JSONDecodeError, ValueError, TypeError):
        pass
    return None


# ── Matching funnel ────────────────────────────────────────────────────────────

def _sender_domain(email_from: str) -> str:
    if "@" in email_from:
        return email_from.split("@")[-1].rstrip("> \t").lower()
    return ""


def _get_candidates(user_id: int, platform: "str | None") -> list:
    """Layer 1 + DB fetch: return active applications for this user, optionally
    filtered by ATS platform (matched via job_url substring)."""
    with get_conn() as conn:
        if platform:
            hints = _PLATFORM_URL_HINTS.get(platform, [])
            if hints:
                where = " OR ".join(["job_url ILIKE %s"] * len(hints))
                rows = conn.execute(
                    f"""SELECT id, company, job_title, ats_company, applied_date
                          FROM applications
                         WHERE user_id = %s AND status = 'active'
                           AND ({where})""",
                    (user_id, *[f"%{h}%" for h in hints]),
                ).fetchall()
                return [dict(r) for r in rows]
        rows = conn.execute(
            """SELECT id, company, job_title, ats_company, applied_date
                 FROM applications
                WHERE user_id = %s AND status = 'active'""",
            (user_id,),
        ).fetchall()
        return [dict(r) for r in rows]


def _match_candidates(user_id: int, company: str, title: "str | None", email_from: str) -> list:
    """
    Run Layers 1-3 of the matching funnel.

    Layer 1 — ATS domain filter (DB query scoped to platform)
    Layer 2 — Qwen extraction already done (company + title passed in as args)
    Layer 3 — rapidfuzz: WRatio ≥ 80 on company (required); token_sort_ratio ≥ 70
               on title (optional — skipped when Qwen didn't extract a title).
    """
    platform = _SENDER_TO_PLATFORM.get(_sender_domain(email_from))
    candidates = _get_candidates(user_id, platform)

    matched = []
    for app in candidates:
        db_company = (app.get("ats_company") or app.get("company") or "").strip()
        if not db_company:
            continue
        if fuzz.WRatio(company.lower(), db_company.lower()) < 80:
            continue
        if title and app.get("job_title"):
            if fuzz.token_sort_ratio(title.lower(), app["job_title"].lower()) < 70:
                continue
        matched.append(app)
    return matched


# ── DB writes ──────────────────────────────────────────────────────────────────

_TERMINAL_STATUSES = frozenset({"rejected", "offer"})


def _update_application(app_id: int, email_status: str, ats_company: "str | None", ats_title: "str | None") -> None:
    """
    Set email_status (and optionally ats_company/ats_title) on an application.
    Also flips status → 'closed' for terminal email statuses (rejected, offer).
    Uses COALESCE so we never overwrite an already-set ats_company/ats_title with NULL.
    """
    with get_conn() as conn:
        if email_status in _TERMINAL_STATUSES:
            conn.execute("""
                UPDATE applications
                   SET email_status = %s,
                       ats_company  = COALESCE(%s, ats_company),
                       ats_title    = COALESCE(%s, ats_title),
                       status       = 'closed'
                 WHERE id = %s
            """, (email_status, ats_company, ats_title, app_id))
        else:
            conn.execute("""
                UPDATE applications
                   SET email_status = %s,
                       ats_company  = COALESCE(%s, ats_company),
                       ats_title    = COALESCE(%s, ats_title)
                 WHERE id = %s
            """, (email_status, ats_company, ats_title, app_id))


def _write_unmatched(
    user_id: int,
    email_from: str,
    subject: str,
    company: "str | None",
    title: "str | None",
    status: "str | None",
    body: str,
    received_at: "str | None",
    gmail_message_id: "str | None" = None,
) -> None:
    with get_conn() as conn:
        conn.execute("""
            INSERT INTO unmatched_emails
                (user_id, gmail_message_id, email_from, email_subject,
                 extracted_company, extracted_title, extracted_status,
                 email_snippet, received_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (gmail_message_id)
            WHERE gmail_message_id IS NOT NULL
            DO NOTHING
        """, (user_id, gmail_message_id, email_from, subject, company,
              title, status, body[:300] if body else None,
              received_at or None))


# ── DLQ ───────────────────────────────────────────────────────────────────────

def _push_to_dlq(r, payload: dict) -> None:
    try:
        entry = json.dumps({
            "_dlq_added_at": datetime.now(timezone.utc).isoformat(),
            "payload": payload,
        })
        pipe = r.pipeline()
        pipe.lpush(REDIS_EMAIL_DLQ, entry)
        pipe.ltrim(REDIS_EMAIL_DLQ, 0, _DLQ_MAX_ENTRIES - 1)
        pipe.execute()
        logger.warning("dlq: email=%s attempts=%s", payload.get("email"), payload.get("_attempts"))
    except Exception as exc:
        logger.debug("dlq push failed (non-critical): %s", exc)


# ── Recovery ───────────────────────────────────────────────────────────────────

def _recover_stuck_jobs(r) -> None:
    """
    On startup: find inflight keys from dead workers, drain them back to the
    source queue so those notifications are retried.

    A worker is considered dead when its heartbeat key
    (worker:alive:email_processor:{hostname}:{pid}) is absent.
    We never drain our own inflight key — WORKER_ID is skipped.
    """
    prefix = "queue:email:push:inflight:"
    cursor = 0
    recovered = 0
    while True:
        cursor, keys = r.scan(cursor, match=f"{prefix}*", count=100)
        for key in keys:
            key_str = key.decode() if isinstance(key, bytes) else key
            peer_id = key_str[len(prefix):]
            if peer_id == WORKER_ID:
                continue
            heartbeat_key = f"worker:alive:email_processor:{peer_id}"
            if r.exists(heartbeat_key):
                continue  # peer is still alive
            # drain tail → source, one item at a time
            while True:
                raw = r.lindex(key_str, -1)
                if raw is None:
                    break
                raw_str = raw.decode() if isinstance(raw, bytes) else raw
                r.rpush(REDIS_EMAIL_PUSH, raw_str)
                r.lrem(key_str, 1, raw_str)
                recovered += 1
        if cursor == 0:
            break
    if recovered:
        logger.info("recovered %d stuck jobs from dead workers", recovered)


# ── Per-message processing ─────────────────────────────────────────────────────

def _process_message(gmail, user_id: int, message_id: str) -> None:
    """
    Process one Gmail message through the full pipeline:
      gate → (if yes/not_sure) fetch body → extract → match funnel → DB update.

    Raises on transient failures (API errors, inference errors) so the caller
    can decide whether to retry the whole notification batch.
    Does NOT raise on logical non-matches — those are written to unmatched_emails.
    """
    # Fetch metadata only — no body, no quota cost for full messages yet
    meta = gmail.users().messages().get(
        userId="me",
        id=message_id,
        format="metadata",
        metadataHeaders=["Subject", "From", "Date"],
    ).execute()

    headers  = {h["name"]: h["value"] for h in meta.get("payload", {}).get("headers", [])}
    subject  = headers.get("Subject", "")
    sender   = headers.get("From", "")
    date_str = headers.get("Date") or None

    # Gate — subject + sender only (~5-6s warm)
    gate = _gate(subject, sender)
    logger.debug("gate=%s message_id=%s subject=%r", gate, message_id, subject[:80])

    if gate == "no":
        logger.debug("gate=no — discarding message_id=%s", message_id)
        return

    # Fetch full body (only when gate says yes or not_sure)
    full    = gmail.users().messages().get(userId="me", id=message_id, format="full").execute()
    body    = _extract_body(full.get("payload", {}))
    snippet = body[:300]

    # Extraction — company, title, status (~25-30s)
    extracted = _extract(body)
    company   = (extracted.get("company") or "").strip()
    title     = extracted.get("title") or None
    status    = extracted.get("status") or None

    if not company or not status:
        logger.debug("extraction incomplete for message_id=%s: %s", message_id, extracted)
        return  # not enough signal to act

    logger.info("extracted company=%r title=%r status=%r message_id=%s", company, title, status, message_id)

    # Layers 1-3: ATS domain filter + fuzzy match
    candidates = _match_candidates(user_id, company, title, sender)

    if not candidates:
        _write_unmatched(user_id, sender, subject, company, title, status, snippet, date_str, message_id)
        logger.info("unmatched: 0 candidates for company=%r user_id=%s message_id=%s", company, user_id, message_id)
        return

    if len(candidates) == 1:
        app_id = candidates[0]["id"]
    else:
        # Layer 4: Qwen disambiguation with /think (~30-60s)
        top3 = candidates[:3]
        app_id = _disambiguate(extracted, top3)
        if app_id is None:
            logger.warning(
                "disambiguation failed company=%r candidates=%s message_id=%s",
                company, [c["id"] for c in top3], message_id,
            )
            _write_unmatched(user_id, sender, subject, company, title, status, snippet, date_str, message_id)
            return

    _update_application(app_id, status, company, title)
    logger.info(
        "updated app_id=%s email_status=%s company=%r user_id=%s message_id=%s",
        app_id, status, company, user_id, message_id,
    )


# ── Notification processing ────────────────────────────────────────────────────

def _process_notification(raw_str: str, r) -> str:
    """
    Expand one Pub/Sub notification into individual Gmail messages and process each.

    Returns:
      "done"    — notification fully handled; remove from inflight
      "retry"   — transient failure; caller will requeue with incremented _attempts
      "discard" — permanent failure (bad payload, unknown user, revoked OAuth); drop
    """
    try:
        payload = json.loads(raw_str)
    except json.JSONDecodeError:
        logger.warning("malformed notification payload: %r", raw_str[:200])
        return "discard"

    email_address = payload.get("email", "")
    history_id    = payload.get("history_id", "")
    direct_msg_id = payload.get("msg_id")  # set by catch-up path; skip history.list

    if not email_address or not history_id:
        logger.warning("notification missing email or history_id: %s", payload)
        return "discard"

    try:
        token_row = get_token_by_email(email_address)
    except Exception as exc:
        logger.error("DB lookup failed for email=%s: %s", email_address, exc, exc_info=True)
        return "retry"

    if token_row is None:
        logger.warning("no token found for email=%s — discarding", email_address)
        return "discard"

    user_id = token_row["user_id"]

    # Build an authenticated Gmail service
    try:
        gmail = _build_gmail_service(token_row)
    except google.auth.exceptions.RefreshError as exc:
        logger.error(
            "OAuth revoked for user_id=%s email=%s — "
            "user must re-authorize at /oauth/start?user_id=%s: %s",
            user_id, email_address, user_id, exc,
        )
        return "discard"
    except HttpError as exc:
        if "invalid_grant" in str(exc):
            logger.error(
                "OAuth revoked for user_id=%s email=%s — "
                "user must re-authorize at /oauth/start?user_id=%s",
                user_id, email_address, user_id,
            )
            return "discard"
        logger.error("Gmail auth failed user_id=%s: %s", user_id, exc, exc_info=True)
        return "retry"
    except Exception as exc:
        logger.error("Gmail service build failed user_id=%s: %s", user_id, exc, exc_info=True)
        return "retry"

    # history.list: expand notification → individual message IDs (paginated)
    # Catch-up payloads include msg_id directly — skip history.list entirely.
    if direct_msg_id:
        message_ids = [direct_msg_id]
        new_history_id = history_id
    else:
        start_id = token_row.get("last_history_id") or history_id
        message_ids = []
        new_history_id = history_id
        page_token = None
        while True:
            kwargs = dict(userId="me", startHistoryId=start_id, historyTypes=["messageAdded"])
            if page_token:
                kwargs["pageToken"] = page_token
            try:
                history_resp = gmail.users().history().list(**kwargs).execute()
            except HttpError as exc:
                if exc.resp.status == 404:
                    logger.warning(
                        "history.list: startHistoryId=%s expired for user_id=%s — "
                        "resetting cursor to notification history_id=%s",
                        start_id, user_id, history_id,
                    )
                    try:
                        update_history_id(user_id, history_id)
                    except Exception as update_exc:
                        logger.error("failed to reset history cursor user_id=%s: %s", user_id, update_exc)
                    return "discard"
                logger.error("history.list failed user_id=%s: %s", user_id, exc, exc_info=True)
                return "retry"
            except Exception as exc:
                logger.error("history.list failed user_id=%s: %s", user_id, exc, exc_info=True)
                return "retry"
            new_history_id = history_resp.get("historyId", new_history_id)
            message_ids.extend(
                added["message"]["id"]
                for record in history_resp.get("history", [])
                for added in record.get("messagesAdded", [])
                if added.get("message", {}).get("id")
            )
            page_token = history_resp.get("nextPageToken")
            if not page_token:
                break

    logger.info(
        "history.list returned %d message(s) for user_id=%s email=%s",
        len(message_ids), user_id, email_address,
    )

    # Process each message; track failures so the cursor is not advanced on error
    any_failed = False
    for message_id in message_ids:
        try:
            _process_message(gmail, user_id, message_id)
        except RuntimeError as exc:
            logger.error("inference error message_id=%s: %s", message_id, exc, exc_info=True)
            any_failed = True
        except HttpError as exc:
            logger.error("Gmail API error message_id=%s: %s", message_id, exc, exc_info=True)
            any_failed = True
        except Exception as exc:
            logger.error("unexpected error message_id=%s: %s", message_id, exc, exc_info=True)
            any_failed = True

    if any_failed:
        return "retry"

    # Advance the cursor only after all messages processed successfully
    try:
        update_history_id(user_id, new_history_id)
    except Exception as exc:
        logger.error("failed to update last_history_id user_id=%s: %s", user_id, exc)
        # Non-fatal: worst case we reprocess the same messages on the next notification
        # (idempotent — email_status just gets overwritten with the same value)

    return "done"


# ── Main loop ──────────────────────────────────────────────────────────────────

def run_worker(once: bool = False) -> None:
    global _INFLIGHT_KEY
    _INFLIGHT_KEY = f"queue:email:push:inflight:{WORKER_ID}"
    heartbeat_key = f"worker:alive:email_processor:{WORKER_ID}"

    r = get_redis()
    _recover_stuck_jobs(r)
    _load_model()

    logger.info(
        "email-processor started worker_id=%s inflight=%s",
        WORKER_ID, _INFLIGHT_KEY,
    )

    while True:
        r.setex(heartbeat_key, 120, "1")

        # LMOVE: atomically pop from source tail → push to inflight head
        # Producers lpush (left); we consume from right → FIFO order
        raw = r.lmove(REDIS_EMAIL_PUSH, _INFLIGHT_KEY, "RIGHT", "LEFT")
        if raw is None:
            # Queue empty — block for up to 5s waiting for a new item
            raw = r.blmove(REDIS_EMAIL_PUSH, _INFLIGHT_KEY, 5, "RIGHT", "LEFT")
        if raw is None:
            if once:
                break
            continue

        raw_str = raw.decode() if isinstance(raw, bytes) else raw

        # Check retry deferral: if this item has a _retry_after timestamp that
        # hasn't elapsed yet, put it back and move on
        try:
            check = json.loads(raw_str)
            retry_after = check.get("_retry_after", 0)
            if retry_after and time.time() < retry_after:
                r.eval(_ATOMIC_REQUEUE_LUA, 2, _INFLIGHT_KEY, REDIS_EMAIL_PUSH, raw_str)
                time.sleep(1)
                if once:
                    break
                continue
        except (json.JSONDecodeError, TypeError):
            pass

        result = _process_notification(raw_str, r)

        if result == "done" or result == "discard":
            r.lrem(_INFLIGHT_KEY, 1, raw_str)

        elif result == "retry":
            try:
                payload = json.loads(raw_str)
            except json.JSONDecodeError:
                payload = {}
            attempts = payload.get("_attempts", 0)

            if attempts >= _MAX_ATTEMPTS - 1:
                _push_to_dlq(r, payload)
                r.lrem(_INFLIGHT_KEY, 1, raw_str)
                logger.warning(
                    "max attempts reached — moved to DLQ email=%s", payload.get("email")
                )
            else:
                delay = _RETRY_DELAYS[attempts] if attempts < len(_RETRY_DELAYS) else _RETRY_DELAYS[-1]
                payload["_attempts"]    = attempts + 1
                payload["_retry_after"] = time.time() + delay
                new_raw = json.dumps(payload)
                r.eval(_ATOMIC_REQUEUE_LUA, 2, _INFLIGHT_KEY, REDIS_EMAIL_PUSH, new_raw)
                logger.info(
                    "retry attempt=%d delay=%ds email=%s",
                    attempts + 1, delay, payload.get("email"),
                )

        if once:
            break

    r.delete(heartbeat_key)
    logger.info("email-processor stopped")


if __name__ == "__main__":
    init_logging("email_processor")
    once = "--once" in sys.argv
    run_worker(once=once)
