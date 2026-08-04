"""
workers/h1b_llm_worker.py — Async LLM disambiguation worker for H1B employer matching.

Reads from Redis stream llm:h1b:disambiguate (written by scripts/fuzzy_match_uscis_dol.py).
For each job:
  1. Checks idempotency — skips if already in uscis_dol_fuzzy_map.
  2. Calls Qwen3 via llama-server to pick the best-matching DOL FEIN from candidates.
  3. Writes match to uscis_dol_fuzzy_map + deletes from uscis_dol_unmatched atomically.
  4. XACKs on success or permanent decision (no match, hallucination, already mapped).
  5. Does NOT XACK on LLM errors — message stays in PEL, reclaimed on next startup.

Single worker — llama-server runs --parallel 1 so multiple consumers would serialize
anyway.  Handles one job at a time; throughput is bounded by LLM inference speed.

Usage:
  python -m workers.h1b_llm_worker          # run forever
  python -m workers.h1b_llm_worker --once   # process one job then exit
"""

import json
import os
import re
import sys
import time

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from config import (
    H1B_DISAMBIG_BLOCK_MS,
    H1B_DISAMBIG_CONSUMER,
    H1B_DISAMBIG_DLQ_STREAM,
    H1B_DISAMBIG_GROUP,
    H1B_DISAMBIG_MAX_DELIVERIES,
    H1B_DISAMBIG_STREAM,
    H1B_LLM_GEMINI_MODEL,
    H1B_LLM_GEMINI_RPM,
    H1B_LLM_GEMINI_TPM,
    H1B_LLM_PROVIDER,
    LLM_BASE_URL,
    REDIS_DB_MAINTENANCE,
)
from db.connection import get_conn
from logger import get_logger, init_logging
from workers.heartbeat import Heartbeat
from workers.llm_client import call_llm as _call_llm_shared   # local/Qwen3 path — kept, used when H1B_LLM_PROVIDER=local
from workers.rate_limiter import DualRateLimiter
from workers.redis_client import get_redis

log = get_logger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# Maintenance window
# ─────────────────────────────────────────────────────────────────────────────

def _is_maintenance(r) -> bool:
    try:
        return bool(r.exists(REDIS_DB_MAINTENANCE))
    except Exception as exc:
        log.warning("Redis maintenance check failed (%s) — assuming not in maintenance", exc)
        return False


# ─────────────────────────────────────────────────────────────────────────────
# LLM — prompts
# ─────────────────────────────────────────────────────────────────────────────

# Local/Qwen3 prompt — /think prefix triggers chain-of-thought reasoning in Qwen3.
# Used when H1B_LLM_PROVIDER=local.
_MATCH_PROMPT_LOCAL = """/think
I need to identify which DOL Labor Condition Application employer record corresponds to
a USCIS H-1B petition employer. Both documents are filed by the same legal entity, so
the names must refer to the same company — though formatting may differ.

USCIS employer name: "{uscis_name}"
USCIS normalized:    "{uscis_norm}"

DOL candidates (same last-4 FEIN, ranked by name similarity):
{candidates}

Instructions:
- Select the candidate that is the SAME legal entity as the USCIS employer.
- Common aliases, DBA names, and abbreviations count as the same entity.
- A subsidiary is NOT the same as its parent (e.g. "Amazon Web Services" ≠ "Amazon.com").
- A branch/campus is NOT the same as the national entity unless the names make it clear.
- If NO candidate is the same legal entity, set "match" to null.

Respond with ONLY a JSON object — no explanation, no markdown:
{{"match": "<FEIN from candidates above, or null>"}}"""

# Gemini prompt — same content, no /think (Qwen3-specific prefix not understood by Gemini).
# Used when H1B_LLM_PROVIDER=gemini.
_MATCH_PROMPT_GEMINI = """\
I need to identify which DOL Labor Condition Application employer record corresponds to
a USCIS H-1B petition employer. Both documents are filed by the same legal entity, so
the names must refer to the same company — though formatting may differ.

USCIS employer name: "{uscis_name}"
USCIS normalized:    "{uscis_norm}"

DOL candidates (same last-4 FEIN, ranked by name similarity):
{candidates}

Instructions:
- Select the candidate that is the SAME legal entity as the USCIS employer.
- Common aliases, DBA names, and abbreviations count as the same entity.
- A subsidiary is NOT the same as its parent (e.g. "Amazon Web Services" ≠ "Amazon.com").
- A branch/campus is NOT the same as the national entity unless the names make it clear.
- If NO candidate is the same legal entity, set "match" to null.

Respond with ONLY a JSON object — no explanation, no markdown:
{{"match": "<FEIN from candidates above, or null>"}}"""

# Keep backward-compat alias so any external code referencing _MATCH_PROMPT still works.
_MATCH_PROMPT = _MATCH_PROMPT_LOCAL


# ─────────────────────────────────────────────────────────────────────────────
# LLM — Gemini provider (used when H1B_LLM_PROVIDER=gemini)
# ─────────────────────────────────────────────────────────────────────────────

_gemini_model = None
_gemini_limiter: "DualRateLimiter | None" = None


def _get_gemini_model():
    global _gemini_model, _gemini_limiter
    if _gemini_model is None:
        from google import genai
        api_key = os.environ.get("GOOGLE_API_KEY")
        if not api_key:
            raise RuntimeError(
                "GOOGLE_API_KEY not set — required when H1B_LLM_PROVIDER=gemini"
            )
        _gemini_model = genai.Client(api_key=api_key)
        _gemini_limiter = DualRateLimiter(rpm=H1B_LLM_GEMINI_RPM, tpm=H1B_LLM_GEMINI_TPM)
        log.info("Gemini provider initialised — model=%s rpm=%d tpm=%d",
                 H1B_LLM_GEMINI_MODEL, H1B_LLM_GEMINI_RPM, H1B_LLM_GEMINI_TPM)
    return _gemini_model, _gemini_limiter


def _ask_gemini(uscis_name: str, uscis_norm: str,
                candidates: list[dict], cand_text: str) -> "str | None":
    """Call Gemini API. Raises on API errors — caller leaves message in PEL."""
    from google.genai import types
    client, limiter = _get_gemini_model()
    prompt = _MATCH_PROMPT_GEMINI.format(
        uscis_name=uscis_name,
        uscis_norm=uscis_norm,
        candidates=cand_text,
    )
    estimated = len(prompt) // 4 + 50
    limiter.wait(estimated_tokens=estimated)
    response = client.models.generate_content(
        model=H1B_LLM_GEMINI_MODEL,
        contents=prompt,
        config=types.GenerateContentConfig(temperature=0),
    )
    tokens = getattr(response.usage_metadata, "total_token_count", 0) or 0
    limiter.record(tokens)
    log.debug("Gemini response tokens=%d [%s]", tokens, limiter.status())
    return response.text.strip()


def _ask_local(uscis_name: str, uscis_norm: str,
               candidates: list[dict], cand_text: str) -> str:
    """Call local llama-server (Qwen3). Raises on connection error — caller leaves in PEL."""
    prompt = _MATCH_PROMPT_LOCAL.format(
        uscis_name=uscis_name,
        uscis_norm=uscis_norm,
        candidates=cand_text,
    )
    return _call_llm_shared(prompt)


def _ask_model(uscis_name: str, uscis_norm: str,
               candidates: list[dict]) -> "str | None":
    """
    Ask configured LLM to pick the best-matching FEIN.
    Returns FEIN string or None (no match).
    Raises on LLM connection/API errors — caller decides whether to ACK or leave in PEL.

    Provider is selected via H1B_LLM_PROVIDER env var (config.py):
      "gemini" — Google AI API (Gemma 4 31B by default).  Default.
      "local"  — local llama-server / Qwen3-8B.
    """
    cand_text = "\n".join(
        f"  [{i+1}] FEIN={c['fein']} | {c['name']} (similarity score: {c['score']:.0f}/100)"
        for i, c in enumerate(candidates)
    )

    if H1B_LLM_PROVIDER == "local":
        text = _ask_local(uscis_name, uscis_norm, candidates, cand_text)
    else:
        text = _ask_gemini(uscis_name, uscis_norm, candidates, cand_text)

    m = re.search(r"\{.*?\}", text, re.DOTALL)
    if not m:
        log.warning("Model returned no JSON for USCIS=%r — raw: %s", uscis_name, text[:200])
        return None
    try:
        return json.loads(m.group()).get("match") or None
    except json.JSONDecodeError as exc:
        log.warning("JSON parse failed for USCIS=%r: %s", uscis_name, exc)
        return None


# ─────────────────────────────────────────────────────────────────────────────
# DB helpers
# ─────────────────────────────────────────────────────────────────────────────

def _already_mapped(conn, employer_legal_norm: str, tax_id: str) -> bool:
    row = conn.execute("""
        SELECT 1 FROM uscis_dol_fuzzy_map
        WHERE employer_legal_norm = %s AND tax_id = %s
    """, (employer_legal_norm, tax_id)).fetchone()
    return row is not None


def _store_match(conn, employer_legal_norm: str, tax_id: str,
                 dol_fein: str, score: float, stage: str,
                 candidates: list[dict]) -> None:
    """Write match to fuzzy_map and delete from unmatched — atomic in one transaction."""
    conn.execute("""
        INSERT INTO uscis_dol_fuzzy_map
            (employer_legal_norm, tax_id, dol_fein, match_score, match_stage, candidates_json)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (employer_legal_norm, tax_id) DO UPDATE SET
            dol_fein        = EXCLUDED.dol_fein,
            match_score     = EXCLUDED.match_score,
            match_stage     = EXCLUDED.match_stage,
            candidates_json = EXCLUDED.candidates_json,
            created_at      = NOW()
    """, (employer_legal_norm, tax_id, dol_fein, score, stage, json.dumps(candidates)))

    conn.execute("""
        DELETE FROM uscis_dol_unmatched
        WHERE employer_legal_norm = %s AND tax_id = %s
    """, (employer_legal_norm, tax_id))


# ─────────────────────────────────────────────────────────────────────────────
# Job processing
# ─────────────────────────────────────────────────────────────────────────────

def _delivery_count(r, msg_id: str) -> int:
    """Return the delivery count from XPENDING for this message, or 0 on error."""
    try:
        entries = r.xpending_range(
            H1B_DISAMBIG_STREAM, H1B_DISAMBIG_GROUP,
            msg_id, msg_id, count=1,
        )
        return entries[0].get("times_delivered", 0) if entries else 0
    except Exception:
        return 0


def _move_to_dlq(r, msg_id: str, fields: dict) -> None:
    """Publish a message to the DLQ stream and XACK it from the main stream."""
    try:
        r.xadd(H1B_DISAMBIG_DLQ_STREAM, fields, maxlen=10000, approximate=True)
    except Exception as exc:
        log.error("DLQ publish failed for msg_id=%s — leaving in PEL for recovery: %s", msg_id, exc)
        return
    r.xack(H1B_DISAMBIG_STREAM, H1B_DISAMBIG_GROUP, msg_id)


def _process_job(r, msg_id, fields) -> bool:
    """Process one LLM disambiguation job.  Returns True if the message was XACKed."""
    # decode_responses=True means all values are already str — no bytes handling needed
    employer_legal_norm = fields.get("employer_legal_norm") or ""
    uscis_name          = fields.get("uscis_name") or ""
    uscis_norm          = fields.get("uscis_norm") or ""
    tax_id              = fields.get("tax_id") or ""
    candidates_raw      = fields.get("candidates") or ""

    try:
        candidates = json.loads(candidates_raw) if candidates_raw else []
    except json.JSONDecodeError:
        log.warning("malformed candidates in msg_id=%s — discarding", msg_id)
        r.xack(H1B_DISAMBIG_STREAM, H1B_DISAMBIG_GROUP, msg_id)
        return True

    if not employer_legal_norm or not tax_id or not candidates:
        log.warning("incomplete job msg_id=%s — discarding", msg_id)
        r.xack(H1B_DISAMBIG_STREAM, H1B_DISAMBIG_GROUP, msg_id)
        return True

    # DLQ guard: move messages that have failed too many times so they don't block the queue
    deliveries = _delivery_count(r, msg_id)
    if deliveries >= H1B_DISAMBIG_MAX_DELIVERIES:
        log.error(
            "msg_id=%s exceeded max deliveries (%d/%d) — moving %r to DLQ",
            msg_id, deliveries, H1B_DISAMBIG_MAX_DELIVERIES, uscis_name,
        )
        _move_to_dlq(r, msg_id, fields)
        return True

    conn = get_conn()
    try:
        if _already_mapped(conn, employer_legal_norm, tax_id):
            log.info("already mapped %r — skipping msg_id=%s", uscis_name, msg_id)
            r.xack(H1B_DISAMBIG_STREAM, H1B_DISAMBIG_GROUP, msg_id)
            return True

        conn.commit()  # release SELECT's transaction before LLM inference (2-3 min)

        log.info("LLM select: %r (tax_id=%s, %d candidates)", uscis_name, tax_id, len(candidates))

        try:
            dol_fein = _ask_model(uscis_name, uscis_norm, candidates)
        except Exception as exc:
            log.error("LLM unavailable for %r: %s — leaving in PEL", uscis_name, exc)
            return False  # no XACK — stays in PEL, reclaimed on next startup

        if dol_fein is None:
            log.debug("no match for %r", uscis_name)
            r.xack(H1B_DISAMBIG_STREAM, H1B_DISAMBIG_GROUP, msg_id)
            return True

        valid_feins = {c["fein"] for c in candidates}
        if dol_fein not in valid_feins:
            log.warning("hallucinated FEIN %r for %r — discarding", dol_fein, uscis_name)
            r.xack(H1B_DISAMBIG_STREAM, H1B_DISAMBIG_GROUP, msg_id)
            return True

        chosen_score = next(c["score"] for c in candidates if c["fein"] == dol_fein)
        _store_match(conn, employer_legal_norm, tax_id, dol_fein, chosen_score, "llm", candidates)
        conn.commit()
        r.xack(H1B_DISAMBIG_STREAM, H1B_DISAMBIG_GROUP, msg_id)
        log.info("matched %r → FEIN %s (score=%.0f)", uscis_name, dol_fein, chosen_score)
        return True

    except Exception as exc:
        log.error("unexpected error processing %r msg_id=%s: %s", uscis_name, msg_id, exc, exc_info=True)
        return False  # no XACK — stays in PEL
    finally:
        conn.close()


# ─────────────────────────────────────────────────────────────────────────────
# Main loop
# ─────────────────────────────────────────────────────────────────────────────

def _ensure_group(r) -> None:
    try:
        r.xgroup_create(H1B_DISAMBIG_STREAM, H1B_DISAMBIG_GROUP, id="0", mkstream=True)
        log.info("created consumer group %s on %s", H1B_DISAMBIG_GROUP, H1B_DISAMBIG_STREAM)
    except Exception as exc:
        if "BUSYGROUP" in str(exc):
            log.debug("consumer group %s already exists", H1B_DISAMBIG_GROUP)
        else:
            raise


def run_worker(once: bool = False) -> None:
    if H1B_LLM_PROVIDER == "local" and not LLM_BASE_URL:
        log.error("LLM_BASE_URL not set — required when H1B_LLM_PROVIDER=local")
        raise RuntimeError("LLM_BASE_URL not configured")

    r = get_redis()
    _ensure_group(r)

    # Drain PEL from any previous crash before processing new messages
    pending_drained = False
    pel_cursor = "0-0"

    hb_count = {"n": 0}
    hb = Heartbeat(r, "h1b_llm_worker", lambda: hb_count["n"], interval_s=30).start()

    log.info("h1b-llm-worker started — stream=%s group=%s consumer=%s",
             H1B_DISAMBIG_STREAM, H1B_DISAMBIG_GROUP, H1B_DISAMBIG_CONSUMER)

    # PEL entries that failed this session (LLM error) — left in PEL, skipped via cursor.
    # XAUTOCLAIM advances past failed entries so later PEL entries are still attempted.
    skipped_pel_ids: set = set()

    try:
        while True:
            while _is_maintenance(r):
                log.info("Maintenance window active — pausing for 30s")
                time.sleep(30)

            if not pending_drained:
                # XAUTOCLAIM with cursor advances past failed entries, unlike XREADGROUP "0"
                # which always re-delivers the oldest pending entry.  min_idle_time=0 means
                # re-deliver immediately; we are already the owner of these entries.
                new_cursor, entries, _ = r.xautoclaim(
                    H1B_DISAMBIG_STREAM, H1B_DISAMBIG_GROUP, H1B_DISAMBIG_CONSUMER,
                    min_idle_time=0, start_id=pel_cursor, count=1,
                )
                if not entries:
                    pending_drained = True
                    log.debug("PEL drained — switching to new messages (%d entries skipped)",
                              len(skipped_pel_ids))
                    if once:
                        break
                    continue

                msg_id, fields = entries[0]
                if msg_id in skipped_pel_ids:
                    # Already failed this session; advance cursor past it and keep draining.
                    pel_cursor = new_cursor
                    if new_cursor == "0-0":
                        pending_drained = True
                        log.debug(
                            "PEL drain complete — %d entry(s) left in PEL "
                            "(LLM unavailable; will retry on next startup)",
                            len(skipped_pel_ids),
                        )
                    continue

                was_acked = _process_job(r, msg_id, fields)
                hb_count["n"] += 1
                if not was_acked:
                    skipped_pel_ids.add(msg_id)
                pel_cursor = new_cursor
                if new_cursor == "0-0":
                    pending_drained = True
                    log.debug("PEL drained — switching to new messages")
            else:
                # Read new messages
                messages = r.xreadgroup(
                    H1B_DISAMBIG_GROUP, H1B_DISAMBIG_CONSUMER,
                    {H1B_DISAMBIG_STREAM: ">"}, count=1,
                    block=H1B_DISAMBIG_BLOCK_MS,
                )

                if not messages:
                    if once:
                        break
                    continue

                for _stream, entries in messages:
                    for msg_id, fields in entries:
                        _process_job(r, msg_id, fields)
                        hb_count["n"] += 1

            if once:
                break
    finally:
        hb.stop()

    log.info("h1b-llm-worker stopped")


if __name__ == "__main__":
    init_logging("h1b_llm_worker")
    once = "--once" in sys.argv
    run_worker(once=once)
