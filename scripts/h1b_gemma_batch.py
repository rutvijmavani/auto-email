"""
scripts/h1b_gemma_batch.py — Batch H1B disambiguation using Gemma 4 31B via Google AI Studio.

Reads from Redis stream llm:h1b:disambiguate (same stream as h1b_llm_worker).
Uses the same consumer group so processed entries are not re-delivered.
Writes matches to uscis_dol_fuzzy_map.

Designed to drain the queue fast (30 RPM vs Qwen3's 1 per 7-11 min).
Rate limiter enforces both RPM and TPM sliding windows — model can use as many
tokens per request as it needs; the limiter pauses when the 60s window fills up.

Usage:
    Stop h1b-llm-worker first to avoid competing for the same stream messages:
        sudo systemctl stop h1b-llm-worker

    Set API key:
        export GOOGLE_API_KEY=<your_key>

    Run:
        python scripts/h1b_gemma_batch.py
        python scripts/h1b_gemma_batch.py --dry-run   # parse + log, no DB writes
"""

import argparse
import json
import os
import re
import sys
import time

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from google import genai
from google.genai import types

from config import (
    H1B_DISAMBIG_DLQ_STREAM,
    H1B_DISAMBIG_GROUP,
    H1B_DISAMBIG_MAX_DELIVERIES,
    H1B_DISAMBIG_STREAM,
)
from db.connection import get_conn
from logger import get_logger, init_logging
from workers.rate_limiter import DualRateLimiter
from workers.redis_client import get_redis

log = get_logger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────

GEMMA_MODEL       = os.getenv("GEMMA_MODEL", "gemma-4-31b-it")
GEMMA_RPM         = int(os.getenv("GEMMA_RPM", "30"))
GEMMA_TPM         = int(os.getenv("GEMMA_TPM", "16000"))
GEMMA_CONSUMER    = os.getenv("GEMMA_CONSUMER", "h1b-gemma-batch-1")


# ── Prompt ────────────────────────────────────────────────────────────────────

# Same logic as h1b_llm_worker but without /think (Qwen3-specific prefix).
_MATCH_PROMPT = """\
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


# ── Gemma client ──────────────────────────────────────────────────────────────

_GEMMA_MAX_CONSECUTIVE_ERRORS = int(os.getenv("GEMMA_MAX_CONSECUTIVE_ERRORS", "5"))


def _init_gemma() -> genai.Client:
    api_key = os.environ.get("GOOGLE_API_KEY")
    if not api_key:
        log.error("GOOGLE_API_KEY env var not set")
        sys.exit(1)
    return genai.Client(api_key=api_key)


def _ask_gemma(client: genai.Client, limiter: DualRateLimiter,
               uscis_name: str, uscis_norm: str,
               candidates: list[dict]) -> tuple["str | None", int]:
    """
    Call Gemma and return (matched_fein_or_None, total_tokens).
    Raises on API errors — caller decides whether to XACK or leave in PEL.
    """
    cand_text = "\n".join(
        f"  [{i+1}] FEIN={c['fein']} | {c['name']} (similarity score: {c['score']:.0f}/100)"
        for i, c in enumerate(candidates)
    )
    prompt = _MATCH_PROMPT.format(
        uscis_name=uscis_name,
        uscis_norm=uscis_norm,
        candidates=cand_text,
    )

    estimated = len(prompt) // 4 + 50
    limiter.wait(estimated_tokens=estimated)

    response = client.models.generate_content(
        model=GEMMA_MODEL,
        contents=prompt,
        config=types.GenerateContentConfig(temperature=0),
    )

    tokens = getattr(response.usage_metadata, "total_token_count", 0) or 0
    limiter.record(tokens)

    text = response.text.strip()
    m = re.search(r"\{.*?\}", text, re.DOTALL)
    if not m:
        log.warning("Gemma returned no JSON for %r — raw: %s", uscis_name, text[:200])
        return None, tokens

    try:
        result = json.loads(m.group()).get("match")
        return result or None, tokens
    except json.JSONDecodeError as exc:
        log.warning("JSON parse failed for %r: %s", uscis_name, exc)
        return None, tokens


# ── DB helpers (same logic as h1b_llm_worker) ────────────────────────────────

def _already_mapped(conn, employer_legal_norm: str, tax_id: str) -> bool:
    row = conn.execute("""
        SELECT 1 FROM uscis_dol_fuzzy_map
        WHERE employer_legal_norm = %s AND tax_id = %s
    """, (employer_legal_norm, tax_id)).fetchone()
    return row is not None


def _store_match(conn, employer_legal_norm: str, tax_id: str,
                 dol_fein: str, score: float,
                 candidates: list[dict]) -> None:
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
    """, (employer_legal_norm, tax_id, dol_fein, score, "llm_gemma", json.dumps(candidates)))

    conn.execute("""
        DELETE FROM uscis_dol_unmatched
        WHERE employer_legal_norm = %s AND tax_id = %s
    """, (employer_legal_norm, tax_id))


# ── Stream processing ─────────────────────────────────────────────────────────

def _delivery_count(r, msg_id: str) -> int:
    try:
        entries = r.xpending_range(
            H1B_DISAMBIG_STREAM, H1B_DISAMBIG_GROUP,
            msg_id, msg_id, count=1,
        )
        return entries[0].get("times_delivered", 0) if entries else 0
    except Exception:
        return 0


def run_batch(dry_run: bool = False) -> None:
    log.info("h1b-gemma-batch starting — model=%s rpm=%d tpm=%d dry_run=%s",
             GEMMA_MODEL, GEMMA_RPM, GEMMA_TPM, dry_run)

    client  = _init_gemma()
    limiter = DualRateLimiter(rpm=GEMMA_RPM, tpm=GEMMA_TPM)
    r       = get_redis()
    conn    = get_conn()

    try:
        # Ensure consumer group exists
        try:
            r.xgroup_create(H1B_DISAMBIG_STREAM, H1B_DISAMBIG_GROUP, id="0", mkstream=False)
            log.info("consumer group %s created", H1B_DISAMBIG_GROUP)
        except Exception:
            log.debug("consumer group %s already exists", H1B_DISAMBIG_GROUP)

        processed = matched = skipped = errors = consecutive_errors = 0
        pel_drained = False
        pel_cursor = "0-0"

        while True:
            # ── PEL drain first ──────────────────────────────────────────────────
            if not pel_drained:
                result = r.xautoclaim(
                    H1B_DISAMBIG_STREAM, H1B_DISAMBIG_GROUP, GEMMA_CONSUMER,
                    min_idle_time=0, start_id=pel_cursor, count=1,
                )
                next_cursor, messages = result[0], result[1]
                if not messages:
                    pel_drained = True
                    log.info("PEL drained — switching to new messages")
                    continue
                pel_cursor = next_cursor
            else:
                # ── New messages ─────────────────────────────────────────────────
                result = r.xreadgroup(
                    H1B_DISAMBIG_GROUP, GEMMA_CONSUMER,
                    {H1B_DISAMBIG_STREAM: ">"}, count=1, block=5000,
                )
                if not result:
                    log.info("queue empty — batch complete. processed=%d matched=%d skipped=%d errors=%d",
                             processed, matched, skipped, errors)
                    break
                messages = result[0][1]

            for msg_id, fields in messages:
                employer_legal_norm = fields.get("employer_legal_norm") or ""
                uscis_name          = fields.get("uscis_name") or ""
                uscis_norm          = fields.get("uscis_norm") or ""
                tax_id              = fields.get("tax_id") or ""
                candidates_raw      = fields.get("candidates") or ""

                try:
                    candidates = json.loads(candidates_raw) if candidates_raw else []
                except json.JSONDecodeError:
                    log.warning("malformed candidates msg_id=%s — discarding", msg_id)
                    r.xack(H1B_DISAMBIG_STREAM, H1B_DISAMBIG_GROUP, msg_id)
                    processed += 1
                    continue

                if not employer_legal_norm or not tax_id or not candidates:
                    log.warning("incomplete job msg_id=%s — discarding", msg_id)
                    r.xack(H1B_DISAMBIG_STREAM, H1B_DISAMBIG_GROUP, msg_id)
                    processed += 1
                    continue

                # DLQ guard
                deliveries = _delivery_count(r, msg_id)
                if deliveries >= H1B_DISAMBIG_MAX_DELIVERIES:
                    log.error("msg_id=%s exceeded max deliveries — moving to DLQ", msg_id)
                    try:
                        r.xadd(H1B_DISAMBIG_DLQ_STREAM, fields, maxlen=10000, approximate=True)
                    except Exception as exc:
                        log.error("DLQ publish failed: %s", exc)
                    r.xack(H1B_DISAMBIG_STREAM, H1B_DISAMBIG_GROUP, msg_id)
                    processed += 1
                    continue

                # Idempotency
                if _already_mapped(conn, employer_legal_norm, tax_id):
                    log.info("already mapped %r — skipping msg_id=%s", uscis_name, msg_id)
                    r.xack(H1B_DISAMBIG_STREAM, H1B_DISAMBIG_GROUP, msg_id)
                    conn.commit()
                    skipped += 1
                    processed += 1
                    continue

                conn.commit()  # release idempotency SELECT before API call

                candidate_feins = {c["fein"] for c in candidates}
                log.info("LLM select: %r (tax_id=%s, %d candidates) [%s]",
                         uscis_name, tax_id, len(candidates), limiter.status())

                try:
                    dol_fein, tokens = _ask_gemma(
                        client, limiter, uscis_name, uscis_norm, candidates,
                    )
                    consecutive_errors = 0
                except Exception as exc:
                    log.error("Gemma API error for %r: %s — leaving in PEL", uscis_name, exc)
                    errors += 1
                    consecutive_errors += 1
                    if consecutive_errors >= _GEMMA_MAX_CONSECUTIVE_ERRORS:
                        log.error("too many consecutive errors (%d) — stopping batch",
                                  consecutive_errors)
                        break
                    backoff = min(2 ** consecutive_errors, 60)
                    log.info("backing off %ds before next attempt", backoff)
                    time.sleep(backoff)
                    continue  # no XACK — stays in PEL for retry

                if dol_fein and not re.match(r"^\d{2}-\d{7}$", dol_fein):
                    log.warning("hallucinated FEIN %r for %r — discarding (tokens=%d)",
                                dol_fein, uscis_name, tokens)
                    r.xack(H1B_DISAMBIG_STREAM, H1B_DISAMBIG_GROUP, msg_id)
                    processed += 1
                    continue

                if dol_fein and dol_fein not in candidate_feins:
                    log.warning("FEIN %r not in candidates for %r — discarding (tokens=%d)",
                                dol_fein, uscis_name, tokens)
                    r.xack(H1B_DISAMBIG_STREAM, H1B_DISAMBIG_GROUP, msg_id)
                    processed += 1
                    continue

                if dol_fein:
                    selected = next(c for c in candidates if c["fein"] == dol_fein)
                    score = selected.get("score", 0)
                    log.info("matched %r → FEIN %s score=%.0f (tokens=%d)",
                             uscis_name, dol_fein, score, tokens)
                    if not dry_run:
                        _store_match(conn, employer_legal_norm, tax_id,
                                     dol_fein, score, candidates)
                        conn.commit()
                    matched += 1
                else:
                    log.info("no match for %r (tokens=%d)", uscis_name, tokens)

                r.xack(H1B_DISAMBIG_STREAM, H1B_DISAMBIG_GROUP, msg_id)
                processed += 1

                if processed % 100 == 0:
                    log.info("progress: processed=%d matched=%d skipped=%d errors=%d [%s]",
                             processed, matched, skipped, errors, limiter.status())

    finally:
        conn.close()
    log.info("batch finished — processed=%d matched=%d skipped=%d errors=%d",
             processed, matched, skipped, errors)


if __name__ == "__main__":
    init_logging("h1b_gemma_batch")
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true",
                        help="Parse and log results but skip DB writes")
    args = parser.parse_args()
    run_batch(dry_run=args.dry_run)
