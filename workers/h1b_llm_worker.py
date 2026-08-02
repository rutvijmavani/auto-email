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

import openai

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from config import (
    H1B_DISAMBIG_BLOCK_MS,
    H1B_DISAMBIG_CONSUMER,
    H1B_DISAMBIG_GROUP,
    H1B_DISAMBIG_STREAM,
    LLM_BASE_URL,
    LLM_MODEL,
)
from db.connection import get_conn
from logger import get_logger, init_logging
from workers.heartbeat import Heartbeat
from workers.redis_client import get_redis

log = get_logger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# LLM
# ─────────────────────────────────────────────────────────────────────────────

_MATCH_PROMPT = """/think
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


def _call_llm(prompt: str) -> str:
    """Send prompt to llama-server. Raises on any connection or API error."""
    client = openai.OpenAI(base_url=LLM_BASE_URL, api_key="none")
    resp = client.chat.completions.create(
        model=LLM_MODEL,
        messages=[{"role": "user", "content": prompt}],
        temperature=0,
    )
    text = resp.choices[0].message.content or ""
    return re.sub(r"<think>.*?</think>", "", text, flags=re.DOTALL).strip()


def _ask_model(uscis_name: str, uscis_norm: str,
               candidates: list[dict]) -> "str | None":
    """
    Ask LLM to pick the best-matching FEIN. Returns FEIN string or None (no match).
    Raises on LLM connection/API errors — caller decides whether to ACK or leave in PEL.
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
    text = _call_llm(prompt)  # raises on connection error
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

def _decode(v) -> str:
    return v.decode() if isinstance(v, bytes) else (v or "")


def _process_job(r, msg_id, fields) -> None:
    employer_legal_norm = _decode(fields.get(b"employer_legal_norm") or fields.get("employer_legal_norm"))
    uscis_name          = _decode(fields.get(b"uscis_name")          or fields.get("uscis_name"))
    uscis_norm          = _decode(fields.get(b"uscis_norm")          or fields.get("uscis_norm"))
    tax_id              = _decode(fields.get(b"tax_id")              or fields.get("tax_id"))
    candidates_raw      = _decode(fields.get(b"candidates")          or fields.get("candidates"))

    try:
        candidates = json.loads(candidates_raw) if candidates_raw else []
    except json.JSONDecodeError:
        log.warning("malformed candidates in msg_id=%s — discarding", msg_id)
        r.xack(H1B_DISAMBIG_STREAM, H1B_DISAMBIG_GROUP, msg_id)
        return

    if not employer_legal_norm or not tax_id or not candidates:
        log.warning("incomplete job msg_id=%s — discarding", msg_id)
        r.xack(H1B_DISAMBIG_STREAM, H1B_DISAMBIG_GROUP, msg_id)
        return

    conn = get_conn()
    try:
        if _already_mapped(conn, employer_legal_norm, tax_id):
            log.info("already mapped %r — skipping msg_id=%s", uscis_name, msg_id)
            r.xack(H1B_DISAMBIG_STREAM, H1B_DISAMBIG_GROUP, msg_id)
            return

        log.info("LLM select: %r (tax_id=%s, %d candidates)", uscis_name, tax_id, len(candidates))

        try:
            dol_fein = _ask_model(uscis_name, uscis_norm, candidates)
        except Exception as exc:
            log.error("LLM unavailable for %r: %s — leaving in PEL", uscis_name, exc)
            return  # no XACK — stays in PEL, reclaimed on next startup

        if dol_fein is None:
            log.debug("no match for %r", uscis_name)
            r.xack(H1B_DISAMBIG_STREAM, H1B_DISAMBIG_GROUP, msg_id)
            return

        valid_feins = {c["fein"] for c in candidates}
        if dol_fein not in valid_feins:
            log.warning("hallucinated FEIN %r for %r — discarding", dol_fein, uscis_name)
            r.xack(H1B_DISAMBIG_STREAM, H1B_DISAMBIG_GROUP, msg_id)
            return

        chosen_score = next(c["score"] for c in candidates if c["fein"] == dol_fein)
        _store_match(conn, employer_legal_norm, tax_id, dol_fein, chosen_score, "llm", candidates)
        conn.commit()
        r.xack(H1B_DISAMBIG_STREAM, H1B_DISAMBIG_GROUP, msg_id)
        log.info("matched %r → FEIN %s (score=%.0f)", uscis_name, dol_fein, chosen_score)

    except Exception as exc:
        log.error("unexpected error processing %r msg_id=%s: %s", uscis_name, msg_id, exc, exc_info=True)
        # no XACK — stays in PEL
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
    if not LLM_BASE_URL:
        log.error("LLM_BASE_URL not set — start llama-server and set LLM_BASE_URL in .env")
        raise RuntimeError("LLM_BASE_URL not configured")

    r = get_redis()
    _ensure_group(r)

    # Drain PEL from any previous crash before processing new messages
    pending_drained = False
    stream_id       = "0"

    hb_count = {"n": 0}
    hb = Heartbeat(r, "h1b_llm_worker", lambda: hb_count["n"], interval_s=30).start()

    log.info("h1b-llm-worker started — stream=%s group=%s consumer=%s",
             H1B_DISAMBIG_STREAM, H1B_DISAMBIG_GROUP, H1B_DISAMBIG_CONSUMER)

    try:
        while True:
            if not pending_drained:
                # Read PEL entries for this consumer (id="0")
                messages = r.xreadgroup(
                    H1B_DISAMBIG_GROUP, H1B_DISAMBIG_CONSUMER,
                    {H1B_DISAMBIG_STREAM: "0"}, count=1,
                )
                if not messages or not messages[0][1]:
                    pending_drained = True
                    log.debug("PEL drained — switching to new messages")
                    if once:
                        break
                    continue
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
