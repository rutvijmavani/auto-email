"""
scripts/fuzzy_match_uscis_dol.py — Fuzzy + LLM matching for unresolved USCIS → DOL rows.

For each row in uscis_dol_unmatched, this script:
  1. Finds all DOL employers sharing the same last-4 FEIN (tax_id).
  2. Applies rapidfuzz token_set_ratio to rank candidates; keeps top 3 with score >= 40.
  3. Sends the top candidates to Qwen3-8B for a selection decision — model picks the best
     matching DOL FEIN from the candidates, or null if none match.
  4. Stores confirmed matches in uscis_dol_fuzzy_map.

After running, re-run --refresh-unmatched to update the diagnostic table:
    python scripts/process_uscis_h1b.py --refresh-unmatched

Usage:
    python scripts/fuzzy_match_uscis_dol.py
    python scripts/fuzzy_match_uscis_dol.py --limit 500   # process at most N unmatched rows
    python scripts/fuzzy_match_uscis_dol.py --dry-run     # print candidates without storing
"""

import argparse
import json
import multiprocessing
import os
import queue
import re
import sys
import time

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from db.connection import get_conn
from logger import get_logger, init_logging

try:
    from rapidfuzz import fuzz as _fuzz
except ImportError:
    print("[ERROR] rapidfuzz is not installed. Run: pip install rapidfuzz")
    sys.exit(1)

try:
    from llama_cpp import Llama
except ImportError:
    print("[ERROR] llama-cpp-python is not installed. Run: pip install llama-cpp-python")
    sys.exit(1)

log = get_logger(__name__)

_MODEL_PATH            = os.environ.get("EMAIL_PROCESSOR_MODEL_PATH", "")
_FUZZY_THRESHOLD       = 40   # min score to include as a candidate at all
_FUZZY_AUTO_THRESHOLD  = 95   # score >= this → auto-match without LLM (match_stage='fuzzy')
_FUZZY_DOMINANT_SCORE  = 80   # dominant-winner rule: best >= this AND gap >= _FUZZY_DOMINANT_GAP
_FUZZY_DOMINANT_GAP    = 25   # minimum gap between best and second score to auto-match
_TOP_N_CANDIDATES      = 3    # max candidates sent to LLM when score < auto threshold
_INFERENCE_TIMEOUT     = 300  # seconds before treating inference as hung (model load ~90s + inference)

_llm: "Llama | None" = None


# ─────────────────────────────────────────────────────────────────────────────
# Model
# ─────────────────────────────────────────────────────────────────────────────

def _load_model() -> None:
    global _llm
    if _llm is not None:
        return
    if not _MODEL_PATH:
        log.error(
            "EMAIL_PROCESSOR_MODEL_PATH is not set — "
            "point it at the Qwen3-8B-Q4_K_M.gguf file"
        )
        sys.exit(1)
    log.info("Loading Qwen3-8B from %s …", _MODEL_PATH)
    _llm = Llama(model_path=_MODEL_PATH, n_ctx=4096, n_threads=2, verbose=False)
    log.info("Model ready")


def _child_infer(q: multiprocessing.Queue, prompt: str, model_path: str) -> None:
    """Top-level (picklable) worker: loads model in child and runs one inference."""
    try:
        from llama_cpp import Llama  # type: ignore[import]
        llm = Llama(model_path=model_path, n_ctx=4096, n_threads=2, verbose=False)
        resp = llm.create_chat_completion(
            messages=[{"role": "user", "content": prompt}],
            max_tokens=120,
            temperature=0,
        )
        q.put(("ok", resp))
    except Exception as exc:
        try:
            q.put(("err", RuntimeError(str(exc))))
        except Exception:
            pass


def _run_with_timeout(prompt: str, timeout: int = _INFERENCE_TIMEOUT):
    """
    Run inference in a child process; terminate on timeout.
    Uses a module-level target so the process is picklable under spawn/forkserver.
    """
    q: multiprocessing.Queue = multiprocessing.Queue()
    p = multiprocessing.Process(target=_child_infer, args=(q, prompt, _MODEL_PATH))
    p.start()
    try:
        p.join(timeout)
        if p.is_alive():
            p.terminate()
            p.join()
            raise RuntimeError(f"Inference timed out after {timeout}s")
        try:
            kind, val = q.get(timeout=2)
            if kind == "ok":
                return val
            raise val
        except queue.Empty:
            raise RuntimeError("Inference process exited without result")
    finally:
        if p.is_alive():
            p.terminate()
            p.join()
        q.close()
        q.join_thread()
        p.close()


def _strip_think(text: str) -> str:
    """Remove Qwen3 <think>…</think> block so only the final answer remains."""
    return re.sub(r"<think>.*?</think>", "", text, flags=re.DOTALL).strip()


# ─────────────────────────────────────────────────────────────────────────────
# Prompt
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


# ─────────────────────────────────────────────────────────────────────────────
# DB helpers
# ─────────────────────────────────────────────────────────────────────────────

def _fetch_unmatched(conn, limit: "int | None") -> list[dict]:
    """Fetch distinct (employer_name, employer_legal_norm, tax_id) from unmatched table,
    joined to petitions to recover employer_legal_norm (not stored in unmatched table)."""
    sql = """
        SELECT DISTINCT
            n.employer_name,
            n.employer_name_norm,
            p.employer_legal_norm,
            n.tax_id,
            n.total_approvals
        FROM uscis_dol_unmatched n
        JOIN uscis_h1b_petitions p
          ON p.employer_name = n.employer_name
         AND p.tax_id        = n.tax_id
        ORDER BY n.total_approvals DESC
    """
    if limit:
        sql += f" LIMIT {int(limit)}"
    rows = conn.execute(sql).fetchall()
    return [dict(r) for r in rows]


def _fetch_dol_candidates(conn, tax_id: str) -> list[dict]:
    """All DOL employers whose last-4 FEIN matches tax_id."""
    rows = conn.execute("""
        SELECT employer_fein, employer_name, employer_name_norm
        FROM dol_h1b_employers
        WHERE right(employer_fein, 4) = %s
    """, (tax_id,)).fetchall()
    return [dict(r) for r in rows]


def _score_candidates(uscis_norm: str, candidates: list[dict]) -> list[dict]:
    """Rank candidates by rapidfuzz token_set_ratio; return top N above threshold."""
    scored = []
    for c in candidates:
        score = _fuzz.token_set_ratio(uscis_norm, c["employer_name_norm"])
        if score >= _FUZZY_THRESHOLD:
            scored.append({**c, "score": score})
    scored.sort(key=lambda x: -x["score"])
    return scored[:_TOP_N_CANDIDATES]


def _already_mapped(conn, employer_legal_norm: str, tax_id: str) -> bool:
    row = conn.execute("""
        SELECT 1 FROM uscis_dol_fuzzy_map
        WHERE employer_legal_norm = %s AND tax_id = %s
    """, (employer_legal_norm, tax_id)).fetchone()
    return row is not None


def _store_match(conn, employer_legal_norm: str, tax_id: str,
                 dol_fein: str, score: float, stage: str) -> None:
    conn.execute("""
        INSERT INTO uscis_dol_fuzzy_map
            (employer_legal_norm, tax_id, dol_fein, match_score, match_stage)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (employer_legal_norm, tax_id) DO UPDATE SET
            dol_fein    = EXCLUDED.dol_fein,
            match_score = EXCLUDED.match_score,
            match_stage = EXCLUDED.match_stage,
            created_at  = NOW()
    """, (employer_legal_norm, tax_id, dol_fein, score, stage))


# ─────────────────────────────────────────────────────────────────────────────
# LLM call
# ─────────────────────────────────────────────────────────────────────────────

def _ask_model(uscis_name: str, uscis_norm: str,
               candidates: list[dict]) -> "str | None":
    """
    Ask Qwen3-8B to select the best-matching DOL FEIN from the candidates.
    Returns the FEIN string if a match is found, None otherwise.
    """
    cand_text = "\n".join(
        f"  [{i+1}] FEIN={c['employer_fein']} | {c['employer_name']} "
        f"(similarity score: {c['score']:.0f}/100)"
        for i, c in enumerate(candidates)
    )
    prompt = _MATCH_PROMPT.format(
        uscis_name=uscis_name,
        uscis_norm=uscis_norm,
        candidates=cand_text,
    )

    try:
        resp = _run_with_timeout(prompt)
        text = _strip_think(resp["choices"][0]["message"]["content"])
        m = re.search(r"\{.*?\}", text, re.DOTALL)
        if not m:
            log.warning("Model returned no JSON for USCIS=%r — raw: %s", uscis_name, text[:200])
            return None
        data = json.loads(m.group())
        return data.get("match") or None
    except Exception as exc:
        log.warning("Inference failed for USCIS=%r: %s", uscis_name, exc)
        return None


# ─────────────────────────────────────────────────────────────────────────────
# Main loop
# ─────────────────────────────────────────────────────────────────────────────

def run(limit: "int | None" = None, dry_run: bool = False) -> None:
    conn = get_conn()
    try:
        _run_body(conn, limit, dry_run)
    finally:
        conn.close()


def _run_body(conn, limit: "int | None", dry_run: bool) -> None:
    unmatched = _fetch_unmatched(conn, limit)
    log.info("Processing %d unmatched USCIS entries", len(unmatched))

    fuzzy_auto = llm_matched = skipped = no_candidates = model_none = already = 0

    for row in unmatched:
        uscis_name      = row["employer_name"]
        # Use COALESCE(NULLIF(employer_legal_norm,''), employer_name_norm) so the
        # stored key matches the SQL join expression used by readers.
        uscis_norm      = row["employer_legal_norm"] or row["employer_name_norm"]
        tax_id          = row["tax_id"]
        total_approvals = row["total_approvals"]

        if _already_mapped(conn, uscis_norm, tax_id):
            already += 1
            continue

        candidates = _fetch_dol_candidates(conn, tax_id)
        if not candidates:
            no_candidates += 1
            log.debug("No DOL candidates for tax_id=%s (%s)", tax_id, uscis_name)
            continue

        top = _score_candidates(uscis_norm, candidates)
        if not top:
            skipped += 1
            log.debug(
                "No fuzzy candidate above threshold for %r (tax_id=%s, %d DOL records checked)",
                uscis_name, tax_id, len(candidates),
            )
            continue

        best = top[0]

        # ── High-confidence fuzzy auto-match (score ≥ 95) ────────────────────
        if best["score"] >= _FUZZY_AUTO_THRESHOLD:
            log.info(
                "fuzzy auto-match: %r → FEIN %s | %s (score=%.0f, approvals=%d)",
                uscis_name, best["employer_fein"], best["employer_name"],
                best["score"], total_approvals,
            )
            if not dry_run:
                _store_match(conn, uscis_norm, tax_id,
                             best["employer_fein"], best["score"], stage="fuzzy")
                conn.commit()
            fuzzy_auto += 1
            continue

        # ── Dominant-winner: clear #1 with large gap from #2 (no LLM needed) ─
        second_score = top[1]["score"] if len(top) > 1 else 0
        if best["score"] >= _FUZZY_DOMINANT_SCORE and (best["score"] - second_score) >= _FUZZY_DOMINANT_GAP:
            log.info(
                "fuzzy dominant-match: %r → FEIN %s | %s (score=%.0f, gap=%.0f, approvals=%d)",
                uscis_name, best["employer_fein"], best["employer_name"],
                best["score"], best["score"] - second_score, total_approvals,
            )
            if not dry_run:
                _store_match(conn, uscis_norm, tax_id,
                             best["employer_fein"], best["score"], stage="fuzzy_dominant")
                conn.commit()
            fuzzy_auto += 1
            continue

        # ── Ambiguous — send to Qwen3-8B for selection (score 40–94, gap < 25) ─
        log.info(
            "LLM select: %r  (approvals=%d, top_score=%.0f, sending %d candidates)",
            uscis_name, total_approvals, best["score"], len(top),
        )
        for c in top:
            log.info("  candidate: FEIN=%s | %s (score=%.0f)",
                     c["employer_fein"], c["employer_name"], c["score"])

        if dry_run:
            continue

        dol_fein = _ask_model(uscis_name, uscis_norm, top)

        if dol_fein is None:
            model_none += 1
            log.debug("Model: no match for %r", uscis_name)
            continue

        # Validate the returned FEIN is one of the candidates (anti-hallucination)
        valid_feins = {c["employer_fein"] for c in top}
        if dol_fein not in valid_feins:
            log.warning(
                "Model returned FEIN %r not in candidates for %r — discarding",
                dol_fein, uscis_name,
            )
            model_none += 1
            continue

        chosen_score = next(c["score"] for c in top if c["employer_fein"] == dol_fein)
        _store_match(conn, uscis_norm, tax_id, dol_fein, chosen_score, stage="llm")
        conn.commit()
        llm_matched += 1
        log.info("  → MATCHED to FEIN %s (score=%.0f)", dol_fein, chosen_score)

    log.info(
        "Done: fuzzy_auto=%d  llm_matched=%d  skipped(below_threshold)=%d  "
        "no_dol_candidates=%d  model_no_match=%d  already_mapped=%d",
        fuzzy_auto, llm_matched, skipped, no_candidates, model_none, already,
    )
    if not dry_run and (fuzzy_auto + llm_matched) > 0:
        log.info("Fuzzy pass complete — caller should refresh uscis_dol_unmatched")


def main():
    parser = argparse.ArgumentParser(
        description="Fuzzy + Qwen3-8B match unresolved USCIS employers to DOL LCA records"
    )
    parser.add_argument("--limit",   type=int, default=None,
                        help="Max unmatched rows to process")
    parser.add_argument("--dry-run", action="store_true",
                        help="Print candidates without running inference or storing results")
    args = parser.parse_args()
    run(limit=args.limit, dry_run=args.dry_run)


if __name__ == "__main__":
    init_logging("fuzzy_match_uscis_dol")
    main()
