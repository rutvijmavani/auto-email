"""
scripts/fuzzy_match_uscis_dol.py — Fuzzy matching for unresolved USCIS → DOL rows.

For each row in uscis_dol_unmatched, this script:
  1. Finds all DOL employers sharing the same last-4 FEIN (tax_id).
  2. Applies rapidfuzz token_set_ratio to rank candidates; keeps top 3 with score >= 40.
  3. Auto-matches high-confidence cases (score >= 95 or dominant winner).
  4. Pushes ambiguous cases to Redis stream llm:h1b:disambiguate for async LLM resolution
     by workers/h1b_llm_worker.py.

Auto-matched rows are written atomically to uscis_dol_fuzzy_map and deleted from
uscis_dol_unmatched in the same transaction.  Ambiguous rows stay in uscis_dol_unmatched
until the LLM worker resolves them.

Usage:
    python scripts/fuzzy_match_uscis_dol.py
    python scripts/fuzzy_match_uscis_dol.py --limit 500   # process at most N unmatched rows
    python scripts/fuzzy_match_uscis_dol.py --dry-run     # print candidates without storing
"""

import argparse
import json
import os
import sys
import time

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from config import H1B_DISAMBIG_MAXLEN, H1B_DISAMBIG_STREAM, REDIS_DB_MAINTENANCE
from db.connection import get_conn
from logger import get_logger, init_logging
from workers.redis_client import get_redis

try:
    from rapidfuzz import fuzz as _fuzz
except ImportError:
    print("[ERROR] rapidfuzz is not installed. Run: pip install rapidfuzz")
    sys.exit(1)

log = get_logger(__name__)

_FUZZY_THRESHOLD       = 40   # min score to include as a candidate at all
_FUZZY_AUTO_THRESHOLD  = 95   # score >= this → auto-match without LLM (match_stage='fuzzy')
_FUZZY_DOMINANT_SCORE  = 80   # dominant-winner rule: best >= this AND gap >= _FUZZY_DOMINANT_GAP
_FUZZY_DOMINANT_GAP    = 25   # minimum gap between best and second score to auto-match
_TOP_N_CANDIDATES      = 3    # max candidates sent to LLM when score < auto threshold


# ─────────────────────────────────────────────────────────────────────────────
# Maintenance window
# ─────────────────────────────────────────────────────────────────────────────

def _is_maintenance(r) -> bool:
    if r is None:
        return False
    try:
        return bool(r.exists(REDIS_DB_MAINTENANCE))
    except Exception as exc:
        log.warning("Redis maintenance check failed (%s) — assuming not in maintenance", exc)
        return False


def _ensure_schema(conn) -> None:
    """Add queued_for_llm column if not yet present (idempotent)."""
    conn.execute("""
        ALTER TABLE uscis_dol_unmatched
        ADD COLUMN IF NOT EXISTS queued_for_llm BOOLEAN DEFAULT FALSE
    """)
    conn.commit()


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
        WHERE n.queued_for_llm IS NOT TRUE
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


def _clean_candidates(candidates: list[dict]) -> list[dict]:
    """Strip internal normalization fields — keep human-readable keys for audit trail."""
    return [
        {"fein": c["employer_fein"], "name": c["employer_name"], "score": round(c["score"], 1)}
        for c in candidates
    ]


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


def _push_to_stream(r, conn, uscis_norm: str, uscis_name: str,
                    tax_id: str, candidates: list[dict]) -> None:
    """Atomically claim the unmatched row, then publish to the LLM stream.

    The UPDATE uses RETURNING id with an IS NOT TRUE guard so the claim is
    idempotent: if the row was already queued (e.g. script restarted mid-run),
    RETURNING returns nothing and we skip the XADD — no duplicate stream entry.
    If XADD fails after commit, the row stays claimed (queued_for_llm=TRUE).
    populate_unmatched preserves that flag, so the orphan is skipped on the next
    fuzzy pass rather than re-queued — acceptable since the stream entry is lost.
    """
    row = conn.execute("""
        UPDATE uscis_dol_unmatched
        SET queued_for_llm = TRUE
        WHERE employer_name = %s AND tax_id = %s
          AND queued_for_llm IS NOT TRUE
        RETURNING id
    """, (uscis_name, tax_id)).fetchone()
    conn.commit()
    if row is None:
        log.debug("Row already claimed for LLM (employer_name=%s, tax_id=%s) — skipping XADD", uscis_name, tax_id)
        return
    r.xadd(
        H1B_DISAMBIG_STREAM,
        {
            "employer_legal_norm": uscis_norm,
            "uscis_name":          uscis_name,
            "uscis_norm":          uscis_norm,
            "tax_id":              tax_id,
            "candidates":          json.dumps(candidates),
        },
        maxlen=H1B_DISAMBIG_MAXLEN,
        approximate=True,
    )


# ─────────────────────────────────────────────────────────────────────────────
# Main loop
# ─────────────────────────────────────────────────────────────────────────────

def run(limit: "int | None" = None, dry_run: bool = False) -> None:
    conn = get_conn()
    r    = None if dry_run else get_redis()
    try:
        if not dry_run:
            _ensure_schema(conn)
        _run_body(conn, r, limit, dry_run)
    finally:
        conn.close()


def _run_body(conn, r, limit: "int | None", dry_run: bool) -> None:
    unmatched = _fetch_unmatched(conn, limit)
    log.info("Processing %d unmatched USCIS entries", len(unmatched))

    fuzzy_auto = queued = skipped = no_candidates = already = 0

    for row in unmatched:
        while _is_maintenance(r):
            log.info("Maintenance window active — pausing for 30s")
            time.sleep(30)

        uscis_name      = row["employer_name"]
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

        best         = top[0]
        clean        = _clean_candidates(top)
        second_score = top[1]["score"] if len(top) > 1 else 0

        # ── High-confidence fuzzy auto-match (score ≥ 95) ────────────────────
        if best["score"] >= _FUZZY_AUTO_THRESHOLD:
            log.info(
                "fuzzy auto-match: %r → FEIN %s | %s (score=%.0f, approvals=%d)",
                uscis_name, best["employer_fein"], best["employer_name"],
                best["score"], total_approvals,
            )
            if not dry_run:
                _store_match(conn, uscis_norm, tax_id,
                             best["employer_fein"], best["score"], "fuzzy", clean)
                conn.commit()
            fuzzy_auto += 1
            continue

        # ── Dominant-winner: clear #1 with large gap from #2 ─────────────────
        if best["score"] >= _FUZZY_DOMINANT_SCORE and (best["score"] - second_score) >= _FUZZY_DOMINANT_GAP:
            log.info(
                "fuzzy dominant-match: %r → FEIN %s | %s (score=%.0f, gap=%.0f, approvals=%d)",
                uscis_name, best["employer_fein"], best["employer_name"],
                best["score"], best["score"] - second_score, total_approvals,
            )
            if not dry_run:
                _store_match(conn, uscis_norm, tax_id,
                             best["employer_fein"], best["score"], "fuzzy_dominant", clean)
                conn.commit()
            fuzzy_auto += 1
            continue

        # ── Ambiguous — push to LLM worker stream ────────────────────────────
        log.info(
            "queuing for LLM: %r  (approvals=%d, top_score=%.0f, %d candidates)",
            uscis_name, total_approvals, best["score"], len(top),
        )
        for c in top:
            log.info("  candidate: FEIN=%s | %s (score=%.0f)",
                     c["employer_fein"], c["employer_name"], c["score"])

        if not dry_run:
            _push_to_stream(r, conn, uscis_norm, uscis_name, tax_id, clean)
            queued += 1

    log.info(
        "Done: fuzzy_auto=%d  queued_for_llm=%d  skipped(below_threshold)=%d  "
        "no_dol_candidates=%d  already_mapped=%d",
        fuzzy_auto, queued, skipped, no_candidates, already,
    )
    if not dry_run and queued > 0:
        log.info("%d ambiguous entries queued — h1b-llm-worker will resolve them asynchronously", queued)


def _sample_candidates(limit: int, include_queued: bool) -> None:
    """
    Print candidates for unmatched rows — regardless of queued_for_llm status.
    Used to diagnose why the LLM rejected rows or why rows were never sent.
    """
    conn = get_conn()
    try:
        queued_filter = "" if include_queued else "WHERE n.queued_for_llm IS NOT TRUE"
        sql = f"""
            SELECT
                n.employer_name,
                n.employer_legal_norm,
                n.tax_id,
                n.total_approvals,
                n.queued_for_llm
            FROM uscis_dol_unmatched n
            {queued_filter}
            ORDER BY n.total_approvals DESC
            LIMIT {int(limit)}
        """
        rows = conn.execute(sql).fetchall()
        if not rows:
            print("No unmatched rows found.")
            return

        print(f"\n{'='*70}")
        print(f"Sampling {len(rows)} unmatched rows (include_queued={include_queued})")
        print(f"{'='*70}\n")

        for row in rows:
            row = dict(row)
            uscis_norm = row["employer_legal_norm"] or row["employer_name"]
            tax_id     = row["tax_id"]
            queued     = row["queued_for_llm"]

            candidates = _fetch_dol_candidates(conn, tax_id)
            top        = _score_candidates(uscis_norm, candidates) if candidates else []

            status = "LLM-processed (no match)" if queued else "never sent"
            best_score = top[0]["score"] if top else 0
            print(f"[{status}] {row['employer_name']!r}  tax_id={tax_id}  approvals={row['total_approvals']}")
            print(f"  norm: {uscis_norm!r}")

            if not candidates:
                print("  NO DOL FEIN CANDIDATES — tax_id collision gap")
            elif not top:
                dol_scores = sorted(
                    [_fuzz.token_set_ratio(uscis_norm, c["employer_name_norm"]) for c in candidates],
                    reverse=True,
                )
                print(f"  {len(candidates)} DOL records share this tax_id — all score < {_FUZZY_THRESHOLD}")
                print(f"  best raw scores: {dol_scores[:5]}")
            else:
                if best_score >= _FUZZY_AUTO_THRESHOLD:
                    verdict = "→ would auto-match (score ≥ 95)"
                elif best_score >= _FUZZY_DOMINANT_SCORE and len(top) >= 2 and (top[0]["score"] - top[1]["score"]) >= _FUZZY_DOMINANT_GAP:
                    verdict = "→ would dominant-match"
                else:
                    verdict = "→ would queue for LLM"
                print(f"  {verdict}")
                for c in top:
                    print(f"    FEIN={c['employer_fein']}  score={c['score']:.0f}  name={c['employer_name']!r}")
            print()
    finally:
        conn.close()


def main():
    parser = argparse.ArgumentParser(
        description="Fuzzy match unresolved USCIS employers to DOL LCA records"
    )
    parser.add_argument("--limit",   type=int, default=None,
                        help="Max unmatched rows to process")
    parser.add_argument("--dry-run", action="store_true",
                        help="Print candidates without running inference or storing results")
    parser.add_argument("--sample",  type=int, default=None, metavar="N",
                        help="Diagnostic: print candidates for top N unmatched rows and exit")
    parser.add_argument("--sample-all", action="store_true",
                        help="With --sample: include rows already sent to LLM (queued_for_llm=TRUE)")
    args = parser.parse_args()

    if args.sample:
        _sample_candidates(args.sample, include_queued=args.sample_all)
        return

    run(limit=args.limit, dry_run=args.dry_run)


if __name__ == "__main__":
    init_logging("fuzzy_match_uscis_dol")
    main()
