"""
scripts/early_exit_miss_rate.py
-------------------------------
Read-only report: how often did the fullscan (safety net) insert a job that adaptive scans should
have found first?  Uses job_postings.found_by ('tier1_adaptive' / 'tier2_fullscan'); the first source to
insert a job keeps the credit (ON CONFLICT DO NOTHING).

A raw tier2 share overstates misses, so three things are separated:
  * cold start  - rows inserted within --cold-hours of a company's FIRST row (its first fullscan
                  inserts the whole board) are excluded.
  * late        - tier2 rows whose posting date (posted_at) is NOT older than --lag-hours before the
                  insert: the job was probably posted after the last adaptive scan, so adaptive could
                  not have found it. Only meaningful where posted_at is a real listing date.
  * likely_miss - tier2 rows posted more than --lag-hours before the insert (adaptive had at least one
                  scan window to see it) PLUS tier2 rows with no posted_at (unknown, shown apart).

No per-scan history is stored (company_ats.last_checked_at is overwritten), so this is an indicator,
not an exact count or a bound: dated jobs the fullscan inserted within --lag-hours of their posting
are excluded (counted as "late"), whatever the cause, and not every counted row is an early-exit miss
(long poll interval, failed scan or filter error also qualify).  Compare windows before and after the
early-exit deploy.

Usage:
    python scripts/early_exit_miss_rate.py [--days 14] [--lag-hours 36] [--cold-hours 24] [--top 15]
"""

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from db.connection import get_conn
from logger import get_logger, init_logging

log = get_logger(__name__)

_SQL = """
WITH first_row AS (
    SELECT company, MIN(created_at) AS t0 FROM job_postings GROUP BY company
),
rows AS (
    SELECT p.company, p.ats_platform, p.found_by, p.posted_at, p.created_at
    FROM job_postings p
    JOIN first_row f ON f.company = p.company
    WHERE p.created_at >= NOW() - (%(days)s || ' days')::interval
      AND p.created_at >  f.t0 + (%(cold)s || ' hours')::interval
      AND p.found_by IN ('tier1_adaptive', 'tier2_fullscan')
      AND p.status <> 'pre_existing'
)
SELECT {key} AS k,
       COUNT(*)                                                         AS total,
       COUNT(*) FILTER (WHERE found_by = 'tier1_adaptive')              AS adaptive,
       COUNT(*) FILTER (WHERE found_by = 'tier2_fullscan')              AS fullscan,
       COUNT(*) FILTER (WHERE found_by = 'tier2_fullscan' AND posted_at IS NOT NULL
                          AND posted_at <  created_at - (%(lag)s || ' hours')::interval) AS likely_miss,
       COUNT(*) FILTER (WHERE found_by = 'tier2_fullscan' AND posted_at IS NOT NULL
                          AND posted_at >= created_at - (%(lag)s || ' hours')::interval) AS late,
       COUNT(*) FILTER (WHERE found_by = 'tier2_fullscan' AND posted_at IS NULL)         AS undated
FROM rows
GROUP BY 1
ORDER BY fullscan DESC, total DESC
"""


def _report(conn, key, label, args):
    cur = conn.cursor()
    cur.execute(_SQL.format(key=key), {"days": args.days, "lag": args.lag_hours, "cold": args.cold_hours})
    rows = cur.fetchall()
    print(f"\n== by {label} (last {args.days}d, cold start {args.cold_hours}h excluded) ==")
    print(f"{label:<28}{'total':>7}{'adapt':>7}{'full':>7}{'full%':>7}{'miss?':>7}{'late':>7}{'undated':>8}")
    for r in rows[: args.top if label == "company" else None]:
        v = list(r.values()) if hasattr(r, "values") else list(r)
        k, total, adaptive, full, miss, late, undated = v
        pct = 100.0 * full / total if total else 0.0
        print(f"{str(k)[:27]:<28}{total:>7}{adaptive:>7}{full:>7}{pct:>6.0f}%{miss:>7}{late:>7}{undated:>8}")
    if not rows:
        print("(no rows)")


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--days", type=int, default=14)
    ap.add_argument("--lag-hours", type=int, default=36)
    ap.add_argument("--cold-hours", type=int, default=24)
    ap.add_argument("--top", type=int, default=15)
    args = ap.parse_args()
    init_logging("early_exit_miss_rate")
    conn = get_conn()
    try:
        _report(conn, "ats_platform", "platform", args)
        _report(conn, "company", "company", args)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
