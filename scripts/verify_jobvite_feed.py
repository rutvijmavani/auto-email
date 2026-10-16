"""On-demand Jobvite completeness check: fetch the board, then cross-check the account XML feed.

Usage:  python scripts/verify_jobvite_feed.py <slug> [<slug> ...]
Read-only; prints one verdict per slug (verified / unverified / incomplete).
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from jobs.ats import jobvite
from logger import get_logger, init_logging

logger = get_logger(__name__)


def main(argv):
    init_logging("verify_jobvite_feed")
    if not argv:
        print(__doc__)
        return 2
    for slug in argv:
        jobs, report = jobvite.fetch_board(slug, slug)
        verdict = jobvite.verify_fetch(slug, jobs, report["stop_reason"])
        print(f"{slug}: {verdict['status']} — {verdict['reason']} "
              f"(jobs={len(jobs)}, pages={report['pages']}, stop={report['stop_reason']}, "
              f"feed={verdict.get('feed_total')}, foreign={verdict.get('foreign')}, "
              f"unresolved={verdict.get('unresolved')}, missing={verdict.get('missing', [])[:10]})")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
