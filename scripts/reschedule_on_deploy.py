"""
scripts/reschedule_on_deploy.py — Manual escape hatch for score corruption.

╔══════════════════════════════════════════════════════════════════════════╗
║  YOU DO NOT NEED TO RUN THIS ON NORMAL DEPLOYMENTS.                     ║
║                                                                          ║
║  rebuild_redis() runs automatically at scheduler startup and handles     ║
║  all restart scenarios:                                                  ║
║    • Fresh deploy / long outage → STALE path → recovery spread           ║
║    • Brief restart / normal deploy → CURRENT path → DB timestamps        ║
║                                                                          ║
║  Run this script ONLY in exceptional cases where Redis ZSET scores       ║
║  have become corrupted or severely clustered while the scheduler is      ║
║  already running and you cannot tolerate a full restart.                 ║
╚══════════════════════════════════════════════════════════════════════════╝

When to use
───────────
  • A bug or manual edit has left scores tightly clustered in Redis while
    workers are live (restarting would invoke rebuild_redis() — prefer that).
  • You need to surgically reset one queue only (--adaptive-only or
    --fullscan-only) without touching the other.

Algorithm
─────────
  poll:adaptive  → each company's next poll = now + slot_offset(company).
  poll:fullscan  → each company's next scan  = now
                   + (slot_offset(company) / 86400) × full_scan_interval.

Both queues use the same deterministic MD5-based slot_offset() so every
run of this script produces the same relative ordering — companies never
shuffle positions between restarts.  No midnight anchoring, no timezone
math, no daily wave.

Usage
─────
  # dry run (print what would change, touch nothing)
  python scripts/reschedule_on_deploy.py --dry-run

  # live run
  python scripts/reschedule_on_deploy.py

Safety
──────
  • Idempotent — safe to run multiple times; companies always get the
    same relative ordering (scores shift with wall-clock but ordering holds).
  • Does NOT clear the ZSETs; only updates existing member scores.
  • Does NOT touch company_poll_stats in the DB — the DB is the
    historical record; Redis is the scheduling surface.
  • Exits non-zero on any Redis connection failure so CI/CD can gate on it.
"""

import sys
import os
import time
import argparse

# Allow running from the project root without installing the package.
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from config import (
    REDIS_POLL_ADAPTIVE,
    REDIS_POLL_FULLSCAN,
    SCHEDULER_FULL_SCAN_INTERVAL_S,
)
from workers.slot import slot_offset
from workers.redis_client import get_redis
from logger import get_logger

logger = get_logger(__name__)


def reschedule_adaptive(r, now_ts: float, dry_run: bool) -> int:
    """
    Re-spread poll:adaptive members across the next 24-h window from now.

    Each company's new score = now + slot_offset(company), which is always
    in the future and spreads companies deterministically over the next 24 h.

    Returns the number of members updated.
    """
    members = r.zrange(REDIS_POLL_ADAPTIVE, 0, -1, withscores=True)

    if not members:
        logger.info("reschedule: poll:adaptive is empty — nothing to do")
        return 0

    updated  = 0
    pipeline = r.pipeline()

    for raw, old_score in members:
        company  = raw.decode() if isinstance(raw, bytes) else raw
        offset_s = slot_offset(company)
        new_score = now_ts + offset_s

        direction = "→" if abs(new_score - old_score) > 1 else "="
        logger.info(
            "adaptive  %-30s  old=%.0f  %s  new=%.0f  (+%.1fh from now)",
            company, old_score, direction, new_score, offset_s / 3600,
        )

        if not dry_run:
            pipeline.zadd(REDIS_POLL_ADAPTIVE, {company: new_score})
        updated += 1

    if not dry_run:
        pipeline.execute()

    return updated


def reschedule_fullscan(r, now_ts: float, dry_run: bool,
                        interval_s: int = SCHEDULER_FULL_SCAN_INTERVAL_S) -> int:
    """
    Re-spread poll:fullscan members across one full-scan window from now.

    Uses slot_offset as a fraction of the window so companies that are
    close together alphabetically don't cluster at the same scan time.

    Returns the number of members updated.
    """
    members = r.zrange(REDIS_POLL_FULLSCAN, 0, -1, withscores=True)

    if not members:
        logger.info("reschedule: poll:fullscan is empty — nothing to do")
        return 0

    updated  = 0
    pipeline = r.pipeline()

    for raw, old_score in members:
        company  = raw.decode() if isinstance(raw, bytes) else raw
        offset_s = slot_offset(company)

        # Map [0, 86400) → [0, interval_s) so the spread covers one full window.
        window_offset = int(offset_s / 86400 * interval_s)
        new_score     = now_ts + window_offset

        direction = "→" if new_score != old_score else "="
        logger.info(
            "fullscan  %-30s  old=%.0f  %s  new=%.0f  (+%ds from now)",
            company, old_score, direction, new_score, window_offset,
        )

        if not dry_run:
            pipeline.zadd(REDIS_POLL_FULLSCAN, {company: new_score})
        updated += 1

    if not dry_run:
        pipeline.execute()

    return updated


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Redistribute scheduler ZSET scores on fresh deployment."
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Print what would change without writing to Redis.",
    )
    parser.add_argument(
        "--adaptive-only", action="store_true",
        help="Only reschedule poll:adaptive (skip poll:fullscan).",
    )
    parser.add_argument(
        "--fullscan-only", action="store_true",
        help="Only reschedule poll:fullscan (skip poll:adaptive).",
    )
    args = parser.parse_args()

    if args.dry_run:
        logger.info("=== DRY RUN — no changes will be written to Redis ===")

    try:
        r = get_redis()
    except Exception as exc:
        logger.error("Cannot connect to Redis: %s", exc)
        return 1

    now_ts   = time.time()
    total    = 0

    if not args.fullscan_only:
        n = reschedule_adaptive(r, now_ts, dry_run=args.dry_run)
        logger.info("adaptive:  %d member(s) %s",
                    n, "would be updated" if args.dry_run else "updated")
        total += n

    if not args.adaptive_only:
        n = reschedule_fullscan(r, now_ts, dry_run=args.dry_run)
        logger.info("fullscan:  %d member(s) %s",
                    n, "would be updated" if args.dry_run else "updated")
        total += n

    logger.info("reschedule complete — %d total member(s) %s",
                total, "would be updated" if args.dry_run else "updated")
    return 0


if __name__ == "__main__":
    sys.exit(main())
