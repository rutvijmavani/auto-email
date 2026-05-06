#!/usr/bin/env python3
"""
scripts/redis_signal.py — Redis pub/sub signals for the nightly cron chain.

Called by run_nightly.sh, run_monday.sh, and run_monthly.sh to coordinate
with adaptive polling workers (scan_worker, detail_worker, fullscan_worker)
during the maintenance window.

Usage:
    python scripts/redis_signal.py pause      # start maintenance window
    python scripts/redis_signal.py heartbeat  # refresh cronchain:alive mid-chain
    python scripts/redis_signal.py resume     # end maintenance window

Pause sequence (called ONCE at chain start):
    PUBLISH pipeline:pause ""
    SET cronchain:alive 1 EX 300
    SET db:maintenance 1          ← no TTL, must be explicitly cleared

Workers react to pipeline:pause by finishing their current page / operation,
committing to DB, writing a checkpoint, and going idle.

Heartbeat sequence (called between EACH step):
    SET cronchain:alive 1 EX 300  ← 5-minute TTL, refreshed repeatedly

Workers auto-resume if cronchain:alive expires AND db:maintenance is absent
(Section 18 — cron chain crash protection). Refreshing between steps keeps
the watchdog happy during long-running backups.

Resume sequence (called ONCE at chain end):
    DEL db:maintenance
    DEL cronchain:alive
    PUBLISH pipeline:resume ""

Workers receive pipeline:resume and restart dispatching immediately. The
DEL of cronchain:alive before resume ensures the auto-resume logic does not
fire a second time (it would be a no-op, but cleaner to avoid).
"""

import sys
import os

# Add project root to path so we can import config / workers
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv
load_dotenv()

import redis as redis_lib
from config import (
    REDIS_URL,
    REDIS_PAUSE_CHANNEL,
    REDIS_RESUME_CHANNEL,
    REDIS_CRONCHAIN_ALIVE,
    REDIS_DB_MAINTENANCE,
)

CRONCHAIN_TTL = 300   # 5 minutes — workers auto-resume if this expires


def _get_redis():
    return redis_lib.from_url(REDIS_URL, decode_responses=True)


def cmd_pause():
    """
    Signal start of maintenance window.

    1. Broadcast pipeline:pause → workers idle after current operation
    2. Set cronchain:alive heartbeat (EX 300)
    3. Set db:maintenance flag (no TTL — must be cleared by cmd_resume)
    """
    r = _get_redis()
    r.publish(REDIS_PAUSE_CHANNEL, "")
    r.set(REDIS_CRONCHAIN_ALIVE, "1", ex=CRONCHAIN_TTL)
    r.set(REDIS_DB_MAINTENANCE, "1")
    print(f"[redis_signal] PAUSE sent | cronchain:alive set (TTL={CRONCHAIN_TTL}s) "
          f"| db:maintenance set")


def cmd_heartbeat():
    """
    Refresh cronchain:alive TTL between cron chain steps.

    Must be called at least once every 5 minutes during the chain to prevent
    workers from auto-resuming mid-maintenance.
    """
    r   = _get_redis()
    ttl = r.expire(REDIS_CRONCHAIN_ALIVE, CRONCHAIN_TTL)
    if not ttl:
        # Key expired or was never set — re-create it
        r.set(REDIS_CRONCHAIN_ALIVE, "1", ex=CRONCHAIN_TTL)
    print(f"[redis_signal] cronchain:alive refreshed (TTL={CRONCHAIN_TTL}s)")


def cmd_resume():
    """
    Signal end of maintenance window.

    1. Delete db:maintenance flag
    2. Delete cronchain:alive (so auto-resume check doesn't re-fire)
    3. Broadcast pipeline:resume → workers restart dispatching
    """
    r = _get_redis()
    r.delete(REDIS_DB_MAINTENANCE)
    r.delete(REDIS_CRONCHAIN_ALIVE)
    r.publish(REDIS_RESUME_CHANNEL, "")
    print("[redis_signal] RESUME sent | db:maintenance cleared | cronchain:alive cleared")


COMMANDS = {
    "pause":     cmd_pause,
    "heartbeat": cmd_heartbeat,
    "resume":    cmd_resume,
}

if __name__ == "__main__":
    if len(sys.argv) < 2 or sys.argv[1] not in COMMANDS:
        print(f"Usage: python scripts/redis_signal.py [pause|heartbeat|resume]")
        sys.exit(1)

    try:
        COMMANDS[sys.argv[1]]()
    except Exception as exc:
        print(f"[redis_signal] ERROR: {exc}")
        # Non-fatal: don't block the cron chain if Redis is unavailable
        # Workers will continue running without pause/resume coordination.
        sys.exit(0)
