"""
workers/watchdog.py — Heartbeat watchdog and orphan detection (Phase 10).

Runs every WATCHDOG_INTERVAL_S seconds and checks for companies that have
been popped from the poll queue (their heartbeat was set) but whose worker
appears to have crashed before the heartbeat could be cleared.

─── Orphan detection ─────────────────────────────────────────────────────────

A company is "orphaned" when ALL of these are true:
    1. heartbeat:{company} key has EXPIRED (TTL elapsed → worker not refreshing)
    2. Company is NOT currently in poll:adaptive ZSET  (was popped and never rescheduled)
    3. Company is NOT currently in poll:fullscan ZSET  (ditto for full scan)

Orphaned companies are re-queued with score=now (immediately due) so they
are picked up by the next adaptive dispatch tick.

─── Hung worker detection ────────────────────────────────────────────────────

A worker is "hung" when:
    1. heartbeat:{company} is still alive (worker process is running)
    2. progress:{company} has EXPIRED (no step update for 120 seconds)

This distinguishes a frozen worker (stuck between HTTP requests with no
progress update) from a legitimately slow worker making a large fetch
(which keeps updating progress:{company} with each page).

The watchdog logs hung workers but does NOT kill them — that requires an
OS-level signal which is outside the watchdog's scope. An alert is raised
for operator action.

─── Usage ────────────────────────────────────────────────────────────────────

    python -m workers.watchdog          # run forever (every 60 seconds)
    python -m workers.watchdog --once   # one check cycle then exit

─── Architecture doc reference ──────────────────────────────────────────────

    Section 18 — Resilience: Worker failures
    Section 15 — Redis: Worker Heartbeats, Worker Progress
"""

import sys
import time
import logging

from workers.redis_client import get_redis, ping
from config import (
    REDIS_POLL_ADAPTIVE,
    REDIS_POLL_FULLSCAN,
    CONCURRENCY_ERROR_RATE_REDUCE,
)

logger = logging.getLogger(__name__)

# How often the watchdog checks for orphaned companies (seconds)
WATCHDOG_INTERVAL_S = 60


# ─────────────────────────────────────────
# BACKOFF HELPER
# ─────────────────────────────────────────

def _get_orphan_requeue_score(r, company: str, now: float) -> float:
    """
    Return the ZADD score (Unix timestamp) for re-queuing an orphaned company.

    If the company's platform is currently above the error-rate reduction
    threshold, the platform is still struggling — re-dispatching immediately
    would repeat the failure.  Use exponential backoff (same counter as
    scan_worker so retries accumulate correctly).

    If the platform is healthy, re-queue immediately (score = now).

    Platform is looked up from prospective_companies; falls back to immediate
    re-queue on any DB error so orphan recovery is never blocked.
    """
    from workers.http_client import get_error_rate
    from workers.scan_worker import _get_backoff_delay

    try:
        from db.db import get_conn
        conn = get_conn()
        try:
            row = conn.execute(
                "SELECT ats_platform FROM prospective_companies WHERE company = %s",
                (company,),
            ).fetchone()
        finally:
            conn.close()
        platform = row["ats_platform"] if row else None
    except Exception as exc:
        logger.debug("watchdog: platform lookup failed for %r: %s", company, exc)
        platform = None

    if platform:
        try:
            error_rate = get_error_rate(r, platform)
            if error_rate > CONCURRENCY_ERROR_RATE_REDUCE:
                delay = _get_backoff_delay(r, company, "scan")
                return now + delay
        except Exception as exc:
            logger.debug("watchdog: error_rate check failed for %r: %s", company, exc)

    return now   # healthy platform — re-queue immediately


# ─────────────────────────────────────────
# ORPHAN DETECTION
# ─────────────────────────────────────────

def check_orphans() -> int:
    """
    Find orphaned companies and re-queue them with score=now.

    A company is orphaned when heartbeat:{company} has expired AND the
    company is absent from both poll:adaptive and poll:fullscan.

    Orphan detection relies on a Redis SCAN of heartbeat:* keys.
    Keys that NO LONGER EXIST means the heartbeat expired — we check if
    the company was ever in-flight by consulting a temporary tracking
    structure.

    Implementation: we scan for progress:{company} keys (set by workers
    alongside heartbeat:{company}). If progress: has expired but the company
    is not in either queue, it was being processed and the worker died.

    Returns count of companies re-queued.
    """
    r         = get_redis()
    now       = time.time()
    requeued  = 0

    # Scan for heartbeat keys still alive
    alive_heartbeats = set()
    cursor = 0
    while True:
        cursor, keys = r.scan(cursor, match="heartbeat:*", count=100)
        for key in keys:
            key_str = key.decode() if isinstance(key, (bytes, bytearray)) else key
            company = key_str.split(":", 1)[1]
            alive_heartbeats.add(company)
        if cursor == 0:
            break

    # Companies with an alive heartbeat are still being processed — skip
    # Companies whose heartbeat has expired may be orphaned
    # We detect them by finding companies NOT in either queue and NOT heartbeat-alive

    # Get all companies currently in either poll queue (not orphaned).
    # r.zrange() returns bytes in redis-py; decode to str for consistent
    # comparisons with the alive_heartbeats set (also populated with str).
    def _decode(val):
        return val.decode() if isinstance(val, (bytes, bytearray)) else val

    in_adaptive = {_decode(m) for m in r.zrange(REDIS_POLL_ADAPTIVE, 0, -1)}
    in_fullscan = {_decode(m) for m in r.zrange(REDIS_POLL_FULLSCAN, 0, -1)}
    in_queues   = in_adaptive | in_fullscan

    # Scan for "was recently dispatched" markers — we use progress:{company}
    # which is set by scan_worker when a job starts (TTL=120s).
    # A recently-expired progress: key means the company WAS in-flight.
    # Since we can't scan expired keys, use a secondary "in-flight" tracking set.
    # See _track_inflight() / _clear_inflight() called by scan_worker.
    inflight_key = "watchdog:inflight"
    # r.smembers() also returns bytes; decode for consistent membership tests.
    in_flight    = {_decode(m) for m in r.smembers(inflight_key)}

    for company in in_flight:
        if company in alive_heartbeats:
            # Worker is still running (heartbeat alive) — not an orphan
            continue
        if company in in_queues:
            # Company already back in a queue — scheduler rescheduled it
            r.srem(inflight_key, company)
            continue

        # heartbeat expired AND not in queues AND was in-flight → orphaned
        # Re-queue with error-rate-aware delay: if the platform is still
        # struggling, use exponential backoff so we don't hammer it again.
        # If healthy, re-queue immediately (score=now).
        requeue_score = _get_orphan_requeue_score(r, company, now)
        logger.warning(
            "watchdog: ORPHAN detected: company=%r — re-queuing "
            "(delay=%ds, score=%.0f)",
            company, max(0, int(requeue_score - now)), requeue_score,
        )
        r.zadd(REDIS_POLL_ADAPTIVE, {company: requeue_score})
        r.srem(inflight_key, company)
        requeued += 1

    if requeued:
        logger.info("watchdog: re-queued %d orphaned companies", requeued)
    else:
        logger.debug("watchdog: no orphans detected")

    return requeued


def track_inflight(company: str) -> None:
    """
    Add a company to the watchdog in-flight tracking set.

    Call this from scan_worker when a job starts processing.
    The set persists across worker crashes — that is the point.
    """
    get_redis().sadd("watchdog:inflight", company)


def clear_inflight(company: str) -> None:
    """
    Remove a company from the watchdog in-flight tracking set.

    Call this from scan_worker on successful completion (or after
    scheduler has rescheduled the company). Also called by watchdog
    itself after re-queuing.
    """
    get_redis().srem("watchdog:inflight", company)


# ─────────────────────────────────────────
# HUNG WORKER DETECTION
# ─────────────────────────────────────────

def check_hung_workers() -> list:
    """
    Detect workers that have a live heartbeat but no recent progress update.

    A worker is "hung" if:
        - heartbeat:{company} EXISTS (worker process alive)
        - progress:{company} does NOT EXIST (no step update in 120s)

    Returns list of company names with potentially hung workers.
    """
    r    = get_redis()
    hung = []

    # Scan all alive heartbeats
    cursor = 0
    while True:
        cursor, keys = r.scan(cursor, match="heartbeat:*", count=100)
        for key in keys:
            key_str      = key.decode() if isinstance(key, (bytes, bytearray)) else key
            company      = key_str.split(":", 1)[1]
            progress_key = f"progress:{company}"
            if not r.exists(progress_key):
                logger.warning(
                    "watchdog: HUNG WORKER suspected: company=%r — "
                    "heartbeat alive but progress key expired "
                    "(no step update in 120s). "
                    "Consider killing worker PID and letting orphan "
                    "detection re-queue.",
                    company,
                )
                hung.append(company)
        if cursor == 0:
            break

    return hung


# ─────────────────────────────────────────
# MAIN LOOP
# ─────────────────────────────────────────

def run_watchdog(once: bool = False) -> None:
    """
    Main watchdog loop.

    Runs check_orphans() and check_hung_workers() every WATCHDOG_INTERVAL_S
    seconds.

    Args:
        once: if True, run one check cycle then exit.
    """
    if not ping():
        import os
        redis_url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
        logger.error("watchdog: Redis not reachable at %s — aborting", redis_url)
        print(f"[watchdog] ERROR: Redis unreachable ({redis_url})")
        sys.exit(1)

    logger.info("watchdog started (interval=%ds)", WATCHDOG_INTERVAL_S)
    print(f"[watchdog] Running — check every {WATCHDOG_INTERVAL_S}s")
    if once:
        print("[watchdog] --once mode: one check cycle then exit")
    else:
        print("[watchdog] Press Ctrl+C to stop")

    while True:
        try:
            requeued = check_orphans()
            hung     = check_hung_workers()

            if requeued or hung:
                print(f"[watchdog] orphans={requeued} hung={len(hung)}")

            if once:
                break

            time.sleep(WATCHDOG_INTERVAL_S)

        except KeyboardInterrupt:
            logger.info("watchdog: KeyboardInterrupt — stopping")
            print("\n[watchdog] Stopping.")
            break
        except Exception as exc:
            logger.error("watchdog: unexpected error: %s", exc, exc_info=True)
            if once:
                break
            time.sleep(WATCHDOG_INTERVAL_S)

    logger.info("watchdog shutdown")


# ─────────────────────────────────────────
# CLI
# ─────────────────────────────────────────

if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s [%(levelname)s] %(message)s")
    once = "--once" in sys.argv
    run_watchdog(once=once)
