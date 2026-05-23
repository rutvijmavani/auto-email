"""
workers/watchdog.py — Hung-worker detection and stream PEL observability.

─── What changed in the two-layer scheduler redesign ────────────────────────

The old watchdog had two responsibilities:

  1. Orphan detection: scan the "watchdog:inflight" SSET for companies whose
     heartbeat expired and who never made it back into a poll queue.  Workers
     populated watchdog:inflight via track_inflight() / clear_inflight().

  2. Hung-worker detection: scan heartbeat:{company} and progress:{company}
     keys to find workers that are alive but making no progress.

Responsibility (1) is now owned by the scheduler's claim_stale_work()
(XAUTOCLAIM).  When a scan-worker dies mid-job the message stays in the
stream PEL.  The scheduler's adaptive_loop() calls claim_stale_work() on
every tick, which uses XAUTOCLAIM to reclaim messages idle longer than
p95×3 ms.  Re-queuing orphaned companies is therefore automatic without
any secondary tracking set.  check_orphans(), track_inflight(),
clear_inflight(), and _get_orphan_requeue_score() have been removed.

Responsibility (2) is still owned by this watchdog.  Scan workers still
call set_heartbeat() / set_progress() so the hung-worker check is intact.

─── Hung worker detection ────────────────────────────────────────────────────

A worker is "hung" when:
    1. heartbeat:{company} EXISTS (worker process is alive)
    2. progress:{company} does NOT EXIST (no step update for 120 seconds)

This distinguishes a frozen worker (stuck between HTTP requests) from a
legitimately slow worker making a large fetch (which keeps updating
progress:{company} with each page).

The watchdog logs hung workers but does NOT kill them — that requires an
OS-level signal.  An alert is raised for operator action.

─── Stream PEL observability ─────────────────────────────────────────────────

check_pel_stats() calls XPENDING on stream:adaptive and stream:fullscan and
logs summary statistics.  This is purely informational — actual reclaim is
performed by claim_stale_work() in scheduler.py.  Use this for dashboards or
alerting on stuck PEL growth.

─── Usage ────────────────────────────────────────────────────────────────────

    python -m workers.watchdog          # run forever (every 60 seconds)
    python -m workers.watchdog --once   # one check cycle then exit

─── Architecture doc reference ──────────────────────────────────────────────

    Section 5  — Two-layer scheduler redesign (Redis Streams + PEL)
    Section 18 — Resilience: Worker failures
    Section 15 — Redis: Worker Heartbeats, Worker Progress
"""

import sys
import time
import logging

from workers.redis_client import get_redis, ping
from config import (
    REDIS_STREAM_ADAPTIVE,
    REDIS_STREAM_FULLSCAN,
    STREAM_CONSUMER_GROUP,
)

logger = logging.getLogger(__name__)

# How often the watchdog checks (seconds)
WATCHDOG_INTERVAL_S = 60

# PEL age threshold for logging a warning (informational only — does not
# trigger recovery; that is claim_stale_work()'s job).
PEL_WARN_AGE_MS = 10 * 60 * 1000   # 10 minutes


# ─────────────────────────────────────────
# HUNG WORKER DETECTION
# ─────────────────────────────────────────

def check_hung_workers() -> list:
    """
    Detect workers that have a live heartbeat but no recent progress update.

    A worker is "hung" if:
        - heartbeat:{company} EXISTS   (worker process is alive)
        - progress:{company} does NOT  (no step update in 120s)

    This supplementary check catches edge cases that fall outside the
    PEL reclaim window — e.g. a worker that is still alive (no crash)
    but has blocked indefinitely inside fetch_jobs().

    Returns list of company names with potentially hung workers.
    Does NOT kill or re-queue — logs a warning for operator action.
    """
    r    = get_redis()
    hung = []

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
                    "(no step update in 120 s).  "
                    "claim_stale_work() will reclaim the PEL entry after "
                    "the p95×3 idle window if the worker is truly stuck.",
                    company,
                )
                hung.append(company)
        if cursor == 0:
            break

    return hung


# ─────────────────────────────────────────
# STREAM PEL OBSERVABILITY
# ─────────────────────────────────────────

def check_pel_stats() -> dict:
    """
    Report Pending Entry List (PEL) summary for adaptive and fullscan streams.

    Calls XPENDING stream group - + for up to 50 oldest entries and reports:
        - total_pending:  overall PEL depth (from the summary XPENDING)
        - oldest_age_ms:  how long the oldest pending message has been idle
        - consumers:      list of (consumer_name, count) tuples

    Logs a WARNING if any entry has been pending longer than PEL_WARN_AGE_MS.
    This is purely informational — actual reclaim is performed by
    claim_stale_work() (XAUTOCLAIM) inside the scheduler's adaptive_loop().

    Returns:
        dict keyed by stream name, each value a sub-dict with the stats above.
        Empty dict if Redis is unreachable.
    """
    r   = get_redis()
    now = int(time.time() * 1000)   # milliseconds
    stats = {}

    for stream_key in (REDIS_STREAM_ADAPTIVE, REDIS_STREAM_FULLSCAN):
        try:
            # Summary form: XPENDING stream group — returns
            # [total, min_id, max_id, [[consumer, count], ...]]
            summary = r.xpending(stream_key, STREAM_CONSUMER_GROUP)
        except Exception as exc:
            logger.debug("watchdog: xpending %s failed: %s", stream_key, exc)
            continue

        if not summary or summary.get("pending") == 0:
            stats[stream_key] = {"total_pending": 0}
            continue

        total     = summary.get("pending", 0)
        consumers = [
            (c["name"] if isinstance(c["name"], str) else c["name"].decode(), c["pending"])
            for c in summary.get("consumers", [])
        ]

        # Fetch lowest-ID pending entry to report its idle time.
        # Note: xpending_range min="-" returns the message with the lowest
        # stream ID (earliest enqueue time), not necessarily the longest-idle.
        # claim_stale_work() handles actual reclaim via XAUTOCLAIM.
        lowest_id_age_ms = None
        try:
            entries = r.xpending_range(
                stream_key, STREAM_CONSUMER_GROUP,
                min="-", max="+", count=1,
            )
            if entries:
                entry     = entries[0]
                msg_id    = entry["message_id"]
                idle_ms   = entry.get("time_since_delivered", 0)
                lowest_id_age_ms = idle_ms

                if idle_ms > PEL_WARN_AGE_MS:
                    logger.warning(
                        "watchdog: PEL WARNING stream=%s — lowest-ID message %r "
                        "has been pending for %d s (threshold %d s). "
                        "claim_stale_work() should have reclaimed it. "
                        "Check scheduler adaptive_loop()/fullscan_loop() health.",
                        stream_key,
                        msg_id.decode() if isinstance(msg_id, bytes) else msg_id,
                        idle_ms // 1000,
                        PEL_WARN_AGE_MS // 1000,
                    )
        except Exception as exc:
            logger.debug(
                "watchdog: xpending_range %s failed: %s", stream_key, exc,
            )

        entry_stats = {
            "total_pending":  total,
            "oldest_age_ms":  lowest_id_age_ms,
            "consumers":      consumers,
        }
        stats[stream_key] = entry_stats

        logger.info(
            "watchdog: PEL stats stream=%s total=%d oldest=%s consumers=%s",
            stream_key,
            total,
            f"{oldest_age_ms // 1000}s" if oldest_age_ms is not None else "n/a",
            consumers,
        )

    return stats


# ─────────────────────────────────────────
# MAIN LOOP
# ─────────────────────────────────────────

def run_watchdog(once: bool = False) -> None:
    """
    Main watchdog loop.

    Runs check_hung_workers() and check_pel_stats() every WATCHDOG_INTERVAL_S
    seconds.

    Orphan detection (check_orphans) was removed in the two-layer scheduler
    redesign — PEL + claim_stale_work() in scheduler.py owns crash recovery
    for scan workers.

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
            hung      = check_hung_workers()
            pel_stats = check_pel_stats()

            # Build a quick summary line for the console
            total_pel = sum(
                v.get("total_pending", 0)
                for v in pel_stats.values()
                if isinstance(v, dict)
            )
            if hung or total_pel:
                print(
                    f"[watchdog] hung_workers={len(hung)} "
                    f"pel_total={total_pel}"
                )

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
