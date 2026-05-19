"""
workers/rebuild.py — Rebuild Redis state from PostgreSQL on startup (Phase 3).

Redis is NOT the source of truth. On any restart (planned or crash), all
Redis structures are rebuilt from PostgreSQL in ~30 seconds.

Rebuild order (from doc Section 18):
    1. poll:adaptive       ← company_poll_stats.next_poll_at
    2. poll:fullscan       ← company_poll_stats.next_full_scan_at
    3. queue:detail:*      ← job_postings WHERE status='pending_detail'
    4. stats:{company}     ← company_poll_stats

NOT rebuilt on startup (by design):
    adaptive_seen:{company} — 24h TTL; next adaptive scan repopulates naturally
    bloom:fullscan:{company} — 36h TTL; next full scan runs as cold start (correct)

Usage:
    from workers.rebuild import rebuild_redis
    rebuild_redis()    # call once at scheduler startup
"""

import json
import logging
import math
import time

from workers.redis_client import get_redis
from db.db import get_conn, init_db
from config import (
    REDIS_POLL_ADAPTIVE,
    REDIS_POLL_FULLSCAN,
    REDIS_DETAIL_ADAPTIVE,
    WORKER_FLOOR,
    STARTUP_AVG_SCAN_TIME_S,
)

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────
# 1. POLL QUEUES
# ─────────────────────────────────────────

def _spread_window_s(n: int, n_workers: int, avg_scan_s: float) -> float:
    """
    Calculate a spread window that exactly matches system throughput.

    With N companies and W scan workers each taking avg_scan_s per scan,
    it takes ceil(N / W) * avg_scan_s seconds to process all of them once.
    Spreading companies evenly across that window means one company becomes
    due roughly every time a worker finishes its current scan — no thundering
    herd, no idle workers.

    Examples (2 workers, 30s avg):
        10 companies  →  5 batches × 30s = 150s  (2.5 min)
        50 companies  → 25 batches × 30s = 750s  (12.5 min)
       150 companies  → 75 batches × 30s = 2250s (37.5 min)
    """
    if n <= 0 or n_workers <= 0:
        return 0.0
    return math.ceil(n / n_workers) * avg_scan_s


def rebuild_poll_queues() -> dict:
    """
    Rebuild poll:adaptive and poll:fullscan ZSETs from company_poll_stats.

    Three-way categorisation replaces the old "all NULLs → now-1" logic:

    NEW companies (last_poll_at IS NULL AND last_full_scan_at IS NULL)
        → poll:fullscan only, spread across a dynamic startup window.
          Full scan runs first; on_fullscan_complete() bootstraps them
          into poll:adaptive afterwards.

    OVERDUE companies (have polling history, next_poll_at in the past or NULL)
        → poll:adaptive, spread across a dynamic recovery window so workers
          are not hammered all at once after a restart.
          Existing next_full_scan_at preserved (or rescheduled +5 min if also
          overdue).

    FUTURE companies (next_poll_at in the future)
        → poll:adaptive with their stored timestamp; full scan preserved.

    Spread window formula: ceil(N / W) × avg_scan_s
        N = companies in each bucket
        W = WORKER_FLOOR (minimum scan workers, conservative)
        avg_scan_s = STARTUP_AVG_SCAN_TIME_S (config, default 30s)

    Returns dict with counts per bucket.
    """
    r   = get_redis()
    now = time.time()

    adaptive_entries: dict = {}
    fullscan_entries: dict = {}

    conn = get_conn()
    try:
        rows = conn.execute("""
            SELECT company, next_poll_at, next_full_scan_at,
                   last_poll_at, last_full_scan_at
            FROM company_poll_stats
        """).fetchall()
    finally:
        conn.close()

    # ── Categorise ────────────────────────────────────────────────────────────
    new_companies:     list = []   # never polled AND never full-scanned
    overdue_companies: list = []   # have history but next_poll_at is past/NULL
    future_companies:  list = []   # next_poll_at is in the future

    for row in rows:
        never_polled    = row["last_poll_at"]     is None
        never_fullscan  = row["last_full_scan_at"] is None
        if never_polled and never_fullscan:
            new_companies.append(row)
        elif row["next_poll_at"] is None or row["next_poll_at"].timestamp() <= now:
            overdue_companies.append(row)
        else:
            future_companies.append(row)

    n_workers = WORKER_FLOOR          # conservative: minimum scan workers
    avg_s     = STARTUP_AVG_SCAN_TIME_S

    # ── 1. NEW companies → poll:fullscan first ────────────────────────────────
    # Do NOT add to poll:adaptive — on_fullscan_complete() will bootstrap them
    # into poll:adaptive once their first full scan finishes.
    new_spread = _spread_window_s(len(new_companies), n_workers, avg_s)
    n_new      = max(len(new_companies), 1)
    for i, row in enumerate(new_companies):
        score = now + (i / n_new) * new_spread
        fullscan_entries[row["company"]] = score

    if new_companies:
        logger.info(
            "rebuild: %d new companies → poll:fullscan only "
            "(spread=%.0fs / %.1f min)",
            len(new_companies), new_spread, new_spread / 60,
        )

    # ── 2. OVERDUE companies → poll:adaptive with recovery spread ─────────────
    # Sort most-overdue first (lowest next_poll_at timestamp) so companies
    # that missed the most polls are processed earliest in the window.
    overdue_companies.sort(key=lambda r: (
        r["next_poll_at"].timestamp() if r["next_poll_at"] else 0.0
    ))
    overdue_spread = _spread_window_s(len(overdue_companies), n_workers, avg_s)
    n_over         = max(len(overdue_companies), 1)
    for i, row in enumerate(overdue_companies):
        score = now + (i / n_over) * overdue_spread
        adaptive_entries[row["company"]] = score

        # Preserve their next full scan if it is still in the future;
        # if the full scan is also overdue, bump it 5 min from now so it
        # doesn't collide with the adaptive startup rush.
        if row["next_full_scan_at"] is not None:
            fs_ts = row["next_full_scan_at"].timestamp()
            fullscan_entries[row["company"]] = fs_ts if fs_ts > now else now + 300

    if overdue_companies:
        logger.info(
            "rebuild: %d overdue companies → poll:adaptive "
            "(spread=%.0fs / %.1f min)",
            len(overdue_companies), overdue_spread, overdue_spread / 60,
        )

    # ── 3. FUTURE companies → use stored timestamps as-is ────────────────────
    for row in future_companies:
        adaptive_entries[row["company"]] = row["next_poll_at"].timestamp()
        if row["next_full_scan_at"] is not None:
            fullscan_entries[row["company"]] = row["next_full_scan_at"].timestamp()

    # ── Write to Redis ─────────────────────────────────────────────────────────
    if adaptive_entries:
        r.zadd(REDIS_POLL_ADAPTIVE, adaptive_entries)
    if fullscan_entries:
        r.zadd(REDIS_POLL_FULLSCAN, fullscan_entries)

    logger.info(
        "rebuild: poll:adaptive=%d poll:fullscan=%d "
        "(new=%d overdue=%d future=%d)",
        len(adaptive_entries), len(fullscan_entries),
        len(new_companies), len(overdue_companies), len(future_companies),
    )
    return {
        "adaptive": len(adaptive_entries),
        "fullscan": len(fullscan_entries),
        "new":      len(new_companies),
        "overdue":  len(overdue_companies),
        "future":   len(future_companies),
    }


# ─────────────────────────────────────────
# 2. DETAIL QUEUE
# ─────────────────────────────────────────

def rebuild_detail_queue() -> int:
    """
    Rebuild queue:detail:adaptive from job_postings WHERE status='pending_detail'.

    These are jobs whose ID was found by a listing scan but whose detail
    page was not yet fetched before the last restart.

    Returns count of jobs re-queued.
    """
    r = get_redis()

    conn = get_conn()
    try:
        rows = conn.execute("""
            SELECT company, job_url, job_id, ats_platform
            FROM job_postings
            WHERE status = 'pending_detail'
            ORDER BY first_seen ASC
        """).fetchall()
    finally:
        conn.close()

    count = 0
    for row in rows:
        payload = json.dumps({
            "company":      row["company"],
            "job_url":      row["job_url"],
            "job_id":       row["job_id"],
            "ats_platform": row["ats_platform"],
            "source":       "rebuild",
        })
        r.lpush(REDIS_DETAIL_ADAPTIVE, payload)
        count += 1

    if count:
        logger.info("rebuild: re-queued %d pending_detail jobs", count)

    return count


# ─────────────────────────────────────────
# 3. STATS CACHE
# ─────────────────────────────────────────

def rebuild_stats_cache() -> int:
    """
    Rebuild stats:{company} Redis Hash from company_poll_stats.

    Used by scheduler for fast interval computation (~0.1ms vs ~5ms DB).

    Returns count of companies loaded.
    """
    r    = get_redis()
    conn = get_conn()

    try:
        rows = conn.execute("""
            SELECT company, adaptive_score, current_interval_s,
                   recent_poll_counts, consecutive_empty, last_poll_at
            FROM company_poll_stats
        """).fetchall()
    finally:
        conn.close()

    pipe = r.pipeline()
    for row in rows:
        key = f"stats:{row['company']}"
        pipe.hset(key, mapping={
            "adaptive_score":     str(row["adaptive_score"] or 0.0),
            "current_interval_s": str(row["current_interval_s"] or 86400),
            "recent_poll_counts": row["recent_poll_counts"] or "[]",
            "consecutive_empty":  str(row["consecutive_empty"] or 0),
            "last_poll_at":       row["last_poll_at"].isoformat() if row["last_poll_at"] else "",
        })
    pipe.execute()

    logger.info("rebuild: stats cache loaded %d companies", len(rows))
    return len(rows)


# ─────────────────────────────────────────
# FULL REBUILD
# ─────────────────────────────────────────

def rebuild_redis() -> dict:
    """
    Full Redis rebuild from PostgreSQL. Call once at scheduler startup.

    adaptive_seen:{company} and bloom:fullscan:{company} are NOT rebuilt —
    they repopulate naturally on the next adaptive/full scan cycle.

    Returns:
        dict with rebuild counts for each structure.
    """
    init_db()
    start = time.time()
    logger.info("rebuild: starting full Redis rebuild from PostgreSQL")

    result = {}

    result.update(rebuild_poll_queues())
    result["pending_detail"] = rebuild_detail_queue()
    result["stats"]          = rebuild_stats_cache()

    elapsed = time.time() - start
    logger.info(
        "rebuild: complete in %.1fs | adaptive=%d fullscan=%d "
        "pending_detail=%d stats=%d",
        elapsed,
        result.get("adaptive", 0),
        result.get("fullscan", 0),
        result.get("pending_detail", 0),
        result.get("stats", 0),
    )
    return result
