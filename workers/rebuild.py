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
from db.db import get_conn, init_db, get_monitorable_companies
from config import (
    REDIS_POLL_ADAPTIVE,
    REDIS_POLL_FULLSCAN,
    REDIS_DETAIL_ADAPTIVE,
    REDIS_CYCLE_START,
    WORKER_FLOOR,
    STARTUP_AVG_SCAN_TIME_S,
    CYCLE_START_HOUR,
)

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────
# CYCLE BOUNDARY HELPER
# ─────────────────────────────────────────

def _current_cycle_start_ts(r) -> float:
    """
    Unix timestamp of the start of the current monitoring cycle.

    The monitoring day runs CYCLE_START_HOUR → CYCLE_START_HOUR (default 7 AM
    → 7 AM), not midnight → midnight.  This boundary separates:

        STALE companies  — next_poll_at is before this timestamp.
                           Their schedule is from a previous monitoring day.
                           They may be clustered → apply recovery spread.

        CURRENT companies — next_poll_at is on or after this timestamp.
                            Their schedule is valid for today's cycle — the
                            DB timestamp is restored directly into the ZSET.
                            If slightly in the past: immediately due.
                            If still in the future: scheduled as stored.

    Resolution order:
        1. cycle:start Redis key — written by record_cycle_start() after each
           --monitor-jobs digest cycle.  Most accurate (reflects actual run
           time, not assumed schedule).
        2. Computed fallback: most recent CYCLE_START_HOUR in local time.
           Used on first startup before any cycle has completed, or if the
           Redis key has expired.
    """
    # Prefer the Redis key set by record_cycle_start()
    try:
        val = r.get(REDIS_CYCLE_START)
        if val:
            return float(val)
    except Exception:
        pass

    # Fallback: compute most recent CYCLE_START_HOUR in local time
    now_s = time.time()
    t     = time.localtime(now_s)
    today_start = time.mktime((
        t.tm_year, t.tm_mon, t.tm_mday,
        CYCLE_START_HOUR, 0, 0,
        t.tm_wday, t.tm_yday, t.tm_isdst,
    ))
    if now_s < today_start:
        # Before CYCLE_START_HOUR today — current cycle started yesterday
        return today_start - 86400
    return today_start


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
    Rebuild poll:adaptive and poll:fullscan ZSETs from company_poll_stats,
    plus any monitorable companies not yet registered in company_poll_stats.

    Four-way categorisation based on CYCLE_START_HOUR (default 7 AM):

    UNREGISTERED companies (in prospective_companies but no row in company_poll_stats)
        → Never been scanned by the new scheduler (e.g. fresh deployment or
          newly added company).  Treated identically to NEW: added to
          poll:fullscan only, spread across a dynamic startup window.
          on_fullscan_complete() writes their first company_poll_stats row
          and bootstraps them into poll:adaptive afterwards.

    NEW companies (row in company_poll_stats; last_poll_at IS NULL AND last_full_scan_at IS NULL)
        → poll:fullscan only, spread across a dynamic startup window.
          Full scan runs first; on_fullscan_complete() bootstraps them
          into poll:adaptive with now + slot_offset afterwards.

    STALE companies (have polling history; next_poll_at before cycle start)
        → next_poll_at is from a previous monitoring day.  These are
          genuinely overdue and may be clustered (e.g. long outage, many
          companies added together).  Spread across a dynamic recovery
          window, most-overdue first, to avoid thundering herd.

    CURRENT companies (have polling history; next_poll_at >= cycle start)
        → The DB is the source of truth.  next_poll_at is restored directly
          into the ZSET — no artificial spread applied.
          • Slightly past (score <= now): immediately due.  Worker picks
            them up in their original priority order.
          • Still future (score > now):  scheduled exactly as stored.
          This path is the normal restart case when work was already evenly
          distributed.  Preserving stored timestamps keeps that distribution.

    Spread window formula: ceil(N / W) × avg_scan_s
        N = companies in each bucket
        W = WORKER_FLOOR (minimum scan workers, conservative)
        avg_scan_s = STARTUP_AVG_SCAN_TIME_S (config, default 30s)

    Returns dict with counts per bucket.
    """
    r   = get_redis()
    now = time.time()

    # Determine cycle boundary: monitoring day starts at CYCLE_START_HOUR.
    # Companies scheduled before this timestamp are from a previous cycle.
    cycle_start = _current_cycle_start_ts(r)

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
    stale_companies:   list = []   # have history; schedule from a previous cycle
    current_companies: list = []   # have history; schedule within current cycle

    for row in rows:
        never_polled   = row["last_poll_at"]     is None
        never_fullscan = row["last_full_scan_at"] is None

        if never_polled and never_fullscan:
            new_companies.append(row)
            continue

        next_ts = row["next_poll_at"].timestamp() if row["next_poll_at"] else None

        if next_ts is None or next_ts < cycle_start:
            # No schedule, or schedule predates the current monitoring day
            stale_companies.append(row)
        else:
            # Scheduled within the current cycle — use DB timestamp directly
            # (whether slightly past or still future)
            current_companies.append(row)

    # ── Unregistered companies ────────────────────────────────────────────────
    # Companies in prospective_companies that have no row in company_poll_stats
    # yet (fresh deployment, or newly added company).  Merge into new_companies
    # so they get the same fullscan-first treatment and spread window.
    known = {row["company"] for row in rows}
    try:
        monitorable = get_monitorable_companies()
        unregistered = [
            {"company": c["company"]}
            for c in monitorable
            if c["company"] not in known
        ]
    except Exception as exc:
        logger.warning("rebuild: could not fetch monitorable companies: %s", exc)
        unregistered = []

    if unregistered:
        new_companies.extend(unregistered)
        logger.info(
            "rebuild: %d unregistered companies found in prospective_companies "
            "(not yet in company_poll_stats) → merged into NEW bucket",
            len(unregistered),
        )

    n_workers = WORKER_FLOOR
    avg_s     = STARTUP_AVG_SCAN_TIME_S

    # ── 1. NEW companies → poll:fullscan first ────────────────────────────────
    # Do NOT add to poll:adaptive — on_fullscan_complete() bootstraps them
    # into poll:adaptive (with now + slot_offset) once their first scan finishes.
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

    # ── 2. STALE companies → recovery spread ─────────────────────────────────
    # Sort most-overdue first (lowest next_poll_at) so companies that missed
    # the most polls are processed earliest in the recovery window.
    stale_companies.sort(key=lambda r: (
        r["next_poll_at"].timestamp() if r["next_poll_at"] else 0.0
    ))
    stale_spread = _spread_window_s(len(stale_companies), n_workers, avg_s)
    n_stale      = max(len(stale_companies), 1)
    for i, row in enumerate(stale_companies):
        score = now + (i / n_stale) * stale_spread
        adaptive_entries[row["company"]] = score

        # Preserve full scan schedule if still in the future;
        # if also stale, bump to +5 min to avoid collision with adaptive rush.
        if row["next_full_scan_at"] is not None:
            fs_ts = row["next_full_scan_at"].timestamp()
            fullscan_entries[row["company"]] = fs_ts if fs_ts > now else now + 300
        else:
            fullscan_entries[row["company"]] = now + 300

    if stale_companies:
        logger.info(
            "rebuild: %d stale companies (schedule before %s) → "
            "poll:adaptive recovery spread (spread=%.0fs / %.1f min)",
            len(stale_companies),
            time.strftime("%H:%M", time.localtime(cycle_start)),
            stale_spread, stale_spread / 60,
        )

    # ── 3. CURRENT companies → DB timestamps restored directly ───────────────
    # DB is the source of truth.  Stored next_poll_at goes straight into ZSET.
    # If score <= now: immediately due (but in their original priority order).
    # If score > now: scheduled for the future exactly as the DB says.
    # _reschedule_adaptive() and fullscan scheduling use _pick_schedule_time()
    # (gap-detection algorithm) to maintain even distribution on every reschedule
    # — no spread needed here when the stored distribution is already healthy.
    n_past = n_future = 0
    for row in current_companies:
        score = row["next_poll_at"].timestamp()
        adaptive_entries[row["company"]] = score
        if score <= now:
            n_past += 1
        else:
            n_future += 1

        if row["next_full_scan_at"] is not None:
            fullscan_entries[row["company"]] = row["next_full_scan_at"].timestamp()
        else:
            # Never had a full scan — co-schedule with adaptive
            fullscan_entries[row["company"]] = score

    if current_companies:
        logger.info(
            "rebuild: %d current-cycle companies → DB timestamps restored "
            "(%d immediately due, %d future)",
            len(current_companies), n_past, n_future,
        )

    # ── Write to Redis ────────────────────────────────────────────────────────
    if adaptive_entries:
        r.zadd(REDIS_POLL_ADAPTIVE, adaptive_entries)
    if fullscan_entries:
        r.zadd(REDIS_POLL_FULLSCAN, fullscan_entries)

    n_unregistered = len(unregistered) if unregistered else 0
    logger.info(
        "rebuild: poll:adaptive=%d poll:fullscan=%d "
        "(new=%d unregistered=%d stale=%d current=%d  cycle_start=%s)",
        len(adaptive_entries), len(fullscan_entries),
        len(new_companies) - n_unregistered, n_unregistered,
        len(stale_companies), len(current_companies),
        time.strftime("%H:%M", time.localtime(cycle_start)),
    )
    return {
        "adaptive":     len(adaptive_entries),
        "fullscan":     len(fullscan_entries),
        "new":          len(new_companies) - n_unregistered,
        "unregistered": n_unregistered,
        "stale":        len(stale_companies),
        "current":      len(current_companies),
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
