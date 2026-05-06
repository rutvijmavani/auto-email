"""
workers/rebuild.py — Rebuild Redis state from PostgreSQL on startup (Phase 3).

Redis is NOT the source of truth. On any restart (planned or crash), all
Redis structures are rebuilt from PostgreSQL in ~30 seconds.

Rebuild order (from doc Section 18):
    1. poll:adaptive       ← company_poll_stats.next_poll_at
    2. poll:fullscan       ← company_poll_stats.next_full_scan_at
    3. queue:detail:*      ← job_postings WHERE status='pending_detail'
    4. seen:{company}      ← job_postings (company + job_id columns)
    5. stats:{company}     ← company_poll_stats

Usage:
    from workers.rebuild import rebuild_redis
    rebuild_redis()    # call once at scheduler startup

    # Or for targeted rebuild:
    from workers.rebuild import rebuild_seen_ids, rebuild_poll_queues
"""

import json
import logging
import time

from workers.redis_client import get_redis
from db.db import get_conn, init_db
from config import (
    REDIS_POLL_ADAPTIVE,
    REDIS_POLL_FULLSCAN,
    REDIS_DETAIL_ADAPTIVE,
)

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────
# 1. POLL QUEUES
# ─────────────────────────────────────────

def rebuild_poll_queues() -> dict:
    """
    Rebuild poll:adaptive and poll:fullscan ZSETs from company_poll_stats.

    Companies with next_poll_at IS NULL are treated as immediately due
    (score = now - 1 so they sort to the front).

    Returns dict with counts: {"adaptive": N, "fullscan": N}
    """
    r   = get_redis()
    now = time.time()

    adaptive_entries: dict = {}
    fullscan_entries: dict = {}

    conn = get_conn()
    try:
        rows = conn.execute("""
            SELECT company, next_poll_at, next_full_scan_at
            FROM company_poll_stats
        """).fetchall()
    finally:
        conn.close()

    for row in rows:
        company = row["company"]

        # Adaptive queue
        if row["next_poll_at"] is None:
            score = now - 1   # immediately due
        else:
            score = row["next_poll_at"].timestamp()
        adaptive_entries[company] = score

        # Full scan queue (only if scheduled)
        if row["next_full_scan_at"] is not None:
            fullscan_entries[company] = row["next_full_scan_at"].timestamp()

    if adaptive_entries:
        r.zadd(REDIS_POLL_ADAPTIVE, adaptive_entries)

    if fullscan_entries:
        r.zadd(REDIS_POLL_FULLSCAN, fullscan_entries)

    logger.info(
        "rebuild: poll:adaptive=%d poll:fullscan=%d",
        len(adaptive_entries), len(fullscan_entries),
    )
    return {"adaptive": len(adaptive_entries), "fullscan": len(fullscan_entries)}


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
            ORDER BY found_at ASC
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
# 3. SEEN:{COMPANY} SETS
# ─────────────────────────────────────────

def rebuild_seen_ids(company: str = None) -> int:
    """
    Rebuild seen:{company} Redis SETs from job_postings.job_id.

    If company is given, rebuilds only that company's set.
    If None, rebuilds ALL companies (slow — use at startup only).

    The seen:{company} SET is used by the listing scan worker for
    incremental diff: new_ids = fetched_ids - seen:{company}

    Returns total job IDs loaded.
    """
    r    = get_redis()
    conn = get_conn()
    total = 0

    try:
        if company:
            rows = conn.execute("""
                SELECT company, job_id FROM job_postings
                WHERE company = ? AND job_id IS NOT NULL
            """, (company,)).fetchall()
        else:
            rows = conn.execute("""
                SELECT company, job_id FROM job_postings
                WHERE job_id IS NOT NULL
            """).fetchall()
    finally:
        conn.close()

    # Group by company and SADD in batches
    by_company: dict = {}
    for row in rows:
        c  = row["company"]
        jid = row["job_id"]
        if c not in by_company:
            by_company[c] = []
        by_company[c].append(jid)

    pipe = r.pipeline()
    for comp, ids in by_company.items():
        key = f"seen:{comp}"
        # Add in chunks of 500 to avoid huge pipeline commands
        for i in range(0, len(ids), 500):
            pipe.sadd(key, *ids[i:i + 500])
        total += len(ids)

    pipe.execute()

    logger.info(
        "rebuild: seen_ids loaded %d IDs across %d companies",
        total, len(by_company),
    )
    return total


# ─────────────────────────────────────────
# 4. STATS CACHE
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

def rebuild_redis(skip_seen_ids: bool = False) -> dict:
    """
    Full Redis rebuild from PostgreSQL. Call once at scheduler startup.

    Args:
        skip_seen_ids: if True, skip the seen:{company} SET rebuild.
                       Use when Redis is warm and SETs are intact
                       (e.g. scheduler restart only, not Redis restart).

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

    if not skip_seen_ids:
        result["seen_ids"] = rebuild_seen_ids()
    else:
        logger.info("rebuild: skipping seen_ids (skip_seen_ids=True)")
        result["seen_ids"] = 0

    elapsed = time.time() - start
    logger.info(
        "rebuild: complete in %.1fs | adaptive=%d fullscan=%d "
        "pending_detail=%d seen_ids=%d stats=%d",
        elapsed,
        result.get("adaptive", 0),
        result.get("fullscan", 0),
        result.get("pending_detail", 0),
        result.get("seen_ids", 0),
        result.get("stats", 0),
    )
    return result
