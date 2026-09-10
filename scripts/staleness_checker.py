"""
scripts/staleness_checker.py â€” Daily cron: push stale companies to enrichment/discovery/redetect queues.

Pass 1a â€” Enrichment staleness (no careers URL):
    fein_domain_map WHERE careers_url IS NULL AND last_enriched_at stale
    â†’ ZADD enrichment:batch petition_count {"fein": ..., "trigger": "staleness"}

Pass 1b â€” Head-check staleness (careers URL known):
    fein_domain_map WHERE careers_url IS NOT NULL AND last_enriched_at stale
    â†’ RPUSH head_check:batch {"fein": ..., "trigger": "staleness"}

Pass 2 â€” Discovery staleness:
    fein_domain_map WHERE last_discovered_at stale AND petition_count >= min
    â†’ ZADD discovery:batch petition_count {"fein": ..., "trigger": "staleness"}

Pass 3 â€” ATS re-detection staleness:
    company_ats WHERE is_monitored=TRUE AND consecutive_empty_days >= JOB_MONITOR_REDETECT_DAYS
        AND platform NOT IN ('unknown','unsupported') AND stale_since IS NULL
    prospective_companies WHERE consecutive_empty_days >= JOB_MONITOR_REDETECT_DAYS
        AND ats_platform NOT IN ('unknown','unsupported','custom')
    â†’ RPUSH head_check:batch {"fein": ..., "trigger": "redetect", "source": "company_ats"|"prospective"}

Pass 4 â€” Stale row purge:
    DELETE FROM company_ats WHERE stale_since IS NOT NULL AND stale_since < NOW() - ATS_STALE_TTL_DAYS days

Worker lifecycle is managed by manager.py (autoscaled on queue depth) â€” this script
only populates the queues and never starts workers directly.

Usage:
    python scripts/staleness_checker.py
    python scripts/staleness_checker.py --dry-run
    python scripts/staleness_checker.py --enrichment-only
    python scripts/staleness_checker.py --discovery-only
    python scripts/staleness_checker.py --redetect-only
"""

import argparse
import json
import os
import sys
import time

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from config import (
    ATS_STALE_TTL_DAYS,
    DISCOVER_REDETECT_EMPTY_DAYS,
    DISCOVERY_BATCH,
    ENRICHMENT_BATCH,
    ENRICH_STALENESS_DAYS,
    HEAD_CHECK_BATCH,
    JOB_MONITOR_REDETECT_DAYS,
    REDIS_DB_MAINTENANCE,
    STALENESS_DISCOVERY_MIN_PETITIONS,
    STALENESS_ZADD_BATCH,
)
from db.connection import get_conn
from db.job_monitor import get_monitorable_companies
from logger import get_logger, init_logging
from workers.redis_client import get_redis

log = get_logger(__name__)


def _is_maintenance(r) -> bool:
    try:
        return bool(r.exists(REDIS_DB_MAINTENANCE))
    except Exception as exc:
        log.warning("Redis maintenance check failed (%s) â€” assuming not in maintenance", exc)
        return False


def _stream_and_zadd(conn, r, sql, params, queue_key, cursor_name, log_prefix, dry_run,
                     trigger="enrichment", source=None, use_list=False):
    """Stream a SELECT query via named cursor and push each row to queue_key.

    use_list=False (default): ZADD to a ZSET with score=petition_count.
    use_list=True: RPUSH to a LIST (HEAD_CHECK_BATCH); petition_count used only for SQL ordering.

    Returns count of rows processed. Handles dry-run logging (first 5 rows),
    pipeline batching (STALENESS_ZADD_BATCH), and final flush.
    Worker lifecycle is managed by manager.py â€” this function never starts workers.
    trigger/source are embedded in every member for consistent queue format across all producers.
    """
    added = 0
    dry_run_sample: list = []
    pipe = None if dry_run else r.pipeline(transaction=False)

    with conn.named_cursor(cursor_name) as cur:
        cur.itersize = 500
        cur.execute(sql, params)
        for row in cur:
            if dry_run:
                if len(dry_run_sample) < 5:
                    dry_run_sample.append(row)
                added += 1
                continue
            member = json.dumps({"fein": row["employer_fein"], "trigger": trigger, "source": source})
            if use_list:
                pipe.rpush(queue_key, member)
            else:
                pipe.zadd(queue_key, {member: row["petition_count"]}, gt=True)
            added += 1
            if added % STALENESS_ZADD_BATCH == 0:
                pipe.execute()
                pipe = r.pipeline(transaction=False)

    if not added:
        log.info("%s: no stale companies", log_prefix)
        return 0

    log.info("%s: %d companies eligible", log_prefix, added)

    if dry_run:
        op = "RPUSH" if use_list else "ZADD"
        for row in dry_run_sample:
            if use_list:
                log.info("[dry-run] would %s %s fein=%s trigger=%s",
                         op, queue_key, row["employer_fein"], trigger)
            else:
                log.info("[dry-run] would %s %s score=%s fein=%s trigger=%s",
                         op, queue_key, row["petition_count"], row["employer_fein"], trigger)
        if added > 5:
            log.info("[dry-run] ... and %d more", added - 5)
        return added

    if added % STALENESS_ZADD_BATCH != 0:
        pipe.execute()

    op = "RPUSH" if use_list else "ZADD"
    log.info("%s: %s %d feins â†’ %s", log_prefix, op, added, queue_key)
    return added


def run_enrichment_staleness(conn, r, dry_run: bool = False) -> int:
    """Push stale companies to the right queue based on whether careers_url is known.

    Pass 1a â€” careers_url IS NULL â†’ ENRICHMENT_BATCH (ZADD, needs full URL discovery)
    Pass 1b â€” careers_url IS NOT NULL â†’ HEAD_CHECK_BATCH (RPUSH, URL known, just verify liveness)

    Returns total count added across both sub-passes.
    """
    monitored_feins = {
        row["employer_fein"] for row in get_monitorable_companies()
        if row.get("employer_fein")
    }

    _stale_interval = f"{ENRICH_STALENESS_DAYS} days"

    def _build_sql_params(careers_url_condition: str):
        if monitored_feins:
            sql = f"""
                SELECT
                    f.employer_fein,
                    COALESCE(u.petition_count, 0) AS petition_count
                FROM fein_domain_map f
                LEFT JOIN uscis_petition_counts u ON u.employer_fein = f.employer_fein
                WHERE {careers_url_condition}
                  AND (
                    (f.public_domain IS NULL
                        AND (f.last_enriched_at IS NULL OR f.last_enriched_at < NOW() - %s::interval))
                    OR (
                        (f.last_enriched_at IS NULL OR f.last_enriched_at < NOW() - %s::interval)
                        AND f.employer_fein = ANY(%s::text[])
                    )
                )
                ORDER BY petition_count DESC
            """
            params = (_stale_interval, _stale_interval, list(monitored_feins))
        else:
            sql = f"""
                SELECT
                    f.employer_fein,
                    COALESCE(u.petition_count, 0) AS petition_count
                FROM fein_domain_map f
                LEFT JOIN uscis_petition_counts u ON u.employer_fein = f.employer_fein
                WHERE {careers_url_condition}
                  AND f.public_domain IS NULL
                  AND (f.last_enriched_at IS NULL OR f.last_enriched_at < NOW() - %s::interval)
                ORDER BY petition_count DESC
            """
            params = (_stale_interval,)
        return sql, params

    # Pass 1a: no careers URL â€” needs full enrichment
    sql_a, params_a = _build_sql_params("f.careers_url IS NULL")
    added_a = _stream_and_zadd(
        conn, r, sql_a, params_a,
        queue_key=ENRICHMENT_BATCH,
        cursor_name="enrichment_staleness_no_url",
        log_prefix="enrichment staleness (no careers URL)",
        dry_run=dry_run,
        trigger="staleness",
    )

    # Pass 1b: careers URL known â€” just verify it's still alive
    sql_b, params_b = _build_sql_params("f.careers_url IS NOT NULL")
    added_b = _stream_and_zadd(
        conn, r, sql_b, params_b,
        queue_key=HEAD_CHECK_BATCH,
        cursor_name="enrichment_staleness_has_url",
        log_prefix="enrichment staleness (has careers URL)",
        dry_run=dry_run,
        trigger="staleness",
        use_list=True,
    )

    return added_a + added_b


def run_discovery_staleness(conn, r, dry_run: bool = False) -> int:
    """Push stale discovery companies to discovery:batch. Returns count added."""
    return _stream_and_zadd(
        conn, r,
        sql="""
            SELECT
                f.employer_fein,
                COALESCE(u.petition_count, 0) AS petition_count
            FROM fein_domain_map f
            LEFT JOIN uscis_petition_counts u ON u.employer_fein = f.employer_fein
            WHERE COALESCE(u.petition_count, 0) >= %s
              AND (
                  f.last_discovered_at IS NULL                        -- never discovered
                  OR f.last_discovered_at < NOW() - %s::interval      -- stale (ATS may have changed)
              )
            ORDER BY petition_count DESC
        """,
        params=(
            STALENESS_DISCOVERY_MIN_PETITIONS,
            f"{JOB_MONITOR_REDETECT_DAYS} days",
        ),
        queue_key=DISCOVERY_BATCH,
        cursor_name="discovery_staleness",
        log_prefix="discovery staleness",
        dry_run=dry_run,
        trigger="staleness",
    )


def run_redetect_staleness(conn, r, dry_run: bool = False) -> int:
    """Push companies with silent monitored ATS paths to head_check:batch with trigger=redetect (pass 3).

    The head_check worker sees trigger="redetect" and routes the company through a liveness check
    before enqueuing to discovery:redetect for full ATS re-detection.

    Two sub-queries:
      3a. company_ats: is_monitored=TRUE, consecutive_empty_days >= JOB_MONITOR_REDETECT_DAYS,
          platform not unknown/unsupported, stale_since IS NULL
      3b. prospective_companies: same empty-days condition, platform not unknown/unsupported/custom
          â€” joined to fein_domain_map via domain to get FEIN.

    Returns total count queued.
    """
    redetect_days = JOB_MONITOR_REDETECT_DAYS
    added = 0
    pipe = None if dry_run else r.pipeline(transaction=False)

    # 3a â€” company_ats silent rows
    with conn.named_cursor("redetect_company_ats") as cur:
        cur.itersize = 500
        cur.execute("""
            SELECT DISTINCT
                ca.employer_fein,
                COALESCE(u.petition_count, 0) AS petition_count
            FROM company_ats ca
            LEFT JOIN uscis_petition_counts u ON u.employer_fein = ca.employer_fein
            WHERE ca.is_monitored = TRUE
              AND ca.consecutive_empty_days >= %s
              AND ca.platform NOT IN ('unknown', 'unsupported')
              AND ca.stale_since IS NULL
              AND ca.employer_fein IS NOT NULL
            ORDER BY petition_count DESC
        """, (redetect_days,))
        for row in cur:
            if dry_run:
                log.info("[dry-run] would RPUSH %s fein=%s trigger=redetect source=company_ats",
                         HEAD_CHECK_BATCH, row["employer_fein"])
                added += 1
                continue
            member = json.dumps({"fein": row["employer_fein"], "trigger": "redetect", "source": "company_ats"})
            pipe.rpush(HEAD_CHECK_BATCH, member)
            added += 1
            if added % STALENESS_ZADD_BATCH == 0:
                pipe.execute()
                pipe = r.pipeline(transaction=False)

    # 3b â€” prospective_companies silent rows
    with conn.named_cursor("redetect_prospective") as cur:
        cur.itersize = 500
        cur.execute("""
            SELECT DISTINCT
                f.employer_fein,
                COALESCE(u.petition_count, 0) AS petition_count
            FROM prospective_companies pc
            JOIN fein_domain_map f
                ON regexp_replace(regexp_replace(LOWER(f.assigned_domain), '^https?://', ''), '^www\\.', '') =
                   regexp_replace(regexp_replace(LOWER(pc.domain), '^https?://', ''), '^www\\.', '')
            LEFT JOIN uscis_petition_counts u ON u.employer_fein = f.employer_fein
            WHERE pc.consecutive_empty_days >= %s
              AND pc.ats_platform IS NOT NULL
              AND pc.ats_platform NOT IN ('unknown', 'unsupported', 'custom')
            ORDER BY petition_count DESC
        """, (redetect_days,))
        for row in cur:
            if dry_run:
                log.info("[dry-run] would RPUSH %s fein=%s trigger=redetect source=prospective",
                         HEAD_CHECK_BATCH, row["employer_fein"])
                added += 1
                continue
            member = json.dumps({"fein": row["employer_fein"], "trigger": "redetect", "source": "prospective"})
            pipe.rpush(HEAD_CHECK_BATCH, member)
            added += 1
            if added % STALENESS_ZADD_BATCH == 0:
                pipe.execute()
                pipe = r.pipeline(transaction=False)

    if not dry_run and pipe is not None and added % STALENESS_ZADD_BATCH != 0:
        pipe.execute()

    if not added:
        log.info("redetect staleness: no companies need re-detection")
    else:
        log.info("redetect staleness: %d companies queued â†’ %s (trigger=redetect)", added, HEAD_CHECK_BATCH)
    return added


def run_stale_purge(conn, dry_run: bool = False) -> int:
    """Delete company_ats rows where stale_since exceeded ATS_STALE_TTL_DAYS (pass 4).

    Returns count of rows deleted (or would-be-deleted in dry-run).
    """
    if dry_run:
        row = conn.execute("""
            SELECT COUNT(*) AS stale_count FROM company_ats
            WHERE stale_since IS NOT NULL
              AND stale_since < NOW() - make_interval(days => %s)
        """, (ATS_STALE_TTL_DAYS,)).fetchone()
        count = row["stale_count"] if row else 0
        log.info("[dry-run] stale purge: %d company_ats rows would be deleted", count)
        return count

    cur = conn.execute("""
        DELETE FROM company_ats
        WHERE stale_since IS NOT NULL
          AND stale_since < NOW() - make_interval(days => %s)
    """, (ATS_STALE_TTL_DAYS,))
    conn.commit()
    count = cur.rowcount
    if count:
        log.info("stale purge: deleted %d stale company_ats rows", count)
    else:
        log.info("stale purge: no stale rows to delete")
    return count


def main(args: argparse.Namespace) -> None:
    r = get_redis()

    if _is_maintenance(r):
        log.info("maintenance window active â€” skipping staleness check")
        return

    conn = get_conn()
    try:
        t0 = time.time()

        enrich_added = 0
        discovery_added = 0
        redetect_added = 0
        purged = 0

        if not args.discovery_only and not args.redetect_only:
            enrich_added = run_enrichment_staleness(conn, r, dry_run=args.dry_run)

        if not args.enrichment_only and not args.redetect_only:
            discovery_added = run_discovery_staleness(conn, r, dry_run=args.dry_run)

        if not args.enrichment_only and not args.discovery_only:
            redetect_added = run_redetect_staleness(conn, r, dry_run=args.dry_run)
            purged = run_stale_purge(conn, dry_run=args.dry_run)

        elapsed = time.time() - t0
        log.info(
            "staleness_checker done in %.1fs â€” enrichment: %d, discovery: %d, "
            "redetect: %d, purged: %d%s",
            elapsed, enrich_added, discovery_added, redetect_added, purged,
            " [dry-run]" if args.dry_run else "",
        )
    finally:
        conn.close()


if __name__ == "__main__":
    init_logging("staleness_checker")
    parser = argparse.ArgumentParser(description="Push stale H1B companies to enrichment/discovery queues")
    parser.add_argument("--dry-run",          action="store_true", help="Log what would be queued without writing to Redis")
    _mode = parser.add_mutually_exclusive_group()
    _mode.add_argument("--enrichment-only",  action="store_true", help="Only run enrichment staleness check")
    _mode.add_argument("--discovery-only",   action="store_true", help="Only run discovery staleness check")
    _mode.add_argument("--redetect-only",    action="store_true", help="Only run redetect staleness check + stale purge")
    main(parser.parse_args())
