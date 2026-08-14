"""
scripts/staleness_checker.py — Daily cron: push stale companies to enrichment/discovery queues.

Enrichment staleness:
    fein_domain_map WHERE last_enriched_at < NOW() - INTERVAL '<ENRICH_STALENESS_DAYS> days'
    AND is_monitored = TRUE (or public_domain IS NULL for uninitialised rows)
    → ZADD domain_enrichment_queue petition_count fein
    → systemctl start domain-enrichment-worker@1 domain-enrichment-worker@2

Discovery staleness:
    fein_domain_map WHERE last_discovered_at < NOW() - INTERVAL '<DISCOVER_REDETECT_EMPTY_DAYS> days'
    AND petition_count >= STALENESS_DISCOVERY_MIN_PETITIONS
    (also catches companies with no ATS yet and petition_count >= threshold)
    → ZADD discovery_queue petition_count {"fein": ..., "trigger": "staleness"}
    → systemctl start discover-h1b-ats-worker@1 discover-h1b-ats-worker@2

Usage:
    python scripts/staleness_checker.py
    python scripts/staleness_checker.py --dry-run
    python scripts/staleness_checker.py --enrichment-only
    python scripts/staleness_checker.py --discovery-only
"""

import argparse
import json
import os
import sys
import time

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from config import (
    DISCOVER_REDETECT_EMPTY_DAYS,
    DISCOVERY_QUEUE,
    DOMAIN_ENRICHMENT_QUEUE,
    ENRICH_STALENESS_DAYS,
    REDIS_DB_MAINTENANCE,
    STALENESS_DISCOVERY_MIN_PETITIONS,
    STALENESS_ZADD_BATCH,
)
from db.connection import get_conn
from db.job_monitor import get_monitorable_companies
from logger import get_logger, init_logging
from workers.redis_client import get_redis
from workers.worker_control import start_workers as _start_workers, ENRICHMENT_WORKERS, DISCOVERY_WORKERS

log = get_logger(__name__)


def _is_maintenance(r) -> bool:
    try:
        return bool(r.exists(REDIS_DB_MAINTENANCE))
    except Exception as exc:
        log.warning("Redis maintenance check failed (%s) — assuming not in maintenance", exc)
        return False


def run_enrichment_staleness(conn, r, dry_run: bool = False) -> int:
    """Push stale enrichment companies to domain_enrichment_queue. Returns count added."""
    # public_domain IS NULL: always re-enrich (uninitialised).
    # stale last_enriched_at: only re-enrich companies actively monitored by job_monitor.
    monitored_feins = {
        row["employer_fein"] for row in get_monitorable_companies()
        if row.get("employer_fein")
    }

    _stale_interval = f"{ENRICH_STALENESS_DAYS} days"
    if monitored_feins:
        rows = conn.execute("""
            SELECT
                f.employer_fein,
                COALESCE(u.petition_count, 0) AS petition_count
            FROM fein_domain_map f
            LEFT JOIN uscis_petition_counts u ON u.employer_fein = f.employer_fein
            WHERE (
                -- Never resolved: re-enrich if also past the staleness window
                -- (prevents re-queuing every day for companies that reliably fail)
                (f.public_domain IS NULL
                    AND (f.last_enriched_at IS NULL OR f.last_enriched_at < NOW() - %s::interval))
                OR (
                    (f.last_enriched_at IS NULL OR f.last_enriched_at < NOW() - %s::interval)
                    AND f.employer_fein = ANY(%s::text[])
                )
            )
            ORDER BY petition_count DESC
        """, (_stale_interval, _stale_interval, list(monitored_feins))).fetchall()
    else:
        # No monitored companies — only pick up uninitialised rows (public_domain IS NULL)
        rows = conn.execute("""
            SELECT
                f.employer_fein,
                COALESCE(u.petition_count, 0) AS petition_count
            FROM fein_domain_map f
            LEFT JOIN uscis_petition_counts u ON u.employer_fein = f.employer_fein
            WHERE f.public_domain IS NULL
              AND (f.last_enriched_at IS NULL OR f.last_enriched_at < NOW() - %s::interval)
            ORDER BY petition_count DESC
        """, (_stale_interval,)).fetchall()

    if not rows:
        log.info("enrichment staleness: no stale companies")
        return 0

    log.info("enrichment staleness: %d companies eligible", len(rows))

    if dry_run:
        for row in rows[:5]:
            log.info("[dry-run] would ZADD %s score=%s fein=%s",
                     DOMAIN_ENRICHMENT_QUEUE, row["petition_count"], row["employer_fein"])
        if len(rows) > 5:
            log.info("[dry-run] ... and %d more", len(rows) - 5)
        return len(rows)

    added = 0
    pipe = r.pipeline(transaction=False)
    for i, row in enumerate(rows):
        member = json.dumps({"fein": row["employer_fein"], "trigger": "staleness"})
        pipe.zadd(
            DOMAIN_ENRICHMENT_QUEUE,
            {member: row["petition_count"]},
            gt=True,    # only raise score — prevents lowering a high-priority item
        )
        added += 1
        if (i + 1) % STALENESS_ZADD_BATCH == 0:
            pipe.execute()
            pipe = r.pipeline(transaction=False)

    if added % STALENESS_ZADD_BATCH != 0:
        pipe.execute()

    log.info("enrichment staleness: ZADD %d feins → %s", added, DOMAIN_ENRICHMENT_QUEUE)
    _start_workers(*ENRICHMENT_WORKERS)
    return added


def run_discovery_staleness(conn, r, dry_run: bool = False) -> int:
    """Push stale discovery companies to discovery_queue. Returns count added."""
    rows = conn.execute("""
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
    """, (
        STALENESS_DISCOVERY_MIN_PETITIONS,
        f"{DISCOVER_REDETECT_EMPTY_DAYS} days",
    )).fetchall()

    if not rows:
        log.info("discovery staleness: no stale companies")
        return 0

    log.info("discovery staleness: %d companies eligible", len(rows))

    if dry_run:
        for row in rows[:5]:
            log.info("[dry-run] would ZADD %s score=%s fein=%s",
                     DISCOVERY_QUEUE, row["petition_count"], row["employer_fein"])
        if len(rows) > 5:
            log.info("[dry-run] ... and %d more", len(rows) - 5)
        return len(rows)

    added = 0
    pipe = r.pipeline(transaction=False)
    for i, row in enumerate(rows):
        member = json.dumps({"fein": row["employer_fein"], "trigger": "staleness"})
        pipe.zadd(
            DISCOVERY_QUEUE,
            {member: row["petition_count"]},
            gt=True,    # only raise score — prevents lowering a high-priority item
        )
        added += 1
        if (i + 1) % STALENESS_ZADD_BATCH == 0:
            pipe.execute()
            pipe = r.pipeline(transaction=False)

    if added % STALENESS_ZADD_BATCH != 0:
        pipe.execute()

    log.info("discovery staleness: ZADD %d feins → %s", added, DISCOVERY_QUEUE)
    _start_workers(*DISCOVERY_WORKERS)
    return added


def main(args: argparse.Namespace) -> None:
    r = get_redis()

    if _is_maintenance(r):
        log.info("maintenance window active — skipping staleness check")
        return

    conn = get_conn()
    try:
        t0 = time.time()

        enrich_added = 0
        discovery_added = 0

        if not args.discovery_only:
            enrich_added = run_enrichment_staleness(conn, r, dry_run=args.dry_run)

        if not args.enrichment_only:
            discovery_added = run_discovery_staleness(conn, r, dry_run=args.dry_run)

        elapsed = time.time() - t0
        log.info(
            "staleness_checker done in %.1fs — enrichment: %d, discovery: %d%s",
            elapsed, enrich_added, discovery_added,
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
    main(parser.parse_args())
