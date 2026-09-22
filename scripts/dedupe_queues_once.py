"""
scripts/dedupe_queues_once.py — ONE-OFF post-deploy cleanup of the doubled enrichment/discovery queues.

Before the 2026-09-21 producer split, fuzzy_match (trigger "enrichment") and staleness_checker
(trigger "staleness") both queued every never-enriched FEIN, and staleness_checker also queued
every FEIN into discovery:batch. The trigger is part of the ZSET member JSON, so ZADD saw
different members and each company was processed twice. The producers are now disjoint; this
script removes the twins already sitting in the queues. Safe because queues are rebuilt from
the DB by the daily producers.

  1. enrichment:batch — for FEINs with last_enriched_at IS NULL, drop the trigger="staleness"
     member (making sure the trigger="enrichment" member exists first, so nothing is lost).
  2. discovery:batch  — drop trigger "staleness"/"enrichment" members (source None) whose FEIN
     has last_enriched_at IS NULL; they re-enter via the enrichment worker's forward once the
     company is enriched. redetect / re_detection / manual triggers and company_ats /
     prospective sources are never touched.
  3. fein_domain_map.last_discovered_at — stamp rows that have an h1b_ats_discovery row but a
     NULL stamp, from h1b_ats_discovery.last_checked (informational only; nothing reads it now).

Deploy order: deploy code -> workers restart (inflight reclaim may re-add a stray member)
-> run this script.

Usage:
    python -m scripts.dedupe_queues_once --dry-run     # counts only, writes nothing
    python -m scripts.dedupe_queues_once
"""

import argparse
import json
import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from config import DISCOVERY_BATCH, ENRICHMENT_BATCH
from db.connection import get_conn
from logger import get_logger, init_logging
from workers.redis_client import get_redis

log = get_logger(__name__)

_SCAN_COUNT  = 1000
_ZREM_BATCH  = 500

# Discovery members with these triggers/sources carry intent that is independent of enrichment.
_KEEP_TRIGGERS = frozenset({"redetect", "re_detection", "manual", "on_demand"})
_KEEP_SOURCES  = frozenset({"company_ats", "prospective"})


def _decode(value):
    return value.decode() if isinstance(value, (bytes, bytearray)) else value


def _never_enriched_feins(conn) -> set:
    rows = conn.execute(
        "SELECT employer_fein FROM fein_domain_map WHERE last_enriched_at IS NULL"
    ).fetchall()
    return {row["employer_fein"] for row in rows}


def _zrem_batched(r, key, members, dry_run) -> int:
    if dry_run or not members:
        return len(members)
    removed = 0
    for i in range(0, len(members), _ZREM_BATCH):
        removed += r.zrem(key, *members[i:i + _ZREM_BATCH])
    return removed


def dedupe_enrichment(r, never_enriched: set, dry_run: bool) -> None:
    by_fein: dict = {}
    total = 0
    for raw, score in r.zscan_iter(ENRICHMENT_BATCH, count=_SCAN_COUNT):
        total += 1
        member = _decode(raw)
        try:
            data = json.loads(member)
        except (TypeError, ValueError):
            continue
        fein = data.get("fein")
        if fein not in never_enriched:
            continue
        slot = by_fein.setdefault(fein, {})
        slot[data.get("trigger")] = (member, score)

    drop, readd = [], []
    for fein, slot in by_fein.items():
        if "staleness" not in slot:
            continue
        stale_member, score = slot["staleness"]
        drop.append(stale_member)
        if "enrichment" not in slot:
            readd.append((fein, score))

    log.info("enrichment:batch — %d members scanned, %d never-enriched FEINs queued, "
             "%d staleness twins to remove (%d of them have no enrichment member and get one re-added)",
             total, len(by_fein), len(drop), len(readd))
    if dry_run:
        return
    if readd:
        pipe = r.pipeline(transaction=False)
        for fein, score in readd:
            member = json.dumps({"fein": fein, "trigger": "enrichment", "source": None, "tier": "batch"})
            pipe.zadd(ENRICHMENT_BATCH, {member: score}, gt=True)
        pipe.execute()
    removed = _zrem_batched(r, ENRICHMENT_BATCH, drop, dry_run)
    log.info("enrichment:batch — removed %d staleness twins", removed)


def dedupe_discovery(r, never_enriched: set, dry_run: bool) -> None:
    drop = []
    total = 0
    for raw, _score in r.zscan_iter(DISCOVERY_BATCH, count=_SCAN_COUNT):
        total += 1
        member = _decode(raw)
        try:
            data = json.loads(member)
        except (TypeError, ValueError):
            continue    # legacy bare-FEIN members are left alone
        if data.get("fein") not in never_enriched:
            continue
        if data.get("trigger") in _KEEP_TRIGGERS or data.get("source") in _KEEP_SOURCES:
            continue
        drop.append(member)

    log.info("discovery:batch — %d members scanned, %d belong to never-enriched FEINs "
             "(re-enter via the enrichment forward)", total, len(drop))
    if dry_run:
        return
    removed = _zrem_batched(r, DISCOVERY_BATCH, drop, dry_run)
    log.info("discovery:batch — removed %d members", removed)


def stamp_last_discovered(conn, dry_run: bool) -> None:
    where = """
        FROM h1b_ats_discovery d
        WHERE d.employer_fein = f.employer_fein
          AND f.last_discovered_at IS NULL
          AND d.last_checked IS NOT NULL
    """
    if dry_run:
        row = conn.execute(f"""
            SELECT COUNT(*) AS n FROM fein_domain_map f
            WHERE EXISTS (SELECT 1 {where})
        """).fetchone()
        log.info("last_discovered_at — %d rows would be stamped from h1b_ats_discovery.last_checked", row["n"])
        return
    cur = conn.execute(f"""
        UPDATE fein_domain_map f
        SET last_discovered_at = d.last_checked
        {where}
    """)
    conn.commit()
    log.info("last_discovered_at — stamped %d rows from h1b_ats_discovery.last_checked", cur.rowcount)


def main(args: argparse.Namespace) -> None:
    r = get_redis()
    r.ping()
    conn = get_conn()
    try:
        tag = " [dry-run]" if args.dry_run else ""
        log.info("queue dedupe start%s — enrichment:batch=%d discovery:batch=%d",
                 tag, r.zcard(ENRICHMENT_BATCH), r.zcard(DISCOVERY_BATCH))
        never_enriched = _never_enriched_feins(conn)
        log.info("%d FEINs have last_enriched_at IS NULL", len(never_enriched))

        dedupe_enrichment(r, never_enriched, args.dry_run)
        dedupe_discovery(r, never_enriched, args.dry_run)
        stamp_last_discovered(conn, args.dry_run)

        log.info("queue dedupe done%s — enrichment:batch=%d discovery:batch=%d",
                 tag, r.zcard(ENRICHMENT_BATCH), r.zcard(DISCOVERY_BATCH))
    finally:
        conn.close()


if __name__ == "__main__":
    init_logging("dedupe_queues_once")
    parser = argparse.ArgumentParser(description="One-off: remove doubled enrichment/discovery queue members")
    parser.add_argument("--dry-run", action="store_true", help="Print counts only; change nothing")
    main(parser.parse_args())
