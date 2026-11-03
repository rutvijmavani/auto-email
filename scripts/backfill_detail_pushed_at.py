"""
scripts/backfill_detail_pushed_at.py — ONE-OFF cut-over backfill for the detail admission gate.

Design: docs/detail-pool-ceiling-th-design.md section 9.3 (option C), step 9b.

job_postings.pushed_at IS NULL on a pending_detail row means HELD (the detail drainer
releases it).  When the column is first added, every existing pending_detail row is NULL,
including the jobs already sitting in the Redis detail queues.  Left alone, the drainer
would push those a second time.  This script stamps pushed_at = now() for exactly the
jobs Redis already holds:

  - the queue lists            queue:detail:adaptive / queue:detail:fullscan
  - the per-worker inflight    <queue>:inflight:*
  - the retry-delay ZSETs      <queue>:retry_delay
  - the dead-letter list       queue:detail:dlq

Every other pending_detail row stays NULL and drains at the bucket rate.

Deploy order: deploy code with the column -> run this script -> start the drainer
(recruiter-detail-drainer) -> producers are already save-only.  Idempotent: only rows
with pushed_at IS NULL are touched.

Usage:
    python -m scripts.backfill_detail_pushed_at --dry-run     # counts only, writes nothing
    python -m scripts.backfill_detail_pushed_at
"""

import argparse
import json
import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from config import REDIS_DETAIL_ADAPTIVE, REDIS_DETAIL_DLQ, REDIS_DETAIL_FULLSCAN
from db.connection import get_conn
from logger import get_logger, init_logging
from workers.redis_client import get_redis

log = get_logger(__name__)

_SCAN_COUNT   = 1000
_UPDATE_BATCH = 500
_RETRY_SUFFIX = ":retry_delay"       # mirrors workers/detail_worker._RETRY_DELAY_KEY_SUFFIX
_QUEUES       = (REDIS_DETAIL_ADAPTIVE, REDIS_DETAIL_FULLSCAN)


def _decode(value):
    return value.decode() if isinstance(value, (bytes, bytearray)) else value


def _url_of(raw):
    """job_url from one queued payload, or None when it cannot be read."""
    try:
        return json.loads(_decode(raw)).get("job_url")
    except (TypeError, ValueError, AttributeError):
        return None


def collect_queued_urls(r) -> dict:
    """job_url set per source, from every place Redis holds a detail job."""
    found = {}
    for queue in _QUEUES:
        found[queue] = {_url_of(raw) for raw in r.lrange(queue, 0, -1)}
        inflight = set()
        for key in r.scan_iter(match=f"{queue}:inflight:*", count=_SCAN_COUNT):
            inflight.update(_url_of(raw) for raw in r.lrange(key, 0, -1))
        found[f"{queue}:inflight"] = inflight
        found[f"{queue}{_RETRY_SUFFIX}"] = {
            _url_of(raw) for raw in r.zrange(f"{queue}{_RETRY_SUFFIX}", 0, -1)
        }
    found[REDIS_DETAIL_DLQ] = {_url_of(raw) for raw in r.lrange(REDIS_DETAIL_DLQ, 0, -1)}
    for source in found:
        found[source].discard(None)
    return found


def stamp_urls(conn, urls: list, dry_run: bool) -> int:
    """Set pushed_at = now() on the held pending_detail rows for these urls."""
    total = 0
    for i in range(0, len(urls), _UPDATE_BATCH):
        batch = urls[i:i + _UPDATE_BATCH]
        if dry_run:
            row = conn.execute("""
                SELECT COUNT(*) AS n FROM job_postings
                WHERE status = 'pending_detail' AND pushed_at IS NULL
                  AND job_url = ANY(%s)
            """, (batch,)).fetchone()
            total += row["n"]
        else:
            cur = conn.execute("""
                UPDATE job_postings SET pushed_at = NOW()
                WHERE status = 'pending_detail' AND pushed_at IS NULL
                  AND job_url = ANY(%s)
            """, (batch,))
            total += cur.rowcount
    if not dry_run:
        conn.commit()
    return total


def main(args: argparse.Namespace) -> None:
    r = get_redis()
    r.ping()
    conn = get_conn()
    try:
        tag = " [dry-run]" if args.dry_run else ""
        found = collect_queued_urls(r)
        for source, urls in found.items():
            log.info("backfill pushed_at%s — %s holds %d job urls", tag, source, len(urls))
        all_urls = sorted(set().union(*found.values()))

        held = conn.execute(
            "SELECT COUNT(*) AS n FROM job_postings "
            "WHERE status = 'pending_detail' AND pushed_at IS NULL"
        ).fetchone()["n"]
        stamped = stamp_urls(conn, all_urls, args.dry_run)
        verb = "would stamp" if args.dry_run else "stamped"
        log.info(
            "backfill pushed_at%s — %d distinct urls in Redis, %s %d rows pushed_at; "
            "%d held pending_detail rows were NULL before, %d stay held for the drainer",
            tag, len(all_urls), verb, stamped, held, held - stamped,
        )
    finally:
        conn.close()


if __name__ == "__main__":
    init_logging("backfill_detail_pushed_at")
    parser = argparse.ArgumentParser(
        description="One-off: stamp pushed_at for pending_detail jobs already in the Redis detail queues")
    parser.add_argument("--dry-run", action="store_true", help="Print counts only; change nothing")
    main(parser.parse_args())
