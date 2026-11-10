# workers/db_capacity.py — live Postgres connection capacity for the manager
#
# The manager's worker budget is counted in worker seats, but the hard limit is
# Postgres connections, and a worker holds more than one.  This module measures
# the conversion instead of assuming it:
#
#   cap          = max_connections - superuser_reserved_connections - margin
#   non_worker   = connections held by processes that are not live workers
#   per_worker   = most connections any live worker process holds right now
#   seat_cap     = (cap - non_worker) // per_worker
#
# Every process labels its connections application_name="pid<PID>" (see
# db/connection.py), so pg_stat_activity attributes each connection to the
# process that owns it; live worker PIDs come from the worker:alive heartbeats.
# Nothing here is remembered between calls.

import re

from config import DB_POOL_MINCONN, DB_CAPACITY_MARGIN
from logger import get_logger

logger = get_logger(__name__)

_APP_NAME_RE = re.compile(r"^pid(\d+)$")


def live_worker_pids(r) -> set:
    """PIDs of the workers that currently have a worker:alive heartbeat."""
    pids = set()
    cursor = 0
    while True:
        cursor, keys = r.scan(cursor, match="worker:alive:*", count=200)
        for k in keys:
            k = k.decode() if isinstance(k, bytes) else k
            try:
                pids.add(int(k.rsplit(":", 1)[-1]))
            except ValueError:
                pass
        if cursor == 0:
            break
    return pids


def probe(r) -> "dict | None":
    """
    Measure Postgres connection capacity now.  Returns None when Postgres cannot
    be queried — the caller must then stop raising the budget rather than guess.

    Keys: max_connections, reserved, margin, cap, used, worker_conns,
    non_worker, per_worker, seat_cap.
    """
    from db.db import get_conn

    try:
        pids = live_worker_pids(r)
        with get_conn() as conn:
            cur = conn.cursor()
            cur.execute("SHOW max_connections")
            max_conn = int(list(cur.fetchone().values())[0])
            cur.execute("SHOW superuser_reserved_connections")
            reserved = int(list(cur.fetchone().values())[0])
            cur.execute(
                "SELECT application_name, COUNT(*) AS n FROM pg_stat_activity "
                "WHERE backend_type = 'client backend' GROUP BY application_name"
            )
            rows = cur.fetchall()
    except Exception as exc:
        logger.error("db_capacity: Postgres probe failed: %s", exc)
        return None

    used = 0
    worker_conns = 0
    per_worker = DB_POOL_MINCONN
    for row in rows:
        n = int(row["n"])
        used += n
        m = _APP_NAME_RE.match(row["application_name"] or "")
        if m and int(m.group(1)) in pids:
            worker_conns += n
            per_worker = max(per_worker, n)
    non_worker = used - worker_conns
    cap = max_conn - reserved - DB_CAPACITY_MARGIN
    return {
        "max_connections": max_conn,
        "reserved": reserved,
        "margin": DB_CAPACITY_MARGIN,
        "cap": cap,
        "used": used,
        "worker_conns": worker_conns,
        "non_worker": non_worker,
        "per_worker": per_worker,
        "seat_cap": max(0, (cap - non_worker) // per_worker),
    }
