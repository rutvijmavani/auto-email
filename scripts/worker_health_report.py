"""
scripts/worker_health_report.py
-------------------------------
Read-only report over the scheduler's hourly worker-pool snapshots (Redis stream
worker:health_history, written by workers/scheduler.py _snapshot_worker_health).

Per snapshot and worker type it shows started / pending_start / consecutive_deaths, and the
change since the previous snapshot in replacements and never-started kills (both counters
are cumulative, so a delta > 0 means it happened in that hour).  Snapshots with anything
worth a look are flagged:
  * kills   - a worker never finished starting and was killed (HUNG START in scheduler.log)
  * repl    - a worker died or was killed and was replaced
  * pending - workers spawned but not started at snapshot time
  * deaths  - consecutive rapid deaths > 0
  * slow    - slowest observed start-up is more than --slow-factor x the median of the series
A scheduler restart resets the counters; a drop is shown as a restart, not a negative delta.

Usage:
    python scripts/worker_health_report.py [--hours 72] [--slow-factor 3] [--all]
"""

import argparse
import json
import statistics
import sys
import time
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from config import REDIS_WORKER_HEALTH_STREAM
from logger import get_logger, init_logging
from workers.redis_client import get_redis

logger = get_logger(__name__)

TYPES = ("scan", "detail", "fullscan")


def _load(r, hours: float) -> list:
    """Snapshots newer than `hours`, oldest first."""
    min_id = f"{int((time.time() - hours * 3600) * 1000)}-0"
    rows = []
    for _id, fields in r.xrange(REDIS_WORKER_HEALTH_STREAM, min=min_id, max="+"):
        raw = fields.get(b"data") or fields.get("data")
        if raw:
            rows.append(json.loads(raw))
    return rows


def _delta(cur: int, prev: int) -> int:
    """Counter change; a lower value means the scheduler restarted (counter reset)."""
    return cur if cur < prev else cur - prev


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--hours", type=float, default=72)
    ap.add_argument("--slow-factor", type=float, default=3.0)
    ap.add_argument("--all", action="store_true", help="print every snapshot, not just flagged ones")
    args = ap.parse_args()
    init_logging("worker_health_report")

    rows = _load(get_redis(), args.hours)
    if not rows:
        print(f"No snapshots in the last {args.hours:g}h (stream {REDIS_WORKER_HEALTH_STREAM}).")
        return

    lat_series = {t: [row.get("start_latency_s", {}).get(t, 0.0) for row in rows] for t in TYPES}
    median = {t: statistics.median(v) for t, v in lat_series.items()}

    flagged = 0
    prev = None
    print(f"{len(rows)} snapshots, {args.hours:g}h window\n")
    for row in rows:
        when = datetime.fromtimestamp(row["ts"]).strftime("%Y-%m-%d %H:%M")
        lines = []
        for t in TYPES:
            cur = row["pool"].get(t, {})
            old = (prev or {}).get("pool", {}).get(t, {})
            restart = prev is not None and (
                cur.get("total_replacements", 0) < old.get("total_replacements", 0)
                or cur.get("unstarted_kills", 0) < old.get("unstarted_kills", 0))
            kills = _delta(cur.get("unstarted_kills", 0), old.get("unstarted_kills", 0)) if prev else 0
            repl = _delta(cur.get("total_replacements", 0), old.get("total_replacements", 0)) if prev else 0
            lat = row.get("start_latency_s", {}).get(t, 0.0)
            flags = []
            if kills:
                flags.append(f"kills+{kills}")
            if repl:
                flags.append(f"repl+{repl}")
            if cur.get("pending_start", 0):
                flags.append(f"pending={cur['pending_start']}")
            if cur.get("consecutive_deaths", 0):
                flags.append(f"deaths={cur['consecutive_deaths']}")
            if median[t] and lat > args.slow_factor * median[t]:
                flags.append(f"slow={lat:g}s(median {median[t]:g}s)")
            if restart:
                flags.append("scheduler-restart")
            lines.append((t, cur, lat, flags))
        any_flag = any(f for *_x, f in lines)
        flagged += any_flag
        if args.all or any_flag:
            print(f"{when}")
            for t, cur, lat, flags in lines:
                print(f"  {t:<8} started={cur.get('started', '?')} alive={cur.get('alive', '?')} "
                      f"slowest_start={lat:g}s  {' '.join(flags) if flags else 'ok'}")
        prev = row

    print(f"\n{flagged} of {len(rows)} snapshots flagged.")


if __name__ == "__main__":
    main()
