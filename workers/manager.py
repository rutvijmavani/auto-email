"""
workers/manager.py — Autoscaler (Layer 0 + Layer 2)

Design doc: docs/scaling-redesign.md §4, §7, §16

Layer 0 (every 60s): delay + utilization formula → scale up/down/urgent per pool.
Layer 1 (midnight):  midnight recompute of worker_ceil from 28-day daily_peak history.
Layer 2 (event):     Lever 1 backpressure + deadlock detection + worker borrowing.

Worker busy_ms signal:
  Each worker's heartbeat (worker:alive:{type}_worker:*) carries a cumulative
  busy_ms odometer plus busy_since (job in progress); utilization is the
  change between snapshots (see _get_pool_busy_ms).
  Scale-up:   util > 0.80 AND delay > WARN×0.5 (2 consecutive cycles).
  Scale-down: util < 0.50 AND delay < WARN×0.25 (5 consecutive cycles).
  Urgent:     delay >= WARN×0.75 regardless of utilization (1 cycle).

Layer 2 signals:
  manager:lever1:{pool}:active  — set by manager as the LAST resort (capacity chain
                                   exhausted and lambda*s > n); read by scheduler/
                                   workers to halt inflow.
  manager:borrow:{src}:{tgt}    — ceiling seats moved from src to tgt (no TTL).
"""

import json
import math
import os
import socket
import time
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

_HOSTNAME = socket.gethostname()
_MANAGER_HB_TTL = 180  # 3× cycle length; stale after 3 missed cycles

from logger import get_logger, init_logging
from workers.redis_client import get_redis
from workers import db_capacity
from workers.queue_stamp import payload_wait_s
from workers.stream_backlog import stream_backlog, waiting_companies
from workers import company_load
from config import (
    WORKER_FLOOR,
    MONITOR_MAX_WORKERS,
    WORKER_SEAT_BUDGET,
    DB_POOL_RAISE_STEP,
    MANAGER_RECOMPUTE_HOUR,
    MANAGER_RECOMPUTE_TZ,
    POOL_CEIL_DIVISOR,
    BOOTSTRAP_BUFFER,
    MANAGER_SCALE_UP_UTIL,
    MANAGER_SCALE_DOWN_UTIL,
    WORKER_SLOW_CHECK_INTERVAL_S,
    DETAIL_QUEUE_HIGH_WATERMARK,
    DETAIL_QUEUE_ALERT_CYCLES,
    REDIS_MEMORY_ALERT_PCT,
    SCAN_DELAY_WARN_MIN_S,
    FULLSCAN_DELAY_WARN_MIN_S,
    SERVICE_TIME_WINDOW_CYCLES,
    REDIS_POLL_ADAPTIVE,
    REDIS_POLL_FULLSCAN,
    REDIS_DETAIL_ADAPTIVE,
    REDIS_DETAIL_FULLSCAN,
    REDIS_STREAM_ADAPTIVE,
    REDIS_STREAM_FULLSCAN,
    STREAM_CONSUMER_GROUP,
    WORKER_OUTAGE_TTL_S,
    WORKER_CONSEC_REDUCTIONS_THRESHOLD,
    WORKER_CONSEC_REDUCTIONS_TTL,
    CONCURRENCY_ERROR_RATE_REDUCE,
    CONCURRENCY_FLOOR,
    CONCURRENCY_FLOOR_DEFAULT,
    REDIS_CONCURRENCY_LIMIT_PREFIX,
    HEAD_CHECK_ON_DEMAND,
    HEAD_CHECK_BATCH,
    ENRICHMENT_ON_DEMAND,
    ENRICHMENT_BATCH,
    ENRICHMENT_INFLIGHT,
    ENRICHMENT_DELAYED,
    DISCOVERY_BATCH,
    DISCOVERY_REDETECT,
    DISCOVERY_INFLIGHT,
    DISCOVERY_DELAYED,
    ATS_MANAGER_SCALE_UP_THRESHOLD,
    ATS_MANAGER_IDLE_CYCLES,
    MOBILE_RELAY_QUEUE,
    MOBILE_RELAY_INFLIGHT,
    MOBILE_RELAY_PROXY_HOST,
    MOBILE_RELAY_PROXY_PORT,
    MOBILE_RELAY_PROBE_TIMEOUT_S,
    MOBILE_RELAY_WATCHER_HOST,
    MOBILE_RELAY_WATCHER_PORT,
    MOBILE_RELAY_WATCHER_TRIGGER_TIMEOUT_S,
)

logger = get_logger(__name__)

SHADOW_MODE: bool = False

# ── Cycle interval ─────────────────────────────────────────────────────────────
MANAGER_CYCLE_S: int = 60

# ── Bootstrap ceilings (used until 28 days of daily_peak data accumulates) ────
# detail raised to 10: burst arrivals (single scan can drop 300+ jobs) require
# more headroom to drain fast enough to stay below the 300s delay_warn threshold.
BOOTSTRAP_CEIL: dict = {
    "scan":     10,
    "detail":   10,
    "fullscan": 5,
}
BOOTSTRAP_DAYS_REQUIRED: int = 28

# ── Cold-start fallbacks for scaling_params (used until DB has real data) ──────
_FALLBACK_PARAMS: dict = {
    "detail":   {"fetch_p75": 1.5,   "delay_warn_s": 300},   # 300s = ~400 jobs at 2 workers; 60s was too tight, fired Lever 1 on every normal burst
    "scan":     {"fetch_p75": 120.0, "delay_warn_s": 1800},
    "fullscan": {"fetch_p75": 300.0, "delay_warn_s": 7200},
}

# ── Hysteresis cycle counters (in-memory, reset on manager restart) ────────────
_scale_up_cycles:   dict = {"scan": 0, "detail": 0, "fullscan": 0}
_scale_down_cycles: dict = {"scan": 0, "detail": 0, "fullscan": 0}
_urgent_active:     dict = {"scan": False, "detail": False, "fullscan": False}

# ── ATS pool idle-cycle counters (simple on/off logic, not Layer 0 formula) ───
_ats_idle_cycles:   dict = {"domain_enrichment": 0, "discovery": 0, "head_check": 0}

# ── Layer 2 constants ──────────────────────────────────────────────────────────
RECOVERY_STABILITY_RATIO = 0.25   # delay < WARN × this = stable during recovery
DEADLOCK_HISTORY_CYCLES  = 4      # rolling window for 3-of-4 check
DEADLOCK_RISING_MIN      = 3      # min cycles above WARN in window
LEVER1_STABLE_REQUIRED   = 3      # consecutive stable cycles to lift Lever 1
REINTRO_STABLE_REQUIRED   = 2      # consecutive stable cycles to end re-introduction
REINTRO_DETAIL_BATCH_MAX  = 3      # max detail jobs pushed per scan/fullscan cycle during re-intro

# ── Lever 1 max hold times (seconds) ──────────────────────────────────────────
# For "scan" and "fullscan" pools, Lever 1 prevents scheduler dispatch, which
# means the ZSET overdue count (and thus delay_s) GROWS under Lever 1 rather
# than recovering — the normal stability condition can never be met.  A max
# hold ensures Lever 1 auto-lifts after this duration even if delay_s hasn't
# returned to stable.  If conditions still warrant it, the manager re-fires
# within one cycle (60s) automatically.
# "detail" has no limit: its queue drains naturally under Lever 1 (workers
# pull from the list, inflow is halted, delay_s decreases organically).
# fired_at is stored in Redis (not in-memory) so the age survives restarts.
LEVER1_MAX_HOLD_S: dict = {
    "scan":     300,   # 5 min
    "fullscan": 600,   # 10 min
    "detail":   None,  # no limit — recovers naturally
}

# ── Layer 2 in-memory state (survives individual cycle failures; Redis holds ───
# persistent borrow/lever1/reintro state across manager restarts)
# _prev_depth / _prev_arrivals start EMPTY: no previous tick means inflow is
# unknown (None), never "the whole backlog arrived this cycle".
_prev_depth:           dict = {}
_prev_arrivals:        dict = {}   # pool -> (organic arrival counter, time.time())
_NO_RATE = object()                # _fire_lever1: caller gave no inflow rate
# Per-cycle inputs the main loop computes once per pool (the arrival counter and
# the service-time window both advance on every read, so they must not be read twice).
_cycle_arrival_rate:   dict = {}   # pool -> organic arrivals/s this cycle (None = unknown)
_cycle_service_s:      dict = {}   # pool -> real service time s this cycle (None = unknown)
_needed_workers:       dict = {}   # pool -> uncapped workers the formula asked for
_last_work_need:       dict = {}   # scan pools -> last need from per-company work (kept while unreadable)
_lever1_stable_cycles: dict = {"scan": 0, "detail": 0, "fullscan": 0}
_reintro_stable_cycles: dict = {"scan": 0, "detail": 0, "fullscan": 0}


# ─────────────────────────────────────────────────────────────────────────────
# Scaling params — loaded once at startup, refreshed on 25h TTL expiry
# ─────────────────────────────────────────────────────────────────────────────

def _compute_scaling_params() -> dict:
    """
    Query DB for P75 fetch durations and derived DELAY_WARN values.
    Returns a dict with the same shape as _FALLBACK_PARAMS.
    Falls back to _FALLBACK_PARAMS on any DB error.
    """
    from db.db import get_conn

    params = {pool: dict(v) for pool, v in _FALLBACK_PARAMS.items()}

    try:
        with get_conn() as conn:
            cur = conn.cursor()
            # detail: P75 across all api_health requests, last 30 days
            cur.execute("""
                SELECT PERCENTILE_CONT(0.75) WITHIN GROUP (
                    ORDER BY total_ms / NULLIF(requests_made, 0)
                ) / 1000.0 AS value
                FROM api_health
                WHERE date > NOW() - INTERVAL '30 days'
                  AND context = 'normal'
            """)
            row = cur.fetchone()
            if row and row["value"] is not None:
                params["detail"]["fetch_p75"] = float(row["value"])

            # scan: P75 of per-company avg_scan_duration_s
            cur.execute("""
                SELECT PERCENTILE_CONT(0.75) WITHIN GROUP (
                    ORDER BY avg_scan_duration_s
                ) AS value
                FROM company_poll_stats
                WHERE avg_scan_duration_s IS NOT NULL
            """)
            row = cur.fetchone()
            if row and row["value"] is not None:
                params["scan"]["fetch_p75"] = float(row["value"])

            # fullscan: P75 of per-company avg_fullscan_duration_s
            cur.execute("""
                SELECT PERCENTILE_CONT(0.75) WITHIN GROUP (
                    ORDER BY avg_fullscan_duration_s
                ) AS value
                FROM company_poll_stats
                WHERE avg_fullscan_duration_s IS NOT NULL
            """)
            row = cur.fetchone()
            if row and row["value"] is not None:
                params["fullscan"]["fetch_p75"] = float(row["value"])

            # scan DELAY_WARN_S: p10 of current_interval_s × 0.10
            cur.execute("""
                SELECT PERCENTILE_CONT(0.1) WITHIN GROUP (
                    ORDER BY current_interval_s
                ) * 0.10 AS value
                FROM company_poll_stats
                WHERE current_interval_s IS NOT NULL
            """)
            row = cur.fetchone()
            if row and row["value"] is not None:
                params["scan"]["delay_warn_s"] = max(SCAN_DELAY_WARN_MIN_S, int(row["value"]))

            # fullscan DELAY_WARN_S: avg(full_scan_interval_s) × 0.10
            cur.execute("""
                SELECT AVG(full_scan_interval_s) * 0.10 AS value
                FROM company_poll_stats
                WHERE full_scan_interval_s IS NOT NULL
            """)
            row = cur.fetchone()
            if row and row["value"] is not None:
                params["fullscan"]["delay_warn_s"] = max(FULLSCAN_DELAY_WARN_MIN_S, int(row["value"]))

        params["computed_at"] = datetime.now(timezone.utc).isoformat()
        logger.info(
            "manager: scaling_params computed from DB: %s",
            json.dumps({p: {k: round(v, 2) if isinstance(v, float) else v
                             for k, v in d.items()}
                        for p, d in params.items() if p != "computed_at"}),
        )
    except Exception as exc:
        logger.warning(
            "manager: DB scaling_params query failed (%s) — using fallbacks", exc,
        )
        params["computed_at"] = "fallback"

    return params


def _load_scaling_params(r) -> dict:
    """
    Load scaling_params from Redis cache, or compute and cache them.
    Called once at startup; returned dict is held in memory for the session.
    """
    raw = r.get("manager:scaling_params")
    if raw:
        try:
            return json.loads(raw)
        except Exception:
            pass

    params = _compute_scaling_params()
    try:
        r.set("manager:scaling_params", json.dumps(params), ex=25 * 3600)
    except Exception as exc:
        logger.warning("manager: failed to cache scaling_params: %s", exc)
    return params


# ─────────────────────────────────────────────────────────────────────────────
# Current pool sizes (from scheduler:health)
# ─────────────────────────────────────────────────────────────────────────────

def _get_pool_sizes(r) -> dict:
    """
    Read current worker counts from scheduler:health.
    Returns {"scan": n, "detail": n, "fullscan": n}.
    Falls back to 0 if scheduler:health is missing or stale.
    """
    raw = r.get("scheduler:health")
    if not raw:
        return {"scan": 0, "detail": 0, "fullscan": 0}
    try:
        data = json.loads(raw)
        pool = data.get("pool", {})
        return {
            "scan":     pool.get("scan",     {}).get("alive", 0),
            "detail":   pool.get("detail",   {}).get("alive", 0),
            "fullscan": pool.get("fullscan", {}).get("alive", 0),
        }
    except Exception as exc:
        logger.warning("manager: failed to parse scheduler:health: %s", exc)
        return {"scan": 0, "detail": 0, "fullscan": 0}


# ─────────────────────────────────────────────────────────────────────────────
# Pool busy_ms — utilization signal
# ─────────────────────────────────────────────────────────────────────────────

# (pool, heartbeat key) -> (snapshot ts, busy total ms, last busy fraction)
_prev_busy_snap: dict = {}


def _get_pool_busy_ms(r, pool: str) -> int:
    """
    Pool busy time, in ms per manager cycle, derived from worker heartbeats.

    Each heartbeat (worker:alive:{pool}_worker:*) carries busy_ms (cumulative
    job time, grows when a job ends) and busy_since (start of the job in
    progress, null when idle).  A worker's busy total at a snapshot is
    busy_ms + (ts - busy_since if busy), so a long scan counts while it runs.

    A worker's busy fraction is the change in that total between the snapshot
    seen at the previous call and the current one, divided by the time between
    the two snapshots' own ts — never the manager's clock, so a job that ended
    before the snapshot is not counted past its end.  A worker seen for the
    first time contributes nothing; a worker whose key has expired (dead) is
    dropped.  If no new beat arrived since the last call the previous fraction
    is reused.

    Returns sum(fractions) * MANAGER_CYCLE_S * 1000, the same unit
    _run_pool_cycle divides by n_workers * MANAGER_CYCLE_S * 1000.
    """
    seen = set()
    fraction_sum = 0.0
    for key in r.scan_iter(match=f"worker:alive:{pool}_worker:*", count=50):
        raw = r.get(key)
        if not raw:
            continue
        try:
            hb = json.loads(raw)
            ts, busy_total = float(hb["ts"]), float(hb["busy_ms"])
            since = hb.get("busy_since")
            if since is not None:
                busy_total += max(ts - float(since), 0.0) * 1000.0
        except (ValueError, TypeError, KeyError):
            continue   # old-format heartbeat (no busy fields) or corrupt
        snap_key = (pool, key.decode() if isinstance(key, bytes) else key)
        seen.add(snap_key)
        prev = _prev_busy_snap.get(snap_key)
        if prev is None:
            _prev_busy_snap[snap_key] = (ts, busy_total, None)
            continue
        prev_ts, prev_total, prev_frac = prev
        if ts > prev_ts:
            frac = min(max((busy_total - prev_total) / ((ts - prev_ts) * 1000.0), 0.0), 1.0)
            _prev_busy_snap[snap_key] = (ts, busy_total, frac)
        else:
            frac = prev_frac
        if frac is not None:
            fraction_sum += frac
    for snap_key in [k for k in _prev_busy_snap if k[0] == pool and k not in seen]:
        del _prev_busy_snap[snap_key]
    return int(fraction_sum * MANAGER_CYCLE_S * 1000)


# ─────────────────────────────────────────────────────────────────────────────
# Real per-job service time s (replaces the single-HTTP-request fetch_p75)
# ─────────────────────────────────────────────────────────────────────────────

# (pool, heartbeat key) -> (busy total ms, processed count) at the previous call
_prev_svc_snap: dict = {}
# pool -> last SERVICE_TIME_WINDOW_CYCLES of (busy seconds, jobs finished)
_svc_window: dict = {}


def _get_pool_service_time(r, pool: str) -> "float | None":
    """
    Seconds of worker time per finished job, summed over the last
    SERVICE_TIME_WINDOW_CYCLES calls: sum(busy delta) / sum(processed delta)
    across the pool's heartbeats (busy_ms odometer, in-progress job counted;
    processed = jobs finished).  This is the service time the pool really
    pays per job, including the DB and queue work around the fetch.

    None while unknown: first call, no job finished inside the window, or no
    heartbeat carries both fields.  Callers then fall back to fetch_p75.
    A worker seen for the first time, or whose counters went backwards
    (restart), contributes nothing for that call.
    """
    seen = set()
    busy_s = 0.0
    done = 0
    try:
        for key in r.scan_iter(match=f"worker:alive:{pool}_worker:*", count=50):
            raw = r.get(key)
            if not raw:
                continue
            try:
                hb = json.loads(raw)
                busy_total = float(hb["busy_ms"])
                since = hb.get("busy_since")
                if since is not None:
                    busy_total += max(float(hb["ts"]) - float(since), 0.0) * 1000.0
                processed = int(hb["processed"])
            except (ValueError, TypeError, KeyError):
                continue
            snap_key = (pool, key.decode() if isinstance(key, bytes) else key)
            seen.add(snap_key)
            prev = _prev_svc_snap.get(snap_key)
            _prev_svc_snap[snap_key] = (busy_total, processed)
            if prev is None or busy_total < prev[0] or processed < prev[1]:
                continue
            busy_s += (busy_total - prev[0]) / 1000.0
            done += processed - prev[1]
    except Exception as exc:
        logger.debug("manager: service time read failed [%s]: %s", pool, exc)
        return None
    for snap_key in [k for k in _prev_svc_snap if k[0] == pool and k not in seen]:
        del _prev_svc_snap[snap_key]
    window = _svc_window.setdefault(pool, [])
    window.append((busy_s, done))
    del window[:-max(SERVICE_TIME_WINDOW_CYCLES, 1)]
    total_done = sum(d for _, d in window)
    if total_done <= 0:
        return None
    return sum(b for b, _ in window) / total_done


# ─────────────────────────────────────────────────────────────────────────────
# Queue metrics
# ─────────────────────────────────────────────────────────────────────────────

def _dispatcher_uptime_s(r, loop: str, now: float, default: float) -> float:
    """
    Seconds the scheduler's dispatch loop has been running unbroken (heartbeat
    up_since, workers/scheduler.py _write_scheduler_heartbeat).  A poll-ZSET score
    older than that is lateness the dispatcher could not have acted on (outage,
    restart), so it is not delay.  Heartbeat key missing = loop down = 0.  An
    unreadable or pre-up_since heartbeat returns default (no cap, the old signal).
    """
    try:
        raw = r.get(f"worker:alive:scheduler:{loop}")
    except Exception as exc:
        logger.warning("manager: scheduler %s heartbeat unreadable: %s", loop, exc)
        return default
    if raw is None:
        return 0.0
    try:
        return max(0.0, now - float(json.loads(raw)["up_since"]))
    except (ValueError, TypeError, KeyError):
        return default


def _get_scan_pool_metrics(r, zset_key: str, stream_key: str, now: float,
                           loop: str) -> dict:
    """
    Backlog of a scan-type pool = work waiting for a worker in either place:
      overdue ZSET   companies due but not yet dispatched (age = score lateness,
                     capped at the dispatcher's unbroken uptime)
      stream         companies dispatched, no worker has taken them (age = time
                     since XADD; see workers/stream_backlog.py)
    depth = overdue + waiting; delay = the older of the two ages.  The parts are
    returned separately for display.  An unreadable stream is reported as
    stream_waiting=None and the pool falls back to the ZSET alone (the old signal),
    never to a fake zero.
    """
    overdue = r.zcount(zset_key, "-inf", now)
    zset_delay = 0.0
    most_overdue = r.zrange(zset_key, 0, 0, withscores=True)
    if most_overdue and most_overdue[0][1] < now:
        zset_delay = max(0.0, now - most_overdue[0][1])
        zset_delay = min(zset_delay, _dispatcher_uptime_s(r, loop, now, zset_delay))

    try:
        waiting, stream_age = stream_backlog(r, stream_key, STREAM_CONSUMER_GROUP, now)
    except Exception as exc:
        logger.warning("manager: stream backlog unreadable for %s: %s", stream_key, exc)
        waiting, stream_age = None, None

    # Names of every late company, no cap: the sizing formula weighs each by its
    # own duration (workers/company_load.py).  None = unreadable (unknown, not
    # empty).  A company in both places counts once.
    late_names = None
    try:
        names = set(r.zrangebyscore(zset_key, "-inf", now))
        if waiting is not None:
            names.update(waiting_companies(r, stream_key, STREAM_CONSUMER_GROUP))
            late_names = sorted(names)
    except Exception as exc:
        logger.warning("manager: late company names unreadable for %s: %s", zset_key, exc)

    return {
        "depth":          overdue + (waiting or 0),
        "delay_s":        max(zset_delay, stream_age or 0.0),
        "overdue":        overdue,
        "zset_delay_s":   zset_delay,
        "stream_waiting": waiting,
        "stream_age_s":   stream_age,
        "late_names":     late_names,
    }


_SCAN_TYPE_POOLS = ("scan", "fullscan")

# Health alerts (moved here from the removed scheduler slow-throughput loop, which
# nothing ever started).  Run every WORKER_SLOW_CHECK_INTERVAL_S, so the detail
# queue alert still means DETAIL_QUEUE_ALERT_CYCLES x that interval.
_last_health_check_ts: float = 0.0
_detail_alert_streak:  int   = 0


def _check_health_alerts(r, now: float) -> None:
    """
    Redis memory alert (CRITICAL at REDIS_MEMORY_ALERT_PCT of maxmemory; under
    noeviction a full Redis rejects writes and queue entries are lost) and detail
    queue depth alert (WARNING once detail:adaptive stays above
    DETAIL_QUEUE_HIGH_WATERMARK for DETAIL_QUEUE_ALERT_CYCLES checks).  Throttled
    to one check per WORKER_SLOW_CHECK_INTERVAL_S; create_alert dedups repeats.
    """
    global _last_health_check_ts, _detail_alert_streak
    if now - _last_health_check_ts < WORKER_SLOW_CHECK_INTERVAL_S:
        return
    _last_health_check_ts = now

    from db.pipeline_alerts import (
        create_alert, ALERT_REDIS_MEMORY, ALERT_DETAIL_QUEUE_DEPTH, CRITICAL, WARNING,
    )

    try:
        mem_info = r.info("memory")
        used_mem = mem_info.get("used_memory", 0)
        max_mem  = mem_info.get("maxmemory", 0)
        if max_mem and max_mem > 0:        # maxmemory=0 means no limit configured
            used_pct = int(100 * used_mem / max_mem)
            if used_pct >= REDIS_MEMORY_ALERT_PCT:
                alert_id = create_alert(
                    alert_type = ALERT_REDIS_MEMORY,
                    severity   = CRITICAL,
                    value      = float(used_pct),
                    threshold  = float(REDIS_MEMORY_ALERT_PCT),
                    message    = (
                        f"Redis memory at {used_pct}% of maxmemory "
                        f"({used_mem // 1024 // 1024} MB / {max_mem // 1024 // 1024} MB) — "
                        f"noeviction policy will block writes at 100%"
                    ),
                )
                if alert_id:
                    logger.critical(
                        "manager: REDIS MEMORY alert used=%dMB max=%dMB pct=%d%% (alert_id=%s)",
                        used_mem // 1024 // 1024, max_mem // 1024 // 1024, used_pct, alert_id,
                    )
    except Exception as exc:
        logger.error("manager: redis memory check failed: %s", exc)

    try:
        detail_depth = r.llen(REDIS_DETAIL_ADAPTIVE)
        if detail_depth > DETAIL_QUEUE_HIGH_WATERMARK:
            _detail_alert_streak += 1
            if _detail_alert_streak >= DETAIL_QUEUE_ALERT_CYCLES:
                alert_id = create_alert(
                    alert_type = ALERT_DETAIL_QUEUE_DEPTH,
                    severity   = WARNING,
                    value      = float(detail_depth),
                    threshold  = float(DETAIL_QUEUE_HIGH_WATERMARK),
                    message    = (
                        f"detail:adaptive queue depth {detail_depth} has exceeded "
                        f"watermark {DETAIL_QUEUE_HIGH_WATERMARK} for "
                        f"{_detail_alert_streak} consecutive checks "
                        f"({_detail_alert_streak * WORKER_SLOW_CHECK_INTERVAL_S // 60} min)"
                    ),
                )
                if alert_id:
                    logger.warning(
                        "manager: DETAIL QUEUE DEPTH alert depth=%d watermark=%d "
                        "checks=%d (alert_id=%s)",
                        detail_depth, DETAIL_QUEUE_HIGH_WATERMARK,
                        _detail_alert_streak, alert_id,
                    )
        else:
            _detail_alert_streak = 0
    except Exception as exc:
        logger.error("manager: detail queue alert check failed: %s", exc)


def _scan_pool_work(r, pool: str, metrics: dict, params: dict) -> "dict | None":
    """
    Workers a scan-type pool needs, from per-company work (company_load.py):

      backlog = sum over late companies of min(d / H, 1)     H = delay_warn_s
      steady  = (sum over all companies of d / (d + T)) / MANAGER_SCALE_UP_UTIL

    d is a company's averaged duration (fetch_p75 when it has no entry), T its
    interval.  Returns None when the late list or the table cannot be read; the
    caller then keeps the last need, never a zero.
    """
    names = metrics.get("late_names")
    if names is None:
        return None
    fetch_p75    = params.get("fetch_p75",    _FALLBACK_PARAMS[pool]["fetch_p75"])
    delay_warn_s = params.get("delay_warn_s", _FALLBACK_PARAMS[pool]["delay_warn_s"])
    try:
        steady_load = company_load.steady_load(r, pool, fetch_p75)
        if steady_load is None:
            return None
        durations = company_load.durations(r, pool, names, fetch_p75)
    except Exception as exc:
        logger.error("manager [%s]: company load unreadable: %s", pool, exc)
        return None
    return {
        "backlog": company_load.backlog_term(durations, delay_warn_s),
        "steady":  steady_load / MANAGER_SCALE_UP_UTIL,
        "late":    len(names),
    }


def _get_queue_metrics(r) -> dict:
    """
    Return queue_depth and actual_delay_s for each pool.

    detail:   queue_depth = LLEN(adaptive) + LLEN(fullscan)
              actual_delay = now - enqueued_at of LINDEX -1 (oldest item)
              enqueued_at is stored as a timestamp in the payload JSON under "enqueued_at"

    scan:     queue_depth = overdue ZSET + undelivered stream entries
              actual_delay = older of the most-overdue score and the oldest
              undelivered stream entry (_get_scan_pool_metrics)

    fullscan: same pattern on poll:fullscan + stream:fullscan
    """
    now = time.time()
    metrics: dict = {}

    # ── detail ────────────────────────────────────────────────────────────────
    try:
        depth_adaptive = r.llen(REDIS_DETAIL_ADAPTIVE)
        depth_fullscan = r.llen(REDIS_DETAIL_FULLSCAN)
        detail_depth = depth_adaptive + depth_fullscan

        # Oldest item is at index -1 (consuming side).  Wait is measured from
        # queued_at (push time), so a rebuilt or requeued backlog is not aged
        # by its build time.  Each queue is read separately; None = unknown.
        adaptive_raw = r.lindex(REDIS_DETAIL_ADAPTIVE, -1)
        fullscan_raw = r.lindex(REDIS_DETAIL_FULLSCAN, -1)
        adaptive_age = payload_wait_s(adaptive_raw, now)
        fullscan_age = payload_wait_s(fullscan_raw, now)

        # Workers serve the older of the two tails first (detail_worker
        # _pop_with_inflight), so the delay is the age of the older tail; an
        # unknown age still reads as 0.0 here.
        detail_delay = max(adaptive_age or 0.0, fullscan_age or 0.0)

        metrics["detail"] = {
            "depth": detail_depth, "delay_s": detail_delay,
            "adaptive_age_s": adaptive_age, "fullscan_age_s": fullscan_age,
        }
    except Exception as exc:
        logger.warning("manager: detail queue metrics failed: %s", exc)
        metrics["detail"] = {"depth": 0, "delay_s": 0.0}

    # ── scan / fullscan ───────────────────────────────────────────────────────
    for pool, zset_key, stream_key, loop in (
        ("scan",     REDIS_POLL_ADAPTIVE, REDIS_STREAM_ADAPTIVE, "adaptive"),
        ("fullscan", REDIS_POLL_FULLSCAN, REDIS_STREAM_FULLSCAN, "fullscan"),
    ):
        try:
            metrics[pool] = _get_scan_pool_metrics(r, zset_key, stream_key, now, loop)
        except Exception as exc:
            logger.warning("manager: %s queue metrics failed: %s", pool, exc)
            metrics[pool] = {"depth": 0, "delay_s": 0.0}

    # ── head_check + enrichment + discovery (autoscaled by _run_ats_pool_cycle) ─
    try:
        # head_check: two LISTs (on_demand + batch) + per-instance inflight LISTs.
        # Scan all head_check:inflight:instance:* keys so that items from a crashed worker
        # (which were already BLPOPed from the source queues) still count toward depth and
        # keep the manager from incorrectly treating the pool as idle.
        _hc_inflight = sum(
            r.llen(k)
            for k in set(r.scan_iter("head_check:inflight:instance:*", count=50))
        )
        head_check_depth = r.llen(HEAD_CHECK_ON_DEMAND) + r.llen(HEAD_CHECK_BATCH) + _hc_inflight
        metrics["head_check"] = {"depth": head_check_depth, "delay_s": 0.0, "depth_known": True}
    except Exception as exc:
        logger.warning("manager: head_check queue metrics failed: %s", exc)
        metrics["head_check"] = {"depth": 0, "delay_s": 0.0, "depth_known": False}

    try:
        # enrichment: on_demand LIST + batch ZSET + inflight ZSET(s) + delayed ZSET
        # Include inflight so workers are not stopped while actively processing items
        # (items move from queue → inflight atomically, leaving queues temporarily empty).
        # Include delayed so the pool isn't scaled to zero while certspotter-429 /
        # processing-error retries are waiting on their not_before timestamp — the
        # worker loop itself blocks on ENRICHMENT_DELAYED rather than exiting, so the
        # autoscaler must see it too or it will shut down the only worker left to drain it.
        _enrich_inflight = sum(r.zcard(k) for k in set(r.scan_iter(f"{ENRICHMENT_INFLIGHT}*", count=10)))
        enrich_depth = (r.llen(ENRICHMENT_ON_DEMAND) + r.zcard(ENRICHMENT_BATCH)
                        + _enrich_inflight + r.zcard(ENRICHMENT_DELAYED))
        metrics["domain_enrichment"] = {"depth": enrich_depth, "delay_s": 0.0, "depth_known": True}
    except Exception as exc:
        logger.warning("manager: enrichment queue metrics failed: %s", exc)
        metrics["domain_enrichment"] = {"depth": 0, "delay_s": 0.0, "depth_known": False}

    try:
        # discovery: redetect ZSET + batch ZSET + inflight ZSET(s) + delayed ZSET
        # (delayed included for the same reason as enrichment above — KG quota
        # exhaustion retries must not let the pool scale to zero.)
        _discov_inflight = sum(r.zcard(k) for k in set(r.scan_iter(f"{DISCOVERY_INFLIGHT}*", count=10)))
        discovery_depth = (r.zcard(DISCOVERY_REDETECT) + r.zcard(DISCOVERY_BATCH)
                           + _discov_inflight + r.zcard(DISCOVERY_DELAYED))
        metrics["discovery"] = {"depth": discovery_depth, "delay_s": 0.0, "depth_known": True}
    except Exception as exc:
        logger.warning("manager: discovery queue metrics failed: %s", exc)
        metrics["discovery"] = {"depth": 0, "delay_s": 0.0, "depth_known": False}

    return metrics


# ─────────────────────────────────────────────────────────────────────────────
# Worker ceiling (bootstrap or midnight recompute)
# ─────────────────────────────────────────────────────────────────────────────

def _get_base_ceil(r, pool: str) -> int:
    """
    Read manager:worker_ceil:{pool} from Redis (set by the nightly recompute or
    the emergency recalc).  Falls back to BOOTSTRAP_CEIL until it exists.
    This is the pool's own ceiling, before any Layer 2 borrow.
    """
    raw = r.get(f"manager:worker_ceil:{pool}")
    if raw:
        try:
            return max(WORKER_FLOOR, int(raw))
        except (ValueError, TypeError):
            pass
    return BOOTSTRAP_CEIL.get(pool, WORKER_FLOOR)


def _get_worker_ceil(r, pool: str) -> int:
    """
    The ceiling Layer 0 works under: the pool's own ceiling minus the seats it
    lent plus the seats it borrowed (manager:borrow:* keys).  The manager is the
    only owner of this number; the scheduler applies it without a second clamp.
    """
    return max(
        WORKER_FLOOR,
        _get_base_ceil(r, pool) - _borrow_out(r, pool) + _borrow_in(r, pool),
    )


def _count_daily_peak_records(r, pool: str) -> int:
    """Count how many daily_peak:{YYYY-MM-DD} records exist for this pool."""
    cursor = 0
    count = 0
    while True:
        cursor, keys = r.scan(
            cursor,
            match=f"manager:pool:{pool}:daily_peak:20*",
            count=50,
        )
        count += len(keys)
        if cursor == 0:
            break
    return count


_POOL_PRIORITY: tuple = ("detail", "scan", "fullscan")   # shortage / spare priority


def _decode(val):
    """Redis value as str (None stays None); the client may return bytes."""
    return val.decode() if isinstance(val, bytes) else val


def _recompute_date() -> str:
    """Local (MANAGER_RECOMPUTE_TZ) calendar date the nightly record is labelled with."""
    return datetime.now(ZoneInfo(MANAGER_RECOMPUTE_TZ)).strftime("%Y-%m-%d")


def _recompute_due(r) -> bool:
    """
    True once per local day, from MANAGER_RECOMPUTE_HOUR on, until the date is
    stored in manager:last_recompute_date (no TTL, so a manager restart inside
    the window neither repeats nor skips the recompute).
    """
    now = datetime.now(ZoneInfo(MANAGER_RECOMPUTE_TZ))
    if now.hour < MANAGER_RECOMPUTE_HOUR:
        return False
    return _decode(r.get("manager:last_recompute_date")) != now.strftime("%Y-%m-%d")


def _get_db_total(r) -> int:
    """Worker seats in force: WORKER_SEAT_BUDGET, or the value the manager raised it to."""
    raw = r.get("manager:db_pool_maxconn")
    try:
        return max(WORKER_SEAT_BUDGET, int(raw)) if raw else WORKER_SEAT_BUDGET
    except (ValueError, TypeError):
        return WORKER_SEAT_BUDGET


_capacity_cache: dict = {"at": 0.0, "probe": None}


def _db_capacity(r) -> "dict | None":
    """Live Postgres capacity (workers/db_capacity.probe), re-read at most once per cycle."""
    now = time.monotonic()
    if now - _capacity_cache["at"] >= MANAGER_CYCLE_S:
        _capacity_cache["probe"] = db_capacity.probe(r)
        _capacity_cache["at"] = now
        p = _capacity_cache["probe"]
        if p:
            logger.info(
                "manager: db capacity max_connections=%d reserved=%d margin=%d used=%d "
                "(workers=%d non-worker=%d) per_worker=%d → seat cap %d",
                p["max_connections"], p["reserved"], p["margin"], p["used"],
                p["worker_conns"], p["non_worker"], p["per_worker"], p["seat_cap"],
            )
    return _capacity_cache["probe"]


def _seat_cap(r) -> int:
    """
    Most worker seats Postgres can host now.  When Postgres cannot be queried the
    current budget stands: the manager stops raising instead of guessing.
    """
    p = _db_capacity(r)
    return p["seat_cap"] if p else _get_db_total(r)


def _raise_step_seats(r) -> int:
    """DB_POOL_RAISE_STEP connections expressed in worker seats (at least one)."""
    p = _db_capacity(r)
    return max(1, DB_POOL_RAISE_STEP // p["per_worker"]) if p else 0


def _pool_soft_ceiling(total: int, pool: str) -> int:
    """Phase 2 first-pass ceiling of a pool: its share of the worker seats."""
    return max(WORKER_FLOOR, total // POOL_CEIL_DIVISOR.get(pool, 5))


def _load_daily_peaks(r, pool: str, include_today: bool) -> list:
    """
    The 28 most recent daily_peak records as (date, value), newest first.
    include_today merges today's running peak into today's record (emergency
    recalc); the nightly run has already written that record.
    """
    daily_peaks: dict = {}
    cursor = 0
    while True:
        cursor, keys = r.scan(
            cursor, match=f"manager:pool:{pool}:daily_peak:20*", count=50,
        )
        for k in keys:
            val = r.get(k)
            if val:
                try:
                    daily_peaks[_decode(k).rsplit(":", 1)[-1]] = int(val)
                except (ValueError, TypeError):
                    pass
        if cursor == 0:
            break
    if include_today:
        today = _recompute_date()
        running = int(r.get(f"manager:pool:{pool}:daily_peak:running") or 0)
        daily_peaks[today] = max(daily_peaks.get(today, 0), running)
    return sorted(daily_peaks.items(), reverse=True)[:28]


def _pool_ideal(r, pool: str, include_today: bool = False) -> tuple:
    """
    Uncapped Layer 1 target of one pool: (ideal, peak_nd, buffer, bootstrap).
    ideal = peak_Nd + max(growth_buffer, volatility_buffer) once 28 days of
    records exist; BOOTSTRAP_CEIL (peak = ideal - BOOTSTRAP_BUFFER) before.
    """
    import statistics

    daily_peaks = _load_daily_peaks(r, pool, include_today)
    if len(daily_peaks) < BOOTSTRAP_DAYS_REQUIRED:
        ideal = BOOTSTRAP_CEIL.get(pool, WORKER_FLOOR)
        return ideal, max(WORKER_FLOOR, ideal - BOOTSTRAP_BUFFER), BOOTSTRAP_BUFFER, True

    peaks = [v for _, v in daily_peaks]
    peak_nd = max(peaks)

    # growth_buffer: recent 7-day peak vs the older 21-day baseline (days 8-28).
    recent_7 = peaks[:7]
    baseline = peaks[7:]
    growth_buffer = 0
    if len(recent_7) >= 7 and baseline and max(baseline) > 0:
        weekly_growth_rate = max(0.0, (max(recent_7) - max(baseline)) / max(baseline) / 3)
        growth_buffer = math.ceil(peak_nd * weekly_growth_rate)

    volatility_buffer = math.ceil(statistics.stdev(peaks)) if len(peaks) >= 3 else 0
    buffer_ = max(growth_buffer, volatility_buffer)
    logger.info(
        "manager: ceiling input [%s] peak_28d=%d growth_buf=%d vol_buf=%d",
        pool, peak_nd, growth_buffer, volatility_buffer,
    )
    return max(WORKER_FLOOR, peak_nd + buffer_), peak_nd, buffer_, False


def _plan_ceilings(r, pools: list[str], include_today: bool = False) -> dict:
    """
    Phases 1-4 of the structural plan (docs/scaling-redesign.md, "When ceiling
    blocks the ideal target").  Returns {"total": int, "pools": {pool: {...}}}.

      1. ideal(pool)  = peak_Nd + buffer, no ceiling applied
      2. actual(pool) = min(ideal, soft ceiling)
      3. spare        = total - reserved - sum(actual)
      4. shortages are filled from spare in priority order detail, scan, fullscan
    A pool still short after that raises the total by the raise step (seats)
    toward the live Postgres seat cap and the phases run again; at the cap the
    remaining shortage is returned (the caller alerts).  Pools still in bootstrap never
    raise the total: their ideal is a fixed start value, not measured demand.
    """
    ideals = {p: _pool_ideal(r, p, include_today) for p in pools}
    total = _get_db_total(r)

    if any(v[3] for v in ideals.values()):
        # Bootstrap: the fixed starting ceilings stand as configured; they are
        # not measured demand, so no soft-ceiling clamp, spare fill or raise.
        return {
            "total": total,
            "pools": {
                p: {"ceil": ideals[p][0], "ideal": ideals[p][0],
                    "peak_nd": ideals[p][1], "buffer": ideals[p][2], "short": 0}
                for p in pools
            },
        }

    cap = _seat_cap(r)
    step = _raise_step_seats(r)
    while True:
        actual = {
            p: max(WORKER_FLOOR, min(ideals[p][0], _pool_soft_ceiling(total, p)))
            for p in pools
        }
        spare = total - sum(actual.values())
        for p in _POOL_PRIORITY:
            if p not in pools:
                continue
            grant = min(max(0, ideals[p][0] - actual[p]), max(0, spare))
            actual[p] += grant
            spare -= grant
        short = {p: ideals[p][0] - actual[p] for p in pools if ideals[p][0] > actual[p]}
        if not short or step == 0 or total >= cap:
            break
        total = min(total + step, cap)

    return {
        "total": total,
        "pools": {
            p: {
                "ceil": actual[p], "ideal": ideals[p][0], "peak_nd": ideals[p][1],
                "buffer": ideals[p][2], "short": short.get(p, 0),
            }
            for p in pools
        },
    }


def _store_plan(r, plan: dict, only: "str | None" = None) -> None:
    """Persist a ceiling plan: total, then ceil / peak_nd / buffer of each pool (or `only`)."""
    if plan["total"] > _get_db_total(r):
        r.set("manager:db_pool_maxconn", plan["total"])
        logger.warning(
            "manager: capacity overflow — worker seats raised to %d "
            "(start=%d, Postgres seat cap=%d)",
            plan["total"], WORKER_SEAT_BUDGET, _seat_cap(r),
        )
    for pool, v in plan["pools"].items():
        if only is not None and pool != only:
            continue
        r.set(f"manager:worker_ceil:{pool}", v["ceil"])
        r.set(f"manager:pool:{pool}:peak_nd", v["peak_nd"])
        r.set(f"manager:pool:{pool}:buffer", v["buffer"])
        if v["short"] > 0:
            logger.error(
                "manager: [Pipeline] pool structural shortage — %s ideal=%d "
                "assigned=%d short=%d (seats=%d, Postgres seat cap=%d)",
                pool, v["ideal"], v["ceil"], v["short"], plan["total"], _seat_cap(r),
            )


def _midnight_recompute(r, pools: list[str]) -> None:
    """
    Layer 1 nightly recompute (MANAGER_RECOMPUTE_HOUR local time).

    1. Writes each pool's daily_peak record for the local date as
       max(existing, running peak); a missing running peak writes nothing.
    2. Plans the ceilings (_plan_ceilings: ideal, soft ceiling, Phase 4 spare
       fill, capacity overflow) and stores them.
    3. Resets manager:pool:{pool}:daily_peak:running to 0.
    """
    today = _recompute_date()

    for pool in pools:
        try:
            running_raw = r.get(f"manager:pool:{pool}:daily_peak:running")
            if running_raw is not None:
                key = f"manager:pool:{pool}:daily_peak:{today}"
                value = max(int(running_raw), int(r.get(key) or 0), WORKER_FLOOR)
                r.set(key, value)
            r.set(f"manager:pool:{pool}:daily_peak:running", 0)
        except Exception as exc:
            logger.error(
                "manager: nightly peak record [%s] failed: %s", pool, exc, exc_info=True,
            )

    # Per-company sizing tables: rebuilt from the DB so the event-built copy cannot
    # drift.  A failed rebuild keeps the old table (RENAME never ran) and its TTL.
    try:
        params_now = _load_scaling_params(r)
    except Exception as exc:
        logger.error("manager: nightly company load: scaling params unreadable: %s", exc)
        params_now = {}
    for pool in _SCAN_TYPE_POOLS:
        try:
            default_d = params_now.get(pool, _FALLBACK_PARAMS[pool])["fetch_p75"]
            company_load.rebuild(r, pool, default_d)
        except Exception as exc:
            logger.error(
                "manager: nightly company load rebuild [%s] failed: %s", pool, exc,
                exc_info=True,
            )

    try:
        plan = _plan_ceilings(r, pools)
        _store_plan(r, plan)
        for pool, v in plan["pools"].items():
            logger.info(
                "manager: nightly recompute [%s] ideal=%d peak=%d buffer=%d → ceil=%d "
                "(budget=%d)",
                pool, v["ideal"], v["peak_nd"], v["buffer"], v["ceil"], plan["total"],
            )
    except Exception as exc:
        logger.error("manager: nightly recompute failed: %s", exc, exc_info=True)


def _emergency_recalc(r, pool: str, pools: list[str]) -> bool:
    """
    Emergency Layer 1 recalculation, run when a pool is at its ceiling and delay
    is rising: the nightly formula again, with today's observed peak included.
    Raises only this pool's ceiling, and only by spare slots (or by a budget
    raise toward the Postgres seat cap).  Returns True if the ceiling changed.
    """
    plan = _plan_ceilings(r, pools, include_today=True)
    if plan["total"] > _get_db_total(r):
        r.set("manager:db_pool_maxconn", plan["total"])
    base = _get_base_ceil(r, pool)
    spare = plan["total"] - sum(_get_base_ceil(r, p) for p in pools)
    new_ceil = min(plan["pools"][pool]["ceil"], base + max(0, spare))
    if new_ceil <= base:
        return False
    r.set(f"manager:worker_ceil:{pool}", new_ceil)
    r.set(f"manager:pool:{pool}:peak_nd", plan["pools"][pool]["peak_nd"])
    r.set(f"manager:pool:{pool}:buffer", plan["pools"][pool]["buffer"])
    logger.warning(
        "manager [%s]: emergency recalc — ceiling %d → %d (today's peak included)",
        pool, base, new_ceil,
    )
    return True


# ─────────────────────────────────────────────────────────────────────────────
# Daily peak tracking (intraday)
# ─────────────────────────────────────────────────────────────────────────────

def _update_daily_peak_running(r, pool: str, n_workers: int) -> None:
    """
    Raise the running daily peak to `n_workers`.  The caller passes the workers
    the pool needed this cycle (before the ceiling clamp, capped at what the DB
    budget can host), so a surge is recorded at its real size, not the ceiling.
    """
    key = f"manager:pool:{pool}:daily_peak:running"
    try:
        current_raw = r.get(key)
        current = int(current_raw or 0)
        if n_workers > current:
            r.set(key, n_workers)
    except Exception as exc:
        logger.debug("manager: daily_peak:running update failed [%s]: %s", pool, exc)


# ─────────────────────────────────────────────────────────────────────────────
# pending_spawns — workers-in-flight counter (avoids double-spawning)
# ─────────────────────────────────────────────────────────────────────────────

def _incr_pending_spawns(r, pool: str, count: int) -> None:
    """
    Increment the pending_spawns counter when a spawn command is sent.

    Workers decrement this on startup.  TTL=90s auto-expires stale counts
    (deadlock 9: worker failed to start before the window closes — next cycle
    treats deficit correctly without the stuck counter blocking a respawn).
    """
    if SHADOW_MODE or count <= 0:
        return
    key = f"manager:pool:{pool}:pending_spawns"
    r.incrby(key, count)
    r.expire(key, 90)


# ─────────────────────────────────────────────────────────────────────────────
# Re-introduction phase — rate-limited resume after Lever 1 lifts
# ─────────────────────────────────────────────────────────────────────────────

def _set_reintro_active(r, pool: str) -> None:
    """
    Enter re-introduction phase for pool.  Set when Lever 1 lifts.

    Scheduler dispatch loops and workers check this flag and limit their
    batch size so the queues don't flood immediately after backpressure lifts.
    Cleared by _check_layer2 after REINTRO_STABLE_REQUIRED stable cycles.
    """
    r.set(f"manager:reintro:{pool}:active", "1")
    logger.info(
        "manager [%s]: re-introduction phase started — trickle dispatch active",
        pool,
    )


def _is_reintro_active(r, pool: str) -> bool:
    return bool(r.exists(f"manager:reintro:{pool}:active"))


def _clear_reintro(r, pool: str) -> None:
    r.delete(f"manager:reintro:{pool}:active")
    logger.info(
        "manager [%s]: re-introduction complete — full dispatch resumed", pool
    )


# ─────────────────────────────────────────────────────────────────────────────
# Send or shadow-log a command
# ─────────────────────────────────────────────────────────────────────────────

def _send_cmd(r, cmd: str) -> None:
    """Push a command to manager:cmds, or log it in shadow mode."""
    if SHADOW_MODE:
        logger.info("manager [SHADOW]: would send cmd=%r", cmd)
        return
    r.rpush("manager:cmds", cmd)


# ─────────────────────────────────────────────────────────────────────────────
# One-pool scaling decision
# ─────────────────────────────────────────────────────────────────────────────

def _run_pool_cycle(
    r,
    pool: str,
    n_workers: int,
    pool_busy_ms: int,
    depth: int,
    delay_s: float,
    params: dict,
    worker_ceil: int,
    peak_nd: int,
    service_s: "float | None" = None,
    arrival_rate: "float | None" = None,
    work: "dict | None" = None,
) -> tuple[str, int]:
    """
    Run one Layer 0 cycle for a single pool.  Returns a decision string.

    Decision modes:
      urgent        — delay >= WARN×0.75; spawn to workers_target in 1 cycle
      urgent_release— workers confirmed online after urgent; lift throttle
      urgent_hold   — urgent active but still understaffed (at ceiling → Layer 2)
      scale_up      — util > 0.80 AND delay > WARN×0.5, 2 consecutive cycles
      scale_down    — util < 0.50 AND delay < WARN×0.25, 5 consecutive cycles
      hold          — stable band (50–80% util, delay in bounds)
      *_pending     — streak accumulating, not fired yet

    Returns (decision, workers_target) so the caller can pass workers_target
    to _check_layer2 and _attempt_borrow without recomputing.
    """
    fetch_p75    = params.get("fetch_p75",    _FALLBACK_PARAMS[pool]["fetch_p75"])
    delay_warn_s = params.get("delay_warn_s", _FALLBACK_PARAMS[pool]["delay_warn_s"])

    # ── Utilization ───────────────────────────────────────────────────────────
    pool_capacity_ms = max(n_workers * MANAGER_CYCLE_S * 1000, 1)
    pool_utilization = min(pool_busy_ms / pool_capacity_ms, 1.0)

    # ── Drain-rate formula ────────────────────────────────────────────────────
    # Needed workers = (backlog B / time left + arrival rate lambda) * real
    # service time s.  s is measured from the heartbeats (_get_pool_service_time);
    # fetch_p75 is only the fallback while it is unknown.  lambda is only counted
    # where an arrival counter exists (detail); elsewhere the formula is the doc's.
    # Scan and fullscan are sized from per-company work instead (work =
    # _scan_pool_work): backlog + steady, see docs/scan-fullscan-sizing-plan.md.
    # When that is unknown this cycle the last computed need stays; only a pool
    # that never had one falls through to the formula above.
    if work is not None:
        workers_target = math.ceil(work["backlog"] + work["steady"])
        _last_work_need[pool] = workers_target
    elif pool in _last_work_need:
        workers_target = _last_work_need[pool]
    else:
        s_used         = service_s if service_s else fetch_p75
        time_left      = max(delay_warn_s - delay_s, 1)
        drain_rate     = depth / time_left + (arrival_rate or 0.0)
        workers_target = math.ceil(drain_rate * s_used)

    if delay_s > delay_warn_s:
        workers_target = math.ceil(workers_target * (delay_s / delay_warn_s))

    _needed_workers[pool] = workers_target   # before the ceiling clamp: the real demand
    workers_target = max(WORKER_FLOOR, min(workers_target, worker_ceil))

    # effective_target adjusts for Layer 2 borrows so Layer 0 doesn't undo them.
    # urgent uses raw workers_target (emergency override, borrow-unaware).
    # scale_up / scale_down use effective_target so borrowed workers are counted.
    effective_target = _get_effective_target(r, pool, workers_target)

    # ── Urgent (1-cycle, no util gate — delay-triggered) ─────────────────────
    needs_urgent = (
        delay_s >= delay_warn_s * 0.75
        or (workers_target >= worker_ceil * 0.75 and workers_target > peak_nd)
    ) and n_workers < min(workers_target, worker_ceil)

    if needs_urgent:
        _urgent_active[pool]     = True
        _scale_up_cycles[pool]   = 0
        _scale_down_cycles[pool] = 0
        decision = "urgent"
        logger.warning(
            "manager [%s]: URGENT — delay=%.0fs warn=%.0fs depth=%d "
            "util=%.0f%% target=%d current=%d ceil=%d",
            pool, delay_s, delay_warn_s, depth,
            pool_utilization * 100, workers_target, n_workers, worker_ceil,
        )
        _send_cmd(r, f"{pool}:target:{workers_target}")
        _incr_pending_spawns(r, pool, max(0, workers_target - n_workers))

    elif _urgent_active[pool]:
        if workers_target <= n_workers:
            _urgent_active[pool]     = False
            _scale_down_cycles[pool] = 1 if workers_target < n_workers - 1 else 0
            decision = "urgent_release"
            logger.info(
                "manager [%s]: urgent RELEASED — workers=%d target=%d "
                "delay=%.0fs util=%.0f%%",
                pool, n_workers, workers_target, delay_s, pool_utilization * 100,
            )
        else:
            decision = "urgent_hold"

    # ── Normal scale-up: util > 80% AND delay building AND understaffed ───────
    # Uses effective_target so a pool that lent workers doesn't see a false deficit.
    elif (pool_utilization > MANAGER_SCALE_UP_UTIL
          and delay_s > delay_warn_s * 0.5
          and effective_target > n_workers):
        _scale_up_cycles[pool]   += 1
        _scale_down_cycles[pool]  = 0
        if _scale_up_cycles[pool] >= 2:
            _scale_up_cycles[pool] = 0
            decision = "scale_up"
            logger.info(
                "manager [%s]: SCALE UP — delay=%.0fs warn=%.0fs depth=%d "
                "util=%.0f%% target=%d eff_target=%d current=%d",
                pool, delay_s, delay_warn_s, depth,
                pool_utilization * 100, workers_target, effective_target, n_workers,
            )
            _send_cmd(r, f"{pool}:target:{n_workers + 1}")
            _incr_pending_spawns(r, pool, 1)
        else:
            decision = "scale_up_pending"

    # ── Normal scale-down: util < 50% AND delay low AND overstaffed ──────────
    # Uses effective_target so a pool that received borrowed workers doesn't
    # immediately scale them back down.
    elif (pool_utilization < MANAGER_SCALE_DOWN_UTIL
          and delay_s < delay_warn_s * 0.25
          and effective_target <= n_workers - 1):
        _scale_down_cycles[pool] += 1
        _scale_up_cycles[pool]   = 0
        if _scale_down_cycles[pool] >= 5:
            _scale_down_cycles[pool] = 0
            decision = "scale_down"
            logger.info(
                "manager [%s]: SCALE DOWN — delay=%.0fs warn=%.0fs depth=%d "
                "util=%.0f%% target=%d eff_target=%d current=%d",
                pool, delay_s, delay_warn_s, depth,
                pool_utilization * 100, workers_target, effective_target, n_workers,
            )
            _send_cmd(r, f"{pool}:target:{max(WORKER_FLOOR, n_workers - 1)}")
        else:
            decision = "scale_down_pending"

    else:
        _scale_up_cycles[pool]   = 0
        _scale_down_cycles[pool] = 0
        decision = "hold"

    logger.debug(
        "manager [%s]: n=%d target=%d eff=%d depth=%d delay=%.0fs warn=%.0fs "
        "util=%.0f%% up=%d down=%d → %s",
        pool, n_workers, workers_target, effective_target,
        depth, delay_s, delay_warn_s,
        pool_utilization * 100,
        _scale_up_cycles[pool], _scale_down_cycles[pool], decision,
    )

    return decision, workers_target


# ─────────────────────────────────────────────────────────────────────────────
# Layer 2 — Lever 1 backpressure + deadlock detection + worker borrowing
# ─────────────────────────────────────────────────────────────────────────────

def _push_delay_history(r, pool: str, delay_s: float) -> list:
    """
    Maintain a Redis list of the last DEADLOCK_HISTORY_CYCLES delay readings.
    Returns the list (oldest → newest) after the push.
    """
    key = f"manager:layer2:{pool}:delay_history"
    pipe = r.pipeline()
    pipe.rpush(key, delay_s)
    pipe.ltrim(key, -DEADLOCK_HISTORY_CYCLES, -1)
    pipe.lrange(key, 0, -1)
    results = pipe.execute()
    return [float(v) for v in results[2]]


def _is_deadlock_rising(delays: list, current_delay: float, delay_warn_s: float) -> bool:
    """
    True when:
    - at least DEADLOCK_RISING_MIN of the last DEADLOCK_HISTORY_CYCLES cycles
      had delay > DELAY_WARN_S, AND
    - current_delay > delays[0] (directional — tolerates 1 brief dip without
      resetting the counter, unlike a strictly-monotonic check).
    """
    if len(delays) < DEADLOCK_HISTORY_CYCLES:
        return False
    above_warn = sum(1 for d in delays if d > delay_warn_s)
    if above_warn < DEADLOCK_RISING_MIN:
        return False
    return current_delay > delays[0]


def _get_lever1_active(r, pool: str) -> bool:
    """True if Lever 1 backpressure is currently active for this pool."""
    return bool(r.exists(f"manager:lever1:{pool}:active"))


def _organic_inflow_rate(r, pool: str) -> "float | None":
    """
    Organic arrivals/s for pool since the previous call, or None if unknown.

    detail: delta of the signal:arrivals:*:organic counters (incremented at
    the producer push sites; rebuild pushes are counted separately and never
    enter here) over the real elapsed time.  Other pools have no arrival
    counter yet and return None (caller falls back to the depth delta).
    None is also returned on the first call, on a counter reset (negative
    delta) and on a zero elapsed time: unknown is never reported as 0.
    """
    if pool != "detail":
        return None
    now = time.time()
    try:
        total = sum(
            int(r.get(f"signal:arrivals:{q}:organic") or 0)
            for q in (REDIS_DETAIL_ADAPTIVE, REDIS_DETAIL_FULLSCAN)
        )
    except Exception:
        return None
    prev = _prev_arrivals.get(pool)
    _prev_arrivals[pool] = (total, now)
    if prev is None:
        return None
    prev_total, prev_ts = prev
    elapsed = now - prev_ts
    if elapsed <= 0 or total < prev_total:
        return None
    return (total - prev_total) / elapsed


def _fire_lever1(r, pool: str, depth: int, prev_depth, inflow_rate=_NO_RATE) -> None:
    """
    Activate Lever 1 backpressure for pool.

    Writes manager:lever1:{pool}:active — read by scheduler dispatch loops and
    scan_worker/fullscan before pushing to detail queues.

    inflow_rate: organic arrivals/s from _organic_inflow_rate (detail), used
    only by the draining guard below.  None = unknown.  When the caller passes
    none (scan/fullscan), it is the depth delta over a cycle; prev_depth None
    (first tick) = unknown.  The demand this fire reveals is already recorded
    by the daily peak (needed workers), so no D/R snapshot is kept.

    Inflow guard: skip if the queue is already draining (depth falling and no
    new inflow). URGENT scale-up is already handling it; halting inflow when
    workers are winning is counterproductive.
    """
    if _get_lever1_active(r, pool):
        return  # idempotent
    if inflow_rate is _NO_RATE:
        inflow_rate = (
            None if prev_depth is None
            else max(0.0, (depth - prev_depth) / MANAGER_CYCLE_S)
        )
    if inflow_rate == 0.0 and prev_depth is not None and depth < prev_depth:
        logger.debug(
            "manager [%s]: Lever 1 skipped — queue draining "
            "(depth %d→%d, inflow=0). URGENT workers handling it.",
            pool, prev_depth, depth,
        )
        return
    pipe = r.pipeline()
    pipe.set(f"manager:lever1:{pool}:active", "1")
    pipe.set(f"manager:lever1:{pool}:fired_at", str(int(time.time())), ex=86400)
    pipe.execute()
    logger.warning(
        "manager [%s]: LEVER 1 FIRED — backpressure active. "
        "depth=%d inflow_rate=%s",
        pool, depth,
        "unknown" if inflow_rate is None else f"{inflow_rate:.4f}/s",
    )


def _lift_lever1(r, pool: str, reason: str = "stable") -> None:
    """Lift Lever 1 backpressure for pool."""
    pipe = r.pipeline()
    pipe.delete(f"manager:lever1:{pool}:active")
    pipe.delete(f"manager:lever1:{pool}:fired_at")
    pipe.execute()
    logger.info("manager [%s]: Lever 1 LIFTED — reason=%s", pool, reason)


def _borrow_seats_of(r, source: str, target: str) -> int:
    """Seats `source` has lent to `target` (manager:borrow:{source}:{target}, no TTL)."""
    try:
        return max(0, int(r.get(f"manager:borrow:{source}:{target}") or 0))
    except (ValueError, TypeError):
        return 0


def _borrow_out(r, pool: str) -> int:
    """Ceiling seats `pool` has lent to other pools."""
    return sum(_borrow_seats_of(r, pool, t) for t in _POOL_PRIORITY if t != pool)


def _borrow_in(r, pool: str) -> int:
    """Ceiling seats `pool` has borrowed from other pools."""
    return sum(_borrow_seats_of(r, s, pool) for s in _POOL_PRIORITY if s != pool)


def _get_effective_target(r, pool: str, workers_target: int) -> int:
    """
    Borrowing moves ceiling seats (see _get_worker_ceil), so the borrow state is
    already inside workers_target through the ceiling clamp; adjusting it again
    here would count each seat twice.  Kept as the single place Layer 0
    reads its target through.
    """
    return workers_target


def _get_lendable(r, source_pool: str, n_workers: int, workers_target: int) -> int:
    """
    Workers source_pool can lend all at once — those running above its own target
    and above WORKER_FLOOR.  Seats it has already lent are subtracted.  Transient
    by nature (scale-down lag, unreturned borrows); not a steady source.

    lendable = n_workers - max(workers_target, WORKER_FLOOR) - borrowed_out
    """
    min_keep = max(workers_target, WORKER_FLOOR) + _borrow_out(r, source_pool)
    return max(0, n_workers - min_keep)


def _get_pending_spawns(r, pool: str) -> int:
    """Workers spawned but not yet online (prevent double-spawning while they start)."""
    val = r.get(f"manager:pool:{pool}:pending_spawns")
    return int(val) if val else 0


def _record_borrow(r, source_pool: str, target_pool: str, count: int) -> None:
    """Persist a borrow of `count` workers from source to target."""
    if count <= 0:
        return
    key = f"manager:borrow:{source_pool}:{target_pool}"
    existing = int(r.get(key) or 0)
    r.set(key, existing + count)
    logger.warning(
        "manager: BORROW %d workers %s → %s (running total: %d)",
        count, source_pool, target_pool, existing + count,
    )


def _return_all_borrows(r, pool: str) -> None:
    """
    Return all borrowed workers for a pool (both as source and target).
    Called when a pool fully recovers and Layer 2 state is cleared.
    """
    cursor = 0
    while True:
        cursor, keys = r.scan(cursor, match=f"manager:borrow:{pool}:*", count=20)
        for key in keys:
            k = key.decode() if isinstance(key, bytes) else key
            logger.info("manager: returning borrow state %s", k)
            r.delete(k)
        if cursor == 0:
            break
    cursor = 0
    while True:
        cursor, keys = r.scan(cursor, match=f"manager:borrow:*:{pool}", count=20)
        for key in keys:
            k = key.decode() if isinstance(key, bytes) else key
            logger.info("manager: returning borrow state %s", k)
            r.delete(k)
        if cursor == 0:
            break


_BORROW_SOURCE_ORDER: tuple = ("fullscan", "scan", "detail")   # seats are taken in this order


def _borrow_seats(r, source: str, target_pool: str, pool_sizes: dict,
                  workers_targets: dict) -> dict:
    """
    Seats `source` can give `target_pool` right now, split by kind:
      free      — adjusted ceiling above max(source target, WORKER_FLOOR)
      lendable  — running workers above target (transient), capped at free
      buffer    — seats above the source's peak_Nd (the safety buffer), capped at free
      headroom  — free seats that are neither buffer nor lendable
    Detail is protected: it gives seats only while its own Lever 1 has halted its
    inflow, and towards fullscan only the workers it runs above target - 1.
    """
    none = {"free": 0, "lendable": 0, "buffer": 0, "headroom": 0}
    if source == "detail" and not _get_lever1_active(r, "detail"):
        return none
    adj_ceil = _get_worker_ceil(r, source)
    src_tgt  = max(workers_targets.get(source, WORKER_FLOOR), WORKER_FLOOR)
    free     = max(0, adj_ceil - src_tgt)
    if source == "detail" and target_pool == "fullscan":
        free = min(free, max(0, pool_sizes.get("detail", 0) - src_tgt - 1))
    if free <= 0:
        return none
    lendable = min(free, _get_lendable(r, source, pool_sizes.get(source, 0), src_tgt))
    peak_nd  = int(r.get(f"manager:pool:{source}:peak_nd") or 0)
    buffer_  = min(free, max(0, adj_ceil - max(peak_nd, WORKER_FLOOR))) if peak_nd else 0
    return {"free": free, "lendable": lendable, "buffer": buffer_,
            "headroom": max(0, free - lendable - buffer_)}


def _give_seats(r, source: str, target_pool: str, count: int, pool_sizes: dict) -> None:
    """Move `count` ceiling seats and shrink the source's workers to its new ceiling."""
    _record_borrow(r, source, target_pool, count)
    new_ceil = _get_worker_ceil(r, source)
    if pool_sizes.get(source, 0) > new_ceil:
        _send_cmd(r, f"{source}:target:{new_ceil}")


def _attempt_borrow(
    r,
    target_pool: str,
    pool_sizes: dict,
    workers_targets: dict,
) -> bool:
    """
    Borrow ceiling seats for target_pool from the other pools (fullscan, scan,
    detail in that order).  Returns True if any seats moved this cycle.  The first
    stage that yields seats ends the cycle, so the next cycle can see its effect:

      1. lendable — workers running above the source's target, all at once
      2. buffer   — seats above the source's peak_Nd, all at once
      3. headroom — peak_Nd - target seats, ONE per cycle
    The target pool's Layer 0 then spawns into the new seats (ceiling clamp);
    the source is told to shrink to its reduced ceiling.
    """
    sources = [p for p in _BORROW_SOURCE_ORDER if p != target_pool]
    seats = {s: _borrow_seats(r, s, target_pool, pool_sizes, workers_targets)
             for s in sources}

    for kind, label in (("lendable", "lendable workers"),
                        ("buffer", "buffer seats")):
        moved = 0
        for s in sources:
            if seats[s][kind] > 0:
                _give_seats(r, s, target_pool, seats[s][kind], pool_sizes)
                moved += seats[s][kind]
        if moved:
            logger.warning(
                "manager: DEADLOCK [%s] — borrowed %d %s. Waiting 1 cycle to assess.",
                target_pool, moved, label,
            )
            return True

    for s in sources:
        if seats[s]["headroom"] > 0:
            _give_seats(r, s, target_pool, 1, pool_sizes)
            logger.warning(
                "manager: DEADLOCK [%s] — borrowed 1 peak-headroom seat from %s. "
                "Waiting 1 cycle.",
                target_pool, s,
            )
            return True

    logger.warning(
        "manager: DEADLOCK [%s] — no lendable, buffer or headroom seats available",
        target_pool,
    )
    return False


def _raise_db_total(r, pool: str) -> bool:
    """
    Capacity overflow: raise the worker seats by DB_POOL_RAISE_STEP connections
    (converted with the live connections per worker) toward the seat cap read
    from Postgres, and give the new seats (up to the spare) to `pool`.  At the
    cap, or when Postgres cannot be queried, nothing is raised and an alert is
    logged.  Returns True if raised.
    """
    total = _get_db_total(r)
    cap = _seat_cap(r)
    step = _raise_step_seats(r)
    if step == 0 or total >= cap:
        logger.error(
            "manager: [Pipeline] capacity overflow — %s needs seats but worker "
            "seats (%d) are at the Postgres seat cap (%d)%s",
            pool, total, cap, "" if step else " [Postgres probe failed]",
        )
        return False
    new_total = min(total + step, cap)
    r.set("manager:db_pool_maxconn", new_total)
    spare = new_total - sum(_get_base_ceil(r, p) for p in BOOTSTRAP_CEIL)
    grant = min(new_total - total, max(0, spare))
    if grant > 0:
        r.set(f"manager:worker_ceil:{pool}", _get_base_ceil(r, pool) + grant)
    logger.warning(
        "manager [%s]: capacity overflow — worker seats %d → %d (Postgres seat cap=%d), "
        "+%d seats to %s",
        pool, total, new_total, cap, grant, pool,
    )
    return True


_borrow_stable_cycles: dict = {}


def _maybe_return_borrows(r, delays: dict, warns: dict) -> None:
    """
    Return a borrow (source <-> target) once BOTH pools have run below
    delay_warn × RECOVERY_STABILITY_RATIO for LEVER1_STABLE_REQUIRED cycles in a row.
    Borrow state lives in manager:borrow:* (no TTL), so it survives restarts.

    Handoff (docs/scaling-redesign.md, "Borrowed worker handoff"): when the seats
    are due back but a third pool is still struggling (delay above its warn), they
    are reassigned to that pool instead of returning to the lender, so a pool that
    resolves first does not hand the seats back just for the next pool to ask for
    them again.  Seats are conserved, so the lender's count and the budget do not
    change.  The reassigned borrow must then pass the same calm test to return.
    """
    for src in _POOL_PRIORITY:
        for tgt in _POOL_PRIORITY:
            if src == tgt or not _borrow_seats_of(r, src, tgt):
                continue
            k = f"manager:borrow:{src}:{tgt}"
            calm = all(
                delays.get(p, 0.0) < warns.get(p, 1e9) * RECOVERY_STABILITY_RATIO
                for p in (src, tgt)
            )
            n = _borrow_stable_cycles.get(k, 0) + 1 if calm else 0
            _borrow_stable_cycles[k] = n
            if n < LEVER1_STABLE_REQUIRED:
                continue
            seats = _borrow_seats_of(r, src, tgt)
            heir = next(
                (p for p in _POOL_PRIORITY
                 if p not in (src, tgt) and delays.get(p, 0.0) > warns.get(p, 1e9)),
                None,
            )
            if heir:
                logger.info(
                    "manager: handing %d borrowed seats %s → %s over to %s "
                    "(still struggling) instead of returning them",
                    seats, src, tgt, heir,
                )
                _record_borrow(r, src, heir, seats)
            else:
                logger.info("manager: returning borrow state %s", k)
            r.delete(k)
            _borrow_stable_cycles.pop(k, None)


def _check_layer2(
    r,
    pool:             str,
    n_workers:        int,
    delay_s:          float,
    depth:            int,
    params:           dict,
    worker_ceil:      int,
    pool_sizes:       dict,
    workers_targets:  dict,
) -> None:
    """
    Layer 2 check — runs after _run_pool_cycle for each pool every cycle.

    Steps:
      1. Update delay history (4-cycle rolling window for 3-of-4 check).
      2. Capacity chain when the pool is at its ceiling with delay above warn
         and rising (3 of last 4 cycles): emergency recalc → borrow → budget
         raise; Lever 1 fires only when none of them yields capacity.
         Snapshots D and R before Lever 1 acts (learning loop input).
      3. Check Lever 1 lift: LEVER1_STABLE_REQUIRED consecutive cycles below
         WARN × RECOVERY_STABILITY_RATIO.  On lift: compute true_required and
         update today's daily_peak (learning loop).  Borrows are returned by
         _maybe_return_borrows, not here.
    """
    delay_warn_s = params.get("delay_warn_s", _FALLBACK_PARAMS[pool]["delay_warn_s"])

    # 1. Track delay history
    delays = _push_delay_history(r, pool, delay_s)

    # 2. Capacity chain, then Lever 1 as the last resort.  The arrival counter is
    # sampled every cycle (not only on fire) so the rate always spans one tick.
    # Once the pool is at its ceiling with delay above warn and rising, each
    # cycle takes the first step that yields capacity:
    #   emergency recalc of its own ceiling → borrow (lendable, buffer, one
    #   headroom seat) → raise the worker budget toward the Postgres seat cap → Lever 1.
    # The main loop reads the arrival counter and the service-time window once per
    # cycle; a second read here would measure a near-zero interval.
    if pool in _cycle_arrival_rate:
        organic_rate = _cycle_arrival_rate.pop(pool)
    else:
        organic_rate = _organic_inflow_rate(r, pool)
    service_s = _cycle_service_s.pop(pool, None)
    lever1_active = _get_lever1_active(r, pool)
    if (delay_s > delay_warn_s
            and n_workers + _get_pending_spawns(r, pool) >= worker_ceil
            and _is_deadlock_rising(delays, delay_s, delay_warn_s)):
        gained = (_emergency_recalc(r, pool, list(BOOTSTRAP_CEIL))
                  or _attempt_borrow(r, pool, pool_sizes, workers_targets)
                  or _raise_db_total(r, pool))
        # Lever 1 halts inflow, so it only helps when inflow is what overloads the
        # pool: lambda * s > n.  Unknown lambda or s (no arrival counter, no
        # finished job yet) is not evidence of safety, so Lever 1 may fire.
        overloaded = (organic_rate is None or service_s is None
                      or organic_rate * service_s > n_workers)
        if not gained and not lever1_active and not overloaded:
            logger.info(
                "manager [%s]: chain exhausted but lambda*s=%.2f <= n=%d — "
                "backlog is draining, Lever 1 not needed",
                pool, organic_rate * service_s, n_workers,
            )
        if not gained and not lever1_active and overloaded:
            if pool == "detail":
                _fire_lever1(r, pool, depth, _prev_depth.get(pool), organic_rate)
            else:
                _fire_lever1(r, pool, depth, _prev_depth.get(pool))
            lever1_active = True

    # 3. Lever 1 lift check
    if lever1_active:
        stable_threshold = delay_warn_s * RECOVERY_STABILITY_RATIO
        if delay_s < stable_threshold:
            _lever1_stable_cycles[pool] = _lever1_stable_cycles.get(pool, 0) + 1
        else:
            _lever1_stable_cycles[pool] = 0

        # Max-hold guard: for scan/fullscan pools, the ZSET delay_s GROWS under
        # Lever 1 (dispatch is halted, companies pile up) so the normal stable
        # condition may never be met.  Force-lift after LEVER1_MAX_HOLD_S so the
        # scheduler can resume.  If conditions still warrant Lever 1, it re-fires
        # within one cycle (60s) automatically.
        max_hold = LEVER1_MAX_HOLD_S.get(pool)
        force_lift = False
        if max_hold is not None:
            fired_at_raw = r.get(f"manager:lever1:{pool}:fired_at")
            if fired_at_raw and (time.time() - float(fired_at_raw)) > max_hold:
                force_lift = True
                logger.warning(
                    "manager [%s]: Lever 1 max hold (%ds) exceeded "
                    "(fired_at=%s delay=%.0fs) — force lifting; "
                    "will re-fire this cycle if conditions still warrant it",
                    pool, max_hold, fired_at_raw.decode()
                    if isinstance(fired_at_raw, bytes) else fired_at_raw,
                    delay_s,
                )

        if force_lift or _lever1_stable_cycles.get(pool, 0) >= LEVER1_STABLE_REQUIRED:
            lift_reason = "max_hold_expired" if force_lift else "stable"
            _lift_lever1(r, pool, reason=lift_reason)
            _lever1_stable_cycles[pool] = 0
            lever1_active = False

            # Enter rate-limited re-introduction phase so queues don't flood
            _set_reintro_active(r, pool)
            _reintro_stable_cycles[pool] = 0

    # 3b. Re-introduction phase tracking (runs only when Lever 1 is not active)
    if not lever1_active and _is_reintro_active(r, pool):
        stable_threshold = delay_warn_s * RECOVERY_STABILITY_RATIO
        if delay_s < stable_threshold:
            _reintro_stable_cycles[pool] = _reintro_stable_cycles.get(pool, 0) + 1
        else:
            _reintro_stable_cycles[pool] = 0

        if _reintro_stable_cycles.get(pool, 0) >= REINTRO_STABLE_REQUIRED:
            _clear_reintro(r, pool)
            _reintro_stable_cycles[pool] = 0


# ─────────────────────────────────────────────────────────────────────────────
# Error spike / outage detection (mirrors _fast_error_check_loop logic)
# ─────────────────────────────────────────────────────────────────────────────

def _check_error_spikes(r) -> None:
    """
    Read manager:platform:{p}:error_spike flags written by adjust_concurrency()
    in http_client.py (set whenever error_rate > CONCURRENCY_ERROR_RATE_REDUCE).

    State machine (mirrors old _fast_error_check_loop, now runs every 60s):

      For each platform with an active spike flag:
        1. Skip if already in outage mode.
        2. If a before_rate snapshot exists from a previous action:
             - error resolved   → reset consec_reductions counter
             - still erroring   → increment consec_reductions; if >= threshold
                                  → declare outage via manager:cmds
        3. If error_rate still above threshold AND concurrency is at floor:
             - Snapshot current error_rate as before_rate (effectiveness check
               next cycle).
             - Send platform:deprioritize:{platform} via manager:cmds.

    Worker-level removal is NOT done here — manager.py already handles pool
    sizing via the util/delay formula.  Deprioritize + outage are the only
    levers this function pulls.
    """
    from db.api_health import record_scaling_event

    cursor = 0
    while True:
        cursor, keys = r.scan(cursor, match="manager:platform:*:error_spike", count=50)
        for key in keys:
            k        = key.decode() if isinstance(key, bytes) else key
            platform = k.removeprefix("manager:platform:").removesuffix(":error_spike")

            # 1. Skip if already in outage
            if r.exists(f"worker:outage:{platform}"):
                continue

            raw = r.get(key)
            if raw is None:
                continue
            try:
                spike = json.loads(raw)
            except Exception:
                continue

            error_rate   = spike.get("error_rate",   0.0)
            baseline     = spike.get("baseline",     0.0)
            spike_factor = spike.get("spike_factor", 0.0)

            # 2. Effectiveness check from previous deprioritize action
            before_key = f"worker:reduction:before_rate:{platform}"
            before_raw = r.get(before_key)
            if before_raw is not None:
                before_rate = float(before_raw)
                r.delete(before_key)

                if error_rate <= CONCURRENCY_ERROR_RATE_REDUCE:
                    r.delete(f"worker:consec_reductions:{platform}")
                    logger.info(
                        "manager: error spike resolved platform=%r "
                        "(%.1f%% → %.1f%%) — consec_reductions reset",
                        platform, before_rate * 100, error_rate * 100,
                    )
                else:
                    count = r.incr(f"worker:consec_reductions:{platform}")
                    r.expire(f"worker:consec_reductions:{platform}",
                             WORKER_CONSEC_REDUCTIONS_TTL)
                    logger.warning(
                        "manager: deprioritize INEFFECTIVE platform=%r "
                        "(%.1f%% → %.1f%%) consec_reductions=%d",
                        platform, before_rate * 100, error_rate * 100, count,
                    )

                    if count >= WORKER_CONSEC_REDUCTIONS_THRESHOLD:
                        _send_cmd(r, f"platform:outage:{platform}:set")
                        r.delete(f"worker:consec_reductions:{platform}")
                        logger.warning(
                            "manager: OUTAGE DECLARED platform=%r "
                            "after %d consecutive ineffective deprioritizations",
                            platform, count,
                        )
                        try:
                            record_scaling_event(
                                "outage_start",
                                trigger_layer="manager",
                                platform=platform,
                                error_rate=error_rate,
                                baseline_error_rate=baseline if baseline > 0 else None,
                                spike_factor=spike_factor,
                                consec_reductions=int(count),
                            )
                        except Exception:
                            pass
                        continue

            # 3. Still erroring — act if concurrency feedback loop is exhausted
            if error_rate <= CONCURRENCY_ERROR_RATE_REDUCE:
                continue

            floor   = CONCURRENCY_FLOOR.get(platform, CONCURRENCY_FLOOR_DEFAULT)
            lim_raw = r.get(f"{REDIS_CONCURRENCY_LIMIT_PREFIX}:{platform}")
            current = int(lim_raw) if lim_raw is not None else floor + 1

            if current > floor:
                continue  # feedback loop still has room to reduce concurrency

            # Concurrency at floor and errors still high — deprioritize
            r.set(before_key, str(error_rate), ex=MANAGER_CYCLE_S * 3)
            _send_cmd(r, f"platform:deprioritize:{platform}")
            logger.warning(
                "manager: platform=%r error=%.1f%% at concurrency floor=%d "
                "— deprioritized",
                platform, error_rate * 100, floor,
            )

        if cursor == 0:
            break


# ─────────────────────────────────────────────────────────────────────────────
# ATS pool autoscaling (simple on/off, not Layer 0 formula)
# ─────────────────────────────────────────────────────────────────────────────

def _get_ats_alive_count(r, hb_prefix: str) -> int:
    """Count alive ATS worker instances by scanning worker:alive:{hb_prefix}* keys.

    Keys have the form worker:alive:{hb_prefix}@{instance}:{pid}.  A single
    instance that restarts quickly can leave a stale PID key alongside a fresh
    one before the old TTL expires, so we deduplicate by instance, not by key.
    """
    cursor = 0
    keys_seen: set = set()
    while True:
        cursor, keys = r.scan(cursor, match=f"worker:alive:{hb_prefix}*", count=50)
        keys_seen.update(keys)
        if cursor == 0:
            break
    instances: set = set()
    for key in keys_seen:
        key_str = key.decode() if isinstance(key, bytes) else key
        at_idx = key_str.rfind("@")
        if at_idx != -1:
            after_at = key_str[at_idx + 1:]          # "{instance}:{pid}"
            colon_idx = after_at.rfind(":")
            inst = after_at[:colon_idx] if colon_idx != -1 else after_at
            instances.add(inst)
        else:
            instances.add(key_str)
    return len(instances)


def _run_ats_pool_cycle(
    r,
    pool_label: str,
    combined_depth: int,
    worker_units: tuple,
    hb_prefix: str,
) -> None:
    """Simple on/off autoscaling for ATS enrichment/discovery pools.

    depth == 0: increment idle counter; stop all workers when idle >= ATS_MANAGER_IDLE_CYCLES.
    depth > 0:  reset idle counter; ensure at least one worker is alive;
                start second worker when depth >= ATS_MANAGER_SCALE_UP_THRESHOLD.
    """
    from workers.worker_control import start_workers, stop_workers

    alive = _get_ats_alive_count(r, hb_prefix)

    if combined_depth == 0:
        _ats_idle_cycles[pool_label] = _ats_idle_cycles.get(pool_label, 0) + 1
        if _ats_idle_cycles[pool_label] >= ATS_MANAGER_IDLE_CYCLES and alive > 0:
            logger.info(
                "manager [%s]: idle for %d cycles — stopping workers",
                pool_label, _ats_idle_cycles[pool_label],
            )
            stop_workers(*worker_units)
            _ats_idle_cycles[pool_label] = 0
        return

    _ats_idle_cycles[pool_label] = 0

    if alive == 0:
        logger.info(
            "manager [%s]: depth=%d, no workers alive — starting %s",
            pool_label, combined_depth, worker_units[0],
        )
        start_workers(worker_units[0])

    if combined_depth >= ATS_MANAGER_SCALE_UP_THRESHOLD and alive < len(worker_units):
        for _unit in worker_units:
            _inst = _unit.rsplit("@", 1)[-1]
            _hb_cursor, _hb_keys = 0, []
            while True:
                _hb_cursor, _batch = r.scan(_hb_cursor, match=f"worker:alive:{hb_prefix}@{_inst}:*", count=10)
                _hb_keys.extend(_batch)
                if _hb_keys or _hb_cursor == 0:
                    break
            if not _hb_keys:
                logger.info(
                    "manager [%s]: depth=%d >= threshold=%d — starting %s",
                    pool_label, combined_depth, ATS_MANAGER_SCALE_UP_THRESHOLD, _unit,
                )
                start_workers(_unit)


def _mobile_relay_inflight_depth(r) -> int:
    """Sum the cardinalities of every per-instance mobile-relay inflight ZSET.

    scripts/mobile_relay_drain_worker.py keys its inflight ZSET as
    "{MOBILE_RELAY_INFLIGHT}:{WORKER_INSTANCE}" (e.g. "...:1" for the
    mobile-relay-drain-worker@1 systemd instance, which sets WORKER_INSTANCE=%i —
    see deploy/systemd/mobile-relay-drain-worker@.service). Reading only the bare
    MOBILE_RELAY_INFLIGHT key here always saw zero, since nothing is ever written
    there under the systemd deployment — undercounting combined_depth and risking
    the autoscaler treating a worker with genuinely inflight items as idle.

    Scans for "{MOBILE_RELAY_INFLIGHT}:*" (per-instance keys) and separately adds
    the bare key (covers a manual/local run with no WORKER_INSTANCE set) — the two
    patterns never overlap, so no double-counting.
    """
    depth = r.zcard(MOBILE_RELAY_INFLIGHT)
    cursor = 0
    while True:
        cursor, keys = r.scan(cursor, match=f"{MOBILE_RELAY_INFLIGHT}:*", count=100)
        for key in keys:
            depth += r.zcard(key)
        if cursor == 0:
            break
    return depth


def _probe_mobile_relay_reachable(timeout_s: float = MOBILE_RELAY_PROBE_TIMEOUT_S) -> bool:
    """TCP-connect probe to the home PC's WireGuard-tunneled SOCKS5 relay.

    A single connect() is enough — we're only checking whether the tunnel + the
    relay process on the other end are both up, not doing a real SOCKS5 handshake.
    Cheap (~50ms typical over an established tunnel), so safe to run every cycle.
    """
    import socket
    try:
        with socket.create_connection((MOBILE_RELAY_PROXY_HOST, MOBILE_RELAY_PROXY_PORT), timeout=timeout_s):
            return True
    except OSError:
        return False


def _trigger_mobile_relay_start() -> None:
    """Best-effort push to scripts/mobile_relay_watcher.py's control listener on
    the home PC (on-demand-start follow-up, 2026-09-28 — see config.py's
    MOBILE_RELAY_WATCHER_* comment). Sends a bare "START\\n" line; the watcher
    spawns scripts/mobile_relay_socks5.py if it isn't already running.

    Never allowed to slow down or fail the autoscaler cycle: a short connect+send
    timeout, and any failure (watcher not running, PC off, tunnel down) is just
    logged at debug level — the existing _probe_mobile_relay_reachable() check
    already treats an unreachable relay as "depth 0" regardless of this trigger's
    outcome, so there is nothing else to fall back to here.
    """
    import socket
    try:
        with socket.create_connection(
            (MOBILE_RELAY_WATCHER_HOST, MOBILE_RELAY_WATCHER_PORT),
            timeout=MOBILE_RELAY_WATCHER_TRIGGER_TIMEOUT_S,
        ) as sock:
            sock.sendall(b"START\n")
    except OSError as e:
        logger.debug("mobile relay watcher trigger failed (PC/watcher likely offline): %s", e)


def _run_mobile_relay_cycle(r) -> None:
    """Autoscaling for the single-instance mobile relay drain worker (Part 3).

    Reuses the same _run_ats_pool_cycle primitive as head_check/enrichment/discovery
    (docs/discovery-pipeline-hardening.md Part 3, "rewire manager.py to reuse
    _run_ats_pool_cycle"). combined_depth is queue+inflight, same shape as the other
    pools — except it is forced to 0 whenever the WireGuard tunnel is unreachable, so
    an unreachable relay scales the pool to 0 exactly like an empty queue would,
    instead of wasting a systemd start on a proxy nothing can reach.
    """
    from workers.worker_control import MOBILE_RELAY_WORKERS

    reachable      = _probe_mobile_relay_reachable()
    combined_depth = r.zcard(MOBILE_RELAY_QUEUE) + _mobile_relay_inflight_depth(r)
    if not reachable and combined_depth > 0:
        logger.info(
            "manager [mobile_relay]: tunnel unreachable — depth=%d treated as 0, "
            "triggering on-demand relay start",
            combined_depth,
        )
        _trigger_mobile_relay_start()
        combined_depth = 0

    _run_ats_pool_cycle(
        r,
        pool_label="mobile_relay",
        combined_depth=combined_depth,
        worker_units=MOBILE_RELAY_WORKERS,
        hb_prefix="mobile_relay_drain_worker",
    )


# ─────────────────────────────────────────────────────────────────────────────
# Main manager loop
# ─────────────────────────────────────────────────────────────────────────────

def run_manager() -> None:
    """
    Main entry point.  Runs the 60s autoscaler cycle indefinitely.

    Startup sequence:
      1. Load scaling_params from Redis cache (or compute from DB)
      2. Set manager:bootstrap flag if < 28 days of daily_peak data
      3. Enter main loop

    Each cycle:
      a. Acquire distributed lock (manager:lock NX EX 90)
      b. Read pool sizes from scheduler:health
      c. Read queue metrics (depth + delay) for each pool
      d. Run Layer 0 formula per pool — log or send commands
      e. Update daily_peak:running
      f. Write manager:backpressure:threshold for watchdog (future use)
      g. Check for midnight (trigger Layer 1 recompute once per day)
      h. Release lock
    """
    from db.db import init_db

    init_db()

    r = get_redis()
    logger.info(
        "manager: starting (SHADOW_MODE=%s cycle=%ds)",
        SHADOW_MODE, MANAGER_CYCLE_S,
    )

    pools = ["detail", "scan", "fullscan"]

    # ── Load scaling params ────────────────────────────────────────────────────
    scaling_params = _load_scaling_params(r)

    # ── Lever 1 startup re-evaluation ─────────────────────────────────────────
    # A prior manager instance may have set Lever 1 and then restarted (e.g.,
    # deploy rollback) before conditions recovered.  The new instance has no
    # in-memory stable-cycle counter, so it will never lift a stale key on its
    # own — the max-hold check above handles the long-term case, but on startup
    # we can resolve it immediately by re-evaluating current conditions.
    #
    # If delay_s is already below delay_warn_s (conditions resolved while manager
    # was down): clear Lever 1 immediately so dispatch resumes without waiting
    # for LEVER1_STABLE_REQUIRED cycles.
    # If conditions still warrant Lever 1: leave it active and reset fired_at to
    # now so the max-hold clock starts fresh for this manager instance.
    _startup_queue = _get_queue_metrics(r)
    for _pool in pools:
        if not _get_lever1_active(r, _pool):
            continue
        _sp      = scaling_params.get(_pool, _FALLBACK_PARAMS[_pool])
        _warn    = _sp.get("delay_warn_s", _FALLBACK_PARAMS[_pool]["delay_warn_s"])
        _delay   = _startup_queue.get(_pool, {}).get("delay_s", 0.0)
        if _delay <= _warn:
            _lift_lever1(r, _pool, reason="startup_conditions_resolved")
            logger.warning(
                "manager [%s]: startup: cleared stale Lever 1 from prior instance "
                "(delay=%.0fs <= warn=%.0fs — conditions resolved)",
                _pool, _delay, _warn,
            )
        else:
            # Conditions still warrant Lever 1 — take ownership: reset fired_at
            # so the max-hold clock starts from this instance's start time.
            r.set(f"manager:lever1:{_pool}:fired_at", str(int(time.time())), ex=86400)
            logger.warning(
                "manager [%s]: startup: inherited active Lever 1 from prior instance "
                "(delay=%.0fs > warn=%.0fs) — maintaining halt, max_hold reset",
                _pool, _delay, _warn,
            )

    # ── Bootstrap flag ─────────────────────────────────────────────────────────
    any_bootstrap = any(
        _count_daily_peak_records(r, p) < BOOTSTRAP_DAYS_REQUIRED
        for p in pools
    )
    if any_bootstrap:
        r.set("manager:bootstrap", "1")
        logger.warning(
            "manager: bootstrap mode — fewer than %d days of daily_peak data",
            BOOTSTRAP_DAYS_REQUIRED,
        )
    else:
        r.delete("manager:bootstrap")

    _cycle_count: int = 0

    # Write an initial heartbeat immediately so health checks during the first
    # 60-second cycle don't report the manager as dead.
    try:
        r.set(
            f"worker:alive:manager:{_HOSTNAME}:{os.getpid()}",
            json.dumps({"pid": os.getpid(), "ts": time.time(), "cycles": 0}),
            ex=_MANAGER_HB_TTL,
        )
    except Exception:
        pass

    while True:
        cycle_start = time.time()
        _cycle_count += 1

        try:
            # ── Distributed lock — skip cycle if already running ──────────────
            lock_acquired = r.set("manager:lock", os.getpid(), nx=True, ex=90)
            if not lock_acquired:
                logger.debug("manager: lock contention — skipping cycle")
                time.sleep(MANAGER_CYCLE_S)
                continue

            try:
                # ── Refresh scaling params if TTL expired ─────────────────────
                if not r.exists("manager:scaling_params"):
                    scaling_params = _load_scaling_params(r)

                # ── Pool sizes + queue metrics ────────────────────────────────
                pool_sizes  = _get_pool_sizes(r)
                queue_data  = _get_queue_metrics(r)

                # ── Per-pool cycle ────────────────────────────────────────────
                # Collect workers_targets for all pools (needed by _check_layer2
                # so _attempt_borrow knows each pool's current demand level).
                all_workers_targets: dict = {}

                for pool in pools:
                    n_workers    = pool_sizes.get(pool, 0)
                    pool_busy_ms = _get_pool_busy_ms(r, pool)
                    depth        = queue_data.get(pool, {}).get("depth", 0)
                    delay_s      = queue_data.get(pool, {}).get("delay_s", 0.0)
                    params       = scaling_params.get(pool, _FALLBACK_PARAMS[pool])
                    worker_ceil  = _get_worker_ceil(r, pool)

                    # peak_nd: today's running peak (proxy until 28 days accumulate)
                    peak_raw = r.get(f"manager:pool:{pool}:daily_peak:running")
                    peak_nd  = max(int(peak_raw or 0), WORKER_FLOOR)

                    service_s    = _get_pool_service_time(r, pool)
                    arrival_rate = _organic_inflow_rate(r, pool)
                    _cycle_service_s[pool]    = service_s
                    _cycle_arrival_rate[pool] = arrival_rate

                    work = None
                    if pool in _SCAN_TYPE_POOLS:
                        _t0  = time.monotonic()
                        work = _scan_pool_work(r, pool, queue_data.get(pool, {}), params)
                        _work_ms = (time.monotonic() - _t0) * 1000

                    decision, workers_target = _run_pool_cycle(
                        r, pool, n_workers, pool_busy_ms,
                        depth, delay_s, params, worker_ceil, peak_nd,
                        service_s, arrival_rate, work,
                    )
                    all_workers_targets[pool] = workers_target
                    needed = _needed_workers.get(pool, workers_target)
                    logger.info(
                        "manager [%s]: n=%d needed=%d target=%d ceil=%d depth=%d "
                        "delay=%.0fs lambda=%s s=%s%s → %s",
                        pool, n_workers, needed, workers_target, worker_ceil, depth,
                        delay_s,
                        "n/a" if arrival_rate is None else f"{arrival_rate:.3f}/s",
                        "n/a" if service_s is None else f"{service_s:.2f}s",
                        "" if pool not in _SCAN_TYPE_POOLS else (
                            " work=unknown" if work is None else
                            f" late={work['late']} backlog={work['backlog']:.2f} "
                            f"steady={work['steady']:.2f} read={_work_ms:.0f}ms"
                        ),
                        decision,
                    )

                    # Running daily peak = workers the pool NEEDED (not the clamped
                    # count), capped at what the budget could ever hold.
                    _update_daily_peak_running(
                        r, pool,
                        max(n_workers, min(needed, _seat_cap(r))),
                    )

                    # Publish backpressure threshold for watchdog (delay_warn_s)
                    try:
                        delay_warn_s = params.get(
                            "delay_warn_s", _FALLBACK_PARAMS[pool]["delay_warn_s"]
                        )
                        r.set(
                            f"manager:backpressure:threshold:{pool}",
                            str(delay_warn_s),
                            ex=MANAGER_CYCLE_S * 3,
                        )
                    except Exception:
                        pass

                _check_health_alerts(r, time.time())

                # ── Layer 2 check (after all Layer 0 decisions are made) ───────
                for pool in pools:
                    try:
                        n_workers   = pool_sizes.get(pool, 0)
                        depth       = queue_data.get(pool, {}).get("depth", 0)
                        delay_s     = queue_data.get(pool, {}).get("delay_s", 0.0)
                        params      = scaling_params.get(pool, _FALLBACK_PARAMS[pool])
                        worker_ceil = _get_worker_ceil(r, pool)
                        _check_layer2(
                            r, pool, n_workers, delay_s, depth,
                            params, worker_ceil, pool_sizes, all_workers_targets,
                        )
                    except Exception as exc:
                        logger.error(
                            "manager: Layer 2 check failed [%s]: %s", pool, exc,
                            exc_info=True,
                        )

                # ── Return borrowed seats once the involved pools are calm ────
                try:
                    _maybe_return_borrows(
                        r,
                        {p: queue_data.get(p, {}).get("delay_s", 0.0) for p in pools},
                        {p: scaling_params.get(p, _FALLBACK_PARAMS[p]).get(
                            "delay_warn_s", _FALLBACK_PARAMS[p]["delay_warn_s"])
                         for p in pools},
                    )
                except Exception as exc:
                    logger.error("manager: borrow return failed: %s", exc, exc_info=True)

                # ── ATS pool autoscaling (simple on/off, not Layer 0) ────────
                try:
                    from workers.worker_control import (
                        ENRICHMENT_WORKERS,
                        DISCOVERY_WORKERS,
                        HEAD_CHECK_WORKERS,
                    )
                    _hc_data = queue_data.get("head_check", {})
                    if _hc_data.get("depth_known", True):
                        _run_ats_pool_cycle(
                            r,
                            pool_label="head_check",
                            combined_depth=_hc_data.get("depth", 0),
                            worker_units=HEAD_CHECK_WORKERS,
                            hb_prefix="head_check_worker",
                        )
                    _enrich_data = queue_data.get("domain_enrichment", {})
                    if _enrich_data.get("depth_known", True):
                        _run_ats_pool_cycle(
                            r,
                            pool_label="domain_enrichment",
                            combined_depth=_enrich_data.get("depth", 0),
                            worker_units=ENRICHMENT_WORKERS,
                            hb_prefix="domain_enrichment_worker",
                        )
                    _discov_data = queue_data.get("discovery", {})
                    if _discov_data.get("depth_known", True):
                        _run_ats_pool_cycle(
                            r,
                            pool_label="discovery",
                            combined_depth=_discov_data.get("depth", 0),
                            worker_units=DISCOVERY_WORKERS,
                            hb_prefix="discover_h1b_ats_worker",
                        )
                except Exception as exc:
                    logger.error("manager: ATS pool cycle failed: %s", exc, exc_info=True)

                # ── Mobile relay pool autoscaling (Part 3 — isolated on its own
                # try/except, separate from the ATS block above, so a bug in this
                # brand-new reachability-probe path can never take head_check/
                # domain_enrichment/discovery scaling down with it) ─────────────
                try:
                    _run_mobile_relay_cycle(r)
                except Exception as exc:
                    logger.error("manager: mobile relay pool cycle failed: %s", exc, exc_info=True)

                # ── Update prev_depth for next cycle's inflow_rate snapshot ────
                for pool in pools:
                    _prev_depth[pool] = queue_data.get(pool, {}).get("depth", 0)

                # ── Error spike detection (placeholder) ───────────────────────
                _check_error_spikes(r)

                # ── Nightly recompute (once per local calendar day, Redis-keyed
                # so a manager restart neither repeats nor skips it) ─────────
                if _recompute_due(r):
                    logger.info("manager: triggering nightly Layer 1 recompute")
                    _midnight_recompute(r, pools)
                    r.set("manager:last_recompute_date", _recompute_date())

                    # Refresh bootstrap flag after recompute
                    any_bootstrap = any(
                        _count_daily_peak_records(r, p) < BOOTSTRAP_DAYS_REQUIRED
                        for p in pools
                    )
                    if any_bootstrap:
                        r.set("manager:bootstrap", "1")
                    else:
                        r.delete("manager:bootstrap")

            finally:
                try:
                    r.delete("manager:lock")
                except Exception:
                    pass

        except KeyboardInterrupt:
            logger.info("manager: shutdown requested")
            break
        except Exception as exc:
            logger.error("manager: cycle error: %s", exc, exc_info=True)

        # Heartbeat — written every cycle (including lock-skip cycles)
        try:
            r.set(
                f"worker:alive:manager:{_HOSTNAME}:{os.getpid()}",
                json.dumps({"pid": os.getpid(), "ts": time.time(), "cycles": _cycle_count}),
                ex=_MANAGER_HB_TTL,
            )
        except Exception:
            pass

        # Sleep for remainder of cycle
        elapsed = time.time() - cycle_start
        sleep_s = max(0, MANAGER_CYCLE_S - elapsed)
        time.sleep(sleep_s)


# ─────────────────────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    init_logging("manager")
    run_manager()
