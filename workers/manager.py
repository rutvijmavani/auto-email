"""
workers/manager.py — Autoscaler (Layer 0 + Layer 2)

Design doc: docs/scaling-redesign.md §4, §7, §16

Layer 0 (every 60s): delay + utilization formula → scale up/down/urgent per pool.
Layer 1 (midnight):  midnight recompute of worker_ceil from 28-day daily_peak history.
Layer 2 (event):     Lever 1 backpressure + deadlock detection + worker borrowing.

Worker busy_ms signal:
  Each worker publishes worker:{type}:busy_ms:{pid} after every job.
  Scale-up:   util > 0.80 AND delay > WARN×0.5 (2 consecutive cycles).
  Scale-down: util < 0.50 AND delay < WARN×0.25 (5 consecutive cycles).
  Urgent:     delay >= WARN×0.75 regardless of utilization (1 cycle).

Layer 2 signals:
  manager:lever1:{pool}:active  — set by manager when delay > DELAY_WARN_S;
                                   read by scheduler/workers to halt inflow.
  manager:borrow:{src}:{tgt}    — borrow count; used by effective_target to
                                   prevent Layer 0 undoing borrows each cycle.
  manager:snapshot:{pool}:D/R   — depth + inflow_rate snapshotted at Lever 1
                                   trigger cycle 1 (before Lever 1 acts); used
                                   by learning loop to compute true_required.
"""

import json
import math
import os
import socket
import time
from datetime import datetime, timezone

_HOSTNAME = socket.gethostname()
_MANAGER_HB_TTL = 180  # 3× cycle length; stale after 3 missed cycles

from logger import get_logger, init_logging
from workers.redis_client import get_redis
from config import (
    WORKER_FLOOR,
    MONITOR_MAX_WORKERS,
    DB_POOL_MAXCONN,
    REDIS_POLL_ADAPTIVE,
    REDIS_POLL_FULLSCAN,
    REDIS_DETAIL_ADAPTIVE,
    REDIS_DETAIL_FULLSCAN,
    REDIS_STREAM_FULLSCAN,
    STREAM_CONSUMER_GROUP,
    WORKER_OUTAGE_TTL_S,
    WORKER_CONSEC_REDUCTIONS_THRESHOLD,
    WORKER_CONSEC_REDUCTIONS_TTL,
    CONCURRENCY_ERROR_RATE_REDUCE,
    CONCURRENCY_FLOOR,
    CONCURRENCY_FLOOR_DEFAULT,
    REDIS_CONCURRENCY_LIMIT_PREFIX,
)

logger = get_logger(__name__)

SHADOW_MODE: bool = False

# ── Cycle interval ─────────────────────────────────────────────────────────────
MANAGER_CYCLE_S: int = 60

# ── Bootstrap ceilings (used until 28 days of daily_peak data accumulates) ────
# These match the current production fleet: 10 scan, 6 detail, 5 fullscan.
# Layer 0 scales down within these ceilings naturally via util < 0.50 filter.
BOOTSTRAP_CEIL: dict = {
    "scan":     10,
    "detail":   6,
    "fullscan": 5,
}
BOOTSTRAP_DAYS_REQUIRED: int = 28

# ── DB pool budget (3 reserved for scheduler/manager) ─────────────────────────
_DB_RESERVED: int = 3

# ── Cold-start fallbacks for scaling_params (used until DB has real data) ──────
_FALLBACK_PARAMS: dict = {
    "detail":   {"fetch_p75": 1.5,   "delay_warn_s": 60},
    "scan":     {"fetch_p75": 120.0, "delay_warn_s": 1800},
    "fullscan": {"fetch_p75": 300.0, "delay_warn_s": 7200},
}

# ── Hysteresis cycle counters (in-memory, reset on manager restart) ────────────
_scale_up_cycles:   dict = {"scan": 0, "detail": 0, "fullscan": 0}
_scale_down_cycles: dict = {"scan": 0, "detail": 0, "fullscan": 0}
_urgent_active:     dict = {"scan": False, "detail": False, "fullscan": False}

# ── Layer 2 constants ──────────────────────────────────────────────────────────
RECOVERY_STABILITY_RATIO = 0.25   # delay < WARN × this = stable during recovery
DEADLOCK_HISTORY_CYCLES  = 4      # rolling window for 3-of-4 check
DEADLOCK_RISING_MIN      = 3      # min cycles above WARN in window
LEVER1_STABLE_REQUIRED   = 3      # consecutive stable cycles to lift Lever 1
REINTRO_STABLE_REQUIRED   = 2      # consecutive stable cycles to end re-introduction
REINTRO_DETAIL_BATCH_MAX  = 3      # max detail jobs pushed per scan/fullscan cycle during re-intro

# ── Layer 2 in-memory state (survives individual cycle failures; Redis holds ───
# persistent borrow/lever1/reintro state across manager restarts)
_prev_depth:           dict = {"scan": 0, "detail": 0, "fullscan": 0}
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
            with conn.cursor() as cur:
                # detail: P75 across all api_health requests, last 30 days
                cur.execute("""
                    SELECT PERCENTILE_CONT(0.75) WITHIN GROUP (
                        ORDER BY total_ms / NULLIF(requests_made, 0)
                    ) / 1000.0
                    FROM api_health
                    WHERE date > NOW() - INTERVAL '30 days'
                      AND context = 'normal'
                """)
                row = cur.fetchone()
                if row and row[0] is not None:
                    params["detail"]["fetch_p75"] = float(row[0])

                # scan: P75 of per-company avg_scan_duration_s
                cur.execute("""
                    SELECT PERCENTILE_CONT(0.75) WITHIN GROUP (
                        ORDER BY avg_scan_duration_s
                    )
                    FROM company_poll_stats
                    WHERE avg_scan_duration_s IS NOT NULL
                """)
                row = cur.fetchone()
                if row and row[0] is not None:
                    params["scan"]["fetch_p75"] = float(row[0])

                # fullscan: P75 of per-company avg_fullscan_duration_s
                cur.execute("""
                    SELECT PERCENTILE_CONT(0.75) WITHIN GROUP (
                        ORDER BY avg_fullscan_duration_s
                    )
                    FROM company_poll_stats
                    WHERE avg_fullscan_duration_s IS NOT NULL
                """)
                row = cur.fetchone()
                if row and row[0] is not None:
                    params["fullscan"]["fetch_p75"] = float(row[0])

                # scan DELAY_WARN_S: p10 of current_interval_s × 0.10
                cur.execute("""
                    SELECT PERCENTILE_CONT(0.1) WITHIN GROUP (
                        ORDER BY current_interval_s
                    ) * 0.10
                    FROM company_poll_stats
                    WHERE current_interval_s IS NOT NULL
                """)
                row = cur.fetchone()
                if row and row[0] is not None:
                    params["scan"]["delay_warn_s"] = max(300, int(row[0]))

                # fullscan DELAY_WARN_S: avg(full_scan_interval_s) × 0.10
                cur.execute("""
                    SELECT AVG(full_scan_interval_s) * 0.10
                    FROM company_poll_stats
                    WHERE full_scan_interval_s IS NOT NULL
                """)
                row = cur.fetchone()
                if row and row[0] is not None:
                    params["fullscan"]["delay_warn_s"] = max(600, int(row[0]))

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

def _get_pool_busy_ms(r, pool: str) -> int:
    """
    Sum all worker:{pool}:busy_ms:{pid} keys for this pool.

    Each worker writes its accumulated busy ms in the current 60s window
    (detail_worker, scan_worker, fullscan each publish after every job,
    TTL = 120s so a worker mid-long-job still counts).  SCAN + GET is a
    fast O(keys) operation — typically 2–10 keys per pool.

    Returns 0 if no keys found (workers not yet publishing or all idle).
    """
    total = 0
    cursor = 0
    while True:
        cursor, keys = r.scan(cursor, match=f"worker:{pool}:busy_ms:*", count=20)
        for key in keys:
            val = r.get(key)
            if val:
                try:
                    total += int(val)
                except (ValueError, TypeError):
                    pass
        if cursor == 0:
            break
    return total


# ─────────────────────────────────────────────────────────────────────────────
# Queue metrics
# ─────────────────────────────────────────────────────────────────────────────

def _get_queue_metrics(r) -> dict:
    """
    Return queue_depth and actual_delay_s for each pool.

    detail:   queue_depth = LLEN(adaptive) + LLEN(fullscan)
              actual_delay = now - enqueued_at of LINDEX -1 (oldest item)
              enqueued_at is stored as a timestamp in the payload JSON under "enqueued_at"

    scan:     queue_depth = ZCOUNT(poll:adaptive, -inf, now) — overdue only
              actual_delay = now - score of ZRANGE(0,0) by score (most-overdue company)

    fullscan: queue_depth = ZCOUNT(poll:fullscan, -inf, now) — overdue only
              actual_delay = same pattern
    """
    now = time.time()
    metrics: dict = {}

    # ── detail ────────────────────────────────────────────────────────────────
    try:
        depth_adaptive = r.llen(REDIS_DETAIL_ADAPTIVE)
        depth_fullscan = r.llen(REDIS_DETAIL_FULLSCAN)
        detail_depth = depth_adaptive + depth_fullscan

        # Oldest item is at index -1 (BRPOP side); try to read enqueued_at
        detail_delay = 0.0
        oldest_raw = r.lindex(REDIS_DETAIL_ADAPTIVE, -1) or r.lindex(REDIS_DETAIL_FULLSCAN, -1)
        if oldest_raw:
            try:
                payload = json.loads(oldest_raw)
                enqueued_at = payload.get("enqueued_at")
                if enqueued_at:
                    enqueued_ts = datetime.fromisoformat(enqueued_at).timestamp()
                    detail_delay = max(0.0, now - enqueued_ts)
            except Exception:
                pass

        metrics["detail"] = {"depth": detail_depth, "delay_s": detail_delay}
    except Exception as exc:
        logger.warning("manager: detail queue metrics failed: %s", exc)
        metrics["detail"] = {"depth": 0, "delay_s": 0.0}

    # ── scan ──────────────────────────────────────────────────────────────────
    try:
        scan_depth = r.zcount(REDIS_POLL_ADAPTIVE, "-inf", now)
        scan_delay = 0.0
        most_overdue = r.zrange(REDIS_POLL_ADAPTIVE, 0, 0, withscores=True)
        if most_overdue:
            _, score = most_overdue[0]
            if score < now:
                scan_delay = max(0.0, now - score)
        metrics["scan"] = {"depth": scan_depth, "delay_s": scan_delay}
    except Exception as exc:
        logger.warning("manager: scan queue metrics failed: %s", exc)
        metrics["scan"] = {"depth": 0, "delay_s": 0.0}

    # ── fullscan ──────────────────────────────────────────────────────────────
    try:
        fullscan_depth = r.zcount(REDIS_POLL_FULLSCAN, "-inf", now)
        fullscan_delay = 0.0
        most_overdue_fs = r.zrange(REDIS_POLL_FULLSCAN, 0, 0, withscores=True)
        if most_overdue_fs:
            _, score = most_overdue_fs[0]
            if score < now:
                fullscan_delay = max(0.0, now - score)
        metrics["fullscan"] = {"depth": fullscan_depth, "delay_s": fullscan_delay}
    except Exception as exc:
        logger.warning("manager: fullscan queue metrics failed: %s", exc)
        metrics["fullscan"] = {"depth": 0, "delay_s": 0.0}

    return metrics


# ─────────────────────────────────────────────────────────────────────────────
# Worker ceiling (bootstrap or midnight recompute)
# ─────────────────────────────────────────────────────────────────────────────

def _get_worker_ceil(r, pool: str) -> int:
    """
    Read manager:worker_ceil:{pool} from Redis (set by midnight recompute).
    Falls back to BOOTSTRAP_CEIL until 28 days of daily_peak data exist.
    """
    raw = r.get(f"manager:worker_ceil:{pool}")
    if raw:
        try:
            return max(WORKER_FLOOR, int(raw))
        except (ValueError, TypeError):
            pass
    return BOOTSTRAP_CEIL.get(pool, WORKER_FLOOR)


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


def _midnight_recompute(r, pools: list[str]) -> None:
    """
    Layer 1 midnight recompute.  Runs once per day at midnight.

    In bootstrap mode (< 28 days of daily_peak records per pool):
    - Uses BOOTSTRAP_CEIL instead of formula.
    - Records a daily_peak from today's running max.

    After 28 days:
    - Reads last 28 days of daily_peak records.
    - Applies peak_Nd + max(growth_buffer, volatility_buffer) formula.

    Side effect: resets manager:pool:{type}:daily_peak:running to 0.
    """
    import statistics

    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    for pool in pools:
        try:
            # Snapshot today's running peak before resetting
            running_raw = r.get(f"manager:pool:{pool}:daily_peak:running")
            running_peak = max(int(running_raw or 0), WORKER_FLOOR)
            r.set(f"manager:pool:{pool}:daily_peak:{today}", running_peak)
            r.set(f"manager:pool:{pool}:daily_peak:running", 0)

            n_records = _count_daily_peak_records(r, pool)

            if n_records < BOOTSTRAP_DAYS_REQUIRED:
                # Bootstrap: use fixed starting ceilings, not formula
                ceil_ = BOOTSTRAP_CEIL.get(pool, WORKER_FLOOR)
                r.set(f"manager:worker_ceil:{pool}", ceil_)
                logger.warning(
                    "manager: midnight recompute [%s] bootstrap mode "
                    "(%d/%d days) — ceiling=%d (fixed)",
                    pool, n_records, BOOTSTRAP_DAYS_REQUIRED, ceil_,
                )
                continue

            # Collect all daily_peak records as (date_key, value) tuples.
            # Sort by date descending; take the 28 most recent.
            daily_peaks: list[tuple[str, int]] = []
            cursor = 0
            while True:
                cursor, keys = r.scan(
                    cursor,
                    match=f"manager:pool:{pool}:daily_peak:20*",
                    count=50,
                )
                for k in keys:
                    val = r.get(k)
                    if val:
                        try:
                            date_str = k if isinstance(k, str) else k.decode()
                            daily_peaks.append((date_str, int(val)))
                        except (ValueError, TypeError):
                            pass
                if cursor == 0:
                    break

            # Sort by date desc and keep only the 28 most recent days
            daily_peaks.sort(key=lambda x: x[0], reverse=True)
            daily_peaks = daily_peaks[:28]

            peaks = [v for _, v in daily_peaks]
            peak_nd = max(peaks) if peaks else WORKER_FLOOR

            # growth_buffer: compare recent 7-day peak vs older 21-day baseline.
            # Using the max of the older window (days 8-28) as baseline means
            # positive week-over-week growth produces a non-zero growth_buffer.
            recent_7  = [v for _, v in daily_peaks[:7]]
            baseline  = [v for _, v in daily_peaks[7:]]   # days 8-28

            if len(recent_7) >= 7 and baseline:
                peak_7d      = max(recent_7)
                baseline_val = max(baseline)
                if baseline_val > 0:
                    weekly_growth_rate = max(0.0, (peak_7d - baseline_val) / baseline_val / 3)
                    growth_buffer = math.ceil(peak_nd * weekly_growth_rate)
                else:
                    growth_buffer = 0
            else:
                growth_buffer = 0

            # volatility_buffer: std_dev of last 28 days × 0.25
            if len(peaks) >= 3:
                std = statistics.stdev(peaks)
                volatility_buffer = math.ceil(std * 0.25)
            else:
                volatility_buffer = 0

            buffer_ = max(growth_buffer, volatility_buffer)
            new_ceil = max(WORKER_FLOOR, min(peak_nd + buffer_, MONITOR_MAX_WORKERS))
            r.set(f"manager:worker_ceil:{pool}", new_ceil)

            logger.info(
                "manager: midnight recompute [%s] "
                "peak_28d=%d growth_buf=%d vol_buf=%d → ceil=%d",
                pool, peak_nd, growth_buffer, volatility_buffer, new_ceil,
            )

        except Exception as exc:
            logger.error(
                "manager: midnight recompute [%s] failed: %s", pool, exc, exc_info=True,
            )


# ─────────────────────────────────────────────────────────────────────────────
# Daily peak tracking (intraday)
# ─────────────────────────────────────────────────────────────────────────────

def _update_daily_peak_running(r, pool: str, n_workers: int) -> None:
    """
    If the current cycle had enough workers to show real demand, update the
    running daily peak.  Uses n_workers as proxy for demand (full util signal
    requires busy_ms publishing from workers — not yet implemented).
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
    time_left      = max(delay_warn_s - delay_s, 1)
    drain_rate     = depth / time_left
    workers_target = math.ceil(drain_rate * fetch_p75)

    if delay_s > delay_warn_s:
        workers_target = math.ceil(workers_target * (delay_s / delay_warn_s))

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
    elif (pool_utilization > 0.80
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
    elif (pool_utilization < 0.50
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


def _fire_lever1(r, pool: str, depth: int, prev_depth: int) -> None:
    """
    Activate Lever 1 backpressure for pool.

    Writes manager:lever1:{pool}:active — read by scheduler dispatch loops and
    scan_worker/fullscan before pushing to detail queues.

    Also snapshots D (depth) and R (inflow rate) at the trigger moment.
    These must be captured BEFORE Lever 1 acts, because by cycle 3 the queue
    is already draining and R ≈ 0 — severely understating true demand.
    """
    if _get_lever1_active(r, pool):
        return  # idempotent
    inflow_rate = max(0.0, (depth - prev_depth) / MANAGER_CYCLE_S)
    pipe = r.pipeline()
    pipe.set(f"manager:lever1:{pool}:active", "1")
    pipe.set(f"manager:snapshot:{pool}:D", str(depth), ex=3600)
    pipe.set(f"manager:snapshot:{pool}:R", str(inflow_rate), ex=3600)
    pipe.execute()
    logger.warning(
        "manager [%s]: LEVER 1 FIRED — backpressure active. "
        "depth=%d inflow_rate=%.4f/s",
        pool, depth, inflow_rate,
    )


def _lift_lever1(r, pool: str) -> None:
    """Lift Lever 1 backpressure for pool."""
    r.delete(f"manager:lever1:{pool}:active")
    logger.info("manager [%s]: Lever 1 LIFTED — backpressure cleared", pool)


def _get_effective_target(r, pool: str, workers_target: int) -> int:
    """
    Adjust workers_target by active borrow state to prevent Layer 0 maintenance
    from undoing Layer 2 borrows each cycle.

    effective_target = workers_target - borrowed_out + borrowed_in

    Example — scan lent 2 workers to fullscan:
      effective_target(scan)     = 8 - 2 + 0 = 6  → deficit=0, no re-spawn
      effective_target(fullscan) = 5 - 0 + 2 = 7  → deficit=0, don't remove
    """
    borrowed_out = 0
    cursor = 0
    while True:
        cursor, keys = r.scan(cursor, match=f"manager:borrow:{pool}:*", count=20)
        for key in keys:
            val = r.get(key)
            if val:
                try:
                    borrowed_out += int(val)
                except (ValueError, TypeError):
                    pass
        if cursor == 0:
            break

    borrowed_in = 0
    cursor = 0
    while True:
        cursor, keys = r.scan(cursor, match=f"manager:borrow:*:{pool}", count=20)
        for key in keys:
            k = key.decode() if isinstance(key, bytes) else key
            if not k.endswith(f":{pool}:{pool}"):  # skip self-referential keys
                val = r.get(key)
                if val:
                    try:
                        borrowed_in += int(val)
                    except (ValueError, TypeError):
                        pass
        if cursor == 0:
            break

    return max(WORKER_FLOOR, workers_target - borrowed_out + borrowed_in)


def _get_lendable(r, source_pool: str, n_workers: int, workers_target: int) -> int:
    """
    Workers source_pool can lend all at once — those above its own target and
    above WORKER_FLOOR. Already-borrowed-out workers are subtracted to avoid
    double-lending.

    lendable = n_workers - max(workers_target, WORKER_FLOOR) - already_borrowed_out
    """
    already_borrowed_out = 0
    cursor = 0
    while True:
        cursor, keys = r.scan(cursor, match=f"manager:borrow:{source_pool}:*", count=20)
        for key in keys:
            val = r.get(key)
            if val:
                try:
                    already_borrowed_out += int(val)
                except (ValueError, TypeError):
                    pass
        if cursor == 0:
            break

    min_keep = max(workers_target, WORKER_FLOOR) + already_borrowed_out
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


def _compute_true_required(r, pool: str, params: dict) -> int:
    """
    true_required = ceil((D × F / W) + (R × F))

    Uses the snapshot taken at Lever 1 trigger cycle 1 (before Lever 1 acts).
    D = queue_depth at snapshot, R = inflow_rate at snapshot.
    F = est_fetch_s (P75), W = DELAY_WARN_S.

    Returns 0 if snapshot is missing (e.g. manager restarted between cycles).
    """
    D_raw = r.get(f"manager:snapshot:{pool}:D")
    R_raw = r.get(f"manager:snapshot:{pool}:R")
    if D_raw is None or R_raw is None:
        return 0
    D = float(D_raw)
    R = float(R_raw)
    F = params.get("fetch_p75",    _FALLBACK_PARAMS[pool]["fetch_p75"])
    W = max(params.get("delay_warn_s", _FALLBACK_PARAMS[pool]["delay_warn_s"]), 1)
    required = math.ceil((D * F / W) + (R * F))
    return max(WORKER_FLOOR, required)


def _write_true_required(r, pool: str, n_required: int) -> None:
    """
    Update today's daily_peak record with true_required_workers (learning loop).

    Writes to the per-day record (not directly to peak_Nd watermark) so the
    rolling 28-day window naturally decays the spike after N days.
    """
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    key   = f"manager:pool:{pool}:daily_peak:{today}"
    existing = int(r.get(key) or 0)
    if n_required > existing:
        r.set(key, n_required)
        logger.info(
            "manager [%s]: learning loop — daily_peak updated to %d (was %d)",
            pool, n_required, existing,
        )


def _attempt_borrow(
    r,
    target_pool: str,
    pool_sizes: dict,
    workers_targets: dict,
) -> bool:
    """
    Attempt to resolve a deadlock for target_pool by borrowing workers from
    other pools.  Returns True if any workers were borrowed this cycle.

    Priority for lending: detail > scan > fullscan (highest priority pool
    protected first; only lend from detail if its own Lever 1 is active).

    Phase 1 — Lendable workers (above own target): take ALL at once.
    Phase 2 — Unused capacity (one by one with a cycle gap between each).
    """
    priority   = ["detail", "scan", "fullscan"]
    sources    = [p for p in priority if p != target_pool]
    borrowed   = 0

    # Phase 1: take all lendable (safe — source still meets its own target)
    for source in sources:
        if source == "detail" and not _get_lever1_active(r, "detail"):
            continue  # never borrow from detail unless its own inflow is halted
        src_n   = pool_sizes.get(source, 0)
        src_tgt = workers_targets.get(source, WORKER_FLOOR)
        lendable = _get_lendable(r, source, src_n, src_tgt)
        if lendable > 0:
            _record_borrow(r, source, target_pool, lendable)
            borrowed += lendable

    if borrowed > 0:
        logger.warning(
            "manager: DEADLOCK [%s] — borrowed %d lendable workers. "
            "Waiting 1 cycle to assess.",
            target_pool, borrowed,
        )
        return True

    # Phase 2: one by one from unused capacity (take 1, wait, check next cycle)
    for source in sources:
        if source == "detail" and not _get_lever1_active(r, "detail"):
            continue
        src_n   = pool_sizes.get(source, 0)
        src_tgt = workers_targets.get(source, WORKER_FLOOR)
        already  = int(r.get(f"manager:borrow:{source}:{target_pool}") or 0)
        # unused capacity = what source has above its own target (including WORKER_FLOOR)
        available_above_floor = src_n - already - max(src_tgt, WORKER_FLOOR)
        if available_above_floor > 0:
            _record_borrow(r, source, target_pool, 1)
            logger.warning(
                "manager: DEADLOCK [%s] — borrowed 1 unused-capacity worker from %s. "
                "Waiting 1 cycle.",
                target_pool, source,
            )
            return True

    logger.error(
        "manager: DEADLOCK [%s] — no lendable or unused-capacity workers available; "
        "deadlock cannot be resolved by borrowing",
        target_pool,
    )
    return False


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
      2. Fire Lever 1 if delay crosses DELAY_WARN_S (first crossing only).
         Snapshots D and R before Lever 1 acts (learning loop input).
      3. Check Lever 1 lift: LEVER1_STABLE_REQUIRED consecutive cycles below
         WARN × RECOVERY_STABILITY_RATIO.  On lift: compute true_required and
         update today's daily_peak (learning loop).  Clear all borrow state.
      4. Deadlock detection: if Lever 1 active + pool at ceiling + delay
         rising for 3 of last 4 cycles → attempt worker borrowing.
    """
    delay_warn_s = params.get("delay_warn_s", _FALLBACK_PARAMS[pool]["delay_warn_s"])

    # 1. Track delay history
    delays = _push_delay_history(r, pool, delay_s)

    # 2. Fire Lever 1 on first crossing
    lever1_active = _get_lever1_active(r, pool)
    if not lever1_active and delay_s > delay_warn_s:
        _fire_lever1(r, pool, depth, _prev_depth.get(pool, depth))
        lever1_active = True

    # 3. Lever 1 lift check
    if lever1_active:
        stable_threshold = delay_warn_s * RECOVERY_STABILITY_RATIO
        if delay_s < stable_threshold:
            _lever1_stable_cycles[pool] = _lever1_stable_cycles.get(pool, 0) + 1
        else:
            _lever1_stable_cycles[pool] = 0

        if _lever1_stable_cycles.get(pool, 0) >= LEVER1_STABLE_REQUIRED:
            _lift_lever1(r, pool)
            _lever1_stable_cycles[pool] = 0
            lever1_active = False

            # Learning loop: teach Layer 1 from this incident
            true_req = _compute_true_required(r, pool, params)
            if true_req > 0:
                _write_true_required(r, pool, true_req)

            # Return all borrows — recovery complete
            _return_all_borrows(r, pool)

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

    # 4. Deadlock detection + worker borrowing
    if (lever1_active
            and delay_s > delay_warn_s
            and n_workers + _get_pending_spawns(r, pool) >= worker_ceil
            and _is_deadlock_rising(delays, delay_s, delay_warn_s)):
        _attempt_borrow(r, pool, pool_sizes, workers_targets)


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

    # ── Midnight recompute tracking ────────────────────────────────────────────
    last_midnight_date: str = ""
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

                    _, workers_target = _run_pool_cycle(
                        r, pool, n_workers, pool_busy_ms,
                        depth, delay_s, params, worker_ceil, peak_nd,
                    )
                    all_workers_targets[pool] = workers_target

                    # Update running daily peak
                    _update_daily_peak_running(r, pool, n_workers)

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

                # ── Update prev_depth for next cycle's inflow_rate snapshot ────
                for pool in pools:
                    _prev_depth[pool] = queue_data.get(pool, {}).get("depth", 0)

                # ── Error spike detection (placeholder) ───────────────────────
                _check_error_spikes(r)

                # ── Midnight recompute (once per calendar day) ────────────────
                today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
                current_hour = datetime.now(timezone.utc).hour
                if today_str != last_midnight_date and current_hour == 0:
                    logger.info("manager: triggering midnight Layer 1 recompute")
                    _midnight_recompute(r, pools)
                    last_midnight_date = today_str

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
