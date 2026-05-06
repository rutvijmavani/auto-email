"""
workers/scheduler.py — Priority queue scheduler (Phase 4).

Implements the continuous Redis ZSET scheduler described in Sections 5, 9,
and 17 of the architecture doc.

Two independent loops:
    adaptive_loop()  — every SCHEDULER_TICK_SECS, pops due companies from
                       poll:adaptive and dispatches listing scan jobs
    fullscan_loop()  — every SCHEDULER_TICK_SECS, pops due companies from
                       poll:fullscan and dispatches full scan jobs

Called from pipeline.py --monitor-jobs AFTER email sent + jobs digested:
    from workers.scheduler import record_cycle_start
    record_cycle_start()   ← writes cycle:start to Redis

The adaptive/fullscan workers run as long-lived background processes.
The scheduler loops are run in daemon threads inside run_scheduler().

Pub/Sub:
    pipeline:pause   → pause all dispatching (nightly maintenance)
    pipeline:resume  → resume
    cronchain:alive  → refreshed by nightly chain; expiry = auto-resume
"""

import ctypes
import json
import logging
import math
import multiprocessing
import random
import threading
import time
from datetime import datetime
from typing import Optional

import pytz

from workers.redis_client import get_redis
from workers.adaptive import (
    update_poll_interval, load_poll_counts, dump_poll_counts,
    build_thresholds_from_scores, DEFAULT_THRESHOLDS,
)
from workers.http_client import discover_workday_dc_keys, seed_concurrency_limits
from workers.rebuild import rebuild_redis
from db.db import get_conn, init_db
from config import (
    REDIS_POLL_ADAPTIVE,
    REDIS_POLL_FULLSCAN,
    REDIS_DETAIL_ADAPTIVE,
    REDIS_CYCLE_START,
    REDIS_PAUSE_CHANNEL,
    REDIS_RESUME_CHANNEL,
    REDIS_CRONCHAIN_ALIVE,
    REDIS_DB_MAINTENANCE,
    REDIS_BAND_THRESHOLDS,
    REDIS_CONCURRENCY_LIMIT_PREFIX,
    SCAN_QUEUE,
    SCHEDULER_TICK_SECS,
    SCHEDULER_DAWN_PATROL_WINDOW,
    SCHEDULER_DAWN_PATROL_SPREAD,
    SCHEDULER_FULL_SCAN_BUFFER_S,
    SCHEDULER_FULL_SCAN_INTERVAL_S,
    SCHEDULER_HEARTBEAT_TTL,
    SCHEDULER_FULL_SCAN_LOCK_TTL,
    DETAIL_QUEUE_MAX_ADAPTIVE,
    ADAPTIVE_CALIBRATION_LOOKBACK_DAYS,
    ADAPTIVE_MIN_COMPANIES_CALIBRATE,
    MONITOR_MAX_WORKERS,
    MONITOR_PLATFORM_CONCURRENCY,
    CONCURRENCY_ERROR_RATE_REDUCE,
    CONCURRENCY_FLOOR,
    CONCURRENCY_FLOOR_DEFAULT,
    WORKER_SHUTDOWN_TIMEOUT_S,
    WORKER_FAST_CHECK_INTERVAL_S,
    WORKER_SLOW_CHECK_INTERVAL_S,
    WORKER_POOL_SCAN_FRACTION,
    WORKER_POOL_DETAIL_FRACTION,
    WORKER_FLOOR,
    WORKER_DEPRIORITISE_SECS,
    DETAIL_QUEUE_HIGH_WATERMARK,
    DB_POOL_MAXCONN,
    # Phase 11
    WORKER_ERROR_STREAK_THRESHOLD,
    DETAIL_QUEUE_ALERT_CYCLES,
    REDIS_MEMORY_ALERT_PCT,
    REACTIVATION_LAG_ALERT_HR,
    # Phase 10
    REDIS_BACKOFF_PREFIX,
    WORKER_OUTAGE_TTL_S,
    WORKER_CANARY_INTERVAL_S,
    WORKER_CONSEC_REDUCTIONS_THRESHOLD,
    WORKER_CONSEC_REDUCTIONS_TTL,
    WORKER_SCALING_LOCK_TTL,
    REDIS_INFLIGHT_PREFIX,
    INFLIGHT_STALE_WINDOW_S,
)

logger = logging.getLogger(__name__)

# Shared pause flag — set by pubsub listener, read by dispatch loops
_paused = threading.Event()

# ─────────────────────────────────────────
# BAND THRESHOLD CACHE
# ─────────────────────────────────────────
# Live band thresholds are stored in Redis under REDIS_BAND_THRESHOLDS and
# cached here in-process for 5 minutes so every on_adaptive_complete call
# doesn't hit Redis.  recalibrate_band_thresholds() writes both Redis and
# this cache at once; get_band_thresholds() lazily refreshes the cache.

_thresholds_cache: Optional[dict] = None
_thresholds_cache_ts: float       = 0.0
_THRESHOLDS_CACHE_TTL: int        = 300   # seconds — 5 min

# ─────────────────────────────────────────
# WORKER POOL STATE (Phase 9)
# ─────────────────────────────────────────
# Two co-scheduled multiprocessing.Process pools managed by run_scheduler().
# _scan_pool   — listing scan workers
# _detail_pool — detail fetch workers
# Each entry is (Process, multiprocessing.Event); the Event signals shutdown.
# _pool_lock   — threading.Lock protecting both lists
# _hysteresis  — consecutive-check counters for slow throughput check

_scan_pool:   list = []
_detail_pool: list = []
_pool_lock: threading.Lock = threading.Lock()

_hysteresis: dict = {
    "scan_add":     0,
    "scan_remove":  0,
    "detail_add":   0,
    "detail_remove": 0,
    "detail_alert": 0,   # Phase 11: consecutive cycles with depth > watermark
}

# ── Phase 10 — per-company DC key cache ──────────────────────────────────────
# Populated lazily in _get_dc_key_for_company().  Avoids a DB query on every
# adaptive_loop tick.  Keyed by company name; value is the dispatch throttle
# key (e.g. "greenhouse", "workday_wd12").  Reset on process restart.
_company_dc_key_cache: dict = {}


# ─────────────────────────────────────────
# CYCLE START (called from --monitor-jobs)
# ─────────────────────────────────────────

def record_cycle_start() -> float:
    """
    Write cycle:start Unix timestamp to Redis.

    Called from pipeline.py --monitor-jobs AFTER:
        1. Digest email successfully sent
        2. job_postings status set to 'digested'

    Uses Redis TIME (not system clock) to avoid NTP skew issues.
    Also computes the canonical 7 AM Eastern timestamp for dawn_patrol.

    Returns the cycle start Unix timestamp.
    """
    r = get_redis()

    # Use Redis TIME for the canonical cycle start
    redis_time_s, _ = r.time()   # (seconds, microseconds)
    r.set(REDIS_CYCLE_START, redis_time_s)

    logger.info("scheduler: cycle:start recorded at %s (unix=%d)",
                datetime.fromtimestamp(redis_time_s).isoformat(),
                redis_time_s)

    # Recalibrate band thresholds from today's actual score distribution so
    # all interval decisions this cycle use up-to-date percentile boundaries.
    try:
        recalibrate_band_thresholds(r)
    except Exception as exc:
        logger.warning(
            "scheduler: band threshold recalibration failed: %s "
            "-- using cached / default thresholds", exc,
        )

    return float(redis_time_s)


def get_cycle_start() -> Optional[float]:
    """Return today's cycle start Unix timestamp, or None if not set."""
    r   = get_redis()
    val = r.get(REDIS_CYCLE_START)
    return float(val) if val else None


# ─────────────────────────────────────────
# BAND THRESHOLD CALIBRATION
# ─────────────────────────────────────────

def recalibrate_band_thresholds(r) -> dict:
    """
    Recompute adaptive band thresholds from the current score distribution.

    Queries adaptive_score from company_poll_stats for all companies that
    have been polled within the last ADAPTIVE_CALIBRATION_LOOKBACK_DAYS days.
    Only companies with score > 0 (i.e. have posted at least one new job
    recently) participate in percentile ranking -- dormant companies (score=0)
    always get ADAPTIVE_DEFAULT_INTERVAL (12h) and are excluded.

    If fewer than ADAPTIVE_MIN_COMPANIES_PERCENTILE active companies exist,
    falls back to DEFAULT_THRESHOLDS so the engine still runs sensibly with
    a small portfolio.

    Stores the thresholds in Redis (REDIS_BAND_THRESHOLDS hash) and updates
    the in-process cache so the next on_adaptive_complete call sees them
    immediately without a cache miss.

    Called:
        - At record_cycle_start() (once per day, after digest is sent)
        - At run_scheduler() startup (so a fresh process calibrates before
          its first dispatch)

    Returns:
        dict with keys "low", "moderate", "active" (the live thresholds).
    """
    global _thresholds_cache, _thresholds_cache_ts

    conn = get_conn()
    try:
        rows = conn.execute("""
            SELECT adaptive_score
            FROM company_poll_stats
            WHERE adaptive_score > 0
              AND last_poll_at IS NOT NULL
              AND last_poll_at > NOW() - (%s * INTERVAL '1 day')
        """, (ADAPTIVE_CALIBRATION_LOOKBACK_DAYS,)).fetchall()
    finally:
        conn.close()

    scores = [float(row["adaptive_score"]) for row in rows]

    thresholds = build_thresholds_from_scores(scores)

    if thresholds is None:
        logger.info(
            "scheduler: band calibration: only %d active companies "
            "(need >= %d) -- using DEFAULT_THRESHOLDS "
            "(low=%.1f moderate=%.1f active=%.1f)",
            len(scores), ADAPTIVE_MIN_COMPANIES_CALIBRATE,
            DEFAULT_THRESHOLDS["low"],
            DEFAULT_THRESHOLDS["moderate"],
            DEFAULT_THRESHOLDS["active"],
        )
        thresholds = DEFAULT_THRESHOLDS.copy()
    else:
        logger.info(
            "scheduler: band calibration: %d active companies -> "
            "low=%.3f (bottom 50%%) moderate=%.3f (top 25%%) active=%.3f (top 10%%)",
            len(scores),
            thresholds["low"], thresholds["moderate"], thresholds["active"],
        )

    # Persist to Redis (no TTL -- refreshed daily at cycle start)
    r.hset(REDIS_BAND_THRESHOLDS, mapping={
        "low":           str(thresholds["low"]),
        "moderate":      str(thresholds["moderate"]),
        "active":        str(thresholds["active"]),
        "calibrated_at": str(int(time.time())),
        "n_companies":   str(len(scores)),
    })

    # Update in-process cache immediately (avoids cache miss on first call)
    _thresholds_cache    = thresholds
    _thresholds_cache_ts = time.time()

    return thresholds


def get_band_thresholds(r) -> dict:
    """
    Return current band thresholds for use in band_lookup / get_max_interval.

    Reads from the in-process cache (refreshed every 5 min or after
    recalibration).  Falls back to DEFAULT_THRESHOLDS if Redis has no
    calibrated values yet (e.g. very first run before cycle start).

    This is called on every on_adaptive_complete() invocation and must be
    fast -- the cache makes it ~0 ns on cache hit, one Redis HGETALL on miss.

    Returns:
        dict with keys "low", "moderate", "active".
    """
    global _thresholds_cache, _thresholds_cache_ts

    now = time.time()
    if _thresholds_cache and (now - _thresholds_cache_ts) < _THRESHOLDS_CACHE_TTL:
        return _thresholds_cache

    # Cache miss — reload from Redis
    raw = r.hgetall(REDIS_BAND_THRESHOLDS)
    if raw and all(k in raw for k in ("low", "moderate", "active")):
        _thresholds_cache = {
            "low":      float(raw["low"]),
            "moderate": float(raw["moderate"]),
            "active":   float(raw["active"]),
        }
        logger.debug(
            "scheduler: band thresholds reloaded from Redis "
            "(low=%.3f moderate=%.3f active=%.3f n=%s calibrated=%s)",
            _thresholds_cache["low"],
            _thresholds_cache["moderate"],
            _thresholds_cache["active"],
            raw.get("n_companies", "?"),
            raw.get("calibrated_at", "?"),
        )
    else:
        # No calibration in Redis yet -- use defaults silently
        _thresholds_cache = DEFAULT_THRESHOLDS.copy()

    _thresholds_cache_ts = now
    return _thresholds_cache


# ─────────────────────────────────────────
# DAWN PATROL (Rule 2)
# ─────────────────────────────────────────

def dawn_patrol() -> int:
    """
    Redistribute adaptive polls that are due after DAWN_PATROL_WINDOW
    into the first DAWN_PATROL_SPREAD seconds of the cycle.

    Ensures dormant companies aren't clustered in the afternoon —
    all companies get their adaptive poll before the full scan runs.

    Returns number of companies redistributed.
    """
    r           = get_redis()
    cycle_start = get_cycle_start()
    if cycle_start is None:
        logger.warning("dawn_patrol: no cycle:start in Redis — skipping")
        return 0

    threshold = cycle_start + SCHEDULER_DAWN_PATROL_WINDOW
    late      = r.zrangebyscore(REDIS_POLL_ADAPTIVE, threshold, "+inf",
                                withscores=False)
    if not late:
        logger.debug("dawn_patrol: no late polls to redistribute")
        return 0

    spread = SCHEDULER_DAWN_PATROL_SPREAD
    step   = spread / max(len(late), 1)

    new_scores = {}
    for i, company in enumerate(late):
        new_time = cycle_start + (i * step) + random.uniform(0, step)
        new_scores[company] = new_time

    r.zadd(REDIS_POLL_ADAPTIVE, new_scores)

    logger.info("dawn_patrol: redistributed %d companies across 2h window",
                len(late))
    return len(late)


# ─────────────────────────────────────────
# HEARTBEAT
# ─────────────────────────────────────────

def set_heartbeat(company: str) -> None:
    """Set worker heartbeat for a company (TTL = SCHEDULER_HEARTBEAT_TTL)."""
    get_redis().set(f"heartbeat:{company}", "processing",
                    ex=SCHEDULER_HEARTBEAT_TTL)


def clear_heartbeat(company: str) -> None:
    """Clear worker heartbeat on completion."""
    get_redis().delete(f"heartbeat:{company}")


def set_progress(company: str, step: str, ttl: int = 120) -> None:
    """Update progress heartbeat for hung worker detection."""
    get_redis().set(f"progress:{company}", step, ex=ttl)


# ─────────────────────────────────────────
# ADAPTIVE COMPLETION HANDLER (Rule 3)
# ─────────────────────────────────────────

def on_adaptive_complete(company: str, new_jobs: int,
                         success: bool = True) -> None:
    """
    Called after a listing scan worker completes.

    Steps:
        1. Load current stats from DB
        2. Run adaptive interval engine
        3. Write updated stats back to DB + Redis stats cache
        4. Reschedule company in poll:adaptive (never orphaned)
        5. If full scan is due (elapsed >= full_scan_interval_s):
               ZADD poll:fullscan with 5-min buffer (Rule 3)

    Args:
        company:  company name
        new_jobs: new jobs found in this scan
        success:  False if scan failed (API error)
    """
    r   = get_redis()
    now = time.time()

    # ── Phase 10: release inflight slot ──────────────────────────────────────
    dc_key = _get_dc_key_for_company(company)
    r.zrem(f"{REDIS_INFLIGHT_PREFIX}:{dc_key}", company)

    # ── Phase 10: canary recovery detection ──────────────────────────────────
    # Check if this company was the canary probe for its platform.  If so,
    # use the scan result to decide whether to clear outage mode early.
    platform           = dc_key.split("_")[0] if "_" in dc_key else dc_key
    canary_company_key = f"worker:outage:canary_company:{platform}"
    canary_company     = r.get(canary_company_key)
    if canary_company == company:
        outage_key = f"worker:outage:{platform}"
        if success:
            # Canary succeeded → platform recovered, exit outage early
            r.delete(outage_key)
            r.delete(canary_company_key)
            r.delete(f"worker:outage:canary_sent:{platform}")
            r.delete(f"worker:consec_reductions:{platform}")
            logger.info(
                "on_adaptive_complete: CANARY SUCCESS for %r — "
                "outage mode cleared early",
                platform,
            )
            from db.api_health import record_scaling_event
            n_scan, n_detail = _get_pool_snapshot()
            record_scaling_event(
                "outage_end",
                trigger_layer="fast_error",
                platform=platform,
                dc_key=dc_key,
                scan_workers_before=n_scan,
                scan_workers_after=n_scan,
                detail_workers_before=n_detail,
                detail_workers_after=n_detail,
                notes=f"canary company={company} recovered early",
            )
        else:
            # Canary failed → platform still down, reset TTL for another hour
            r.set(outage_key, "1", ex=WORKER_OUTAGE_TTL_S)
            r.delete(canary_company_key)
            r.delete(f"worker:outage:canary_sent:{platform}")
            logger.warning(
                "on_adaptive_complete: CANARY FAILED for %r — "
                "outage TTL reset to %ds",
                platform, WORKER_OUTAGE_TTL_S,
            )

    conn = get_conn()
    try:
        row = conn.execute("""
            SELECT current_interval_s, recent_poll_counts,
                   last_full_scan_at, full_scan_interval_s,
                   consecutive_errors, last_success_at
            FROM company_poll_stats
            WHERE company = ?
        """, (company,)).fetchone()
    finally:
        conn.close()

    if not row:
        logger.warning("on_adaptive_complete: no poll_stats row for %r", company)
        _reschedule_adaptive(company, 86400)
        return

    if success:
        counts     = load_poll_counts(row["recent_poll_counts"])
        thresholds = get_band_thresholds(r)
        result     = update_poll_interval(counts, row["current_interval_s"], new_jobs,
                                          thresholds=thresholds)
        interval   = result["current_interval_s"]
        consec_errors = 0
    else:
        # On failure: keep current interval, increment error counter
        interval      = row["current_interval_s"]
        result        = {
            "recent_poll_counts": load_poll_counts(row["recent_poll_counts"]),
            "adaptive_score":     0.0,
            "current_interval_s": interval,
        }
        consec_errors = (row["consecutive_errors"] or 0) + 1

    next_poll_at = datetime.fromtimestamp(now + interval)

    # Persist to DB
    conn = get_conn()
    try:
        conn.execute("""
            UPDATE company_poll_stats SET
                recent_poll_counts = ?,
                adaptive_score     = ?,
                current_interval_s = ?,
                last_poll_at       = CURRENT_TIMESTAMP,
                next_poll_at       = ?,
                consecutive_empty  = CASE WHEN ? > 0 THEN 0
                                     ELSE consecutive_empty + 1 END,
                consecutive_errors = ?,
                last_success_at    = CASE WHEN ? THEN CURRENT_TIMESTAMP
                                     ELSE last_success_at END,
                last_error_at      = CASE WHEN ? THEN CURRENT_TIMESTAMP
                                     ELSE last_error_at END,
                updated_at         = CURRENT_TIMESTAMP
            WHERE company = ?
        """, (
            dump_poll_counts(result["recent_poll_counts"]),
            result["adaptive_score"],
            interval,
            next_poll_at,
            new_jobs,             # for consecutive_empty CASE
            consec_errors,
            success,              # for last_success_at CASE
            not success,          # for last_error_at CASE
            company,
        ))
        conn.commit()
    finally:
        conn.close()

    # Update stats cache in Redis
    r.hset(f"stats:{company}", mapping={
        "adaptive_score":     str(result["adaptive_score"]),
        "current_interval_s": str(interval),
        "recent_poll_counts": dump_poll_counts(result["recent_poll_counts"]),
        "last_poll_at":       datetime.fromtimestamp(now).isoformat(),
    })

    # Reschedule adaptive (Rule 3 part 1: never orphaned)
    _reschedule_adaptive(company, interval)
    clear_heartbeat(company)

    logger.info(
        "on_adaptive_complete: company=%r new_jobs=%d interval=%ds "
        "next_poll=%s success=%s",
        company, new_jobs, interval,
        next_poll_at.strftime("%H:%M"), success,
    )

    # ── Phase 11: error streak alert ─────────────────────────────────────────
    # Fire a WARNING when a company has failed WORKER_ERROR_STREAK_THRESHOLD
    # consecutive scans.  Uses company as the platform dedup key so each
    # company gets its own 24h dedup window (not shared with other companies
    # on the same ATS platform).
    if not success and consec_errors >= WORKER_ERROR_STREAK_THRESHOLD:
        try:
            from db.pipeline_alerts import (
                create_alert, ALERT_ERROR_STREAK, WARNING,
            )
            alert_id = create_alert(
                alert_type = ALERT_ERROR_STREAK,
                severity   = WARNING,
                platform   = company,   # dedup per company
                value      = float(consec_errors),
                threshold  = float(WORKER_ERROR_STREAK_THRESHOLD),
                message    = (
                    f"{company} ({platform}): {consec_errors} consecutive "
                    f"scan failures — may need manual investigation"
                ),
            )
            if alert_id:
                logger.warning(
                    "on_adaptive_complete: ERROR STREAK alert created "
                    "company=%r platform=%r consec_errors=%d (alert_id=%s)",
                    company, platform, consec_errors, alert_id,
                )
        except Exception as exc:
            logger.debug(
                "on_adaptive_complete: error streak alert failed: %s", exc
            )

    # ── Phase 11: reactivation lag alert ────────────────────────────────────
    # When a company just recovered (success=True after consecutive failures),
    # check how long it was dark.  A lag above REACTIVATION_LAG_ALERT_HR means
    # we likely missed a polling window and jobs may have gone undetected.
    #
    # Uses last_success_at from the DB row read BEFORE this successful scan
    # so the timestamp reflects the end of the dark period, not now.
    # Deduped per company (same 24h window as error_streak).
    if success and row.get("consecutive_errors") and row["consecutive_errors"] > 0:
        last_ok = row.get("last_success_at")
        if last_ok is not None:
            try:
                lag_s  = now - last_ok.timestamp()
                lag_hr = lag_s / 3600.0
                if lag_hr > REACTIVATION_LAG_ALERT_HR:
                    from db.pipeline_alerts import (
                        create_alert, ALERT_REACTIVATION_LAG, WARNING,
                    )
                    alert_id = create_alert(
                        alert_type = ALERT_REACTIVATION_LAG,
                        severity   = WARNING,
                        platform   = company,   # dedup per company
                        value      = round(lag_hr, 1),
                        threshold  = float(REACTIVATION_LAG_ALERT_HR),
                        message    = (
                            f"{company} ({platform}): recovered after "
                            f"{lag_hr:.1f}h dark "
                            f"({row['consecutive_errors']} consecutive failures) — "
                            f"possible missed polling window"
                        ),
                    )
                    if alert_id:
                        logger.warning(
                            "on_adaptive_complete: REACTIVATION LAG alert "
                            "company=%r platform=%r lag_hr=%.1f consec_errors=%d "
                            "(alert_id=%s)",
                            company, platform, lag_hr,
                            row["consecutive_errors"], alert_id,
                        )
            except Exception as exc:
                logger.debug(
                    "on_adaptive_complete: reactivation lag check failed: %s", exc
                )

    # Rule 3 part 2: trigger full scan if due
    if success and _should_trigger_full_scan(company, row):
        _schedule_full_scan(company)


def _reschedule_adaptive(company: str, interval_s: int) -> None:
    """Add company back to poll:adaptive ZSET with its new interval."""
    score = time.time() + interval_s
    get_redis().zadd(REDIS_POLL_ADAPTIVE, {company: score})


def _should_trigger_full_scan(company: str, row) -> bool:
    """
    Return True if a full scan should be triggered for this company.

    Conditions (Rule 3):
        - Never been full-scanned (last_full_scan_at IS NULL), OR
        - full_scan_interval_s has elapsed since last full scan

    Suppressed when the company has an active scan backoff key
    (retry:backoff:scan:{company}).  A full scan makes significantly more
    requests than a listing scan — triggering one while the ATS is already
    struggling accelerates the problem.
    """
    # Suppress full scan if listing scan is in backoff (ATS struggling)
    r = get_redis()
    if r.exists(f"{REDIS_BACKOFF_PREFIX}:scan:{company}"):
        logger.debug(
            "_should_trigger_full_scan: %r has active scan backoff — "
            "suppressing full scan trigger",
            company,
        )
        return False

    if row["last_full_scan_at"] is None:
        return True   # never been full-scanned
    elapsed  = time.time() - row["last_full_scan_at"].timestamp()
    interval = row["full_scan_interval_s"] or SCHEDULER_FULL_SCAN_INTERVAL_S
    return elapsed >= interval


def _schedule_full_scan(company: str) -> None:
    """Queue company for full scan with 5-minute buffer (Rule 3)."""
    score = time.time() + SCHEDULER_FULL_SCAN_BUFFER_S
    get_redis().zadd(REDIS_POLL_FULLSCAN, {company: score})
    logger.info("scheduler: full scan queued for %r (in %ds)",
                company, SCHEDULER_FULL_SCAN_BUFFER_S)


# ─────────────────────────────────────────
# ADAPTIVE DISPATCH LOOP
# ─────────────────────────────────────────

def adaptive_loop() -> None:
    """
    Continuous adaptive scheduler loop.

    Every SCHEDULER_TICK_SECS:
        1. Check if paused (pipeline:pause)
        2. ZPOPMIN poll:adaptive — get companies due now
        3. For each: set heartbeat + dispatch to SCAN_QUEUE
        4. Result consumer reads scan:results and calls on_adaptive_complete

    Runs until KeyboardInterrupt or thread stop.
    """
    r = get_redis()
    logger.info("adaptive_loop: started")

    while True:
        try:
            if _paused.is_set():
                time.sleep(SCHEDULER_TICK_SECS)
                _check_auto_resume()
                continue

            now = time.time()

            # Pop all companies due right now (score <= now)
            due = r.zrangebyscore(REDIS_POLL_ADAPTIVE, "-inf", now,
                                  withscores=False)

            for company in due:
                # Atomic pop — prevent double dispatch
                removed = r.zrem(REDIS_POLL_ADAPTIVE, company)
                if not removed:
                    continue   # another scheduler instance beat us

                # ── Backpressure: detail queue overloaded ─────────────────────
                depth = r.llen(REDIS_DETAIL_ADAPTIVE)
                if depth > DETAIL_QUEUE_MAX_ADAPTIVE:
                    r.zadd(REDIS_POLL_ADAPTIVE, {company: now + 30})
                    logger.debug(
                        "adaptive_loop: backpressure company=%r queue_depth=%d",
                        company, depth,
                    )
                    continue

                # ── Phase 10: outage mode check ───────────────────────────────
                dc_key   = _get_dc_key_for_company(company)
                platform = dc_key.split("_")[0] if "_" in dc_key else dc_key

                outage_key = f"worker:outage:{platform}"
                if r.exists(outage_key):
                    outage_ttl = r.ttl(outage_key)
                    # Canary window: dispatch ONE company when outage_ttl has
                    # dropped below WORKER_CANARY_INTERVAL_S (30 min mark).
                    # Guard with canary_sent flag so only one fires per window.
                    canary_sent_key   = f"worker:outage:canary_sent:{platform}"
                    canary_company_key = f"worker:outage:canary_company:{platform}"
                    canary_due        = (0 < outage_ttl < WORKER_CANARY_INTERVAL_S)
                    canary_sent       = r.exists(canary_sent_key)

                    if canary_due and not canary_sent:
                        # Let this one company through as the canary probe.
                        # Record which company so on_adaptive_complete can detect recovery.
                        r.set(canary_sent_key,    "1",     ex=WORKER_OUTAGE_TTL_S)
                        r.set(canary_company_key, company, ex=WORKER_OUTAGE_TTL_S)
                        logger.info(
                            "adaptive_loop: CANARY dispatch company=%r platform=%r "
                            "(outage_ttl=%ds)",
                            company, platform, outage_ttl,
                        )
                        from db.api_health import record_scaling_event
                        n_scan, n_detail = _get_pool_snapshot()
                        record_scaling_event(
                            "canary_probe",
                            trigger_layer="fast_error",
                            platform=platform,
                            dc_key=dc_key,
                            scan_workers_before=n_scan,
                            scan_workers_after=n_scan,
                            detail_workers_before=n_detail,
                            detail_workers_after=n_detail,
                            notes=f"canary company={company} outage_ttl_remaining={outage_ttl}s",
                        )
                        # Fall through to dispatch (canary)
                    else:
                        # Normal outage suppression — push company forward
                        delay = max(outage_ttl, 60) if outage_ttl > 0 else 60
                        r.zadd(REDIS_POLL_ADAPTIVE, {company: now + delay})
                        continue

                # ── Phase 10: per-DC learned ceiling throttle ─────────────────
                ceiling_raw = r.get(f"worker:ceil:learned:{dc_key}")
                if ceiling_raw:
                    # Stale cleanup before counting (removes crashed-worker entries)
                    stale_cutoff = now - INFLIGHT_STALE_WINDOW_S
                    r.zremrangebyscore(
                        f"{REDIS_INFLIGHT_PREFIX}:{dc_key}", 0, stale_cutoff
                    )
                    inflight = r.zcard(f"{REDIS_INFLIGHT_PREFIX}:{dc_key}")
                    if inflight >= int(ceiling_raw):
                        # At ceiling — hold briefly, retry next tick
                        r.zadd(REDIS_POLL_ADAPTIVE, {company: now + 30})
                        logger.debug(
                            "adaptive_loop: ceiling throttle company=%r dc=%r "
                            "inflight=%d ceil=%s",
                            company, dc_key, inflight, ceiling_raw,
                        )
                        continue

                set_heartbeat(company)

                # Track in-flight for learned-ceiling enforcement
                r.zadd(f"{REDIS_INFLIGHT_PREFIX}:{dc_key}", {company: now})

                # context: 'canary' when dispatched as an outage probe so
                # api_health rows from this scan are tagged separately and
                # excluded from baseline queries (WHERE context = 'normal').
                canary_sent_key = f"worker:outage:canary_company:{platform}"
                dispatch_context = (
                    "canary"
                    if r.get(canary_sent_key) == company
                    else "normal"
                )

                payload = json.dumps({
                    "company":     company,
                    "scan_type":   "adaptive",
                    "enqueued_at": datetime.utcnow().isoformat(),
                    "request_id":  f"adp-{int(now)}",
                    "dc_key":      dc_key,
                    "context":     dispatch_context,
                })
                r.lpush(SCAN_QUEUE, payload)

                logger.debug("adaptive_loop: dispatched %r (dc=%s)", company, dc_key)

            time.sleep(SCHEDULER_TICK_SECS)

        except KeyboardInterrupt:
            logger.info("adaptive_loop: stopping")
            break
        except Exception as exc:
            logger.error("adaptive_loop: error: %s", exc, exc_info=True)
            time.sleep(5)


# ─────────────────────────────────────────
# RESULT CONSUMER (reads scan:results)
# ─────────────────────────────────────────

def result_consumer_loop() -> None:
    """
    Reads completion events from scan:results and calls on_adaptive_complete.

    Runs continuously alongside the adaptive dispatch loop.
    """
    r = get_redis()
    logger.info("result_consumer_loop: started")

    while True:
        try:
            item = r.brpop("scan:results", timeout=5)
            if item is None:
                continue

            _, raw = item
            result = json.loads(raw)

            company   = result.get("company", "")
            new_jobs  = result.get("new_jobs", 0)
            success   = result.get("success", False)
            scan_type = result.get("scan_type", "adaptive")

            if scan_type == "adaptive" and company:
                on_adaptive_complete(company, new_jobs, success)

        except KeyboardInterrupt:
            logger.info("result_consumer_loop: stopping")
            break
        except Exception as exc:
            logger.error("result_consumer_loop: error: %s", exc, exc_info=True)
            time.sleep(1)


# ─────────────────────────────────────────
# PAUSE / RESUME (Pub/Sub)
# ─────────────────────────────────────────

def pubsub_listener_loop() -> None:
    """
    Listens on pipeline:pause and pipeline:resume channels.
    Sets/clears the shared _paused Event used by dispatch loops.
    """
    r      = get_redis()
    pubsub = r.pubsub()
    pubsub.subscribe(REDIS_PAUSE_CHANNEL, REDIS_RESUME_CHANNEL)
    logger.info("pubsub_listener: subscribed to pause/resume channels")

    for message in pubsub.listen():
        if message["type"] != "message":
            continue
        channel = message["channel"]
        if channel == REDIS_PAUSE_CHANNEL:
            logger.info("pubsub_listener: PAUSE received — halting dispatchers")
            _paused.set()
        elif channel == REDIS_RESUME_CHANNEL:
            logger.info("pubsub_listener: RESUME received — resuming dispatchers")
            _paused.clear()


def _check_auto_resume() -> None:
    """
    Auto-resume if cronchain:alive expired AND db:maintenance not set.
    Prevents workers being paused forever if nightly chain crashed.
    (Section 18 — Workers paused — never forever)
    """
    r = get_redis()
    cron_alive     = r.exists(REDIS_CRONCHAIN_ALIVE)
    db_maintenance = r.exists(REDIS_DB_MAINTENANCE)

    if not cron_alive and not db_maintenance:
        logger.warning(
            "scheduler: cron chain heartbeat expired and no db:maintenance flag — "
            "auto-resuming workers (cron chain may have crashed)"
        )
        _paused.clear()


# ─────────────────────────────────────────
# DYNAMIC WORKER POOLS (Phase 9+10 — Section 9)
# ─────────────────────────────────────────

def _get_dc_key_for_company(company: str) -> str:
    """
    Return the dispatch-throttle key for a company.

    Non-Workday platforms: key = platform name (e.g. "greenhouse").
    Workday: key = "workday_{dc}" derived from ats_slug["wd"] field.

    Cached in _company_dc_key_cache to avoid per-tick DB queries.
    Falls back to "unknown" on any error — the inflight check is skipped
    gracefully when the key is unknown (no learned ceiling exists).
    """
    global _company_dc_key_cache
    if company in _company_dc_key_cache:
        return _company_dc_key_cache[company]

    dc_key = "unknown"
    try:
        import json as _json
        conn = get_conn()
        try:
            row = conn.execute("""
                SELECT ats_platform, ats_slug
                FROM prospective_companies
                WHERE company = %s
            """, (company,)).fetchone()
        finally:
            conn.close()

        if row:
            platform = row["ats_platform"] or "unknown"
            if platform in ("workday", "workdaysites"):
                try:
                    slug = _json.loads(row["ats_slug"] or "{}")
                    wd   = slug.get("wd")
                    dc_key = f"workday_{wd}" if wd else "workday_default"
                except Exception:
                    dc_key = "workday_default"
            else:
                dc_key = platform
    except Exception as exc:
        logger.debug("_get_dc_key_for_company %r: %s", company, exc)

    _company_dc_key_cache[company] = dc_key
    return dc_key


def _get_pool_snapshot() -> tuple:
    """Return (n_scan, n_detail) current pool sizes — thread-safe."""
    with _pool_lock:
        return len(_scan_pool), len(_detail_pool)


def calculate_worker_counts(r) -> tuple:
    """
    Calculate required scan and detail worker counts from 30-day api_health history.

    Called at scheduler startup (after dawn_patrol).  Queries historical
    average response times and today's expected workload to give a data-driven
    starting point.  The three monitoring layers (liveness, fast error, slow
    throughput) then fine-tune the counts throughout the day.

    Formula (Section 9):
        scan_workers_needed   = ceil(scan_polls_today × avg_listing_scan_s / window_s)
        detail_workers_needed = ceil(expected_new_jobs × avg_detail_fetch_s / window_s)

    Both counts are clamped to [WORKER_FLOOR, db_pool_ceil].

    Returns:
        (scan_count, detail_count) — both ints >= WORKER_FLOOR
    """
    window_s = 23 * 3600   # 23-hour working window

    # DB pool ceiling split 60/40 (no workers running yet at startup)
    db_budget   = DB_POOL_MAXCONN - 3   # 3 reserved for scheduler + maintenance
    scan_ceil   = max(WORKER_FLOOR, int(db_budget * WORKER_POOL_SCAN_FRACTION))
    detail_ceil = max(WORKER_FLOOR, int(db_budget * WORKER_POOL_DETAIL_FRACTION))

    # Number of companies registered for adaptive polling today
    scan_polls_today = max(1, r.zcard(REDIS_POLL_ADAPTIVE))

    # ── Average listing scan time ─────────────────────────────────────────────
    avg_listing_scan_s = 3.0   # fallback: 3s per listing scan
    try:
        from db.api_health import query_30day_avg_response_ms
        ms_samples = []
        for platform in ("greenhouse", "lever", "ashby", "workday", "smartrecruiters"):
            ms = query_30day_avg_response_ms(platform)
            if ms > 0:
                ms_samples.append(ms)
        if ms_samples:
            avg_listing_scan_s = (sum(ms_samples) / len(ms_samples)) / 1000.0
    except Exception as exc:
        logger.warning(
            "calculate_worker_counts: avg_listing_scan query failed: %s — "
            "using %.1fs fallback", exc, avg_listing_scan_s,
        )

    # ── Expected new jobs per day ─────────────────────────────────────────────
    expected_new_jobs = float(scan_polls_today) * 2.0   # fallback: 2 per company
    try:
        conn = get_conn()
        try:
            row = conn.execute("""
                SELECT COUNT(*)::float / 30.0 AS daily_avg
                FROM job_postings
                WHERE created_at >= NOW() - INTERVAL '30 days'
            """).fetchone()
        finally:
            conn.close()
        if row and row["daily_avg"]:
            expected_new_jobs = max(1.0, float(row["daily_avg"]))
    except Exception as exc:
        logger.warning(
            "calculate_worker_counts: expected_new_jobs query failed: %s — "
            "using %.0f fallback", exc, expected_new_jobs,
        )

    # ── Average detail fetch time (Mode B platforms) ──────────────────────────
    avg_detail_fetch_s = 2.0   # fallback: 2s per detail fetch
    try:
        from db.api_health import query_30day_avg_response_ms
        mode_b_samples = []
        for platform in ("workday", "icims", "jobvite"):
            ms = query_30day_avg_response_ms(platform)
            if ms > 0:
                mode_b_samples.append(ms)
        if mode_b_samples:
            avg_detail_fetch_s = (sum(mode_b_samples) / len(mode_b_samples)) / 1000.0
    except Exception as exc:
        logger.warning(
            "calculate_worker_counts: avg_detail_fetch query failed: %s — "
            "using %.1fs fallback", exc, avg_detail_fetch_s,
        )

    # ── Apply formula ─────────────────────────────────────────────────────────
    scan_workers_needed   = math.ceil(
        (scan_polls_today * avg_listing_scan_s) / window_s
    )
    detail_workers_needed = math.ceil(
        (expected_new_jobs * avg_detail_fetch_s) / window_s
    )

    scan_count   = max(WORKER_FLOOR, min(scan_workers_needed, scan_ceil))
    detail_count = max(WORKER_FLOOR, min(detail_workers_needed, detail_ceil))

    logger.info(
        "calculate_worker_counts: polls=%d avg_scan=%.2fs "
        "expected_new_jobs=%.0f avg_detail=%.2fs "
        "→ scan=%d (ceil=%d) detail=%d (ceil=%d)",
        scan_polls_today, avg_listing_scan_s,
        expected_new_jobs, avg_detail_fetch_s,
        scan_count, scan_ceil, detail_count, detail_ceil,
    )
    return scan_count, detail_count


def _remaining_work_minimum(r) -> int:
    """
    Compute the minimum scan workers needed to finish today's remaining companies.

    Dynamic floor that shrinks as the day progresses so we never reduce the
    scan pool below what's required to drain the queue before midnight.

    Formula (Section 9):
        min_workers = ceil((remaining_companies × avg_listing_scan_s) / remaining_window_s)

    Returns WORKER_FLOOR if cycle_start is unknown or the day window has ended.
    """
    cycle_start = get_cycle_start()
    if cycle_start is None:
        return WORKER_FLOOR

    now             = time.time()
    cycle_end       = cycle_start + 23 * 3600
    remaining_s     = max(300.0, cycle_end - now)   # at least 5 min — avoids ÷0

    # Companies still in the adaptive queue (includes those due later today)
    remaining       = r.zcount(REDIS_POLL_ADAPTIVE, now, cycle_end + 3600)

    if not remaining:
        return WORKER_FLOOR

    avg_scan_s = 3.0   # consistent with calculate_worker_counts fallback
    return max(WORKER_FLOOR, math.ceil(
        (remaining * avg_scan_s) / remaining_s
    ))


# ── Process target functions ──────────────────────────────────────────────────

def _reset_inherited_db_pool() -> None:
    """
    Discard any PostgreSQL connection pool inherited from the parent process.

    multiprocessing.Process forks the parent, so the child inherits the
    parent's _pool which holds open TCP sockets to PostgreSQL.  Both parent
    and child would then share the same file descriptors, causing libpq to
    emit "error with status PGRES_TUPLES_OK and no message from the libpq"
    when the child tries to use or re-initialise the connection.

    Calling this at the top of every worker-process target function ensures
    the child creates its own fresh pool on first get_conn() call.
    """
    import db.connection as _dbc
    if _dbc._pool is not None:
        try:
            _dbc._pool.closeall()
        except Exception:
            pass
        _dbc._pool = None


def _scan_worker_process(shutdown_event: multiprocessing.Event) -> None:
    """
    Target function for scan worker processes (multiprocessing.Process).

    Runs workers/scan_worker.run_worker() in this process.
    A daemon watcher thread injects KeyboardInterrupt into the main thread
    when shutdown_event is set — the run_worker() loop's
    `except KeyboardInterrupt: break` then exits cleanly.
    """
    _reset_inherited_db_pool()

    def _watcher() -> None:
        shutdown_event.wait()
        # Inject KeyboardInterrupt into this process's main thread.
        # PyThreadState_SetAsyncExc is CPython-specific but stable; it safely
        # interrupts a blocking BLPOP call at the next Python opcode boundary.
        ctypes.pythonapi.PyThreadState_SetAsyncExc(
            ctypes.c_ulong(threading.main_thread().ident),
            ctypes.py_object(KeyboardInterrupt),
        )

    threading.Thread(target=_watcher, daemon=True, name="scan_shutdown_watcher").start()

    from workers.scan_worker import run_worker
    run_worker(shutdown_event=shutdown_event, skip_init_db=True)


def _detail_worker_process(shutdown_event: multiprocessing.Event) -> None:
    """
    Target function for detail worker processes (multiprocessing.Process).

    Mirrors _scan_worker_process but runs detail_worker.run_worker().
    """
    _reset_inherited_db_pool()

    def _watcher() -> None:
        shutdown_event.wait()
        ctypes.pythonapi.PyThreadState_SetAsyncExc(
            ctypes.c_ulong(threading.main_thread().ident),
            ctypes.py_object(KeyboardInterrupt),
        )

    threading.Thread(target=_watcher, daemon=True, name="detail_shutdown_watcher").start()

    from workers.detail_worker import run_worker
    run_worker(shutdown_event=shutdown_event, skip_init_db=True)


# ── Pool management helpers ───────────────────────────────────────────────────

def _spawn_worker(worker_type: str) -> tuple:
    """
    Spawn one worker process of the given type.

    Args:
        worker_type: "scan" or "detail"

    Returns:
        (multiprocessing.Process, multiprocessing.Event) — process handle and
        its shutdown event.  Store this tuple in the pool list.
    """
    shutdown_event = multiprocessing.Event()
    target         = _scan_worker_process if worker_type == "scan" \
                     else _detail_worker_process
    proc = multiprocessing.Process(
        target = target,
        args   = (shutdown_event,),
        name   = f"{worker_type}_worker_{int(time.time())}",
        daemon = False,   # non-daemon: scheduler waits for clean exit at shutdown
    )
    proc.start()
    logger.info("scheduler: spawned %s_worker pid=%d", worker_type, proc.pid)
    return proc, shutdown_event


def _replace_dead_workers() -> None:
    """
    Liveness check (Layer 1) — replace crashed worker processes immediately.

    Called from _liveness_check_loop() every ~5 seconds.  If a process has
    died unexpectedly (not via our shutdown signal), a fresh replacement is
    spawned and added to the same pool to maintain the target headcount.

    Phase 10: emits record_scaling_event("worker_add", trigger_layer="liveness")
    for each replacement.  The event is recorded AFTER releasing _pool_lock
    so the DB write never blocks the pool management lock.
    """
    global _scan_pool, _detail_pool
    replacements: list = []   # (ptype, old_pid, exitcode) — collected under lock

    with _pool_lock:
        for pool, ptype in ((_scan_pool, "scan"), (_detail_pool, "detail")):
            for i, (proc, event) in enumerate(pool):
                if not proc.is_alive():
                    logger.warning(
                        "scheduler: %s_worker pid=%d died unexpectedly "
                        "(exit=%s) — spawning replacement",
                        ptype, proc.pid, proc.exitcode,
                    )
                    replacements.append((ptype, proc.pid, proc.exitcode))
                    pool[i] = _spawn_worker(ptype)

    # Emit scaling events outside the lock — DB write must not hold _pool_lock
    if replacements:
        from db.api_health import record_scaling_event
        n_scan, n_detail = _get_pool_snapshot()
        for ptype, old_pid, old_exit in replacements:
            record_scaling_event(
                "worker_add",
                trigger_layer="liveness",
                worker_type=ptype,
                scan_workers_before=n_scan,
                scan_workers_after=n_scan,
                detail_workers_before=n_detail,
                detail_workers_after=n_detail,
                notes=(
                    f"replaced crashed {ptype}_worker "
                    f"pid={old_pid} exitcode={old_exit}"
                ),
            )


def _add_one_worker(worker_type: str, ceil_: int) -> bool:
    """
    Add one worker to the given pool if below its ceiling.

    Args:
        worker_type: "scan" or "detail"
        ceil_:       maximum pool size for this worker type right now

    Returns:
        True if a worker was added, False if already at ceil_.
    """
    global _scan_pool, _detail_pool
    pool = _scan_pool if worker_type == "scan" else _detail_pool

    with _pool_lock:
        if len(pool) >= ceil_:
            return False
        pool.append(_spawn_worker(worker_type))

    return True


def _remove_one_worker(worker_type: str, floor_: int) -> bool:
    """
    Remove one worker from the given pool if above its floor.

    Signals the LAST entry to finish its current job and exit cleanly.
    The entry is removed from the pool list immediately so the liveness
    check doesn't spawn a replacement for it.

    Args:
        worker_type: "scan" or "detail"
        floor_:      minimum pool size for this worker type right now

    Returns:
        True if a worker was removed, False if already at floor_.
    """
    global _scan_pool, _detail_pool
    pool = _scan_pool if worker_type == "scan" else _detail_pool

    with _pool_lock:
        if len(pool) <= floor_:
            return False
        proc, event = pool.pop()

    event.set()
    logger.info(
        "scheduler: removing %s_worker pid=%d (shutdown event set, "
        "waiting for it to finish current job)",
        worker_type, proc.pid,
    )
    return True


def _deprioritise_platform(r, platform: str) -> int:
    """
    Push all companies for a given ATS platform forward in poll:adaptive.

    Called by _fast_error_check_loop() when a platform's error rate is high
    but its concurrency limit is already at floor (errors are not concurrency-
    induced).  Pushing companies forward gives the platform breathing room to
    recover before their next scan is attempted.

    Only companies currently in poll:adaptive are affected, and only those
    not already pushed far into the future (to avoid cascading delays).

    Returns:
        Number of companies pushed forward.
    """
    conn = get_conn()
    try:
        rows = conn.execute("""
            SELECT company FROM prospective_companies
            WHERE ats_platform = %s
        """, (platform,)).fetchall()
    finally:
        conn.close()

    if not rows:
        return 0

    now            = time.time()
    deprioritised  = 0
    max_score_push = now + WORKER_DEPRIORITISE_SECS * 3   # don't push beyond 15 min

    for row in rows:
        company = row["company"]
        score   = r.zscore(REDIS_POLL_ADAPTIVE, company)
        if score is not None and score < max_score_push:
            r.zincrby(REDIS_POLL_ADAPTIVE, WORKER_DEPRIORITISE_SECS, company)
            deprioritised += 1

    if deprioritised:
        logger.info(
            "scheduler: deprioritised %d %r companies (+%ds in poll:adaptive)",
            deprioritised, platform, WORKER_DEPRIORITISE_SECS,
        )
    return deprioritised


# ── Monitoring threads ────────────────────────────────────────────────────────

def _liveness_check_loop() -> None:
    """
    Layer 1: liveness monitor — checks worker processes every ~5 seconds.

    A crashed worker is replaced immediately to maintain target pool sizes.
    Runs as a daemon thread alongside the adaptive and throughput monitors.
    """
    logger.info("liveness_check_loop: started")
    while True:
        try:
            time.sleep(5)
            _replace_dead_workers()
        except KeyboardInterrupt:
            logger.info("liveness_check_loop: stopping")
            break
        except Exception as exc:
            logger.error("liveness_check_loop: error: %s", exc, exc_info=True)
            time.sleep(10)


def _fast_error_check_loop() -> None:
    """
    Layer 2: fast error monitor — runs every WORKER_FAST_CHECK_INTERVAL_S (5 min).

    Phase 10 full logic (Section 9 — ATS outage detection):

    For each platform per cycle:
      1. If in outage mode → skip (canary dispatch is handled in adaptive_loop).
      2. Check effectiveness of the PREVIOUS reduction (if a before_rate snapshot
         exists from last cycle): if error rate improved → reset consec_reductions
         and update learned ceiling; if not → increment consec_reductions.
      3. If consec_reductions reaches WORKER_CONSEC_REDUCTIONS_THRESHOLD → declare
         outage, workers untouched, log outage_start event.
      4. If error rate is above threshold AND concurrency is at floor → two-lever:
         a. Snapshot before_rate for next-cycle effectiveness check.
         b. Set scaling lock (prevents slow check from undoing this).
         c. Update learned ceiling (inflight at error time - 1).
         d. Remove one scan worker.
         e. Deprioritise platform companies in poll:adaptive.
         f. Record scaling event.
    """
    from workers.http_client import get_error_rate, get_baseline_error_rate
    from db.api_health import record_scaling_event

    r = get_redis()
    logger.info("fast_error_check_loop: started")

    while True:
        try:
            time.sleep(WORKER_FAST_CHECK_INTERVAL_S)

            for platform in MONITOR_PLATFORM_CONCURRENCY:
                # ── Skip platforms in outage mode ─────────────────────────────
                if r.exists(f"worker:outage:{platform}"):
                    continue

                error_rate    = get_error_rate(r, platform)
                baseline_rate = get_baseline_error_rate(r, platform)
                spike_factor  = (error_rate / (baseline_rate + 0.001)
                                 if baseline_rate > 0.0 else None)

                # ── Check effectiveness of previous reduction ─────────────────
                before_rate_key = f"worker:reduction:before_rate:{platform}"
                before_raw      = r.get(before_rate_key)
                if before_raw is not None:
                    before_rate = float(before_raw)
                    r.delete(before_rate_key)

                    if error_rate <= CONCURRENCY_ERROR_RATE_REDUCE:
                        # Reduction was effective — reset counter
                        r.delete(f"worker:consec_reductions:{platform}")
                        # Update learned ceiling: inflight count at improvement time
                        dc_key       = platform   # platform-level ceiling update
                        stale_cutoff = time.time() - INFLIGHT_STALE_WINDOW_S
                        r.zremrangebyscore(
                            f"{REDIS_INFLIGHT_PREFIX}:{dc_key}", 0, stale_cutoff
                        )
                        inflight = r.zcard(f"{REDIS_INFLIGHT_PREFIX}:{dc_key}")
                        new_ceil = max(WORKER_FLOOR, inflight)
                        r.set(f"worker:ceil:learned:{dc_key}", new_ceil)
                        r.set(f"worker:ceil:last_error:{dc_key}", int(time.time()))
                        logger.info(
                            "fast_error_check: platform=%r reduction EFFECTIVE "
                            "(%.1f%% → %.1f%%) — learned ceiling set to %d",
                            platform, before_rate * 100, error_rate * 100, new_ceil,
                        )
                        n_scan, n_detail = _get_pool_snapshot()
                        record_scaling_event(
                            "ceiling_learned",
                            trigger_layer="fast_error",
                            platform=platform,
                            dc_key=dc_key,
                            scan_workers_before=n_scan,
                            scan_workers_after=n_scan,
                            detail_workers_before=n_detail,
                            detail_workers_after=n_detail,
                            error_rate=error_rate,
                            baseline_error_rate=baseline_rate if baseline_rate > 0 else None,
                            spike_factor=spike_factor,
                            inflight_count=inflight,
                            learned_ceiling=new_ceil,
                            notes=f"effective reduction: {before_rate*100:.1f}% → {error_rate*100:.1f}%",
                        )
                    else:
                        # Reduction was NOT effective — increment consec counter
                        count = r.incr(f"worker:consec_reductions:{platform}")
                        r.expire(
                            f"worker:consec_reductions:{platform}",
                            WORKER_CONSEC_REDUCTIONS_TTL,
                        )
                        logger.warning(
                            "fast_error_check: platform=%r reduction INEFFECTIVE "
                            "(%.1f%% → %.1f%%) consec_reductions=%d",
                            platform, before_rate * 100, error_rate * 100, count,
                        )

                        if count >= WORKER_CONSEC_REDUCTIONS_THRESHOLD:
                            # Outage detected — pause dispatching for this platform
                            r.set(
                                f"worker:outage:{platform}", "1",
                                ex=WORKER_OUTAGE_TTL_S,
                            )
                            r.delete(f"worker:consec_reductions:{platform}")
                            logger.warning(
                                "fast_error_check: OUTAGE DECLARED for platform=%r "
                                "after %d consecutive ineffective reductions — "
                                "dispatching paused for %ds",
                                platform, count, WORKER_OUTAGE_TTL_S,
                            )
                            n_scan, n_detail = _get_pool_snapshot()
                            record_scaling_event(
                                "outage_start",
                                trigger_layer="fast_error",
                                platform=platform,
                                scan_workers_before=n_scan,
                                scan_workers_after=n_scan,
                                detail_workers_before=n_detail,
                                detail_workers_after=n_detail,
                                error_rate=error_rate,
                                baseline_error_rate=baseline_rate if baseline_rate > 0 else None,
                                spike_factor=spike_factor,
                                consec_reductions=count,
                            )
                            continue   # skip reduction this cycle; outage mode handles it

                # ── Check if action is needed this cycle ──────────────────────
                if error_rate <= CONCURRENCY_ERROR_RATE_REDUCE:
                    continue

                floor   = CONCURRENCY_FLOOR.get(platform, CONCURRENCY_FLOOR_DEFAULT)
                raw     = r.get(f"{REDIS_CONCURRENCY_LIMIT_PREFIX}:{platform}")
                current = int(raw) if raw is not None else floor + 1

                if current > floor:
                    # Concurrency still above floor → feedback loop is handling it
                    continue

                # Concurrency at floor AND errors still high → worker-level response
                logger.warning(
                    "fast_error_check: platform=%r error_rate=%.1f%% "
                    "concurrency at floor=%d — reducing workers",
                    platform, error_rate * 100, floor,
                )

                dynamic_floor = _remaining_work_minimum(r)
                n_scan, n_detail = _get_pool_snapshot()

                # Snapshot before_rate for next-cycle effectiveness check
                r.set(
                    f"worker:reduction:before_rate:{platform}",
                    str(error_rate),
                    ex=WORKER_FAST_CHECK_INTERVAL_S * 3,
                )

                # Set scaling lock — prevents slow check from adding workers back
                r.set(
                    f"worker:scaling_lock:{platform}", "1",
                    ex=WORKER_SCALING_LOCK_TTL,
                )

                removed = _remove_one_worker("scan", dynamic_floor)
                _deprioritise_platform(r, platform)

                n_scan_after, n_detail_after = _get_pool_snapshot()
                record_scaling_event(
                    "worker_remove",
                    trigger_layer="fast_error",
                    platform=platform,
                    worker_type="scan",
                    scan_workers_before=n_scan,
                    scan_workers_after=n_scan_after,
                    detail_workers_before=n_detail,
                    detail_workers_after=n_detail_after,
                    error_rate=error_rate,
                    baseline_error_rate=baseline_rate if baseline_rate > 0 else None,
                    spike_factor=spike_factor,
                    scan_queue_depth=r.llen(SCAN_QUEUE),
                    detail_queue_depth=r.llen(REDIS_DETAIL_ADAPTIVE),
                    notes=f"dynamic_floor={dynamic_floor} removed={removed}",
                )

        except KeyboardInterrupt:
            logger.info("fast_error_check_loop: stopping")
            break
        except Exception as exc:
            logger.error("fast_error_check_loop: error: %s", exc, exc_info=True)
            time.sleep(60)


def _slow_throughput_check_loop() -> None:
    """
    Layer 3: slow throughput monitor — runs every WORKER_SLOW_CHECK_INTERVAL_S (30 min).

    Phase 10 additions:
      - Checks worker:scaling_lock:{platform} before adding workers — prevents
        this loop from undoing a deliberate fast_error reduction.
      - Learned ceiling decay: after 24h clean operation for a DC key, increments
        the ceiling by 1 (probes upward toward true safe maximum).
      - Emits record_scaling_event for every worker add/remove.

    Cascade logic (Section 9):
        1. detail queue > HIGH_WATERMARK  → add detail worker (up to detail_ceil)
        2. detail queue > HIGH_WATERMARK AND detail at ceil  → block scan growth;
           if queue still growing (2nd consecutive check) → remove scan worker
        3. detail queue healthy AND scan backlog  → add scan worker (up to scan_ceil)
        4. Queues draining AND scan pool > dynamic_floor  → remove scan worker

    DB pool ceiling is recalculated on every check to account for pool changes.
    """
    global _hysteresis

    from db.api_health import record_scaling_event

    r = get_redis()
    logger.info("slow_throughput_check_loop: started")

    while True:
        try:
            time.sleep(WORKER_SLOW_CHECK_INTERVAL_S)

            detail_depth  = r.llen(REDIS_DETAIL_ADAPTIVE)
            scan_backlog  = r.zcard(REDIS_POLL_ADAPTIVE)
            n_scan, n_detail = _get_pool_snapshot()

            db_budget    = DB_POOL_MAXCONN - 3
            scan_ceil    = max(WORKER_FLOOR,
                               int((db_budget - n_detail) * WORKER_POOL_SCAN_FRACTION))
            detail_ceil  = max(WORKER_FLOOR,
                               int((db_budget - n_scan) * WORKER_POOL_DETAIL_FRACTION))
            dynamic_floor = _remaining_work_minimum(r)

            # ── Phase 11: Redis memory alert ──────────────────────────────────
            # Check used_memory vs maxmemory on every slow-check cycle.
            # Under noeviction policy a full Redis stops accepting writes —
            # poll queues and seen-sets would silently drop entries.
            # maxmemory=0 means no limit is configured — skip check.
            try:
                mem_info   = r.info("memory")
                used_mem   = mem_info.get("used_memory", 0)
                max_mem    = mem_info.get("maxmemory", 0)
                if max_mem and max_mem > 0:
                    used_pct = int(100 * used_mem / max_mem)
                    if used_pct >= REDIS_MEMORY_ALERT_PCT:
                        from db.pipeline_alerts import (
                            create_alert, ALERT_REDIS_MEMORY, CRITICAL,
                        )
                        alert_id = create_alert(
                            alert_type = ALERT_REDIS_MEMORY,
                            severity   = CRITICAL,
                            value      = float(used_pct),
                            threshold  = float(REDIS_MEMORY_ALERT_PCT),
                            message    = (
                                f"Redis memory at {used_pct}% of maxmemory "
                                f"({used_mem // 1024 // 1024} MB / "
                                f"{max_mem // 1024 // 1024} MB) — "
                                f"noeviction policy will block writes at 100%"
                            ),
                        )
                        if alert_id:
                            logger.critical(
                                "slow_throughput: REDIS MEMORY alert "
                                "used=%dMB max=%dMB pct=%d%% (alert_id=%s)",
                                used_mem // 1024 // 1024,
                                max_mem  // 1024 // 1024,
                                used_pct, alert_id,
                            )
            except Exception as exc:
                logger.debug("slow_throughput: redis memory check failed: %s", exc)

            # ── Phase 10: learned ceiling decay ───────────────────────────────
            # For each known DC key, if 24h have passed since the last error
            # event, increment the ceiling by 1 (probe upward toward true max).
            now = time.time()
            cursor = 0
            while True:
                cursor, keys = r.scan(cursor, match="worker:ceil:learned:*", count=50)
                for key in keys:
                    dc_key   = key.split("worker:ceil:learned:", 1)[1]
                    last_err = r.get(f"worker:ceil:last_error:{dc_key}")
                    if last_err and (now - float(last_err)) > 86400:
                        old_ceil = int(r.get(key) or WORKER_FLOOR)
                        new_ceil = min(old_ceil + 1, MONITOR_MAX_WORKERS)
                        r.set(key, new_ceil)
                        logger.info(
                            "slow_throughput: ceil:learned relaxed for dc=%r "
                            "%d → %d (24h clean)",
                            dc_key, old_ceil, new_ceil,
                        )
                        record_scaling_event(
                            "ceiling_learned",
                            trigger_layer="slow_throughput",
                            dc_key=dc_key,
                            learned_ceiling=new_ceil,
                            notes=f"24h decay from {old_ceil}",
                        )
                if cursor == 0:
                    break

            # ── Phase 11: detail queue depth alert ────────────────────────────
            # Track how many consecutive slow-check cycles the queue has stayed
            # above the watermark.  If it hasn't drained in DETAIL_QUEUE_ALERT_CYCLES
            # cycles (default 3 × 30 min = 90 min), something is stuck.
            if detail_depth > DETAIL_QUEUE_HIGH_WATERMARK:
                _hysteresis["detail_alert"] += 1
                if _hysteresis["detail_alert"] >= DETAIL_QUEUE_ALERT_CYCLES:
                    try:
                        from db.pipeline_alerts import (
                            create_alert, ALERT_DETAIL_QUEUE_DEPTH, WARNING,
                        )
                        alert_id = create_alert(
                            alert_type = ALERT_DETAIL_QUEUE_DEPTH,
                            severity   = WARNING,
                            value      = float(detail_depth),
                            threshold  = float(DETAIL_QUEUE_HIGH_WATERMARK),
                            message    = (
                                f"detail:adaptive queue depth {detail_depth} "
                                f"has exceeded watermark {DETAIL_QUEUE_HIGH_WATERMARK} "
                                f"for {_hysteresis['detail_alert']} consecutive "
                                f"slow-check cycles "
                                f"({_hysteresis['detail_alert'] * WORKER_SLOW_CHECK_INTERVAL_S // 60} min)"
                            ),
                        )
                        if alert_id:
                            logger.warning(
                                "slow_throughput: DETAIL QUEUE DEPTH alert "
                                "depth=%d watermark=%d cycles=%d (alert_id=%s)",
                                detail_depth, DETAIL_QUEUE_HIGH_WATERMARK,
                                _hysteresis["detail_alert"], alert_id,
                            )
                    except Exception as exc:
                        logger.debug(
                            "slow_throughput: detail queue alert failed: %s", exc
                        )
            else:
                _hysteresis["detail_alert"] = 0

            # ── Detail cascade ─────────────────────────────────────────────────
            if detail_depth > DETAIL_QUEUE_HIGH_WATERMARK:
                _hysteresis["detail_add"]    += 1
                _hysteresis["detail_remove"]  = 0

                if _hysteresis["detail_add"] >= 2:
                    if n_detail < detail_ceil:
                        if _add_one_worker("detail", detail_ceil):
                            n_scan_a, n_detail_a = _get_pool_snapshot()
                            logger.info(
                                "slow_throughput: detail_queue=%d > watermark=%d "
                                "→ added detail_worker (pool now %d)",
                                detail_depth, DETAIL_QUEUE_HIGH_WATERMARK, n_detail_a,
                            )
                            record_scaling_event(
                                "worker_add",
                                trigger_layer="slow_throughput",
                                worker_type="detail",
                                scan_workers_before=n_scan,
                                scan_workers_after=n_scan_a,
                                detail_workers_before=n_detail,
                                detail_workers_after=n_detail_a,
                                detail_queue_depth=detail_depth,
                                scan_queue_depth=scan_backlog,
                            )
                            _hysteresis["detail_add"] = 0
                    else:
                        # detail at ceiling — cascade: shed a scan worker
                        _hysteresis["scan_remove"] += 1
                        _hysteresis["scan_add"]     = 0

                        if _hysteresis["scan_remove"] >= 2:
                            if _remove_one_worker("scan", dynamic_floor):
                                n_scan_a, n_detail_a = _get_pool_snapshot()
                                logger.info(
                                    "slow_throughput: detail at ceil=%d queue=%d "
                                    "→ removed scan_worker to free capacity "
                                    "(pool now %d)",
                                    detail_ceil, detail_depth, n_scan_a,
                                )
                                record_scaling_event(
                                    "worker_remove",
                                    trigger_layer="cascade",
                                    worker_type="scan",
                                    scan_workers_before=n_scan,
                                    scan_workers_after=n_scan_a,
                                    detail_workers_before=n_detail,
                                    detail_workers_after=n_detail_a,
                                    detail_queue_depth=detail_depth,
                                    scan_queue_depth=scan_backlog,
                                    notes=f"detail_ceil={detail_ceil}",
                                )
                                _hysteresis["scan_remove"] = 0
            else:
                _hysteresis["detail_add"] = 0

                # ── Scan growth (respects scaling lock) ────────────────────────
                if scan_backlog > 0 and n_scan < scan_ceil:
                    # Check if any platform has an active scaling lock
                    any_locked = any(
                        r.exists(f"worker:scaling_lock:{p}")
                        for p in MONITOR_PLATFORM_CONCURRENCY
                    )
                    if any_locked:
                        logger.debug(
                            "slow_throughput: scan growth suppressed — "
                            "active scaling lock present"
                        )
                        _hysteresis["scan_add"] = 0
                    else:
                        _hysteresis["scan_add"]    += 1
                        _hysteresis["scan_remove"]  = 0

                        if _hysteresis["scan_add"] >= 2:
                            if _add_one_worker("scan", scan_ceil):
                                n_scan_a, n_detail_a = _get_pool_snapshot()
                                logger.info(
                                    "slow_throughput: scan_backlog=%d detail healthy "
                                    "→ added scan_worker (pool now %d)",
                                    scan_backlog, n_scan_a,
                                )
                                record_scaling_event(
                                    "worker_add",
                                    trigger_layer="slow_throughput",
                                    worker_type="scan",
                                    scan_workers_before=n_scan,
                                    scan_workers_after=n_scan_a,
                                    detail_workers_before=n_detail,
                                    detail_workers_after=n_detail_a,
                                    detail_queue_depth=detail_depth,
                                    scan_queue_depth=scan_backlog,
                                )
                                _hysteresis["scan_add"] = 0
                else:
                    _hysteresis["scan_add"] = 0

                    # ── Idle contraction ───────────────────────────────────────
                    if n_scan > dynamic_floor:
                        _hysteresis["scan_remove"] += 1

                        if _hysteresis["scan_remove"] >= 2:
                            if _remove_one_worker("scan", dynamic_floor):
                                n_scan_a, n_detail_a = _get_pool_snapshot()
                                logger.info(
                                    "slow_throughput: scan_pool=%d > "
                                    "dynamic_floor=%d queues draining "
                                    "→ removed scan_worker (pool now %d)",
                                    n_scan, dynamic_floor, n_scan_a,
                                )
                                record_scaling_event(
                                    "worker_remove",
                                    trigger_layer="slow_throughput",
                                    worker_type="scan",
                                    scan_workers_before=n_scan,
                                    scan_workers_after=n_scan_a,
                                    detail_workers_before=n_detail,
                                    detail_workers_after=n_detail_a,
                                    detail_queue_depth=detail_depth,
                                    scan_queue_depth=scan_backlog,
                                    notes=f"dynamic_floor={dynamic_floor} idle_contraction",
                                )
                                _hysteresis["scan_remove"] = 0
                    else:
                        _hysteresis["scan_remove"] = 0

            logger.debug(
                "slow_throughput: scan=%d/%d detail=%d/%d "
                "detail_q=%d scan_backlog=%d dynamic_floor=%d",
                n_scan, scan_ceil, n_detail, detail_ceil,
                detail_depth, scan_backlog, dynamic_floor,
            )

        except KeyboardInterrupt:
            logger.info("slow_throughput_check_loop: stopping")
            break
        except Exception as exc:
            logger.error("slow_throughput_check_loop: error: %s", exc, exc_info=True)
            time.sleep(120)


def _shutdown_worker_pools() -> None:
    """
    Gracefully stop all worker processes (called at KeyboardInterrupt).

    Steps (Section 9 — graceful shutdown):
        1. Set every worker's shutdown Event — workers finish their current
           job then break out of their BLPOP loop.
        2. Join each process with WORKER_SHUTDOWN_TIMEOUT_S deadline.
        3. Force-kill any process that hasn't exited by the deadline.
    """
    logger.info(
        "scheduler: initiating graceful shutdown of %d scan + %d detail workers",
        len(_scan_pool), len(_detail_pool),
    )

    with _pool_lock:
        all_entries = [("scan",   _scan_pool),
                       ("detail", _detail_pool)]

        # Pass 1 — signal all workers to stop
        for ptype, pool in all_entries:
            for proc, event in pool:
                if proc.is_alive():
                    event.set()
                    logger.debug(
                        "scheduler: shutdown event set for %s_worker pid=%d",
                        ptype, proc.pid,
                    )

        # Pass 2 — join with shared deadline, SIGKILL stragglers
        deadline = time.time() + WORKER_SHUTDOWN_TIMEOUT_S
        for ptype, pool in all_entries:
            for proc, event in pool:
                remaining = max(0.5, deadline - time.time())
                proc.join(timeout=remaining)
                if proc.is_alive():
                    logger.warning(
                        "scheduler: %s_worker pid=%d still alive after %ds — "
                        "sending SIGKILL",
                        ptype, proc.pid, WORKER_SHUTDOWN_TIMEOUT_S,
                    )
                    try:
                        proc.kill()   # SIGKILL on Unix / TerminateProcess on Windows
                    except Exception:
                        pass

    logger.info("scheduler: all worker pools shut down")


# ─────────────────────────────────────────
# FULL SCHEDULER STARTUP
# ─────────────────────────────────────────

def run_scheduler(skip_rebuild: bool = False) -> None:
    """
    Start the full adaptive polling scheduler (Phase 9).

    Runs indefinitely:
        1. Rebuilds Redis from PostgreSQL on startup (unless skip_rebuild)
        2. Calibrates band thresholds and seeds concurrency limits
        3. Runs dawn_patrol() to redistribute late adaptive polls
        4. Calculates data-driven startup worker counts from 30-day history
        5. Spawns two co-scheduled multiprocessing.Process pools:
               scan_pool   — listing scan workers
               detail_pool — detail fetch workers
        6. Starts five daemon threads:
               adaptive_loop          — dispatches companies from poll:adaptive
               result_consumer_loop   — calls on_adaptive_complete on results
               pubsub_listener_loop   — handles pause/resume pub/sub
               _liveness_check_loop   — Layer 1: replace dead workers every 5s
               _fast_error_check_loop — Layer 2: error-triggered scaling every 5m
               _slow_throughput_check_loop — Layer 3: throughput scaling every 30m
        7. On Ctrl+C: graceful shutdown → SIGKILL stragglers after 30s

    Args:
        skip_rebuild: pass True if Redis is already warm (dev restarts)
    """
    init_db()

    if not skip_rebuild:
        rebuild_redis()

    r = get_redis()

    # ── Band threshold calibration ────────────────────────────────────────────
    try:
        recalibrate_band_thresholds(r)
    except Exception as exc:
        logger.warning(
            "scheduler: startup band calibration failed: %s "
            "-- using cached / default thresholds", exc,
        )

    # ── Concurrency limit seeding ─────────────────────────────────────────────
    try:
        dc_keys = discover_workday_dc_keys()
        seed_concurrency_limits(r, dc_keys)
    except Exception as exc:
        logger.warning(
            "scheduler: concurrency limit seeding failed: %s "
            "-- workers will self-seed on first request", exc,
        )

    # ── Dawn patrol ───────────────────────────────────────────────────────────
    dawn_patrol()

    # ── Calculate initial worker counts ──────────────────────────────────────
    try:
        scan_count, detail_count = calculate_worker_counts(r)
    except Exception as exc:
        logger.warning(
            "scheduler: calculate_worker_counts failed: %s "
            "— falling back to MONITOR_MAX_WORKERS / 2", exc,
        )
        half = max(WORKER_FLOOR, MONITOR_MAX_WORKERS // 2)
        scan_count = detail_count = half

    # ── Spawn worker pools ────────────────────────────────────────────────────
    global _scan_pool, _detail_pool
    with _pool_lock:
        for _ in range(scan_count):
            _scan_pool.append(_spawn_worker("scan"))
        for _ in range(detail_count):
            _detail_pool.append(_spawn_worker("detail"))

    logger.info(
        "scheduler: spawned %d scan_workers + %d detail_workers",
        scan_count, detail_count,
    )

    # ── Start daemon threads ──────────────────────────────────────────────────
    threads = [
        threading.Thread(target=adaptive_loop,
                         name="adaptive_loop",           daemon=True),
        threading.Thread(target=result_consumer_loop,
                         name="result_consumer",         daemon=True),
        threading.Thread(target=pubsub_listener_loop,
                         name="pubsub_listener",         daemon=True),
        threading.Thread(target=_liveness_check_loop,
                         name="liveness_check",          daemon=True),
        threading.Thread(target=_fast_error_check_loop,
                         name="fast_error_check",        daemon=True),
        threading.Thread(target=_slow_throughput_check_loop,
                         name="slow_throughput_check",   daemon=True),
    ]

    for t in threads:
        t.start()

    logger.info("scheduler: all loops started — %d threads + %d processes",
                len(threads), scan_count + detail_count)
    print(f"[scheduler] Running — "
          f"{scan_count} scan + {detail_count} detail workers — Ctrl+C to stop")

    try:
        while True:
            time.sleep(60)
    except KeyboardInterrupt:
        logger.info("scheduler: shutdown requested — draining worker pools")
        print("\n[scheduler] Shutting down workers...")
        _shutdown_worker_pools()
        print("[scheduler] Done.")


# ─────────────────────────────────────────
# CLI
# ─────────────────────────────────────────

if __name__ == "__main__":
    import sys
    from logger import get_logger, init_logging
    init_logging("scheduler")
    skip = "--skip-rebuild" in sys.argv
    run_scheduler(skip_rebuild=skip)
