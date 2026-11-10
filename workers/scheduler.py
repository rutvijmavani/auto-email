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
import redis.exceptions
import math
import multiprocessing
import os

from logger import get_logger
import random
import threading
import sys
import time
import uuid
from datetime import datetime, timezone
from typing import Optional

from workers.redis_client import get_redis, get_pubsub_redis
from workers.adaptive import (
    update_poll_interval, load_poll_counts, dump_poll_counts,
    build_thresholds_from_scores, DEFAULT_THRESHOLDS,
)
from workers.http_client import discover_workday_dc_keys, seed_concurrency_limits
from workers.rebuild import rebuild_redis
from workers.stream_backlog import trim_delivered
from workers import company_load, progress
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
    REDIS_DETAIL_FULLSCAN,
    ADAPTIVE_DEFAULT_INTERVAL,
    SCAN_QUEUE,
    SCHEDULER_TICK_SECS,
    SCHEDULER_FULL_SCAN_BUFFER_S,
    SCHEDULER_FULL_SCAN_INTERVAL_S,
    SCHEDULER_HEARTBEAT_TTL,
    ADAPTIVE_CALIBRATION_LOOKBACK_DAYS,
    ADAPTIVE_MIN_COMPANIES_CALIBRATE,
    MONITOR_MAX_WORKERS,
    MONITOR_PLATFORM_CONCURRENCY,
    CONCURRENCY_ERROR_RATE_REDUCE,
    CONCURRENCY_FLOOR,
    CONCURRENCY_FLOOR_DEFAULT,
    WORKER_SHUTDOWN_TIMEOUT_S,
    WORKER_SLOW_CHECK_INTERVAL_S,
    WORKER_FLOOR,
    WORKER_DEPRIORITISE_SECS,
    DETAIL_QUEUE_HIGH_WATERMARK,
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
    REDIS_INFLIGHT_FULLSCAN,
    REDIS_INFLIGHT_FULLSCAN_DC_PREFIX,
    WORKER_CURRENT_JOB_FULLSCAN_PREFIX,
    # Stream-based two-layer scheduler (Section 5 / 9 redesign)
    REDIS_STREAM_ADAPTIVE,
    REDIS_STREAM_FULLSCAN,
    STREAM_CONSUMER_GROUP,
    STREAM_CONSUMER_PRUNE_IDLE_S,
    MAX_STREAM_REDELIVERIES,
    REDIS_REAPED_PREFIX,
    REDIS_INFLIGHT_MSG_PREFIX,
    REDIS_WORKER_CUR_PREFIX,
    REDIS_START_LATENCY_HASH,
    REDIS_WORKER_HEALTH_STREAM, WORKER_HEALTH_SNAPSHOT_INTERVAL_S,
    WORKER_HEALTH_STREAM_MAXLEN,
    PROGRESS_GAP_FACTOR,
    PROGRESS_COLD_GAP_S,
    # WARMING lifecycle
    WARMING_POLLS_COUNT,
    WARMING_INTERVAL_S,
)

logger = get_logger(__name__)

# Pause / resume events — set by pubsub listener, read by dispatch loops.
# _pause_event: set when pipeline:pause received; cleared on resume.
# _resume_event: cleared when paused; set when pipeline:resume received.
#   Workers call _resume_event.wait(timeout=N) instead of sleep() so they
#   unblock within ~1s of a resume signal (Section 18 — "never paused forever").
# Both start clear/set (not paused at startup).
_pause_event  = threading.Event()   # set = paused
_resume_event = threading.Event()   # set = running (cleared when paused)
_resume_event.set()                 # default: running

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
    "scan_add":       0,
    "scan_remove":    0,
    "detail_add":     0,
    "detail_remove":  0,
    "detail_alert":   0,   # Phase 11: consecutive cycles with depth > watermark
    "fullscan_add":   0,
    "fullscan_remove": 0,
}

# ── Pool health tracking (published to scheduler:health Redis key) ────────────
# Read by the watchdog to make informed alerting decisions without duplicating
# the scheduler's pool-management logic.
#
# _consecutive_deaths:  how many workers of a type died quickly (< _WORKER_STABLE_AFTER_S)
#                       in a row without a stable worker in between.
#                       Reset to 0 when a replacement lives ≥ _WORKER_STABLE_AFTER_S.
# _total_replacements:  total spawns due to unexpected death since scheduler start.
# _worker_spawn_times:  pid → (ptype, spawn_epoch) — populated in _spawn_worker,
#                       consumed in _replace_dead_workers to compute worker age.
# _WORKER_STABLE_AFTER_S: a replacement that lives at least this long is "stable"
#                          (it survived — next death resets the streak).
_consecutive_deaths:      dict = {"scan": 0, "detail": 0, "fullscan": 0}
_total_replacements:      dict = {"scan": 0, "detail": 0, "fullscan": 0}
_unstarted_kills:         dict = {"scan": 0, "detail": 0, "fullscan": 0}  # killed: never started
_worker_spawn_times:      dict = {}   # pid → (ptype, spawn_epoch)
_worker_started:          set  = set()  # pids seen with a heartbeat (finished starting)
# ZJ: workers removed by _remove_one_worker that are still finishing their job.
# Each entry is (ptype, proc, removed_at_epoch); _reap_retiring_workers joins
# finished ones (no zombie) and kills any still alive past the shutdown grace.
_retiring_workers:        list = []
_death_streak_started_at: dict = {"scan": 0.0, "detail": 0.0, "fullscan": 0.0}
# Timestamp (epoch) when the current consecutive-death streak began.
# A replacement must have been spawned AFTER this time and lived >=
# _WORKER_STABLE_AFTER_S before we reset the streak.  This prevents a
# long-lived "old" worker from masking a sibling that keeps rapid-crashing.
_WORKER_STABLE_AFTER_S: int  = 60  # seconds
_SCHEDULER_HEALTH_KEY   = "scheduler:health"
_SCHEDULER_HEALTH_TTL   = 600      # 10 min — expires naturally if scheduler dies
_SCHEDULER_STARTUP_HB_TTL = 180    # startup grace: survives init_db()+rebuild_redis() before loop takes over

# ── Phase 10 — per-company DC key cache ──────────────────────────────────────
# Populated lazily in _get_dc_key_for_company().  Avoids a DB query on every
# adaptive_loop tick.  Keyed by company name; value is the dispatch throttle
# key (e.g. "greenhouse", "workday_wd12").  Reset on process restart.
_company_dc_key_cache: dict = {}


# ─────────────────────────────────────────
# STREAM HELPERS
# ─────────────────────────────────────────

def _init_consumer_group(r, stream_key: str, group: str = STREAM_CONSUMER_GROUP) -> None:
    """
    Ensure the consumer group exists for a stream (idempotent).

    Uses id='$' so only NEW messages are delivered to workers — we never
    want to replay history from before this process started.

    BUSYGROUP means the group already exists — safe to ignore.
    MKSTREAM creates the stream key itself if it doesn't exist yet.
    """
    try:
        r.xgroup_create(stream_key, group, id="$", mkstream=True)
        logger.debug("stream: created consumer group %r on %r", group, stream_key)
    except Exception as exc:
        if "BUSYGROUP" in str(exc):
            pass   # group already exists — fine
        else:
            logger.warning(
                "stream: xgroup_create failed for %r/%r: %s",
                stream_key, group, exc,
            )


def _get_stream_pending_count(r, stream_key: str,
                               group: str = STREAM_CONSUMER_GROUP) -> int:
    """Return number of pending (in-flight) messages in the consumer group PEL."""
    try:
        info = r.xpending(stream_key, group)
        return info.get("pending", 0) if isinstance(info, dict) else 0
    except Exception:
        return 0


def _get_stream_undelivered_companies(r, stream_key: str,
                                      group: str = STREAM_CONSUMER_GROUP):
    """Return the set of companies queued in a stream but not yet claimed by a worker.

    Entries after the group's last-delivered-id have reached no consumer, so
    they are in neither the poll ZSETs nor the inflight ZSET.  Returns None
    when the stream state cannot be read, so callers can fail safe.
    """
    def _s(v):
        return v.decode() if isinstance(v, bytes) else v

    try:
        last_id = None
        for g in r.xinfo_groups(stream_key):
            if _s(g.get("name", b"")) == group:
                last_id = _s(g.get("last-delivered-id", "0-0"))
                break
        if last_id is None:
            return set()
        entries = r.xrange(stream_key, min=f"({last_id}", max="+")
        companies = set()
        for _msg_id, fields in entries:
            raw = fields.get("company", fields.get(b"company"))
            if raw:
                companies.add(_s(raw))
        return companies
    except Exception as exc:
        logger.warning("stream: undelivered scan failed for %r: %s", stream_key, exc)
        return None


def _trim_stream(r, stream_key: str) -> None:
    """Delete delivered stream entries (no MAXLEN); a failure never blocks dispatch."""
    try:
        trim_delivered(r, stream_key, STREAM_CONSUMER_GROUP)
    except Exception as exc:
        logger.warning("stream: trim failed for %r: %s", stream_key, exc)


# Owner states of a pending stream entry (see workers/progress.py):
#   RUNNING    owner alive, its worker:cur names this message, deadline not passed
#   STALLED    owner alive, worker:cur names this message, deadline passed
#   ABANDONED  owner alive, but it is not working on this message
#   DEAD       owner has no heartbeat key (or is not a worker consumer)
# Everything except RUNNING is reaped once seen in the same state on two
# consecutive calls (a message just read, or just finished and not yet XACK'd,
# is momentarily ABANDONED).
_OWNER_RUNNING, _OWNER_STALLED, _OWNER_ABANDONED, _OWNER_DEAD = (
    "running", "stalled", "abandoned", "dead")

_reap_seen: dict = {}     # stream_key -> {msg_id: state seen on the previous call}

_STREAM_WORKER_TYPE = {
    REDIS_STREAM_ADAPTIVE: "scan_worker",
    REDIS_STREAM_FULLSCAN: "fullscan_worker",
}


def _s(v) -> str:
    return v.decode() if isinstance(v, bytes) else str(v)


def _split_consumer(consumer: str):
    """'worker-{host}-{pid}' -> (host, pid); (None, None) for any other consumer."""
    if not consumer.startswith("worker-"):
        return None, None
    host, _, pid = consumer[len("worker-"):].rpartition("-")
    if not host or not pid.isdigit():
        return None, None
    return host, int(pid)


def _owner_state(r, stream_key: str, consumer: str, msg_id: str, now: float) -> str:
    host, pid = _split_consumer(consumer)
    if host is None:
        return _OWNER_DEAD
    worker_type = _STREAM_WORKER_TYPE.get(stream_key, "scan_worker")
    if not r.exists(f"worker:alive:{worker_type}:{host}:{pid}"):
        return _OWNER_DEAD
    raw = r.get(f"{REDIS_WORKER_CUR_PREFIX}:{consumer}")
    try:
        cur = json.loads(raw) if raw else None
    except (TypeError, ValueError):
        cur = None
    if not cur or cur.get("msg_id") != msg_id:
        return _OWNER_ABANDONED
    return _OWNER_RUNNING if now <= float(cur.get("alive_until", 0)) else _OWNER_STALLED


def _kill_stalled_worker(r, consumer: str) -> None:
    """Terminate a stalled worker on this host; the pool respawns it."""
    host, pid = _split_consumer(consumer)
    if host is None:
        return
    import socket as _socket
    progress.terminate(r, consumer, host, pid, _socket.gethostname())


def clear_reaped(r, op_type: str, company: str) -> None:
    """A scan of this company succeeded: its reap history starts over."""
    r.delete(f"{REDIS_REAPED_PREFIX}:{op_type}:{company}")


def claim_stale_work(r, stream_key: str, group: str, op_type: str = "scan") -> None:
    """
    Reap pending stream entries whose owner is not actively working on them.

    Called from the scheduler dispatch loops on every tick.  For each PEL entry
    the owner's state is read from its heartbeat key and worker:cur report (no
    idle window): RUNNING entries are left alone however long they take; every
    other state is reaped after two consecutive sightings.

    Reap = XACK + ZADD GT into the poll ZSET (an existing, later schedule wins) at now + backoff, where the backoff grows from the first reap
    (reaped:{op}:{company}, cleared by clear_reaped on a successful scan).  The
    company's inflight slot is released.  A STALLED owner is terminated first.

    Args:
        r:           Redis client
        stream_key:  Stream to inspect (e.g. REDIS_STREAM_ADAPTIVE)
        group:       Consumer group name
        op_type:     "scan" | "fullscan" — which ZSET / counter family to use
    """
    from workers.scan_worker import backoff_delay_for

    try:
        pending = r.xpending_range(stream_key, group, min="-", max="+", count=100)
    except Exception as exc:
        # LGAP: an unreadable PEL means no reaping this tick; that must be visible.
        logger.warning("claim_stale_work: xpending error on %r: %s", stream_key, exc)
        return

    now = time.time()
    seen_before = _reap_seen.get(stream_key, {})
    seen_now = {}
    for entry in pending:
        msg_id = _s(entry["message_id"])
        consumer = _s(entry["consumer"])
        state = _owner_state(r, stream_key, consumer, msg_id, now)
        if state == _OWNER_RUNNING:
            continue
        seen_now[msg_id] = state
        if seen_before.get(msg_id) != state:
            continue                       # first sighting in this state — confirm next call

        if state == _OWNER_STALLED:
            _kill_stalled_worker(r, consumer)

        msgs = r.xrange(stream_key, min=msg_id, max=msg_id, count=1)
        fields = msgs[0][1] if msgs else None
        company = _s(fields.get("company", "")) if fields else ""
        r.xack(stream_key, group, msg_id)
        if not company:
            logger.warning("claim_stale_work: %r entry %s has no company (%s) — acked",
                           stream_key, msg_id, state)
            continue

        reaps = r.incr(f"{REDIS_REAPED_PREFIX}:{op_type}:{company}")
        if reaps == 1:
            r.expire(f"{REDIS_REAPED_PREFIX}:{op_type}:{company}", 86400)
        delay = backoff_delay_for(reaps - 1)
        target_zset = REDIS_POLL_FULLSCAN if op_type == "fullscan" else REDIS_POLL_ADAPTIVE
        r.zadd(target_zset, {company: now + delay}, gt=True)
        _release_inflight(r, op_type, company, _s(fields.get("dc_key", "")))
        logger.log(
            logging.WARNING if reaps >= MAX_STREAM_REDELIVERIES else logging.INFO,
            "claim_stale_work: reaped %r from %s (owner %s %s, reap #%d) → backoff +%ds in %s",
            company, stream_key, consumer, state, reaps, delay, target_zset,
        )

    # Sightings are kept per stream (the adaptive and fullscan loops share this dict):
    # replace only this stream's entry, in one assignment.
    _reap_seen[stream_key] = seen_now


def _release_inflight(r, op_type: str, company: str, dc_key: str) -> None:
    """Free the company's inflight slot(s) and stored stream id."""
    if not dc_key:
        dc_key = _get_dc_key_for_company(company)
    r.delete(f"{REDIS_INFLIGHT_MSG_PREFIX}:{op_type}:{company}")
    if op_type == "fullscan":
        r.zrem(f"{REDIS_INFLIGHT_FULLSCAN_DC_PREFIX}:{dc_key}", company)
        r.zrem(REDIS_INFLIGHT_FULLSCAN, company)
        r.delete(f"fullscan:lock:{company}")     # lease of the reaped owner
    else:
        r.zrem(f"{REDIS_INFLIGHT_PREFIX}:{dc_key}", company)


def _id_key(msg_id: str) -> tuple:
    ms, _, seq = msg_id.partition("-")
    return int(ms), int(seq or 0)


def sweep_inflight(r, op_type: str, stream_key: str, group: str) -> None:
    """
    Release inflight slots whose stream message no longer exists.

    A slot is live while its message (stored in inflight:msg:{op}:{company}) is
    undelivered or still pending; a pending message whose owner stopped working
    is released by claim_stale_work.  A slot with no stored id, or whose message
    was delivered and acknowledged without the worker releasing the slot, is a
    leak and is freed here.  Replaces the fixed-age ZREMRANGEBYSCORE pruning.
    """
    try:
        last = next((_s(g.get("last-delivered-id", "0-0")) for g in r.xinfo_groups(stream_key)
                     if _s(g.get("name")) == group), None)
    except Exception as exc:
        logger.debug("sweep_inflight: xinfo error on %r: %s", stream_key, exc)
        return
    if last is None:
        return
    last_key = _id_key(last)

    if op_type == "fullscan":
        pattern = f"{REDIS_INFLIGHT_FULLSCAN_DC_PREFIX}:*"
    else:
        pattern = f"{REDIS_INFLIGHT_PREFIX}:*"
    slots = []                                  # (zset_key, company)
    for zkey in r.scan_iter(match=pattern):
        zkey = _s(zkey)
        slots += [(zkey, _s(c)) for c in r.zrange(zkey, 0, -1)]
    if op_type == "fullscan":
        slots += [(REDIS_INFLIGHT_FULLSCAN, _s(c)) for c in r.zrange(REDIS_INFLIGHT_FULLSCAN, 0, -1)]

    for zkey, company in slots:
        raw = r.get(f"{REDIS_INFLIGHT_MSG_PREFIX}:{op_type}:{company}")
        live = False
        if raw:
            msg_id = _s(raw)
            if _id_key(msg_id) > last_key:
                live = True                      # not delivered yet
            else:
                live = bool(r.xpending_range(stream_key, group, min=msg_id, max=msg_id, count=1))
        if live:
            continue
        r.zrem(zkey, company)
        r.delete(f"{REDIS_INFLIGHT_MSG_PREFIX}:{op_type}:{company}")
        if op_type == "fullscan":
            r.zrem(REDIS_INFLIGHT_FULLSCAN, company)
            r.delete(f"fullscan:lock:{company}")
        logger.warning("sweep_inflight: released leaked %s slot %s for %r", op_type, zkey, company)

    prune_idle_consumers(r, stream_key, group)


def prune_idle_consumers(r, stream_key: str, group: str) -> int:
    """
    CL: delete consumers of the group that hold no pending messages and have been
    idle STREAM_CONSUMER_PRUNE_IDLE_S.  Each worker restart creates a new
    consumer name, so without this the consumer list only grows.  A consumer
    with pending messages is never touched (claim_stale_work owns those).
    Returns the number deleted; a failure never blocks the dispatch loop.
    """
    removed = 0
    try:
        for c in r.xinfo_consumers(stream_key, group):
            if int(c.get("pending", 0)) != 0:
                continue
            if int(c.get("idle", 0)) < STREAM_CONSUMER_PRUNE_IDLE_S * 1000:
                continue
            r.xgroup_delconsumer(stream_key, group, _s(c.get("name", "")))
            removed += 1
        if removed:
            logger.info("prune_idle_consumers: deleted %d idle consumer(s) from %r/%r",
                        removed, stream_key, group)
    except Exception as exc:
        logger.debug("prune_idle_consumers: %r/%r failed: %s", stream_key, group, exc)
    return removed


# ─────────────────────────────────────────
# CYCLE START (called from --monitor-jobs)
# ─────────────────────────────────────────

def record_cycle_start(counts_snapshot=None) -> float:
    """
    Write cycle:start Unix timestamp to Redis.

    Called from pipeline.py --monitor-jobs AFTER:
        1. Digest email successfully sent
        2. job_postings status set to 'digested'

    Uses Redis TIME (not system clock) to avoid NTP skew issues.

    Returns the cycle start Unix timestamp.
    """
    r = get_redis()

    # Use Redis TIME for the canonical cycle start
    redis_time_s, _ = r.time()   # (seconds, microseconds)
    r.set(REDIS_CYCLE_START, redis_time_s)

    logger.info("scheduler: cycle:start recorded at %s (unix=%d)",
                datetime.fromtimestamp(redis_time_s).isoformat(),
                redis_time_s)

    # New digest window: drop the per-company full-scan counts the digest just used
    # (only those in counts_snapshot when given, so counts written since survive).
    from workers.digest_counts import reset_fullscan_counts
    reset_fullscan_counts(r, counts_snapshot)

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
    progress.touch()   # phase marker is also a checkpoint of the worker's progress report


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
    r.delete(f"{REDIS_INFLIGHT_MSG_PREFIX}:scan:{company}")
    if success:
        clear_reaped(r, "scan", company)

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
                   consecutive_errors, last_success_at,
                   warming_polls_remaining,
                   next_full_scan_at, avg_fullscan_duration_s,
                   avg_scan_duration_s
            FROM company_poll_stats
            WHERE company = %s
        """, (company,)).fetchone()
    finally:
        conn.close()

    if not row:
        logger.warning("on_adaptive_complete: no poll_stats row for %r", company)
        _reschedule_adaptive(company, 86400)
        return

    # ── WARMING lifecycle check ────────────────────────────────────────────────
    # warming_polls_remaining: NULL=STABLE; 3/2/1=WARMING (new companies).
    # During WARMING, use a fixed 2h interval regardless of adaptive score so
    # the engine has enough data before driving scheduling decisions.
    warming = row["warming_polls_remaining"]

    if success:
        counts     = load_poll_counts(row["recent_poll_counts"])
        thresholds = get_band_thresholds(r)
        result     = update_poll_interval(counts, row["current_interval_s"], new_jobs,
                                          thresholds=thresholds)
        interval   = result["current_interval_s"]
        consec_errors = 0

        # Override interval during WARMING — still compute score for future use
        if warming is not None:
            interval = WARMING_INTERVAL_S
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

    # Compute WARMING decrement (only on success; leave unchanged on failure)
    if success and warming is not None:
        new_warming = (warming - 1) if warming > 1 else None   # None = STABLE
    else:
        new_warming = warming   # unchanged on failure

    # Persist to DB
    conn = get_conn()
    try:
        conn.execute("""
            UPDATE company_poll_stats SET
                recent_poll_counts      = %s,
                adaptive_score          = %s,
                current_interval_s      = %s,
                -- LPF (by design): last_poll_at is the last ATTEMPT, stamped on failure
                -- too.  Rule 4 (fullscan) relies on this: a failing adaptive scan must
                -- not defer the fullscan forever.  Success time is last_success_at.
                last_poll_at            = CURRENT_TIMESTAMP,
                next_poll_at            = %s,
                consecutive_errors      = %s,
                last_success_at         = CASE WHEN %s THEN CURRENT_TIMESTAMP
                                          ELSE last_success_at END,
                last_error_at           = CASE WHEN %s THEN CURRENT_TIMESTAMP
                                          ELSE last_error_at END,
                warming_polls_remaining = %s,
                updated_at              = CURRENT_TIMESTAMP
            WHERE company = %s
        """, (
            dump_poll_counts(result["recent_poll_counts"]),
            result["adaptive_score"],
            interval,
            next_poll_at,
            consec_errors,        # consecutive_empty is owned by upsert_poll_stats only
            success,              # for last_success_at CASE
            not success,          # for last_error_at CASE
            new_warming,          # NULL = STABLE, int = still WARMING
            company,
        ))
        conn.commit()
    finally:
        conn.close()

    # Scan sizing table (workers/company_load.py): d is the EMA scan_worker already
    # persisted for this run, T the interval just stored.  A failed run changes
    # neither, so nothing to write.
    if success:
        company_load.update(r, "scan", company, row["avg_scan_duration_s"], interval)

    if success and warming is not None:
        remaining_after = (warming - 1) if warming > 1 else 0
        logger.info(
            "on_adaptive_complete: WARMING company=%r polls_remaining=%d→%d "
            "interval=%ds (fixed)",
            company, warming, remaining_after, interval,
        )

    # Update stats cache in Redis
    r.hset(f"stats:{company}", mapping={
        "adaptive_score":     str(result["adaptive_score"]),
        "current_interval_s": str(interval),
        "recent_poll_counts": dump_poll_counts(result["recent_poll_counts"]),
        "last_poll_at":       datetime.fromtimestamp(now).isoformat(),
    })

    # Reschedule adaptive (Rule 3 part 1: never orphaned)
    _scheduled_ts = _reschedule_adaptive(company, interval)
    clear_heartbeat(company)

    # JITLOG: log the slot actually written (gap-picked / jittered), not now+interval.
    # None means an existing future score was kept (BO).
    if isinstance(_scheduled_ts, (int, float)):
        _next_poll_str = datetime.fromtimestamp(_scheduled_ts).strftime("%H:%M")
    else:
        _next_poll_str = "kept-existing"
    logger.info(
        "on_adaptive_complete: company=%r new_jobs=%d interval=%ds "
        "next_poll=%s success=%s",
        company, new_jobs, interval,
        _next_poll_str, success,
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

    # Rule 3 part 2: queue fullscan if not scheduled or won't finish before digest
    if success:
        _maybe_reschedule_full_scan(company, row)


def _next_digest_deadline(now: float) -> float:
    """
    Return the Unix timestamp of the next 7 AM Eastern digest boundary.

    Used by _pick_schedule_time() so fullscan scheduling skips gap midpoints
    where the predicted scan duration would push completion past 7 AM ET.

    Examples:
        now = 2 PM ET  → returns tomorrow 7 AM ET
        now = 4 AM ET  → returns today   7 AM ET (3 h away)
        now = 7 AM ET  → returns tomorrow 7 AM ET (exactly on boundary)
    """
    try:
        from zoneinfo import ZoneInfo
    except ImportError:
        from backports.zoneinfo import ZoneInfo
    from datetime import datetime as _dt, timedelta
    from config import CYCLE_START_HOUR, SEND_TIMEZONE

    tz       = ZoneInfo(SEND_TIMEZONE)
    dt       = _dt.fromtimestamp(now, tz=tz)
    deadline = dt.replace(hour=CYCLE_START_HOUR, minute=0, second=0, microsecond=0)
    if dt >= deadline:
        deadline = deadline + timedelta(days=1)
    return deadline.timestamp()


def _pick_schedule_time(
    target_ts:     float,
    queue_key:     str,
    interval_s:    int,
    tolerance_pct: float,
    r,
    *,
    deadline_ts:    Optional[float] = None,
    avg_duration_s: float = 0.0,
) -> float:
    """
    Gap-detection scheduling algorithm (Phase 2 — thundering herd prevention).

    Replaces _least_loaded_slot() (20-slot min-heap) with a continuous gap
    approach that guarantees maximum separation from existing neighbours.

    Algorithm
    ─────────
    1. Compute a tolerance window [lo, hi] centred on target_ts:
           window_s = interval_s × tolerance_pct   (e.g. 20% of 24 h = 4.8 h)
           lo = target_ts − window_s / 2
           hi = target_ts + window_s / 2

    2. Fetch all existing ZSET scores within [lo, hi] via ZRANGEBYSCORE.

    3. Build a gap list including sentinel edges at lo and hi:
           points = sorted([lo] + existing_scores + [hi])
           gaps   = [(size, gap_lo, gap_hi) for each consecutive pair]

    4. Sort gaps by size descending; tiebreaker = gap midpoint closest to target_ts.

    5. For each gap (largest first), compute midpoint and:
         - If deadline_ts set and midpoint + avg_duration_s ≥ deadline_ts → skip
           (scan would not finish before the 7 AM digest boundary)
         - Otherwise → return midpoint

    6. Fallback: all gaps violate deadline (extremely rare) → return target_ts.

    Comparison with _least_loaded_slot()
    ──────────────────────────────────────
    Old approach: divide window into 20 fixed slots, count companies per slot,
    return centre of the least-loaded slot.  Two companies can still land on
    the same slot centre.  Slot boundaries create periodic clustering.

    New approach: works at arbitrary resolution.  If 50 companies are already
    scheduled, the algorithm finds the actual largest gap between them regardless
    of slot size.  Self-corrects from full clustering in 2–3 cycles:
        Day 1: all companies at T → all gaps = window_s / n → midpoints spread
        Day 2: midpoints now fill the window evenly
        Day 3+: stable, maximum-spread distribution maintained

    Args:
        target_ts:      Ideal next-poll Unix timestamp (now + interval_s).
        queue_key:      Redis ZSET to inspect (poll:adaptive or poll:fullscan).
        interval_s:     Full polling cycle in seconds; sets window width.
        tolerance_pct:  Fraction of interval_s defining the search window.
                        0.20 → ±10% (4.8 h on a 24 h cycle).
        r:              Redis connection.
        deadline_ts:    Optional Unix timestamp of next digest (7 AM ET).
                        When set, midpoints that would violate the deadline
                        are skipped.  Pass _next_digest_deadline(now).
        avg_duration_s: Predicted scan duration (seconds) for deadline check.
                        Read from company_poll_stats.avg_fullscan_duration_s.

    Returns:
        Unix timestamp (float) for the scheduled time.
    """
    window_s = interval_s * tolerance_pct
    lo       = target_ts - window_s / 2
    hi       = target_ts + window_s / 2

    # All existing scheduled times within the window (one Redis call)
    raw      = r.zrangebyscore(queue_key, lo, hi, withscores=True)
    scores   = sorted(float(s) for _, s in raw)

    # Build gaps including window-edge sentinels
    points = [lo, *scores, hi]
    gaps   = [
        (points[i + 1] - points[i], points[i], points[i + 1])
        for i in range(len(points) - 1)
    ]

    # Largest gap first; tiebreaker: midpoint closest to target_ts
    gaps.sort(key=lambda g: (-g[0], abs((g[1] + g[2]) / 2 - target_ts)))

    for _gap_size, gap_lo, gap_hi in gaps:
        midpoint = (gap_lo + gap_hi) / 2
        if deadline_ts and avg_duration_s > 0:
            # Compute the correct deadline for THIS candidate: the 7 AM that
            # immediately follows the midpoint's local clock (not target_ts's).
            # A candidate before midnight uses today's 7 AM; one after midnight
            # uses the same day's 7 AM — which is different from target_ts's
            # deadline when the window straddles midnight.
            # Only checked when deadline_ts was explicitly passed by the caller
            # (fullscan callers) — adaptive callers never pass deadline_ts.
            candidate_deadline = _next_digest_deadline(midpoint)
            if midpoint + avg_duration_s >= candidate_deadline:
                continue   # scan would not finish before 7 AM — try next gap
        return midpoint

    # All gaps violate deadline (fleet so large that no slot is safe this cycle).
    logger.warning(
        "_pick_schedule_time: all gaps in window violate deadline for %r "
        "(avg_duration=%.0fs deadline=%s) — using target_ts fallback",
        queue_key, avg_duration_s,
        datetime.fromtimestamp(deadline_ts).strftime("%H:%M") if deadline_ts else "none",
    )
    if deadline_ts and avg_duration_s > 0:
        # Apply the same deadline guard to the fallback: if target_ts itself would
        # miss the digest, schedule after the next digest so the scan runs in the
        # following cycle rather than starting too close to the cutoff.
        _fallback_deadline = _next_digest_deadline(target_ts)
        if target_ts + avg_duration_s >= _fallback_deadline:
            # 15-min buffer after the digest lets the email send before scanning
            _post_digest = _fallback_deadline + 900
            logger.warning(
                "_pick_schedule_time: target_ts also violates deadline for %r "
                "— scheduling after digest at %s",
                queue_key,
                datetime.fromtimestamp(_post_digest).strftime("%H:%M"),
            )
            return _post_digest
    return target_ts


# ── Atomic scheduling lock ─────────────────────────────────────────────────────
# _pick_schedule_time() reads the ZSET and _then_ we ZADD.  When multiple
# scan-worker processes call _reschedule_adaptive() concurrently (all completing
# around the same time), they all read the same gap and write the same score —
# re-creating the thundering-herd problem the algorithm is designed to prevent.
#
# Fix: hold a Redis lock (2 s TTL) around the ZRANGEBYSCORE → ZADD pair
# so at most one process selects a slot at a time.  2 s covers ZRANGEBYSCORE +
# gap selection + ZADD even under network jitter.  If the lock is contended, the
# caller falls back to scheduling at target_ts directly — still correct, just
# without gap detection for that one call; the next reschedule call rebalances.
#
# KEYS[1] = lock key    ARGV[1] = owner token
_SCHEDULING_UNLOCK_LUA = """
if redis.call('GET', KEYS[1]) == ARGV[1] then
    return redis.call('DEL', KEYS[1])
end
return 0
"""

# Atomic ceiling check + slot claim for fullscan dispatch.
#
# Counts adaptive and fullscan inflight for the same DC key together so both
# scan types share the learned ceiling.  Uses separate ZSET keys so each type
# can be released independently.  Nothing is pruned by age here: slots are
# released by the worker on completion, by claim_stale_work when it reaps a
# message, and by sweep_inflight when the message no longer exists.
#
# Returns 1 if slot was claimed, 0 if ceiling was hit (caller should re-queue).
#
# KEYS[1] = inflight:scans:{dc_key}      — adaptive inflight ZSET
# KEYS[2] = inflight:fullscans:{dc_key}  — fullscan inflight ZSET
# ARGV[1] = ceiling (int)
# ARGV[2] = company (member to ZADD on success)
# ARGV[3] = now (score for ZADD)
_FULLSCAN_INFLIGHT_CLAIM_LUA = """
if redis.call('ZSCORE', KEYS[2], ARGV[2]) then
    return 0
end
local total = redis.call('ZCARD', KEYS[1]) + redis.call('ZCARD', KEYS[2])
if total >= tonumber(ARGV[1]) then
    return 0
end
redis.call('ZADD', KEYS[2], ARGV[3], ARGV[2])
return 1
"""


def _atomic_schedule(
    r,
    queue_key:     str,
    company:       str,
    target_ts:     float,
    interval_s:    float,
    tolerance_pct: float,
    *,
    deadline_ts:     Optional[float] = None,
    avg_duration_s:  float           = 1800.0,
    skip_if_future:  bool            = False,
) -> Optional[float]:
    """
    Pick a gap-avoiding ZSET slot and immediately ZADD the company, all under a
    short-lived Redis lock so concurrent processes cannot select the same gap.

    If skip_if_future=True, the ZADD is skipped (under the lock) when the company
    already has a valid future score in queue_key, preventing a race between a
    pre-lock zscore check and the eventual ZADD.

    Returns the scheduled Unix timestamp, or None when skip_if_future suppressed the ZADD.
    """
    lock_key = f"scheduling:lock:{queue_key}"
    token    = f"{os.getpid()}:{time.monotonic_ns()}"
    acquired = r.set(lock_key, token, px=2000, nx=True)
    try:
        if acquired:
            if skip_if_future:
                _existing = r.zscore(queue_key, company)
                if _existing is not None and float(_existing) > time.time():
                    logger.debug(
                        "_atomic_schedule: %r already in %r at %.0f — skip_if_future suppressed ZADD",
                        company, queue_key, float(_existing),
                    )
                    return None
            score = _pick_schedule_time(
                target_ts      = target_ts,
                queue_key      = queue_key,
                interval_s     = interval_s,
                tolerance_pct  = tolerance_pct,
                r              = r,
                deadline_ts    = deadline_ts,
                avg_duration_s = avg_duration_s,
            )
            score = max(score, time.time())  # never insert a past-due ZADD score
        else:
            # Lock contended — check skip_if_future before falling through to jitter.
            if skip_if_future:
                _existing_c = r.zscore(queue_key, company)
                if _existing_c is not None and float(_existing_c) > time.time():
                    logger.debug(
                        "_atomic_schedule: %r already in %r at %.0f — skip_if_future suppressed ZADD (lock-contended path)",
                        company, queue_key, float(_existing_c),
                    )
                    return None
            # Another process is scheduling — apply a deterministic per-company
            # offset so concurrent callers don't all land on the same timestamp
            # (thundering herd).  Use a hash of the company string to produce a
            # reproducible, unique positive spread within the safe window or 0–10% of interval.
            import hashlib as _hashlib
            _hash_int = int(_hashlib.md5(company.encode(), usedforsecurity=False).hexdigest(), 16)
            if deadline_ts and (deadline_ts - avg_duration_s) > target_ts:
                # Spread jitter within the remaining safe window so multiple
                # concurrent lock-busy callers don't all clamp to the same score.
                # Use purely positive jitter (0 to 10% of safe window) so the
                # hash distributes evenly without half the values collapsing to
                # target_ts via max(target_ts, target_ts + negative_jitter).
                _safe_window = (deadline_ts - avg_duration_s) - target_ts
                _jitter_s = int(_hash_int % max(1, int(_safe_window * 0.10)))
                score = target_ts + _jitter_s
            else:
                # No safe window — jitter within 0–10% of interval so all callers
                # spread positively away from target_ts without clamping.
                _jitter_s = int(_hash_int % max(1, int(interval_s * 0.10)))
                score = target_ts + _jitter_s
                if deadline_ts:
                    # Cap score so the scan can still finish before the digest.
                    _upper = max(target_ts, deadline_ts - avg_duration_s)
                    score  = min(score, _upper)
                    # When no safe window remains before the digest, apply the same
                    # post-digest fallback that _pick_schedule_time uses.
                    if avg_duration_s > 0 and _upper <= target_ts:
                        _fb_dl = _next_digest_deadline(target_ts)
                        if target_ts + avg_duration_s >= _fb_dl:
                            score = _fb_dl + 900
            logger.debug(
                "_atomic_schedule: lock busy for %r — scheduling %r at target_ts+%ds",
                queue_key, company, _jitter_s,
            )
        r.zadd(queue_key, {company: score})
        return score
    finally:
        if acquired:
            try:
                r.eval(_SCHEDULING_UNLOCK_LUA, 1, lock_key, token)
            except Exception as _unlock_err:
                logger.debug(
                    "_atomic_schedule: unlock failed for %r: %s — TTL will expire it",
                    lock_key, _unlock_err,
                )


def _reschedule_adaptive(company: str, interval_s: int) -> Optional[float]:
    """
    Add company back to poll:adaptive ZSET with its new interval.

    Returns the score actually written (jitter / gap-picking applied), or None
    when the company already holds a future score and that score was kept (BO:
    a later backoff score set elsewhere is not overwritten).

    Uses _atomic_schedule() which wraps _pick_schedule_time() + ZADD under a
    short Redis lock so concurrent scan_worker processes do not all read the
    same gap and schedule at the same slot (would recreate thundering herd).

    Adaptive scans are short (seconds to a minute) so no deadline check is
    needed — only fullscan scheduling uses the 7 AM digest deadline guard.
    """
    r   = get_redis()
    now = time.time()
    return _atomic_schedule(
        r, REDIS_POLL_ADAPTIVE, company,
        now + interval_s, interval_s, 0.20,
        skip_if_future=True,
    )


def _maybe_reschedule_full_scan(company: str, row) -> None:
    """
    Queue or reschedule a fullscan only when necessary (Rule 3).

    Does nothing when a fullscan is already scheduled AND will finish
    comfortably before the next 7 AM digest deadline.

    Triggers (re)scheduling when:
      - next_full_scan_at IS NULL        → never scheduled
      - next_full_scan_at is in the past → overdue / stale ZSET entry lost
      - next_full_scan_at + avg_fullscan_duration_s >= next 7 AM deadline
                                         → would miss the digest

    Suppressed when the ATS has an active scan backoff (struggling ATS —
    a fullscan makes far more requests than a listing scan).
    """
    r = get_redis()
    if r.exists(f"{REDIS_BACKOFF_PREFIX}:scan:{company}"):
        logger.debug(
            "_maybe_reschedule_full_scan: %r has active scan backoff — suppressing",
            company,
        )
        return

    if r.zscore(REDIS_INFLIGHT_FULLSCAN, company) is not None:
        logger.debug(
            "_maybe_reschedule_full_scan: %r is actively in-flight — suppressing",
            company,
        )
        return

    now          = time.time()
    avg_duration = float(row["avg_fullscan_duration_s"] or 1800.0)
    next_scan    = row["next_full_scan_at"]

    if next_scan is not None:
        next_ts  = next_scan.timestamp() if hasattr(next_scan, "timestamp") else float(next_scan)
        deadline = _next_digest_deadline(now)
        if next_ts > now and (next_ts + avg_duration) < deadline:
            return  # already scheduled and will finish before digest

    # Guard against overwriting a valid ZSET entry when next_full_scan_at is NULL
    # or stale (e.g., queued by _queue_fullscans_for_missed or a prior call here).
    existing_zset = r.zscore(REDIS_POLL_FULLSCAN, company)
    if existing_zset is not None and float(existing_zset) > now:
        logger.debug(
            "_maybe_reschedule_full_scan: %r already in poll:fullscan at %.0f — skipping",
            company, float(existing_zset),
        )
        return

    _atomic_schedule(
        r, REDIS_POLL_FULLSCAN, company,
        now + SCHEDULER_FULL_SCAN_BUFFER_S,
        SCHEDULER_FULL_SCAN_INTERVAL_S, 0.20,
        deadline_ts    = _next_digest_deadline(now),
        avg_duration_s = avg_duration,
        skip_if_future = True,
    )
    logger.info(
        "scheduler: full scan queued for %r (next_full_scan_at=%s avg_duration=%.0fs)",
        company, next_scan, avg_duration,
    )


# loop -> (up_since, last_write_ts, last_ttl) of the current heartbeat run
_hb_run: dict = {}


def _write_scheduler_heartbeat(r, loop: str, dispatched: int, ttl: int = 30,
                               ready: bool = True) -> None:
    """Write per-loop liveness key so a hung loop is detected independently.

    Key: worker:alive:scheduler:{loop}  (e.g. :adaptive or :fullscan)
    Watchdog checks both keys; if either goes stale the loop is flagged dead
    even while the other loop keeps the process alive.
    ttl defaults to 30s (loop rewrites every 1s tick); pass _SCHEDULER_STARTUP_HB_TTL
    for the initial write before the loop starts.

    up_since is the start of the current unbroken run of heartbeats: it resets when
    the previous write is older than that write's own ttl (the key had expired, so
    the loop was down or hung).  The manager caps the poll-ZSET delay at
    now - up_since so downtime before dispatch resumed is never counted as delay.
    """
    now = time.time()
    prev = _hb_run.get(loop)
    up_since = prev[0] if prev and now - prev[1] <= prev[2] else now
    _hb_run[loop] = (up_since, now, ttl)
    try:
        r.set(f"worker:alive:scheduler:{loop}", json.dumps({
            "pid":        os.getpid(),
            "ts":         now,
            "up_since":   up_since,
            "dispatched": dispatched,
            # GATEORD: False only for the startup heartbeat (init_db/rebuild still
            # running); the loop's own writes are ready=True.
            "ready":      ready,
        }), ex=ttl)
    except Exception as _hb_err:
        logger.debug("scheduler: %s_loop heartbeat write failed: %s", loop, _hb_err)


# ─────────────────────────────────────────
# ADAPTIVE DISPATCH LOOP
# ─────────────────────────────────────────

def adaptive_loop() -> None:
    """
    Continuous adaptive scheduler loop — two-layer dispatch (Section 5).

    Every SCHEDULER_TICK_SECS:
        1. Check if paused (pipeline:pause signal from pubsub)
        2. ZRANGEBYSCORE poll:adaptive — non-destructive read of due companies
        3. For each due company:
               a. Backpressure / outage / ceiling checks (same as before)
               b. XADD stream:adaptive   ← crash-safe delivery
               c. ZREM poll:adaptive     ← remove from scheduling ledger
               d. ZADD inflight:scans    ← keep for Phase 10 ceiling tracking
        4. XAUTOCLAIM to reclaim stale messages (worker died mid-scan)

    The ZRANGEBYSCORE→XADD→ZREM ordering is intentional (Section 5):
        - Crash between XADD and ZREM → duplicate in stream, harmless
          (adaptive_seen deduplicates job IDs within a day)
        - ZPOPMIN would lose the company entirely on crash before XADD

    Workers read from REDIS_STREAM_ADAPTIVE via XREADGROUP, call
    on_adaptive_complete(), then XACK.  The result_consumer_loop pattern
    is retired — callbacks happen inline in the worker process.

    Runs until KeyboardInterrupt or thread stop.
    """
    r = get_redis()
    _init_consumer_group(r, REDIS_STREAM_ADAPTIVE)
    logger.info("adaptive_loop: started (stream=%s)", REDIS_STREAM_ADAPTIVE)

    _hw_dispatched = 0   # total adaptive dispatches — reported in heartbeat

    while True:
        try:
            # ── Scheduler heartbeat (worker:alive:scheduler:adaptive) ─────────
            # Tick is 1s; TTL = 30s.  Written at top of every iteration so
            # the key stays live even when no companies are due and during
            # pause periods.  Watchdog compares this key independently from
            # worker:alive:scheduler:fullscan so a hung adaptive_loop is
            # detected even while fullscan_loop is still healthy.
            _write_scheduler_heartbeat(r, "adaptive", _hw_dispatched)

            if _pause_event.is_set():
                time.sleep(SCHEDULER_TICK_SECS)
                _check_auto_resume()
                continue

            # ── Layer 2 Lever 1: halt scan dispatch when manager signals ──────
            if r.exists("manager:lever1:scan:active"):
                logger.debug(
                    "adaptive_loop: Layer 2 Lever 1 active — halting scan dispatch"
                )
                time.sleep(SCHEDULER_TICK_SECS)
                continue

            now = time.time()

            # ── Layer 2 re-intro: trickle dispatch during post-Lever-1 recovery ─
            _reintro_scan = bool(r.exists("manager:reintro:scan:active"))

            # ── Reap pending messages whose owner is not working on them ──────
            claim_stale_work(
                r, REDIS_STREAM_ADAPTIVE, STREAM_CONSUMER_GROUP, op_type="scan",
            )
            sweep_inflight(r, "scan", REDIS_STREAM_ADAPTIVE, STREAM_CONSUMER_GROUP)
            _trim_stream(r, REDIS_STREAM_ADAPTIVE)

            # ── Two-layer dispatch ────────────────────────────────────────────
            # Non-destructive read: ZRANGEBYSCORE, then XADD, then ZREM.
            due = r.zrangebyscore(REDIS_POLL_ADAPTIVE, "-inf", now,
                                  withscores=False)

            for company in due:
                # Refresh heartbeat at the start of each iteration so backpressure
                # or outage `continue` branches don't let the 15s TTL expire.
                _write_scheduler_heartbeat(r, "adaptive", _hw_dispatched)

                # ── Phase 10: outage mode check ───────────────────────────────
                dc_key   = _get_dc_key_for_company(company)
                platform = dc_key.split("_")[0] if "_" in dc_key else dc_key

                # ── Already scanning: never dispatch a second copy ────────────
                # The inflight slot is released by the scan's completion or by
                # the reaper, both of which reschedule the company.
                if r.zscore(f"{REDIS_INFLIGHT_PREFIX}:{dc_key}", company) is not None:
                    logger.debug("adaptive_loop: %r already in flight — skipped", company)
                    continue

                outage_key = f"worker:outage:{platform}"
                if r.exists(outage_key):
                    outage_ttl = r.ttl(outage_key)
                    canary_sent_key    = f"worker:outage:canary_sent:{platform}"
                    canary_company_key = f"worker:outage:canary_company:{platform}"
                    canary_due         = (0 < outage_ttl < WORKER_CANARY_INTERVAL_S)
                    canary_sent        = r.exists(canary_sent_key)

                    if canary_due and not canary_sent:
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
                        delay = max(outage_ttl, 60) if outage_ttl > 0 else 60
                        r.zadd(REDIS_POLL_ADAPTIVE, {company: now + delay})
                        continue

                # ── Phase 10: per-DC learned ceiling throttle ─────────────────
                # Use the per-DC inflight ZSET (not the global PEL count) so
                # each DC key is throttled independently.
                ceiling_raw = r.get(f"worker:ceil:learned:{dc_key}")
                if ceiling_raw:
                    inflight_key = f"{REDIS_INFLIGHT_PREFIX}:{dc_key}"
                    # Count adaptive + fullscan inflight together — both share the
                    # same DC ceiling.  Slots are released by completion, the reaper
                    # and sweep_inflight, never by age.
                    _fullscan_inflight_key = f"{REDIS_INFLIGHT_FULLSCAN_DC_PREFIX}:{dc_key}"
                    pending_count = (
                        r.zcard(inflight_key)
                        + r.zcard(_fullscan_inflight_key)
                    )
                    if pending_count >= int(ceiling_raw):
                        r.zadd(REDIS_POLL_ADAPTIVE, {company: now + 30})
                        logger.debug(
                            "adaptive_loop: ceiling throttle company=%r dc=%r "
                            "pending=%d ceil=%s",
                            company, dc_key, pending_count, ceiling_raw,
                        )
                        continue

                # ── Dispatch: claim inflight slot → XADD → ZREM ──────────────
                # Claim the adaptive inflight slot BEFORE XADD so the fullscan
                # Lua script sees it and cannot consume the last ceiling slot
                # between our pending_count check and the actual dispatch.
                # Release the slot if XADD fails to prevent a slot leak.
                canary_company_key = f"worker:outage:canary_company:{platform}"
                dispatch_context = (
                    "canary"
                    if r.get(canary_company_key) == company
                    else "normal"
                )

                _adaptive_inflight_key = f"{REDIS_INFLIGHT_PREFIX}:{dc_key}"
                r.zadd(_adaptive_inflight_key, {company: now})
                try:
                    _stream_id = r.xadd(
                        REDIS_STREAM_ADAPTIVE,
                        {
                            "company":     company,
                            "scan_type":   "adaptive",
                            "dc_key":      dc_key,
                            "context":     dispatch_context,
                            "enqueued_at": datetime.now(timezone.utc).isoformat(),   # LOGTZ: tz-aware like the other producers
                            "request_id":  f"adp-{int(now)}-{uuid.uuid4().hex[:8]}",
                        },
                    )
                except Exception as _xadd_err:
                    r.zrem(_adaptive_inflight_key, company)
                    logger.error(
                        "adaptive_loop: XADD failed for %r (dc=%s): %s — "
                        "rescheduling in 60s",
                        company, dc_key, _xadd_err,
                    )
                    r.zadd(REDIS_POLL_ADAPTIVE, {company: now + 60})
                    continue
                r.set(f"{REDIS_INFLIGHT_MSG_PREFIX}:scan:{company}", _stream_id)
                r.zrem(REDIS_POLL_ADAPTIVE, company)
                _hw_dispatched += 1

                logger.debug(
                    "adaptive_loop: dispatched %r to %s (dc=%s context=%s)",
                    company, REDIS_STREAM_ADAPTIVE, dc_key, dispatch_context,
                )

                # Re-intro: dispatch at most 1 company per tick during recovery
                if _reintro_scan:
                    logger.debug(
                        "adaptive_loop: re-intro active — halting after 1 dispatch"
                    )
                    break

            time.sleep(SCHEDULER_TICK_SECS)

        except KeyboardInterrupt:
            logger.info("adaptive_loop: stopping")
            break
        except Exception as exc:
            logger.error("adaptive_loop: error: %s", exc, exc_info=True)
            time.sleep(5)


# ─────────────────────────────────────────
# FULL SCAN DISPATCH LOOP
# ─────────────────────────────────────────

def fullscan_loop() -> None:
    """
    Continuous full scan scheduler loop — two-layer dispatch (Section 9).

    Every SCHEDULER_TICK_SECS:
        1. Check if paused
        2. ZRANGEBYSCORE poll:fullscan — non-destructive read of due companies
        3. For each due company:
               a. Backpressure check (fullscan detail queue depth)
               b. XADD stream:fullscan   ← crash-safe delivery
               c. ZREM poll:fullscan     ← remove from scheduling ledger
        4. XAUTOCLAIM to reclaim stale messages (fullscan worker died mid-scan)

    Fullscan workers read from REDIS_STREAM_FULLSCAN via XREADGROUP, run the
    complete Bloom-filter-based scan, call on_fullscan_complete() (or handle
    scheduling directly in fullscan.py), then XACK.

    The old SCAN_QUEUE → scan_worker → result_consumer_loop path for fullscans
    is retired.  Full scans now go through dedicated fullscan worker processes
    running workers/fullscan.py.

    Runs until KeyboardInterrupt or thread stop.
    """
    r = get_redis()
    _init_consumer_group(r, REDIS_STREAM_FULLSCAN)
    logger.info("fullscan_loop: started (stream=%s)", REDIS_STREAM_FULLSCAN)


    _hw_dispatched = 0   # total fullscan dispatches — reported in heartbeat

    while True:
        try:
            # ── Scheduler heartbeat (worker:alive:scheduler:fullscan) ─────────
            # Written at the TOP of every iteration — before the pause check and
            # before the due-company loop — so the key stays live even when no
            # fullscans are due (the common case).  Without this, the key would
            # expire after 15 idle ticks and trigger a false dead-loop alert.
            _write_scheduler_heartbeat(r, "fullscan", _hw_dispatched)

            if _pause_event.is_set():
                time.sleep(SCHEDULER_TICK_SECS)
                _check_auto_resume()
                continue

            # ── Layer 2 Lever 1: halt fullscan dispatch when manager signals ──
            if r.exists("manager:lever1:fullscan:active"):
                logger.debug(
                    "fullscan_loop: Layer 2 Lever 1 active — halting fullscan dispatch"
                )
                time.sleep(SCHEDULER_TICK_SECS)
                continue

            now = time.time()

            # ── Layer 2 re-intro: trickle dispatch during post-Lever-1 recovery ─
            _reintro_fullscan = bool(r.exists("manager:reintro:fullscan:active"))

            # ── Reap pending messages whose owner is not working on them ──────
            claim_stale_work(
                r, REDIS_STREAM_FULLSCAN, STREAM_CONSUMER_GROUP, op_type="fullscan",
            )
            sweep_inflight(r, "fullscan", REDIS_STREAM_FULLSCAN, STREAM_CONSUMER_GROUP)
            _trim_stream(r, REDIS_STREAM_FULLSCAN)

            # ── Two-layer dispatch ────────────────────────────────────────────
            due = r.zrangebyscore(REDIS_POLL_FULLSCAN, "-inf", now,
                                  withscores=False)

            for company in due:
                # Refresh mid-loop so a large due-list doesn't let the TTL
                # expire between the top-of-loop write and the last dispatch.
                _write_scheduler_heartbeat(r, "fullscan", _hw_dispatched)

                dc_key = _get_dc_key_for_company(company)

                # ── Already scanning: never dispatch a second copy ────────────
                if (r.zscore(f"{REDIS_INFLIGHT_FULLSCAN_DC_PREFIX}:{dc_key}", company) is not None
                        or r.zscore(REDIS_INFLIGHT_FULLSCAN, company) is not None):
                    logger.debug("fullscan_loop: %r already in flight — skipped", company)
                    continue

                # ── Phase 10: per-DC learned ceiling throttle (atomic) ────────
                # Fullscan and adaptive workers share the same learned ceiling for
                # a DC key.  Use a Lua script so check + slot claim is atomic —
                # no two dispatches can both read "slot available" and both proceed.
                ceiling_raw = r.get(f"worker:ceil:learned:{dc_key}")
                if ceiling_raw:
                    _now_ts      = time.time()
                    _claimed     = r.eval(
                        _FULLSCAN_INFLIGHT_CLAIM_LUA,
                        2,
                        f"{REDIS_INFLIGHT_PREFIX}:{dc_key}",
                        f"{REDIS_INFLIGHT_FULLSCAN_DC_PREFIX}:{dc_key}",
                        int(ceiling_raw),
                        company,
                        _now_ts,
                    )
                    if not _claimed:
                        r.zadd(REDIS_POLL_FULLSCAN, {company: now + 30})
                        logger.debug(
                            "fullscan_loop: ceiling throttle company=%r dc=%r ceil=%s",
                            company, dc_key, ceiling_raw,
                        )
                        continue

                try:
                    _stream_id = r.xadd(
                        REDIS_STREAM_FULLSCAN,
                        {
                            "company":     company,
                            "scan_type":   "fullscan",
                            "dc_key":      dc_key,
                            "context":     "normal",
                            "enqueued_at": datetime.now(timezone.utc).isoformat(),   # LOGTZ: tz-aware like the other producers
                            "request_id":  f"full-{int(now)}-{uuid.uuid4().hex[:8]}",
                        },
                    )
                except Exception as _xadd_exc:
                    # XADD failed — if the Lua script claimed a DC inflight slot
                    # (ceiling_raw path), release it now so it doesn't leak until
                    # the stale cleanup window expires.
                    if ceiling_raw:
                        try:
                            r.zrem(
                                f"{REDIS_INFLIGHT_FULLSCAN_DC_PREFIX}:{dc_key}",
                                company,
                            )
                        except Exception as _zrem_exc:
                            logger.debug(
                                "fullscan_loop: ZREM inflight slot failed for %r (dc=%s): %s — slot will expire via stale window",
                                company, dc_key, _zrem_exc,
                            )
                    logger.error(
                        "fullscan_loop: XADD failed for %r (dc=%s): %s — rescheduling",
                        company, dc_key, _xadd_exc,
                    )
                    r.zadd(REDIS_POLL_FULLSCAN, {company: now + 60})
                    continue
                if not ceiling_raw:
                    # No ceiling learned yet — slot was not claimed by Lua above;
                    # ZADD here so the inflight ZSET is populated from day one and
                    # the ceiling learner has data to work with.
                    r.zadd(
                        f"{REDIS_INFLIGHT_FULLSCAN_DC_PREFIX}:{dc_key}",
                        {company: time.time()},
                    )
                r.set(f"{REDIS_INFLIGHT_MSG_PREFIX}:fullscan:{company}", _stream_id)
                r.zrem(REDIS_POLL_FULLSCAN, company)
                _hw_dispatched += 1

                # Re-intro: dispatch at most 1 company per tick during recovery
                if _reintro_fullscan:
                    logger.debug(
                        "fullscan_loop: re-intro active — halting after 1 dispatch"
                    )
                    break

                logger.info(
                    "fullscan_loop: dispatched %r to %s (dc=%s)",
                    company, REDIS_STREAM_FULLSCAN, dc_key,
                )

            time.sleep(SCHEDULER_TICK_SECS)

        except KeyboardInterrupt:
            logger.info("fullscan_loop: stopping")
            break
        except Exception as exc:
            logger.error("fullscan_loop: error: %s", exc, exc_info=True)
            time.sleep(5)


# ─────────────────────────────────────────
# PAUSE / RESUME (Pub/Sub)
# ─────────────────────────────────────────

def pubsub_listener_loop() -> None:
    """
    Listens on pipeline:pause and pipeline:resume channels.

    Sets/clears the two shared Events used by dispatch loops and workers:
        _pause_event  — set on pause; cleared on resume
        _resume_event — cleared on pause; set on resume

    Workers call _resume_event.wait(timeout=N) when paused so they unblock
    within ~1s of a resume signal rather than spinning on a fixed sleep.
    _check_auto_resume() can also clear _pause_event / set _resume_event if
    the cronchain heartbeat expires (safety net against permanent pause).

    Uses get_message(timeout=5) instead of listen() so a silently wedged TCP
    session is detected within 5 s and the subscription is re-established.
    """
    import time as _time
    while True:
        try:
            r      = get_pubsub_redis()
            pubsub = r.pubsub()
            pubsub.subscribe(REDIS_PAUSE_CHANNEL, REDIS_RESUME_CHANNEL)
            logger.info("pubsub_listener: subscribed to pause/resume channels")
            while True:
                message = pubsub.get_message(timeout=5.0)
                if message is None or message["type"] != "message":
                    continue
                channel = message["channel"]
                if channel == REDIS_PAUSE_CHANNEL:
                    logger.info("pubsub_listener: PAUSE received — halting dispatchers")
                    _pause_event.set()
                    _resume_event.clear()
                elif channel == REDIS_RESUME_CHANNEL:
                    logger.info("pubsub_listener: RESUME received — resuming dispatchers")
                    _resume_event.set()
                    _pause_event.clear()
        except Exception as _ps_err:
            logger.warning(
                "pubsub_listener: connection lost (%s) — reconnecting in 5s", _ps_err
            )
            _time.sleep(5)


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
        if not _pause_event.is_set():
            # AR: a resume was already received (the chain's normal final step
            # races the heartbeat expiry) — nothing to auto-resume.
            logger.info("scheduler: cron chain heartbeat expired after a normal resume — no action")
            return
        logger.warning(
            "scheduler: cron chain heartbeat expired and no db:maintenance flag — "
            "auto-resuming workers (cron chain may have crashed)"
        )
        _resume_event.set()
        _pause_event.clear()


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
            if company.startswith("ca:"):
                id_ = int(company[3:])
                row = conn.execute("""
                    SELECT platform AS ats_platform, slug AS ats_slug
                    FROM company_ats WHERE id = %s
                """, (id_,)).fetchone()
            else:
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


# ── Process target functions ──────────────────────────────────────────────────

_mp_ctx = None


def _worker_mp_context():
    """
    Process-creation context for pool workers (bug FK).

    A plain fork from a scheduler thread copies every lock another thread holds at
    that instant (module import locks, the DB pool lock) with no thread left in the
    child to release it, so the worker hangs before it starts.  The "forkserver"
    start method forks workers from a separate single-threaded server process, so no
    scheduler lock can be inherited.  The server preloads the worker modules once, so
    a new worker starts as fast as a fork.  Platforms without forkserver (Windows)
    keep their default method.  The first call must come from the main thread before
    the scheduler threads start (run_scheduler's initial spawn does).
    """
    global _mp_ctx
    if _mp_ctx is None:
        if "forkserver" in multiprocessing.get_all_start_methods():
            ctx = multiprocessing.get_context("forkserver")
            ctx.set_forkserver_preload([
                "workers.scheduler", "workers.scan_worker",
                "workers.detail_worker", "workers.fullscan",
            ])
        else:
            ctx = multiprocessing.get_context()
        _mp_ctx = ctx
    return _mp_ctx


def _init_worker_logging() -> None:
    """Send this worker's log lines to scheduler.log, as the forked workers did."""
    from logger import init_logging
    init_logging("scheduler")


def _reset_inherited_db_pool() -> None:
    """
    Discard any PostgreSQL connection pool inherited from the parent process.

    Under a plain fork the child inherits the parent's _pool, which holds open
    TCP sockets to PostgreSQL.  Both parent and child would then share the same
    file descriptors, causing libpq to emit "error with status PGRES_TUPLES_OK and
    no message from the libpq" when the child tries to use or re-initialise the
    connection.  Workers are now started through the fork server (see
    _worker_mp_context), so nothing is inherited; this stays as a guard for the
    platforms that fall back to fork.

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
    _install_stack_dump()
    _init_worker_logging()
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
    _install_stack_dump()
    _init_worker_logging()
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


def _fullscan_worker_process(shutdown_event: multiprocessing.Event) -> None:
    """
    Target function for fullscan worker processes (multiprocessing.Process).

    Runs workers/fullscan.py:run_worker() which reads from stream:fullscan
    via XREADGROUP, executes the complete Bloom-filter full scan, handles
    checkpoint/resume on pause, and XACKs on completion.

    Shutdown: watcher injects KeyboardInterrupt into the main thread when
    shutdown_event fires — fullscan.run_worker()'s KeyboardInterrupt handler
    exits the XREADGROUP loop cleanly.
    """
    _install_stack_dump()
    _init_worker_logging()
    _reset_inherited_db_pool()

    def _watcher() -> None:
        shutdown_event.wait()
        ctypes.pythonapi.PyThreadState_SetAsyncExc(
            ctypes.c_ulong(threading.main_thread().ident),
            ctypes.py_object(KeyboardInterrupt),
        )

    threading.Thread(
        target=_watcher, daemon=True, name="fullscan_shutdown_watcher",
    ).start()

    from workers.fullscan import run_worker
    run_worker(shutdown_event=shutdown_event, skip_init_db=True)


# ── Pool management helpers ───────────────────────────────────────────────────

def _spawn_worker(worker_type: str) -> tuple:
    """
    Spawn one worker process of the given type.

    Args:
        worker_type: "scan", "detail", or "fullscan"

    Returns:
        (multiprocessing.Process, multiprocessing.Event) — process handle and
        its shutdown event.  Store this tuple in the pool list.
    """
    ctx = _worker_mp_context()
    shutdown_event = ctx.Event()
    target_map = {
        "scan":     _scan_worker_process,
        "detail":   _detail_worker_process,
        "fullscan": _fullscan_worker_process,
    }
    target = target_map.get(worker_type, _scan_worker_process)
    proc = ctx.Process(
        target = target,
        args   = (shutdown_event,),
        name   = f"{worker_type}_worker_{int(time.time())}",
        daemon = False,   # non-daemon: scheduler waits for clean exit at shutdown
    )
    proc.start()
    _worker_spawn_times[proc.pid] = (worker_type, time.time())
    logger.info("scheduler: spawned %s_worker pid=%d", worker_type, proc.pid)
    return proc, shutdown_event


def _worker_consumer(worker_type: str, pid: int) -> str:
    """The progress-report consumer name a pool worker publishes under."""
    import socket as _socket
    host = _socket.gethostname()
    if worker_type == "detail":
        return f"detail_worker:{host}:{pid}"
    return f"worker-{host}-{pid}"


def _stalled_pids(r, worker_type: str, pool: list) -> set:
    """PIDs in the pool whose progress deadline has passed (hung, not merely slow)."""
    stalled = set()
    for proc, _event in list(pool):
        try:
            if proc.is_alive() and progress.is_stalled(
                    r, _worker_consumer(worker_type, proc.pid)):
                stalled.add(proc.pid)
        except Exception:
            continue
    return stalled


_stack_dump_file = None   # kept open so faulthandler can write to it from a signal


def _install_stack_dump() -> None:
    """
    Worker side of the hung-start evidence: on SIGUSR1 write every thread's stack to
    logs/worker_stack_<pid>.log (faulthandler runs at C level, so it works even when
    the interpreter is deadlocked on a lock inherited at start).  POSIX only.
    """
    global _stack_dump_file
    try:
        import faulthandler
        import signal
        from logger import LOG_DIR
        if not hasattr(signal, "SIGUSR1"):
            return
        LOG_DIR.mkdir(parents=True, exist_ok=True)
        _stack_dump_file = open(LOG_DIR / f"worker_stack_{os.getpid()}.log", "a")
        faulthandler.register(signal.SIGUSR1, file=_stack_dump_file, all_threads=True)
    except Exception:
        pass


def _stack_file(pid: int):
    from logger import LOG_DIR
    return LOG_DIR / f"worker_stack_{pid}.log"


def _remove_stack_file(pid: int) -> None:
    """Delete the (empty) stack-dump file of a worker that exited normally."""
    try:
        _stack_file(pid).unlink()
    except Exception:
        pass


def _dump_worker_stack(ptype: str, pid: int) -> None:
    """
    Record where a worker that never started is stuck, before it is killed.

    Asks the worker for a faulthandler dump (SIGUSR1; present in workers of this
    version) and also tries py-spy (works on workers of any version).  Both are
    best effort and logged at ERROR so the evidence reaches the log.
    """
    import shutil
    import signal
    import subprocess
    import sys
    if hasattr(signal, "SIGUSR1"):
        try:
            os.kill(pid, signal.SIGUSR1)
            time.sleep(1.0)
        except Exception as exc:
            logger.error("scheduler: stack signal to %s_worker pid=%d failed: %s",
                         ptype, pid, exc)
    try:
        text = _stack_file(pid).read_text(errors="replace").strip()
    except Exception:
        text = ""
    if text:
        logger.error("scheduler: %s_worker pid=%d stack (faulthandler):\n%s",
                     ptype, pid, text[-6000:])
    spy = shutil.which("py-spy") or os.path.join(os.path.dirname(sys.executable), "py-spy")
    if os.path.exists(spy):
        try:
            out = subprocess.run([spy, "dump", "--pid", str(pid)],
                                 capture_output=True, text=True, timeout=15)
            logger.error("scheduler: %s_worker pid=%d stack (py-spy):\n%s",
                         ptype, pid, (out.stdout or out.stderr).strip()[-6000:])
        except Exception as exc:
            logger.error("scheduler: py-spy dump of %s_worker pid=%d failed: %s",
                         ptype, pid, exc)
    elif not text:
        logger.error("scheduler: no stack evidence for %s_worker pid=%d "
                     "(no faulthandler output, py-spy not installed)", ptype, pid)


def _start_state(r, ptype: str, pool: list) -> dict:
    """
    Spawn-vs-started picture of one pool for scheduler:health.

    started = workers that have published a heartbeat; pending = spawned but not yet;
    oldest_pending_start_s = age of the longest-waiting one; start_warn_s = the learned
    slowest start-up x PROGRESS_GAP_FACTOR (0 until one start has been observed).
    """
    try:
        hw = float(r.hget(REDIS_START_LATENCY_HASH, ptype) or 0)
    except Exception:
        hw = 0.0
    now = time.time()
    pending = 0
    starting = 0
    oldest = 0.0
    for proc, _event in list(pool):
        info = _worker_spawn_times.get(proc.pid)
        if info is not None and proc.pid not in _worker_started:
            age = now - info[1]
            pending += 1
            oldest = max(oldest, age)
            # still inside its start-up allowance: expected to join, so it counts
            # as a worker (before any start was observed the kill deadline applies)
            if hw <= 0 or age <= hw * PROGRESS_GAP_FACTOR:
                starting += 1
    return {
        "started":                len(pool) - pending,
        "starting":               starting,
        "pending_start":          pending,
        "oldest_pending_start_s": round(oldest, 1),
        "start_warn_s":           round(hw * PROGRESS_GAP_FACTOR, 1),
        "unstarted_kills":        _unstarted_kills[ptype],
    }


def _audit_orphan_workers(spawned: set) -> None:
    """
    At scheduler start, report worker processes this scheduler did not spawn.

    A heartbeat key worker:alive:{type}_worker:{host}:{pid} whose process is still
    running (POSIX only) and is not one of ours is a leftover from an earlier
    scheduler or a manual run, logged at ERROR.
    """
    if os.name != "posix":
        return
    import socket as _socket
    host = _socket.gethostname()
    try:
        r = get_redis()
        for wtype in ("scan_worker", "detail_worker", "fullscan_worker"):
            for key in r.scan_iter(match=f"worker:alive:{wtype}:{host}:*", count=100):
                k = key.decode() if isinstance(key, bytes) else key
                try:
                    pid = int(k.rsplit(":", 1)[1])
                except ValueError:
                    continue
                if pid in spawned or pid == os.getpid():
                    continue
                try:
                    os.kill(pid, 0)
                except ProcessLookupError:
                    continue
                except Exception:
                    pass
                logger.error("scheduler: %s pid=%d is running but was not started by "
                             "this scheduler (orphan from an earlier run or a manual "
                             "start)", wtype, pid)
    except Exception as exc:
        logger.warning("scheduler: orphan-worker audit failed: %s", exc)


def _unstarted_pids(r, snapshot: list) -> set:
    """
    PIDs of alive pool workers that never finished starting (bug FK safety net).

    A worker has started once it publishes worker:alive:{type}_worker:{host}:{pid}
    (its heartbeat is written right after "started").  Each observed spawn->heartbeat
    time updates the slowest start-up seen per type (REDIS_START_LATENCY_HASH).  A
    worker with no heartbeat after max(slowest start x PROGRESS_GAP_FACTOR,
    PROGRESS_COLD_GAP_S) is unstarted: the same learned allowance and floor the stall
    check uses, so a start that is merely slow under load is never killed.
    snapshot: [(ptype, pool_list), ...].  Redis errors skip the check.
    """
    import socket as _socket
    host = _socket.gethostname()
    now = time.time()
    unstarted: set = set()
    live: set = set()
    for ptype, pool in snapshot:
        try:
            hw = float(r.hget(REDIS_START_LATENCY_HASH, ptype) or 0)
        except Exception:
            hw = 0.0
        deadline = max(hw * PROGRESS_GAP_FACTOR, float(PROGRESS_COLD_GAP_S))
        for proc, _event in pool:
            pid = proc.pid
            live.add(pid)
            info = _worker_spawn_times.get(pid)
            if pid in _worker_started or info is None:
                continue
            try:
                if not proc.is_alive():
                    continue
                age = now - info[1]
                if r.exists(f"worker:alive:{ptype}_worker:{host}:{pid}"):
                    _worker_started.add(pid)
                    if age > hw:
                        r.hset(REDIS_START_LATENCY_HASH, ptype, round(age, 1))
                        hw = age
                elif age > deadline:
                    unstarted.add(pid)
            except Exception:
                continue
    _worker_started.intersection_update(live)
    return unstarted


_last_health_snapshot_ts = 0.0


def _snapshot_worker_health(r, payload: dict) -> None:
    """
    Append the current pool state to the trimmed stream worker:health_history, at most
    once per WORKER_HEALTH_SNAPSHOT_INTERVAL_S, so a hang that clears itself leaves a
    record (counters are cumulative: the report reads deltas between rows).
    scripts/worker_health_report.py prints it.
    """
    global _last_health_snapshot_ts
    now = time.time()
    if now - _last_health_snapshot_ts < WORKER_HEALTH_SNAPSHOT_INTERVAL_S:
        return
    try:
        lat = r.hgetall(REDIS_START_LATENCY_HASH) or {}
        lat = {(k.decode() if isinstance(k, bytes) else k):
               float(v.decode() if isinstance(v, bytes) else v) for k, v in lat.items()}
        r.xadd(REDIS_WORKER_HEALTH_STREAM,
               {"data": json.dumps({"ts": now, "pool": payload["pool"],
                                    "start_latency_s": lat})},
               maxlen=WORKER_HEALTH_STREAM_MAXLEN, approximate=True)
        _last_health_snapshot_ts = now
    except Exception as exc:
        logger.warning("scheduler: worker health snapshot failed: %s", exc)


def _write_scheduler_health() -> None:
    """
    Publish current pool state and consecutive-death counters to Redis as
    scheduler:health (TTL = _SCHEDULER_HEALTH_TTL).

    The watchdog reads this key to make informed decisions without duplicating
    the scheduler's internal pool-management logic:

      - consecutive_deaths ≥ 5  → ERROR  (workers failing on startup repeatedly)
      - consecutive_deaths ≥ 3  → WARNING (scheduler struggling to stabilize)
      - consecutive_deaths < 3  → OK     (transient deaths, scheduler handling it)

    The key expires naturally if the scheduler dies — the watchdog treats a
    missing key as an additional signal that the scheduler is gone (beyond the
    faster worker:alive:scheduler heartbeat check).

    Called after every pool event: death + respawn, scale up, scale down.
    Never called under _pool_lock — Redis write must not hold the pool lock.
    """
    try:
        _fp = globals().get("_fullscan_pool_ref", [])
        _r = get_redis()
        _pools = {"scan": _scan_pool, "detail": _detail_pool, "fullscan": _fp}
        _st = {t: len(_stalled_pids(_r, t, p)) for t, p in _pools.items()}
        _ss = {t: _start_state(_r, t, p) for t, p in _pools.items()}
        payload = {"ts": time.time(), "pool": {}}
        for _t in _pools:
            payload["pool"][_t] = {
                # alive = heartbeating and not stalled, plus workers still starting
                # inside their learned allowance; a start that overruns it (probably
                # hung) drops out.  The manager sizes the pool from this number.
                "alive":              max(0, _ss[_t]["started"] + _ss[_t]["starting"]
                                          - _st[_t]),
                "consecutive_deaths": _consecutive_deaths[_t],
                "total_replacements": _total_replacements[_t],
                **_ss[_t],
            }
        get_redis().set(
            _SCHEDULER_HEALTH_KEY,
            json.dumps(payload),
            ex=_SCHEDULER_HEALTH_TTL,
        )
        _snapshot_worker_health(_r, payload)
    except Exception as exc:
        logger.warning("scheduler: failed to write scheduler:health: %s", exc)


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
    _fullscan_pool = globals().get("_fullscan_pool_ref", [])
    replacements: list = []   # (ptype, old_pid, exitcode) — collected under lock

    # A worker past its progress deadline is hung even though its process lives;
    # it is killed and replaced like a crashed one.  Redis is read before the lock.
    stalled: set = set()
    unstarted: set = set()
    try:
        _sr = get_redis()
        with _pool_lock:
            _snap = [("scan", list(_scan_pool)), ("detail", list(_detail_pool)),
                     ("fullscan", list(_fullscan_pool))]
        for _pt, _pl in _snap:
            stalled |= _stalled_pids(_sr, _pt, _pl)
        unstarted = _unstarted_pids(_sr, _snap)
        if unstarted:
            logger.error(
                "scheduler: HUNG START — pids %s never finished starting (no "
                "heartbeat past the start-up allowance) — dumping stacks, killing",
                sorted(unstarted))
            for _pt, _pl in _snap:
                for _p, _e in _pl:
                    if _p.pid in unstarted:
                        _dump_worker_stack(_pt, _p.pid)
            stalled |= unstarted
    except Exception as exc:
        logger.warning("scheduler: stalled-worker check failed: %s", exc)

    with _pool_lock:
        for pool, ptype in (
            (_scan_pool,     "scan"),
            (_detail_pool,   "detail"),
            (_fullscan_pool, "fullscan"),
        ):
            for i, (proc, event) in enumerate(pool):
                if proc.pid in stalled and proc.is_alive():
                    logger.warning(
                        "scheduler: %s_worker pid=%d past its progress deadline "
                        "— killing", ptype, proc.pid)
                    try:
                        get_redis().delete(
                            f"{REDIS_WORKER_CUR_PREFIX}:{_worker_consumer(ptype, proc.pid)}")
                    except Exception:
                        pass
                    proc.kill()
                    proc.join(timeout=2)
                if not proc.is_alive():
                    logger.warning(
                        "scheduler: %s_worker pid=%d died unexpectedly "
                        "(exit=%s) — spawning replacement",
                        ptype, proc.pid, proc.exitcode,
                    )
                    replacements.append((ptype, proc.pid, proc.exitcode))

                    # ── Consecutive-death tracking ────────────────────────────
                    # If the dead worker lived ≥ _WORKER_STABLE_AFTER_S we treat
                    # it as a one-off: reset the streak.  Quick repeated deaths
                    # increment the counter so the watchdog can escalate.
                    spawn_info = _worker_spawn_times.pop(proc.pid, None)
                    if spawn_info:
                        worker_age = time.time() - spawn_info[1]
                        if proc.pid in unstarted:
                            # A hung start is a failed start however long it
                            # waited: always escalate so the watchdog alerts.
                            _unstarted_kills[ptype] += 1
                            if _consecutive_deaths[ptype] == 0:
                                _death_streak_started_at[ptype] = time.time()
                            _consecutive_deaths[ptype] += 1
                        elif worker_age >= _WORKER_STABLE_AFTER_S:
                            _remove_stack_file(proc.pid)
                            _consecutive_deaths[ptype]      = 0    # stable run — reset
                            _death_streak_started_at[ptype] = 0.0  # streak cleared
                        else:
                            if _consecutive_deaths[ptype] == 0:
                                # First rapid death — record when the streak began
                                _death_streak_started_at[ptype] = time.time()
                            _consecutive_deaths[ptype] += 1  # rapid death — escalate
                    else:
                        if _consecutive_deaths[ptype] == 0:
                            _death_streak_started_at[ptype] = time.time()
                        _consecutive_deaths[ptype] += 1      # no spawn record
                    _total_replacements[ptype] += 1

                    pool[i] = _spawn_worker(ptype)

        # ── Stable-worker pre-reset ───────────────────────────────────────────
        # The dead-worker branch above resets consecutive_deaths only when a
        # stable worker eventually dies.  If a replacement is still running and
        # has already reached stability (alive ≥ _WORKER_STABLE_AFTER_S), reset
        # the counter now so the watchdog stops emitting false WARNING/ERROR
        # while the system is actually healthy.
        #
        # _worker_spawn_times at this point contains only LIVING workers — dead
        # workers were popped in the loop above.
        _now_ts = time.time()
        for _pid, (_wtype, _spawn_ts) in list(_worker_spawn_times.items()):
            if (
                (_now_ts - _spawn_ts) >= _WORKER_STABLE_AFTER_S
                and _consecutive_deaths[_wtype] > 0
                # A worker that has not finished starting is not stable, however old.
                and _pid in _worker_started
                # Only reset if this worker was spawned AFTER the death streak
                # began.  An old pre-streak worker being alive does not mean
                # the streak workers are now healthy.
                and _spawn_ts > _death_streak_started_at[_wtype]
            ):
                logger.info(
                    "scheduler: %s_worker pid=%d (spawned after streak) stable for %ds — "
                    "resetting consecutive_deaths from %d to 0",
                    _wtype, _pid, int(_now_ts - _spawn_ts), _consecutive_deaths[_wtype],
                )
                _consecutive_deaths[_wtype]      = 0
                _death_streak_started_at[_wtype] = 0.0

    # Publish updated pool state AFTER releasing the lock.
    # Always refresh — not just on replacements — so the key stays alive during
    # stable periods (TTL = 10 min; liveness check runs every ~5 s).  Without
    # periodic refresh the key expires when no workers die, making the watchdog
    # report "scheduler:health missing — pool state unknown" on a healthy system.
    _write_scheduler_health()

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

    # ── Release per-DC inflight slots for dead fullscan workers ──────────────
    # _run_fullscan() writes worker:current_job:fullscan:{pid} = "company|dc_key"
    # at scan start.  If the worker crashed, the finally block never ran, so the
    # inflight:fullscans:{dc_key} slot is still held.  Release it here.
    if any(ptype == "fullscan" for ptype, _, _ in replacements):
        _r = get_redis()
        for ptype, old_pid, _exit in replacements:
            if ptype != "fullscan":
                continue
            job_key = f"{WORKER_CURRENT_JOB_FULLSCAN_PREFIX}:{old_pid}"
            try:
                raw = _r.get(job_key)
                if raw:
                    parts = raw.split("|", 1)
                    if len(parts) == 2:
                        dead_company, dead_dc_key = parts
                        if dead_dc_key:
                            _r.zrem(
                                f"{REDIS_INFLIGHT_FULLSCAN_DC_PREFIX}:{dead_dc_key}",
                                dead_company,
                            )
                            logger.info(
                                "scheduler: released inflight slot for dead "
                                "fullscan_worker pid=%d company=%r dc=%r",
                                old_pid, dead_company, dead_dc_key,
                            )
                _r.delete(job_key)
            except Exception as exc:
                logger.warning(
                    "scheduler: inflight cleanup failed for dead "
                    "fullscan_worker pid=%d: %s",
                    old_pid, exc,
                )


def _manager_ceiling(r, pool: str) -> int:
    """Worker ceiling of a pool as the manager currently holds it (base - lent + borrowed)."""
    from workers.manager import _get_worker_ceil
    return _get_worker_ceil(r, pool)


def _add_one_worker(worker_type: str, ceil_: int) -> bool:
    """
    Add one worker to the given pool if below its ceiling.

    Args:
        worker_type: "scan", "detail", or "fullscan"
        ceil_:       maximum pool size for this worker type right now

    Returns:
        True if a worker was added, False if already at ceil_.
    """
    global _scan_pool, _detail_pool
    if worker_type == "fullscan":
        pool = globals().get("_fullscan_pool_ref")
        if pool is None:
            return False
    else:
        pool = _scan_pool if worker_type == "scan" else _detail_pool

    with _pool_lock:
        if len(pool) >= ceil_:
            return False
        pool.append(_spawn_worker(worker_type))

    _write_scheduler_health()   # publish updated pool size
    return True


def _busy_since_by_pid(r, worker_type: str) -> dict:
    """
    pid -> busy_since (start of the job in progress, None when idle), read from the
    local workers' heartbeats worker:alive:{type}_worker:{host}:{pid}.  A pid with
    no readable heartbeat is absent (the reading is up to one beat old).
    """
    out = {}
    for key in r.scan_iter(match=f"worker:alive:{worker_type}_worker:*", count=50):
        raw = r.get(key)
        if not raw:
            continue
        try:
            pid = int((key.decode() if isinstance(key, bytes) else key).rsplit(":", 1)[1])
            out[pid] = json.loads(raw).get("busy_since")
        except (TypeError, ValueError, IndexError):
            continue
    return out


def _pick_removal_index(pids: list, stalled: set, busy_since: dict) -> int:
    """
    Index of the worker to remove: a stalled one first, else an idle one, else the
    busy one whose job started most recently.  Latest list entry wins ties; a pid
    with no heartbeat reading counts as busy with the oldest start, so it is only
    picked when nothing better exists.
    """
    for i in range(len(pids) - 1, -1, -1):
        if pids[i] in stalled:
            return i
    for i in range(len(pids) - 1, -1, -1):
        if pids[i] in busy_since and busy_since[pids[i]] is None:
            return i
    return max(
        range(len(pids)),
        key=lambda i: (busy_since.get(pids[i]) or 0.0, i),
    )


def _remove_one_worker(worker_type: str, floor_: int) -> bool:
    """
    Remove one worker from the given pool if above its floor.

    Signals one entry to finish its current job and exit cleanly: a stalled
    worker first, then an idle one, then the busy one whose job started last.
    The entry is removed from the pool list immediately so the liveness
    check doesn't spawn a replacement for it.

    Args:
        worker_type: "scan", "detail", or "fullscan"
        floor_:      minimum pool size for this worker type right now

    Returns:
        True if a worker was removed, False if already at floor_.
    """
    global _scan_pool, _detail_pool
    if worker_type == "fullscan":
        pool = globals().get("_fullscan_pool_ref")
        if pool is None:
            return False
    else:
        pool = _scan_pool if worker_type == "scan" else _detail_pool

    # Redis is read before taking the lock.  Order of removal: stalled, then idle,
    # then busy (the job started most recently, so the least work is thrown away).
    try:
        with _pool_lock:
            _snap = list(pool)
        _r = get_redis()
        _sp = _stalled_pids(_r, worker_type, _snap)
        _busy = _busy_since_by_pid(_r, worker_type)
    except Exception:
        _sp, _busy = set(), {}

    with _pool_lock:
        if len(pool) <= floor_:
            return False
        _idx = _pick_removal_index([p.pid for p, _e in pool], _sp, _busy)
        proc, event = pool.pop(_idx)
        _worker_spawn_times.pop(proc.pid, None)   # prevent stale entry accumulation
        _retiring_workers.append((worker_type, proc, time.time()))

    event.set()
    logger.info(
        "scheduler: removing %s_worker pid=%d (shutdown event set, "
        "waiting for it to finish current job)",
        worker_type, proc.pid,
    )
    _write_scheduler_health()   # publish updated pool size
    return True


def _get_fullscan_pool_size() -> int:
    """Return current fullscan pool size — thread-safe."""
    _fp = globals().get("_fullscan_pool_ref", [])
    with _pool_lock:
        return len(_fp)


def _deprioritise_platform(r, platform: str) -> int:
    """
    Push all companies for a given ATS platform forward in poll:adaptive.

    Called by manager._check_error_spikes() (via manager:cmds) when a
    platform's error rate is high but its concurrency limit is already at
    floor.  Pushing companies forward gives the platform breathing room to
    recover before their next scan is attempted.

    Only companies currently in poll:adaptive are affected, and only those
    not already pushed far into the future (to avoid cascading delays).

    Returns:
        Number of companies pushed forward.
    """
    conn = get_conn()
    try:
        pc_rows = conn.execute("""
            SELECT company FROM prospective_companies
            WHERE ats_platform = %s
        """, (platform,)).fetchall()
        ca_rows = conn.execute("""
            SELECT 'ca:' || id::text AS company
            FROM company_ats
            WHERE platform = %s AND is_monitored = TRUE
        """, (platform,)).fetchall()
    finally:
        conn.close()

    rows = list(pc_rows) + list(ca_rows)
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

def _reap_retiring_workers() -> None:
    """
    ZJ: reap workers removed from a pool by _remove_one_worker.

    A removed worker leaves the pool list at once, so the liveness check no
    longer sees it.  Without this it stays a zombie after it exits, and a worker
    that never exits on its shutdown event runs on forever.  Finished procs are
    joined; ones still alive WORKER_SHUTDOWN_TIMEOUT_S after removal are killed.
    """
    now = time.time()
    with _pool_lock:
        entries = list(_retiring_workers)
    done = []
    for entry in entries:
        ptype, proc, removed_at = entry
        try:
            if not proc.is_alive():
                proc.join(timeout=0)
                done.append(entry)
            elif now - removed_at >= WORKER_SHUTDOWN_TIMEOUT_S:
                logger.warning(
                    "scheduler: retiring %s_worker pid=%d still alive %ds after "
                    "removal — killing", ptype, proc.pid, int(now - removed_at))
                try:
                    get_redis().delete(
                        f"{REDIS_WORKER_CUR_PREFIX}:{_worker_consumer(ptype, proc.pid)}")
                except Exception:
                    pass
                proc.kill()
                proc.join(timeout=2)
                done.append(entry)
        except Exception as exc:
            logger.warning("scheduler: reaping retiring %s_worker failed: %s", ptype, exc)
            done.append(entry)   # do not retry a proc that errors on every check
    if done:
        with _pool_lock:
            for entry in done:
                try:
                    _retiring_workers.remove(entry)
                except ValueError:
                    pass


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
            _reap_retiring_workers()
        except KeyboardInterrupt:
            logger.info("liveness_check_loop: stopping")
            break
        except Exception as exc:
            logger.error("liveness_check_loop: error: %s", exc, exc_info=True)
            time.sleep(10)


# Module-level: recorded once at import time so the watchdog can compare
# Redis uptime against scheduler process uptime.
_SCHEDULER_START_TS = time.time()


def _register_new_companies(r) -> int:
    """
    Register monitorable companies that were never scanned into poll:fullscan
    without a scheduler restart.  Returns the number registered.

    rebuild_poll_queues() does this only at startup; this is the live
    equivalent.  It is deliberately narrow so it can never disturb a healthy
    queue:

      * candidates are monitorable companies with no company_poll_stats row, or
        a row with neither last_poll_at nor last_full_scan_at (rebuild's NEW
        bucket) — companies that already have scan history are never touched
        (an in-flight adaptive scan is out of every ZSET and would otherwise be
        mistaken for unregistered);
      * skipped when already in poll:fullscan / poll:adaptive / inflight:fullscan,
        queued in stream:fullscan and not yet claimed by a worker, or when a
        registration marker (TTL) exists — covers the window between
        dispatch and the first fullscan writing its stats row;
      * the batch is spread over the same ceil(N / WORKER_FLOOR) *
        STARTUP_AVG_SCAN_TIME_S window rebuild uses, and each slot goes through
        _atomic_schedule() (gap-avoiding, Redis-locked, skip_if_future), so a
        batch cannot cluster on itself or on already-scheduled fullscans.
    """
    from db.job_monitor import get_monitorable_companies
    from workers.rebuild import _spread_window_s
    from config import (
        STARTUP_AVG_SCAN_TIME_S, COMPANY_REGISTRATION_MARKER_TTL_S,
    )

    keys = [c["company"] for c in get_monitorable_companies()]
    if not keys:
        return 0

    conn = get_conn()
    try:
        scanned = {
            row["company"] for row in conn.execute(
                "SELECT company FROM company_poll_stats "
                "WHERE company = ANY(%s) "
                "AND (last_poll_at IS NOT NULL OR last_full_scan_at IS NOT NULL)",
                (keys,),
            ).fetchall()
        }
    finally:
        conn.close()

    candidates = [k for k in keys if k not in scanned]
    if not candidates:
        return 0

    pipe = r.pipeline()
    for company in candidates:
        pipe.zscore(REDIS_POLL_FULLSCAN, company)
        pipe.zscore(REDIS_POLL_ADAPTIVE, company)
        pipe.zscore(REDIS_INFLIGHT_FULLSCAN, company)
        pipe.exists(f"poll:registered:{company}")
    flags = pipe.execute()

    # Dispatched to stream:fullscan but not yet claimed by a worker: out of the
    # poll ZSET and not yet inflight, so only the stream itself shows it.
    queued = _get_stream_undelivered_companies(r, REDIS_STREAM_FULLSCAN)
    if queued is None:
        return 0

    pending = []
    for i, company in enumerate(candidates):
        z_full, z_adaptive, z_inflight, marker = flags[i * 4:(i + 1) * 4]
        if (z_full is None and z_adaptive is None and z_inflight is None
                and not marker and company not in queued):
            pending.append(company)
    if not pending:
        return 0

    now    = time.time()
    n      = len(pending)
    window = _spread_window_s(n, WORKER_FLOOR, STARTUP_AVG_SCAN_TIME_S)
    done   = 0
    for i, company in enumerate(pending):
        scheduled = _atomic_schedule(
            r, REDIS_POLL_FULLSCAN, company,
            now + (i / n) * window,
            SCHEDULER_FULL_SCAN_INTERVAL_S, 0.20,
            skip_if_future=True,
        )
        if scheduled is not None:
            r.set(f"poll:registered:{company}", "1",
                  ex=COMPANY_REGISTRATION_MARKER_TTL_S)
            done += 1
    logger.info(
        "registration_sweep: registered %d new compan%s into poll:fullscan "
        "(spread=%.0fs)", done, "y" if done == 1 else "ies", window,
    )
    return done


def _deregister_disabled_companies(r) -> int:
    """
    Remove companies that are no longer monitorable (disabled in Discover, marked
    stale, or shadowed) from the live scheduling state, without a restart.
    Returns the number of companies cleaned.  The mirror of _register_new_companies().

    Candidates = members of poll:adaptive / poll:fullscan plus every
    company_poll_stats row, minus get_monitorable_companies().  Per company:

      * ZREM from poll:adaptive, poll:fullscan and inflight:fullscan;
      * DEL the scheduling/transient keys (fullscan lock, retry backoffs, stats
        cache, heartbeat, progress, registration marker) and HDEL its digest
        fullscan-count field;
      * DELETE its company_poll_stats row.  Deleting (not nulling) the row keeps
        the coverage watchdog and the percentile aggregates from seeing a
        never-scanned company, and lets _register_new_companies() treat a later
        re-enable as brand new.

    Deliberately NOT touched: bloom filters and adaptive_seen (they self-expire,
    and keeping them stops a re-enabled company replaying its whole listing) and
    detail-queue payloads (they carry the display name, not the routing key, and
    drain on their own).

    Safe by construction:
      * if the monitorable list is empty the function does nothing — an empty
        read must never wipe every company's schedule;
      * a company with a fullscan in flight is left for the next tick (its
        completion would otherwise re-create the state);
      * workers also skip a disabled company at pick-up, so a stale stream
        entry never reschedules it;
      * idempotent — a scan that finishes after disable and re-adds the company
        is cleaned on the next tick.
    """
    from db.job_monitor import get_monitorable_companies
    from config import (
        REDIS_BACKOFF_PREFIX, REDIS_DIGEST_FULLSCAN_COUNTS,
    )

    monitorable = {c["company"] for c in get_monitorable_companies()}
    if not monitorable:
        logger.warning(
            "deregister_disabled: monitorable list is empty — doing nothing "
            "(refusing to wipe every schedule)"
        )
        return 0

    conn = get_conn()
    try:
        stats_companies = {
            row["company"] for row in conn.execute(
                "SELECT company FROM company_poll_stats"
            ).fetchall()
        }
    finally:
        conn.close()

    known = (
        set(r.zrange(REDIS_POLL_ADAPTIVE, 0, -1))
        | set(r.zrange(REDIS_POLL_FULLSCAN, 0, -1))
        | stats_companies
    )
    candidates = sorted(known - monitorable)
    if not candidates:
        return 0

    pipe = r.pipeline()
    for company in candidates:
        pipe.zscore(REDIS_INFLIGHT_FULLSCAN, company)
    inflight = pipe.execute()
    targets = [c for c, z in zip(candidates, inflight) if z is None]
    if not targets:
        return 0

    pipe = r.pipeline()
    for company in targets:
        pipe.zrem(REDIS_POLL_ADAPTIVE, company)
        pipe.zrem(REDIS_POLL_FULLSCAN, company)
        pipe.zrem(REDIS_INFLIGHT_FULLSCAN, company)
        pipe.delete(
            f"fullscan:lock:{company}",
            f"{REDIS_BACKOFF_PREFIX}:scan:{company}",
            f"{REDIS_BACKOFF_PREFIX}:detail:{company}",
            f"{REDIS_BACKOFF_PREFIX}:fullscan:{company}",
            f"stats:{company}",
            f"heartbeat:{company}",
            f"progress:{company}",
            f"poll:registered:{company}",
        )
        pipe.hdel(REDIS_DIGEST_FULLSCAN_COUNTS, company)
    pipe.execute()

    conn = get_conn()
    try:
        conn.execute(
            "DELETE FROM company_poll_stats WHERE company = ANY(%s)",
            (targets,),
        )
        conn.commit()
    finally:
        conn.close()

    logger.info(
        "deregister_disabled: cleaned %d no-longer-monitorable compan%s: %s",
        len(targets), "y" if len(targets) == 1 else "ies", targets[:20],
    )
    return len(targets)


def _reconcile_company_registry(r) -> None:
    """Deregister disabled companies, then register new ones.

    Deregister runs in its own try/except so a failure there can never block
    registration of newly enabled companies.
    """
    try:
        _deregister_disabled_companies(r)
    except Exception as exc:
        logger.error("registration_sweep: deregister failed: %s", exc, exc_info=True)
    _register_new_companies(r)


def _registration_sweep_loop() -> None:
    """Run _reconcile_company_registry() every COMPANY_REGISTRATION_SWEEP_INTERVAL_S."""
    from config import COMPANY_REGISTRATION_SWEEP_INTERVAL_S

    r = get_redis()
    logger.info("registration_sweep_loop: started (interval=%ds)",
                COMPANY_REGISTRATION_SWEEP_INTERVAL_S)
    while True:
        try:
            time.sleep(COMPANY_REGISTRATION_SWEEP_INTERVAL_S)
            if _pause_event.is_set():
                continue
            _reconcile_company_registry(r)
        except KeyboardInterrupt:
            logger.info("registration_sweep_loop: stopping")
            break
        except Exception as exc:
            logger.error("registration_sweep_loop: error: %s", exc, exc_info=True)


def _redis_watchdog_loop() -> None:
    """
    Detect Redis restarts while the scheduler process is still running and
    automatically rebuild both ZSETs from PostgreSQL.

    Detects restarts by tracking the last observed Redis uptime_in_seconds and
    rebuilding when the current value is lower than the previous (backward
    movement = Redis restarted).  The original scheduler-uptime comparison was
    permanently true after any restart because both uptimes grow at the same
    rate — the gap never closes.

    _last_redis_uptime is only updated after rebuild_redis() succeeds, so a
    failed rebuild is retried on the next tick.  On normal ticks it is updated
    to the current uptime so steady-state forward growth is never re-triggered.

    Runs every 60 seconds.  rebuild_redis() is idempotent.
    """
    r = get_redis()
    logger.info("redis_watchdog_loop: started")
    _last_redis_uptime = 0.0

    while True:
        try:
            time.sleep(60)
            info = r.info("server")
            redis_uptime_s = float(info.get("uptime_in_seconds", 0))

            if redis_uptime_s < _last_redis_uptime:
                scheduler_uptime_s = time.time() - _SCHEDULER_START_TS
                logger.warning(
                    "redis_watchdog_loop: Redis uptime moved backward "
                    "(%.0fs → %.0fs, scheduler_uptime=%.0fs) "
                    "— Redis restarted while scheduler was running; rebuilding ZSETs",
                    _last_redis_uptime, redis_uptime_s, scheduler_uptime_s,
                )
                try:
                    from db.pipeline_alerts import create_alert, ALERT_REDIS_RESTART, WARNING
                    create_alert(
                        alert_type=ALERT_REDIS_RESTART,
                        severity=WARNING,
                        platform="redis",
                        value=redis_uptime_s,
                        threshold=_last_redis_uptime,
                        message=(
                            f"Redis uptime moved backward "
                            f"({_last_redis_uptime:.0f}s → {redis_uptime_s:.0f}s, "
                            f"scheduler_uptime={scheduler_uptime_s:.0f}s) "
                            f"— ZSETs rebuilt automatically"
                        ),
                    )
                except Exception as _alert_exc:
                    logger.warning(
                        "redis_watchdog_loop: create_alert failed (non-fatal): %s",
                        _alert_exc,
                    )
                rebuild_redis()
                logger.info("redis_watchdog_loop: rebuild complete")
            _last_redis_uptime = redis_uptime_s
        except KeyboardInterrupt:
            logger.info("redis_watchdog_loop: stopping")
            break
        except Exception as exc:
            logger.error("redis_watchdog_loop: error: %s", exc, exc_info=True)
            time.sleep(30)


def _manager_cmds_loop() -> None:
    """
    Reads commands from the `manager:cmds` Redis LIST (BLPOP, 5s timeout) and
    dispatches them to the appropriate scheduler primitives.

    Command format (plain strings pushed by manager.py):
        "{pool}:target:{n}"                 — adjust pool to exactly n workers
        "platform:deprioritize:{platform}"  — call _deprioritise_platform()
        "platform:outage:{platform}:set"    — set worker:outage:{platform} (TTL from config)
        "platform:outage:{platform}:clear"  — delete worker:outage:{platform}
        "registration:sweep"                — run _reconcile_company_registry() now

    Zero-risk when manager.py is not running: the list stays empty, BLPOP
    returns None every 5s, loop sleeps and tries again.  No state is changed.
    """
    from config import WORKER_OUTAGE_TTL_S, WORKER_FLOOR

    r = get_redis()
    logger.info("manager_cmds_loop: started — listening on manager:cmds")

    while True:
        try:
            result = r.blpop("manager:cmds", timeout=5)
            if result is None:
                continue

            _, raw = result
            cmd = raw.decode() if isinstance(raw, bytes) else raw
            parts = cmd.split(":")

            # ── {pool}:target:{n} ─────────────────────────────────────────────
            if parts[1:2] == ["target"] and len(parts) in (3, 5):
                pool, _, n_str = parts[:3]
                seen = None
                try:
                    target = int(n_str)
                    if len(parts) == 5:
                        if parts[3] != "seen":
                            raise ValueError(cmd)
                        seen = int(parts[4])
                except ValueError:
                    logger.warning("manager_cmds: bad target command %r", cmd)
                    continue

                if pool in ("scan", "detail", "fullscan"):
                    if pool == "fullscan":
                        current = _get_fullscan_pool_size()
                    else:
                        n_scan, n_detail = _get_pool_snapshot()
                        current = n_scan if pool == "scan" else n_detail

                    # "{pool}:target:{n}:seen:{s}": the manager wants n after seeing
                    # s workers, i.e. a change of n - s.  Apply that change to the
                    # real pool, so a count that differs from the manager's (workers
                    # still starting, a stale read) cannot turn "one fewer" into two.
                    if seen is not None:
                        target = current + (target - seen)

                    # The manager owns the ceilings (it already clamps the target
                    # to the pool's ceiling plus borrowed seats); the scheduler
                    # only applies the number it is given.
                    target = max(WORKER_FLOOR, target)

                    _refused = 0
                    if target > current:
                        for _ in range(target - current):
                            if not _add_one_worker(pool, target):
                                _refused += 1
                    elif target < current:
                        for _ in range(current - target):
                            if not _remove_one_worker(pool, WORKER_FLOOR):
                                _refused += 1

                    n_scan_a, n_detail_a = _get_pool_snapshot()
                    # CLAMPLOG: "applied" only when the pool really changed.
                    if target == current:
                        logger.debug(
                            "manager_cmds: %s:target:%d no change (pool already %d)",
                            pool, target, current,
                        )
                    elif _refused:
                        logger.warning(
                            "manager_cmds: %s:target:%d partly refused "
                            "(%d of %d steps refused; was %d → scan=%d detail=%d fullscan=%d)",
                            pool, target, _refused, abs(target - current), current,
                            n_scan_a, n_detail_a, _get_fullscan_pool_size(),
                        )
                    else:
                        logger.info(
                            "manager_cmds: %s:target:%d applied "
                            "(was %d → scan=%d detail=%d fullscan=%d)",
                            pool, target, current,
                            n_scan_a, n_detail_a, _get_fullscan_pool_size(),
                        )
                else:
                    logger.warning("manager_cmds: unknown pool %r in cmd %r", pool, cmd)

            # ── platform:deprioritize:{platform} ──────────────────────────────
            elif len(parts) == 3 and parts[0] == "platform" and parts[1] == "deprioritize":
                platform = parts[2]
                n = _deprioritise_platform(r, platform)
                logger.info(
                    "manager_cmds: deprioritized platform=%r (%d companies pushed forward)",
                    platform, n,
                )

            # ── platform:outage:{platform}:set / :clear ───────────────────────
            elif len(parts) == 4 and parts[0] == "platform" and parts[1] == "outage":
                platform, action = parts[2], parts[3]
                if action == "set":
                    r.set(f"worker:outage:{platform}", "1", ex=WORKER_OUTAGE_TTL_S)
                    logger.warning(
                        "manager_cmds: outage DECLARED for platform=%r (TTL=%ds)",
                        platform, WORKER_OUTAGE_TTL_S,
                    )
                elif action == "clear":
                    r.delete(f"worker:outage:{platform}")
                    logger.info(
                        "manager_cmds: outage CLEARED for platform=%r", platform,
                    )
                else:
                    logger.warning("manager_cmds: unknown outage action %r in cmd %r", action, cmd)

            # ── registration:sweep — register newly enabled companies now ─────
            elif cmd == "registration:sweep":
                _reconcile_company_registry(r)
                logger.info("manager_cmds: registration:sweep completed")

            else:
                logger.warning("manager_cmds: unrecognised command %r", cmd)

        except KeyboardInterrupt:
            logger.info("manager_cmds_loop: stopping")
            break
        except redis.exceptions.TimeoutError:
            logger.warning("manager_cmds_loop: Redis socket timeout (system load?) — retrying")
            continue
        except Exception as exc:
            logger.error("manager_cmds_loop: error: %s", exc, exc_info=True)
            time.sleep(5)



def _ceiling_decay_loop() -> None:
    """
    Standalone learned-ceiling decay — runs every WORKER_SLOW_CHECK_INTERVAL_S (30 min).

    Extracted from _slow_throughput_check_loop so it survives when that loop is
    deleted during manager.py migration.  Logic is unchanged:

    For each `worker:ceil:learned:{dc_key}` key, if 24h have passed since the last
    error event (`worker:ceil:last_error:{dc_key}`), increment the ceiling by 1.
    This probes upward toward the true safe maximum after a clean period.

    DOC-LEARN / DECAY-DEAD: currently a no-op.  Nothing in the repo ever writes
    `worker:ceil:last_error:{dc_key}`, and the `ceil:learned` high-water mark is only
    written by http_client's concurrency adjuster (itself part of the parked D7 per-platform
    concurrency chain).  The loop runs but never finds a key to relax.  Kept (not deleted)
    until the D7 decision; see the parked-D7 note in project memory.

    Writes are guarded: if worker:ceil:last_error is missing or too recent, the key
    is left alone.  Only moves UP (decay means relaxing the ceiling, not lowering it).
    """
    from db.api_health import record_scaling_event

    r = get_redis()
    logger.info("ceiling_decay_loop: started")

    while True:
        try:
            time.sleep(WORKER_SLOW_CHECK_INTERVAL_S)

            now = time.time()
            cursor = 0
            while True:
                cursor, keys = r.scan(cursor, match="worker:ceil:learned:*", count=50)
                for key in keys:
                    dc_key   = key.split("worker:ceil:learned:", 1)[1] if isinstance(key, str) \
                               else key.decode().split("worker:ceil:learned:", 1)[1]
                    last_err = r.get(f"worker:ceil:last_error:{dc_key}")
                    if last_err and (now - float(last_err)) > 86400:
                        old_ceil = int(r.get(key) or WORKER_FLOOR)
                        new_ceil = min(old_ceil + 1, MONITOR_MAX_WORKERS)
                        r.set(key, new_ceil)
                        logger.info(
                            "ceiling_decay: ceil:learned relaxed for dc=%r %d → %d (24h clean)",
                            dc_key, old_ceil, new_ceil,
                        )
                        record_scaling_event(
                            "ceiling_learned",
                            trigger_layer="ceiling_decay",
                            dc_key=dc_key,
                            learned_ceiling=new_ceil,
                            notes=f"24h decay from {old_ceil}",
                        )
                if cursor == 0:
                    break

        except KeyboardInterrupt:
            logger.info("ceiling_decay_loop: stopping")
            break
        except Exception as exc:
            logger.error("ceiling_decay_loop: error: %s", exc, exc_info=True)
            time.sleep(60)


def _shutdown_worker_pools() -> None:
    """
    Gracefully stop all worker processes (called at KeyboardInterrupt).

    Steps (Section 9 — graceful shutdown):
        1. Set every worker's shutdown Event — workers finish their current
           job then break out of their BLPOP loop.
        2. Join each process with WORKER_SHUTDOWN_TIMEOUT_S deadline.
        3. Force-kill any process that hasn't exited by the deadline.
    """
    # _fullscan_pool is local to run_scheduler() — access via module-level flag
    # if available; otherwise handle scan + detail only (safe on partial start).
    _fp = globals().get("_fullscan_pool_ref", [])

    logger.info(
        "scheduler: initiating graceful shutdown of "
        "%d scan + %d detail + %d fullscan workers",
        len(_scan_pool), len(_detail_pool), len(_fp),
    )

    with _pool_lock:
        all_entries = [("scan",     _scan_pool),
                       ("detail",   _detail_pool),
                       ("fullscan", _fp)]

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
        3. Calculates data-driven startup worker counts from 30-day history
        5. Spawns three co-scheduled multiprocessing.Process pools:
               scan_pool     — listing scan workers
               detail_pool   — detail fetch workers
               fullscan_pool — fullscan workers (stream:fullscan consumers)
        6. Starts five daemon threads:
               adaptive_loop               — dispatches to stream:adaptive (two-layer)
               fullscan_loop               — dispatches to stream:fullscan (two-layer)
               pubsub_listener_loop        — handles pause/resume pub/sub
               _liveness_check_loop        — Layer 1: replace dead workers every 5s
               _ceiling_decay_loop         — relaxes learned ceilings after 24h clean (30m)
               _manager_cmds_loop          — executes add/remove/deprioritize/outage commands from manager.py
               _redis_watchdog_loop        — detects Redis restarts and rebuilds ZSETs
               NOTE: _fast_error_check_loop removed — replaced by manager._check_error_spikes() (60s cycle)
               NOTE: _slow_throughput_check_loop removed — replaced by workers/manager.py (Layer 0)
           NOTE: result_consumer_loop is retired — on_adaptive_complete() is called
           inline by scan_worker, and on_fullscan_complete() is handled in fullscan.py.
        7. On Ctrl+C: graceful shutdown → SIGKILL stragglers after 30s

    Args:
        skip_rebuild: pass True if Redis is already warm (dev restarts)
    """
    from workers.sentry_init import init_sentry
    init_sentry()

    from workers.startup import validate_startup
    validate_startup("scheduler", check_redis=True, check_db=True, check_config=True)

    # Write startup heartbeats immediately after Redis is confirmed reachable so
    # the deploy health check (runs ~70s after service restart) sees the scheduler
    # as alive even while init_db() / rebuild_redis() are still running.
    # Same pattern as manager.py (_MANAGER_HB_TTL=180).  The loop's 1s-tick
    # writes take over once adaptive_loop / fullscan_loop start.
    _r_startup = get_redis()
    _write_scheduler_heartbeat(_r_startup, "adaptive", 0, ttl=_SCHEDULER_STARTUP_HB_TTL, ready=False)
    _write_scheduler_heartbeat(_r_startup, "fullscan", 0, ttl=_SCHEDULER_STARTUP_HB_TTL, ready=False)
    logger.info("scheduler: startup heartbeats written (ttl=%ds)", _SCHEDULER_STARTUP_HB_TTL)

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

    # ── Spawn worker pools ────────────────────────────────────────────────────
    # Every pool starts at WORKER_FLOOR.  The scheduler only executes: the
    # worker count is owned by manager.py (Layer 0 scales up when delay exceeds
    # DELAY_WARN and back down when utilization drops below 50% for 5 cycles).
    scan_count = detail_count = fullscan_count = WORKER_FLOOR

    global _scan_pool, _detail_pool
    _fullscan_pool: list = []

    _audit_orphan_workers(set())   # before our own spawns: anything running is foreign

    with _pool_lock:
        for _ in range(scan_count):
            _scan_pool.append(_spawn_worker("scan"))
        for _ in range(detail_count):
            _detail_pool.append(_spawn_worker("detail"))
        for _ in range(fullscan_count):
            _fullscan_pool.append(_spawn_worker("fullscan"))

    # Expose fullscan pool globally so _shutdown_worker_pools can drain it
    globals()["_fullscan_pool_ref"] = _fullscan_pool

    logger.info(
        "scheduler: spawned %d scan_workers + %d detail_workers + %d fullscan_workers",
        scan_count, detail_count, fullscan_count,
    )

    # Publish initial pool state so the watchdog has baseline data immediately
    _write_scheduler_health()

    # ── Initialise stream consumer groups ────────────────────────────────────
    # Done here (after workers spawn) so the groups exist before any XADD.
    r_init = get_redis()
    _init_consumer_group(r_init, REDIS_STREAM_ADAPTIVE)
    _init_consumer_group(r_init, REDIS_STREAM_FULLSCAN)

    # ── Start daemon threads ──────────────────────────────────────────────────
    # NOTE: result_consumer_loop is retired — on_adaptive_complete() is now
    # called inline by scan_worker after XREADGROUP, and on_fullscan_complete()
    # is handled inside fullscan.py.  The XACK replaces the result push/consume
    # pattern, eliminating a potential message-loss window.
    threads = [
        threading.Thread(target=adaptive_loop,
                         name="adaptive_loop",           daemon=True),
        threading.Thread(target=fullscan_loop,
                         name="fullscan_loop",           daemon=True),
        threading.Thread(target=pubsub_listener_loop,
                         name="pubsub_listener",         daemon=True),
        threading.Thread(target=_liveness_check_loop,
                         name="liveness_check",          daemon=True),
        threading.Thread(target=_ceiling_decay_loop,
                         name="ceiling_decay",           daemon=True),
        threading.Thread(target=_manager_cmds_loop,
                         name="manager_cmds",            daemon=True),
        threading.Thread(target=_redis_watchdog_loop,
                         name="redis_watchdog",          daemon=True),
        threading.Thread(target=_registration_sweep_loop,
                         name="registration_sweep",      daemon=True),
    ]

    for t in threads:
        t.start()

    logger.info("scheduler: all loops started — %d threads + %d processes",
                len(threads), scan_count + detail_count + fullscan_count)
    if sys.stdout.isatty():  # CTRLC: no interactive hint under systemd
        print(f"[scheduler] Running — "
              f"{scan_count} scan + {detail_count} detail + {fullscan_count} fullscan workers "
              f"— Ctrl+C to stop")

    from logger import cleanup_logs_if_due
    try:
        while True:
            time.sleep(60)
            cleanup_logs_if_due()
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
