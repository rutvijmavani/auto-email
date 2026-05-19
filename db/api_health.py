# db/api_health.py — CRUD for api_health table
#
# Tracks every ATS API request per platform per day.
# Used to detect rate limiting and performance issues.
# Powers --monitor-status health table and alert emails.
#
# Thread safety: record_request() uses a background writer thread
# with a Queue to avoid lock contention during parallel
# --monitor-jobs runs. All query functions are unchanged.

import logging
from datetime import datetime, date, timedelta
from db.connection import get_conn

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────
# THREAD-SAFE WRITE QUEUE
# ─────────────────────────────────────────
# Initialized lazily on first record_request() call.
# The writer thread drains the queue and batches DB writes,
# eliminating lock contention when 20 threads fire simultaneously.

import queue
import threading
import atexit

_write_queue   = None
_writer_thread = None
_queue_lock    = threading.Lock()
_writer_error  = None   # stores first unhandled exception from _writer_loop
_closed        = False  # prevents new writes after flush() begins


def _ensure_writer_running():
    """
    Start the writer thread and queue if not already running.
    MUST be called while _queue_lock is held by the caller.
    Does NOT acquire _queue_lock itself — avoids re-entrancy deadlock.
    """
    global _write_queue, _writer_thread
    if _write_queue is not None:
        return
    _write_queue = queue.Queue()
    t = threading.Thread(
        target=_writer_loop,
        args=(_write_queue,),
        daemon=True,    # daemon: Python won't wait for this thread at shutdown;
                        # atexit.register(flush) sends the sentinel and joins
                        # the thread while it is still alive — before daemon
                        # threads are killed — so data is not lost on exit.
        name="api_health_writer",
    )
    _writer_thread = t
    t.start()
    # Register flush() for normal process exit so records are not
    # lost when run() is interrupted before its explicit flush call.
    # flush() is idempotent — safe to call multiple times.
    atexit.register(flush)


def _get_write_queue():
    """
    Return the singleton write queue, starting the writer thread
    on first call. Thread-safe via _queue_lock.
    Callers that already hold _queue_lock must use _ensure_writer_running()
    directly to avoid deadlock (Lock is not reentrant).
    """
    if _write_queue is not None:
        return _write_queue
    with _queue_lock:
        _ensure_writer_running()
    return _write_queue


def _writer_loop(q):
    """
    Background thread: drains write queue and commits to PostgreSQL.
    Batches up to 50 records per commit to reduce write frequency.
    Runs until sentinel None is received.
    Any unhandled exception is stored in _writer_error so flush() can log it.

    known_pairs: set of (date, platform) tuples whose api_health row is
    confirmed to already exist. Skipping the INSERT for known pairs prevents
    the BIGSERIAL sequence from burning an ID on every ON CONFLICT DO NOTHING
    no-op — which was causing large ID gaps (e.g. 1, 139, 326, ...) when
    a busy platform made thousands of requests per day. The set resets on
    process restart (once per day at 7 AM cycle start — acceptable).
    """
    global _writer_error
    BATCH_SIZE    = 50
    DRAIN_TIMEOUT = 0.05   # seconds to wait for more items before committing

    pending     = []
    known_pairs: set = set()   # (date, platform) rows confirmed in DB

    try:
        while True:
            # Block until at least one item arrives
            try:
                item = q.get(timeout=1.0)
            except queue.Empty:
                if pending:
                    _flush_batch(pending, known_pairs)
                    pending = []
                continue

            if item is None:   # sentinel — shutdown
                q.task_done()
                if pending:
                    _flush_batch(pending, known_pairs)
                break

            pending.append(item)
            q.task_done()

            # Drain more items without blocking
            while len(pending) < BATCH_SIZE:
                try:
                    item = q.get_nowait()
                    if item is None:
                        q.task_done()
                        if pending:
                            _flush_batch(pending, known_pairs)
                        return
                    pending.append(item)
                    q.task_done()
                except queue.Empty:
                    break

            if pending:
                _flush_batch(pending, known_pairs)
                pending = []
    except Exception as exc:
        _writer_error = exc
        logger.error("api_health writer thread died unexpectedly: %s",
                     exc, exc_info=True)


def _flush_batch(records, known_pairs: set):
    """
    Write a batch of api_health records to PostgreSQL.
    One connection, one commit for the whole batch.
    known_pairs is updated in-place as new rows are created.
    """
    conn = get_conn()
    try:
        for rec in records:
            _write_one(conn, rec, known_pairs)
        conn.commit()
    except Exception as e:
        logger.error("api_health: batch write failed (%d records): %s",
                     len(records), e, exc_info=True)
        try:
            conn.rollback()
        except Exception:
            pass
    finally:
        conn.close()


def _write_one(conn, rec, known_pairs: set):
    """
    Write one api_health record to an open connection.
    Called inside _flush_batch() — no commit here.

    known_pairs is a set of (date, platform, context) tuples maintained by
    the writer thread. If the triple is not yet known, we INSERT the row and
    add it to the set. If already known, we skip the INSERT entirely —
    avoiding the BIGSERIAL sequence burn that caused large ID gaps.

    context values: 'normal' | 'backoff' | 'canary'
    Baseline queries filter WHERE context='normal' so managed-error periods
    (backoff retries, canary probes) do not distort the historical average.
    """
    today        = rec["date"]
    platform     = rec["platform"]
    status_code  = rec["status_code"]
    response_ms  = rec["response_ms"]
    backoff_s    = rec["backoff_s"]
    context      = rec.get("context", "normal")

    if status_code == 200:
        ok_inc, e429_inc, e404_inc, err_inc = 1, 0, 0, 0
    elif status_code == 429:
        ok_inc, e429_inc, e404_inc, err_inc = 0, 1, 0, 0
    elif status_code == 404:
        ok_inc, e429_inc, e404_inc, err_inc = 0, 0, 1, 0
    else:
        ok_inc, e429_inc, e404_inc, err_inc = 0, 0, 0, 1

    # Error sub-type breakdown (Fix 1 — adaptive polling architecture)
    timeout_inc = 1 if rec.get("error_type") == "requests_timeout"  else 0
    conn_inc    = 1 if rec.get("error_type") == "requests_conn_err" else 0
    e5xx_inc    = 1 if rec.get("error_type") == "requests_5xx"      else 0
    other_inc   = 1 if rec.get("error_type") == "requests_other_err" else 0

    triple = (today, platform, context)
    if triple not in known_pairs:
        # First time this (date, platform, context) triple is seen in this
        # process — create the row if it doesn't exist yet.
        # ON CONFLICT DO NOTHING handles parallel workers and process restarts.
        conn.execute("""
            INSERT INTO api_health (date, platform, context)
            VALUES (?, ?, ?)
            ON CONFLICT(date, platform, context) DO NOTHING
        """, triple)
        known_pairs.add(triple)
    # If triple already in known_pairs: row confirmed to exist — skip INSERT.

    # GREATEST() replaces MAX() in UPDATE context (MAX() does not work as a
    # two-argument comparison function in PostgreSQL UPDATE statements).
    conn.execute("""
        UPDATE api_health SET
            requests_made        = requests_made        + 1,
            requests_ok          = requests_ok          + ?,
            requests_429         = requests_429         + ?,
            requests_404         = requests_404         + ?,
            requests_error       = requests_error       + ?,
            requests_timeout     = requests_timeout     + ?,
            requests_conn_err    = requests_conn_err    + ?,
            requests_5xx         = requests_5xx         + ?,
            requests_other_err   = requests_other_err   + ?,
            total_ms             = total_ms             + ?,
            max_response_ms      = GREATEST(max_response_ms, ?),
            backoff_total_s      = backoff_total_s      + ?,
            first_429_at         = CASE
                WHEN ? = 1 AND first_429_at IS NULL
                THEN CURRENT_TIMESTAMP
                ELSE first_429_at
            END
        WHERE date = ? AND platform = ? AND context = ?
    """, (
        ok_inc, e429_inc, e404_inc, err_inc,
        timeout_inc, conn_inc, e5xx_inc, other_inc,
        response_ms, response_ms,
        backoff_s,
        e429_inc,
        today, platform, context,
    ))

    conn.execute("""
        UPDATE api_health SET
            avg_response_ms = CASE
                WHEN requests_made > 0
                THEN total_ms / requests_made
                ELSE 0
            END
        WHERE date = ? AND platform = ? AND context = ?
    """, (today, platform, context))


# ─────────────────────────────────────────
# RECORD REQUESTS (public API — unchanged signature)
# ─────────────────────────────────────────

def record_request(platform, status_code, response_ms, backoff_s=0,
                   error_type=None, context="normal"):
    """
    Record one API request in api_health.
    Non-blocking — enqueues to background writer thread.
    Safe to call from multiple threads simultaneously.

    Args:
        platform:     e.g. "greenhouse"
        status_code:  HTTP status code (0 for non-HTTP errors)
        response_ms:  response time in milliseconds
        backoff_s:    seconds waited due to rate limit
        error_type:   one of requests_timeout | requests_conn_err |
                      requests_5xx | requests_other_err  (None = not an error)
        context:      'normal' (default) | 'backoff' | 'canary'
                      Tags the operational context so baseline queries can
                      filter to context='normal' only, preventing managed-error
                      periods from distorting the 30-day historical average.
    """
    # Hold _queue_lock for the entire check-and-enqueue so it is atomic
    # with flush().  Without the lock, flush() could run between the _closed
    # check and the put(), set _write_queue=None, and force _get_write_queue()
    # to spin up a new writer thread that never receives a sentinel.
    # We call _ensure_writer_running() directly (not _get_write_queue()) to
    # avoid re-acquiring _queue_lock inside _get_write_queue() — Lock is not
    # reentrant and doing so would deadlock.
    with _queue_lock:
        if _closed:
            return
        _ensure_writer_running()
        _write_queue.put({
            "date":        date.today().isoformat(),
            "platform":    platform,
            "status_code": status_code,
            "response_ms": response_ms,
            "backoff_s":   backoff_s,
            "error_type":  error_type,
            "context":     context,
        })


# ─────────────────────────────────────────
# QUERY FUNCTIONS
# ─────────────────────────────────────────

def get_platform_stats(platform, for_date=None):
    """Get api_health stats for one platform on one date."""
    for_date = for_date or date.today().isoformat()
    conn = get_conn()
    try:
        row = conn.execute("""
            SELECT * FROM api_health
            WHERE date = ? AND platform = ?
        """, (for_date, platform)).fetchone()
        return dict(row) if row else None
    finally:
        conn.close()


def get_health_summary(days=7):
    """
    Get aggregated health stats per platform for the last N days.
    Returns list of dicts sorted by platform.
    """
    since = (date.today() - timedelta(days=days)).isoformat()
    conn  = get_conn()
    try:
        rows = conn.execute("""
            SELECT
                platform,
                SUM(requests_made)   AS total_requests,
                SUM(requests_ok)     AS total_ok,
                SUM(requests_429)    AS total_429s,
                SUM(requests_404)    AS total_404s,
                SUM(requests_error)  AS total_errors,
                CASE
                    WHEN SUM(requests_made) > 0
                    THEN ROUND(
                        100.0 * SUM(requests_429)
                        / SUM(requests_made), 1)
                    ELSE 0
                END AS rate_429_pct,
                CASE
                    WHEN SUM(requests_made) > 0
                    THEN ROUND(
                        100.0 * SUM(requests_error)
                        / SUM(requests_made), 1)
                    ELSE 0
                END AS error_pct,
                CASE
                    WHEN SUM(requests_made) > 0
                    THEN SUM(total_ms) / SUM(requests_made)
                    ELSE 0
                END AS avg_response_ms,
                MAX(max_response_ms) AS max_response_ms,
                COUNT(DISTINCT date) AS days_with_data
            FROM api_health
            WHERE date >= ?
            GROUP BY platform
            ORDER BY platform ASC
        """, (since,)).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def get_todays_stats():
    """Get all platform stats for today."""
    today = date.today().isoformat()
    conn  = get_conn()
    try:
        rows = conn.execute("""
            SELECT * FROM api_health
            WHERE date = ?
            ORDER BY platform ASC
        """, (today,)).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def get_run_429_rate(platform):
    """
    Get today's aggregate 429 rate for a platform.
    Returns percentage (0-100).
    """
    conn = get_conn()
    try:
        today = date.today().isoformat()
        row   = conn.execute("""
            SELECT requests_made, requests_429
            FROM api_health
            WHERE date = ? AND platform = ?
        """, (today, platform)).fetchone()
        if not row or row["requests_made"] == 0:
            return 0.0
        return round(
            100.0 * row["requests_429"] / row["requests_made"], 1
        )
    finally:
        conn.close()


def get_error_breakdown(platform, for_date=None):
    """
    Return per-error-type counts for a platform on a given date.
    Used by --monitor-status to show detailed failure reasons.
    """
    for_date = for_date or date.today().isoformat()
    conn = get_conn()
    try:
        row = conn.execute("""
            SELECT
                requests_timeout,
                requests_conn_err,
                requests_5xx,
                requests_other_err
            FROM api_health
            WHERE date = ? AND platform = ?
        """, (for_date, platform)).fetchone()
        return dict(row) if row else {
            "requests_timeout": 0, "requests_conn_err": 0,
            "requests_5xx": 0,    "requests_other_err": 0,
        }
    finally:
        conn.close()


def query_30day_avg_error_rate(platform: str) -> float:
    """
    Compute the 30-day average combined error rate for a platform.

    "Combined error rate" = (requests_timeout + requests_5xx +
                             requests_429 + requests_404) / requests_made
    across all days in the last 30 days that have at least 1 request recorded.

    Used by get_baseline_error_rate() in http_client.py to populate the
    Redis baseline cache (baseline:error_rate:{platform}, 1h TTL).

    Returns:
        float in [0.0, 1.0] — 0.0 if fewer than CONCURRENCY_BASELINE_MIN_DAYS
        (7) days of data exist for this platform (caller skips spike_factor).
    """
    from config import CONCURRENCY_BASELINE_MIN_DAYS
    since = (date.today() - timedelta(days=30)).isoformat()
    conn  = get_conn()
    try:
        row = conn.execute("""
            SELECT
                COUNT(DISTINCT date)                          AS days_with_data,
                SUM(requests_made)                            AS total_requests,
                SUM(requests_timeout + requests_5xx
                    + requests_429 + requests_404)            AS total_errors
            FROM api_health
            WHERE platform = ?
              AND date >= ?
              AND context = 'normal'
              AND requests_made > 0
        """, (platform, since)).fetchone()
    finally:
        conn.close()

    if not row or not row["days_with_data"]:
        return 0.0
    if row["days_with_data"] < CONCURRENCY_BASELINE_MIN_DAYS:
        return 0.0   # insufficient history — caller skips spike_factor
    if not row["total_requests"]:
        return 0.0
    return float(row["total_errors"]) / float(row["total_requests"])


def query_30day_avg_response_ms(platform: str) -> float:
    """
    Compute the 30-day average response time (ms) for a platform.

    Used by calculate_worker_counts() in scheduler.py to estimate per-request
    duration when computing how many workers are needed at cycle start.

    Returns 0.0 if no data exists (caller uses a hardcoded fallback constant).
    """
    since = (date.today() - timedelta(days=30)).isoformat()
    conn  = get_conn()
    try:
        row = conn.execute("""
            SELECT
                SUM(total_ms)      AS total_ms,
                SUM(requests_made) AS total_requests
            FROM api_health
            WHERE platform = ?
              AND date >= ?
              AND context = 'normal'
              AND requests_made > 0
        """, (platform, since)).fetchone()
    finally:
        conn.close()

    if not row or not row["total_requests"]:
        return 0.0
    return float(row["total_ms"]) / float(row["total_requests"])


# Listing-scan platforms (used by query_p95_response_ms)
_LISTING_SCAN_PLATFORMS = (
    "greenhouse", "lever", "ashby", "smartrecruiters",
    "workday", "oracle_hcm", "icims",
)


def query_p95_response_ms(scan_type: str) -> int:
    """
    Estimate p95 response time (ms) for XAUTOCLAIM idle-timeout calculation.

    The api_health table stores per-day aggregates, not individual request
    durations, so a true p95 is not directly computable.  We use a
    conservative approximation:

        listing_scan: average of max_response_ms across listing platforms
                      (last 7 days, normal context) × 1.5
        full_scan:    same × 3 (full scans fetch all pages — much slower)

    The result is used as:  idle_ms = max(p95_ms × 3, 300_000)
    A moderately wrong p95 estimate is fine — the 5-minute minimum floor
    and the 3× multiplier give ample crash-recovery time even with a bad
    estimate.

    Args:
        scan_type: "listing_scan" (for adaptive) or "full_scan" (for fullscan).
                   Any other value returns a 30-second default.

    Returns:
        Estimated p95 in milliseconds (int).  Never 0.
    """
    from datetime import date, timedelta
    from db.connection import get_conn

    since = (date.today() - timedelta(days=7)).isoformat()
    conn  = get_conn()
    try:
        row = conn.execute("""
            SELECT AVG(max_response_ms) AS avg_max_ms
            FROM api_health
            WHERE platform = ANY(%s)
              AND date >= %s
              AND context = 'normal'
              AND max_response_ms > 0
        """, (list(_LISTING_SCAN_PLATFORMS), since)).fetchone()
    except Exception:
        return 30_000
    finally:
        conn.close()

    avg_max = (row["avg_max_ms"] or 0) if row else 0
    if avg_max <= 0:
        # No data — use safe defaults
        return 30_000 if scan_type == "listing_scan" else 120_000

    multiplier = 1.5 if scan_type == "listing_scan" else 3.0
    return max(1_000, int(avg_max * multiplier))


# ─────────────────────────────────────────
# WORKER SCALING EVENTS
# ─────────────────────────────────────────

def record_scaling_event(
    event_type: str,
    *,
    trigger_layer=None,
    platform=None,
    dc_key=None,
    worker_type=None,
    scan_workers_before=None,
    scan_workers_after=None,
    detail_workers_before=None,
    detail_workers_after=None,
    error_rate=None,
    baseline_error_rate=None,
    spike_factor=None,
    scan_queue_depth=None,
    detail_queue_depth=None,
    inflight_count=None,
    learned_ceiling=None,
    consec_reductions=None,
    notes=None,
) -> None:
    """
    Record one worker scaling decision to worker_scaling_events.

    Synchronous write — volume is too low to warrant a background queue
    (at most one event per 5-minute monitoring cycle per platform).
    Non-blocking on error: logs warning and continues so a DB hiccup never
    disrupts the scheduler's main logic.

    event_type values (see Section 16 schema):
        worker_add | worker_remove | ceiling_learned |
        outage_start | canary_probe | outage_end
    """
    try:
        conn = get_conn()
        try:
            conn.execute("""
                INSERT INTO worker_scaling_events (
                    event_type, trigger_layer, platform, dc_key, worker_type,
                    scan_workers_before, scan_workers_after,
                    detail_workers_before, detail_workers_after,
                    error_rate, baseline_error_rate, spike_factor,
                    scan_queue_depth, detail_queue_depth, inflight_count,
                    learned_ceiling, consec_reductions, notes
                ) VALUES (
                    ?, ?, ?, ?, ?,
                    ?, ?,
                    ?, ?,
                    ?, ?, ?,
                    ?, ?, ?,
                    ?, ?, ?
                )
            """, (
                event_type, trigger_layer, platform, dc_key, worker_type,
                scan_workers_before, scan_workers_after,
                detail_workers_before, detail_workers_after,
                error_rate, baseline_error_rate, spike_factor,
                scan_queue_depth, detail_queue_depth, inflight_count,
                learned_ceiling, consec_reductions, notes,
            ))
            conn.commit()
        finally:
            conn.close()
    except Exception as exc:
        logger.warning(
            "api_health: record_scaling_event failed (event_type=%r platform=%r): %s",
            event_type, platform, exc,
        )


def flush():
    """
    Block until all pending api_health writes are committed to PostgreSQL.
    Call once at process exit to ensure no records are lost.

    Uses sentinel + thread join rather than q.join() because task_done()
    is called immediately on dequeue (before _flush_batch), so q.join()
    can return while the last batch is still being written.  Joining the
    writer thread guarantees the DB commit has completed.

    Safe to call only once — the writer thread exits after the sentinel
    and is not restarted.  Any record_request() calls after flush() will
    be blocked by _closed flag and return immediately.
    """
    global _write_queue, _writer_thread, _closed
    if _write_queue is None:
        return
    with _queue_lock:
        q = _write_queue
        t = _writer_thread
        if q is None:
            return
        _closed = True              # prevent new writes
        q.put(None)                 # sentinel → writer drains pending + exits
        _write_queue   = None
        _writer_thread = None
    if t is not None:
        t.join()                    # wait for full thread exit (DB write done)
        if _writer_error is not None:
            logger.error("api_health: writer thread had an unhandled error — "
                         "some health records may be lost: %s", _writer_error)
