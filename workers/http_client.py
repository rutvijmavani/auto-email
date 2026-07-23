"""
workers/http_client.py — Central HTTP wrapper for all ATS requests (Phase 8+9).

Every ATS module uses ats_get() instead of requests.get() directly.
This single entry point enforces:

  1. Distributed semaphore  — Redis counter pair enforces global concurrency
                              limit across all worker processes and machines.
  2. Sliding window         — Per-request update of errwin:{key}:{bucket}
                              tracks 429 + 404 + timeout + 5xx over 10 minutes.
  3. Feedback loop          — After every call the concurrency limit is adjusted
                              using a two-condition check: raw error_rate threshold
                              + spike_factor vs 30-day baseline (Phase 9).
                              spike_factor > CONCURRENCY_SPIKE_FACTOR_THRESHOLD
                              → aggressive reduction (limit - 2); else cautious (-1).
  4. api_health             — Non-blocking write to PostgreSQL api_health table.
  5. Error classification   — classify_error() maps exceptions to the four
                              sub-types used by Phase 9 (Section 20).

Key design decisions:
  - Redis round-trip (~0.1 ms) is negligible vs HTTP call (500–3000 ms).
  - Distributed semaphore is the only correct solution for multi-process workers:
    a per-process threading.Semaphore multiplies by the number of processes.
  - Workday callers pass dc_key (e.g. "workday_wd12"); all other callers pass
    only platform.  The semaphore and errwin key use dc_key when provided so
    each Workday DC is rate-limited independently.
  - discover_workday_dc_keys() is called once at run_scheduler() startup to
    pre-seed Redis concurrency:limit keys for all known Workday DCs.
  - Baseline error rate (30-day avg from api_health) is cached in Redis at
    baseline:error_rate:{platform} with 1h TTL.  Cold-start platforms (<7 days
    history) skip spike_factor and use the raw error_rate threshold only.

HTTP timeouts (Section 18):
  connect_timeout = 10s
  read_timeout    = 30s
"""

import json
import random
import threading
import time

from logger import get_logger
from typing import Optional

import requests

from config import (
    CONCURRENCY_ERROR_RATE_REDUCE,
    CONCURRENCY_ERROR_RATE_INCREASE,
    CONCURRENCY_WINDOW_MINUTES,
    CONCURRENCY_WINDOW_TTL,
    CONCURRENCY_BACKOFF_BASE,
    CONCURRENCY_BACKOFF_MAX,
    CONCURRENCY_MAX_RETRIES,
    CONCURRENCY_WORKDAY_DEFAULT,
    CONCURRENCY_FLOOR,
    CONCURRENCY_FLOOR_DEFAULT,
    CONCURRENCY_CEIL,
    CONCURRENCY_CEIL_DEFAULT,
    MONITOR_PLATFORM_CONCURRENCY,
    MONITOR_PLATFORM_CONCURRENCY_DEFAULT,
    REDIS_ERRWIN_PREFIX,
    REDIS_CONCURRENCY_ACTIVE_PREFIX,
    REDIS_CONCURRENCY_LIMIT_PREFIX,
    REDIS_BASELINE_PREFIX,
    BASELINE_CACHE_TTL,
    CONCURRENCY_SPIKE_FACTOR_THRESHOLD,
)
from workers.redis_client import get_redis

logger = get_logger(__name__)


class CeilingExceeded(Exception):
    """
    Raised by ats_get() when the distributed concurrency semaphore cannot be
    acquired after all retries — the platform is at its ceiling.

    Each worker type catches this and puts the job back without processing it:
      detail_worker  → RPUSH to tail of source queue (next pop)
      scan_worker    → ZADD poll:adaptive score=now+30s, leave in PEL
      fullscan       → ZADD poll:fullscan score=now+30s, return ceiling_exceeded
    """

# HTTP timeouts (Section 18 — Resilience)
_CONNECT_TIMEOUT = 10
_READ_TIMEOUT    = 30

# ─────────────────────────────────────────
# REQUEST CONTEXT (Phase 10 — baseline purity)
# ─────────────────────────────────────────
# Thread-local that tags every api_health row written during a scan with the
# operational context in which the HTTP request was made.
#
# Values: 'normal' | 'backoff' | 'canary'
#
# Set by scan_worker._run_listing_scan() and detail_worker._process_detail()
# before calling fetch_jobs() / fetch_job_detail(), then reset to 'normal'
# in a finally block.  ATS modules never need to know about context — they
# call ats_get() transparently and the correct tag is applied automatically.
#
# This avoids touching all 19 ATS modules while still tagging every request
# made during a backoff retry or canary probe correctly.

_request_context: threading.local = threading.local()


def set_request_context(context: str) -> None:
    """
    Set the api_health context tag for all ats_get() calls on this thread.

    Call BEFORE fetch_jobs() / fetch_job_detail() and reset in a finally
    block.  Safe to call from multiprocessing workers — each process has
    its own address space and its own _request_context instance.

    Args:
        context: 'normal' | 'backoff' | 'canary'
    """
    _request_context.value = context


def _get_request_context() -> str:
    """Return the current thread's request context (default: 'normal')."""
    return getattr(_request_context, "value", "normal")


# ─────────────────────────────────────────
# ERROR CLASSIFICATION
# ─────────────────────────────────────────

def classify_error(exc: Exception, status_code: Optional[int] = None) -> str:
    """
    Map an exception (or HTTP status) to one of the four error sub-types
    defined in Section 20 of the architecture doc.

    Args:
        exc:         the caught exception (or None for HTTP-level errors)
        status_code: HTTP status code if the request completed (or None)

    Returns:
        One of: "requests_timeout" | "requests_conn_err" |
                "requests_5xx" | "requests_other_err"
    """
    if exc is not None:
        if isinstance(exc, requests.Timeout):
            return "requests_timeout"
        if isinstance(exc, requests.ConnectionError):
            return "requests_conn_err"
        if isinstance(exc, requests.HTTPError):
            code = getattr(exc.response, "status_code", 0) if exc.response is not None else 0
            if code >= 500:
                return "requests_5xx"
            return "requests_other_err"
        return "requests_other_err"

    if status_code is not None and status_code >= 500:
        return "requests_5xx"

    return "requests_other_err"


# ─────────────────────────────────────────
# SLIDING WINDOW
# ─────────────────────────────────────────

def _bucket() -> int:
    """Current 10-minute bucket index (Unix seconds // 600)."""
    return int(time.time() // (CONCURRENCY_WINDOW_MINUTES * 60))


def record_errwin(r, key: str, status_code: int,
                  error_type: Optional[str] = None) -> None:
    """
    Increment the sliding window counters for one HTTP call.

    Tracks four error signals (Phase 9 — Section 20):
        errors_429      — explicit rate limit
        errors_404      — Workday overload response
        errors_timeout  — earliest concurrency signal (server queueing)
        errors_5xx      — soft rate limit / server instability

    Args:
        r:           Redis client
        key:         concurrency key — platform or Workday DC key
        status_code: HTTP response status code (0 for non-HTTP exception)
        error_type:  classify_error() output — used to track timeout + 5xx
    """
    bucket  = _bucket()
    rk      = f"{REDIS_ERRWIN_PREFIX}:{key}:{bucket}"
    pipe    = r.pipeline()
    pipe.hincrby(rk, "total_requests", 1)
    if status_code == 429:
        pipe.hincrby(rk, "errors_429", 1)
    elif status_code == 404:
        pipe.hincrby(rk, "errors_404", 1)
    if error_type == "requests_timeout":
        pipe.hincrby(rk, "errors_timeout", 1)
    elif error_type == "requests_5xx" or (status_code >= 500 and status_code != 0):
        pipe.hincrby(rk, "errors_5xx", 1)
    pipe.expire(rk, CONCURRENCY_WINDOW_TTL)
    pipe.execute()


def get_error_rate(r, key: str) -> float:
    """
    Compute the error rate over the last 10 minutes for a concurrency key.

    Sums the current bucket and the previous one to get a true rolling window
    rather than a reset-at-boundary snapshot.

    Tracks all four Phase 9 error signals (Section 20):
        errors_429     — explicit rate limit
        errors_404     — Workday overload response
        errors_timeout — earliest concurrency signal (server queueing)
        errors_5xx     — soft rate limit / server instability

    Returns 0.0 if no requests have been recorded yet.
    """
    now    = _bucket()
    totals = {
        "total_requests": 0,
        "errors_429":     0,
        "errors_404":     0,
        "errors_timeout": 0,
        "errors_5xx":     0,
    }
    for b in (now, now - 1):
        raw = r.hgetall(f"{REDIS_ERRWIN_PREFIX}:{key}:{b}")
        for field in totals:
            totals[field] += int(raw.get(field, 0))
    if totals["total_requests"] == 0:
        return 0.0
    errors = (
        totals["errors_429"]
        + totals["errors_404"]
        + totals["errors_timeout"]
        + totals["errors_5xx"]
    )
    return errors / totals["total_requests"]


def get_baseline_error_rate(r, platform: str) -> float:
    """
    Return the 30-day average error rate for a platform (Phase 9 — Section 20).

    Lookup order:
        1. Redis cache at baseline:error_rate:{platform} (TTL = BASELINE_CACHE_TTL)
        2. PostgreSQL via query_30day_avg_error_rate() on cache miss
        3. 0.0 if fewer than CONCURRENCY_BASELINE_MIN_DAYS (7) days of data
           (cold-start guard — caller skips spike_factor when result is 0.0)

    Args:
        r:        Redis client
        platform: ATS platform name (e.g. "greenhouse", "workday")

    Returns:
        Combined error rate as a float in [0.0, 1.0].
        0.0 signals insufficient history — do NOT interpret as "zero errors".
    """
    cache_key = f"{REDIS_BASELINE_PREFIX}:{platform}"
    cached    = r.get(cache_key)
    if cached is not None:
        return float(cached)

    # Cache miss — query PostgreSQL
    try:
        from db.api_health import query_30day_avg_error_rate
        baseline = query_30day_avg_error_rate(platform)
    except Exception:
        logger.warning(
            "http_client: get_baseline_error_rate failed for platform=%r — "
            "returning 0.0 (spike_factor disabled this window)",
            platform,
        )
        return 0.0

    # Cache the result (including 0.0 so we don't hammer Postgres on cold start)
    try:
        r.set(cache_key, baseline, ex=BASELINE_CACHE_TTL)
    except Exception:
        pass   # stale read is acceptable

    return baseline


# ─────────────────────────────────────────
# DISTRIBUTED SEMAPHORE
# ─────────────────────────────────────────

def _default_limit(key: str) -> int:
    """
    Return the starting concurrency limit for a key.

    Workday DC keys (workday_wd*) use CONCURRENCY_WORKDAY_DEFAULT.
    All other keys use MONITOR_PLATFORM_CONCURRENCY or the default.
    """
    if key.startswith("workday_"):
        return CONCURRENCY_WORKDAY_DEFAULT
    return MONITOR_PLATFORM_CONCURRENCY.get(key, MONITOR_PLATFORM_CONCURRENCY_DEFAULT)


def _get_limit(r, key: str) -> int:
    """Read the current concurrency limit from Redis; seed it if missing."""
    limit_key = f"{REDIS_CONCURRENCY_LIMIT_PREFIX}:{key}"
    raw = r.get(limit_key)
    if raw is None:
        default = _default_limit(key)
        r.set(limit_key, default)
        return default
    return int(raw)


def _acquire(r, key: str) -> bool:
    """
    Atomically increment the active counter and check against the limit.

    Returns True if the slot was acquired, False if the limit is reached.
    On False the counter is immediately decremented (no slot consumed).
    """
    active_key = f"{REDIS_CONCURRENCY_ACTIVE_PREFIX}:{key}"
    active     = r.incr(active_key)
    limit      = _get_limit(r, key)
    if active <= limit:
        return True
    # Over the limit — give the slot back immediately
    r.decr(active_key)
    return False


def _release(r, key: str) -> None:
    """Decrement the in-flight counter, clamped to 0."""
    active_key = f"{REDIS_CONCURRENCY_ACTIVE_PREFIX}:{key}"
    # DECR can go negative if a worker crashed mid-call; clamp to 0.
    new_val = r.decr(active_key)
    if new_val < 0:
        r.set(active_key, 0)


# ─────────────────────────────────────────
# FEEDBACK LOOP
# ─────────────────────────────────────────

def adjust_concurrency(r, key: str, error_rate: float) -> None:
    """
    Adjust the concurrency limit for a key based on the current error rate
    and, when sufficient history exists, the spike_factor vs 30-day baseline.

    Phase 9 two-condition reduction logic (Section 20):
        spike_factor = error_rate / (baseline + 0.001)

        spike_factor > CONCURRENCY_SPIKE_FACTOR_THRESHOLD (5.0)
            → aggressive reduction: max(current - 2, floor)
              Interpretation: today's errors are ≥5× the historical norm —
              almost certainly concurrency-induced.

        spike_factor ≤ threshold (or baseline == 0.0, i.e. cold start)
            → cautious reduction: max(current - 1, floor)
              Interpretation: errors within normal variance — reduce gently
              so we don't over-correct.

    On the increase side there is no spike_factor check — we grow only when
    error_rate is clearly below the increase threshold.

    Args:
        r:          Redis client
        key:        concurrency key — platform or Workday DC key
        error_rate: value from get_error_rate() — combined 4-signal rate
    """
    # Determine floor/ceil for this key
    if key.startswith("workday_"):
        floor    = CONCURRENCY_FLOOR.get("workday", CONCURRENCY_FLOOR_DEFAULT)
        ceil_    = CONCURRENCY_CEIL.get("workday", CONCURRENCY_CEIL_DEFAULT)
        platform = "workday"
    else:
        floor    = CONCURRENCY_FLOOR.get(key, CONCURRENCY_FLOOR_DEFAULT)
        ceil_    = CONCURRENCY_CEIL.get(key, CONCURRENCY_CEIL_DEFAULT)
        platform = key

    current = _get_limit(r, key)

    if error_rate > CONCURRENCY_ERROR_RATE_REDUCE:
        # Determine reduction step via spike_factor
        baseline     = get_baseline_error_rate(r, platform)
        spike_factor = error_rate / (baseline + 0.001)

        # Signal manager._check_error_spikes() — TTL is 1.5× manager cycle (90s)
        # so the flag survives until the next 60s cycle even if written mid-cycle.
        try:
            r.set(
                f"manager:platform:{platform}:error_spike",
                json.dumps({
                    "error_rate":   error_rate,
                    "baseline":     baseline,
                    "spike_factor": spike_factor,
                    "ts":           time.time(),
                }),
                ex=90,
            )
        except Exception:
            pass

        if baseline > 0.0 and spike_factor > CONCURRENCY_SPIKE_FACTOR_THRESHOLD:
            # Concurrency-induced spike — aggressive reduction
            step      = 2
            reduction = "aggressive"
        else:
            # Normal variance or cold start — cautious reduction
            step      = 1
            reduction = "cautious"

        new_limit = max(current - step, floor)
        logger.info(
            "http_client: concurrency reduced key=%r %d→%d "
            "(error_rate=%.1f%% baseline=%.1f%% spike_factor=%.2f %s)",
            key, current, new_limit,
            error_rate * 100, baseline * 100, spike_factor, reduction,
        )

    elif error_rate < CONCURRENCY_ERROR_RATE_INCREASE:
        # error_rate is a 20-min rolling window across ALL concurrent workers —
        # if < 2%, the collective concurrent load has been clean. Record the
        # current active count as the proven high-water mark (learned ceiling).
        # active is read BEFORE _release() so it still includes this request.
        active_raw = r.get(f"{REDIS_CONCURRENCY_ACTIVE_PREFIX}:{key}")
        active = int(active_raw) if active_raw is not None else 0
        if active > 0:
            learned_raw = r.get(f"worker:ceil:learned:{key}")
            learned = int(learned_raw) if learned_raw is not None else 0
            if active > learned:
                r.set(f"worker:ceil:learned:{key}", active)

        # Utilization-gated probing: only increase limit when actually using
        # the current limit — prevents blind climbing when workers are idle.
        if active >= current:
            new_limit = min(current + 1, ceil_)
        else:
            return   # underutilised — hold limit, don't climb
        logger.info(
            "http_client: concurrency increased key=%r %d→%d "
            "(error_rate=%.1f%% active=%d learned_ceil=%d)",
            key, current, new_limit, error_rate * 100, active,
            int(r.get(f"worker:ceil:learned:{key}") or 0),
        )

    else:
        return   # stable — no change

    if new_limit != current:
        r.set(f"{REDIS_CONCURRENCY_LIMIT_PREFIX}:{key}", new_limit)


# ─────────────────────────────────────────
# WORKDAY DC KEY HELPERS
# ─────────────────────────────────────────

def extract_workday_dc_key(ats_slug) -> str:
    """
    Return the Workday DC concurrency key for a company.

    The 'wd' field in the ats_slug JSON contains the data-center suffix
    already identified at ATS detection time.

    Args:
        ats_slug: dict or JSON string from prospective_companies.ats_slug

    Returns:
        Key string e.g. "workday_wd12", "workday_wd1", "workday_default"

    Examples:
        {"slug": "salesforce", "wd": "wd12", ...} → "workday_wd12"
        {"slug": "amazon",     "wd": "wd5",  ...} → "workday_wd5"
        {"slug": "company"}                        → "workday_default"
    """
    if isinstance(ats_slug, str):
        try:
            ats_slug = json.loads(ats_slug)
        except (json.JSONDecodeError, TypeError):
            return "workday_default"

    if not isinstance(ats_slug, dict):
        return "workday_default"

    wd = ats_slug.get("wd")
    return f"workday_{wd}" if wd else "workday_default"


def discover_workday_dc_keys() -> set:
    """
    Query prospective_companies and return the set of all known Workday DC keys.

    Called once at run_scheduler() startup so Redis semaphore limit keys are
    pre-seeded for every DC before the first dispatch.

    Returns:
        Set of DC key strings, e.g. {"workday_wd1", "workday_wd12", "workday_default"}
    """
    from db.db import get_conn
    conn = get_conn()
    try:
        rows = conn.execute("""
            SELECT DISTINCT ats_slug
            FROM prospective_companies
            WHERE ats_platform IN ('workday', 'workdaysites')
              AND ats_slug IS NOT NULL
        """).fetchall()
    finally:
        conn.close()

    dc_keys = set()
    for row in rows:
        dc_keys.add(extract_workday_dc_key(row["ats_slug"]))

    return dc_keys or {"workday_default"}


def seed_concurrency_limits(r, dc_keys: set) -> None:
    """
    Pre-seed Redis concurrency:limit keys for all known platforms and Workday DCs.

    Called once at scheduler startup (run_scheduler → seed_concurrency_limits).
    Uses SET NX so existing limits written by the feedback loop are preserved
    across restarts — only missing keys are initialised.

    Args:
        r:       Redis client
        dc_keys: set of Workday DC key strings from discover_workday_dc_keys()
    """
    all_keys = list(MONITOR_PLATFORM_CONCURRENCY.keys()) + list(dc_keys)
    for key in all_keys:
        limit_key = f"{REDIS_CONCURRENCY_LIMIT_PREFIX}:{key}"
        # NX = only set if not already present (preserves feedback-loop values)
        r.set(limit_key, _default_limit(key), nx=True)
    logger.info(
        "http_client: seeded concurrency limits for %d keys (%d Workday DCs)",
        len(all_keys), len(dc_keys),
    )


# ─────────────────────────────────────────
# MAIN PUBLIC API
# ─────────────────────────────────────────

def ats_get(
    url: str,
    platform: str,
    dc_key: Optional[str] = None,
    **kwargs,
) -> requests.Response:
    """
    Drop-in replacement for requests.get() for all ATS HTTP calls.

    Enforces the distributed semaphore, updates the sliding window, runs
    the feedback loop, and records to api_health — all transparently.

    Args:
        url:      the URL to fetch
        platform: ATS platform name (e.g. "greenhouse", "workday")
        dc_key:   Workday DC key from extract_workday_dc_key() — when provided,
                  per-DC rate limiting is used instead of the generic platform key.
                  Pass None for all non-Workday platforms.
        **kwargs: forwarded to requests.get() (headers, params, timeout, etc.)

    Returns:
        requests.Response

    Raises:
        requests.RequestException subclasses on network/HTTP failure
        (caller is responsible for handling and recording the error).

    Note on timeouts:
        If the caller does not pass a 'timeout' kwarg, the standard
        (connect=10s, read=30s) timeouts are applied automatically.
    """
    r   = get_redis()
    key = dc_key if dc_key else platform

    # Apply default timeouts unless caller overrides
    if "timeout" not in kwargs:
        kwargs["timeout"] = (_CONNECT_TIMEOUT, _READ_TIMEOUT)

    # ── Acquire distributed semaphore ─────────────────────────────────────────
    acquired = False
    for attempt in range(CONCURRENCY_MAX_RETRIES + 1):
        if _acquire(r, key):
            acquired = True
            break
        if attempt < CONCURRENCY_MAX_RETRIES:
            backoff = CONCURRENCY_BACKOFF_BASE * (2 ** attempt) + random.uniform(
                0, CONCURRENCY_BACKOFF_MAX
            )
            logger.debug(
                "http_client: semaphore full key=%r attempt=%d backoff=%.2fs",
                key, attempt + 1, backoff,
            )
            time.sleep(backoff)

    if not acquired:
        raise CeilingExceeded(key)

    # ── HTTP call ─────────────────────────────────────────────────────────────
    start_ms    = int(time.monotonic() * 1000)
    status_code = 0
    error_type  = None
    exc_caught  = None

    try:
        response    = requests.get(url, **kwargs)
        status_code = response.status_code
        return response

    except requests.Timeout as exc:
        error_type = "requests_timeout"
        exc_caught = exc
        raise

    except requests.ConnectionError as exc:
        error_type = "requests_conn_err"
        exc_caught = exc
        raise

    except requests.HTTPError as exc:
        code       = getattr(exc.response, "status_code", 0) if exc.response is not None else 0
        error_type = "requests_5xx" if code >= 500 else "requests_other_err"
        exc_caught = exc
        raise

    except Exception as exc:
        error_type = "requests_other_err"
        exc_caught = exc
        raise

    finally:
        response_ms = int(time.monotonic() * 1000) - start_ms

        # Always release semaphore
        if acquired:
            _release(r, key)

        # Update sliding window (per-request, in-Redis)
        record_errwin(r, key, status_code, error_type=error_type)

        # Non-blocking write to api_health (PostgreSQL)
        try:
            from db.api_health import record_request
            record_request(
                platform    = platform,
                status_code = status_code,
                response_ms = response_ms,
                error_type  = error_type,
                context     = _get_request_context(),
            )
        except Exception:
            pass   # api_health is observability — never crash the caller

        # Feedback loop — adjust limit based on current 10-min error rate
        try:
            error_rate = get_error_rate(r, key)
            adjust_concurrency(r, key, error_rate)
        except Exception:
            pass   # feedback loop is advisory — never crash the caller
