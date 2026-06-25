"""
workers/sentry_init.py — Shared Sentry initialisation with Redis-backed dedup.

Import and call init_sentry() once at the start of every long-lived entry
point (pipeline.py, scheduler, detail_worker, fullscan, watchdog …).

Dedup behaviour (mirrors log_monitor.py):
  ┌───────────────────────────────────────────────────────────────────────┐
  │  First occurrence of a unique error   →  forwarded to Sentry (1 hit)  │
  │  Same error within DEDUP_WINDOW (7d)  →  dropped  (return None)       │
  │  Error stops for N_CYCLES × avg_IAT   →  resolved (act key expires)   │
  └───────────────────────────────────────────────────────────────────────┘

  act TTL is DYNAMIC — derived from the error's own firing frequency:
    TTL = clamp(N_CYCLES × avg_inter_arrival_time, 5 min, 24 h)
  This means an error that fires every 8 h won't be marked "resolved"
  after just 4 h of silence — it has to miss 3 full 8-hour cycles (24 h).

Redis key schema  (separate namespace from log_monitor.py):
  sentry:err:{fp}    "1"     TTL = DEDUP_WINDOW  (7 days)
                     Set on first occurrence; suppresses all repeats.
  sentry:act:{fp}    "1"     TTL = dynamic (5 min – 24 h)
                     Refreshed on every occurrence.  Expiry = "resolved".
  sentry:ts:{fp}     list    TTL = DEDUP_WINDOW  (7 days)
                     Last HISTORY_SIZE timestamps; drives act TTL calc.

How fingerprinting works:
  Sentry supplies the parsed exception object inside before_send(), so we
  can fingerprint on the actual exception class + the innermost stack frame
  (filename + line number).  This is more stable than text-pattern matching
  in log files because variable-length error messages don't affect it.

  Example fingerprint inputs:
    TypeError   + db/pipeline_alerts.py:385   →  md5("TypeError:pipeline_alerts.py:385")[:16]
    DataError   + workers/scheduler.py:268    →  md5("DataError:scheduler.py:268")[:16]

Setup:
  1. pip install sentry-sdk loguru-sentry-handler   (or: sentry-sdk[loguru])
  2. Add SENTRY_DSN=https://xxx@oYYY.ingest.sentry.io/ZZZ  to .env
  3. Call init_sentry() at the top of each entry point (before any work).

  If SENTRY_DSN is absent or empty, init_sentry() is a no-op — safe to
  call everywhere without breaking anything.
"""

from __future__ import annotations

import hashlib
import logging
import os
import time
from pathlib import Path
from typing import Any, Optional

# ── Tuning (keep in sync with log_monitor.py) ─────────────────────────────────
DEDUP_WINDOW_S    = 7 * 24 * 3600  # suppress repeat of same error for 7 days

# Dynamic active-TTL: act TTL = N_CYCLES × avg inter-arrival time (IAT).
# Error is "resolved" when it misses N_CYCLES consecutive expected cycles.
HISTORY_SIZE      = 10    # timestamps to keep per fingerprint
N_CYCLES          = 3     # consecutive missed cycles → resolved
MIN_ACT_TTL_S     = 300   # 5 min floor  (very frequent errors, e.g. every 30 s)
MAX_ACT_TTL_S     = 86400 # 24 h ceiling (daily / infrequent errors)
DEFAULT_ACT_TTL_S = 3600  # 1 h — used until ≥ 2 occurrences are recorded

_PFX_ERR = "sentry:err:"
_PFX_ACT = "sentry:act:"
_PFX_TS  = "sentry:ts:"

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# Fingerprinting
# ─────────────────────────────────────────────────────────────────────────────

def _fingerprint(event: dict) -> Optional[str]:
    """
    Derive a stable fingerprint from a Sentry event.

    Uses the innermost stack frame (filename + lineno) + exception type.
    Strips the full path down to the filename so the fingerprint survives
    server path changes between deploys.

    Returns None for events we cannot fingerprint (let them through always).
    """
    try:
        exc_values = (event.get("exception") or {}).get("values") or []
        if not exc_values:
            return None                           # non-exception event

        exc      = exc_values[-1]
        exc_type = exc.get("type") or "Unknown"
        frames   = (exc.get("stacktrace") or {}).get("frames") or []

        if frames:
            last = frames[-1]
            # Use the full relative path from the frame, not just the basename.
            # Path.name would make workers/utils.py and scripts/utils.py generate
            # identical fingerprints at the same line number.
            filename = last.get("filename") or "?"
            lineno   = last.get("lineno") or "?"
            location = f"{filename}:{lineno}"
        else:
            location = "unknown"

        raw = f"{exc_type}:{location}"
        return hashlib.sha256(raw.encode()).hexdigest()[:16]

    except Exception:
        return None


# ─────────────────────────────────────────────────────────────────────────────
# Frequency tracking helpers
# ─────────────────────────────────────────────────────────────────────────────

def _update_frequency(r, ts_key: str) -> None:
    """Push current timestamp into the history ring-buffer (newest-first)."""
    r.lpush(ts_key, time.time())
    r.ltrim(ts_key, 0, HISTORY_SIZE - 1)
    r.expire(ts_key, DEDUP_WINDOW_S)   # auto-clean with the err key


def _compute_act_ttl(r, ts_key: str) -> int:
    """
    Derive a dynamic act-key TTL from observed inter-arrival time (IAT).

    TTL = clamp(N_CYCLES × avg_IAT, MIN_ACT_TTL_S, MAX_ACT_TTL_S)

    Examples
    --------
    Error every 2 min   → avg_IAT=120 s  → TTL =   360 s  (6 min)
    Error every 2 h     → avg_IAT=7200 s → TTL = 21600 s  (6 h)
    Error every 8 h     → avg_IAT=28800s → TTL = 86400 s  (24 h, ceiling)

    Falls back to DEFAULT_ACT_TTL_S while fewer than 2 timestamps are recorded.
    """
    raw        = r.lrange(ts_key, 0, -1)
    timestamps = sorted(float(t) for t in raw)

    if len(timestamps) < 2:
        return DEFAULT_ACT_TTL_S

    iats    = [timestamps[i + 1] - timestamps[i] for i in range(len(timestamps) - 1)]
    avg_iat = sum(iats) / len(iats)
    ttl     = int(N_CYCLES * avg_iat)
    return max(MIN_ACT_TTL_S, min(MAX_ACT_TTL_S, ttl))


# Atomic dedup: SET NX err_key, EXISTS act_key, SET act_key — all in one call
# so concurrent workers cannot both forward the same error.
# Returns [{OK|nil}, {0|1}] — is_new and act_active.
_DEDUP_LUA = """
local is_new     = redis.call('SET', KEYS[1], '1', 'EX', tonumber(ARGV[1]), 'NX')
local act_active = redis.call('EXISTS', KEYS[2])
redis.call('SET', KEYS[2], '1', 'EX', tonumber(ARGV[2]))
return {is_new, act_active}
"""

# ─────────────────────────────────────────────────────────────────────────────
# before_send hook
# ─────────────────────────────────────────────────────────────────────────────

def _make_lazy_before_send():
    """
    Return a before_send that acquires (or re-acquires) a Redis client on
    demand, so dedup works even when Redis is unavailable at init_sentry() time
    and recovers later in the worker's lifetime.

    The client is cached after the first successful ping; on any Redis error the
    cache is cleared so the next event triggers a fresh connection attempt.
    """
    _state: dict = {"r": None}

    def before_send(event: dict, hint: dict) -> Optional[dict]:
        # Try to get / reconnect Redis
        if _state["r"] is None:
            try:
                from config import REDIS_URL
                import redis as _redis_mod
                _r = _redis_mod.from_url(REDIS_URL, decode_responses=True,
                                          socket_timeout=1)
                _r.ping()
                _state["r"] = _r
            except Exception as _conn_err:
                logger.debug(
                    "sentry_init: Redis unavailable, dedup skipped: %s", _conn_err
                )
                return event   # forward without dedup until Redis is back

        try:
            fp = _fingerprint(event)
            if fp is None:
                return event

            err_key = f"{_PFX_ERR}{fp}"
            act_key = f"{_PFX_ACT}{fp}"
            ts_key  = f"{_PFX_TS}{fp}"

            _update_frequency(_state["r"], ts_key)
            act_ttl = _compute_act_ttl(_state["r"], ts_key)

            _res       = _state["r"].eval(_DEDUP_LUA, 2, err_key, act_key,
                                           DEDUP_WINDOW_S, act_ttl)
            is_new     = _res[0]
            act_active = _res[1]

            if not is_new and act_active:
                return None

        except Exception as _dedup_err:
            _state["r"] = None   # reset so next event reconnects
            logger.debug("sentry dedup error (ignored): %s", _dedup_err, exc_info=True)

        return event

    return before_send


# ─────────────────────────────────────────────────────────────────────────────
# Public API
# ─────────────────────────────────────────────────────────────────────────────

def init_sentry(
    release: Optional[str] = None,
    extra_integrations: Optional[list] = None,
) -> bool:
    """
    Initialise the Sentry SDK with Redis-backed dedup.

    Returns True if Sentry was initialised, False if SENTRY_DSN is absent
    (so callers can log a warning if they care).

    Args:
        release:            Optional release string (e.g. git commit SHA).
                            Defaults to the GIT_COMMIT env var if set.
        extra_integrations: Additional sentry_sdk integrations to pass
                            alongside the Loguru integration.
    """
    try:
        from dotenv import load_dotenv
        load_dotenv(Path(__file__).resolve().parent.parent / ".env")
    except Exception as _env_err:
        logger.debug("sentry .env load failed (ignored): %s", _env_err, exc_info=True)

    dsn = os.environ.get("SENTRY_DSN", "").strip()
    if not dsn:
        return False   # no-op — safe to call without SENTRY_DSN configured

    try:
        import sentry_sdk
        from sentry_sdk.integrations.logging import LoggingIntegration
    except ImportError:
        # sentry-sdk not installed — silently skip rather than crashing workers
        return False

    # ── Wire up before_send with lazy Redis reconnection ─────────────────────
    # Do NOT permanently fall back to _noop_before_send if Redis is briefly
    # unavailable at startup — the worker runs for hours and Redis usually
    # recovers quickly.  Instead, use a closure that tries to acquire (or
    # re-acquire) the Redis client on the first event after each failure, so
    # dedup re-activates automatically once Redis comes back.
    before_send_fn = _make_lazy_before_send()

    # ── Integrations ──────────────────────────────────────────────────────────
    integrations = [
        # Capture logger.error() / logger.critical() as Sentry events.
        # level=ERROR means logger.warning() is NOT forwarded — keeps quota down.
        LoggingIntegration(
            level=logging.ERROR,         # capture as breadcrumb from ERROR up
            event_level=logging.ERROR,   # create Sentry event from ERROR up
        ),
    ]
    if extra_integrations:
        integrations.extend(extra_integrations)

    # ── Try Loguru integration if loguru-sentry-handler is installed ──────────
    try:
        from sentry_sdk.integrations.loguru import LoguruIntegration
        integrations.append(
            LoguruIntegration(
                level=logging.ERROR,
                event_level=logging.ERROR,
            )
        )
    except ImportError:
        pass   # standard logging integration above already covers this

    sentry_sdk.init(
        dsn           = dsn,
        integrations  = integrations,
        before_send   = before_send_fn,
        environment   = os.environ.get("ENVIRONMENT", "production"),
        release       = release or os.environ.get("GIT_COMMIT", ""),
        # Don't send PII (email addresses, IP addresses) by default
        send_default_pii = False,
        # Sample rate for performance tracing (set to 0 to disable tracing)
        traces_sample_rate = 0.0,
    )

    return True
