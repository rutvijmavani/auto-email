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
            last     = frames[-1]
            filename = Path(last.get("filename") or "?").name
            lineno   = last.get("lineno") or "?"
            location = f"{filename}:{lineno}"
        else:
            location = "unknown"

        raw = f"{exc_type}:{location}"
        return hashlib.md5(raw.encode()).hexdigest()[:16]

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


# ─────────────────────────────────────────────────────────────────────────────
# before_send hook
# ─────────────────────────────────────────────────────────────────────────────

def _make_before_send(r):
    """
    Return a Sentry before_send callback wired to Redis client *r*.

    State machine per fingerprint fp:
      ┌───────────────┬──────────────────────────────────────────────────────┐
      │  err:{fp}     │  Decision                                            │
      ├───────────────┼──────────────────────────────────────────────────────┤
      │  not exists   │  NEW   → forward to Sentry, set err + act keys      │
      │  exists       │  KNOWN → suppress (return None), refresh act TTL    │
      └───────────────┴──────────────────────────────────────────────────────┘

    act TTL is dynamic: N_CYCLES × avg inter-arrival time, clamped to
    [MIN_ACT_TTL_S, MAX_ACT_TTL_S].  When act expires the error is
    "resolved"; err also expires after DEDUP_WINDOW_S.  The next occurrence
    after both expire is treated as brand-new (1 Sentry credit per
    unique error per 7-day window, regardless of firing frequency).
    """
    def before_send(event: dict, hint: dict) -> Optional[dict]:
        try:
            fp = _fingerprint(event)
            if fp is None:
                return event               # can't fingerprint → always forward

            err_key = f"{_PFX_ERR}{fp}"
            act_key = f"{_PFX_ACT}{fp}"
            ts_key  = f"{_PFX_TS}{fp}"

            err_exists = bool(r.exists(err_key))

            # Record this occurrence in frequency history (always, new or known)
            _update_frequency(r, ts_key)
            act_ttl = _compute_act_ttl(r, ts_key)

            if err_exists:
                # Known error — suppress, refresh act with dynamic TTL
                r.set(act_key, "1", ex=act_ttl)
                return None               # drop — zero Sentry credits used

            # Brand-new error — forward once and start tracking
            r.set(err_key, "1", ex=DEDUP_WINDOW_S)
            r.set(act_key, "1", ex=act_ttl)

        except Exception:
            # Never let dedup logic prevent error reporting
            pass

        return event

    return before_send


def _noop_before_send(event: dict, hint: dict) -> dict:
    """Fallback used when Redis is unavailable — forwards everything."""
    return event


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
    except Exception:
        pass

    dsn = os.environ.get("SENTRY_DSN", "").strip()
    if not dsn:
        return False   # no-op — safe to call without SENTRY_DSN configured

    try:
        import sentry_sdk
        from sentry_sdk.integrations.logging import LoggingIntegration
    except ImportError:
        # sentry-sdk not installed — silently skip rather than crashing workers
        return False

    # ── Try to get a Redis client for dedup ───────────────────────────────────
    r              = None
    before_send_fn = _noop_before_send

    try:
        from config import REDIS_URL
        import redis as _redis
        r = _redis.from_url(REDIS_URL, decode_responses=True, socket_timeout=1)
        r.ping()   # verify connection before wiring into before_send
        before_send_fn = _make_before_send(r)
    except Exception as exc:
        # Redis unavailable at startup — Sentry still works, just no dedup.
        # Workers will log this but continue normally.
        import sys
        print(
            f"[sentry_init] Redis unavailable — Sentry initialised WITHOUT "
            f"dedup (all errors forwarded): {exc}",
            file=sys.stderr,
        )

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
