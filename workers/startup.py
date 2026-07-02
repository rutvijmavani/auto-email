"""
workers/startup.py — Shared startup validation for all pipeline workers.

Called once at the top of each run_worker() before the main loop starts.
Fail-fast with a clear error message so systemd logs pinpoint the root cause
immediately rather than the worker crashing on first DB/Redis operation.

Checks:
    1. Redis reachability — PING test (existing per-worker check consolidated here)
    2. PostgreSQL connectivity — SELECT 1 to verify the connection pool is live
    3. Required config keys — GMAIL_EMAIL, GMAIL_APP_PASSWORD, REDIS_URL, DATABASE_URL present

Usage:
    from workers.startup import validate_startup
    validate_startup("scan_worker")   # exits with sys.exit(1) on any failure
"""

import os
import sys
import logging

logger = logging.getLogger(__name__)

# Config keys required for all pipeline workers.
_REQUIRED_ENV_KEYS = [
    "REDIS_URL",
    "DATABASE_URL",
]

# Additional keys required only by workers that send email.
_GMAIL_ENV_KEYS = [
    "GMAIL_EMAIL",
    "GMAIL_APP_PASSWORD",
]


def validate_startup(worker_name: str, *, check_db: bool = True,
                     check_redis: bool = True, check_config: bool = True,
                     check_gmail: bool = False) -> None:
    """
    Run all startup checks for a pipeline worker.

    Prints a clear error and calls sys.exit(1) on the first failure found.
    All checks are designed to be fast (<1s total under normal conditions).

    Args:
        worker_name:   Name used in log/print messages (e.g. "scan_worker").
        check_db:      Whether to verify PostgreSQL connectivity.
        check_redis:   Whether to verify Redis connectivity.
        check_config:  Whether to check required environment variables.
        check_gmail:   Whether to verify Gmail credentials are present.
                       Only needed by workers that send email directly.
    """
    prefix = f"[{worker_name}]"

    # ── 1. Required config keys ───────────────────────────────────────────────
    if check_config:
        _check_config(prefix, include_gmail=check_gmail)

    # ── 2. Redis reachability ─────────────────────────────────────────────────
    if check_redis:
        _check_redis(prefix)

    # ── 3. PostgreSQL connectivity ────────────────────────────────────────────
    if check_db:
        _check_postgres(prefix)

    logger.info("%s startup validation passed", worker_name)


# ─────────────────────────────────────────
# CHECK IMPLEMENTATIONS
# ─────────────────────────────────────────

def _check_config(prefix: str, *, include_gmail: bool = False) -> None:
    """Verify required environment variables are set."""
    keys_to_check = list(_REQUIRED_ENV_KEYS)
    if include_gmail:
        keys_to_check.extend(_GMAIL_ENV_KEYS)
    missing = []
    for key in keys_to_check:
        val = os.getenv(key, "").strip()
        if not val:
            missing.append(key)

    if missing:
        msg = (
            f"{prefix} STARTUP FAILED — missing required config keys: "
            f"{', '.join(missing)}\n"
            f"  Check your .env file at the project root.\n"
            f"  Each key must be set to a non-empty value."
        )
        print(msg, file=sys.stderr)
        logger.error("%s startup: missing config keys: %s", prefix, missing)
        sys.exit(1)

    logger.debug("%s config check passed (%d keys)", prefix, len(_REQUIRED_ENV_KEYS))


def _check_redis(prefix: str) -> None:
    """Verify Redis is reachable with a PING."""
    try:
        # Use a dedicated client with short timeouts for ALL Redis checks so a
        # hung endpoint never blocks startup indefinitely.  The shared get_redis()
        # has no socket timeout, so even the initial PING must use this client.
        import redis as _redis_lib
        redis_url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
        r = _redis_lib.from_url(
            redis_url,
            socket_timeout=5,
            socket_connect_timeout=3,
        )
        try:
            if not r.ping():
                raise ConnectionError("PING returned False")
            r.set(f"startup:check:{prefix.strip('[]')}", "1", ex=10)
            # LMOVE (used by detail_worker for at-least-once delivery) requires Redis ≥6.2.
            _info = r.info("server")
            _ver  = tuple(int(x) for x in _info.get("redis_version", "0.0").split(".")[:2])
            if _ver < (6, 2):
                raise RuntimeError(
                    f"Redis {_info.get('redis_version')} is too old — "
                    "pipeline requires Redis ≥6.2 (LMOVE command)"
                )
        finally:
            r.close()
    except Exception as exc:
        redis_url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
        # Mask password so it never appears in systemd/journal logs.
        try:
            from urllib.parse import urlparse, urlunparse
            _p = urlparse(redis_url)
            if _p.password:
                user_prefix = f"{_p.username}:" if _p.username else ""
                safe_url = urlunparse(_p._replace(
                    netloc=f"{user_prefix}***@{_p.hostname}"
                           + (f":{_p.port}" if _p.port else "")
                ))
            else:
                safe_url = redis_url
        except Exception:
            safe_url = "(could not parse REDIS_URL)"
        msg = (
            f"{prefix} STARTUP FAILED — Redis is unreachable\n"
            f"  URL: {safe_url}\n"
            f"  Error: {exc}\n"
            f"  Fix: sudo systemctl status redis"
        )
        print(msg, file=sys.stderr)
        logger.error("%s startup: Redis unreachable: %s", prefix, exc)
        sys.exit(1)

    logger.debug("%s Redis check passed", prefix)


def _check_postgres(prefix: str) -> None:
    """Verify PostgreSQL is reachable and the schema is accessible."""
    conn = None
    try:
        from db.db import get_conn
        conn = get_conn()
        conn.execute("SELECT 1").fetchone()
        logger.debug("%s PostgreSQL check passed", prefix)
    except Exception as exc:
        db_url_raw = os.getenv("DATABASE_URL", "(not set)")
        # Mask password in log output
        try:
            from urllib.parse import urlparse, urlunparse
            parsed = urlparse(db_url_raw)
            db_user_prefix = f"{parsed.username}:" if parsed.username else ""
            safe_url = urlunparse(parsed._replace(
                netloc=f"{db_user_prefix}***@{parsed.hostname}"
                       + (f":{parsed.port}" if parsed.port else "")
            ))
        except Exception:
            safe_url = "(could not parse DATABASE_URL)"

        msg = (
            f"{prefix} STARTUP FAILED — PostgreSQL is unreachable\n"
            f"  URL: {safe_url}\n"
            f"  Error: {exc}\n"
            f"  Fix: check DATABASE_URL in .env; verify PostgreSQL is running."
        )
        print(msg, file=sys.stderr)
        logger.error("%s startup: PostgreSQL check failed: %s", prefix, exc)
        sys.exit(1)
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception as _close_err:
                logger.debug("startup: PostgreSQL connection close failed: %s", _close_err)
