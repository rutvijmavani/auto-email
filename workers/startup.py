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

    logger.debug("%s config check passed (%d keys)", prefix, len(keys_to_check))


def _mask_url(url: str) -> str:
    """Return *url* with the password replaced by *** for safe logging."""
    try:
        from urllib.parse import urlparse, urlunparse
        _p = urlparse(url)
        if _p.password:
            user_prefix = f"{_p.username}:" if _p.username else ""
            return urlunparse(_p._replace(
                netloc=f"{user_prefix}***@{_p.hostname}"
                       + (f":{_p.port}" if _p.port else "")
            ))
        return url
    except Exception:
        return "(could not parse URL)"


class _RedisVersionError(Exception):
    """Raised when Redis is reachable but below the required version."""


def _check_redis(prefix: str) -> None:
    """Verify Redis is reachable with a PING."""
    redis_url = os.getenv("REDIS_URL")
    if not redis_url:
        sys.stderr.write(
            f"{prefix} STARTUP FAILED — REDIS_URL is not set; "
            "cannot probe Redis without an explicit URL\n"
        )
        sys.exit(1)
    try:
        # Use a dedicated client with stricter timeouts (5s/3s) so a hung
        # endpoint never blocks startup indefinitely.  The shared get_redis()
        # uses longer timeouts suited for runtime use, not fail-fast startup probes.
        import redis as _redis_lib
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
            _info    = r.info("server")
            _ver_str = _info.get("redis_version", "0.0")
            try:
                _ver = tuple(int(x) for x in _ver_str.split(".")[:2])
            except ValueError as err:
                raise _RedisVersionError(
                    f"Redis version string {_ver_str!r} could not be parsed — "
                    "ensure Redis ≥6.2 is installed"
                ) from err
            if _ver < (6, 2):
                raise _RedisVersionError(
                    f"Redis {_info.get('redis_version')} is too old — "
                    "pipeline requires Redis ≥6.2 (LMOVE command)"
                )
        except _RedisVersionError as _ver_err:
            # Version check failed — Redis is reachable but unsupported.
            msg = (
                f"{prefix} STARTUP FAILED — Redis is reachable but too old\n"
                f"  URL: {_mask_url(redis_url)}\n"
                f"  Error: {_ver_err}\n"
                f"  Fix: upgrade Redis to ≥6.2 (sudo apt install redis-server)"
            )
            print(msg, file=sys.stderr)
            logger.error("%s startup: Redis version unsupported: %s", prefix, _ver_err)
            sys.exit(1)
        finally:
            r.close()
    except Exception as exc:
        msg = (
            f"{prefix} STARTUP FAILED — Redis is unreachable\n"
            f"  URL: {_mask_url(redis_url)}\n"
            f"  Error: {exc}\n"
            f"  Fix: sudo systemctl status redis"
        )
        print(msg, file=sys.stderr)
        logger.error("%s startup: Redis unreachable: %s", prefix, exc)
        sys.exit(1)

    logger.debug("%s Redis check passed", prefix)


def _check_postgres(prefix: str) -> None:
    """Verify PostgreSQL is reachable and the schema is accessible."""
    db_url_raw = os.getenv("DATABASE_URL", "(not set)")
    conn = None
    try:
        import psycopg2
        # Bypass the shared pool to enforce a bounded connect_timeout so a
        # slow or unreachable database never blocks startup indefinitely.
        # The pool (get_conn) uses the raw DATABASE_URL which may have no timeout.
        conn = psycopg2.connect(dsn=db_url_raw, connect_timeout=5)
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
            cur.fetchone()
        logger.debug("%s PostgreSQL check passed", prefix)
    except Exception as exc:
        msg = (
            f"{prefix} STARTUP FAILED — PostgreSQL is unreachable\n"
            f"  URL: {_mask_url(db_url_raw)}\n"
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
