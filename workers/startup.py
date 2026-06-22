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

# Config keys that MUST be non-empty for any pipeline worker to function.
# These are checked by name via os.getenv() so the check is independent of
# how config.py processes them (avoids import-time failures masking the root cause).
_REQUIRED_ENV_KEYS = [
    "REDIS_URL",
    "DATABASE_URL",
    "GMAIL_EMAIL",
    "GMAIL_APP_PASSWORD",
]


def validate_startup(worker_name: str, *, check_db: bool = True,
                     check_redis: bool = True, check_config: bool = True) -> None:
    """
    Run all startup checks for a pipeline worker.

    Prints a clear error and calls sys.exit(1) on the first failure found.
    All checks are designed to be fast (<1s total under normal conditions).

    Args:
        worker_name:   Name used in log/print messages (e.g. "scan_worker").
        check_db:      Whether to verify PostgreSQL connectivity.
        check_redis:   Whether to verify Redis connectivity.
        check_config:  Whether to check required environment variables.
    """
    prefix = f"[{worker_name}]"

    # ── 1. Required config keys ───────────────────────────────────────────────
    if check_config:
        _check_config(prefix)

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

def _check_config(prefix: str) -> None:
    """Verify required environment variables are set."""
    missing = []
    for key in _REQUIRED_ENV_KEYS:
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
        from workers.redis_client import ping, get_redis
        if not ping():
            raise ConnectionError("PING returned False")
        # Also verify we can actually write (catches auth errors that ping misses)
        r = get_redis()
        r.set(f"startup:check:{prefix.strip('[]')}", "1", ex=10)
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
    """Verify PostgreSQL is reachable and the schema is initialized."""
    try:
        from db.db import init_db, get_conn
        init_db()
        conn = get_conn()
        # Reference the real table (catches schema-not-migrated errors) but
        # use LIMIT 1 instead of COUNT(*) to avoid a full-table sequential scan.
        conn.execute("SELECT 1 FROM job_postings LIMIT 1").fetchone()
        conn.close()
        logger.debug("%s PostgreSQL check passed (schema accessible)", prefix)
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
            f"{prefix} STARTUP FAILED — PostgreSQL is unreachable or schema is missing\n"
            f"  URL: {safe_url}\n"
            f"  Error: {exc}\n"
            f"  Fix: check DATABASE_URL in .env; verify PostgreSQL is running;\n"
            f"       run migrations if this is a fresh install."
        )
        print(msg, file=sys.stderr)
        logger.error("%s startup: PostgreSQL check failed: %s", prefix, exc)
        sys.exit(1)
