"""
workers/redis_client.py — Singleton Redis connection for the scan worker.

All workers import get_redis() — one client per process, thread-safe
(redis-py client uses a connection pool internally).

Configuration:
    REDIS_URL env var (default: redis://localhost:6379/0)
    Set in .env:  REDIS_URL=redis://localhost:6379/0
"""

import redis as _redis_lib
from config import REDIS_URL

_client: "_redis_lib.Redis | None" = None


def get_redis() -> "_redis_lib.Redis":
    """
    Return the singleton Redis client, creating it on first call.
    Uses decode_responses=True so all values are str (not bytes).
    Not thread-safe to initialise from multiple threads simultaneously —
    acceptable because workers call this once at startup.
    """
    global _client
    if _client is None:
        _client = _redis_lib.from_url(REDIS_URL, decode_responses=True)
    return _client


def ping() -> bool:
    """Return True if Redis is reachable. Used by startup health check."""
    try:
        get_redis().ping()
        return True
    except Exception:
        return False
