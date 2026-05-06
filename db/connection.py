# db/connection.py — PostgreSQL connection pool with SQLite-compatible adapter
#
# Switches from sqlite3 to psycopg2 (PostgreSQL).
# Provides an adapter layer so all existing DB modules continue to work:
#   - ? placeholders are converted to %s automatically
#   - conn.execute() / conn.cursor() surface API is preserved
#   - conn.close() returns the connection to the pool (does NOT close it)
#   - RealDictCursor is used so row["column"] access works everywhere
#
# Configuration:
#   DATABASE_URL  — PostgreSQL DSN (env var)
#                   default: postgresql://localhost/recruiter_pipeline
#
# Backward compat:
#   DB_FILE is kept as an alias for DATABASE_URL so existing test overrides
#   and imports from db.db still work without changes.
#   Tests should migrate to: db_connection.DATABASE_URL = "postgresql://..."
#                             db_connection._pool = None  # force pool reset

import os
import re
import threading

import psycopg2
import psycopg2.extras
import psycopg2.pool
from dotenv import load_dotenv

load_dotenv()   # no-op if env vars already set; loads .env on first import

DATABASE_URL: str = os.environ.get(
    "DATABASE_URL",
    "postgresql://localhost/recruiter_pipeline",
)

# Backward compat alias — points to PostgreSQL DSN now
DB_FILE: str = DATABASE_URL

DAILY_LIMITS: dict = {
    "gemini-2.5-flash-lite": 20,
    "gemini-2.5-flash":      20,
    "gemma-4-31b-it":        1500,
}

RPM_LIMITS: dict = {
    "gemini-2.5-flash-lite": 10,
    "gemini-2.5-flash":       5,
    "gemma-4-31b-it":        15,
}


# ─────────────────────────────────────────
# CONNECTION POOL
# ─────────────────────────────────────────

_pool: "psycopg2.pool.ThreadedConnectionPool | None" = None
_pool_lock: threading.Lock = threading.Lock()


def _get_pool() -> "psycopg2.pool.ThreadedConnectionPool":
    """
    Return the singleton connection pool, creating it on first call.
    Thread-safe via double-checked locking.
    """
    import db.connection as _self
    if _self._pool is None:
        with _pool_lock:
            if _self._pool is None:
                _self._pool = psycopg2.pool.ThreadedConnectionPool(
                    minconn=2,
                    maxconn=25,
                    dsn=_self.DATABASE_URL,
                )
    return _self._pool


# ─────────────────────────────────────────
# SQL ADAPTER — ? → %s
# ─────────────────────────────────────────

_PLACEHOLDER_RE = re.compile(r'\?')


def _adapt_sql(sql: str) -> str:
    """
    Convert SQLite ? placeholders to psycopg2 %s placeholders.

    Safe because:
    - All SQL in this codebase uses only ? as a parameter marker
    - No SQL strings contain bare % characters
    If you add LIKE '%%foo%%' patterns in future, double the %% as required
    by psycopg2 when params are present.
    """
    return _PLACEHOLDER_RE.sub('%s', sql)


# ─────────────────────────────────────────
# CURSOR ADAPTER
# ─────────────────────────────────────────

class _Cursor:
    """
    Wraps a psycopg2 RealDictCursor with a SQLite-compatible surface.

    Key differences from sqlite3.Cursor:
    - ? placeholders auto-converted to %s
    - .lastrowid is NOT supported — use RETURNING id + .fetchone()["id"]
    - .rowcount works identically
    - .fetchone() / .fetchall() return RealDictRow objects (dict-like)
    """
    __slots__ = ("_cur",)

    def __init__(self, cur: "psycopg2.extras.RealDictCursor"):
        self._cur = cur

    def execute(self, sql: str, params=None) -> "_Cursor":
        self._cur.execute(_adapt_sql(sql), params)
        return self

    def executemany(self, sql: str, params_list) -> "_Cursor":
        self._cur.executemany(_adapt_sql(sql), params_list)
        return self

    def fetchone(self):
        return self._cur.fetchone()

    def fetchall(self):
        return self._cur.fetchall()

    @property
    def rowcount(self) -> int:
        return self._cur.rowcount

    def __iter__(self):
        return iter(self._cur)


# ─────────────────────────────────────────
# CONNECTION ADAPTER
# ─────────────────────────────────────────

class _Connection:
    """
    Pool-backed PostgreSQL connection with a SQLite-compatible surface API.

    Usage pattern:
        conn = get_conn()
        try:
            conn.execute("INSERT ... RETURNING id", params)
            conn.commit()
        finally:
            conn.close()     # returns to pool

    Or via context manager:
        with get_conn() as conn:
            conn.execute(...)  # auto-commit on __exit__, rollback on error

    conn.cursor() returns a _Cursor (RealDictCursor wrapper).
    conn.execute(sql, params) is shorthand for cursor + execute.
    conn.close() returns the connection to the pool — does NOT close the socket.
    """
    __slots__ = ("_conn", "_pool")

    def __init__(self, conn, pool):
        self._conn = conn
        self._pool = pool

    # ── Cursor ──────────────────────────

    def cursor(self) -> _Cursor:
        return _Cursor(
            self._conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        )

    # ── Shorthand execute ────────────────

    def execute(self, sql: str, params=None) -> _Cursor:
        cur = self.cursor()
        cur.execute(sql, params)
        return cur

    def executemany(self, sql: str, params_list) -> _Cursor:
        cur = self.cursor()
        cur.executemany(sql, params_list)
        return cur

    # ── Transaction control ──────────────

    def commit(self):
        self._conn.commit()

    def rollback(self):
        self._conn.rollback()

    # ── Pool return ───────────────────────

    def close(self):
        """Return this connection to the pool. Does NOT close the socket."""
        self._pool.putconn(self._conn)

    # ── Context manager ───────────────────

    def __enter__(self) -> "_Connection":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type:
            try:
                self.rollback()
            except Exception:
                pass
        self.close()
        return False


# ─────────────────────────────────────────
# PUBLIC API
# ─────────────────────────────────────────

def get_conn() -> _Connection:
    """
    Borrow a connection from the pool.
    Caller MUST call conn.close() (or use as context manager) to return it.
    The connection has autocommit=False — all writes require conn.commit().
    """
    pool = _get_pool()
    raw  = pool.getconn()
    raw.autocommit = False
    return _Connection(raw, pool)
