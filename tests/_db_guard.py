"""
tests/_db_guard.py — Make it impossible for the test suite to touch the
production database.

Rules (enforced at import of the tests package and again before every
TRUNCATE in conftest.cleanup_db):

1. The target database is TEST_DATABASE_URL if set, otherwise the
   DATABASE_URL already present in the real environment (CI sets this).
   The ``.env`` file's DATABASE_URL is never used by tests.
2. The database NAME must contain "test" (e.g. test_pipeline).  Anything
   else aborts the run before a single query is sent.
3. The chosen URL is exported as DATABASE_URL before db.connection is
   imported, so the connection pool can only ever reach the test database.

Local setup (one time):
    createdb test_pipeline
    set TEST_DATABASE_URL=postgresql://localhost/test_pipeline
(or put TEST_DATABASE_URL=... in .env — it is read here, DATABASE_URL from
.env is ignored).
"""

import os
import sys

import psycopg2.extensions

_SETUP_HELP = (
    "Create a throwaway database and point the tests at it:\n"
    "    createdb test_pipeline\n"
    "    TEST_DATABASE_URL=postgresql://localhost/test_pipeline\n"
    "(export it, or add it to .env — DATABASE_URL in .env is NOT used by tests)."
)


class UnsafeTestDatabase(RuntimeError):
    """Raised when the tests would run against a non-test database."""


def db_name(url):
    """Return the database name of a libpq URL or key=value DSN ('' if none)."""
    try:
        return (psycopg2.extensions.parse_dsn(url).get("dbname") or "")
    except Exception:
        return ""


def assert_test_url(url, source="database URL"):
    """Raise UnsafeTestDatabase unless the database name contains 'test'."""
    name = db_name(url or "")
    if "test" not in name.lower():
        raise UnsafeTestDatabase(
            f"REFUSING TO RUN TESTS: {source} points at database "
            f"{name!r}, whose name does not contain 'test'. Tests TRUNCATE "
            f"tables and must never run against a real database.\n"
            + _SETUP_HELP
        )


def _env_file_value(key):
    """Read one key from .env without touching os.environ."""
    try:
        from dotenv import dotenv_values
        root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
        return dotenv_values(os.path.join(root, ".env")).get(key)
    except Exception:
        return None


def resolve_test_url():
    """Pick the test DSN: real-env TEST_DATABASE_URL, .env TEST_DATABASE_URL,
    then a real-env DATABASE_URL (CI).  Never .env's DATABASE_URL."""
    return (
        os.environ.get("TEST_DATABASE_URL")
        or _env_file_value("TEST_DATABASE_URL")
        or os.environ.get("DATABASE_URL")
    )


def enforce_test_database():
    """Validate and pin the test database.  Idempotent.  Returns the URL."""
    url = resolve_test_url()
    if not url:
        raise UnsafeTestDatabase(
            "REFUSING TO RUN TESTS: no TEST_DATABASE_URL set.\n" + _SETUP_HELP
        )
    assert_test_url(url, "TEST_DATABASE_URL/DATABASE_URL")

    # Pin it before db.connection (and its load_dotenv) is imported.
    os.environ["DATABASE_URL"] = url

    mod = sys.modules.get("db.connection")
    if mod is not None:
        # Already imported earlier (e.g. by a plugin): it must agree.
        assert_test_url(getattr(mod, "DATABASE_URL", ""), "db.connection.DATABASE_URL")
    return url


def assert_live_connection_is_test():
    """Check the URL the pool will actually use right now (tests may reassign
    db.connection.DATABASE_URL).  Called by cleanup_db before TRUNCATE."""
    mod = sys.modules.get("db.connection")
    if mod is None:
        import db.connection as mod  # noqa: F401
    assert_test_url(getattr(mod, "DATABASE_URL", ""), "db.connection.DATABASE_URL")
