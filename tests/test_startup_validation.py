"""
tests/test_startup_validation.py
─────────────────────────────────────────────────────────────────────────────
Tests for workers/startup.py — startup validation for all pipeline workers.

Phase 3.4 — Startup validation in each worker.

Coverage map
────────────
  TestRequiredEnvKeys
    · _REQUIRED_ENV_KEYS contains REDIS_URL, DATABASE_URL (universal keys)
    · _GMAIL_ENV_KEYS contains GMAIL_EMAIL, GMAIL_APP_PASSWORD (email-only keys)
    · _REQUIRED_ENV_KEYS has exactly 2 entries; _GMAIL_ENV_KEYS has exactly 2 entries

  TestValidateStartup
    · All checks pass → no sys.exit called
    · Missing one env var → sys.exit(1)
    · Missing multiple env vars → all listed in message
    · Error message printed to stderr, not stdout
    · Missing var key name appears in error message
    · Worker name (prefix) appears in error message
    · Whitespace-only env var treated as missing (strip() applied)
    · Redis unreachable (ping returns False) → sys.exit(1)
    · Redis raises exception → sys.exit(1)
    · DB init_db() raises → sys.exit(1)
    · check_redis=False → Redis not checked at all
    · check_db=False → PostgreSQL not checked at all
    · check_config=False → env vars not checked at all
    · All checks disabled → always passes immediately
    · STARTUP FAILED string appears in stderr on any failure
"""

import os
import io
import sys
import unittest
from unittest.mock import MagicMock, patch

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _full_env():
    """Return a dict with all required env vars set to valid values."""
    return {
        "REDIS_URL":          "redis://localhost:6379/0",
        "DATABASE_URL":       "postgresql://localhost/test",
        "GMAIL_EMAIL":        "test@example.com",
        "GMAIL_APP_PASSWORD": "secret",
    }


def _missing_env(key):
    """Return a dict with all required vars set EXCEPT `key` (set to empty)."""
    env = _full_env()
    env[key] = ""
    return env


# ─────────────────────────────────────────────────────────────────────────────
# TestRequiredEnvKeys
# ─────────────────────────────────────────────────────────────────────────────

class TestRequiredEnvKeys(unittest.TestCase):
    """_REQUIRED_ENV_KEYS has universal keys; _GMAIL_ENV_KEYS has email-only keys."""

    def setUp(self):
        from workers.startup import _REQUIRED_ENV_KEYS, _GMAIL_ENV_KEYS
        self.keys      = _REQUIRED_ENV_KEYS
        self.gmail_keys = _GMAIL_ENV_KEYS

    def test_contains_redis_url(self):
        self.assertIn("REDIS_URL", self.keys)

    def test_contains_database_url(self):
        self.assertIn("DATABASE_URL", self.keys)

    def test_gmail_email_in_gmail_keys(self):
        self.assertIn("GMAIL_EMAIL", self.gmail_keys)

    def test_gmail_app_password_in_gmail_keys(self):
        self.assertIn("GMAIL_APP_PASSWORD", self.gmail_keys)

    def test_exactly_two_universal_keys(self):
        self.assertEqual(len(self.keys), 2)

    def test_exactly_two_gmail_keys(self):
        self.assertEqual(len(self.gmail_keys), 2)


# ─────────────────────────────────────────────────────────────────────────────
# TestValidateStartup
# ─────────────────────────────────────────────────────────────────────────────

class TestValidateStartup(unittest.TestCase):
    """
    validate_startup() is the public entry point.  It runs up to three
    checks (config, redis, db) and calls sys.exit(1) on the first failure.
    """

    # ── Happy path ────────────────────────────────────────────────────────────

    def test_all_checks_pass_no_exit(self):
        """All checks pass → validate_startup returns without calling sys.exit."""
        mock_conn = MagicMock()
        mock_r = MagicMock()
        # _check_redis creates its own bounded client via redis.from_url.
        # r.ping() must return True; r.info() must return a valid version.
        mock_r.ping.return_value = True
        mock_r.info.return_value = {"redis_version": "7.0.0"}
        with patch.dict(os.environ, _full_env()), \
             patch("redis.from_url", return_value=mock_r), \
             patch("db.db.init_db"), \
             patch("db.db.get_conn", return_value=mock_conn):
            from workers.startup import validate_startup
            try:
                validate_startup("test_worker")
            except SystemExit as exc:
                self.fail(f"validate_startup raised SystemExit({exc.code}) on success")
        mock_conn.close.assert_called_once()

    def test_all_false_passes_immediately(self):
        """All three checks disabled → always passes regardless of environment."""
        from workers.startup import validate_startup
        try:
            validate_startup("test_worker",
                             check_config=False, check_redis=False, check_db=False)
        except SystemExit as exc:
            self.fail(f"Should not exit when all checks disabled, got exit({exc.code})")

    # ── Config check ─────────────────────────────────────────────────────────

    def test_missing_single_env_var_exits_1(self):
        """One missing required env var → sys.exit(1)."""
        with patch.dict(os.environ, _missing_env("REDIS_URL")):
            from workers.startup import validate_startup
            with self.assertRaises(SystemExit) as ctx:
                validate_startup("test_worker", check_db=False, check_redis=False)
            self.assertEqual(ctx.exception.code, 1)

    def test_missing_database_url_exits_1(self):
        """DATABASE_URL missing → sys.exit(1)."""
        with patch.dict(os.environ, _missing_env("DATABASE_URL")):
            from workers.startup import validate_startup
            with self.assertRaises(SystemExit) as ctx:
                validate_startup("test_worker", check_db=False, check_redis=False)
            self.assertEqual(ctx.exception.code, 1)

    def test_missing_gmail_email_exits_1(self):
        """GMAIL_EMAIL missing → sys.exit(1) when check_gmail=True."""
        with patch.dict(os.environ, _missing_env("GMAIL_EMAIL")):
            from workers.startup import validate_startup
            with self.assertRaises(SystemExit) as ctx:
                validate_startup("test_worker",
                                 check_db=False, check_redis=False, check_gmail=True)
            self.assertEqual(ctx.exception.code, 1)

    def test_missing_gmail_app_password_exits_1(self):
        """GMAIL_APP_PASSWORD missing → sys.exit(1) when check_gmail=True."""
        with patch.dict(os.environ, _missing_env("GMAIL_APP_PASSWORD")):
            from workers.startup import validate_startup
            with self.assertRaises(SystemExit) as ctx:
                validate_startup("test_worker",
                                 check_db=False, check_redis=False, check_gmail=True)
            self.assertEqual(ctx.exception.code, 1)

    def test_missing_gmail_email_no_exit_when_check_gmail_false(self):
        """GMAIL_EMAIL missing → no exit when check_gmail=False (default)."""
        with patch.dict(os.environ, _missing_env("GMAIL_EMAIL")):
            from workers.startup import validate_startup
            try:
                validate_startup("test_worker", check_db=False, check_redis=False)
            except SystemExit as exc:
                self.fail(f"Should not exit when check_gmail=False, got exit({exc.code})")

    def test_missing_env_var_error_to_stderr(self):
        """Missing env var → error message written to stderr, not stdout."""
        err_buf = io.StringIO()
        with patch.dict(os.environ, _missing_env("REDIS_URL")), \
             patch("sys.stderr", err_buf):
            from workers.startup import validate_startup
            with self.assertRaises(SystemExit):
                validate_startup("test_worker", check_db=False, check_redis=False)
        self.assertTrue(len(err_buf.getvalue()) > 0,
                        "Expected output to stderr")

    def test_missing_env_var_includes_startup_failed(self):
        """Error message contains 'STARTUP FAILED'."""
        err_buf = io.StringIO()
        with patch.dict(os.environ, _missing_env("REDIS_URL")), \
             patch("sys.stderr", err_buf):
            from workers.startup import validate_startup
            with self.assertRaises(SystemExit):
                validate_startup("test_worker", check_db=False, check_redis=False)
        self.assertIn("STARTUP FAILED", err_buf.getvalue())

    def test_missing_key_name_in_error_message(self):
        """Missing key name appears in the error output."""
        err_buf = io.StringIO()
        with patch.dict(os.environ, _missing_env("GMAIL_EMAIL")), \
             patch("sys.stderr", err_buf):
            from workers.startup import validate_startup
            with self.assertRaises(SystemExit):
                validate_startup("test_worker",
                                 check_db=False, check_redis=False, check_gmail=True)
        self.assertIn("GMAIL_EMAIL", err_buf.getvalue())

    def test_worker_name_in_error_message(self):
        """Worker name appears in the error output."""
        err_buf = io.StringIO()
        with patch.dict(os.environ, _missing_env("DATABASE_URL")), \
             patch("sys.stderr", err_buf):
            from workers.startup import validate_startup
            with self.assertRaises(SystemExit):
                validate_startup("my_special_worker", check_db=False, check_redis=False)
        self.assertIn("my_special_worker", err_buf.getvalue())

    def test_whitespace_only_env_var_treated_as_missing(self):
        """Whitespace-only value is stripped → treated as missing → exit(1)."""
        env = _full_env()
        env["REDIS_URL"] = "   "
        with patch.dict(os.environ, env):
            from workers.startup import validate_startup
            with self.assertRaises(SystemExit) as ctx:
                validate_startup("test_worker", check_db=False, check_redis=False)
            self.assertEqual(ctx.exception.code, 1)

    def test_multiple_missing_vars_all_listed(self):
        """All missing keys appear in the single error message when check_gmail=True."""
        err_buf = io.StringIO()
        env = {
            "REDIS_URL": "", "DATABASE_URL": "",
            "GMAIL_EMAIL": "", "GMAIL_APP_PASSWORD": "",
        }
        with patch.dict(os.environ, env), \
             patch("sys.stderr", err_buf):
            from workers.startup import validate_startup
            with self.assertRaises(SystemExit):
                validate_startup("test_worker",
                                 check_db=False, check_redis=False, check_gmail=True)
        output = err_buf.getvalue()
        for key in ("REDIS_URL", "DATABASE_URL", "GMAIL_EMAIL", "GMAIL_APP_PASSWORD"):
            self.assertIn(key, output, f"Expected {key!r} in error message")

    def test_check_config_false_skips_env_check(self):
        """check_config=False → missing env vars not checked (no exit)."""
        env_missing = {k: "" for k in
                       ["REDIS_URL", "DATABASE_URL", "GMAIL_EMAIL", "GMAIL_APP_PASSWORD"]}
        _mock_r = MagicMock()
        _mock_r.ping.return_value = True
        _mock_r.info.return_value = {"redis_version": "7.0.0"}
        with patch.dict(os.environ, env_missing), \
             patch("redis.from_url", return_value=_mock_r), \
             patch("db.db.init_db"), \
             patch("db.db.get_conn", return_value=MagicMock()):
            from workers.startup import validate_startup
            try:
                validate_startup("test_worker", check_config=False)
            except SystemExit as exc:
                self.fail(f"Should not exit when check_config=False, got exit({exc.code})")

    # ── Redis check ───────────────────────────────────────────────────────────

    def test_redis_ping_false_exits_1(self):
        """r.ping() returns False → sys.exit(1)."""
        mock_r = MagicMock()
        mock_r.ping.return_value = False
        with patch.dict(os.environ, _full_env()), \
             patch("redis.from_url", return_value=mock_r):
            from workers.startup import validate_startup
            with self.assertRaises(SystemExit) as ctx:
                validate_startup("test_worker", check_db=False, check_config=False)
            self.assertEqual(ctx.exception.code, 1)

    def test_redis_ping_exception_exits_1(self):
        """redis.from_url() raises ConnectionError → sys.exit(1)."""
        with patch.dict(os.environ, _full_env()), \
             patch("redis.from_url",
                   side_effect=ConnectionError("connection refused")):
            from workers.startup import validate_startup
            with self.assertRaises(SystemExit) as ctx:
                validate_startup("test_worker", check_db=False, check_config=False)
            self.assertEqual(ctx.exception.code, 1)

    def test_redis_failure_error_to_stderr(self):
        """Redis ping failure → STARTUP FAILED written to stderr."""
        err_buf = io.StringIO()
        mock_r = MagicMock()
        mock_r.ping.return_value = False
        with patch.dict(os.environ, _full_env()), \
             patch("redis.from_url", return_value=mock_r), \
             patch("sys.stderr", err_buf):
            from workers.startup import validate_startup
            with self.assertRaises(SystemExit):
                validate_startup("test_worker", check_db=False, check_config=False)
        self.assertIn("STARTUP FAILED", err_buf.getvalue())

    def test_redis_set_failure_exits_1(self):
        """r.set() raises → startup fails with exit(1)."""
        mock_r = MagicMock()
        mock_r.ping.return_value = True
        mock_r.info.return_value = {"redis_version": "7.0.0"}
        mock_r.set.side_effect = Exception("READONLY You can't write against a read only replica")
        with patch.dict(os.environ, _full_env()), \
             patch("redis.from_url", return_value=mock_r):
            from workers.startup import validate_startup
            with self.assertRaises(SystemExit) as ctx:
                validate_startup("test_worker", check_db=False, check_config=False)
            self.assertEqual(ctx.exception.code, 1)

    def test_redis_old_version_exits_1(self):
        """Redis version < 6.2 (missing LMOVE) → startup fails with exit(1)."""
        mock_r = MagicMock()
        mock_r.ping.return_value = True
        mock_r.info.return_value = {"redis_version": "5.0.14"}
        with patch.dict(os.environ, _full_env()), \
             patch("redis.from_url", return_value=mock_r):
            from workers.startup import validate_startup
            with self.assertRaises(SystemExit) as ctx:
                validate_startup("test_worker", check_db=False, check_config=False)
            self.assertEqual(ctx.exception.code, 1)

    def test_redis_old_version_error_to_stderr(self):
        """Redis version < 6.2 → STARTUP FAILED written to stderr."""
        mock_r = MagicMock()
        mock_r.ping.return_value = True
        mock_r.info.return_value = {"redis_version": "6.0.0"}
        err_buf = io.StringIO()
        with patch.dict(os.environ, _full_env()), \
             patch("redis.from_url", return_value=mock_r), \
             patch("sys.stderr", err_buf):
            from workers.startup import validate_startup
            with self.assertRaises(SystemExit):
                validate_startup("test_worker", check_db=False, check_config=False)
        self.assertIn("STARTUP FAILED", err_buf.getvalue())

    def test_check_redis_false_skips_redis(self):
        """check_redis=False → redis.from_url is never called."""
        mock_conn = MagicMock()
        with patch.dict(os.environ, _full_env()), \
             patch("db.db.get_conn", return_value=mock_conn), \
             patch("redis.from_url",
                   side_effect=AssertionError("redis.from_url must not be called when check_redis=False")) as mock_from_url:
            from workers.startup import validate_startup
            try:
                validate_startup("test_worker", check_redis=False, check_config=False)
            except SystemExit as exc:
                self.fail(f"Should not exit when check_redis=False, got exit({exc.code})")
            mock_from_url.assert_not_called()

    # ── DB check ─────────────────────────────────────────────────────────────

    def test_db_execute_exception_exits_1(self):
        """conn.execute() raises → sys.exit(1) (_check_postgres uses SELECT 1)."""
        mock_conn = MagicMock()
        mock_conn.execute.side_effect = Exception("connection refused")
        with patch.dict(os.environ, _full_env()), \
             patch("db.db.get_conn", return_value=mock_conn):
            from workers.startup import validate_startup
            with self.assertRaises(SystemExit) as ctx:
                validate_startup("test_worker", check_redis=False, check_config=False)
            self.assertEqual(ctx.exception.code, 1)

    def test_db_get_conn_exception_exits_1(self):
        """get_conn() raises → sys.exit(1)."""
        with patch.dict(os.environ, _full_env()), \
             patch("db.db.init_db"), \
             patch("db.db.get_conn", side_effect=Exception("no DB")):
            from workers.startup import validate_startup
            with self.assertRaises(SystemExit) as ctx:
                validate_startup("test_worker", check_redis=False, check_config=False)
            self.assertEqual(ctx.exception.code, 1)

    def test_db_failure_error_to_stderr(self):
        """DB failure → STARTUP FAILED written to stderr."""
        err_buf = io.StringIO()
        mock_conn = MagicMock()
        mock_conn.execute.side_effect = Exception("DB down")
        with patch.dict(os.environ, _full_env()), \
             patch("db.db.get_conn", return_value=mock_conn), \
             patch("sys.stderr", err_buf):
            from workers.startup import validate_startup
            with self.assertRaises(SystemExit):
                validate_startup("test_worker", check_redis=False, check_config=False)
        self.assertIn("STARTUP FAILED", err_buf.getvalue())

    def test_check_db_false_skips_db(self):
        """check_db=False → PostgreSQL not checked (no exit even if DB would fail)."""
        _mock_r = MagicMock()
        _mock_r.ping.return_value = True
        _mock_r.info.return_value = {"redis_version": "7.0.0"}
        with patch.dict(os.environ, _full_env()), \
             patch("redis.from_url", return_value=_mock_r):
            from workers.startup import validate_startup
            try:
                validate_startup("test_worker", check_db=False, check_config=False)
            except SystemExit as exc:
                self.fail(f"Should not exit when check_db=False, got exit({exc.code})")


if __name__ == "__main__":
    unittest.main(verbosity=2)
