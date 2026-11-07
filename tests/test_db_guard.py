"""Tests for tests/_db_guard.py — the suite must refuse non-test databases."""

import os
import sys
import unittest
from unittest import mock

from tests import _db_guard as g


class TestAssertTestUrl(unittest.TestCase):
    def test_production_name_refused(self):
        with self.assertRaises(g.UnsafeTestDatabase):
            g.assert_test_url("postgresql://localhost/recruiter_pipeline")

    def test_test_name_accepted(self):
        g.assert_test_url("postgresql://u:p@host:5432/test_pipeline")

    def test_kv_dsn_supported(self):
        g.assert_test_url("host=localhost dbname=pipeline_test user=x")
        with self.assertRaises(g.UnsafeTestDatabase):
            g.assert_test_url("host=localhost dbname=recruiter_pipeline")

    def test_unparseable_or_empty_refused(self):
        for bad in ("", None, "not a dsn ::"):
            with self.assertRaises(g.UnsafeTestDatabase):
                g.assert_test_url(bad)

    def test_host_containing_test_does_not_count(self):
        with self.assertRaises(g.UnsafeTestDatabase):
            g.assert_test_url("postgresql://test.example.com/recruiter_pipeline")


class TestEnforce(unittest.TestCase):
    def test_no_url_refused(self):
        with mock.patch.dict(os.environ, {}, clear=True), \
                mock.patch.object(g, "_env_file_value", return_value=None):
            with self.assertRaises(g.UnsafeTestDatabase):
                g.enforce_test_database()

    def test_prod_database_url_in_env_refused(self):
        env = {"DATABASE_URL": "postgresql://localhost/recruiter_pipeline"}
        with mock.patch.dict(os.environ, env, clear=True), \
                mock.patch.object(g, "_env_file_value", return_value=None):
            with self.assertRaises(g.UnsafeTestDatabase):
                g.enforce_test_database()

    def test_test_url_wins_and_is_pinned(self):
        env = {
            "TEST_DATABASE_URL": "postgresql://localhost/test_pipeline",
            "DATABASE_URL": "postgresql://localhost/recruiter_pipeline",
        }
        with mock.patch.dict(os.environ, env, clear=True):
            url = g.enforce_test_database()
            self.assertEqual(url, "postgresql://localhost/test_pipeline")
            self.assertEqual(os.environ["DATABASE_URL"], url)

    def test_env_file_database_url_is_never_used(self):
        def fake(key):
            return {"DATABASE_URL": "postgresql://localhost/recruiter_pipeline"}.get(key)
        with mock.patch.dict(os.environ, {}, clear=True), \
                mock.patch.object(g, "_env_file_value", side_effect=fake):
            with self.assertRaises(g.UnsafeTestDatabase):
                g.enforce_test_database()

    def test_already_imported_connection_must_agree(self):
        fake_mod = mock.Mock(DATABASE_URL="postgresql://localhost/recruiter_pipeline")
        env = {"TEST_DATABASE_URL": "postgresql://localhost/test_pipeline"}
        with mock.patch.dict(os.environ, env, clear=True), \
                mock.patch.dict(sys.modules, {"db.connection": fake_mod}):
            with self.assertRaises(g.UnsafeTestDatabase):
                g.enforce_test_database()


class TestCleanupDbRefusesProduction(unittest.TestCase):
    def test_cleanup_db_raises_and_sends_no_query(self):
        from tests.conftest import cleanup_db
        import db.connection as dbc
        with mock.patch.object(dbc, "DATABASE_URL",
                               "postgresql://localhost/recruiter_pipeline"), \
                mock.patch.object(dbc, "get_conn") as get_conn:
            with self.assertRaises(g.UnsafeTestDatabase):
                cleanup_db()
            get_conn.assert_not_called()


if __name__ == "__main__":
    unittest.main()
