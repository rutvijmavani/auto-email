"""
tests/test_adaptive_metrics.py
------------------------------
Nightly adaptive_poll_metrics snapshot (db/adaptive_metrics.py) and the code that
feeds it. SQL is PostgreSQL-only (LATERAL, FILTER), so the DB is mocked and these
tests check orchestration, parameters, error isolation and the counter plumbing.
"""

import inspect
import os
import sys
import unittest
from datetime import date, timedelta
from unittest.mock import MagicMock, patch

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))


def _conn(rowcount=3, neg=0, missing=()):
    conn = MagicMock()

    def _execute(sql, params=None):
        cur = MagicMock()
        cur.rowcount = rowcount
        if "generate_series" in sql:
            cur.fetchall.return_value = [{"d": d} for d in missing]
        else:
            cur.fetchone.return_value = {"n": neg}
        return cur

    conn.execute.side_effect = _execute
    return conn


class TestSnapshotCounters(unittest.TestCase):
    def test_commits_and_returns_rowcount(self):
        from db import adaptive_metrics as am
        conn = _conn(rowcount=5)
        with patch.object(am, "get_conn", return_value=conn):
            self.assertEqual(am.snapshot_counters(date(2026, 11, 7)), 5)
        conn.commit.assert_called_once()
        conn.close.assert_called_once()

    def test_negative_delta_logged(self):
        from db import adaptive_metrics as am
        conn = _conn(neg=2)
        with patch.object(am, "get_conn", return_value=conn), \
             patch.object(am.logger, "warning") as warn:
            am.snapshot_counters(date(2026, 11, 7))
        warn.assert_called_once()

    def test_rollback_on_error(self):
        from db import adaptive_metrics as am
        conn = MagicMock()
        conn.execute.side_effect = RuntimeError("boom")
        with patch.object(am, "get_conn", return_value=conn):
            with self.assertRaises(RuntimeError):
                am.snapshot_counters(date(2026, 11, 7))
        conn.rollback.assert_called_once()
        conn.close.assert_called_once()

    def test_first_row_delta_is_zero_and_clamped_in_sql(self):
        from db import adaptive_metrics as am
        self.assertIn("prev.company IS NULL THEN 0", am._COUNTERS_SQL)
        self.assertIn("GREATEST(", am._COUNTERS_SQL)
        self.assertIn("ON CONFLICT (date, company) DO UPDATE", am._COUNTERS_SQL)


class TestSnapshotJobs(unittest.TestCase):
    def test_params_come_from_config_and_registry(self):
        from db import adaptive_metrics as am
        from jobs.ats.registry import LISTING_DATE_RELIABLE
        conn = _conn()
        with patch.object(am, "get_conn", return_value=conn):
            am.snapshot_jobs(date(2026, 11, 6))
        params = conn.execute.call_args[0][1]
        self.assertEqual(params["d"], date(2026, 11, 6))
        self.assertEqual(params["reliable"], sorted(LISTING_DATE_RELIABLE))
        self.assertIn("eightfold", params["timed"])
        self.assertEqual(params["cold"], 24)

    def test_excludes_pre_existing_and_cold_start(self):
        from db import adaptive_metrics as am
        self.assertIn("<> 'pre_existing'", am._JOBS_SQL)
        self.assertIn("f.t0", am._JOBS_SQL)


class TestRunNightly(unittest.TestCase):
    def test_refreshes_today_yesterday_and_missing_days(self):
        from db import adaptive_metrics as am
        today = date(2026, 11, 7)
        missing = [today - timedelta(days=4)]
        with patch.object(am, "snapshot_counters", return_value=9) as sc, \
             patch.object(am, "_missing_days", return_value=missing), \
             patch.object(am, "snapshot_jobs") as sj:
            out = am.run_nightly(today)
        sc.assert_called_once_with(today)
        self.assertEqual(
            [c.args[0] for c in sj.call_args_list],
            sorted({today, today - timedelta(days=1), missing[0]}),
        )
        self.assertEqual(out["counters"], 9)
        self.assertEqual(out["errors"], 0)

    def test_counter_failure_does_not_block_jobs(self):
        from db import adaptive_metrics as am
        today = date(2026, 11, 7)
        with patch.object(am, "snapshot_counters", side_effect=RuntimeError("x")), \
             patch.object(am, "_missing_days", return_value=[]), \
             patch.object(am, "snapshot_jobs") as sj:
            out = am.run_nightly(today)
        self.assertEqual(sj.call_count, 2)
        self.assertEqual(out["errors"], 1)

    def test_one_day_failing_does_not_block_others(self):
        from db import adaptive_metrics as am
        today = date(2026, 11, 7)

        def _jobs(d):
            if d == today:
                raise RuntimeError("x")
            return 1

        with patch.object(am, "snapshot_counters", return_value=0), \
             patch.object(am, "_missing_days", return_value=[]), \
             patch.object(am, "snapshot_jobs", side_effect=_jobs):
            out = am.run_nightly(today)
        self.assertEqual(out["errors"], 1)
        self.assertEqual(out["jobs_days"], [(today - timedelta(days=1)).isoformat()])


class TestUpsertPollStats(unittest.TestCase):
    def _run(self, new_jobs, early):
        from db import job_monitor
        conn = MagicMock()
        with patch.object(job_monitor, "get_conn", return_value=conn):
            job_monitor.upsert_poll_stats("acme", "workday", new_jobs, 5000,
                                          early_exited=early)
        return conn.execute.call_args[0][1]

    def test_empty_scan_increments_empty_not_early(self):
        p = self._run(0, False)
        self.assertEqual(p[-2:], (1, 0))

    def test_early_exit_scan_with_jobs(self):
        p = self._run(4, True)
        self.assertEqual(p[-2:], (0, 1))

    def test_default_early_exited_false(self):
        sig = inspect.signature(__import__("db.job_monitor", fromlist=["x"]).upsert_poll_stats)
        self.assertFalse(sig.parameters["early_exited"].default)


class TestSchedulerNoLongerOwnsConsecutiveEmpty(unittest.TestCase):
    def test_on_adaptive_complete_does_not_assign_consecutive_empty(self):
        from workers import scheduler
        src = inspect.getsource(scheduler)
        start = src.index("def on_adaptive_complete")
        end = src.index("\ndef ", start + 10)
        self.assertNotIn("consecutive_empty", src[start:end].replace(
            "consecutive_empty is owned by upsert_poll_stats only", ""))


class TestManagerHook(unittest.TestCase):
    def test_thread_runs_run_nightly_with_parsed_date(self):
        from workers import manager
        started = {}

        class _T:
            def __init__(self, target, name, daemon):
                started["target"], started["name"], started["daemon"] = target, name, daemon

            def start(self):
                started["target"]()

        with patch("threading.Thread", _T), \
             patch("db.adaptive_metrics.run_nightly") as rn:
            manager._start_adaptive_snapshot("2026-11-07")
        rn.assert_called_once_with(date(2026, 11, 7))
        self.assertTrue(started["daemon"])
        self.assertEqual(started["name"], "adaptive-snapshot")

    def test_failure_is_swallowed(self):
        from workers import manager

        class _T:
            def __init__(self, target, name, daemon):
                self.t = target

            def start(self):
                self.t()

        with patch("threading.Thread", _T), \
             patch("db.adaptive_metrics.run_nightly", side_effect=RuntimeError("x")):
            manager._start_adaptive_snapshot("2026-11-07")  # must not raise


if __name__ == "__main__":
    unittest.main()
