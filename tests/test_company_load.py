"""
tests/test_company_load.py — per-company scan sizing (docs/scan-fullscan-sizing-plan.md).

Covers the pure math (backlog / steady), the table reads (missing table rebuilds
from the DB, unknown company uses the default d) and the manager's sizing path.
"""

import math
import unittest
from unittest.mock import MagicMock, patch

import workers.manager as mgr
from workers import company_load


H = 1683.0   # scan delay_warn_s on 2026-10-29


class TestMath(unittest.TestCase):

    def test_load_of(self):
        self.assertAlmostEqual(company_load.load_of(40, 3560), 40 / 3600)
        self.assertEqual(company_load.load_of(None, 100), 0.0)
        self.assertEqual(company_load.load_of(0, 0), 0.0)

    def test_300_late_companies_at_40s(self):
        self.assertAlmostEqual(company_load.backlog_term([40.0] * 300, H), 300 * 40 / H)

    def test_30_tiny_late_companies_are_not_30_workers(self):
        self.assertLess(company_load.backlog_term([5.0] * 30, H), 0.1)

    def test_long_job_counts_at_most_one_worker(self):
        self.assertEqual(company_load.backlog_term([7000.0], H), 1.0)

    def test_two_long_plus_small_leaves_a_free_worker(self):
        backlog = company_load.backlog_term([7000.0, 7000.0] + [5.0] * 30, H)
        self.assertEqual(math.ceil(backlog + 1.18), 4)


class TestReads(unittest.TestCase):

    def test_unknown_company_uses_default_d(self):
        r = MagicMock()
        r.hmget.return_value = ["40.0,3600.0", None, "garbage"]
        self.assertEqual(company_load.durations(r, "scan", ["a", "b", "c"], 35.0),
                         [40.0, 35.0, 35.0])

    def test_missing_table_is_rebuilt_not_zero(self):
        r = MagicMock()
        r.get.side_effect = [None, "1.5"]
        with patch.object(company_load, "rebuild") as rebuild:
            self.assertEqual(company_load.steady_load(r, "scan", 35.0), 1.5)
        rebuild.assert_called_once_with(r, "scan", 35.0)

    def test_failed_rebuild_is_unknown_not_zero(self):
        r = MagicMock()
        r.get.return_value = None
        with patch.object(company_load, "rebuild", side_effect=RuntimeError("db")):
            self.assertIsNone(company_load.steady_load(r, "scan", 35.0))

    def test_update_never_raises(self):
        r = MagicMock()
        r.eval.side_effect = RuntimeError("redis down")
        company_load.update(r, "scan", "acme", 40.0, 3600.0)


class TestManagerSizing(unittest.TestCase):
    PARAMS = {"fetch_p75": 35.0, "delay_warn_s": H}

    def setUp(self):
        mgr._last_work_need.clear()

    def test_work_sizes_the_pool(self):
        r = MagicMock()
        with patch.object(company_load, "steady_load", return_value=0.94), \
             patch.object(company_load, "durations", return_value=[40.0] * 300):
            work = mgr._scan_pool_work(r, "scan", {"late_names": ["x"] * 300}, self.PARAMS)
        self.assertAlmostEqual(work["steady"], 0.94 / mgr.MANAGER_SCALE_UP_UTIL)
        self.assertEqual(math.ceil(work["backlog"] + work["steady"]), 9)

    def test_unreadable_late_list_is_unknown(self):
        self.assertIsNone(mgr._scan_pool_work(MagicMock(), "scan", {"late_names": None},
                                              self.PARAMS))

    def test_unknown_work_keeps_last_need(self):
        r = MagicMock()
        r.get.return_value = None
        r.exists.return_value = False
        params = {"fetch_p75": 35.0, "delay_warn_s": H}
        mgr._run_pool_cycle(r, "scan", 3, 0, 0, 0.0, params, 20, 5,
                            work={"backlog": 6.0, "steady": 1.0, "late": 1})
        mgr._run_pool_cycle(r, "scan", 3, 0, 0, 0.0, params, 20, 5, work=None)
        self.assertEqual(mgr._needed_workers["scan"], 7)


if __name__ == "__main__":
    unittest.main()
