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


class TestRunningTerm(unittest.TestCase):
    """docs/scan-running-work-design.md: target = ceil(backlog + running + steady_rest)."""
    H300 = 300.0
    PARAMS = {"fetch_p75": 35.0, "delay_warn_s": H300}
    # 11-10 operator Redis values: (d, T)
    ACC, CA27, BOA = (3045.0, 10800.0), (3496.0, 21600.0), (8549.0, 43196.0)
    TOTAL = 1.03 * mgr.MANAGER_SCALE_UP_UTIL      # logged steady 1.03

    def work(self, running, late_d=(), total=None):
        table = {"acc": self.ACC, "ca27": self.CA27, "boa": self.BOA}
        rows = [(*table[n], True) if n in table else (0.2, 3600.0, True) for n in running]
        r = MagicMock()
        with patch.object(company_load, "steady_load",
                          return_value=self.TOTAL if total is None else total), \
             patch.object(company_load, "durations", return_value=list(late_d)), \
             patch.object(company_load, "entries", return_value=rows):
            return mgr._scan_pool_work(
                r, "scan", {"late_names": ["l"] * len(late_d), "running_names": list(running)},
                self.PARAMS)

    def target(self, w):
        return math.ceil(w["backlog"] + w["running"] + w["steady"])

    def test_two_long_running_small_late_raises_target(self):
        w = self.work(["ca27", "acc"], late_d=[0.2] * 2)
        self.assertEqual(self.target(w), 3)
        self.assertAlmostEqual(w["running"], 2.0)

    def test_two_long_running_nothing_late(self):
        self.assertEqual(self.target(self.work(["ca27", "acc"])), 3)

    def test_only_one_long_running_target_stays_two(self):
        self.assertEqual(self.target(self.work(["acc"])), 2)

    def test_three_long_running(self):
        self.assertEqual(self.target(self.work(["boa", "acc", "ca27"])), 4)

    def test_small_running_scans_add_almost_nothing(self):
        w = self.work(["s1", "s2"], late_d=[0.2] * 200)
        self.assertEqual(self.target(w), 2)

    def test_running_company_not_in_table_subtracts_nothing(self):
        r = MagicMock()
        with patch.object(company_load, "steady_load", return_value=1.0), \
             patch.object(company_load, "durations", return_value=[]), \
             patch.object(company_load, "entries", return_value=[(35.0, None, False)]):
            w = mgr._scan_pool_work(r, "scan", {"late_names": [], "running_names": ["new"]},
                                    self.PARAMS)
        self.assertAlmostEqual(w["steady"], 1.0 / mgr.MANAGER_SCALE_UP_UTIL)
        self.assertAlmostEqual(w["running"], 35.0 / self.H300)

    def test_steady_rest_clamped_at_zero(self):
        self.assertEqual(company_load.steady_excluding(0.1, [(3000.0, 1000.0, True)]), 0.0)

    def test_unreadable_running_list_is_unknown(self):
        self.assertIsNone(mgr._scan_pool_work(
            MagicMock(), "scan", {"late_names": [], "running_names": None}, self.PARAMS))

    def test_entries_unknown_company(self):
        r = MagicMock()
        r.hmget.return_value = ["40.0,3600.0", None, "garbage"]
        self.assertEqual(company_load.entries(r, "scan", ["a", "b", "c"], 35.0),
                         [(40.0, 3600.0, True), (35.0, None, False), (35.0, None, False)])


class _FakeStreamRedis:
    def __init__(self, pending, alive, cur, msgs):
        self.pending, self.alive, self.cur, self.msgs = pending, alive, cur, msgs

    def xpending(self, *a):
        return {"pending": len(self.pending)}

    def xpending_range(self, *a, **k):
        return [{"message_id": m, "consumer": c} for m, c in self.pending]

    def exists(self, key):
        return key in self.alive

    def get(self, key):
        return self.cur.get(key)

    def xrange(self, key, min=None, max=None, count=None):
        return [(min, {"company": self.msgs[min]})]


class TestGetRunningCompanies(unittest.TestCase):

    def test_only_live_running_owner_counts(self):
        import json
        now = 1000.0
        r = _FakeStreamRedis(
            pending=[("1-0", "worker-h-11"), ("2-0", "worker-h-12"), ("3-0", "worker-h-13"),
                     ("4-0", "other")],
            alive={"worker:alive:scan_worker:h:11", "worker:alive:scan_worker:h:12"},
            cur={"worker:cur:worker-h-11": json.dumps({"msg_id": "1-0", "alive_until": now + 5}),
                 "worker:cur:worker-h-12": json.dumps({"msg_id": "2-0", "alive_until": now - 5})},
            msgs={"1-0": "acc", "2-0": "ca27", "3-0": "x"})
        self.assertEqual(
            mgr._get_running_companies(r, mgr.REDIS_STREAM_ADAPTIVE, "g", now), ["acc"])

    def test_empty_pel(self):
        r = _FakeStreamRedis([], set(), {}, {})
        self.assertEqual(mgr._get_running_companies(r, mgr.REDIS_STREAM_ADAPTIVE, "g", 1.0), [])


if __name__ == "__main__":
    unittest.main()
