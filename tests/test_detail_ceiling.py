"""
tests/test_detail_ceiling.py — nightly detail ceiling (TH step 6).

Covers workers.detail_ceiling (series, windows, percentile, drain simulation,
p-ladder, seat raise, alerts) and manager._plan_detail_nightly (bootstrap while
under DETAIL_ARRIVALS_MIN_HOURS of history; stored plan afterwards).
"""

import json
import math
import os
import sys
import unittest
from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from workers import detail_ceiling as dc

KW = dict(u=0.8, window_min=30, start_pct=99, floor_pct=75, max_wait_min=120,
          others=5, total=20, cap=20, step=2, floor_workers=2)


class TestHelpers(unittest.TestCase):

    def test_dense_series_fills_gaps_with_zero(self):
        t = datetime(2026, 11, 1, 10, 0)
        rows = [(t, 3), (t + timedelta(minutes=1), 4), (t + timedelta(minutes=4), 5)]
        self.assertEqual(dc.dense_series(rows), [3, 4, 0, 0, 5])

    def test_window_sums(self):
        self.assertEqual(dc.window_sums([1, 2, 3, 4], 2), [3, 5, 7])
        self.assertEqual(dc.window_sums([1, 2], 3), [])

    def test_percentile_interpolates_like_percentile_cont(self):
        self.assertEqual(dc.percentile([10, 20, 30, 40], 50), 25)
        self.assertEqual(dc.percentile([10, 20, 30, 40], 100), 40)
        self.assertEqual(dc.percentile([7], 99), 7)

    def test_drain_minutes(self):
        # 10 min x 100 jobs, one worker at 60 s/job = 1 job/min: backlog 990, 990 min
        self.assertAlmostEqual(dc.drain_minutes([100] * 10 + [0] * 5, 1, 60.0), 990.0)
        self.assertEqual(dc.drain_minutes([0] * 5, 1, 60.0), 0.0)


class TestPlan(unittest.TestCase):

    def test_short_history_gives_none(self):
        self.assertIsNone(dc.plan_detail_ceiling([5] * 10, 7.0, **KW))

    def test_steady_load_ceiling_and_design_delay(self):
        # 10 jobs/min -> 300 per 30 min; L = 300 x 7 / (1800 x 0.8) = 1.46 -> 2
        plan = dc.plan_detail_ceiling([10] * 120, 7.0, **KW)
        self.assertEqual(plan["ceil"], 2)
        self.assertEqual(plan["eff_pct"], 99)
        self.assertAlmostEqual(plan["d_s"], 300 * 7.0 / 2)
        self.assertEqual(plan["alerts"], [])

    def test_ladder_steps_down_to_first_level_that_fits(self):
        series = [1] * 1000 + [100] * 30 + [1] * 1000
        roomy = dc.plan_detail_ceiling(series, 7.0, **KW)
        tight = dc.plan_detail_ceiling(series, 7.0, **{**KW, "total": 8, "cap": 8})
        self.assertEqual(roomy["eff_pct"], 99)
        self.assertGreater(roomy["ceil"], tight["ceil"])
        self.assertLessEqual(tight["ceil"], tight["c_avail"])
        self.assertLess(tight["eff_pct"], 99)
        # highest level that fits: the next level up would not
        sums = sorted(dc.window_sums(series, 30))
        need = lambda p: max(2, math.ceil(dc.percentile(sums, p) * 7.0 / (1800 * 0.8)))
        self.assertLessEqual(need(tight["eff_pct"]), tight["c_avail"])
        self.assertGreater(need(tight["eff_pct"] + 1), tight["c_avail"])

    def test_seat_total_is_raised_before_the_level_is_reduced(self):
        series = [1] * 1000 + [100] * 30 + [1] * 1000
        start_need = dc.plan_detail_ceiling(series, 7.0, **KW)["need_start"]
        low = {**KW, "total": 6, "cap": 40, "step": 2}
        plan = dc.plan_detail_ceiling(series, 7.0, **low)
        self.assertGreaterEqual(plan["total"] - low["others"], start_need)
        self.assertEqual(plan["eff_pct"], 99)
        self.assertEqual((plan["total"] - 6) % 2, 0)

    def test_nothing_fits_below_floor_is_critical_and_takes_what_is_left(self):
        series = [200] * 1000 + [1] * 100      # even p75 of the windows is huge
        plan = dc.plan_detail_ceiling(series, 7.0, **{**KW, "total": 7, "cap": 7})
        self.assertIsNone(plan["eff_pct"])
        self.assertEqual(plan["ceil"], 2)
        self.assertTrue(any(a.startswith("CRITICAL") for a in plan["alerts"]))

    def test_long_drain_and_design_delay_raise_alerts(self):
        series = [100] * 60 + [0] * 60
        plan = dc.plan_detail_ceiling(series, 7.0, **{**KW, "total": 7, "cap": 7})
        text = " ".join(plan["alerts"])
        self.assertIn("drain", text)
        self.assertIn("design delay", text)


class TestManagerNightly(unittest.TestCase):

    def _run(self, rows, window=None):
        import workers.manager as mgr
        r = MagicMock()
        r.get.return_value = json.dumps(window) if window else None
        conn = MagicMock()
        conn.__enter__.return_value = conn
        conn.cursor.return_value.fetchall.return_value = [
            {"minute": m, "arrivals": a} for m, a in rows]
        scan_full = {"total": 20, "pools": {"scan": {"ceil": 6}, "fullscan": {"ceil": 4}}}
        with patch("db.db.get_conn", return_value=conn), \
             patch.object(mgr, "_plan_ceilings", return_value=scan_full), \
             patch.object(mgr, "_store_plan") as store, \
             patch.object(mgr, "_get_db_total", return_value=20), \
             patch.object(mgr, "_seat_cap", return_value=20), \
             patch.object(mgr, "_raise_step_seats", return_value=2):
            self.store = store
            plan = mgr._plan_detail_nightly(r, {"detail": {"fetch_p75": 2.0}})
        return plan, r

    def test_under_min_hours_is_bootstrap_and_stores_nothing(self):
        t = datetime(2026, 11, 1, 0, 0)
        plan, r = self._run([(t + timedelta(minutes=i), 5) for i in range(600)])
        self.assertIsNone(plan)
        r.set.assert_not_called()

    def test_enough_history_stores_plan_using_window_s(self):
        t = datetime(2026, 11, 1, 0, 0)
        rows = [(t + timedelta(minutes=i), 10) for i in range(25 * 60)]
        plan, r = self._run(rows, window={"window": [[70.0, 10]], "carry": 0})
        self.assertEqual(plan["s"], 7.0)
        self.assertEqual(plan["s_src"], "window")
        self.assertEqual(plan["ceil"], 2)
        self.assertEqual(plan["others"], 10)
        self.assertEqual(r.set.call_args_list[0].args[0], "manager:detail_plan")
        # applied: scan/fullscan stored, then the detail ceiling
        self.store.assert_called_once()
        r.set.assert_any_call("manager:worker_ceil:detail", 2)

    def test_cold_s_falls_back_to_fetch_p75(self):
        t = datetime(2026, 11, 1, 0, 0)
        rows = [(t + timedelta(minutes=i), 10) for i in range(25 * 60)]
        plan, _ = self._run(rows)
        self.assertEqual((plan["s"], plan["s_src"]), (2.0, "fetch_p75"))


if __name__ == "__main__":
    unittest.main()
