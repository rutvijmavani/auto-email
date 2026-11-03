"""
Step 8 of docs/detail-pool-ceiling-th-design.md: bucket parameters, held-backlog
shortfall signal, derived D, reclaim of lent seats, borrower-only borrow return.
"""

import json
import os
import sys
import time
import unittest
from unittest.mock import MagicMock, patch

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import workers.manager as mgr
from workers.detail_drainer import BUCKET_PARAMS_KEY, HELD_KEY


def _redis(data=None):
    store = dict(data or {})
    r = MagicMock()
    r.get.side_effect = lambda k: store.get(k)
    r.set.side_effect = lambda k, v, **kw: store.__setitem__(k, str(v))
    r.delete.side_effect = lambda *ks: [store.pop(k, None) for k in ks]
    r._store = store
    return r


class TestNBucket(unittest.TestCase):
    """Base ceiling 7, 3 borrowed: target 10 -> 7, 8 -> 9, <= 7 -> 10."""

    def _n(self, target):
        r = _redis({"manager:worker_ceil:detail": "7", "manager:borrow:scan:detail": "3"})
        with patch.object(mgr, "_get_base_ceil", return_value=7):
            return mgr._detail_n_bucket(r, target)

    def test_widening_follows_unmet_demand(self):
        self.assertEqual(self._n(10), 7)
        self.assertEqual(self._n(8), 9)
        self.assertEqual(self._n(5), 10)


class TestDesignDelay(unittest.TestCase):

    def test_none_without_plan(self):
        self.assertIsNone(mgr._detail_design_delay(_redis()))

    def test_cycle_params_replaces_detail_warn_only(self):
        r = _redis({mgr._DETAIL_PLAN_KEY: json.dumps({"d_s": 840.0})})
        sp = {"detail": {"fetch_p75": 2.0, "delay_warn_s": 300}, "scan": {"delay_warn_s": 60}}
        out = mgr._cycle_params(r, sp)
        self.assertEqual(out["detail"]["delay_warn_s"], 840.0)
        self.assertEqual(out["detail"]["fetch_p75"], 2.0)
        self.assertEqual(out["scan"]["delay_warn_s"], 60)
        self.assertEqual(sp["detail"]["delay_warn_s"], 300)       # input untouched

    def test_bootstrap_keeps_params(self):
        sp = {"detail": {"fetch_p75": 2.0, "delay_warn_s": 300}}
        self.assertIs(mgr._cycle_params(_redis(), sp), sp)


class TestPublishBucket(unittest.TestCase):

    def test_bootstrap_values(self):
        r = _redis()
        with patch.object(mgr, "_get_base_ceil", return_value=7), \
             patch.object(mgr, "_stored_service_time", return_value=None):
            mgr._needed_workers["detail"] = 9
            mgr._publish_detail_bucket(r, {"detail": {"fetch_p75": 1.5}})
        got = json.loads(r._store[BUCKET_PARAMS_KEY])
        self.assertEqual(got["n"], 7)
        self.assertEqual(got["s"], 1.5)
        self.assertGreater(got["d"], 0)


class TestHeld(unittest.TestCase):

    def test_growing_needs_three_rises_and_net_growth(self):
        self.assertTrue(mgr._is_held_growing([1, 2, 3, 4, 5]))
        self.assertTrue(mgr._is_held_growing([1, 2, 2, 3, 4]))
        self.assertFalse(mgr._is_held_growing([1, 2, 2, 2, 3]))
        self.assertFalse(mgr._is_held_growing([5, 6, 7, 8, 4]))
        self.assertFalse(mgr._is_held_growing([1, 2, 3, 4]))      # too short

    def test_read_held_fresh_stale_missing(self):
        fresh = json.dumps({"count": 4, "oldest_age_s": 90.0, "ts": time.time()})
        stale = json.dumps({"count": 4, "oldest_age_s": 90.0,
                            "ts": time.time() - 10 * mgr.MANAGER_CYCLE_S})
        self.assertEqual(mgr._read_held(_redis({HELD_KEY: fresh})), (4, 90.0))
        self.assertEqual(mgr._read_held(_redis({HELD_KEY: stale})), (0, 0.0))
        self.assertEqual(mgr._read_held(_redis()), (0, 0.0))


class TestReclaim(unittest.TestCase):

    def _r(self):
        return _redis({"manager:borrow:detail:scan": "3"})

    def test_idle_seats_return_at_once_without_shrinking(self):
        r = self._r()
        with patch.object(mgr, "_get_worker_ceil", return_value=8), \
             patch.object(mgr, "_send_cmd") as send:
            # scan ceiling 8, running 6 -> 2 idle of the 3 lent
            self.assertTrue(mgr._reclaim_lent_seats(r, "detail", {"scan": 6}))
        self.assertEqual(r._store["manager:borrow:detail:scan"], "1")
        send.assert_not_called()

    def test_seats_in_use_return_one_per_cycle_and_borrower_shrinks(self):
        r = self._r()
        ceils = iter([8, 7])          # before and after the one-seat reclaim
        with patch.object(mgr, "_get_worker_ceil", side_effect=lambda *_: next(ceils)), \
             patch.object(mgr, "_send_cmd") as send:
            self.assertTrue(mgr._reclaim_lent_seats(r, "detail", {"scan": 8}))
        self.assertEqual(r._store["manager:borrow:detail:scan"], "2")
        send.assert_called_once_with(r, "scan:target:7")

    def test_nothing_lent_nothing_reclaimed(self):
        self.assertFalse(mgr._reclaim_lent_seats(_redis(), "detail", {}))


class TestReturnOnBorrowerCalm(unittest.TestCase):

    def test_returns_when_borrower_calm_even_if_lender_is_not(self):
        r = _redis({"manager:borrow:detail:scan": "2"})
        mgr._borrow_stable_cycles.clear()
        delays = {"detail": 999.0, "scan": 0.0, "fullscan": 0.0}
        warns = {"detail": 100.0, "scan": 100.0, "fullscan": 100.0}
        for _ in range(mgr.LEVER1_STABLE_REQUIRED):
            mgr._maybe_return_borrows(r, delays, warns)
        self.assertNotIn("manager:borrow:detail:scan", r._store)


if __name__ == "__main__":
    unittest.main()
