"""
tests/test_paginator.py
─────────────────────────────────────────────────────────────────────────────
Tests for workers/paginator.py — the adaptive early-exit gate.

Coverage map
────────────
  TestEarlyExitGate
    · page of all-seen jobs adds 1; stops at PAGINATOR_CONFIRM_PAGES (2)
    · any unseen job resets the counter to 0 (strict rule, no percentage)
    · 1 unseen job in an otherwise seen page still resets
    · seen, new, seen, seen → stops only after the two clean pages that follow the new one
    · empty page / page without ids leaves the counter alone and never stops
    · once stopped the gate keeps answering True
    · fresh() gives an independent zeroed gate with the same seen-check
    · custom confirm_pages and id_key respected
    · PAGINATOR_CONFIRM_PAGES is 2

  TestMakeSeenCheck
    · seen when any check says so; None checks ignored; no checks → never seen

  TestEstimateScanDepth
    · waste_ratio, keys, rounding, zero fetched
"""

import sys
import os
import unittest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from config import PAGINATOR_CONFIRM_PAGES
from workers.paginator import EarlyExitGate, estimate_scan_depth, make_seen_check


def _jobs(ids):
    return [{"job_id": str(i)} for i in ids]


def _gate(seen_ids, **kw):
    seen = {str(i) for i in seen_ids}
    return EarlyExitGate(seen.__contains__, **kw)


class TestEarlyExitGate(unittest.TestCase):

    def test_confirm_pages_is_2(self):
        self.assertEqual(PAGINATOR_CONFIRM_PAGES, 2)

    def test_one_clean_page_does_not_stop(self):
        g = _gate([1, 2, 3])
        self.assertFalse(g(_jobs([1, 2, 3])))
        self.assertEqual(g.streak, 1)

    def test_two_clean_pages_stop(self):
        g = _gate([1, 2, 3, 4])
        self.assertFalse(g(_jobs([1, 2])))
        self.assertTrue(g(_jobs([3, 4])))
        self.assertTrue(g.stopped)

    def test_new_job_keeps_going_and_resets(self):
        g = _gate([1, 2])
        self.assertFalse(g(_jobs([9, 10])))
        self.assertEqual(g.streak, 0)

    def test_single_new_job_in_seen_page_resets(self):
        """No percentage: 9 of 10 seen is NOT a clean page."""
        g = _gate(range(9))
        g(_jobs(range(9)))
        self.assertEqual(g.streak, 1)
        self.assertFalse(g(_jobs(list(range(9)) + [99])))
        self.assertEqual(g.streak, 0)

    def test_clean_new_clean_clean_stops_on_fourth_page(self):
        """The case the user flagged: a new job between clean pages restarts the count."""
        g = _gate([1, 2, 3, 4, 5, 6])
        self.assertFalse(g(_jobs([1, 2])))        # streak 1
        self.assertFalse(g(_jobs([99])))          # new → 0
        self.assertFalse(g(_jobs([3, 4])))        # streak 1
        self.assertTrue(g(_jobs([5, 6])))         # streak 2 → stop
        self.assertEqual(g.pages, 4)

    def test_empty_page_never_stops_and_keeps_counter(self):
        g = _gate([1])
        g(_jobs([1]))
        self.assertFalse(g([]))
        self.assertFalse(g(None))
        self.assertEqual(g.streak, 1)
        self.assertEqual(g.pages, 1)

    def test_page_without_ids_is_not_judged(self):
        g = _gate([1])
        self.assertFalse(g([{"title": "x"}, {"job_id": ""}]))
        self.assertEqual(g.pages, 0)

    def test_stays_stopped(self):
        g = _gate([1, 2])
        g(_jobs([1]))
        g(_jobs([2]))
        self.assertTrue(g(_jobs([77])))   # a late page is still told to stop; a feeder has already quit

    def test_fresh_gate_is_independent(self):
        g = _gate([1, 2])
        g(_jobs([1]))
        f = g.fresh()
        self.assertEqual(f.streak, 0)
        self.assertFalse(f(_jobs([2])))
        self.assertEqual(g.streak, 1)

    def test_custom_confirm_pages(self):
        g = _gate([1, 2, 3], confirm_pages=3)
        self.assertFalse(g(_jobs([1])))
        self.assertFalse(g(_jobs([2])))
        self.assertTrue(g(_jobs([3])))

    def test_custom_id_key(self):
        g = EarlyExitGate({"a", "b"}.__contains__, confirm_pages=1, id_key="jid")
        self.assertTrue(g([{"jid": "a"}, {"jid": "b"}]))


class TestMakeSeenCheck(unittest.TestCase):

    def test_any_check_true_means_seen(self):
        s = make_seen_check({"a"}.__contains__, {"b"}.__contains__)
        self.assertTrue(s("a"))
        self.assertTrue(s("b"))
        self.assertFalse(s("c"))

    def test_none_checks_ignored(self):
        s = make_seen_check(None, {"a"}.__contains__)
        self.assertTrue(s("a"))

    def test_no_checks_never_seen(self):
        self.assertFalse(make_seen_check()("a"))


class TestAcceptsStopCheck(unittest.TestCase):

    def test_detects_declared_parameter(self):
        from types import SimpleNamespace
        from jobs.ats.registry import accepts_stop_check
        with_cb = SimpleNamespace(fetch_jobs=lambda slug_info, company, stop_check=None: [])
        without = SimpleNamespace(fetch_jobs=lambda slug_info, company: [])
        self.assertTrue(accepts_stop_check(with_cb))
        self.assertFalse(accepts_stop_check(without))
        self.assertFalse(accepts_stop_check(object()))


class TestEstimateScanDepth(unittest.TestCase):

    def test_all_jobs_new_zero_waste(self):
        self.assertEqual(estimate_scan_depth(100, 100, False)["waste_ratio"], 0.0)

    def test_no_new_jobs_full_waste(self):
        self.assertEqual(estimate_scan_depth(100, 0, False)["waste_ratio"], 1.0)

    def test_zero_fetched_no_division_error(self):
        self.assertEqual(estimate_scan_depth(0, 0, False)["waste_ratio"], 0.0)

    def test_keys_and_flag(self):
        r = estimate_scan_depth(50, 10, True)
        for key in ("total_fetched", "new_found", "wasted", "waste_ratio", "early_exit"):
            self.assertIn(key, r)
        self.assertTrue(r["early_exit"])
        self.assertEqual(r["wasted"], 40)

    def test_rounded_to_3_places(self):
        self.assertEqual(estimate_scan_depth(3, 2, False)["waste_ratio"], round(1 / 3, 3))


if __name__ == "__main__":
    unittest.main(verbosity=2)
