"""
tests/test_paginator.py
─────────────────────────────────────────────────────────────────────────────
Comprehensive tests for workers/paginator.py — smart early-exit pagination.

Coverage map
────────────
  TestShouldContinuePaginatingSorted   (sorted_by_recency=True)
    · Empty page → (False, unchanged_overlap_pages)
    · Page with no identifiable job IDs → (True, 0)
    · 0% overlap → continue, reset overlap_pages to 0
    · < 80% overlap → continue, reset overlap_pages to 0
    · Exactly 80% overlap on first page → continue (need 2 confirm pages)
    · > 80% overlap on first page → continue (need 2 confirm pages)
    · 80% on first page + 80% on second → stop (2 consecutive)
    · 100% overlap single page → continue (need 2 confirm pages for sorted)
    · 100% on two consecutive pages → stop
    · First high-overlap page advances overlap_pages counter by 1
    · New jobs on intermediate page resets overlap_pages counter to 0
    · PAGINATOR_OVERLAP_THRESHOLD used correctly (0.80)
    · PAGINATOR_CONFIRM_PAGES used correctly (2)
    · overlap_pages > 0 from previous call is carried correctly
    · Custom id_key respected

  TestShouldContinuePaginatingUnsorted (sorted_by_recency=False)
    · Empty page → (False, overlap_pages)
    · Page with no IDs → (True, 0)
    · 0% overlap → (True, 0)
    · < 100% overlap → (True, 0) — continue
    · Exactly 100% overlap single page → (False, overlap+1) — stop
    · Partial overlap (99%) → continue
    · Full page of unknowns (0%) → continue
    · overlap_pages resets to 0 when new jobs found

  TestWouldHaveExitedAt
    · Empty all_pages → returns 0 immediately
    · Single page, no overlap → returns 1 (never exited)
    · Would exit at page 1 (second page, idx=1) for sorted with 2 high-overlap pages
    · Would exit at page 0 for unsorted with 100% overlap first page
    · Returns len(all_pages) when exit never triggered
    · Works with multiple platforms (sorted vs unsorted)

  TestEstimateScanDepth
    · waste_ratio = (fetched - new) / max(fetched, 1)
    · All jobs new → waste_ratio = 0.0
    · No new jobs → waste_ratio = 1.0
    · Partial → correct fraction
    · early_exit flag preserved in output
    · Returns dict with required keys
    · Zero fetched → no division by zero (waste_ratio uses max(fetched,1))
    · Returns waste_ratio rounded to 3 decimal places
    · total_fetched, new_found, wasted keys correct
"""

import sys
import os
import unittest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from config import PAGINATOR_OVERLAP_THRESHOLD, PAGINATOR_CONFIRM_PAGES
from workers.paginator import (
    should_continue_paginating,
    would_have_exited_at,
    estimate_scan_depth,
)


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _make_jobs(ids):
    """Create minimal job dicts with job_id fields."""
    return [{"job_id": str(jid)} for jid in ids]


def _known(*ids):
    """Create a set of known job IDs (as strings)."""
    return {str(jid) for jid in ids}


# ─────────────────────────────────────────────────────────────────────────────
# TestShouldContinuePaginatingSorted
# ─────────────────────────────────────────────────────────────────────────────

class TestShouldContinuePaginatingSorted(unittest.TestCase):
    """Tests for should_continue_paginating() with sorted_by_recency=True."""

    def _run(self, page_ids, seen_ids, overlap_pages=0):
        jobs = _make_jobs(page_ids)
        seen = _known(*seen_ids)
        return should_continue_paginating(jobs, seen, overlap_pages,
                                          sorted_by_recency=True)

    # ── Empty / no-ID pages ───────────────────────────────────────────────────

    def test_empty_page_stops(self):
        """Empty page → stop (False)."""
        cont, pages = should_continue_paginating([], set(), 0, sorted_by_recency=True)
        self.assertFalse(cont)

    def test_empty_page_preserves_overlap_counter(self):
        """Empty page does not reset overlap_pages."""
        cont, pages = should_continue_paginating([], set(), 2, sorted_by_recency=True)
        self.assertEqual(pages, 2)

    def test_page_with_no_identifiable_ids_continues(self):
        """Page where all jobs lack job_id → continue, overlap=0."""
        jobs = [{"title": "SWE"}, {"title": "PM"}]
        cont, pages = should_continue_paginating(jobs, set(), 0, sorted_by_recency=True)
        self.assertTrue(cont)

    # ── 0% overlap (all new) ──────────────────────────────────────────────────

    def test_zero_overlap_continues(self):
        """0% seen → continue, reset overlap_pages to 0."""
        cont, pages = self._run([1, 2, 3, 4, 5], [], overlap_pages=0)
        self.assertTrue(cont)
        self.assertEqual(pages, 0)

    def test_zero_overlap_resets_carry_forward_counter(self):
        """Previous overlap_pages=1 reset to 0 when new jobs found."""
        cont, pages = self._run([10, 11, 12], [], overlap_pages=1)
        self.assertTrue(cont)
        self.assertEqual(pages, 0)

    # ── Sub-threshold overlap ─────────────────────────────────────────────────

    def test_below_80pct_overlap_continues(self):
        """79% overlap → continue, reset."""
        # 8 seen out of 10 total = 80% → not below. Use 7/10 = 70%
        cont, pages = self._run([1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
                                [1, 2, 3, 4, 5, 6, 7],  # 7/10 = 70%
                                overlap_pages=0)
        self.assertTrue(cont)
        self.assertEqual(pages, 0)

    # ── First high-overlap page (threshold met but < CONFIRM_PAGES) ───────────

    def test_exactly_80pct_overlap_first_page_continues(self):
        """80% overlap first page → continue (need 2 confirm pages)."""
        cont, pages = self._run([1, 2, 3, 4, 5],
                                [1, 2, 3, 4],  # 4/5 = 80%
                                overlap_pages=0)
        self.assertTrue(cont)
        self.assertEqual(pages, 1)

    def test_100pct_overlap_first_page_continues_for_sorted(self):
        """100% overlap on first page → continue (SORTED needs 2 confirm pages)."""
        cont, pages = self._run([1, 2, 3, 4, 5],
                                [1, 2, 3, 4, 5],
                                overlap_pages=0)
        self.assertTrue(cont)
        self.assertEqual(pages, 1)

    def test_first_high_overlap_increments_counter(self):
        """First ≥80% overlap page: overlap_pages goes from 0 → 1."""
        _, pages = self._run([1, 2, 3, 4, 5],
                             [1, 2, 3, 4],  # 80%
                             overlap_pages=0)
        self.assertEqual(pages, 1)

    # ── Two consecutive high-overlap pages → stop ─────────────────────────────

    def test_two_consecutive_high_overlap_stops(self):
        """2nd consecutive ≥80% overlap page → stop."""
        cont, pages = self._run([1, 2, 3, 4, 5],
                                [1, 2, 3, 4],  # 80%
                                overlap_pages=1)  # carry forward 1 from prev page
        self.assertFalse(cont)
        self.assertEqual(pages, 2)

    def test_two_consecutive_100pct_stops(self):
        """2nd consecutive 100% overlap page → stop."""
        cont, pages = self._run([1, 2, 3],
                                [1, 2, 3],  # 100%
                                overlap_pages=1)
        self.assertFalse(cont)

    # ── Reset counter when new jobs found ─────────────────────────────────────

    def test_new_jobs_after_overlap_resets_counter(self):
        """After a high-overlap page, new jobs on next page resets counter."""
        cont, pages = self._run([1, 2, 3, 100],  # job 100 is new
                                [1, 2, 3],       # 3/4 = 75% → below threshold
                                overlap_pages=1)
        self.assertTrue(cont)
        self.assertEqual(pages, 0)  # counter reset

    def test_overlap_counter_not_accumulated_after_reset(self):
        """After reset, need 2 fresh consecutive pages again to stop."""
        # Page with new job → reset to 0
        _, pages = self._run([1, 2, 3, 100], [1, 2, 3], overlap_pages=1)
        self.assertEqual(pages, 0)
        # Now need 2 more high-overlap pages
        cont, pages2 = self._run([1, 2, 3, 4, 5], [1, 2, 3, 4], overlap_pages=pages)
        self.assertTrue(cont)
        self.assertEqual(pages2, 1)

    # ── Custom id_key ─────────────────────────────────────────────────────────

    def test_custom_id_key(self):
        """Custom id_key parameter respected."""
        jobs = [{"jid": "a"}, {"jid": "b"}, {"jid": "c"}]
        seen = {"a", "b", "c"}
        cont, pages = should_continue_paginating(
            jobs, seen, 1, sorted_by_recency=True, id_key="jid"
        )
        self.assertFalse(cont)  # 100% overlap with carry=1 → stop

    # ── PAGINATOR constants ───────────────────────────────────────────────────

    def test_overlap_threshold_is_80pct(self):
        """PAGINATOR_OVERLAP_THRESHOLD = 0.80."""
        self.assertAlmostEqual(PAGINATOR_OVERLAP_THRESHOLD, 0.80, places=3)

    def test_confirm_pages_is_2(self):
        """PAGINATOR_CONFIRM_PAGES = 2."""
        self.assertEqual(PAGINATOR_CONFIRM_PAGES, 2)


# ─────────────────────────────────────────────────────────────────────────────
# TestShouldContinuePaginatingUnsorted
# ─────────────────────────────────────────────────────────────────────────────

class TestShouldContinuePaginatingUnsorted(unittest.TestCase):
    """Tests for should_continue_paginating() with sorted_by_recency=False."""

    def _run(self, page_ids, seen_ids, overlap_pages=0):
        jobs = _make_jobs(page_ids)
        seen = _known(*seen_ids)
        return should_continue_paginating(jobs, seen, overlap_pages,
                                          sorted_by_recency=False)

    def test_empty_page_stops(self):
        """Empty page → stop."""
        cont, _ = should_continue_paginating([], set(), 0, sorted_by_recency=False)
        self.assertFalse(cont)

    def test_page_with_no_ids_continues(self):
        """Page with no identifiable IDs → continue."""
        jobs = [{"title": "Eng"}]
        cont, _ = should_continue_paginating(jobs, set(), 0, sorted_by_recency=False)
        self.assertTrue(cont)

    def test_zero_overlap_continues(self):
        """0% overlap → continue."""
        cont, pages = self._run([1, 2, 3], [], overlap_pages=0)
        self.assertTrue(cont)
        self.assertEqual(pages, 0)

    def test_partial_overlap_continues(self):
        """50% overlap → continue (unsorted needs 100% to stop)."""
        cont, pages = self._run([1, 2, 3, 4], [1, 2], overlap_pages=0)
        self.assertTrue(cont)
        self.assertEqual(pages, 0)

    def test_99pct_overlap_continues(self):
        """99% overlap → continue (needs exactly 100%)."""
        # 9/10 seen = 90%... make it 99%: 99 seen out of 100
        page_ids = list(range(100))
        seen_ids = list(range(99))
        cont, pages = self._run(page_ids, seen_ids)
        self.assertTrue(cont)
        self.assertEqual(pages, 0)

    def test_100pct_overlap_stops(self):
        """100% overlap single page → stop."""
        cont, pages = self._run([1, 2, 3, 4, 5], [1, 2, 3, 4, 5])
        self.assertFalse(cont)

    def test_100pct_overlap_increments_overlap_pages(self):
        """100% overlap: overlap_pages incremented by 1."""
        _, pages = self._run([1, 2, 3], [1, 2, 3], overlap_pages=0)
        self.assertEqual(pages, 1)

    def test_partial_overlap_resets_counter(self):
        """Partial overlap resets counter to 0."""
        cont, pages = self._run([1, 2, 3, 100], [1, 2, 3], overlap_pages=1)
        self.assertTrue(cont)
        self.assertEqual(pages, 0)

    def test_single_new_job_prevents_stop(self):
        """Even a single new job prevents the 100% stop."""
        cont, pages = self._run([1, 2, 3, 999], [1, 2, 3])
        # 3/4 = 75% overlap → continue
        self.assertTrue(cont)


# ─────────────────────────────────────────────────────────────────────────────
# TestWouldHaveExitedAt
# ─────────────────────────────────────────────────────────────────────────────

class TestWouldHaveExitedAt(unittest.TestCase):

    def test_empty_pages_returns_zero(self):
        """No pages → exits at index 0."""
        result = would_have_exited_at([], set(), sorted_by_recency=True)
        self.assertEqual(result, 0)

    def test_single_page_no_overlap_sorted_returns_1(self):
        """One page, no overlap → exit never triggered → returns len=1."""
        pages = [_make_jobs([1, 2, 3])]
        result = would_have_exited_at(pages, set(), sorted_by_recency=True)
        self.assertEqual(result, 1)

    def test_would_exit_at_page_1_for_two_high_overlap_sorted(self):
        """
        Sorted: 2 consecutive ≥80% overlap pages → exit at page 1 (idx=1).
        Page 0: 80% overlap → overlap_pages=1 (continue)
        Page 1: 80% overlap → overlap_pages=2 ≥ CONFIRM_PAGES → stop → exit at 1
        """
        seen = _known(1, 2, 3, 4)  # 4 known out of 5 per page = 80%
        pages = [
            _make_jobs([1, 2, 3, 4, 5]),   # page 0: 4/5 = 80%
            _make_jobs([1, 2, 3, 4, 6]),   # page 1: 4/5 = 80% (carry=1) → STOP
            _make_jobs([7, 8, 9, 10, 11]), # page 2: would not be reached
        ]
        result = would_have_exited_at(pages, seen, sorted_by_recency=True)
        self.assertEqual(result, 1)

    def test_would_exit_at_page_0_unsorted_100pct(self):
        """Unsorted: 100% overlap on first page → exit at page 0."""
        seen = _known(1, 2, 3, 4, 5)
        pages = [
            _make_jobs([1, 2, 3, 4, 5]),   # 100% overlap → stop at page 0
            _make_jobs([6, 7, 8, 9, 10]),
        ]
        result = would_have_exited_at(pages, seen, sorted_by_recency=False)
        self.assertEqual(result, 0)

    def test_never_exits_returns_len_pages(self):
        """No early exit triggered → returns len(all_pages)."""
        pages = [
            _make_jobs([1, 2, 3]),
            _make_jobs([4, 5, 6]),
            _make_jobs([7, 8, 9]),
        ]
        result = would_have_exited_at(pages, set(), sorted_by_recency=True)
        self.assertEqual(result, 3)

    def test_new_jobs_between_overlap_pages_resets(self):
        """
        Sorted: overlap page, then new jobs, then overlap page again.
        Should NOT exit — counter resets after new jobs.
        """
        seen = _known(1, 2, 3, 4)
        pages = [
            _make_jobs([1, 2, 3, 4, 5]),    # 4/5=80% → overlap_pages=1, continue
            _make_jobs([1, 2, 3, 100, 101]), # 3/5=60% → reset, continue
            _make_jobs([1, 2, 3, 4, 5]),     # 4/5=80% → overlap_pages=1, continue
        ]
        result = would_have_exited_at(pages, seen, sorted_by_recency=True)
        # Exit never triggered in 3 pages → return 3
        self.assertEqual(result, 3)


# ─────────────────────────────────────────────────────────────────────────────
# TestEstimateScanDepth
# ─────────────────────────────────────────────────────────────────────────────

class TestEstimateScanDepth(unittest.TestCase):

    def test_all_jobs_new_zero_waste(self):
        """All jobs new → waste_ratio = 0.0."""
        result = estimate_scan_depth(total_fetched=100, new_found=100, early_exit=False)
        self.assertEqual(result["waste_ratio"], 0.0)

    def test_no_new_jobs_full_waste(self):
        """No new jobs → waste_ratio = 1.0."""
        result = estimate_scan_depth(total_fetched=100, new_found=0, early_exit=False)
        self.assertEqual(result["waste_ratio"], 1.0)

    def test_partial_waste_correct_fraction(self):
        """50% new → waste_ratio = 0.5."""
        result = estimate_scan_depth(total_fetched=100, new_found=50, early_exit=False)
        self.assertAlmostEqual(result["waste_ratio"], 0.5, places=3)

    def test_zero_fetched_no_division_error(self):
        """Zero fetched → no ZeroDivisionError (uses max(fetched, 1))."""
        try:
            result = estimate_scan_depth(total_fetched=0, new_found=0, early_exit=False)
            self.assertEqual(result["waste_ratio"], 0.0)
        except ZeroDivisionError:
            self.fail("ZeroDivisionError with total_fetched=0")

    def test_returns_dict_with_required_keys(self):
        """Result has all required keys."""
        result = estimate_scan_depth(total_fetched=50, new_found=10, early_exit=True)
        for key in ("total_fetched", "new_found", "wasted", "waste_ratio", "early_exit"):
            self.assertIn(key, result, f"Missing key: {key}")

    def test_early_exit_flag_preserved(self):
        """early_exit flag is passed through unchanged."""
        for flag in (True, False):
            result = estimate_scan_depth(100, 50, flag)
            self.assertEqual(result["early_exit"], flag)

    def test_wasted_count_correct(self):
        """wasted = total_fetched - new_found."""
        result = estimate_scan_depth(total_fetched=200, new_found=30, early_exit=False)
        self.assertEqual(result["wasted"], 170)

    def test_total_fetched_and_new_found_preserved(self):
        """total_fetched and new_found preserved in output."""
        result = estimate_scan_depth(total_fetched=77, new_found=23, early_exit=False)
        self.assertEqual(result["total_fetched"], 77)
        self.assertEqual(result["new_found"], 23)

    def test_waste_ratio_rounded_to_3_decimal_places(self):
        """waste_ratio is rounded to 3 decimal places."""
        # 1/3 = 0.333...
        result = estimate_scan_depth(total_fetched=3, new_found=2, early_exit=False)
        # waste = 1, waste_ratio = 1/3 = 0.333
        self.assertEqual(result["waste_ratio"], round(1/3, 3))

    def test_high_efficiency_low_waste(self):
        """9 new out of 10 → waste_ratio = 0.1."""
        result = estimate_scan_depth(total_fetched=10, new_found=9, early_exit=False)
        self.assertAlmostEqual(result["waste_ratio"], 0.1, places=3)

    def test_waste_ratio_never_exceeds_1(self):
        """waste_ratio is always in [0, 1]."""
        for fetched, new in [(0, 0), (100, 0), (100, 100), (100, 50)]:
            result = estimate_scan_depth(fetched, new, False)
            self.assertGreaterEqual(result["waste_ratio"], 0.0)
            self.assertLessEqual(result["waste_ratio"], 1.0)


if __name__ == "__main__":
    unittest.main(verbosity=2)
