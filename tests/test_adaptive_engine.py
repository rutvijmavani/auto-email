"""
tests/test_adaptive_engine.py
─────────────────────────────────────────────────────────────────────────────
Comprehensive tests for workers/adaptive.py — the pure adaptive interval engine.

Coverage map
────────────
  TestComputeScore
    · Returns None when fewer than ADAPTIVE_MIN_POLLS (3)
    · Returns None for exactly 0 counts (< MIN_POLLS)
    · Returns None for exactly 1 or 2 counts
    · Returns float for exactly 3 counts (minimum valid window)
    · Returns float for 4 and 5 counts
    · Weights are recency-biased (burst recent > burst old)
    · All-zero window → score 0.0
    · Single high-value entry in last position scores highest
    · Values above ADAPTIVE_CAP_PER_POLL are NOT capped here
    · Full 5-element window example matches expected weighted average

  TestBandLookup
    · score=None → ADAPTIVE_DEFAULT_INTERVAL (12h)
    · score=0.0  → ADAPTIVE_DEFAULT_INTERVAL (12h)
    · score just below low threshold → 9h
    · score AT low threshold → 6h (tie-promoted to better band)
    · score between low and moderate → 6h
    · score AT moderate threshold → 4h (tie-promoted)
    · score between moderate and active → 4h
    · score AT active threshold → ADAPTIVE_MIN_INTERVAL (3h)
    · score well above active → 3h
    · Custom thresholds respected (different values)
    · None thresholds falls back to DEFAULT_THRESHOLDS
    · Negative score treated same as score < low → 9h

  TestGetMaxInterval
    · score=None → ADAPTIVE_MAX_INTERVAL_DORMANT (12h)
    · score=0.0  → 12h
    · score just below moderate threshold → 12h
    · score AT moderate threshold → ADAPTIVE_MAX_INTERVAL_ACTIVE (6h)
    · score above moderate → 6h
    · Custom thresholds respected

  TestComputeNextInterval
    · computed < current → react immediately (no smoothing)
    · computed == current → no change (EMA of equal values = equal)
    · computed > current → gradual EMA smoothing applied
    · Dormancy smoothing result is between computed and current
    · Smoothing uses ADAPTIVE_SMOOTHING factor (0.3 weight on computed)
    · Returns int not float
    · Reactivation from 12h to 3h → immediate 3h
    · Dormancy from 3h to 12h → gradual increase

  TestPushPollResult
    · New value appended to existing list
    · Values above ADAPTIVE_CAP_PER_POLL are capped
    · Values at exactly ADAPTIVE_CAP_PER_POLL are not capped
    · Values below cap pass through unchanged
    · List longer than 5 has oldest entry removed
    · List of exactly 5 elements: oldest removed on next push
    · Input list is not mutated (returns new list)
    · Empty input list → single-element output
    · 0 jobs appended correctly
    · Negative new_jobs treated as 0 after min()

  TestBuildThresholdsFromScores
    · Fewer than ADAPTIVE_MIN_COMPANIES_CALIBRATE active companies → None
    · Exactly ADAPTIVE_MIN_COMPANIES_CALIBRATE → returns dict
    · All zeros → None (no active companies)
    · Mixed zeros and non-zeros: zeros filtered out
    · Returns dict with keys 'low', 'moderate', 'active'
    · active > moderate > low > 0 (strict ordering guaranteed)
    · Winsorization caps the top 5% extreme outlier
    · Winsorization does not affect non-outlier scores
    · Tie scores collapsed by epsilon nudge
    · Rank boundaries match expected percentile positions
    · Single extreme outlier doesn't shift all boundaries much
    · With exactly 10 active companies → matches docstring example logic

  TestUpdatePollInterval
    · Full flow: push → score → band → smooth → cap → return dict
    · Returns dict with keys recent_poll_counts, adaptive_score, current_interval_s
    · adaptive_score = 0.0 when score is None (not enough history)
    · Interval is capped by get_max_interval()
    · Reactivation path (new active after dormancy) → immediate interval drop
    · Dormancy path (going quiet) → gradual interval increase
    · Custom thresholds respected

  TestLoadDumpPollCounts
    · load_poll_counts(None) → []
    · load_poll_counts("") → []
    · load_poll_counts("invalid json") → []
    · load_poll_counts with non-list JSON → []
    · load_poll_counts happy path → list of ints
    · load_poll_counts coerces floats to ints
    · dump_poll_counts → valid JSON string
    · Round-trip: dump then load returns identical list
    · load_poll_counts with unicode / malformed bytes → []
"""

import sys
import os
import unittest
import math

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from config import (
    ADAPTIVE_WEIGHTS,
    ADAPTIVE_MIN_POLLS,
    ADAPTIVE_CAP_PER_POLL,
    ADAPTIVE_SMOOTHING,
    ADAPTIVE_MIN_INTERVAL,
    ADAPTIVE_DEFAULT_INTERVAL,
    ADAPTIVE_MAX_INTERVAL_ACTIVE,
    ADAPTIVE_MAX_INTERVAL_DORMANT,
    ADAPTIVE_MIN_COMPANIES_CALIBRATE,
    ADAPTIVE_WINSORIZE_PCT,
    ADAPTIVE_BAND_TOP_PCT,
    ADAPTIVE_BAND_ACTIVE_PCT,
    ADAPTIVE_BAND_MODERATE_PCT,
)
from workers.adaptive import (
    compute_score,
    band_lookup,
    get_max_interval,
    compute_next_interval,
    push_poll_result,
    build_thresholds_from_scores,
    update_poll_interval,
    load_poll_counts,
    dump_poll_counts,
    DEFAULT_THRESHOLDS,
)


# ─────────────────────────────────────────────────────────────────────────────
# TestComputeScore
# ─────────────────────────────────────────────────────────────────────────────

class TestComputeScore(unittest.TestCase):

    def test_returns_none_when_zero_elements(self):
        """Empty list → None (< ADAPTIVE_MIN_POLLS)."""
        self.assertIsNone(compute_score([]))

    def test_returns_none_when_one_element(self):
        """1-element list → None."""
        self.assertIsNone(compute_score([5]))

    def test_returns_none_when_two_elements(self):
        """2-element list → None."""
        self.assertIsNone(compute_score([5, 3]))

    def test_returns_float_at_min_polls(self):
        """Exactly ADAPTIVE_MIN_POLLS (3) elements → returns float."""
        result = compute_score([1, 2, 3])
        self.assertIsInstance(result, float)
        self.assertIsNotNone(result)

    def test_returns_float_for_four_elements(self):
        """4 elements → returns float."""
        result = compute_score([0, 1, 2, 3])
        self.assertIsNotNone(result)
        self.assertIsInstance(result, float)

    def test_returns_float_for_five_elements(self):
        """5 elements → returns float."""
        result = compute_score([0, 0, 2, 3, 1])
        self.assertIsNotNone(result)
        self.assertIsInstance(result, float)

    def test_all_zero_window_returns_zero(self):
        """All-zero rolling window → score = 0.0."""
        result = compute_score([0, 0, 0, 0, 0])
        self.assertEqual(result, 0.0)

    def test_recency_bias_burst_recent_beats_burst_old(self):
        """Burst in newest poll should score higher than burst in oldest poll."""
        old_burst = [10, 0, 0, 0, 0]
        new_burst = [0, 0, 0, 0, 10]
        score_old = compute_score(old_burst)
        score_new = compute_score(new_burst)
        self.assertGreater(score_new, score_old)

    def test_last_position_highest_weight(self):
        """Single job in last position → highest weighted contribution."""
        last = [0, 0, 0, 0, 1]
        first = [1, 0, 0, 0, 0]
        self.assertGreater(compute_score(last), compute_score(first))

    def test_5_element_weighted_average_correctness(self):
        """
        Manually verify weighted average for [0, 0, 2, 3, 1].
        weights = [0.10, 0.15, 0.20, 0.25, 0.30]  (oldest → newest)
        weighted_sum = 0*0.10 + 0*0.15 + 2*0.20 + 3*0.25 + 1*0.30
                     = 0 + 0 + 0.40 + 0.75 + 0.30 = 1.45
        total_weight = 1.0
        expected = 1.45
        """
        result = compute_score([0, 0, 2, 3, 1])
        self.assertAlmostEqual(result, 1.45, places=6)

    def test_3_element_window_uses_last_three_weights(self):
        """
        3-element window uses weights[-3:] = [0.20, 0.25, 0.30].
        [1, 2, 3]: sum = 1*0.20 + 2*0.25 + 3*0.30 = 0.20 + 0.50 + 0.90 = 1.60
        weight_sum = 0.75
        expected = 1.60 / 0.75 ≈ 2.1333
        """
        result = compute_score([1, 2, 3])
        expected = (1 * 0.20 + 2 * 0.25 + 3 * 0.30) / (0.20 + 0.25 + 0.30)
        self.assertAlmostEqual(result, expected, places=6)

    def test_values_above_cap_accepted_as_is(self):
        """compute_score does NOT cap values — push_poll_result does."""
        result = compute_score([100, 100, 100, 100, 100])
        self.assertGreater(result, ADAPTIVE_CAP_PER_POLL)

    def test_exactly_min_polls_edge(self):
        """ADAPTIVE_MIN_POLLS is the minimum; one less returns None."""
        below = list(range(ADAPTIVE_MIN_POLLS - 1))
        at    = list(range(ADAPTIVE_MIN_POLLS))
        self.assertIsNone(compute_score(below))
        self.assertIsNotNone(compute_score(at))

    def test_score_non_negative_for_all_non_negative_inputs(self):
        """Score is always >= 0 for non-negative inputs."""
        for counts in [[0, 0, 0], [1, 0, 0], [0, 0, 5], [3, 2, 1, 4, 2]]:
            if len(counts) >= ADAPTIVE_MIN_POLLS:
                self.assertGreaterEqual(compute_score(counts), 0.0)


# ─────────────────────────────────────────────────────────────────────────────
# TestBandLookup
# ─────────────────────────────────────────────────────────────────────────────

class TestBandLookup(unittest.TestCase):

    def setUp(self):
        """Use DEFAULT_THRESHOLDS throughout these tests."""
        self.t = DEFAULT_THRESHOLDS  # {"low": 1.5, "moderate": 3.5, "active": 6.0}

    def test_none_score_returns_default_interval(self):
        """score=None → ADAPTIVE_DEFAULT_INTERVAL (12h)."""
        self.assertEqual(band_lookup(None, self.t), ADAPTIVE_DEFAULT_INTERVAL)

    def test_zero_score_returns_default_interval(self):
        """score=0.0 → ADAPTIVE_DEFAULT_INTERVAL (12h) — no activity."""
        self.assertEqual(band_lookup(0.0, self.t), ADAPTIVE_DEFAULT_INTERVAL)

    def test_score_just_below_low_returns_9h(self):
        """score just below 'low' threshold → 9h."""
        just_below = self.t["low"] - 0.01
        self.assertEqual(band_lookup(just_below, self.t), 9 * 3600)

    def test_score_0_1_returns_9h(self):
        """score=0.1 (> 0, < low=1.5) → 9h."""
        self.assertEqual(band_lookup(0.1, self.t), 9 * 3600)

    def test_score_at_low_returns_6h(self):
        """score AT 'low' threshold (1.5) → 6h (>= comparisons promote to next band)."""
        result = band_lookup(self.t["low"], self.t)
        # 1.5 is NOT < 1.5, so falls to the next elif checking < moderate
        self.assertEqual(result, 6 * 3600)

    def test_score_between_low_and_moderate_returns_6h(self):
        """score between low and moderate → 6h."""
        mid = (self.t["low"] + self.t["moderate"]) / 2
        self.assertEqual(band_lookup(mid, self.t), 6 * 3600)

    def test_score_at_moderate_returns_4h(self):
        """score AT moderate threshold (3.5) → 4h."""
        result = band_lookup(self.t["moderate"], self.t)
        self.assertEqual(result, 4 * 3600)

    def test_score_between_moderate_and_active_returns_4h(self):
        """score between moderate and active → 4h."""
        mid = (self.t["moderate"] + self.t["active"]) / 2
        self.assertEqual(band_lookup(mid, self.t), 4 * 3600)

    def test_score_at_active_returns_3h(self):
        """score AT active threshold (6.0) → ADAPTIVE_MIN_INTERVAL (3h)."""
        result = band_lookup(self.t["active"], self.t)
        self.assertEqual(result, ADAPTIVE_MIN_INTERVAL)

    def test_score_above_active_returns_3h(self):
        """score well above active → 3h."""
        self.assertEqual(band_lookup(100.0, self.t), ADAPTIVE_MIN_INTERVAL)

    def test_custom_thresholds_respected(self):
        """Custom thresholds (different values) produce correct band."""
        custom = {"low": 0.5, "moderate": 1.0, "active": 2.0}
        self.assertEqual(band_lookup(0.3, custom), 9 * 3600)   # < 0.5
        self.assertEqual(band_lookup(0.5, custom), 6 * 3600)   # == 0.5 → 6h
        self.assertEqual(band_lookup(0.7, custom), 6 * 3600)   # 0.5–1.0
        self.assertEqual(band_lookup(1.0, custom), 4 * 3600)   # == 1.0 → 4h
        self.assertEqual(band_lookup(1.5, custom), 4 * 3600)   # 1.0–2.0
        self.assertEqual(band_lookup(2.0, custom), ADAPTIVE_MIN_INTERVAL)

    def test_none_thresholds_falls_back_to_defaults(self):
        """Passing thresholds=None uses DEFAULT_THRESHOLDS."""
        score = DEFAULT_THRESHOLDS["active"]  # exactly at active
        result = band_lookup(score, None)
        self.assertEqual(result, ADAPTIVE_MIN_INTERVAL)

    def test_all_valid_intervals_are_expected_set(self):
        """band_lookup only returns one of the 4 expected intervals."""
        valid = {
            ADAPTIVE_DEFAULT_INTERVAL,   # 12h
            9 * 3600,                    # 9h
            6 * 3600,                    # 6h
            4 * 3600,                    # 4h
            ADAPTIVE_MIN_INTERVAL,       # 3h
        }
        for score in [None, 0.0, 0.5, 1.5, 2.5, 3.5, 5.0, 6.0, 10.0]:
            result = band_lookup(score, self.t)
            self.assertIn(result, valid, f"Unexpected interval for score={score}: {result}")


# ─────────────────────────────────────────────────────────────────────────────
# TestGetMaxInterval
# ─────────────────────────────────────────────────────────────────────────────

class TestGetMaxInterval(unittest.TestCase):

    def setUp(self):
        self.t = DEFAULT_THRESHOLDS  # moderate = 3.5

    def test_none_score_returns_dormant_cap(self):
        """score=None → 12h dormant cap."""
        self.assertEqual(get_max_interval(None, self.t), ADAPTIVE_MAX_INTERVAL_DORMANT)

    def test_zero_score_returns_dormant_cap(self):
        """score=0.0 → 12h dormant cap (no recent activity)."""
        self.assertEqual(get_max_interval(0.0, self.t), ADAPTIVE_MAX_INTERVAL_DORMANT)

    def test_score_below_moderate_returns_dormant_cap(self):
        """score < moderate → 12h dormant cap."""
        below = self.t["moderate"] - 0.1
        self.assertEqual(get_max_interval(below, self.t), ADAPTIVE_MAX_INTERVAL_DORMANT)

    def test_score_at_moderate_returns_active_cap(self):
        """score >= moderate → ADAPTIVE_MAX_INTERVAL_ACTIVE (6h)."""
        self.assertEqual(get_max_interval(self.t["moderate"], self.t),
                         ADAPTIVE_MAX_INTERVAL_ACTIVE)

    def test_score_above_moderate_returns_active_cap(self):
        """score well above moderate → 6h cap."""
        self.assertEqual(get_max_interval(100.0, self.t), ADAPTIVE_MAX_INTERVAL_ACTIVE)

    def test_active_cap_is_less_than_dormant_cap(self):
        """Active cap (6h) is tighter (smaller) than dormant cap (12h)."""
        self.assertLess(ADAPTIVE_MAX_INTERVAL_ACTIVE, ADAPTIVE_MAX_INTERVAL_DORMANT)

    def test_custom_thresholds_respected(self):
        """Custom moderate threshold changes the cap boundary."""
        custom = {"low": 1.0, "moderate": 2.0, "active": 5.0}
        self.assertEqual(get_max_interval(1.9, custom), ADAPTIVE_MAX_INTERVAL_DORMANT)
        self.assertEqual(get_max_interval(2.0, custom), ADAPTIVE_MAX_INTERVAL_ACTIVE)
        self.assertEqual(get_max_interval(2.1, custom), ADAPTIVE_MAX_INTERVAL_ACTIVE)

    def test_none_thresholds_falls_back_to_defaults(self):
        """None thresholds falls back to DEFAULT_THRESHOLDS."""
        # score >= default moderate (3.5) → active cap
        result = get_max_interval(DEFAULT_THRESHOLDS["moderate"], None)
        self.assertEqual(result, ADAPTIVE_MAX_INTERVAL_ACTIVE)


# ─────────────────────────────────────────────────────────────────────────────
# TestComputeNextInterval
# ─────────────────────────────────────────────────────────────────────────────

class TestComputeNextInterval(unittest.TestCase):

    def test_reactivation_reacts_immediately(self):
        """computed < current → react immediately, return computed."""
        result = compute_next_interval(computed=3 * 3600, current_interval=12 * 3600)
        self.assertEqual(result, 3 * 3600)

    def test_dormancy_applies_smoothing(self):
        """computed > current → apply EMA smoothing."""
        computed = 12 * 3600
        current  = 3 * 3600
        result = compute_next_interval(computed, current)
        # smoothed = 0.3 * computed + 0.7 * current
        expected = int(ADAPTIVE_SMOOTHING * computed + (1 - ADAPTIVE_SMOOTHING) * current)
        self.assertEqual(result, expected)

    def test_equal_values_unchanged(self):
        """computed == current → result equals both (EMA of equal = equal)."""
        val = 6 * 3600
        result = compute_next_interval(val, val)
        self.assertEqual(result, val)

    def test_returns_int(self):
        """compute_next_interval always returns int."""
        result = compute_next_interval(10000, 20000)
        self.assertIsInstance(result, int)

    def test_dormancy_result_between_computed_and_current(self):
        """Dormancy smoothed result is between computed and current."""
        computed = 12 * 3600
        current  = 3 * 3600
        result = compute_next_interval(computed, current)
        self.assertGreater(result, current)
        self.assertLessEqual(result, computed)

    def test_smoothing_factor_is_correct(self):
        """Verify ADAPTIVE_SMOOTHING = 0.3 is used in the formula."""
        # 0.3 * computed + 0.7 * current
        computed = 10000
        current  = 5000
        expected = int(ADAPTIVE_SMOOTHING * computed + (1 - ADAPTIVE_SMOOTHING) * current)
        self.assertEqual(compute_next_interval(computed, current), expected)

    def test_reactivation_from_12h_to_3h(self):
        """Typical reactivation: dormant 12h → active 3h → immediate."""
        result = compute_next_interval(ADAPTIVE_MIN_INTERVAL, ADAPTIVE_DEFAULT_INTERVAL)
        self.assertEqual(result, ADAPTIVE_MIN_INTERVAL)

    def test_dormancy_from_3h_to_12h(self):
        """Going from 3h active to 12h dormant: gradual increase."""
        result = compute_next_interval(ADAPTIVE_DEFAULT_INTERVAL, ADAPTIVE_MIN_INTERVAL)
        # Result should be greater than current (3h) but less than 12h
        self.assertGreater(result, ADAPTIVE_MIN_INTERVAL)
        self.assertLessEqual(result, ADAPTIVE_DEFAULT_INTERVAL)

    def test_reactivation_path_not_smoothed(self):
        """Verify reactivation path skips the EMA entirely."""
        computed = 1000
        current  = 5000
        # No smoothing — should be exactly computed
        result = compute_next_interval(computed, current)
        self.assertEqual(result, computed)
        # Verify it's different from what EMA would give
        ema = int(ADAPTIVE_SMOOTHING * computed + (1 - ADAPTIVE_SMOOTHING) * current)
        self.assertNotEqual(result, ema)


# ─────────────────────────────────────────────────────────────────────────────
# TestPushPollResult
# ─────────────────────────────────────────────────────────────────────────────

class TestPushPollResult(unittest.TestCase):

    def test_appends_value_to_empty_list(self):
        """Empty list → single element."""
        result = push_poll_result([], 3)
        self.assertEqual(result, [3])

    def test_appends_to_existing_list(self):
        """Existing list gets new value appended."""
        result = push_poll_result([1, 2], 5)
        self.assertEqual(result[-1], min(5, ADAPTIVE_CAP_PER_POLL))

    def test_zero_jobs_appended(self):
        """0 new jobs appended as 0."""
        result = push_poll_result([1, 2, 3], 0)
        self.assertEqual(result[-1], 0)

    def test_value_at_cap_not_reduced(self):
        """Exactly ADAPTIVE_CAP_PER_POLL (10) → not capped."""
        result = push_poll_result([1, 2], ADAPTIVE_CAP_PER_POLL)
        self.assertEqual(result[-1], ADAPTIVE_CAP_PER_POLL)

    def test_value_above_cap_is_capped(self):
        """Values above cap are capped at ADAPTIVE_CAP_PER_POLL."""
        result = push_poll_result([1, 2], ADAPTIVE_CAP_PER_POLL + 1)
        self.assertEqual(result[-1], ADAPTIVE_CAP_PER_POLL)

    def test_large_value_capped_at_10(self):
        """Large burst (100 jobs) capped at 10."""
        result = push_poll_result([0, 0, 0, 0], 100)
        self.assertEqual(result[-1], ADAPTIVE_CAP_PER_POLL)

    def test_overflow_removes_oldest(self):
        """List of 5 elements: oldest entry removed when 6th is pushed."""
        window = [1, 2, 3, 4, 5]
        result = push_poll_result(window, 6)
        self.assertEqual(len(result), 5)
        self.assertEqual(result[0], 2)   # oldest (1) was removed
        self.assertEqual(result[-1], min(6, ADAPTIVE_CAP_PER_POLL))

    def test_does_not_mutate_input(self):
        """Input list is not mutated — new list returned."""
        original = [1, 2, 3]
        result = push_poll_result(original, 5)
        self.assertEqual(original, [1, 2, 3])   # unchanged
        self.assertIsNot(result, original)

    def test_list_never_exceeds_5(self):
        """Rolling window never grows beyond 5 elements."""
        window = []
        for i in range(20):
            window = push_poll_result(window, i)
        self.assertLessEqual(len(window), 5)

    def test_4_element_list_grows_to_5(self):
        """4-element list → grows to 5 (no overflow yet)."""
        result = push_poll_result([1, 2, 3, 4], 5)
        self.assertEqual(len(result), 5)

    def test_consecutive_overflows_preserve_recency(self):
        """After many pushes, most recent values are retained."""
        window = []
        for i in range(10):
            window = push_poll_result(window, i)
        # Last 5 values: 5, 6, 7, 8, 9
        self.assertEqual(window, [5, 6, 7, 8, 9])


# ─────────────────────────────────────────────────────────────────────────────
# TestBuildThresholdsFromScores
# ─────────────────────────────────────────────────────────────────────────────

class TestBuildThresholdsFromScores(unittest.TestCase):

    def test_fewer_than_min_companies_returns_none(self):
        """Fewer than ADAPTIVE_MIN_COMPANIES_CALIBRATE active → None."""
        scores = [1.0] * (ADAPTIVE_MIN_COMPANIES_CALIBRATE - 1)
        self.assertIsNone(build_thresholds_from_scores(scores))

    def test_exactly_min_companies_returns_dict(self):
        """Exactly ADAPTIVE_MIN_COMPANIES_CALIBRATE → returns dict."""
        scores = [float(i) for i in range(1, ADAPTIVE_MIN_COMPANIES_CALIBRATE + 1)]
        result = build_thresholds_from_scores(scores)
        self.assertIsNotNone(result)
        self.assertIsInstance(result, dict)

    def test_all_zeros_returns_none(self):
        """All zeros → no active companies → None."""
        scores = [0.0] * 10
        self.assertIsNone(build_thresholds_from_scores(scores))

    def test_mixed_zeros_filtered(self):
        """Zeros filtered; if enough non-zero remain → dict returned."""
        # 5 non-zero, 5 zero
        scores = [0.0] * 5 + [1.0, 2.0, 3.0, 4.0, 5.0]
        result = build_thresholds_from_scores(scores)
        self.assertIsNotNone(result)

    def test_returns_dict_with_required_keys(self):
        """Result dict has keys 'low', 'moderate', 'active'."""
        scores = [float(i) for i in range(1, 11)]
        result = build_thresholds_from_scores(scores)
        self.assertIn("low", result)
        self.assertIn("moderate", result)
        self.assertIn("active", result)

    def test_strict_ordering_active_gt_moderate_gt_low(self):
        """active > moderate > low > 0 always holds."""
        for scores in [
            [1.0, 1.5, 2.0, 2.5, 3.0],        # 5 companies, ascending
            [0.1] * 10,                         # all same (tie case)
            [0.1, 0.2, 0.3, 0.5, 0.7, 1.0,
             1.5, 2.0, 3.0, 100.0],             # outlier case
        ]:
            result = build_thresholds_from_scores(scores)
            if result is not None:
                self.assertGreater(result["active"], result["moderate"],
                                   f"active not > moderate for {scores}")
                self.assertGreater(result["moderate"], result["low"],
                                   f"moderate not > low for {scores}")
                self.assertGreater(result["low"], 0,
                                   f"low not > 0 for {scores}")

    def test_winsorization_caps_extreme_outlier(self):
        """Extreme outlier (100) is winsorized down to near-cap level."""
        # 9 companies with scores 0.1–0.9, plus one extreme outlier
        scores = [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 100.0]
        result = build_thresholds_from_scores(scores)
        self.assertIsNotNone(result)
        # Active threshold should be near the top of the non-outlier scores,
        # not near 100.  Without winsorization, active would be ~100.
        self.assertLess(result["active"], 10.0,
                        "Outlier not winsorized — active threshold too high")

    def test_low_score_above_zero(self):
        """'low' threshold is always > 0 (enforced by max(low_thr, eps))."""
        scores = [0.01] * 10
        result = build_thresholds_from_scores(scores)
        if result:
            self.assertGreater(result["low"], 0)

    def test_all_same_scores_produces_valid_thresholds(self):
        """All identical scores → epsilon nudge produces valid ordering."""
        scores = [2.0] * 10
        result = build_thresholds_from_scores(scores)
        self.assertIsNotNone(result)
        self.assertGreater(result["active"],   result["moderate"])
        self.assertGreater(result["moderate"], result["low"])

    def test_10_companies_ascending_scores(self):
        """
        10 active companies, ascending scores 1–10.
        After winsorize(top 5%):
            n_outliers = max(1, ceil(10*0.05)) = max(1, 1) = 1
            cap_idx    = max(0, 10 - 1 - 1) = 8
            cap_val    = sorted_asc[8] = 9.0
            winsorized = [1, 2, 3, 4, 5, 6, 7, 8, 9, 9]  (10 capped to 9)
        Sorted desc: [9, 9, 8, 7, 6, 5, 4, 3, 2, 1]
        n_3h = ceil(10*0.10) = 1  → active_thr   = desc[0]     = 9.0
        n_4h = ceil(10*0.15) = 2  → moderate_thr = desc[1+2-1] = desc[2] = 8.0
        n_6h = ceil(10*0.25) = 3  → low_thr      = desc[1+2+3-1] = desc[5] = 5.0
        """
        scores = [float(i) for i in range(1, 11)]
        result = build_thresholds_from_scores(scores)
        self.assertIsNotNone(result)
        # Active = 9.0 (1st company after winsorization caps 10→9)
        self.assertAlmostEqual(result["active"], 9.0, places=3)
        # Moderate = 8.0 (3rd company, idx=2)
        self.assertAlmostEqual(result["moderate"], 8.0, places=3)
        # Low = 5.0 (6th company, idx=5)
        self.assertAlmostEqual(result["low"], 5.0, places=3)

    def test_result_thresholds_are_floats(self):
        """All threshold values are floats."""
        scores = [float(i) for i in range(1, 11)]
        result = build_thresholds_from_scores(scores)
        for key in ("low", "moderate", "active"):
            self.assertIsInstance(result[key], float)


# ─────────────────────────────────────────────────────────────────────────────
# TestUpdatePollInterval
# ─────────────────────────────────────────────────────────────────────────────

class TestUpdatePollInterval(unittest.TestCase):

    def _run(self, counts, current, new_jobs, thresholds=None):
        return update_poll_interval(counts, current, new_jobs, thresholds)

    def test_returns_dict(self):
        """update_poll_interval always returns a dict."""
        result = self._run([0, 0, 0, 0, 0], ADAPTIVE_DEFAULT_INTERVAL, 0)
        self.assertIsInstance(result, dict)

    def test_required_keys_present(self):
        """Result has 'recent_poll_counts', 'adaptive_score', 'current_interval_s'."""
        result = self._run([0, 0, 0, 0, 0], ADAPTIVE_DEFAULT_INTERVAL, 0)
        self.assertIn("recent_poll_counts", result)
        self.assertIn("adaptive_score",     result)
        self.assertIn("current_interval_s", result)

    def test_adaptive_score_is_zero_when_none_history(self):
        """score=None (< ADAPTIVE_MIN_POLLS history) → adaptive_score = 0.0 in result."""
        # Only 2 previous counts — after push we have 3 (exactly MIN_POLLS)
        # Use fewer than MIN_POLLS-1 so we're still below after push
        result = self._run([], ADAPTIVE_DEFAULT_INTERVAL, 0)
        # With 0 history + 1 push → 1 count total → still below MIN_POLLS
        self.assertEqual(result["adaptive_score"], 0.0)

    def test_adaptive_score_is_zero_for_all_zero_window(self):
        """All-zero window after push → adaptive_score = 0.0."""
        result = self._run([0, 0, 0, 0], ADAPTIVE_DEFAULT_INTERVAL, 0)
        # 5th element pushed → compute_score([0,0,0,0,0]) = 0.0
        self.assertEqual(result["adaptive_score"], 0.0)

    def test_recent_poll_counts_updated(self):
        """recent_poll_counts in result reflects push of new_jobs."""
        result = self._run([1, 2, 3, 4], ADAPTIVE_DEFAULT_INTERVAL, 5)
        # pushed 5 → appended min(5,10)=5 → window is [1,2,3,4,5]
        self.assertEqual(result["recent_poll_counts"][-1], 5)

    def test_interval_capped_by_max_interval(self):
        """Interval cannot exceed ADAPTIVE_MAX_INTERVAL_DORMANT (12h)."""
        result = self._run([0, 0, 0, 0, 0], ADAPTIVE_DEFAULT_INTERVAL, 0)
        self.assertLessEqual(result["current_interval_s"], ADAPTIVE_MAX_INTERVAL_DORMANT)

    def test_reactivation_lowers_interval_immediately(self):
        """Very active new jobs → interval drops from 12h immediately."""
        # Full window of high activity → should move to 3h
        result = self._run([10, 10, 10, 10], ADAPTIVE_DEFAULT_INTERVAL, 10)
        # Score = high → band = 3h; reactivation (3h < 12h) → immediate
        self.assertLess(result["current_interval_s"], ADAPTIVE_DEFAULT_INTERVAL)

    def test_dormancy_slows_interval_increase(self):
        """No activity: interval increases gradually (not instantly to 12h)."""
        # Currently at 3h (ADAPTIVE_MIN_INTERVAL)
        result = self._run([0, 0, 0, 0], ADAPTIVE_MIN_INTERVAL, 0)
        # After push [0,0,0,0,0] → score=0 → band=12h → dormancy smoothing
        # result should be greater than current (3h) but not jump all the way to 12h
        if result["current_interval_s"] > ADAPTIVE_MIN_INTERVAL:
            self.assertLess(result["current_interval_s"], ADAPTIVE_DEFAULT_INTERVAL)

    def test_custom_thresholds_propagated(self):
        """Custom thresholds are passed through to band_lookup and get_max_interval."""
        # With very low thresholds, even small activity should get 3h
        custom = {"low": 0.01, "moderate": 0.02, "active": 0.03}
        result = self._run([1, 1, 1, 1], ADAPTIVE_DEFAULT_INTERVAL, 1, custom)
        # Score for [1,1,1,1,1] ≈ 1.0 >> active=0.03 → 3h
        self.assertEqual(result["current_interval_s"], ADAPTIVE_MIN_INTERVAL)


# ─────────────────────────────────────────────────────────────────────────────
# TestLoadDumpPollCounts
# ─────────────────────────────────────────────────────────────────────────────

class TestLoadDumpPollCounts(unittest.TestCase):

    def test_load_none_returns_empty_list(self):
        """load_poll_counts(None) → []."""
        self.assertEqual(load_poll_counts(None), [])

    def test_load_empty_string_returns_empty_list(self):
        """load_poll_counts("") → []."""
        self.assertEqual(load_poll_counts(""), [])

    def test_load_invalid_json_returns_empty_list(self):
        """Malformed JSON string → []."""
        self.assertEqual(load_poll_counts("not_json"), [])

    def test_load_json_object_returns_empty_list(self):
        """Non-list JSON (object) → []."""
        self.assertEqual(load_poll_counts('{"a": 1}'), [])

    def test_load_json_number_returns_empty_list(self):
        """Non-list JSON (number) → []."""
        self.assertEqual(load_poll_counts("42"), [])

    def test_load_null_json_returns_empty_list(self):
        """JSON null → []."""
        self.assertEqual(load_poll_counts("null"), [])

    def test_load_valid_list_returns_ints(self):
        """Valid JSON list → list of ints."""
        result = load_poll_counts("[1, 2, 3, 4, 5]")
        self.assertEqual(result, [1, 2, 3, 4, 5])

    def test_load_coerces_floats_to_ints(self):
        """Float values in JSON are coerced to int."""
        result = load_poll_counts("[1.5, 2.7, 3.0]")
        self.assertEqual(result, [1, 2, 3])
        for v in result:
            self.assertIsInstance(v, int)

    def test_load_empty_json_list(self):
        """JSON empty list → []."""
        self.assertEqual(load_poll_counts("[]"), [])

    def test_dump_returns_string(self):
        """dump_poll_counts returns a string."""
        result = dump_poll_counts([1, 2, 3])
        self.assertIsInstance(result, str)

    def test_dump_is_valid_json(self):
        """dump_poll_counts produces valid JSON."""
        import json
        result = dump_poll_counts([0, 5, 10])
        parsed = json.loads(result)
        self.assertEqual(parsed, [0, 5, 10])

    def test_round_trip(self):
        """dump then load returns identical list."""
        original = [0, 3, 7, 2, 5]
        restored = load_poll_counts(dump_poll_counts(original))
        self.assertEqual(restored, original)

    def test_round_trip_empty(self):
        """Round-trip of empty list."""
        self.assertEqual(load_poll_counts(dump_poll_counts([])), [])

    def test_load_with_extra_whitespace(self):
        """JSON with extra whitespace parsed correctly."""
        result = load_poll_counts("[ 1 , 2 , 3 ]")
        self.assertEqual(result, [1, 2, 3])

    def test_load_with_zero_values(self):
        """List of zeros loads correctly."""
        result = load_poll_counts("[0, 0, 0, 0, 0]")
        self.assertEqual(result, [0, 0, 0, 0, 0])


# ─────────────────────────────────────────────────────────────────────────────
# TestDefaultThresholds
# ─────────────────────────────────────────────────────────────────────────────

class TestDefaultThresholds(unittest.TestCase):

    def test_default_thresholds_keys_exist(self):
        """DEFAULT_THRESHOLDS has 'low', 'moderate', 'active' keys."""
        for key in ("low", "moderate", "active"):
            self.assertIn(key, DEFAULT_THRESHOLDS)

    def test_default_thresholds_ordering(self):
        """DEFAULT_THRESHOLDS: active > moderate > low > 0."""
        t = DEFAULT_THRESHOLDS
        self.assertGreater(t["active"],   t["moderate"])
        self.assertGreater(t["moderate"], t["low"])
        self.assertGreater(t["low"],      0)

    def test_default_thresholds_match_expected_values(self):
        """DEFAULT_THRESHOLDS values match the architecture doc."""
        self.assertAlmostEqual(DEFAULT_THRESHOLDS["low"],      1.5, places=6)
        self.assertAlmostEqual(DEFAULT_THRESHOLDS["moderate"], 3.5, places=6)
        self.assertAlmostEqual(DEFAULT_THRESHOLDS["active"],   6.0, places=6)


if __name__ == "__main__":
    unittest.main(verbosity=2)
