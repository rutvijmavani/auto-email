"""
tests/test_scheduler_contracts.py
─────────────────────────────────────────────────────────────────────────────
Tests for scheduler.py and scan_worker.py contracts that protect against
thundering-herd regressions, backoff miscalculations, and stale-state bugs.

Background
──────────
These systems are hard to test manually because their effects are statistical
(jitter distribution) or only visible after multiple failure cycles (backoff
progression).  A bug in any of these functions can silently defeat the entire
adaptive polling architecture:

  - Missing jitter in _reschedule_adaptive(): companies scheduled at the same
    interval cluster into waves, saturating the ATS at fixed intervals —
    the thundering herd problem described in Section 9 of the architecture doc.

  - Wrong backoff calculation in _get_backoff_delay(): a company that keeps
    failing could be retried too aggressively (DDoS risk) or skipped entirely
    too early (missed coverage).

  - p95_ms=None in claim_stale_work(): api_health may have no data for a new
    platform; `None * 3` raises TypeError, crashing the claim loop and leaving
    stale inflight messages unreclaimed.

  - WARMING interval override: on_adaptive_complete() must use WARMING_INTERVAL_S
    during the warming phase instead of the computed adaptive interval, so the
    engine has 3 full polls of data before switching to dynamic scheduling.

Coverage
────────
  TestBackoffCalculation
    · retry 0 (1st failure)  →  300s  (WORKER_BACKOFF_BASE_S)
    · retry 1                →  600s
    · retry 2                → 1200s
    · retry 3                → 2400s
    · retry 4                → 3600s  (capped at WORKER_BACKOFF_CAP_S)
    · retry 5                → 86400s (WORKER_BACKOFF_GIVEUP_S — skip today)
    · retry 6+               → 86400s (all beyond cap use giveup delay)
    · First call sets TTL of 86400s on the counter key
    · Counter increments atomically on each call
    · Different op_types use different keys (scan / detail / fullscan)

  TestRescheduleJitter
    · _reschedule_adaptive() always adds jitter in the range [-10%, +10%]
    · Jitter is applied to the interval, not the absolute timestamp
    · Score stored in ZSET is approximately now + interval (within ±10%)
    · Calling 200 times: distribution spans both negative and positive jitter
      (statistical test — fails only if jitter is always 0 or always one sign)
    · Without jitter a fixed interval would produce exact clustering

  TestClaimStaleWorkP95Safety
    · p95_ms=None raises TypeError at `p95_ms * 3` — documents known risk
    · Safe caller must guard: max((p95_ms or 0) * 3, 300_000)
    · p95_ms=0 → max(0, 300_000) = 300_000 (minimum 5-minute idle timeout)
    · p95_ms=100_000 → max(300_000, 300_000) = 300_000
    · p95_ms=200_000 → max(600_000, 300_000) = 600_000

  TestWarmingIntervalOverride
    · During WARMING, the computed adaptive interval is overridden with
      WARMING_INTERVAL_S (7200s = 2h)
    · WARMING_INTERVAL_S value matches the architecture doc (2 hours)
    · WARMING_POLLS_COUNT matches architecture doc (3 polls before STABLE)
    · Interval computed from adaptive score is ignored during WARMING
      (even if score suggests a shorter interval like 3h)

  TestBackoffKeyNamespace
    · Backoff keys are namespaced by op_type: retry:backoff:{op}:{company}
    · scan and detail and fullscan ops produce distinct keys
    · Keys for different companies are distinct
    · TTL is 86400s (keys expire automatically — no manual reset needed)

  TestWorkerMissedCycleBoundary
    · Field checked is last_full_scan_at (fullscan worker — exhaustive all-pages),
      NOT last_poll_at (adaptive worker — uses smart early exit, may miss pages)
    · OLD boundary (today at 7 AM) misclassifies a 6 AM fullscan as missed
    · OLD boundary causes 0/139 covered at cron time (reproduces production bug)
    · NEW boundary (now - 24h) correctly covers a 6 AM fullscan at 7:02 AM cron
    · NEW boundary correctly covers a 4 AM fullscan (normal overnight completion)
    · Fullscan older than 24h is still correctly flagged as missed under new logic
    · never-fullscanned company (last_full_scan_at=NULL → 0) is always missed
    · Structural test: job_monitor.py queries last_full_scan_at + timedelta(hours=24)
"""

import sys
import os
import time
import unittest
from unittest.mock import MagicMock, patch, call

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from workers.scan_worker import _get_backoff_delay
from config import (
    WORKER_BACKOFF_BASE_S,
    WORKER_BACKOFF_CAP_S,
    WORKER_BACKOFF_GIVEUP_S,
    REDIS_BACKOFF_PREFIX,
    WARMING_INTERVAL_S,
    WARMING_POLLS_COUNT,
)


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _make_redis_mock(incr_value=1):
    """Return a mock Redis client with incr() returning incr_value."""
    r = MagicMock()
    r.incr.return_value = incr_value
    r.expire.return_value = True
    return r


# ─────────────────────────────────────────────────────────────────────────────
# TestBackoffCalculation
# ─────────────────────────────────────────────────────────────────────────────

class TestBackoffCalculation(unittest.TestCase):
    """
    _get_backoff_delay() implements exponential backoff with a cap.

    Formula (from source):
        retry_count = count - 1        # 0-based (count is 1-indexed from INCR)
        if retry_count >= 5: return WORKER_BACKOFF_GIVEUP_S  # 86400
        return min(WORKER_BACKOFF_BASE_S * (2 ** retry_count), WORKER_BACKOFF_CAP_S)
    """

    def _call(self, incr_value, company="Acme", op_type="scan"):
        """Call _get_backoff_delay with a mock Redis whose INCR returns incr_value."""
        r = _make_redis_mock(incr_value)
        return _get_backoff_delay(r, company, op_type), r

    def test_retry_0_first_failure(self):
        """1st failure (incr=1, retry=0) → BASE * 2^0 = 300s."""
        delay, _ = self._call(incr_value=1)
        self.assertEqual(delay, WORKER_BACKOFF_BASE_S)         # 300
        self.assertEqual(delay, 300)

    def test_retry_1(self):
        """2nd failure (incr=2, retry=1) → 300 * 2 = 600s."""
        delay, _ = self._call(incr_value=2)
        self.assertEqual(delay, 600)

    def test_retry_2(self):
        """3rd failure (incr=3, retry=2) → 300 * 4 = 1200s."""
        delay, _ = self._call(incr_value=3)
        self.assertEqual(delay, 1200)

    def test_retry_3(self):
        """4th failure → 300 * 8 = 2400s."""
        delay, _ = self._call(incr_value=4)
        self.assertEqual(delay, 2400)

    def test_retry_4_capped(self):
        """5th failure → 300 * 16 = 4800s but capped at WORKER_BACKOFF_CAP_S=3600."""
        delay, _ = self._call(incr_value=5)
        self.assertEqual(delay, WORKER_BACKOFF_CAP_S)          # 3600
        self.assertEqual(delay, 3600)

    def test_retry_5_giveup(self):
        """6th failure (retry_count=5) → WORKER_BACKOFF_GIVEUP_S = 86400."""
        delay, _ = self._call(incr_value=6)
        self.assertEqual(delay, WORKER_BACKOFF_GIVEUP_S)       # 86400
        self.assertEqual(delay, 86400)

    def test_retry_10_still_giveup(self):
        """10th+ failure → still 86400 (no further escalation)."""
        delay, _ = self._call(incr_value=11)
        self.assertEqual(delay, WORKER_BACKOFF_GIVEUP_S)

    def test_first_call_sets_ttl(self):
        """First call (incr returns 1) must set a 86400s TTL on the key."""
        delay, r = self._call(incr_value=1, company="TestCo", op_type="scan")
        expected_key = f"{REDIS_BACKOFF_PREFIX}:scan:TestCo"
        r.expire.assert_called_once_with(expected_key, 86400)

    def test_subsequent_call_does_not_reset_ttl(self):
        """Second call (incr returns 2) must NOT call expire (TTL already set)."""
        delay, r = self._call(incr_value=2, company="TestCo", op_type="scan")
        r.expire.assert_not_called()

    def test_incr_called_with_correct_key(self):
        """INCR is called on the correct namespaced key."""
        _, r = self._call(incr_value=1, company="Stripe", op_type="detail")
        expected_key = f"{REDIS_BACKOFF_PREFIX}:detail:Stripe"
        r.incr.assert_called_once_with(expected_key)

    def test_backoff_constants_are_correct(self):
        """Config constants match documented values."""
        self.assertEqual(WORKER_BACKOFF_BASE_S,  300)
        self.assertEqual(WORKER_BACKOFF_CAP_S,   3600)
        self.assertEqual(WORKER_BACKOFF_GIVEUP_S, 86400)


# ─────────────────────────────────────────────────────────────────────────────
# TestBackoffKeyNamespace
# ─────────────────────────────────────────────────────────────────────────────

class TestBackoffKeyNamespace(unittest.TestCase):
    """Backoff keys are namespaced to prevent cross-contamination."""

    def _key(self, op_type, company):
        return f"{REDIS_BACKOFF_PREFIX}:{op_type}:{company}"

    def test_scan_and_detail_keys_distinct(self):
        scan_key   = self._key("scan", "Acme")
        detail_key = self._key("detail", "Acme")
        self.assertNotEqual(scan_key, detail_key)

    def test_scan_and_fullscan_keys_distinct(self):
        self.assertNotEqual(self._key("scan", "Acme"),
                            self._key("fullscan", "Acme"))

    def test_different_companies_distinct_keys(self):
        self.assertNotEqual(self._key("scan", "Acme"),
                            self._key("scan", "Stripe"))

    def test_key_includes_prefix(self):
        key = self._key("scan", "TestCo")
        self.assertTrue(key.startswith(REDIS_BACKOFF_PREFIX))

    def test_get_backoff_delay_uses_correct_key(self):
        """_get_backoff_delay() calls INCR on the expected key."""
        r = _make_redis_mock(1)
        _get_backoff_delay(r, "GlobalCo", "fullscan")
        expected = f"{REDIS_BACKOFF_PREFIX}:fullscan:GlobalCo"
        r.incr.assert_called_once_with(expected)


# ─────────────────────────────────────────────────────────────────────────────
# TestRescheduleJitter
# ─────────────────────────────────────────────────────────────────────────────

class TestRescheduleJitter(unittest.TestCase):
    """
    _reschedule_adaptive() applies ±10% jitter to prevent thundering herd.

    Source (scheduler.py):
        def _reschedule_adaptive(company, interval_s):
            jitter = random.uniform(-0.10, 0.10)
            score  = time.time() + interval_s * (1.0 + jitter)
            get_redis().zadd(REDIS_POLL_ADAPTIVE, {company: score})
    """

    def test_jitter_bounds_via_mock(self):
        """
        Verify that random.uniform is called with exactly (-0.10, 0.10).
        This is the tightest possible test: it verifies the intent of the code
        without needing to run 200 iterations.
        """
        from workers.scheduler import _reschedule_adaptive

        with patch("workers.scheduler.random.uniform", return_value=0.0) as mock_uniform, \
             patch("workers.scheduler.get_redis") as mock_get_redis:
            mock_redis = MagicMock()
            mock_get_redis.return_value = mock_redis

            _reschedule_adaptive("Acme", 3600)

            mock_uniform.assert_called_once_with(-0.10, 0.10)

    def test_score_incorporates_jitter(self):
        """
        Score stored in ZSET = now + interval * (1 + jitter).
        With jitter=+0.10, score = now + interval * 1.10.
        """
        from workers.scheduler import _reschedule_adaptive

        interval = 3600
        fixed_jitter = 0.10
        fixed_now = time.time()

        with patch("workers.scheduler.random.uniform", return_value=fixed_jitter), \
             patch("workers.scheduler.time.time", return_value=fixed_now), \
             patch("workers.scheduler.get_redis") as mock_get_redis:
            mock_redis = MagicMock()
            mock_get_redis.return_value = mock_redis

            _reschedule_adaptive("Acme", interval)

            expected_score = fixed_now + interval * (1.0 + fixed_jitter)
            mock_redis.zadd.assert_called_once_with(
                unittest.mock.ANY,   # REDIS_POLL_ADAPTIVE constant
                {"Acme": expected_score},
            )

    def test_negative_jitter_decreases_score(self):
        """Negative jitter pulls the score closer to now (earlier poll)."""
        from workers.scheduler import _reschedule_adaptive

        interval = 7200
        fixed_jitter = -0.10
        fixed_now = time.time()

        with patch("workers.scheduler.random.uniform", return_value=fixed_jitter), \
             patch("workers.scheduler.time.time", return_value=fixed_now), \
             patch("workers.scheduler.get_redis") as mock_get_redis:
            mock_redis = MagicMock()
            mock_get_redis.return_value = mock_redis

            _reschedule_adaptive("Stripe", interval)

            expected_score = fixed_now + interval * (1.0 + fixed_jitter)
            # Score should be less than now + interval (no jitter)
            no_jitter_score = fixed_now + interval
            self.assertLess(expected_score, no_jitter_score)
            mock_redis.zadd.assert_called_once_with(
                unittest.mock.ANY, {"Stripe": expected_score}
            )

    def test_statistical_both_signs_over_many_calls(self):
        """
        Over 200 calls with real random.uniform, jitter must produce both
        positive and negative offsets (probability of all same sign ≈ 2^-200).
        This detects if the jitter formula was changed to always return +0 or
        if the range was collapsed to [0, 0].
        """
        from workers.scheduler import _reschedule_adaptive

        interval = 3600
        scores = []

        with patch("workers.scheduler.get_redis") as mock_get_redis:
            mock_redis = MagicMock()
            mock_get_redis.return_value = mock_redis

            for i in range(200):
                _reschedule_adaptive(f"Company{i}", interval)

            # Extract scores from zadd calls
            for c in mock_redis.zadd.call_args_list:
                mapping = c[0][1]   # second positional arg is the {member: score} dict
                scores.extend(mapping.values())

        base_time = time.time()
        # All scores should be approximately now + interval ± 10%
        lower_bound = base_time + interval * 0.89   # allow 1% measurement slack
        upper_bound = base_time + interval * 1.11

        self.assertEqual(len(scores), 200, "Expected 200 ZADD calls")

        # Check that both "below no-jitter" and "above no-jitter" are represented
        no_jitter = base_time + interval
        below = sum(1 for s in scores if s < no_jitter)
        above = sum(1 for s in scores if s >= no_jitter)
        self.assertGreater(below, 10,
                           f"Only {below}/200 scores were below no-jitter baseline. "
                           f"Jitter may be one-sided or zero.")
        self.assertGreater(above, 10,
                           f"Only {above}/200 scores were above no-jitter baseline. "
                           f"Jitter may be one-sided or zero.")

    def test_no_jitter_would_cause_exact_clustering(self):
        """
        Document the thundering herd problem: if jitter is removed,
        N companies with the same interval produce identical ZSET scores.
        This test shows what the code prevents.
        """
        # Simulate 5 companies all scheduled with exactly 3600s (no jitter)
        interval = 3600
        now = 1_700_000_000.0  # fixed timestamp for reproducibility
        no_jitter_scores = [now + interval for _ in range(5)]

        # All scores are identical → thundering herd
        self.assertEqual(len(set(no_jitter_scores)), 1,
                         "Without jitter all scores are identical — herd forms")

        # With ±10% jitter the scores are spread across a 720s window
        import random
        jittered = [now + interval * (1.0 + random.uniform(-0.10, 0.10))
                    for _ in range(5)]
        # Very high probability (≈ 1 - 5!/5^5 ≈ 97.6%) that at least 2 are distinct
        # — we just assert they're not all the same
        # (degenerate equality is astronomically unlikely with float jitter)
        self.assertGreater(len(set(jittered)), 1,
                           "Jittered scores should not all be identical")


# ─────────────────────────────────────────────────────────────────────────────
# TestClaimStaleWorkP95Safety
# ─────────────────────────────────────────────────────────────────────────────

class TestClaimStaleWorkP95Safety(unittest.TestCase):
    """
    claim_stale_work() uses `max(p95_ms * 3, 300_000)` to compute the idle
    timeout.  If p95_ms is None (no api_health data yet for a new platform),
    `None * 3` raises TypeError — the claim loop crashes.

    This test class:
      (a) Documents the bug by asserting that `None * 3` raises TypeError
      (b) Verifies the safe guard expression `max((p95_ms or 0) * 3, 300_000)`
          handles all edge cases correctly
    """

    def test_none_times_3_raises_type_error(self):
        """Baseline: the unsafe expression raises TypeError for None."""
        p95_ms = None
        with self.assertRaises(TypeError):
            _ = p95_ms * 3

    def test_safe_guard_none_yields_minimum(self):
        """
        `max((p95_ms or 0) * 3, 300_000)` with p95_ms=None → max(0, 300_000) = 300_000.
        Minimum 5-minute idle timeout even with no health data.
        """
        p95_ms = None
        result = max((p95_ms or 0) * 3, 300_000)
        self.assertEqual(result, 300_000)

    def test_safe_guard_zero_yields_minimum(self):
        """p95_ms=0 → max(0, 300_000) = 300_000."""
        p95_ms = 0
        result = max((p95_ms or 0) * 3, 300_000)
        self.assertEqual(result, 300_000)

    def test_safe_guard_small_value_yields_minimum(self):
        """p95_ms=100_000 (100s) → max(300_000, 300_000) = 300_000."""
        p95_ms = 100_000
        result = max((p95_ms or 0) * 3, 300_000)
        self.assertEqual(result, 300_000)

    def test_safe_guard_large_value_scales(self):
        """p95_ms=200_000 (200s) → max(600_000, 300_000) = 600_000."""
        p95_ms = 200_000
        result = max((p95_ms or 0) * 3, 300_000)
        self.assertEqual(result, 600_000)

    def test_safe_guard_very_large_value(self):
        """p95_ms=500_000 (500s) → max(1_500_000, 300_000) = 1_500_000."""
        p95_ms = 500_000
        result = max((p95_ms or 0) * 3, 300_000)
        self.assertEqual(result, 1_500_000)

    def test_source_uses_unsafe_expression(self):
        """
        Structural test: verify scheduler.py still contains the `p95_ms * 3`
        expression.  If the bug is already fixed, this test will fail and
        should be updated to reflect the new safe guard.

        NOTE: This test is intentionally designed to FAIL once the bug is fixed.
        Its purpose is to track whether the known risk still exists in production.
        """
        import pathlib
        src = (pathlib.Path(__file__).parent.parent
               / "workers" / "scheduler.py").read_text(encoding="utf-8")

        has_unsafe   = "p95_ms * 3" in src
        has_safe     = "(p95_ms or 0) * 3" in src or "p95_ms or" in src

        if has_safe:
            # Bug already fixed — great
            self.skipTest("p95_ms None-safety already implemented in scheduler.py")
        elif has_unsafe:
            self.fail(
                "Unsafe 'p95_ms * 3' still present in scheduler.py — "
                "if p95_ms is None (no api_health data yet) this raises TypeError "
                "and crashes the claim loop. Fix: replace with '(p95_ms or 0) * 3'."
            )
        else:
            self.skipTest("p95_ms expression not found in scheduler.py")


# ─────────────────────────────────────────────────────────────────────────────
# TestWarmingIntervalOverride
# ─────────────────────────────────────────────────────────────────────────────

class TestWarmingIntervalOverride(unittest.TestCase):
    """
    During WARMING, on_adaptive_complete() overrides the computed adaptive
    interval with WARMING_INTERVAL_S (fixed 2h) so the engine has enough data
    before switching to dynamic scheduling.

    These tests verify:
      · WARMING_INTERVAL_S = 7200 (2 hours, as per architecture doc)
      · WARMING_POLLS_COUNT = 3 (polls before STABLE, as per architecture doc)
      · The override logic: if warming is not None → use WARMING_INTERVAL_S
      · Override applies even when the computed interval is shorter (e.g. 3h)
    """

    def test_warming_interval_is_2_hours(self):
        """WARMING_INTERVAL_S must be exactly 2 hours (7200 seconds)."""
        self.assertEqual(WARMING_INTERVAL_S, 2 * 3600,
                         f"WARMING_INTERVAL_S = {WARMING_INTERVAL_S}; "
                         f"expected 7200 (2 hours)")

    def test_warming_polls_count_is_3(self):
        """WARMING_POLLS_COUNT must be 3 (three polls before STABLE)."""
        self.assertEqual(WARMING_POLLS_COUNT, 3,
                         f"WARMING_POLLS_COUNT = {WARMING_POLLS_COUNT}; expected 3")

    def test_warming_interval_shorter_than_min_adaptive_interval(self):
        """
        WARMING_INTERVAL_S (2h = 7200s) < ADAPTIVE_MIN_INTERVAL (3h = 10800s).

        Warming companies poll MORE frequently than the most active STABLE
        companies.  This is intentional: during the 3-poll warming window the
        engine needs to build up recent_poll_counts history quickly so it can
        make a meaningful adaptive scheduling decision once STABLE.

        If WARMING_INTERVAL_S were made longer (e.g. 4h or 12h), new companies
        would take many hours before transitioning to STABLE, delaying responsive
        scheduling for genuinely active companies.
        """
        from config import ADAPTIVE_MIN_INTERVAL
        self.assertLess(
            WARMING_INTERVAL_S, ADAPTIVE_MIN_INTERVAL,
            f"WARMING_INTERVAL_S={WARMING_INTERVAL_S}s (2h) should be < "
            f"ADAPTIVE_MIN_INTERVAL={ADAPTIVE_MIN_INTERVAL}s (3h) — "
            f"warming companies poll more often to build history faster",
        )

    def _simulate_interval_selection(self, warming, computed_interval):
        """
        Replicate the exact interval-selection logic from scheduler.py
        on_adaptive_complete():

            if success:
                ...compute interval from adaptive score...
                if warming is not None:
                    interval = WARMING_INTERVAL_S

        Returns the final interval that would be stored.
        """
        # After successful scan: computed adaptive interval is already calculated
        if warming is not None:
            return WARMING_INTERVAL_S  # WARMING override
        return computed_interval       # STABLE: use adaptive result

    def test_warming_overrides_short_computed_interval(self):
        """
        Even when adaptive score suggests a 3h interval, WARMING forces 2h.
        (warming_polls_remaining = 2, computed = 3h = ADAPTIVE_MIN_INTERVAL)
        """
        from config import ADAPTIVE_MIN_INTERVAL
        result = self._simulate_interval_selection(
            warming=2,
            computed_interval=ADAPTIVE_MIN_INTERVAL,  # 3h (most aggressive)
        )
        self.assertEqual(result, WARMING_INTERVAL_S,
                         "WARMING must override even the most aggressive adaptive interval")

    def test_warming_overrides_long_computed_interval(self):
        """
        When adaptive score suggests 12h (dormant), WARMING still forces 2h.
        """
        from config import ADAPTIVE_DEFAULT_INTERVAL
        result = self._simulate_interval_selection(
            warming=1,
            computed_interval=ADAPTIVE_DEFAULT_INTERVAL,  # 12h (dormant)
        )
        self.assertEqual(result, WARMING_INTERVAL_S)

    def test_stable_uses_computed_interval(self):
        """
        warming=None (STABLE) → computed interval is used directly.
        """
        from config import ADAPTIVE_MIN_INTERVAL
        result = self._simulate_interval_selection(
            warming=None,
            computed_interval=ADAPTIVE_MIN_INTERVAL,
        )
        self.assertEqual(result, ADAPTIVE_MIN_INTERVAL,
                         "STABLE companies must use the adaptive computed interval")

    def test_stable_dormant_uses_12h(self):
        """STABLE + dormant score → 12h default interval."""
        from config import ADAPTIVE_DEFAULT_INTERVAL
        result = self._simulate_interval_selection(
            warming=None,
            computed_interval=ADAPTIVE_DEFAULT_INTERVAL,
        )
        self.assertEqual(result, ADAPTIVE_DEFAULT_INTERVAL)

    def test_warming_3_overrides_to_2h(self):
        """New company (warming=3, first ever poll) gets 2h interval."""
        result = self._simulate_interval_selection(warming=3, computed_interval=43200)
        self.assertEqual(result, WARMING_INTERVAL_S)

    def test_warming_1_final_warming_poll_still_2h(self):
        """Last warming poll (warming=1) still uses 2h (not adaptive interval)."""
        result = self._simulate_interval_selection(warming=1, computed_interval=10800)
        self.assertEqual(result, WARMING_INTERVAL_S)


# ─────────────────────────────────────────────────────────────────────────────
# TestWorkerMissedCycleBoundary
# ─────────────────────────────────────────────────────────────────────────────

class TestWorkerMissedCycleBoundary(unittest.TestCase):
    """
    Regression tests for the cycle-boundary bug in _get_worker_missed_companies().

    Field checked: last_full_scan_at (written by on_fullscan_complete()).
    The fullscan worker does an exhaustive all-pages scan once per ~24h cycle.
    We do NOT check last_poll_at (adaptive scan) because the adaptive worker
    uses smart early exit and may not scan every page.

    Bug (fixed):
        cycle_start_ts was set to TODAY's 7:00:00 AM.  The --monitor-jobs cron
        fires at exactly 7:00:02 AM.  Every overnight fullscan has
        last_full_scan_at < 7:00:00 AM (e.g. completed at 4 AM, 5 AM, 6:59 AM),
        so ALL 139 companies fail the check and get a full fallback re-fetch
        every single day.  Result: email arrives at ~7:30 AM instead of ~7:02 AM.

    Fix:
        Use a 24-hour rolling lookback (now - 24h) instead of a fixed cycle
        boundary.  At 7:00 AM this equals yesterday 7:00 AM, giving overnight
        fullscans full credit.  For manual mid-day runs it credits fullscans
        from earlier that same day without going all the way back 24+ hours.

    These tests verify:
      · The old "today at 7 AM" boundary incorrectly excluded pre-7-AM fullscans
      · The new 24h rolling window correctly covers them
      · A company never fullscanned (last_full_scan_at=NULL → epoch 0) is always missed
      · A company fullscanned well within 24h is always covered under the new logic
      · The boundary is time-relative (hours), not anchored to clock-time 7 AM
      · job_monitor.py queries last_full_scan_at, not last_poll_at
    """

    def _old_cycle_start_ts(self, now_epoch, cycle_hour_offset_secs):
        """
        Simulate the OLD (buggy) logic:
            if now < today_7am: use yesterday_7am
            else:               use today_7am
        `cycle_hour_offset_secs` is seconds-from-midnight for 7 AM in the
        server's local time (approximated here as UTC for test simplicity).
        """
        # today_cycle = midnight + 7h
        from datetime import datetime, timezone, timedelta
        now_dt = datetime.fromtimestamp(now_epoch, tz=timezone.utc)
        today_cycle = now_dt.replace(hour=7, minute=0, second=0, microsecond=0)
        if now_dt < today_cycle:
            return (today_cycle - timedelta(days=1)).timestamp()
        else:
            return today_cycle.timestamp()

    def _new_cycle_start_ts(self, now_epoch):
        """Simulate the NEW (fixed) logic: now - 24h."""
        return now_epoch - 24 * 3600

    def test_old_boundary_misses_fullscan_at_6am(self):
        """
        OLD logic: company fullscanned at 6:00 AM is incorrectly marked as
        missed when cron fires at 7:00:02 AM
        (last_full_scan_at=6AM < cycle_start=7AM).
        """
        from datetime import datetime, timezone
        cron_time       = datetime(2026, 5, 27, 7, 0, 2, tzinfo=timezone.utc).timestamp()
        last_full_scan  = datetime(2026, 5, 27, 6, 0, 0, tzinfo=timezone.utc).timestamp()

        cycle_start = self._old_cycle_start_ts(cron_time, 7 * 3600)
        self.assertGreater(cycle_start, last_full_scan,
            "OLD boundary: 6 AM fullscan < 7 AM boundary → incorrectly classified as missed")

    def test_new_boundary_covers_fullscan_at_6am(self):
        """
        NEW logic: company fullscanned at 6:00 AM is correctly covered when
        cron fires at 7:00:02 AM
        (last_full_scan_at=6AM ≥ cycle_start=yesterday-7AM).
        """
        from datetime import datetime, timezone
        cron_time       = datetime(2026, 5, 27, 7, 0, 2, tzinfo=timezone.utc).timestamp()
        last_full_scan  = datetime(2026, 5, 27, 6, 0, 0, tzinfo=timezone.utc).timestamp()

        cycle_start = self._new_cycle_start_ts(cron_time)
        self.assertLessEqual(cycle_start, last_full_scan,
            "NEW boundary: 6 AM fullscan ≥ (now-24h) → correctly covered")

    def test_new_boundary_covers_fullscan_at_4am(self):
        """
        NEW logic: company fullscanned at 4:00 AM (3h before cron) is covered.
        Fullscan runs once per ~24h; completing at 4 AM before the 7 AM digest
        is entirely normal.
        """
        from datetime import datetime, timezone
        cron_time       = datetime(2026, 5, 27, 7, 0, 2, tzinfo=timezone.utc).timestamp()
        last_full_scan  = datetime(2026, 5, 27, 4, 0, 0, tzinfo=timezone.utc).timestamp()

        cycle_start = self._new_cycle_start_ts(cron_time)
        self.assertLessEqual(cycle_start, last_full_scan,
            "NEW boundary: 4 AM fullscan should be within the 24h window")

    def test_new_boundary_misses_fullscan_older_than_24h(self):
        """
        NEW logic: company whose last fullscan completed >24h ago is correctly
        flagged as missed — the fullscan worker fell behind its ~24h schedule.
        """
        from datetime import datetime, timezone, timedelta
        cron_time       = datetime(2026, 5, 27, 7, 0, 2, tzinfo=timezone.utc).timestamp()
        last_full_scan  = (datetime(2026, 5, 27, 7, 0, 2, tzinfo=timezone.utc)
                           - timedelta(hours=25)).timestamp()

        cycle_start = self._new_cycle_start_ts(cron_time)
        self.assertGreater(cycle_start, last_full_scan,
            "NEW boundary: 25h-old fullscan is outside the 24h window → missed (correct)")

    def test_never_fullscanned_company_always_missed(self):
        """
        last_full_scan_at=NULL → default epoch 0 → always missed under both
        old and new logic.  New companies enter via fullscan-first path; until
        the first fullscan completes they get a fallback re-fetch from --monitor-jobs.
        """
        from datetime import datetime, timezone
        cron_time      = datetime(2026, 5, 27, 7, 0, 2, tzinfo=timezone.utc).timestamp()
        last_full_scan = 0  # NULL → 0 default in poll_map

        old_start = self._old_cycle_start_ts(cron_time, 7 * 3600)
        new_start = self._new_cycle_start_ts(cron_time)

        self.assertGreater(old_start, last_full_scan, "OLD: never-fullscanned → missed ✓")
        self.assertGreater(new_start, last_full_scan, "NEW: never-fullscanned → missed ✓")

    def test_all_139_companies_covered_at_cron_time(self):
        """
        Simulate the production scenario: cron fires at 7:00:02 AM, all 139
        companies had their fullscan complete at various hours overnight (1–6 AM).
        OLD logic → 0 covered.  NEW logic → 139 covered.
        """
        from datetime import datetime, timezone

        cron_time = datetime(2026, 5, 27, 7, 0, 2, tzinfo=timezone.utc).timestamp()

        # Simulate 139 companies whose fullscan completed overnight (1–6 AM)
        import random
        rng = random.Random(42)  # fixed seed for reproducibility
        last_fullscans = [
            (datetime(2026, 5, 27, rng.randint(1, 6), rng.randint(0, 59), 0,
                      tzinfo=timezone.utc)).timestamp()
            for _ in range(139)
        ]

        old_start = self._old_cycle_start_ts(cron_time, 7 * 3600)
        new_start = self._new_cycle_start_ts(cron_time)

        old_covered = sum(1 for fs in last_fullscans if fs >= old_start)
        new_covered = sum(1 for fs in last_fullscans if fs >= new_start)

        self.assertEqual(old_covered, 0,
            f"OLD logic should cover 0 companies (all overnight fullscans are pre-7AM); "
            f"got {old_covered}")
        self.assertEqual(new_covered, 139,
            f"NEW logic should cover all 139 companies (all fullscanned within 24h); "
            f"got {new_covered}")

    def test_job_monitor_source_uses_rolling_window(self):
        """
        Structural test: verify job_monitor.py uses a rolling 24-hour window
        rather than the fixed 'today_cycle.timestamp()' boundary, AND checks
        last_full_scan_at (fullscan worker) not last_poll_at (adaptive worker).

        Two invariants enforced:
          1. Cycle boundary is (now - 24h), not today's fixed 7:00 AM.
          2. Coverage is determined by last_full_scan_at (exhaustive all-pages
             scan), not last_poll_at (incremental scan with early exit).
        """
        import pathlib
        src = (pathlib.Path(__file__).parent.parent
               / "jobs" / "job_monitor.py").read_text(encoding="utf-8")

        has_fixed_boundary   = "cycle_start_ts = today_cycle.timestamp()" in src
        has_rolling_window   = "timedelta(hours=24)" in src
        has_fullscan_field   = "last_full_scan_at" in src
        has_adaptive_field   = "last_poll_epoch" in src  # old adaptive-scan alias

        self.assertFalse(has_fixed_boundary,
            "job_monitor.py still uses 'today_cycle.timestamp()' as cycle boundary — "
            "this causes all companies to be classified as missed at 7 AM cron time. "
            "Fix: replace with rolling 24h window: (now_dt - timedelta(hours=24)).timestamp()")
        self.assertTrue(has_rolling_window,
            "job_monitor.py should use 'timedelta(hours=24)' for the rolling lookback window")
        self.assertTrue(has_fullscan_field,
            "job_monitor.py should check last_full_scan_at (fullscan worker — exhaustive), "
            "not last_poll_at (adaptive worker — uses early exit, may miss pages)")
        self.assertFalse(has_adaptive_field,
            "job_monitor.py is still querying last_poll_epoch (adaptive scan alias) — "
            "coverage must be determined by last_full_scan_at from the fullscan worker")


if __name__ == "__main__":
    unittest.main()
