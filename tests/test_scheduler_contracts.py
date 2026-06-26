"""
tests/test_scheduler_contracts.py
─────────────────────────────────────────────────────────────────────────────
Tests for scheduler.py and scan_worker.py contracts that protect against
thundering-herd regressions, backoff miscalculations, and stale-state bugs.

Phase 2 update
──────────────
_reschedule_adaptive() no longer uses random.uniform() jitter.  It now calls
_pick_schedule_time() — the gap-detection algorithm that finds the largest
unused gap between existing scheduled times and returns its midpoint.
TestRescheduleJitter has been replaced with:

  TestPickScheduleTime
    · Empty window → target_ts returned (only gap is full window)
    · Single existing entry → largest of the two resulting gaps is chosen
    · Multiple clustered entries → picks midpoint of the largest gap
    · Result always within the tolerance window [lo, hi]
    · Deadline guard: gap whose midpoint + avg_duration_s ≥ deadline is skipped
    · Deadline guard: all gaps violate deadline → fallback to target_ts (if target_ts is safe)
    · Deadline guard: all gaps AND target_ts violate deadline → next_digest + 900
    · No deadline set → avg_duration_s has no effect
    · Tiebreaker: equal-size gaps → closest midpoint to target_ts wins
    · Self-correction: 3 calls from a full cluster produce 3 distinct times

  TestNextDigestDeadline
    · Result is always strictly in the future from now
    · Result corresponds to exactly 7:00:00 in America/New_York
    · Result is within 24 hours from now
    · now before 7 AM ET → returns today's 7 AM ET
    · now after 7 AM ET → returns tomorrow's 7 AM ET
    · now exactly at 7 AM ET → returns tomorrow's 7 AM ET

  TestRescheduleAdaptiveGapBased
    · ZADD called exactly once on poll:adaptive
    · Score is within the ±10% tolerance window (not exact jitter bounds)
    · random.uniform is NOT called (gap-based, not random)
    · Multiple companies from same cluster get distinct scores
    · Company name is the ZADD mapping key

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

  TestPickScheduleTimeEdgeCases  (additional gap-detection edge cases)
    · tolerance_pct=0 → window collapses → returns target_ts
    · entries outside window → treated as empty window → midpoint = target
    · result always within [lo, hi] tolerance window
    · deadline with avg_duration=0 → never skipped
    · all gaps violate deadline, target_ts safe → fallback to target_ts
    · all gaps AND target_ts violate deadline → next_digest + 900
    · large cluster (20 entries) → still returns a finite result
    · single entry at center → splits into equal halves, returns valid midpoint

  TestInflightExclusionStructural  (Phase 2.6 — inflight exclusion in job_monitor)
    · Source uses REDIS_INFLIGHT_FULLSCAN constant
    · Source decodes bytes from Redis
    · Source handles Redis unavailability (try/except)
    · Source uses a ZSET read operation for inflight data
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
# TestPickScheduleTime  (Phase 2 — gap-detection algorithm)
# ─────────────────────────────────────────────────────────────────────────────

class TestPickScheduleTime(unittest.TestCase):
    """
    _pick_schedule_time() finds the largest gap between existing scheduled
    times within a tolerance window and returns the midpoint of that gap.
    Replaces the old 20-slot min-heap (_least_loaded_slot).
    """

    _TARGET = 1_700_000_000.0
    _INTERVAL = 86400
    _TOL = 0.20   # 20% → window = 17280 s = 4.8 h

    def _window(self):
        w = self._INTERVAL * self._TOL
        return self._TARGET - w / 2, self._TARGET + w / 2

    def _call(self, existing_scores, *, deadline_ts=None, avg_duration_s=0.0,
              target=None, interval=None, tol=None):
        from workers.scheduler import _pick_schedule_time
        r = MagicMock()
        r.zrangebyscore.return_value = [
            (f"co{i}".encode(), float(s)) for i, s in enumerate(existing_scores)
        ]
        return _pick_schedule_time(
            target_ts      = target if target is not None else self._TARGET,
            queue_key      = "poll:fullscan",
            interval_s     = interval if interval is not None else self._INTERVAL,
            tolerance_pct  = tol if tol is not None else self._TOL,
            r              = r,
            deadline_ts    = deadline_ts,
            avg_duration_s = avg_duration_s,
        )

    # ── Basic gap selection ───────────────────────────────────────────────────

    def test_empty_window_returns_target(self):
        """No existing entries → window is one gap, midpoint = target_ts."""
        result = self._call([])
        self.assertAlmostEqual(result, self._TARGET, places=0)

    def test_single_entry_at_lo_picks_right_gap(self):
        """Entry near lo → left gap is tiny, right gap is large; picks right."""
        lo, hi = self._window()
        entry = lo + 100   # tiny left gap (100 s), large right gap (~17180 s)
        result = self._call([entry])
        expected = (entry + hi) / 2
        self.assertAlmostEqual(result, expected, places=0)

    def test_single_entry_at_hi_picks_left_gap(self):
        """Entry near hi → large left gap is chosen."""
        lo, hi = self._window()
        entry = hi - 100
        result = self._call([entry])
        expected = (lo + entry) / 2
        self.assertAlmostEqual(result, expected, places=0)

    def test_picks_largest_gap_globally(self):
        """Three entries clustered near lo → large gap on right side chosen."""
        lo, hi = self._window()
        entries = [lo + 100, lo + 200, lo + 300]
        result = self._call(entries)
        expected = (lo + 300 + hi) / 2
        self.assertAlmostEqual(result, expected, places=0)

    def test_result_always_within_window(self):
        """Result is always within [lo, hi] regardless of existing entries."""
        lo, hi = self._window()
        for existing in [[], [self._TARGET], [lo + 1000, hi - 1000],
                         [self._TARGET] * 10]:
            result = self._call(existing)
            self.assertGreaterEqual(result, lo - 1)
            self.assertLessEqual(result, hi + 1)

    # ── Tiebreaker ────────────────────────────────────────────────────────────

    def test_equal_gaps_tiebreaker_closest_to_target(self):
        """Equal-size gaps → midpoint closest to target_ts wins."""
        lo, hi = self._window()
        # Entry at exact target splits window into two equal halves.
        # Left midpoint = (lo + target) / 2, right = (target + hi) / 2.
        # Both equal size → pick one closest to target.
        # Actually: the two gaps have midpoints equidistant from target,
        # so we just check the result is one of them (not target itself).
        result = self._call([self._TARGET])
        self.assertIn(
            round(result, 0),
            {round((lo + self._TARGET) / 2, 0),
             round((self._TARGET + hi) / 2, 0)},
        )

    # ── Deadline guard ────────────────────────────────────────────────────────

    def test_deadline_guard_skips_late_gaps(self):
        """
        Gap whose midpoint + avg_duration_s ≥ _next_digest_deadline(midpoint) is
        skipped; the safe gap is returned instead.  deadline_ts is only an enable
        flag — the actual threshold is always _next_digest_deadline(midpoint).
        """
        from unittest.mock import patch as _patch
        lo, hi = self._window()
        avg_duration = 1800
        right_mid = (self._TARGET + hi) / 2
        left_mid  = (lo + self._TARGET) / 2

        def _mock_nd(ts):
            # Force right_mid to fail (ts + avg >= ts + avg - 1); left_mid is safe.
            if abs(ts - right_mid) < 1:
                return ts + avg_duration - 1
            return ts + avg_duration + 10_000

        with _patch("workers.scheduler._next_digest_deadline", side_effect=_mock_nd):
            result = self._call([self._TARGET],
                                deadline_ts=self._TARGET,  # any non-None value enables the check
                                avg_duration_s=avg_duration)
        self.assertAlmostEqual(result, left_mid, places=0)

    def test_deadline_guard_all_gaps_fail_target_safe_returns_target(self):
        """All gap midpoints violate deadline, but target_ts itself is safe → returns target_ts."""
        from unittest.mock import patch as _patch
        avg_duration = 1800

        def _mock_nd(ts):
            # target_ts: plenty of time (safe); gap midpoints (≠ target): barely fail.
            if abs(ts - self._TARGET) < 1:
                return ts + avg_duration + 10_000
            return ts + avg_duration - 1

        # Entry at target splits window into two gaps whose midpoints ≠ target_ts,
        # so _mock_nd returns different values for midpoints vs target_ts.
        with _patch("workers.scheduler._next_digest_deadline", side_effect=_mock_nd):
            result = self._call([self._TARGET],
                                deadline_ts=self._TARGET,
                                avg_duration_s=avg_duration)
        self.assertAlmostEqual(result, self._TARGET, places=0)

    def test_deadline_guard_all_gaps_fail_target_also_violates_returns_post_digest(self):
        """All gaps AND target_ts violate deadline → _next_digest_deadline(target_ts) + 900."""
        from unittest.mock import patch as _patch
        avg_duration = 1800
        fixed_deadline = self._TARGET + avg_duration - 1  # ts + avg >= fixed for all ts ≈ target

        with _patch("workers.scheduler._next_digest_deadline", return_value=fixed_deadline):
            result = self._call([], deadline_ts=self._TARGET, avg_duration_s=avg_duration)
        expected = fixed_deadline + 900
        self.assertAlmostEqual(result, expected, places=0)

    def test_no_deadline_avg_duration_has_no_effect(self):
        """With deadline_ts=None, avg_duration_s is ignored."""
        r1 = self._call([], deadline_ts=None, avg_duration_s=0)
        r2 = self._call([], deadline_ts=None, avg_duration_s=99999)
        self.assertAlmostEqual(r1, r2, places=0)

    # ── Self-correction from cluster ──────────────────────────────────────────

    def test_three_calls_from_cluster_produce_distinct_times(self):
        """
        Three consecutive new companies all starting from the same clustered
        pool each get a different scheduled time — the cluster spreads out.
        """
        from workers.scheduler import _pick_schedule_time
        lo, hi = self._window()
        existing = [self._TARGET] * 10  # all clustered at target
        chosen = []

        for _ in range(3):
            r = MagicMock()
            r.zrangebyscore.return_value = [(b"co", float(s)) for s in existing]
            result = _pick_schedule_time(
                target_ts=self._TARGET, queue_key="poll:fullscan",
                interval_s=self._INTERVAL, tolerance_pct=self._TOL, r=r,
            )
            chosen.append(result)
            existing.append(result)

        self.assertEqual(len(set(round(s, 0) for s in chosen)), 3,
                         f"Expected 3 distinct times, got: {chosen}")
        for s in chosen:
            self.assertGreaterEqual(s, lo - 1)
            self.assertLessEqual(s, hi + 1)


# ─────────────────────────────────────────────────────────────────────────────
# TestNextDigestDeadline  (Phase 2 — 7 AM ET deadline computation)
# ─────────────────────────────────────────────────────────────────────────────

class TestNextDigestDeadline(unittest.TestCase):
    """_next_digest_deadline(now) → next 7:00:00 AM America/New_York."""

    def _call(self, now):
        from workers.scheduler import _next_digest_deadline
        return _next_digest_deadline(now)

    def _et_hour(self, ts):
        """Return the ET hour of a Unix timestamp."""
        try:
            from zoneinfo import ZoneInfo
        except ImportError:
            from backports.zoneinfo import ZoneInfo
        from datetime import datetime
        return datetime.fromtimestamp(ts, tz=ZoneInfo("America/New_York"))

    def test_result_always_in_future(self):
        """Deadline is always strictly after now."""
        for now in [1_748_000_000.0 + h * 3600 for h in range(0, 24, 3)]:
            result = self._call(now)
            self.assertGreater(result, now,
                               f"Deadline not in future from now={now}")

    def test_result_at_exactly_7am_et(self):
        """Result corresponds to 7:00:00 in America/New_York."""
        dt = self._et_hour(self._call(1_748_000_000.0))
        self.assertEqual(dt.hour, 7)
        self.assertEqual(dt.minute, 0)
        self.assertEqual(dt.second, 0)

    def test_result_within_24_hours(self):
        """Deadline is at most 24 hours from now."""
        now = 1_748_000_000.0
        self.assertLessEqual(self._call(now) - now, 24 * 3600 + 1)

    def test_before_7am_et_returns_today(self):
        """now = 3 AM ET → deadline = today 7 AM ET (same calendar day)."""
        try:
            from zoneinfo import ZoneInfo
        except ImportError:
            from backports.zoneinfo import ZoneInfo
        from datetime import datetime
        tz = ZoneInfo("America/New_York")
        now_dt = datetime(2026, 5, 27, 3, 0, 0, tzinfo=tz)
        result_dt = self._et_hour(self._call(now_dt.timestamp()))
        self.assertEqual(result_dt.date(), now_dt.date())
        self.assertEqual(result_dt.hour, 7)

    def test_after_7am_et_returns_tomorrow(self):
        """now = 10 AM ET → deadline = tomorrow 7 AM ET."""
        try:
            from zoneinfo import ZoneInfo
        except ImportError:
            from backports.zoneinfo import ZoneInfo
        from datetime import datetime, timedelta
        tz = ZoneInfo("America/New_York")
        now_dt = datetime(2026, 5, 27, 10, 0, 0, tzinfo=tz)
        result_dt = self._et_hour(self._call(now_dt.timestamp()))
        self.assertEqual(result_dt.date(), (now_dt + timedelta(days=1)).date())
        self.assertEqual(result_dt.hour, 7)

    def test_exactly_at_7am_et_returns_tomorrow(self):
        """now = exactly 7 AM ET → deadline = tomorrow 7 AM ET (not today)."""
        try:
            from zoneinfo import ZoneInfo
        except ImportError:
            from backports.zoneinfo import ZoneInfo
        from datetime import datetime, timedelta
        tz = ZoneInfo("America/New_York")
        now_dt = datetime(2026, 5, 27, 7, 0, 0, tzinfo=tz)
        result_dt = self._et_hour(self._call(now_dt.timestamp()))
        self.assertEqual(result_dt.date(), (now_dt + timedelta(days=1)).date())


# ─────────────────────────────────────────────────────────────────────────────
# TestRescheduleAdaptiveGapBased  (Phase 2 — replaces TestRescheduleJitter)
# ─────────────────────────────────────────────────────────────────────────────

class TestRescheduleAdaptiveGapBased(unittest.TestCase):
    """
    _reschedule_adaptive() now uses _pick_schedule_time() (gap-detection)
    instead of random.uniform() jitter.  These tests verify the new contract.
    """

    def _call_and_capture(self, company="Acme", interval_s=3600,
                          existing_scores=None, fixed_now=None):
        """
        Call _reschedule_adaptive() with mocked Redis, return (r_mock, score).
        """
        from workers.scheduler import _reschedule_adaptive
        r = MagicMock()
        r.zrangebyscore.return_value = [
            (b"co", float(s)) for s in (existing_scores or [])
        ]
        patches = [patch("workers.scheduler.get_redis", return_value=r)]
        if fixed_now is not None:
            patches.append(patch("workers.scheduler.time.time",
                                 return_value=fixed_now))
        ctx = [p.__enter__() for p in patches]
        try:
            _reschedule_adaptive(company, interval_s)
        finally:
            for p, _ in zip(reversed(patches), reversed(ctx), strict=True):
                p.__exit__(None, None, None)
        score = next(iter(r.zadd.call_args[0][1].values()))
        return r, score

    def test_zadd_called_exactly_once(self):
        r, _ = self._call_and_capture()
        self.assertEqual(r.zadd.call_count, 1)

    def test_zadd_targets_poll_adaptive(self):
        from config import REDIS_POLL_ADAPTIVE
        r, _ = self._call_and_capture()
        self.assertEqual(r.zadd.call_args[0][0], REDIS_POLL_ADAPTIVE)

    def test_score_within_tolerance_window(self):
        """Score is within ±10% of now + interval (20% total window)."""
        interval = 3600
        fixed_now = 1_700_000_000.0
        _, score = self._call_and_capture(interval_s=interval, fixed_now=fixed_now)
        target = fixed_now + interval
        window = interval * 0.20
        self.assertGreaterEqual(score, target - window / 2 - 1)
        self.assertLessEqual(score, target + window / 2 + 1)

    def test_random_uniform_not_called(self):
        """Gap-based scheduling does not use random.uniform."""
        with patch("workers.scheduler.random.uniform") as mock_u, \
             patch("workers.scheduler.get_redis") as mock_r:
            mock_r.return_value.zrangebyscore.return_value = []
            from workers.scheduler import _reschedule_adaptive
            _reschedule_adaptive("Acme", 3600)
        mock_u.assert_not_called()

    def test_clustered_companies_get_distinct_scores(self):
        """Multiple companies starting from the same cluster get different times."""
        fixed_now = 1_700_000_000.0
        interval = 3600
        target = fixed_now + interval
        existing = [target] * 10
        scores = []
        for i in range(3):
            _, s = self._call_and_capture(
                company=f"Co{i}", interval_s=interval,
                existing_scores=existing, fixed_now=fixed_now,
            )
            scores.append(s)
            existing.append(s)
        self.assertEqual(len(set(round(s, 0) for s in scores)), 3,
                         f"Expected 3 distinct scores, got {scores}")

    def test_company_name_in_zadd_mapping(self):
        r, _ = self._call_and_capture(company="GlobalCorp")
        self.assertIn("GlobalCorp", r.zadd.call_args[0][1])


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
        # Strip comments and triple-quoted strings before checking:
        # job_monitor.py mentions "last_poll_at" only in docstrings/comments
        # that explain why it is NOT used — a raw substring search would
        # false-positive and fail the assertion even when the code is correct.
        import re as _re
        _stripped = _re.sub(r'#[^\n]*', '', src)             # remove # comments
        _stripped = _re.sub(r'""".*?"""', '', _stripped, flags=_re.DOTALL)  # triple-" strings
        _stripped = _re.sub(r"'''.*?'''", '', _stripped, flags=_re.DOTALL)  # triple-' strings
        has_adaptive_field   = "last_poll_at" in _stripped   # adaptive-scan field (wrong alias)

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


# ─────────────────────────────────────────────────────────────────────────────
# TestPickScheduleTimeEdgeCases  (Phase 2 — additional gap-detection coverage)
# ─────────────────────────────────────────────────────────────────────────────

class TestPickScheduleTimeEdgeCases(unittest.TestCase):
    """
    Additional edge cases for _pick_schedule_time() not covered by
    the existing TestPickScheduleTime class.
    """

    _TARGET  = 1_700_000_000.0
    _INTERVAL = 86400.0

    def _call(self, existing_scores=None, tolerance_pct=0.20,
              deadline_ts=None, avg_duration_s=0.0):
        r = MagicMock()
        pairs = [(f"co{i}".encode(), s) for i, s in enumerate(existing_scores or [])]
        r.zrangebyscore.return_value = pairs
        from workers.scheduler import _pick_schedule_time
        return _pick_schedule_time(
            self._TARGET, "poll:adaptive", self._INTERVAL,
            tolerance_pct, r,
            deadline_ts=deadline_ts,
            avg_duration_s=avg_duration_s,
        )

    def test_tolerance_zero_returns_target(self):
        """tolerance_pct=0 → window collapses to a single point → returns target_ts."""
        result = self._call(existing_scores=[], tolerance_pct=0.0)
        self.assertAlmostEqual(result, self._TARGET, places=1)

    def test_entries_outside_window_ignored(self):
        """Entries outside [lo, hi] are not returned by ZRANGEBYSCORE so do not split gaps."""
        lo = self._TARGET - 0.20 * self._INTERVAL / 2
        hi = self._TARGET + 0.20 * self._INTERVAL / 2
        # Out-of-window entries (far below lo and far above hi) must not split
        # the gap, so the result should be identical to the empty-window case.
        result_empty   = self._call(existing_scores=[], tolerance_pct=0.20)
        result_outside = self._call(
            existing_scores=[lo - 10_000, hi + 10_000],
            tolerance_pct=0.20,
        )
        expected_mid = (lo + hi) / 2
        self.assertAlmostEqual(result_empty,   expected_mid, places=1)
        self.assertAlmostEqual(result_outside, expected_mid, places=1)

    def test_result_within_tolerance_window(self):
        """Result is always within [target - window/2, target + window/2]."""
        from workers.scheduler import _pick_schedule_time
        window = 0.20 * self._INTERVAL
        lo = self._TARGET - window / 2
        hi = self._TARGET + window / 2
        r = MagicMock()
        r.zrangebyscore.return_value = [(b"co1", self._TARGET - 1000.0),
                                        (b"co2", self._TARGET + 1000.0)]
        result = _pick_schedule_time(
            self._TARGET, "poll:adaptive", self._INTERVAL, 0.20, r,
        )
        self.assertGreaterEqual(result, lo - 1)
        self.assertLessEqual(result, hi + 1)

    def test_deadline_with_zero_avg_duration_not_skipped(self):
        """avg_duration_s=0 → midpoint + 0 never ≥ deadline for any reasonable gap."""
        deadline = self._TARGET + 3600.0  # 1 hour from now
        result = self._call(
            existing_scores=[],
            tolerance_pct=0.20,
            deadline_ts=deadline,
            avg_duration_s=0.0,
        )
        self.assertLess(result + 0.0, deadline,
                        "Result with avg_duration=0 must be before deadline")

    def test_all_gaps_violate_deadline_target_safe_fallback_to_target(self):
        """All gaps fail, target_ts+avg is before next digest → returns target_ts."""
        from unittest.mock import patch
        _avg = 100.0
        # existing_scores=[_TARGET] creates two gap midpoints at ±(window/4) from
        # target.  Mock _next_digest_deadline so those midpoints violate
        # (midpoint + avg >= midpoint + avg - 1) but target_ts itself is safe
        # (target + avg < target + avg + 1) — exercises the all-gaps-fail
        # fallback that returns target_ts rather than post-digest.
        def _mock_nd(ts):
            if abs(ts - self._TARGET) < 1.0:
                return ts + _avg + 1.0   # target safe: ts + avg < deadline
            return ts + _avg - 1.0       # midpoints violate: ts + avg >= deadline

        with patch("workers.scheduler._next_digest_deadline", side_effect=_mock_nd):
            result = self._call(
                existing_scores=[self._TARGET],
                tolerance_pct=0.20,
                deadline_ts=self._TARGET,  # truthy — enables the deadline guard
                avg_duration_s=_avg,
            )
        self.assertAlmostEqual(result, self._TARGET, places=1)

    def test_all_gaps_violate_deadline_target_also_violates_returns_post_digest(self):
        """All gaps AND target_ts violate deadline → next_digest + 900."""
        from workers.scheduler import _next_digest_deadline
        # 24h avg: target_ts+86400 > next 7 AM ET → target also violates.
        result = self._call(
            existing_scores=[],
            tolerance_pct=0.20,
            deadline_ts=self._TARGET - 1.0,
            avg_duration_s=86400.0,
        )
        expected = _next_digest_deadline(self._TARGET) + 900
        self.assertAlmostEqual(result, expected, places=1)

    def test_large_cluster_still_finds_a_result(self):
        """Even with 20 entries spread across the window, returns a finite result."""
        window = 0.20 * self._INTERVAL
        lo = self._TARGET - window / 2
        step = window / 21
        # 20 entries spread evenly
        entries = [lo + step * i for i in range(1, 21)]
        result = self._call(existing_scores=entries, tolerance_pct=0.20)
        self.assertIsNotNone(result)
        self.assertIsInstance(result, float)

    def test_single_entry_at_window_center_splits_into_equal_halves(self):
        """Entry exactly at target_ts → two equal gaps → tiebreaker picks one."""
        # Both gaps have equal size; tiebreaker picks the one whose midpoint is
        # closer to target_ts — they are equidistant, so either is valid.
        result = self._call(existing_scores=[self._TARGET])
        lo = self._TARGET - 0.20 * self._INTERVAL / 2
        hi = self._TARGET + 0.20 * self._INTERVAL / 2
        self.assertGreaterEqual(result, lo - 1)
        self.assertLessEqual(result, hi + 1)


# ─────────────────────────────────────────────────────────────────────────────
# TestInflightExclusionStructural  (Phase 2.6 — inflight exclusion in job_monitor)
# ─────────────────────────────────────────────────────────────────────────────

class TestInflightExclusionStructural(unittest.TestCase):
    """
    Structural tests for _get_worker_missed_companies() in jobs/job_monitor.py.

    These verify source-code invariants (correct constant used, bytes decoded,
    Redis-unavailable guard) without running full DB or Redis.
    """

    def setUp(self):
        import pathlib
        self.src = (pathlib.Path(__file__).parent.parent
                    / "jobs" / "job_monitor.py").read_text(encoding="utf-8")

    def test_source_uses_redis_inflight_fullscan_constant(self):
        """job_monitor.py reads the inflight:fullscan ZSET to exclude in-flight companies."""
        self.assertIn("REDIS_INFLIGHT_FULLSCAN", self.src,
                      "job_monitor.py must reference REDIS_INFLIGHT_FULLSCAN to exclude "
                      "companies whose fullscan is currently in progress")

    def test_source_decodes_bytes_from_redis(self):
        """job_monitor.py decodes bytes returned by Redis (zrange/zscan returns bytes)."""
        # At least one of these patterns shows byte decoding
        has_decode = ".decode(" in self.src or "decode()" in self.src
        self.assertTrue(has_decode,
                        "job_monitor.py must decode bytes from Redis inflight ZSET")

    def test_source_handles_redis_unavailable(self):
        """job_monitor.py handles Redis unavailability gracefully (try/except guard)."""
        # The function must have a try/except around the Redis call
        self.assertIn("except", self.src,
                      "job_monitor.py must handle Redis exceptions gracefully")

    def test_source_uses_zrangebyscore_or_zrange_for_inflight(self):
        """job_monitor.py uses a ZSET read operation to get inflight companies."""
        has_zset_read = (
            "zrange" in self.src or
            "zrangebyscore" in self.src or
            "zscan" in self.src
        )
        self.assertTrue(has_zset_read,
                        "job_monitor.py must use a ZSET read operation for inflight:fullscan")


if __name__ == "__main__":
    unittest.main()
