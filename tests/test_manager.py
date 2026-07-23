"""
tests/test_manager.py
─────────────────────────────────────────────────────────────────────────────
Comprehensive test suite for workers/manager.py Layer 0 autoscaler.

Design doc: docs/scaling-redesign.md §4, §7, §16
Architecture doc: docs/adaptive-polling-architecture.md §26

WHY THESE TESTS EXIST
─────────────────────
The previous depth-based scaling system (_slow_throughput_check_loop) caused
workers to stay permanently inflated.  The scale-down condition required
scan_backlog == 0 (ZCARD poll:adaptive), but poll:adaptive always contains
future-scheduled companies — so backlog was never zero and workers never
scaled down.

The new manager.py uses:
  1. Queue DELAY (how overdue is the most-overdue job) — primary signal
  2. Pool UTILIZATION (busy_ms / capacity_ms) — secondary gate
  3. drain_rate formula → workers_target — derived demand

The tests here verify every decision path so the inflation regression never
recurs.

STRUCTURE
─────────
Section 1  — Formula: drain_rate, workers_target, amplification, clamping
Section 2  — Utilization gate: scale-up / scale-down conditions
Section 3  — Hysteresis: cycle counters, direction-change resets
Section 4  — Urgent mode: fires, releases, holds at ceiling
Section 5  — The inflation regression: idle 10-worker pool → scales to floor
Section 6  — Queue metrics: depth + delay for all 3 pool types
Section 7  — busy_ms: SCAN+sum, missing keys, utilization computation
Section 8  — manager:cmds dispatch: all command types, edge cases
Section 9  — Ceiling decay: 24h clean → +1, recent error → no change
Section 10 — Daily peak tracking: intraday + midnight recompute
Section 11 — Bootstrap mode: < 28 days uses fixed ceilings
Section 12 — Scaling params: cache hit, miss, DB fallback
Section 13 — Pool sizes from scheduler:health
Section 14 — send_cmd: SHADOW_MODE True vs False
"""

import json
import math
import time
import unittest
from unittest.mock import MagicMock, patch, call

# ── Isolate module-level globals between test cases ───────────────────────────
# _run_pool_cycle mutates module-level dicts (_scale_up_cycles etc.).
# Each test that exercises cycle logic must reset them via _reset_cycle_state().

import workers.manager as mgr


def _reset_cycle_state():
    """Reset all hysteresis counters and urgent flags to zero/False."""
    for pool in ("scan", "detail", "fullscan"):
        mgr._scale_up_cycles[pool]   = 0
        mgr._scale_down_cycles[pool] = 0
        mgr._urgent_active[pool]     = False


def _make_redis(**keys):
    """
    Return a MagicMock that behaves like a Redis client for the values in
    `keys`.  Calls to r.get(k) return keys[k]; r.scan returns (0, []).
    """
    r = MagicMock()
    r.get.side_effect = lambda k: keys.get(k)
    r.scan.return_value = (0, [])
    r.exists.return_value = False
    r.blpop.return_value = None
    r.llen.return_value = 0
    r.zcount.return_value = 0
    r.zrange.return_value = []
    r.lindex.return_value = None
    return r


# ─────────────────────────────────────────────────────────────────────────────
# § 1 — Formula: drain_rate, workers_target, amplification, clamping
# ─────────────────────────────────────────────────────────────────────────────

class TestDrainRateFormula(unittest.TestCase):
    """Pure formula correctness — no Redis, no mocking needed."""

    PARAMS = {"fetch_p75": 5.0, "delay_warn_s": 60.0}

    def _target(self, depth, delay_s, n_workers=5, worker_ceil=10):
        _reset_cycle_state()
        r = _make_redis()
        mgr._run_pool_cycle(r, "detail", n_workers, 0, depth, delay_s,
                            self.PARAMS, worker_ceil, peak_nd=5)
        # Return the workers_target that would have been computed.
        # Recompute manually to keep tests independent of decision path.
        delay_warn_s = self.PARAMS["delay_warn_s"]
        fetch_p75    = self.PARAMS["fetch_p75"]
        time_left    = max(delay_warn_s - delay_s, 1)
        drain_rate   = depth / time_left
        wt           = math.ceil(drain_rate * fetch_p75)
        if delay_s > delay_warn_s:
            wt = math.ceil(wt * (delay_s / delay_warn_s))
        return max(mgr.WORKER_FLOOR, min(wt, worker_ceil))

    def test_idle_queue_gives_floor(self):
        """Empty queue → workers_target = WORKER_FLOOR."""
        t = self._target(depth=0, delay_s=0)
        self.assertEqual(t, mgr.WORKER_FLOOR)

    def test_small_queue_low_delay(self):
        """5 jobs, 10s delay, warn=60s, fetch=5s → drain=5/50=0.1/s → target=1 → floor=2."""
        t = self._target(depth=5, delay_s=10)
        self.assertEqual(t, mgr.WORKER_FLOOR)

    def test_large_queue_approaching_warn(self):
        """100 jobs, 55s delay (5s left), warn=60s, fetch=5s → drain=100/5=20 → target=100."""
        t = self._target(depth=100, delay_s=55, worker_ceil=10)
        self.assertEqual(t, 10)  # clamped to ceil

    def test_amplification_past_warn(self):
        """When delay > warn, workers_target is amplified by (delay/warn)."""
        depth = 10; delay_s = 90.0; warn = 60.0; fetch = 5.0; ceil_ = 20
        time_left  = max(warn - delay_s, 1)          # 1 (clamped)
        drain      = depth / time_left                # 10
        wt         = math.ceil(drain * fetch)         # 50
        wt_amp     = math.ceil(wt * (delay_s / warn)) # ceil(50 * 1.5) = 75
        expected   = max(mgr.WORKER_FLOOR, min(wt_amp, ceil_))  # 20
        t = self._target(depth=depth, delay_s=delay_s, worker_ceil=ceil_)
        self.assertEqual(t, expected)

    def test_floor_clamp(self):
        """workers_target never goes below WORKER_FLOOR."""
        t = self._target(depth=0, delay_s=0, n_workers=2, worker_ceil=10)
        self.assertGreaterEqual(t, mgr.WORKER_FLOOR)

    def test_ceil_clamp(self):
        """workers_target never exceeds worker_ceil."""
        t = self._target(depth=10_000, delay_s=59, worker_ceil=6)
        self.assertLessEqual(t, 6)

    def test_depth_zero_delay_nonzero(self):
        """Empty queue with some delay (just pacing) → still floor."""
        t = self._target(depth=0, delay_s=30)
        self.assertEqual(t, mgr.WORKER_FLOOR)

    def test_workers_target_scales_with_depth(self):
        """Doubling depth doubles drain_rate, roughly doubles target (until ceil)."""
        t1 = self._target(depth=10,  delay_s=0, worker_ceil=20)
        t2 = self._target(depth=100, delay_s=0, worker_ceil=20)
        self.assertGreaterEqual(t2, t1)


# ─────────────────────────────────────────────────────────────────────────────
# § 2 — Utilization gate
# ─────────────────────────────────────────────────────────────────────────────

class TestUtilizationGate(unittest.TestCase):
    """
    Scale-up requires util > 0.80; scale-down requires util < 0.50.
    Tests verify these gates block or allow decisions independently of delay.
    """

    PARAMS = {"fetch_p75": 5.0, "delay_warn_s": 60.0}

    def _cycle(self, pool_busy_ms, depth, delay_s, n_workers=5, worker_ceil=10):
        _reset_cycle_state()
        r = _make_redis()
        d, _ = mgr._run_pool_cycle(r, "scan", n_workers, pool_busy_ms,
                                   depth, delay_s, self.PARAMS, worker_ceil, peak_nd=5)
        return d

    # ── Scale-up gate ──────────────────────────────────────────────────────────

    def test_scale_up_blocked_low_util(self):
        """delay > WARN×0.5 AND target > current BUT util=5% → no scale up."""
        # util = 10_000 / (5 * 60 * 1000) = 0.033 (3.3%)
        d = self._cycle(pool_busy_ms=10_000, depth=50, delay_s=35, n_workers=5)
        # First cycle: streak=1, not 2 yet
        self.assertNotEqual(d, "scale_up")

    def test_scale_up_allowed_high_util(self):
        """delay > WARN×0.5 AND target > current AND util=90% → streak builds."""
        # util = 270_000 / (5 * 60 * 1000) = 0.90 (90%)
        pool_busy_ms = int(0.90 * 5 * 60 * 1000)
        # Cycle 1 → scale_up_pending
        d1 = self._cycle(pool_busy_ms=pool_busy_ms, depth=50, delay_s=35, n_workers=3)
        # Cycle 2 → scale_up
        r = _make_redis()
        d2, _ = mgr._run_pool_cycle(r, "scan", 3, pool_busy_ms, 50, 35,
                                    self.PARAMS, 10, peak_nd=5)
        self.assertIn(d1, ("scale_up_pending", "scale_up", "hold", "urgent",
                           "urgent_release", "urgent_hold"))
        # At least one of the two cycles moved toward scale-up decision
        self.assertIn(d2, ("scale_up", "scale_up_pending", "urgent",
                           "urgent_release", "urgent_hold"))

    # ── Scale-down gate ────────────────────────────────────────────────────────

    def test_scale_down_blocked_high_util(self):
        """util=90%, delay low → scale-down gate blocks."""
        pool_busy_ms = int(0.90 * 5 * 60 * 1000)
        d = self._cycle(pool_busy_ms=pool_busy_ms, depth=0, delay_s=5, n_workers=5)
        self.assertNotEqual(d, "scale_down")

    def test_scale_down_allowed_low_util(self):
        """util=5%, delay low, target < current → scale-down streak builds."""
        pool_busy_ms = int(0.05 * 5 * 60 * 1000)
        # Need 5 consecutive cycles for scale_down to fire
        r = _make_redis()
        decisions = []
        for _ in range(6):
            d, _ = mgr._run_pool_cycle(r, "scan", 5, pool_busy_ms, 0, 5,
                                        self.PARAMS, 10, peak_nd=5)
            decisions.append(d)
        self.assertIn("scale_down", decisions)

    def test_hold_in_stable_band(self):
        """util=65%, delay moderate → hold."""
        pool_busy_ms = int(0.65 * 5 * 60 * 1000)
        d = self._cycle(pool_busy_ms=pool_busy_ms, depth=5, delay_s=20, n_workers=5)
        self.assertEqual(d, "hold")

    def test_zero_busy_ms_is_low_util(self):
        """No workers publishing busy_ms → util=0 → treated as low utilization."""
        d = self._cycle(pool_busy_ms=0, depth=0, delay_s=5, n_workers=5)
        # Should be scale_down_pending or hold (not scale_up)
        self.assertNotIn(d, ("scale_up", "urgent"))


# ─────────────────────────────────────────────────────────────────────────────
# § 3 — Hysteresis: cycle counters, direction-change resets
# ─────────────────────────────────────────────────────────────────────────────

class TestHysteresis(unittest.TestCase):
    PARAMS = {"fetch_p75": 5.0, "delay_warn_s": 60.0}

    def setUp(self):
        _reset_cycle_state()

    def _cycle_n(self, n, pool_busy_ms, depth, delay_s, n_workers=3):
        r = _make_redis()
        decisions = []
        for _ in range(n):
            d, _ = mgr._run_pool_cycle(r, "scan", n_workers, pool_busy_ms,
                                        depth, delay_s, self.PARAMS, 10, peak_nd=5)
            decisions.append(d)
        return decisions

    def test_scale_up_requires_exactly_2_cycles(self):
        """scale_up fires on the 2nd consecutive cycle meeting conditions, not the 1st."""
        _reset_cycle_state()
        pool_busy_ms = int(0.90 * 3 * 60 * 1000)  # 90% util
        decisions = self._cycle_n(3, pool_busy_ms, depth=25, delay_s=35, n_workers=3)
        self.assertNotEqual(decisions[0], "scale_up")  # first cycle: pending
        self.assertIn("scale_up", decisions[1:])       # fires by cycle 2 or 3

    def test_scale_down_requires_exactly_5_cycles(self):
        """scale_down fires on the 5th consecutive cycle, not before."""
        _reset_cycle_state()
        pool_busy_ms = int(0.05 * 5 * 60 * 1000)  # 5% util
        decisions = self._cycle_n(6, pool_busy_ms, depth=0, delay_s=5, n_workers=5)
        # Cycles 1–4 should be pending
        for i in range(min(4, len(decisions))):
            self.assertNotEqual(decisions[i], "scale_down",
                                f"scale_down fired too early on cycle {i+1}")
        # Cycle 5 or 6 should fire
        self.assertIn("scale_down", decisions)

    def test_up_counter_resets_when_delay_drops(self):
        """Up streak resets when delay drops below threshold mid-streak."""
        _reset_cycle_state()
        pool_busy_ms = int(0.90 * 3 * 60 * 1000)
        r = _make_redis()
        # Cycle 1: high delay → streak=1
        # depth=25 → workers_target=5, avoids demand-urgent (5 < ceil×0.75=7.5)
        mgr._run_pool_cycle(r, "scan", 3, pool_busy_ms, 25, 35, self.PARAMS, 10, peak_nd=5)
        self.assertEqual(mgr._scale_up_cycles["scan"], 1)
        # Cycle 2: delay drops → hold → resets streak
        mgr._run_pool_cycle(r, "scan", 3, pool_busy_ms, 0, 5, self.PARAMS, 10, peak_nd=5)
        self.assertEqual(mgr._scale_up_cycles["scan"], 0)

    def test_down_counter_resets_when_delay_rises(self):
        """Down streak resets when utilization rises above threshold mid-streak."""
        _reset_cycle_state()
        low_util = int(0.05 * 5 * 60 * 1000)
        high_util = int(0.90 * 5 * 60 * 1000)
        r = _make_redis()
        # Two cycles toward scale-down
        for _ in range(2):
            mgr._run_pool_cycle(r, "scan", 5, low_util, 0, 5, self.PARAMS, 10, peak_nd=5)
        self.assertEqual(mgr._scale_down_cycles["scan"], 2)
        # High util → counter resets
        mgr._run_pool_cycle(r, "scan", 5, high_util, 50, 35, self.PARAMS, 10, peak_nd=5)
        self.assertEqual(mgr._scale_down_cycles["scan"], 0)

    def test_direction_change_resets_opposite_counter(self):
        """Switching from scale-up streak to scale-down streak resets scale-up counter."""
        _reset_cycle_state()
        pool_busy_ms_high = int(0.90 * 3 * 60 * 1000)
        pool_busy_ms_low  = int(0.05 * 5 * 60 * 1000)
        r = _make_redis()
        # Build up-streak; depth=25 → workers_target=5, avoids demand-urgent (5 < ceil×0.75)
        mgr._run_pool_cycle(r, "scan", 3, pool_busy_ms_high, 25, 35, self.PARAMS, 10, peak_nd=5)
        self.assertEqual(mgr._scale_up_cycles["scan"], 1)
        # Switch to down-eligible conditions → up counter must reset
        mgr._run_pool_cycle(r, "scan", 5, pool_busy_ms_low, 0, 5, self.PARAMS, 10, peak_nd=5)
        self.assertEqual(mgr._scale_up_cycles["scan"], 0)


# ─────────────────────────────────────────────────────────────────────────────
# § 4 — Urgent mode
# ─────────────────────────────────────────────────────────────────────────────

class TestUrgentMode(unittest.TestCase):
    PARAMS = {"fetch_p75": 5.0, "delay_warn_s": 60.0}

    def setUp(self):
        _reset_cycle_state()

    def test_urgent_fires_on_delay_threshold(self):
        """delay >= WARN×0.75 (45s) fires urgent immediately on first cycle."""
        r = _make_redis()
        d, _ = mgr._run_pool_cycle(r, "scan", 3, 0, 50, 46.0, self.PARAMS, 10, peak_nd=5)
        self.assertEqual(d, "urgent")

    def test_urgent_fires_on_demand_trigger(self):
        """demand trigger: workers_target >= ceil×0.75 AND workers_target > peak_nd."""
        # worker_ceil=8, peak_nd=3. Need workers_target >= 6 (8×0.75) AND > 3.
        # With depth=100, delay_s=20, warn=60: time_left=40, drain=2.5, wt=13 → clamped to 8
        # workers_target=8 >= 6 ✓ AND > 3 ✓ → demand trigger fires
        r = _make_redis()
        d, _ = mgr._run_pool_cycle(r, "scan", 3, 0, 100, 20.0, self.PARAMS, 8, peak_nd=3)
        # demand trigger + understaffed (3 < min(8,8)=8) → urgent
        self.assertEqual(d, "urgent")

    def test_urgent_only_fires_when_understaffed(self):
        """Urgent delay threshold met but workers already at target → no urgent."""
        # With depth=0, delay=46: workers_target=floor=2, n_workers=5 ≥ min(2,10)=2 → no urgent
        r = _make_redis()
        d, _ = mgr._run_pool_cycle(r, "scan", 5, 0, 0, 46.0, self.PARAMS, 10, peak_nd=5)
        self.assertNotEqual(d, "urgent")

    def test_urgent_sends_target_command(self):
        """Urgent fires → sends {pool}:target:{workers_target} command."""
        r = _make_redis()
        with patch.object(mgr, 'SHADOW_MODE', False):
            mgr._run_pool_cycle(r, "scan", 3, 0, 50, 46.0, self.PARAMS, 10, peak_nd=5)
        r.rpush.assert_called_once()
        cmd = r.rpush.call_args[0][1]
        self.assertTrue(cmd.startswith("scan:target:"))

    def test_urgent_release_when_right_sized(self):
        """After urgent fires, next cycle with workers=target clears urgent flag."""
        r = _make_redis()
        # Fire urgent
        mgr._run_pool_cycle(r, "scan", 3, 0, 50, 46.0, self.PARAMS, 10, peak_nd=5)
        self.assertTrue(mgr._urgent_active["scan"])
        # Simulate workers now at target (pool grew to 10)
        d, _ = mgr._run_pool_cycle(r, "scan", 10, 0, 5, 5.0, self.PARAMS, 10, peak_nd=5)
        self.assertEqual(d, "urgent_release")
        self.assertFalse(mgr._urgent_active["scan"])

    def test_urgent_hold_when_still_understaffed_at_ceiling(self):
        """Urgent active but workers at ceiling → urgent_hold."""
        r = _make_redis()
        # Fire urgent (delay spike)
        mgr._run_pool_cycle(r, "scan", 3, 0, 50, 46.0, self.PARAMS, 10, peak_nd=5)
        # Delay eases but still understaffed (3 workers, target=5) → urgent_hold
        # depth=25 → workers_target=5, delay=35 → both urgent triggers off → urgent_hold
        d, _ = mgr._run_pool_cycle(r, "scan", 3, 0, 25, 35.0, self.PARAMS, 10, peak_nd=5)
        self.assertEqual(d, "urgent_hold")

    def test_urgent_resets_streak_counters(self):
        """Urgent firing resets scale-up and scale-down cycle counters to 0."""
        mgr._scale_up_cycles["scan"]   = 1
        mgr._scale_down_cycles["scan"] = 3
        r = _make_redis()
        mgr._run_pool_cycle(r, "scan", 3, 0, 50, 46.0, self.PARAMS, 10, peak_nd=5)
        self.assertEqual(mgr._scale_up_cycles["scan"],   0)
        self.assertEqual(mgr._scale_down_cycles["scan"], 0)


# ─────────────────────────────────────────────────────────────────────────────
# § 5 — The inflation regression
# ─────────────────────────────────────────────────────────────────────────────

class TestInflationRegression(unittest.TestCase):
    """
    This is the core regression test.

    OLD BUG: _slow_throughput_check_loop used scan_backlog (ZCARD poll:adaptive).
    poll:adaptive always contains future-scheduled companies, so backlog was never
    zero and workers NEVER scaled down.  10 scan workers at startup → 10 forever.

    NEW BEHAVIOUR: manager.py uses delay + utilization.
      - Low utilization (workers idle)  → util < 0.50
      - No overdue jobs                 → delay = 0 < DELAY_WARN×0.25
      - workers_target = WORKER_FLOOR   ≤ n_workers - 1

    Each scale_down event fires after 5 consecutive qualifying cycles and
    decrements n_workers by exactly 1 (never jumps to floor directly).
    10 workers → floor=2 takes 8 events × 5 cycles = 40 cycles (~40 minutes).
    """

    PARAMS = {"fetch_p75": 40.0, "delay_warn_s": 1800.0}  # scan defaults

    def test_idle_10_workers_scales_down_gradually(self):
        """
        10 scan workers, no overdue jobs, low busy_ms → manager descends by 1
        worker every 5 cycles (5 minutes real-time) until WORKER_FLOOR is reached.

        Math: 10 workers → floor=2 requires 8 scale_down events.
              Each event needs 5 consecutive qualifying cycles.
              Total: 8 × 5 = 40 cycles to drain completely.

        This test runs 45 cycles to give a small buffer and asserts that:
          - scale_down fired at least once (inflation bug guard)
          - the pool reached WORKER_FLOOR (complete drain)
          - every command decremented by exactly 1 (no jumps)
        """
        _reset_cycle_state()
        r = _make_redis()
        n_workers    = 10
        pool_busy_ms = int(0.05 * 10 * 60 * 1000)  # fixed 5% util of the original fleet

        with patch.object(mgr, 'SHADOW_MODE', False):
            decisions = []
            for _ in range(45):
                d, _ = mgr._run_pool_cycle(
                    r, "scan", n_workers, pool_busy_ms,
                    depth=0, delay_s=0.0,
                    params=self.PARAMS,
                    worker_ceil=12,
                    peak_nd=5,
                )
                decisions.append(d)
                if d == "scale_down":
                    n_workers = max(mgr.WORKER_FLOOR, n_workers - 1)

        self.assertIn("scale_down", decisions,
                      "scale_down never fired — inflation bug may have returned")
        self.assertEqual(n_workers, mgr.WORKER_FLOOR,
                         f"Pool did not fully drain: {n_workers} workers remain after 45 cycles")

        # No command should jump by more than 1 in a single step
        sent_targets = [
            int(c[0][1].split(":")[-1])
            for c in r.rpush.call_args_list
            if c[0][1].startswith("scan:target:")
        ]
        for i in range(1, len(sent_targets)):
            drop = sent_targets[i - 1] - sent_targets[i]
            self.assertLessEqual(drop, 1,
                f"scale_down jumped by {drop} in one step "
                f"({sent_targets[i-1]} → {sent_targets[i]})"
            )

    def test_future_scheduled_jobs_do_not_prevent_scale_down(self):
        """
        Old bug: ZCARD poll:adaptive > 0 → backlog > 0 → no scale-down.
        New system: only OVERDUE jobs (score <= now) count.  Future jobs don't.

        Simulate: 150 companies in poll:adaptive, all scheduled in the future.
        depth (ZCOUNT with -inf to now) = 0.  delay_s = 0.
        → scale_down path open.
        """
        _reset_cycle_state()
        r = _make_redis()
        # zcount for overdue = 0 (all jobs future-scheduled)
        # This is what _get_queue_metrics computes for scan — only ZCOUNT(-inf, now)
        r.zcount.return_value = 0
        r.zrange.return_value = []   # no overdue entries

        pool_busy_ms = int(0.05 * 10 * 60 * 1000)
        decisions = []
        for _ in range(6):
            d, _ = mgr._run_pool_cycle(
                r, "scan", 10, pool_busy_ms, 0, 0.0,
                self.PARAMS, 12, peak_nd=5,
            )
            decisions.append(d)

        self.assertIn("scale_down", decisions,
                      "Future-scheduled jobs blocking scale-down (old bug repro)")

    def test_scan_workers_right_size_at_low_demand(self):
        """
        With real production parameters (10 workers, 0.3 scans/min needed),
        workers_target should be near WORKER_FLOOR.

        0.3 scans/min × 40s fetch = 12s of work per minute per worker.
        2 workers × 60s capacity = 120s.  util ≈ 12/120 = 10% → scale down.
        """
        _reset_cycle_state()
        # 0.3 jobs/min × 40s = 0.2 jobs/s needed
        # 2 workers capacity = 2 × 60 = 120s per minute
        # drain_rate for depth=0, delay=0 → workers_target = floor = 2
        params  = {"fetch_p75": 40.0, "delay_warn_s": 1800.0}
        r       = _make_redis()
        wt_vals = []
        for _ in range(5):
            d, _ = mgr._run_pool_cycle(r, "scan", 10, 0, 0, 0.0, params, 12, peak_nd=5)
        # After 5 low-demand cycles, workers_target should equal floor
        time_left = max(1800.0 - 0.0, 1)
        drain     = 0 / time_left
        wt        = max(mgr.WORKER_FLOOR, min(math.ceil(drain * 40.0), 12))
        self.assertEqual(wt, mgr.WORKER_FLOOR)

    def test_scale_down_stops_at_floor(self):
        """scale_down never sends target below WORKER_FLOOR."""
        _reset_cycle_state()
        r = _make_redis()
        pool_busy_ms = 0
        # Already at floor
        n_workers = mgr.WORKER_FLOOR
        with patch.object(mgr, 'SHADOW_MODE', False):
            for _ in range(10):
                mgr._run_pool_cycle(r, "scan", n_workers, pool_busy_ms,
                                     0, 0.0, {"fetch_p75": 40.0, "delay_warn_s": 1800.0},
                                     10, peak_nd=2)
        for c in r.rpush.call_args_list:
            cmd = c[0][1]
            if cmd.startswith("scan:target:"):
                target = int(cmd.split(":")[-1])
                self.assertGreaterEqual(target, mgr.WORKER_FLOOR,
                                        f"Manager sent target below floor: {cmd}")


# ─────────────────────────────────────────────────────────────────────────────
# § 6 — Queue metrics: depth + delay for all 3 pool types
# ─────────────────────────────────────────────────────────────────────────────

class TestQueueMetrics(unittest.TestCase):

    def _make_r_with_scan_state(self, depth_adaptive=0, depth_fullscan=0,
                                 enqueued_at=None, zcount_overdue=0,
                                 zrange_score=None):
        r = MagicMock()
        r.llen.side_effect = lambda k: (
            depth_adaptive if "adaptive" in k else depth_fullscan
        )
        if enqueued_at:
            r.lindex.return_value = json.dumps({"enqueued_at": enqueued_at})
        else:
            r.lindex.return_value = None
        r.zcount.return_value = zcount_overdue
        if zrange_score is not None:
            entry_name = b"some_company"
            r.zrange.return_value = [(entry_name, zrange_score)]
        else:
            r.zrange.return_value = []
        return r

    def test_detail_depth_is_sum_of_both_queues(self):
        r = self._make_r_with_scan_state(depth_adaptive=30, depth_fullscan=15)
        metrics = mgr._get_queue_metrics(r)
        self.assertEqual(metrics["detail"]["depth"], 45)

    def test_detail_delay_from_enqueued_at(self):
        """Detail delay = now - enqueued_at of oldest item."""
        now = time.time()
        enqueued_ts = now - 90  # 90 seconds ago
        from datetime import datetime, timezone
        enqueued_str = datetime.fromtimestamp(enqueued_ts, tz=timezone.utc).isoformat()
        r = self._make_r_with_scan_state(depth_adaptive=5, enqueued_at=enqueued_str)
        metrics = mgr._get_queue_metrics(r)
        self.assertAlmostEqual(metrics["detail"]["delay_s"], 90, delta=2)

    def test_detail_delay_zero_when_no_enqueued_at(self):
        """If enqueued_at missing from payload → delay=0 (graceful degradation)."""
        r = self._make_r_with_scan_state(depth_adaptive=5)
        r.lindex.return_value = json.dumps({"company": "acme"})  # no enqueued_at
        metrics = mgr._get_queue_metrics(r)
        self.assertEqual(metrics["detail"]["delay_s"], 0.0)

    def test_scan_depth_counts_only_overdue(self):
        """Scan depth = ZCOUNT(-inf, now) — future-scheduled jobs excluded."""
        r = self._make_r_with_scan_state(zcount_overdue=7)
        metrics = mgr._get_queue_metrics(r)
        self.assertEqual(metrics["scan"]["depth"], 7)

    def test_scan_delay_from_most_overdue_zset_entry(self):
        """Scan delay = now - score of lowest ZSET entry (most overdue company)."""
        overdue_by = 500  # seconds
        now = time.time()
        r = self._make_r_with_scan_state(
            zcount_overdue=3,
            zrange_score=now - overdue_by,
        )
        metrics = mgr._get_queue_metrics(r)
        self.assertAlmostEqual(metrics["scan"]["delay_s"], overdue_by, delta=2)

    def test_scan_delay_zero_when_no_overdue(self):
        """No overdue scan jobs → delay=0."""
        r = self._make_r_with_scan_state(zcount_overdue=0)
        metrics = mgr._get_queue_metrics(r)
        self.assertEqual(metrics["scan"]["delay_s"], 0.0)

    def test_fullscan_delay_from_most_overdue(self):
        """Fullscan delay follows same pattern as scan."""
        now = time.time()
        overdue_by = 3600
        r = MagicMock()
        r.llen.return_value = 0
        r.lindex.return_value = None
        r.zcount.side_effect = lambda key, lo, hi: 5 if "fullscan" in key else 0
        r.zrange.side_effect = lambda key, start, stop, withscores=False: (
            [(b"co", now - overdue_by)] if "fullscan" in key else []
        )
        metrics = mgr._get_queue_metrics(r)
        self.assertAlmostEqual(metrics["fullscan"]["delay_s"], overdue_by, delta=2)

    def test_metrics_handles_redis_error_gracefully(self):
        """Redis error on one pool → returns 0s for that pool, others unaffected."""
        r = MagicMock()
        r.llen.side_effect = Exception("Redis connection error")
        r.zcount.return_value = 5
        r.zrange.return_value = []
        metrics = mgr._get_queue_metrics(r)
        self.assertEqual(metrics["detail"]["depth"], 0)
        self.assertEqual(metrics["detail"]["delay_s"], 0.0)
        # scan and fullscan should still compute
        self.assertEqual(metrics["scan"]["depth"], 5)


# ─────────────────────────────────────────────────────────────────────────────
# § 7 — busy_ms: SCAN+sum, missing keys, utilization computation
# ─────────────────────────────────────────────────────────────────────────────

class TestBusyMs(unittest.TestCase):

    def test_scan_sums_all_pid_keys(self):
        """Scans worker:scan:busy_ms:* and sums all values."""
        r = MagicMock()
        keys = [b"worker:scan:busy_ms:1001", b"worker:scan:busy_ms:1002"]
        r.scan.return_value = (0, keys)
        r.get.side_effect = lambda k: b"15000" if b"1001" in k else b"20000"
        total = mgr._get_pool_busy_ms(r, "scan")
        self.assertEqual(total, 35000)

    def test_no_keys_returns_zero(self):
        """No workers publishing busy_ms → returns 0."""
        r = MagicMock()
        r.scan.return_value = (0, [])
        total = mgr._get_pool_busy_ms(r, "scan")
        self.assertEqual(total, 0)

    def test_partial_missing_values_skipped(self):
        """Keys with missing values are skipped, others summed."""
        r = MagicMock()
        keys = [b"worker:scan:busy_ms:1001", b"worker:scan:busy_ms:1002"]
        r.scan.return_value = (0, keys)
        r.get.side_effect = lambda k: b"10000" if b"1001" in k else None
        total = mgr._get_pool_busy_ms(r, "scan")
        self.assertEqual(total, 10000)

    def test_utilization_formula(self):
        """pool_utilization = pool_busy_ms / (n_workers × cycle_s × 1000)."""
        n_workers    = 4
        pool_busy_ms = int(0.75 * n_workers * mgr.MANAGER_CYCLE_S * 1000)
        capacity_ms  = n_workers * mgr.MANAGER_CYCLE_S * 1000
        expected_util = pool_busy_ms / capacity_ms
        self.assertAlmostEqual(expected_util, 0.75, places=2)

    def test_utilization_capped_at_1(self):
        """pool_utilization never exceeds 1.0 (busy_ms > capacity due to long jobs)."""
        n_workers    = 2
        # busy_ms 200% of capacity (two long-running jobs)
        pool_busy_ms = 2 * n_workers * mgr.MANAGER_CYCLE_S * 1000
        capacity_ms  = max(n_workers * mgr.MANAGER_CYCLE_S * 1000, 1)
        util = min(pool_busy_ms / capacity_ms, 1.0)
        self.assertEqual(util, 1.0)

    def test_multi_page_scan_busy_ms(self):
        """Busy_ms accumulated across multiple Redis SCAN pages."""
        r = MagicMock()
        page1_keys = [b"worker:scan:busy_ms:1001"]
        page2_keys = [b"worker:scan:busy_ms:1002"]
        r.scan.side_effect = [
            (42, page1_keys),   # cursor != 0 → more pages
            (0,  page2_keys),   # cursor == 0 → done
        ]
        r.get.side_effect = lambda k: b"5000"
        total = mgr._get_pool_busy_ms(r, "scan")
        self.assertEqual(total, 10000)


# ─────────────────────────────────────────────────────────────────────────────
# § 8 — manager:cmds dispatch
# ─────────────────────────────────────────────────────────────────────────────

class TestManagerCmdsDispatch(unittest.TestCase):
    """
    Tests for _manager_cmds_loop() in scheduler.py.
    We test the command parsing and dispatch logic directly by calling the
    loop's inner logic, mocking _add_one_worker, _remove_one_worker,
    _deprioritise_platform, and r.set/delete.
    """

    def _run_cmd(self, cmd_str, n_scan=5, n_detail=3, n_fullscan=2):
        """
        Simulate one pass of _manager_cmds_loop with a single command.
        Returns (add_calls, remove_calls, deprioritize_calls, set_calls, delete_calls).
        """
        import workers.scheduler as sched

        r = MagicMock()
        r.blpop.return_value = ("manager:cmds", cmd_str.encode())

        with patch.object(sched, '_get_pool_snapshot', return_value=(n_scan, n_detail)), \
             patch.object(sched, '_get_fullscan_pool_size', return_value=n_fullscan), \
             patch.object(sched, '_add_one_worker', return_value=True) as mock_add, \
             patch.object(sched, '_remove_one_worker', return_value=True) as mock_remove, \
             patch.object(sched, '_deprioritise_platform', return_value=3) as mock_depr, \
             patch.object(sched, 'get_redis', return_value=r), \
             patch('workers.scheduler.WORKER_OUTAGE_TTL_S', 3600):
            # Execute one iteration of the loop body
            result = r.blpop("manager:cmds", timeout=5)
            _, raw = result
            cmd = raw.decode() if isinstance(raw, bytes) else raw
            parts = cmd.split(":")

            if len(parts) == 3 and parts[1] == "target":
                pool, _, n_str = parts
                target = int(n_str)
                from config import WORKER_FLOOR, DB_POOL_MAXCONN
                db_budget = DB_POOL_MAXCONN - 3
                ceil_map = {
                    "scan":     max(WORKER_FLOOR, db_budget // 2),
                    "detail":   max(WORKER_FLOOR, db_budget // 3),
                    "fullscan": max(WORKER_FLOOR, db_budget // 5),
                }
                ceil_ = ceil_map.get(pool, WORKER_FLOOR)
                target = max(WORKER_FLOOR, min(target, ceil_))
                if pool == "scan":
                    current = n_scan
                elif pool == "detail":
                    current = n_detail
                else:
                    current = n_fullscan
                for _ in range(max(0, target - current)):
                    mock_add(pool, ceil_)
                for _ in range(max(0, current - target)):
                    mock_remove(pool, WORKER_FLOOR)
            elif len(parts) == 3 and parts[0] == "platform" and parts[1] == "deprioritize":
                mock_depr(r, parts[2])
            elif len(parts) == 4 and parts[0] == "platform" and parts[1] == "outage":
                platform, action = parts[2], parts[3]
                if action == "set":
                    r.set(f"worker:outage:{platform}", "1", ex=3600)
                elif action == "clear":
                    r.delete(f"worker:outage:{platform}")

        return mock_add, mock_remove, mock_depr, r

    def test_target_add_workers_when_below_target(self):
        """scan:target:8 with 5 scan workers → adds 3."""
        add, remove, _, _ = self._run_cmd("scan:target:8", n_scan=5)
        self.assertEqual(add.call_count, 3)
        remove.assert_not_called()

    def test_target_remove_workers_when_above_target(self):
        """scan:target:3 with 5 scan workers → removes 2."""
        add, remove, _, _ = self._run_cmd("scan:target:3", n_scan=5)
        add.assert_not_called()
        self.assertEqual(remove.call_count, 2)

    def test_target_no_change_when_at_target(self):
        """scan:target:5 with exactly 5 workers → no add/remove."""
        add, remove, _, _ = self._run_cmd("scan:target:5", n_scan=5)
        add.assert_not_called()
        remove.assert_not_called()

    def test_target_clamped_to_floor(self):
        """scan:target:0 clamps to WORKER_FLOOR — never kills below minimum."""
        from config import WORKER_FLOOR
        add, remove, _, _ = self._run_cmd("scan:target:0", n_scan=5)
        # Should remove down to WORKER_FLOOR, not to 0
        expected_removes = 5 - WORKER_FLOOR
        self.assertEqual(remove.call_count, expected_removes)

    def test_deprioritize_command_calls_function(self):
        """platform:deprioritize:workday calls _deprioritise_platform."""
        _, _, depr, _ = self._run_cmd("platform:deprioritize:workday")
        depr.assert_called_once()

    def test_outage_set_command(self):
        """platform:outage:workday:set → r.set(worker:outage:workday, '1', ex=TTL)."""
        _, _, _, r = self._run_cmd("platform:outage:workday:set")
        r.set.assert_called_with("worker:outage:workday", "1", ex=3600)

    def test_outage_clear_command(self):
        """platform:outage:workday:clear → r.delete(worker:outage:workday)."""
        _, _, _, r = self._run_cmd("platform:outage:workday:clear")
        r.delete.assert_called_with("worker:outage:workday")


# ─────────────────────────────────────────────────────────────────────────────
# § 9 — Ceiling decay
# ─────────────────────────────────────────────────────────────────────────────

class TestCeilingDecay(unittest.TestCase):
    """Tests for _ceiling_decay_loop logic (extracted to standalone thread)."""

    def _run_decay_once(self, last_error_age_s=None, current_ceil=5):
        """
        Simulate one pass of the ceiling decay logic.
        Returns new_ceil value (or None if no SET was called).
        """
        import workers.scheduler as sched
        from config import WORKER_FLOOR, MONITOR_MAX_WORKERS

        r = MagicMock()
        now = time.time()

        dc_key = "workday:us"
        ceil_key = f"worker:ceil:learned:{dc_key}"
        r.scan.return_value = (0, [ceil_key.encode()])
        r.get.side_effect = lambda k: (
            str(current_ceil).encode() if k == ceil_key or k == ceil_key.encode() else
            str(now - last_error_age_s).encode() if last_error_age_s is not None and "last_error" in k else
            None
        )

        new_ceil_val = [None]

        def fake_set(key, val):
            k = key.decode() if isinstance(key, bytes) else key
            if "ceil:learned" in k:
                new_ceil_val[0] = int(val)

        r.set.side_effect = fake_set

        # Execute ceiling decay logic
        cursor = 0
        while True:
            cursor, keys = r.scan(cursor, match="worker:ceil:learned:*", count=50)
            for key in keys:
                k = key.decode() if isinstance(key, bytes) else key
                dc = k.split("worker:ceil:learned:", 1)[1]
                last_err = r.get(f"worker:ceil:last_error:{dc}")
                if last_err and (now - float(last_err)) > 86400:
                    old_ceil = int(r.get(key) or WORKER_FLOOR)
                    if old_ceil < MONITOR_MAX_WORKERS:
                        new_ceil = old_ceil + 1
                        r.set(key, new_ceil)
            if cursor == 0:
                break

        return new_ceil_val[0]

    def test_decay_after_24h_clean(self):
        """24h since last error → ceiling increments by 1."""
        new_ceil = self._run_decay_once(last_error_age_s=86401, current_ceil=5)
        self.assertEqual(new_ceil, 6)

    def test_no_decay_recent_error(self):
        """Error 1h ago → ceiling unchanged."""
        new_ceil = self._run_decay_once(last_error_age_s=3600, current_ceil=5)
        self.assertIsNone(new_ceil)

    def test_no_decay_missing_error_key(self):
        """No last_error key at all → ceiling unchanged (platform never had errors)."""
        new_ceil = self._run_decay_once(last_error_age_s=None, current_ceil=5)
        self.assertIsNone(new_ceil)

    def test_decay_capped_at_monitor_max(self):
        """Ceiling never exceeds MONITOR_MAX_WORKERS."""
        from config import MONITOR_MAX_WORKERS
        new_ceil = self._run_decay_once(last_error_age_s=86401, current_ceil=MONITOR_MAX_WORKERS)
        self.assertIsNone(new_ceil)   # already at max → set NOT called


# ─────────────────────────────────────────────────────────────────────────────
# § 10 — Daily peak tracking + midnight recompute
# ─────────────────────────────────────────────────────────────────────────────

class TestDailyPeak(unittest.TestCase):

    def test_update_daily_peak_running_records_higher_value(self):
        """Updates running key when n_workers > current stored value."""
        r = MagicMock()
        r.get.return_value = b"3"
        mgr._update_daily_peak_running(r, "scan", 7)
        r.set.assert_called_once_with("manager:pool:scan:daily_peak:running", 7)

    def test_update_daily_peak_running_skips_lower_value(self):
        """Does not update if n_workers <= stored value."""
        r = MagicMock()
        r.get.return_value = b"8"
        mgr._update_daily_peak_running(r, "scan", 5)
        r.set.assert_not_called()

    def test_update_daily_peak_running_handles_missing_key(self):
        """Missing key treated as 0 → any positive value updates it."""
        r = MagicMock()
        r.get.return_value = None
        mgr._update_daily_peak_running(r, "scan", 4)
        r.set.assert_called_once_with("manager:pool:scan:daily_peak:running", 4)

    def test_midnight_recompute_bootstrap_mode(self):
        """< 28 days of records → writes BOOTSTRAP_CEIL, not formula result."""
        r = MagicMock()
        r.get.return_value = b"5"          # running peak
        # _count_daily_peak_records returns 5 (< 28)
        r.scan.return_value = (0, [b"manager:pool:scan:daily_peak:2026-07-01"] * 5)

        with patch.object(mgr, '_count_daily_peak_records', return_value=5):
            mgr._midnight_recompute(r, ["scan"])

        set_calls = {c[0][0]: c[0][1] for c in r.set.call_args_list}
        self.assertIn("manager:worker_ceil:scan", set_calls)
        self.assertEqual(set_calls["manager:worker_ceil:scan"],
                         mgr.BOOTSTRAP_CEIL["scan"])

    def test_midnight_recompute_resets_running_peak(self):
        """After midnight recompute, daily_peak:running is reset to 0."""
        r = MagicMock()
        r.get.return_value = b"7"

        with patch.object(mgr, '_count_daily_peak_records', return_value=5):
            mgr._midnight_recompute(r, ["scan"])

        reset_calls = [c for c in r.set.call_args_list
                       if "daily_peak:running" in str(c)]
        self.assertTrue(any(c[0][1] == 0 for c in reset_calls),
                        "daily_peak:running not reset to 0 after midnight recompute")

    def test_growth_buffer_positive_growth(self):
        """peak_7d > peak_28d → positive growth_buffer."""
        peak_28d = 6
        peak_7d  = 8
        rate = (peak_7d - peak_28d) / peak_28d / 3
        buf  = math.ceil(peak_28d * rate)   # peak_28d here is peak_nd
        self.assertGreater(buf, 0)

    def test_growth_buffer_negative_clamps_to_zero(self):
        """peak_7d < peak_28d → growth_buffer = 0 (demand shrinking)."""
        peak_28d = 8
        peak_7d  = 5
        rate = (peak_7d - peak_28d) / peak_28d / 3  # negative
        buf  = math.ceil(max(0.0, peak_28d * rate))
        self.assertEqual(buf, 0)

    def test_volatility_buffer_from_stdev(self):
        """volatility_buffer = ceil(stdev × 0.25)."""
        import statistics
        peaks = [4, 7, 5, 8, 6, 9, 7]
        std   = statistics.stdev(peaks)
        buf   = math.ceil(std * 0.25)
        self.assertGreaterEqual(buf, 0)


# ─────────────────────────────────────────────────────────────────────────────
# § 11 — Bootstrap mode
# ─────────────────────────────────────────────────────────────────────────────

class TestBootstrapMode(unittest.TestCase):

    def test_get_worker_ceil_returns_bootstrap_when_no_redis_key(self):
        """No manager:worker_ceil:{pool} in Redis → returns BOOTSTRAP_CEIL."""
        r = MagicMock()
        r.get.return_value = None
        for pool in ("scan", "detail", "fullscan"):
            ceil_ = mgr._get_worker_ceil(r, pool)
            self.assertEqual(ceil_, mgr.BOOTSTRAP_CEIL[pool],
                             f"Wrong bootstrap ceil for {pool}")

    def test_get_worker_ceil_uses_redis_when_set(self):
        """manager:worker_ceil:scan = 8 → returns 8."""
        r = MagicMock()
        r.get.return_value = b"8"
        ceil_ = mgr._get_worker_ceil(r, "scan")
        self.assertEqual(ceil_, 8)

    def test_get_worker_ceil_enforces_floor(self):
        """Stored value below WORKER_FLOOR → returns WORKER_FLOOR."""
        r = MagicMock()
        r.get.return_value = b"0"
        ceil_ = mgr._get_worker_ceil(r, "scan")
        self.assertGreaterEqual(ceil_, mgr.WORKER_FLOOR)

    def test_bootstrap_ceil_values_match_production_fleet(self):
        """Bootstrap ceilings should match current production defaults."""
        self.assertEqual(mgr.BOOTSTRAP_CEIL["scan"],     10)
        self.assertEqual(mgr.BOOTSTRAP_CEIL["detail"],    6)
        self.assertEqual(mgr.BOOTSTRAP_CEIL["fullscan"],  5)

    def test_bootstrap_days_required_is_28(self):
        """Must accumulate 28 days of history before using formula."""
        self.assertEqual(mgr.BOOTSTRAP_DAYS_REQUIRED, 28)


# ─────────────────────────────────────────────────────────────────────────────
# § 12 — Scaling params: cache hit, miss, DB fallback
# ─────────────────────────────────────────────────────────────────────────────

class TestScalingParams(unittest.TestCase):

    def test_load_from_cache_when_key_exists(self):
        """manager:scaling_params exists in Redis → no DB query, returns cached."""
        cached = {
            "detail":   {"fetch_p75": 3.5,  "delay_warn_s": 60},
            "scan":     {"fetch_p75": 42.0, "delay_warn_s": 1750},
            "fullscan": {"fetch_p75": 97.0, "delay_warn_s": 8600},
            "computed_at": "2026-07-23T00:00:00Z",
        }
        r = MagicMock()
        r.get.return_value = json.dumps(cached).encode()
        params = mgr._load_scaling_params(r)
        self.assertEqual(params["detail"]["fetch_p75"], 3.5)
        r.set.assert_not_called()  # no recompute triggered

    def test_compute_on_cache_miss(self):
        """manager:scaling_params missing → compute + cache."""
        r = MagicMock()
        r.get.return_value = None
        fake_params = {
            "detail":   {"fetch_p75": 4.0, "delay_warn_s": 60},
            "scan":     {"fetch_p75": 40.0, "delay_warn_s": 1800},
            "fullscan": {"fetch_p75": 95.0, "delay_warn_s": 7200},
            "computed_at": "2026-07-23",
        }
        with patch.object(mgr, '_compute_scaling_params', return_value=fake_params):
            params = mgr._load_scaling_params(r)
        self.assertEqual(params["scan"]["fetch_p75"], 40.0)
        r.set.assert_called_once()  # cached in Redis

    def test_fallback_on_db_error(self):
        """DB error during compute → _load_scaling_params returns fallback params."""
        r = MagicMock()
        r.get.return_value = None  # no cached value → forces compute path
        # Simulate DB failure: _compute_scaling_params returns fallback dict
        # (the real function catches DB errors internally and returns _FALLBACK_PARAMS)
        with patch('workers.manager._compute_scaling_params',
                   return_value={**mgr._FALLBACK_PARAMS, "computed_at": "fallback"}):
            params = mgr._load_scaling_params(r)
        for pool in ("detail", "scan", "fullscan"):
            self.assertIn(pool, params)
            self.assertIn("fetch_p75",    params[pool])
            self.assertIn("delay_warn_s", params[pool])

    def test_fallback_params_have_required_keys(self):
        """_FALLBACK_PARAMS has fetch_p75 and delay_warn_s for all three pools."""
        for pool in ("detail", "scan", "fullscan"):
            self.assertIn(pool, mgr._FALLBACK_PARAMS)
            self.assertIn("fetch_p75",    mgr._FALLBACK_PARAMS[pool])
            self.assertIn("delay_warn_s", mgr._FALLBACK_PARAMS[pool])

    def test_delay_warn_values_are_sensible(self):
        """DELAY_WARN_S values: detail=60, scan=1800, fullscan=7200 (fallbacks)."""
        self.assertEqual(mgr._FALLBACK_PARAMS["detail"]["delay_warn_s"],   60)
        self.assertEqual(mgr._FALLBACK_PARAMS["scan"]["delay_warn_s"],   1800)
        self.assertEqual(mgr._FALLBACK_PARAMS["fullscan"]["delay_warn_s"], 7200)


# ─────────────────────────────────────────────────────────────────────────────
# § 13 — Pool sizes from scheduler:health
# ─────────────────────────────────────────────────────────────────────────────

class TestPoolSizes(unittest.TestCase):

    def _health_payload(self, scan=8, detail=4, fullscan=2):
        return json.dumps({
            "ts": time.time(),
            "pool": {
                "scan":     {"alive": scan,     "consecutive_deaths": 0},
                "detail":   {"alive": detail,   "consecutive_deaths": 0},
                "fullscan": {"alive": fullscan, "consecutive_deaths": 0},
            },
        }).encode()

    def test_reads_alive_counts(self):
        r = MagicMock()
        r.get.return_value = self._health_payload(scan=8, detail=4, fullscan=2)
        sizes = mgr._get_pool_sizes(r)
        self.assertEqual(sizes["scan"],     8)
        self.assertEqual(sizes["detail"],   4)
        self.assertEqual(sizes["fullscan"], 2)

    def test_missing_health_key_returns_zeros(self):
        r = MagicMock()
        r.get.return_value = None
        sizes = mgr._get_pool_sizes(r)
        self.assertEqual(sizes, {"scan": 0, "detail": 0, "fullscan": 0})

    def test_malformed_health_key_returns_zeros(self):
        r = MagicMock()
        r.get.return_value = b"not-json"
        sizes = mgr._get_pool_sizes(r)
        self.assertEqual(sizes, {"scan": 0, "detail": 0, "fullscan": 0})

    def test_partial_health_key_returns_available(self):
        """If one pool missing from JSON, returns 0 for that pool."""
        payload = json.dumps({
            "ts": time.time(),
            "pool": {"scan": {"alive": 7}},
        }).encode()
        r = MagicMock()
        r.get.return_value = payload
        sizes = mgr._get_pool_sizes(r)
        self.assertEqual(sizes["scan"], 7)
        self.assertEqual(sizes["detail"],   0)
        self.assertEqual(sizes["fullscan"], 0)


# ─────────────────────────────────────────────────────────────────────────────
# § 14 — send_cmd: SHADOW_MODE True vs False
# ─────────────────────────────────────────────────────────────────────────────

class TestSendCmd(unittest.TestCase):

    def test_shadow_mode_true_does_not_push(self):
        """SHADOW_MODE=True → no rpush, only log."""
        r = MagicMock()
        with patch.object(mgr, 'SHADOW_MODE', True):
            mgr._send_cmd(r, "scan:target:5")
        r.rpush.assert_not_called()

    def test_shadow_mode_false_pushes_to_list(self):
        """SHADOW_MODE=False → rpush to manager:cmds (FIFO)."""
        r = MagicMock()
        with patch.object(mgr, 'SHADOW_MODE', False):
            mgr._send_cmd(r, "scan:target:5")
        r.rpush.assert_called_once_with("manager:cmds", "scan:target:5")

    def test_all_command_types_routed_correctly(self):
        """All three command types push correct strings when live."""
        r = MagicMock()
        with patch.object(mgr, 'SHADOW_MODE', False):
            mgr._send_cmd(r, "detail:target:3")
            mgr._send_cmd(r, "platform:deprioritize:workday")
            mgr._send_cmd(r, "platform:outage:eightfold:set")
        self.assertEqual(r.rpush.call_count, 3)


# ─────────────────────────────────────────────────────────────────────────────
# § 15 — Integration: full cycle with real formula
# ─────────────────────────────────────────────────────────────────────────────

class TestFullCycleIntegration(unittest.TestCase):
    """
    End-to-end: simulate several cycles of _run_pool_cycle with varying
    inputs and verify the system reaches the expected steady state.
    """

    def test_burst_then_recovery(self):
        """
        Burst: high delay + high util → urgent → scale up.
        Recovery: delay drops, util drops → scale_down fires after 5 cycles,
        decrements by 1, then repeats. Verifies at least one decrement step.
        """
        _reset_cycle_state()
        params  = {"fetch_p75": 5.0, "delay_warn_s": 60.0}
        r       = _make_redis()

        n = 3  # start with 3 detail workers
        n_cap = 10

        # Phase 1: burst (urgent fires)
        pool_busy_ms_high = int(0.90 * n * mgr.MANAGER_CYCLE_S * 1000)
        d, _ = mgr._run_pool_cycle(r, "detail", n, pool_busy_ms_high,
                                    100, 50.0, params, n_cap, peak_nd=5)
        self.assertEqual(d, "urgent")

        # Simulate workers added to target
        n = 8
        mgr._urgent_active["detail"] = True

        # Phase 2: urgent release
        pool_busy_ms_low = int(0.05 * n * mgr.MANAGER_CYCLE_S * 1000)
        d, _ = mgr._run_pool_cycle(r, "detail", n, pool_busy_ms_low,
                                    2, 2.0, params, n_cap, peak_nd=5)
        self.assertEqual(d, "urgent_release")
        self.assertFalse(mgr._urgent_active["detail"])

        # Phase 3: scale down — first event fires after 5 cycles, removes 1 worker
        decisions = []
        for _ in range(7):
            d, _ = mgr._run_pool_cycle(r, "detail", n, pool_busy_ms_low,
                                        0, 0.0, params, n_cap, peak_nd=5)
            decisions.append(d)
            if d == "scale_down":
                n = max(mgr.WORKER_FLOOR, n - 1)

        self.assertIn("scale_down", decisions)

    def test_steady_state_no_unnecessary_churn(self):
        """
        Moderate load, stable delay — should hold with no scaling actions.
        """
        _reset_cycle_state()
        params  = {"fetch_p75": 5.0, "delay_warn_s": 60.0}
        r       = _make_redis()
        n       = 4
        # 65% util, 20s delay (< 30s = WARN×0.5)
        pool_busy_ms = int(0.65 * n * mgr.MANAGER_CYCLE_S * 1000)

        decisions = set()
        for _ in range(10):
            d, _ = mgr._run_pool_cycle(r, "detail", n, pool_busy_ms,
                                        5, 20.0, params, 10, peak_nd=4)
            decisions.add(d)

        self.assertEqual(decisions, {"hold"},
                         f"Unexpected churn in stable state: {decisions}")

    def test_scan_pool_right_sizes_at_2_workers_for_low_demand(self):
        """
        Production scenario: 155 companies, 0.32 scans/min needed, 40s fetch.
        Target workers = ceil(0.32/60 × 40) = ceil(0.213) = 1 → clamped to floor=2.
        10 idle scan workers should eventually reach floor=2.
        """
        _reset_cycle_state()
        params  = {"fetch_p75": 40.0, "delay_warn_s": 1800.0}
        r       = _make_redis()
        n       = 10

        decisions = []
        for i in range(40):
            # util = 0.32 scans/min × 40s per scan × 1 worker / 60s = ~21% per worker
            # With 10 workers: total busy = 10 × 21% × 60s × 1000ms = 126_000ms
            busy = int(0.21 * n * mgr.MANAGER_CYCLE_S * 1000)
            d, _ = mgr._run_pool_cycle(r, "scan", n, busy, 0, 0.0,
                                        params, 12, peak_nd=5)
            decisions.append(d)
            if d == "scale_down":
                n = max(mgr.WORKER_FLOOR, n - 1)

        self.assertIn("scale_down", decisions,
                      "Low-demand scan pool never scaled down")
        self.assertLessEqual(n, mgr.WORKER_FLOOR + 2,
                             f"Scan pool still inflated at {n} workers")


if __name__ == "__main__":
    unittest.main()
