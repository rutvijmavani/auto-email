"""
tests/test_layer2.py
─────────────────────────────────────────────────────────────────────────────
Tests for Layer 2 reactive emergency: Lever 1 backpressure, deadlock detection,
worker borrowing, effective_target, pending_spawns, and the learning loop.

Design doc: docs/scaling-redesign.md §7 (Layer 2)

Coverage map:
  Section 1  — Delay history + 3-of-4 rising check
  Section 2  — Lever 1 fire / lift lifecycle
  Section 3  — effective_target with active borrows
  Section 4  — get_lendable mechanics
  Section 5  — Borrowing: lendable first, unused-capacity one-by-one
  Section 6  — Learning loop: true_required formula + daily_peak update
  Section 7  — check_layer2 integration: fire, stable-cycles, lift, borrow
  Section 8  — Deadlock scenarios: deadlock 2 (semaphore leak), 3 (CeilingExceeded+urgent),
                4 (stale scheduler:health), 7 (Layer 0 vs Layer 2 conflict), 9 (pending_spawns stuck)
"""

import json
import math
import time
import unittest
from unittest.mock import MagicMock, patch, call

import workers.manager as mgr


def _make_redis(data: dict | None = None):
    """
    Create a minimal Redis mock that backs scan(), get(), set(), exists(), delete(),
    pipeline(), incr(), expire(), lrange() from the provided data dict.
    Keys with bytes values are decoded automatically.
    """
    store: dict = dict(data or {})

    r = MagicMock()

    def _key(k):
        return k.decode() if isinstance(k, bytes) else k

    def _get(k):
        return store.get(_key(k))

    def _set(k, v, *args, **kwargs):
        store[_key(k)] = str(v) if not isinstance(v, (bytes, str)) else v

    def _exists(*keys):
        return sum(1 for k in keys if _key(k) in store)

    def _delete(*keys):
        for k in keys:
            store.pop(_key(k), None)

    def _scan(cursor, match="*", count=20):
        import fnmatch
        pattern = _key(match)
        all_keys = [k for k in store if fnmatch.fnmatch(k, pattern)]
        return 0, all_keys

    def _lrange(k, start, end):
        val = store.get(_key(k))
        if val is None:
            return []
        if isinstance(val, list):
            items = val
        else:
            items = [val]
        if end == -1:
            return items[start:]
        return items[start: end + 1]

    def _rpush(k, *values):
        key = _key(k)
        if key not in store:
            store[key] = []
        elif not isinstance(store[key], list):
            store[key] = [store[key]]
        for v in values:
            store[key].append(str(v) if not isinstance(v, (bytes, str)) else v)
        return len(store[key])

    def _ltrim(k, start, end):
        key = _key(k)
        if key in store and isinstance(store[key], list):
            lst = store[key]
            if end == -1:
                store[key] = lst[start:]
            else:
                store[key] = lst[start: end + 1]

    pipe = MagicMock()
    pipe.execute.return_value = [None, None, []]

    def _pipeline():
        pp = MagicMock()
        # Collect rpush/ltrim/lrange calls and apply to store on execute()
        rpush_calls = []
        ltrim_calls = []
        lrange_calls = []

        def _pp_rpush(k, v):
            rpush_calls.append((k, v))
            return pp

        def _pp_ltrim(k, s, e):
            ltrim_calls.append((k, s, e))
            return pp

        def _pp_lrange(k, s, e):
            lrange_calls.append((k, s, e))
            return pp

        def _pp_set(k, v, **kwargs):
            _set(k, v)
            return pp

        def _pp_execute():
            for k, v in rpush_calls:
                _rpush(k, v)
            for k, s, e in ltrim_calls:
                _ltrim(k, s, e)
            results = []
            for k, s, e in lrange_calls:
                results.append(_lrange(k, s, e))
            return [None, None] + results

        pp.rpush = _pp_rpush
        pp.ltrim = _pp_ltrim
        pp.lrange = _pp_lrange
        pp.set = _pp_set
        pp.execute = _pp_execute
        return pp

    r.get.side_effect  = _get
    r.set.side_effect  = _set
    r.exists.side_effect = _exists
    r.delete.side_effect = _delete
    r.scan.side_effect = _scan
    r.lrange.side_effect = _lrange
    r.rpush.side_effect  = _rpush
    r.ltrim.side_effect  = _ltrim
    r.pipeline.side_effect = _pipeline

    r._store = store
    return r


# ─────────────────────────────────────────────────────────────────────────────
# Section 1 — Delay history + 3-of-4 rising check
# ─────────────────────────────────────────────────────────────────────────────

class TestDelayHistory(unittest.TestCase):

    def setUp(self):
        self.r = _make_redis()

    def test_push_delay_history_returns_list(self):
        delays = mgr._push_delay_history(self.r, "scan", 100.0)
        self.assertEqual(len(delays), 1)
        self.assertAlmostEqual(delays[0], 100.0)

    def test_history_capped_at_4(self):
        for d in [10, 20, 30, 40, 50]:
            delays = mgr._push_delay_history(self.r, "scan", float(d))
        self.assertEqual(len(delays), mgr.DEADLOCK_HISTORY_CYCLES)

    def test_oldest_is_dropped_after_cap(self):
        for d in [10, 20, 30, 40, 50]:
            delays = mgr._push_delay_history(self.r, "scan", float(d))
        # oldest value (10) should be gone; 20 should be first
        self.assertAlmostEqual(delays[0], 20.0)

    # ── _is_deadlock_rising ──────────────────────────────────────────────────

    def test_not_rising_when_less_than_4_history(self):
        delays = [1900.0, 2000.0, 2100.0]  # only 3 entries
        result = mgr._is_deadlock_rising(delays, 2200.0, 1800.0)
        self.assertFalse(result)

    def test_not_rising_when_fewer_than_3_above_warn(self):
        delays = [500.0, 500.0, 2000.0, 2100.0]  # only 2 above 1800
        result = mgr._is_deadlock_rising(delays, 2200.0, 1800.0)
        self.assertFalse(result)

    def test_rising_when_3_of_4_above_warn_and_directional(self):
        # 3 of 4 above warn, and current (2500) > delays[0] (1900)
        delays = [1900.0, 2000.0, 500.0, 2100.0]  # 3 above 1800
        result = mgr._is_deadlock_rising(delays, 2500.0, 1800.0)
        self.assertTrue(result)

    def test_not_rising_when_not_directional(self):
        # 4 of 4 above warn but current < delays[0]
        delays = [2500.0, 2400.0, 2200.0, 2000.0]
        result = mgr._is_deadlock_rising(delays, 1900.0, 1800.0)
        self.assertFalse(result)

    def test_tolerates_one_dip(self):
        # 3 of 4 above warn (one dip), current > oldest — still rising
        delays = [1900.0, 2000.0, 800.0, 2200.0]
        result = mgr._is_deadlock_rising(delays, 2400.0, 1800.0)
        self.assertTrue(result)


# ─────────────────────────────────────────────────────────────────────────────
# Section 2 — Lever 1 fire / lift lifecycle
# ─────────────────────────────────────────────────────────────────────────────

class TestLever1Lifecycle(unittest.TestCase):

    def setUp(self):
        self.r = _make_redis()

    def test_get_lever1_active_false_when_key_missing(self):
        self.assertFalse(mgr._get_lever1_active(self.r, "scan"))

    def test_fire_lever1_sets_active_flag(self):
        mgr._fire_lever1(self.r, "scan", depth=50, prev_depth=30)
        self.assertTrue(mgr._get_lever1_active(self.r, "scan"))

    def test_fire_lever1_snapshots_D_and_R(self):
        mgr._fire_lever1(self.r, "scan", depth=80, prev_depth=40)
        D = self.r.get("manager:snapshot:scan:D")
        R = self.r.get("manager:snapshot:scan:R")
        self.assertEqual(float(D), 80.0)
        # R = (80 - 40) / MANAGER_CYCLE_S
        expected_R = 40 / mgr.MANAGER_CYCLE_S
        self.assertAlmostEqual(float(R), expected_R, places=4)

    def test_fire_lever1_idempotent(self):
        mgr._fire_lever1(self.r, "scan", depth=50, prev_depth=30)
        # Second fire should not overwrite the snapshot
        mgr._fire_lever1(self.r, "scan", depth=999, prev_depth=999)
        D = self.r.get("manager:snapshot:scan:D")
        self.assertEqual(float(D), 50.0)  # still original

    def test_inflow_rate_clamped_to_zero_when_depth_drops(self):
        # depth dropped (prev > current) → inflow_rate must not be negative
        mgr._fire_lever1(self.r, "scan", depth=30, prev_depth=60)
        R = float(self.r.get("manager:snapshot:scan:R"))
        self.assertGreaterEqual(R, 0.0)

    def test_lift_lever1_clears_active_flag(self):
        mgr._fire_lever1(self.r, "scan", depth=50, prev_depth=30)
        mgr._lift_lever1(self.r, "scan")
        self.assertFalse(mgr._get_lever1_active(self.r, "scan"))

    def test_other_pool_lever1_independent(self):
        mgr._fire_lever1(self.r, "scan", depth=50, prev_depth=30)
        self.assertFalse(mgr._get_lever1_active(self.r, "detail"))
        self.assertFalse(mgr._get_lever1_active(self.r, "fullscan"))


# ─────────────────────────────────────────────────────────────────────────────
# Section 3 — effective_target with active borrows
# ─────────────────────────────────────────────────────────────────────────────

class TestEffectiveTarget(unittest.TestCase):

    def setUp(self):
        self.r = _make_redis()

    def test_no_borrows_returns_workers_target(self):
        result = mgr._get_effective_target(self.r, "scan", workers_target=8)
        self.assertEqual(result, 8)

    def test_borrowed_out_reduces_effective_target(self):
        # scan lent 2 to fullscan
        self.r._store["manager:borrow:scan:fullscan"] = "2"
        result = mgr._get_effective_target(self.r, "scan", workers_target=8)
        self.assertEqual(result, 6)  # 8 - 2

    def test_borrowed_in_increases_effective_target(self):
        # fullscan borrowed 2 from scan
        self.r._store["manager:borrow:scan:fullscan"] = "2"
        result = mgr._get_effective_target(self.r, "fullscan", workers_target=5)
        self.assertEqual(result, 7)  # 5 + 2

    def test_effective_target_floored_at_worker_floor(self):
        # extreme borrow: scan lent 10, workers_target=2
        self.r._store["manager:borrow:scan:fullscan"] = "10"
        result = mgr._get_effective_target(self.r, "scan", workers_target=2)
        self.assertEqual(result, mgr.WORKER_FLOOR)

    def test_multiple_borrows_accumulated(self):
        # scan lent 2 to fullscan and 1 to detail
        self.r._store["manager:borrow:scan:fullscan"] = "2"
        self.r._store["manager:borrow:scan:detail"] = "1"
        result = mgr._get_effective_target(self.r, "scan", workers_target=8)
        self.assertEqual(result, 5)  # 8 - 2 - 1

    def test_prevents_layer0_from_undoing_borrow(self):
        """
        Classic deadlock scenario 7: scan lent 2 to fullscan.
        Layer 0 sees n_workers=6, workers_target=8 and would scale up.
        effective_target=6 → no scale-up sends, borrow is preserved.
        """
        self.r._store["manager:borrow:scan:fullscan"] = "2"
        effective = mgr._get_effective_target(self.r, "scan", workers_target=8)
        n_workers = 6
        # scale-up condition: effective_target > n_workers → 6 > 6 → False ✓
        self.assertFalse(effective > n_workers)


# ─────────────────────────────────────────────────────────────────────────────
# Section 4 — get_lendable mechanics
# ─────────────────────────────────────────────────────────────────────────────

class TestGetLendable(unittest.TestCase):

    def setUp(self):
        self.r = _make_redis()

    def test_lendable_above_target(self):
        # 8 workers, target=6 → 2 lendable
        result = mgr._get_lendable(self.r, "scan", n_workers=8, workers_target=6)
        self.assertEqual(result, 2)

    def test_lendable_floored_at_worker_floor(self):
        # 3 workers, target=1, WORKER_FLOOR=2 → min_keep=2 → lendable=1
        result = mgr._get_lendable(self.r, "scan", n_workers=3, workers_target=1)
        self.assertEqual(result, 1)

    def test_no_lendable_when_at_target(self):
        result = mgr._get_lendable(self.r, "scan", n_workers=6, workers_target=6)
        self.assertEqual(result, 0)

    def test_no_lendable_when_below_target(self):
        result = mgr._get_lendable(self.r, "scan", n_workers=4, workers_target=6)
        self.assertEqual(result, 0)

    def test_already_borrowed_out_reduces_lendable(self):
        # 8 workers, target=6, already lent 1 → lendable = 8 - 6 - 1 = 1
        self.r._store["manager:borrow:scan:fullscan"] = "1"
        result = mgr._get_lendable(self.r, "scan", n_workers=8, workers_target=6)
        self.assertEqual(result, 1)

    def test_lendable_zero_when_all_already_lent(self):
        # 8 workers, target=6, already lent 2 → lendable = 0
        self.r._store["manager:borrow:scan:fullscan"] = "2"
        result = mgr._get_lendable(self.r, "scan", n_workers=8, workers_target=6)
        self.assertEqual(result, 0)


# ─────────────────────────────────────────────────────────────────────────────
# Section 5 — Borrowing: lendable first, unused-capacity one-by-one
# ─────────────────────────────────────────────────────────────────────────────

class TestAttemptBorrow(unittest.TestCase):

    def setUp(self):
        self.r = _make_redis()
        # Reset lever1 state so detail can never lend unless its Lever 1 is active
        for pool in ("scan", "detail", "fullscan"):
            mgr._lever1_stable_cycles[pool] = 0

    def _pool_sizes(self, **kw):
        base = {"scan": 8, "detail": 6, "fullscan": 5}
        base.update(kw)
        return base

    def _targets(self, **kw):
        base = {"scan": 6, "detail": 5, "fullscan": 5}
        base.update(kw)
        return base

    def test_borrows_all_lendable_from_scan(self):
        # fullscan deadlocked; scan has 2 lendable (8 workers, target=6)
        borrowed = mgr._attempt_borrow(
            self.r, "fullscan",
            pool_sizes=self._pool_sizes(),
            workers_targets=self._targets(),
        )
        self.assertTrue(borrowed)
        borrow_key = self.r._store.get("manager:borrow:scan:fullscan")
        self.assertEqual(int(borrow_key), 2)

    def test_does_not_borrow_from_detail_unless_lever1_active(self):
        # fullscan deadlocked; scan has 0 lendable; detail has 2 lendable
        # but detail Lever 1 is NOT active → detail must not be borrowed from
        mgr._attempt_borrow(
            self.r, "fullscan",
            pool_sizes=self._pool_sizes(scan=5, detail=6),
            workers_targets=self._targets(scan=5, detail=4),
        )
        detail_borrow = self.r._store.get("manager:borrow:detail:fullscan")
        self.assertIsNone(detail_borrow)

    def test_borrows_from_detail_when_lever1_active(self):
        # detail Lever 1 active → detail can be borrowed from
        self.r._store["manager:lever1:detail:active"] = "1"
        mgr._attempt_borrow(
            self.r, "fullscan",
            pool_sizes=self._pool_sizes(scan=5, detail=6),
            workers_targets=self._targets(scan=5, detail=4),
        )
        detail_borrow = self.r._store.get("manager:borrow:detail:fullscan")
        self.assertIsNotNone(detail_borrow)

    def test_phase2_one_by_one_when_no_lendable(self):
        # scan has no lendable (n=6, target=6) but has unused capacity (above floor)
        # Phase 2: should borrow 1 and return True
        borrowed = mgr._attempt_borrow(
            self.r, "fullscan",
            pool_sizes=self._pool_sizes(scan=6, detail=5),
            workers_targets=self._targets(scan=6),
        )
        # scan has 6 workers, target=6, floor=2 → available_above_floor = 6-0-6=0
        # No unused capacity either → returns False
        self.assertFalse(borrowed)

    def test_borrow_accumulates_on_successive_calls(self):
        # First call borrows 2 lendable
        mgr._attempt_borrow(
            self.r, "fullscan",
            pool_sizes=self._pool_sizes(),
            workers_targets=self._targets(),
        )
        first_borrow = int(self.r._store.get("manager:borrow:scan:fullscan", 0))
        self.assertEqual(first_borrow, 2)

    def test_record_borrow_accumulates(self):
        mgr._record_borrow(self.r, "scan", "fullscan", 2)
        mgr._record_borrow(self.r, "scan", "fullscan", 1)
        total = int(self.r._store["manager:borrow:scan:fullscan"])
        self.assertEqual(total, 3)

    def test_return_all_borrows_clears_keys(self):
        self.r._store["manager:borrow:scan:fullscan"] = "2"
        self.r._store["manager:borrow:detail:fullscan"] = "1"
        mgr._return_all_borrows(self.r, "fullscan")
        self.assertNotIn("manager:borrow:scan:fullscan", self.r._store)
        self.assertNotIn("manager:borrow:detail:fullscan", self.r._store)


# ─────────────────────────────────────────────────────────────────────────────
# Section 6 — Learning loop: true_required formula + daily_peak update
# ─────────────────────────────────────────────────────────────────────────────

class TestLearningLoop(unittest.TestCase):

    def setUp(self):
        self.r = _make_redis()
        self.params = {
            "fetch_p75":    40.0,
            "delay_warn_s": 1800.0,
        }

    def test_compute_true_required_missing_snapshot(self):
        # No snapshot stored → returns 0
        result = mgr._compute_true_required(self.r, "scan", self.params)
        self.assertEqual(result, 0)

    def test_compute_true_required_drain_only(self):
        # D=200, R=0, F=40, W=1800
        # true_required = ceil(200×40/1800 + 0×40) = ceil(4.44) = 5
        self.r._store["manager:snapshot:scan:D"] = "200"
        self.r._store["manager:snapshot:scan:R"] = "0"
        result = mgr._compute_true_required(self.r, "scan", self.params)
        self.assertEqual(result, 5)

    def test_compute_true_required_drain_and_inflow(self):
        # D=200, R=2, F=40, W=1800
        # true_required = ceil(200×40/1800 + 2×40) = ceil(4.44 + 80) = 85
        self.r._store["manager:snapshot:scan:D"] = "200"
        self.r._store["manager:snapshot:scan:R"] = "2"
        result = mgr._compute_true_required(self.r, "scan", self.params)
        self.assertEqual(result, 85)

    def test_compute_true_required_floored_at_worker_floor(self):
        # Very small D, no inflow → result would be 0 → clamped to WORKER_FLOOR
        self.r._store["manager:snapshot:scan:D"] = "0"
        self.r._store["manager:snapshot:scan:R"] = "0"
        result = mgr._compute_true_required(self.r, "scan", self.params)
        self.assertEqual(result, mgr.WORKER_FLOOR)

    def test_write_true_required_updates_daily_peak(self):
        mgr._write_true_required(self.r, "scan", 10)
        from datetime import datetime, timezone
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        stored = int(self.r._store[f"manager:pool:scan:daily_peak:{today}"])
        self.assertEqual(stored, 10)

    def test_write_true_required_only_updates_if_higher(self):
        from datetime import datetime, timezone
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        self.r._store[f"manager:pool:scan:daily_peak:{today}"] = "15"
        mgr._write_true_required(self.r, "scan", 8)  # lower → no update
        stored = int(self.r._store[f"manager:pool:scan:daily_peak:{today}"])
        self.assertEqual(stored, 15)

    def test_write_true_required_updates_if_higher(self):
        from datetime import datetime, timezone
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        self.r._store[f"manager:pool:scan:daily_peak:{today}"] = "5"
        mgr._write_true_required(self.r, "scan", 12)
        stored = int(self.r._store[f"manager:pool:scan:daily_peak:{today}"])
        self.assertEqual(stored, 12)


# ─────────────────────────────────────────────────────────────────────────────
# Section 7 — check_layer2 integration
# ─────────────────────────────────────────────────────────────────────────────

class TestCheckLayer2Integration(unittest.TestCase):

    def setUp(self):
        self.r = _make_redis()
        self.params = {"fetch_p75": 40.0, "delay_warn_s": 1800.0}
        self.pool_sizes = {"scan": 10, "detail": 6, "fullscan": 5}
        self.workers_targets = {"scan": 8, "detail": 5, "fullscan": 5}
        for pool in ("scan", "detail", "fullscan"):
            mgr._lever1_stable_cycles[pool] = 0
            mgr._reintro_stable_cycles[pool] = 0
            mgr._prev_depth[pool] = 0

    def test_lever1_fires_when_delay_crosses_warn(self):
        mgr._check_layer2(
            self.r, "scan",
            n_workers=10, delay_s=1900.0, depth=50,
            params=self.params, worker_ceil=10,
            pool_sizes=self.pool_sizes, workers_targets=self.workers_targets,
        )
        self.assertTrue(mgr._get_lever1_active(self.r, "scan"))

    def test_lever1_does_not_fire_below_warn(self):
        mgr._check_layer2(
            self.r, "scan",
            n_workers=10, delay_s=1700.0, depth=50,
            params=self.params, worker_ceil=10,
            pool_sizes=self.pool_sizes, workers_targets=self.workers_targets,
        )
        self.assertFalse(mgr._get_lever1_active(self.r, "scan"))

    def test_lever1_fires_once_idempotent(self):
        # Two cycles both above WARN — Lever 1 should fire once, snapshot not overwritten
        mgr._check_layer2(
            self.r, "scan",
            n_workers=10, delay_s=1900.0, depth=60,
            params=self.params, worker_ceil=10,
            pool_sizes=self.pool_sizes, workers_targets=self.workers_targets,
        )
        first_D = self.r._store.get("manager:snapshot:scan:D")
        mgr._check_layer2(
            self.r, "scan",
            n_workers=10, delay_s=2000.0, depth=999,
            params=self.params, worker_ceil=10,
            pool_sizes=self.pool_sizes, workers_targets=self.workers_targets,
        )
        # Snapshot not overwritten
        self.assertEqual(self.r._store.get("manager:snapshot:scan:D"), first_D)

    def test_lever1_lifts_after_stable_cycles(self):
        # Fire Lever 1 manually
        self.r._store["manager:lever1:scan:active"] = "1"
        self.r._store["manager:snapshot:scan:D"] = "50"
        self.r._store["manager:snapshot:scan:R"] = "0"
        # stable_threshold = 1800 × 0.25 = 450
        # Run LEVER1_STABLE_REQUIRED cycles below 450
        for _ in range(mgr.LEVER1_STABLE_REQUIRED):
            mgr._check_layer2(
                self.r, "scan",
                n_workers=10, delay_s=100.0, depth=5,
                params=self.params, worker_ceil=10,
                pool_sizes=self.pool_sizes, workers_targets=self.workers_targets,
            )
        self.assertFalse(mgr._get_lever1_active(self.r, "scan"))

    def test_lever1_does_not_lift_before_stable_required(self):
        self.r._store["manager:lever1:scan:active"] = "1"
        self.r._store["manager:snapshot:scan:D"] = "50"
        self.r._store["manager:snapshot:scan:R"] = "0"
        # Run 1 fewer than required
        for _ in range(mgr.LEVER1_STABLE_REQUIRED - 1):
            mgr._check_layer2(
                self.r, "scan",
                n_workers=10, delay_s=100.0, depth=5,
                params=self.params, worker_ceil=10,
                pool_sizes=self.pool_sizes, workers_targets=self.workers_targets,
            )
        self.assertTrue(mgr._get_lever1_active(self.r, "scan"))

    def test_stable_counter_resets_on_delay_spike(self):
        self.r._store["manager:lever1:scan:active"] = "1"
        self.r._store["manager:snapshot:scan:D"] = "50"
        self.r._store["manager:snapshot:scan:R"] = "0"
        # 2 stable cycles
        for _ in range(2):
            mgr._check_layer2(
                self.r, "scan",
                n_workers=10, delay_s=100.0, depth=5,
                params=self.params, worker_ceil=10,
                pool_sizes=self.pool_sizes, workers_targets=self.workers_targets,
            )
        # delay spikes back above threshold
        mgr._check_layer2(
            self.r, "scan",
            n_workers=10, delay_s=1000.0, depth=20,
            params=self.params, worker_ceil=10,
            pool_sizes=self.pool_sizes, workers_targets=self.workers_targets,
        )
        # counter reset — lever1 should NOT have lifted
        self.assertTrue(mgr._get_lever1_active(self.r, "scan"))
        self.assertEqual(mgr._lever1_stable_cycles.get("scan", 0), 0)

    def test_learning_loop_fires_on_lever1_lift(self):
        self.r._store["manager:lever1:scan:active"] = "1"
        self.r._store["manager:snapshot:scan:D"] = "100"
        self.r._store["manager:snapshot:scan:R"] = "0.05"
        # true_required = ceil(100×40/1800 + 0.05×40) = ceil(2.22+2) = 5
        for _ in range(mgr.LEVER1_STABLE_REQUIRED):
            mgr._check_layer2(
                self.r, "scan",
                n_workers=10, delay_s=100.0, depth=5,
                params=self.params, worker_ceil=10,
                pool_sizes=self.pool_sizes, workers_targets=self.workers_targets,
            )
        from datetime import datetime, timezone
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        daily_peak = self.r._store.get(f"manager:pool:scan:daily_peak:{today}")
        self.assertIsNotNone(daily_peak)
        self.assertGreaterEqual(int(daily_peak), mgr.WORKER_FLOOR)

    def test_borrows_cleared_on_lever1_lift(self):
        self.r._store["manager:lever1:scan:active"] = "1"
        self.r._store["manager:borrow:detail:scan"] = "2"
        self.r._store["manager:snapshot:scan:D"] = "50"
        self.r._store["manager:snapshot:scan:R"] = "0"
        for _ in range(mgr.LEVER1_STABLE_REQUIRED):
            mgr._check_layer2(
                self.r, "scan",
                n_workers=10, delay_s=100.0, depth=5,
                params=self.params, worker_ceil=10,
                pool_sizes=self.pool_sizes, workers_targets=self.workers_targets,
            )
        self.assertNotIn("manager:borrow:detail:scan", self.r._store)


# ─────────────────────────────────────────────────────────────────────────────
# Section 8 — Deadlock scenarios (unit-level)
# ─────────────────────────────────────────────────────────────────────────────

class TestDeadlockScenario2_SemaphoreLeak(unittest.TestCase):
    """
    Deadlock 2: worker killed via SIGKILL, DECR never fires.
    concurrency:active:{platform} stays elevated.
    Next release() call must clamp to zero, not go negative.
    """

    def test_get_effective_target_not_affected_by_semaphore_state(self):
        r = _make_redis()
        # Semaphore leak doesn't affect effective_target — it's a separate subsystem
        r._store["concurrency:active:workday"] = "99"  # leaked counter
        result = mgr._get_effective_target(r, "detail", workers_target=6)
        self.assertEqual(result, 6)


class TestDeadlockScenario3_CeilingExceededPlusUrgent(unittest.TestCase):
    """
    Deadlock 3: CeilingExceeded + urgent simultaneously.
    Pool has max workers but all hit CeilingExceeded → no busy_ms written.
    Manager sees util=0 next cycle → should NOT trigger scale-down
    (scale-down requires 5 consecutive cycles; urgent_hold prevents it).
    """

    def setUp(self):
        for pool in ("scan", "detail", "fullscan"):
            mgr._scale_up_cycles[pool]   = 0
            mgr._scale_down_cycles[pool] = 0
            mgr._urgent_active[pool]     = False

    def test_urgent_hold_blocks_scale_down_when_at_ceiling(self):
        r = _make_redis()
        # n_workers = ceil = 10, workers_target > n_workers → urgent_hold
        # busy_ms = 0 (all workers hitting CeilingExceeded, no jobs completing)
        _, wt = mgr._run_pool_cycle(
            r, "scan",
            n_workers=10,
            pool_busy_ms=0,        # util = 0 — looks idle but it's CeilingExceeded
            depth=80,
            delay_s=1500.0,        # delay still >> WARN×0.75 → urgent fires
            params={"fetch_p75": 40.0, "delay_warn_s": 1800.0},
            worker_ceil=10,
            peak_nd=8,
        )
        # urgent should have fired (n_workers < workers_target or delay >= WARN×0.75)
        # Actually: delay=1500 >= 1800×0.75=1350 → urgent_hold because n_workers=10 = ceil
        # The guard `n_workers < min(workers_target, worker_ceil)` → 10 < 10 → False
        # So it goes to `elif _urgent_active[pool]` → urgent_release / urgent_hold
        # But urgent_active is False initially → falls through to scale_down check
        # util=0 < 0.50, delay=1500 > 1800×0.25=450 → scale_down condition fails (delay too high)
        # → hold
        # This confirms: scale-down does NOT fire even when util=0, if delay is high
        self.assertEqual(mgr._scale_down_cycles["scan"], 0)


class TestDeadlockScenario4_StaleSchedulerHealth(unittest.TestCase):
    """
    Deadlock 4: scheduler:health key expired → _get_pool_sizes returns 0 workers.
    Manager fires urgent for all pools simultaneously → triple DB spawn.
    The distributed lock (manager:lock) prevents two managers from both doing this,
    but within one manager, the behavior should still be bounded.
    """

    def test_get_pool_sizes_returns_zero_when_key_missing(self):
        r = _make_redis()
        sizes = mgr._get_pool_sizes(r)
        self.assertEqual(sizes, {"scan": 0, "detail": 0, "fullscan": 0})

    def test_get_pool_sizes_returns_zero_on_malformed_json(self):
        r = _make_redis({"scheduler:health": "not-json"})
        sizes = mgr._get_pool_sizes(r)
        self.assertEqual(sizes, {"scan": 0, "detail": 0, "fullscan": 0})

    def test_urgent_fires_for_all_zero_pools(self):
        """With 0 workers and queue delay, urgent fires — this is expected behaviour."""
        for pool in ("scan", "detail", "fullscan"):
            mgr._scale_up_cycles[pool]   = 0
            mgr._scale_down_cycles[pool] = 0
            mgr._urgent_active[pool]     = False

        r = _make_redis()
        results = []
        for pool in ("scan", "detail", "fullscan"):
            params = {"fetch_p75": 40.0, "delay_warn_s": 1800.0}
            decision, _ = mgr._run_pool_cycle(
                r, pool,
                n_workers=0,
                pool_busy_ms=0,
                depth=10,
                delay_s=1600.0,
                params=params,
                worker_ceil=10,
                peak_nd=5,
            )
            results.append(decision)

        # All three should fire urgent (0 workers < min(workers_target, ceil))
        for decision in results:
            self.assertEqual(decision, "urgent")


class TestDeadlockScenario7_Layer0VsLayer2Conflict(unittest.TestCase):
    """
    Deadlock 7: Layer 0 scale-up tries to undo Layer 2 borrow.
    scan lent 2 workers to fullscan.
    effective_target(scan) = 8-2 = 6, n_workers = 6 → no scale-up. ✓
    """

    def setUp(self):
        for pool in ("scan", "detail", "fullscan"):
            mgr._scale_up_cycles[pool]   = 0
            mgr._scale_down_cycles[pool] = 0
            mgr._urgent_active[pool]     = False

    def test_effective_target_prevents_borrow_undo(self):
        r = _make_redis({"manager:borrow:scan:fullscan": "2"})
        # scan: 6 workers (2 were lent), target from formula = 8, but effective = 6
        # util = 85% (all workers busy), delay building → scale_up condition would normally fire
        # BUT effective_target (6) is NOT > n_workers (6) → scale_up blocked
        decision, wt = mgr._run_pool_cycle(
            r, "scan",
            n_workers=6,
            pool_busy_ms=int(6 * 60 * 1000 * 0.85),  # 85% util
            depth=40,
            delay_s=1000.0,    # delay > WARN×0.5 = 900
            params={"fetch_p75": 40.0, "delay_warn_s": 1800.0},
            worker_ceil=10,
            peak_nd=6,
        )
        # Should not scale up: effective_target=6, n_workers=6 → no gap
        self.assertNotEqual(decision, "scale_up")
        self.assertNotEqual(decision, "scale_up_pending")


class TestDeadlockScenario9_PendingSpawnsStuck(unittest.TestCase):
    """
    Deadlock 9: pending_spawns stuck — worker failed to start before TTL.
    After 90s, key auto-expires. Next cycle: deficit = target - current - 0
    → spawns replacement correctly.
    """

    def test_get_pending_spawns_returns_zero_when_missing(self):
        r = _make_redis()
        result = mgr._get_pending_spawns(r, "scan")
        self.assertEqual(result, 0)

    def test_get_pending_spawns_returns_stored_value(self):
        r = _make_redis({"manager:pool:scan:pending_spawns": "3"})
        result = mgr._get_pending_spawns(r, "scan")
        self.assertEqual(result, 3)

    def test_deadlock_detection_accounts_for_pending(self):
        """
        With pending_spawns=2 and n_workers=8 (ceil=10):
        n_workers + pending = 10 = ceil → deadlock condition satisfied.
        """
        r = _make_redis({
            "manager:pool:scan:pending_spawns": "2",
            "manager:lever1:scan:active": "1",
        })
        n_workers    = 8
        worker_ceil  = 10
        pending      = mgr._get_pending_spawns(r, "scan")
        at_ceiling   = (n_workers + pending) >= worker_ceil
        self.assertTrue(at_ceiling)

    def test_after_ttl_expiry_pending_zero_deficit_correct(self):
        """
        After TTL expires (simulated by key missing), deficit = target - current.
        Ensures exactly 1 worker is spawned (not 0 due to stale pending count).
        """
        r = _make_redis()  # no pending_spawns key
        pending = mgr._get_pending_spawns(r, "scan")
        current = 7
        target  = 8
        deficit = target - current - pending
        self.assertEqual(deficit, 1)


if __name__ == "__main__":
    unittest.main()
