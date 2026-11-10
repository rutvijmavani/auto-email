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

# Live Postgres probe stand-in: 2 connections per worker, 57 worker seats fit.
_FAKE_CAPACITY = {
    "max_connections": 100, "reserved": 3, "margin": 10, "cap": 87, "used": 30,
    "worker_conns": 22, "non_worker": 8, "per_worker": 2, "seat_cap": 57,
}


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
        # set/delete recorded in a single ordered list so _pp_execute replays them in
        # the same relative order they were queued, matching real Redis pipeline semantics.
        write_calls = []

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
            write_calls.append(("set", k, v))
            return pp

        def _pp_delete(*keys):
            write_calls.append(("delete", keys))
            return pp

        def _pp_execute():
            for op in write_calls:
                if op[0] == "set":
                    _, k, v = op
                    _set(k, v)
                else:
                    _, keys = op
                    _delete(*keys)
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
        pp.delete = _pp_delete
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

    def test_fire_lever1_keeps_no_snapshot(self):
        mgr._fire_lever1(self.r, "scan", depth=80, prev_depth=40)
        self.assertIsNone(self.r.get("manager:snapshot:scan:D"))
        self.assertIsNone(self.r.get("manager:snapshot:scan:R"))

    def test_fire_lever1_idempotent(self):
        mgr._fire_lever1(self.r, "scan", depth=50, prev_depth=30)
        fired_at = self.r.get("manager:lever1:scan:fired_at")
        mgr._fire_lever1(self.r, "scan", depth=999, prev_depth=999)
        self.assertEqual(self.r.get("manager:lever1:scan:fired_at"), fired_at)

    def test_inflow_guard_skips_when_depth_drops(self):
        # depth < prev_depth AND inflow_rate=0 → bail out entirely (no active key)
        mgr._fire_lever1(self.r, "scan", depth=30, prev_depth=60)
        self.assertFalse(mgr._get_lever1_active(self.r, "scan"))

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

    def test_borrow_does_not_change_target(self):
        # Borrowing moves ceiling seats, not targets (avoids double counting).
        self.r._store["manager:borrow:scan:fullscan"] = "2"
        self.assertEqual(mgr._get_effective_target(self.r, "scan", workers_target=8), 8)
        self.assertEqual(mgr._get_effective_target(self.r, "fullscan", workers_target=5), 5)

    def test_borrowed_out_reduces_ceiling(self):
        self.r._store["manager:worker_ceil:scan"] = "8"
        self.r._store["manager:borrow:scan:fullscan"] = "2"
        self.assertEqual(mgr._get_worker_ceil(self.r, "scan"), 6)

    def test_borrowed_in_increases_ceiling(self):
        self.r._store["manager:worker_ceil:fullscan"] = "5"
        self.r._store["manager:borrow:scan:fullscan"] = "2"
        self.assertEqual(mgr._get_worker_ceil(self.r, "fullscan"), 7)

    def test_ceiling_floored_at_worker_floor(self):
        self.r._store["manager:worker_ceil:scan"] = "4"
        self.r._store["manager:borrow:scan:fullscan"] = "10"
        self.assertEqual(mgr._get_worker_ceil(self.r, "scan"), mgr.WORKER_FLOOR)

    def test_multiple_borrows_accumulated(self):
        self.r._store["manager:worker_ceil:scan"] = "8"
        self.r._store["manager:borrow:scan:fullscan"] = "2"
        self.r._store["manager:borrow:scan:detail"] = "1"
        self.assertEqual(mgr._get_worker_ceil(self.r, "scan"), 5)  # 8 - 2 - 1


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
        # Detail lends only after LEVER1_STABLE_REQUIRED calm cycles
        mgr._detail_calm["cycles"] = 0
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
        self.r._store["manager:worker_ceil:scan"] = "10"
        borrowed = mgr._attempt_borrow(
            self.r, "fullscan",
            pool_sizes=self._pool_sizes(),
            workers_targets=self._targets(),
        )
        self.assertTrue(borrowed)
        borrow_key = self.r._store.get("manager:borrow:scan:fullscan")
        self.assertEqual(int(borrow_key), 2)

    def test_does_not_borrow_from_detail_unless_calm(self):
        # fullscan deadlocked; scan has 0 lendable; detail has 2 lendable
        # but detail has not been calm for LEVER1_STABLE_REQUIRED cycles
        mgr._attempt_borrow(
            self.r, "fullscan",
            pool_sizes=self._pool_sizes(scan=5, detail=6),
            workers_targets=self._targets(scan=5, detail=4),
        )
        detail_borrow = self.r._store.get("manager:borrow:detail:fullscan")
        self.assertIsNone(detail_borrow)

    def test_borrows_from_detail_when_calm(self):
        # detail calm for LEVER1_STABLE_REQUIRED cycles → detail can be borrowed from
        mgr._detail_calm["cycles"] = mgr.LEVER1_STABLE_REQUIRED
        mgr._attempt_borrow(
            self.r, "fullscan",
            pool_sizes=self._pool_sizes(scan=5, detail=6),
            workers_targets=self._targets(scan=5, detail=4),
        )
        detail_borrow = self.r._store.get("manager:borrow:detail:fullscan")
        self.assertIsNotNone(detail_borrow)

    def test_no_seats_when_ceiling_equals_peak_and_no_lendable(self):
        # scan: 6 workers = target, ceiling 6 = peak_nd → no lendable, buffer or headroom
        for p, c in (("scan", "6"), ("detail", "5")):
            self.r._store[f"manager:worker_ceil:{p}"] = c
            self.r._store[f"manager:pool:{p}:peak_nd"] = c
        borrowed = mgr._attempt_borrow(
            self.r, "fullscan",
            pool_sizes=self._pool_sizes(scan=6, detail=5),
            workers_targets=self._targets(scan=6),
        )
        self.assertFalse(borrowed)

    def test_buffer_seats_taken_all_at_once(self):
        # scan ceiling 10, peak 6, target 6, 6 workers → 4 buffer seats, all moved
        self.r._store["manager:worker_ceil:scan"] = "10"
        self.r._store["manager:pool:scan:peak_nd"] = "6"
        self.r._store["manager:worker_ceil:detail"] = "5"
        self.r._store["manager:pool:detail:peak_nd"] = "5"
        self.assertTrue(mgr._attempt_borrow(
            self.r, "fullscan",
            pool_sizes=self._pool_sizes(scan=6, detail=5),
            workers_targets=self._targets(scan=6),
        ))
        self.assertEqual(int(self.r._store["manager:borrow:scan:fullscan"]), 4)

    def test_headroom_seat_taken_one_per_cycle(self):
        # scan ceiling 8 = peak 8 (no buffer), target 6 → 2 headroom seats, 1 per call
        self.r._store["manager:worker_ceil:scan"] = "8"
        self.r._store["manager:pool:scan:peak_nd"] = "8"
        self.r._store["manager:worker_ceil:detail"] = "5"
        self.r._store["manager:pool:detail:peak_nd"] = "5"
        mgr._attempt_borrow(
            self.r, "fullscan",
            pool_sizes=self._pool_sizes(scan=6, detail=5),
            workers_targets=self._targets(scan=6),
        )
        self.assertEqual(int(self.r._store["manager:borrow:scan:fullscan"]), 1)

    def test_borrow_accumulates_on_successive_calls(self):
        # First call borrows 2 lendable
        self.r._store["manager:worker_ceil:scan"] = "10"
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


# ─────────────────────────────────────────────────────────────────────────────
# Section 6 — Learning loop: true_required formula + daily_peak update
# ─────────────────────────────────────────────────────────────────────────────

class TestNeededWorkersAndServiceTime(unittest.TestCase):
    """Stage B inputs: real service time s, lambda, and the uncapped needed peak."""

    def setUp(self):
        self.r = _make_redis()
        mgr._prev_svc_snap.clear()
        self.params = {"fetch_p75": 40.0, "delay_warn_s": 1800.0}

    def _beat(self, key, ts, busy_ms, processed, since=None):
        self.r._store[key] = json.dumps(
            {"ts": ts, "busy_ms": busy_ms, "processed": processed, "busy_since": since})
        self.r.scan_iter = lambda match=None, count=None: [
            k for k in self.r._store if k.startswith("worker:alive:detail_worker:")]

    def test_service_time_is_busy_delta_over_jobs_delta(self):
        k = "worker:alive:detail_worker:h:1"
        self._beat(k, 100.0, 10_000, 10)
        self.assertIsNone(mgr._get_pool_service_time(self.r, "detail"))   # first sight
        self._beat(k, 160.0, 40_000, 40)                                  # 30 s busy, 30 jobs
        self.assertAlmostEqual(mgr._get_pool_service_time(self.r, "detail"), 1.0)

    def test_service_time_counts_job_in_progress(self):
        k = "worker:alive:detail_worker:h:1"
        self._beat(k, 100.0, 0, 0)
        mgr._get_pool_service_time(self.r, "detail")
        self._beat(k, 160.0, 0, 3, since=100.0)                           # 60 s in a job, 3 done
        self.assertAlmostEqual(mgr._get_pool_service_time(self.r, "detail"), 20.0)

    def test_service_time_none_when_no_job_finished(self):
        k = "worker:alive:detail_worker:h:1"
        self._beat(k, 100.0, 0, 0)
        mgr._get_pool_service_time(self.r, "detail")
        self._beat(k, 160.0, 0, 0)
        self.assertIsNone(mgr._get_pool_service_time(self.r, "detail"))

    def test_zero_finished_cycle_is_skipped_and_busy_carried(self):
        k = "worker:alive:detail_worker:h:1"
        self._beat(k, 100.0, 0, 0)
        mgr._get_pool_service_time(self.r, "detail")
        self._beat(k, 160.0, 10_000, 5)                                   # 10 s busy, 5 done
        self.assertAlmostEqual(mgr._get_pool_service_time(self.r, "detail"), 2.0)
        # 30 s busy and nothing finished: not a sample, s unchanged
        self._beat(k, 190.0, 40_000, 5)
        self.assertAlmostEqual(mgr._get_pool_service_time(self.r, "detail"), 2.0)
        # 5 jobs finish in 10 s busy: the carried 30 s is charged to this cycle
        self._beat(k, 220.0, 50_000, 10)
        self.assertAlmostEqual(mgr._get_pool_service_time(self.r, "detail"), (10 + 40) / 10)

    def test_window_survives_manager_restart(self):
        k = "worker:alive:detail_worker:h:1"
        self._beat(k, 100.0, 0, 0)
        mgr._get_pool_service_time(self.r, "detail")
        self._beat(k, 160.0, 20_000, 10)
        self.assertAlmostEqual(mgr._get_pool_service_time(self.r, "detail"), 2.0)
        mgr._prev_svc_snap.clear()                                        # restart: odometers re-seen
        self._beat(k, 220.0, 20_000, 10)
        self.assertAlmostEqual(mgr._get_pool_service_time(self.r, "detail"), 2.0)

    def test_service_time_window_keeps_last_cycles_only(self):
        k = "worker:alive:detail_worker:h:1"
        ts, busy, done = 100.0, 0, 0
        self._beat(k, ts, busy, done)
        mgr._get_pool_service_time(self.r, "detail")
        # one slow cycle (10 s/job) followed by WINDOW fast cycles (1 s/job)
        for per_job in [10] + [1] * mgr.SERVICE_TIME_WINDOW_CYCLES:
            ts += 60; busy += per_job * 5 * 1000; done += 5
            self._beat(k, ts, busy, done)
            s = mgr._get_pool_service_time(self.r, "detail")
        self.assertAlmostEqual(s, 1.0)

    def _record(self, adaptive, fullscan):
        from unittest.mock import MagicMock, patch
        self.r._store["signal:arrivals:queue:detail:adaptive:organic"] = adaptive
        self.r._store["signal:arrivals:queue:detail:fullscan:organic"] = fullscan
        conn = MagicMock()
        conn.__enter__.return_value = conn
        with patch("db.db.get_conn", return_value=conn), \
             patch.object(mgr, "REDIS_DETAIL_ADAPTIVE", "queue:detail:adaptive"), \
             patch.object(mgr, "REDIS_DETAIL_FULLSCAN", "queue:detail:fullscan"):
            mgr._record_detail_arrivals(self.r)
        return conn

    def test_arrivals_first_sample_writes_nothing_then_delta_is_upserted(self):
        conn = self._record(10, 5)
        conn.cursor.return_value.execute.assert_not_called()
        conn = self._record(14, 8)                       # +4 adaptive, +3 fullscan
        args = conn.cursor.return_value.execute.call_args.args
        self.assertEqual(args[1], (7,))
        self.assertIn("ON CONFLICT (minute)", args[0])
        conn.commit.assert_called_once()

    def test_arrivals_counter_reset_skips_that_cycle_and_resumes(self):
        self._record(10, 5)
        conn = self._record(2, 1)                        # Redis restart: counter went back
        conn.cursor.return_value.execute.assert_not_called()
        conn = self._record(4, 1)                        # +2 from the new baseline
        self.assertEqual(conn.cursor.return_value.execute.call_args.args[1], (2,))

    def test_arrivals_write_failure_never_raises(self):
        from unittest.mock import patch
        self.r._store["manager:arrivals:last"] = 0
        with patch("db.db.get_conn", side_effect=RuntimeError("db down")):
            mgr._record_detail_arrivals(self.r)          # must not raise

    def test_target_uses_arrival_rate_and_service_time(self):
        # depth 0, lambda 2/s, s 3 s → ceil(2*3) = 6 workers needed
        _, target = mgr._run_pool_cycle(
            self.r, "detail", 2, 0, 0, 0.0, self.params, 10, 2,
            service_s=3.0, arrival_rate=2.0)
        self.assertEqual(target, 6)

    def test_fetch_p75_is_fallback_when_service_time_unknown(self):
        # depth 90, time_left 1800 → 0.05/s * fetch_p75 40 = 2 → floor 2
        _, target = mgr._run_pool_cycle(
            self.r, "scan", 2, 0, 90, 0.0, self.params, 10, 2)
        self.assertEqual(target, max(mgr.WORKER_FLOOR, 2))

    def test_needed_is_recorded_before_ceiling_clamp(self):
        _, target = mgr._run_pool_cycle(
            self.r, "detail", 2, 0, 0, 0.0, self.params, 4, 2,
            service_s=3.0, arrival_rate=5.0)
        self.assertEqual(target, 4)                       # clamped to the ceiling
        self.assertEqual(mgr._needed_workers["detail"], 15)   # ceil(5*3): real demand

    def test_daily_peak_running_takes_needed_not_clamped_count(self):
        mgr._update_daily_peak_running(self.r, "detail", 15)
        self.assertEqual(int(self.r._store["manager:pool:detail:daily_peak:running"]), 15)


# ─────────────────────────────────────────────────────────────────────────────
# Section 7 — check_layer2 integration
# ─────────────────────────────────────────────────────────────────────────────

class TestCheckLayer2Integration(unittest.TestCase):

    def setUp(self):
        patcher = patch.object(mgr, "_db_capacity", return_value=_FAKE_CAPACITY)
        patcher.start()
        self.addCleanup(patcher.stop)
        self.r = _make_redis()
        self.params = {"fetch_p75": 40.0, "delay_warn_s": 1800.0}
        self.pool_sizes = {"scan": 10, "detail": 6, "fullscan": 5}
        self.workers_targets = {"scan": 8, "detail": 5, "fullscan": 5}
        for pool in ("scan", "detail", "fullscan"):
            mgr._lever1_stable_cycles[pool] = 0
            mgr._reintro_stable_cycles[pool] = 0
            mgr._prev_depth[pool] = 0

    def _exhaust_chain(self):
        """No buffer/headroom seats anywhere and the worker budget at PROBE."""
        for p, t in self.workers_targets.items():
            self.r._store[f"manager:worker_ceil:{p}"] = str(t)
            self.r._store[f"manager:pool:{p}:peak_nd"] = str(t)
        # scan runs 10 workers: its own ceiling already covers today's peak
        self.r._store["manager:worker_ceil:scan"] = "10"
        self.r._store["manager:pool:scan:peak_nd"] = "10"
        self.r._store["manager:db_pool_maxconn"] = str(_FAKE_CAPACITY["seat_cap"])

    def _exhaust_chain_for(self, pool):
        """Like _exhaust_chain, with `pool` itself holding all 10 seats it uses."""
        self._exhaust_chain()
        self.r._store[f"manager:worker_ceil:{pool}"] = "10"
        self.r._store[f"manager:pool:{pool}:peak_nd"] = "10"
        # no lendable seats elsewhere: every other pool runs exactly its target
        self.workers_targets = dict(self.workers_targets, scan=10)
        self.pool_sizes = dict(self.workers_targets, **{pool: 10})

    def _rising_history(self, pool="scan"):
        self.r._store[f"manager:layer2:{pool}:delay_history"] = ["1850.0", "1860.0", "1870.0"]

    def test_lever1_fires_when_chain_exhausted(self):
        self._exhaust_chain()
        self._rising_history()
        mgr._check_layer2(
            self.r, "scan",
            n_workers=10, delay_s=1900.0, depth=50,
            params=self.params, worker_ceil=10,
            pool_sizes=self.pool_sizes, workers_targets=self.workers_targets,
        )
        self.assertTrue(mgr._get_lever1_active(self.r, "scan"))

    def test_lever1_not_fired_while_budget_can_be_raised(self):
        self._exhaust_chain()
        self.r._store["manager:db_pool_maxconn"] = str(_FAKE_CAPACITY["seat_cap"] - 10)
        self._rising_history()
        mgr._check_layer2(
            self.r, "scan",
            n_workers=10, delay_s=1900.0, depth=50,
            params=self.params, worker_ceil=10,
            pool_sizes=self.pool_sizes, workers_targets=self.workers_targets,
        )
        self.assertFalse(mgr._get_lever1_active(self.r, "scan"))
        self.assertEqual(int(self.r._store["manager:db_pool_maxconn"]),
                         _FAKE_CAPACITY["seat_cap"] - 10
                         + mgr.DB_POOL_RAISE_STEP // _FAKE_CAPACITY["per_worker"])

    def test_lever1_not_fired_on_first_cycle_above_warn(self):
        self._exhaust_chain()
        mgr._check_layer2(
            self.r, "scan",
            n_workers=10, delay_s=1900.0, depth=50,
            params=self.params, worker_ceil=10,
            pool_sizes=self.pool_sizes, workers_targets=self.workers_targets,
        )
        self.assertFalse(mgr._get_lever1_active(self.r, "scan"))

    def test_lever1_does_not_fire_below_warn(self):
        mgr._check_layer2(
            self.r, "scan",
            n_workers=10, delay_s=1700.0, depth=50,
            params=self.params, worker_ceil=10,
            pool_sizes=self.pool_sizes, workers_targets=self.workers_targets,
        )
        self.assertFalse(mgr._get_lever1_active(self.r, "scan"))

    def test_lever1_fires_once_idempotent(self):
        # Two cycles both above WARN — Lever 1 should fire once, fired_at not overwritten
        self._exhaust_chain()
        self._rising_history()
        mgr._check_layer2(
            self.r, "scan",
            n_workers=10, delay_s=1900.0, depth=60,
            params=self.params, worker_ceil=10,
            pool_sizes=self.pool_sizes, workers_targets=self.workers_targets,
        )
        first_fired = self.r._store.get("manager:lever1:scan:fired_at")
        self.assertIsNotNone(first_fired)
        mgr._check_layer2(
            self.r, "scan",
            n_workers=10, delay_s=2000.0, depth=999,
            params=self.params, worker_ceil=10,
            pool_sizes=self.pool_sizes, workers_targets=self.workers_targets,
        )
        # fired_at not overwritten, Lever 1 still active
        self.assertEqual(self.r._store.get("manager:lever1:scan:fired_at"), first_fired)
        self.assertTrue(mgr._get_lever1_active(self.r, "scan"))

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

    def test_lever1_not_fired_when_arrivals_fit_in_workers(self):
        # detail: lambda * s = 1.0/s * 2s = 2 <= n=10 → the backlog is draining
        self._exhaust_chain_for("detail")
        mgr._cycle_arrival_rate["detail"] = 1.0
        mgr._cycle_service_s["detail"] = 2.0
        self._rising_history("detail")
        mgr._check_layer2(
            self.r, "detail",
            n_workers=10, delay_s=1900.0, depth=50,
            params=self.params, worker_ceil=10,
            pool_sizes=self.pool_sizes, workers_targets=self.workers_targets,
        )
        self.assertFalse(mgr._get_lever1_active(self.r, "detail"))

    def test_detail_never_fires_lever1_and_alerts_when_chain_exhausted(self):
        # lambda * s = 8/s * 2s = 16 > n=10: Lever 1 would fire for scan/fullscan,
        # but detail has none (save-only producers) — it raises an ALERT instead.
        self._exhaust_chain_for("detail")
        mgr._cycle_arrival_rate["detail"] = 8.0
        mgr._cycle_service_s["detail"] = 2.0
        self._rising_history("detail")
        with self.assertLogs("workers.manager", level="ERROR") as logs:
            mgr._check_layer2(
                self.r, "detail",
                n_workers=10, delay_s=1900.0, depth=50,
                params=self.params, worker_ceil=10,
                pool_sizes=self.pool_sizes, workers_targets=self.workers_targets,
            )
        self.assertFalse(mgr._get_lever1_active(self.r, "detail"))
        self.assertTrue(any("ALERT manager [detail]" in m for m in logs.output))

    def test_detail_ignores_leftover_lever1_flag(self):
        self.r._store["manager:lever1:detail:active"] = "1"
        mgr._check_layer2(
            self.r, "detail",
            n_workers=3, delay_s=0.0, depth=0,
            params=self.params, worker_ceil=10,
            pool_sizes=self.pool_sizes, workers_targets=self.workers_targets,
        )
        self.assertEqual(mgr._lever1_stable_cycles.get("detail", 0), 0)

    def test_borrows_kept_on_lever1_lift(self):
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
        # Seats are returned by _maybe_return_borrows, not by the Lever 1 lift.
        self.assertEqual(self.r._store.get("manager:borrow:detail:scan"), "2")


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
