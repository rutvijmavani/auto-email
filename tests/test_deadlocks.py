"""
tests/test_deadlocks.py — End-to-end deadlock detection and resolution scenarios.

Covers scenarios not already exercised at unit level in test_layer2.py Section 8.
All tests drive _check_layer2() (or its component functions) through mocked Redis.

Scenario coverage:
  1.  Detail pool deadlock — lever1 fires, borrow from scan resolves it
  2.  Scan pool deadlock — borrow from fullscan resolves it
  3.  Fullscan deadlock, detail protected — scan lends but detail (no lever1) does NOT
  4.  Fullscan deadlock, detail lever1 active — both scan and detail can lend
  5.  Borrow priority — scan preferred over fullscan as source (bigger lendable pool)
  6.  No borrow when NOT at ceiling (n_workers < worker_ceil, even with rising delay)
  7.  No borrow when delay flat/falling (3-of-4 check fails)
  8.  Phase-2 borrow (1 unused-capacity worker) when Phase-1 lendable already taken
  9.  Borrow accumulates correctly via _record_borrow across successive cycles
  10. Recovery integration — lever1 lift clears borrows and starts re-intro
  11. _attempt_borrow returns False when ALL pools are at target (no capacity anywhere)
"""

import sys
import os
import unittest
from unittest.mock import MagicMock

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import workers.manager as mgr
from workers.manager import (
    LEVER1_STABLE_REQUIRED,
    RECOVERY_STABILITY_RATIO,
    _attempt_borrow,
    _check_layer2,
    _get_lever1_active,
    _is_reintro_active,
    _record_borrow,
)
from config import WORKER_FLOOR


# ─────────────────────────────────────────────────────────────────────────────
# Shared Redis helper
# ─────────────────────────────────────────────────────────────────────────────

def _make_redis(data=None):
    store = dict(data or {})
    r = MagicMock()

    def _key(k):
        return k.decode() if isinstance(k, bytes) else k

    def _get(k):
        return store.get(_key(k))

    def _set(k, v, *a, **kw):
        store[_key(k)] = str(v) if not isinstance(v, (bytes, str)) else v

    def _exists(*keys):
        return sum(1 for k in keys if _key(k) in store)

    def _delete(*keys):
        for k in keys:
            store.pop(_key(k), None)

    def _scan(cursor, match="*", count=20):
        import fnmatch
        return 0, [k for k in store if fnmatch.fnmatch(_key(k), _key(match))]

    def _lrange(k, start, end):
        val = store.get(_key(k))
        if val is None:
            return []
        items = val if isinstance(val, list) else [val]
        return items[start:] if end == -1 else items[start: end + 1]

    def _rpush(k, *values):
        key = _key(k)
        store.setdefault(key, [])
        if not isinstance(store[key], list):
            store[key] = [store[key]]
        for v in values:
            store[key].append(str(v) if not isinstance(v, (bytes, str)) else v)
        return len(store[key])

    def _ltrim(k, start, end):
        key = _key(k)
        if key in store and isinstance(store[key], list):
            lst = store[key]
            store[key] = lst[start:] if end == -1 else lst[start: end + 1]

    def _pipeline():
        pp = MagicMock()
        rpush_calls, ltrim_calls, lrange_calls = [], [], []
        pp.rpush  = lambda k, v:    rpush_calls.append((k, v)) or pp
        pp.ltrim  = lambda k, s, e: ltrim_calls.append((k, s, e)) or pp
        pp.lrange = lambda k, s, e: lrange_calls.append((k, s, e)) or pp
        pp.set    = lambda k, v, **kw: _set(k, v) or pp

        def _exec():
            for k, v in rpush_calls:
                _rpush(k, v)
            for k, s, e in ltrim_calls:
                _ltrim(k, s, e)
            return [None, None] + [_lrange(k, s, e) for k, s, e in lrange_calls]

        pp.execute = _exec
        return pp

    r.get.side_effect      = _get
    r.set.side_effect      = _set
    r.exists.side_effect   = _exists
    r.delete.side_effect   = _delete
    r.scan.side_effect     = _scan
    r.lrange.side_effect   = _lrange
    r.rpush.side_effect    = _rpush
    r.ltrim.side_effect    = _ltrim
    r.pipeline.side_effect = _pipeline
    r._store = store
    return r


def _reset_state(*pools):
    for p in pools:
        mgr._lever1_stable_cycles[p]  = 0
        mgr._reintro_stable_cycles[p] = 0
        mgr._prev_depth[p]            = 0


def _prefill_delay_history(r, pool, values):
    """Pre-load delay history so _is_deadlock_rising sees a full 4-cycle window."""
    r._store[f"manager:layer2:{pool}:delay_history"] = [str(v) for v in values]


_PARAMS = {"fetch_p75": 40.0, "delay_warn_s": 1800.0}
_CEIL   = 10


def _run(r, pool, delay_s, n_workers=_CEIL,
         pool_sizes=None, workers_targets=None, depth=50):
    ps  = pool_sizes      or {"scan": _CEIL, "detail": _CEIL, "fullscan": _CEIL}
    wts = workers_targets or {"scan": _CEIL, "detail": _CEIL, "fullscan": _CEIL}
    _check_layer2(r, pool, n_workers, delay_s, depth, _PARAMS, _CEIL, ps, wts)


# ─────────────────────────────────────────────────────────────────────────────
# Scenario 1 — Detail pool deadlock: borrow from scan
# ─────────────────────────────────────────────────────────────────────────────

class TestDeadlock1_DetailBorrowsFromScan(unittest.TestCase):
    """detail at ceiling, delay rising 3-of-4 → borrow lendable workers from scan."""

    def setUp(self):
        _reset_state("detail", "scan", "fullscan")
        self.r = _make_redis({
            "manager:lever1:detail:active": "1",
            "manager:snapshot:detail:D":    "100",
            "manager:snapshot:detail:R":    "0",
        })

    def test_borrow_from_scan_when_lendable(self):
        warn = _PARAMS["delay_warn_s"]
        _prefill_delay_history(self.r, "detail", [warn+10, warn+50, warn+100, warn+200])
        pool_sizes      = {"scan": 8, "detail": _CEIL, "fullscan": 5}
        workers_targets = {"scan": 5, "detail": _CEIL, "fullscan": 5}
        _run(self.r, "detail", delay_s=warn + 300, n_workers=_CEIL,
             pool_sizes=pool_sizes, workers_targets=workers_targets)
        borrow = self.r._store.get("manager:borrow:scan:detail")
        self.assertIsNotNone(borrow, "scan must lend to detail")
        self.assertGreater(int(borrow), 0)

    def test_fullscan_not_touched_while_scan_lends(self):
        warn = _PARAMS["delay_warn_s"]
        _prefill_delay_history(self.r, "detail", [warn+10, warn+50, warn+100, warn+200])
        pool_sizes      = {"scan": 8, "detail": _CEIL, "fullscan": 5}
        workers_targets = {"scan": 5, "detail": _CEIL, "fullscan": 5}
        _run(self.r, "detail", delay_s=warn + 300, n_workers=_CEIL,
             pool_sizes=pool_sizes, workers_targets=workers_targets)
        self.assertIsNone(self.r._store.get("manager:borrow:fullscan:detail"),
                          "fullscan has no lendable capacity (pool_size equals target)")


# ─────────────────────────────────────────────────────────────────────────────
# Scenario 2 — Scan pool deadlock: borrow from fullscan
# ─────────────────────────────────────────────────────────────────────────────

class TestDeadlock2_ScanBorrowsFromFullscan(unittest.TestCase):
    """scan at ceiling; scan needs help; fullscan has lendable workers."""

    def setUp(self):
        _reset_state("scan", "detail", "fullscan")
        self.r = _make_redis({
            "manager:lever1:scan:active": "1",
            "manager:snapshot:scan:D":    "80",
            "manager:snapshot:scan:R":    "0",
        })

    def test_borrow_from_fullscan(self):
        warn = _PARAMS["delay_warn_s"]
        _prefill_delay_history(self.r, "scan", [warn+20, warn+60, warn+100, warn+180])
        pool_sizes      = {"scan": _CEIL, "detail": 5, "fullscan": 8}
        workers_targets = {"scan": _CEIL, "detail": 5, "fullscan": 5}
        _run(self.r, "scan", delay_s=warn + 200, n_workers=_CEIL,
             pool_sizes=pool_sizes, workers_targets=workers_targets)
        borrow = self.r._store.get("manager:borrow:fullscan:scan")
        self.assertIsNotNone(borrow, "fullscan must lend to scan")
        self.assertGreater(int(borrow), 0)


# ─────────────────────────────────────────────────────────────────────────────
# Scenario 3 — Fullscan deadlock, detail protected
# ─────────────────────────────────────────────────────────────────────────────

class TestDeadlock3_FullscanDetailProtected(unittest.TestCase):
    """
    fullscan deadlocked; scan has lendable.
    detail has lendable BUT its Lever 1 is NOT active → detail must NOT lend.
    """

    def setUp(self):
        _reset_state("fullscan", "scan", "detail")
        self.r = _make_redis({
            "manager:lever1:fullscan:active": "1",
            "manager:snapshot:fullscan:D":    "50",
            "manager:snapshot:fullscan:R":    "0",
        })

    def test_scan_lends_detail_does_not(self):
        warn = _PARAMS["delay_warn_s"]
        _prefill_delay_history(self.r, "fullscan", [warn+10, warn+30, warn+60, warn+90])
        pool_sizes      = {"scan": 8, "detail": 8, "fullscan": _CEIL}
        workers_targets = {"scan": 5, "detail": 5, "fullscan": _CEIL}
        _run(self.r, "fullscan", delay_s=warn + 100, n_workers=_CEIL,
             pool_sizes=pool_sizes, workers_targets=workers_targets)
        self.assertIsNotNone(self.r._store.get("manager:borrow:scan:fullscan"),
                             "scan must lend to fullscan")
        self.assertIsNone(self.r._store.get("manager:borrow:detail:fullscan"),
                          "detail must NOT lend without its own Lever 1")


# ─────────────────────────────────────────────────────────────────────────────
# Scenario 4 — Fullscan deadlock, detail lever1 active
# ─────────────────────────────────────────────────────────────────────────────

class TestDeadlock4_FullscanDetailAllowed(unittest.TestCase):
    """detail Lever 1 IS active → detail is allowed to lend to fullscan."""

    def setUp(self):
        _reset_state("fullscan", "scan", "detail")
        self.r = _make_redis({
            "manager:lever1:fullscan:active": "1",
            "manager:lever1:detail:active":   "1",
            "manager:snapshot:fullscan:D":    "50",
            "manager:snapshot:fullscan:R":    "0",
        })

    def test_detail_lends_when_lever1_active(self):
        warn = _PARAMS["delay_warn_s"]
        _prefill_delay_history(self.r, "fullscan", [warn+10, warn+30, warn+60, warn+90])
        # scan at target (no lendable), detail has 3 lendable
        pool_sizes      = {"scan": 5, "detail": 8, "fullscan": _CEIL}
        workers_targets = {"scan": 5, "detail": 5, "fullscan": _CEIL}
        _run(self.r, "fullscan", delay_s=warn + 100, n_workers=_CEIL,
             pool_sizes=pool_sizes, workers_targets=workers_targets)
        detail_borrow = self.r._store.get("manager:borrow:detail:fullscan")
        self.assertIsNotNone(detail_borrow, "detail must lend when its Lever 1 active")
        self.assertGreater(int(detail_borrow), 0)


# ─────────────────────────────────────────────────────────────────────────────
# Scenario 5 — Borrow source priority
# ─────────────────────────────────────────────────────────────────────────────

class TestDeadlock5_BorrowPriority(unittest.TestCase):
    """Both scan and fullscan have lendable workers; Phase 1 takes from both."""

    def setUp(self):
        _reset_state("detail", "scan", "fullscan")

    def test_scan_taken_first_when_both_lendable(self):
        r = _make_redis({"manager:lever1:detail:active": "1"})
        pool_sizes      = {"scan": 8, "detail": _CEIL, "fullscan": 7}
        workers_targets = {"scan": 5, "detail": _CEIL, "fullscan": 5}
        result = _attempt_borrow(r, "detail", pool_sizes, workers_targets)
        self.assertTrue(result)
        scan_borrow = r._store.get("manager:borrow:scan:detail")
        self.assertIsNotNone(scan_borrow)
        self.assertEqual(int(scan_borrow), 3)   # all 3 lendable from scan taken
        # Phase 1 continues to fullscan (2 lendable = 7 - max(5, WORKER_FLOOR))
        fullscan_borrow = r._store.get("manager:borrow:fullscan:detail")
        self.assertIsNotNone(fullscan_borrow)
        self.assertEqual(int(fullscan_borrow), 2)


# ─────────────────────────────────────────────────────────────────────────────
# Scenario 6 — No borrow when NOT at ceiling
# ─────────────────────────────────────────────────────────────────────────────

class TestDeadlock6_NoBorrowBelowCeiling(unittest.TestCase):
    """n_workers < worker_ceil → deadlock gate fails → no borrow regardless of delay."""

    def setUp(self):
        _reset_state("detail", "scan", "fullscan")
        self.r = _make_redis({"manager:lever1:detail:active": "1"})

    def test_no_borrow_when_4_workers_below_ceil_10(self):
        warn = _PARAMS["delay_warn_s"]
        _prefill_delay_history(self.r, "detail", [warn+10, warn+30, warn+60, warn+90])
        pool_sizes      = {"scan": 8, "detail": 4, "fullscan": 5}
        workers_targets = {"scan": 5, "detail": 4, "fullscan": 5}
        _run(self.r, "detail", delay_s=warn + 100, n_workers=4,
             pool_sizes=pool_sizes, workers_targets=workers_targets)
        self.assertIsNone(self.r._store.get("manager:borrow:scan:detail"))


# ─────────────────────────────────────────────────────────────────────────────
# Scenario 7 — No borrow when delay not rising
# ─────────────────────────────────────────────────────────────────────────────

class TestDeadlock7_NoBorrowNotRising(unittest.TestCase):
    """At ceiling but 3-of-4 rising check fails → no borrow."""

    def setUp(self):
        _reset_state("detail", "scan", "fullscan")
        self.r = _make_redis({"manager:lever1:detail:active": "1"})

    def test_no_borrow_when_only_one_cycle_above_warn(self):
        warn = _PARAMS["delay_warn_s"]
        _prefill_delay_history(self.r, "detail",
                               [warn - 500, warn - 300, warn - 100, warn + 50])
        pool_sizes      = {"scan": 8, "detail": _CEIL, "fullscan": 5}
        workers_targets = {"scan": 5, "detail": _CEIL, "fullscan": 5}
        _run(self.r, "detail", delay_s=warn + 50, n_workers=_CEIL,
             pool_sizes=pool_sizes, workers_targets=workers_targets)
        self.assertIsNone(self.r._store.get("manager:borrow:scan:detail"))

    def test_no_borrow_when_not_directional(self):
        warn = _PARAMS["delay_warn_s"]
        # All 4 above warn but current < oldest → falling
        _prefill_delay_history(self.r, "detail",
                               [warn + 500, warn + 400, warn + 300, warn + 200])
        pool_sizes      = {"scan": 8, "detail": _CEIL, "fullscan": 5}
        workers_targets = {"scan": 5, "detail": _CEIL, "fullscan": 5}
        _run(self.r, "detail", delay_s=warn + 100, n_workers=_CEIL,
             pool_sizes=pool_sizes, workers_targets=workers_targets)
        self.assertIsNone(self.r._store.get("manager:borrow:scan:detail"))


# ─────────────────────────────────────────────────────────────────────────────
# Scenario 8 — Phase-2 borrow (1 unused-capacity worker)
# ─────────────────────────────────────────────────────────────────────────────

class TestDeadlock8_Phase2Borrow(unittest.TestCase):
    """
    scan already lent all its lendable to detail.
    Phase-1 lendable for fullscan = 0 (total already borrowed = lendable window).
    Phase-2 takes exactly 1 from unused capacity.
    """

    def setUp(self):
        _reset_state("fullscan", "scan", "detail")

    def test_phase2_takes_one_from_unused_capacity(self):
        # scan lent 2 to detail; n=7, target=5 → Phase-1 lendable to fullscan = 7-5-2 = 0
        # Phase-2 available = 7 - 0(borrow:scan:fullscan) - 5 = 2 → take 1
        r = _make_redis({"manager:borrow:scan:detail": "2"})
        pool_sizes      = {"scan": 7, "detail": 5, "fullscan": 5}
        workers_targets = {"scan": 5, "detail": 5, "fullscan": 5}
        result = _attempt_borrow(r, "fullscan", pool_sizes, workers_targets)
        self.assertTrue(result)
        self.assertEqual(r._store.get("manager:borrow:scan:fullscan"), "1")


# ─────────────────────────────────────────────────────────────────────────────
# Scenario 9 — Borrow accumulation across calls
# ─────────────────────────────────────────────────────────────────────────────

class TestDeadlock9_BorrowAccumulation(unittest.TestCase):
    """_record_borrow adds to existing count; independent pairs do not interfere."""

    def test_accumulates_same_pair(self):
        r = _make_redis()
        _record_borrow(r, "scan", "detail", 2)
        _record_borrow(r, "scan", "detail", 3)
        self.assertEqual(int(r._store["manager:borrow:scan:detail"]), 5)

    def test_independent_pairs_independent(self):
        r = _make_redis()
        _record_borrow(r, "scan",     "detail",   2)
        _record_borrow(r, "fullscan", "detail",   1)
        self.assertEqual(int(r._store["manager:borrow:scan:detail"]),     2)
        self.assertEqual(int(r._store["manager:borrow:fullscan:detail"]), 1)


# ─────────────────────────────────────────────────────────────────────────────
# Scenario 10 — Recovery integration
# ─────────────────────────────────────────────────────────────────────────────

class TestDeadlock10_RecoveryIntegration(unittest.TestCase):
    """
    lever1 was active with an active borrow.
    After LEVER1_STABLE_REQUIRED stable cycles: lever1 cleared, borrows deleted, re-intro started.
    """

    def setUp(self):
        _reset_state("detail", "scan", "fullscan")

    def test_full_recovery_path(self):
        r = _make_redis({
            "manager:lever1:detail:active": "1",
            "manager:borrow:scan:detail":   "2",
            "manager:snapshot:detail:D":    "60",
            "manager:snapshot:detail:R":    "0",
        })
        stable_delay = _PARAMS["delay_warn_s"] * RECOVERY_STABILITY_RATIO * 0.5

        for _ in range(LEVER1_STABLE_REQUIRED):
            _run(r, "detail", delay_s=stable_delay, n_workers=6)

        self.assertFalse(_get_lever1_active(r, "detail"), "lever1 must be lifted")
        self.assertIsNone(r._store.get("manager:borrow:scan:detail"),  "borrow must be cleared")
        self.assertTrue(_is_reintro_active(r, "detail"),               "re-intro must be active")


# ─────────────────────────────────────────────────────────────────────────────
# Scenario 11 — No capacity anywhere
# ─────────────────────────────────────────────────────────────────────────────

class TestDeadlock11_NoCapacityAnywhere(unittest.TestCase):
    """All pools at exact target with nothing to lend → returns False, no keys written."""

    def test_returns_false(self):
        r = _make_redis()
        pool_sizes      = {"scan": 5, "detail": 5, "fullscan": 5}
        workers_targets = {"scan": 5, "detail": 5, "fullscan": 5}
        self.assertFalse(_attempt_borrow(r, "detail", pool_sizes, workers_targets))

    def test_no_borrow_keys_written(self):
        r = _make_redis()
        pool_sizes      = {"scan": 2, "detail": 2, "fullscan": 2}
        workers_targets = {"scan": 2, "detail": 2, "fullscan": 2}
        _attempt_borrow(r, "detail", pool_sizes, workers_targets)
        borrow_keys = [k for k in r._store if k.startswith("manager:borrow:")]
        self.assertEqual(borrow_keys, [])


if __name__ == "__main__":
    unittest.main()
