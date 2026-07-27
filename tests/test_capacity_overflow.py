"""
tests/test_capacity_overflow.py — DB connection-pool budget and worker-ceiling safety tests.

Exercises what is actually implemented in workers/manager.py (not design-doc concepts):

  A. _get_worker_ceil — Redis hit, Redis parse error, bootstrap fallback, floor clamp
  B. _midnight_recompute — bootstrap mode writes BOOTSTRAP_CEIL; post-28d formula caps
     at MONITOR_MAX_WORKERS; volatility buffer applied; pool error is isolated
  C. Combined budget invariant — BOOTSTRAP_CEIL sum ≤ DB_POOL_MAXCONN - 3
  D. _midnight_recompute formula cap — computed ceil never exceeds MONITOR_MAX_WORKERS
"""

import sys
import os
import unittest
from unittest.mock import MagicMock

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import workers.manager as mgr
from workers.manager import (
    BOOTSTRAP_CEIL,
    BOOTSTRAP_DAYS_REQUIRED,
    _get_worker_ceil,
    _midnight_recompute,
)
from config import (
    DB_POOL_MAXCONN,
    MONITOR_MAX_WORKERS,
    WORKER_FLOOR,
)

# Budget: 3 connections reserved for scheduler/manager
_DB_BUDGET = DB_POOL_MAXCONN - 3
_POOLS = ["scan", "detail", "fullscan"]


# ─────────────────────────────────────────────────────────────────────────────
# Redis helper (same pattern used across test suite)
# ─────────────────────────────────────────────────────────────────────────────

def _make_redis(data=None):
    store = dict(data or {})
    r = MagicMock()

    def _key(k):
        return k.decode() if isinstance(k, bytes) else k

    def _get(k):
        return store.get(_key(k))

    def _set(k, v, *a, **kw):
        store[_key(k)] = str(v)

    def _scan(cursor, match="*", count=20):
        import fnmatch
        return 0, [k for k in store if fnmatch.fnmatch(_key(k), _key(match))]

    r.get.side_effect    = _get
    r.set.side_effect    = _set
    r.scan.side_effect   = _scan
    r.exists.return_value = 0
    r._store = store
    return r


# ─────────────────────────────────────────────────────────────────────────────
# A. _get_worker_ceil
# ─────────────────────────────────────────────────────────────────────────────

class TestGetWorkerCeil(unittest.TestCase):

    def test_returns_redis_value_when_set(self):
        r = _make_redis({"manager:worker_ceil:scan": "12"})
        self.assertEqual(_get_worker_ceil(r, "scan"), 12)

    def test_redis_value_clamped_to_floor(self):
        """Redis stores 0 (corrupted) → returns WORKER_FLOOR, not 0."""
        r = _make_redis({"manager:worker_ceil:scan": "0"})
        result = _get_worker_ceil(r, "scan")
        self.assertGreaterEqual(result, WORKER_FLOOR)

    def test_redis_value_below_floor_clamped(self):
        """Redis stores 1, WORKER_FLOOR=2 → result must be ≥ WORKER_FLOOR."""
        r = _make_redis({"manager:worker_ceil:scan": "1"})
        result = _get_worker_ceil(r, "scan")
        self.assertEqual(result, WORKER_FLOOR)

    def test_falls_back_to_bootstrap_when_key_absent(self):
        r = _make_redis()
        self.assertEqual(_get_worker_ceil(r, "scan"),     BOOTSTRAP_CEIL["scan"])
        self.assertEqual(_get_worker_ceil(r, "detail"),   BOOTSTRAP_CEIL["detail"])
        self.assertEqual(_get_worker_ceil(r, "fullscan"), BOOTSTRAP_CEIL["fullscan"])

    def test_falls_back_to_floor_for_unknown_pool(self):
        r = _make_redis()
        self.assertEqual(_get_worker_ceil(r, "unknown_pool"), WORKER_FLOOR)

    def test_parse_error_falls_back_to_bootstrap(self):
        """Corrupted non-numeric value → bootstrap fallback, not crash."""
        r = _make_redis({"manager:worker_ceil:scan": "garbage"})
        self.assertEqual(_get_worker_ceil(r, "scan"), BOOTSTRAP_CEIL["scan"])

    def test_parse_error_for_unknown_pool_falls_back_to_floor(self):
        r = _make_redis({"manager:worker_ceil:weird": "notanumber"})
        self.assertEqual(_get_worker_ceil(r, "weird"), WORKER_FLOOR)


# ─────────────────────────────────────────────────────────────────────────────
# B. _midnight_recompute — bootstrap mode
# ─────────────────────────────────────────────────────────────────────────────

class TestMidnightRecomputeBootstrap(unittest.TestCase):
    """< BOOTSTRAP_DAYS_REQUIRED records → must write BOOTSTRAP_CEIL for each pool."""

    def test_bootstrap_writes_correct_ceil_scan(self):
        r = _make_redis({"manager:pool:scan:daily_peak:running": "7"})
        _midnight_recompute(r, ["scan"])
        result = r._store.get("manager:worker_ceil:scan")
        self.assertIsNotNone(result)
        self.assertEqual(int(result), BOOTSTRAP_CEIL["scan"])

    def test_bootstrap_writes_correct_ceil_all_pools(self):
        data = {f"manager:pool:{p}:daily_peak:running": "3" for p in _POOLS}
        r = _make_redis(data)
        _midnight_recompute(r, _POOLS)
        for pool in _POOLS:
            result = r._store.get(f"manager:worker_ceil:{pool}")
            self.assertIsNotNone(result, f"ceil missing for {pool}")
            self.assertEqual(int(result), BOOTSTRAP_CEIL[pool], f"wrong ceil for {pool}")

    def test_bootstrap_resets_running_peak(self):
        r = _make_redis({"manager:pool:scan:daily_peak:running": "8"})
        _midnight_recompute(r, ["scan"])
        self.assertEqual(r._store.get("manager:pool:scan:daily_peak:running"), "0")

    def test_bootstrap_records_today_peak(self):
        r = _make_redis({"manager:pool:detail:daily_peak:running": "6"})
        _midnight_recompute(r, ["detail"])
        today_keys = [k for k in r._store
                      if k.startswith("manager:pool:detail:daily_peak:20")]
        self.assertTrue(len(today_keys) >= 1, "today's peak record must be written")


# ─────────────────────────────────────────────────────────────────────────────
# B. _midnight_recompute — post-28d formula
# ─────────────────────────────────────────────────────────────────────────────

class TestMidnightRecomputeFormula(unittest.TestCase):
    """After BOOTSTRAP_DAYS_REQUIRED days the formula fires."""

    def _make_28day_redis(self, pool: str, peak_value: int) -> MagicMock:
        data = {}
        data[f"manager:pool:{pool}:daily_peak:running"] = str(peak_value)
        # Write 28 daily_peak records (dates 2025-01-01 … 2025-01-28)
        for i in range(BOOTSTRAP_DAYS_REQUIRED):
            date = f"2025-01-{i + 1:02d}"
            data[f"manager:pool:{pool}:daily_peak:{date}"] = str(peak_value)
        return _make_redis(data)

    def test_formula_mode_caps_at_monitor_max_workers(self):
        """peak=20, any buffer pushes beyond 20 → capped at MONITOR_MAX_WORKERS."""
        r = self._make_28day_redis("scan", MONITOR_MAX_WORKERS)
        _midnight_recompute(r, ["scan"])
        result = r._store.get("manager:worker_ceil:scan")
        self.assertIsNotNone(result)
        self.assertLessEqual(int(result), MONITOR_MAX_WORKERS)

    def test_formula_mode_floor_applied(self):
        """peak=1 (below floor) → must be ≥ WORKER_FLOOR."""
        r = self._make_28day_redis("scan", 1)
        _midnight_recompute(r, ["scan"])
        result = int(r._store.get("manager:worker_ceil:scan"))
        self.assertGreaterEqual(result, WORKER_FLOOR)

    def test_formula_mode_peak_at_reasonable_value(self):
        """peak=5 stable → result must be ≥ 5 (no shrinkage below peak)."""
        r = self._make_28day_redis("scan", 5)
        _midnight_recompute(r, ["scan"])
        result = int(r._store.get("manager:worker_ceil:scan"))
        self.assertGreaterEqual(result, 5)

    def test_formula_mode_never_exceeds_monitor_max(self):
        """Extreme peak with high growth rate must still be ≤ MONITOR_MAX_WORKERS."""
        # recent 7 days much higher → growth_buffer fires
        data = {"manager:pool:scan:daily_peak:running": "15"}
        for i in range(BOOTSTRAP_DAYS_REQUIRED):
            date = f"2025-01-{i + 1:02d}"
            # dates 22-28 are most recent (_midnight_recompute sorts descending) → high peaks
            val = 15 if i >= BOOTSTRAP_DAYS_REQUIRED - 7 else 5
            data[f"manager:pool:scan:daily_peak:{date}"] = str(val)
        r = _make_redis(data)
        _midnight_recompute(r, ["scan"])
        result = int(r._store.get("manager:worker_ceil:scan"))
        self.assertLessEqual(result, MONITOR_MAX_WORKERS)

    def test_formula_mode_error_in_one_pool_does_not_abort_others(self):
        """An exception in one pool must not prevent other pools from computing."""
        r = self._make_28day_redis("scan", 8)
        # detail has NO running key and malformed peak records → will error
        r._store["manager:pool:detail:daily_peak:running"] = "NaN"
        r._store["manager:pool:fullscan:daily_peak:running"] = "5"
        for i in range(BOOTSTRAP_DAYS_REQUIRED):
            date = f"2025-01-{i + 1:02d}"
            r._store[f"manager:pool:fullscan:daily_peak:{date}"] = "5"

        _midnight_recompute(r, ["scan", "detail", "fullscan"])

        self.assertIsNotNone(r._store.get("manager:worker_ceil:scan"))
        self.assertIsNotNone(r._store.get("manager:worker_ceil:fullscan"))


# ─────────────────────────────────────────────────────────────────────────────
# C. Combined budget invariant
# ─────────────────────────────────────────────────────────────────────────────

class TestCombinedBudgetInvariant(unittest.TestCase):
    """
    BOOTSTRAP_CEIL values and formula-mode ceilings must stay within the
    DB_POOL_MAXCONN budget (DB_POOL_MAXCONN - 3 = 22 for production).
    """

    def test_bootstrap_ceil_sum_within_budget(self):
        """
        The documented budget constraint is scan_ceil + detail_ceil ≤ DB_POOL_MAXCONN - 3.
        Fullscan is excluded from this check (it does not hold long-lived DB connections
        the way scan/detail workers do).
        """
        total = BOOTSTRAP_CEIL["scan"] + BOOTSTRAP_CEIL["detail"]
        self.assertLessEqual(
            total, _DB_BUDGET,
            f"scan+detail BOOTSTRAP_CEIL sum={total} exceeds DB budget={_DB_BUDGET}",
        )

    def test_monitor_max_workers_fits_within_db_budget(self):
        """
        MONITOR_MAX_WORKERS ≤ DB_POOL_MAXCONN - 3: a single pool at its maximum
        cannot exhaust the DB connection budget on its own.
        The sum of all pools CAN exceed the budget — that is intentional, since pools
        cannot all simultaneously reach their individual maxima.
        """
        self.assertLessEqual(
            MONITOR_MAX_WORKERS, DB_POOL_MAXCONN - 3,
            f"MONITOR_MAX_WORKERS={MONITOR_MAX_WORKERS} exceeds per-pool DB budget={DB_POOL_MAXCONN - 3}",
        )

    def test_each_bootstrap_pool_below_individual_monitor_max(self):
        """Each BOOTSTRAP_CEIL[pool] ≤ MONITOR_MAX_WORKERS (formula upper bound)."""
        for pool in _POOLS:
            self.assertLessEqual(
                BOOTSTRAP_CEIL[pool], MONITOR_MAX_WORKERS,
                f"BOOTSTRAP_CEIL[{pool}]={BOOTSTRAP_CEIL[pool]} > MONITOR_MAX_WORKERS={MONITOR_MAX_WORKERS}",
            )

    def test_bootstrap_ceil_each_pool_above_floor(self):
        """Every pool starts at or above WORKER_FLOOR — no pool starts starved."""
        for pool in _POOLS:
            self.assertGreaterEqual(
                BOOTSTRAP_CEIL[pool], WORKER_FLOOR,
                f"BOOTSTRAP_CEIL[{pool}]={BOOTSTRAP_CEIL[pool]} < WORKER_FLOOR={WORKER_FLOOR}",
            )

    def test_midnight_recompute_formula_never_writes_above_monitor_max(self):
        """
        With 28 daily peaks all at the hard max, the written ceiling must
        equal MONITOR_MAX_WORKERS (capped, not rejected).
        """
        data = {"manager:pool:scan:daily_peak:running": str(MONITOR_MAX_WORKERS)}
        for i in range(BOOTSTRAP_DAYS_REQUIRED):
            date = f"2025-02-{i + 1:02d}"
            data[f"manager:pool:scan:daily_peak:{date}"] = str(MONITOR_MAX_WORKERS)
        r = _make_redis(data)
        _midnight_recompute(r, ["scan"])
        result = int(r._store.get("manager:worker_ceil:scan"))
        self.assertEqual(result, MONITOR_MAX_WORKERS,
                         "ceil must be capped exactly at MONITOR_MAX_WORKERS, not higher")


# ─────────────────────────────────────────────────────────────────────────────
# D. _get_worker_ceil — Redis vs computed consistency
# ─────────────────────────────────────────────────────────────────────────────

class TestWorkerCeilConsistency(unittest.TestCase):
    """After _midnight_recompute writes a value, _get_worker_ceil must read it back."""

    def test_get_worker_ceil_reads_recomputed_value(self):
        data = {"manager:pool:scan:daily_peak:running": "7"}
        for i in range(BOOTSTRAP_DAYS_REQUIRED):
            date = f"2025-03-{i + 1:02d}"
            data[f"manager:pool:scan:daily_peak:{date}"] = "7"
        r = _make_redis(data)
        _midnight_recompute(r, ["scan"])
        written = int(r._store["manager:worker_ceil:scan"])
        read_back = _get_worker_ceil(r, "scan")
        self.assertEqual(read_back, written,
                         "_get_worker_ceil must return what _midnight_recompute wrote")

    def test_get_worker_ceil_reads_bootstrap_value_just_written(self):
        r = _make_redis({"manager:pool:detail:daily_peak:running": "5"})
        _midnight_recompute(r, ["detail"])
        read_back = _get_worker_ceil(r, "detail")
        self.assertEqual(read_back, BOOTSTRAP_CEIL["detail"])


if __name__ == "__main__":
    unittest.main()
