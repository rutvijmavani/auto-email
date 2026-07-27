"""
tests/test_layer1.py — Layer 1 midnight recompute.

_update_daily_peak_running():
  · updates running key when n_workers > current
  · does NOT update when n_workers <= current (no regression)

_midnight_recompute() — bootstrap mode (< 28 daily_peak records):
  · uses BOOTSTRAP_CEIL[pool] for the worker ceiling
  · stores today's running peak as the daily_peak date record
  · resets daily_peak:running to 0

_midnight_recompute() — formula mode (>= 28 daily_peak records):
  · switches from BOOTSTRAP_CEIL to formula on exactly day 28
  · quiet day (running_peak=0): floor applied, not literal 0
  · growth_buffer > 0 when recent demand exceeds baseline
  · growth_buffer = 0 when demand is shrinking (negative rate clamped)
  · growth_buffer = 0 when fewer than 7 recent records
  · volatility_buffer from stdev × 0.25, ceiling applied
  · buffer = max(growth_buffer, volatility_buffer)
  · peaks beyond 28-day window are excluded (natural decay)
"""

import math
import statistics
import sys
import os
import unittest
from datetime import datetime, timezone, timedelta
from unittest.mock import patch

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from config import WORKER_FLOOR, MONITOR_MAX_WORKERS
from workers.manager import (
    BOOTSTRAP_CEIL,
    BOOTSTRAP_DAYS_REQUIRED,
    _midnight_recompute,
    _update_daily_peak_running,
)


# ─────────────────────────────────────────────────────────────────────────────
# Minimal dict-backed fake Redis (no external dependencies)
# ─────────────────────────────────────────────────────────────────────────────

class _FakeRedis:
    """
    Dict-backed fake Redis for _midnight_recompute tests.

    Supports: get, set, scan (single-shot, no cursor pagination), delete,
    exists, incr, expire, rpush, pipeline (stub).
    """

    def __init__(self, initial=None):
        self._d = dict(initial or {})

    # ── Read / write ──────────────────────────────────────────────────────────

    def get(self, key):
        v = self._d.get(key)
        # Return bytes so callers that do .decode() work correctly
        if isinstance(v, str):
            return v.encode()
        return v

    def set(self, key, value, **kwargs):
        self._d[key] = str(value)

    def delete(self, *keys):
        for k in keys:
            self._d.pop(k, None)

    def exists(self, key):
        return 1 if key in self._d else 0

    def incr(self, key):
        v = int(self._d.get(key, 0)) + 1
        self._d[key] = str(v)
        return v

    def expire(self, key, ttl):
        pass

    def rpush(self, key, *values):
        pass

    def pipeline(self):
        return _FakePipeline(self)

    # ── Scan (single-shot — always returns cursor=0) ──────────────────────────

    def scan(self, cursor, match=None, count=None):
        import fnmatch
        if match:
            matched = [k for k in self._d if fnmatch.fnmatch(k, match)]
        else:
            matched = list(self._d.keys())
        return 0, matched

    # ── Convenience ───────────────────────────────────────────────────────────

    def read(self, key) -> str | None:
        """Read a stored value as a decoded string (test helper)."""
        v = self._d.get(key)
        return v if isinstance(v, str) else (v.decode() if v else None)


class _FakePipeline:
    def __init__(self, r):
        self._r = r
        self._cmds = []

    def set(self, *a, **kw):
        self._cmds.append(("set", a, kw))
        return self

    def rpush(self, *a, **kw):
        self._cmds.append(("rpush", a, kw))
        return self

    def execute(self):
        results = []
        for cmd, a, kw in self._cmds:
            getattr(self._r, cmd)(*a, **kw)
            results.append(None)
        return results


# ─────────────────────────────────────────────────────────────────────────────
# Helper: populate daily_peak date records
# ─────────────────────────────────────────────────────────────────────────────

def _today_utc():
    return datetime.now(timezone.utc).date()


def _set_peak(r, pool, date_obj, value):
    key = f"manager:pool:{pool}:daily_peak:{date_obj.strftime('%Y-%m-%d')}"
    r._d[key] = str(value)


def _set_running(r, pool, value):
    r._d[f"manager:pool:{pool}:daily_peak:running"] = str(value)


def _get_ceil(r, pool) -> int:
    v = r.read(f"manager:worker_ceil:{pool}")
    return int(v) if v is not None else None


def _populate_peaks(r, pool, n_days, peak_value=5, offset_start=1):
    """
    Insert n_days of daily_peak records starting from offset_start days ago.
    So offset_start=1 means yesterday, offset_start=2 means 2 days ago, etc.
    """
    today = _today_utc()
    for i in range(n_days):
        d = today - timedelta(days=offset_start + i)
        _set_peak(r, pool, d, peak_value)


# ─────────────────────────────────────────────────────────────────────────────
# _update_daily_peak_running
# ─────────────────────────────────────────────────────────────────────────────

class TestUpdateDailyPeakRunning(unittest.TestCase):

    def test_updates_when_higher(self):
        """n_workers > current running peak → running key is updated."""
        r = _FakeRedis({"manager:pool:detail:daily_peak:running": "3"})
        _update_daily_peak_running(r, "detail", 7)
        self.assertEqual(r.read("manager:pool:detail:daily_peak:running"), "7")

    def test_no_update_when_equal_or_lower(self):
        """n_workers <= current → running key unchanged (no regression)."""
        r = _FakeRedis({"manager:pool:detail:daily_peak:running": "8"})
        _update_daily_peak_running(r, "detail", 6)
        self.assertEqual(r.read("manager:pool:detail:daily_peak:running"), "8")

    def test_seeds_from_zero(self):
        """Key missing → any n_workers seeds the running peak."""
        r = _FakeRedis()
        _update_daily_peak_running(r, "detail", 4)
        self.assertEqual(r.read("manager:pool:detail:daily_peak:running"), "4")


# ─────────────────────────────────────────────────────────────────────────────
# _midnight_recompute — bootstrap mode
# ─────────────────────────────────────────────────────────────────────────────

class TestMidnightRecomputeBootstrap(unittest.TestCase):

    def _run(self, r):
        _midnight_recompute(r, ["detail"])

    def test_bootstrap_uses_fixed_ceil(self):
        """< 28 daily_peak records → BOOTSTRAP_CEIL used, not formula."""
        r = _FakeRedis()
        _set_running(r, "detail", 5)
        # 0 pre-existing records; after the run today is added → 1 total
        self._run(r)
        self.assertEqual(_get_ceil(r, "detail"), BOOTSTRAP_CEIL["detail"])

    def test_running_reset_to_zero(self):
        """daily_peak:running is always reset to 0 by midnight recompute."""
        r = _FakeRedis()
        _set_running(r, "detail", 9)
        self._run(r)
        self.assertEqual(r.read("manager:pool:detail:daily_peak:running"), "0")

    def test_today_record_stored_from_running(self):
        """Today's running peak is persisted as the daily_peak date record."""
        today = _today_utc().strftime("%Y-%m-%d")
        r = _FakeRedis()
        _set_running(r, "detail", 7)
        self._run(r)
        today_key = f"manager:pool:detail:daily_peak:{today}"
        self.assertEqual(r.read(today_key), "7")

    def test_quiet_day_applies_floor_not_zero(self):
        """running_peak=0 → WORKER_FLOOR stored, never literal 0."""
        today = _today_utc().strftime("%Y-%m-%d")
        r = _FakeRedis()
        # running peak missing → treated as 0 → max(0, WORKER_FLOOR) = WORKER_FLOOR
        self._run(r)
        today_key = f"manager:pool:detail:daily_peak:{today}"
        stored = int(r.read(today_key))
        self.assertGreaterEqual(stored, WORKER_FLOOR)


# ─────────────────────────────────────────────────────────────────────────────
# _midnight_recompute — bootstrap → formula switchover at day 28
# ─────────────────────────────────────────────────────────────────────────────

class TestMidnightRecomputeSwitchover(unittest.TestCase):

    def test_exactly_27_records_stays_in_bootstrap(self):
        """
        26 pre-existing records + today (written by _midnight_recompute) = 27 total.
        27 < BOOTSTRAP_DAYS_REQUIRED (28) → still bootstrap mode.
        """
        r = _FakeRedis()
        _set_running(r, "detail", 5)
        # 26 pre-existing → today makes 27 total → still bootstrap
        _populate_peaks(r, "detail", 26, peak_value=5)
        _midnight_recompute(r, ["detail"])
        self.assertEqual(_get_ceil(r, "detail"), BOOTSTRAP_CEIL["detail"],
                         "27 records: still bootstrap")

    def test_exactly_28_records_uses_formula(self):
        """
        27 pre-existing records + today (written by _midnight_recompute) = 28
        → formula mode.  The resulting ceiling must NOT equal BOOTSTRAP_CEIL
        for at least some peak distributions (here: uniform peaks where buffer=0
        and formula gives peak_nd, which equals 5 ≠ 10 = BOOTSTRAP_CEIL["detail"]).
        """
        r = _FakeRedis()
        _set_running(r, "detail", 5)
        # 27 pre-existing → today makes 28 total → formula mode
        _populate_peaks(r, "detail", 27, peak_value=5)
        _midnight_recompute(r, ["detail"])
        expected_formula = max(WORKER_FLOOR, min(5, MONITOR_MAX_WORKERS))
        self.assertEqual(_get_ceil(r, "detail"), expected_formula,
                         "28 records: formula ceil = peak_nd=5 with zero buffer")


# ─────────────────────────────────────────────────────────────────────────────
# _midnight_recompute — growth_buffer
# ─────────────────────────────────────────────────────────────────────────────

class TestGrowthBuffer(unittest.TestCase):
    """
    growth_buffer = ceil(peak_nd × max(0, (peak_7d − baseline_val) / baseline_val / 3))

    peak_7d      = max of daily_peaks[:7]   (most recent 7 days, including today)
    baseline_val = max of daily_peaks[7:]   (days 8–28)
    """

    def _compute(self, recent_7, baseline_21):
        """
        Populate fake Redis with exactly 27 pre-existing records then run
        _midnight_recompute (which writes today = 28 total → formula mode).

        recent_7   : 7 values; [0] becomes today's running peak, [1:] = days 1–6
        baseline_21: 21 values; days 7–27

        Total pre-existing = 6 + 21 = 27; after today is written = 28 → formula.
        """
        if len(recent_7) != 7 or len(baseline_21) != 21:
            raise ValueError("Supply exactly 7 recent + 21 baseline peaks")

        today = _today_utc()
        r = _FakeRedis()
        _set_running(r, "detail", recent_7[0])          # today via running

        for i, val in enumerate(recent_7[1:], start=1):  # days 1–6
            _set_peak(r, "detail", today - timedelta(days=i), val)
        for i, val in enumerate(baseline_21, start=7):   # days 7–27
            _set_peak(r, "detail", today - timedelta(days=i), val)

        _midnight_recompute(r, ["detail"])
        return _get_ceil(r, "detail")

    def _expected(self, recent_7, baseline_21):
        """Compute expected ceiling using the same formula as the code."""
        all_28    = list(recent_7) + list(baseline_21)
        peak_nd   = max(all_28)
        peak_7d   = max(recent_7)
        base_val  = max(baseline_21)
        if base_val > 0:
            rate = max(0.0, (peak_7d - base_val) / base_val / 3)
            g_buf = math.ceil(peak_nd * rate)
        else:
            g_buf = 0
        v_buf = math.ceil(statistics.stdev(all_28) * 0.25) if len(all_28) >= 3 else 0
        buf   = max(g_buf, v_buf)
        return max(WORKER_FLOOR, min(peak_nd + buf, MONITOR_MAX_WORKERS))

    def test_growth_buffer_positive_when_growing(self):
        """peak_7d > baseline → growth_buffer > 0 → ceil > peak_nd."""
        recent   = [10] * 7
        baseline = [5]  * 21

        ceil_    = self._compute(recent, baseline)
        expected = self._expected(recent, baseline)
        self.assertEqual(ceil_, expected)
        self.assertGreater(ceil_, max(recent),
                           "ceiling must exceed peak_nd when demand is growing")

    def test_growth_buffer_zero_when_shrinking(self):
        """peak_7d < baseline → negative rate clamped to 0 → growth_buffer = 0."""
        recent   = [3]  * 7
        baseline = [10] * 21

        ceil_    = self._compute(recent, baseline)
        expected = self._expected(recent, baseline)
        self.assertEqual(ceil_, expected)
        # Ceiling should not exceed peak_nd + vol_buffer (growth contributes 0)
        all_28   = recent + baseline
        peak_nd  = max(all_28)
        vol_buf  = math.ceil(statistics.stdev(all_28) * 0.25)
        self.assertLessEqual(ceil_, max(WORKER_FLOOR, min(peak_nd + vol_buf, MONITOR_MAX_WORKERS)))

    def test_growth_buffer_zero_with_equal_peaks(self):
        """All peaks identical → growth_rate = 0 → growth_buffer = 0, vol = 0."""
        recent   = [5] * 7
        baseline = [5] * 21

        ceil_    = self._compute(recent, baseline)
        # stdev = 0 → buf = 0 → ceil = peak_nd = 5
        expected = max(WORKER_FLOOR, min(5, MONITOR_MAX_WORKERS))
        self.assertEqual(ceil_, expected)


# ─────────────────────────────────────────────────────────────────────────────
# _midnight_recompute — volatility_buffer
# ─────────────────────────────────────────────────────────────────────────────

class TestVolatilityBuffer(unittest.TestCase):

    def test_volatility_buffer_with_variable_peaks(self):
        """ceil(stdev × 0.25) contributes to ceiling when peaks vary."""
        today = _today_utc()
        r = _FakeRedis()
        # Use running = 10 (today's value) + 27 pre-existing of alternating 2 and 10
        _set_running(r, "detail", 10)
        for i in range(1, 28):
            v = 10 if i % 2 == 0 else 2
            _set_peak(r, "detail", today - timedelta(days=i), v)

        _midnight_recompute(r, ["detail"])
        ceil_ = _get_ceil(r, "detail")

        all_peaks = [10] + [10 if i % 2 == 0 else 2 for i in range(1, 28)]
        all_peaks = all_peaks[:28]
        peak_nd   = max(all_peaks)
        std       = statistics.stdev(all_peaks)
        vol_buf   = math.ceil(std * 0.25)
        expected  = max(WORKER_FLOOR, min(peak_nd + vol_buf, MONITOR_MAX_WORKERS))
        self.assertEqual(ceil_, expected)

    def test_volatility_zero_with_uniform_peaks(self):
        """All peaks identical → stdev=0 → volatility_buffer=0."""
        today = _today_utc()
        r = _FakeRedis()
        _set_running(r, "detail", 6)
        for i in range(1, 28):
            _set_peak(r, "detail", today - timedelta(days=i), 6)

        _midnight_recompute(r, ["detail"])
        self.assertEqual(_get_ceil(r, "detail"), max(WORKER_FLOOR, min(6, MONITOR_MAX_WORKERS)))


# ─────────────────────────────────────────────────────────────────────────────
# _midnight_recompute — buffer = max(growth, volatility)
# ─────────────────────────────────────────────────────────────────────────────

class TestBufferIsMax(unittest.TestCase):

    def test_uses_growth_when_growth_exceeds_volatility(self):
        """buffer = max(growth_buffer, vol_buffer) picks the larger contributor."""
        today = _today_utc()
        r = _FakeRedis()
        # Flat peaks 5 for baseline, recent high peaks → growth dominates
        _set_running(r, "detail", 20)     # today: spike
        for i in range(1, 7):
            _set_peak(r, "detail", today - timedelta(days=i), 20)   # recent 6
        for i in range(7, 28):
            _set_peak(r, "detail", today - timedelta(days=i), 5)    # baseline 21

        _midnight_recompute(r, ["detail"])
        ceil_ = _get_ceil(r, "detail")

        all_28 = [20] + [20]*6 + [5]*21
        peak_nd = max(all_28)
        baseline_val = max([5]*21)
        weekly_growth = max(0.0, (20 - baseline_val) / baseline_val / 3)
        growth_buf = math.ceil(peak_nd * weekly_growth)
        vol_buf    = math.ceil(statistics.stdev(all_28) * 0.25)
        buf        = max(growth_buf, vol_buf)
        expected   = max(WORKER_FLOOR, min(peak_nd + buf, MONITOR_MAX_WORKERS))
        self.assertEqual(ceil_, expected)


# ─────────────────────────────────────────────────────────────────────────────
# _midnight_recompute — natural decay (peaks beyond 28 days are excluded)
# ─────────────────────────────────────────────────────────────────────────────

class TestNaturalDecay(unittest.TestCase):

    def test_old_spike_excluded_from_28d_window(self):
        """
        A very high spike from day 29+ ago must not appear in the 28-day window
        that the formula uses — daily_peaks[:28] excludes it naturally.
        """
        today = _today_utc()
        r = _FakeRedis()
        _set_running(r, "detail", 5)             # today

        # Days 1-27: normal peaks (5)
        for i in range(1, 28):
            _set_peak(r, "detail", today - timedelta(days=i), 5)

        # Day 29: very high spike (must be excluded from 28-day window)
        _set_peak(r, "detail", today - timedelta(days=29), 100)

        _midnight_recompute(r, ["detail"])
        ceil_ = _get_ceil(r, "detail")

        # All 28 values in the window are 5 (today + 27 days) → peak_nd = 5
        # buffer = 0 (uniform) → ceil = max(WORKER_FLOOR, 5)
        expected = max(WORKER_FLOOR, min(5, MONITOR_MAX_WORKERS))
        self.assertEqual(ceil_, expected,
                         "spike from day 29 must not influence the ceiling")

    def test_exactly_28_most_recent_are_used(self):
        """
        With 30 pre-existing records (+ today = 31 total), only the most
        recent 28 should be used in the formula.
        """
        today = _today_utc()
        r = _FakeRedis()
        _set_running(r, "detail", 5)

        # Days 1-27: normal (5)
        for i in range(1, 28):
            _set_peak(r, "detail", today - timedelta(days=i), 5)

        # Days 28-30: high values (must be excluded — outside the 28-day window)
        for i in range(28, 31):
            _set_peak(r, "detail", today - timedelta(days=i), 999)

        _midnight_recompute(r, ["detail"])
        ceil_ = _get_ceil(r, "detail")

        # Only [today(5), days1-27(5)] = 28 uniform fives → peak_nd=5, buffer=0
        expected = max(WORKER_FLOOR, min(5, MONITOR_MAX_WORKERS))
        self.assertEqual(ceil_, expected,
                         "records from day 29+ must be excluded from the window")


# ─────────────────────────────────────────────────────────────────────────────
# _midnight_recompute — multiple pools (scan, fullscan, detail all run)
# ─────────────────────────────────────────────────────────────────────────────

class TestMidnightRecomputeAllPools(unittest.TestCase):

    def test_all_three_pools_updated(self):
        """_midnight_recompute(['detail','scan','fullscan']) writes ceilings for all."""
        r = _FakeRedis()
        for pool in ("detail", "scan", "fullscan"):
            _set_running(r, pool, 5)

        _midnight_recompute(r, ["detail", "scan", "fullscan"])

        for pool in ("detail", "scan", "fullscan"):
            self.assertIsNotNone(
                r.read(f"manager:worker_ceil:{pool}"),
                f"worker_ceil:{pool} must be set after midnight recompute",
            )
            # Running peak must be reset for each pool
            self.assertEqual(
                r.read(f"manager:pool:{pool}:daily_peak:running"), "0",
                f"daily_peak:running must be reset to 0 for {pool}",
            )


if __name__ == "__main__":
    unittest.main()
