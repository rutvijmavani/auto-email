"""
tests/test_health_alerts.py — Redis memory and detail-queue alerts in the manager.

These alerts used to live only in the scheduler's slow-throughput loop, which
nothing started; workers.manager._check_health_alerts now runs them.
"""

import unittest
from unittest.mock import MagicMock, patch

import workers.manager as mgr
from config import (
    DETAIL_QUEUE_ALERT_CYCLES,
    DETAIL_QUEUE_HIGH_WATERMARK,
    REDIS_MEMORY_ALERT_PCT,
    WORKER_SLOW_CHECK_INTERVAL_S,
)

MB = 1024 * 1024


def _redis(used_mb=10, max_mb=100, detail_depth=0):
    r = MagicMock()
    r.info.return_value = {"used_memory": used_mb * MB, "maxmemory": max_mb * MB}
    r.llen.return_value = detail_depth
    return r


class TestHealthAlerts(unittest.TestCase):

    def setUp(self):
        mgr._last_health_check_ts = 0.0
        mgr._detail_alert_streak = 0
        self.now = float(10 * WORKER_SLOW_CHECK_INTERVAL_S)
        patcher = patch("db.pipeline_alerts.create_alert", return_value=1)
        self.create_alert = patcher.start()
        self.addCleanup(patcher.stop)

    def _alert_types(self):
        return [c.kwargs["alert_type"] for c in self.create_alert.call_args_list]

    def test_memory_alert_fires_at_threshold(self):
        mgr._check_health_alerts(_redis(used_mb=REDIS_MEMORY_ALERT_PCT), self.now)
        self.assertEqual(self._alert_types(), ["redis_memory"])
        self.assertEqual(self.create_alert.call_args.kwargs["severity"], "critical")

    def test_no_memory_alert_below_threshold(self):
        mgr._check_health_alerts(_redis(used_mb=REDIS_MEMORY_ALERT_PCT - 1), self.now)
        self.assertEqual(self._alert_types(), [])

    def test_no_memory_alert_without_maxmemory(self):
        mgr._check_health_alerts(_redis(used_mb=500, max_mb=0), self.now)
        self.assertEqual(self._alert_types(), [])

    def test_detail_alert_needs_consecutive_checks(self):
        r = _redis(detail_depth=DETAIL_QUEUE_HIGH_WATERMARK + 1)
        for i in range(DETAIL_QUEUE_ALERT_CYCLES):
            self.assertEqual(self._alert_types(), [])
            mgr._check_health_alerts(r, self.now + i * WORKER_SLOW_CHECK_INTERVAL_S)
        self.assertEqual(self._alert_types(), ["detail_queue_depth"])

    def test_detail_streak_resets_when_queue_drains(self):
        high = _redis(detail_depth=DETAIL_QUEUE_HIGH_WATERMARK + 1)
        low = _redis(detail_depth=0)
        step = WORKER_SLOW_CHECK_INTERVAL_S
        mgr._check_health_alerts(high, self.now)
        mgr._check_health_alerts(low, self.now + step)
        self.assertEqual(mgr._detail_alert_streak, 0)

    def test_throttled_to_check_interval(self):
        r = _redis(detail_depth=DETAIL_QUEUE_HIGH_WATERMARK + 1)
        mgr._check_health_alerts(r, self.now)
        mgr._check_health_alerts(r, self.now + 1)
        self.assertEqual(mgr._detail_alert_streak, 1)

    def test_redis_error_does_not_raise(self):
        r = MagicMock()
        r.info.side_effect = RuntimeError("down")
        r.llen.side_effect = RuntimeError("down")
        mgr._check_health_alerts(r, self.now)


if __name__ == "__main__":
    unittest.main()
