"""
tests/test_watchdog.py
─────────────────────────────────────────────────────────────────────────────
Comprehensive tests for workers/watchdog.py — heartbeat watchdog and
orphan / hung-worker detection.

All Redis interactions are mocked; the functions under test are exercised
directly without a real Redis instance.

Coverage map
────────────
  TestCheckOrphans
    · No companies in inflight set → 0 requeued
    · Company in inflight + heartbeat alive → not an orphan
    · Company in inflight + in poll:adaptive queue → not an orphan (already rescheduled)
    · Company in inflight + in poll:fullscan queue → not an orphan
    · Company in inflight + no heartbeat + not in queues → orphan: re-queued
    · Multiple orphans → all re-queued, count returned
    · Orphan requeue calls zadd with company and score
    · Orphan removed from inflight set after requeue
    · Company in both queues and inflight → not an orphan
    · Empty inflight set returns 0 immediately

  TestCheckHungWorkers
    · No alive heartbeats → no hung workers
    · Heartbeat alive + progress key exists → not hung
    · Heartbeat alive + progress key missing → hung (added to list)
    · Multiple heartbeats: only ones with missing progress are hung
    · Returns list of company names for hung workers
    · Empty heartbeat set → empty list returned

  TestTrackClearInflight
    · track_inflight calls sadd with correct key and company
    · clear_inflight calls srem with correct key and company

  TestGetOrphanRequeueScore
    · Platform healthy (error_rate=0) → score = now (immediate requeue)
    · Platform error rate above CONCURRENCY_ERROR_RATE_REDUCE → score > now (backoff)
    · DB lookup failure → falls back to immediate requeue (score = now)
    · No platform found in DB → falls back to immediate requeue
    · Backoff delay is positive when platform is struggling

  TestWatchdogConstants
    · WATCHDOG_INTERVAL_S = 60
"""

import sys
import os
import time
import unittest
from unittest.mock import MagicMock, patch, call

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _make_redis_mock(
    inflight=None,
    alive_heartbeats=None,
    in_adaptive=None,
    in_fullscan=None,
    progress_exists=None,
):
    """
    Build a minimal Redis mock for watchdog tests.

    inflight:         set of company strings in watchdog:inflight
    alive_heartbeats: list of b"heartbeat:{company}" keys returned by SCAN
    in_adaptive:      set of companies in poll:adaptive ZSET
    in_fullscan:      set of companies in poll:fullscan ZSET
    progress_exists:  dict mapping company → bool (does progress key exist?)
    """
    r = MagicMock()

    # SCAN for heartbeat:* keys
    alive_hb_keys = [f"heartbeat:{c}".encode() for c in (alive_heartbeats or [])]
    r.scan.side_effect = [(0, alive_hb_keys)]  # single scan iteration

    # smembers("watchdog:inflight") returns set of bytes
    inflight_set = {c.encode() if isinstance(c, str) else c
                    for c in (inflight or [])}
    r.smembers.return_value = inflight_set

    # zrange for poll:adaptive and poll:fullscan
    adaptive_set = {c.encode() if isinstance(c, str) else c
                    for c in (in_adaptive or [])}
    fullscan_set = {c.encode() if isinstance(c, str) else c
                    for c in (in_fullscan or [])}

    def _zrange(key, *args, **kwargs):
        if "adaptive" in key:
            return adaptive_set
        return fullscan_set

    r.zrange.side_effect = _zrange

    # zadd / srem — just track calls
    r.zadd.return_value = 1
    r.srem.return_value = 1

    # exists(progress:{company}) — used by check_hung_workers
    progress_map = progress_exists or {}

    def _exists(key):
        if key.startswith("progress:"):
            company = key.split(":", 1)[1]
            return int(progress_map.get(company, False))
        return 0

    r.exists.side_effect = _exists

    return r


# ─────────────────────────────────────────────────────────────────────────────
# TestCheckOrphans
# ─────────────────────────────────────────────────────────────────────────────

class TestCheckOrphans(unittest.TestCase):

    def _run(self, inflight=None, alive_heartbeats=None,
             in_adaptive=None, in_fullscan=None, requeue_score=None):
        r = _make_redis_mock(
            inflight=inflight,
            alive_heartbeats=alive_heartbeats,
            in_adaptive=in_adaptive,
            in_fullscan=in_fullscan,
        )
        # Patch get_redis to return our mock, and _get_orphan_requeue_score
        # to return "now" (so requeue is immediate — simplify orphan path)
        score = requeue_score if requeue_score is not None else time.time()
        with patch("workers.watchdog.get_redis", return_value=r), \
             patch("workers.watchdog._get_orphan_requeue_score",
                   return_value=score):
            from workers.watchdog import check_orphans
            return check_orphans(), r

    def test_empty_inflight_returns_zero(self):
        """No companies in inflight → 0 requeued."""
        count, _ = self._run(inflight=[])
        self.assertEqual(count, 0)

    def test_company_with_alive_heartbeat_not_orphan(self):
        """Company in inflight AND heartbeat still alive → not an orphan."""
        count, _ = self._run(
            inflight=["CompanyA"],
            alive_heartbeats=["CompanyA"],
        )
        self.assertEqual(count, 0)

    def test_company_in_adaptive_queue_not_orphan(self):
        """Company in inflight AND in poll:adaptive → already rescheduled, not orphan."""
        count, _ = self._run(
            inflight=["CompanyA"],
            alive_heartbeats=[],
            in_adaptive=["CompanyA"],
        )
        self.assertEqual(count, 0)

    def test_company_in_fullscan_queue_not_orphan(self):
        """Company in inflight AND in poll:fullscan → not an orphan."""
        count, _ = self._run(
            inflight=["CompanyA"],
            alive_heartbeats=[],
            in_fullscan=["CompanyA"],
        )
        self.assertEqual(count, 0)

    def test_orphan_detected_and_requeued(self):
        """Company in inflight + no heartbeat + not in queues → orphaned, requeued."""
        count, r = self._run(
            inflight=["CompanyOrphan"],
            alive_heartbeats=[],
            in_adaptive=[],
            in_fullscan=[],
        )
        self.assertEqual(count, 1)
        # Should have called zadd to re-queue
        r.zadd.assert_called_once()

    def test_orphan_removed_from_inflight_after_requeue(self):
        """Orphaned company is removed from watchdog:inflight after re-queue."""
        count, r = self._run(
            inflight=["OrphanCo"],
            alive_heartbeats=[],
        )
        self.assertEqual(count, 1)
        # srem called to remove from inflight
        srem_calls = [str(c) for c in r.srem.call_args_list]
        self.assertTrue(any("inflight" in c for c in srem_calls))

    def test_multiple_orphans_all_requeued(self):
        """Multiple orphaned companies → all requeued, count = number of orphans."""
        count, r = self._run(
            inflight=["A", "B", "C"],
            alive_heartbeats=[],
            in_adaptive=[],
            in_fullscan=[],
        )
        self.assertEqual(count, 3)
        self.assertEqual(r.zadd.call_count, 3)

    def test_mix_of_orphan_and_non_orphan(self):
        """Only true orphans requeued; heartbeat-alive companies skipped."""
        count, r = self._run(
            inflight=["OrphanCo", "AliveCo"],
            alive_heartbeats=["AliveCo"],  # AliveCo has heartbeat
            in_adaptive=[],
            in_fullscan=[],
        )
        self.assertEqual(count, 1)

    def test_company_in_both_queues_not_orphan(self):
        """Company in both adaptive and fullscan queues → not an orphan."""
        count, _ = self._run(
            inflight=["MultipleCo"],
            alive_heartbeats=[],
            in_adaptive=["MultipleCo"],
            in_fullscan=["MultipleCo"],
        )
        self.assertEqual(count, 0)

    def test_requeue_uses_correct_redis_queue_key(self):
        """Orphan is requeued to poll:adaptive ZSET."""
        from config import REDIS_POLL_ADAPTIVE
        _, r = self._run(
            inflight=["OrphanCo"],
            alive_heartbeats=[],
        )
        r.zadd.assert_called_once()
        args = r.zadd.call_args
        # First positional arg to zadd should be the queue key
        queue_key = args[0][0]
        self.assertEqual(queue_key, REDIS_POLL_ADAPTIVE)


# ─────────────────────────────────────────────────────────────────────────────
# TestCheckHungWorkers
# ─────────────────────────────────────────────────────────────────────────────

class TestCheckHungWorkers(unittest.TestCase):

    def _run(self, alive_heartbeats=None, progress_exists=None):
        r = MagicMock()
        # SCAN for heartbeat:* → returns alive_heartbeats
        hb_keys = [f"heartbeat:{c}".encode()
                   for c in (alive_heartbeats or [])]
        r.scan.side_effect = [(0, hb_keys)]

        progress_map = progress_exists or {}

        def _exists(key):
            if key.startswith("progress:"):
                company = key.split(":", 1)[1]
                return int(progress_map.get(company, False))
            return 0

        r.exists.side_effect = _exists

        with patch("workers.watchdog.get_redis", return_value=r):
            from workers.watchdog import check_hung_workers
            return check_hung_workers()

    def test_no_alive_heartbeats_returns_empty_list(self):
        """No heartbeat keys → empty list."""
        result = self._run(alive_heartbeats=[])
        self.assertEqual(result, [])

    def test_heartbeat_alive_with_progress_key_not_hung(self):
        """Heartbeat alive AND progress key exists → not hung."""
        result = self._run(
            alive_heartbeats=["CompanyA"],
            progress_exists={"CompanyA": True},
        )
        self.assertEqual(result, [])

    def test_heartbeat_alive_without_progress_key_is_hung(self):
        """Heartbeat alive but progress key expired → hung."""
        result = self._run(
            alive_heartbeats=["CompanyA"],
            progress_exists={"CompanyA": False},
        )
        self.assertEqual(result, ["CompanyA"])

    def test_returns_company_name_not_key(self):
        """Returns company name (not the full 'heartbeat:...' key)."""
        result = self._run(
            alive_heartbeats=["MyCompany"],
            progress_exists={"MyCompany": False},
        )
        self.assertIn("MyCompany", result)
        self.assertNotIn("heartbeat:MyCompany", result)

    def test_multiple_heartbeats_only_hung_ones_returned(self):
        """Only companies with missing progress keys appear in result."""
        result = self._run(
            alive_heartbeats=["Co1", "Co2", "Co3"],
            progress_exists={"Co1": True, "Co2": False, "Co3": True},
        )
        self.assertEqual(result, ["Co2"])

    def test_all_hung_returns_all_companies(self):
        """All companies with expired progress → all in result."""
        result = self._run(
            alive_heartbeats=["A", "B", "C"],
            progress_exists={},  # all False by default
        )
        self.assertCountEqual(result, ["A", "B", "C"])

    def test_returns_list_not_set(self):
        """check_hung_workers returns a list."""
        result = self._run(alive_heartbeats=[])
        self.assertIsInstance(result, list)


# ─────────────────────────────────────────────────────────────────────────────
# TestTrackClearInflight
# ─────────────────────────────────────────────────────────────────────────────

class TestTrackClearInflight(unittest.TestCase):

    def test_track_inflight_calls_sadd(self):
        """track_inflight adds company to watchdog:inflight."""
        mock_r = MagicMock()
        with patch("workers.watchdog.get_redis", return_value=mock_r):
            from workers.watchdog import track_inflight
            track_inflight("CompanyX")
        mock_r.sadd.assert_called_once_with("watchdog:inflight", "CompanyX")

    def test_clear_inflight_calls_srem(self):
        """clear_inflight removes company from watchdog:inflight."""
        mock_r = MagicMock()
        with patch("workers.watchdog.get_redis", return_value=mock_r):
            from workers.watchdog import clear_inflight
            clear_inflight("CompanyX")
        mock_r.srem.assert_called_once_with("watchdog:inflight", "CompanyX")

    def test_track_uses_correct_key(self):
        """track_inflight uses the 'watchdog:inflight' key (not any other)."""
        mock_r = MagicMock()
        with patch("workers.watchdog.get_redis", return_value=mock_r):
            from workers.watchdog import track_inflight
            track_inflight("Foo")
        args = mock_r.sadd.call_args[0]
        self.assertEqual(args[0], "watchdog:inflight")

    def test_clear_uses_correct_key(self):
        """clear_inflight uses the 'watchdog:inflight' key."""
        mock_r = MagicMock()
        with patch("workers.watchdog.get_redis", return_value=mock_r):
            from workers.watchdog import clear_inflight
            clear_inflight("Foo")
        args = mock_r.srem.call_args[0]
        self.assertEqual(args[0], "watchdog:inflight")


# ─────────────────────────────────────────────────────────────────────────────
# TestGetOrphanRequeueScore
# ─────────────────────────────────────────────────────────────────────────────

class TestGetOrphanRequeueScore(unittest.TestCase):

    def _run(self, error_rate, platform="greenhouse", db_error=False):
        """
        Simulate _get_orphan_requeue_score() with mocked DB and error-rate.
        """
        mock_r = MagicMock()
        now = time.time()

        mock_conn = MagicMock()
        mock_row  = MagicMock()
        mock_row.__getitem__ = lambda self, key: platform if key == "ats_platform" else None
        mock_row.__bool__ = lambda self: True
        mock_conn.execute.return_value.fetchone.return_value = mock_row

        if db_error:
            mock_conn.execute.side_effect = Exception("DB error")

        with patch("workers.http_client.get_error_rate", return_value=error_rate), \
             patch("workers.scan_worker._get_backoff_delay", return_value=300), \
             patch("db.db.get_conn", return_value=mock_conn):
            from workers.watchdog import _get_orphan_requeue_score
            return _get_orphan_requeue_score(mock_r, "TestCo", now), now

    def test_healthy_platform_requeues_immediately(self):
        """Error rate = 0 (healthy) → score = now (immediate requeue)."""
        from config import CONCURRENCY_ERROR_RATE_REDUCE
        score, now = self._run(error_rate=0.0)
        # Should be at or near now — allow 1 second tolerance
        self.assertAlmostEqual(score, now, delta=1.0)

    def test_struggling_platform_uses_backoff(self):
        """Error rate above threshold → score = now + backoff_delay."""
        from config import CONCURRENCY_ERROR_RATE_REDUCE
        above_threshold = CONCURRENCY_ERROR_RATE_REDUCE + 0.01
        score, now = self._run(error_rate=above_threshold)
        # _get_backoff_delay returns 300, so score should be now + 300
        self.assertAlmostEqual(score, now + 300, delta=1.0)

    def test_db_error_falls_back_to_immediate_requeue(self):
        """DB lookup failure → falls back to immediate requeue."""
        mock_r = MagicMock()
        now = time.time()
        with patch("db.db.get_conn", side_effect=Exception("DB down")):
            from workers.watchdog import _get_orphan_requeue_score
            score = _get_orphan_requeue_score(mock_r, "TestCo", now)
        # Should not raise; should return approximately now
        self.assertAlmostEqual(score, now, delta=2.0)

    def test_backoff_delay_is_positive_when_struggling(self):
        """Backoff delay adds a positive number of seconds."""
        from config import CONCURRENCY_ERROR_RATE_REDUCE
        above = CONCURRENCY_ERROR_RATE_REDUCE + 0.05
        score, now = self._run(error_rate=above)
        self.assertGreater(score, now)


# ─────────────────────────────────────────────────────────────────────────────
# TestWatchdogConstants
# ─────────────────────────────────────────────────────────────────────────────

class TestWatchdogConstants(unittest.TestCase):

    def test_watchdog_interval_is_60(self):
        """WATCHDOG_INTERVAL_S = 60 seconds."""
        from workers.watchdog import WATCHDOG_INTERVAL_S
        self.assertEqual(WATCHDOG_INTERVAL_S, 60)

    def test_watchdog_interval_positive(self):
        """WATCHDOG_INTERVAL_S must be positive."""
        from workers.watchdog import WATCHDOG_INTERVAL_S
        self.assertGreater(WATCHDOG_INTERVAL_S, 0)

    def test_inflight_key_string(self):
        """The inflight tracking Redis key is the expected string."""
        # Verify by calling track_inflight and inspecting the call
        mock_r = MagicMock()
        with patch("workers.watchdog.get_redis", return_value=mock_r):
            from workers.watchdog import track_inflight
            track_inflight("test")
        key = mock_r.sadd.call_args[0][0]
        self.assertEqual(key, "watchdog:inflight")


# ─────────────────────────────────────────────────────────────────────────────
# TestRunWatchdogOnce
# ─────────────────────────────────────────────────────────────────────────────

class TestRunWatchdogOnce(unittest.TestCase):
    """
    Test that run_watchdog(once=True) calls check_orphans and check_hung_workers
    exactly once when Redis is reachable.
    """

    def test_once_mode_calls_both_checks(self):
        """run_watchdog(once=True) calls check_orphans and check_hung_workers."""
        with patch("workers.watchdog.ping", return_value=True), \
             patch("workers.watchdog.check_orphans", return_value=0) as mock_orphans, \
             patch("workers.watchdog.check_hung_workers", return_value=[]) as mock_hung:
            from workers.watchdog import run_watchdog
            run_watchdog(once=True)
        mock_orphans.assert_called_once()
        mock_hung.assert_called_once()

    def test_redis_unreachable_exits(self):
        """run_watchdog aborts when Redis not reachable."""
        with patch("workers.watchdog.ping", return_value=False), \
             self.assertRaises(SystemExit):
            from workers.watchdog import run_watchdog
            run_watchdog(once=True)


if __name__ == "__main__":
    unittest.main(verbosity=2)
