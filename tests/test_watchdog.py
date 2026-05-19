"""
tests/test_watchdog.py
─────────────────────────────────────────────────────────────────────────────
Tests for workers/watchdog.py — hung-worker detection and stream PEL
observability.

NOTE: The old check_orphans() / track_inflight() / clear_inflight() /
_get_orphan_requeue_score() functions were removed in the two-layer scheduler
redesign (Section 5).  PEL + claim_stale_work() (XAUTOCLAIM) in scheduler.py
owns crash-recovery for scan workers.  Tests for those functions have been
removed accordingly.

All Redis interactions are mocked; the functions under test are exercised
directly without a real Redis instance.

Coverage map
────────────
  TestCheckHungWorkers
    · No alive heartbeats → no hung workers
    · Heartbeat alive + progress key exists → not hung
    · Heartbeat alive + progress key missing → hung (added to list)
    · Multiple heartbeats: only ones with missing progress are hung
    · All heartbeats missing progress → all returned
    · Returns list of company names (not full key strings)
    · Returns list type

  TestCheckPelStats
    · Empty PEL → stats dict has total_pending=0 for each stream
    · Non-empty PEL → total_pending reflects xpending count
    · Oldest message above PEL_WARN_AGE_MS → warning logged
    · Oldest message below PEL_WARN_AGE_MS → no warning
    · Consumer list is parsed from xpending summary
    · xpending failure → stream key absent from result (no crash)
    · Returns dict keyed by stream name

  TestWatchdogConstants
    · WATCHDOG_INTERVAL_S = 60
    · PEL_WARN_AGE_MS is positive integer
    · PEL_WARN_AGE_MS ≥ 60_000 (at least 1 minute)

  TestRunWatchdogOnce
    · run_watchdog(once=True) calls check_hung_workers and check_pel_stats
    · run_watchdog aborts when Redis not reachable
"""

import sys
import os
import time
import unittest
from unittest.mock import MagicMock, patch, call

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))


# ─────────────────────────────────────────────────────────────────────────────
# TestCheckHungWorkers
# ─────────────────────────────────────────────────────────────────────────────

class TestCheckHungWorkers(unittest.TestCase):

    def _run(self, alive_heartbeats=None, progress_exists=None):
        """
        Run check_hung_workers() with a mocked Redis.

        alive_heartbeats: list of company names whose heartbeat:{name} key exists
        progress_exists:  dict mapping company name → bool (does progress key exist?)
        """
        r = MagicMock()
        hb_keys = [f"heartbeat:{c}".encode() for c in (alive_heartbeats or [])]
        r.scan.side_effect = [(0, hb_keys)]   # single scan iteration

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
        """Returns company name string, not the full 'heartbeat:...' key."""
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
            progress_exists={},   # all False by default
        )
        self.assertCountEqual(result, ["A", "B", "C"])

    def test_returns_list_not_set(self):
        """check_hung_workers returns a list, not a set or other type."""
        result = self._run(alive_heartbeats=[])
        self.assertIsInstance(result, list)


# ─────────────────────────────────────────────────────────────────────────────
# TestCheckPelStats
# ─────────────────────────────────────────────────────────────────────────────

def _make_xpending_summary(total, consumers=None):
    """Build a minimal xpending summary dict as returned by redis-py."""
    return {
        "pending":   total,
        "min":       b"1000-0",
        "max":       b"9999-0",
        "consumers": [
            {"name": name.encode(), "pending": count}
            for name, count in (consumers or [])
        ],
    }


def _make_xpending_range_entry(msg_id, idle_ms, delivery_count=1, consumer=b"w1"):
    return {
        "message_id":           msg_id.encode() if isinstance(msg_id, str) else msg_id,
        "consumer":             consumer,
        "time_since_delivered": idle_ms,
        "times_delivered":      delivery_count,
    }


class TestCheckPelStats(unittest.TestCase):

    def _make_redis(self, stream_configs):
        """
        Build a mocked Redis for check_pel_stats().

        stream_configs: dict of stream_key → {
            "pending":     int,             # total pending count
            "consumers":   [(name, count)], # consumer list
            "oldest_idle": int | None,      # idle_ms of oldest entry (None = empty range)
            "xpending_error": bool,         # if True, xpending raises
        }
        """
        r = MagicMock()

        def _xpending(stream_key, group):
            cfg = stream_configs.get(stream_key, {})
            if cfg.get("xpending_error"):
                raise Exception("NOGROUP")
            total = cfg.get("pending", 0)
            if total == 0:
                return {"pending": 0, "min": None, "max": None, "consumers": []}
            return _make_xpending_summary(total, cfg.get("consumers", []))

        def _xpending_range(stream_key, group, min, max, count):
            cfg = stream_configs.get(stream_key, {})
            idle = cfg.get("oldest_idle")
            if idle is None:
                return []
            return [_make_xpending_range_entry("1000-0", idle)]

        r.xpending.side_effect      = _xpending
        r.xpending_range.side_effect = _xpending_range
        return r

    def _run(self, stream_configs=None):
        r = self._make_redis(stream_configs or {})
        with patch("workers.watchdog.get_redis", return_value=r):
            from workers.watchdog import check_pel_stats
            return check_pel_stats(), r

    def test_empty_pel_returns_zero_pending(self):
        """Empty PEL → total_pending=0 for both streams."""
        from config import REDIS_STREAM_ADAPTIVE, REDIS_STREAM_FULLSCAN
        result, _ = self._run({
            REDIS_STREAM_ADAPTIVE: {"pending": 0},
            REDIS_STREAM_FULLSCAN: {"pending": 0},
        })
        self.assertEqual(result.get(REDIS_STREAM_ADAPTIVE, {}).get("total_pending"), 0)
        self.assertEqual(result.get(REDIS_STREAM_FULLSCAN, {}).get("total_pending"), 0)

    def test_nonempty_pel_reports_count(self):
        """Non-empty PEL → total_pending matches xpending summary."""
        from config import REDIS_STREAM_ADAPTIVE
        result, _ = self._run({
            REDIS_STREAM_ADAPTIVE: {
                "pending": 5,
                "consumers": [("worker-1", 3), ("worker-2", 2)],
                "oldest_idle": 1000,
            },
        })
        stats = result.get(REDIS_STREAM_ADAPTIVE, {})
        self.assertEqual(stats["total_pending"], 5)

    def test_consumer_list_parsed(self):
        """Consumer list is parsed from xpending summary."""
        from config import REDIS_STREAM_ADAPTIVE
        result, _ = self._run({
            REDIS_STREAM_ADAPTIVE: {
                "pending": 3,
                "consumers": [("w1", 2), ("w2", 1)],
                "oldest_idle": 500,
            },
        })
        consumers = result[REDIS_STREAM_ADAPTIVE]["consumers"]
        names = [c[0] for c in consumers]
        self.assertIn("w1", names)
        self.assertIn("w2", names)

    def test_oldest_age_ms_recorded(self):
        """oldest_age_ms reflects idle time of oldest pending entry."""
        from config import REDIS_STREAM_ADAPTIVE
        result, _ = self._run({
            REDIS_STREAM_ADAPTIVE: {
                "pending": 1,
                "consumers": [("w1", 1)],
                "oldest_idle": 30_000,
            },
        })
        self.assertEqual(result[REDIS_STREAM_ADAPTIVE]["oldest_age_ms"], 30_000)

    def test_old_entry_triggers_warning(self):
        """Entry idle longer than PEL_WARN_AGE_MS → warning logged."""
        from config import REDIS_STREAM_ADAPTIVE
        from workers.watchdog import PEL_WARN_AGE_MS
        r = self._make_redis({
            REDIS_STREAM_ADAPTIVE: {
                "pending": 1,
                "consumers": [("w1", 1)],
                "oldest_idle": PEL_WARN_AGE_MS + 1,
            },
        })
        with patch("workers.watchdog.get_redis", return_value=r), \
             patch("workers.watchdog.logger") as mock_log:
            from workers.watchdog import check_pel_stats
            check_pel_stats()
        # At least one warning should mention the stream
        warning_calls = [str(c) for c in mock_log.warning.call_args_list]
        self.assertTrue(
            any(REDIS_STREAM_ADAPTIVE in c for c in warning_calls),
            msg="Expected a warning mentioning the stream key",
        )

    def test_young_entry_no_warning(self):
        """Entry idle less than PEL_WARN_AGE_MS → no warning logged."""
        from config import REDIS_STREAM_ADAPTIVE
        from workers.watchdog import PEL_WARN_AGE_MS
        r = self._make_redis({
            REDIS_STREAM_ADAPTIVE: {
                "pending": 1,
                "consumers": [("w1", 1)],
                "oldest_idle": PEL_WARN_AGE_MS - 1,
            },
        })
        with patch("workers.watchdog.get_redis", return_value=r), \
             patch("workers.watchdog.logger") as mock_log:
            from workers.watchdog import check_pel_stats
            check_pel_stats()
        warning_calls = [str(c) for c in mock_log.warning.call_args_list]
        # Warnings about PEL age should contain "PEL WARNING"
        pel_warnings = [c for c in warning_calls if "PEL WARNING" in c]
        self.assertEqual(len(pel_warnings), 0)

    def test_xpending_error_stream_absent_from_result(self):
        """xpending raising an exception → that stream key absent from result (no crash)."""
        from config import REDIS_STREAM_ADAPTIVE
        result, _ = self._run({
            REDIS_STREAM_ADAPTIVE: {"xpending_error": True},
        })
        self.assertNotIn(REDIS_STREAM_ADAPTIVE, result)

    def test_returns_dict(self):
        """check_pel_stats returns a dict."""
        result, _ = self._run()
        self.assertIsInstance(result, dict)


# ─────────────────────────────────────────────────────────────────────────────
# TestWatchdogConstants
# ─────────────────────────────────────────────────────────────────────────────

class TestWatchdogConstants(unittest.TestCase):

    def test_watchdog_interval_is_60(self):
        """WATCHDOG_INTERVAL_S = 60 seconds."""
        from workers.watchdog import WATCHDOG_INTERVAL_S
        self.assertEqual(WATCHDOG_INTERVAL_S, 60)

    def test_watchdog_interval_positive(self):
        """WATCHDOG_INTERVAL_S must be a positive integer."""
        from workers.watchdog import WATCHDOG_INTERVAL_S
        self.assertGreater(WATCHDOG_INTERVAL_S, 0)
        self.assertIsInstance(WATCHDOG_INTERVAL_S, int)

    def test_pel_warn_age_ms_positive(self):
        """PEL_WARN_AGE_MS is a positive integer."""
        from workers.watchdog import PEL_WARN_AGE_MS
        self.assertIsInstance(PEL_WARN_AGE_MS, int)
        self.assertGreater(PEL_WARN_AGE_MS, 0)

    def test_pel_warn_age_ms_at_least_one_minute(self):
        """PEL_WARN_AGE_MS should be at least 60 000 ms (1 minute)."""
        from workers.watchdog import PEL_WARN_AGE_MS
        self.assertGreaterEqual(PEL_WARN_AGE_MS, 60_000)


# ─────────────────────────────────────────────────────────────────────────────
# TestRunWatchdogOnce
# ─────────────────────────────────────────────────────────────────────────────

class TestRunWatchdogOnce(unittest.TestCase):

    def test_once_mode_calls_hung_and_pel_checks(self):
        """run_watchdog(once=True) calls check_hung_workers and check_pel_stats."""
        with patch("workers.watchdog.ping", return_value=True), \
             patch("workers.watchdog.check_hung_workers", return_value=[]) as mock_hung, \
             patch("workers.watchdog.check_pel_stats", return_value={}) as mock_pel:
            from workers.watchdog import run_watchdog
            run_watchdog(once=True)
        mock_hung.assert_called_once()
        mock_pel.assert_called_once()

    def test_redis_unreachable_exits(self):
        """run_watchdog aborts with SystemExit when Redis not reachable."""
        with patch("workers.watchdog.ping", return_value=False), \
             self.assertRaises(SystemExit):
            from workers.watchdog import run_watchdog
            run_watchdog(once=True)

    def test_once_mode_does_not_sleep(self):
        """run_watchdog(once=True) runs one cycle without sleeping."""
        with patch("workers.watchdog.ping", return_value=True), \
             patch("workers.watchdog.check_hung_workers", return_value=[]), \
             patch("workers.watchdog.check_pel_stats", return_value={}), \
             patch("time.sleep") as mock_sleep:
            from workers.watchdog import run_watchdog
            run_watchdog(once=True)
        mock_sleep.assert_not_called()


if __name__ == "__main__":
    unittest.main(verbosity=2)
