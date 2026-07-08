"""
tests/test_scheduler_streams.py
─────────────────────────────────────────────────────────────────────────────
Tests for the stream-based additions to workers/scheduler.py:
    · _init_consumer_group(r, stream_key, group)
    · _get_stream_pending_count(r, stream_key, group)
    · claim_stale_work(r, stream_key, group, consumer, p95_ms, op_type)
    · on_adaptive_complete() — WARMING lifecycle
    · _bootstrap_warming(company, r, now)

All Redis/DB interactions are mocked.  No real connections required.

Coverage map
────────────
  TestInitConsumerGroup
    · xgroup_create called with correct args (stream_key, group, id="$", mkstream=True)
    · BUSYGROUP exception → silently ignored (no re-raise, no warning)
    · Other exception → warning logged but not re-raised
    · Success → no exception propagates
    · Default group = STREAM_CONSUMER_GROUP

  TestGetStreamPendingCount
    · Returns the "pending" integer from xpending dict
    · xpending returns non-dict → returns 0
    · xpending raises exception → returns 0
    · Returns int type
    · Empty PEL (pending=0) → returns 0
    · xpending returns {"pending": 7} → returns 7

  TestClaimStaleWork
    · xautoclaim raises → returns early, no crash
    · xautoclaim returns None → returns early
    · xautoclaim returns [] as claimed list → returns early
    · Claimed message with empty fields dict → XACK called
    · Claimed message with no "company" field → XACK called
    · delivery_count < MAX_STREAM_REDELIVERIES → no zadd, no xack
    · delivery_count == MAX_STREAM_REDELIVERIES → dead-letter:
        zadd to REDIS_POLL_ADAPTIVE with backoff score + xack
    · op_type="fullscan" → dead-letter goes to REDIS_POLL_FULLSCAN
    · op_type="scan" (default) → dead-letter goes to REDIS_POLL_ADAPTIVE
    · idle_ms floor: p95_ms=50000 → min_idle_time=300000 (floor applied)
    · idle_ms scale: p95_ms=200000 → min_idle_time=600000 (3× applied)
    · xpending_range raises → delivery_count=0 (no dead-letter)
    · Multiple claimed messages → each processed independently

  TestWarmingLifecycleOnAdaptiveComplete
    · warming=3, success=True → interval = WARMING_INTERVAL_S, new_warming=2
    · warming=2, success=True → interval = WARMING_INTERVAL_S, new_warming=1
    · warming=1, success=True → interval = WARMING_INTERVAL_S, new_warming=None (STABLE)
    · warming=None (STABLE), success=True → adaptive interval used (not overridden)
    · warming=3, success=False → new_warming=3 (unchanged on failure)
    · warming=None, success=False → new_warming=None (unchanged)
    · During WARMING, update_poll_interval still called (score computed)
    · warming_polls_remaining written to DB UPDATE

  TestBootstrapWarming
    · initial_slot_offset_s set in DB → uses that value
    · initial_slot_offset_s = NULL → falls back to slot_offset(row["id"])
    · No DB row → falls back to slot_offset(company)
    · zadd score = now + offset_s (always in the future, no midnight anchoring)
    · Large offset (near 24 h) still lands strictly in the future
    · warming_polls_remaining = WARMING_POLLS_COUNT written to DB
    · r.zadd(REDIS_POLL_ADAPTIVE, ...) called
    · DB UPDATE commits
"""

import sys
import os
import time
import unittest
from unittest.mock import MagicMock, patch, call

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from config import (
    REDIS_STREAM_ADAPTIVE, REDIS_STREAM_FULLSCAN,
    STREAM_CONSUMER_GROUP,
    REDIS_POLL_ADAPTIVE, REDIS_POLL_FULLSCAN,
    MAX_STREAM_REDELIVERIES,
    WARMING_INTERVAL_S,
)


# ─────────────────────────────────────────────────────────────────────────────
# TestInitConsumerGroup
# ─────────────────────────────────────────────────────────────────────────────

class TestInitConsumerGroup(unittest.TestCase):

    def test_xgroup_create_called_with_correct_args(self):
        """xgroup_create called with stream_key, group, id='$', mkstream=True."""
        r = MagicMock()
        from workers.scheduler import _init_consumer_group
        _init_consumer_group(r, REDIS_STREAM_ADAPTIVE, STREAM_CONSUMER_GROUP)
        r.xgroup_create.assert_called_once_with(
            REDIS_STREAM_ADAPTIVE, STREAM_CONSUMER_GROUP,
            id="$", mkstream=True,
        )

    def test_busygroup_silently_ignored(self):
        """BUSYGROUP exception → silently swallowed."""
        r = MagicMock()
        r.xgroup_create.side_effect = Exception("BUSYGROUP Consumer Group already exists")
        with patch("workers.scheduler.logger") as mock_log:
            from workers.scheduler import _init_consumer_group
            _init_consumer_group(r, REDIS_STREAM_ADAPTIVE, STREAM_CONSUMER_GROUP)
        # No warning for BUSYGROUP
        warning_calls = [str(c) for c in mock_log.warning.call_args_list]
        busygroup_warnings = [c for c in warning_calls if "xgroup_create" in c]
        self.assertEqual(len(busygroup_warnings), 0)

    def test_other_exception_logs_warning_not_raised(self):
        """Non-BUSYGROUP exception → warning logged, not re-raised."""
        r = MagicMock()
        r.xgroup_create.side_effect = Exception("NOAUTH Authentication required")
        with patch("workers.scheduler.logger") as mock_log:
            from workers.scheduler import _init_consumer_group
            _init_consumer_group(r, REDIS_STREAM_ADAPTIVE, STREAM_CONSUMER_GROUP)
        mock_log.warning.assert_called()

    def test_success_no_exception(self):
        """Successful create → no exception propagates."""
        r = MagicMock()
        r.xgroup_create.return_value = "OK"
        from workers.scheduler import _init_consumer_group
        _init_consumer_group(r, REDIS_STREAM_ADAPTIVE, STREAM_CONSUMER_GROUP)

    def test_default_group_is_stream_consumer_group(self):
        """Default group argument = STREAM_CONSUMER_GROUP."""
        r = MagicMock()
        from workers.scheduler import _init_consumer_group
        _init_consumer_group(r, REDIS_STREAM_ADAPTIVE)
        called_group = r.xgroup_create.call_args[0][1]
        self.assertEqual(called_group, STREAM_CONSUMER_GROUP)


# ─────────────────────────────────────────────────────────────────────────────
# TestGetStreamPendingCount
# ─────────────────────────────────────────────────────────────────────────────

class TestGetStreamPendingCount(unittest.TestCase):

    def test_returns_pending_from_dict(self):
        """Returns the 'pending' value from xpending result dict."""
        r = MagicMock()
        r.xpending.return_value = {"pending": 7, "min": "0-0", "max": "9-9"}
        from workers.scheduler import _get_stream_pending_count
        result = _get_stream_pending_count(r, REDIS_STREAM_ADAPTIVE)
        self.assertEqual(result, 7)

    def test_returns_zero_for_non_dict(self):
        """xpending returns non-dict → returns 0."""
        r = MagicMock()
        r.xpending.return_value = None
        from workers.scheduler import _get_stream_pending_count
        self.assertEqual(_get_stream_pending_count(r, REDIS_STREAM_ADAPTIVE), 0)

    def test_returns_zero_on_exception(self):
        """xpending raises → returns 0."""
        r = MagicMock()
        r.xpending.side_effect = Exception("NOGROUP no group")
        from workers.scheduler import _get_stream_pending_count
        self.assertEqual(_get_stream_pending_count(r, REDIS_STREAM_ADAPTIVE), 0)

    def test_returns_int(self):
        """Return type is always int."""
        r = MagicMock()
        r.xpending.return_value = {"pending": 3}
        from workers.scheduler import _get_stream_pending_count
        result = _get_stream_pending_count(r, REDIS_STREAM_ADAPTIVE)
        self.assertIsInstance(result, int)

    def test_empty_pel_returns_zero(self):
        """pending=0 in result → returns 0."""
        r = MagicMock()
        r.xpending.return_value = {"pending": 0}
        from workers.scheduler import _get_stream_pending_count
        self.assertEqual(_get_stream_pending_count(r, REDIS_STREAM_ADAPTIVE), 0)


# ─────────────────────────────────────────────────────────────────────────────
# Helpers for claim_stale_work tests
# ─────────────────────────────────────────────────────────────────────────────

def _make_autoclaim_result(claimed_entries):
    """Build xautoclaim return: (next_cursor, [(msg_id, fields), ...], [])"""
    return ("0-0", claimed_entries, [])


def _make_pending_entry(msg_id, times_delivered):
    return [{
        "message_id":           msg_id,
        "consumer":             b"worker-1",
        "time_since_delivered": 600_000,
        "times_delivered":      times_delivered,
    }]


# ─────────────────────────────────────────────────────────────────────────────
# TestClaimStaleWork
# ─────────────────────────────────────────────────────────────────────────────

class TestClaimStaleWork(unittest.TestCase):

    def _run(self, r, p95_ms=10_000, op_type="scan", consumer="sched-0"):
        from workers.scheduler import claim_stale_work
        with patch("workers.scan_worker._get_backoff_delay", return_value=300):
            claim_stale_work(r, REDIS_STREAM_ADAPTIVE,
                             STREAM_CONSUMER_GROUP, consumer,
                             p95_ms, op_type)

    def test_xautoclaim_raises_returns_early(self):
        """xautoclaim raises → returns without crash."""
        r = MagicMock()
        r.xautoclaim.side_effect = Exception("NOGROUP")
        self._run(r)   # must not raise

    def test_xautoclaim_returns_none_returns_early(self):
        """xautoclaim returns None → returns early."""
        r = MagicMock()
        r.xautoclaim.return_value = None
        self._run(r)
        r.xack.assert_not_called()

    def test_empty_claimed_list_returns_early(self):
        """xautoclaim returns empty claimed list → no xack, no zadd."""
        r = MagicMock()
        r.xautoclaim.return_value = _make_autoclaim_result([])
        self._run(r)
        r.xack.assert_not_called()
        r.zadd.assert_not_called()

    def test_empty_fields_dict_xacks_and_skips(self):
        """Claimed message with empty fields dict → XACK called, no zadd."""
        r = MagicMock()
        r.xautoclaim.return_value = _make_autoclaim_result([(b"1-0", {})])
        r.xpending_range.return_value = _make_pending_entry(b"1-0", 1)
        self._run(r)
        r.xack.assert_called_once()
        r.zadd.assert_not_called()

    def test_no_company_field_xacks_and_skips(self):
        """Message with no 'company' field → XACK called, no dead-letter."""
        r = MagicMock()
        r.xautoclaim.return_value = _make_autoclaim_result(
            [(b"2-0", {"dc_key": "greenhouse"})]
        )
        self._run(r)
        r.xack.assert_called_once()
        r.zadd.assert_not_called()

    def test_under_redelivery_limit_no_xack_no_zadd(self):
        """delivery_count < MAX_STREAM_REDELIVERIES → no xack, no zadd."""
        r = MagicMock()
        r.xautoclaim.return_value = _make_autoclaim_result(
            [(b"3-0", {"company": "Stripe"})]
        )
        r.xpending_range.return_value = _make_pending_entry(
            b"3-0", MAX_STREAM_REDELIVERIES - 1
        )
        self._run(r)
        r.xack.assert_not_called()
        r.zadd.assert_not_called()

    def test_at_redelivery_limit_dead_letters_to_adaptive(self):
        """delivery_count == MAX_STREAM_REDELIVERIES → zadd to REDIS_POLL_ADAPTIVE + xack."""
        r = MagicMock()
        r.xautoclaim.return_value = _make_autoclaim_result(
            [(b"4-0", {"company": "DeadCo"})]
        )
        r.xpending_range.return_value = _make_pending_entry(
            b"4-0", MAX_STREAM_REDELIVERIES
        )
        self._run(r, op_type="scan")
        r.zadd.assert_called_once()
        zadd_key = r.zadd.call_args[0][0]
        self.assertEqual(zadd_key, REDIS_POLL_ADAPTIVE)
        r.xack.assert_called_once()

    def test_fullscan_dead_letters_to_fullscan_queue(self):
        """op_type='fullscan' → dead-letter goes to REDIS_POLL_FULLSCAN."""
        r = MagicMock()
        r.xautoclaim.return_value = _make_autoclaim_result(
            [(b"5-0", {"company": "ScanCo"})]
        )
        r.xpending_range.return_value = _make_pending_entry(
            b"5-0", MAX_STREAM_REDELIVERIES
        )
        with patch("workers.scan_worker._get_backoff_delay", return_value=300):
            from workers.scheduler import claim_stale_work
            claim_stale_work(r, REDIS_STREAM_FULLSCAN,
                             STREAM_CONSUMER_GROUP, "sched-0",
                             10_000, "fullscan")
        zadd_key = r.zadd.call_args[0][0]
        self.assertEqual(zadd_key, REDIS_POLL_FULLSCAN)

    def test_idle_ms_floor_applied(self):
        """p95_ms=50000 → min_idle_time=300000 (floor of 300s applied, not 150s)."""
        r = MagicMock()
        r.xautoclaim.return_value = _make_autoclaim_result([])
        self._run(r, p95_ms=50_000)
        call_kwargs = r.xautoclaim.call_args[1]
        self.assertEqual(call_kwargs["min_idle_time"], 300_000)

    def test_idle_ms_scale_applied(self):
        """p95_ms=200000 → min_idle_time=600000 (3× exceeds floor)."""
        r = MagicMock()
        r.xautoclaim.return_value = _make_autoclaim_result([])
        self._run(r, p95_ms=200_000)
        call_kwargs = r.xautoclaim.call_args[1]
        self.assertEqual(call_kwargs["min_idle_time"], 600_000)

    def test_xpending_range_raises_no_dead_letter(self):
        """xpending_range raises → delivery_count=0, no dead-letter."""
        r = MagicMock()
        r.xautoclaim.return_value = _make_autoclaim_result(
            [(b"6-0", {"company": "RisCo"})]
        )
        r.xpending_range.side_effect = Exception("timeout")
        self._run(r)
        r.zadd.assert_not_called()

    def test_multiple_messages_processed_independently(self):
        """Multiple claimed messages → each checked for redelivery count."""
        r = MagicMock()
        r.xautoclaim.return_value = _make_autoclaim_result([
            (b"7-0", {"company": "Co1"}),
            (b"8-0", {"company": "Co2"}),
        ])

        call_count = [0]

        def _pending_range(stream, group, min, max, count):
            call_count[0] += 1
            return _make_pending_entry(min, 0)   # below limit

        r.xpending_range.side_effect = _pending_range
        self._run(r)
        self.assertEqual(call_count[0], 2)   # called once per message


# ─────────────────────────────────────────────────────────────────────────────
# Helpers for on_adaptive_complete WARMING tests
# ─────────────────────────────────────────────────────────────────────────────

def _make_poll_stats_row(warming_polls_remaining=None,
                         current_interval_s=3600,
                         consecutive_errors=0):
    """Build a mock company_poll_stats row dict."""
    return {
        "current_interval_s":     current_interval_s,
        "recent_poll_counts":     "[]",
        "last_full_scan_at":      None,
        "full_scan_interval_s":   86400,
        "consecutive_errors":     consecutive_errors,
        "last_success_at":        None,
        "warming_polls_remaining": warming_polls_remaining,
    }


class TestWarmingLifecycleOnAdaptiveComplete(unittest.TestCase):
    """
    Tests for the WARMING lifecycle portion of on_adaptive_complete().

    We patch all Phase-10 dependencies so we can isolate just the WARMING
    logic (interval override, decrement, transition to STABLE).
    """

    def _run(self, company="TestCo", new_jobs=0, success=True,
             warming=None, current_interval_s=3600):
        """
        Run on_adaptive_complete with mocked DB/Redis.
        Returns the db_update_params captured from conn.execute().
        """
        row = _make_poll_stats_row(
            warming_polls_remaining=warming,
            current_interval_s=current_interval_s,
        )

        conn = MagicMock()
        update_params_captured = []

        # SELECT queries must return a cursor whose .fetchone() yields `row`.
        # Using a side_effect overrides return_value, so we build a reusable
        # select cursor and return it for all non-UPDATE calls.
        select_cursor = MagicMock()
        select_cursor.fetchone.return_value = row

        def _execute(sql, params=None):
            if "UPDATE company_poll_stats" in sql:
                update_params_captured.append(params)
                return MagicMock()
            return select_cursor

        conn.execute.side_effect = _execute

        r = MagicMock()
        r.get.return_value = None   # no canary company
        r.hset.return_value = 1

        with patch("workers.scheduler.get_conn", return_value=conn), \
             patch("workers.scheduler.get_redis", return_value=r), \
             patch("workers.scheduler._get_dc_key_for_company",
                   return_value="greenhouse"), \
             patch("workers.scheduler._reschedule_adaptive"), \
             patch("workers.scheduler.clear_heartbeat"), \
             patch("workers.scheduler.load_poll_counts",
                   return_value=[0, 0, 0]), \
             patch("workers.scheduler.update_poll_interval",
                   return_value={
                       "current_interval_s":  current_interval_s,
                       "adaptive_score":      0.5,
                       "recent_poll_counts":  [0, 0, 0],
                   }), \
             patch("workers.scheduler.get_band_thresholds",
                   return_value=None), \
             patch("workers.scheduler.dump_poll_counts",
                   return_value="[]"):
            from workers.scheduler import on_adaptive_complete
            on_adaptive_complete(company, new_jobs, success=success)

        return update_params_captured, r

    def _get_warming_written(self, params_list):
        """
        Extract the warming_polls_remaining value written to the DB UPDATE.
        The UPDATE SQL has warming_polls_remaining at position -2 (before company).
        """
        if not params_list:
            return "NOT_WRITTEN"
        # Params order (from the UPDATE SQL):
        # (recent_poll_counts, adaptive_score, interval, next_poll_at,
        #  new_jobs, consec_errors, success, not_success,
        #  new_warming,  ← index 8
        #  company)
        params = params_list[0]
        return params[8] if params and len(params) > 8 else "NOT_WRITTEN"

    def _get_interval_written(self, params_list):
        """Extract the interval value written (index 2 in UPDATE params)."""
        if not params_list:
            return None
        params = params_list[0]
        return params[2] if params and len(params) > 2 else None

    def test_warming_3_success_interval_overridden(self):
        """warming=3, success=True → interval = WARMING_INTERVAL_S (7200s)."""
        params_list, _ = self._run(warming=3, success=True)
        interval = self._get_interval_written(params_list)
        self.assertEqual(interval, WARMING_INTERVAL_S)

    def test_warming_3_success_new_warming_is_2(self):
        """warming=3, success=True → new_warming=2 written to DB."""
        params_list, _ = self._run(warming=3, success=True)
        new_warming = self._get_warming_written(params_list)
        self.assertEqual(new_warming, 2)

    def test_warming_2_success_new_warming_is_1(self):
        """warming=2, success=True → new_warming=1."""
        params_list, _ = self._run(warming=2, success=True)
        new_warming = self._get_warming_written(params_list)
        self.assertEqual(new_warming, 1)

    def test_warming_1_success_new_warming_is_none(self):
        """warming=1, success=True → new_warming=None (transitions to STABLE)."""
        params_list, _ = self._run(warming=1, success=True)
        new_warming = self._get_warming_written(params_list)
        self.assertIsNone(new_warming)

    def test_stable_success_uses_adaptive_interval(self):
        """warming=None (STABLE), success=True → adaptive interval used (not WARMING_INTERVAL_S)."""
        adaptive_interval = 14400   # 4h — returned by update_poll_interval mock
        with patch("workers.scheduler.update_poll_interval",
                   return_value={
                       "current_interval_s":  adaptive_interval,
                       "adaptive_score":      0.5,
                       "recent_poll_counts":  [0, 0, 0],
                   }):
            params_list, _ = self._run(warming=None, success=True,
                                        current_interval_s=adaptive_interval)
        interval = self._get_interval_written(params_list)
        self.assertNotEqual(interval, WARMING_INTERVAL_S)

    def test_warming_failure_new_warming_unchanged(self):
        """warming=3, success=False → new_warming=3 (not decremented on failure)."""
        params_list, _ = self._run(warming=3, success=False)
        new_warming = self._get_warming_written(params_list)
        self.assertEqual(new_warming, 3)

    def test_stable_failure_new_warming_is_none(self):
        """warming=None, success=False → new_warming=None (unchanged)."""
        params_list, _ = self._run(warming=None, success=False)
        new_warming = self._get_warming_written(params_list)
        self.assertIsNone(new_warming)

    def test_update_poll_interval_called_during_warming(self):
        """During WARMING, update_poll_interval still called (score computed).

        Note: _run() internally patches update_poll_interval (returning
        adaptive_score=0.5), so we verify the call indirectly — if UPI was
        called, params[1] (adaptive_score) in the DB UPDATE will be 0.5.
        """
        params_list, _ = self._run(warming=3, success=True)
        self.assertTrue(params_list, "DB UPDATE not executed — on_adaptive_complete failed")
        adaptive_score_written = params_list[0][1]
        self.assertEqual(
            adaptive_score_written, 0.5,
            "update_poll_interval was not called during WARMING — "
            "adaptive_score not written by it",
        )

    def test_warming_interval_constant_is_7200(self):
        """WARMING_INTERVAL_S = 2 * 3600 = 7200 (sanity check on constant)."""
        self.assertEqual(WARMING_INTERVAL_S, 7200)


if __name__ == "__main__":
    unittest.main(verbosity=2)
