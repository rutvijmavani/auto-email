"""
tests/test_scheduler_streams.py
─────────────────────────────────────────────────────────────────────────────
Tests for the stream-based additions to workers/scheduler.py:
    · _init_consumer_group(r, stream_key, group)
    · _get_stream_pending_count(r, stream_key, group)
    · claim_stale_work(r, stream_key, group, op_type)
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
# TestClaimStaleWork — owner-state reaper
# ─────────────────────────────────────────────────────────────────────────────

class _ReapRedis:
    """Minimal Redis double for claim_stale_work: PEL, stream, heartbeats, cur keys."""

    def __init__(self, pel, fields=None, alive=(), cur=None):
        self.pel = pel                      # [(msg_id, consumer)]
        self.fields = fields or {}          # msg_id -> stream fields
        self.alive = set(alive)             # heartbeat keys that exist
        self.cur = cur or {}                # consumer -> {"msg_id", "alive_until"}
        self.acked, self.zadds, self.zrems, self.deleted = [], [], [], []
        self.counters = {}

    def xpending_range(self, stream, group, min="-", max="+", count=100):
        return [{"message_id": m, "consumer": c, "time_since_delivered": 0,
                 "times_delivered": 1} for m, c in self.pel]

    def exists(self, key):
        return key in self.alive

    def get(self, key):
        import json
        cons = key[len("worker:cur:"):] if key.startswith("worker:cur:") else None
        return json.dumps(self.cur[cons]) if cons in self.cur else None

    def xrange(self, stream, min, max, count=1):
        return [(min, self.fields[min])] if min in self.fields else []

    def xack(self, stream, group, msg_id):
        self.acked.append(msg_id)
        self.pel = [(m, c) for m, c in self.pel if m != msg_id]

    def incr(self, key):
        self.counters[key] = self.counters.get(key, 0) + 1
        return self.counters[key]

    def expire(self, *a):
        pass

    def zadd(self, key, mapping, **kw):
        self.zadds.append((key, mapping, kw))

    def zrem(self, key, member):
        self.zrems.append((key, member))

    def delete(self, key):
        self.deleted.append(key)


class TestClaimStaleWork(unittest.TestCase):

    HOST, PID = "vm1", 4242
    CONSUMER = "worker-vm1-4242"
    HB = "worker:alive:scan_worker:vm1:4242"

    def setUp(self):
        import workers.scheduler as sch
        sch._reap_seen.clear()
        self.sch = sch

    def _run(self, r, stream=REDIS_STREAM_ADAPTIVE, op="scan", times=2):
        for _ in range(times):
            self.sch.claim_stale_work(r, stream, STREAM_CONSUMER_GROUP, op)

    def _redis(self, **kw):
        return _ReapRedis([("1-0", self.CONSUMER)],
                          fields={"1-0": {"company": "Stripe", "dc_key": "greenhouse"}}, **kw)

    def test_running_owner_is_left_alone(self):
        r = self._redis(alive=[self.HB],
                        cur={self.CONSUMER: {"msg_id": "1-0", "alive_until": time.time() + 500}})
        self._run(r, times=5)
        self.assertEqual(r.acked, [])
        self.assertEqual(r.zadds, [])

    def test_dead_owner_reaped_on_second_sighting_only(self):
        r = self._redis()                       # no heartbeat key → DEAD
        self._run(r, times=1)
        self.assertEqual(r.acked, [])
        self._run(r, times=1)
        self.assertEqual(r.acked, ["1-0"])

    def test_reap_reschedules_with_backoff_gt_and_releases_slot(self):
        r = self._redis()
        self._run(r)
        key, mapping, kw = r.zadds[0]
        self.assertEqual(key, REDIS_POLL_ADAPTIVE)
        self.assertGreater(mapping["Stripe"], time.time())
        self.assertTrue(kw.get("gt"))
        self.assertIn(("inflight:scans:greenhouse", "Stripe"), r.zrems)
        self.assertIn("inflight:msg:scan:Stripe", r.deleted)

    def test_abandoned_owner_reaped(self):
        r = self._redis(alive=[self.HB],
                        cur={self.CONSUMER: {"msg_id": "9-9", "alive_until": time.time() + 500}})
        self._run(r)
        self.assertEqual(r.acked, ["1-0"])

    def test_owner_without_cur_report_reaped(self):
        r = self._redis(alive=[self.HB])
        self._run(r)
        self.assertEqual(r.acked, ["1-0"])

    def test_stalled_owner_killed_then_reaped(self):
        r = self._redis(alive=[self.HB],
                        cur={self.CONSUMER: {"msg_id": "1-0", "alive_until": time.time() - 5}})
        with patch("socket.gethostname", return_value=self.HOST), \
                patch("os.kill") as kill:
            self._run(r)
        kill.assert_called_once()
        self.assertEqual(kill.call_args[0][0], self.PID)
        self.assertEqual(r.acked, ["1-0"])

    def test_state_change_between_sightings_restarts_confirmation(self):
        r = self._redis()                       # DEAD on first sighting
        self._run(r, times=1)
        r.alive.add(self.HB)                    # now alive, no report → ABANDONED
        self._run(r, times=1)
        self.assertEqual(r.acked, [])
        self._run(r, times=1)
        self.assertEqual(r.acked, ["1-0"])

    def test_entry_without_company_acked_without_reschedule(self):
        r = _ReapRedis([("2-0", self.CONSUMER)], fields={"2-0": {"dc_key": "x"}})
        self._run(r)
        self.assertEqual(r.acked, ["2-0"])
        self.assertEqual(r.zadds, [])

    def test_trimmed_entry_acked_without_reschedule(self):
        r = _ReapRedis([("3-0", self.CONSUMER)])
        self._run(r)
        self.assertEqual(r.acked, ["3-0"])
        self.assertEqual(r.zadds, [])

    def test_repeat_reaps_back_off_longer(self):
        r = self._redis()
        delays = []
        for _ in range(3):
            r.pel = [("1-0", self.CONSUMER)]
            self.sch._reap_seen.clear()
            before = len(r.zadds)
            self._run(r)
            delays.append(r.zadds[before][1]["Stripe"] - time.time())
        self.assertLess(delays[0], delays[1])
        self.assertLess(delays[1], delays[2])

    def test_fullscan_uses_fullscan_queue_and_slots(self):
        r = _ReapRedis([("4-0", "worker-vm1-4242")],
                       fields={"4-0": {"company": "ScanCo", "dc_key": "workday_a"}})
        self._run(r, stream=REDIS_STREAM_FULLSCAN, op="fullscan")
        self.assertEqual(r.zadds[0][0], REDIS_POLL_FULLSCAN)
        self.assertIn(("inflight:fullscans:workday_a", "ScanCo"), r.zrems)
        self.assertIn(("inflight:fullscan", "ScanCo"), r.zrems)

    def test_xpending_error_returns_quietly(self):
        r = MagicMock()
        r.xpending_range.side_effect = Exception("NOGROUP")
        self.sch.claim_stale_work(r, REDIS_STREAM_ADAPTIVE, STREAM_CONSUMER_GROUP)

    def test_clear_reaped_deletes_counter(self):
        r = MagicMock()
        self.sch.clear_reaped(r, "scan", "Stripe")
        r.delete.assert_called_once_with("reaped:scan:Stripe")

    def test_split_consumer(self):
        self.assertEqual(self.sch._split_consumer("worker-my-host-12"), ("my-host", 12))
        self.assertEqual(self.sch._split_consumer("scheduler-h-1"), (None, None))


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
        "avg_scan_duration_s":    None,
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
