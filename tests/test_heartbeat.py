"""
tests/test_heartbeat.py
─────────────────────────────────────────────────────────────────────────────
Tests for workers/heartbeat.py — Heartbeat daemon thread.

Phase 3.2 — Worker heartbeat in Redis.
Phase 3.3 — Per-PID key format (multi-worker architecture).

Each worker instance writes its own key: worker:alive:{worker_type}:{hostname}:{pid}
so multiple workers of the same type (even on different hosts) do not overwrite each other's heartbeat.

Coverage map
────────────
  TestHeartbeatClass
    · start() writes the heartbeat key immediately (synchronous, before thread)
    · Key format is worker:alive:{worker_type}:{hostname}:{pid}
    · TTL = 3 x interval_s
    · Payload JSON contains pid, ts, processed fields
    · pid field matches os.getpid()
    · processed field comes from get_count lambda
    · Redis failure in _write() is swallowed — no exception raised
    · Thread is daemon=True (dies with process)
    · Default interval_s = 10
    · fullscan_worker with interval_s=60 gets TTL=180
    · stop() sets the internal _stop event
    · get_count lambda is called on every write
"""

import json
import os
import sys
import time
import inspect
import unittest
from unittest.mock import MagicMock, patch

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))


class TestHeartbeatClass(unittest.TestCase):
    """Heartbeat daemon thread: Redis key writing, TTL, payload, lifecycle."""

    def _make_r(self):
        return MagicMock()

    # ── Immediate first write ─────────────────────────────────────────────────

    def test_start_writes_key_immediately(self):
        """start() writes the heartbeat key synchronously before the thread starts."""
        r = self._make_r()
        from workers.heartbeat import Heartbeat
        hb = Heartbeat(r, "scan_worker", lambda: 0, interval_s=10)
        hb.start()
        hb.stop()
        self.assertGreaterEqual(r.set.call_count, 1,
                                "Expected at least one r.set() call from start()")

    # ── Key format ────────────────────────────────────────────────────────────

    def test_key_format_is_worker_alive_type_pid(self):
        """Written key is 'worker:alive:{worker_type}:{hostname}:{pid}'."""
        import socket
        r = self._make_r()
        from workers.heartbeat import Heartbeat
        hb = Heartbeat(r, "scan_worker", lambda: 0, interval_s=10)
        hb.start()
        hb.stop()
        key = r.set.call_args[0][0]
        self.assertEqual(key, f"worker:alive:scan_worker:{socket.gethostname()}:{os.getpid()}")

    def test_fullscan_worker_key_format(self):
        """Key uses the exact worker_type string and hostname:pid suffix."""
        import socket
        r = self._make_r()
        from workers.heartbeat import Heartbeat
        hb = Heartbeat(r, "fullscan_worker", lambda: 0, interval_s=60)
        hb.start()
        hb.stop()
        key = r.set.call_args[0][0]
        self.assertEqual(key, f"worker:alive:fullscan_worker:{socket.gethostname()}:{os.getpid()}")

    def test_two_workers_same_type_write_different_keys(self):
        """
        Simulate two workers of the same type: each uses its own PID as the
        key suffix, so they never overwrite each other's heartbeat.

        In production they run in separate processes; here we verify the key
        template includes os.getpid() so different processes get different keys.
        """
        r1 = self._make_r()
        r2 = self._make_r()
        from workers.heartbeat import Heartbeat
        # Both use "scan_worker" but in separate processes they'd have different PIDs.
        # We can only verify the key ends with the current PID; the multi-process
        # guarantee is by construction (os.getpid() differs per process).
        hb1 = Heartbeat(r1, "scan_worker", lambda: 0, interval_s=10)
        hb1.start()
        hb1.stop()
        key1 = r1.set.call_args[0][0]
        self.assertIn(str(os.getpid()), key1,
                      "Key must embed the current PID so concurrent workers don't collide")

        hb2 = Heartbeat(r2, "scan_worker", lambda: 0, interval_s=10)
        hb2.start()
        hb2.stop()
        key2 = r2.set.call_args[0][0]
        self.assertIn(str(os.getpid()), key2,
                      "Second worker key must also embed the PID")
        # Both in the same process → same key format (per-PID multi-process isolation
        # is by construction; different processes get different PIDs → different keys).
        self.assertEqual(key1, key2)

    # ── TTL ───────────────────────────────────────────────────────────────────

    def test_ttl_is_3x_interval_s(self):
        """TTL (ex=) is 3 x interval_s."""
        r = self._make_r()
        from workers.heartbeat import Heartbeat
        interval = 15
        hb = Heartbeat(r, "scan_worker", lambda: 0, interval_s=interval)
        hb.start()
        hb.stop()
        kwargs = r.set.call_args[1]
        self.assertEqual(kwargs.get("ex"), interval * 3)

    def test_fullscan_worker_ttl_is_180(self):
        """fullscan_worker with interval_s=60 gets TTL=180."""
        r = self._make_r()
        from workers.heartbeat import Heartbeat
        hb = Heartbeat(r, "fullscan_worker", lambda: 0, interval_s=60)
        hb.start()
        hb.stop()
        kwargs = r.set.call_args[1]
        self.assertEqual(kwargs.get("ex"), 180)

    def test_default_interval_gives_30s_ttl(self):
        """Default interval_s=10 → TTL=30."""
        r = self._make_r()
        from workers.heartbeat import Heartbeat
        hb = Heartbeat(r, "scan_worker", lambda: 0)  # use default interval
        hb.start()
        hb.stop()
        kwargs = r.set.call_args[1]
        self.assertEqual(kwargs.get("ex"), 30)

    # ── Payload ───────────────────────────────────────────────────────────────

    def test_payload_is_valid_json(self):
        """Written value is valid JSON."""
        r = self._make_r()
        from workers.heartbeat import Heartbeat
        hb = Heartbeat(r, "scan_worker", lambda: 0, interval_s=10)
        hb.start()
        hb.stop()
        value = r.set.call_args[0][1]
        data = json.loads(value)
        self.assertIsInstance(data, dict)

    def test_payload_contains_pid_ts_processed(self):
        """Payload contains pid, ts, and processed fields."""
        r = self._make_r()
        from workers.heartbeat import Heartbeat
        hb = Heartbeat(r, "scan_worker", lambda: 0, interval_s=10)
        hb.start()
        hb.stop()
        data = json.loads(r.set.call_args[0][1])
        for field in ("pid", "ts", "processed"):
            self.assertIn(field, data, f"Missing field: {field}")

    def test_payload_pid_matches_os_getpid(self):
        """pid field matches os.getpid()."""
        r = self._make_r()
        from workers.heartbeat import Heartbeat
        hb = Heartbeat(r, "scan_worker", lambda: 0, interval_s=10)
        hb.start()
        hb.stop()
        data = json.loads(r.set.call_args[0][1])
        self.assertEqual(data["pid"], os.getpid())

    def test_payload_processed_from_get_count(self):
        """processed field is the return value of the get_count lambda."""
        r = self._make_r()
        from workers.heartbeat import Heartbeat
        hb = Heartbeat(r, "scan_worker", lambda: 77, interval_s=10)
        hb.start()
        hb.stop()
        data = json.loads(r.set.call_args[0][1])
        self.assertEqual(data["processed"], 77)

    def test_get_count_updated_between_writes(self):
        """get_count is called fresh on every write, reflecting current count."""
        r = self._make_r()
        counter = [0]

        def _get():
            counter[0] += 1
            return counter[0]

        from workers.heartbeat import Heartbeat
        hb = Heartbeat(r, "scan_worker", _get, interval_s=10)
        hb._write()  # first write
        hb._write()  # second write
        hb.stop()

        # Two writes → two increments → processed values differ
        calls = r.set.call_args_list
        values = [json.loads(c[0][1])["processed"] for c in calls]
        self.assertNotEqual(values[0], values[1],
                            "processed should update between writes")

    # ── Fault tolerance ───────────────────────────────────────────────────────

    def test_redis_failure_is_swallowed(self):
        """Redis failures in _write() must not propagate to the caller."""
        r = self._make_r()
        r.set.side_effect = ConnectionError("Redis down")
        from workers.heartbeat import Heartbeat
        hb = Heartbeat(r, "scan_worker", lambda: 0, interval_s=10)
        hb.start()
        hb.stop()

    def test_write_exception_does_not_kill_thread(self):
        """Even if _write() raises, calling it again works (swallows each time)."""
        r = self._make_r()
        r.set.side_effect = RuntimeError("unexpected")
        from workers.heartbeat import Heartbeat
        hb = Heartbeat(r, "detail_worker", lambda: 0, interval_s=10)
        try:
            hb._write()
            hb._write()  # must not raise either
        except RuntimeError:
            self.fail("_write() must swallow all exceptions")

    # ── Thread properties ─────────────────────────────────────────────────────

    def test_thread_is_daemon(self):
        """Thread must be daemon=True so it dies with the main process."""
        r = self._make_r()
        from workers.heartbeat import Heartbeat
        hb = Heartbeat(r, "scan_worker", lambda: 0, interval_s=10)
        self.assertTrue(hb._thread.daemon,
                        "Heartbeat thread must be daemon=True")

    # ── Default interval_s ────────────────────────────────────────────────────

    def test_interval_s_default_is_10(self):
        """Default interval_s parameter is 10."""
        from workers.heartbeat import Heartbeat
        sig = inspect.signature(Heartbeat.__init__)
        default = sig.parameters["interval_s"].default
        self.assertEqual(default, 10)

    # ── Stop lifecycle ────────────────────────────────────────────────────────

    def test_stop_sets_stop_event(self):
        """stop() sets the internal _stop threading.Event."""
        r = self._make_r()
        from workers.heartbeat import Heartbeat
        hb = Heartbeat(r, "scan_worker", lambda: 0, interval_s=10)
        hb.start()
        self.assertFalse(hb._stop.is_set(),
                         "_stop should not be set before stop()")
        hb.stop()
        self.assertTrue(hb._stop.is_set(),
                        "_stop should be set after stop()")

    def test_start_returns_self_for_chaining(self):
        """start() returns self to support chaining: hb = Heartbeat(...).start()."""
        r = self._make_r()
        from workers.heartbeat import Heartbeat
        hb = Heartbeat(r, "scan_worker", lambda: 0, interval_s=10)
        result = hb.start()
        hb.stop()
        self.assertIs(result, hb)


if __name__ == "__main__":
    unittest.main(verbosity=2)
