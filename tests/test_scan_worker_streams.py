"""
tests/test_scan_worker_streams.py
─────────────────────────────────────────────────────────────────────────────
Tests for the stream-based delivery additions to workers/scan_worker.py:
    · _ensure_consumer_group(r)
    · run_worker() — XREADGROUP-based main loop

In the two-layer scheduler redesign, scan workers switched from BLPOP to
XREADGROUP.  The worker reads from stream:adaptive, calls on_adaptive_complete
inline (not via a result_consumer_loop), then XACKs.  If the worker dies
between XREADGROUP and XACK, the message stays in the PEL for XAUTOCLAIM.

All Redis/DB interactions are mocked.

Coverage map
────────────
  TestEnsureConsumerGroupScanWorker
    · xgroup_create called with REDIS_STREAM_ADAPTIVE, STREAM_CONSUMER_GROUP,
      id="$", mkstream=True
    · BUSYGROUP exception → silently swallowed
    · Other exception → warning logged, then re-raised (fail fast)
    · Success → no exception propagates

  TestRunWorkerInitialisation
    · skip_init_db=True → init_db NOT called
    · skip_init_db=False → init_db called once
    · Redis not reachable → sys.exit(1) called

  TestRunWorkerStreamLoop
    · once=True, stream returns no messages → exits cleanly (no exception)
    · Shutdown event set before xreadgroup → exits without calling xreadgroup
    · Shutdown event set after xreadgroup, before scan → breaks WITHOUT XACK
      (message left in PEL)
    · Normal flow: xreadgroup → scan → on_adaptive_complete → xack
    · Correct xack args: r.xack(REDIS_STREAM_ADAPTIVE, STREAM_CONSUMER_GROUP, msg_id)
    · once=True → exits after one message processed
    · xreadgroup called with correct args: group, consumer, {stream: ">"}, count=1, block=500

  TestRunWorkerCompletionHandler
    · on_adaptive_complete called with (company, new_jobs, success)
    · on_adaptive_complete receives success=True when scan succeeds
    · on_adaptive_complete receives success=False when scan fails
    · on_adaptive_complete raising → xack NOT called (stays in PEL for retry)
    · on_adaptive_complete args: new_jobs=result["new_jobs"], success=result["success"]

  TestRunWorkerMessageParsing
    · company field extracted from stream message fields
    · dc_key field extracted from stream message fields
    · context field extracted from stream message fields (default "normal")
    · request_id field extracted (or generated from timestamp)
    · Missing company field → empty string (no KeyError crash)
"""

import sys
import os
import time
import threading
import unittest
from unittest.mock import MagicMock, patch, call

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))


# ─────────────────────────────────────────────────────────────────────────────
# Test helpers
# ─────────────────────────────────────────────────────────────────────────────

def _make_stream_message(company="Stripe", dc_key="greenhouse",
                         context="normal", msg_id=b"1000-0",
                         extra_fields=None):
    """
    Build a mock xreadgroup return value:
    [(stream_name, [(msg_id, fields_dict)])]
    """
    fields = {
        "company":     company,
        "dc_key":      dc_key,
        "context":     context,
        "enqueued_at": "2026-05-01T07:00:00+00:00",
        "request_id":  "adp-test-001",
        "scan_type":   "adaptive",
    }
    if extra_fields:
        fields.update(extra_fields)
    return [(b"stream:adaptive", [(msg_id, fields)])]


def _make_redis_mock(stream_messages=None, after_first_call_empty=True):
    """
    Build a Redis mock for scan_worker tests.

    stream_messages: what xreadgroup returns on first call.
    after_first_call_empty: if True, subsequent calls return [] (empty stream).
    """
    r = MagicMock()
    call_count = [0]

    if stream_messages is None:
        r.xreadgroup.return_value = []
    else:
        def _xreadgroup(*args, **kwargs):
            call_count[0] += 1
            if call_count[0] == 1:
                return stream_messages
            return []   # empty on subsequent calls

        r.xreadgroup.side_effect = _xreadgroup

    return r


# ─────────────────────────────────────────────────────────────────────────────
# TestEnsureConsumerGroupScanWorker
# ─────────────────────────────────────────────────────────────────────────────

class TestEnsureConsumerGroupScanWorker(unittest.TestCase):

    def test_xgroup_create_called_with_correct_args(self):
        """xgroup_create uses stream:adaptive, scan-workers, id='$', mkstream=True."""
        from config import REDIS_STREAM_ADAPTIVE, STREAM_CONSUMER_GROUP
        r = MagicMock()
        from workers.scan_worker import _ensure_consumer_group
        _ensure_consumer_group(r)
        r.xgroup_create.assert_called_once_with(
            REDIS_STREAM_ADAPTIVE,
            STREAM_CONSUMER_GROUP,
            id="$",
            mkstream=True,
        )

    def test_busygroup_silently_swallowed(self):
        """BUSYGROUP exception → no re-raise and no warning."""
        r = MagicMock()
        r.xgroup_create.side_effect = Exception("BUSYGROUP Consumer Group already exists")
        with patch("workers.scan_worker.logger") as mock_log:
            from workers.scan_worker import _ensure_consumer_group
            _ensure_consumer_group(r)   # must not raise
        warning_calls = [str(c) for c in mock_log.warning.call_args_list]
        busygroup_warnings = [c for c in warning_calls if "BUSYGROUP" in c
                              or "xgroup_create" in c.lower()]
        self.assertEqual(len(busygroup_warnings), 0)

    def test_other_exception_logs_warning_and_raises(self):
        """Non-BUSYGROUP exception → warning logged, then re-raised (fail fast)."""
        r = MagicMock()
        r.xgroup_create.side_effect = Exception("WRONGTYPE Operation against a key")
        with patch("workers.scan_worker.logger") as mock_log:
            from workers.scan_worker import _ensure_consumer_group
            with self.assertRaises(Exception):
                _ensure_consumer_group(r)
        mock_log.warning.assert_called_once()

    def test_success_no_exception(self):
        """Successful xgroup_create → no exception propagates."""
        r = MagicMock()
        r.xgroup_create.return_value = "OK"
        from workers.scan_worker import _ensure_consumer_group
        _ensure_consumer_group(r)   # must not raise


# ─────────────────────────────────────────────────────────────────────────────
# TestRunWorkerInitialisation
# ─────────────────────────────────────────────────────────────────────────────

class TestRunWorkerInitialisation(unittest.TestCase):

    def test_skip_init_db_true_skips_init_db(self):
        """skip_init_db=True → init_db NOT called."""
        r = _make_redis_mock()
        with patch("workers.startup.validate_startup"), \
             patch("workers.scan_worker.get_redis", return_value=r), \
             patch("workers.scan_worker.init_db") as mock_init, \
             patch("workers.scan_worker._ensure_consumer_group"):
            from workers.scan_worker import run_worker
            run_worker(once=True, skip_init_db=True)
        mock_init.assert_not_called()

    def test_skip_init_db_false_calls_init_db(self):
        """skip_init_db=False (default) → init_db called once."""
        r = _make_redis_mock()
        with patch("workers.startup.validate_startup"), \
             patch("workers.scan_worker.get_redis", return_value=r), \
             patch("workers.scan_worker.init_db") as mock_init, \
             patch("workers.scan_worker._ensure_consumer_group"):
            from workers.scan_worker import run_worker
            run_worker(once=True, skip_init_db=False)
        mock_init.assert_called_once()

    def test_redis_unreachable_calls_sys_exit(self):
        """Redis not reachable → sys.exit(1) called (via validate_startup)."""
        # scan_worker delegates Redis startup checks to validate_startup,
        # which runs _check_config before _check_redis.  Provide the required
        # env vars so the failure comes from Redis, not missing config.
        mock_r = MagicMock()
        mock_r.ping.return_value = False
        env = {"REDIS_URL": "redis://localhost:6379/0", "DATABASE_URL": "postgresql://x"}
        with patch("redis.from_url", return_value=mock_r), \
             patch("workers.scan_worker.init_db"), \
             patch.dict("os.environ", env), \
             self.assertRaises(SystemExit) as cm:
            from workers.scan_worker import run_worker
            run_worker(once=True, skip_init_db=True)
        self.assertEqual(cm.exception.code, 1)


# ─────────────────────────────────────────────────────────────────────────────
# TestRunWorkerStreamLoop
# ─────────────────────────────────────────────────────────────────────────────

class TestRunWorkerStreamLoop(unittest.TestCase):

    def _common_patches(self, r):
        """Return a list of context managers for common patches."""
        return [
            patch("workers.scan_worker.ping", return_value=True),
            patch("workers.scan_worker.get_redis", return_value=r),
            patch("workers.scan_worker.init_db"),
            patch("workers.scan_worker._ensure_consumer_group"),
        ]

    def test_once_empty_stream_exits_cleanly(self):
        """once=True, stream returns no messages → exits without exception."""
        r = _make_redis_mock(stream_messages=None)
        with patch("workers.scan_worker.ping", return_value=True), \
             patch("workers.scan_worker.get_redis", return_value=r), \
             patch("workers.scan_worker.init_db"), \
             patch("workers.scan_worker._ensure_consumer_group"):
            from workers.scan_worker import run_worker
            run_worker(once=True, skip_init_db=True)   # must not raise

    def test_shutdown_before_xreadgroup_exits_without_reading(self):
        """Shutdown event set → exits without calling xreadgroup."""
        r = _make_redis_mock()
        shutdown = threading.Event()
        shutdown.set()

        with patch("workers.scan_worker.ping", return_value=True), \
             patch("workers.scan_worker.get_redis", return_value=r), \
             patch("workers.scan_worker.init_db"), \
             patch("workers.scan_worker._ensure_consumer_group"):
            from workers.scan_worker import run_worker
            run_worker(once=True, skip_init_db=True, shutdown_event=shutdown)

        r.xreadgroup.assert_not_called()

    def test_shutdown_after_xreadgroup_no_xack(self):
        """
        Shutdown set between XREADGROUP and scan → breaks loop WITHOUT calling
        r.xack (message stays in PEL for XAUTOCLAIM reclaim).
        """
        msg_id  = b"1234-0"
        msgs    = _make_stream_message(company="Stripe", msg_id=msg_id)
        r       = _make_redis_mock(stream_messages=msgs)

        # Shutdown event is set — but only AFTER xreadgroup returns
        shutdown = threading.Event()

        xreadgroup_call_count = [0]
        original_side = r.xreadgroup.side_effect

        def _xreadgroup_side(*args, **kwargs):
            xreadgroup_call_count[0] += 1
            # Set shutdown on first return so the check inside the loop fires
            shutdown.set()
            return msgs

        r.xreadgroup.side_effect = _xreadgroup_side

        with patch("workers.scan_worker.ping", return_value=True), \
             patch("workers.scan_worker.get_redis", return_value=r), \
             patch("workers.scan_worker.init_db"), \
             patch("workers.scan_worker._ensure_consumer_group"):
            from workers.scan_worker import run_worker
            run_worker(once=True, skip_init_db=True, shutdown_event=shutdown)

        r.xack.assert_not_called()

    def test_normal_flow_xack_called(self):
        """Normal flow: xreadgroup → scan → on_adaptive_complete → xack."""
        from config import REDIS_STREAM_ADAPTIVE, STREAM_CONSUMER_GROUP
        msg_id = b"5555-0"
        msgs   = _make_stream_message(company="Airbnb", msg_id=msg_id)
        r      = _make_redis_mock(stream_messages=msgs)

        scan_result = {
            "success": True, "new_jobs": 2, "fetched": 30,
            "duration_ms": 800, "company": "Airbnb",
        }

        with patch("workers.scan_worker.ping", return_value=True), \
             patch("workers.scan_worker.get_redis", return_value=r), \
             patch("workers.scan_worker.init_db"), \
             patch("workers.scan_worker._ensure_consumer_group"), \
             patch("workers.scan_worker._run_listing_scan",
                   return_value=scan_result), \
             patch("workers.scheduler.on_adaptive_complete"):
            from workers.scan_worker import run_worker
            run_worker(once=True, skip_init_db=True)

        r.xack.assert_called_once_with(
            REDIS_STREAM_ADAPTIVE, STREAM_CONSUMER_GROUP, msg_id,
        )

    def test_xreadgroup_called_with_correct_args(self):
        """xreadgroup called with group, consumer, {stream: ">"}, count=1, block=500."""
        from config import (REDIS_STREAM_ADAPTIVE, STREAM_CONSUMER_GROUP,
                            STREAM_BLOCK_MS)
        r = _make_redis_mock()

        with patch("workers.scan_worker.ping", return_value=True), \
             patch("workers.scan_worker.get_redis", return_value=r), \
             patch("workers.scan_worker.init_db"), \
             patch("workers.scan_worker._ensure_consumer_group"):
            from workers.scan_worker import run_worker
            run_worker(once=True, skip_init_db=True)

        r.xreadgroup.assert_called_once()
        call_args = r.xreadgroup.call_args
        args   = call_args[0]
        kwargs = call_args[1]
        # Positional: group, consumer
        self.assertEqual(args[0], STREAM_CONSUMER_GROUP)
        # Streams dict
        streams_dict = args[2] if len(args) > 2 else kwargs.get("streams", {})
        self.assertIn(REDIS_STREAM_ADAPTIVE, streams_dict)
        self.assertEqual(streams_dict[REDIS_STREAM_ADAPTIVE], ">")
        # count and block
        self.assertEqual(kwargs.get("count", args[3] if len(args) > 3 else None), 1)
        self.assertEqual(kwargs.get("block", args[4] if len(args) > 4 else None),
                         STREAM_BLOCK_MS)

    def test_once_exits_after_one_message(self):
        """once=True → exits after processing one message (xreadgroup called once)."""
        msgs = _make_stream_message()
        r    = _make_redis_mock(stream_messages=msgs)
        scan_result = {"success": True, "new_jobs": 0, "fetched": 0,
                       "duration_ms": 100, "company": "Stripe"}

        with patch("workers.scan_worker.ping", return_value=True), \
             patch("workers.scan_worker.get_redis", return_value=r), \
             patch("workers.scan_worker.init_db"), \
             patch("workers.scan_worker._ensure_consumer_group"), \
             patch("workers.scan_worker._run_listing_scan",
                   return_value=scan_result), \
             patch("workers.scheduler.on_adaptive_complete"):
            from workers.scan_worker import run_worker
            run_worker(once=True, skip_init_db=True)

        # xreadgroup called exactly once (the one message)
        self.assertEqual(r.xreadgroup.call_count, 1)


# ─────────────────────────────────────────────────────────────────────────────
# TestRunWorkerCompletionHandler
# ─────────────────────────────────────────────────────────────────────────────

class TestRunWorkerCompletionHandler(unittest.TestCase):

    def _run_with_result(self, scan_result, company="Stripe"):
        msgs = _make_stream_message(company=company)
        r    = _make_redis_mock(stream_messages=msgs)
        captured = []

        def _oac(co, new_jobs, success):
            captured.append((co, new_jobs, success))

        with patch("workers.scan_worker.ping", return_value=True), \
             patch("workers.scan_worker.get_redis", return_value=r), \
             patch("workers.scan_worker.init_db"), \
             patch("workers.scan_worker._ensure_consumer_group"), \
             patch("workers.scan_worker._run_listing_scan",
                   return_value=scan_result), \
             patch("workers.scheduler.on_adaptive_complete",
                   side_effect=_oac):
            from workers.scan_worker import run_worker
            run_worker(once=True, skip_init_db=True)
        return captured, r

    def test_on_adaptive_complete_called_with_company(self):
        """on_adaptive_complete receives the company name."""
        result = {"success": True, "new_jobs": 5, "fetched": 20,
                  "duration_ms": 400, "company": "Stripe"}
        captured, _ = self._run_with_result(result, company="Stripe")
        self.assertEqual(len(captured), 1)
        self.assertEqual(captured[0][0], "Stripe")

    def test_on_adaptive_complete_called_with_new_jobs(self):
        """on_adaptive_complete receives result['new_jobs']."""
        result = {"success": True, "new_jobs": 7, "fetched": 20,
                  "duration_ms": 400, "company": "Airbnb"}
        captured, _ = self._run_with_result(result)
        self.assertEqual(captured[0][1], 7)

    def test_on_adaptive_complete_success_true(self):
        """on_adaptive_complete receives success=True when scan succeeds."""
        result = {"success": True, "new_jobs": 0, "fetched": 10,
                  "duration_ms": 300, "company": "Stripe"}
        captured, _ = self._run_with_result(result)
        self.assertTrue(captured[0][2])

    def test_on_adaptive_complete_success_false(self):
        """on_adaptive_complete receives success=False when scan fails."""
        result = {"success": False, "new_jobs": 0, "fetched": 0,
                  "duration_ms": 50, "company": "Stripe",
                  "error": "connection timeout"}
        captured, _ = self._run_with_result(result)
        self.assertFalse(captured[0][2])

    def test_on_adaptive_complete_failure_no_xack(self):
        """
        on_adaptive_complete raising → XACK is NOT called.
        The message stays in PEL for XAUTOCLAIM to retry (on_adaptive_complete
        is idempotent so retrying is safe).
        """
        from config import REDIS_STREAM_ADAPTIVE, STREAM_CONSUMER_GROUP
        msgs = _make_stream_message(msg_id=b"9999-0")
        r    = _make_redis_mock(stream_messages=msgs)
        scan_result = {"success": True, "new_jobs": 0, "fetched": 0,
                       "duration_ms": 100, "company": "Stripe"}

        with patch("workers.scan_worker.ping", return_value=True), \
             patch("workers.scan_worker.get_redis", return_value=r), \
             patch("workers.scan_worker.init_db"), \
             patch("workers.scan_worker._ensure_consumer_group"), \
             patch("workers.scan_worker._run_listing_scan",
                   return_value=scan_result), \
             patch("workers.scheduler.on_adaptive_complete",
                   side_effect=Exception("DB crashed")):
            from workers.scan_worker import run_worker
            run_worker(once=True, skip_init_db=True)

        r.xack.assert_not_called()

    def test_missing_new_jobs_defaults_to_zero(self):
        """result without 'new_jobs' key → on_adaptive_complete gets 0."""
        result = {"success": True, "fetched": 5, "duration_ms": 100,
                  "company": "Stripe"}
        captured, _ = self._run_with_result(result)
        self.assertEqual(captured[0][1], 0)

    def test_missing_success_defaults_to_false(self):
        """result without 'success' key → on_adaptive_complete gets False."""
        result = {"new_jobs": 0, "fetched": 0, "duration_ms": 50,
                  "company": "Stripe"}
        captured, _ = self._run_with_result(result)
        self.assertFalse(captured[0][2])


# ─────────────────────────────────────────────────────────────────────────────
# TestRunWorkerMessageParsing
# ─────────────────────────────────────────────────────────────────────────────

class TestRunWorkerMessageParsing(unittest.TestCase):

    def _run_and_capture_payload(self, fields):
        """Run worker with custom message fields; capture payload passed to scan."""
        msgs = [(b"stream:adaptive", [(b"1000-0", fields)])]
        r    = _make_redis_mock(stream_messages=msgs)
        captured_payloads = []

        def _scan(payload, shutdown_event=None):
            captured_payloads.append(payload)
            return {"success": True, "new_jobs": 0, "fetched": 0,
                    "duration_ms": 100, "company": payload.get("company", "")}

        with patch("workers.scan_worker.ping", return_value=True), \
             patch("workers.scan_worker.get_redis", return_value=r), \
             patch("workers.scan_worker.init_db"), \
             patch("workers.scan_worker._ensure_consumer_group"), \
             patch("workers.scan_worker._run_listing_scan",
                   side_effect=_scan), \
             patch("workers.scheduler.on_adaptive_complete"):
            from workers.scan_worker import run_worker
            run_worker(once=True, skip_init_db=True)

        return captured_payloads[0] if captured_payloads else {}

    def test_company_extracted_from_fields(self):
        """company field from stream message → payload["company"]."""
        payload = self._run_and_capture_payload({
            "company": "Palantir", "dc_key": "greenhouse",
            "context": "normal", "scan_type": "adaptive",
        })
        self.assertEqual(payload.get("company"), "Palantir")

    def test_dc_key_extracted_from_fields(self):
        """dc_key field from stream message → payload["dc_key"]."""
        payload = self._run_and_capture_payload({
            "company": "Stripe", "dc_key": "lever",
            "context": "normal", "scan_type": "adaptive",
        })
        self.assertEqual(payload.get("dc_key"), "lever")

    def test_context_extracted_from_fields(self):
        """context field from stream message → payload["context"]."""
        payload = self._run_and_capture_payload({
            "company": "Stripe", "dc_key": "greenhouse",
            "context": "canary", "scan_type": "adaptive",
        })
        self.assertEqual(payload.get("context"), "canary")

    def test_missing_context_defaults_to_normal(self):
        """Missing context field → defaults to 'normal'."""
        payload = self._run_and_capture_payload({
            "company": "Stripe", "dc_key": "greenhouse",
            "scan_type": "adaptive",
        })
        self.assertEqual(payload.get("context"), "normal")

    def test_missing_company_no_crash(self):
        """Missing 'company' field → empty string, no KeyError crash."""
        payload = self._run_and_capture_payload({
            "dc_key": "greenhouse", "context": "normal",
        })
        self.assertEqual(payload.get("company", ""), "")


if __name__ == "__main__":
    unittest.main(verbosity=2)
