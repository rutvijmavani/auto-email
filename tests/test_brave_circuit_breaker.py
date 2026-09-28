"""
tests/test_brave_circuit_breaker.py
─────────────────────────────────────────────────────────────────────────────
Covers the Brave 402 fast-fail circuit breaker in
scripts/discover_h1b_ats.py::brave_career_search (added 2026-09-22, per user
request after live VM logs showed HTTP 402 Payment Required — a Brave
account/billing issue, not our local _BRAVE_QUOTA_LIMIT counter — repeated
on every single call, wasting one request per company for the rest of the run).

  - A 402 response trips the breaker: sets module-level _brave_blocked_until
    _BRAVE_402_COOLDOWN_S seconds into the future.
  - While tripped, brave_career_search() short-circuits before making any
    HTTP call (no requests.get invocation at all).
  - Once the cooldown window has passed, calls resume normally.
  - The 402 is still recorded via record_external_request (for the pipeline
    health report), unlike the local quota-exceeded early-return which is
    not recorded at all (that's HTTP-call volume, not an HTTP response).
"""

import os
import sys
import time
import unittest
from unittest.mock import MagicMock, patch

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import scripts.discover_h1b_ats as dh


def _reset_breaker():
    dh._brave_blocked_until = 0.0


class TestBrave402CircuitBreaker(unittest.TestCase):

    def setUp(self):
        _reset_breaker()

    def tearDown(self):
        _reset_breaker()

    @patch("scripts.discover_h1b_ats.record_external_request")
    @patch("scripts.discover_h1b_ats._brave_load_quota", return_value={"calls": 0})
    @patch("scripts.discover_h1b_ats._BRAVE_API_KEY", "fake-key")
    @patch("scripts.discover_h1b_ats.requests.get")
    def test_402_trips_breaker_and_is_recorded(self, mock_get, _mock_quota, mock_record):
        mock_resp = MagicMock()
        mock_resp.status_code = 402
        mock_get.return_value = mock_resp

        result = dh.brave_career_search("Acme Corp")

        self.assertIsNone(result)
        self.assertGreater(dh._brave_blocked_until, time.time())
        mock_record.assert_called_once()
        self.assertEqual(mock_record.call_args[0][0], "brave")
        self.assertEqual(mock_record.call_args[0][1], 402)

    @patch("scripts.discover_h1b_ats.record_external_request")
    @patch("scripts.discover_h1b_ats._brave_load_quota", return_value={"calls": 0})
    @patch("scripts.discover_h1b_ats._BRAVE_API_KEY", "fake-key")
    @patch("scripts.discover_h1b_ats.requests.get")
    def test_tripped_breaker_skips_http_call_entirely(self, mock_get, _mock_quota, mock_record):
        dh._brave_blocked_until = time.time() + dh._BRAVE_402_COOLDOWN_S

        result = dh.brave_career_search("Acme Corp")

        self.assertIsNone(result)
        mock_get.assert_not_called()
        mock_record.assert_not_called()

    @patch("scripts.discover_h1b_ats.record_external_request")
    @patch("scripts.discover_h1b_ats._brave_load_quota", return_value={"calls": 0})
    @patch("scripts.discover_h1b_ats._BRAVE_API_KEY", "fake-key")
    @patch("scripts.discover_h1b_ats.requests.get")
    def test_breaker_expired_allows_call_through(self, mock_get, _mock_quota, mock_record):
        # Cooldown already elapsed — should behave as if never tripped.
        dh._brave_blocked_until = time.time() - 1

        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {"web": {"results": []}}
        mock_get.return_value = mock_resp

        result = dh.brave_career_search("Acme Corp")

        mock_get.assert_called_once()
        self.assertIsNone(result)  # no plausible candidates in the fake response
        mock_record.assert_called_once_with("brave", 200, unittest.mock.ANY)


if __name__ == "__main__":
    unittest.main()
