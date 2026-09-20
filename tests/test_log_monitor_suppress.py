"""
tests/test_log_monitor_suppress.py — scripts.log_monitor benign-WARNING suppression

KG domain mismatch and certspotter 429 / non-200 warnings are expected and handled
in-code, so they must not be emailed by log_monitor. ERROR-level lines with the same
text, and certspotter 401/403 (revoked/invalid key), must still alert.
"""

import json
import os
import sys
import types
import unittest

# log_monitor imports fcntl at module level (POSIX only); stub it so the pure
# line-classification helpers can be tested on any platform.
sys.modules.setdefault("fcntl", types.ModuleType("fcntl"))
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from scripts.log_monitor import _is_flagged, _is_suppressed


def _pipe(level, msg):
    return f"2026-09-20 15:00:00.000 | {level:<8} | jobs.public_domain | {msg}"


def _json(level, msg):
    return json.dumps({"time": "2026-09-20T15:00:00", "level": level,
                       "logger": "x", "msg": msg})


class TestLogMonitorSuppress(unittest.TestCase):

    def _both(self, level, msg):
        return (_pipe(level, msg), _json(level, msg))

    def test_kg_domain_mismatch_warning_suppressed(self):
        msg = ("  KG domain mismatch: kg_url='https://a.com' p856=None "
               "assigned='b.com' — discarding KG entry")
        for line in self._both("WARNING", msg):
            self.assertTrue(_is_suppressed(line))

    def test_kg_domain_mismatch_batch_variant_suppressed(self):
        msg = ("  [123456789] KG domain mismatch: kg_url=None p856='x.com' "
               "assigned='y.com' — discarding")
        for line in self._both("WARNING", msg):
            self.assertTrue(_is_suppressed(line))

    def test_certspotter_429_suppressed(self):
        for line in self._both("WARNING", "certspotter 429 for acme.com — retry after 3600s"):
            self.assertTrue(_is_suppressed(line))

    def test_certspotter_other_non_200_suppressed(self):
        for code in (404, 500, 502, 503):
            for line in self._both("WARNING", f"certspotter HTTP {code} for acme.com"):
                self.assertTrue(_is_suppressed(line), code)

    def test_certspotter_auth_failures_still_alert(self):
        for code in (401, 403):
            for line in self._both("WARNING", f"certspotter HTTP {code} for acme.com"):
                self.assertFalse(_is_suppressed(line), code)
                self.assertTrue(_is_flagged(line))

    def test_error_level_with_same_text_still_alerts(self):
        for msg in ("KG domain mismatch: kg_url=None", "certspotter HTTP 503 for acme.com"):
            for line in self._both("ERROR", msg):
                self.assertFalse(_is_suppressed(line))
                self.assertTrue(_is_flagged(line))

    def test_crtsh_and_certspotter_network_errors_suppressed(self):
        for msg in ("crt.sh HTTP 502 for windycitytechnologies.com",
                    "crt.sh error for collabriumsystems.com: HTTPSConnectionPool(host='crt.sh', "
                    "port=443): Read timed out. (read timeout=30)",
                    "certspotter error for acme.com: timeout"):
            for line in self._both("WARNING", msg):
                self.assertTrue(_is_suppressed(line), msg)

    def test_unrelated_warnings_still_alert(self):
        for msg in ("public_domain: rejecting private address 10.0.0.1",
                    "KG API daily limit (100k) reached"):
            for line in self._both("WARNING", msg):
                self.assertFalse(_is_suppressed(line), msg)
                self.assertTrue(_is_flagged(line), msg)


if __name__ == "__main__":
    unittest.main()
