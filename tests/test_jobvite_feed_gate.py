# tests/test_jobvite_feed_gate.py — workers/fullscan.py::_verify_jobvite_feed Redis gate handling

import os
import sys
import unittest
from unittest.mock import MagicMock, patch

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
os.environ["TEST_MODE"] = "1"

from workers import fullscan


class TestFeedGate(unittest.TestCase):

    def test_gate_released_when_verification_fails(self):
        r = MagicMock()
        r.set.return_value = True
        with patch("jobs.ats.jobvite.verify_fetch", side_effect=RuntimeError("boom")):
            fullscan._verify_jobvite_feed("Acme", r, "acme", [])
        r.delete.assert_called_once()

    def test_gate_kept_when_verification_succeeds(self):
        r = MagicMock()
        r.set.return_value = True
        with patch("jobs.ats.jobvite.verify_fetch", return_value={"status": "verified", "reason": "ok"}), \
             patch.object(fullscan, "annotate_latest"):
            fullscan._verify_jobvite_feed("Acme", r, "acme", [])
        r.delete.assert_not_called()

    def test_gate_already_held_skips_check_and_keeps_gate(self):
        r = MagicMock()
        r.set.return_value = None
        with patch("jobs.ats.jobvite.verify_fetch") as vf:
            fullscan._verify_jobvite_feed("Acme", r, "acme", [])
        vf.assert_not_called()
        r.delete.assert_not_called()


if __name__ == "__main__":
    unittest.main()
