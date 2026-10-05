"""OnFailure alert allowlist: every oneshot timer unit must be accepted, else its failure alert itself raises."""
import unittest

from scripts import startup_failure_alert as sfa


class TestAlertAllowlist(unittest.TestCase):
    def test_pd_candidates_accepted_and_oneshot(self):
        sfa._validate_service("pd-candidates")          # must not raise
        self.assertIn("pd-candidates", sfa._ONESHOT_SERVICES)
        self.assertIn("pd-candidates", sfa._DIAGNOSE_HINTS)
        self.assertIn("pd-candidates", sfa._SERVICE_DISPLAY)

    def test_unknown_service_still_rejected(self):
        with self.assertRaises(ValueError):
            sfa._validate_service("not-a-service")


if __name__ == "__main__":
    unittest.main()
