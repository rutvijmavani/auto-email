"""discover_public_domain / _write_domain host handling (Stage 2 production path)."""
import unittest
from unittest import mock

import jobs.public_domain as pdm


def _r(confirmed, root="", final_host="", status=200, verdict="real", reason=""):
    return {"confirmed": confirmed, "root": root, "final_host": final_host, "status": status,
            "verdict": verdict, "reason": reason, "cross_domain": False,
            "resolved_by": "oci", "final_url": ""}


class TestDiscover(unittest.TestCase):
    def _run(self, probes, **kw):
        it = iter(probes)
        with mock.patch.object(pdm, "_probe_host", side_effect=lambda *a, **k: next(it)), \
             mock.patch.object(pdm, "_is_private_ip_literal", return_value=False), \
             mock.patch.object(pdm, "_ct_domains", return_value=([], None, "crtsh")):
            return pdm.discover_public_domain("example.com", **kw)

    def test_same_domain_returns_host(self):
        out = self._run([_r(True, "example.com", "www.example.com")])
        self.assertEqual(out, ("example.com", "same_domain", None, None, "www.example.com"))

    def test_redirect_returns_target_host(self):
        out = self._run([_r(True, "real.com", "www.real.com")])
        self.assertEqual(out[:2], ("real.com", "http_redirect"))
        self.assertEqual(out[4], "www.real.com")

    def test_host_dropped_when_root_mismatch(self):
        self.assertIsNone(pdm._pd_host("real.com", {"final_host": "evil.org"}))
        self.assertEqual(pdm._pd_host("real.com", {"final_host": "www.real.com"}), "www.real.com")

    def test_parked_stores_nothing(self):
        out = self._run([_r(False, verdict="parked", status=200, reason="parked")])
        self.assertIsNone(out[0])
        self.assertIsNone(out[4])

    def test_403_not_relay_is_inconclusive(self):
        out = self._run([_r(False, status=403, verdict="blocked", reason="http_403")])
        self.assertIsNone(out[0])
        self.assertEqual(out[3], 403)

    def test_relay_mode_forwarded(self):
        seen = {}

        def fake(host, session=None, relay_mode=False):
            seen["rm"] = relay_mode
            return _r(True, "example.com", "example.com", status=403)
        with mock.patch.object(pdm, "_probe_host", side_effect=fake), \
             mock.patch.object(pdm, "_is_private_ip_literal", return_value=False):
            out = pdm.discover_public_domain("example.com", relay_mode=True)
        self.assertTrue(seen["rm"])
        self.assertEqual(out[0], "example.com")


if __name__ == "__main__":
    unittest.main()
