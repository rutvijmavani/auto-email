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


class TestWriteDomainHost(unittest.TestCase):
    """_write_domain stores public_domain_host only when root(host) == public_domain."""

    def _write(self, public_domain, host):
        from workers.domain_enrichment_worker import _write_domain
        conn = mock.MagicMock()
        _write_domain(conn, "12-3456789", public_domain, "same_domain", None, 0, host)
        return conn.execute.call_args[0][1]

    def test_matching_host_stored(self):
        params = self._write("example.com", "www.example.com")
        self.assertEqual(params, ("example.com", "www.example.com", "same_domain", "12-3456789"))

    def test_mismatched_host_nulled(self):
        params = self._write("example.com", "www.evil.org")
        self.assertIsNone(params[1])
        self.assertEqual(params[0], "example.com")

    def test_no_host_stored_as_null(self):
        self.assertIsNone(self._write("example.com", None)[1])

    def test_failure_branch_never_touches_host(self):
        from workers.domain_enrichment_worker import _write_domain
        conn = mock.MagicMock()
        _write_domain(conn, "12-3456789", None, "", 503, 1, "www.example.com")
        sql = conn.execute.call_args[0][0]
        self.assertNotIn("public_domain_host", sql)


def _chain(status, final_url="https://example.com/", body="", cookies=(), headers=None):
    return {"status": status, "headers": headers or {}, "body": body, "final_url": final_url,
            "cookies": set(cookies), "error_type": ""}


class TestWorkerTierClassification(unittest.TestCase):
    """CF Worker 2xx is classified (body/cookie signatures) instead of confirmed blindly."""

    def _probe(self, worker, direct=None, relay_mode=False):
        direct = direct or _chain(403)
        with mock.patch.object(pdm, "_fetch_chain", return_value=direct), \
             mock.patch.object(pdm, "_fetch_via_worker", return_value=worker) as fw, \
             mock.patch.object(pdm, "record_external_request"), \
             mock.patch.object(pdm, "PD_PROBE_RECORD_ENABLED", False):
            out = pdm._probe_host("example.com", session=object(), relay_mode=relay_mode)
        return out, fw

    def test_worker_real_page_confirms(self):
        out, _ = self._probe(_chain(200, body="<html>" + "x" * 5000 + "</html>"))
        self.assertTrue(out["confirmed"])
        self.assertEqual(out["resolved_by"], "worker")

    def test_worker_parked_cookies_store_nothing(self):
        out, _ = self._probe(_chain(200, body="<html>" + "x" * 5000 + "</html>",
                                    cookies=("lander_type", "traffic_target", "caf_ipaddr")))
        self.assertFalse(out["confirmed"])
        self.assertEqual(out["verdict"], "parked")

    def test_worker_parked_stub_body_store_nothing(self):
        out, _ = self._probe(_chain(200, body="<html>lander</html>",
                                    final_url="https://example.com/cgi-sys/defaultwebpage.cgi"))
        self.assertFalse(out["confirmed"])
        self.assertEqual(out["verdict"], "parked")

    def test_worker_challenge_confirms(self):
        out, _ = self._probe(_chain(200, body="<html>Just a moment...</html>",
                                    headers={"cf-mitigated": "challenge"}))
        self.assertEqual(out["verdict"], "challenge")
        self.assertTrue(out["confirmed"])

    def test_old_worker_reply_without_headers_cookies(self):
        # Old Worker: no headers/cookies keys -> _fetch_via_worker yields empty ones; still a real page.
        out, _ = self._probe(_chain(200, body="<html>" + "x" * 5000 + "</html>", headers={}))
        self.assertTrue(out["confirmed"])

    def test_worker_non_2xx_unchanged(self):
        out, _ = self._probe(_chain(403))
        self.assertFalse(out["confirmed"])
        self.assertEqual(out["status"], 403)

    def test_relay_mode_never_uses_worker(self):
        out, fw = self._probe(_chain(200), relay_mode=True)
        fw.assert_not_called()
        self.assertTrue(out["confirmed"])   # 403 from the relay is kept
        self.assertEqual(out["resolved_by"], "relay")


class TestRelayJunkLanding(unittest.TestCase):
    """A relay 403 is kept as the pd, except when it landed on a junk-landing root."""

    def _relay(self, final_url):
        with mock.patch.object(pdm, "_fetch_chain", return_value=_chain(403, final_url=final_url)), \
             mock.patch.object(pdm, "record_external_request"), \
             mock.patch.object(pdm, "PD_PROBE_RECORD_ENABLED", False):
            return pdm._probe_host("example.com", session=object(), relay_mode=True)

    def test_relay_403_on_junk_root_not_confirmed(self):
        out = self._relay("https://www.godaddy.com/")
        self.assertFalse(out["confirmed"])
        self.assertEqual(out["verdict"], "inconclusive")
        self.assertTrue(out["reason"].startswith("junk_landing:"))
        self.assertTrue(out["cross_domain"])

    def test_relay_403_on_other_root_still_confirmed(self):
        out = self._relay("https://www.real-co.com/")
        self.assertTrue(out["confirmed"])
        self.assertEqual(out["root"], "real-co.com")


class TestProbeEvidenceRecording(unittest.TestCase):
    """_record_probe_result: each tier writes only its own evidence columns."""

    def _record(self, relay_mode, worker=None, direct=None):
        direct = direct or _chain(403, body="blockpage", cookies=("oci_c",))
        with mock.patch.object(pdm, "_fetch_chain", return_value=direct), \
             mock.patch.object(pdm, "_fetch_via_worker", return_value=worker), \
             mock.patch.object(pdm, "record_external_request"), \
             mock.patch.object(pdm, "PD_PROBE_RECORD_ENABLED", True), \
             mock.patch.object(pdm, "_get_default_curl_session", return_value=mock.MagicMock()), \
             mock.patch.object(pdm, "record_probe") as rec:
            pdm._probe_host("example.com", session=object(), relay_mode=relay_mode)
        return rec.call_args[0][1]

    def test_worker_response_goes_to_worker_columns_not_oci(self):
        w = _chain(200, body="<html>" + "w" * 5000 + "</html>", cookies=("wc",))
        obs = self._record(False, worker=w)
        self.assertEqual(obs["status"], 403)                       # OCI keeps the direct 403
        self.assertEqual(obs["cookie_names"], "oci_c")
        self.assertEqual(obs["worker_status"], 200)
        self.assertEqual(obs["worker_cookie_names"], "wc")
        self.assertTrue(obs["worker_body_hash"])
        self.assertEqual(obs["resolved_by"], "worker")

    def test_worker_columns_cleared_when_worker_did_not_run(self):
        obs = self._record(False, worker=None)
        for k in ("worker_status", "worker_verdict", "worker_body_hash", "worker_cookie_names"):
            self.assertIn(k, obs)
            self.assertIsNone(obs[k])

    def test_non_2xx_worker_still_recorded_as_evidence(self):
        obs = self._record(False, worker=_chain(403, body="wafwall"))
        self.assertEqual(obs["worker_status"], 403)
        self.assertEqual(obs["worker_verdict"], "blocked")
        self.assertTrue(obs["worker_body_hash"])

    def test_relay_mode_writes_only_relay_and_decision_columns(self):
        obs = self._record(True, direct=_chain(200, body="<html>" + "r" * 5000 + "</html>", cookies=("rc",)))
        self.assertEqual(obs["relay_status"], 200)
        self.assertEqual(obs["relay_cookie_names"], "rc")
        self.assertTrue(obs["relay_body_hash"])
        for k in ("status", "body_hash", "title", "cookie_names", "worker_status", "worker_body_hash"):
            self.assertNotIn(k, obs)


class TestFetchViaWorkerShape(unittest.TestCase):
    """_fetch_via_worker returns a res-shaped dict and tolerates an old Worker reply."""

    def _call(self, payload):
        resp = mock.MagicMock(status_code=200)
        resp.json.return_value = payload
        with mock.patch.object(pdm, "CF_WORKER_URL", "https://w"), \
             mock.patch.object(pdm, "CF_WORKER_SECRET", "s"), \
             mock.patch.object(pdm, "CF_WORKER_DAILY_LIMIT", 10), \
             mock.patch.object(pdm, "get_day_request_count", return_value=0), \
             mock.patch.object(pdm, "record_external_request"), \
             mock.patch.object(pdm.requests, "post", return_value=resp) as post:
            return pdm._fetch_via_worker("https://example.com/"), post

    def test_new_worker_fields_passed_through(self):
        out, post = self._call({"status": 200, "final_url": "https://example.com/", "body": "hi",
                                "headers": {"Server": "nginx"}, "cookies": ["a", "b"],
                                "truncated": False, "error": None})
        self.assertEqual(out["cookies"], {"a", "b"})
        self.assertEqual(out["headers"], {"server": "nginx"})
        self.assertEqual(out["body"], "hi")
        sent = post.call_args.kwargs["json"]
        self.assertEqual(sent["max_bytes"], pdm.PD_BODY_MAX_BYTES)
        self.assertEqual(sent["max_hops"], pdm.CF_WORKER_MAX_HOPS)

    def test_old_worker_reply_defaults_empty(self):
        out, _ = self._call({"status": 200, "final_url": "https://example.com/", "body": "hi"})
        self.assertEqual(out["cookies"], set())
        self.assertEqual(out["headers"], {})

    def test_truncated_body_marked_oversize(self):
        out, _ = self._call({"status": 200, "final_url": "https://example.com/", "body": "x",
                             "truncated": True})
        self.assertEqual(out["body"], "")
        self.assertIn("x-scan-too-large", out["headers"])

    def test_old_worker_body_at_cap_marked_oversize(self):
        out, _ = self._call({"status": 200, "final_url": "https://example.com/",
                             "body": "x" * pdm.PD_BODY_MAX_BYTES})
        self.assertIn("x-scan-too-large", out["headers"])


if __name__ == "__main__":
    unittest.main()
