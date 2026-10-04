# tests/test_pd_classify.py — Tests for jobs/pd_classify.py + db/pd_probe.py
#
# Rule 1 vendor-signature classifier (pure functions, no network/DB) and the best-effort probe
# recorder. Bodies are trimmed from real responses fetched while building
# data/parked_domain_scan_v4.py (domains named in each test).

import os
import sys
import unittest
from unittest import mock

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from jobs import pd_classify as pc
from db import pd_probe


def _res(status=200, body="", headers=None, final_url="https://example.com/", cookies=(), **kw):
    r = {"status": status, "body": body, "headers": headers or {}, "final_url": final_url,
         "cookies": set(cookies), "error_type": ""}
    r.update(kw)
    return r


REAL_PAGE = "<!DOCTYPE html><html><head><title>Acme</title></head><body>" + ("x" * 5000) + "</body></html>"
DREAMHOST = ('<!doctype html> <html> <head> <title>Site not found &middot; DreamHost</title> '
             '<meta name="description" content="The owner of this domain has not yet uploaded their website." />')
TURBIFY = ('<!DOCTYPE html> <html> <head> <title>Under Construction</title> <style>body{}</style></head> <body> '
           '<header><img src="https://s.turbifycdn.com/yf/nrp/image/turbify/turbify-logo-v1-purple.svg" /></header>')
COMING_SOON = ('<!DOCTYPE html> <html> <head> <title>Coming Soon</title> <style type="text/css"> '
               '.container { margin-left: auto; margin-top: 177px; max-width: 1170px; } </style>')
CF_CHALLENGE = "<!DOCTYPE html><html><head><title>Just a moment...</title></head><body></body></html>"
CLIENT_CHALLENGE = "<!DOCTYPE html><html><head><title>Client Challenge</title></head><body></body></html>"
NETLIFY = ('<!doctype html><html lang=en><meta charset=utf-8><title>Site not found</title>'
           '<style>:root{--colorRgbFace:1}</style>')


class TestRule1Signatures(unittest.TestCase):
    def test_real_page_is_ok(self):
        self.assertEqual(pc.classify(_res(body=REAL_PAGE)), ("ok", ""))

    def test_godaddy_lander_stub_is_parked(self):   # btacs / premium swc false-ok root cause
        r = _res(body='<script>window.location.href="/lander"</script>')
        self.assertEqual(pc.classify(r), ("parked", "godaddy_lander_stub"))

    def test_godaddy_parkweb_cookies_are_parked(self):
        r = _res(body=REAL_PAGE, cookies={"lander_type", "traffic_target", "caf_ipaddr"})
        self.assertEqual(pc.classify(r), ("parked", "godaddy_parkweb"))

    def test_bluehost_suspended_path_is_parked(self):
        r = _res(final_url="https://x.com/cgi-sys/suspendedpage.cgi", body="s")
        self.assertEqual(pc.classify(r), ("parked", "bluehost_suspended"))

    def test_default_vhost_is_parked(self):
        r = _res(body="<html><body>This is the default server vhost</body></html>")
        self.assertEqual(pc.classify(r), ("parked", "unconfigured_default_vhost"))

    def test_cloudflare_just_a_moment_is_challenge(self):
        self.assertEqual(pc.classify(_res(status=403, body=CF_CHALLENGE)), ("challenge", "body_signature"))

    def test_client_challenge_title_is_challenge(self):    # monolithicpower.com
        self.assertEqual(pc.classify(_res(body=CLIENT_CHALLENGE)), ("challenge", "body_signature"))

    def test_vendor_presence_header_on_full_page_is_not_challenge(self):
        # x-iinfo is stamped on every Imperva response, including full 200 pages
        r = _res(body=REAL_PAGE, headers={"X-Iinfo": "1"})
        self.assertEqual(pc.classify(r), ("ok", ""))

    def test_vendor_presence_header_on_stub_is_challenge(self):
        r = _res(status=200, body="<html></html>", headers={"X-Iinfo": "1"})
        self.assertEqual(pc.classify(r), ("challenge", "header:x-iinfo"))

    def test_cf_mitigated_header(self):
        r = _res(status=403, body="x", headers={"cf-mitigated": "challenge"})
        self.assertEqual(pc.classify(r), ("challenge", "cf_mitigated"))

    def test_vendor_domain_landing_is_challenge(self):
        r = _res(body=REAL_PAGE)
        self.assertEqual(pc.classify(r, final_root="perfdrive.com", challenge_domains=frozenset({"perfdrive.com"})),
                         ("challenge", "vendor_domain"))

    def test_interim_202_is_challenge(self):
        self.assertEqual(pc.classify(_res(status=202, body=REAL_PAGE)), ("challenge", "interim_202_unrecognized"))

    def test_plain_403_is_blocked_real(self):
        self.assertEqual(pc.classify(_res(status=403, body="Forbidden")), ("blocked", "403"))

    def test_plain_404_is_inconclusive(self):
        self.assertEqual(pc.classify(_res(status=404, body="Not Found")), ("inconclusive", "404"))

    def test_no_response_is_error(self):
        self.assertEqual(pc.classify(_res(status=None, error_type="timeout")), ("error", "timeout"))


class TestPlatformTemplates(unittest.TestCase):
    def test_platform_soft_until_www_checked(self):
        r = _res(status=200, body=DREAMHOST)                       # no www_conflict key = unchecked
        self.assertEqual(pc.classify(r), ("inconclusive", "platform_no_site:dreamhost:unchecked"))

    def test_dreamhost_200_confirmed_when_www_agrees(self):    # blackguam.com, sysarchinc.com
        r = _res(status=200, body=DREAMHOST, www_conflict="")
        self.assertEqual(pc.classify(r), ("parked", "platform_no_site:dreamhost"))

    def test_turbify_placeholder(self):                         # velagainc.com
        r = _res(status=200, body=TURBIFY, www_conflict="")
        self.assertEqual(pc.classify(r), ("parked", "platform_no_site:turbify"))

    def test_shared_coming_soon_template(self):                 # kosservices.com, oberonit.com
        r = _res(status=200, body=COMING_SOON, www_conflict="")
        self.assertEqual(pc.classify(r), ("parked", "platform_no_site:shared_coming_soon"))

    def test_www_differs_stays_inconclusive(self):              # taruntech.com (www = real site)
        r = _res(status=200, body=TURBIFY, www_conflict="www_differs")
        self.assertEqual(pc.classify(r), ("inconclusive", "platform_no_site:turbify:www_differs"))

    def test_netlify_404_template(self):                        # ivytechsol.us
        r = _res(status=404, body=NETLIFY, www_conflict="")
        self.assertEqual(pc.classify(r), ("parked", "platform_no_site:netlify"))

    def test_google_sites_needs_ghs_server(self):
        body = "<title>Error 404 (Not Found)!!1</title>"
        self.assertEqual(pc.classify(_res(status=404, body=body, www_conflict="")), ("inconclusive", "404"))
        r = _res(status=404, body=body, headers={"Server": "ghs"}, www_conflict="")
        self.assertEqual(pc.classify(r), ("parked", "platform_no_site:google_sites"))

    def test_200_only_templates_do_not_fire_on_other_200s(self):
        # a 200 page carrying the Netlify title must NOT count (netlify answers 404)
        self.assertEqual(pc.classify(_res(status=200, body=NETLIFY, www_conflict="")), ("ok", ""))

    def test_large_body_never_a_platform_template(self):
        self.assertEqual(pc.platform_no_site(_res(status=200, body=DREAMHOST + "x" * 20000)), "")


class TestOutcomeRules(unittest.TestCase):
    def test_503_and_429_become_retry_later(self):
        self.assertEqual(pc.apply_outcome_rules("blocked", "503", "a.com", "a.com"),
                         ("inconclusive", "retry_later:503", False))
        self.assertEqual(pc.apply_outcome_rules("blocked", "429", "a.com", "a.com"),
                         ("inconclusive", "retry_later:429", False))

    def test_403_stays_blocked(self):
        self.assertEqual(pc.apply_outcome_rules("blocked", "403", "a.com", "a.com"), ("blocked", "403", False))

    def test_cross_domain_accepted(self):                       # mmm.com -> 3m.com
        self.assertEqual(pc.apply_outcome_rules("ok", "", "mmm.com", "3m.com"), ("ok", "", True))

    def test_junk_landing_stores_nothing(self):                 # forsale.godaddy.com
        self.assertEqual(pc.apply_outcome_rules("ok", "", "venisa.com", "godaddy.com"),
                         ("inconclusive", "junk_landing:godaddy.com", True))

    def test_non_ok_verdict_untouched(self):
        self.assertEqual(pc.apply_outcome_rules("parked", "x", "a.com", "b.com"), ("parked", "x", False))


class TestDescribe(unittest.TestCase):
    def test_fingerprint_fields(self):
        d = pc.describe(_res(body='<html><head><title> Hi  there </title><script src="a.js"></script></head></html>'), 20)
        self.assertEqual(d["title"], "Hi there")
        self.assertEqual(d["ext_refs"], 1)
        self.assertEqual(len(d["snippet"]), 20)
        self.assertEqual(len(d["body_hash"]), 64)

    def test_identical_bodies_share_hash(self):
        a, b = pc.describe(_res(body="<p>x</p>"), 10), pc.describe(_res(body=" <p>x</p> "), 10)
        self.assertEqual(a["body_hash"], b["body_hash"])


class TestRecordProbe(unittest.TestCase):
    def test_requires_domain_and_verdict(self):
        with mock.patch.object(pd_probe, "get_conn") as gc:
            self.assertFalse(pd_probe.record_probe("", {"final_verdict": "ok"}))
            self.assertFalse(pd_probe.record_probe("a.com", {}))
            gc.assert_not_called()

    def test_writes_one_param_per_column_plus_domain(self):
        conn = mock.MagicMock()
        with mock.patch.object(pd_probe, "get_conn", return_value=conn):
            self.assertTrue(pd_probe.record_probe(" A.com ", {"final_verdict": "ok", "status": 200}))
        sql, params = conn.execute.call_args[0]
        self.assertEqual(params[0], "a.com")
        self.assertEqual(len(params), 1 + len(pd_probe._FIELDS))
        self.assertEqual(sql.count("?"), len(params))
        conn.commit.assert_called_once()

    def test_failure_is_swallowed_and_retried_once(self):
        conn = mock.MagicMock()
        conn.execute.side_effect = RuntimeError("db down")
        with mock.patch.object(pd_probe, "get_conn", return_value=conn):
            self.assertFalse(pd_probe.record_probe("a.com", {"final_verdict": "ok"}))
        self.assertEqual(conn.execute.call_count, 2)


if __name__ == "__main__":
    unittest.main()
