"""
tests/test_careers_url_check.py — ownership checks for stored careers_url values
(jobs/careers_url_check.py) and their wiring into Phase 4 (brave_career_search).
"""
import os
import sys
import unittest
from unittest.mock import MagicMock, patch

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import scripts.discover_h1b_ats as dh
from jobs.careers_url_check import (
    REASON_AGGREGATOR, REASON_ATS_NO_NAME, REASON_CHALLENGE, REASON_OFF_DOMAIN, REASON_VENDOR,
    _name_owns_root, blocked_reason, phase4_anchor_check,
)

ATS = dh._KNOWN_ATS_DOMAINS


class TestBlockedReason(unittest.TestCase):
    def test_challenge_vendor_always_blocked(self):
        self.assertEqual(blocked_reason("https://validate.perfdrive.com/x", "Acme Corp"), REASON_CHALLENGE)

    def test_aggregator_always_blocked(self):
        self.assertEqual(blocked_reason("https://www.myvisajobs.com/c/acme", "Acme Corp"), REASON_AGGREGATOR)

    def test_vendor_blocked_for_unrelated_employer(self):
        self.assertEqual(blocked_reason("https://www.google.com/about/careers", "Tek Data LLC"), REASON_VENDOR)
        self.assertEqual(blocked_reason("https://www.cloudflare.com/careers", "Cloud Tek Data LLC"), REASON_VENDOR)

    def test_vendor_allowed_for_the_vendor_itself(self):
        self.assertEqual(blocked_reason("https://careers.google.com/jobs", "Google LLC"), "")
        self.assertEqual(blocked_reason("https://www.cloudflare.com/careers", "Cloudflare, Inc."), "")
        self.assertEqual(blocked_reason("https://careers.microsoft.com/", "Microsoft Corporation"), "")

    def test_normal_host_not_blocked(self):
        self.assertEqual(blocked_reason("https://careers.teradata.com", "Teradata Corporation"), "")


class TestPhase4AnchorCheck(unittest.TestCase):
    def check(self, url, anchor, name):
        return phase4_anchor_check(url, anchor, name, ATS)

    def test_pd_root_accepted(self):
        self.assertEqual(self.check("https://careers.cummins.com/x", "cummins.com", "Cummins Inc"), (True, ""))

    def test_same_brand_other_suffix_accepted(self):
        self.assertTrue(self.check("https://jobs.geisinger.org/x", "geisinger.edu", "Geisinger Health")[0])
        self.assertTrue(self.check("https://commonspirit.careers/x", "commonspirithealth.org", "CommonSpirit")[0])

    def test_unrelated_host_rejected(self):
        ok, why = self.check("https://www.example-jobs.com/acme", "acme.com", "Acme Corp")
        self.assertFalse(ok)
        self.assertEqual(why, REASON_OFF_DOMAIN)

    def test_aggregator_rejected_even_if_name_matches(self):
        ok, why = self.check("https://www.myvisajobs.com/acme", "acme.com", "Acme Corp")
        self.assertEqual((ok, why), (False, REASON_AGGREGATOR))

    def test_ats_host_with_employer_slug_accepted(self):
        self.assertTrue(self.check("https://boards.greenhouse.io/acmecorp", "acme.com", "Acme Corp")[0])
        self.assertTrue(self.check("https://acme.wd5.myworkdayjobs.com/en-US/careers", "acme.com", "Acme Corp")[0])

    def test_ats_host_for_other_company_rejected(self):
        ok, why = self.check("https://boards.greenhouse.io/zenith", "acme.com", "Acme Corp")
        self.assertEqual((ok, why), (False, REASON_ATS_NO_NAME))

    def test_ats_tenant_must_equal_identifier_not_contain_it(self):
        ok, why = self.check("https://boards.greenhouse.io/acme-other", "acme.com", "Acme Corp")
        self.assertEqual((ok, why), (False, REASON_ATS_NO_NAME))
        ok, why = self.check("https://acmeother.wd5.myworkdayjobs.com/en-US/x", "acme.com", "Acme Corp")
        self.assertEqual((ok, why), (False, REASON_ATS_NO_NAME))
        self.assertTrue(self.check("https://boards.greenhouse.io/acme-corp", "acme.com", "Acme Corp")[0])
        self.assertTrue(self.check("https://jobs.lever.co/acme/abc123", "acme.com", "Acme Corp")[0])

    def test_provider_host_label_is_not_a_tenant(self):
        ok, why = self.check("https://boards.greenhouse.io/zenith", "boards.com", "Boards Inc")
        self.assertEqual((ok, why), (False, REASON_ATS_NO_NAME))
        self.assertTrue(self.check("https://boards.greenhouse.io/boards", "boards.com", "Boards Inc")[0])

    def test_vendor_ownership_is_whole_name_not_containment(self):
        self.assertFalse(_name_owns_root("Business Solutions Inc", "business.site"))
        self.assertEqual(blocked_reason("https://careers.google.com/", "Google Public Sector"), REASON_VENDOR)
        self.assertEqual(blocked_reason("https://careers.google.com/", "Google LLC"), "")

    def test_vendor_anchor_never_blesses_vendor_landing(self):
        # pd is a Workspace-email domain: a google.com result for an unrelated employer stays rejected
        ok, why = self.check("https://www.google.com/about/careers", "tekdata.com", "Tek Data LLC")
        self.assertEqual((ok, why), (False, REASON_VENDOR))


def _brave_response(urls):
    resp = MagicMock()
    resp.status_code = 200
    resp.json.return_value = {"web": {"results": [{"url": u} for u in urls]}}
    return resp


@patch("scripts.discover_h1b_ats.record_external_request")
@patch("scripts.discover_h1b_ats._brave_load_quota", return_value={"calls": 0})
@patch("scripts.discover_h1b_ats._BRAVE_API_KEY", "fake-key")
@patch("scripts.discover_h1b_ats._is_public_url", return_value=True)
@patch("scripts.discover_h1b_ats.requests.get")
class TestBraveAnchorWiring(unittest.TestCase):
    def setUp(self):
        dh._brave_blocked_until = 0.0

    def test_no_anchor_makes_no_call(self, mock_get, *_):
        self.assertIsNone(dh.brave_career_search("Acme Corp", anchor_domain=None))
        mock_get.assert_not_called()

    def test_unanchored_results_dropped(self, mock_get, *_):
        mock_get.return_value = _brave_response([
            "https://www.myvisajobs.com/careers/acme",
            "https://www.zippia.com/acme-careers",
            "https://random-jobs-site.com/acme/careers",
        ])
        self.assertIsNone(dh.brave_career_search("Acme Corp", anchor_domain="acme.com"))

    def test_anchored_result_returned(self, mock_get, *_):
        mock_get.return_value = _brave_response([
            "https://www.myvisajobs.com/careers/acme",
            "https://careers.acme.com/jobs",
        ])
        self.assertEqual(dh.brave_career_search("Acme Corp", anchor_domain="acme.com"),
                         "https://careers.acme.com/jobs")


if __name__ == "__main__":
    unittest.main()
