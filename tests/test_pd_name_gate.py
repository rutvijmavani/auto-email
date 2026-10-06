"""jobs/pd_name_gate.py — strict employer-name check for cross-domain redirects; db/pd_redirect_review.py SQL shape."""
import unittest
from unittest import mock

from db import pd_redirect_review as rr
from jobs.pd_name_gate import HINT_ACQUISITION_LIKE, HINT_NO_NAME_MATCH, check_redirect


class TestCheckRedirect(unittest.TestCase):
    def test_name_matches_new_domain(self):
        self.assertEqual(check_redirect("Acme Staffing LLC", "acmestaffing.com", "acme.com"), (True, ""))
        self.assertEqual(check_redirect("BNP Paribas", "x.com", "group.bnpparibas"), (True, ""))

    def test_old_and_new_contain_each_other(self):
        self.assertEqual(check_redirect("Zzzz Qqqq", "exampleco.com", "example.com"), (True, ""))

    def test_same_brand_different_suffix(self):
        for old, new in (("svitinc.net", "svitinc.com"), ("cme.com", "cmegroup.com"), ("vercel.app", "vercel.com")):
            self.assertEqual(check_redirect("Zzzz Qqqq", old, new), (True, ""), (old, new))

    def test_name_matches_only_old_domain_goes_to_review(self):
        ok, hint = check_redirect("Faurecia Interior Systems, Inc.", "faurecia.com", "forvia.com")
        self.assertFalse(ok)
        self.assertEqual(hint, HINT_ACQUISITION_LIKE)
        ok, hint = check_redirect("Stakaha Inc", "stakaha.com", "largourugs.com")
        self.assertFalse(ok)
        self.assertEqual(hint, HINT_ACQUISITION_LIKE)

    def test_name_matches_neither(self):
        ok, hint = check_redirect("Kimberly-Clark Corporation", "blink.app", "loftware.com")
        self.assertFalse(ok)
        self.assertEqual(hint, HINT_NO_NAME_MATCH)

    def test_stop_words_and_short_tokens_are_not_evidence(self):
        # 'systems'/'solutions' are stop words; 'abc' is under the minimum token length
        self.assertFalse(check_redirect("Global Systems Solutions Inc", "foo.com", "systemsolutions.com")[0])
        self.assertFalse(check_redirect("ABC Inc", "foo.com", "abc.com")[0])

    def test_empty_name_never_matches(self):
        self.assertEqual(check_redirect("", "a.com", "b.com"), (False, HINT_NO_NAME_MATCH))


class TestReviewTable(unittest.TestCase):
    def _conn(self, rowcount=1):
        conn = mock.MagicMock()
        conn.execute.return_value.rowcount = rowcount
        return conn

    def test_queue_new_pair_inserts_only(self):
        conn = self._conn(1)
        self.assertTrue(rr.queue_pair(conn, "1", "a.com", "b.com", "www.b.com", "N", "acquisition-like", "backfill"))
        self.assertEqual(conn.execute.call_count, 1)

    def test_queue_known_pair_only_refreshes_last_seen(self):
        conn = self._conn(0)
        self.assertFalse(rr.queue_pair(conn, "1", "a.com", "b.com", None, "N", "x", "enrichment"))
        sql = conn.execute.call_args[0][0]
        self.assertIn("SET last_seen_at = NOW()", sql)
        self.assertNotIn("status", sql)          # a decided pair is never re-opened
        self.assertNotIn("notified_at", sql)     # and never re-emailed

    def test_decide_rejects_bad_status(self):
        with self.assertRaises(ValueError):
            rr.decide(self._conn(), "1", "pending")

    def test_decide_scopes_to_new_domain_when_given(self):
        conn = self._conn()
        rr.decide(conn, "1", "approved", "b.com")
        self.assertIn("AND new_domain = ?", conn.execute.call_args[0][0])


class TestGatedDiscovery(unittest.TestCase):
    """jobs.public_domain.discover_public_domain_gated: name gate + review table around the resolver."""

    def _run(self, resolved, status=None, name="Kimberly-Clark Corporation", assigned="blink.app"):
        from jobs import public_domain as pdm
        conn = mock.MagicMock()
        with mock.patch.object(pdm, "discover_public_domain", return_value=resolved), \
             mock.patch.object(rr, "get_status", return_value=status), \
             mock.patch.object(rr, "queue_pair") as q:
            out = pdm.discover_public_domain_gated(conn, "12", name, assigned, source="enrichment")
        return out, q

    def test_failed_gate_stores_nothing_and_queues(self):
        out, q = self._run(("loftware.com", "http_redirect", None, None, "www.loftware.com"))
        self.assertEqual(out, (None, "name_gate_held", None, None, None))
        self.assertEqual(q.call_args[0][1:5], ("12", "blink.app", "loftware.com", "www.loftware.com"))
        self.assertEqual(q.call_args[0][-1], "enrichment")

    def test_name_matching_new_domain_passes(self):
        res = ("loftware.com", "http_redirect", None, None, "loftware.com")
        out, q = self._run(res, name="Loftware Inc")
        self.assertEqual(out, res)
        q.assert_not_called()

    def test_approved_pair_passes_despite_name(self):
        res = ("loftware.com", "root_fallback", None, None, None)
        out, q = self._run(res, status=rr.APPROVED)
        self.assertEqual(out, res)
        q.assert_not_called()

    def test_rejected_pair_stores_nothing_and_is_not_requeued(self):
        out, q = self._run(("loftware.com", "http_redirect", None, None, None), status=rr.REJECTED)
        self.assertEqual(out[:2], (None, "name_gate_held"))
        q.assert_not_called()

    def test_same_root_and_non_redirect_methods_are_not_gated(self):
        for res, assigned in ((("blink.app", "same_domain", None, None, "blink.app"), "blink.app"),
                              (("example.com", "root_fallback", None, None, None), "ny.mail.example.com"),
                              (("other.com", "certspotter", None, None, None), "blink.app"),
                              ((None, "no_signal", None, 403, None), "blink.app")):
            out, q = self._run(res, assigned=assigned)
            self.assertEqual(out, res, res)
            q.assert_not_called()

    def test_vendor_root_dropped_for_every_method(self):
        none = (None, "no_signal", None, None, None)
        for method, pd, assigned, name in (
                ("certspotter", "cloudflaressl.com", "align.com", "Align Technology"),
                ("http_redirect", "icloud.com", "me.com", "Tiny Staffing LLC"),
                ("same_domain", "google.com", "google.com", "Verily Life Sciences"),
                ("http_redirect", "business.site", "foo.com", "Foo Consulting"),
                ("certspotter", "att.net", "att.net", "Bar Systems Inc")):
            out, q = self._run((pd, method, None, None, pd), assigned=assigned, name=name)
            self.assertEqual(out, none, (method, pd))
            q.assert_not_called()

    def test_vendor_root_kept_for_the_vendor_itself(self):
        for pd, name in (("google.com", "Google LLC"), ("cloudflare.com", "Cloudflare, Inc."),
                         ("microsoft.com", "Microsoft Corporation")):
            res = (pd, "same_domain", None, None, pd)
            out, _ = self._run(res, assigned=pd, name=name)
            self.assertEqual(out, res, pd)


if __name__ == "__main__":
    unittest.main()
