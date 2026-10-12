"""Tests for jobs/ats/taleo.py portal-id discovery."""
import unittest
from unittest.mock import MagicMock, patch

import requests

import jobs.ats.taleo as taleo

HOST = "https://acme.taleo.net"


def _resp(status=200, text="", url=HOST + "/"):
    r = MagicMock()
    r.status_code, r.text, r.url = status, text, url
    return r


def _session(pages, redirect=None):
    """GET fake. pages: {section: html} for /careersection/<section>/ URLs.
    redirect: (final_url, html) served for the section-less URL, else 404."""
    session = MagicMock()

    def _get(url, **_kw):
        if "/careersection/jobsearch.ftl" in url:
            if redirect:
                return _resp(200, redirect[1], redirect[0])
            return _resp(404)
        for section, text in pages.items():
            if f"/careersection/{section}/" in url:
                return _resp(200, text, url)
        return _resp(404)

    session.get.side_effect = _get
    return session


def _urls(session):
    return [c.args[0] for c in session.get.call_args_list]


class TestDiscoverPortalId(unittest.TestCase):

    def test_stored_section_is_tried_first(self):
        session = _session({"ext": "x portal=8100108034 y"})
        with patch.object(taleo, "_make_session", return_value=session):
            self.assertEqual(taleo._discover_portal_id("uab", "ext"),
                             ("8100108034", "ext"))
        self.assertEqual(len(_urls(session)), 1)

    def test_default_redirect_finds_unguessable_section(self):
        self.assertNotIn("kndext", taleo.SECTIONS)
        final = "https://kindred.taleo.net:443/careersection/kndext/jobsearch.ftl"
        session = _session({}, redirect=(final, "portal=7654321"))
        with patch.object(taleo, "_make_session", return_value=session):
            self.assertEqual(taleo._discover_portal_id("kindred"),
                             ("7654321", "kndext"))
        self.assertEqual(len(_urls(session)), 1)

    def test_wrong_stored_section_falls_to_default_redirect(self):
        final = HOST + "/careersection/oh_ext/jobsearch.ftl"
        session = _session({}, redirect=(final, "portal=1234567"))
        with patch.object(taleo, "_make_session", return_value=session):
            self.assertEqual(taleo._discover_portal_id("acme", "bogus"),
                             ("1234567", "oh_ext"))

    def test_redirect_off_taleo_is_not_accepted_and_logs_warning(self):
        off = "https://careers.example.com/careersection/ext/jobsearch.ftl"
        session = _session({}, redirect=(off, "portal=9999999"))
        with patch.object(taleo, "_make_session", return_value=session), \
             self.assertLogs(taleo.logger, level="WARNING") as cm:
            self.assertEqual(taleo._discover_portal_id("acme", "lateral"),
                             (None, None))
        self.assertIn("acme", cm.output[0])
        self.assertIn("lateral", cm.output[0])

    def test_fixed_list_is_last_resort(self):
        session = _session({"ext": "portal=8100108034"})
        with patch.object(taleo, "_make_session", return_value=session):
            self.assertEqual(taleo._discover_portal_id("uab"),
                             ("8100108034", "ext"))

    def test_no_duplicate_probe_when_stored_section_is_in_defaults(self):
        session = _session({})
        with patch.object(taleo, "_make_session", return_value=session), \
             self.assertLogs(taleo.logger, level="WARNING"):
            self.assertEqual(taleo._discover_portal_id("acme", "cs"), (None, None))
        urls = _urls(session)
        self.assertEqual(len(urls), len(set(urls)))
        self.assertEqual(len(urls), len(taleo.SECTIONS) + 1)  # + default redirect

    def test_request_errors_fall_through(self):
        session = MagicMock()
        session.get.side_effect = [
            requests.RequestException("boom"),            # stored section
            _resp(200, "portal=7654321",
                  HOST + "/careersection/ex/jobsearch.ftl"),  # default redirect
        ]
        with patch.object(taleo, "_make_session", return_value=session):
            self.assertEqual(taleo._discover_portal_id("acme", "ext"),
                             ("7654321", "ex"))


class TestFetchJobsPassesStoredSection(unittest.TestCase):

    def test_empty_portal_id_discovers_with_stored_section_and_persists_it(self):
        slug_info = {"company": "uab", "portal_id": "", "section": "ext"}
        with patch.object(taleo, "_discover_portal_id",
                          return_value=("8100108034", "ext")) as disc, \
             patch.object(taleo, "_make_session") as ms:
            ms.return_value.post.side_effect = requests.RequestException("stop")
            self.assertEqual(taleo.fetch_jobs(slug_info, "UAB"), [])
        disc.assert_called_once_with("uab", "ext")
        self.assertEqual(slug_info["portal_id"], "8100108034")


if __name__ == "__main__":
    unittest.main()
