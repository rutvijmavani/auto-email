"""Tests for jobs/ats/taleo.py portal-id discovery."""
import unittest
from unittest.mock import MagicMock, patch

import requests

import jobs.ats.taleo as taleo


def _session(pages):
    """Session whose GET returns the page registered for a section, else 404."""
    session = MagicMock()

    def _get(url, **_kw):
        resp = MagicMock()
        for section, text in pages.items():
            if f"/careersection/{section}/" in url:
                resp.status_code, resp.text = 200, text
                return resp
        resp.status_code, resp.text = 404, ""
        return resp

    session.get.side_effect = _get
    return session


class TestDiscoverPortalId(unittest.TestCase):

    def test_stored_section_is_tried_first(self):
        session = _session({"ext": "x portal=8100108034 y"})
        with patch.object(taleo, "_make_session", return_value=session):
            portal, section = taleo._discover_portal_id("uab", "ext")
        self.assertEqual((portal, section), ("8100108034", "ext"))
        self.assertIn("/careersection/ext/", session.get.call_args_list[0].args[0])

    def test_section_outside_default_list_found_via_stored_section(self):
        self.assertNotIn("custom9", taleo.SECTIONS)
        session = _session({"custom9": "portal=1234567"})
        with patch.object(taleo, "_make_session", return_value=session):
            self.assertEqual(taleo._discover_portal_id("acme", "custom9"),
                             ("1234567", "custom9"))

    def test_ext_is_a_default_section(self):
        session = _session({"ext": "portal=8100108034"})
        with patch.object(taleo, "_make_session", return_value=session):
            self.assertEqual(taleo._discover_portal_id("uab"),
                             ("8100108034", "ext"))

    def test_no_duplicate_probe_when_stored_section_is_in_defaults(self):
        session = _session({})
        with patch.object(taleo, "_make_session", return_value=session):
            self.assertEqual(taleo._discover_portal_id("acme", "cs"), (None, None))
        urls = [c.args[0] for c in session.get.call_args_list]
        self.assertEqual(len(urls), len(set(urls)))
        self.assertEqual(len(urls), len(taleo.SECTIONS))

    def test_request_errors_fall_through_to_next_section(self):
        session = MagicMock()
        ok = MagicMock(status_code=200, text="portal=7654321")
        session.get.side_effect = [requests.RequestException("boom"), ok]
        with patch.object(taleo, "_make_session", return_value=session):
            self.assertEqual(taleo._discover_portal_id("acme", "ext"),
                             ("7654321", taleo.SECTIONS[0]))


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
