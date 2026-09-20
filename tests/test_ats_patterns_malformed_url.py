"""
tests/test_ats_patterns_malformed_url.py
─────────────────────────────────────────────────────────────────────────────
  · match_ats_pattern / _decode_google_redirect tolerate URLs that urlparse
    rejects ("Invalid IPv6 URL"), as found in scraped career-page HTML
  · discover_h1b_ats._find_ats_in_html still finds a valid ATS URL that follows
    a malformed one
"""

import os
import sys
import unittest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from jobs.ats.patterns import _decode_google_redirect, match_ats_pattern


class TestMalformedUrl(unittest.TestCase):
    BAD = "http://[abc/careers"

    def test_decode_returns_input_unchanged(self):
        self.assertEqual(_decode_google_redirect(self.BAD), self.BAD)

    def test_match_returns_none(self):
        self.assertIsNone(match_ats_pattern(self.BAD))

    def test_google_redirect_still_decoded(self):
        url = "https://www.google.com/url?q=https://boards.greenhouse.io/stripe/jobs&sa=x"
        self.assertEqual(
            _decode_google_redirect(url), "https://boards.greenhouse.io/stripe/jobs"
        )

    def test_find_ats_in_html_skips_bad_url(self):
        from scripts.discover_h1b_ats import _find_ats_in_html
        html = (
            f'<a href="{self.BAD}">x</a>'
            '<a href="https://boards.greenhouse.io/stripe/jobs">y</a>'
        )
        platform, slug = _find_ats_in_html(html)
        self.assertEqual(platform, "greenhouse")
        self.assertEqual(slug, "stripe")


if __name__ == "__main__":
    unittest.main()
