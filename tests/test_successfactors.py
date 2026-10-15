"""Tests for jobs/ats/successfactors.py feed parsing (malformed unnamed tags)."""
import unittest
from unittest.mock import MagicMock, patch

from jobs.ats import successfactors as sf


def _job(i, extra=""):
    return (
        "<Job>"
        f"<JobTitle>Software Engineer {i}</JobTitle>"
        f"<ReqId>{1000 + i}</ReqId>"
        "<Posted-Date>08/12/2025</Posted-Date>"
        f"{extra}"
        "<Job-Description><![CDATA[<p>desc</p>]]></Job-Description>"
        "</Job>"
    )


def _feed(n, extra=""):
    return ('<?xml version="1.0" encoding="UTF-8"?><Jobs>'
            + "".join(_job(i, extra) for i in range(n)) + "</Jobs>")


def _resp(text):
    r = MagicMock()
    r.text = text
    r.headers = {"content-type": "application/xml"}
    return r


SLUG = {"slug": "capgemitecP3", "dc": "5", "region": "eu"}


class TestSuccessFactorsParsing(unittest.TestCase):

    def test_clean_feed_parses_all_jobs(self):
        with patch.object(sf, "fetch_html", return_value=_resp(_feed(5))):
            jobs = sf.fetch_jobs(SLUG, "Capgemini")
        self.assertEqual(len(jobs), 5)

    def test_unnamed_tags_do_not_truncate_feed(self):
        feed = _feed(5, extra="<>123</><>456</>")
        with patch.object(sf, "fetch_html", return_value=_resp(feed)):
            jobs = sf.fetch_jobs(SLUG, "Capgemini")
        self.assertEqual(len(jobs), 5)
        self.assertEqual(jobs[4]["job_id"], "1004")

    def test_report_counts_distinct_usable_jobs_not_parsed_elements(self):
        # two <Job> elements share a ReqId, and one has no JobTitle (dropped as unusable)
        feed = ('<?xml version="1.0" encoding="UTF-8"?><Jobs>'
                + _job(1) + _job(1) + _job(2)
                + "<Job><ReqId>9</ReqId></Job></Jobs>")
        with patch.object(sf, "fetch_html", return_value=_resp(feed)):
            jobs = sf.fetch_jobs(SLUG, "Capgemini")
        self.assertEqual(len(jobs), 3)
        self.assertEqual(jobs.report.fetched_distinct, 2)
        self.assertEqual(jobs.report.unusable, 1)

    def test_unnamed_tags_do_not_truncate_detect_sample(self):
        feed = _feed(5, extra="<>123</>")
        with patch.object(sf, "fetch_html", return_value=_resp(feed)):
            slug_info, sample = sf.detect("Software Engineer Corp")
        self.assertIsNotNone(slug_info)
        self.assertEqual(len(sample), 3)

    def test_no_warning_when_all_jobs_parsed(self):
        with patch.object(sf, "logger") as lg:
            sf._parse_jobs(_feed(3, extra="<>1</>"), "X")
        lg.warning.assert_not_called()

    def test_warns_when_jobs_are_lost(self):
        # Simulate a parser that stops early: 3 <Job> in the text, 1 parsed.
        fake_soup = MagicMock()
        fake_soup.find_all.return_value = [object()]
        with patch.object(sf, "BeautifulSoup", return_value=fake_soup), \
             patch.object(sf, "logger") as lg:
            sf._parse_jobs(_feed(3), "X")
        lg.warning.assert_called_once()


if __name__ == "__main__":
    unittest.main()
