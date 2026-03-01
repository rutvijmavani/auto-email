"""
tests/test_job_scraper.py — Comprehensive tests for job scraper.

Test classes:
    TestDetectPortal         — URL-based portal detection (18 tests)
    TestHelperFunctions      — _text, _extract_json_ld, _get_nested, _deep_find, _clean_text (15 tests)
    TestGreenhouseScraper    — Greenhouse API + HTML fallback (6 tests)
    TestLeverScraper         — Lever JSON API + HTML fallback (6 tests)
    TestAshbyScraper         — Ashby JSON-LD + CSS fallback (6 tests)
    TestSmartRecruitersScraper — JSON-LD + HTML fallback (5 tests)
    TestWorkdayScraper       — JSON script tags + HTML selectors (5 tests)
    TestBambooHRScraper      — CSS selectors (4 tests)
    TestGenericScraper       — JSON-LD, OpenGraph, CSS fallback chain (7 tests)
    TestJobScraperOrchestrator — JobScraper.scrape() routing + error handling (8 tests)
    TestJobFetcherIntegration — fetch_job_description + cache integration (7 tests)
    TestJobScraperLive       — Real HTTP requests (run with --live flag) (8 tests)

Usage:
    python tests/test_job_scraper.py           # all mocked tests
    python tests/test_job_scraper.py --live    # include live HTTP tests
"""

import sys
import os
import json
import unittest
from unittest.mock import patch, MagicMock, PropertyMock
from bs4 import BeautifulSoup

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import db.db as db_module

TEST_DB = "data/test_pipeline.db"
LIVE = "--live" in sys.argv

# Live job URLs — update with current active postings before running --live
LIVE_URLS = {
    "greenhouse":      "https://boards.greenhouse.io/figma/jobs/5227294004",
    "lever":           "https://jobs.lever.co/anthropic/97b39b3a-e91d-43e2-9f49-5d6de3d6c8fb",
    "ashby":           "https://jobs.ashbyhq.com/collective/54259edc-c096-481d-aa65-1f421c2acc1e",
    "smartrecruiters": "https://jobs.smartrecruiters.com/Stripe/744000041462696",
    "workday":         "https://nvidia.wd5.myworkdayjobs.com/en-US/NVIDIAExternalCareerSite/job/Senior-Software-Engineer_JR1985007",
    "bamboohr":        "https://underdog.bamboohr.com/careers/430",
    "generic":         "https://www.apple.com/careers/us/jobs/200591520",
    "lever_api":       "https://jobs.lever.co/anthropic/97b39b3a-e91d-43e2-9f49-5d6de3d6c8fb/json",
}

MIN_DESCRIPTION_LENGTH = 200


def make_soup(html):
    return BeautifulSoup(html, "html.parser")


def long_text(n=300):
    return "A" * n


# ─────────────────────────────────────────────────────────────
# TEST CLASS 1: Portal Detection
# ─────────────────────────────────────────────────────────────

class TestDetectPortal(unittest.TestCase):

    def setUp(self):
        from jobs.job_scraper import detect_portal
        self.detect = detect_portal

    def test_greenhouse_boards_subdomain(self):
        self.assertEqual(self.detect("https://boards.greenhouse.io/figma/jobs/123"), "greenhouse")

    def test_greenhouse_root_domain(self):
        self.assertEqual(self.detect("https://greenhouse.io/jobs/123"), "greenhouse")

    def test_lever_jobs_subdomain(self):
        self.assertEqual(self.detect("https://jobs.lever.co/anthropic/abc"), "lever")

    def test_lever_root_domain(self):
        self.assertEqual(self.detect("https://lever.co/company/jobs/abc"), "lever")

    def test_ashby_jobs_subdomain(self):
        self.assertEqual(self.detect("https://jobs.ashbyhq.com/collective/abc"), "ashby")

    def test_ashby_root_domain(self):
        self.assertEqual(self.detect("https://ashbyhq.com/jobs/abc"), "ashby")

    def test_smartrecruiters(self):
        self.assertEqual(self.detect("https://jobs.smartrecruiters.com/Stripe/abc"), "smartrecruiters")

    def test_workday_myworkdayjobs(self):
        self.assertEqual(self.detect("https://nvidia.wd5.myworkdayjobs.com/jobs/abc"), "workday")

    def test_workday_root(self):
        self.assertEqual(self.detect("https://company.workday.com/jobs/abc"), "workday")

    def test_bamboohr(self):
        self.assertEqual(self.detect("https://company.bamboohr.com/careers/123"), "bamboohr")

    def test_icims(self):
        self.assertEqual(self.detect("https://careers.icims.com/jobs/123"), "icims")

    def test_taleo(self):
        self.assertEqual(self.detect("https://company.taleo.net/jobs/123"), "taleo")

    def test_linkedin(self):
        self.assertEqual(self.detect("https://www.linkedin.com/jobs/view/123"), "linkedin")

    def test_indeed(self):
        self.assertEqual(self.detect("https://www.indeed.com/viewjob?jk=abc"), "indeed")

    def test_jobvite(self):
        self.assertEqual(self.detect("https://jobs.jobvite.com/company/job/abc"), "jobvite")

    def test_generic_unknown_domain(self):
        self.assertEqual(self.detect("https://www.unknowncompany.com/careers/swe"), "generic")

    def test_url_with_query_params_preserved(self):
        """Query params should not affect portal detection."""
        url = "https://jobs.ashbyhq.com/collective/abc?src=LinkedIn&utm_source=Simplify"
        self.assertEqual(self.detect(url), "ashby")

    def test_html_fallback_greenhouse(self):
        """Falls back to HTML content if URL is ambiguous."""
        html = '<html><body class="greenhouse-job">description</body></html>'
        result = self.detect("https://somecompany.com/careers/job", html)
        self.assertEqual(result, "greenhouse")


# ─────────────────────────────────────────────────────────────
# TEST CLASS 2: Helper Functions
# ─────────────────────────────────────────────────────────────

class TestHelperFunctions(unittest.TestCase):

    def test_text_returns_first_matching_selector(self):
        from jobs.job_scraper import _text
        html = '<div><h1>Job Title</h1><p class="desc">Description here</p></div>'
        soup = make_soup(html)
        result = _text(soup, ["h2", "h1", "p"])
        self.assertEqual(result, "Job Title")

    def test_text_skips_empty_elements(self):
        from jobs.job_scraper import _text
        html = '<div><h1></h1><h2>Actual Title</h2></div>'
        soup = make_soup(html)
        result = _text(soup, ["h1", "h2"])
        self.assertEqual(result, "Actual Title")

    def test_text_returns_empty_when_no_match(self):
        from jobs.job_scraper import _text
        html = '<div><p>Some content</p></div>'
        soup = make_soup(html)
        result = _text(soup, ["h1", "h2", ".title"])
        self.assertEqual(result, "")

    def test_extract_json_ld_finds_job_posting(self):
        from jobs.job_scraper import _extract_json_ld
        html = '''<html><head>
            <script type="application/ld+json">
            {"@type": "JobPosting", "title": "Backend Engineer", "description": "Build systems"}
            </script></head></html>'''
        soup = make_soup(html)
        result = _extract_json_ld(soup, "JobPosting")
        self.assertEqual(result["title"], "Backend Engineer")

    def test_extract_json_ld_handles_array(self):
        from jobs.job_scraper import _extract_json_ld
        html = '''<html><head>
            <script type="application/ld+json">
            [{"@type": "Organization"}, {"@type": "JobPosting", "title": "SWE"}]
            </script></head></html>'''
        soup = make_soup(html)
        result = _extract_json_ld(soup, "JobPosting")
        self.assertEqual(result["title"], "SWE")

    def test_extract_json_ld_returns_empty_when_not_found(self):
        from jobs.job_scraper import _extract_json_ld
        html = '<html><head></head></html>'
        soup = make_soup(html)
        result = _extract_json_ld(soup, "JobPosting")
        self.assertEqual(result, {})

    def test_extract_json_ld_handles_invalid_json(self):
        from jobs.job_scraper import _extract_json_ld
        html = '''<html><head>
            <script type="application/ld+json">INVALID JSON {{{</script>
            </head></html>'''
        soup = make_soup(html)
        result = _extract_json_ld(soup, "JobPosting")
        self.assertEqual(result, {})

    def test_get_nested_single_level(self):
        from jobs.job_scraper import _get_nested
        d = {"title": "Engineer"}
        self.assertEqual(_get_nested(d, "title"), "Engineer")

    def test_get_nested_multiple_levels(self):
        from jobs.job_scraper import _get_nested
        d = {"hiringOrganization": {"name": "Google"}}
        self.assertEqual(_get_nested(d, "hiringOrganization.name"), "Google")

    def test_get_nested_returns_none_for_missing_key(self):
        from jobs.job_scraper import _get_nested
        d = {"a": {"b": "value"}}
        self.assertIsNone(_get_nested(d, "a.c"))

    def test_deep_find_in_nested_dict(self):
        from jobs.job_scraper import _deep_find
        d = {"a": {"b": {"title": "Found it"}}}
        self.assertEqual(_deep_find(d, "title"), "Found it")

    def test_deep_find_in_list(self):
        from jobs.job_scraper import _deep_find
        d = {"items": [{"name": "first"}, {"name": "second"}]}
        self.assertEqual(_deep_find(d, "name"), "first")

    def test_deep_find_returns_none_when_missing(self):
        from jobs.job_scraper import _deep_find
        d = {"a": {"b": "value"}}
        self.assertIsNone(_deep_find(d, "nonexistent"))

    def test_clean_text_removes_extra_whitespace(self):
        from jobs.job_scraper import _clean_text
        text = "  Line 1  \n\n\n\n  Line 2  \n   Line 3   "
        result = _clean_text(text)
        self.assertNotIn("    ", result)
        self.assertIn("Line 1", result)
        self.assertIn("Line 2", result)

    def test_clean_text_preserves_content(self):
        from jobs.job_scraper import _clean_text
        text = "Backend Engineer\nPython experience required\nKubernetes knowledge preferred"
        result = _clean_text(text)
        self.assertIn("Backend Engineer", result)
        self.assertIn("Python experience required", result)


# ─────────────────────────────────────────────────────────────
# TEST CLASS 3: Greenhouse Scraper
# ─────────────────────────────────────────────────────────────

class TestGreenhouseScraper(unittest.TestCase):

    def test_scrapes_html_title(self):
        from jobs.job_scraper import GreenhouseScraper
        html = f'<html><h1>Senior Backend Engineer</h1><div id="content">{long_text()}</div></html>'
        job = GreenhouseScraper.scrape("https://boards.greenhouse.io/test/jobs/123", make_soup(html), html)
        self.assertEqual(job.title, "Senior Backend Engineer")

    def test_scrapes_html_description(self):
        from jobs.job_scraper import GreenhouseScraper
        desc = "We are hiring a backend engineer with Python and Go experience. " + long_text()
        html = f'<html><h1>SWE</h1><div id="content">{desc}</div></html>'
        job = GreenhouseScraper.scrape("https://boards.greenhouse.io/test/jobs/123", make_soup(html), html)
        self.assertGreater(len(job.description), 50)

    def test_falls_back_to_html_when_no_api_match(self):
        from jobs.job_scraper import GreenhouseScraper
        html = f'<html><h1>Engineer</h1><div class="job-description">{long_text()}</div></html>'
        job = GreenhouseScraper.scrape("https://greenhouse.io/jobs/123", make_soup(html), html)
        self.assertIsNotNone(job)
        self.assertEqual(job.portal, "greenhouse")

    @patch("requests.get")
    def test_uses_api_when_url_matches(self, mock_get):
        from jobs.job_scraper import GreenhouseScraper
        mock_response = MagicMock()
        mock_response.ok = True
        mock_response.json.return_value = {
            "title": "API Engineer",
            "company": {"name": "Testco"},
            "location": {"name": "Remote"},
            "content": "<p>Build APIs and services at scale using Python and Go.</p>",
            "departments": [{"name": "Engineering"}]
        }
        mock_get.return_value = mock_response

        html = "<html></html>"
        job = GreenhouseScraper.scrape(
            "https://boards.greenhouse.io/testco/jobs/12345",
            make_soup(html), html
        )
        self.assertEqual(job.title, "API Engineer")
        self.assertEqual(job.company, "Testco")
        self.assertIn("APIs", job.description)

    @patch("requests.get")
    def test_falls_back_to_html_when_api_fails(self, mock_get):
        from jobs.job_scraper import GreenhouseScraper
        mock_get.side_effect = Exception("Network error")
        desc = long_text(300)
        html = f'<html><h1>Fallback Engineer</h1><div id="content">{desc}</div></html>'
        job = GreenhouseScraper.scrape(
            "https://boards.greenhouse.io/testco/jobs/12345",
            make_soup(html), html
        )
        self.assertEqual(job.title, "Fallback Engineer")

    def test_portal_set_correctly(self):
        from jobs.job_scraper import GreenhouseScraper
        html = f'<html><h1>T</h1><div id="content">{long_text()}</div></html>'
        job = GreenhouseScraper.scrape("https://boards.greenhouse.io/test/jobs/1", make_soup(html), html)
        self.assertEqual(job.portal, "greenhouse")


# ─────────────────────────────────────────────────────────────
# TEST CLASS 4: Lever Scraper
# ─────────────────────────────────────────────────────────────

class TestLeverScraper(unittest.TestCase):

    @patch("requests.get")
    def test_uses_json_api(self, mock_get):
        from jobs.job_scraper import LeverScraper
        mock_response = MagicMock()
        mock_response.ok = True
        mock_response.json.return_value = {
            "title": "Backend Engineer",
            "company": "Anthropic",
            "workplaceType": "Remote",
            "categories": {"department": "Engineering", "commitment": "Full-time"},
            "lists": [
                {
                    "text": "Requirements",
                    "content": "<li>Python</li><li>Distributed systems</li>"
                }
            ],
            "descriptionBody": ""
        }
        mock_get.return_value = mock_response

        html = "<html></html>"
        job = LeverScraper.scrape("https://jobs.lever.co/anthropic/abc", make_soup(html), html)
        self.assertEqual(job.title, "Backend Engineer")
        self.assertEqual(job.company, "Anthropic")
        self.assertEqual(job.job_type, "Full-time")

    @patch("requests.get")
    def test_uses_description_body_when_no_lists(self, mock_get):
        from jobs.job_scraper import LeverScraper
        mock_response = MagicMock()
        mock_response.ok = True
        mock_response.json.return_value = {
            "title": "SWE",
            "company": "Test",
            "lists": [],
            "descriptionBody": "<p>Build scalable backend systems using Python and Kubernetes.</p>"
        }
        mock_get.return_value = mock_response
        html = "<html></html>"
        job = LeverScraper.scrape("https://jobs.lever.co/test/abc", make_soup(html), html)
        self.assertIn("Python", job.description)

    @patch("requests.get")
    def test_falls_back_to_html_when_api_fails(self, mock_get):
        from jobs.job_scraper import LeverScraper
        mock_get.side_effect = Exception("API down")
        desc = "Backend engineer role requiring Python, Go, and Kubernetes experience. " + long_text()
        html = f'<html><h2>Engineer</h2><div class="posting-description">{desc}</div></html>'
        job = LeverScraper.scrape("https://jobs.lever.co/test/abc", make_soup(html), html)
        self.assertGreater(len(job.description), 50)

    def test_html_fallback_title(self):
        from jobs.job_scraper import LeverScraper
        with patch("requests.get") as mock_get:
            mock_get.side_effect = Exception("fail")
            html = f'<html><h2 class="posting-headline">Lead Engineer</h2><div class="posting-description">{long_text()}</div></html>'
            job = LeverScraper.scrape("https://jobs.lever.co/test/abc", make_soup(html), html)
            self.assertIsNotNone(job)

    def test_portal_set_correctly(self):
        from jobs.job_scraper import LeverScraper
        with patch("requests.get") as mock_get:
            mock_get.side_effect = Exception("fail")
            html = f'<html><h2>T</h2><div class="posting-description">{long_text()}</div></html>'
            job = LeverScraper.scrape("https://jobs.lever.co/test/abc", make_soup(html), html)
            self.assertEqual(job.portal, "lever")

    @patch("requests.get")
    def test_extracts_department(self, mock_get):
        from jobs.job_scraper import LeverScraper
        mock_response = MagicMock()
        mock_response.ok = True
        mock_response.json.return_value = {
            "title": "SWE",
            "company": "Test",
            "categories": {"department": "Platform Engineering", "commitment": "Full-time"},
            "lists": [],
            "descriptionBody": "<p>Build things.</p>"
        }
        mock_get.return_value = mock_response
        html = "<html></html>"
        job = LeverScraper.scrape("https://jobs.lever.co/test/abc", make_soup(html), html)
        self.assertEqual(job.department, "Platform Engineering")


# ─────────────────────────────────────────────────────────────
# TEST CLASS 5: Ashby Scraper
# ─────────────────────────────────────────────────────────────

class TestAshbyScraper(unittest.TestCase):

    def test_extracts_from_json_ld(self):
        from jobs.job_scraper import AshbyScraper
        html = f'''<html><head>
            <script type="application/ld+json">
            {{
                "@type": "JobPosting",
                "title": "Full Stack Engineer",
                "description": "Build end to end features. {long_text()}",
                "jobLocation": {{"address": {{"addressLocality": "San Francisco"}}}}
            }}
            </script></head>
            <body><h1>Full Stack Engineer</h1></body></html>'''
        job = AshbyScraper.scrape("https://jobs.ashbyhq.com/test/abc", make_soup(html), html)
        self.assertIn("Build end to end", job.description)
        self.assertEqual(job.portal, "ashby")

    def test_falls_back_to_css_when_no_json_ld(self):
        from jobs.job_scraper import AshbyScraper
        desc = "Backend engineer with Python, Go, Kubernetes. " + long_text()
        html = f'<html><h1>SWE</h1><div class="ashby-job-posting-description">{desc}</div></html>'
        job = AshbyScraper.scrape("https://jobs.ashbyhq.com/test/abc", make_soup(html), html)
        self.assertGreater(len(job.description), 50)

    def test_falls_back_to_main_section(self):
        from jobs.job_scraper import AshbyScraper
        desc = "Senior engineer with distributed systems experience. " + long_text()
        html = f'<html><h1>Senior SWE</h1><main><section>{desc}</section></main></html>'
        job = AshbyScraper.scrape("https://jobs.ashbyhq.com/test/abc", make_soup(html), html)
        self.assertGreater(len(job.description), 50)

    def test_extracts_title(self):
        from jobs.job_scraper import AshbyScraper
        html = f'''<html><head>
            <script type="application/ld+json">
            {{"@type": "JobPosting", "title": "Platform Engineer", "description": "{long_text()}"}}
            </script></head>
            <body><h1>Platform Engineer</h1></body></html>'''
        job = AshbyScraper.scrape("https://jobs.ashbyhq.com/test/abc", make_soup(html), html)
        self.assertEqual(job.title, "Platform Engineer")

    def test_extracts_location_from_json_ld(self):
        from jobs.job_scraper import AshbyScraper
        html = f'''<html><head>
            <script type="application/ld+json">
            {{
                "@type": "JobPosting",
                "title": "SWE",
                "description": "{long_text()}",
                "jobLocation": {{"address": {{"addressLocality": "New York"}}}}
            }}
            </script></head></html>'''
        job = AshbyScraper.scrape("https://jobs.ashbyhq.com/test/abc", make_soup(html), html)
        self.assertEqual(job.location, "New York")

    def test_portal_set_correctly(self):
        from jobs.job_scraper import AshbyScraper
        html = f'<html><h1>T</h1><main><section>{long_text()}</section></main></html>'
        job = AshbyScraper.scrape("https://jobs.ashbyhq.com/test/abc", make_soup(html), html)
        self.assertEqual(job.portal, "ashby")


# ─────────────────────────────────────────────────────────────
# TEST CLASS 6: SmartRecruiters Scraper
# ─────────────────────────────────────────────────────────────

class TestSmartRecruitersScraper(unittest.TestCase):

    def test_extracts_from_json_ld(self):
        from jobs.job_scraper import SmartRecruitersScraper
        html = f'''<html><head>
            <script type="application/ld+json">
            {{
                "@type": "JobPosting",
                "title": "Backend Engineer",
                "description": "<p>Build scalable systems. {long_text()}</p>",
                "hiringOrganization": {{"name": "Stripe"}},
                "jobLocation": {{"address": {{"addressLocality": "San Francisco"}}}}
            }}
            </script></head></html>'''
        job = SmartRecruitersScraper.scrape(
            "https://jobs.smartrecruiters.com/Stripe/abc", make_soup(html), html
        )
        self.assertEqual(job.title, "Backend Engineer")
        self.assertEqual(job.company, "Stripe")
        self.assertGreater(len(job.description), 50)

    def test_falls_back_to_css_selectors(self):
        from jobs.job_scraper import SmartRecruitersScraper
        desc = "Platform engineer with Kubernetes experience required. " + long_text()
        html = f'<html><h1 class="job-title">Platform Engineer</h1><div class="job-sections">{desc}</div></html>'
        job = SmartRecruitersScraper.scrape(
            "https://jobs.smartrecruiters.com/test/abc", make_soup(html), html
        )
        self.assertGreater(len(job.description), 50)

    def test_extracts_salary_from_json_ld(self):
        from jobs.job_scraper import SmartRecruitersScraper
        html = f'''<html><head>
            <script type="application/ld+json">
            {{
                "@type": "JobPosting",
                "title": "SWE",
                "description": "{long_text()}",
                "baseSalary": {{
                    "currency": "USD",
                    "value": {{"minValue": 120000, "maxValue": 180000, "unitText": "YEAR"}}
                }}
            }}
            </script></head></html>'''
        job = SmartRecruitersScraper.scrape(
            "https://jobs.smartrecruiters.com/test/abc", make_soup(html), html
        )
        self.assertIn("120000", job.salary)

    def test_portal_set_correctly(self):
        from jobs.job_scraper import SmartRecruitersScraper
        html = f'<html><h1 class="job-title">T</h1><div class="job-sections">{long_text()}</div></html>'
        job = SmartRecruitersScraper.scrape(
            "https://jobs.smartrecruiters.com/test/abc", make_soup(html), html
        )
        self.assertEqual(job.portal, "smartrecruiters")

    def test_handles_missing_json_ld_gracefully(self):
        from jobs.job_scraper import SmartRecruitersScraper
        html = f'<html><h1>Engineer</h1><div id="job-description">{long_text()}</div></html>'
        job = SmartRecruitersScraper.scrape(
            "https://jobs.smartrecruiters.com/test/abc", make_soup(html), html
        )
        self.assertIsNotNone(job)


# ─────────────────────────────────────────────────────────────
# TEST CLASS 7: Workday Scraper
# ─────────────────────────────────────────────────────────────

class TestWorkdayScraper(unittest.TestCase):

    def test_extracts_from_json_script_tag(self):
        from jobs.job_scraper import WorkdayScraper
        data = json.dumps({
            "title": "Software Engineer",
            "jobDescription": f"<p>Build distributed systems. {long_text()}</p>"
        })
        html = f'<html><head><script type="application/json">{data}</script></head></html>'
        job = WorkdayScraper.scrape(
            "https://nvidia.wd5.myworkdayjobs.com/jobs/abc", make_soup(html), html
        )
        self.assertEqual(job.title, "Software Engineer")
        self.assertGreater(len(job.description), 50)

    def test_falls_back_to_html_selectors(self):
        from jobs.job_scraper import WorkdayScraper
        desc = "Senior engineer with Kubernetes and Python experience. " + long_text()
        html = f'''<html>
            <h1 data-automation-id="jobPostingHeader">Senior SWE</h1>
            <div data-automation-id="jobPostingDescription">{desc}</div>
        </html>'''
        job = WorkdayScraper.scrape(
            "https://company.wd5.myworkdayjobs.com/jobs/abc", make_soup(html), html
        )
        self.assertEqual(job.title, "Senior SWE")
        self.assertGreater(len(job.description), 50)

    def test_extracts_location(self):
        from jobs.job_scraper import WorkdayScraper
        html = f'''<html>
            <h1>Engineer</h1>
            <div data-automation-id="locations">Austin, TX</div>
            <div data-automation-id="jobPostingDescription">{long_text()}</div>
        </html>'''
        job = WorkdayScraper.scrape(
            "https://company.myworkdayjobs.com/jobs/abc", make_soup(html), html
        )
        self.assertEqual(job.location, "Austin, TX")

    def test_portal_set_correctly(self):
        from jobs.job_scraper import WorkdayScraper
        html = f'<html><h1>T</h1><div data-automation-id="jobPostingDescription">{long_text()}</div></html>'
        job = WorkdayScraper.scrape(
            "https://company.myworkdayjobs.com/jobs/abc", make_soup(html), html
        )
        self.assertEqual(job.portal, "workday")

    def test_handles_invalid_json_scripts_gracefully(self):
        from jobs.job_scraper import WorkdayScraper
        html = f'''<html>
            <script type="application/json">INVALID{{{{</script>
            <h1>SWE</h1>
            <div data-automation-id="jobPostingDescription">{long_text()}</div>
        </html>'''
        job = WorkdayScraper.scrape(
            "https://company.myworkdayjobs.com/jobs/abc", make_soup(html), html
        )
        self.assertIsNotNone(job)


# ─────────────────────────────────────────────────────────────
# TEST CLASS 8: BambooHR Scraper
# ─────────────────────────────────────────────────────────────

class TestBambooHRScraper(unittest.TestCase):

    def test_extracts_title(self):
        from jobs.job_scraper import BambooHRScraper
        html = f'<html><div class="BambooRich"><h2>Backend Developer</h2>{long_text()}</div></html>'
        job = BambooHRScraper.scrape(
            "https://company.bamboohr.com/careers/123", make_soup(html), html
        )
        self.assertEqual(job.title, "Backend Developer")

    def test_extracts_description(self):
        from jobs.job_scraper import BambooHRScraper
        desc = "Backend developer with Python, PostgreSQL, and Docker skills. " + long_text()
        html = f'<html><div class="BambooRich"><h2>Dev</h2>{desc}</div></html>'
        job = BambooHRScraper.scrape(
            "https://company.bamboohr.com/careers/123", make_soup(html), html
        )
        self.assertGreater(len(job.description), 50)

    def test_falls_back_to_description_selector(self):
        from jobs.job_scraper import BambooHRScraper
        desc = "Engineering role requiring distributed systems expertise. " + long_text()
        html = f'<html><h1>Engineer</h1><div id="job-description">{desc}</div></html>'
        job = BambooHRScraper.scrape(
            "https://company.bamboohr.com/careers/123", make_soup(html), html
        )
        self.assertGreater(len(job.description), 50)

    def test_portal_set_correctly(self):
        from jobs.job_scraper import BambooHRScraper
        html = f'<html><div class="BambooRich">{long_text()}</div></html>'
        job = BambooHRScraper.scrape(
            "https://company.bamboohr.com/careers/123", make_soup(html), html
        )
        self.assertEqual(job.portal, "bamboohr")


# ─────────────────────────────────────────────────────────────
# TEST CLASS 9: Generic Scraper
# ─────────────────────────────────────────────────────────────

class TestGenericScraper(unittest.TestCase):

    def test_extracts_from_json_ld(self):
        from jobs.job_scraper import GenericScraper
        html = f'''<html><head>
            <script type="application/ld+json">
            {{
                "@type": "JobPosting",
                "title": "Backend Engineer",
                "description": "Build scalable APIs. {long_text()}",
                "hiringOrganization": {{"name": "Apple"}},
                "jobLocation": {{"address": {{"addressLocality": "Cupertino"}}}}
            }}
            </script></head></html>'''
        job = GenericScraper.scrape("https://apple.com/careers/jobs/123", make_soup(html), html)
        self.assertEqual(job.title, "Backend Engineer")
        self.assertEqual(job.company, "Apple")
        self.assertEqual(job.location, "Cupertino")

    def test_falls_back_to_opengraph(self):
        from jobs.job_scraper import GenericScraper
        desc = "Engineer role at Apple. " + long_text()
        html = f'''<html><head>
            <meta property="og:title" content="iOS Engineer">
        </head>
        <body><main>{desc}</main></body></html>'''
        job = GenericScraper.scrape("https://apple.com/careers/jobs/123", make_soup(html), html)
        self.assertEqual(job.title, "iOS Engineer")

    def test_falls_back_to_css_selectors(self):
        from jobs.job_scraper import GenericScraper
        desc = "Full stack engineer with React and Python experience. " + long_text()
        html = f'<html><h1>Full Stack Engineer</h1><div class="job-description">{desc}</div></html>'
        job = GenericScraper.scrape("https://company.com/careers/swe", make_soup(html), html)
        self.assertGreater(len(job.description), 50)

    def test_extracts_company_from_domain(self):
        from jobs.job_scraper import GenericScraper
        html = f'<html><h1>Engineer</h1><main>{long_text()}</main></html>'
        job = GenericScraper.scrape("https://www.stripe.com/careers/swe", make_soup(html), html)
        self.assertIsNotNone(job.company)

    def test_extracts_employment_type_from_json_ld(self):
        from jobs.job_scraper import GenericScraper
        html = f'''<html><head>
            <script type="application/ld+json">
            {{
                "@type": "JobPosting",
                "title": "SWE",
                "description": "{long_text()}",
                "employmentType": "FULL_TIME"
            }}
            </script></head></html>'''
        job = GenericScraper.scrape("https://company.com/jobs/1", make_soup(html), html)
        self.assertEqual(job.job_type, "FULL_TIME")

    def test_extracts_salary_from_json_ld(self):
        from jobs.job_scraper import GenericScraper
        html = f'''<html><head>
            <script type="application/ld+json">
            {{
                "@type": "JobPosting",
                "title": "SWE",
                "description": "{long_text()}",
                "baseSalary": {{
                    "currency": "USD",
                    "value": {{"minValue": 100000, "maxValue": 150000, "unitText": "YEAR"}}
                }}
            }}
            </script></head></html>'''
        job = GenericScraper.scrape("https://company.com/jobs/1", make_soup(html), html)
        self.assertIn("100000", job.salary)

    def test_handles_page_with_no_structured_data(self):
        from jobs.job_scraper import GenericScraper
        html = f'<html><article><h1>Engineer</h1><p>{long_text()}</p></article></html>'
        job = GenericScraper.scrape("https://company.com/careers/swe", make_soup(html), html)
        self.assertIsNotNone(job)


# ─────────────────────────────────────────────────────────────
# TEST CLASS 10: JobScraper Orchestrator
# ─────────────────────────────────────────────────────────────

class TestJobScraperOrchestrator(unittest.TestCase):

    @patch("requests.Session.get")
    def test_routes_to_correct_scraper(self, mock_get):
        from jobs.job_scraper import JobScraper
        mock_response = MagicMock()
        mock_response.ok = True
        mock_response.status_code = 200
        mock_response.text = f'<html><h1>Engineer</h1><div id="content">{long_text()}</div></html>'
        mock_response.raise_for_status = MagicMock()
        mock_get.return_value = mock_response

        scraper = JobScraper()
        job = scraper.scrape("https://boards.greenhouse.io/test/jobs/123")
        self.assertIsNotNone(job)
        self.assertEqual(job.portal, "greenhouse")

    @patch("requests.Session.get")
    def test_returns_none_on_http_error(self, mock_get):
        import requests
        from jobs.job_scraper import JobScraper
        mock_get.side_effect = requests.HTTPError("404 Not Found")
        scraper = JobScraper()
        job = scraper.scrape("https://boards.greenhouse.io/test/jobs/999")
        self.assertIsNone(job)

    @patch("requests.Session.get")
    def test_returns_none_on_generic_exception(self, mock_get):
        from jobs.job_scraper import JobScraper
        mock_get.side_effect = Exception("Connection timeout")
        scraper = JobScraper()
        job = scraper.scrape("https://boards.greenhouse.io/test/jobs/999")
        self.assertIsNone(job)

    @patch("requests.Session.get")
    def test_cleans_whitespace_in_description(self, mock_get):
        from jobs.job_scraper import JobScraper
        mock_response = MagicMock()
        mock_response.ok = True
        mock_response.status_code = 200
        mock_response.text = f'<html><h1>SWE</h1><div id="content">  {"Line\n\n\n\nLine " * 50}  </div></html>'
        mock_response.raise_for_status = MagicMock()
        mock_get.return_value = mock_response

        scraper = JobScraper()
        job = scraper.scrape("https://boards.greenhouse.io/test/jobs/123")
        if job and job.description:
            self.assertNotIn("\n\n\n\n", job.description)

    @patch("requests.Session.get")
    def test_scrape_many_returns_multiple(self, mock_get):
        from jobs.job_scraper import JobScraper
        mock_response = MagicMock()
        mock_response.ok = True
        mock_response.status_code = 200
        mock_response.text = f'<html><h1>SWE</h1><div id="content">{long_text()}</div></html>'
        mock_response.raise_for_status = MagicMock()
        mock_get.return_value = mock_response

        scraper = JobScraper(delay=0)
        urls = [
            "https://boards.greenhouse.io/test/jobs/1",
            "https://boards.greenhouse.io/test/jobs/2",
        ]
        jobs = scraper.scrape_many(urls)
        self.assertEqual(len(jobs), 2)

    @patch("requests.Session.get")
    def test_uses_generic_scraper_for_unknown_portal(self, mock_get):
        from jobs.job_scraper import JobScraper
        mock_response = MagicMock()
        mock_response.ok = True
        mock_response.status_code = 200
        mock_response.text = f'<html><h1>Engineer</h1><div class="job-description">{long_text()}</div></html>'
        mock_response.raise_for_status = MagicMock()
        mock_get.return_value = mock_response

        scraper = JobScraper()
        job = scraper.scrape("https://www.unknowncompany.com/careers/swe")
        self.assertIsNotNone(job)
        self.assertEqual(job.portal, "generic")

    @patch("requests.Session.get")
    def test_job_posting_has_url(self, mock_get):
        from jobs.job_scraper import JobScraper
        mock_response = MagicMock()
        mock_response.ok = True
        mock_response.status_code = 200
        mock_response.text = f'<html><h1>SWE</h1><div id="content">{long_text()}</div></html>'
        mock_response.raise_for_status = MagicMock()
        mock_get.return_value = mock_response

        scraper = JobScraper()
        url = "https://boards.greenhouse.io/test/jobs/123"
        job = scraper.scrape(url)
        self.assertEqual(job.url, url)

    def test_job_posting_to_dict(self):
        from jobs.job_scraper import JobPosting
        job = JobPosting(
            url="https://test.com",
            title="SWE",
            company="Test",
            description="Build things.",
            portal="greenhouse"
        )
        d = job.to_dict()
        self.assertEqual(d["title"], "SWE")
        self.assertNotIn("raw_html", d)


# ─────────────────────────────────────────────────────────────
# TEST CLASS 11: Job Fetcher Integration
# ─────────────────────────────────────────────────────────────

class TestJobFetcherIntegration(unittest.TestCase):

    def setUp(self):
        db_module.DB_FILE = TEST_DB
        if os.path.exists(TEST_DB):
            os.remove(TEST_DB)
        db_module.init_db()

    def tearDown(self):
        if os.path.exists(TEST_DB):
            os.remove(TEST_DB)

    def test_returns_none_for_empty_url(self):
        from jobs.job_fetcher import fetch_job_description
        result = fetch_job_description("")
        self.assertIsNone(result)

    def test_returns_none_for_none_url(self):
        from jobs.job_fetcher import fetch_job_description
        result = fetch_job_description(None)
        self.assertIsNone(result)

    def test_returns_cached_result_without_calling_scraper(self):
        from jobs.job_fetcher import fetch_job_description
        content = "Job Title: SWE\nDescription: " + long_text()
        db_module.save_job("https://test.com/jobs/cached", content)

        with patch("jobs.job_fetcher.scraper") as mock_scraper:
            result = fetch_job_description("https://test.com/jobs/cached")
            mock_scraper.scrape.assert_not_called()

        self.assertIsNotNone(result)

    def test_does_not_cache_short_descriptions(self):
        from jobs.job_fetcher import fetch_job_description
        with patch("jobs.job_fetcher.scraper") as mock_scraper:
            mock_job = MagicMock()
            mock_job.title = "SWE"
            mock_job.description = "Too short"
            mock_job.company = mock_job.location = mock_job.job_type = ""
            mock_job.department = mock_job.salary = ""
            mock_scraper.scrape.return_value = mock_job

            result = fetch_job_description("https://test.com/jobs/short")

        self.assertIsNone(result)
        self.assertIsNone(db_module.get_job("https://test.com/jobs/short"))

    def test_caches_valid_description(self):
        from jobs.job_fetcher import fetch_job_description
        desc = "Backend engineer role. " + long_text()
        with patch("jobs.job_fetcher.scraper") as mock_scraper:
            mock_job = MagicMock()
            mock_job.title = "Backend Engineer"
            mock_job.description = desc
            mock_job.company = "Test"
            mock_job.location = "Remote"
            mock_job.job_type = "Full-time"
            mock_job.department = ""
            mock_job.salary = ""
            mock_scraper.scrape.return_value = mock_job

            fetch_job_description("https://test.com/jobs/valid")

        cached = db_module.get_job("https://test.com/jobs/valid")
        self.assertIsNotNone(cached)
        self.assertIn("Backend Engineer", cached)

    def test_uses_playwright_for_ashby(self):
        from jobs.job_fetcher import fetch_job_description
        with patch("jobs.job_fetcher.scraper") as mock_scraper:
            mock_job = MagicMock()
            mock_job.title = "Full Stack Engineer"
            mock_job.description = "Build features end to end. " + long_text()
            mock_job.company = mock_job.location = mock_job.job_type = ""
            mock_job.department = mock_job.salary = ""
            mock_scraper.scrape.return_value = mock_job

            fetch_job_description("https://jobs.ashbyhq.com/collective/abc")
            mock_scraper.scrape.assert_called_once_with(
                "https://jobs.ashbyhq.com/collective/abc",
                use_playwright=True
            )

    def test_returns_none_when_scraper_returns_none(self):
        from jobs.job_fetcher import fetch_job_description
        with patch("jobs.job_fetcher.scraper") as mock_scraper:
            mock_scraper.scrape.return_value = None
            result = fetch_job_description("https://boards.greenhouse.io/test/jobs/999")

        self.assertIsNone(result)


# ─────────────────────────────────────────────────────────────
# TEST CLASS 12: Live Tests
# ─────────────────────────────────────────────────────────────

@unittest.skipUnless(LIVE, "Skipping live tests. Run with --live to enable.")
class TestJobScraperLive(unittest.TestCase):
    """
    Live tests — make real HTTP requests to actual job postings.
    Run with: python tests/test_job_scraper.py --live
    Update LIVE_URLS at top of file with current active postings before running.
    """

    def setUp(self):
        db_module.DB_FILE = TEST_DB
        if os.path.exists(TEST_DB):
            os.remove(TEST_DB)
        db_module.init_db()
        from jobs.job_scraper import JobScraper
        self.scraper = JobScraper()

    def tearDown(self):
        if os.path.exists(TEST_DB):
            os.remove(TEST_DB)

    def _assert_valid_job(self, job, portal):
        self.assertIsNotNone(job, f"Scraper returned None for {portal}")
        self.assertIsNotNone(job.description, f"{portal} has no description")
        self.assertGreater(
            len(job.description), MIN_DESCRIPTION_LENGTH,
            f"{portal} description too short ({len(job.description)} chars)"
        )
        print(f"[OK] LIVE: {portal} | title: {job.title[:50]}")
        print(f"     Description: {len(job.description)} chars")

    def test_greenhouse_live(self):
        job = self.scraper.scrape(LIVE_URLS["greenhouse"])
        self._assert_valid_job(job, "greenhouse")

    def test_lever_live(self):
        job = self.scraper.scrape(LIVE_URLS["lever"])
        self._assert_valid_job(job, "lever")

    def test_ashby_live(self):
        job = self.scraper.scrape(LIVE_URLS["ashby"], use_playwright=True)
        self._assert_valid_job(job, "ashby")

    def test_smartrecruiters_live(self):
        job = self.scraper.scrape(LIVE_URLS["smartrecruiters"])
        self._assert_valid_job(job, "smartrecruiters")

    def test_workday_live(self):
        job = self.scraper.scrape(LIVE_URLS["workday"], use_playwright=True)
        self._assert_valid_job(job, "workday")

    def test_bamboohr_live(self):
        job = self.scraper.scrape(LIVE_URLS["bamboohr"])
        self._assert_valid_job(job, "bamboohr")

    def test_generic_live(self):
        job = self.scraper.scrape(LIVE_URLS["generic"])
        self._assert_valid_job(job, "generic")

    def test_lever_json_api_live(self):
        job = self.scraper.scrape(LIVE_URLS["lever_api"])
        self._assert_valid_job(job, "lever_api")


if __name__ == "__main__":
    if "--live" in sys.argv:
        sys.argv.remove("--live")

    print("=" * 60)
    print("Job Scraper Test Suite")
    print(f"Mode: {'LIVE + MOCKED' if LIVE else 'MOCKED ONLY'}")
    print("Run with --live to include real HTTP tests")
    print("=" * 60)
    unittest.main(verbosity=2)