"""
Dynamic Job Description Scraper
Supports: Greenhouse, Workday, Lever, Ashby, SmartRecruiters, BambooHR, 
          Indeed, LinkedIn (basic), and generic job pages.

Install dependencies:
    pip install requests beautifulsoup4 selenium playwright lxml

For Playwright (handles JS-heavy sites like Workday):
    playwright install chromium
"""

import re
import json
import time
import logging
from dataclasses import dataclass, field
from typing import Optional
from urllib.parse import urlparse , urlunparse

import requests
from bs4 import BeautifulSoup

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────
# Data Model
# ─────────────────────────────────────────────

@dataclass
class JobPosting:
    url: str
    title: str = ""
    company: str = ""
    location: str = ""
    description: str = ""
    job_type: str = ""          # Full-time, Part-time, Contract, etc.
    salary: str = ""
    department: str = ""
    portal: str = "unknown"
    raw_html: str = field(default="", repr=False)

    def to_dict(self):
        return {k: v for k, v in self.__dict__.items() if k != "raw_html"}

    def __str__(self):
        return (
            f"{'='*60}\n"
            f"Portal:      {self.portal}\n"
            f"Title:       {self.title}\n"
            f"Company:     {self.company}\n"
            f"Location:    {self.location}\n"
            f"Job Type:    {self.job_type}\n"
            f"Salary:      {self.salary}\n"
            f"Description: {self.description[:300]}...\n"
            f"URL:         {self.url}\n"
            f"{'='*60}"
        )


# ─────────────────────────────────────────────
# Portal Detectors
# ─────────────────────────────────────────────

def detect_portal(url: str, html: str = "") -> str:
    """Detect which job portal a URL belongs to."""
    domain = urlparse(url).netloc.lower()

    rules = {
        "greenhouse":      ["greenhouse.io", "boards.greenhouse.io"],
        "workday":         ["myworkdayjobs.com", "workday.com"],
        "lever":           ["jobs.lever.co", "lever.co"],
        "ashby":           ["jobs.ashbyhq.com", "ashbyhq.com"],
        "smartrecruiters": ["jobs.smartrecruiters.com", "smartrecruiters.com"],
        "bamboohr":        ["bamboohr.com"],
        "icims":           ["icims.com", "careers.icims.com"],
        "taleo":           ["taleo.net"],
        "indeed":          ["indeed.com"],
        "linkedin":        ["linkedin.com"],
        "jobvite":         ["jobs.jobvite.com", "jobvite.com"],
        "breezy":          ["breezy.hr"],
        "recruitee":       ["recruitee.com"],
        "dover":           ["dover.com", "jobs.dover.com"],
    }

    for portal, patterns in rules.items():
        if any(p in domain for p in patterns):
            return portal

    # Fallback: check HTML for clues
    if html:
        if "greenhouse" in html.lower():
            return "greenhouse"
        if "workday" in html.lower():
            return "workday"
        if "lever" in html.lower():
            return "lever"

    return "generic"


# ─────────────────────────────────────────────
# Portal-Specific Scrapers (Static HTML)
# ─────────────────────────────────────────────

class GreenhouseScraper:
    """Greenhouse boards use a consistent JSON API."""

    @staticmethod
    def scrape(url: str, soup: BeautifulSoup, html: str) -> JobPosting:
        job = JobPosting(url=url, portal="greenhouse")

        # Try the JSON API first: boards.greenhouse.io/company/jobs/ID
        api_match = re.search(r"greenhouse\.io/(\w+)/jobs/(\d+)", url)
        if api_match:
            company_slug, job_id = api_match.groups()
            try:
                api_url = f"https://boards-api.greenhouse.io/v1/boards/{company_slug}/jobs/{job_id}"
                resp = requests.get(api_url, timeout=10)
                if resp.ok:
                    data = resp.json()
                    job.title = data.get("title", "")
                    job.company = data.get("company", {}).get("name", company_slug)
                    job.location = data.get("location", {}).get("name", "")
                    job.description = BeautifulSoup(
                        data.get("content", ""), "html.parser"
                    ).get_text(separator="\n", strip=True)
                    job.department = ", ".join(
                        d.get("name", "") for d in data.get("departments", [])
                    )
                    return job
            except Exception as e:
                logger.warning(f"Greenhouse API failed, falling back to HTML: {e}")

        # HTML fallback
        job.title = _text(soup, ["h1.app-title", "h1", ".job-title"])
        job.location = _text(soup, [".location", ".job-location", "[class*='location']"])
        job.description = _text(soup, ["#content", ".content", ".job-description", "#job-description"])
        return job


class LeverScraper:
    """Lever has a clean JSON API at /json endpoint."""

    @staticmethod
    def scrape(url: str, soup: BeautifulSoup, html: str) -> JobPosting:
        job = JobPosting(url=url, portal="lever")

        #json_url = url.rstrip("/") + "/json" if "/json" not in url else url
        parsed = urlparse(url)
        if parsed.path.rstrip("/").endswith("/json"):
            json_url = url
        else:
            json_path = parsed.path.rstrip("/") + "/json"
            json_url = urlunparse(parsed._replace(path=json_path, query="", fragment=""))

        try:
            resp = requests.get(json_url, timeout=10)
            if resp.ok:
                data = resp.json()
                job.title = data.get("title", "")
                job.company = data.get("company", "")
                job.location = data.get("workplaceType", "") or _get_nested(data, "categories.location")
                job.department = _get_nested(data, "categories.department") or ""
                job.job_type = _get_nested(data, "categories.commitment") or ""

                # Description from lists
                lists = data.get("lists", [])
                parts = [f"{lst['text']}:\n" + "\n".join(
                    BeautifulSoup(item, "html.parser").get_text()
                    for item in lst.get("content", "").split("</li>") if item.strip()
                ) for lst in lists]
                job.description = "\n\n".join(parts) if parts else BeautifulSoup(
                    data.get("descriptionBody", data.get("description", "")), "html.parser"
                ).get_text(separator="\n", strip=True)
                return job
        except Exception as e:
            logger.warning(f"Lever JSON API failed: {e}")

        # HTML fallback
        job.title = _text(soup, [".posting-headline h2", "h2", "h1"])
        job.location = _text(soup, [".posting-categories .location", ".location"])
        job.description = _text(soup, [".posting-description", ".section-wrapper"])
        return job


class AshbyScraper:
    @staticmethod
    def scrape(url: str, soup: BeautifulSoup, html: str) -> JobPosting:
        job = JobPosting(url=url, portal="ashby")
        job.title = _text(soup, ["h1", ".ashby-job-posting-heading"])
        job.location = _text(soup, [".ashby-job-posting-brief-item", "[class*='location']"])

        # Try JSON-LD first (most reliable for Ashby)
        ld_data = _extract_json_ld(soup, "JobPosting")
        if ld_data:
            job.title = job.title or ld_data.get("title", "")
            job.description = ld_data.get("description", "")
            job.location = job.location or ld_data.get("jobLocation", {}).get("address", {}).get("addressLocality", "")

        # Fallback to CSS selectors
        if not job.description:
            job.description = _text(soup, [
                ".ashby-job-posting-description",
                "[class*='jobDescription']",
                "[class*='description']",
                "[data-testid*='description']",
                "main section",
                "article",
            ])

        return job


class SmartRecruitersScraper:
    @staticmethod
    def scrape(url: str, soup: BeautifulSoup, html: str) -> JobPosting:
        job = JobPosting(url=url, portal="smartrecruiters")
        # Try embedded JSON-LD
        ld_data = _extract_json_ld(soup, "JobPosting")
        if ld_data:
            job.title = ld_data.get("title", "")
            job.company = _get_nested(ld_data, "hiringOrganization.name") or ""
            job.location = _get_nested(ld_data, "jobLocation.address.addressLocality") or ""
            job.description = BeautifulSoup(
                ld_data.get("description", ""), "html.parser"
            ).get_text(separator="\n", strip=True)
            job.salary = _extract_salary_from_ld(ld_data)
            return job

        job.title = _text(soup, ["h1.job-title", "h1", ".jobTitle"])
        job.location = _text(soup, [".job-details li", ".location"])
        job.description = _text(soup, [".job-sections", "#job-description", ".description"])
        return job


class WorkdayScraper:
    """
    Workday uses heavy JavaScript rendering.
    This static scraper works for cached/simple pages.
    For full support, use the PlaywrightScraper below.
    """

    @staticmethod
    def scrape(url: str, soup: BeautifulSoup, html: str) -> JobPosting:
        job = JobPosting(url=url, portal="workday")

        # Workday embeds data in script tags as JSON
        scripts = soup.find_all("script", type="application/json")
        for script in scripts:
            try:
                data = json.loads(script.string or "")
                if isinstance(data, dict):
                    title = (data.get("title") or data.get("jobTitle") or
                             _deep_find(data, "title"))
                    if title:
                        job.title = title
                    desc = _deep_find(data, "jobDescription") or _deep_find(data, "description")
                    if desc:
                        job.description = BeautifulSoup(desc, "html.parser").get_text(
                            separator="\n", strip=True
                        )
            except Exception:
                continue

        # HTML selectors
        if not job.title:
            job.title = _text(soup, [
                "[data-automation-id='jobPostingHeader']",
                "h1",
                ".job-title"
            ])
        if not job.description:
            job.description = _text(soup, [
                "[data-automation-id='jobPostingDescription']",
                ".job-description",
                "main"
            ])
        job.location = _text(soup, [
            "[data-automation-id='locations']",
            ".location",
            "[class*='location']"
        ])
        return job


class BambooHRScraper:
    @staticmethod
    def scrape(url: str, soup: BeautifulSoup, html: str) -> JobPosting:
        job = JobPosting(url=url, portal="bamboohr")
        job.title = _text(soup, [".BambooRich h2", "h1", ".job-title"])
        job.location = _text(soup, [
            "span[data-testid='joblisting-location']",
            ".job-location",
            ".location"
        ])
        job.description = _text(soup, [
            ".BambooRich",
            "#job-description",
            "[class*='description']"
        ])
        return job


class GenericScraper:
    """
    Fallback scraper using JSON-LD, OpenGraph, and common CSS patterns.
    Works surprisingly well on most company career pages.
    """

    TITLE_SELECTORS = [
        "h1.job-title", "h1.posting-title", ".job-title h1",
        "[class*='job-title']", "[class*='jobTitle']",
        "[class*='position-title']", "h1", "title"
    ]
    LOCATION_SELECTORS = [
        "[class*='location']", "[class*='Location']",
        "[itemprop='jobLocation']", ".job-location",
        "[class*='city']", "[data-testid*='location']"
    ]
    DESCRIPTION_SELECTORS = [
        "[class*='job-description']", "[class*='jobDescription']",
        "[class*='job-details']", "[class*='jobDetails']",
        "[id*='job-description']", "[id*='jobDescription']",
        "[class*='description']", "article", "main",
        ".content", "#content"
    ]

    @staticmethod
    def scrape(url: str, soup: BeautifulSoup, html: str) -> JobPosting:
        job = JobPosting(url=url, portal="generic")

        # 1. Try JSON-LD structured data (most reliable when present)
        ld_data = _extract_json_ld(soup, "JobPosting")
        if ld_data:
            job.title = ld_data.get("title", "")
            job.company = _get_nested(ld_data, "hiringOrganization.name") or ""
            loc = ld_data.get("jobLocation", {})
            if isinstance(loc, list):
                loc = loc[0] if loc else {}
            job.location = _get_nested(loc, "address.addressLocality") or \
                           _get_nested(loc, "address.addressRegion") or ""
            raw_desc = ld_data.get("description", "")
            job.description = BeautifulSoup(raw_desc, "html.parser").get_text(
                separator="\n", strip=True
            )
            job.job_type = ld_data.get("employmentType", "")
            job.salary = _extract_salary_from_ld(ld_data)
            if job.title and job.description:
                return job

        # 2. OpenGraph / meta tags
        og_title = soup.find("meta", property="og:title")
        if og_title and not job.title:
            job.title = og_title.get("content", "")

        # 3. CSS selector fallback
        if not job.title:
            job.title = _text(soup, GenericScraper.TITLE_SELECTORS)
        if not job.location:
            job.location = _text(soup, GenericScraper.LOCATION_SELECTORS)
        if not job.description:
            job.description = _text(soup, GenericScraper.DESCRIPTION_SELECTORS)

        # 4. Extract company from domain
        if not job.company:
            domain = urlparse(url).netloc.replace("www.", "").replace("jobs.", "")
            job.company = domain.split(".")[0].title()

        return job


# ─────────────────────────────────────────────
# Playwright Scraper (JS-heavy sites like Workday)
# ─────────────────────────────────────────────

class PlaywrightScraper:
    """
    Uses Playwright for JavaScript-rendered pages.
    Activate by passing use_playwright=True to JobScraper.scrape().

    Install: pip install playwright && playwright install chromium
    """

    @staticmethod
    def fetch_html(url: str, wait_selector: str = "body", timeout: int = 30000) -> str:
        try:
            from playwright.sync_api import sync_playwright
        except ImportError:
            raise ImportError(
                "Playwright not installed. Run: pip install playwright && playwright install chromium"
            )

        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            context = browser.new_context(
                user_agent=(
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/120.0.0.0 Safari/537.36"
                )
            )
            page = context.new_page()
            page.goto(url, wait_until="networkidle", timeout=timeout)

            # Extra waits for known slow portals
            portal = detect_portal(url)
            if portal == "workday":
                try:
                    page.wait_for_selector(
                        "[data-automation-id='jobPostingDescription']",
                        timeout=15000
                    )
                except Exception:
                    pass
            elif portal == "lever":
                try:
                    page.wait_for_selector(".posting-description", timeout=10000)
                except Exception:
                    pass
            elif portal == "ashby":
                try:
                    page.wait_for_selector(
                        ".ashby-job-posting-description, [class*='description'], main section",
                        timeout=10000
                    )
                except Exception:
                    pass
            else:
                page.wait_for_selector(wait_selector, timeout=timeout)

            html = page.content()
            browser.close()
            return html


# ─────────────────────────────────────────────
# Main Scraper Orchestrator
# ─────────────────────────────────────────────

PORTAL_SCRAPER_MAP = {
    "greenhouse":      GreenhouseScraper,
    "lever":           LeverScraper,
    "ashby":           AshbyScraper,
    "smartrecruiters": SmartRecruitersScraper,
    "workday":         WorkdayScraper,
    "bamboohr":        BambooHRScraper,
}

DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}


class JobScraper:
    def __init__(self, delay: float = 1.5, timeout: int = 15):
        self.delay = delay
        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers.update(DEFAULT_HEADERS)

    def scrape(self, url: str, use_playwright: bool = False) -> Optional[JobPosting]:
        """
        Scrape a job posting from any supported URL.

        Args:
            url:             Direct link to the job posting.
            use_playwright:  Set True for JS-heavy pages (Workday, some LinkedIn, etc.)
        """
        logger.info(f"Scraping: {url}")

        try:
            if use_playwright:
                html = PlaywrightScraper.fetch_html(url)
            else:
                response = self.session.get(url, timeout=self.timeout)
                response.raise_for_status()
                html = response.text
                time.sleep(self.delay)

            soup = BeautifulSoup(html, "lxml")
            portal = detect_portal(url, html)
            logger.info(f"Detected portal: {portal}")

            scraper_class = PORTAL_SCRAPER_MAP.get(portal, GenericScraper)
            job = scraper_class.scrape(url, soup, html)

            # Clean up whitespace
            job.description = _clean_text(job.description)
            job.title = job.title.strip()
            job.location = job.location.strip()

            return job

        except requests.HTTPError as e:
            logger.error(f"HTTP error for {url}: {e}")
        except Exception as e:
            logger.error(f"Failed to scrape {url}: {e}")

        return None

    def scrape_many(
        self,
        urls: list[str],
        use_playwright: bool = False
    ) -> list[JobPosting]:
        """Scrape multiple job URLs with rate limiting."""
        results = []
        for url in urls:
            job = self.scrape(url, use_playwright=use_playwright)
            if job:
                results.append(job)
            time.sleep(self.delay)
        return results


# ─────────────────────────────────────────────
# Utility Helpers
# ─────────────────────────────────────────────

def _text(soup: BeautifulSoup, selectors: list[str]) -> str:
    """Try selectors in order, return first non-empty text."""
    for sel in selectors:
        try:
            el = soup.select_one(sel)
            if el:
                text = el.get_text(separator="\n", strip=True)
                if text:
                    return text
        except Exception:
            continue
    return ""


def _extract_json_ld(soup: BeautifulSoup, schema_type: str) -> dict:
    """Extract first JSON-LD block matching a schema type."""
    for script in soup.find_all("script", type="application/ld+json"):
        try:
            data = json.loads(script.string or "")
            if isinstance(data, list):
                data = next((d for d in data if d.get("@type") == schema_type), {})
            if data.get("@type") == schema_type:
                return data
        except Exception:
            continue
    return {}


def _get_nested(d: dict, path: str):
    """Get nested dict value using dot notation."""
    keys = path.split(".")
    for key in keys:
        if isinstance(d, dict):
            d = d.get(key)
        else:
            return None
    return d


def _deep_find(d, target_key: str):
    """Recursively find first value for a key in nested dict."""
    if isinstance(d, dict):
        if target_key in d:
            return d[target_key]
        for v in d.values():
            result = _deep_find(v, target_key)
            if result is not None:
                return result
    elif isinstance(d, list):
        for item in d:
            result = _deep_find(item, target_key)
            if result is not None:
                return result
    return None


def _extract_salary_from_ld(ld_data: dict) -> str:
    salary = ld_data.get("baseSalary", {})
    if not salary:
        return ""
    value = salary.get("value", {})
    if isinstance(value, dict):
        min_val = value.get("minValue", "")
        max_val = value.get("maxValue", "")
        currency = salary.get("currency", "USD")
        unit = value.get("unitText", "")
        if min_val and max_val:
            return f"{currency} {min_val}–{max_val} {unit}".strip()
    return str(salary)


def _clean_text(text: str) -> str:
    """Remove excessive whitespace from scraped text."""
    lines = [line.strip() for line in text.splitlines()]
    lines = [line for line in lines if line]
    # Collapse more than 2 consecutive blank lines
    result, blanks = [], 0
    for line in lines:
        if not line:
            blanks += 1
            if blanks <= 2:
                result.append(line)
        else:
            blanks = 0
            result.append(line)
    return "\n".join(result)


# ─────────────────────────────────────────────
# Quick Demo
# ─────────────────────────────────────────────

# if __name__ == "__main__":
#     scraper = JobScraper()

#     # Replace these URLs with real job postings to test
#     test_urls = [
#         # Greenhouse example
#         "https://job-boards.greenhouse.io/greenhouse/jobs/7571826?gh_jid=7571826",

#         # Lever example
#         # "https://jobs.lever.co/stripe/some-job-id",

#         # Workday example (use use_playwright=True for these)
#         "https://generalmotors.wd5.myworkdayjobs.com/en-US/Careers_GM/job/Warren-Michigan-United-States-of-America/PROGRAMMER-FULL-STACK_JR-202603193?source=Indeed&jr_id=699f75fe81476f6176b9093a",

#         # Generic company career page
#         "https://careers.plansource.com/jobs/4792?lang=en-us&jr_id=697a68e58dbbf73badc78be7",
#     ]

#     for url in test_urls:
#         portal = detect_portal(url)
#         use_pw = portal in ("workday", "icims", "taleo")  # Use Playwright for JS-heavy sites
#         job = scraper.scrape(url, use_playwright=use_pw)
#         if job:
#             print(job)
#             # Save to JSON
#             with open(f"job_{job.title[:30].replace(' ', '_')}.json", "w") as f:
#                 json.dump(job.to_dict(), f, indent=2)