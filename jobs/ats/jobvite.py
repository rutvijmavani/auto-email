# jobs/ats/jobvite.py — Jobvite job board scraper
#
# Jobvite does not expose a public JSON API for job listings.
# Jobs are fetched via HTML scraping of the career site.
#
# URL pattern:  jobs.jobvite.com/{slug}/jobs
# Pagination:   jobs.jobvite.com/{slug}/search?c={category}&p={page}
# Job selector: table a[href*="/job/"]
#
# Option C freshness strategy:
#   1. Fetch listing page → extract all job IDs
#   2. Compare with DB → find new job IDs only
#   3. Fetch detail page ONLY for new jobs
#   4. Extract description + location from detail page
#   5. posted_at = None (Jobvite does not expose posting dates publicly)
#
# Date field: NOT AVAILABLE — Jobvite hides posting dates from public pages
#             posted_at will always be None
#             Freshness relies on URL deduplication only (first_seen)

import re
import time
from bs4 import BeautifulSoup
from jobs.ats.base import fetch_html, slugify, validate_company_match

BASE_URL      = "https://jobs.jobvite.com/{slug}/jobs"
SEARCH_URL    = "https://jobs.jobvite.com/{slug}/search"
MAX_PAGES     = 20   # safety cap on category pagination
JOB_ID_RE     = re.compile(r"/job/([A-Za-z0-9]+)(?:/|$)")


# ─────────────────────────────────────────
# DETECTION
# ─────────────────────────────────────────

def detect(company):
    """
    Try to detect if company uses Jobvite.
    Returns (slug, sample_jobs) or (None, None).

    Probes jobs.jobvite.com/{slug}/jobs for each slug variant.
    Validates by checking job anchor links are present.
    """
    for slug in slugify(company):
        url  = BASE_URL.format(slug=slug)
        resp = fetch_html(url, platform="jobvite", track=False)

        if resp is None:
            continue

        soup    = BeautifulSoup(resp.text, "html.parser")
        anchors = _extract_job_anchors(soup)

        if not anchors:
            continue

        # Validate company match using first job title
        first_title = anchors[0].get_text(strip=True)
        if not validate_company_match(first_title, company):
            # Title match may fail (job titles rarely contain company name)
            # Fall back to URL — slug must contain a company keyword
            if not validate_company_match(slug, company):
                continue

        sample = _anchors_to_stubs(anchors[:3], slug, company)
        return slug, sample

    return None, None


# ─────────────────────────────────────────
# FETCH JOBS (listing)
# ─────────────────────────────────────────

def fetch_jobs(slug, company):
    """
    Fetch all jobs for company from Jobvite.

    Strategy:
      1. Fetch /jobs listing page — gets all jobs grouped by category
      2. Follow "Show More" pagination per category via /search?c=...&p=...
      3. Deduplicate by job ID across all categories

    Args:
        slug:    company slug (e.g. "nutanix")
        company: company name (e.g. "Nutanix")

    Returns:
        List of normalized job dicts.
        posted_at is always None — Jobvite hides posting dates.
        location and description filled by fetch_job_detail().
    """
    if not slug:
        return []

    all_jobs = []
    seen_ids = set()

    # ── Step 1: Fetch main listing page ──
    url  = BASE_URL.format(slug=slug)
    resp = fetch_html(url, platform="jobvite")

    if resp is None:
        return []

    soup    = BeautifulSoup(resp.text, "html.parser")
    anchors = _extract_job_anchors(soup)

    for anchor in anchors:
        job = _anchor_to_job(anchor, slug, company)
        if job and job["job_id"] not in seen_ids:
            seen_ids.add(job["job_id"])
            all_jobs.append(job)

    # ── Step 2: Follow "Show More" category pagination ──
    # Jobvite shows ~20 jobs per category then a "Show More" link
    # /search?c={category}&p=0 returns page 0, p=1 next page, etc.
    show_more_links = soup.select("a[href*='/search?c=']")
    categories_seen = set()

    for link in show_more_links:
        href = link.get("href", "")
        m    = re.search(r"[?&]c=([^&]+)", href)
        if not m:
            continue

        category = m.group(1)
        if category in categories_seen:
            continue
        categories_seen.add(category)

        for page in range(MAX_PAGES):
            page_url  = SEARCH_URL.format(slug=slug)
            page_resp = fetch_html(
                page_url,
                params={"c": category, "p": page},
                platform="jobvite",
            )
            if page_resp is None:
                break

            page_soup    = BeautifulSoup(page_resp.text, "html.parser")
            page_anchors = _extract_job_anchors(page_soup)

            if not page_anchors:
                break

            new_on_page = 0
            for anchor in page_anchors:
                job = _anchor_to_job(anchor, slug, company)
                if job and job["job_id"] not in seen_ids:
                    seen_ids.add(job["job_id"])
                    all_jobs.append(job)
                    new_on_page += 1

            # No new jobs on this page → end of category
            if new_on_page == 0:
                break

            time.sleep(0.3)

    return all_jobs


# ─────────────────────────────────────────
# FETCH JOB DETAIL
# ─────────────────────────────────────────

def fetch_job_detail(job):
    """
    Fetch job detail page for a single job.
    Extracts location and description.
    posted_at remains None — not available on Jobvite detail pages.

    Called only for NEW jobs (Option C strategy).

    Args:
        job: job dict from fetch_jobs()

    Returns:
        Updated job dict with location and description filled.
    """
    job_url = job.get("job_url", "")
    if not job_url:
        return job

    resp = fetch_html(job_url, platform="jobvite")
    if resp is None:
        return job

    try:
        soup = BeautifulSoup(resp.text, "html.parser")

        location    = _extract_location(soup)
        description = _extract_description(soup)

        job             = dict(job)
        job["location"]    = location or ""
        job["description"] = description or ""
        # posted_at stays None — Jobvite does not expose it

        return job

    except Exception:
        return job


# ─────────────────────────────────────────
# HELPERS — HTML PARSING
# ─────────────────────────────────────────

def _extract_job_anchors(soup):
    """
    Extract all job link anchors from a Jobvite listing page.
    Jobvite renders jobs as <a href="/slug/job/{id}"> inside tables.
    Excludes nav/utility links (Apply, Back, etc).
    """
    anchors = []
    for a in soup.find_all("a", href=True):
        href = a.get("href", "")
        if not JOB_ID_RE.search(href):
            continue
        # Skip apply links (/job/{id}/apply) and other utility paths
        if "/apply" in href or "/referral" in href:
            continue
        text = a.get_text(strip=True)
        if not text or len(text) < 3:
            continue
        anchors.append(a)
    return anchors


def _extract_job_id(href):
    """Extract alphanumeric job ID from Jobvite URL."""
    m = JOB_ID_RE.search(href)
    return m.group(1) if m else None


def _clean_job_url(href, slug):
    """Build clean job URL without query params."""
    if href.startswith("http"):
        # Strip query string
        return href.split("?")[0].rstrip("/")
    if href.startswith("/"):
        return f"https://jobs.jobvite.com{href.split('?')[0].rstrip('/')}"
    return f"https://jobs.jobvite.com/{href.split('?')[0].rstrip('/')}"


def _anchor_to_job(anchor, slug, company):
    """Convert a single job anchor tag to a job stub dict."""
    href   = anchor.get("href", "")
    job_id = _extract_job_id(href)
    if not job_id:
        return None

    title = _clean_title(anchor.get_text(strip=True))
    if not title:
        return None

    # Try to extract location from sibling/parent td
    location = ""
    parent_tr = anchor.find_parent("tr")
    if parent_tr:
        tds = parent_tr.find_all("td")
        if len(tds) >= 2:
            location = tds[-1].get_text(strip=True)

    return {
        "company":     company,
        "title":       title,
        "job_url":     _clean_job_url(href, slug),
        "job_id":      job_id,
        "location":    location,
        "posted_at":   None,      # not available on Jobvite
        "description": "",        # filled by fetch_job_detail
        "ats":         "jobvite",
        "_slug":       slug,      # used for detail fetch
    }


def _anchors_to_stubs(anchors, slug, company):
    """Convert list of anchors to job stubs (used by detect())."""
    jobs = []
    for anchor in anchors:
        job = _anchor_to_job(anchor, slug, company)
        if job:
            jobs.append(job)
    return jobs


def _clean_title(raw_title):
    """Strip whitespace and normalize job title."""
    title = re.sub(r"\s+", " ", raw_title).strip()
    return title


def _extract_location(soup):
    """Extract location from Jobvite detail page."""
    heading = soup.find("h2")
    if not heading:
        return ""

    raw_parts = []
    for el in heading.find_next_siblings():
        text = el.get_text(separator=" ", strip=True)
        if not text:
            continue
        if "Apply" in text or "← Back" in text:
            break
        raw_parts.append(text)
        if len(raw_parts) == 3:
            break

    full_text = " ".join(raw_parts)
    full_text = re.sub(r"\s+", " ", full_text).strip()
    full_text = re.sub(r"Req\.Num\..*$", "", full_text, flags=re.IGNORECASE).strip()

    # Structure: "{Category} {City}, {State}"
    # Split on comma — everything after last word before comma is city
    # "Professional Services Barcelona, Spain"
    #  → before_comma = "Professional Services Barcelona"
    #  → after_comma  = "Spain"
    #  → last word before comma = "Barcelona"
    if "," in full_text:
        before_comma, after_comma = full_text.rsplit(",", 1)
        city  = before_comma.strip().rsplit(" ", 1)[-1]  # last word = city
        state = after_comma.strip()
        if city and state:
            return f"{city}, {state}"

    # Fallback: return as-is if short
    if len(full_text) < 60:
        return full_text

    return ""

def _extract_description(soup):
    """Extract job description from Jobvite detail page."""
    # Jobvite detail page uses .jv-job-detail-description or similar
    for sel in [
        ".jv-job-detail-description",
        "#job-description",
        "[class*='description']",
        "[class*='job-detail']",
        ".content",
        "article",
    ]:
        el = soup.select_one(sel)
        if el:
            text = el.get_text(separator="\n", strip=True)
            if len(text) > 100:
                return text[:5000]

    # Fallback: largest text block on page
    main = soup.select_one("main, #main, .main")
    if main:
        return main.get_text(separator="\n", strip=True)[:5000]

    return ""