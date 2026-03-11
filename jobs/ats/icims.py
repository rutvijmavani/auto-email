# jobs/ats/icims.py — iCIMS job board scraper
#
# iCIMS does not provide a public JSON API.
# Jobs are fetched via HTML scraping using the ?in_iframe=1 endpoint
# which returns clean HTML without JavaScript rendering.
#
# URL pattern:  careers-{slug}.icims.com/jobs/search?in_iframe=1
# Pagination:   ?pr=0&in_iframe=1, ?pr=1&in_iframe=1, ...
# Job selector: a.iCIMS_Anchor
#
# Option C freshness strategy:
#   1. Fetch listing page → extract all job IDs
#   2. Compare with DB → find new job IDs only
#   3. Fetch detail page ONLY for new jobs
#   4. Extract posted_at from JSON-LD or body text
#   5. Store in job_postings
#
# Date field: Posted Date in body text (DD/MM/YYYY HH:MM)
#             → reliable original posting date

import re
import json
import requests
from datetime import datetime
from bs4 import BeautifulSoup

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/121.0.0.0 Safari/537.36"
    ),
    "Accept":          "text/html,application/xhtml+xml",
    "Accept-Language": "en-US,en;q=0.9",
}

TIMEOUT       = 12
MAX_PAGES     = 20    # safety cap on pagination
PAGE_DELAY    = 0.3   # seconds between page requests


def fetch_jobs(slug, company):
    """
    Fetch all jobs for company from iCIMS.

    Args:
        slug:    company slug (e.g. "schwab")
        company: company name (e.g. "Charles Schwab")

    Returns:
        List of normalized job dicts.
        posted_at is None for existing jobs (Option C —
        only fetched for new jobs via fetch_job_detail).
    """
    if not slug:
        return []

    # Handle both "schwab" and "careers-schwab" slug formats
    # patterns.py strips "careers-" prefix so slug is always bare
    # but guard against both forms for safety
    if slug.startswith("careers-"):
        base_url = f"https://{slug}.icims.com"
    else:
        base_url = f"https://careers-{slug}.icims.com"
    all_jobs  = []
    seen_ids  = set()

    import time
    for page in range(MAX_PAGES):
        url  = f"{base_url}/jobs/search?pr={page}&in_iframe=1"
        jobs = _fetch_listing_page(url, base_url, company, seen_ids)

        if jobs is None:
            break  # network/HTTP error — stop pagination

        if len(jobs) == 0:
            break  # genuine end of results

        all_jobs.extend(jobs)
        time.sleep(PAGE_DELAY)

    return all_jobs


def _fetch_listing_page(url, base_url, company, seen_ids):
    """
    Fetch one listing page and extract job stubs.

    Returns:
        list of job dicts  — page fetched (may be empty = end of results)
        None               — network/HTTP error (caller should stop)
    """
    try:
        resp = requests.get(url, headers=HEADERS, timeout=TIMEOUT)

        # Non-200 = server error, not end of pages → return None
        if resp.status_code == 404:
            return []   # 404 = no jobs board exists
        if resp.status_code != 200:
            return None  # error — stop pagination

        # Detect JS redirect — company migrated away from iCIMS
        # e.g. AMD: window.top.location.href = 'https://careers.amd.com/jobs'
        # Page is tiny (<500 chars) and contains a redirect script
        if (len(resp.text) < 500 and
                "location.href" in resp.text and
                "window.top" in resp.text):
            return None  # company no longer on iCIMS — stop

        soup = BeautifulSoup(resp.text, "html.parser")
        anchors = soup.select("a.iCIMS_Anchor")

        if not anchors:
            return []   # genuine empty page = end of results

        jobs = []
        for anchor in anchors:
            raw_title = anchor.text.strip()
            href      = anchor.get("href", "")

            if not href:
                continue

            # Strip "Job Title\n" prefix that iCIMS adds
            title = _clean_title(raw_title)
            if not title:
                continue

            # Extract job ID from URL
            job_id = _extract_job_id(href)
            if not job_id or job_id in seen_ids:
                continue
            seen_ids.add(job_id)

            # Build clean job URL (without in_iframe)
            job_url = _clean_job_url(href, base_url)

            jobs.append({
                "company":     company,
                "title":       title,
                "job_url":     job_url,
                "job_id":      job_id,
                "location":    "",        # filled by fetch_job_detail
                "posted_at":   None,      # filled by fetch_job_detail
                "description": "",        # filled by fetch_job_detail
                "ats":         "icims",
                "_base_url":   base_url,  # used for detail fetch
            })

        return jobs

    except requests.exceptions.Timeout:
        return []
    except Exception:
        return []


def fetch_job_detail(job):
    """
    Fetch job detail page for a single job.
    Extracts posted_at, location, description.

    Called only for NEW jobs (Option C strategy).

    Args:
        job: job dict from fetch_jobs()

    Returns:
        Updated job dict with posted_at, location, description filled.
    """
    base_url = job.get("_base_url", "")
    job_url  = job.get("job_url", "")

    if not job_url:
        return job

    # Use in_iframe=1 for detail page too
    detail_url = job_url
    if "in_iframe=1" not in detail_url:
        sep = "&" if "?" in detail_url else "?"
        detail_url = f"{detail_url}{sep}in_iframe=1"

    try:
        resp = requests.get(detail_url, headers=HEADERS, timeout=TIMEOUT)
        if resp.status_code != 200:
            return job

        soup = BeautifulSoup(resp.text, "html.parser")
        text = soup.get_text(separator=" ", strip=True)

        # Extract posted_at from body text
        # Format: "Posted Date 3 days ago (05/03/2026 13:46)"
        posted_at = _extract_posted_date(text)

        # Try JSON-LD for richer data
        json_ld = _extract_json_ld(soup)
        if json_ld:
            # Location from JSON-LD
            location = _extract_location_from_json_ld(json_ld)
            # posted_at from JSON-LD datePosted if not found in body
            if not posted_at:
                date_posted = json_ld.get("datePosted", "")
                if date_posted:
                    try:
                        posted_at = datetime.fromisoformat(
                            date_posted.replace("Z", "+00:00")
                        )
                    except (ValueError, AttributeError):
                        pass
        else:
            location = _extract_location_from_html(soup, text)

        # Extract description
        description = _extract_description(soup)

        job = dict(job)
        job["posted_at"]   = posted_at
        job["location"]    = location or ""
        job["description"] = description or ""

        return job

    except requests.exceptions.Timeout:
        return job
    except Exception:
        return job


# ─────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────

def _clean_title(raw_title):
    """
    iCIMS prepends "Job Title\n" to anchor text.
    Strip it and clean up whitespace.
    """
    title = re.sub(r'^Job\s+Title\s*\n?\s*', '', raw_title, flags=re.IGNORECASE)
    title = re.sub(r'\s+', ' ', title).strip()
    return title


def _extract_job_id(href):
    """Extract numeric job ID from iCIMS URL."""
    m = re.search(r'/jobs/(\d+)/', href)
    return m.group(1) if m else None


def _clean_job_url(href, base_url):
    """
    Build clean job URL without in_iframe parameter.
    Ensures full URL with base domain.
    """
    # Remove in_iframe param
    url = re.sub(r'[?&]in_iframe=1', '', href).rstrip('?&')

    # Ensure full URL
    if url.startswith('http'):
        return url
    if url.startswith('/'):
        return f"{base_url}{url}"
    return f"{base_url}/{url}"


def _extract_posted_date(text):
    """
    Extract posted date from iCIMS body text.
    Format: "Posted Date 3 days ago (05/03/2026 13:46)"
    Returns datetime or None.
    """
    # Pattern: date in parentheses after "Posted Date"
    m = re.search(
        r'Posted\s+Date[^(]*\((\d{2}/\d{2}/\d{4})',
        text, re.IGNORECASE
    )
    if m:
        try:
            return datetime.strptime(m.group(1), "%d/%m/%Y")
        except ValueError:
            pass

    # Fallback: any date in DD/MM/YYYY format near "posted"
    m = re.search(
        r'[Pp]osted[^(]{0,50}(\d{2}/\d{2}/\d{4})',
        text
    )
    if m:
        try:
            return datetime.strptime(m.group(1), "%d/%m/%Y")
        except ValueError:
            pass

    return None


def _extract_json_ld(soup):
    """Extract and parse JSON-LD structured data."""
    for script in soup.select("script[type='application/ld+json']"):
        try:
            data = json.loads(script.string or "")
            if isinstance(data, dict):
                return data
        except (json.JSONDecodeError, TypeError):
            continue
    return None


def _extract_location_from_json_ld(json_ld):
    """Extract location string from JSON-LD jobLocation."""
    try:
        locations = json_ld.get("jobLocation", [])
        if not locations:
            return ""
        if isinstance(locations, dict):
            locations = [locations]
        addr = locations[0].get("address", {})
        parts = [
            addr.get("addressLocality", ""),
            addr.get("addressRegion", ""),
            addr.get("addressCountry", ""),
        ]
        return ", ".join(p for p in parts if p)
    except Exception:
        return ""


def _extract_location_from_html(soup, text):
    """Extract location from iCIMS HTML fields."""
    # Try iCIMS location field
    for sel in [".iCIMS_JobHeaderLocation", "[class*='location']"]:
        el = soup.select_one(sel)
        if el:
            loc = el.text.strip()
            if loc:
                return loc

    # Try body text: "Job Locations {location}"
    m = re.search(r'Job\s+Locations?\s+([^\n]+)', text, re.IGNORECASE)
    if m:
        return m.group(1).strip()[:100]

    return ""


def _extract_description(soup):
    """Extract job description from iCIMS detail page."""
    # iCIMS uses .iCIMS_InfoMsg sections for overview/responsibilities
    sections = soup.select(".iCIMS_InfoMsg")
    if sections:
        parts = []
        for section in sections:
            header = section.find_previous_sibling()
            text   = section.get_text(separator="\n", strip=True)
            if text:
                parts.append(text)
        if parts:
            return "\n\n".join(parts)[:5000]

    # Fallback: main content area
    main = soup.select_one("#iCIMS_Content, .iCIMS_Content, main")
    if main:
        return main.get_text(separator="\n", strip=True)[:5000]

    return ""