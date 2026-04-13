# jobs/ats/eightfold.py — Eightfold.ai career portal scraper
#
# Eightfold.ai is an AI-powered talent platform used by Nvidia, Micron,
# Starbucks, Cognizant, Genpact and others.
#
# API endpoint (confirmed from DevTools — GET, not POST):
#   GET https://{slug}.eightfold.ai/api/pcsx/search
#       ?domain={domain}&query=&location=United+States
#       &start={offset}&num={page_size}
#       &sort_by=distance&filter_include_remote=1&hl=en-US
#
# CSRF flow (required):
#   1. GET https://{slug}.eightfold.ai/careers → session cookie + CSRF token
#   2. Pass CSRF token as x-csrf-token header on all API calls
#
# Response shape:
#   data.count       → total job count
#   data.positions[] → job array
#
# Per job fields used:
#   id               → internal Eightfold job ID (used in job_url)
#   atsJobId         → ATS reference number (stored as job_id)
#   name             → job title
#   standardizedLocations[] → clean location strings
#   locations[]      → fallback full address strings
#   postedTs         → unix timestamp (seconds) → posted_at RELIABLE
#   department       → job category
#   workLocationOption → "onsite" / "remote" / "hybrid"
#   positionUrl      → relative URL e.g. "/careers/job/481077366501"
#
# Description: NOT in listing response — fetched via Option C
#   GET https://{slug}.eightfold.ai/careers/job/{id}
#   Extract from HTML using known Eightfold CSS selectors.
#
# Slug format stored in DB (JSON):
#   {"slug": "starbucks", "domain": "starbucks.com"}
#   slug   = subdomain before .eightfold.ai
#   domain = value passed to domain= API param (company's real domain)
#
# Detection:
#   Handled by patterns.py matching {slug}.eightfold.ai in job URL.
#   domain is extracted from the career page or stored from job URL domain.

import re
import time
import uuid
import json
import logging
import requests
from datetime import datetime, timezone
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────
# CONSTANTS
# ─────────────────────────────────────────

PAGE_SIZE = 25   # sent as num= param but Eightfold ignores it and uses its own
                 # default (observed: 10 jobs/page). Pagination uses actual
                 # len(positions) returned, not this constant. Kept for the param.
MAX_PAGES = 200  # safety cap: 200 × 25 = 5,000 jobs max per run

# Confirmed required from DevTools — requests fail or return empty without these
HEADERS = {
    "accept":             "application/json, text/plain, */*",
    "accept-language":    "en-US,en;q=0.9",
    "priority":           "u=1, i",
    "sec-ch-ua":          '"Not:A-Brand";v="99", "Google Chrome";v="145", "Chromium";v="145"',
    "sec-ch-ua-mobile":   "?0",
    "sec-ch-ua-platform": '"Windows"',
    "sec-fetch-dest":     "empty",
    "sec-fetch-mode":     "cors",
    "sec-fetch-site":     "same-origin",
    "user-agent":         "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36",
}


# ─────────────────────────────────────────
# FETCH JOBS
# ─────────────────────────────────────────

def fetch_jobs(slug_info, company):
    """
    Fetch all jobs from Eightfold.ai search API.

    Args:
        slug_info: JSON string or dict with:
                   {"slug": "starbucks", "domain": "starbucks.com"}
        company:   company name for normalization

    Returns:
        List of normalized job dicts.
        description is empty — filled by fetch_job_detail() (Option C).
    """
    if not slug_info:
        return []
    if isinstance(slug_info, str):
        try:
            slug_info = json.loads(slug_info)
        except (json.JSONDecodeError, TypeError):
            logger.error("eightfold: invalid slug_info JSON for %s", company)
            return []

    slug   = slug_info.get("slug", "")
    domain = slug_info.get("domain", "")

    if not slug or not domain:
        logger.error("eightfold: missing slug or domain for %s", company)
        return []

    base_url = f"https://{slug}.eightfold.ai"

    # Step 1: GET /careers to obtain session cookie + CSRF token
    session    = _make_session()
    csrf_token = _fetch_csrf_token(session, slug)

    # Step 2: Paginate through all jobs
    all_jobs = []
    start    = 0

    for page in range(MAX_PAGES):
        positions, total = _fetch_page(
            session, slug, domain, base_url, start, csrf_token
        )

        if positions is None:
            # Hard error — stop
            break

        if not positions:
            # Empty page = end of results
            break

        for pos in positions:
            job = _normalize(pos, company, slug, base_url)
            if job:
                all_jobs.append(job)

        start += len(positions)

        logger.debug(
            "eightfold: %s page %d — %d jobs fetched, %d total",
            slug, page + 1, len(all_jobs), total,
        )

        if start >= total:
            break

    logger.info("eightfold: fetched %d jobs for %s", len(all_jobs), company)
    return all_jobs


# ─────────────────────────────────────────
# FETCH JOB DETAIL
# ─────────────────────────────────────────

def fetch_job_detail(job):
    """
    Fetch job description from Eightfold detail page.

    Called only for NEW jobs (Option C strategy).
    title, location, posted_at are already populated from fetch_jobs().

    Args:
        job: job dict from fetch_jobs()

    Returns:
        Updated job dict with description filled.
    """
    job_url = job.get("job_url", "")
    if not job_url:
        return job

    try:
        resp = requests.get(
            job_url,
            headers={**HEADERS, "accept": "text/html,application/xhtml+xml,*/*"},
            timeout=12,
        )
        if resp.status_code != 200:
            return job

        soup        = BeautifulSoup(resp.text, "html.parser")
        description = _extract_description(soup)

        job              = dict(job)
        job["description"] = description
        return job

    except Exception as e:
        logger.debug("eightfold: fetch_job_detail failed for %s: %s", job_url, e)
        return job


# ─────────────────────────────────────────
# HELPERS — SESSION + CSRF
# ─────────────────────────────────────────

def _make_session():
    """Create a requests.Session with base headers."""
    session = requests.Session()
    session.headers.update(HEADERS)
    return session


def _fetch_csrf_token(session, slug):
    """
    GET /careers to obtain session cookie and CSRF token.

    Eightfold embeds the CSRF token in the page HTML.
    The session cookie set during this request is required for
    subsequent API calls — without it the API may return 401 or empty data.

    Returns token string or "" if not found (API may still work without it
    for some tenants — will proceed and let caller handle failure).
    """
    url = f"https://{slug}.eightfold.ai/careers?hl=en-US"
    try:
        resp = session.get(url, timeout=15)
        if resp.status_code != 200:
            logger.warning("eightfold: careers page returned %s for %s",
                           resp.status_code, slug)
            return ""

        # Pattern 1: meta tag
        m = re.search(
            r'<meta[^>]+name=["\']csrf-token["\'][^>]+content=["\']([^"\']+)["\']',
            resp.text, re.IGNORECASE,
        )
        if m:
            return m.group(1)

        # Pattern 2: JS variable — csrfToken: "..." or csrf_token = "..."
        m = re.search(
            r'csrf[_-]?[Tt]oken["\s:=]+["\']([A-Za-z0-9+/=._\-]{20,})["\']',
            resp.text,
        )
        if m:
            return m.group(1)

        # Pattern 3: JSON blob in page
        m = re.search(r'"csrfToken"\s*:\s*"([^"]{20,})"', resp.text)
        if m:
            return m.group(1)

        # Pattern 4: cookie
        for name, val in session.cookies.items():
            if "csrf" in name.lower() or "xsrf" in name.lower():
                return val

        logger.debug("eightfold: CSRF token not found for %s — proceeding without", slug)
        return ""

    except Exception as e:
        logger.warning("eightfold: CSRF fetch failed for %s: %s", slug, e)
        return ""


# ─────────────────────────────────────────
# HELPERS — PAGINATION
# ─────────────────────────────────────────

def _fetch_page(session, slug, domain, base_url, start, csrf_token):
    """
    Fetch one page of jobs from the Eightfold search API.

    Returns (positions_list, total_count) or (None, 0) on hard error.
    Returns ([], 0) when the page is genuinely empty (end of results).
    """
    url    = f"{base_url}/api/pcsx/search"
    params = {
        "domain":                domain,
        "query":                 "",
        "location":              "United States",
        "start":                 start,
        "num":                   PAGE_SIZE,  # server ignores this, uses its own default (~10)
        "sort_by":               "distance",
        "filter_include_remote": "1",
        "hl":                    "en-US",
    }
    headers = {
        **HEADERS,
        "referer":                f"{base_url}/careers?hl=en-US",
        "x-browser-request-time": str(time.time()),
        "x-csrf-token":           csrf_token,
        # sentry-trace: Eightfold sends this but doesn't validate the value
        "sentry-trace":           f"{uuid.uuid4().hex}-{uuid.uuid4().hex[:16]}-0",
    }

    try:
        resp = session.get(url, params=params, headers=headers, timeout=15)

        if resp.status_code == 401:
            logger.error("eightfold: 401 for %s — CSRF or session invalid", slug)
            return None, 0

        if resp.status_code != 200:
            logger.error("eightfold: HTTP %s for %s start=%d",
                         resp.status_code, slug, start)
            return None, 0

        data      = resp.json()
        inner     = data.get("data", {})
        positions = inner.get("positions", [])
        total     = int(inner.get("count", 0))

        return positions, total

    except (requests.RequestException, ValueError) as e:
        logger.error("eightfold: fetch_page error for %s start=%d: %s",
                     slug, start, e)
        return None, 0


# ─────────────────────────────────────────
# HELPERS — NORMALIZATION
# ─────────────────────────────────────────

def _normalize(pos, company, slug, base_url):
    """
    Normalize one Eightfold position dict to standard pipeline format.

    Field mapping:
      pos.name                → title
      pos.id                  → used in job_url
      pos.atsJobId            → job_id (ATS reference)
      pos.standardizedLocations → location (preferred, cleaner)
      pos.locations           → fallback location
      pos.postedTs            → posted_at (unix seconds, RELIABLE)
      pos.department          → not stored but logged
      pos.workLocationOption  → not stored but used for location enrichment
      pos.positionUrl         → relative path for job_url
    """
    title     = pos.get("name", "").strip()
    pos_id    = pos.get("id")
    ats_id    = str(pos.get("atsJobId") or pos.get("displayJobId") or pos_id or "")
    pos_url   = pos.get("positionUrl", "")
    posted_ts = pos.get("postedTs")
    wlo       = pos.get("workLocationOption", "")

    if not title or not pos_id:
        return None

    # Build full job URL from relative positionUrl
    if pos_url:
        job_url = f"{base_url}{pos_url}"
    else:
        job_url = f"{base_url}/careers/job/{pos_id}"

    # Convert unix timestamp to datetime
    posted_at = None
    if posted_ts:
        try:
            posted_at = datetime.fromtimestamp(int(posted_ts), tz=timezone.utc)
        except (ValueError, OSError):
            pass

    # Location: prefer standardizedLocations (cleaner) over raw locations
    location = _extract_location(
        pos.get("standardizedLocations", []),
        pos.get("locations", []),
        wlo,
    )

    return {
        "company":     company,
        "title":       title,
        "job_url":     job_url,
        "job_id":      ats_id,
        "location":    location,
        "posted_at":   posted_at,
        "description": "",   # filled by fetch_job_detail
        "ats":         "eightfold",
    }


def _extract_location(std_locs, raw_locs, work_option):
    """
    Pick the most useful location string.

    standardizedLocations contains clean entries like:
      ["US", "Remote"]               → remote job (but also appears on onsite!)
      ["US", "Minden, NV, US"]       → specific location
      ["US", "WA, US"]               → state-level

    Observed: Eightfold puts "Remote" in standardizedLocations even for
    onsite jobs (workLocationOption="onsite"). Use workLocationOption as
    the authoritative remote signal — not the presence of "Remote" in locs.

    Priority:
      1. Specific city/state from standardizedLocations (not US/Remote)
      2. Specific address from raw locations (not United States/Remote)
      3. "Remote" if workLocationOption == "remote"
      4. First raw location as last resort
    """
    SKIP_STD  = {"US", "United States", "Remote"}
    SKIP_RAW  = {"United States", "Remote", "US"}

    # From standardized: prefer specific city/state over vague entries
    for loc in std_locs:
        if loc and loc not in SKIP_STD:
            return loc

    # From raw locations: prefer full address over country names
    for loc in raw_locs:
        if loc and loc not in SKIP_RAW:
            # Strip trailing office codes like "(SANJOSE)"
            loc = re.sub(r"\s*\([A-Z0-9]+\)\s*$", "", loc).strip()
            if loc:
                return loc

    # workLocationOption is authoritative for remote
    if work_option == "remote":
        return "Remote"

    return raw_locs[0] if raw_locs else ""


def _extract_description(soup):
    """
    Extract job description from Eightfold detail page HTML.

    Eightfold renders job detail as a React SPA, but with SSR (server-side
    rendering) the description is present in the initial HTML inside known
    container elements.
    """
    # Confirmed selectors from Eightfold career detail pages
    for sel in [
        "[data-testid='job-description']",
        ".job-description",
        ".position-description",
        "#job-description",
        "[class*='JobDescription']",
        "[class*='job-desc']",
        ".careers-job-description",
        "section.description",
    ]:
        el = soup.select_one(sel)
        if el:
            text = el.get_text(separator="\n", strip=True)
            text = re.sub(r"\n{3,}", "\n\n", text).strip()
            if len(text) > 100:
                return text[:5000]

    # Fallback: largest content block on page
    best, best_len = "", 0
    for el in soup.select("section, article, div.content, main"):
        text = el.get_text(separator="\n", strip=True)
        text = re.sub(r"\n{3,}", "\n\n", text).strip()
        if len(text) > best_len and len(text) > 200:
            best_len = len(text)
            best     = text[:5000]

    return best