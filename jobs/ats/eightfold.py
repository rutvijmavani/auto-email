# jobs/ats/eightfold.py — Eightfold.ai career portal scraper
#
# Eightfold.ai is an AI-powered talent platform used by Nvidia, Micron,
# Starbucks, Cognizant, Genpact and others.
#
# API endpoint (confirmed from DevTools — GET, not POST):
#   GET https://{slug}.eightfold.ai/api/pcsx/search
#       ?domain={domain}&query=&start={offset}&num={page_size}
#       &sort_by=distance&filter_include_remote=1&hl=en-US
#
# NOTE: NO server-side location filter is sent.
#   Real browser curls show no location= param — tenants vary on which
#   location filters they accept and some return 0 results or errors
#   when an unexpected location param is present (e.g. Qualcomm, LamResearch
#   use no location filter; Starbucks accepts it but others don't).
#   We fetch ALL jobs globally and filter by country on our side using
#   _country_code extracted from standardizedLocations.
#
# NOTE: domain= param is tenant-specific:
#   Some tenants (LamResearch) require domain=lamresearch.com.
#   Others (Starbucks, Qualcomm) omit it entirely.
#   We send it when present in slug_info — API ignores unknown params.
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
#
# NOTE on API path stability:
#   /api/pcsx/search is the confirmed primary path. A small number of tenants
#   have been observed returning 404 on this path despite having valid sessions.
#   fetch_jobs() will automatically fall back to CANDIDATE_PATHS if the primary
#   path fails, and cache the working path for the remainder of the session.

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
MAX_PAGES = 2000  # safety cap: 2000 × 25 = 5,0000 jobs max per run

# Primary path — confirmed from DevTools across most Eightfold tenants.
PRIMARY_API_PATH = "/api/pcsx/search"

# Fallback paths tried in order if PRIMARY_API_PATH returns 404.
# These cover observed tenant variants and versioned rollouts.
FALLBACK_API_PATHS = [
    "/api/v2/pcsx/search",
    "/api/search",
    "/api/jobs",
]

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

    if not isinstance(slug_info, dict):
        logger.error("eightfold: invalid slug_info JSON for %s", company)
        return []

    slug   = slug_info.get("slug", "")
    stored_domain = slug_info.get("domain", "")

    if not slug:
        logger.error("eightfold: missing slug for %s", company)
        return []

    base_url = f"https://{slug}.eightfold.ai"

    # Step 1: GET /careers to obtain session cookie + CSRF token
    session    = _make_session()
    csrf_token = _fetch_csrf_token(session, slug)

    # Step 2: Discover both the working API path AND the correct domain= value.
    #
    # We cannot know beforehand whether a tenant requires domain=, accepts it,
    # or rejects it — this varies per tenant and the DB stores the eightfold
    # subdomain (e.g. "lamresearch.eightfold.ai") rather than the company domain.
    #
    # Strategy: probe candidate (api_path, domain) pairs in priority order.
    # First combination that returns HTTP 200 + at least one job wins and is
    # used for all subsequent pagination requests.
    #
    # Domain candidates tried in order:
    #   1. slug.com  — derived from slug (covers lamresearch→lamresearch.com,
    #                  qualcomm→qualcomm.com, nvidia→nvidia.com, etc.)
    #   2. ""        — omit domain= entirely (works for Starbucks, Qualcomm)
    #   3. stored_domain if it looks like a real domain (not .eightfold.ai)
    #                  — covers cases where DB is correctly populated
    api_path, domain = _resolve_api_path_and_domain(
        session, slug, stored_domain, base_url, csrf_token
    )

    if api_path is None:
        logger.error(
            "eightfold: no working (api_path, domain) combination found for %s",
            slug,
        )
        return []

    if api_path != PRIMARY_API_PATH:
        logger.warning(
            "eightfold: primary path 404 for %s — using fallback %s",
            slug, api_path,
        )

    # Step 3: Paginate through all jobs
    all_jobs = []
    start    = 0

    for page in range(MAX_PAGES):
        positions, total, _ = _fetch_page(
            session, slug, domain, base_url, start, csrf_token, api_path
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
# HELPERS — API PATH + DOMAIN RESOLUTION
# ─────────────────────────────────────────

def _candidate_domains(slug, stored_domain):
    """
    Build an ordered list of domain= values to probe for this tenant.

    CONFIRMED from live testing (starbucks, lamresearch, qualcomm):
      - domain=slug.com       → ✅ works for all three
      - domain= omitted       → 422 "Missing data for required field" on ALL tenants
      - domain=slug.eightfold.ai → 404 (what DB currently stores — always wrong)

    domain= is REQUIRED by all tested Eightfold tenants. Omitting it is never
    correct. The DB stores the eightfold subdomain which is always wrong.

    Probe order:
      1. slug.com              — derived heuristic; correct for the vast majority
                                 (lamresearch→lamresearch.com, qualcomm→qualcomm.com)
      2. stored_domain         — only if it looks like a real company domain
                                 (not .eightfold.ai); covers correctly-populated DBs
                                 and edge cases like slug≠domain (e.g. slug="sbux",
                                 domain="starbucks.com")
      3. slug.net / slug.org   — rare fallbacks for non-.com company domains

    "" (omit domain=) is intentionally excluded — confirmed to cause 422.
    """
    candidates = []

    # Candidate 1: slug.com — correct for most tenants
    slug_com = f"{slug}.com"
    candidates.append(slug_com)

    # Candidate 2: stored domain if it looks like a real company domain
    # Covers: correctly-populated DB, or slug that differs from domain
    # (e.g. slug="sbux" but real domain="starbucks.com")
    if (stored_domain
            and not stored_domain.lower().endswith(".eightfold.ai")
            and stored_domain not in candidates):
        candidates.append(stored_domain)

    # Candidates 3-4: non-.com TLDs — rare but possible
    for tld in (".net", ".org"):
        candidates.append(f"{slug}{tld}")

    return candidates


def _resolve_api_path_and_domain(session, slug, stored_domain, base_url, csrf_token):
    """
    Probe to find both the working API path and the correct domain= value.

    Iterates (api_path, domain) combinations in priority order and returns
    the first pair that yields HTTP 200 with at least one job.

    Requiring positions > 0 (not just HTTP 200) is intentional: a wrong
    domain= value may return 200 with count=0, which would be mistaken for
    "works but no jobs" rather than "wrong domain".

    Returns (api_path, domain) tuple, or (None, None) if all probes fail.

    Probe matrix (tried in this order):
      PRIMARY_API_PATH  × each domain candidate
      FALLBACK_PATH_1   × each domain candidate
      ...

    On a 4xx/5xx for a given api_path, that path is abandoned immediately
    and we move to the next — no point retrying other domains on a dead path.
    On a 422 specifically, we continue to next domain (422 = wrong domain
    value, not a broken path).
    """
    domain_candidates = _candidate_domains(slug, stored_domain)
    api_paths         = [PRIMARY_API_PATH] + FALLBACK_API_PATHS

    for api_path in api_paths:
        for domain in domain_candidates:
            positions, total, status = _fetch_page(
                session, slug, domain, base_url, start=0,
                csrf_token=csrf_token, api_path=api_path,
            )

            if status == 422:
                # Wrong domain value — try next domain candidate on same path
                logger.debug(
                    "eightfold: probe 422 (bad domain) path=%s domain=%r for %s",
                    api_path, domain, slug,
                )
                continue

            if status == 404:
                # Path itself doesn't exist for this tenant — abandon path
                logger.debug(
                    "eightfold: probe 404 (bad path) path=%s domain=%r for %s",
                    api_path, domain, slug,
                )
                break  # next api_path

            if positions is None:
                # Other hard error — abandon path
                logger.debug(
                    "eightfold: probe hard error path=%s domain=%r for %s",
                    api_path, domain, slug,
                )
                break  # next api_path

            if positions:
                # Got real jobs — this combination is confirmed working
                logger.debug(
                    "eightfold: resolved path=%s domain=%r for %s (total=%d)",
                    api_path, domain, slug, total,
                )
                return api_path, domain

            # HTTP 200, positions=[], total=0 — could be genuinely no jobs
            # or a domain that filters to 0. Try remaining domains to be safe.
            logger.debug(
                "eightfold: probe 0 jobs path=%s domain=%r for %s "
                "— trying next domain candidate",
                api_path, domain, slug,
            )

    logger.error(
        "eightfold: all (path, domain) probes exhausted for %s "
        "— paths=%s domains=%s",
        slug, api_paths, domain_candidates,
    )
    return None, None


# ─────────────────────────────────────────
# HELPERS — PAGINATION
# ─────────────────────────────────────────

def _fetch_page(session, slug, domain, base_url, start, csrf_token,
                api_path=PRIMARY_API_PATH):
    """
    Fetch one page of jobs from the Eightfold search API.

    Returns (positions, total, status_code) always.

    Callers interpret the tuple as:
      status=200, positions non-empty  → success, use results
      status=200, positions=[]         → valid response but no jobs on this page
      status=422                       → wrong domain= value (try another)
      status=404                       → api_path doesn't exist for this tenant
      status=other / positions=None    → hard error

    Returning status explicitly lets _resolve_api_path_and_domain distinguish
    422 (bad domain, try next) from 404 (bad path, abandon path entirely).
    """
    url    = f"{base_url}{api_path}"
    params = {
        "query":                 "",
        "start":                 start,
        "num":                   PAGE_SIZE,  # server ignores this, uses its own default (~10)
        "sort_by":               "distance",
        "filter_include_remote": "1",
        "hl":                    "en-US",
    }
    # domain= is REQUIRED by all tested Eightfold tenants (omitting → 422).
    # Always include it. The value is resolved by _resolve_api_path_and_domain
    # before pagination starts, so by the time we get here it's correct.
    if domain:
        params["domain"] = domain

    # NO location= filter — confirmed from real browser curls across all tenants.
    # Sending location=United+States causes 0-result responses on some tenants
    # and the production 404s. Fetch all jobs globally; filter by _country_code
    # (extracted from standardizedLocations) on our side.

    headers = {
        **HEADERS,
        "referer":                f"{base_url}/careers?hl=en-US",
        "x-browser-request-time": str(time.time()),
        "x-csrf-token":           csrf_token,
        # sentry-trace: Eightfold sends this but doesn't validate the value
        "sentry-trace":           f"{uuid.uuid4().hex}-{uuid.uuid4().hex[:16]}-0",
    }

    try:
        resp   = session.get(url, params=params, headers=headers, timeout=15)
        status = resp.status_code

        if status == 200:
            data      = resp.json()
            inner     = data.get("data", {})
            positions = inner.get("positions", [])
            total     = int(inner.get("count", 0))
            return positions, total, status

        # Non-200: log appropriately and return empty with real status code
        if status in (404, 422):
            logger.debug("eightfold: HTTP %s for %s path=%s domain=%r start=%d",
                         status, slug, api_path, domain, start)
        else:
            logger.error("eightfold: HTTP %s for %s path=%s domain=%r start=%d",
                         status, slug, api_path, domain, start)

        return None, 0, status

    except (requests.RequestException, ValueError) as e:
        logger.error("eightfold: fetch_page error for %s path=%s start=%d: %s",
                     slug, api_path, start, e)
        return None, 0, 0


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

    std_locs = pos.get("standardizedLocations", [])
    raw_locs = pos.get("locations", [])

    # Location: prefer standardizedLocations (cleaner) over raw locations
    location = _extract_location(std_locs, raw_locs, wlo)

    # Country code (Tier 1 gate)
    # standardizedLocations entries follow "City, State, CountryCode" format,
    # e.g. "Vacaville, CA, US" or "Bangalore, KA, IN".
    # The trailing alpha-2 code is authoritative — far safer than text-parsing
    # the location string where "IN" (India) conflicts with Indiana (Signal 3).
    country_code = _extract_country_code(std_locs)

    return {
        "company":       company,
        "title":         title,
        "job_url":       job_url,
        "job_id":        ats_id,
        "location":      location,
        "posted_at":     posted_at,
        "description":   "",   # filled by fetch_job_detail
        "ats":           "eightfold",
        "_country_code": country_code,
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

    return ""


def _extract_country_code(std_locs):
    """
    Extract ISO alpha-2 country code from standardizedLocations.

    Eightfold's standardizedLocations entries follow a consistent format:
      "City, State, CountryCode"  e.g. "Vacaville, CA, US"
                                       "Bangalore, KA, IN"
      "State, CountryCode"        e.g. "WA, US"
      "CountryCode"               e.g. "US"

    The country code is always the LAST comma-separated segment.
    We scan the list and return the first non-"Remote"/"" entry's last segment.

    Returns uppercase alpha-2 string (e.g. "US", "IN", "GB") or "".
    """
    SKIP = {"Remote", "United States", "US", ""}

    for loc in std_locs:
        if not loc or loc in SKIP:
            continue
        parts = [p.strip() for p in loc.split(",")]
        last  = parts[-1].upper()
        # Must be exactly 2 alphabetic characters to be a valid alpha-2 code
        if len(last) == 2 and last.isalpha():
            return last

    return ""


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