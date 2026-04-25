# jobs/ats/adp.py — ADP WorkforceNow (myjobs.adp.com) job board scraper
#
# Two-step auth-free flow (no login required):
#
#   Step 1 — Config probe (public, no auth):
#     GET https://myjobs.adp.com/public/staffing/v1/career-site/{slug}
#     → returns orgoid + myJobsToken for the company
#
#   Step 2 — Job listing API (uses myJobsToken as header):
#     GET https://my.adp.com/myadp_prefix/mycareer/public/staffing/v1/
#              job-requisitions/apply-custom-filters
#         ?$select=...&$top={n}&$skip={offset}&$filter=&tz=America/New_York
#     Headers: myjobstoken, rolecode: manager
#
# Pagination: OData $top / $skip — both must be literal (not %24-encoded)
#
# HTTP client: curl_cffi with Chrome impersonation to pass Akamai Bot Manager.
# A session is used so Akamai cookies (ak_bmsc, bm_sv) established during the
# career-page warm-up carry through to the jobs API call.
#
# All data available at listing level — no detail fetch required:
#   title       → publishedJobTitle  (falls back to jobTitle)
#   location    → requisitionLocations[0].address  (city, state, country)
#   posted_at   → postingDate  (ISO datetime)
#   description → jobDescription + jobQualifications  (full HTML → text)
#
# Slug: company slug from myjobs.adp.com URL  e.g. "apply", "scacareers"
# Stored as plain string.

import re
import json
from datetime import datetime
from bs4 import BeautifulSoup
from jobs.ats.base import slugify, validate_company_match, alpha3_to_alpha2
from logger import get_logger

try:
    from curl_cffi import requests as _requests
    from curl_cffi.requests import Session as _Session
    _IMPERSONATE = "chrome146"
    _USE_CURL_CFFI = True
except ImportError:
    import requests as _requests
    from requests import Session as _Session
    _IMPERSONATE = None
    _USE_CURL_CFFI = False

logger = get_logger(__name__)

CONFIG_URL  = "https://myjobs.adp.com/public/staffing/v1/career-site/{slug}"
JOBS_URL    = (
    "https://my.adp.com/myadp_prefix/mycareer/public/staffing/v1"
    "/job-requisitions/apply-custom-filters"
)
JOB_URL     = "https://myjobs.adp.com/{slug}/cx/job-details?reqId={req_id}"

SELECT      = (
    "reqId,jobTitle,publishedJobTitle,jobDescription,jobQualifications,"
    "workLevelCode,requisitionLocations"
    # Note: postingDate is in the schema but ADP does not return it at listing
    # level — posted_at will always be None for ADP jobs.
)
PAGE_SIZE   = 100
MAX_PAGES   = 50      # 100 × 50 = 5 000 jobs safety cap
TIMEOUT     = 15

_BASE_HEADERS = {
    "User-Agent":      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36",
    "Accept":          "application/json, text/plain, */*",
    "Accept-Language": "en-US",
    "Origin":          "https://myjobs.adp.com",
    "Referer":         "https://myjobs.adp.com/",
}

_NAV_HEADERS = {
    **_BASE_HEADERS,
    "Accept":          "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "sec-fetch-dest":  "document",
    "sec-fetch-mode":  "navigate",
    "sec-fetch-site":  "none",
}

_API_HEADERS = {
    **_BASE_HEADERS,
    "priority":        "u=1, i",
    "sec-fetch-dest":  "empty",
    "sec-fetch-mode":  "cors",
    "sec-fetch-site":  "same-site",
}


def _make_session():
    """Create a curl_cffi (or requests) session with Chrome impersonation."""
    if _USE_CURL_CFFI:
        return _Session(impersonate=_IMPERSONATE)
    return _Session()


def _warm_session(session, slug):
    """
    Visit the career page to seed Akamai Bot Manager cookies (ak_bmsc, bm_sv).
    Must be called before the first jobs API request.
    """
    try:
        session.get(
            f"https://myjobs.adp.com/{slug}/cx/job-listing",
            headers=_NAV_HEADERS,
            timeout=TIMEOUT,
        )
    except Exception as e:
        logger.debug("adp: session warm-up failed slug=%r: %s", slug, e)


# ─────────────────────────────────────────
# DETECTION
# ─────────────────────────────────────────

def detect(company):
    """
    Try to detect if company uses ADP WorkforceNow (myjobs.adp.com).

    Probes the public config endpoint for each slug variant derived from
    the company name.  A valid response confirms ADP is in use.

    Returns:
        (slug, sample_jobs)  or  (None, None)
    """
    for slug in slugify(company):
        cfg = _fetch_config(slug)
        if not cfg:
            continue

        # Quick sanity: clientName in config should loosely match company
        client_name = cfg.get("clientName", "") or cfg.get("name", "")
        if client_name and not validate_company_match(client_name, company):
            continue

        sample = _fetch_page(slug, cfg, offset=0, top=3)
        return slug, sample

    return None, None


# ─────────────────────────────────────────
# FETCH JOBS (listing)
# ─────────────────────────────────────────

def fetch_jobs(slug, company):
    """
    Fetch all jobs for company from ADP WorkforceNow.

    Step 1: fetch company config (orgoid + myJobsToken) — public, no auth.
    Step 2: paginate job-requisitions API using myJobsToken as header.
            A shared session carries Akamai cookies seeded in warm-up.

    Args:
        slug:    company slug  e.g. "scacareers"
                 Also accepts JSON {"slug": "..."} for forward compat.
        company: company name

    Returns:
        List of normalized job dicts — all fields populated at listing level.
    """
    if not slug:
        return []

    if isinstance(slug, str) and slug.strip().startswith("{"):
        try:
            slug = json.loads(slug).get("slug", slug)
        except (json.JSONDecodeError, TypeError):
            pass

    # Shared session for the entire fetch (carries Akamai cookies)
    session = _make_session()
    _warm_session(session, slug)

    # Step 1: get fresh myJobsToken
    cfg = _fetch_config(slug, session=session)
    if not cfg:
        logger.warning("adp: config fetch failed for slug=%r", slug)
        return []

    # Step 2: paginate
    all_jobs = []
    seen_ids = set()
    offset   = 0
    total    = None

    for _ in range(MAX_PAGES):
        reqs, page_total = _fetch_reqs(slug, cfg, offset, session=session)

        if reqs is None:   # request failed
            break
        if not reqs:       # empty page — done
            break

        if total is None:
            total = page_total

        for job in _parse_reqs(reqs, slug, company):
            if job["job_id"] not in seen_ids:
                seen_ids.add(job["job_id"])
                all_jobs.append(job)

        offset += len(reqs)
        if total is not None and offset >= total:
            break

    return all_jobs


# ─────────────────────────────────────────
# INTERNAL — CONFIG + API CALLS
# ─────────────────────────────────────────

def _fetch_config(slug, session=None):
    """
    Fetch company config from the public career-site endpoint.
    Returns the parsed JSON dict, or None on failure.

    The response includes:
      orgoid      — organization identifier
      myJobsToken — session token (public, rotates per request)
      clientName  — human-readable company name
    """
    url = CONFIG_URL.format(slug=slug)
    try:
        hdrs = {
            **_BASE_HEADERS,
            "sec-fetch-dest": "empty",
            "sec-fetch-mode": "cors",
            "sec-fetch-site": "same-origin",
        }
        requester = session or _requests
        kw = {"impersonate": _IMPERSONATE} if (_USE_CURL_CFFI and session is None) else {}
        resp = requester.get(url, headers=hdrs, timeout=TIMEOUT, **kw)
        if resp.status_code != 200:
            return None
        data = resp.json()
        return data if (data.get("orgoid") or data.get("myJobsToken")) else None
    except Exception as e:
        logger.debug("adp: config error slug=%r: %s", slug, e)
        return None


def _fetch_reqs(slug, cfg, offset, session=None):
    """
    Fetch one page of job requisitions.

    Returns (reqs_list, total_count) or (None, None) on error.
    """
    token = cfg.get("myJobsToken", "")

    # OData params — built as literal string to avoid %24 encoding of "$"
    params = (
        f"$select={SELECT}"
        f"&$top={PAGE_SIZE}&$skip={offset}"
        f"&$filter=&tz=America/New_York"
    )
    url = f"{JOBS_URL}?{params}"

    hdrs = {
        **_API_HEADERS,
        "rolecode": "manager",
    }
    if token:
        hdrs["myjobstoken"] = token

    try:
        requester = session or _requests
        kw = {"impersonate": _IMPERSONATE} if (_USE_CURL_CFFI and session is None) else {}
        resp = requester.get(url, headers=hdrs, timeout=TIMEOUT, **kw)
        if resp.status_code != 200:
            logger.warning("adp: jobs API %s for slug=%r offset=%d",
                           resp.status_code, slug, offset)
            return None, None
        data = resp.json()
        return data.get("jobRequisitions", []), data.get("count", 0)
    except Exception as e:
        logger.debug("adp: fetch_reqs error slug=%r offset=%d: %s", slug, offset, e)
        return None, None


def _fetch_page(slug, cfg, offset=0, top=3):
    """Convenience wrapper — fetch a small sample for detect()."""
    session = _make_session()
    _warm_session(session, slug)
    reqs, _ = _fetch_reqs(slug, cfg, offset, session=session)
    if not reqs:
        return []
    return _parse_reqs(reqs[:top], slug, "")


# ─────────────────────────────────────────
# HELPERS — PARSING
# ─────────────────────────────────────────

def _parse_reqs(reqs, slug, company):
    """Convert ADP jobRequisitions list to normalized job dicts."""
    jobs     = []
    seen_ids = set()

    for req in reqs:
        req_id = req.get("reqId", "")
        if not req_id or req_id in seen_ids:
            continue
        seen_ids.add(req_id)

        title            = req.get("publishedJobTitle") or req.get("jobTitle", "")
        posted_at        = _parse_date(req.get("postingDate", ""))
        location, alpha3 = _extract_location(req)
        description      = _extract_description(req)
        job_url          = JOB_URL.format(slug=slug, req_id=req_id)

        jobs.append({
            "company":        company,
            "job_id":         req_id,
            "title":          title,
            "location":       location,
            "posted_at":      posted_at,
            "description":    description,
            "job_url":        job_url,
            "ats":            "adp",
            "_slug":          slug,
            "_country_code3": alpha3,   # "USA", "CAN" — used by get_country_code()
        })

    return jobs


# ─────────────────────────────────────────
# COUNTRY CODE
# ─────────────────────────────────────────

def get_country_code(job):
    """
    Return ISO alpha-2 country code for a job dict.
    ADP stores alpha-3 codes ("USA", "CAN") in _country_code3.
    Used by job_monitor for the listing-level country gate.
    """
    return alpha3_to_alpha2(job.get("_country_code3", ""))


# ─────────────────────────────────────────
# HELPERS — LOCATION / DESCRIPTION / DATE
# ─────────────────────────────────────────

def _extract_location(req):
    """
    Extract location string and alpha-3 country code from ADP
    requisitionLocations.

    Location format (always includes country code):
      US  →  "Charlotte, North Carolina, USA"
      Int →  "Mississauga, Ontario, CAN"

    Returns (location_str, alpha3_country_code).
    Falls back to nameCode.longName stripped of verbose parentheticals.
    """
    locs = req.get("requisitionLocations", [])
    if not locs:
        return "", ""

    primary      = next((l for l in locs if l.get("primaryIndicator")), locs[0])
    addr         = primary.get("address") or {}
    city         = addr.get("cityName", "")
    state_long   = (addr.get("countrySubdivisionLevel1") or {}).get("longName", "")
    country_code = (addr.get("country") or {}).get("codeValue", "")   # "USA", "CAN"

    if city and state_long:
        loc = f"{city}, {state_long}, {country_code}" if country_code else f"{city}, {state_long}"
        return loc, country_code

    if city:
        return (f"{city}, {country_code}" if country_code else city), country_code

    # Fallback: nameCode.longName minus parenthetical detail
    name_long = (primary.get("nameCode") or {}).get("longName", "")
    if name_long:
        return re.sub(r"\s*\(.*\)$", "", name_long).strip(), country_code

    return "", country_code


def _extract_description(req):
    """
    Combine jobDescription + jobQualifications HTML → plain text.
    Full description is returned at listing level — no detail fetch needed.
    """
    desc  = req.get("jobDescription",    "") or ""
    quals = req.get("jobQualifications", "") or ""

    combined = desc
    if quals:
        combined = f"{combined}\n\n{quals}" if combined else quals
    if not combined:
        return ""

    soup = BeautifulSoup(combined, "html.parser")
    text = soup.get_text(separator="\n", strip=True)
    text = re.sub(r"\n{3,}", "\n\n", text).strip()
    return text[:5000]


def _parse_date(date_str):
    """Parse ADP postingDate to datetime.  Format: "2026-04-23T16:09:11Z" """
    if not date_str:
        return None
    for fmt in ("%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(date_str, fmt)
        except ValueError:
            continue
    try:
        return datetime.fromisoformat(
            date_str.replace("Z", "+00:00")
        ).replace(tzinfo=None)
    except Exception:
        return None
