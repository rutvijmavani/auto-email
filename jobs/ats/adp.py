# jobs/ats/adp.py — ADP WorkforceNow (myjobs.adp.com) job board scraper
#
# URL pattern:
#   Career page / API: https://myjobs.adp.com/{slug}/cx/job-listing
#   Job detail page:   https://myjobs.adp.com/{slug}/cx/job-details?reqId={reqId}
#
# API response: JSON  {count: N, jobRequisitions: [...]}
# Pagination:   ?$top={page_size}&$skip={offset}   (OData convention)
#
# All data is available at listing level — no detail fetch required:
#   title       → publishedJobTitle  (falls back to jobTitle)
#   location    → requisitionLocations[].address  (city + state for US)
#   posted_at   → postingDate  (ISO datetime: "2026-04-23T16:09:11Z")
#   description → jobDescription + jobQualifications  (full HTML → plain text)
#
# Slug: company identifier segment in the myjobs.adp.com URL path
#   e.g.  "apply"      (ADP itself)
#         "scacareers" (Student Conservation Association)
#   Stored as plain string.

import re
import json
from datetime import datetime
from bs4 import BeautifulSoup
from jobs.ats.base import fetch_json, slugify, validate_company_match

BASE_URL  = "https://myjobs.adp.com"
PAGE_SIZE = 100
MAX_PAGES = 50      # 100 × 50 = 5 000 jobs safety cap


# ─────────────────────────────────────────
# DETECTION
# ─────────────────────────────────────────

def detect(company):
    """
    Try to detect if company uses ADP WorkforceNow (myjobs.adp.com).

    Probes https://myjobs.adp.com/{slug}/cx/job-listing for each slug variant
    generated from the company name.

    Returns:
        (slug, sample_jobs)  or  (None, None)
    """
    for slug in slugify(company):
        url  = f"{BASE_URL}/{slug}/cx/job-listing"
        data = fetch_json(url, platform="adp", track=False)

        if not _valid_response(data):
            continue

        reqs = data.get("jobRequisitions", [])
        if not reqs:
            continue

        sample = _parse_reqs(reqs[:3], slug, company)
        return slug, sample

    return None, None


# ─────────────────────────────────────────
# FETCH JOBS (listing)
# ─────────────────────────────────────────

def fetch_jobs(slug, company):
    """
    Fetch all jobs for company from ADP WorkforceNow.

    Paginates via $top / $skip until count is exhausted.

    Args:
        slug:    company slug  e.g. "scacareers"
                 Also accepts a JSON string {"slug": "..."} for forward compat.
        company: company name

    Returns:
        List of normalized job dicts.  All fields populated at listing level
        — location, description, posted_at all available without a detail fetch.
    """
    if not slug:
        return []

    # Accept JSON slug for forward compatibility
    if isinstance(slug, str) and slug.strip().startswith("{"):
        try:
            slug = json.loads(slug).get("slug", slug)
        except (json.JSONDecodeError, TypeError):
            pass

    all_jobs = []
    offset   = 0

    for _ in range(MAX_PAGES):
        url  = f"{BASE_URL}/{slug}/cx/job-listing"
        data = fetch_json(
            url,
            params={"$top": PAGE_SIZE, "$skip": offset},
            platform="adp",
        )

        if not _valid_response(data):
            break

        reqs = data.get("jobRequisitions", [])
        if not reqs:
            break

        all_jobs.extend(_parse_reqs(reqs, slug, company))

        total   = data.get("count", 0)
        offset += len(reqs)
        if offset >= total:
            break

    return all_jobs


# ─────────────────────────────────────────
# HELPERS — RESPONSE VALIDATION
# ─────────────────────────────────────────

def _valid_response(data):
    """Return True if response looks like an ADP job listing API response."""
    return (
        isinstance(data, dict)
        and "jobRequisitions" in data
        and isinstance(data["jobRequisitions"], list)
    )


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

        title                = req.get("publishedJobTitle") or req.get("jobTitle", "")
        posted_at            = _parse_date(req.get("postingDate", ""))
        location, alpha3     = _extract_location(req)
        description          = _extract_description(req)
        job_url              = f"{BASE_URL}/{slug}/cx/job-details?reqId={req_id}"

        jobs.append({
            "company":          company,
            "job_id":           req_id,
            "title":            title,
            "location":         location,
            "posted_at":        posted_at,
            "description":      description,
            "job_url":          job_url,
            "ats":              "adp",
            "_slug":            slug,
            "_country_code3":   alpha3,   # "USA", "CAN", … — used by get_country_code()
        })

    return jobs


# ISO 3166-1 alpha-3 → alpha-2 mapping (common ADP countries)
_ALPHA3_TO_ALPHA2 = {
    "USA": "US", "CAN": "CA", "GBR": "GB", "AUS": "AU",
    "IND": "IN", "DEU": "DE", "FRA": "FR", "MEX": "MX",
    "BRA": "BR", "CHN": "CN", "JPN": "JP", "KOR": "KR",
    "SGP": "SG", "IRL": "IE", "NLD": "NL", "ESP": "ES",
    "ITA": "IT", "POL": "PL", "SWE": "SE", "CHE": "CH",
    "BEL": "BE", "ARG": "AR", "CHL": "CL", "COL": "CO",
    "NZL": "NZ", "ZAF": "ZA", "ARE": "AE", "ISR": "IL",
}


def get_country_code(job):
    """
    Return ISO alpha-2 country code for a job dict.
    ADP stores alpha-3 codes ("USA", "CAN") in _country_code3.
    Used by job_monitor for the listing-level country gate.
    """
    alpha3 = job.get("_country_code3", "")
    return _ALPHA3_TO_ALPHA2.get(alpha3.upper(), "")


def _extract_location(req):
    """
    Extract location string and alpha-3 country code from ADP
    requisitionLocations.

    Location format (always includes country):
      US  →  "Charlotte, North Carolina, USA"
      Int →  "Mississauga, Ontario, CAN"

    Returns (location_str, alpha3_country_code).
    Falls back to nameCode.longName stripped of verbose parentheticals.
    """
    locs = req.get("requisitionLocations", [])
    if not locs:
        return "", ""

    # Prefer primary location; fall back to first entry
    primary = next((l for l in locs if l.get("primaryIndicator")), locs[0])

    addr         = primary.get("address") or {}
    city         = addr.get("cityName", "")
    state_long   = (addr.get("countrySubdivisionLevel1") or {}).get("longName", "")
    country_code = (addr.get("country") or {}).get("codeValue", "")   # "USA", "CAN", …

    if city and state_long:
        loc = f"{city}, {state_long}, {country_code}" if country_code else f"{city}, {state_long}"
        return loc, country_code

    if city:
        loc = f"{city}, {country_code}" if country_code else city
        return loc, country_code

    # Fallback: nameCode.longName minus parenthetical detail
    name_long = (primary.get("nameCode") or {}).get("longName", "")
    if name_long:
        return re.sub(r"\s*\(.*\)$", "", name_long).strip(), country_code

    return "", country_code


def _extract_description(req):
    """
    Combine jobDescription + jobQualifications HTML → plain text.

    ADP returns the full description at listing level, so no detail
    fetch is needed.  jobQualifications (preferred qualifications, benefits,
    etc.) is appended after a blank line when present.
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
    """
    Parse ADP postingDate to datetime.
    Primary format: "2026-04-23T16:09:11Z"
    """
    if not date_str:
        return None
    for fmt in ("%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(date_str, fmt)
        except ValueError:
            continue
    # Handle timezone-aware ISO strings ("2026-04-23T16:09:11+00:00")
    try:
        return datetime.fromisoformat(
            date_str.replace("Z", "+00:00")
        ).replace(tzinfo=None)
    except Exception:
        return None
