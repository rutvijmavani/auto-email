# jobs/ats/successfactors.py — SAP SuccessFactors job board scraper
#
# SuccessFactors exposes a hidden but consistent XML feed for all tenants
# using Recruiting Management (not all tenants — SAP itself returns HTML).
#
# XML Feed URL:
#   https://career{dc}.successfactors.{region}/career
#     ?company={slug}&career_ns=job_listing_summary&resultType=XML
#
# DC variants:  2, 4, 5, 8, 10, 12
# Region:       com, eu
#
# Job URL pattern (canonical):
#   https://career{dc}.successfactors.{region}/career
#     ?company={slug}&career_job_req_id={req_id}
#
# XML structure (varies per tenant — generalised extraction):
#   <Job>
#     <JobTitle>      → title
#     <ReqId>         → job_id
#     <Posted-Date>   → posted_at (MM/DD/YYYY)
#     <Job-Description> → description (HTML)
#     <Location>      → location (NetApp style: "San Jose, CA, USA")
#     <mfield1>       → "Country/Area{value}" (Ericsson style)
#     <mfield3>       → "state/province{value}" (Ericsson style)
#   </Job>
#
# Date field: <Posted-Date> MM/DD/YYYY — RELIABLE original posting date
# Job ID:     <ReqId> — integer string e.g. "782174"
#
# Slug format stored in DB (JSON):
#   {"slug": "Ericsson", "dc": "2", "region": "eu"}
#   {"slug": "netappinc", "dc": "4", "region": "com"}
#   {"slug": "SAP", "dc": "5", "region": "eu", "path": "/careers"}

import re
import json
import html as html_lib
from datetime import datetime
from bs4 import BeautifulSoup
from jobs.ats.base import fetch_html, slugify, validate_company_match

# ─────────────────────────────────────────
# CONSTANTS
# ─────────────────────────────────────────

DC_VARIANTS  = ["2", "4", "5", "8", "10", "12"]
REGIONS      = ["com", "eu"]
DATE_FORMATS = ["%m/%d/%Y", "%Y-%m-%d", "%d/%m/%Y"]

# Prefixes to strip from mfield values
MFIELD_PREFIXES = [
    "Country/Area", "country/area",
    "state/province", "State/Province",
    "City", "city",
    "Job Category", "job category",
]


# ─────────────────────────────────────────
# DETECTION
# ─────────────────────────────────────────

def detect(company):
    """
    Try to detect if company uses SuccessFactors XML feed.

    Probes all DC/region combinations with each slug variant.
    Tries both /career and /careers path variants per probe.
    Validates by checking XML response contains job data.

    Returns (slug_info dict, sample_jobs) or (None, None).
    slug_info = {"slug": "ericsson", "dc": "2", "region": "eu"}
    slug_info = {"slug": "SAP", "dc": "5", "region": "eu", "path": "/careers"}
    """
    for slug in slugify(company):
        for dc in DC_VARIANTS:
            for region in REGIONS:
                # Try both path variants — /career (most tenants) and
                # /careers (SAP and some other tenants).
                # Store "path" in slug_info only when non-default (/careers)
                # so existing slug_infos without "path" keep working.
                for path in ("/career", "/careers"):
                    url  = _feed_url(slug, dc, region, path=path)
                    resp = fetch_html(url, platform="successfactors",
                                      track=False)
                    if resp is None:
                        continue

                    # Must be XML — HTML response = wrong tenant/slug/path
                    ctype = resp.headers.get("content-type", "").lower()
                    if "html" in ctype and "xml" not in ctype:
                        continue
                    if not resp.text.strip().startswith("<?xml"):
                        continue

                    soup = BeautifulSoup(resp.text, "xml")
                    jobs = soup.find_all("Job")
                    if not jobs:
                        continue

                    # Validate company match using first job title + slug
                    first_title = jobs[0].find("JobTitle")
                    text        = (
                        (first_title.get_text(strip=True) if first_title else "")
                        + slug
                    )
                    if not validate_company_match(text, company):
                        continue

                    slug_info = {"slug": slug, "dc": dc, "region": region}
                    if path != "/career":
                        slug_info["path"] = path   # only store non-default
                    sample = [_normalize(j, company, slug_info)
                              for j in jobs[:3]]
                    return slug_info, sample

    return None, None


# ─────────────────────────────────────────
# FETCH JOBS
# ─────────────────────────────────────────

def fetch_jobs(slug_info, company):
    """
    Fetch all jobs for company from SuccessFactors XML feed.

    The XML feed returns ALL jobs in a single request — no pagination needed.
    Description is included inline — no detail page fetch required.
    This is Option A (all data in one call), not Option C.

    Args:
        slug_info: dict with "slug", "dc", "region", and optionally "path"
                   or JSON string (stored in DB)
        company:   company name

    Returns:
        List of normalized job dicts with all fields populated.
        posted_at and description filled from XML — no detail fetch needed.
    """
    if not slug_info:
        return []

    if isinstance(slug_info, str):
        try:
            slug_info = json.loads(slug_info)
        except (json.JSONDecodeError, TypeError):
            return []

    slug   = slug_info.get("slug", "")
    dc     = slug_info.get("dc", "")
    region = slug_info.get("region", "")

    if not all([slug, dc, region]):
        return []

    # Use stored path if present (e.g. "/careers" for SAP).
    # Defaults to "/career" for existing slug_infos without path key.
    path = slug_info.get("path", "/career")
    url  = _feed_url(slug, dc, region, path=path)
    resp = fetch_html(url, platform="successfactors")

    if resp is None:
        return []

    # Validate XML response
    ctype = resp.headers.get("content-type", "").lower()
    if "html" in ctype and "xml" not in ctype:
        return []
    if not resp.text.strip().startswith("<?xml"):
        return []

    soup = BeautifulSoup(resp.text, "xml")
    jobs = soup.find_all("Job")

    return [_normalize(j, company, slug_info) for j in jobs if j.find("JobTitle")]


# ─────────────────────────────────────────
# NORMALIZATION
# ─────────────────────────────────────────

def _normalize(job, company, slug_info):
    """
    Normalize SuccessFactors XML job to standard format.

    Field extraction is generalised — handles both:
      Ericsson style: mfield1="Country/AreaIndia", mfield3="state/provinceUttar Pradesh"
      NetApp style:   Location="San Jose, CA, USA Office (SANJOSE)"
    """
    slug   = slug_info.get("slug", "")
    dc     = slug_info.get("dc", "")
    region = slug_info.get("region", "")
    path   = slug_info.get("path", "/career")

    title       = _text(job, "JobTitle")
    req_id      = _text(job, "ReqId")
    date_str    = _text(job, "Posted-Date")
    description = _extract_description(job)
    location    = _extract_location(job)
    job_url     = _job_url(slug, dc, region, req_id, path=path)

    return {
        "company":     company,
        "title":       title,
        "job_url":     job_url,
        "job_id":      req_id,
        "location":    location,
        "posted_at":   _parse_date(date_str),
        "description": description,
        "ats":         "successfactors",
    }


# ─────────────────────────────────────────
# HELPERS — XML EXTRACTION
# ─────────────────────────────────────────

def _text(job, tag_name):
    """Extract plain text from a named XML tag."""
    tag = job.find(tag_name)
    if not tag:
        return ""
    return tag.get_text(strip=True)


def _extract_location(job):
    """
    Extract location from SuccessFactors job XML.

    Three tenant styles:
      NetApp:   <Location>San Jose, CA, USA Office (SANJOSE)</Location>
      Ericsson: <mfield1>Country/AreaIndia</mfield1>
                <mfield3>state/provinceUttar Pradesh</mfield3>
      SAP:      <filter7>CountryBulgaria</filter7>
                <filter8>Internal Posting LocationSofia</filter8>
                <filter6>RegionEurope</filter6>
    """
    # Style 1 — explicit Location tag (NetApp)
    location = _text(job, "Location")
    if location:
        location = re.sub(r"\s*\([A-Z0-9]+\)\s*$", "", location).strip()
        return location

    # Style 2 — mfield tags (Ericsson)
    country = _strip_mfield_prefix(_text(job, "mfield1"))
    state   = _strip_mfield_prefix(_text(job, "mfield3"))
    city    = _strip_mfield_prefix(_text(job, "mfield4"))
    if country or state or city:
        parts = [p for p in [city, state, country] if p]
        return ", ".join(parts)

    # Style 3 — filter tags (SAP)
    # filter6=Region, filter7=Country, filter8=City/Internal Location
    _SAP_PREFIXES = [
        "Country", "Internal Posting Location", "Region",
        "Work Area", "Career Status", "Employment Type",
        "Expected Travel", "Additional Locations",
    ]
    city    = _strip_known_prefixes(_text(job, "filter8"), _SAP_PREFIXES)
    country = _strip_known_prefixes(_text(job, "filter7"), _SAP_PREFIXES)
    region  = _strip_known_prefixes(_text(job, "filter6"), _SAP_PREFIXES)
    parts   = [p for p in [city, country, region] if p]
    return ", ".join(parts)


def _strip_known_prefixes(value, prefixes):
    """
    Strip known label prefixes from filter/mfield values.
    "CountryBulgaria" → "Bulgaria"
    "Internal Posting LocationSofia" → "Sofia"
    "RegionEurope" → "Europe"
    Skips values that are pure labels with no data after stripping.
    """
    if not value:
        return ""
    for prefix in sorted(prefixes, key=len, reverse=True):  # longest first
        if value.startswith(prefix):
            stripped = value[len(prefix):].strip()
            if stripped:
                return stripped
            return ""
    return value


def _strip_mfield_prefix(value):
    """
    Strip label prefix from mfield values.
    "Country/AreaIndia" → "India"
    "state/provinceUttar Pradesh" → "Uttar Pradesh"
    "Job Category" → "" (category field, not location)
    """
    if not value:
        return ""
    for prefix in MFIELD_PREFIXES:
        if value.startswith(prefix):
            stripped = value[len(prefix):].strip()
            # Skip if the value IS just the label with no data
            if stripped and not stripped.startswith("/"):
                return stripped
            return ""
    # If value matches a pure label with no data, skip it
    if value in {"Job Category", "job category", "Country/Area", "state/province"}:
        return ""
    return value


def _extract_description(job):
    """
    Extract plain text description from SuccessFactors XML.
    <Job-Description> contains HTML — strip tags.
    Also handles HTML entities.
    """
    tag = job.find("Job-Description")
    if not tag:
        return ""

    raw = tag.get_text(strip=True)
    if not raw:
        # Try CDATA content directly
        raw = str(tag)
        raw = re.sub(r"<[^>]+>", "", raw)

    # Unescape HTML entities
    raw  = html_lib.unescape(raw)
    # Strip any remaining HTML tags
    soup = BeautifulSoup(raw, "html.parser")
    text = soup.get_text(separator="\n", strip=True)
    text = re.sub(r"\n{3,}", "\n\n", text).strip()
    return text[:5000]


# ─────────────────────────────────────────
# HELPERS — URL BUILDING
# ─────────────────────────────────────────

def _feed_url(slug, dc, region, path="/career"):
    """
    Build SuccessFactors XML feed URL.
    path="/career"  for most tenants (Ericsson, NetApp, etc.)
    path="/careers" for SAP-hosted tenants (career5.successfactors.eu/careers)
    """
    base = f"https://career{dc}.successfactors.{region}"
    return (
        f"{base}{path}"
        f"?company={slug}"
        f"&career_ns=job_listing_summary"
        f"&resultType=XML"
    )


def _job_url(slug, dc, region, req_id, path="/career"):
    """
    Build canonical job detail URL from ReqId.
    path="/career"  for most tenants
    path="/careers" for SAP-hosted tenants
    """
    if not req_id:
        return ""
    base = f"https://career{dc}.successfactors.{region}"
    return f"{base}{path}?company={slug}&career_job_req_id={req_id}"


# ─────────────────────────────────────────
# HELPERS — DATE PARSING
# ─────────────────────────────────────────

def _parse_date(date_str):
    """
    Parse SuccessFactors date string to datetime or None.
    Primary format: MM/DD/YYYY e.g. "03/29/2026"
    """
    if not date_str:
        return None
    date_str = date_str.strip()
    for fmt in DATE_FORMATS:
        try:
            return datetime.strptime(date_str, fmt)
        except ValueError:
            continue
    return None