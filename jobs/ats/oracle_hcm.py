# jobs/ats/oracle_hcm.py — Oracle HCM Cloud API client
# Date field: PostedDate (RELIABLE — original posting date)
# Used by: JPMorgan Chase, Goldman Sachs, and other enterprise companies
#
# URL format: {slug}.fa.oraclecloud.com/hcmUI/CandidateExperience/
#             en/sites/{site_id}/jobs

import json
import logging
from datetime import datetime
from jobs.ats.base import fetch_json


# Oracle HCM REST API endpoint
# Correct finder format discovered from browser XHR inspection:
#   finder=findReqs;siteNumber={site_id},limit={limit},offset={offset}
# Parameters are semicolon-separated key=value pairs inside the finder value
#
# URL variants:
#   Standard: {slug}.fa.oraclecloud.com                (JPMorgan)
#   Regional: {slug}.fa.{region}.oraclecloud.com       (Goldman Sachs → us2)
#   OCS:      {slug}.fa.ocs.oraclecloud.com            (Akamai, Nokia)
def _build_oracle_url(slug, region, site_id, limit, offset, ocs=False):
    """Build Oracle HCM API URL handling optional region/OCS subdomain.

    IMPORTANT: limit and offset must be inside the finder value
    as semicolon-separated params, NOT as separate URL params.
    Verified via browser XHR inspection on jpmc.fa.oraclecloud.com

    ocs=True  → host is {slug}.fa.ocs.oraclecloud.com  (OCS cluster tenants)
    region    → host is {slug}.fa.{region}.oraclecloud.com (regional tenants)
    neither   → host is {slug}.fa.oraclecloud.com
    """
    if ocs:
        host = f"{slug}.fa.ocs.oraclecloud.com"
    elif region:
        host = f"{slug}.fa.{region}.oraclecloud.com"
    else:
        host = f"{slug}.fa.oraclecloud.com"
    # Build finder value with limit+offset embedded
    from urllib.parse import quote
    finder = (
        f"findReqs;siteNumber={site_id},"
        f"limit={limit},"
        f"offset={offset},"
        f"sortBy=POSTING_DATES_DESC"
    )
    return (
        f"https://{host}/hcmRestApi/resources/latest/"
        f"recruitingCEJobRequisitions?"
        f"onlyData=true&"
        f"expand=requisitionList.workLocation,"
        f"requisitionList.otherWorkLocations,"
        f"requisitionList.secondaryLocations,"
        f"requisitionList.requisitionFlexFields&"
        f"finder={quote(finder)}"
    )

def _build_careers_url(slug, region, site_id, ocs=False):
    """Build Oracle HCM careers page URL."""
    if ocs:
        host = f"{slug}.fa.ocs.oraclecloud.com"
    elif region:
        host = f"{slug}.fa.{region}.oraclecloud.com"
    else:
        host = f"{slug}.fa.oraclecloud.com"
    return f"https://{host}/hcmUI/CandidateExperience/en/sites/{site_id}/jobs"


def detect(company, domain):
    """
    Detect Oracle HCM tenant from company career page.
    Follows documented pipeline:
      company.com/careers -> oraclecloud URL -> tenant -> organizationName

    Returns slug_info dict or None.
    """
    import re
    import requests

    CAREER_PATHS = [
        "/careers",
        "/careers/",
        "/jobs",
        "/about/careers",
        "/company/careers",
        "/en/careers",
        "/global/careers",
        "/us/careers",
        "/about-us/careers",
        "/join-us",
        "/work-with-us",
        "/opportunities",
        "/career",
    ]
    HEADERS = {"User-Agent": "Mozilla/5.0"}

    if not domain:
        return None

    domain = re.sub(r"^https?://", "", domain).rstrip("/")
    # company is used for logging context
    logger = logging.getLogger(__name__)
    logger.debug("Oracle detect: company=%s domain=%s", company, domain)

    # Steps 1+2: Visit career pages, find oraclecloud URL in HTML
    oracle_url = None
    # Match: {tenant}.fa.oraclecloud.com or {tenant}.fa.{region}.oraclecloud.com
    oracle_pattern = re.compile(
        r"https://[a-zA-Z0-9-]+\.fa(?:\.[a-z0-9]+)?\.oraclecloud\.com"
        r"/hcmUI/CandidateExperience[^\s\"'<>]*"
    )

    for path in CAREER_PATHS:
        try:
            resp = requests.get(
                f"https://{domain}{path}",
                headers=HEADERS, timeout=8,
                allow_redirects=True
            )
            if resp.status_code != 200:
                continue
            # Check 1: final redirect URL points directly to Oracle
            if oracle_pattern.search(resp.url):
                m = oracle_pattern.search(resp.url)
                oracle_url = m.group(0)
                break
            # Check 2: Oracle URL embedded in page HTML
            match = oracle_pattern.search(resp.text)
            if match:
                oracle_url = match.group(0)
                break
        except Exception as e:
            logger.debug("Oracle career page fetch failed %s%s: %s",
                         domain, path, e)
            continue

    if not oracle_url:
        return None

    # Step 3: Extract tenant/region/site_id from URL
    from jobs.ats.patterns import match_ats_pattern
    result = match_ats_pattern(oracle_url)
    if not result or result["platform"] != "oracle_hcm":
        return None

    try:
        slug_info = json.loads(result["slug"])
    except (ValueError, TypeError):
        return None

    # URL was found on company's own career page — it IS their tenant
    # No further API validation needed (company domain = ground truth)
    return slug_info


def _discover_site_id(slug, region, ocs=False):
    """
    Auto-discover the career site code for an Oracle HCM tenant.

    Called when the slug was detected from a JS fingerprint URL (no /sites/ in URL)
    and site_id was not captured at detection time.

    Oracle HCM exposes its site list at:
      /hcmRestApi/resources/latest/recruitingCESites?onlyData=true

    Returns the SiteNumber of the first active external site, or "" on failure.
    Typical values: "CX_1", "CX_1001", "CX_MAIN", "ERlive" …
    """
    if ocs:
        host = f"{slug}.fa.ocs.oraclecloud.com"
    elif region:
        host = f"{slug}.fa.{region}.oraclecloud.com"
    else:
        host = f"{slug}.fa.oraclecloud.com"

    url = f"https://{host}/hcmRestApi/resources/latest/recruitingCESites?onlyData=true"
    data = fetch_json(url, platform="oracle_hcm")
    if not data:
        return ""

    items = data.get("items", [])
    # Pick first active external site; fall back to first item if no status field
    for item in items:
        site_num = item.get("SiteNumber") or item.get("siteNumber") or item.get("SiteCode") or ""
        if site_num:
            return str(site_num).rstrip("/")

    return ""


def fetch_jobs(slug_info, company):
    """
    Fetch all jobs for company from Oracle HCM.
    slug_info = {"slug": "jpmc", "site": "CX_1001"}
    Returns list of normalized job dicts.
    """
    if isinstance(slug_info, str):
        try:
            slug_info = json.loads(slug_info)
        except (json.JSONDecodeError, TypeError):
            return []

    slug    = slug_info.get("slug", "")
    site_id = slug_info.get("site", "")
    region  = slug_info.get("region", "")
    ocs     = slug_info.get("ocs", False)

    if not slug:
        return []

    # site="" means the tenant was detected from a JS asset URL (custom-domain career page)
    # and /sites/{id} was not present — auto-discover the site number via the sites API.
    if not site_id:
        logger = logging.getLogger(__name__)
        logger.debug("Oracle HCM: site_id missing for %r, attempting auto-discovery", slug)
        site_id = _discover_site_id(slug, region, ocs=ocs)
        if not site_id:
            logger.warning("Oracle HCM: could not discover site_id for slug=%r — skipping", slug)
            return []
        logger.info("Oracle HCM: auto-discovered site_id=%r for slug=%r", site_id, slug)

    all_jobs = []
    offset   = 0
    limit    = 200

    while True:
        url  = _build_oracle_url(slug, region, site_id, limit, offset, ocs=ocs)
        data = fetch_json(url, platform="oracle_hcm")  # tracked for api_health
        if not data:
            break

        items = data.get("items", [])
        if not items:
            break

        # Oracle always returns 1 item wrapper — jobs are in requisitionList
        reqs = items[0].get("requisitionList", [])
        if not reqs:
            break

        all_jobs.extend(reqs)

        # Paginate by job count — stop when fewer than limit returned
        if len(reqs) < limit:
            break

        offset += limit

    return [_normalize(j, company, slug_info, slug, site_id)
            for j in all_jobs if j.get("Title")]


def _normalize(job, company, slug_info, slug, site_id):
    """Normalize Oracle HCM job to standard format."""
    posted_at = None
    posted    = job.get("PostedDate")
    if posted:
        try:
            posted_at = datetime.fromisoformat(
                posted.replace("Z", "+00:00")
            )
        except (ValueError, AttributeError):
            posted_at = None

    # Location
    primary  = job.get("PrimaryLocation", "")
    location = primary if primary else ""

    # Country code (Tier 1 gate)
    # PrimaryLocationCountry is an ISO alpha-2 code present on every listing
    # ("US", "TR", "IN", "GB" …) — far more reliable than text-parsing PrimaryLocation.
    # Stored as _country_code; job_monitor's listing-level alpha-2 gate uses it
    # to drop non-US jobs before any further processing.
    country_code = (job.get("PrimaryLocationCountry") or "").strip().upper()

    # Build job URL
    req_id  = job.get("Id", "") or \
            job.get("ExternalJobId", "") or \
            job.get("RequisitionId", "")
    region  = slug_info.get("region", "") if isinstance(slug_info, dict) else ""
    ocs     = slug_info.get("ocs", False) if isinstance(slug_info, dict) else False
    job_url = (
        f"{_build_careers_url(slug, region, site_id, ocs=ocs).rstrip('/jobs')}"
        f"/job/{req_id}"
        if req_id else
        _build_careers_url(slug, region, site_id, ocs=ocs)
    )

    return {
        "company":       company,
        "title":         job.get("Title", ""),
        "job_url":       job_url,
        "location":      location,
        "posted_at":     posted_at,
        "job_id":        str(req_id) if req_id else "",
        "description":   job.get("ShortDescriptionStr", ""),  # teaser only — full description pending
        "ats":           "oracle_hcm",
        "_country_code": country_code,
    }