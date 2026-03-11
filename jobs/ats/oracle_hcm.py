# jobs/ats/oracle_hcm.py — Oracle HCM Cloud API client
# Date field: PostedDate (RELIABLE — original posting date)
# Used by: JPMorgan Chase, Goldman Sachs, and other enterprise companies
#
# URL format: {slug}.fa.oraclecloud.com/hcmUI/CandidateExperience/
#             en/sites/{site_id}/jobs

import json
from datetime import datetime
from jobs.ats.base import fetch_json


# Oracle HCM REST API endpoint
# Correct finder format discovered from browser XHR inspection:
#   finder=findReqs;siteNumber={site_id},limit={limit},offset={offset}
# Parameters are semicolon-separated key=value pairs inside the finder value
#
# URL variants:
#   Standard: {slug}.fa.oraclecloud.com          (JPMorgan)
#   Regional: {slug}.fa.{region}.oraclecloud.com (Goldman Sachs → us2)
def _build_oracle_url(slug, region, site_id, limit, offset):
    """Build Oracle HCM API URL handling optional region subdomain."""
    if region:
        host = f"{slug}.fa.{region}.oraclecloud.com"
    else:
        host = f"{slug}.fa.oraclecloud.com"
    return (
        f"https://{host}/hcmRestApi/resources/latest/"
        f"recruitingCEJobRequisitions?"
        f"onlyData=true&"
        f"expand=requisitionList.workLocation,"
        f"requisitionList.otherWorkLocations,"
        f"requisitionList.secondaryLocations,"
        f"requisitionList.requisitionFlexFields&"
        f"finder=findReqs%3BsiteNumber%3D{site_id}"
        f"%2Climit%3D{limit}%2Coffset%3D{offset}"
        f"%2CsortBy%3DPOSTING_DATES_DESC"
    )

def _build_careers_url(slug, region, site_id):
    """Build Oracle HCM careers page URL."""
    if region:
        host = f"{slug}.fa.{region}.oraclecloud.com"
    else:
        host = f"{slug}.fa.oraclecloud.com"
    return f"https://{host}/hcmUI/CandidateExperience/en/sites/{site_id}/jobs"


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

    if not slug or not site_id:
        return []

    all_jobs = []
    offset   = 0
    limit    = 25

    while True:
        url = _build_oracle_url(slug, region, site_id, limit, offset)
        data = fetch_json(url)
        if not data:
            break

        items = data.get("items", [])
        if not items:
            break

        # Oracle wraps jobs in requisitionList inside each item
        for item in items:
            reqs = item.get("requisitionList", [])
            if isinstance(reqs, list):
                all_jobs.extend(reqs)

        # Oracle returns count not totalResults
        total = data.get("count", 0) or data.get("totalResults", 0)
        offset += limit
        if not data.get("hasMore", False) or offset >= total:
            break

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
    primary = job.get("PrimaryLocation", "")
    location = primary if primary else ""

    # Build job URL
    req_id  = job.get("ExternalJobId", "") or \
              job.get("RequisitionId", "")
    region  = slug_info.get("region", "") if isinstance(slug_info, dict) else ""
    job_url = (
        f"{_build_careers_url(slug, region, site_id).rstrip('/jobs')}"
        f"/job/{req_id}"
        if req_id else
        _build_careers_url(slug, region, site_id)
    )

    return {
        "company":     company,
        "title":       job.get("Title", ""),
        "job_url":     job_url,
        "location":    location,
        "posted_at":   posted_at,
        "description": job.get("ShortDescriptionStr", ""),
        "ats":         "oracle_hcm",
    }