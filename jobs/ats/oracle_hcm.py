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
# site_id varies per company (e.g. CX_1001 for JPMorgan)
BASE_URL = (
    "https://{slug}.fa.oraclecloud.com/hcmRestApi/resources/latest/"
    "recruitingCEJobRequisitions?"
    "finder=CandidateExperience&"
    "CandidateExperienceId={site_id}&"
    "limit={limit}&offset={offset}&"
    "expand=requisitionList.secondaryLocations,"
    "requisitionList.otherWorkLocations"
)

# Careers page URL (used for verification)
CAREERS_URL = (
    "https://{slug}.fa.oraclecloud.com/hcmUI/CandidateExperience/"
    "en/sites/{site_id}/jobs"
)


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

    if not slug or not site_id:
        return []

    all_jobs = []
    offset   = 0
    limit    = 25

    while True:
        url  = BASE_URL.format(
            slug=slug, site_id=site_id,
            limit=limit, offset=offset
        )
        data = fetch_json(url)
        if not data:
            break

        items = data.get("items", [])
        if not items:
            break

        # Oracle wraps jobs in requisitionList
        for item in items:
            reqs = item.get("requisitionList", [])
            all_jobs.extend(reqs)

        total = data.get("totalResults", 0)
        offset += limit
        if offset >= total:
            break

    return [_normalize(j, company, slug, site_id)
            for j in all_jobs if j.get("Title")]


def _normalize(job, company, slug, site_id):
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
    job_url = (
        f"https://{slug}.fa.oraclecloud.com/hcmUI/"
        f"CandidateExperience/en/sites/{site_id}/"
        f"job/{req_id}"
        if req_id else
        CAREERS_URL.format(slug=slug, site_id=site_id)
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