# jobs/ats/smartrecruiters.py — SmartRecruiters public API client
# Date field: releasedDate (RELIABLE — original release date)

import html as _html
from datetime import datetime

from jobs.ats.base import fetch_json, slugify, validate_company_match
from jobs.utils import clean_html


BASE_URL    = "https://api.smartrecruiters.com/v1/companies/{slug}/postings"
DETAIL_URL  = "https://api.smartrecruiters.com/v1/companies/{slug}/postings/{job_id}"

# Sections returned by the detail endpoint — extracted in this order
_DESCRIPTION_SECTIONS = (
    "companyDescription",
    "jobDescription",
    "qualifications",
    "additionalInformation",
)


def detect(company):
    """
    Try to detect if company uses SmartRecruiters.
    Returns (slug, sample_jobs) or (None, None).
    """
    for slug in slugify(company):
        data = fetch_json(BASE_URL.format(slug=slug),
                          platform="smartrecruiters", track=False)  # detection — don't track
        if data is None:
            continue
        jobs = data.get("content", [])
        if len(jobs) == 0:
            # Check if response structure is valid
            if "totalFound" in data:
                return slug, []
            continue
        # Validate using company name in response
        resp_company = data.get("company", {}).get("name", "")
        if not validate_company_match(resp_company, company):
            continue
        return slug, jobs
    return None, None


def fetch_jobs(slug, company):
    """
    Fetch all jobs for company from SmartRecruiters.
    Handles pagination.
    Returns list of normalized job dicts.
    """
    all_jobs = []
    offset = 0
    limit = 100

    while True:
        data = fetch_json(
            BASE_URL.format(slug=slug),
            params={"limit": limit, "offset": offset},
            platform="smartrecruiters",  # tracked for api_health
        )
        if not data:
            break
        jobs = data.get("content", [])
        if not jobs:
            break
        all_jobs.extend(jobs)
        total = data.get("totalFound", 0)
        offset += limit
        if offset >= total:
            break

    return [_normalize(j, company, slug) for j in all_jobs if j.get("name")]


def fetch_job_detail(job):
    """
    Fetch full job description from SmartRecruiters detail endpoint.
    Called only for NEW jobs (Option C strategy).

    GET /v1/companies/{slug}/postings/{job_id}
    Response includes jobAd.sections with four possible sub-sections:
      companyDescription, jobDescription, qualifications, additionalInformation
    Each section's "text" field contains HTML — unescaped and stripped via clean_html().

    Args:
        job: job dict from fetch_jobs() — must contain job_id and _company_slug

    Returns:
        Updated job dict with description filled.
    """
    job_id       = job.get("job_id", "")
    company_slug = job.get("_company_slug", "")
    if not job_id or not company_slug:
        return job

    url  = DETAIL_URL.format(slug=company_slug, job_id=job_id)
    data = fetch_json(url, platform="smartrecruiters")
    if not data:
        return job

    sections  = (data.get("jobAd") or {}).get("sections", {})
    raw_parts = []
    for key in _DESCRIPTION_SECTIONS:
        text = (sections.get(key) or {}).get("text", "")
        if text:
            raw_parts.append(_html.unescape(text))

    job              = dict(job)
    job["description"] = clean_html("\n\n".join(raw_parts))
    return job


def _normalize(job, company, company_slug=""):
    """Normalize SmartRecruiters job to standard format."""
    posted_at = None
    released = job.get("releasedDate")
    if released:
        try:
            posted_at = datetime.fromisoformat(
                released.replace("Z", "+00:00")
            )
        except (ValueError, AttributeError):
            posted_at = None

    location = job.get("location", {}) or {}
    loc_str = ", ".join(filter(None, [
        location.get("city", ""),
        location.get("region", ""),
        location.get("country", ""),
    ]))
    if location.get("remote"):
        loc_str = "Remote"

    # Build job URL using slug derived from caller
    job_id     = job.get("id", "")
    raw_name   = job.get("name")
    name_str   = str(raw_name).strip() if raw_name is not None else ""
    title_slug = slugify(name_str)[0] if name_str and slugify(name_str) else ""
    job_suffix = f"{job_id}-{title_slug}" if title_slug else job_id
    job_url = (
        f"https://jobs.smartrecruiters.com/{company_slug}/{job_suffix}"
        if company_slug else
        f"https://jobs.smartrecruiters.com/{job_suffix}"
    )

    return {
        "company":       company,
        "title":         job.get("name", ""),
        "job_url":       job_url,
        "location":      loc_str,
        "posted_at":     posted_at,
        "job_id":        str(job_id),
        "description":   "",             # filled by fetch_job_detail
        "ats":           "smartrecruiters",
        "_company_slug": company_slug,   # used by fetch_job_detail
    }