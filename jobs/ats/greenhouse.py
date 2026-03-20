# jobs/ats/greenhouse.py — Greenhouse public API client
# Date field: updated_at (UNRELIABLE — changes on edit)
# Freshness: first_seen + content_hash approach

from jobs.ats.base import fetch_json, slugify, validate_company_match


BASE_URL = "https://boards-api.greenhouse.io/v1/boards/{slug}/jobs"


def detect(company):
    """
    Try to detect if company uses Greenhouse.
    Returns (slug, sample_jobs) or (None, None).
    """
    for slug in slugify(company):
        url = BASE_URL.format(slug=slug)
        data = fetch_json(url, params={"content": "true"})
        if data is None:
            continue
        jobs = data.get("jobs", [])
        if len(jobs) == 0:
            # Valid response but no jobs — confirm by checking structure
            # Return slug but mark as 'detected_empty'
            return slug, []
        # Validate company match using first job's URL
        first_url = jobs[0].get("absolute_url", "")
        if not validate_company_match(first_url, company):
            continue
        return slug, jobs
    return None, None


def fetch_jobs(slug, company):
    """
    Fetch all jobs for company from Greenhouse.
    Returns list of normalized job dicts.
    """
    url = BASE_URL.format(slug=slug)
    # Fetch all pages
    all_jobs = []
    page = 1
    while True:
        data = fetch_json(url, params={"content": "true", "page": page})
        if not data:
            break
        jobs = data.get("jobs", [])
        if not jobs:
            break
        all_jobs.extend(jobs)
        # Greenhouse meta has total count
        meta = data.get("meta", {})
        total = meta.get("total", 0)
        if len(all_jobs) >= total or len(jobs) == 0:
            break
        page += 1

    return [_normalize(j, company) for j in all_jobs if j.get("title")]


def _normalize(job, company):
    """Normalize Greenhouse job to standard format."""
    return {
        "company":     company,
        "title":       job.get("title", ""),
        "job_url":     job.get("absolute_url", ""),
        "location":    job.get("location", {}).get("name", ""),
        "posted_at":   None,  # updated_at unreliable — not used
        "job_id":      str(job.get("id", "")), 
        "description": job.get("content", ""),
        "ats":         "greenhouse",
    }