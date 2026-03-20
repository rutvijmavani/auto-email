# jobs/ats/ashby.py — Ashby public API client
# Date field: publishedAt (RELIABLE — original publish date)

from datetime import datetime
from jobs.ats.base import fetch_json, slugify, validate_company_match


BASE_URL = "https://api.ashbyhq.com/posting-api/job-board/{slug}"


def detect(company):
    """
    Try to detect if company uses Ashby.
    Returns (slug, sample_jobs) or (None, None).
    """
    for slug in slugify(company):
        data = fetch_json(BASE_URL.format(slug=slug))
        if data is None:
            continue
        jobs = data.get("jobs", [])
        if len(jobs) == 0:
            return slug, []
        # Validate using first job URL
        first_url = jobs[0].get("jobUrl", "")
        if not validate_company_match(first_url, company):
            continue
        return slug, jobs
    return None, None


def fetch_jobs(slug, company):
    """
    Fetch all jobs for company from Ashby.
    Returns list of normalized job dicts.
    """
    data = fetch_json(BASE_URL.format(slug=slug))
    if not data:
        return []
    jobs = data.get("jobs", [])
    return [_normalize(j, company) for j in jobs if j.get("title")]


def _normalize(job, company):
    """Normalize Ashby job to standard format."""
    posted_at = None
    published = job.get("publishedAt")
    if published:
        try:
            # ISO format: "2026-03-04T08:00:00.000Z"
            posted_at = datetime.fromisoformat(
                published.replace("Z", "+00:00")
            )
        except (ValueError, AttributeError):
            posted_at = None

    # Location
    location = job.get("location", "") or \
               job.get("locationName", "") or ""

    return {
        "company":     company,
        "title":       job.get("title", ""),
        "job_url":     job.get("jobUrl", ""),
        "location":    location,
        "posted_at":   posted_at,
        "job_id":      str(job.get("id", "")),
        "description": _strip_html(job.get("descriptionHtml", "")),
        "ats":         "ashby",
    }


def _strip_html(html):
    """Strip HTML tags from description."""
    if not html:
        return ""
    import re
    return re.sub(r"<[^>]+>", " ", html).strip()