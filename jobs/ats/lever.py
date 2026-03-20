# jobs/ats/lever.py — Lever public API client
# Date field: createdAt (RELIABLE — Unix timestamp, never changes)

from datetime import datetime
from jobs.ats.base import fetch_json, slugify, validate_company_match


BASE_URL = "https://api.lever.co/v0/postings/{slug}"


def detect(company):
    """
    Try to detect if company uses Lever.
    Returns (slug, sample_jobs) or (None, None).
    """
    for slug in slugify(company):
        data = fetch_json(BASE_URL.format(slug=slug),
                          params={"mode": "json"})
        if data is None:
            continue
        if not isinstance(data, list):
            continue
        if len(data) == 0:
            return slug, []
        # Validate using first job URL
        first_url = data[0].get("hostedUrl", "")
        if not validate_company_match(first_url, company):
            continue
        return slug, data
    return None, None


def fetch_jobs(slug, company):
    """
    Fetch all jobs for company from Lever.
    Returns list of normalized job dicts.
    """
    data = fetch_json(BASE_URL.format(slug=slug),
                      params={"mode": "json"})
    if not data or not isinstance(data, list):
        return []
    return [_normalize(j, company) for j in data if j.get("text")]


def _normalize(job, company):
    """Normalize Lever job to standard format."""
    # createdAt is Unix timestamp in milliseconds
    posted_at = None
    created_ms = job.get("createdAt")
    if created_ms:
        try:
            posted_at = datetime.fromtimestamp(int(created_ms) / 1000)
        except (ValueError, OSError, TypeError):
            posted_at = None

    categories = job.get("categories") or {}
    if isinstance(categories, dict):
        all_locs = categories.get("allLocations")
        location = (
            categories.get("location", "") or
            (all_locs[0] if isinstance(all_locs, list) and all_locs else "")
        )
    else:
        location = ""

    return {
        "company":     company,
        "title":       job.get("text", ""),
        "job_url":     job.get("hostedUrl", ""),
        "location":    location,
        "posted_at":   posted_at,
        "job_id":      str(job.get("id", "")),
        "description": job.get("descriptionPlain", ""),
        "ats":         "lever",
    }