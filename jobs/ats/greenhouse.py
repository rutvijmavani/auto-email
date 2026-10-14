# jobs/ats/greenhouse.py — Greenhouse public API client
# Date field: first_published (RELIABLE — original publish date)
# Freshness: first_seen + content_hash approach

import html as _html

from jobs.ats.base import fetch_json, slugify, validate_company_match
from jobs.ats.fetch_report import (
    FetchReport, STOP_ALL_PAGES, STOP_EMPTY_PAGE, STOP_REPEAT_PAGE, STOP_REQUEST_FAILED,
    STOP_SINGLE_RESPONSE, TOTAL_ATS, attach, count_distinct,
)
from jobs.utils import clean_html


BASE_URL = "https://boards-api.greenhouse.io/v1/boards/{slug}/jobs"


def _parse_date(date_str):
    """Parse ISO date string to datetime or None."""
    if not date_str:
        return None
    try:
        from datetime import datetime
        return datetime.fromisoformat(date_str.replace("Z", "+00:00"))
    except (ValueError, AttributeError):
        return None


def detect(company):
    """
    Try to detect if company uses Greenhouse.
    Returns (slug, sample_jobs) or (None, None).
    """
    for slug in slugify(company):
        url = BASE_URL.format(slug=slug)
        data = fetch_json(url, params={"content": "true"},
                          platform="greenhouse", track=False)  # detection — don't track
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


def _key(job):
    """Identity of a raw row for repeat-page detection: id, else url."""
    return job.get("id") or job.get("absolute_url")


def _key(job):
    """Identity of a raw row for repeat-page detection: id, else url."""
    return job.get("id") or job.get("absolute_url")


def fetch_jobs(slug, company):
    """
    Fetch all jobs for company from Greenhouse.
    Returns list of normalized job dicts.
    """
    url = BASE_URL.format(slug=slug)
    # Fetch all pages
    all_jobs = []
    seen     = set()
    page     = 1
    total    = None
    stop     = STOP_ALL_PAGES
    while True:
        data = fetch_json(url, params={"content": "true", "page": page},
                          platform="greenhouse")  # tracked for api_health
        if not data:
            stop = STOP_REQUEST_FAILED      # an empty board is still a dict, so falsy means the call failed
            break
        jobs = data.get("jobs", [])
        if not jobs:
            stop = STOP_EMPTY_PAGE
            break
        new = [j for j in jobs if _key(j) not in seen]
        if not new:
            stop = STOP_REPEAT_PAGE         # the board ignored `page` and sent the same rows again
            break
        seen.update(_key(j) for j in new)
        all_jobs.extend(new)
        # Greenhouse meta has total count (live-confirmed: the whole board arrives on page 1)
        total = (data.get("meta") or {}).get("total")
        if total is None or len(all_jobs) >= total:
            stop = STOP_ALL_PAGES if total is not None else STOP_SINGLE_RESPONSE
            break
        page += 1

    result = [_normalize(j, company) for j in all_jobs if j.get("title")]
    return attach(result, FetchReport(
        stop_reason      = stop,
        pages            = page,
        fetched_distinct = count_distinct(result),
        unusable         = len(all_jobs) - len(result),
        reported_total   = total if isinstance(total, int) else None,
        total_source     = TOTAL_ATS if isinstance(total, int) else None,
        repeated_pages   = 1 if stop == STOP_REPEAT_PAGE else 0,
    ))


def _normalize(job, company):
    """Normalize Greenhouse job to standard format."""
    return {
        "company":     company,
        "title":       job.get("title", ""),
        "job_url":     job.get("absolute_url", ""),
        "location":    job.get("location", {}).get("name", ""),
        "posted_at":   _parse_date(job.get("first_published")),  # updated_at unreliable — not used
        "job_id":      str(job.get("id", "")),
        "description": clean_html(_html.unescape(job.get("content", ""))),
        "ats":         "greenhouse",
    }