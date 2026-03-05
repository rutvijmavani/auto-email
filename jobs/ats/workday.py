# jobs/ats/workday.py — Workday undocumented public API client
# Date field: postedOn (RELIABLE — original posting date)
# Risk: undocumented — monitor for structural changes

from datetime import datetime
from jobs.ats.base import fetch_json, slugify, validate_company_match


# Workday uses different instance numbers (wd1-wd5)
WD_VARIANTS = ["wd5", "wd1", "wd2", "wd3", "wd4"]
BASE_URL = "https://{slug}.{wd}.myworkdayjobs.com/wday/cxs/{slug}/careers/jobs"


def detect(company):
    """
    Try to detect if company uses Workday.
    Tries all slug variants × WD instance variants.
    Returns (slug_info, sample_jobs) or (None, None).
    slug_info = {"slug": "jpmorgan", "wd": "wd5"}
    """
    for slug in slugify(company):
        for wd in WD_VARIANTS:
            url = BASE_URL.format(slug=slug, wd=wd)
            data = fetch_json(url, params={"limit": 20, "offset": 0})
            if data is None:
                continue
            jobs = data.get("jobPostings", [])
            if not isinstance(jobs, list):
                continue
            if len(jobs) == 0:
                # Valid Workday response structure but no jobs
                return {"slug": slug, "wd": wd}, []
            # Validate company match
            first_title = jobs[0].get("title", "")
            first_url = jobs[0].get("externalUrl", "")
            if not validate_company_match(
                first_url + first_title, company
            ):
                continue
            return {"slug": slug, "wd": wd}, jobs
    return None, None


def fetch_jobs(slug_info, company):
    """
    Fetch all jobs for company from Workday.
    Handles pagination.
    Returns list of normalized job dicts.
    slug_info = {"slug": "jpmorgan", "wd": "wd5"}
    """
    slug = slug_info["slug"]
    wd   = slug_info["wd"]
    url  = BASE_URL.format(slug=slug, wd=wd)

    all_jobs = []
    offset   = 0
    limit    = 20  # Workday default page size

    while True:
        data = fetch_json(url, params={"limit": limit, "offset": offset})
        if not data:
            break
        jobs = data.get("jobPostings", [])
        if not jobs:
            break
        all_jobs.extend(jobs)
        total = data.get("total", 0)
        offset += len(jobs)  # use actual jobs returned not limit
        if len(jobs) < limit or offset >= total:
            break

    return [_normalize(j, company, slug, wd)
            for j in all_jobs if j.get("title")]


def _normalize(job, company, slug, wd):
    """Normalize Workday job to standard format."""
    posted_at = None
    posted = job.get("postedOn")
    if posted:
        try:
            # Format: "03/04/2026" or ISO format
            if "/" in posted:
                posted_at = datetime.strptime(posted, "%m/%d/%Y")
            else:
                posted_at = datetime.fromisoformat(
                    posted.replace("Z", "+00:00")
                )
        except (ValueError, AttributeError):
            posted_at = None

    # Build job URL
    external_url = job.get("externalUrl", "")
    if not external_url:
        job_id = job.get("bulletFields", [""])[0] if job.get("bulletFields") else ""
        external_url = (
            f"https://{slug}.{wd}.myworkdayjobs.com/"
            f"careers/job/{job_id}"
        )

    return {
        "company":     company,
        "title":       job.get("title", ""),
        "job_url":     external_url,
        "location":    job.get("locationsText", ""),
        "posted_at":   posted_at,
        "description": " ".join(job.get("bulletFields", [])),
        "ats":         "workday",
    }