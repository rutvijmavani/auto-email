# jobs/ats/workday.py — Workday undocumented public API client
# Date field: postedOn (RELIABLE — original posting date)
# Risk: undocumented — monitor for structural changes

from datetime import datetime
from jobs.ats.base import fetch_json, fetch_json_post, slugify, validate_company_match



# Workday uses different instance numbers
WD_VARIANTS = [
    "wd5", "wd1", "wd2", "wd3", "wd4",
    "wd6", "wd7", "wd8", "wd10", "wd12",  # extended variants
]

# Base URL templates — two domains used by Workday
# myworkdayjobs: {tenant}.{wd}.myworkdayjobs.com/wday/cxs/{tenant}/{path}/jobs
# myworkdaysite: {wd}.myworkdaysite.com/wday/cxs/{tenant}/{path}/jobs
BASE_URL      = "https://{slug}.{wd}.myworkdayjobs.com/wday/cxs/{slug}/{path}/jobs"
BASE_URL_SITE = "https://{wd}.myworkdaysite.com/wday/cxs/{slug}/{path}/jobs"


def _build_url(slug_info):
    """Build correct API URL based on which Workday domain is used."""
    slug = slug_info["slug"]
    wd   = slug_info["wd"]
    path = slug_info.get("path", "careers")
    if slug_info.get("site") == "myworkdaysite":
        return BASE_URL_SITE.format(slug=slug, wd=wd, path=path)
    return BASE_URL.format(slug=slug, wd=wd, path=path)

# Common path variants to try per company
# Ordered by frequency of use
WD_PATH_VARIANTS = [
    "careers",
    "External",
    "jobs",
    "Careers",
    "career",
    # Note: "en-US" removed — it is a locale prefix, never a career site name
    # e.g. nvidia.wd5.myworkdayjobs.com/en-US/NVIDIAExternalCareerSite/...
    #      the API path needs "NVIDIAExternalCareerSite" not "en-US"
]


def detect(company):
    """
    Try to detect if company uses Workday.
    Tries all slug variants × WD instance variants × path variants.
    Returns (slug_info, sample_jobs) or (None, None).
    slug_info = {"slug": "capitalone", "wd": "wd12", "path": "Capital_One"}
    """
    for slug in slugify(company):
        for wd in WD_VARIANTS:
            for path in _get_path_variants(slug, company):
                url = _build_url({"slug": slug, "wd": wd, "path": path})
                data = fetch_json_post(url, body={"limit": 20, "offset": 0})
                if data is None:
                    continue
                jobs = data.get("jobPostings", [])
                if not isinstance(jobs, list):
                    continue
                if len(jobs) == 0:
                    # Valid Workday structure confirmed
                    if "total" in data:
                        return {"slug": slug, "wd": wd, "path": path}, []
                    continue
                # Validate company match
                first_title = jobs[0].get("title", "")
                first_url   = jobs[0].get("externalUrl", "")
                if not validate_company_match(
                    first_url + first_title, company
                ):
                    continue
                return {"slug": slug, "wd": wd, "path": path}, jobs
    return None, None


def _get_path_variants(slug, company):
    """
    Generate path variants to try for a given company.
    Includes common paths + company-name-derived paths.
    """
    import re
    # Company name → CamelCase path (e.g. "Capital One" → "Capital_One")
    words   = re.sub(r"[^a-zA-Z0-9\s]", "", company).split()
    camel   = "_".join(w.capitalize() for w in words if w)
    camel2  = "".join(w.capitalize() for w in words if w)
    slug_up = slug.capitalize()

    # Deduplicate preserving order
    seen     = set()
    variants = []
    for v in WD_PATH_VARIANTS + [camel, camel2, slug_up, slug]:
        if v and v not in seen:
            seen.add(v)
            variants.append(v)
    return variants


def fetch_jobs(slug_info, company):
    """
    Fetch all jobs for company from Workday.
    Handles pagination.
    Returns list of normalized job dicts.
    slug_info = {"slug": "capitalone", "wd": "wd12", "path": "Capital_One"}
    """
    slug = slug_info.get("slug", "")
    wd   = slug_info.get("wd", "")
    url  = _build_url(slug_info)

    all_jobs = []
    offset   = 0
    limit    = 20  # Workday default page size
    total    = None  # only populated on first page

    while True:
        data = fetch_json_post(url, body={"limit": limit, "offset": offset})
        if not data:
            break
        jobs = data.get("jobPostings", [])
        if not jobs:
            break
        all_jobs.extend(jobs)
        # total is only returned on first page — cache it
        if total is None:
            total = data.get("total", 0)
        offset += len(jobs)
        # Stop if: fewer jobs than limit (last page)
        # or we have fetched everything
        if len(jobs) < limit or (total and offset >= total):
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

    # Build job URL — use _build_url so myworkdaysite tenants get correct URL
    external_url = job.get("externalUrl", "")
    if not external_url:
        job_id       = job.get("bulletFields", [""])[0] if job.get("bulletFields") else ""
        slug_info_fb = {"slug": slug, "wd": wd, "path": "careers"}
        base         = _build_url(slug_info_fb).replace("/jobs", "")
        external_url = f"{base}/job/{job_id}" if job_id else base

    return {
        "company":     company,
        "title":       job.get("title", ""),
        "job_url":     external_url,
        "location":    job.get("locationsText", ""),
        "posted_at":   posted_at,
        "description": " ".join(job.get("bulletFields", [])),
        "ats":         "workday",
    }