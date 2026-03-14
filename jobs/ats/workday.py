# jobs/ats/workday.py — Workday undocumented public API client
# Date field: postedOn (RELIABLE — original posting date)
# Risk: undocumented — monitor for structural changes

from datetime import datetime
from jobs.ats.base import fetch_json, fetch_json_post, slugify, validate_company_match


WD_VARIANTS = [
    "wd5", "wd1", "wd2", "wd3", "wd4",
    "wd6", "wd7", "wd8", "wd10", "wd12",
]

BASE_URL      = "https://{slug}.{wd}.myworkdayjobs.com/wday/cxs/{slug}/{path}/jobs"
BASE_URL_SITE = "https://{wd}.myworkdaysite.com/wday/cxs/{slug}/{path}/jobs"

# Base domain — used to construct full job URLs
BASE_DOMAIN      = "https://{slug}.{wd}.myworkdayjobs.com"
BASE_DOMAIN_SITE = "https://{wd}.myworkdaysite.com"

WD_PATH_VARIANTS = [
    "careers",
    "External",
    "jobs",
    "Careers",
    "career",
]

# Workday requires full browser headers — plain User-Agent causes total=0 on page 2+
WORKDAY_HEADERS = {
    "User-Agent":      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Accept":          "application/json",
    "Content-Type":    "application/json",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer":         "https://www.myworkdayjobs.com/",
}


def _build_url(slug_info):
    """Build correct API URL based on which Workday domain is used."""
    slug = slug_info["slug"]
    wd   = slug_info["wd"]
    path = slug_info.get("path", "careers")
    if slug_info.get("site") == "myworkdaysite":
        return BASE_URL_SITE.format(slug=slug, wd=wd, path=path)
    return BASE_URL.format(slug=slug, wd=wd, path=path)


def _build_domain(slug_info):
    """Build base domain for constructing full job URLs."""
    slug = slug_info["slug"]
    wd   = slug_info["wd"]
    if slug_info.get("site") == "myworkdaysite":
        return BASE_DOMAIN_SITE.format(wd=wd)
    return BASE_DOMAIN.format(slug=slug, wd=wd)


def detect(company):
    """
    Try to detect if company uses Workday.
    Returns (slug_info, sample_jobs) or (None, None).
    """
    for slug in slugify(company):
        for wd in WD_VARIANTS:
            for path in _get_path_variants(slug, company):
                url  = _build_url({"slug": slug, "wd": wd, "path": path})
                data = fetch_json_post(url, body={"limit": 20, "offset": 0},
                                       headers=WORKDAY_HEADERS)
                if data is None:
                    continue
                jobs = data.get("jobPostings", [])
                if not isinstance(jobs, list):
                    continue
                if len(jobs) == 0:
                    if "total" in data:
                        return {"slug": slug, "wd": wd, "path": path}, []
                    continue
                first_title = jobs[0].get("title", "")
                # Support both externalPath (real API) and externalUrl (tests/legacy)
                first_path  = jobs[0].get("externalPath", "") or jobs[0].get("externalUrl", "")
                if not validate_company_match(first_path + first_title, company):
                    continue
                return {"slug": slug, "wd": wd, "path": path}, jobs
    return None, None


def _get_path_variants(slug, company):
    import re
    words   = re.sub(r"[^a-zA-Z0-9\s]", "", company).split()
    camel   = "_".join(w.capitalize() for w in words if w)
    camel2  = "".join(w.capitalize() for w in words if w)
    slug_up = slug.capitalize()

    seen, variants = set(), []
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
    slug_info = {"slug": "att", "wd": "wd1", "path": "ATTGeneral"}
    """
    url    = _build_url(slug_info)
    domain = _build_domain(slug_info)
    path   = slug_info.get("path", "careers")  # e.g. "ATTGeneral"

    all_jobs = []
    offset   = 0
    limit    = 20
    total    = None

    while True:
        data = fetch_json_post(url, body={"limit": limit, "offset": offset},
                               headers=WORKDAY_HEADERS)
        if not data:
            break
        jobs = data.get("jobPostings", [])
        if not jobs:
            break
        all_jobs.extend(jobs)
        # total only comes back reliably on page 1 — cache once, never overwrite
        if total is None:
            total = data.get("total", 0)
        offset += len(jobs)
        if total is not None:
            if offset >= total:
                break
        elif len(jobs) < limit:
            break

    results = []
    for j in all_jobs:
        if not j.get("title"):
            continue
        norm = _normalize(j, company, domain, path)
        if norm is not None:
            results.append(norm)
    return results


def _normalize(job, company, domain, path):
    """
    Normalize Workday job to standard format.

    externalPath from API: /job/Reynoldsburg-Ohio/Sr-B2B-Sales_R-104046
    Full URL:  https://{domain}/{path}/job/Reynoldsburg-Ohio/Sr-B2B-Sales_R-104046
    e.g.:      https://att.wd1.myworkdayjobs.com/ATTGeneral/job/Reynoldsburg-Ohio/...
    """
    posted_at = None
    posted = job.get("postedOn", "")
    if posted:
        try:
            if "/" in posted:
                posted_at = datetime.strptime(posted, "%m/%d/%Y")
            elif "T" in posted or posted.endswith("Z"):
                posted_at = datetime.fromisoformat(posted.replace("Z", "+00:00"))
            elif "today" in posted.lower():
                posted_at = datetime.utcnow()
            elif "day" in posted.lower():
                import re as _re
                from datetime import timedelta
                m = _re.search(r"(\d+)", posted)
                if m:
                    posted_at = datetime.utcnow() - timedelta(days=int(m.group(1)))
        except (ValueError, AttributeError):
            pass

    # Support both externalPath (real API) and externalUrl (tests/legacy)
    # externalPath: /job/Reynoldsburg-Ohio/Sr-B2B-Sales_R-104046  (relative, needs domain+path prefix)
    # externalUrl:  https://jpmorgan.wd5.myworkdayjobs.com/1       (absolute, use as-is)
    external_path = job.get("externalPath", "").strip()
    external_url  = job.get("externalUrl",  "").strip()

    
    if external_path:
        # Relative path from real Workday API — prepend domain + career site name
        job_url = domain.rstrip("/") + "/" + path.strip("/") + "/" + external_path.lstrip("/")
    elif external_url and external_url.startswith("http"):
        # Absolute URL provided directly (legacy/test data)
        job_url = external_url
    else:
        # Neither field present — build a best-effort fallback from domain + path
        # so we never silently drop a job with a valid title
        job_url = domain.rstrip("/") + "/" + path.strip("/")

    return {
        "company":     company,
        "title":       job.get("title", ""),
        "job_url":     job_url,
        "location":    job.get("locationsText", ""),
        "posted_at":   posted_at,
        "description": " ".join(job.get("bulletFields", [])),
        "ats":         "workday",
    }