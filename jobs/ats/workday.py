# jobs/ats/workday.py — Workday undocumented public API client
# Date field: postedOn (RELIABLE — original posting date)
# Risk: undocumented — monitor for structural changes

from datetime import datetime
from jobs.ats.base import fetch_json, fetch_json_post, slugify, validate_company_match
from jobs.utils import clean_html


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
                                       headers=WORKDAY_HEADERS,
                                       platform="workday", track=False)  # detection — don't track
                if data is None:
                    continue
                jobs = data.get("jobPostings", [])
                if not isinstance(jobs, list):
                    continue
                if len(jobs) == 0:
                    if "total" in data:
                        return {"slug": slug, "wd": wd, "path": path}, []
                    continue
                first_title = jobs[0].get("title") or ""
                # Support both externalPath (real API) and externalUrl (tests/legacy)
                first_path  = jobs[0].get("externalPath") or jobs[0].get("externalUrl") or ""
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
                               headers=WORKDAY_HEADERS,
                               platform="workday")  # tracked for api_health
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
        norm = _normalize(j, company, domain, path, slug_info)
        if norm is not None:
            results.append(norm)
    return results


def fetch_job_detail(job):
    """
    Fetch full job description from Workday detail endpoint.
    Called only for NEW jobs (Option C strategy).

    Confirmed URL pattern (tested against AT&T/wd1, State Street/wd1, Red Hat/wd5):
      GET https://{slug}.{wd}.myworkdayjobs.com/wday/cxs/{slug}/{path}{externalPath}
      e.g. https://att.wd1.myworkdayjobs.com/wday/cxs/att/ATTGeneral/job/Mesa-Arizona/..._R-107385

    Response: jobPostingInfo.jobDescription  — HTML, present in all tenants tested.
    Requires WORKDAY_HEADERS (plain User-Agent returns 406).

    Args:
        job: job dict from fetch_jobs() — must contain _slug, _wd, _path, _external_path

    Returns:
        Updated job dict with description filled.
    """
    slug          = job.get("_slug", "")
    wd            = job.get("_wd", "")
    path          = job.get("_path", "")
    external_path = job.get("_external_path", "")
    site          = job.get("_site")

    if not all([slug, wd, path, external_path]):
        return job

    # Build URL based on which Workday domain variant is used
    if site == "myworkdaysite":
        url = (
            f"https://{wd}.myworkdaysite.com"
            f"/wday/cxs/{slug}/{path}{external_path}"
        )
    else:
        url = (
            f"https://{slug}.{wd}.myworkdayjobs.com"
            f"/wday/cxs/{slug}/{path}{external_path}"
        )

    data = fetch_json(url, platform="workday", headers=WORKDAY_HEADERS)
    if not data:
        return job

    info = data.get("jobPostingInfo", {})
    if not info:
        return job

    job = dict(job)

    # ── Description ───────────────────────────────────────────────────────
    desc = info.get("jobDescription", "")
    if desc:
        job["description"] = clean_html(desc)

    # ── Location ──────────────────────────────────────────────────────────
    # Listing only gives locationsText ("2 Locations", "London") — too vague
    # for is_us_location() filtering and incomplete for display.
    # Detail gives precise location + additionalLocations + country fallback.
    location = _build_detail_location(info)
    if location:
        job["location"] = location

    return job


def _build_detail_location(info):
    """
    Build a clean location string from jobPostingInfo.

    Primary:   info["location"]             e.g. "Irving, TX"
    Additional: info["additionalLocations"] — two known formats:
      • Human-readable: "Fort Myers, FL"              (Gartner, most tenants)
      • Internal code:  "USA:AZ:Gilbert:addr:RET/RET" (AT&T)
    Country fallback: info["country"]["descriptor"]    e.g. "United States of America"
      Appended to primary when primary has no country context (e.g. bare "London").

    Returns semicolon-joined string of all unique locations, or "" if nothing found.
    """
    primary    = (info.get("location") or "").strip()
    additional = info.get("additionalLocations") or []
    country    = ((info.get("country") or {}).get("descriptor") or "").strip()

    parts = []

    if primary:
        # Append country descriptor when primary looks like a bare city/city+state
        # without a country component, e.g. "London" → "London, United Kingdom".
        # Skip when country is US (state code already disambiguates) or already present.
        if country and "united states" not in country.lower():
            if "," not in primary:
                primary = f"{primary}, {country}"
        parts.append(primary)

    for loc in additional:
        parsed = _parse_additional_location(loc)
        if parsed and parsed not in parts:
            parts.append(parsed)

    return "; ".join(parts)


def _parse_additional_location(loc):
    """
    Parse one additionalLocations entry — handles two formats seen in the wild:

    Human-readable (Gartner, most tenants):
        "Fort Myers, FL"  →  "Fort Myers, FL"

    Internal code (AT&T):
        "USA:AZ:Gilbert:2224 E Williams Field Rd:RET/RET"
        →  parts[0]=country code, [1]=state, [2]=city
        →  US:  "Gilbert, AZ"          (country omitted — state code is enough for S3)
        →  Non-US: "London, ENG, GBR"  (country code kept in UPPERCASE so Signal 2
                                         alpha-3 gate recognises it as non-US)

    Returns clean string or "" if unparseable.
    """
    if not loc:
        return ""
    loc = loc.strip()
    parts = loc.split(":")
    if len(parts) >= 3:
        country_code = parts[0].strip().upper()
        state        = parts[1].strip()
        city         = parts[2].strip()
        if country_code == "USA":
            # US — state code alone is sufficient for Signal 3 detection
            if city and state:
                return f"{city}, {state}"
            return city or ""
        else:
            # Non-US — include country code in UPPERCASE so Signal 2 can
            # recognise the alpha-3 code (e.g. "GBR") and return False
            if city and state:
                return f"{city}, {state}, {country_code}"
            elif city:
                return f"{city}, {country_code}"
            return country_code or ""
    # Human-readable — use as-is
    return loc


def _normalize(job, company, domain, path, slug_info=None):
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
            posted_lower = posted.lower().strip()
            # Check human-readable strings FIRST before ISO format
            # to avoid false matches (e.g. "T" in "Posted Today")
            if "today" in posted_lower:
                posted_at = datetime.utcnow()
            elif "yesterday" in posted_lower:
                from datetime import timedelta
                posted_at = datetime.utcnow() - timedelta(days=1)
            elif "30+" in posted:
                from datetime import timedelta
                posted_at = datetime.utcnow() - timedelta(days=30)
            elif "day" in posted_lower:
                import re as _re
                from datetime import timedelta
                m = _re.search(r"(\d+)", posted)
                if m:
                    posted_at = datetime.utcnow() - timedelta(days=int(m.group(1)))
            elif "/" in posted:
                posted_at = datetime.strptime(posted, "%m/%d/%Y")
            elif "T" in posted or posted.endswith("Z"):
                posted_at = datetime.fromisoformat(posted.replace("Z", "+00:00"))
        except (ValueError, AttributeError):
            pass

    # Support both externalPath (real API) and externalUrl (tests/legacy)
    # externalPath: /job/Reynoldsburg-Ohio/Sr-B2B-Sales_R-104046  (relative, needs domain+path prefix)
    # externalUrl:  https://jpmorgan.wd5.myworkdayjobs.com/1       (absolute, use as-is)
    external_path = (job.get("externalPath") or "").strip()
    external_url  = (job.get("externalUrl") or "").strip()

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

    # bulletFields[0] is the job req ID (e.g. "R-107385") — confirmed across tenants.
    # Fall back to regex on job_url for safety.
    import re as _re
    _bullet_id = (job.get("bulletFields") or [""])[0]
    if not _bullet_id:
        _wd_match  = _re.search(r'_((?:JR|R)-?\d+(?:-\d+)?)', job_url)
        _bullet_id = _wd_match.group(1) if _wd_match else ""

    _si = slug_info or {}
    return {
        "company":        company,
        "title":          job.get("title", ""),
        "job_url":        job_url,
        "location":       job.get("locationsText", ""),
        "posted_at":      posted_at,
        "description":    "",              # filled by fetch_job_detail
        "ats":            "workday",
        "job_id":         _bullet_id,
        # Detail-fetch fields — used by fetch_job_detail(), not stored in DB
        "_external_path": external_path,
        "_slug":          _si.get("slug", ""),
        "_wd":            _si.get("wd", ""),
        "_path":          path,
        "_site":          _si.get("site"),
    }