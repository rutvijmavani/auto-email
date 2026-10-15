# jobs/ats/workday.py — Workday undocumented public API client
# Date field: postedOn (RELIABLE — original posting date)
# Risk: undocumented — monitor for structural changes

import logging
from datetime import datetime
from config import FETCH_TOLERANCE_PCT, WORKDAY_FACET_SPLIT_AT, WORKDAY_PAGE_SIZE, WORKDAY_RESULT_CAP, WORKDAY_SPLIT_FACETS
from jobs.ats.base import fetch_json, fetch_json_post, slugify, validate_company_match
from jobs.ats.fetch_report import (
    FetchReport, STOP_ALL_PAGES, STOP_PAGE_CAP, STOP_REQUEST_FAILED, TOTAL_ATS, TOTAL_FACET_SUM,
    attach, count_distinct,
)
from jobs.utils import clean_html

logger = logging.getLogger(__name__)


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

    # Workday caps every query at WORKDAY_RESULT_CAP results (Nvidia reports total=2000 for 2700 jobs;
    # offsets past the cap repeat the last page). A board at/above WORKDAY_FACET_SPLIT_AT is therefore
    # fetched as one query per facet value (jobFamilyGroup, then timeType/workerSubType if a slice is
    # still too large) and the slices are merged by job path.
    rep      = {"pages": 0, "failed": False, "capped": False, "expected": None}
    first    = _fetch_pages(url, {}, company, defer_large=True, rep=rep)
    all_jobs = _collect(url, {}, (), first, company, rep)

    results = []
    for j in all_jobs:
        if not j.get("title"):
            continue
        norm = _normalize(j, company, domain, path, slug_info)
        if norm is not None:
            results.append(norm)

    if rep["failed"]:
        stop = STOP_REQUEST_FAILED
    elif rep["capped"]:
        stop = STOP_PAGE_CAP
    else:
        stop = STOP_ALL_PAGES
    split = rep["expected"] is not None
    return attach(results, FetchReport(
        stop_reason      = stop,
        pages            = rep["pages"],
        fetched_distinct = count_distinct(results),
        unusable         = len(all_jobs) - len(results),
        reported_total   = rep["expected"] if split else first[1],
        total_source     = TOTAL_FACET_SUM if split else TOTAL_ATS,
        note             = "split by facet" if split else "",
    ))


def _job_key(posting):
    """Identity of a listing across facet slices: its relative path, else req id, else title+location."""
    return (posting.get("externalPath")
            or (posting.get("bulletFields") or [""])[0]
            or f"{posting.get('title', '')}|{posting.get('locationsText', '')}")


def _fetch_pages(url, applied, company, defer_large=False, rep=None):
    """
    Page one query (optionally filtered by appliedFacets) until its reported total is reached.

    Returns (postings, total, facets). `total` and `facets` come from page 1 only (later pages report
    total=0 on some tenants). A failed or empty page before the total is reached stops the loop with a
    WARNING instead of looking like a normal end of list.

    defer_large: stop after page 1 when the total is at/above WORKDAY_FACET_SPLIT_AT — the caller splits
    by facet instead, so paging the capped query would be wasted requests (_collect re-pages it in full
    only when no facet can split it).

    rep: optional dict collecting the completeness facts ("pages" count, "failed" on a failed or
    short-ended page) for the FetchReport built in fetch_jobs.
    """
    postings, offset, total, facets = [], 0, None, []
    while True:
        body = {"limit": WORKDAY_PAGE_SIZE, "offset": offset}
        if applied:
            body["appliedFacets"] = applied
        data = fetch_json_post(url, body=body, headers=WORKDAY_HEADERS,
                               platform="workday")  # tracked for api_health
        if rep is not None:
            rep["pages"] += 1
        if not data:
            if rep is not None:
                rep["failed"] = True
            if offset or total:
                logger.warning("workday fetch: request failed at offset=%d (total=%s) for %r applied=%s "
                               "— partial result", offset, total, company, applied)
            break
        jobs = data.get("jobPostings", [])
        if total is None:
            total  = data.get("total", 0)
            facets = data.get("facets") or []
        if not jobs:
            if total and offset < total:
                if rep is not None:
                    rep["failed"] = True
                logger.warning("workday fetch: empty page at offset=%d of total=%d for %r applied=%s "
                               "— partial result", offset, total, company, applied)
            break
        postings.extend(jobs)
        offset += len(jobs)
        if offset >= total:
            break
        if defer_large and total >= WORKDAY_FACET_SPLIT_AT:
            break
    return postings, total or 0, facets


def _flatten_facets(facets):
    """
    Expand grouped facets. Some tenants (Accenture's `locationMainGroup`) hold no values of their own: they
    hold groups ("Country", "City"), each with its own facetParameter ("locationCountry", "locations") and
    id/count values. The group's facetParameter is what appliedFacets takes, so each group is yielded as a
    facet of its own; plain facets pass through unchanged.
    """
    out = []
    for f in facets:
        if not isinstance(f, dict):
            continue
        groups = [g for g in f.get("values") or []
                  if isinstance(g, dict) and g.get("facetParameter") and isinstance(g.get("values"), list)]
        out.extend(groups if groups else [f])
    return out


def _pick_split_facet(facets, used):
    """First configured split facet not yet applied that offers 2+ non-empty values; else (None, [])."""
    by_name = {f.get("facetParameter"): f for f in _flatten_facets(facets)}
    for name in WORKDAY_SPLIT_FACETS:
        if name in used or name not in by_name:
            continue
        values = [v for v in by_name[name].get("values") or [] if v.get("id") and v.get("count", 0) > 0]
        if len(values) >= 2:
            return name, values
    return None, []


def _collect(url, applied, used, first, company, rep=None, slice_count=None):
    """
    Return every posting reachable for `applied`: the plain paged result when it is under the split
    threshold, otherwise the union (deduped by _job_key) of one query per facet value, recursing with
    another facet when a slice is still at the threshold.

    rep (see _fetch_pages) also receives "capped" when a slice cannot be split below the result cap, and
    "expected" — the top-level facet-count sum — when the board was split.

    slice_count: the parent facet's own count for this slice. A further split can lose jobs that carry no
    value for the new facet (e.g. a city), so the merged result is checked against it and flagged capped
    when it falls more than FETCH_TOLERANCE_PCT short.
    """
    postings, total, facets = first
    if total < WORKDAY_FACET_SPLIT_AT:
        return postings
    name, values = _pick_split_facet(facets, used)
    if not name:
        logger.warning("workday fetch: %r reports %d jobs (cap %d) and cannot be split further "
                       "(applied=%s) — result is incomplete", company, total, WORKDAY_RESULT_CAP, applied)
        if rep is not None and total >= WORKDAY_RESULT_CAP:
            rep["capped"] = True
        return _fetch_pages(url, applied, company, rep=rep)[0]   # first was deferred after page 1: page it in full
    merged = {}
    for value in values:
        slice_applied = {**applied, name: [value["id"]]}
        for p in _collect(url, slice_applied, used + (name,),
                          _fetch_pages(url, slice_applied, company, defer_large=True, rep=rep),
                          company, rep, value.get("count")):
            merged.setdefault(_job_key(p), p)
    if slice_count and len(merged) < slice_count * (1 - FETCH_TOLERANCE_PCT / 100):
        logger.warning("workday fetch: %r slice %s reports %d jobs but split by %s fetched %d distinct "
                       "— some jobs carry no %s value", company, applied, slice_count, name, len(merged), name)
        if rep is not None:
            rep["capped"] = True
    expected = sum(v.get("count", 0) for v in values)
    if rep is not None and not used:
        rep["expected"] = expected
    if len(merged) < expected:
        logger.warning("workday fetch: %r split by %s fetched %d distinct of %d expected (applied=%s)",
                       company, name, len(merged), expected, applied)
    else:
        logger.info("workday fetch: %r split by %s into %d slices, %d distinct jobs",
                    company, name, len(values), len(merged))
    return list(merged.values())


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
        logger.debug(
            "workday.fetch_job_detail: API returned None/empty for %r | %s | url=%s",
            job.get("company"), job.get("job_id"), url,
        )
        return job

    info = data.get("jobPostingInfo", {})
    if not info:
        logger.debug(
            "workday.fetch_job_detail: jobPostingInfo absent/empty for %r | %s "
            "| top-level keys=%s",
            job.get("company"), job.get("job_id"), list(data.keys()),
        )
        return job

    job = dict(job)

    # ── Description ───────────────────────────────────────────────────────
    desc = info.get("jobDescription", "")
    if desc:
        job["description"] = clean_html(desc)

    # ── Country code (Tier 1) ─────────────────────────────────────────────
    # jobRequisitionLocation.country.alpha2Code is the authoritative signal.
    # Confirmed present across tenants: "US" (Gartner), "IN" (Capital One).
    # Stored as _country_code; job_monitor uses it before text parsing.
    # Falls back to "" — job_monitor then uses descriptor-embedded location.
    req_loc = info.get("jobRequisitionLocation") or {}
    alpha2  = ((req_loc.get("country") or {}).get("alpha2Code") or "").upper()
    job["_country_code"] = alpha2

    # ── Location (display + Tier 3 fallback) ─────────────────────────────
    # _build_detail_location() embeds country.descriptor into the location
    # string for non-US jobs (e.g. "Bangalore, In" → "Bangalore, In, India")
    # so is_us_location() has the full country name if alpha2Code is absent.
    location = _build_detail_location(info)
    if location:
        job["location"] = location

    # ── Debug: log when both location and _country_code are absent ────────
    # This reveals ATCI / non-standard Workday job structures where the detail
    # API uses different field names than the parser expects.
    if not alpha2 and not location:
        logger.debug(
            "workday.fetch_job_detail: no location/alpha2 extracted for %r | %s "
            "| jobPostingInfo keys=%s | jobRequisitionLocation=%r "
            "| info.location=%r | info.country=%r",
            job.get("company"), job.get("job_id"),
            list(info.keys()),
            info.get("jobRequisitionLocation"),
            info.get("location"),
            info.get("country"),
        )

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
        # Append country descriptor when the country is non-US and not already
        # present in the primary string, e.g.:
        #   "London"       → "London, United Kingdom"
        #   "London, ENG"  → "London, ENG, United Kingdom"
        # Skip when country is US (state code already disambiguates) or when
        # the descriptor is already contained in the primary string.
        if country and "united states" not in country.lower() \
                and country.lower() not in primary.lower():
            primary = f"{primary}, {country}"
        parts.append(primary)

    for loc in additional:
        parsed = _parse_additional_location(loc)
        if parsed:
            # Append country descriptor to additional locations too, so
            # human-readable non-US entries like "Berlin" become "Berlin, Germany"
            # and is_us_location() can reject them via Signal 4.
            if country and "united states" not in country.lower() \
                    and country.lower() not in parsed.lower():
                parsed = f"{parsed}, {country}"
            if parsed not in parts:
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

    # When externalPath is absent but externalUrl is a job-specific URL (not just
    # the careers homepage), derive external_path from it so fetch_job_detail()
    # can build the CXS detail endpoint.  This handles tenants that omit the
    # relative-path field but return an absolute job URL.
    if not external_path and external_url and external_url.startswith("http"):
        from urllib.parse import urlparse as _urlparse
        _parsed_path = _urlparse(external_url).path  # e.g. "/en-US/NVIDIAExternalCareerSite/job/Loc/Title_R-1"
        _path_prefix = "/" + path.strip("/")          # e.g. "/NVIDIAExternalCareerSite"
        # Use find so locale-prefixed URLs (e.g. /en-US/NVIDIAExternalCareerSite/...)
        # are handled the same as unprefixed ones (/NVIDIAExternalCareerSite/...).
        _path_idx = _parsed_path.find(_path_prefix + "/")
        if _path_idx != -1:
            external_path = _parsed_path[_path_idx + len(_path_prefix):]  # e.g. "/job/Loc/Title_R-1"

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