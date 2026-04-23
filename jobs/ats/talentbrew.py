# jobs/ats/talentbrew.py — TalentBrew (Radancy) job board scraper
#
# TalentBrew does not expose a public JSON API.
# Jobs are discovered via sitemap.xml and detail pages scraped via JSON-LD.
#
# URL pattern:
#   jobs.intuit.com/job/{city}/{title-slug}/{tenant_id}/{job_id}
#   e.g. jobs.intuit.com/job/mountain-view/senior-staff-data-scientist/27595/87495090384
#
# Sitemap:
#   jobs.intuit.com/sitemap.xml → 1803 URLs including 709 job URLs
#   IMPORTANT: sitemap has BOM (ï»¿) prefix — must use html.parser not xml parser
#
# Option C freshness strategy:
#   1. Fetch sitemap.xml → extract all /job/{city}/{slug}/{tenant}/{id} URLs
#   2. Compare with DB → find new job IDs only
#   3. Fetch detail page ONLY for new jobs
#   4. Extract all fields from JSON-LD JobPosting schema
#   5. Store in job_postings
#
# Date field: JSON-LD datePosted — format "2025-10-2" (partial ISO, no zero-padding)
#             RELIABLE original posting date
# Job ID:     JSON-LD identifier (integer) e.g. 17030
#             Also in URL last segment e.g. 87495090384 (external ID)
#             identifier from JSON-LD is the ATS req ID — use as job_id
#
# Slug format stored in DB:
#   {"base": "https://jobs.intuit.com", "tenant_id": "27595"}
#
# NOTE on tenant_id drift:
#   TalentBrew tenant IDs can change when companies migrate or restructure
#   their Radancy instance (observed: Schwab changed from 27326 → 33727).
#   fetch_jobs() auto-detects the live tenant_id from the sitemap and updates
#   slug_info in-place so the caller can persist the corrected value.
#   The stored tenant_id is used only as a hint / starting point.

import re
import json
import html as html_lib
from collections import Counter
from datetime import datetime
from bs4 import BeautifulSoup
from jobs.ats.base import fetch_html, slugify, validate_company_match


# ─────────────────────────────────────────
# CONSTANTS
# ─────────────────────────────────────────

# /job/{city}/{title-slug}/{tenant_id}/{job_id}
JOB_URL_RE   = re.compile(
    r"/job/[^/]+/[^/]+/(\d+)/(\d+)/?$",
    re.IGNORECASE
)
DATE_FORMATS = ["%Y-%m-%d", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%SZ"]

# Known TalentBrew custom domains
KNOWN_DOMAINS = {
    "jobs.intuit.com":        {"tenant_id": "27595"},
    "jobs.disneycareers.com": {"tenant_id": "391"},
    # Schwab: tenant_id intentionally omitted — auto-detected from sitemap
    # because it drifted from 27326 → 33727. Will be resolved at runtime.
    "www.schwabjobs.com":     {"tenant_id": ""},
}


# ─────────────────────────────────────────
# DETECTION
# ─────────────────────────────────────────

def detect(company):
    """
    Try to detect if company uses TalentBrew/Radancy.

    Strategy:
      1. Check known custom domains
      2. Probe sitemap for TalentBrew URL pattern

    Returns (slug_info dict, sample_jobs) or (None, None).
    slug_info = {"base": "https://...", "tenant_id": "27595"}
    """
    for domain, config in KNOWN_DOMAINS.items():
        sitemap_url = f"https://{domain}/sitemap.xml"
        resp        = fetch_html(sitemap_url, platform="talentbrew", track=False)
        if resp is None:
            continue

        # Use html.parser — BOM prefix breaks xml parser
        soup = BeautifulSoup(resp.text, "html.parser")

        # Auto-detect tenant_id from sitemap — don't trust stored value
        # (tenant IDs can drift when companies migrate Radancy instances)
        live_tenant_id = _detect_tenant_id(soup)
        if not live_tenant_id:
            continue

        if not validate_company_match(domain, company):
            continue

        slug_info = {
            "base":      f"https://{domain}",
            "tenant_id": live_tenant_id,
        }
        job_urls = _extract_job_urls(soup, live_tenant_id)
        sample   = _urls_to_stubs(job_urls[:3], slug_info, company)
        return slug_info, sample

    return None, None


# ─────────────────────────────────────────
# FETCH JOBS (listing via sitemap)
# ─────────────────────────────────────────

def fetch_jobs(slug_info, company):
    """
    Fetch all jobs for company from TalentBrew via sitemap.xml.

    Args:
        slug_info: dict with "base" and "tenant_id" keys
                   or JSON string (stored in DB)
        company:   company name

    Returns:
        List of normalized job dicts.
        All fields filled by fetch_job_detail().

    Side-effect:
        If the live tenant_id in the sitemap differs from the stored one,
        slug_info["tenant_id"] is updated in-place so the caller can
        persist the corrected value back to the DB.
    """
    if not slug_info:
        return []

    if isinstance(slug_info, str):
        try:
            slug_info = json.loads(slug_info)
        except (json.JSONDecodeError, TypeError):
            return []

    base      = slug_info.get("base", "")
    tenant_id = slug_info.get("tenant_id", "")
    if not base:
        return []

    sitemap_url = f"{base}/sitemap.xml"
    # (connect_timeout=10, read_timeout=None):
    #   - 10s to establish the TCP connection (catches dead hosts fast)
    #   - No read timeout — once data flows, download completes regardless
    #     of sitemap size (UnitedHealthGroup has 5000+ jobs; hard-capping
    #     the read would break large tenants every time they add more jobs)
    resp = fetch_html(sitemap_url, platform="talentbrew", timeout=(10, None))
    if resp is None:
        return []

    # IMPORTANT: use html.parser — BOM prefix breaks xml/lxml-xml parsers
    soup = BeautifulSoup(resp.text, "html.parser")

    # Always auto-detect the live tenant_id from the sitemap.
    # Stored tenant_id is used as a fallback only — it can drift when
    # companies migrate Radancy instances (e.g. Schwab: 27326 → 33727).
    live_tenant_id = _detect_tenant_id(soup)

    if live_tenant_id and live_tenant_id != tenant_id:
        import logging
        logging.getLogger(__name__).warning(
            "talentbrew: tenant_id mismatch for %s — stored=%s live=%s — "
            "updating slug_info (caller should persist this to DB)",
            company, tenant_id, live_tenant_id,
        )
        slug_info["tenant_id"] = live_tenant_id  # update in-place for caller
        tenant_id = live_tenant_id

    if not tenant_id:
        import logging
        logging.getLogger(__name__).error(
            "talentbrew: could not determine tenant_id for %s — aborting",
            company,
        )
        return []

    job_urls = _extract_job_urls(soup, tenant_id)
    return _urls_to_stubs(job_urls, slug_info, company)


# ─────────────────────────────────────────
# FETCH JOB DETAIL
# ─────────────────────────────────────────

def fetch_job_detail(job):
    """
    Fetch job detail page for a single job.
    Extracts all fields from JSON-LD JobPosting schema.

    TalentBrew job pages include a complete JSON-LD JobPosting block:
      title       → job title
      datePosted  → partial ISO date e.g. "2025-10-2" (no zero padding)
      description → real HTML (not encoded) → strip with BeautifulSoup
      jobLocation → list of Place objects with address
      identifier  → integer ATS req ID (e.g. 17030)

    Called only for NEW jobs (Option C strategy).
    """
    job_url = job.get("job_url", "")
    if not job_url:
        return job

    resp = fetch_html(job_url, platform="talentbrew")
    if resp is None:
        return job

    try:
        soup = BeautifulSoup(resp.text, "html.parser")
        ld   = _extract_json_ld(soup)

        if not ld:
            return _fallback_from_meta(soup, job)

        title       = ld.get("title", "") or job.get("title", "")
        date_str    = ld.get("datePosted", "")
        posted_at   = _parse_date(date_str)
        description = _extract_description(ld)
        location    = _extract_location(ld)

        # Job ID — use identifier (ATS req ID) as canonical job_id
        # External job_id from URL is a different system ID
        identifier = ld.get("identifier")
        if identifier and str(identifier).isdigit():
            job_id = str(identifier)
        else:
            job_id = job.get("job_id", "")

        job              = dict(job)
        job["title"]       = title
        job["location"]    = location
        job["posted_at"]   = posted_at
        job["description"] = description
        job["job_id"]      = job_id

        return job

    except Exception:
        return job


# ─────────────────────────────────────────
# HELPERS — SITEMAP PARSING
# ─────────────────────────────────────────

def _detect_tenant_id(soup):
    """
    Auto-detect the dominant tenant_id from sitemap job URLs.

    Finds all /job/.../tenant_id/job_id URLs and returns the tenant_id
    that appears most frequently. This is robust to mixed-tenant sitemaps
    and handles tenant ID drift without requiring a DB update first.

    Returns the tenant_id string, or "" if no job URLs found.
    """
    counts = Counter()
    for loc in soup.find_all("loc"):
        m = JOB_URL_RE.search(loc.text.strip())
        if m:
            counts[m.group(1)] += 1

    if not counts:
        return ""

    # Return the most common tenant_id — handles edge cases where a sitemap
    # may contain a handful of stale URLs from a prior tenant ID
    return counts.most_common(1)[0][0]


def _extract_job_urls(soup, tenant_id=""):
    """
    Extract individual job URLs from TalentBrew sitemap.
    Pattern: /job/{city}/{title-slug}/{tenant_id}/{job_id}

    If tenant_id provided, only match URLs for that tenant.
    """
    job_urls = []
    for loc in soup.find_all("loc"):
        url = loc.text.strip()
        m   = JOB_URL_RE.search(url)
        if not m:
            continue
        # Filter by tenant_id if provided
        if tenant_id and m.group(1) != tenant_id:
            continue
        job_urls.append(url)
    return job_urls


def _extract_job_id(url):
    """
    Extract external job ID from TalentBrew URL (last numeric segment).
    /job/mountain-view/senior-staff/.../27595/87495090384 → "87495090384"
    """
    m = JOB_URL_RE.search(url)
    return m.group(2) if m else None


def _title_from_url(url):
    """
    Extract human-readable title from URL slug.
    /job/mountain-view/senior-staff-data-scientist/27595/123 → "Senior Staff Data Scientist"
    """
    # Pattern: /job/{city}/{title-slug}/{tenant}/{id}
    m = re.search(r"/job/[^/]+/([^/]+)/\d+/\d+/?$", url)
    if not m:
        return ""
    return m.group(1).replace("-", " ").strip()


def _urls_to_stubs(job_urls, slug_info, company):
    """Convert list of sitemap URLs to job stub dicts."""
    jobs     = []
    seen_ids = set()

    for url in job_urls:
        job_id = _extract_job_id(url)
        if not job_id or job_id in seen_ids:
            continue
        seen_ids.add(job_id)

        title = _title_from_url(url)

        jobs.append({
            "company":     company,
            "title":       title,
            "job_url":     url,
            "job_id":      job_id,
            "location":    "",    # filled by fetch_job_detail
            "posted_at":   None,  # filled by fetch_job_detail
            "description": "",    # filled by fetch_job_detail
            "ats":         "talentbrew",
            "_slug_info":  slug_info,
        })

    return jobs


# ─────────────────────────────────────────
# HELPERS — JSON-LD EXTRACTION
# ─────────────────────────────────────────

def _extract_json_ld(soup):
    """Extract JobPosting JSON-LD. Returns parsed dict or None."""
    for script in soup.find_all("script", type="application/ld+json"):
        try:
            data = json.loads(script.string or "")
            if isinstance(data, list):
                data = next(
                    (d for d in data if d.get("@type") == "JobPosting"), {}
                )
            if data.get("@type") == "JobPosting":
                return data
        except (json.JSONDecodeError, TypeError):
            continue
    return None


def _extract_location(ld):
    """
    Extract location from JSON-LD jobLocation.

    TalentBrew structure (list of Place objects):
      "jobLocation": [
        {
          "@type": "Place",
          "address": {
            "addressLocality": "Mountain View",
            "addressRegion":   "California",
            "addressCountry":  "United States"
          }
        }
      ]
    Returns first location as "City, State, Country".
    """
    try:
        locations = ld.get("jobLocation", [])
        if isinstance(locations, dict):
            locations = [locations]
        if not locations:
            return ""
        addr  = locations[0].get("address", {})
        parts = [
            addr.get("addressLocality", ""),
            addr.get("addressRegion", ""),
            addr.get("addressCountry", ""),
        ]
        return ", ".join(p for p in parts if p)
    except Exception:
        return ""


def _extract_description(ld):
    """
    Extract plain text description from JSON-LD.
    TalentBrew stores real HTML in description (not encoded).
    BeautifulSoup strips tags directly — no html.unescape() needed.
    """
    raw = ld.get("description", "")
    if not raw:
        return ""
    soup = BeautifulSoup(raw, "html.parser")
    text = soup.get_text(separator="\n", strip=True)
    text = re.sub(r"\n{3,}", "\n\n", text).strip()
    return text[:5000]


def _fallback_from_meta(soup, job):
    """Fallback when JSON-LD is missing — extract from og:title meta."""
    og    = soup.find("meta", {"property": "og:title"})
    title = ""
    if og:
        raw   = og.get("content", "")
        # Strip "job in City | Category at Company" suffix
        title = re.sub(
            r"\s*(job in|at)\s+.+$", "", raw, flags=re.IGNORECASE
        ).strip()
    job          = dict(job)
    job["title"] = title or job.get("title", "")
    return job


# ─────────────────────────────────────────
# HELPERS — UTILITY
# ─────────────────────────────────────────

def _parse_date(date_str):
    """
    Parse TalentBrew date string to datetime or None.
    TalentBrew uses partial ISO without zero-padding: "2025-10-2" not "2025-10-02"
    Standard datetime.strptime("%Y-%m-%d") fails on this — handle manually.
    """
    if not date_str:
        return None
    date_str = date_str.strip()

    # Try standard formats first
    for fmt in DATE_FORMATS:
        try:
            return datetime.strptime(date_str, fmt)
        except ValueError:
            continue

    # Handle partial ISO: "2025-10-2" (no zero padding)
    try:
        parts = date_str.split("T")[0].split("-")
        if len(parts) == 3:
            return datetime(int(parts[0]), int(parts[1]), int(parts[2]))
    except (ValueError, IndexError):
        pass

    return None