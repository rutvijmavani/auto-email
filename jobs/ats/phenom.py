# jobs/ats/phenom.py — Phenom People (TRM) job board scraper
#
# Phenom People does not expose a public JSON API.
# Jobs are discovered via sitemap.xml and detail pages scraped via JSON-LD.
#
# URL patterns:
#   Chewy: careers.chewy.com/us/en/job/{job_id}/{title-slug}
#   eBay:  jobs.ebayinc.com/us/en/job/{job_id}/{title-slug}
#
# Sitemap:
#   careers.chewy.com/us/en/sitemap.xml   → job URLs + category pages
#   jobs.ebayinc.com/us/en/sitemap1.xml   → job URLs (sub-sitemap)
#   jobs.ebayinc.com/us/en/sitemap.xml    → sitemap index → sitemap1.xml
#
# Option C freshness strategy:
#   1. Fetch sitemap.xml → extract all /job/{id}/{slug} URLs
#   2. Compare with DB → find new job IDs only
#   3. Fetch detail page ONLY for new jobs
#   4. Extract all fields from JSON-LD JobPosting schema
#   5. Store in job_postings
#
# Date field: JSON-LD datePosted (ISO format YYYY-MM-DD) — RELIABLE
# Job ID:     JSON-LD identifier.value (e.g. "R29110", "R0068508")
#
# Slug format stored in DB:
#   {"base": "https://careers.chewy.com", "path": "us/en"}
#   {"base": "https://jobs.ebayinc.com",  "path": "us/en"}

import re
import json
from datetime import datetime
import html
from bs4 import BeautifulSoup
from jobs.ats.base import fetch_html, slugify, validate_company_match

# ─────────────────────────────────────────
# CONSTANTS
# ─────────────────────────────────────────

JOB_URL_RE  = re.compile(r"/job/([^/]+)/[^/]+/?$", re.IGNORECASE)
DATE_FORMATS = ["%Y-%m-%d", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%SZ"]

# Known Phenom People custom domains
# Maps domain → {path, sitemap_path}
KNOWN_DOMAINS = {
    "careers.chewy.com": {
        "path":    "us/en",
        "sitemap": "us/en/sitemap.xml",
    },
    "jobs.ebayinc.com": {
        "path":    "us/en",
        "sitemap": "us/en/sitemap1.xml",  # eBay uses sub-sitemap directly
    },
}


# ─────────────────────────────────────────
# DETECTION
# ─────────────────────────────────────────

def detect(company):
    """
    Try to detect if company uses Phenom People.

    Strategy:
      1. Check known custom domains first
      2. Try {slug}.phenompeople.com (hosted variant)

    Returns (slug_info dict, sample_jobs) or (None, None).
    slug_info = {"base": "https://...", "path": "us/en"}
    """
    # Check known custom domains
    for domain, config in KNOWN_DOMAINS.items():
        sitemap_url = f"https://{domain}/{config['sitemap']}"
        resp        = fetch_html(sitemap_url, platform="phenom", track=False)
        if resp is None:
            continue

        soup     = BeautifulSoup(resp.text, "xml")
        job_urls = _extract_job_urls(soup)
        if not job_urls:
            continue

        # Validate company match using domain
        if not validate_company_match(domain, company):
            continue

        slug_info = {
            "base":    f"https://{domain}",
            "path":    config["path"],
            "sitemap": config["sitemap"],
        }
        sample = _urls_to_stubs(job_urls[:3], slug_info, company)
        return slug_info, sample

    # Try hosted Phenom subdomain: {slug}.phenompeople.com
    for slug in slugify(company):
        base        = f"https://{slug}.phenompeople.com"
        sitemap_url = f"{base}/sitemap.xml"
        resp        = fetch_html(sitemap_url, platform="phenom", track=False)
        if resp is None:
            continue

        soup     = BeautifulSoup(resp.text, "xml")
        job_urls = _extract_job_urls(soup)
        if not job_urls:
            continue

        slug_info = {
            "base":    base,
            "path":    "",
            "sitemap": "sitemap.xml",
        }
        sample = _urls_to_stubs(job_urls[:3], slug_info, company)
        return slug_info, sample

    return None, None


# ─────────────────────────────────────────
# FETCH JOBS (listing via sitemap)
# ─────────────────────────────────────────

def fetch_jobs(slug_info, company):
    """
    Fetch all jobs for company from Phenom People via sitemap.xml.

    Strategy:
      Parse sitemap → handle sitemap index (eBay) or direct job URLs (Chewy)
      Return job stubs — detail filled by fetch_job_detail()

    Args:
        slug_info: dict with "base", "path", "sitemap" keys
                   or JSON string (stored in DB)
        company:   company name

    Returns:
        List of normalized job dicts.
        All fields filled by fetch_job_detail().
    """
    if not slug_info:
        return []

    if isinstance(slug_info, str):
        try:
            slug_info = json.loads(slug_info)
        except (json.JSONDecodeError, TypeError):
            return []

    base    = slug_info.get("base", "")
    sitemap = slug_info.get("sitemap", "sitemap.xml")
    if not base:
        return []

    sitemap_url = f"{base}/{sitemap}"
    # (connect_timeout=10, read_timeout=None) — see talentbrew.py for rationale.
    # Applied to sub-sitemap fetches too (sitemap-index tenants have N child files).
    resp        = fetch_html(sitemap_url, platform="phenom", timeout=(10, None))
    if resp is None:
        return []

    soup = BeautifulSoup(resp.text, "xml")

    # Handle sitemap index — points to sub-sitemaps
    sub_sitemaps = soup.find_all("sitemap")
    if sub_sitemaps:
        all_urls = []
        for sub in sub_sitemaps:
            loc = sub.find("loc")
            if not loc:
                continue
            sub_resp = fetch_html(loc.text, platform="phenom", timeout=(10, None))
            if sub_resp is None:
                continue
            sub_soup = BeautifulSoup(sub_resp.text, "xml")
            all_urls.extend(_extract_job_urls(sub_soup))
        return _urls_to_stubs(all_urls, slug_info, company)

    # Direct job URLs in sitemap
    job_urls = _extract_job_urls(soup)
    return _urls_to_stubs(job_urls, slug_info, company)


# ─────────────────────────────────────────
# FETCH JOB DETAIL
# ─────────────────────────────────────────

def fetch_job_detail(job):
    """
    Fetch job detail page for a single job.
    Extracts all fields from JSON-LD JobPosting schema.

    Phenom People job pages include a complete JSON-LD JobPosting block:
      title       → job title
      datePosted  → ISO date string (YYYY-MM-DD) — reliable posted_at
      description → full job description HTML → converted to plain text
      jobLocation → address with city, state, country
      identifier  → {value: "R29110"} — canonical job ID

    Called only for NEW jobs (Option C strategy).

    Args:
        job: job dict from fetch_jobs()

    Returns:
        Updated job dict with all fields filled from JSON-LD.
    """
    job_url = job.get("job_url", "")
    if not job_url:
        return job

    resp = fetch_html(job_url, platform="phenom")
    if resp is None:
        return job

    try:
        soup   = BeautifulSoup(resp.text, "html.parser")
        ld     = _extract_json_ld(soup)

        if not ld:
            # JSON-LD missing — fall back to meta tags
            return _fallback_from_meta(soup, job)

        title       = ld.get("title", "") or job.get("title", "")
        date_str    = ld.get("datePosted", "")
        posted_at   = _parse_date(date_str)
        description = _extract_description(ld)
        location    = _extract_location(ld)

        # Job ID from identifier — more reliable than URL
        identifier = ld.get("identifier", {})
        if isinstance(identifier, dict):
            job_id = str(identifier.get("value", "")) or job.get("job_id", "")
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

def _extract_job_urls(soup):
    """
    Extract all individual job URLs from Phenom sitemap.
    Pattern: /job/{job_id}/{title-slug}
    Excludes category pages, landing pages, etc.
    """
    job_urls = []
    for loc in soup.find_all("loc"):
        url = loc.text.strip()
        if JOB_URL_RE.search(url):
            job_urls.append(url)
    return job_urls


def _extract_job_id(url):
    """Extract job ID from Phenom URL. e.g. R29110, R0068508"""
    m = JOB_URL_RE.search(url)
    return m.group(1) if m else None


def _title_from_url(url):
    """
    Extract human-readable title from URL slug.
    /job/R29110/Software-Engineer-II → "Software Engineer II"
    """
    m = re.search(r"/job/[^/]+/([^/?]+)/?$", url)
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
            "ats":         "phenom",
            "_slug_info":  slug_info,
        })

    return jobs


# ─────────────────────────────────────────
# HELPERS — JSON-LD EXTRACTION
# ─────────────────────────────────────────

def _extract_json_ld(soup):
    """
    Extract JobPosting JSON-LD from Phenom detail page.
    Returns parsed dict or None.
    """
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
    Extract location string from JSON-LD jobLocation.

    Phenom structure:
      "jobLocation": {
        "address": {
          "addressLocality": "Bellevue",
          "addressRegion":   "WA",
          "addressCountry":  "United States"
        }
      }
    Also handles list of locations.
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
    Phenom stores HTML-encoded description in JSON-LD
    e.g. &lt;p&gt; instead of <p> — must unescape before stripping.
    """
    raw = ld.get("description", "")
    if not raw:
        return ""
    
    raw  = html.unescape(raw)
    soup = BeautifulSoup(raw, "html.parser")
    text = soup.get_text(separator="\n", strip=True)
    text = re.sub(r"\n{3,}", "\n\n", text).strip()
    return text[:5000]


def _fallback_from_meta(soup, job):
    """
    Fallback extraction from meta tags when JSON-LD is missing.
    Uses og:title and other available meta.
    """
    title = ""
    og    = soup.find("meta", {"property": "og:title"})
    if og:
        raw   = og.get("content", "")
        # Strip "job in City, State | Category at Company" suffix
        title = re.sub(r"\s*(job in|at)\s+.+$", "", raw,
                       flags=re.IGNORECASE).strip()

    job            = dict(job)
    job["title"]   = title or job.get("title", "")
    job["posted_at"] = None
    return job


# ─────────────────────────────────────────
# HELPERS — UTILITY
# ─────────────────────────────────────────

def _parse_date(date_str):
    """
    Parse Phenom date string to datetime or None.
    Primary format: "2026-03-29" (ISO date)
    """
    if not date_str:
        return None
    date_str = date_str.strip()
    for fmt in DATE_FORMATS:
        try:
            return datetime.strptime(date_str, fmt)
        except ValueError:
            continue
    # Handle partial ISO: "2025-10-2" (Intuit)
    try:
        parts = date_str.split("-")
        if len(parts) == 3:
            return datetime(int(parts[0]), int(parts[1]), int(parts[2]))
    except (ValueError, IndexError):
        pass
    return None