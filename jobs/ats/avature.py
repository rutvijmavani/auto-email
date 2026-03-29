# jobs/ats/avature.py — Avature job board scraper
#
# Avature does not expose a public JSON API.
# Jobs are discovered via sitemap.xml and fetched via HTML scraping.
#
# URL patterns:
#   Hosted:       {slug}.avature.net/careers/JobDetail/{title-slug}/{job_id}
#   Custom domain: jobs.ea.com/en_US/careers/JobDetail/{title-slug}/{job_id}
#                  jobs.siemens.com/en_US/externaljobs/JobDetail/{title-slug}/{job_id}
#
# Sitemap:      {base_url}/careers/sitemap.xml
#               {base_url}/{path}/sitemap.xml  (custom domain variant)
#
# Option C freshness strategy:
#   1. Fetch sitemap.xml → extract all JobDetail URLs + job IDs
#   2. Compare with DB → find new job IDs only
#   3. Fetch detail page ONLY for new jobs
#   4. Extract title, location, posted_at, description from HTML
#   5. Store in job_postings
#
# Date field: "Date Posted" field in job detail HTML (DD-Mon-YYYY format)
#             e.g. "03-Sep-2024" → reliable original posting date
#
# Slug format stored in DB:
#   Hosted:       {"base": "https://synopsys.avature.net", "path": "careers"}
#   Custom domain:{"base": "https://jobs.ea.com", "path": "en_US/careers"}
#                 {"base": "https://jobs.siemens.com", "path": "en_US/externaljobs"}

import re
import json
from datetime import datetime
from bs4 import BeautifulSoup
from jobs.ats.base import fetch_html, fetch_json, slugify, validate_company_match


# ─────────────────────────────────────────
# CONSTANTS
# ─────────────────────────────────────────

JOB_DETAIL_RE = re.compile(r"/JobDetail/[^/]+/(\d+)/?$", re.IGNORECASE)
DATE_FORMATS   = ["%d-%b-%Y", "%Y-%m-%d", "%B %d, %Y", "%b %d, %Y"]

# Known Avature custom domains — maps domain → careers path
CUSTOM_DOMAINS = {
    "jobs.ea.com":       "en_US/careers",
    "jobs.siemens.com":  "en_US/externaljobs",
    "jobs.intuit.com":   "en_US/externalCareers",
}

# Domain → expected company mapping for validation
COMPANY_BY_DOMAIN = {
    "jobs.ea.com":       "ea",
    "jobs.siemens.com":  "siemens",
    "jobs.intuit.com":   "intuit",
}


# ─────────────────────────────────────────
# DETECTION
# ─────────────────────────────────────────

def detect(company):
    """
    Try to detect if company uses Avature.

    Strategy:
      1. Try {slug}.avature.net/careers/sitemap.xml
      2. Check known custom domains

    Returns (slug_info dict, sample_jobs) or (None, None).
    slug_info = {"base": "https://...", "path": "careers"}
    """
    # Try hosted Avature subdomain
    for slug in slugify(company):
        base     = f"https://{slug}.avature.net"
        path     = "careers"
        sitemap  = f"{base}/{path}/sitemap.xml"
        resp     = fetch_html(sitemap, platform="avature", track=False)

        if resp is None:
            continue

        soup    = BeautifulSoup(resp.text, "xml")
        job_urls = _extract_job_urls(soup)

        if not job_urls:
            continue

        # Validate company match using first job title from URL slug
        first_url  = job_urls[0]
        title_slug = _title_slug_from_url(first_url)
        if not validate_company_match(title_slug, company):
            if not validate_company_match(slug, company):
                continue

        slug_info = {"base": base, "path": path}
        sample    = _urls_to_stubs(job_urls[:3], slug_info, company)
        return slug_info, sample

    # Try known custom domains
    for domain, path in CUSTOM_DOMAINS.items():
        # Validate at domain level using company mapping
        expected_company = COMPANY_BY_DOMAIN.get(domain, "")
        if expected_company and not validate_company_match(expected_company, company):
            continue

        base    = f"https://{domain}"
        sitemap = f"{base}/{path}/sitemap.xml"
        resp    = fetch_html(sitemap, platform="avature", track=False)

        if resp is None:
            continue

        soup     = BeautifulSoup(resp.text, "xml")
        job_urls = _extract_job_urls(soup)

        if not job_urls:
            continue

        slug_info = {"base": base, "path": path}
        sample    = _urls_to_stubs(job_urls[:3], slug_info, company)
        return slug_info, sample

    return None, None


# ─────────────────────────────────────────
# FETCH JOBS (listing via sitemap)
# ─────────────────────────────────────────

def fetch_jobs(slug_info, company):
    """
    Fetch all jobs for company from Avature via sitemap.xml.

    Strategy:
      Parse sitemap.xml → extract all /JobDetail/{title}/{id} URLs
      Return job stubs (title/location/description filled by fetch_job_detail)

    Args:
        slug_info: dict with "base" and "path" keys
                   e.g. {"base": "https://synopsys.avature.net", "path": "careers"}
        company:   company name

    Returns:
        List of normalized job dicts.
        location, posted_at, description filled by fetch_job_detail().
    """
    if not slug_info:
        return []

    # Handle both dict (new) and JSON string (stored in DB)
    if isinstance(slug_info, str):
        try:
            slug_info = json.loads(slug_info)
        except (json.JSONDecodeError, TypeError):
            return []

    base    = slug_info.get("base", "")
    path    = slug_info.get("path", "careers")
    if not base:
        return []

    sitemap_url = f"{base}/{path}/sitemap.xml"
    resp        = fetch_html(sitemap_url, platform="avature")

    if resp is None:
        return []

    soup     = BeautifulSoup(resp.text, "xml")
    job_urls = _extract_job_urls(soup)

    if not job_urls:
        return []

    return _urls_to_stubs(job_urls, slug_info, company)


# ─────────────────────────────────────────
# FETCH JOB DETAIL
# ─────────────────────────────────────────

def fetch_job_detail(job):
    """
    Fetch job detail page for a single job.
    Extracts title, location, posted_at, description from HTML.

    Avature detail pages are server-side rendered — no Playwright needed.
    This implementation is fully generalised — works across any Avature
    tenant regardless of which fields they expose.

    Generalised extraction strategy:
      Title:       (1) any field labelled "Job Title" / "Title" / "Position"
                   (2) og:title meta tag fallback
      Location:    (1) "City" + "Country" fields (Synopsys style)
                   (2) "Location" / "Locations" field value
                   (3) "Locations :" pattern in General Information section
      Date:        any field label containing "date" or "posted" or "since"
                   → None if not found (freshness via URL dedup)
      Description: largest article section excluding General Information,
                   Share, and Apply sections (works for all tenants)
      Job ID:      avature.portallist.search meta tag (most reliable)

    Called only for NEW jobs (Option C strategy).
    """
    job_url = job.get("job_url", "")
    if not job_url:
        return job

    resp = fetch_html(job_url, platform="avature")
    if resp is None:
        return job

    try:
        soup   = BeautifulSoup(resp.text, "html.parser")
        fields = _extract_fields(soup)

        title       = _generalised_title(soup, fields, job)
        location    = _generalised_location(soup, fields)
        posted_at   = _generalised_date(fields)
        description = _generalised_description(soup)

        # Job ID from meta tag — more reliable than URL parsing
        job_id = _extract_meta(soup, "avature.portallist.search") or job.get("job_id", "")

        job              = dict(job)
        job["title"]       = title
        job["location"]    = location
        job["posted_at"]   = posted_at
        job["description"] = description or ""
        job["job_id"]      = job_id or job.get("job_id", "")

        return job

    except Exception:
        return job


# ─────────────────────────────────────────
# HELPERS — SITEMAP PARSING
# ─────────────────────────────────────────

def _extract_job_urls(soup):
    """
    Extract all JobDetail URLs from Avature sitemap.xml.
    Filters out non-job URLs (AgentCreate, Login, etc.)
    Returns list of full job URLs.
    """
    job_urls = []
    for loc in soup.find_all("loc"):
        url = loc.text.strip()
        if JOB_DETAIL_RE.search(url):
            # Skip the bare /JobDetail page (no ID)
            if re.search(r"/JobDetail/[^/]+/\d+/?$", url):
                job_urls.append(url)
    return job_urls


def _extract_job_id(url):
    """Extract numeric job ID from Avature JobDetail URL."""
    m = JOB_DETAIL_RE.search(url)
    return m.group(1) if m else None


def _title_slug_from_url(url):
    """
    Extract human-readable title slug from URL for company validation.
    /careers/JobDetail/ASIC-Digital-Design-Principal-Engineer-5055/5055
    → "ASIC Digital Design Principal Engineer"
    """
    m = re.search(r"/JobDetail/([^/]+)/\d+/?$", url, re.IGNORECASE)
    if not m:
        return ""
    return m.group(1).replace("-", " ")


def _urls_to_stubs(job_urls, slug_info, company):
    """Convert list of sitemap URLs to job stub dicts."""
    jobs     = []
    seen_ids = set()

    for url in job_urls:
        job_id = _extract_job_id(url)
        if not job_id or job_id in seen_ids:
            continue
        seen_ids.add(job_id)

        # Extract title from URL slug as placeholder
        # (overwritten by fetch_job_detail with real title)
        title_slug = _title_slug_from_url(url)
        title      = _clean_title(title_slug)

        jobs.append({
            "company":     company,
            "title":       title,
            "job_url":     url,
            "job_id":      job_id,
            "location":    "",     # filled by fetch_job_detail
            "posted_at":   None,   # filled by fetch_job_detail
            "description": "",     # filled by fetch_job_detail
            "ats":         "avature",
            "_slug_info":  slug_info,  # used for detail fetch context
        })

    return jobs


# ─────────────────────────────────────────
# HELPERS — GENERALISED EXTRACTION
# ─────────────────────────────────────────

# Field label keyword maps — order matters, first match wins
_TITLE_LABELS    = ["job title", "title", "position", "position title", "role"]
_LOCATION_LABELS = ["city", "location", "locations", "job location", "work location", "site"]
_COUNTRY_LABELS  = ["country", "country/region"]
_DATE_LABELS     = ["date posted", "posted", "posted since", "posting date", "post date", "date"]

# Article section headers to skip when finding description
_SKIP_SECTIONS   = {"general information", "share this role", "share", "apply", ""}


def _extract_fields(soup):
    """
    Extract all label→value pairs from Avature job detail page.
    Universal across all Avature tenants.

    Avature structure:
      <div class="article__content__view__field">
        <div class="...label">Job Title</div>
        <div class="...value">Senior Engineer</div>
      </div>

    Returns dict of {lowercase_label: raw_value_text}
    Also preserves original-case key for exact lookups.
    """
    fields = {}
    for field in soup.select(".article__content__view__field"):
        label_el = field.select_one(".article__content__view__field__label")
        value_el = field.select_one(".article__content__view__field__value")
        if not label_el or not value_el:
            continue
        label = re.sub(r"\s+", " ", label_el.get_text(strip=True)).strip()
        value = re.sub(r"\s+", " ", value_el.get_text(
            separator=" ", strip=True)).strip()
        if label and value:
            fields[label]            = value   # original case
            fields[label.lower()]    = value   # lowercase for fuzzy match
    return fields


def _match_field(fields, candidates):
    """
    Find first matching field value from a list of label candidates.
    Tries exact lowercase match first, then partial match.
    """
    for candidate in candidates:
        if candidate in fields:
            return fields[candidate]
    # Partial match fallback
    for key, value in fields.items():
        for candidate in candidates:
            if candidate in key.lower():
                return value
    return ""


def _extract_meta(soup, name):
    """Extract content from a named meta tag."""
    tag = soup.find("meta", {"name": name})
    if tag:
        return tag.get("content", "").strip()
    # Try property= variant
    tag = soup.find("meta", {"property": name})
    return tag.get("content", "").strip() if tag else ""


def _generalised_title(soup, fields, job):
    """
    Extract job title using multiple fallback strategies.
    1. Known title field labels
    2. og:title meta tag (strip trailing company name)
    3. URL slug from job dict
    """
    # Strategy 1 — field label match
    title = _match_field(fields, _TITLE_LABELS)
    if title:
        return title

    # Strategy 2 — og:title meta (EA uses this)
    og_title = _extract_meta(soup, "og:title") or _extract_meta(soup, "twitter:title")
    if og_title:
        # Strip trailing " - Company Name" suffix
        og_title = re.sub(r"\s*[-|]\s*[^-|]+$", "", og_title).strip()
        if og_title:
            return og_title

    # Strategy 3 — fall back to stub title from URL slug
    return job.get("title", "")


def _generalised_location(soup, fields):
    """
    Extract location using multiple fallback strategies.

    Strategy 1: City + Country separate fields (Synopsys)
    Strategy 2: Single location field value (generic)
    Strategy 3: "Locations :" pattern in General Information section text (EA)
    """
    # Strategy 1 — City + Country fields
    city    = _match_field(fields, ["city"])
    country = _match_field(fields, _COUNTRY_LABELS)
    if city or country:
        return ", ".join(p for p in [city, country] if p)

    # Strategy 2 — single location field
    location = _match_field(fields, _LOCATION_LABELS)
    if location and location.lower() not in {"multiple locations", "various"}:
        return location

    # Strategy 3 — parse from General Information section text (EA)
    # EA renders: "General Information Locations : Bucharest, Romania Role ID..."
    for article in soup.select("article"):
        header = article.select_one("h3")
        if not header:
            continue
        if "general information" not in header.get_text(strip=True).lower():
            continue
        text = re.sub(r"\s+", " ", article.get_text(separator=" ", strip=True))
        # Match "Locations : City, State/Country" or "Location: City, Country"
        m = re.search(
            r"Locations?\s*[:\-]\s*([A-Za-z][A-Za-z\s,\.]{2,60})(?=\s+[A-Z][a-z]|\s*$)",
            text
        )
        if m:
            loc = m.group(1).strip().rstrip(",.")
            if loc and len(loc) > 2:
                return loc

    return ""


def _generalised_date(fields):
    """
    Extract posted date from any date-related field.
    Returns datetime or None if not found.
    """
    date_str = _match_field(fields, _DATE_LABELS)
    return _parse_date(date_str)


def _generalised_description(soup):
    """
    Extract job description from the largest content article section.

    Strategy: find all article sections, skip non-content ones
    (General Information, Share, Apply), return text of largest remaining.

    Works for both:
      Synopsys: "Descriptions & Requirements" section with field value
      EA: "Description & Requirements" section with direct text content
    """
    best_text = ""
    best_len  = 0

    for article in soup.select("article"):
        header     = article.select_one("h3")
        header_txt = header.get_text(strip=True).lower() if header else ""

        # Skip non-content sections
        if any(skip in header_txt for skip in _SKIP_SECTIONS):
            if header_txt != "":
                continue

        text = article.get_text(separator="\n", strip=True)
        text = re.sub(r"\n{3,}", "\n\n", text).strip()

        # Strip section header from text
        if header_txt:
            text = re.sub(
                rf"^{re.escape(header.get_text(strip=True))}\s*",
                "", text, flags=re.IGNORECASE
            ).strip()

        if len(text) > best_len:
            best_len  = len(text)
            best_text = text

    return best_text[:5000]


# ─────────────────────────────────────────
# HELPERS — UTILITY
# ─────────────────────────────────────────

def _clean_title(title_slug):
    """
    Clean URL slug into readable title.
    "ASIC-Digital-Design-Principal-Engineer-5055" → "ASIC Digital Design Principal Engineer"
    """
    title = re.sub(r"\s+\d+$", "", title_slug).strip()
    return title


def _parse_date(date_str):
    """
    Parse Avature date string to datetime or None.
    Handles all known formats across tenants:
      "03-Sep-2024"       → datetime(2024, 9, 3)   Synopsys
      "27-Mar-2026"       → datetime(2026, 3, 27)   Siemens
      "2024-09-03"        → datetime(2024, 9, 3)
      "September 3, 2024" → datetime(2024, 9, 3)
    """
    if not date_str:
        return None
    date_str = date_str.strip()
    for fmt in DATE_FORMATS:
        try:
            return datetime.strptime(date_str, fmt)
        except ValueError:
            continue
    return None