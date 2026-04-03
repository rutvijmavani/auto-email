# jobs/ats/sitemap.py — Universal XML/sitemap job scraper
#
# Last-resort scraper for any company where you have a public XML feed
# or sitemap URL but no dedicated ATS scraper.
#
# Handles ALL known XML formats automatically:
#
#   Format 1 — Custom XML job feed (Google, SuccessFactors-style)
#     <jobs><job><title>...</title><url>...</url></job></jobs>
#     <Job-Listing><Job><JobTitle>...</JobTitle><ReqId>...</ReqId></Job></Job-Listing>
#
#   Format 2 — Standard sitemap (Nintendo, JnJ, Elevance, MyFlorida, Databricks)
#     <urlset><url><loc>https://...</loc><lastmod>2026-01-01</lastmod></url></urlset>
#
#   Format 3 — Sitemap index → sub-sitemaps (Airbnb, many others)
#     <sitemapindex><sitemap><loc>https://.../sitemap-1.xml</loc></sitemap></sitemapindex>
#
# Strategy:
#   1. Detect format from root XML tag
#   2. For job feeds → extract all data inline (Option A, no detail fetch)
#   3. For sitemaps → collect URLs, filter job-like ones, fetch details (Option C)
#   4. For sitemap indexes → follow sub-sitemaps recursively, then treat as sitemap
#
# Job URL filtering philosophy:
#   TRUST the sitemap — if a URL is in a sitemap, assume it's a job UNLESS
#   it clearly looks like a non-job page (home, about, category listing, etc.)
#   The exclusion list is intentionally minimal — better to over-include than miss jobs.
#
# Detail page extraction:
#   1. JSON-LD JobPosting (most reliable — works for Nintendo, JnJ, Elevance)
#   2. og:title + meta tags fallback
#   3. lastmod from sitemap used as posted_at if detail page has no date
#
# slug_info format (stored in DB, provided via Google Form):
#   {"url": "https://careers.jnj.com/sitemap.xml"}
#   {"url": "https://google.com/about/careers/applications/jobs/feed.xml"}
#   {"url": "...", "locale": "en"}   optional: prefer this locale for multi-locale sites
#   {"url": "...", "job_pattern": "/en/jobs/\\d"}  optional: custom job URL regex

import re
import json
import html as html_lib
import warnings
from datetime import datetime
from urllib.parse import urlparse, urljoin
from bs4 import BeautifulSoup, XMLParsedAsHTMLWarning
from jobs.ats.base import fetch_html

warnings.filterwarnings("ignore", category=XMLParsedAsHTMLWarning)

# ─────────────────────────────────────────
# CONSTANTS
# ─────────────────────────────────────────

MAX_SUB_SITEMAPS = 20
MAX_JOBS         = 5000

# URL segments that definitively indicate a non-job page — static assets only
NON_JOB_EXCLUDE = re.compile(
    r"\.(xsl|css|js|png|jpg|gif|svg|ico|pdf|zip)(?:[?#]|$)",
    re.IGNORECASE
)

# Segments that strongly indicate a listing/category/content page (not individual job)
LISTING_PAGE_SIGNALS = re.compile(
    # Search/browse pages
    r"/(?:search|category|categories|browse|explore|filter|tag|sitemap)(?:[/-]|$)"
    # Culture/about pages
    r"|/(?:about|culture|life-at|benefits|blog|news|press|events|locations|faq)(?:[/-]|$)"
    # Program/listing pages (bare, no ID after)
    r"|/(?:student-opportunities|campus|internship-program|leadership-program)/?$"
    r"|/(?:studios?|offices?|teams?)/?$"
    # Privacy/legal pages
    r"|/(?:privacy|candidate-privacy|legal|terms|cookie|accessibility)(?:[/-]|$)"
    # Bare listing pages (ends with these words, no ID follows)
    r"|/jobs/?$"                          # bare /jobs/ listing
    r"|/careers/?$"                       # bare /careers/ listing
    r"|/open-positions/?$"                # bare open positions page
    r"|/our-history/?$"
    r"|/people-culture/?$"
    r"|/life-at-\w+/?$",                  # /life-at-jj/, /life-at-company/
    re.IGNORECASE
)

# Patterns that strongly indicate an individual job page
JOB_PAGE_SIGNALS = re.compile(
    r"/jobs?/\d"                          # /jobs/12345 or /job/12345
    r"|/jobs?/[A-Za-z0-9]{5,}[a-z]/"     # /jobs/2406217121w/ (JnJ)
    r"|/job/[0-9A-Fa-f]{16,}"            # /job/HEX (Elevance)
    r"|/details?/\d"                      # /details/123
    r"|/positions?/\d"                    # /positions/123
    r"|/openings?/\d"                     # /openings/123
    r"|/requisitions?/\d"                 # /requisitions/123
    r"|[/-]\d{6,}(?:[/-]|$)"             # 6+ digit ID anywhere
    r"|[/?&](?:gh_jid|jobId|job_id|req_id|pipelineId)=\d",  # query params
    re.IGNORECASE
)

# Google location: "Mountain ViewCAUSA" → needs splitting
# Pattern: lowercase→UPPERCASE 2-letter state + 3-letter country
_GOOGLE_LOC_RE = re.compile(
    r"([a-z])([A-Z]{2})(USA|GBR|IND|CAN|DEU|FRA|SGP|AUS|JPN|BRA|IRL|NLD|"
    r"CHE|SWE|POL|ISR|MEX|ESP|ITA|BEL|DNK|NOR|FIN|AUT|NZL|HKG|TWN|KOR|"
    r"CHN|ZAF|ARE|SAU|PHL|IDN|MYS|THA|VNM|CZE|HUN|ROU|UKR|PRT|GRC|ARG|CHL|COL)",
    re.UNICODE
)

DATE_FORMATS = [
    "%Y-%m-%dT%H:%M:%S",
    "%Y-%m-%dT%H:%M:%SZ",
    "%Y-%m-%dT%H:%M:%S.%fZ",
    "%Y-%m-%dT%H:%M:%S.%f",
    "%Y-%m-%d",
    "%m/%d/%Y",
]


# ─────────────────────────────────────────
# FETCH JOBS — main entry point
# ─────────────────────────────────────────

def fetch_jobs(slug_info, company):
    """
    Fetch all jobs from a public XML feed or sitemap URL.

    Automatically detects format and handles:
      - Custom XML job feeds (Google, SuccessFactors-style)
      - Standard sitemaps (Nintendo, JnJ, Elevance, MyFlorida)
      - Sitemap indexes (Airbnb, Databricks)

    Args:
        slug_info: {"url": "https://..."} or JSON string or plain URL string
        company:   company name

    Returns:
        List of job dicts. For XML feeds: all fields populated (no detail fetch needed).
        For sitemaps: stubs with posted_at from lastmod, detail filled by fetch_job_detail().
    """
    if not slug_info:
        return []

    if isinstance(slug_info, str):
        try:
            slug_info = json.loads(slug_info)
        except (json.JSONDecodeError, TypeError):
            if slug_info.startswith("http"):
                slug_info = {"url": slug_info}
            else:
                return []

    url         = slug_info.get("url", "")
    job_pattern = slug_info.get("job_pattern", "")
    locale_pref = slug_info.get("locale", "en")

    if not url:
        return []

    resp = fetch_html(url, platform="sitemap")
    if resp is None:
        return []

    text = resp.text.strip()
    if not text:
        return []

    # Detect XML format from root tag
    soup = BeautifulSoup(text, "html.parser")

    # ── Format 1: Custom XML job feed ──────────────────────────────────────
    # Google: <jobs>, SuccessFactors: <Job-Listing>
    root_tag = soup.find(True)
    root_name = root_tag.name.lower() if root_tag else ""

    if root_name in ("jobs", "job-listing", "feed", "rss"):
        return _parse_xml_feed(soup, slug_info, company, url)

    # ── Format 2: Sitemap index ─────────────────────────────────────────────
    if soup.find("sitemapindex") or soup.find("sitemap"):
        # Check if it's truly an index (has <sitemap> children with <loc>)
        sitemap_tags = soup.find_all("sitemap")
        if sitemap_tags and any(s.find("loc") for s in sitemap_tags):
            all_entries = []
            for sitemap_tag in sitemap_tags[:MAX_SUB_SITEMAPS]:
                loc = sitemap_tag.find("loc")
                if not loc:
                    continue
                sub_url  = loc.get_text(strip=True)
                sub_resp = fetch_html(sub_url, platform="sitemap")
                if sub_resp is None:
                    continue
                sub_soup = BeautifulSoup(sub_resp.text, "html.parser")
                all_entries.extend(_extract_sitemap_entries(sub_soup))
            return _entries_to_stubs(all_entries, slug_info, company,
                                     job_pattern, locale_pref)

    # ── Format 3: Standard sitemap ─────────────────────────────────────────
    entries = _extract_sitemap_entries(soup)
    return _entries_to_stubs(entries, slug_info, company, job_pattern, locale_pref)


# ─────────────────────────────────────────
# FETCH JOB DETAIL
# ─────────────────────────────────────────

def fetch_job_detail(job):
    """
    Fetch job detail page for a single job stub.

    Skipped for XML feed jobs (_feed_type=xml) — they already have all data.

    Tries in order:
      1. JSON-LD JobPosting schema (most reliable)
      2. og:title + meta tags
      3. Largest text block as description fallback

    posted_at from sitemap lastmod is preserved if detail page has no date.
    """
    # XML feed jobs already have all data inline — no detail fetch needed
    if job.get("_feed_type") == "xml":
        return job

    job_url = job.get("job_url", "")
    if not job_url:
        return job

    resp = fetch_html(job_url, platform="sitemap")
    if resp is None:
        return job

    try:
        soup = BeautifulSoup(resp.text, "html.parser")

        # Strategy 1 — JSON-LD
        ld = _extract_json_ld(soup)
        if ld:
            return _from_json_ld(ld, job)

        # Strategy 2 — meta + HTML fallback
        return _from_html(soup, job)

    except Exception:
        return job


# ─────────────────────────────────────────
# XML FEED PARSING (Format 1)
# ─────────────────────────────────────────

def _parse_xml_feed(soup, slug_info, company, base_url):
    """
    Parse a custom XML job feed (Google-style or SuccessFactors-style).
    Returns fully populated job dicts — no detail fetch needed.

    Handles field name variations across different feed formats:
      Google:          <jobid>, <title>, <url>, <published>, <locations>, <description>
      SuccessFactors:  <ReqId>, <JobTitle>, no URL, <Posted-Date>, <Location>/<mfield*>
    """
    # Find job container tags — try common names
    jobs = (soup.find_all("job") or
            soup.find_all("Job") or
            soup.find_all("item") or
            soup.find_all("entry"))

    if not jobs:
        return []

    result  = []
    seen_ids = set()

    for job_tag in jobs:
        # ── Title ──
        title = (_tag_text(job_tag, "title") or
                 _tag_text(job_tag, "JobTitle") or
                 _tag_text(job_tag, "name") or "")
        if not title:
            continue

        # ── Job ID ──
        job_id = (_tag_text(job_tag, "jobid") or
                  _tag_text(job_tag, "ReqId") or
                  _tag_text(job_tag, "id") or
                  _tag_text(job_tag, "identifier") or "")

        if not job_id:
            continue
        if job_id in seen_ids:
            continue
        seen_ids.add(job_id)

        # ── Job URL ──
        job_url = (_tag_text(job_tag, "url") or
                   _tag_text(job_tag, "link") or
                   _tag_text(job_tag, "guid") or
                   _build_sf_url(job_tag, base_url, job_id))

        # ── Date ──
        date_str = (_tag_text(job_tag, "published") or
                    _tag_text(job_tag, "Posted-Date") or
                    _tag_text(job_tag, "pubDate") or
                    _tag_text(job_tag, "updated") or "")
        posted_at = _parse_date(date_str)

        # ── Location ──
        location = _extract_feed_location(job_tag)

        # ── Description ──
        description = _extract_feed_description(job_tag)

        result.append({
            "company":     company,
            "title":       title,
            "job_url":     job_url,
            "job_id":      job_id,
            "location":    location,
            "posted_at":   posted_at,
            "description": description,
            "ats":         "sitemap",
            "_feed_type":  "xml",  # marks inline data — skip fetch_job_detail
        })

    return result


def _extract_feed_location(job_tag):
    """Extract location from XML feed job tag — handles multiple field names."""
    # Direct location tags
    for tag_name in ["location", "Location", "locations", "city"]:
        val = _tag_text(job_tag, tag_name)
        if val:
            return _clean_location(val)

    # SuccessFactors mfield style
    mfield1 = _tag_text(job_tag, "mfield1") or ""
    mfield3 = _tag_text(job_tag, "mfield3") or ""
    country = _strip_sf_prefix(mfield1)
    state   = _strip_sf_prefix(mfield3)
    if country or state:
        return ", ".join(p for p in [state, country] if p)

    # SuccessFactors filter style (SAP)
    filter7 = _strip_sf_prefix(_tag_text(job_tag, "filter7") or "")
    filter8 = _strip_sf_prefix(_tag_text(job_tag, "filter8") or "")
    if filter7 or filter8:
        return ", ".join(p for p in [filter8, filter7] if p)

    # Google-style: city + state + country separate tags
    city    = _tag_text(job_tag, "city") or ""
    state   = _tag_text(job_tag, "state") or ""
    country = _tag_text(job_tag, "country") or ""
    if city or state or country:
        return ", ".join(p for p in [city, state, country] if p)

    return ""


def _clean_location(raw):
    """
    Clean and parse location string.
    Handles Google's concatenated format: "Mountain ViewCAUSA" → "Mountain View, CA, USA"
    Also strips internal office codes like "(SANJOSE)".
    """
    if not raw:
        return ""
    # Strip office codes in parens: "(SANJOSE)", "(NYC)", "(EMEA)"
    raw = re.sub(r"\s*\([A-Z][A-Z0-9]{1,15}\)\s*$", "", raw).strip()
    # Fix Google's concatenated location: "Mountain ViewCAUSA"
    raw = _GOOGLE_LOC_RE.sub(r"\1, \2, \3", raw)
    # Also handle city + 3-letter country directly: "LondonGBR"
    raw = re.sub(r"([a-z])([A-Z]{3})$", r"\1, \2", raw)
    return raw.strip()


def _extract_feed_description(job_tag):
    """Extract description from XML feed job tag."""
    for tag_name in ["description", "Job-Description", "content", "summary"]:
        tag = job_tag.find(tag_name)
        if not tag:
            continue
        raw = tag.get_text(strip=True)
        if not raw:
            continue
        raw  = html_lib.unescape(raw)
        soup = BeautifulSoup(raw, "html.parser")
        text = soup.get_text(separator="\n", strip=True)
        text = re.sub(r"\n{3,}", "\n\n", text).strip()
        if text:
            return text[:5000]
    return ""


def _build_sf_url(job_tag, base_url, req_id):
    """Build job URL for SuccessFactors-style feeds that don't include URL."""
    if not req_id:
        return ""
    # Parse base_url to extract SF params
    from urllib.parse import urlparse, parse_qs
    parsed = urlparse(base_url)
    params = parse_qs(parsed.query)
    company = params.get("company", [""])[0]
    if company and "successfactors" in parsed.netloc:
        return f"{parsed.scheme}://{parsed.netloc}/career?company={company}&career_job_req_id={req_id}"
    return ""


def _strip_sf_prefix(value):
    """
    Strip label prefix from SuccessFactors mfield/filter values.
    "Country/AreaIndia" → "India"
    "state/provinceUttar Pradesh" → "Uttar Pradesh"
    "CountryBulgaria" → "Bulgaria"
    """
    if not value:
        return ""
    prefixes = [
        "Country/Area", "country/area", "state/province", "State/Province",
        "Country", "Internal Posting Location", "Region", "Work Area",
        "Career Status", "Employment Type", "Expected Travel",
        "Additional Locations", "Job Category", "City", "city",
    ]
    for prefix in sorted(prefixes, key=len, reverse=True):
        if value.startswith(prefix):
            stripped = value[len(prefix):].strip()
            return stripped if stripped else ""
    return value


# ─────────────────────────────────────────
# SITEMAP PARSING (Formats 2 + 3)
# ─────────────────────────────────────────

def _extract_sitemap_entries(soup):
    """
    Extract all URL entries from a standard sitemap.
    Returns list of {"url": str, "lastmod": str|None}.
    """
    entries = []
    for url_tag in soup.find_all("url"):
        loc = url_tag.find("loc")
        if not loc:
            continue
        url     = loc.get_text(strip=True)
        lastmod = url_tag.find("lastmod")
        entries.append({
            "url":     url,
            "lastmod": lastmod.get_text(strip=True) if lastmod else None,
        })
    return entries


def _is_job_url(url, custom_re=None):
    """
    Determine if a URL is likely an individual job page.

    Philosophy: TRUST the sitemap. Most URLs in job sitemaps are jobs.
    Only exclude things that are obviously NOT jobs.
    """
    if custom_re:
        return bool(custom_re.search(url))

    # Hard excludes — static assets, clearly non-job pages
    if NON_JOB_EXCLUDE.search(url):
        return False

    # Strong listing/category page signals — skip
    if LISTING_PAGE_SIGNALS.search(url):
        return False

    # Strong job page signals — include
    if JOB_PAGE_SIGNALS.search(url):
        return True

    # For anything else: include if URL has enough path depth
    # (avoid bare domain or single-segment URLs like /careers/)
    parsed = urlparse(url)
    parts  = [p for p in parsed.path.split("/") if p]
    if len(parts) >= 2:
        # Has at least 2 path segments — likely a content page
        # Check if last segment looks like a slug (not just a category word)
        last = parts[-1]
        # Has digits in it → very likely a job
        if re.search(r"\d{4,}", last):
            return True
        # Long hyphenated slug → likely a job title
        if len(last) > 15 and "-" in last:
            return True

    return False


def _extract_job_id(url):
    """
    Extract canonical job ID from URL.
    Tries multiple patterns in order of reliability.
    Returns a string ID or None.
    """
    patterns = [
        # JnJ: /jobs/2406217121w/ — digits + optional letter
        re.compile(r"/jobs?/(\d{5,}[a-zA-Z])/", re.IGNORECASE),
        # Nintendo/MyFlorida segment: /jobs/4094803009/
        re.compile(r"/jobs?/(\d{6,})/?"),
        # Elevance hex UUID: /job/0022E1AA68A75386EE40BE8BF5DC2E7D
        re.compile(r"/job/([0-9A-Fa-f]{20,})"),
        # Databricks/Greenhouse trailing ID: title-8441871002
        re.compile(r"-(\d{9,})$"),
        # Generic trailing numeric: /1370705200/ or /1370705200
        re.compile(r"/(\d{7,})/?$"),
        # Generic 6+ digit anywhere
        re.compile(r"/(\d{6,})"),
        # Query params
        re.compile(r"[?&](?:gh_jid|jobId|job_id|req_id|pipelineId|id)=(\w+)", re.IGNORECASE),
        # Alphanumeric ID (hex/UUID style)
        re.compile(r"/([0-9A-F]{16,})/?$"),
        # Last path segment as fallback (for unique slugs)
        re.compile(r"/([^/?#]{8,})/?$"),
    ]

    for pattern in patterns:
        m = pattern.search(url)
        if m:
            candidate = m.group(1)
            # Skip generic words that aren't IDs
            if candidate.lower() in {
                "jobs", "careers", "job", "career", "position", "opening",
                "apply", "detail", "search", "results", "en", "us", "en-us"
            }:
                continue
            return candidate

    return None


def _title_from_url(url):
    """
    Extract a human-readable title hint from URL slug.
    Used as placeholder until detail page is fetched.
    Returns empty string if no good title can be extracted.
    """
    parsed = urlparse(url)
    parts  = [p for p in parsed.path.rstrip("/").split("/") if p]

    if not parts:
        return ""

    # Skip generic segments
    skip = {"en", "us", "gb", "ca", "au", "jobs", "job", "careers", "career",
            "company", "position", "opening", "detail", "apply", "en-us",
            "en-gb", "en-ca", "field-engineering---other"}

    # Find best title segment — look for the longest non-ID, non-skip slug
    candidates = []
    for part in parts:
        if part.lower() in skip:
            continue
        # Strip trailing job ID
        clean = re.sub(r"-?\d{6,}[a-z]?$", "", part).strip("-")
        if not clean or clean.lower() in skip:
            continue
        if re.fullmatch(r"[0-9A-Fa-f]{16,}", clean):
            continue  # pure hex ID
        if clean.isdigit():
            continue
        candidates.append(clean)

    if not candidates:
        return ""

    # Prefer longer segments (more descriptive titles)
    best = max(candidates, key=len)
    return best.replace("-", " ").replace("_", " ").title()


def _pick_best_locale(candidates, locale_pref="en"):
    """
    From multiple URL entries for the same job ID (multi-locale sites like JnJ),
    pick the preferred locale URL.
    """
    # Build locale preference order
    preferred = [
        f"/{locale_pref}/",
        "/en/",
        "/en-us/",
        "/en-gb/",
        "/en-ca/",
    ]
    for pref in preferred:
        for entry in candidates:
            if pref in entry["url"].lower():
                return entry
    return candidates[0]


def _entries_to_stubs(entries, slug_info, company, job_pattern="", locale_pref="en"):
    """
    Convert sitemap entries to job stub dicts.
    Filters to job URLs, deduplicates by job ID, picks best locale.
    """
    custom_re = re.compile(job_pattern, re.IGNORECASE) if job_pattern else None

    # Group entries by job ID (handles multi-locale duplicates)
    id_to_entries = {}
    skipped       = 0

    for entry in entries:
        url = entry["url"]

        if not _is_job_url(url, custom_re):
            skipped += 1
            continue

        job_id = _extract_job_id(url)
        if not job_id:
            skipped += 1
            continue

        if job_id not in id_to_entries:
            id_to_entries[job_id] = []
        id_to_entries[job_id].append(entry)

    # Build stubs — one per unique job ID
    jobs = []
    for job_id, candidates in list(id_to_entries.items())[:MAX_JOBS]:
        entry     = _pick_best_locale(candidates, locale_pref)
        url       = entry["url"]
        lastmod   = entry.get("lastmod")
        posted_at = _parse_date(lastmod)
        title     = _title_from_url(url)

        jobs.append({
            "company":     company,
            "title":       title,
            "job_url":     url,
            "job_id":      job_id,
            "location":    "",
            "posted_at":   posted_at,
            "description": "",
            "ats":         "sitemap",
            "_slug_info":  slug_info,
        })

    return jobs


# ─────────────────────────────────────────
# DETAIL PAGE EXTRACTION
# ─────────────────────────────────────────

def _extract_json_ld(soup):
    """Extract JobPosting JSON-LD. Returns dict or None."""
    for script in soup.find_all("script", type="application/ld+json"):
        try:
            data = json.loads(script.string or "")
            if isinstance(data, list):
                data = next(
                    (d for d in data if d.get("@type") == "JobPosting"), {}
                )
            if isinstance(data, dict) and data.get("@type") == "JobPosting":
                return data
        except (json.JSONDecodeError, TypeError):
            continue
    return None


def _from_json_ld(ld, job):
    """Extract all fields from JSON-LD JobPosting."""
    title     = ld.get("title", "") or job.get("title", "")
    posted_at = _parse_date(ld.get("datePosted", "")) or job.get("posted_at")

    # Location
    location = ""
    try:
        locations = ld.get("jobLocation", [])
        if isinstance(locations, dict):
            locations = [locations]
        if locations:
            addr  = locations[0].get("address", {})
            parts = [
                addr.get("addressLocality", ""),
                addr.get("addressRegion", ""),
                addr.get("addressCountry", ""),
            ]
            location = ", ".join(p for p in parts if p)
    except Exception:
        pass

    # Description
    raw  = ld.get("description", "")
    raw  = html_lib.unescape(raw)
    soup = BeautifulSoup(raw, "html.parser")
    desc = soup.get_text(separator="\n", strip=True)
    desc = re.sub(r"\n{3,}", "\n\n", desc).strip()[:5000]

    # Job ID from identifier
    identifier = ld.get("identifier", {})
    if isinstance(identifier, dict):
        job_id = str(identifier.get("value", "")) or job.get("job_id", "")
    else:
        job_id = str(identifier) if identifier else job.get("job_id", "")

    job              = dict(job)
    job["title"]       = title
    job["location"]    = location
    job["posted_at"]   = posted_at
    job["description"] = desc
    job["job_id"]      = job_id or job.get("job_id", "")
    return job


def _from_html(soup, job):
    """HTML fallback extraction when JSON-LD is missing."""
    # Title
    title = ""
    for sel in [
        ("meta", {"property": "og:title"}),
        ("meta", {"name": "twitter:title"}),
    ]:
        tag = soup.find(*sel)
        if tag:
            raw = tag.get("content", "").strip()
            # Strip " - Company Name" suffix
            title = re.sub(r"\s*[-|]\s*.{3,50}$", "", raw).strip()
            if title:
                break
    if not title:
        h1 = soup.find("h1")
        if h1:
            title = h1.get_text(strip=True)

    # Date — preserve lastmod fallback from stub
    posted_at = job.get("posted_at")
    for meta in soup.find_all("meta"):
        name    = meta.get("name", "") or meta.get("property", "")
        content = meta.get("content", "")
        if content and any(k in name.lower() for k in ["dateposted", "date", "posted", "publish"]):
            d = _parse_date(content)
            if d:
                posted_at = d
                break

    # Location
    location = ""
    for meta in soup.find_all("meta"):
        name    = meta.get("name", "") or meta.get("property", "")
        content = meta.get("content", "")
        if "location" in name.lower() and content and len(content) < 100:
            location = content.strip()
            break

    # Description — largest content block
    desc     = ""
    best_len = 0
    for sel in [
        "[class*='description']", "[class*='job-detail']",
        "[class*='posting']",     "[class*='content']",
        "article",                "main",
        "[role='main']",
    ]:
        for el in soup.select(sel):
            text = el.get_text(separator="\n", strip=True)
            text = re.sub(r"\n{3,}", "\n\n", text).strip()
            if len(text) > best_len and len(text) > 200:
                best_len = len(text)
                desc     = text[:5000]

    job              = dict(job)
    job["title"]       = title or job.get("title", "")
    job["location"]    = location
    job["posted_at"]   = posted_at
    job["description"] = desc
    return job


# ─────────────────────────────────────────
# UTILITIES
# ─────────────────────────────────────────

def _tag_text(tag, name):
    """Extract text from a named child tag. Case-insensitive."""
    found = tag.find(name)
    if not found:
        # Try case-insensitive search
        found = tag.find(lambda t: t.name and t.name.lower() == name.lower())
    return found.get_text(strip=True) if found else ""


def _parse_date(date_str):
    """Parse date string to datetime. Handles ISO, partial ISO, and MM/DD/YYYY."""
    if not date_str:
        return None
    date_str = date_str.strip()
    # Normalize: strip timezone, fractional seconds
    date_str = re.sub(r"[+-]\d{2}:\d{2}$", "", date_str)
    date_str = date_str.replace("Z", "").strip()
    date_str = re.sub(r"\.\d+$", "", date_str)

    for fmt in DATE_FORMATS:
        try:
            return datetime.strptime(date_str, fmt)
        except ValueError:
            continue

    # Partial ISO: "2025-10-2" (no zero padding)
    try:
        parts = date_str.split("T")[0].split("-")
        if len(parts) == 3:
            return datetime(int(parts[0]), int(parts[1]), int(parts[2]))
    except (ValueError, IndexError):
        pass

    return None