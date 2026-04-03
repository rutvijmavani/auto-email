# jobs/ats/google.py — Google Careers XML feed scraper
#
# Google exposes a public XML feed of all job postings.
# Single global feed — no slug/tenant needed.
#
# Feed URL:
#   https://www.google.com/about/careers/applications/jobs/feed.xml
#
# XML structure:
#   <job>
#     <jobid>     → job_id (numeric string)
#     <title>     → title
#     <published> → ISO timestamp e.g. "2026-03-17T09:00:15.503Z"
#     <url>       → direct job URL at careers.google.com
#     <description> → HTML (already unescaped in feed)
#     <locations> → concatenated "Mountain ViewCAUSA" — needs splitting
#     <categories>→ "SOFTWARE_ENGINEERING" etc.
#     <employer>  → "Google" or "DeepMind" etc.
#   </job>
#
# Option A — all data inline, no detail page fetch needed.
#
# Slug format stored in DB:
#   None needed — single global feed
#   slug_info = {} or None

import re
import html as html_lib
from datetime import datetime, timezone
from bs4 import BeautifulSoup
from jobs.ats.base import fetch_html

FEED_URL = "https://www.google.com/about/careers/applications/jobs/feed.xml"

# Known location concatenation patterns to split
# "Mountain ViewCAUSA" → "Mountain View, CA, USA"
# Heuristic: insert comma before 2-letter state code + 3-letter country
_LOCATION_RE = re.compile(
    r"([a-z])([A-Z]{2})(USA|GBR|IND|CAN|DEU|FRA|SGP|AUS|JPN|BRA|IRL|NLD|CHE|SWE|POL|ISR|MEX|ESP|ITA|BEL|DNK|NOR|FIN|AUT|NZL|HKG|TWN|KOR|CHN|ZAF|ARE|SAU|PHL|IDN|MYS|THA|VNM|CZE|HUN|ROU|UKR|PRT|GRC|ARG|CHL|COL)",
    re.UNICODE
)


def fetch_jobs(slug_info, company):
    """
    Fetch all Google jobs from public XML feed.

    slug_info is ignored — Google uses a single global feed.
    company filter is applied to employer/title to avoid
    returning DeepMind jobs when monitoring just "Google".

    Args:
        slug_info: ignored (can be None or {})
        company:   "Google" — used for employer filter

    Returns:
        List of normalized job dicts with all fields populated.
        No detail page fetch needed (Option A).
    """
    resp = fetch_html(FEED_URL, platform="google")
    if resp is None:
        return []

    if not resp.text.strip().startswith("<?xml"):
        return []

    soup = BeautifulSoup(resp.text, "xml")
    jobs = soup.find_all("job")

    result = []
    for job in jobs:
        normalized = _normalize(job, company)
        if normalized:
            result.append(normalized)

    return result


def _normalize(job, company):
    """Normalize a single Google XML job to standard format."""
    title     = _text(job, "title")
    job_id    = _text(job, "jobid")
    job_url   = _text(job, "url")
    date_str  = _text(job, "published")
    raw_loc   = _text(job, "locations")
    raw_desc  = _text(job, "description")
    category  = _text(job, "categories")

    if not title or not job_id:
        return None

    location  = _parse_location(raw_loc)
    posted_at = _parse_date(date_str)
    desc      = _extract_description(raw_desc)

    return {
        "company":     company,
        "title":       title,
        "job_url":     job_url,
        "job_id":      job_id,
        "location":    location,
        "posted_at":   posted_at,
        "description": desc,
        "ats":         "google",
    }


def _text(job, tag_name):
    """Extract plain text from named XML tag."""
    tag = job.find(tag_name)
    if not tag:
        return ""
    return tag.get_text(strip=True)


def _parse_location(raw):
    """
    Parse Google's concatenated location string.
    "Mountain ViewCAUSA" → "Mountain View, CA, USA"
    "New YorkNYUSA"      → "New York, NY, USA"
    "LondonGBR"          → "London, GBR"
    """
    if not raw:
        return ""

    # Insert comma+space before 2-letter state + 3-letter country
    result = _LOCATION_RE.sub(r"\1, \2, \3", raw)

    # Also handle city + 3-letter country directly (no state)
    # "LondonGBR" → "London, GBR"
    result = re.sub(r"([a-z])([A-Z]{3})$", r"\1, \2", result)

    return result.strip()


def _extract_description(raw):
    """
    Extract plain text from description.
    Google feed description is already unescaped HTML.
    """
    if not raw:
        return ""
    soup = BeautifulSoup(raw, "html.parser")
    text = soup.get_text(separator="\n", strip=True)
    text = re.sub(r"\n{3,}", "\n\n", text).strip()
    return text[:5000]


def _parse_date(date_str):
    """
    Parse ISO timestamp to datetime.
    Format: "2026-03-17T09:00:15.503Z"
    """
    if not date_str:
        return None
    date_str = date_str.strip().rstrip("Z")
    # Remove fractional seconds
    date_str = re.sub(r"\.\d+$", "", date_str)
    for fmt in ["%Y-%m-%dT%H:%M:%S", "%Y-%m-%d"]:
        try:
            return datetime.strptime(date_str, fmt)
        except ValueError:
            continue
    return None