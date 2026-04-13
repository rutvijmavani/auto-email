"""
jobs/utils.py — Shared utilities for all jobs modules.

All helpers here are pure functions with no side effects.
No imports from db/, no imports from other jobs/ submodules.
Safe to import from anywhere in the jobs/ tree.

Contents:
  HTTP helpers      — is_json, is_valid_url, domain_from_url
  Request helpers   — build_request_kwargs, should_skip_header
  Text helpers      — clean_html, parse_salary_text, normalize_text
  Date helpers      — parse_date_value
  URL helpers       — extract_url_from_value, extract_job_id_from_path

Constants:
  SKIP_HEADERS      — HTTP/2 pseudo + hop-by-hop headers to omit
  REQUEST_TIMEOUT   — default timeout for all outbound requests
"""

import re
import json
from datetime import datetime, timezone
from urllib.parse import urlparse

from bs4 import BeautifulSoup


# ─────────────────────────────────────────
# CONSTANTS
# ─────────────────────────────────────────

# Headers that must never be forwarded when replaying curl requests.
# HTTP/2 pseudo-headers and hop-by-hop headers cause errors if sent.
SKIP_HEADERS = {
    ":method", ":path", ":scheme", ":authority",
    "content-length", "transfer-encoding", "connection", "host",
}

# Default timeout for all outbound HTTP requests (seconds)
REQUEST_TIMEOUT = 20

# Unix timestamp bounds for date detection
UNIX_TS_MIN    = 1_000_000_000       # Sep 2001 — anything below is not a ts
UNIX_TS_MS_MIN = 1_000_000_000_000   # millisecond timestamps
UNIX_TS_MS_MAX = 2_000_000_000_000   # May 2033 — anything above is not a ts
                                     # e.g. Microsoft position IDs like
                                     # 1970393556855691 are above this


# ─────────────────────────────────────────
# HTTP HELPERS
# ─────────────────────────────────────────

def is_json(s):
    """
    Return True if string is valid JSON.
    Used to decide whether to send as json= or data= in requests.
    """
    if not s:
        return False
    try:
        json.loads(s)
        return True
    except (json.JSONDecodeError, TypeError, ValueError):
        return False


def is_valid_url(url):
    """Return True if url starts with http:// or https://."""
    if not url:
        return False
    return bool(re.match(r"https?://", url.strip()))


def domain_from_url(url):
    """
    Extract registrable domain from any URL.
    Returns e.g. 'starbucks.com', or hostname if tldextract unavailable.

    Examples:
        https://jobs.stripe.com/careers → stripe.com
        https://boards.greenhouse.io/stripe → greenhouse.io
    """
    if not url:
        return None
    try:
        import tldextract
        extracted = tldextract.extract(urlparse(url).hostname or "")
        return extracted.registered_domain or urlparse(url).hostname
    except Exception:
        return urlparse(url).hostname


def should_skip_header(header_name):
    """
    Return True if this header should be omitted when replaying requests.
    Covers HTTP/2 pseudo-headers and hop-by-hop headers.
    """
    return header_name.lower() in SKIP_HEADERS


def build_request_kwargs(method, url, params=None, body=None,
                         extra_headers=None):
    """
    Build kwargs dict for requests.get() or requests.post().
    Handles JSON vs form-encoded body detection automatically.

    Args:
        method        — "GET" or "POST"
        url           — target URL
        params        — dict of query params (None = omit)
        body          — raw body string (None = omit)
        extra_headers — dict of headers to add (applied by caller)

    Returns dict suitable for **requests.request(method, url, **kwargs).
    """
    kwargs = {
        "params":  params or None,
        "timeout": REQUEST_TIMEOUT,
    }

    if method.upper() == "POST" and body:
        if is_json(body):
            kwargs["json"] = json.loads(body)
        else:
            kwargs["data"] = body

    return kwargs


# ─────────────────────────────────────────
# TEXT HELPERS
# ─────────────────────────────────────────

def clean_html(text, max_length=8000):
    """
    Strip HTML tags from text and normalize whitespace.
    Returns plain text string truncated to max_length.

    If text contains no HTML tags, normalizes whitespace only.
    """
    if not text:
        return ""
    if "<" not in text:
        cleaned = re.sub(r"\n{3,}", "\n\n", text).strip()
        return cleaned[:max_length]
    try:
        soup    = BeautifulSoup(text, "html.parser")
        cleaned = soup.get_text(separator="\n", strip=True)
        cleaned = re.sub(r"\n{3,}", "\n\n", cleaned).strip()
        return cleaned[:max_length]
    except Exception:
        # BeautifulSoup failed — strip tags manually as fallback
        stripped = re.sub(r"<[^>]+>", " ", text)
        stripped = re.sub(r"\s+", " ", stripped).strip()
        return stripped[:max_length]


def parse_salary_text(text):
    """
    Parse salary range from free-form text.
    Handles formats like: '$120,000 - $160,000', '$120k-$160k',
    '120000 to 180000 USD annually'.

    Returns (salary_min, salary_max) as strings, or ("", "") if not found.
    """
    if not text:
        return "", ""
    # Extract all numeric sequences that look like salaries (4+ digits)
    nums = re.findall(r"[\$£€]?\s*(\d[\d,\.]+)", text)
    nums = [
        n.replace(",", "").split(".")[0]   # strip commas + decimals
        for n in nums
        if len(n.replace(",", "")) >= 4    # at least 4 digits
    ]
    # Handle 'k' suffix: 120k → 120000
    raw_nums = re.findall(r"(\d+(?:\.\d+)?)\s*[kK]\b", text)
    k_nums = [str(int(float(n) * 1000)) for n in raw_nums]
    nums = k_nums if k_nums else nums

    if len(nums) >= 2:
        return nums[0], nums[1]
    if len(nums) == 1:
        return nums[0], nums[0]
    return "", ""


def normalize_text(text):
    """
    Lowercase, strip basic accents, collapse whitespace.
    Used for consistent text matching in filters and hashing.
    """
    if not text:
        return ""
    replacements = {
        "é": "e", "è": "e", "ê": "e", "ë": "e",
        "à": "a", "â": "a", "ä": "a",
        "ü": "u", "ú": "u", "û": "u",
        "ó": "o", "ô": "o", "ö": "o",
        "ñ": "n", "ç": "c",
    }
    result = text.lower()
    for accented, plain in replacements.items():
        result = result.replace(accented, plain)
    return " ".join(result.split())


# ─────────────────────────────────────────
# DATE HELPERS
# ─────────────────────────────────────────

def parse_date_value(val):
    """
    Parse a date value from various formats into a datetime object.
    Handles:
      - datetime objects (returned as-is, made timezone-aware)
      - Unix timestamps (int, seconds or milliseconds)
      - ISO 8601 strings
      - Common date format strings

    Returns datetime (UTC-aware) or None if unparseable.
    """
    if val is None:
        return None

    # Already a datetime
    if isinstance(val, datetime):
        if val.tzinfo is None:
            return val.replace(tzinfo=timezone.utc)
        return val

    # Unix timestamp (int)
    if isinstance(val, int):
        try:
            if val > UNIX_TS_MS_MIN:
                return datetime.fromtimestamp(val / 1000, tz=timezone.utc)
            if val > UNIX_TS_MIN:
                return datetime.fromtimestamp(val, tz=timezone.utc)
        except (ValueError, OSError):
            pass
        return None

    # String
    if isinstance(val, str):
        val = val.strip()
        if not val:
            return None
        # ISO 8601
        try:
            return datetime.fromisoformat(val.replace("Z", "+00:00"))
        except (ValueError, AttributeError):
            pass
        # Common formats
        for fmt in (
            "%Y-%m-%d",
            "%m/%d/%Y",
            "%d/%m/%Y",
            "%B %d, %Y",    # January  5, 2026
            "%b %d, %Y",    # Jan 5, 2026
            "%B %d %Y",
            "%b %d %Y",
            "%d %B %Y",
            "%d %b %Y",
        ):
            try:
                return datetime.strptime(val[:30].strip(), fmt).replace(
                    tzinfo=timezone.utc
                )
            except ValueError:
                continue

    return None


# ─────────────────────────────────────────
# URL HELPERS
# ─────────────────────────────────────────

def extract_url_from_value(val, base_url=""):
    """
    Extract a clean absolute URL from a raw field value.
    Handles absolute URLs, relative paths, and None/empty values.

    Args:
        val      — raw value from API response field
        base_url — base URL to prepend to relative paths

    Returns absolute URL string or empty string.
    """
    if not isinstance(val, str) or not val.strip():
        return ""
    val = val.strip()
    if val.startswith("http"):
        return val
    if val.startswith("/") and base_url:
        parsed = urlparse(base_url)
        return f"{parsed.scheme}://{parsed.netloc}{val}"
    return val


def extract_job_id_from_path(url):
    """
    Extract job ID from a URL path — best effort.
    Tries multiple patterns in priority order.

    Returns job_id string or empty string.

    Examples:
        /en/jobs/3152869/title         → "3152869"
        /careers/job/title--248705     → "248705"
        /results/80662660827226822-eng → "80662660827226822"
        /job/slug/12345                → "12345"
    """
    if not url:
        return ""
    path = urlparse(url).path

    # Long numeric before dash (Google style: 15+ digit IDs)
    m = re.search(r"/(\d{15,})-", path)
    if m:
        return m.group(1)

    # Numeric before slug segment: /jobs/3152869/title
    m = re.search(r"/(\d{5,})/[a-z0-9\-]+(?:$|\?)", path)
    if m:
        return m.group(1)

    # After double dash: /job/title--248705
    m = re.search(r"--(\d{5,})(?:/|$|\?)", path)
    if m:
        return m.group(1)

    # Numeric last segment: /JobDetail/499961 or /careers/list/156740/
    m = re.search(r"/(\d{4,})(?:/|\?|$)", path)
    if m:
        return m.group(1)

    # After slug with separator: /job/slug-title/12345
    m = re.search(r"/[a-z][a-z0-9\-]+-(\d{5,})(?:\?|$|/)", path)
    if m:
        return m.group(1)

    # UUID
    m = re.search(
        r"/([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}"
        r"-[0-9a-f]{4}-[0-9a-f]{12})(?:/|$|\?)",
        path, re.I
    )
    if m:
        return m.group(1)

    return ""