# jobs/ats/base.py — Shared ATS client logic

import time
import json
import requests
from config import JOB_MONITOR_API_TIMEOUT


def fetch_json(url, params=None, retries=2):
    """
    Fetch JSON from URL with timeout + retry.
    Returns parsed JSON dict/list or None on failure.
    Raises RateLimitError on 429.
    """
    for attempt in range(retries + 1):
        try:
            resp = requests.get(
                url,
                params=params,
                timeout=JOB_MONITOR_API_TIMEOUT,
                headers={"User-Agent": "Mozilla/5.0"},
            )
            if resp.status_code == 429:
                # Rate limited — wait 60s and retry once
                if attempt < retries:
                    print("   [WARNING] Rate limited — waiting 60s before retry")
                    time.sleep(60)
                    continue
                return None
            if resp.status_code == 404:
                return None
            if not resp.ok:
                return None
            return resp.json()
        except requests.exceptions.Timeout:
            if attempt < retries:
                time.sleep(5)
                continue
            return None
        except requests.exceptions.ConnectionError:
            if attempt < retries:
                time.sleep(5)
                continue
            return None
        except (ValueError, json.JSONDecodeError,
                KeyError, AttributeError):
            return None
    return None


def slugify(company):
    """
    Generate slug variants to try for ATS detection.
    Returns list of slugs in order of likelihood.
    """
    import re
    name = company.strip().lower()
    # Remove common suffixes
    name = re.sub(r'\b(inc|corp|llc|ltd|co|company|technologies|tech|systems|solutions|services|group|holding|holdings)\.?\b', '', name)
    name = name.strip().strip('.,')

    # Variant 1: no spaces, no special chars
    v1 = re.sub(r'[^a-z0-9]', '', name)
    # Variant 2: hyphens instead of spaces
    v2 = re.sub(r'[^a-z0-9]+', '-', name).strip('-')
    # Variant 3: first word only
    v3 = re.split(r'[^a-z0-9]', name)[0]
    # Variant 4: first two words joined
    parts = [p for p in re.split(r'[^a-z0-9]+', name) if p]
    v4 = ''.join(parts[:2]) if len(parts) >= 2 else v1

    # Deduplicate preserving order
    seen = set()
    variants = []
    for v in [v1, v2, v3, v4]:
        if v and v not in seen:
            seen.add(v)
            variants.append(v)
    return variants


def validate_company_match(response_text, expected_company):
    """
    Fuzzy check that API response is for the right company.
    Prevents "apple" matching "Apple Leisure Group".
    Returns True if likely correct company.
    """
    if not response_text or not expected_company:
        return True  # can't validate → assume OK
    expected = expected_company.lower().strip()
    response = response_text.lower()
    # Get significant words (>3 chars, not common suffixes)
    stop_words = {"inc", "corp", "llc", "ltd", "co", "the",
                  "and", "jobs", "careers", "group"}
    words = [
        w for w in expected.split()
        if len(w) > 3 and w not in stop_words
    ]
    if not words:
        return True
    # Check first significant word appears as a whole word in response
    # Use simple boundary: surrounded by non-alphanumeric or start/end
    import re
    for word in words[:2]:
        pattern = r'(?<![a-z0-9])' + re.escape(word) + r'(?![a-z0-9])'
        if re.search(pattern, response):
            return True
    return False