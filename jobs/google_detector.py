# jobs/google_detector.py — Google search based ATS detection
#
# Philosophy: Google already knows the correct ATS URL for every company.
# Search "{company} site:{ats_domain}" and extract ATS + slug from result URL.
# Validate slug contains company keywords to prevent false positives.

import re
import time
from urllib.parse import quote, urlparse, parse_qs, unquote

from careershift.utils import human_delay
from jobs.ats.patterns import (
    ATS_SITE_SEARCHES,
    match_ats_pattern,
    validate_slug_for_company,
)

# Google search URL with autocorrect disabled
GOOGLE_SEARCH_URL = "https://www.google.com/search?q={query}&nfpr=1&num=10"

# Timeout for page load (ms)
PAGE_TIMEOUT = 30000

# Max results to scan per search
MAX_RESULTS = 10


def detect_via_google(company, page):
    """
    Search Google for company ATS URL using site: filter per platform.
    Tries each ATS platform in order, stops at first valid match.

    Args:
        company: company name (e.g. "Capital One")
        page:    Playwright page object

    Returns:
        {platform, slug} or None
    """
    found_platforms = set()

    for platform, site_filter in ATS_SITE_SEARCHES:
        if platform in found_platforms:
            continue

        query   = f"{company} {site_filter}"
        encoded = quote(query)
        url     = GOOGLE_SEARCH_URL.format(query=encoded)

        try:
            page.goto(url, wait_until="domcontentloaded",
                      timeout=PAGE_TIMEOUT)
            human_delay(2.0, 4.0)

            # Get content ONCE — used for both CAPTCHA + no-results
            content = page.content()

            # Check for CAPTCHA (pass content not page)
            if _is_captcha(content):
                print(f"   [WARNING] Google CAPTCHA detected — "
                      f"waiting 120s before retry")
                time.sleep(120)
                page.goto(url, wait_until="domcontentloaded",
                          timeout=PAGE_TIMEOUT)
                human_delay(3.0, 5.0)
                content = page.content()

                if _is_captcha(content):
                    print(f"   [WARNING] CAPTCHA persists — "
                          f"skipping Google detection for {company}")
                    return None

            # Check for no results
            if _no_results(content):
                continue

            # Extract all URLs from page
            urls = _extract_urls(page)

            # Match against ATS patterns + validate slug
            for url_candidate in urls[:MAX_RESULTS]:
                result = match_ats_pattern(url_candidate)
                if not result:
                    continue
                if result["platform"] != platform:
                    continue

                # Extract plain slug for validation
                # Workday/Oracle slugs are JSON — extract inner slug
                slug_for_validation = result["slug"]
                if result["platform"] in ("workday", "oracle_hcm"):
                    try:
                        import json as _json
                        slug_for_validation = _json.loads(
                            result["slug"]
                        ).get("slug", result["slug"])
                    except (ValueError, TypeError):
                        pass

                # Validate company name in slug
                if not validate_slug_for_company(
                    slug_for_validation, company
                ):
                    continue

                print(f"   [GOOGLE] {company} → "
                      f"{result['platform']} "
                      f"(slug: {result['slug']})")
                return result

        except Exception as e:
            print(f"   [WARNING] Google search failed for "
                  f"{company}/{platform}: {e}")
            continue

    return None


def _extract_urls(page):
    """
    Extract all URLs from Google search results page.
    Handles multiple Google result formats + redirect URLs.
    """
    urls = []

    try:
        # Try multiple selectors for robustness
        # Google changes HTML structure occasionally
        selectors = [
            "a[href]",
            "[data-ved] a[href]",
            ".yuRUbf a[href]",
            "cite",  # URL shown in result snippet
        ]

        seen = set()
        for selector in selectors:
            try:
                elements = page.query_selector_all(selector)
                for el in elements:
                    href = el.get_attribute("href")
                    if not href:
                        continue

                    # Decode Google redirect first
                    decoded = _decode_redirect(href)
                    if not decoded:
                        continue

                    # Deduplicate on decoded URL
                    if decoded in seen:
                        continue
                    seen.add(decoded)
                    urls.append(decoded)
            except Exception:
                continue

    except Exception as e:
        print(f"   [WARNING] URL extraction failed: {e}")

    return urls


def _decode_redirect(url):
    """
    Decode Google redirect URLs:
    /url?q=https://boards.greenhouse.io/stripe → actual URL
    """
    if not url:
        return None
    try:
        parsed = urlparse(url)
        if parsed.path in ("/url", "/search"):
            qs = parse_qs(parsed.query)
            if "q" in qs:
                return unquote(qs["q"][0])
        # Only keep http/https URLs
        if url.startswith(("http://", "https://")):
            return url
    except Exception:
        pass
    return None


def _is_captcha(content):
    """Detect if Google is showing CAPTCHA. Accepts HTML content string."""
    try:
        content_lower = content.lower() if content else ""
        return (
            "captcha" in content_lower or
            "recaptcha" in content_lower or
            "unusual traffic" in content_lower or
            "verify you're human" in content_lower or
            "i'm not a robot" in content_lower
        )
    except Exception:
        return False


def _no_results(content):
    """Detect if Google returned no results."""
    indicators = [
        "did not match any documents",
        "no results found",
        "your search - ",  # "Your search - X - did not match"
    ]
    content_lower = content.lower()
    return any(ind in content_lower for ind in indicators)