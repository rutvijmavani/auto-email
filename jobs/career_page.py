# jobs/career_page.py — Phase 3a: HTML + redirect scan
#
# Fetches company career page and scans for ATS fingerprints.
# Two detection methods:
#   1. Redirect: company.com/careers → redirects to ATS URL directly
#   2. HTML scan: ATS domain found in page source
#
# Works for ~30% of Phase 3 companies (non-JS career pages).
# Fast (~100ms), free, no browser needed.

import re
import requests
from logger import get_logger
from jobs.ats.patterns import match_ats_pattern, validate_slug_for_company

logger = get_logger(__name__)

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/121.0.0.0 Safari/537.36"
    ),
    "Accept":          "text/html,application/xhtml+xml,application/xml",
    "Accept-Language": "en-US,en;q=0.9",
}

TIMEOUT = 10

# Common career page paths to try
CAREER_PATHS = [
    "/careers",
    "/careers/",
    "/jobs",
    "/jobs/",
    "/about/careers",
    "/company/careers",
    "/en/careers",
    "/us/careers",
    "/join-us",
    "/work-with-us",
    "/work-here",
    "/opportunities",
]

# ATS domains to scan for in HTML
ATS_FINGERPRINTS = [
    "boards.greenhouse.io",
    "job-boards.greenhouse.io",
    "jobs.lever.co",
    "jobs.ashbyhq.com",
    "jobs.smartrecruiters.com",
    "myworkdayjobs.com",
    "fa.oraclecloud.com",
    "icims.com",
    "successfactors.com",
]


def _slug_valid_for_company(result, company):
    """
    Validate ATS slug against company name.
    Only validates platforms where slug encodes company name
    (Greenhouse, Lever, Ashby, iCIMS).
    Skips validation for Workday/Oracle where slugs are opaque
    (e.g. "wf" for Wells Fargo, "jpmc" for JPMorgan).
    """
    platform = result.get("platform", "")
    # Opaque slug platforms — trust URL found on company site
    if platform in ("workday", "oracle_hcm"):
        return True
    slug = result.get("slug", "")
    return validate_slug_for_company(slug, company)


def detect_via_career_page(company, domain):
    """
    Phase 3a: Scan company career page for ATS fingerprints.

    Args:
        company: company name (e.g. "Capital One")
        domain:  company domain (e.g. "capitalone.com")

    Returns:
        {platform, slug} if found
        None             if not found
    """
    if not domain:
        logger.debug("[P3a] No domain for %r — skipping", company)
        return None

    # Clean domain
    domain = domain.lower().strip()
    if domain.startswith("http"):
        domain = re.sub(r'^https?://', '', domain).rstrip('/')

    logger.debug("[P3a] Scanning career page: company=%r domain=%s", company, domain)

    # Try career paths
    for path in CAREER_PATHS:
        url = f"https://{domain}{path}"
        result = _scan_url(url, company)
        if result:
            logger.info("[P3a HIT] company=%r → platform=%s slug=%s via %s",
                        company, result["platform"], result["slug"], url)
            return result

    logger.debug("[P3a MISS] No ATS found for %r (domain=%s)", company, domain)
    return None


def _scan_url(url, company):
    """
    Fetch URL and scan for ATS fingerprints.
    Checks both redirect URL and HTML content.
    """
    try:
        resp = requests.get(
            url, headers=HEADERS,
            timeout=TIMEOUT,
            allow_redirects=True
        )

        # Check 1: Final URL after redirect
        # e.g. capitalone.com/careers → capitalone.wd12.myworkdayjobs.com
        final_url = resp.url
        if final_url != url:
            logger.debug("[P3a] Redirect detected: %s → %s", url, final_url)
            result = match_ats_pattern(final_url)
            if result:
                # Validate slug for platforms where slug = company name
                # Skip for Workday/Oracle — slugs are opaque (wf, jpmc)
                if _slug_valid_for_company(result, company):
                    logger.debug("[P3a] Redirect match: platform=%s slug=%s",
                                 result["platform"], result["slug"])
                    return result

        if resp.status_code != 200:
            logger.debug("[P3a] Non-200 response: url=%s status=%d", url, resp.status_code)
            return None

        # Check 2: Scan HTML for ATS domain fingerprints
        html = resp.text
        return _scan_html(html, company)

    except requests.exceptions.Timeout:
        logger.debug("[P3a] Timeout: url=%s", url)
        return None
    except requests.exceptions.SSLError:
        # Try http fallback
        logger.debug("[P3a] SSL error — trying http fallback: %s", url)
        try:
            http_url = url.replace("https://", "http://")
            resp = requests.get(
                http_url, headers=HEADERS,
                timeout=TIMEOUT, allow_redirects=True
            )
            final_url = resp.url
            if final_url != http_url:
                result = match_ats_pattern(final_url)
                if result and _slug_valid_for_company(result, company):
                    logger.debug("[P3a] HTTP fallback redirect match: platform=%s slug=%s",
                                 result["platform"], result["slug"])
                    return result
            if resp.status_code == 200:
                return _scan_html(resp.text, company)
        except Exception:
            pass
        return None
    except Exception as e:
        logger.debug("[P3a] Scan error for %s: %s", url, e)
        return None


def _scan_html(html, company):
    """
    Scan HTML content for ATS URL fingerprints.
    Extracts URLs containing known ATS domains.
    """
    # Find all URLs in HTML containing ATS domains
    url_pattern = re.compile(
        r'https?://[^\s"\'<>]+(?:' +
        '|'.join(re.escape(d) for d in ATS_FINGERPRINTS) +
        r')[^\s"\'<>]*',
        re.IGNORECASE
    )

    for url_match in url_pattern.finditer(html):
        url = url_match.group(0).rstrip('.,;)')
        result = match_ats_pattern(url)
        if not result:
            continue

        if _slug_valid_for_company(result, company):
            logger.debug("[P3a] HTML fingerprint match: url=%s platform=%s slug=%s",
                         url, result["platform"], result["slug"])
            return result

    return None