# jobs/career_page.py — Phase 3a: HTML + redirect scan
#
# Fetches company career page and scans for ATS fingerprints.
# Two detection methods:
#   1. Redirect: company.com/careers → redirects to ATS URL directly
#   2. HTML scan: ATS domain found in page source
#
# Platforms detected:
#   Greenhouse, Lever, Ashby, SmartRecruiters, Workday, Oracle HCM,
#   iCIMS, SuccessFactors (redirect only)
#   Phenom People, TalentBrew/Radancy, Avature (HTML fingerprint)
#
# Note: Phenom/TalentBrew/Avature slug extraction is done in
# prospective_form_sync.py which has the full career page URL context.
# This file only signals the platform — slug extraction happens upstream.

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

# ATS domains to scan for in HTML — ordered by specificity
# Each entry: (signal_string, platform_hint)
# signal_string found in HTML → platform_hint returned alongside URL match
ATS_FINGERPRINTS = [
    # Standard ATS — URL-based detection via match_ats_pattern
    "boards.greenhouse.io",
    "job-boards.greenhouse.io",
    "jobs.lever.co",
    "jobs.ashbyhq.com",
    "jobs.smartrecruiters.com",
    "myworkdayjobs.com",
    "fa.oraclecloud.com",
    "icims.com",
    "successfactors.com",
    # HTML-fingerprinted platforms — detected by CDN/script presence
    "cdn.phenompeople.com",     # Phenom People
    "tbcdn.talentbrew.com",     # TalentBrew / Radancy
    "avature.net",              # Avature (custom domain tenants)
    "apply.workable.com",       # Workable
]

# Platforms where slug is opaque (not derived from company name)
# Trust any URL found on the company's own career page
OPAQUE_SLUG_PLATFORMS = {"workday", "oracle_hcm"}

# Platforms we can detect but can't get useful slugs from URL alone
# slug extraction needs full career page context (done in prospective_form_sync)
RICH_SLUG_PLATFORMS = {"phenom", "talentbrew", "avature"}


def _slug_valid_for_company(result, company):
    """
    Validate ATS slug against company name.
    Skips validation for opaque slug platforms (Workday/Oracle).
    Skips validation for rich slug platforms (Phenom/TalentBrew/Avature).
    """
    platform = result.get("platform", "")
    if platform in OPAQUE_SLUG_PLATFORMS:
        return True
    if platform in RICH_SLUG_PLATFORMS:
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

    for path in CAREER_PATHS:
        url    = f"https://{domain}{path}"
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
        final_url = resp.url
        if final_url != url:
            logger.debug("[P3a] Redirect: %s → %s", url, final_url)
            result = match_ats_pattern(final_url)
            if result and _slug_valid_for_company(result, company):
                return result

        if resp.status_code != 200:
            return None

        return _scan_html(resp.text, company)

    except requests.exceptions.Timeout:
        logger.debug("[P3a] Timeout: %s", url)
        return None
    except requests.exceptions.SSLError:
        try:
            http_url = url.replace("https://", "http://")
            resp     = requests.get(http_url, headers=HEADERS,
                                    timeout=TIMEOUT, allow_redirects=True)
            if resp.url != http_url:
                result = match_ats_pattern(resp.url)
                if result and _slug_valid_for_company(result, company):
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

    For standard ATS: extracts URL and calls match_ats_pattern().
    For Phenom/TalentBrew/Avature: detects via CDN/script presence,
    returns platform with empty slug (slug extracted upstream with full context).
    """
    # Standard ATS — extract URLs and pattern match
    url_pattern = re.compile(
        r'https?://[^\s"\'<>]+(?:' +
        '|'.join(re.escape(d) for d in ATS_FINGERPRINTS) +
        r')[^\s"\'<>]*',
        re.IGNORECASE
    )

    for url_match in url_pattern.finditer(html):
        url    = url_match.group(0).rstrip('.,;)')
        result = match_ats_pattern(url)
        if not result:
            # Check if it's a rich-slug platform detected by CDN presence
            for platform, signal in [
                ("phenom",     "cdn.phenompeople.com"),
                ("talentbrew", "tbcdn.talentbrew.com"),
                ("avature",    "avature.net"),
            ]:
                if signal in url:
                    logger.debug("[P3a] Rich-slug platform detected: %s", platform)
                    # Return platform with empty slug — upstream extracts full slug_info
                    return {"platform": platform, "slug": ""}
            continue

        if _slug_valid_for_company(result, company):
            return result

    return None