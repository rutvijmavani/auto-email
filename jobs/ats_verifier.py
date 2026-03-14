import logging

from logger import get_logger
logger = get_logger(__name__)

# jobs/ats_verifier.py — Phase 2: ATS detection via API name probe
#
# Each supported ATS exposes a public endpoint that returns
# company name + jobs. We probe with slug variants and verify
# the returned company name matches our target.
#
# Returns 404 → definitively not on this ATS (no false positive)
# Returns 200 → verify company name → accept or reject
#
# No browser. No CAPTCHA. Pure requests.get().

import re
import requests
from jobs.ats.base import slugify
from jobs.ats.patterns import validate_slug_for_company

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/121.0.0.0 Safari/537.36"
    )
}

TIMEOUT = 8


def detect_via_api(company):
    """
    Phase 2: Probe ATS APIs with slug variants.
    Verify company name from API response.

    Tries: Greenhouse, Lever, Ashby, SmartRecruiters.
    Workday + Oracle HCM skipped (no public name API).

    Args:
        company: company name (e.g. "Charles Schwab")

    Returns:
        {platform, slug} if found
        None             if not found
    """
    logger.debug("[P2] detect_via_api: company=%r", company)
    candidates = slugify(company)
    logger.debug("[P2] slug candidates: %s", candidates)

    # Try each platform in reliability order
    for probe_fn in [
        _probe_greenhouse,
        _probe_lever,
        _probe_ashby,
        _probe_smartrecruiters,
        _probe_icims,
    ]:
        result = probe_fn(company, candidates)
        if result:
            logger.info("[P2 HIT] company=%r → platform=%s slug=%s",
                        company, result["platform"], result["slug"])
            return result

    logger.debug("[P2 MISS] No API match for %r", company)
    return None


# ─────────────────────────────────────────
# PLATFORM PROBERS
# ─────────────────────────────────────────

def _probe_greenhouse(company, candidates):
    """
    Probe Greenhouse API.
    boards-api.greenhouse.io/v1/boards/{slug}
    → {"name": "Stripe", "jobs": [...]}
    """
    for slug in candidates:
        url = f"https://boards-api.greenhouse.io/v1/boards/{slug}"
        result = _probe(url, company, "greenhouse", slug,
                        name_key="name")
        if result:
            return result
    return None


def _probe_lever(company, candidates):
    """
    Probe Lever API.
    api.lever.co/v0/postings/{slug}?mode=json
    → Returns list of job postings, each with company name in URLs
    """
    for slug in candidates:
        url = f"https://api.lever.co/v0/postings/{slug}?mode=json"
        try:
            resp = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
            if resp.status_code == 404:
                continue
            if resp.status_code != 200:
                continue

            data = resp.json()

            # Lever returns list — verify slug matches company
            if isinstance(data, list):
                # Verify via slug validation
                if validate_slug_for_company(slug, company):
                    logger.debug("[P2] Lever slug match: slug=%s company=%r", slug, company)
                    return {"platform": "lever", "slug": slug}
                # Also check job URLs for company name
                if data:
                    job_url = data[0].get("hostedUrl", "")
                    if company.lower().replace(" ", "") in job_url.lower():
                        logger.debug("[P2] Lever URL match: slug=%s company=%r", slug, company)
                        return {"platform": "lever", "slug": slug}

        except Exception as e:
            logger.debug("Probe failed: %s", e, exc_info=True)
            continue

    return None


def _probe_ashby(company, candidates):
    """
    Probe Ashby API.
    jobs.ashbyhq.com/api/non-user-graphql?op=ApiJobBoardWithTeams
    Simpler: check if job board page exists
    """
    for slug in candidates:
        url = (f"https://api.ashbyhq.com/posting-api/job-board/{slug}"
               f"?includeCompensation=true")
        try:
            resp = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
            if resp.status_code == 404:
                continue
            if resp.status_code != 200:
                continue

            data = resp.json()
            # Ashby returns {"jobBoard": {"name": "Linear"}, "jobs": [...]}
            board    = data.get("jobBoard", {})
            api_name = board.get("name", "")

            if not api_name:
                # No name — verify via slug only
                if validate_slug_for_company(slug, company):
                    logger.debug("[P2] Ashby slug match (no name): slug=%s company=%r",
                                 slug, company)
                    return {"platform": "ashby", "slug": slug}
                continue

            if _name_matches(api_name, company, slug):
                logger.debug("[P2] Ashby name match: slug=%s api_name=%r company=%r",
                             slug, api_name, company)
                return {"platform": "ashby", "slug": slug}

        except Exception as e:
            logger.debug("Probe failed: %s", e, exc_info=True)
            continue

    return None


def _probe_icims(company, candidates):
    """
    Probe iCIMS job board.
    careers-{slug}.icims.com/jobs/search?in_iframe=1
    Returns listing page with iCIMS_Anchor links if exists.
    """
    for slug in candidates:
        url = (f"https://careers-{slug}.icims.com"
               f"/jobs/search?in_iframe=1")
        try:
            resp = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
            if resp.status_code == 404:
                continue
            if resp.status_code != 200:
                continue

            # Verify it's a real iCIMS page
            if "iCIMS" not in resp.text and "icims" not in resp.text.lower():
                continue

            # Verify slug matches company
            if not validate_slug_for_company(slug, company):
                continue

            # Extra check — page title or company name should match
            from bs4 import BeautifulSoup
            soup  = BeautifulSoup(resp.text, "html.parser")
            title = soup.title.text if soup.title else ""
            if title and not _name_matches(title, company, slug):
                # Try without title check if page has jobs
                anchors = soup.select("a.iCIMS_Anchor")
                if not anchors:
                    continue

            logger.debug("[P2] iCIMS match: slug=%s company=%r", slug, company)
            return {"platform": "icims", "slug": slug}

        except Exception as e:
            logger.debug("Probe failed: %s", e, exc_info=True)
            continue

    return None


def _probe_smartrecruiters(company, candidates):
    """
    Probe SmartRecruiters API.
    api.smartrecruiters.com/v1/companies/{slug}
    → {"name": "Adobe"}
    """
    # SmartRecruiters uses capitalized slugs sometimes
    all_candidates = candidates + [c.capitalize() for c in candidates]
    for slug in all_candidates:
        url = f"https://api.smartrecruiters.com/v1/companies/{slug}"
        result = _probe(url, company, "smartrecruiters",
                        slug.lower(), name_key="name")
        if result:
            return result
    return None


# ─────────────────────────────────────────
# SHARED PROBE HELPER
# ─────────────────────────────────────────

def _probe(url, company, platform, slug, name_key="name"):
    """
    Make one API probe request.

    Returns {platform, slug} if company name matches.
    Returns None on 404, error, or name mismatch.
    """
    try:
        resp = requests.get(url, headers=HEADERS, timeout=TIMEOUT)

        if resp.status_code == 404:
            return None  # definitively not here

        if resp.status_code != 200:
            return None

        data = resp.json()
        if not isinstance(data, dict):
            return None

        api_name = data.get(name_key, "")

        if api_name:
            if _name_matches(api_name, company, slug):
                logger.debug("[P2] _probe match: platform=%s slug=%s api_name=%r company=%r",
                             platform, slug, api_name, company)
                return {"platform": platform, "slug": slug}
            else:
                logger.debug("[P2] _probe name mismatch: platform=%s slug=%s "
                             "api_name=%r company=%r", platform, slug, api_name, company)
                return None  # name mismatch — wrong company

        # No name in response — fall back to slug validation
        if validate_slug_for_company(slug, company):
            logger.debug("[P2] _probe slug fallback match: platform=%s slug=%s company=%r",
                         platform, slug, company)
            return {"platform": platform, "slug": slug}

        return None

    except requests.exceptions.Timeout:
        logger.debug("[P2] _probe timeout: url=%s", url)
        return None
    except Exception:
        return None


# ─────────────────────────────────────────
# NAME MATCHING (no fuzzy — deterministic)
# ─────────────────────────────────────────

def _name_matches(api_name, company, slug):
    """
    Verify API-returned company name matches our target company.

    Strategy (NO fuzzy matching):
    1. Exact normalized match
    2. All significant keywords present in API name
    3. Slug-based validation as final disambiguator

    The slug parameter is used as a final check when keyword
    logic is ambiguous (e.g. API name has extra significant words).
    """
    if not api_name or not company:
        return False

    api_lower     = _normalize(api_name)
    company_lower = _normalize(company)

    # Rule 1: Normalized exact or substring match
    if company_lower == api_lower:
        return True
    if company_lower in api_lower:
        return _all_keywords_present(company, api_name)

    # Rule 2: All keywords present
    if not _all_keywords_present(company, api_name):
        return False

    # Rule 3: Slug-based fallback when keyword match is ambiguous
    # e.g. company="Block", api_name="H&R Block"
    # keywords pass but slug "block" should not match "hrblock"
    if slug:
        from jobs.ats.patterns import validate_slug_for_company
        return validate_slug_for_company(slug, company)

    return True


def _all_keywords_present(company, api_name):
    """
    ALL significant keywords from company must appear in api_name.

    "Capital One" → ["capital", "one"]
    api_name = "Capital Group" → has "capital" but NOT "one" → REJECT
    api_name = "Capital One Financial" → has both → ACCEPT

    Edge case: "Block" → ["block"]
    api_name = "H&R Block" → has "block"
    BUT: we also check that api_name keywords don't contain
    unrelated words that suggest a different company
    """
    from config import ATS_KEYWORD_STOP_WORDS

    # Get significant keywords from company name
    company_clean = re.sub(r'[^a-z0-9\s]', ' ', company.lower())
    company_words = [
        w for w in company_clean.split()
        if w not in ATS_KEYWORD_STOP_WORDS and len(w) >= 2
    ]

    if not company_words:
        return validate_slug_for_company(company.lower(), company)

    api_clean = re.sub(r'[^a-z0-9\s]', ' ', api_name.lower())
    api_words = set(api_clean.split())

    # All company keywords must appear in api name
    for word in company_words:
        if word not in api_words:
            return False

    # Extra disambiguation for short single-keyword companies
    # "Block" → keyword "block" appears in "H&R Block"
    # BUT H&R Block has extra words "h" and "r"
    # → Check: are there significant non-stop words in api_name
    #   that are NOT in company name?
    if len(company_words) == 1:
        extra_api_words = [
            w for w in api_words
            if (w not in ATS_KEYWORD_STOP_WORDS
                and len(w) >= 2
                and w not in company_words)
        ]
        # If api_name has significant extra words → probably different company
        if len(extra_api_words) >= 2:
            return False

    return True


def _normalize(name):
    """Normalize company name for comparison."""
    from config import ATS_KEYWORD_STOP_WORDS
    name = re.sub(r'[^a-z0-9\s]', ' ', name.lower())
    words = [
        w for w in name.split()
        if w not in ATS_KEYWORD_STOP_WORDS and len(w) >= 2
    ]
    return " ".join(words)