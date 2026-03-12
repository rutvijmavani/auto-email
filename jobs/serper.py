import logging

logger = logging.getLogger(__name__)

# jobs/serper.py — Phase 3b: Serper.dev API for Workday + Oracle detection
#
# Only used when Phase 1 (sitemap), Phase 2 (API probe),
# and Phase 3a (HTML redirect) all fail.
#
# Searches: "{company} site:myworkdayjobs.com"
#           "{company} site:fa.oraclecloud.com"
#
# Uses 2 credits per company.
# 2500 free credits on signup → covers 1250 companies.
# Sends email alert when < 50 credits remain.

import json
import requests
from config import SERPER_API_KEY, SERPER_API_URL
from db.serper_quota import increment_serper_credits, has_serper_credits
from jobs.ats.patterns import match_ats_pattern, validate_slug_for_company

# Only search these platforms via Serper
# (others handled by sitemap/API probe)
# Only Workday + Oracle HCM via Serper (2 credits per company)
# iCIMS removed — handled by Phase 2 API probe + Brave Search
SERPER_SEARCHES = [
    ("workday",    "site:myworkdayjobs.com"),
    ("oracle_hcm", "site:fa.oraclecloud.com"),
]

# Sentinel for exhausted credits
SERPER_EXHAUSTED = object()


def _verify_slug_via_api(result):
    """
    Verify a detected slug by actually calling the ATS API.
    Returns True if API responds with jobs (or valid empty board).
    Returns False if API returns error/404.

    This is the ground truth — no string matching needed.
    Works for any company regardless of slug naming convention.
    """
    platform = result["platform"]
    slug     = result["slug"]

    try:
        if platform == "workday":
            from jobs.ats.workday import _build_url, fetch_json_post
            url  = _build_url(json.loads(slug) if slug.startswith("{") else
                              {"slug": slug, "wd": "wd1", "path": "careers"})
            data = fetch_json_post(url, body={"limit": 1, "offset": 0})
            if data is None:
                return False
            # Valid if API responds — even 0 jobs means board exists
            return "jobPostings" in data or "total" in data

        elif platform == "oracle_hcm":
            from jobs.ats.oracle_hcm import _build_oracle_url, fetch_json_get
            info = json.loads(slug) if isinstance(slug, str) else slug
            url  = _build_oracle_url(
                info.get("slug", ""), info.get("region", ""),
                info.get("site", ""), 1, 0
            )
            from jobs.ats.base import fetch_json
            data = fetch_json(url)
            return data is not None and "items" in data

        elif platform == "greenhouse":
            from jobs.ats.base import fetch_json
            url  = f"https://boards-api.greenhouse.io/v1/boards/{slug}/jobs"
            data = fetch_json(url)
            return data is not None and "jobs" in data

        elif platform == "lever":
            from jobs.ats.base import fetch_json
            url  = f"https://api.lever.co/v0/postings/{slug}"
            data = fetch_json(url)
            return isinstance(data, list)

        elif platform == "ashby":
            from jobs.ats.base import fetch_json
            url  = f"https://api.ashbyhq.com/posting-api/job-board/{slug}"
            data = fetch_json(url)
            return data is not None and "jobs" in data

        # For other platforms — fall back to accepting the result
        return True

    except Exception as e:
        logger.debug("API verification failed for %s/%s: %s",
                     platform, slug, e)
        return True  # conservative — accept on error


def detect_via_serper(company):
    """
    Phase 3b: Search Google via Serper API for Workday/Oracle ATS.

    Args:
        company: company name (e.g. "Capital One")

    Returns:
        {platform, slug}  — ATS found ✓
        None              — not found
        SERPER_EXHAUSTED  — credits exhausted
    """
    if not SERPER_API_KEY:
        print("   [ERROR] SERPER_API_KEY not set in .env")
        return None

    for platform, site_filter in SERPER_SEARCHES:

        # Check credits before each search
        if not has_serper_credits(needed=1):
            print(f"   [WARNING] Serper credits exhausted")
            return SERPER_EXHAUSTED

        query = f"{company} {site_filter}"

        try:
            resp = requests.post(
                SERPER_API_URL,
                headers={
                    "X-API-KEY":    SERPER_API_KEY,
                    "Content-Type": "application/json",
                },
                json={"q": query, "num": 10},
                timeout=10
            )

            if resp.status_code == 429:
                print("   [WARNING] Serper rate limited — retry later")
                # 429 = transient, not exhausted — don't charge credit
                continue

            if resp.status_code in (401, 403):
                print(f"   [WARNING] Serper auth error {resp.status_code} "
                      f"— check SERPER_API_KEY")
                return None  # no credit charged on auth failure

            if resp.status_code != 200:
                print(f"   [WARNING] Serper error {resp.status_code} "
                      f"for {company}/{platform}")
                continue  # no credit charged on error

            # Only charge credit on successful response
            increment_serper_credits(1)

            items = resp.json().get("organic", [])

            if not items:
                continue

            for item in items:
                url    = item.get("link", "")
                result = match_ats_pattern(url)

                if not result:
                    continue
                if result["platform"] != platform:
                    continue

                # Validate by checking page title/snippet first
                # This is a quick pre-filter before hitting the API
                page_title   = item.get("title", "")
                page_snippet = item.get("snippet", "")
                combined     = f"{page_title} {page_snippet}"

                if combined.strip():
                    from jobs.ats.base import validate_company_match
                    if not validate_company_match(combined, company):
                        logger.debug(
                            "Serper result rejected by title: "
                            "company=%s title=%s",
                            company, page_title
                        )
                        continue

                # Final validation: actually call the API
                # Truth comes from the API, not string matching
                # If API returns jobs → slug is correct
                # If API returns 0 or error → try next result
                if not _verify_slug_via_api(result):
                    logger.debug(
                        "Serper result rejected by API: "
                        "company=%s slug=%s",
                        company, result["slug"]
                    )
                    continue

                print(f"   [SERPER] {company} -> {platform} "
                      f"(slug: {result['slug']})")
                return result

        except requests.exceptions.Timeout:
            print(f"   [WARNING] Serper timeout for {company}/{platform}")
            continue
        except Exception as e:
            print(f"   [WARNING] Serper error for {company}/{platform}: {e}")
            continue

    return None