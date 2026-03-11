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

                # Extract plain slug for validation
                slug_for_validation = result["slug"]
                if result["platform"] in ("workday", "oracle_hcm"):
                    try:
                        slug_for_validation = json.loads(
                            result["slug"]
                        ).get("slug", result["slug"])
                    except (ValueError, TypeError):
                        pass
                # iCIMS: slug is already plain string
                # (extracted from subdomain)

                if not validate_slug_for_company(
                    slug_for_validation, company
                ):
                    continue

                # Additional validation: check page title/snippet
                # contains the company name to avoid false positives
                # e.g. "Ford Motor Company" → "fordfoundation" rejected
                # because "Ford Foundation" != "Ford Motor Company"
                page_title   = item.get("title", "")
                page_snippet = item.get("snippet", "")
                combined     = f"{page_title} {page_snippet}"

                if combined.strip():
                    from jobs.ats.base import validate_company_match
                    if not validate_company_match(combined, company):
                        logger.debug(
                            "Serper result rejected: company=%s "
                            "title=%s slug=%s",
                            company, page_title, slug_for_validation
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