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
SERPER_SEARCHES = [
    ("workday",    "site:myworkdayjobs.com"),
    ("oracle_hcm", "site:fa.oraclecloud.com"),
    ("icims",      "site:icims.com"),
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

            # Track credit usage
            increment_serper_credits(1)

            if resp.status_code == 429:
                print(f"   [WARNING] Serper rate limited")
                return SERPER_EXHAUSTED

            if resp.status_code in (401, 403):
                print(f"   [WARNING] Serper auth error {resp.status_code} "
                      f"— check SERPER_API_KEY")
                return None

            if resp.status_code != 200:
                print(f"   [WARNING] Serper error {resp.status_code} "
                      f"for {company}/{platform}")
                continue

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