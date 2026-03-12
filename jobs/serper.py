import logging
import re

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
from jobs.ats.patterns import match_ats_pattern

# Workday only via Serper (1 credit per company)
# Oracle HCM removed — site:fa.oraclecloud.com returns JPMorgan results
# for any company because Oracle job descriptions mention competitor names.
# Oracle HCM is auto-detected via Phase 3a: ats_detector.py calls
# oracle_hcm.detect() which follows company.com/careers → oraclecloud URL.
SERPER_SEARCHES = [
    ("workday", "site:myworkdayjobs.com"),
]

# Sentinel for exhausted credits
SERPER_EXHAUSTED = object()


def _match_compact_identifier(identifier, company):
    """
    Match company name against a compact ATS identifier.
    validate_company_match uses word boundaries which fail on CamelCase.

    Handles:
      "WellsFargoJobs"           → splits → "Wells Fargo Jobs" ✓
      "NVIDIAExternalCareerSite" → splits → "NVIDIA External Career Site" ✓
      "ASMLExternalCareerSite"   → splits → "ASML External Career Site" ✓
      "BlackRock_Professional"   → splits → "Black Rock Professional" ✓
    """
    from jobs.ats.base import validate_company_match
    # Replace underscores/hyphens with spaces
    s = identifier.replace("_", " ").replace("-", " ")
    # Split CamelCase: lowercase→Uppercase
    s = re.sub(r"([a-z])([A-Z])", r"\1 \2", s)
    # Split ALL_CAPS→Capital: "NVIDIAExternal" → "NVIDIA External"
    s = re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1 \2", s)
    if validate_company_match(s, company):
        return True
    # Fallback: direct prefix match for short all-caps names (e.g. ASML, NXP)
    company_clean    = re.sub(r"[^a-z0-9]", "", company.lower())
    identifier_clean = re.sub(r"[^a-z0-9]", "", identifier.lower())
    if company_clean and identifier_clean.startswith(company_clean):
        return True
    return False


def _get_workday_site_title(slug_info):
    """
    Extract company-identifying text from Workday career site HTML.

    Workday embeds company info in a JS object:
        window.eexworkday = {
            tenant: "wf",
            urlWID: "WellsFargoJobs",   <- company identifier
        }

    urlWID always contains the company name:
        "WellsFargoJobs"           -> Wells Fargo
        "NVIDIAExternalCareerSite" -> Nvidia
        "ASMLExternalCareerSite"   -> ASML
    """
    import re
    import requests
    from jobs.ats.workday import _build_url

    base_url = _build_url(slug_info).replace("/jobs", "")

    try:
        resp = requests.get(
            base_url,
            headers={"User-Agent": "Mozilla/5.0"},
            timeout=10,
        )
        if resp.status_code == 200:
            # Extract urlWID from window.eexworkday JS object
            # e.g.  urlWID: "WellsFargoJobs"
            match = re.search(r'urlWID\s*:\s*"([^"]+)"', resp.text)
            if match:
                return match.group(1).strip()
            logger.debug("urlWID not found in Workday HTML for %s",
                         slug_info.get("slug", "?"))
        else:
            logger.debug("Workday site fetch returned %s for %s",
                         resp.status_code, slug_info.get("slug", "?"))
    except Exception as e:
        logger.debug("Workday site title fetch failed for %s: %s",
                     slug_info.get("slug", "?"), e)

    # Fallback: path often contains company name
    path = slug_info.get("path", "")
    if path and path not in ("careers", "jobs", "External", "Careers"):
        return path

    # Last resort: slug itself
    return slug_info.get("slug", "")


def _verify_slug_via_api(result, company, title_verified=False, has_text=False):
    """
    Verify detected slug by calling the ATS API and confirming
    the company name appears in the response.

    Workday:    fetches career site title ("Wells Fargo Jobs") → match
    Greenhouse: fetches board name ("Stripe") → match
    Ashby:      fetches jobBoard.name → match
    Oracle:     confirms board exists (limited company data)
    Lever:      falls back to slug validation (no company name in API)
    """
    from jobs.ats.base import validate_company_match
    platform = result["platform"]
    slug     = result["slug"]

    try:
        if platform == "workday":
            # Parse slug info
            if isinstance(slug, str) and slug.startswith("{"):
                try:
                    slug_info = json.loads(slug)
                    if not isinstance(slug_info, dict):
                        return False
                except (ValueError, TypeError):
                    return False
            else:
                slug_info = {"slug": slug, "wd": "wd1", "path": "careers"}
            slug_info.setdefault("wd",   "wd1")
            slug_info.setdefault("path", "careers")

            # Generic urlWIDs/paths that carry no company signal
            GENERIC = {"careers", "jobs", "external", "search",
                       "career", "opportunities", "home", "ext"}

            # Layer 1: urlWID from career site HTML
            # e.g. "ASMLExternalCareerSite" → contains "asml" ✓
            # Some return generic: "careers", "External" → skip
            site_title = _get_workday_site_title(slug_info)
            if site_title and site_title.lower() not in GENERIC:
                if _match_compact_identifier(site_title, company):
                    return True
                # False → don't stop, try next layer

            # Layer 2: path from Serper URL
            # e.g. "qualcomm_careers" → contains "qualcomm" ✓
            # More reliable than urlWID — comes directly from URL
            path = slug_info.get("path", "")
            if path and path.lower() not in GENERIC:
                if _match_compact_identifier(path, company):
                    return True
                # False → don't stop, try layer 3

            # Layer 3: title_verified fallback
            # Both urlWID and path are generic (ms/External, qualcomm/careers)
            # Only trust if Serper returned meaningful title/snippet text
            # Prevents false positives on empty Serper responses
            if title_verified and has_text:
                return True

            return False

        elif platform == "oracle_hcm":
            # Oracle HCM API does not expose company name in search results
            # items[] is a search result object with internal dept codes,
            # not job requisitions with organizationName
            # Oracle detection must come from P3a (career page HTML)
            # which is already the ground truth — return False here
            # so Serper never accepts Oracle results
            return False

        elif platform == "greenhouse":
            from jobs.ats.base import fetch_json
            # /v1/boards/{slug} returns {"name": "Stripe", ...}
            url  = f"https://boards-api.greenhouse.io/v1/boards/{slug}"
            data = fetch_json(url)
            if not data:
                return False
            board_name = data.get("name", "")
            return validate_company_match(board_name, company)                    if board_name else False

        elif platform == "lever":
            from jobs.ats.base import fetch_json
            from jobs.ats.patterns import validate_slug_for_company
            url  = f"https://api.lever.co/v0/postings/{slug}"
            data = fetch_json(url)
            if not isinstance(data, list):
                return False
            # Lever has no company name in API — use slug check
            return validate_slug_for_company(slug, company)

        elif platform == "ashby":
            from jobs.ats.base import fetch_json
            url  = f"https://api.ashbyhq.com/posting-api/job-board/{slug}"
            data = fetch_json(url)
            if not data:
                return False
            board_name = data.get("jobBoard", {}).get("name", "")
            return validate_company_match(board_name, company)                    if board_name else False

        elif platform == "icims":
            from jobs.ats.base import validate_company_match
            import requests as _req
            import re as _re

            # Step 1: slug itself encodes company name
            # careers-nyit → nyit, jobs-microsoft → microsoft
            # Use startswith to only strip leading prefix
            tenant = slug
            for prefix in ("careers-", "jobs-", "career-"):
                if tenant.startswith(prefix):
                    tenant = tenant[len(prefix):]
                    break  # only strip one prefix
            if validate_company_match(tenant, company):
                return True

            # Step 2: page title as fallback
            # "Job Opportunities | Human Resources | NYIT | ..."
            try:
                url = (
                    f"https://{slug}.icims.com/jobs/search"
                    f"?ss=1&in_iframe=1"
                )
                resp = _req.get(
                    url,
                    headers={"User-Agent": "Mozilla/5.0"},
                    timeout=10,
                )
                if resp.status_code == 200:
                    m = _re.search(
                        r"<title[^>]*>([^<]+)</title>",
                        resp.text, _re.IGNORECASE
                    )
                    if m:
                        return validate_company_match(m.group(1), company)
            except Exception as e:
                logger.debug("iCIMS title fetch failed for %s: %s",
                             slug, e)
            return False

        return True

    except Exception as e:
        logger.warning("API verification failed for %s/%s: %s",
                       platform, slug, e)
        return False


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

            # Step 1: Collect ALL unique slugs from all 10 results
            # into a hashmap — deduplicates by slug value
            # Also record whether Serper title confirmed company name
            # (used as fallback signal in Oracle verification)
            from jobs.ats.base import validate_company_match
            slug_map = {}  # slug_str → (result, title_verified)
            for item in items:
                url    = item.get("link", "")
                result = match_ats_pattern(url)
                if not result:
                    continue
                if result["platform"] != platform:
                    continue
                slug_key = result["slug"]
                if slug_key not in slug_map:
                    page_title   = item.get("title", "")
                    page_snippet = item.get("snippet", "")
                    combined     = f"{page_title} {page_snippet}".strip()
                    # has_text: Serper returned meaningful content
                    # prevents accepting title_verified on empty responses
                    has_text       = bool(combined)
                    title_verified = has_text and validate_company_match(
                        combined, company
                    )
                    slug_map[slug_key] = (result, title_verified, has_text)

            if not slug_map:
                continue

            logger.debug(
                "Serper found %d unique slugs for %s/%s: %s",
                len(slug_map), company, platform,
                list(slug_map.keys())
            )

            # Step 2: API-verify each slug — check company name
            # appears in the actual API response (ground truth)
            for slug_key, (result, title_verified, has_text) in slug_map.items():
                if _verify_slug_via_api(result, company,
                                        title_verified=title_verified,
                                        has_text=has_text):
                    print(f"   [SERPER] {company} -> {platform} "
                          f"(slug: {result['slug']})")
                    return result
                else:
                    logger.debug(
                        "Slug rejected by API: company=%s slug=%s "
                        "title_verified=%s",
                        company, slug_key, title_verified
                    )

        except requests.exceptions.Timeout:
            print(f"   [WARNING] Serper timeout for {company}/{platform}")
            continue
        except Exception as e:
            print(f"   [WARNING] Serper error for {company}/{platform}: {e}")
            continue

    return None