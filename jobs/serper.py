import logging
import re

from logger import get_logger
logger = get_logger(__name__)

# jobs/serper.py — Phase 3b: Serper.dev API for Workday + Oracle detection
#
# Only used when Phase 1 (sitemap), Phase 2 (API probe),
# and Phase 3a (HTML redirect) all fail.
#
# Searches: "{company} site:myworkdayjobs.com"
#
# Uses 1 credit per company.
# 2500 free credits on signup → covers 2500 companies.
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
# career_page.detect_via_career_page() which follows company.com/careers
# and scans HTML for oraclecloud URLs.
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

    Returns:
        (title, source) tuple where source is one of:
          "html"   — urlWID extracted from live HTML (definitive)
          "path"   — path field from slug_info (fallback)
          "slug"   — slug field from slug_info (last resort)
          None     — no title available at all

    Callers MUST check source before treating the title as definitive.
    Only source=="html" should cause early rejection on mismatch.
    source=="path" and source=="slug" should only be used as
    supplementary signals (Layer 2/3), never as Layer 1 grounds for
    rejection.
    """
    import requests
    from jobs.ats.workday import _build_url

    base_url = _build_url(slug_info).replace("/jobs", "")
    logger.debug("Fetching Workday site title: %s", base_url)

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
                title = match.group(1).strip()
                logger.debug("urlWID found: %r (source=html)", title)
                return title, "html"
            logger.debug("urlWID not found in Workday HTML for %s",
                         slug_info.get("slug", "?"))
        else:
            logger.debug("Workday site fetch returned %s for %s",
                         resp.status_code, slug_info.get("slug", "?"))
    except Exception as e:
        logger.debug("Workday site title fetch failed for %s: %s",
                     slug_info.get("slug", "?"), e)

    # Fallback: path often contains company name.
    # These are NOT returned as "html" — callers treat them as Layer 2/3
    # signals and must not use them for definitive Layer 1 rejection.
    path = slug_info.get("path", "")
    if path and path not in ("careers", "jobs", "External", "Careers"):
        logger.debug("Using path fallback: %r (source=path)", path)
        return path, "path"

    # Last resort: slug itself
    slug_val = slug_info.get("slug", "")
    if slug_val:
        logger.debug("Using slug fallback: %r (source=slug)", slug_val)
        return slug_val, "slug"

    return None, None


def _verify_slug_via_api(result, company, title_verified=False, has_text=False):
    """
    Verify detected slug by calling the ATS API and confirming
    the company name appears in the response.

    Workday:    fetches career site title ("Wells Fargo Jobs") → match
    Greenhouse: fetches board name ("Stripe") → match
    Ashby:      fetches jobBoard.name → match
    Oracle:     always returns False — Oracle not detected via Serper
    Lever:      falls back to slug validation (no company name in API)
    """
    from jobs.ats.base import validate_company_match
    platform = result["platform"]
    slug     = result["slug"]

    logger.debug("Verifying slug: company=%r platform=%s slug=%s "
                 "title_verified=%s has_text=%s",
                 company, platform, slug, title_verified, has_text)

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

            # Layer 1: urlWID from career site HTML (source == "html")
            # e.g. "ASMLExternalCareerSite" → contains "asml" ✓
            #
            # IMPORTANT: only treat as definitive when source == "html".
            # Path/slug fallbacks from _get_workday_site_title are NOT
            # definitive here — they feed Layer 2 below instead, so we
            # never prematurely reject based on a non-HTML fallback value.
            site_title, title_source = _get_workday_site_title(slug_info)
            if title_source == "html":
                site_title_specific = (
                    site_title and site_title.lower() not in GENERIC
                )
                if site_title_specific:
                    # HTML-derived and non-generic: definitive match/reject
                    match_result = _match_compact_identifier(site_title, company)
                    logger.debug("Layer 1 (html urlWID): site_title=%r match=%s",
                                 site_title, match_result)
                    return match_result
                # HTML-derived but generic (e.g. urlWID == "External"):
                # fall through to Layer 2 — don't reject yet
                logger.debug("Layer 1: urlWID=%r is generic — falling through to Layer 2",
                             site_title)

            # Layer 2: path from Serper URL slug_info
            # e.g. "qualcomm_careers" → contains "qualcomm" ✓
            # Only use slug_info["path"] directly here (not the fallback
            # path returned by _get_workday_site_title) to avoid double-
            # counting the same value as both Layer 1 and Layer 2.
            path = slug_info.get("path", "")
            path_specific = path and path.lower() not in GENERIC
            if path_specific:
                match_result = _match_compact_identifier(path, company)
                logger.debug("Layer 2 (path): path=%r match=%s", path, match_result)
                return match_result

            # Layer 3: title_verified fallback
            # Only reached when BOTH urlWID (html) and path are generic
            # e.g. ms.wd5/External — no specific identifier available
            # Only trust if Serper returned meaningful title/snippet text
            if title_verified and has_text:
                logger.debug("Layer 3 (title_verified fallback): accepted")
                return True

            logger.debug("All layers exhausted — rejecting slug=%s for %r", slug, company)
            return False

        elif platform == "oracle_hcm":
            # Oracle HCM is never detected via Serper (removed in Session 4).
            # site:fa.oraclecloud.com returns JPMorgan results for any company.
            # Oracle detection comes exclusively from P3a (career_page.py HTML
            # redirect scan for oraclecloud URLs). Always reject here.
            logger.debug("Oracle HCM rejected — not detected via Serper")
            return False

        elif platform == "greenhouse":
            from jobs.ats.base import fetch_json
            # /v1/boards/{slug} returns {"name": "Stripe", ...}
            url  = f"https://boards-api.greenhouse.io/v1/boards/{slug}"
            data = fetch_json(url)
            if not data:
                return False
            board_name = data.get("name", "")
            match_result = validate_company_match(board_name, company) \
                           if board_name else False
            logger.debug("Greenhouse verify: board_name=%r match=%s", board_name, match_result)
            return match_result

        elif platform == "lever":
            from jobs.ats.base import fetch_json
            from jobs.ats.patterns import validate_slug_for_company
            url  = f"https://api.lever.co/v0/postings/{slug}"
            data = fetch_json(url)
            if not isinstance(data, list):
                return False
            # Lever has no company name in API — use slug check
            match_result = validate_slug_for_company(slug, company)
            logger.debug("Lever verify: slug=%r match=%s", slug, match_result)
            return match_result

        elif platform == "ashby":
            from jobs.ats.base import fetch_json
            url  = f"https://api.ashbyhq.com/posting-api/job-board/{slug}"
            data = fetch_json(url)
            if not data:
                return False
            board_name = data.get("jobBoard", {}).get("name", "")
            match_result = validate_company_match(board_name, company) \
                           if board_name else False
            logger.debug("Ashby verify: board_name=%r match=%s", board_name, match_result)
            return match_result

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
                logger.debug("iCIMS verify: tenant=%r matched company=%r", tenant, company)
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
                        match_result = validate_company_match(m.group(1), company)
                        logger.debug("iCIMS verify (page title): title=%r match=%s",
                                     m.group(1), match_result)
                        return match_result
            except Exception as e:
                logger.debug("iCIMS title fetch failed for %s: %s", slug, e)
            return False

        return True

    except Exception as e:
        logger.warning("API verification failed for %s/%s: %s",
                       platform, slug, e)
        return False


def detect_via_serper(company):
    """
    Phase 3b: Search Google via Serper API for Workday ATS.

    Args:
        company: company name (e.g. "Capital One")

    Returns:
        {platform, slug}  — ATS found ✓
        None              — not found
        SERPER_EXHAUSTED  — credits exhausted
    """
    logger.debug("[P3b] detect_via_serper: company=%r", company)

    if not SERPER_API_KEY:
        logger.error("SERPER_API_KEY not set in .env")
        print("   [ERROR] SERPER_API_KEY not set in .env")
        return None

    for platform, site_filter in SERPER_SEARCHES:

        # Check credits before each search
        if not has_serper_credits(needed=1):
            logger.warning("[P3b] Serper credits exhausted for %r", company)
            print(f"   [WARNING] Serper credits exhausted")
            return SERPER_EXHAUSTED

        query = f"{company} {site_filter}"
        logger.debug("[P3b] Serper query: %r", query)

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
                logger.warning("[P3b] Serper rate limited — retry later")
                print("   [WARNING] Serper rate limited — retry later")
                # 429 = transient, not exhausted — don't charge credit
                continue

            if resp.status_code in (401, 403):
                logger.error("[P3b] Serper auth error %d — check SERPER_API_KEY",
                             resp.status_code)
                print(f"   [WARNING] Serper auth error {resp.status_code} "
                      f"— check SERPER_API_KEY")
                return None  # no credit charged on auth failure

            if resp.status_code != 200:
                logger.warning("[P3b] Serper error %d for %r/%s",
                               resp.status_code, company, platform)
                print(f"   [WARNING] Serper error {resp.status_code} "
                      f"for {company}/{platform}")
                continue  # no credit charged on error

            # Only charge credit on successful response
            increment_serper_credits(1)

            items = resp.json().get("organic", [])
            logger.debug("[P3b] Serper returned %d organic results for %r",
                         len(items), company)

            if not items:
                continue

            # Step 1: Collect ALL unique slugs from all 10 results
            # into a hashmap keyed by slug value — deduplicates across hits.
            #
            # For each slug we track:
            #   title_verified: Serper title/snippet contained company name
            #   has_text:       Serper returned any title/snippet text at all
            #
            # When the same slug appears in multiple results we keep the
            # STRONGEST signal: prefer title_verified=True over False,
            # then prefer has_text=True over False.  This ensures a strong
            # confirmation from a later hit is never discarded in favour of
            # a weaker first hit.
            from jobs.ats.base import validate_company_match
            slug_map = {}  # slug_str → (result, title_verified, has_text)

            for item in items:
                url    = item.get("link", "")
                result = match_ats_pattern(url)
                if not result:
                    continue
                if result["platform"] != platform:
                    continue

                slug_key     = result["slug"]
                page_title   = item.get("title", "")
                page_snippet = item.get("snippet", "")
                combined     = f"{page_title} {page_snippet}".strip()

                has_text       = bool(combined)
                title_verified = has_text and validate_company_match(
                    combined, company
                )

                if slug_key not in slug_map:
                    # First time seeing this slug — store unconditionally
                    slug_map[slug_key] = (result, title_verified, has_text)
                else:
                    # Slug already seen — keep the stronger signal.
                    # Strength order: title_verified > has_text > neither.
                    # Only replace when the new entry is strictly stronger
                    # than the existing one to avoid losing good data.
                    existing_result, existing_tv, existing_ht = (
                        slug_map[slug_key]
                    )
                    new_is_stronger = (
                        (title_verified and not existing_tv) or
                        (has_text and not existing_ht and not existing_tv)
                    )
                    if new_is_stronger:
                        slug_map[slug_key] = (result, title_verified, has_text)

            if not slug_map:
                logger.debug("[P3b] No valid slugs found for %r/%s", company, platform)
                continue

            logger.debug(
                "[P3b] %d unique slug(s) found for %r/%s: %s",
                len(slug_map), company, platform,
                list(slug_map.keys())
            )

            # Step 2: API-verify each slug — check company name
            # appears in the actual API response (ground truth)
            for slug_key, (result, title_verified, has_text) in slug_map.items():
                if _verify_slug_via_api(result, company,
                                        title_verified=title_verified,
                                        has_text=has_text):
                    logger.info("[P3b HIT] company=%r → platform=%s slug=%s",
                                company, platform, result["slug"])
                    print(f"   [SERPER] {company} -> {platform} "
                          f"(slug: {result['slug']})")
                    return result
                else:
                    logger.debug(
                        "[P3b] Slug rejected by API: company=%r slug=%s "
                        "title_verified=%s",
                        company, slug_key, title_verified
                    )

        except requests.exceptions.Timeout:
            logger.warning("[P3b] Serper timeout for %r/%s", company, platform)
            print(f"   [WARNING] Serper timeout for {company}/{platform}")
            continue
        except Exception as e:
            logger.error("[P3b] Serper error for %r/%s: %s", company, platform, e)
            print(f"   [WARNING] Serper error for {company}/{platform}: {e}")
            continue

    logger.debug("[P3b MISS] No Workday found for %r via Serper", company)
    return None