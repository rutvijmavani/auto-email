"""
jobs/prospective_form_sync.py — Syncs prospective company form responses.

Google Form fields (0-based column index):
  0: Timestamp
  1: Company Name
  2: Job URL
  3: Domain
  4: Career Page URL
  5: XML/Sitemap URL
  6: Listing Curl Command
  7: Detail Curl Command   ← new optional column
  8: Notes

Flow:
  1. Read rows from "Prospective" tab in Google Sheet
  2. Store raw curl strings verbatim in DB (before any parsing)
  3. For each row, resolve ATS platform + slug via priority order:
     a. Curl Command provided → parse via curl_parser, replay, detect
        → platform=custom, slug=enriched config
        → career_page_url stored in slug_info for dynamic session refresh
        → cookies discarded (re-acquired dynamically on every run)
     b. XML/Sitemap URL → platform=sitemap/google/apple/successfactors
     c. Career Page URL → scan for Phenom/TalentBrew/Avature fingerprints
     d. Job URL pattern match → known ATS
     e. Domain fallback scan
     f. All fail → store with platform=None for manual override
  4. Add/update prospective_companies with resolved platform + slug
  5. Delete processed row from sheet
"""

import os
import re
import json
from datetime import datetime
from urllib.parse import urlparse

import gspread
from google.oauth2.service_account import Credentials
from dotenv import load_dotenv

from logger import get_logger
from jobs.utils import (
    SKIP_HEADERS,
    REQUEST_TIMEOUT,
    is_json       as _is_json,
    is_valid_url  as _is_valid_url,
    domain_from_url as _domain_from_url,
)

logger = get_logger(__name__)

load_dotenv()

SHEET_ID         = os.getenv("GOOGLE_SHEET_ID")
CREDENTIALS_FILE = os.path.join(os.path.dirname(__file__), "..", "credentials.json")
SHEET_NAME       = "Prospective"

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

# Column indices (0-based)
# Timestamp | Company Name | Job URL | Career Page URL | Domain |
# XML/Sitemap URL | Notes | Listing Curl | Detail Curl
COL_TIMESTAMP   = 0
COL_COMPANY     = 1
COL_JOB_URL     = 2
COL_CAREER_PAGE = 3   # Career Page URL
COL_DOMAIN      = 4   # Domain
COL_XML_URL     = 5   # XML/Sitemap URL
COL_NOTES       = 6   # Notes
COL_CURL        = 7   # Listing curl command
COL_DETAIL_CURL = 8   # Detail curl command (optional)

# Hard ATS platforms — job URL gives no useful slug
HARD_ATS = set()

# Well-known single global feeds
GLOBAL_FEEDS = {
    "google.com/about/careers/applications/jobs/feed.xml": ("google",  "{}"),
    "jobs.apple.com/sitemap":                              ("apple",   "{}"),
}


# ─────────────────────────────────────────
# SHEET CONNECTION
# ─────────────────────────────────────────

def _get_sheet():
    logger.debug("Loading credentials from: %s", CREDENTIALS_FILE)
    try:
        creds = Credentials.from_service_account_file(
            CREDENTIALS_FILE, scopes=SCOPES
        )
    except FileNotFoundError:
        raise RuntimeError(
            f"Credentials file not found: {CREDENTIALS_FILE}"
        ) from None
    except (ValueError, Exception) as e:
        raise RuntimeError(
            f"Invalid credentials in {CREDENTIALS_FILE}: {e}"
        ) from e

    client = gspread.authorize(creds)
    sheet  = client.open_by_key(SHEET_ID)
    try:
        ws = sheet.worksheet(SHEET_NAME)
        logger.debug("Opened worksheet: %r", SHEET_NAME)
        return ws
    except gspread.WorksheetNotFound:
        logger.warning("Worksheet %r not found — creating it", SHEET_NAME)
        ws = sheet.add_worksheet(SHEET_NAME, rows=200, cols=9)
        ws.update("A1:I1", [[
            "Timestamp", "Company Name", "Job URL",
            "Career Page URL", "Domain", "XML/Sitemap URL",
            "Notes", "Listing Curl", "Detail Curl",
        ]])
        logger.info("Created '%s' tab with 9 columns", SHEET_NAME)
        print(f"[OK] Created '{SHEET_NAME}' tab in Google Sheet")
        return ws


# _is_valid_url, _domain_from_url imported from jobs.utils


# ─────────────────────────────────────────
# PRIORITY 0 — CURL COMMAND
# ─────────────────────────────────────────

def _resolve_from_curl(company, curl_str, career_page_url=None,
                       detail_curl_str=None, sample_job_url=None):
    """
    Parse listing + detail curls, replay listing, auto-detect structure.
    career_page_url stored in slug_info for dynamic session refresh.
    detail_curl_str parsed and stored under slug_info["detail"].

    Returns {"platform": "custom", "slug": json_string} or None.
    """
    if not curl_str or not curl_str.strip():
        return None

    logger.info("[sync] %r: curl command provided", company)
    print("       [Curl] Parsing listing curl...")

    try:
        from jobs.curl_parser import curl_to_slug_info, parse_detail_curl
        slug_info = curl_to_slug_info(
            curl_str, career_page_url=career_page_url
        )
    except Exception as e:
        logger.warning("[sync] %r: curl parse failed: %s", company, e)
        print(f"       [Curl] Parse failed: {e} — falling through")
        return None

    print(f"       [Curl] URL: {slug_info['url']}")
    print(f"       [Curl] Method: {slug_info['method']}, "
          f"Params: {len(slug_info.get('params', {}))}, "
          f"Headers: {len(slug_info.get('headers', {}))}")

    if career_page_url:
        print(f"       [Curl] Career page: {career_page_url} "
              f"(session refreshed dynamically)")
    else:
        print("       [Curl] No career page URL — cookies stored as-is")

    # Parse detail curl if provided
    if detail_curl_str and detail_curl_str.strip():
        print("       [Curl] Parsing detail curl...")
        try:
            detail_config = parse_detail_curl(detail_curl_str, slug_info)
            slug_info["detail"] = detail_config
            print(f"       [Curl] Detail URL template: "
                  f"{detail_config.get('url_template', '?')}")
            print(f"       [Curl] Detail job_id location: "
                  f"{detail_config.get('id_location', '?')} "
                  f"pattern={detail_config.get('id_pattern', '?')}")
        except Exception as e:
            logger.warning(
                "[sync] %r: detail curl parse failed: %s", company, e
            )
            print(f"       [Curl] Detail parse failed: {e} — "
                  f"detail fetch disabled for this company")
    
    if sample_job_url and sample_job_url.strip():
        slug_info["sample_job_url"] = sample_job_url.strip()
        print(f"       [Curl] Sample job URL: {sample_job_url.strip()}")

    # Replay listing request to verify
    print("       [Curl] Replaying listing request...")
    raw_bytes = _replay_request(slug_info, company)
    if raw_bytes is None:
        print("       [Curl] Replay failed — falling through")
        return None

    print(f"       [Curl] Got {len(raw_bytes)} bytes")

    # Detect structure
    print("       [Curl] Detecting response structure...")
    try:
        from jobs.ats.custom_career import _detect_structure, _extract_jobs_array
        slug_info["_company"] = company 
        detected = _detect_structure(raw_bytes, slug_info)
    except Exception as e:
        logger.warning("[sync] %r: structure detection error: %s", company, e)
        print(f"       [Curl] Detection error: {e} — falling through")

        # Flag diagnostic — unknown structure
        try:
            from db.custom_ats_diagnostics import flag_diagnostic_once, BLOCKED
            flag_diagnostic_once(
                company      = company,
                step         = "structure_detect",
                severity     = BLOCKED,
                pattern_hint = "detection_exception",
                raw_response = raw_bytes,
                notes        = (
                    f"Structure detection raised exception: {e}. "
                    f"Raw response stored for inspection."
                ),
            )
        except Exception:
            pass
        return None

    if not detected:
        print("       [Curl] No jobs found in response — "
              "ensure curl is from the job listing endpoint")
        logger.warning(
            "[sync] %r: curl response has no detectable jobs", company
        )

        # Flag diagnostic — new response pattern
        try:
            from db.custom_ats_diagnostics import flag_diagnostic_once, BLOCKED
            flag_diagnostic_once(
                company      = company,
                step         = "structure_detect",
                severity     = BLOCKED,
                pattern_hint = "no_array_found",
                raw_response = raw_bytes,
                notes        = (
                    f"_detect_structure() returned None. "
                    f"Raw response stored — inspect to identify new pattern. "
                    f"Ensure curl is from the job LISTING endpoint."
                ),
            )
        except Exception:
            pass
        return None

    # Merge detected config
    slug_info = {**slug_info, **detected}

    # Report
    jobs_arr  = _extract_jobs_array(raw_bytes, slug_info)
    job_count = len(jobs_arr) if jobs_arr else 0
    field_map = detected.get("field_map", {})
    pag       = detected.get("pagination", {})

    print(f"       [Curl] ✓ format={detected.get('format')} "
          f"jobs_on_page={job_count} "
          f"pagination={pag.get('type', 'none')}")
    print(f"       [Curl] Field map: "
          f"title={field_map.get('title')} "
          f"url={field_map.get('job_url')} "
          f"location={field_map.get('location')}")

    if jobs_arr:
        sample    = jobs_arr[0]
        title_val = sample.get(field_map.get("title", ""), "")
        print(f"       [Curl] Sample job: {str(title_val)[:60]}")

    if detail_curl_str and slug_info.get("detail"):
        print(f"       [Curl] Detail config ready — "
              f"will fetch descriptions for matched jobs")
    elif detail_curl_str:
        print("       [Curl] Detail curl provided but parse failed")
    else:
        print("       [Curl] No detail curl — "
              "jobs saved without description")

    logger.info(
        "[sync] %r: curl resolved — format=%s jobs=%d pagination=%s "
        "detail=%s",
        company, detected.get("format"), job_count,
        pag.get("type"),
        "yes" if slug_info.get("detail") else "no",
    )

    print(f"[CURL DEBUG] returning slug length={len(json.dumps(slug_info))}")

    return {
        "platform": "custom",
        "slug":     json.dumps(slug_info),
    }


def _replay_request(slug_info, company):
    """
    Replay the listing request using the warm session system.
    Returns raw response bytes or None.
    """
    import requests as req_lib
    from jobs.ats.custom_career import _warm_session, _build_legacy_session

    # Use warm session if career_page_url available
    career_page_url = slug_info.get("career_page_url")
    if career_page_url:
        session, strategy = _warm_session(slug_info, company)
        if session is None:
            logger.warning(
                "[sync] %r: career page warm failed — "
                "falling back to stored cookies", company
            )
            session = _build_legacy_session(slug_info)
    else:
        session = _build_legacy_session(slug_info)

    method = slug_info.get("method", "GET").upper()
    params = slug_info.get("params") or None
    body   = slug_info.get("body")

    # Handle GraphQL body rebuild
    if slug_info.get("graphql_config"):
        lsd = slug_info.get("_lsd", "")
        rev = slug_info.get("_rev", "")
        if not lsd:
            logger.warning(
                "[sync] %r: graphql strategy but _lsd not set — "
                "lsd extraction from career page may have failed. "
                "Falling back to original curl body.",
                company
            )
            # Debug: show what the career page HTML actually contains
            # so we can fix META_LSD_PATTERN
            career_page_url = slug_info.get("career_page_url")
            if career_page_url:
                import requests as _req
                try:
                    _r = _req.get(career_page_url,
                                  headers={"User-Agent": "Mozilla/5.0"},
                                  timeout=15)
                    _html = _r.text
                    import re as _re
                    # Show any line containing lsd/LSD
                    for _line in _html.splitlines():
                        if "lsd" in _line.lower() and len(_line) < 300:
                            print(f"       [LSD debug] {_line.strip()[:200]}")
                    # Also show raw context around LSD
                    _idx = _html.lower().find('"lsd"')
                    if _idx >= 0:
                        print(f"       [LSD context] {_html[_idx-10:_idx+60]!r}")
                    _idx2 = _html.find('["LSD"')
                    if _idx2 >= 0:
                        print(f"       [LSD array] {_html[_idx2:_idx2+60]!r}")
                except Exception as _e:
                    print(f"       [LSD debug] fetch failed: {_e}")
        else:
            from jobs.curl_parser import build_graphql_body
            body = build_graphql_body(
                slug_info["graphql_config"], lsd, rev
            )
            # Meta requires x-fb-lsd header to match lsd in body
            if lsd and hasattr(session, 'headers'):
                session.headers["x-fb-lsd"] = lsd
            logger.debug(
                "[sync] %r: rebuilt GraphQL body lsd=%s... doc_id=%s",
                company, lsd[:8] if lsd else None,
                slug_info["graphql_config"].get("doc_id")
            )
            print(f"       [GraphQL] lsd extracted: {lsd[:16] if lsd else None}...")
            print(f"       [GraphQL] doc_id: {slug_info['graphql_config'].get('doc_id')}")
            stable = slug_info["graphql_config"].get("stable_params", {})
            print(f"       [GraphQL] stable keys: {sorted(stable.keys())}")
            # Show first 400 chars of body to see what's being sent
            from urllib.parse import unquote_plus as _uqp
            body_preview = {k: v[:30] for part in body.split("&")[:8]
                           if "=" in part
                           for k, v in [part.split("=", 1)]}
            print(f"       [GraphQL] body fields: {body_preview}")

    try:
        if method == "POST":
            content_type = slug_info.get("headers", {}).get(
                "content-type", ""
            ).lower()
            if "form" in content_type or (
                body and not _is_json(body)
            ):
                resp = session.post(
                    slug_info["url"],
                    params=params,
                    data=body,
                    timeout=20,
                )
            else:
                resp = session.post(
                    slug_info["url"],
                    params=params,
                    json=json.loads(body) if body and _is_json(body) else None,
                    data=body if body and not _is_json(body) else None,
                    timeout=20,
                )
        else:
            resp = session.get(
                slug_info["url"],
                params=params,
                timeout=20,
            )

        if resp.status_code in (401, 403):
            logger.warning(
                "[sync] %r: auth error %d during replay",
                company, resp.status_code
            )
            print(f"       [Curl] Auth error {resp.status_code} — "
                  + ("career page session did not authenticate"
                     if career_page_url
                     else "stored cookies may be expired"))
            return None

        if not resp.ok:
            logger.warning(
                "[sync] %r: replay returned HTTP %d body=%s",
                company, resp.status_code,
                resp.content[:200]
            )
            print(f"       [Curl] HTTP {resp.status_code} — replay failed")
            print(f"       [Curl] Response: {resp.content[:300]}")
            return None

        return resp.content

    except Exception as e:
        logger.warning("[sync] %r: replay exception: %s", company, e)
        print(f"       [Curl] Request error: {e}")
        return None


# ─────────────────────────────────────────
# MAIN ATS RESOLVER
# ─────────────────────────────────────────

def _resolve_ats(company, job_url, career_page_url, domain, xml_url,
                 curl_str=None, detail_curl_str=None):
    """Resolve ATS platform + slug via priority waterfall."""
    from jobs.ats.patterns import match_ats_pattern

    # Priority 0: Curl command
    if curl_str and curl_str.strip():
        result = _resolve_from_curl(
            company, curl_str,
            career_page_url = career_page_url or None,
            detail_curl_str = detail_curl_str or None,
            sample_job_url  = job_url or None,
        )
        print(f"[ATS DEBUG] _resolve_from_curl returned: {result is not None}")
        if result:
            logger.info("[sync] %r: ATS from curl — custom", company)
            return result

    # Priority 1: XML/Sitemap URL
    if xml_url and _is_valid_url(xml_url):
        result = _resolve_from_xml_url(xml_url, company)
        if result:
            logger.info("[sync] %r: ATS from XML — %s",
                        company, result["platform"])
            print(f"       [ATS via XML] {result['platform']} / "
                  f"{str(result['slug'])[:50]}")
            return result

    # Priority 2: Career page fingerprint
    scan_url = career_page_url if _is_valid_url(career_page_url or "") else None
    if scan_url:
        result = _scan_career_page(company, scan_url)
        if result and result.get("platform"):
            logger.info("[sync] %r: ATS from career page — %s",
                        company, result["platform"])
            print(f"       [ATS via career page] {result['platform']} / "
                  f"{str(result['slug'])[:50]}")
            return result

    # Priority 3: Job URL pattern
    if job_url and _is_valid_url(job_url):
        ats_result = match_ats_pattern(job_url)
        if ats_result:
            platform = ats_result["platform"]
            if platform not in HARD_ATS:
                logger.info("[sync] %r: ATS from job URL — %s",
                            company, platform)
                print(f"       [ATS] {platform} / "
                      f"{str(ats_result['slug'])[:50]}")
                return ats_result
            else:
                print(f"       [INFO] {platform} detected — "
                      f"trying career page instead")

    # Priority 4: Domain fallback
    if domain:
        from jobs.career_page import detect_via_career_page
        result = detect_via_career_page(company, domain)
        if result and result.get("platform"):
            logger.info("[sync] %r: ATS from domain — %s",
                        company, result["platform"])
            print(f"       [ATS via domain] {result['platform']} / "
                  f"{str(result['slug'])[:50]}")
            return result

    # Priority 5: Nothing found
    logger.info("[sync] %r: no ATS — needs manual override", company)
    print("       [INFO] No ATS detected — stored for manual override")
    return None


# ─────────────────────────────────────────
# EXISTING HELPERS (unchanged)
# ─────────────────────────────────────────

def _resolve_from_xml_url(xml_url, company):
    from jobs.ats.patterns import match_ats_pattern

    for pattern, (platform, slug) in GLOBAL_FEEDS.items():
        if pattern in xml_url:
            return {"platform": platform, "slug": slug}

    sf_match = re.search(
        r"career(\d+)\.successfactors\.(com|eu)/career\?company=([^&\s]+)",
        xml_url, re.IGNORECASE
    )
    if sf_match:
        slug = json.dumps({
            "slug":   sf_match.group(3),
            "dc":     sf_match.group(1),
            "region": sf_match.group(2),
        })
        return {"platform": "successfactors", "slug": slug}

    result = match_ats_pattern(xml_url)
    if result:
        return result

    return {"platform": "sitemap", "slug": json.dumps({"url": xml_url})}


def _scan_career_page(company, career_page_url):
    import requests
    from jobs.ats.patterns import match_ats_pattern

    HEADERS = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/121.0.0.0 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml,*/*;q=0.8",
    }

    try:
        resp = requests.get(
            career_page_url, headers=HEADERS,
            timeout=12, allow_redirects=True
        )
    except Exception as e:
        logger.debug("[sync] career page fetch failed for %r: %s",
                     career_page_url, e)
        return None

    if resp.status_code != 200:
        return None

    final_url = resp.url
    html      = resp.text

    if final_url != career_page_url:
        result = match_ats_pattern(final_url)
        if result and result.get("platform") not in HARD_ATS:
            return result

    if ("cdn.phenompeople.com" in html or
            ("use-widget" in html and "ph-" in html)):
        slug = _extract_phenom_slug(html, career_page_url)
        if slug:
            return {"platform": "phenom", "slug": slug}

    if "tbcdn.talentbrew.com" in html or "radancy" in html.lower():
        slug = _extract_talentbrew_slug(html, career_page_url)
        if slug:
            return {"platform": "talentbrew", "slug": slug}

    if "avature.net" in html:
        slug = _extract_avature_slug(html, career_page_url)
        if slug:
            return {"platform": "avature", "slug": slug}
    
    # Jibe (iCIMS Jibe) — careers powered by app.jibecdn.com
    if "app.jibecdn.com" in html or "jibe-widget" in html:
        parsed = urlparse(career_page_url)
        slug   = parsed.netloc.lower()   # e.g. "careers.rivian.com"
        if slug:
            return {"platform": "jibe", "slug": slug}

    # Eightfold.ai — cdn.eightfold.ai or {slug}.eightfold.ai iframe
    if "eightfold.ai" in html:
        # Try to extract slug from script/iframe src, skipping "cdn"
        matches = re.findall(r'([a-z0-9][a-z0-9\-]*)\.eightfold\.ai', html, re.IGNORECASE)
        slug = None
        for match in matches:
            if match.lower() != "cdn":
                slug = match.lower()
                break
        if not slug and matches:
            # Fallback: use first match even if it's "cdn"
            slug = matches[0].lower()
        if slug:
            domain = _domain_from_url(career_page_url) or ""
            return {
                "platform": "eightfold",
                "slug":     json.dumps({"slug": slug, "domain": domain}),
            }

    # Taleo — taleo.net in script src or form action
    if ".taleo.net" in html:
        m = re.search(r'([a-z0-9][a-z0-9\-]*)\.taleo\.net', html, re.IGNORECASE)
        if m:
            company_slug = m.group(1).lower()
            return {
                "platform": "taleo",
                "slug":     json.dumps({
                    "company":   company_slug,
                    "portal_id": "",
                    "section":   "ex",
                }),
            }

    from jobs.career_page import _scan_html
    result = _scan_html(html, "")
    if result and result.get("platform") not in HARD_ATS:
        return result

    return None


def _extract_phenom_slug(html, career_page_url):
    parsed  = urlparse(career_page_url)
    base    = f"{parsed.scheme}://{parsed.netloc}"
    path    = parsed.path.strip("/")
    sitemap = f"{path}/sitemap.xml" if path else "sitemap.xml"
    return json.dumps({"base": base, "path": path, "sitemap": sitemap})


def _extract_talentbrew_slug(html, career_page_url):
    parsed = urlparse(career_page_url)
    base   = f"{parsed.scheme}://{parsed.netloc}"
    m = re.search(
        r'<meta[^>]+name=["\']site-tenant-id["\'][^>]*content=["\'](\d+)["\']',
        html, re.IGNORECASE
    )
    if not m:
        m = re.search(
            r'<meta[^>]+content=["\'](\d+)["\'][^>]*name=["\']site-tenant-id["\']',
            html, re.IGNORECASE
        )
    tenant_id = m.group(1) if m else ""
    return json.dumps({"base": base, "tenant_id": tenant_id})


def _extract_avature_slug(html, career_page_url):
    parsed = urlparse(career_page_url)
    base   = f"{parsed.scheme}://{parsed.netloc}"
    path   = (parsed.path.strip("/").split("/")[0]
              if parsed.path.strip("/") else "careers")
    return json.dumps({"base": base, "path": path})


# ─────────────────────────────────────────
# MAIN RUN
# ─────────────────────────────────────────

def run():
    """Sync prospective companies form responses to pipeline DB."""
    logger.info("════════════════════════════════════════")
    logger.info("--sync-forms (prospective) starting")

    if not SHEET_ID:
        logger.error("GOOGLE_SHEET_ID not set")
        print("[ERROR] GOOGLE_SHEET_ID not set in .env")
        return

    if not os.path.exists(CREDENTIALS_FILE):
        print(f"[ERROR] credentials.json not found at {CREDENTIALS_FILE}")
        return

    print("[INFO] Syncing prospective companies form...")
    try:
        worksheet = _get_sheet()
    except Exception as e:
        logger.error("Could not connect to Google Sheets: %s", e)
        print(f"[ERROR] Could not connect to Google Sheets: {e}")
        return

    all_rows = worksheet.get_all_values()
    if len(all_rows) <= 1:
        print("[INFO] No new prospective companies to process.")
        return

    data_rows = all_rows[1:]
    logger.info("Found %d row(s)", len(data_rows))
    print(f"[INFO] Found {len(data_rows)} prospective company entries.\n")

    from db.connection import get_conn

    imported       = 0
    skipped        = 0
    rows_to_delete = []

    conn = get_conn()
    try:
        for i, row in enumerate(data_rows):
            sheet_row = i + 2

            # Pad to 9 columns (8 original + detail curl)
            while len(row) < 9:
                row.append("")

            company      = row[COL_COMPANY].strip()
            job_url      = row[COL_JOB_URL].strip()
            career_page  = row[COL_CAREER_PAGE].strip()
            domain_input = row[COL_DOMAIN].strip()
            xml_url      = row[COL_XML_URL].strip()
            curl_str     = row[COL_CURL].strip()
            detail_curl  = row[COL_DETAIL_CURL].strip()

            logger.info("── Row %d: company=%r curl=%s detail_curl=%s",
                        sheet_row, company,
                        "yes" if curl_str else "no",
                        "yes" if detail_curl else "no")
            print(f"  [{i+1}] {company}"
                  + (" [listing curl]" if curl_str else "")
                  + (" [detail curl]" if detail_curl else ""))

            if not company:
                logger.warning("Row %d: missing company name", sheet_row)
                print("       [SKIP] Missing company name")
                skipped += 1
                rows_to_delete.append(sheet_row)
                continue

            # Resolve domain
            domain = None
            if domain_input:
                normalized = domain_input.lower().strip()
                for prefix in ("https://", "http://"):
                    if normalized.startswith(prefix):
                        normalized = normalized[len(prefix):]
                domain = normalized.rstrip("/")
            elif job_url and _is_valid_url(job_url):
                domain = _domain_from_url(job_url)
            elif career_page and _is_valid_url(career_page):
                domain = _domain_from_url(career_page)

            # ── Store raw curls BEFORE any parsing ───────────────
            # This ensures we always have the original curl for
            # debugging even if parsing fails or patterns change.
            if curl_str or detail_curl:
                try:
                    # Upsert company row first if it doesn't exist
                    # so we have a row to update
                    existing_check = conn.execute(
                        "SELECT id FROM prospective_companies "
                        "WHERE company = ?", (company,)
                    ).fetchone()

                    if not existing_check:
                        conn.execute(
                            "INSERT OR IGNORE INTO prospective_companies "
                            "(company, domain, priority, status, created_at) "
                            "VALUES (?, ?, 2, 'active', ?)",
                            (company, domain, datetime.utcnow())
                        )

                    update_parts = []
                    update_vals  = []
                    if curl_str:
                        update_parts.append("listing_curl_raw = ?")
                        update_vals.append(curl_str)
                    if detail_curl:
                        update_parts.append("detail_curl_raw = ?")
                        update_vals.append(detail_curl)

                    if update_parts:
                        conn.execute(
                            f"UPDATE prospective_companies "
                            f"SET {', '.join(update_parts)} "
                            f"WHERE company = ?",
                            update_vals + [company]
                        )
                    conn.commit()
                    logger.debug(
                        "Row %d: stored raw curl(s) for %r",
                        sheet_row, company
                    )
                except Exception as e:
                    logger.warning(
                        "Row %d: could not store raw curls for %r: %s",
                        sheet_row, company, e
                    )

            # Resolve ATS
            ats_result = _resolve_ats(
                company, job_url, career_page, domain, xml_url,
                curl_str       = curl_str,
                detail_curl_str = detail_curl,
            )
            platform = ats_result["platform"] if ats_result else None
            slug     = ats_result["slug"]     if ats_result else None

            # Write to DB
            try:
                existing = conn.execute(
                    "SELECT id, status, ats_platform "
                    "FROM prospective_companies WHERE company = ?",
                    (company,)
                ).fetchone()

                if existing:
                    # Update ATS if:
                    # - new detection found something
                    # - existing platform is null/unknown/custom
                    needs_ats_update = (
                        ats_result and (
                            existing["ats_platform"] is None or
                            existing["ats_platform"] in
                            ("unknown", "custom")
                        )
                    )

                    if needs_ats_update:
                        conn.execute(
                            "UPDATE prospective_companies "
                            "SET status='active', "
                            "ats_platform=?, ats_slug=?, "
                            "ats_detected_at=?, "
                            "domain = CASE WHEN domain IS NULL "
                            "  OR domain = '' THEN ? ELSE domain END "
                            "WHERE company=?",
                            (platform, slug, datetime.utcnow(),
                             domain, company)
                        )
                    else:
                        conn.execute(
                            "UPDATE prospective_companies "
                            "SET status='active', "
                            "domain = CASE WHEN domain IS NULL "
                            "  OR domain = '' THEN ? ELSE domain END "
                            "WHERE company=?",
                            (domain, company)
                        )
                    conn.commit()
                    print("       [OK] Updated existing company")

                else:
                    conn.execute(
                        "INSERT INTO prospective_companies "
                        "(company, domain, ats_platform, ats_slug, "
                        "ats_detected_at, priority, status, created_at) "
                        "VALUES (?, ?, ?, ?, ?, 2, 'active', ?)",
                        (
                            company, domain, platform, slug,
                            datetime.utcnow() if ats_result else None,
                            datetime.utcnow(),
                        )
                    )
                    conn.commit()
                    print("       [OK] Added to pipeline")

            except Exception as e:
                logger.error(
                    "Row %d: DB error for %r: %s",
                    sheet_row, company, e, exc_info=True
                )
                raise

            imported += 1
            rows_to_delete.append(sheet_row)

    finally:
        conn.close()

    # Delete processed rows (reverse order preserves indices)
    if rows_to_delete:
        print(f"\n[INFO] Cleaning {len(rows_to_delete)} row(s) from sheet...")
        for row_index in sorted(rows_to_delete, reverse=True):
            try:
                worksheet.delete_rows(row_index)
            except Exception as e:
                logger.error(
                    "Could not delete row %d: %s", row_index, e
                )
                print(f"   [WARNING] Could not delete row {row_index}: {e}")

    logger.info("Sync complete — imported=%d skipped=%d",
                imported, skipped)
    print(f"\n{'='*55}")
    print(f"[OK] Imported: {imported} | Skipped: {skipped}")
    logger.info("════ --sync-forms (prospective) finished ════")