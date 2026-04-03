"""
jobs/prospective_form_sync.py — Syncs prospective company form responses.

Second Google Form (separate from job applications form).
Form fields:
  Timestamp | Company Name | Job URL | Career Page URL | Domain | XML/Sitemap URL | Notes

Flow:
  1. Read rows from "Prospective" tab in Google Sheet
  2. For each row, resolve ATS platform + slug via priority order:
     a. XML/Sitemap URL provided → platform=sitemap/google/apple/successfactors
     b. Job URL matches known ATS pattern (Greenhouse/Lever/Workday/SF/etc.)
     c. Job URL is hard ATS (Avature/Eightfold/Taleo/SF) OR no match →
        scan Career Page URL for Phenom/TalentBrew/Avature fingerprints
     d. Career page scan fails → scan domain/careers
     e. All fail → store with platform=None for manual override later
  3. Add/update prospective_companies with resolved platform + slug
  4. Delete processed row from sheet
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
# Timestamp | Company Name | Job URL | Career Page URL | Domain | XML/Sitemap URL | Notes
COL_TIMESTAMP   = 0
COL_COMPANY     = 1
COL_JOB_URL     = 2
COL_DOMAIN      = 3
COL_CAREER_PAGE = 4
COL_XML_URL     = 5
COL_NOTES       = 6

# Hard ATS platforms — job URL gives no useful slug, try career page instead
# Note: Avature removed from this list as it can be detected via career page scan
HARD_ATS = {"eightfold", "taleo"}

# Well-known single global feeds — platform set directly from XML URL
GLOBAL_FEEDS = {
    "google.com/about/careers/applications/jobs/feed.xml": ("google",  "{}"),
    "jobs.apple.com/sitemap":                              ("apple",   "{}"),
}


def _get_sheet():
    logger.debug("Loading credentials from: %s", CREDENTIALS_FILE)
    try:
        creds = Credentials.from_service_account_file(
            CREDENTIALS_FILE, scopes=SCOPES
        )
    except FileNotFoundError:
        logger.error("Credentials file not found: %s", CREDENTIALS_FILE)
        raise RuntimeError(f"Credentials file not found: {CREDENTIALS_FILE}") from None
    except (ValueError, Exception) as e:
        logger.error("Invalid credentials in %s: %s", CREDENTIALS_FILE, e)
        raise RuntimeError(f"Invalid credentials in {CREDENTIALS_FILE}: {e}") from e

    client = gspread.authorize(creds)
    sheet  = client.open_by_key(SHEET_ID)
    try:
        ws = sheet.worksheet(SHEET_NAME)
        logger.debug("Opened worksheet: %r", SHEET_NAME)
        return ws
    except gspread.WorksheetNotFound:
        logger.warning("Worksheet %r not found — creating it", SHEET_NAME)
        ws = sheet.add_worksheet(SHEET_NAME, rows=200, cols=7)
        ws.update("A1:G1", [[
            "Timestamp", "Company Name", "Job URL",
            "Career Page URL", "Domain", "XML/Sitemap URL", "Notes"
        ]])
        logger.info("Created '%s' tab with 7 columns", SHEET_NAME)
        print(f"[OK] Created '{SHEET_NAME}' tab in Google Sheet")
        return ws


def _is_valid_url(url):
    return bool(re.match(r"https?://", url.strip())) if url else False


def _domain_from_url(url):
    """Extract registrable domain from any URL. Returns e.g. 'starbucks.com'."""
    if not url:
        return None
    try:
        import tldextract
        extracted = tldextract.extract(urlparse(url).hostname or "")
        return extracted.registered_domain or urlparse(url).hostname
    except Exception:
        return urlparse(url).hostname


def _resolve_ats(company, job_url, career_page_url, domain, xml_url):
    """
    Resolve ATS platform + slug for a company using priority order.

    Priority:
      1. XML/Sitemap URL provided → platform=google/apple/successfactors/sitemap
      2. Job URL matches known ATS pattern directly
      3. Job URL is hard ATS → scan career page URL for Phenom/TalentBrew/etc.
      4. No job URL match → scan career page URL
      5. No career page URL → scan domain/careers as fallback
      6. All fail → return None (manual override needed)

    Returns:
      {"platform": str, "slug": str} or None
    """
    from jobs.ats.patterns import match_ats_pattern

    # ── Step 1: XML/Sitemap URL ────────────────────────────────────────────
    if xml_url and _is_valid_url(xml_url):
        result = _resolve_from_xml_url(xml_url, company)
        if result:
            logger.info("[sync] %r: ATS from XML URL — %s", company, result["platform"])
            print(f"       [ATS via XML] {result['platform']} / {str(result['slug'])[:50]}")
            return result

    # ── Step 2: Job URL pattern match ─────────────────────────────────────
    ats_result = None
    if job_url and _is_valid_url(job_url):
        ats_result = match_ats_pattern(job_url)
        if ats_result:
            platform = ats_result["platform"]
            # Hard ATS — slug from URL is useless for fetching jobs
            # Fall through to career page scan instead
            if platform not in HARD_ATS:
                logger.info("[sync] %r: ATS from job URL — %s", company, platform)
                print(f"       [ATS] {platform} / {str(ats_result['slug'])[:50]}")
                return ats_result
            else:
                logger.info("[sync] %r: job URL is hard ATS (%s) — trying career page",
                            company, platform)
                print(f"       [INFO] {platform} detected — trying career page instead")

    # ── Step 3 + 4: Career page fingerprint scan ──────────────────────────
    scan_url = career_page_url if (_is_valid_url(career_page_url or "")) else None

    if scan_url:
        result = _scan_career_page(company, scan_url)
        # Accept results except for hard ATS platforms (excluding avature which can be scanned)
        if result and result.get("platform") not in {"eightfold", "taleo", "successfactors"}:
            logger.info("[sync] %r: ATS from career page — %s", company, result["platform"])
            print(f"       [ATS via career page] {result['platform']} / "
                  f"{str(result['slug'])[:50]}")
            return result

    # ── Step 5: Domain fallback scan ──────────────────────────────────────
    if domain:
        from jobs.career_page import detect_via_career_page
        result = detect_via_career_page(company, domain)
        # Accept results except for hard ATS platforms (excluding avature which can be scanned)
        if result and result.get("platform") not in {"eightfold", "taleo", "successfactors"}:
            logger.info("[sync] %r: ATS from domain scan — %s", company, result["platform"])
            print(f"       [ATS via domain] {result['platform']} / "
                  f"{str(result['slug'])[:50]}")
            return result

    # ── Step 6: Nothing found ─────────────────────────────────────────────
    logger.info("[sync] %r: no ATS detected — will need manual override", company)
    print("       [INFO] No ATS detected — stored for manual override")
    return None


def _resolve_from_xml_url(xml_url, company):
    """
    Resolve platform + slug from an XML feed or sitemap URL.

    Handles:
      - Google global feed
      - Apple sitemap
      - SuccessFactors XML feed (dc/region/slug in URL)
      - Generic sitemap → platform=sitemap, slug={"url": xml_url}
    """
    from jobs.ats.patterns import match_ats_pattern

    # Check global feeds first
    for pattern, (platform, slug) in GLOBAL_FEEDS.items():
        if pattern in xml_url:
            return {"platform": platform, "slug": slug}

    # SuccessFactors XML feed URL contains dc/region/slug
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

    # Try patterns.py — may match Phenom/TalentBrew/Avature sitemap URLs
    result = match_ats_pattern(xml_url)
    if result:
        return result

    # Generic sitemap — store URL as slug_info for sitemap.py
    slug = json.dumps({"url": xml_url})
    return {"platform": "sitemap", "slug": slug}


def _scan_career_page(company, career_page_url):
    """
    Fetch career page HTML and fingerprint for Phenom, TalentBrew, Avature, etc.
    Returns {platform, slug} or None.
    """
    import requests
    from jobs.ats.patterns import match_ats_pattern

    HEADERS = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    }

    try:
        resp = requests.get(career_page_url, headers=HEADERS,
                            timeout=12, allow_redirects=True)
    except Exception as e:
        logger.debug("[sync] career page fetch failed for %r: %s", career_page_url, e)
        return None

    if resp.status_code != 200:
        return None

    final_url = resp.url
    html      = resp.text

    # Check 1: redirect to known ATS
    if final_url != career_page_url:
        result = match_ats_pattern(final_url)
        if result and result.get("platform") not in HARD_ATS:
            return result

    # Check 2: Phenom People — cdn.phenompeople.com or use-widget: ph-
    if ("cdn.phenompeople.com" in html) or ("use-widget" in html and "ph-" in html):
        slug = _extract_phenom_slug(html, career_page_url)
        if slug:
            return {"platform": "phenom", "slug": slug}

    # Check 3: TalentBrew/Radancy — tbcdn.talentbrew.com
    if "tbcdn.talentbrew.com" in html or "radancy" in html.lower():
        slug = _extract_talentbrew_slug(html, career_page_url)
        if slug:
            return {"platform": "talentbrew", "slug": slug}

    # Check 4: Avature — avature.net in HTML
    if "avature.net" in html:
        slug = _extract_avature_slug(html, career_page_url)
        if slug:
            return {"platform": "avature", "slug": slug}

    # Check 5: scan for any other known ATS patterns in HTML
    from jobs.career_page import _scan_html
    domain = _domain_from_url(career_page_url) or ""
    result = _scan_html(html, "")  # company validation skipped — URL is user-provided
    if result and result.get("platform") not in HARD_ATS:
        return result

    return None


def _extract_phenom_slug(html, career_page_url):
    """
    Extract Phenom People slug_info from career page HTML.
    Looks for use-widget meta tag to confirm, uses base URL + path.
    """
    parsed = urlparse(career_page_url)
    base   = f"{parsed.scheme}://{parsed.netloc}"
    path   = parsed.path.strip("/")

    # Try to get canonical path from use-widget meta
    m = re.search(r'name=["\']use-widget["\'][^>]*content=["\']ph-([^"\']+)["\']', html)
    if not m:
        m = re.search(r'content=["\']ph-([^"\']+)["\'][^>]*name=["\']use-widget', html)

    sitemap = f"{path}/sitemap.xml" if path else "sitemap.xml"
    return json.dumps({"base": base, "path": path, "sitemap": sitemap})


def _extract_talentbrew_slug(html, career_page_url):
    """
    Extract TalentBrew slug_info from career page HTML.
    Extracts site-tenant-id meta tag value.
    """
    parsed    = urlparse(career_page_url)
    base      = f"{parsed.scheme}://{parsed.netloc}"

    # Extract tenant_id from meta tag
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
    """
    Extract Avature slug_info from career page HTML.
    Uses base URL and path from the career page URL.
    """
    parsed = urlparse(career_page_url)
    base   = f"{parsed.scheme}://{parsed.netloc}"
    # Path is typically "careers" or "en_US/careers" etc.
    path   = parsed.path.strip("/").split("/")[0] if parsed.path.strip("/") else "careers"
    return json.dumps({"base": base, "path": path})


def run():
    """Sync prospective companies form responses to pipeline DB."""
    logger.info("════════════════════════════════════════")
    logger.info("--sync-forms (prospective) starting")

    if not SHEET_ID:
        logger.error("GOOGLE_SHEET_ID not set in .env — aborting")
        print("[ERROR] GOOGLE_SHEET_ID not set in .env")
        return

    if not os.path.exists(CREDENTIALS_FILE):
        logger.error("credentials.json not found at: %s", CREDENTIALS_FILE)
        print(f"[ERROR] credentials.json not found at {CREDENTIALS_FILE}")
        return

    print("[INFO] Syncing prospective companies form...")
    try:
        worksheet = _get_sheet()
    except Exception as e:
        logger.error("Could not connect to Google Sheets: %s", e, exc_info=True)
        print(f"[ERROR] Could not connect to Google Sheets: {e}")
        return

    all_rows = worksheet.get_all_values()
    logger.debug("Total rows in sheet (including header): %d", len(all_rows))

    if len(all_rows) <= 1:
        logger.info("No data rows found — sheet is empty or header-only")
        print("[INFO] No new prospective companies to process.")
        return

    data_rows = all_rows[1:]
    logger.info("Found %d prospective company row(s) to process", len(data_rows))
    print(f"[INFO] Found {len(data_rows)} prospective company entries.\n")

    from db.connection import get_conn

    imported       = 0
    skipped        = 0
    rows_to_delete = []

    for i, row in enumerate(data_rows):
        sheet_row = i + 2

        # Pad row to expected column count
        while len(row) < 7:
            row.append("")

        company      = row[COL_COMPANY].strip()
        job_url      = row[COL_JOB_URL].strip()
        career_page  = row[COL_CAREER_PAGE].strip()
        domain_input = row[COL_DOMAIN].strip()
        xml_url      = row[COL_XML_URL].strip()

        logger.info("── Row %d: company=%r", sheet_row, company)
        print(f"  [{i+1}] {company}")

        if not company:
            logger.warning("Row %d: missing company name — skipping", sheet_row)
            print("       [SKIP] Missing company name")
            skipped += 1
            rows_to_delete.append(sheet_row)
            continue

        # Resolve domain — form field takes priority, else extract from job URL
        domain = None
        if domain_input:
            normalized = domain_input.lower().strip()
            if normalized.startswith("https://"):
                normalized = normalized[8:]
            elif normalized.startswith("http://"):
                normalized = normalized[7:]
            domain = normalized.rstrip("/")
        elif job_url and _is_valid_url(job_url):
            domain = _domain_from_url(job_url)
        elif career_page and _is_valid_url(career_page):
            domain = _domain_from_url(career_page)

        logger.debug("Row %d: domain=%s job_url=%s career_page=%s xml_url=%s",
                     sheet_row, domain, job_url[:60] if job_url else "",
                     career_page[:60] if career_page else "",
                     xml_url[:60] if xml_url else "")

        # Resolve ATS platform + slug
        ats_result = _resolve_ats(company, job_url, career_page, domain, xml_url)
        platform   = ats_result["platform"] if ats_result else None
        slug       = ats_result["slug"]     if ats_result else None

        # Write to DB
        conn = get_conn()
        try:
            existing = conn.execute(
                "SELECT id, status, ats_platform FROM prospective_companies "
                "WHERE company = ?", (company,)
            ).fetchone()

            if existing:
                needs_ats_update = (
                    ats_result and (
                        existing["ats_platform"] is None or
                        existing["ats_platform"] == "unknown"
                    )
                )

                if needs_ats_update:
                    conn.execute(
                        "UPDATE prospective_companies "
                        "SET status='active', "
                        "ats_platform=?, ats_slug=?, ats_detected_at=?, "
                        "domain = CASE WHEN domain IS NULL OR domain = '' THEN ? ELSE domain END "
                        "WHERE company=?",
                        (platform, slug, datetime.utcnow(),
                         domain, company)
                    )
                else:
                    conn.execute(
                        "UPDATE prospective_companies "
                        "SET status='active', "
                        "domain = CASE WHEN domain IS NULL OR domain = '' THEN ? ELSE domain END "
                        "WHERE company=?",
                        (domain, company)
                    )
                conn.commit()
                print("       [OK] Updated existing company")

            else:
                conn.execute(
                    "INSERT INTO prospective_companies "
                    "(company, domain, ats_platform, ats_slug, ats_detected_at, "
                    "priority, status, created_at) "
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
            logger.error("Row %d: DB error for %r: %s", sheet_row, company, e, exc_info=True)
            raise
        finally:
            conn.close()

        imported += 1
        rows_to_delete.append(sheet_row)

    # Delete processed rows from sheet
    if rows_to_delete:
        print(f"\n[INFO] Cleaning {len(rows_to_delete)} row(s) from sheet...")
        for row_index in sorted(rows_to_delete, reverse=True):
            try:
                worksheet.delete_rows(row_index)
            except Exception as e:
                logger.error("Could not delete sheet row %d: %s", row_index, e)
                print(f"   [WARNING] Could not delete row {row_index}: {e}")

    logger.info("Sync complete — imported=%d  skipped=%d", imported, skipped)
    print(f"\n{'='*55}")
    print(f"[OK] Sync complete — Imported: {imported} | Skipped: {skipped}")
    logger.info("════ --sync-forms (prospective) finished ════")