"""
jobs/form_sync.py — Syncs Google Form responses to SQLite applications table.

Flow:
    1. Read all rows from Google Sheet (Form_Responses tab)
    2. For each unprocessed row:
       a. Parse fields (company, job_url, job_title, applied_date)
       b. Insert into applications table
       c. Scrape job description → store in jobs table
       d. Delete row from sheet (keeps Drive clean)
    3. Skip rows with missing required fields
"""

import os
import re
from datetime import datetime

import gspread
from google.oauth2.service_account import Credentials
from dotenv import load_dotenv

from logger import get_logger
from db.db import init_db, add_application
from jobs.job_fetcher import fetch_job_description

logger = get_logger(__name__)

load_dotenv()

SHEET_ID         = os.getenv("GOOGLE_SHEET_ID")
CREDENTIALS_FILE = os.path.join(os.path.dirname(__file__), "..", "credentials.json")
SHEET_NAME       = "Responses"

# Google API scopes needed
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

# Column indices (0-based) matching sheet headers:
# Timestamp | Company Name | Job URL | Job Title | Applied Date
COL_TIMESTAMP    = 0
COL_COMPANY      = 1
COL_JOB_URL      = 2
COL_JOB_TITLE    = 3
COL_APPLIED_DATE = 4


def _get_sheet():
    """Authenticate and return the Google Sheet worksheet."""
    logger.debug("Loading credentials from: %s", CREDENTIALS_FILE)
    creds  = Credentials.from_service_account_file(CREDENTIALS_FILE, scopes=SCOPES)
    client = gspread.authorize(creds)
    logger.debug("Opening sheet ID: %s  worksheet: %s", SHEET_ID, SHEET_NAME)
    sheet  = client.open_by_key(SHEET_ID)
    return sheet.worksheet(SHEET_NAME)


def _parse_date(date_str):
    """
    Convert Google Form date (M/D/YYYY) to SQLite format (YYYY-MM-DD).
    Falls back to today if empty or unrecognized.
    """
    if not date_str or not date_str.strip():
        return datetime.now().strftime("%Y-%m-%d")
    try:
        parsed = datetime.strptime(date_str.strip(), "%m/%d/%Y")
        return parsed.strftime("%Y-%m-%d")
    except ValueError:
        try:
            # Already in YYYY-MM-DD format
            datetime.strptime(date_str.strip(), "%Y-%m-%d")
            return date_str.strip()
        except ValueError:
            logger.debug("Unrecognised date format %r — defaulting to today", date_str)
            return datetime.now().strftime("%Y-%m-%d")


def _is_valid_url(url):
    """Basic URL validation."""
    return bool(re.match(r"https?://", url.strip()))


def _sync_to_pipeline(company, job_url):
    """
    Store ATS slug in ats_discovery.db if detected from job URL.
    source='application' — user-submitted URL is ground truth.
    Does NOT write to prospective_companies.

    company_name is only written if the existing DB row has none —
    avoids overwriting canonical ATS metadata or falsely marking
    is_enriched=1 for slugs already enriched from another source.

    Delegates to upsert_company(only_set_name_if_missing=True, is_active=True)
    which opens a single BEGIN IMMEDIATE transaction covering the name check,
    INSERT OR IGNORE, and UPDATE — no split-commit race possible.
    """
    # ── ATS detection — wrapped separately so failures don't abort the sync ──
    from jobs.ats.patterns import match_ats_pattern
    from db.ats_companies import upsert_company
    from db.schema_discovery import init_discovery_db

    try:
        ats = match_ats_pattern(job_url)
    except Exception as e:
        logger.error("ATS pattern match failed for %r (url=%s): %s",
                     company, job_url, e, exc_info=True)
        return

    if not ats:
        logger.debug("No ATS pattern matched for %r — skipping ats_discovery.db write",
                     company)
        return

    platform = ats["platform"]
    slug     = ats["slug"]

    # ── Persistence — narrow try/except covers only DB writes ──────────
    try:
        init_discovery_db()
        upsert_company(
            platform=platform,
            slug=slug,
            company_name=company,
            source="application",
            is_active=True,
            only_set_name_if_missing=True,
        )
        logger.info("Stored/updated in ats_discovery.db: platform=%s slug=%s company=%r",
                    platform, slug, company)
        print(f"       [ATS-DB] Stored in ats_discovery.db ({platform}/{slug})")

    except Exception as e:
        # Only DB write failures reach here — detection errors surface above
        logger.error("ats_discovery.db write failed for %r (platform=%s slug=%s): %s",
                     company, platform, slug, e, exc_info=True)
        print(f"       [WARNING] ats_discovery.db write failed: {e}")


def run():
    """Main sync function — reads sheet, imports to DB, deletes processed rows."""
    from pipeline import extract_expected_domain  # local import avoids circular dependency

    logger.info("════════════════════════════════════════")
    logger.info("--sync-forms starting")

    if not SHEET_ID:
        logger.error("GOOGLE_SHEET_ID not set in .env — aborting")
        print("[ERROR] GOOGLE_SHEET_ID not set in .env")
        return

    if not os.path.exists(CREDENTIALS_FILE):
        logger.error("credentials.json not found at %s", CREDENTIALS_FILE)
        print(f"[ERROR] credentials.json not found at {CREDENTIALS_FILE}")
        return

    init_db()

    print("[INFO] Connecting to Google Sheets...")
    try:
        worksheet = _get_sheet()
        logger.debug("Connected to Google Sheets OK")
    except Exception as e:
        logger.error("Could not connect to Google Sheets: %s", e, exc_info=True)
        print(f"[ERROR] Could not connect to Google Sheets: {e}")
        return

    # Get all rows including header
    all_rows = worksheet.get_all_values()
    logger.debug("Total rows in sheet (including header): %d", len(all_rows))

    if len(all_rows) <= 1:
        logger.info("No data rows found — sheet is empty or header-only")
        print("[INFO] No new form responses to process.")
        return

    header    = all_rows[0]
    data_rows = all_rows[1:]  # skip header row

    logger.info("Found %d form response(s) to process", len(data_rows))
    print(f"[INFO] Found {len(data_rows)} form response(s) to process.\n")

    imported       = 0
    skipped        = 0
    failed         = 0
    # Track which sheet rows to delete (1-based, accounting for header)
    rows_to_delete = []

    for i, row in enumerate(data_rows):
        sheet_row_index = i + 2  # +2 because sheet is 1-based and row 1 is header

        # Pad row if shorter than expected
        while len(row) < 5:
            row.append("")

        company      = row[COL_COMPANY].strip()
        job_url      = row[COL_JOB_URL].strip()
        job_title    = row[COL_JOB_TITLE].strip() or None
        applied_date = _parse_date(row[COL_APPLIED_DATE])

        logger.info("── Row %d: company=%r  url=%r  title=%r  date=%s",
                    sheet_row_index, company, job_url, job_title, applied_date)
        print(f"  [{i+1}] {company} | {job_url[:50]}...")

        # Validate required fields
        if not company:
            logger.warning("Row %d: missing company name — skipping", sheet_row_index)
            print(f"       [WARNING]  Missing company name — skipping")
            skipped += 1
            rows_to_delete.append(sheet_row_index)
            continue

        if not job_url or not _is_valid_url(job_url):
            logger.warning("Row %d: missing or invalid job URL %r — skipping",
                           sheet_row_index, job_url)
            print(f"       [WARNING]  Missing or invalid job URL — skipping")
            skipped += 1
            rows_to_delete.append(sheet_row_index)
            continue

        # Extract ATS from job URL and store in ats_discovery.db
        _sync_to_pipeline(company, job_url)

        # Insert into applications table
        expected_domain = extract_expected_domain(job_url)
        logger.debug("Row %d: expected_domain=%s", sheet_row_index, expected_domain)
        app_id, created = add_application(
            company=company,
            job_url=job_url,
            job_title=job_title,
            applied_date=applied_date,
            expected_domain=expected_domain,
        )

        if not app_id:
            logger.error("Row %d: failed to insert application for %r", sheet_row_index, company)
            print("       [ERROR] Failed to insert application — skipping")
            failed += 1
            # Do NOT append to rows_to_delete — leave row in sheet for retry
            continue

        if not created:
            logger.info("Row %d: %r already exists in DB (id=%s) — skipping",
                        sheet_row_index, company, app_id)
            print(f"       [SKIP] Already exists in DB — skipping")
            skipped += 1
            rows_to_delete.append(sheet_row_index)
            continue

        logger.info("Row %d: inserted application for %r (id=%s)", sheet_row_index, company, app_id)
        print(f"       [OK] Added to DB (id={app_id})")

        # Scrape job description
        print(f"       [INFO] Scraping job description...")
        logger.debug("Row %d: scraping JD from %s", sheet_row_index, job_url)
        try:
            result = fetch_job_description(job_url)
            if result:
                logger.info("Row %d: JD cached for %r", sheet_row_index, company)
                print(f"       [OK] JD cached")
            else:
                logger.warning("Row %d: could not scrape JD for %r — will retry during --find-only",
                               sheet_row_index, company)
                print(f"       [WARNING]  Could not scrape JD — will retry during --find-only")
        except Exception as e:
            logger.error("Row %d: JD scraping failed for %r: %s",
                         sheet_row_index, company, e, exc_info=True)
            print(f"       [WARNING]  JD scraping failed: {e}")

        imported += 1
        rows_to_delete.append(sheet_row_index)

    # Delete processed rows from sheet in reverse order
    # (reverse so row indices stay correct as we delete)
    if rows_to_delete:
        logger.info("Deleting %d processed row(s) from sheet: %s",
                    len(rows_to_delete), rows_to_delete)
        print(f"\n[INFO]️  Deleting {len(rows_to_delete)} processed row(s) from sheet...")
        for row_index in sorted(rows_to_delete, reverse=True):
            try:
                worksheet.delete_rows(row_index)
                logger.debug("Deleted sheet row %d", row_index)
            except Exception as e:
                logger.error("Could not delete sheet row %d: %s", row_index, e)
                print(f"   [WARNING]  Could not delete row {row_index}: {e}")
        print(f"[OK] Sheet cleaned up")

    logger.info("Sync complete — imported=%d  skipped=%d  failed=%d",
                imported, skipped, failed)
    print(f"\n{'='*55}")
    print(f"[OK] Sync complete — Imported: {imported} | Skipped: {skipped} | Failed: {failed}")
    logger.info("════ --sync-forms finished ════")