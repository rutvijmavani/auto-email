"""
jobs/prospective_form_sync.py — Syncs prospective company form responses.

Second Google Form (separate from job applications form).
Form fields:
  Timestamp | Company Name | Job URL (any job from their site) | Notes

Flow:
  1. Read rows from "Prospective" tab in Google Sheet
  2. For each row:
     a. Extract ATS from job URL if provided
     b. Add to prospective_companies with status='active'
     c. Mark row as processed (delete from sheet)
"""

import os
import re
from datetime import datetime

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
# Timestamp | Company Name | Job URL | Notes
COL_TIMESTAMP = 0
COL_COMPANY   = 1
COL_JOB_URL   = 2
COL_NOTES     = 3


def _get_sheet():
    logger.debug("Loading credentials from: %s", CREDENTIALS_FILE)
    try:
        creds = Credentials.from_service_account_file(
            CREDENTIALS_FILE, scopes=SCOPES
        )
    except FileNotFoundError:
        logger.error("Credentials file not found: %s", CREDENTIALS_FILE)
        raise RuntimeError(
            f"Credentials file not found: {CREDENTIALS_FILE}"
        ) from None
    except (ValueError, Exception) as e:
        logger.error("Invalid credentials in %s: %s", CREDENTIALS_FILE, e)
        raise RuntimeError(
            f"Invalid credentials in {CREDENTIALS_FILE}: {e}"
        ) from e
    logger.debug("Credentials loaded OK — opening sheet ID: %s", SHEET_ID)
    client = gspread.authorize(creds)
    sheet  = client.open_by_key(SHEET_ID)
    try:
        ws = sheet.worksheet(SHEET_NAME)
        logger.debug("Opened worksheet: %r", SHEET_NAME)
        return ws
    except gspread.WorksheetNotFound:
        # Create the tab if it doesn't exist yet
        logger.warning("Worksheet %r not found — creating it", SHEET_NAME)
        ws = sheet.add_worksheet(SHEET_NAME, rows=200, cols=4)
        ws.update("A1:D1", [["Timestamp", "Company Name", "Job URL", "Notes"]])
        logger.info("Created '%s' tab in Google Sheet", SHEET_NAME)
        print(f"[OK] Created '{SHEET_NAME}' tab in Google Sheet")
        return ws


def _is_valid_url(url):
    return bool(re.match(r"https?://", url.strip())) if url else False


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

    from jobs.ats.patterns import match_ats_pattern
    from db.connection import get_conn

    imported       = 0
    skipped        = 0
    rows_to_delete = []

    for i, row in enumerate(data_rows):
        sheet_row = i + 2

        while len(row) < 4:
            row.append("")

        company = row[COL_COMPANY].strip()
        job_url = row[COL_JOB_URL].strip()

        logger.info("── Row %d: company=%r  url=%r", sheet_row, company, job_url)
        print(f"  [{i+1}] {company}")

        if not company:
            logger.warning("Row %d: missing company name — skipping", sheet_row)
            print("       [SKIP] Missing company name")
            skipped += 1
            rows_to_delete.append(sheet_row)
            continue

        # Extract ATS from job URL — no validation, user URL is ground truth
        # match_ats_pattern extracts platform/slug from known ATS URL patterns
        # If URL is not a known ATS URL (e.g. company careers page),
        # domain is still stored so P3a can detect ATS automatically
        ats_result = None
        if job_url and _is_valid_url(job_url):
            logger.debug("Row %d: running ATS pattern match on: %s", sheet_row, job_url)
            ats_result = match_ats_pattern(job_url)
            if ats_result:
                logger.info("Row %d: ATS matched — platform=%s slug=%s",
                            sheet_row, ats_result["platform"],
                            str(ats_result["slug"])[:60])
                print(f"       [ATS] {ats_result['platform']} / "
                      f"{ats_result['slug'][:50]}")
            else:
                logger.info("Row %d: no ATS pattern matched — "
                            "domain stored for P3a career page scan", sheet_row)
                print("       [INFO] URL stored — ATS will be detected "
                      "automatically via career page scan")

        platform = ats_result["platform"] if ats_result else None
        slug     = ats_result["slug"]     if ats_result else None

        # Add to prospective_companies
        logger.debug("Row %d: opening DB connection", sheet_row)
        conn = get_conn()
        try:
            existing = conn.execute(
                "SELECT id, status, ats_platform FROM prospective_companies "
                "WHERE company = ?", (company,)
            ).fetchone()

            if existing:
                # Derive domain here so both update branches can use it
                from urllib.parse import urlparse as _urlparse
                domain = _urlparse(job_url).hostname if job_url else None

                logger.debug("Row %d: existing record — id=%s status=%s ats_platform=%s domain=%s",
                             sheet_row, existing["id"], existing["status"],
                             existing["ats_platform"], domain)

                # Update ATS if we now have it and didn't before
                # Treat None and 'unknown' as equivalent to missing
                needs_ats_update = (
                    ats_result and (
                        existing["ats_platform"] is None or
                        existing["ats_platform"] == "unknown"
                    )
                )
                logger.debug("Row %d: needs_ats_update=%s", sheet_row, needs_ats_update)

                if needs_ats_update:
                    logger.info("Row %d: updating ATS for %r — platform=%s slug=%s domain=%s",
                                sheet_row, company, platform, slug, domain)
                    conn.execute(
                        "UPDATE prospective_companies "
                        "SET status='active', "
                        "ats_platform=?, ats_slug=?, ats_detected_at=? "
                        "WHERE company=?",
                        (platform, slug, datetime.utcnow(), company)
                    )
                    conn.commit()
                    logger.info("Row %d: ATS update committed for %r", sheet_row, company)
                    print("       [OK] ATS updated for existing company")
                    imported += 1
                    rows_to_delete.append(sheet_row)
                    continue  # skip epilogue — already counted
                else:
                    # Update domain/status/ATS even for existing rows
                    # User may be providing a new URL or activating a company
                    logger.info("Row %d: updating status/domain for %r "
                                "(ats_result=%s domain=%s)",
                                sheet_row, company, bool(ats_result), domain)
                    conn.execute(
                        "UPDATE prospective_companies "
                        "SET status='active'"
                        + (", ats_platform=?, ats_slug=?, ats_detected_at=?"
                           if ats_result else "") +
                        " WHERE company=?",
                        (
                            (platform, slug, datetime.utcnow(), company)
                            if ats_result else
                            (company,)
                        )
                    )
                    conn.commit()
                    logger.info("Row %d: update committed for %r", sheet_row, company)
                    # FIX: removed unnecessary f-prefix (Ruff F541 — no interpolation)
                    print("       [OK] Updated existing company (status→active)")
                    imported += 1
                    rows_to_delete.append(sheet_row)
                    continue
            else:
                # Extract domain from job URL for P3a career page detection
                from urllib.parse import urlparse as _urlparse
                domain = _urlparse(job_url).hostname if job_url else None
                logger.info("Row %d: inserting NEW company %r — "
                            "platform=%s slug=%s domain=%s",
                            sheet_row, company, platform, slug, domain)
                conn.execute(
                    "INSERT INTO prospective_companies "
                    "(company, ats_platform, ats_slug, ats_detected_at, "
                    "priority, status, created_at) "
                    "VALUES (?, ?, ?, ?, 2, 'active', ?)",
                    (
                        company, platform, slug,
                        datetime.utcnow() if ats_result else None,
                        datetime.utcnow(),
                    )
                )
                conn.commit()
                logger.info("Row %d: INSERT committed for %r", sheet_row, company)
                print("       [OK] Added to pipeline (status=active)")
        except Exception as e:
            logger.error("Row %d: DB operation failed for %r: %s",
                         sheet_row, company, e, exc_info=True)
            raise
        finally:
            conn.close()
            logger.debug("Row %d: DB connection closed", sheet_row)

        imported += 1
        rows_to_delete.append(sheet_row)

    # Delete processed rows from sheet
    if rows_to_delete:
        logger.info("Deleting %d processed row(s) from sheet: %s",
                    len(rows_to_delete), rows_to_delete)
        print(f"\n[INFO] Cleaning {len(rows_to_delete)} row(s) from sheet...")
        for row_index in sorted(rows_to_delete, reverse=True):
            try:
                worksheet.delete_rows(row_index)
                logger.debug("Deleted sheet row %d", row_index)
            except Exception as e:
                logger.error("Could not delete sheet row %d: %s", row_index, e)
                print(f"   [WARNING] Could not delete row {row_index}: {e}")

    logger.info("Sync complete — imported=%d  skipped=%d", imported, skipped)
    print(f"\n{'='*55}")
    print(f"[OK] Sync complete — "
          f"Imported: {imported} | Skipped: {skipped}")
    print("     New companies will be detected on next --detect-ats run")
    logger.info("════ --sync-forms (prospective) finished ════")