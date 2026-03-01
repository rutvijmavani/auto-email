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

from db.db import init_db, add_application, save_job
from jobs.job_fetcher import fetch_job_description

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
    creds = Credentials.from_service_account_file(CREDENTIALS_FILE, scopes=SCOPES)
    client = gspread.authorize(creds)
    sheet = client.open("Job Applications")
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
            return datetime.now().strftime("%Y-%m-%d")


def _is_valid_url(url):
    """Basic URL validation."""
    return bool(re.match(r"https?://", url.strip()))


def run():
    """Main sync function — reads sheet, imports to DB, deletes processed rows."""
    if not SHEET_ID:
        print("[ERROR] GOOGLE_SHEET_ID not set in .env")
        return

    if not os.path.exists(CREDENTIALS_FILE):
        print(f"[ERROR] credentials.json not found at {CREDENTIALS_FILE}")
        return

    init_db()

    print("[INFO] Connecting to Google Sheets...")
    try:
        worksheet = _get_sheet()
    except Exception as e:
        print(f"[ERROR] Could not connect to Google Sheets: {e}")
        return

    # Get all rows including header
    all_rows = worksheet.get_all_values()

    if len(all_rows) <= 1:
        print("[INFO] No new form responses to process.")
        return

    header = all_rows[0]
    data_rows = all_rows[1:]  # skip header row

    print(f"[INFO] Found {len(data_rows)} form response(s) to process.\n")

    imported   = 0
    skipped    = 0
    failed     = 0
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

        print(f"  [{i+1}] {company} | {job_url[:50]}...")

        # Validate required fields
        if not company:
            print(f"       [WARNING]  Missing company name — skipping")
            skipped += 1
            rows_to_delete.append(sheet_row_index)
            continue

        if not job_url or not _is_valid_url(job_url):
            print(f"       [WARNING]  Missing or invalid job URL — skipping")
            skipped += 1
            rows_to_delete.append(sheet_row_index)
            continue

        # Insert into applications table
        app_id = add_application(
            company=company,
            job_url=job_url,
            job_title=job_title,
            applied_date=applied_date,
        )

        if not app_id:
            print(f"       [SKIP]  Already exists in DB — skipping")
            skipped += 1
            rows_to_delete.append(sheet_row_index)
            continue

        print(f"       [OK] Added to DB (id={app_id})")

        # Scrape job description
        print(f"       [INFO] Scraping job description...")
        try:
            result = fetch_job_description(job_url)
            if result:
                print(f"       [OK] JD cached")
            else:
                print(f"       [WARNING]  Could not scrape JD — will retry during --find-only")
        except Exception as e:
            print(f"       [WARNING]  JD scraping failed: {e}")

        imported += 1
        rows_to_delete.append(sheet_row_index)

    # Delete processed rows from sheet in reverse order
    # (reverse so row indices stay correct as we delete)
    if rows_to_delete:
        print(f"\n[INFO]️  Deleting {len(rows_to_delete)} processed row(s) from sheet...")
        for row_index in sorted(rows_to_delete, reverse=True):
            try:
                worksheet.delete_rows(row_index)
            except Exception as e:
                print(f"   [WARNING]  Could not delete row {row_index}: {e}")
        print(f"[OK] Sheet cleaned up")

    print(f"\n{'='*55}")
    print(f"[OK] Sync complete — Imported: {imported} | Skipped: {skipped} | Failed: {failed}")