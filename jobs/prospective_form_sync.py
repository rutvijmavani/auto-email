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
    creds  = Credentials.from_service_account_file(CREDENTIALS_FILE, scopes=SCOPES)
    client = gspread.authorize(creds)
    sheet  = client.open_by_key(SHEET_ID)
    try:
        return sheet.worksheet(SHEET_NAME)
    except gspread.WorksheetNotFound:
        # Create the tab if it doesn't exist yet
        ws = sheet.add_worksheet(SHEET_NAME, rows=200, cols=5)
        ws.update("A1:D1", [["Timestamp", "Company Name", "Job URL", "Notes"]])
        print(f"[OK] Created '{SHEET_NAME}' tab in Google Sheet")
        return ws


def _is_valid_url(url):
    return bool(re.match(r"https?://", url.strip())) if url else False


def run():
    """Sync prospective companies form responses to pipeline DB."""
    if not SHEET_ID:
        print("[ERROR] GOOGLE_SHEET_ID not set in .env")
        return

    if not os.path.exists(CREDENTIALS_FILE):
        print(f"[ERROR] credentials.json not found at {CREDENTIALS_FILE}")
        return

    print("[INFO] Syncing prospective companies form...")
    try:
        worksheet = _get_sheet()
    except Exception as e:
        print(f"[ERROR] Could not connect to Google Sheets: {e}")
        return

    all_rows = worksheet.get_all_values()
    if len(all_rows) <= 1:
        print("[INFO] No new prospective companies to process.")
        return

    data_rows = all_rows[1:]
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
        notes   = row[COL_NOTES].strip()

        print(f"  [{i+1}] {company}")

        if not company:
            print(f"       [SKIP] Missing company name")
            skipped += 1
            rows_to_delete.append(sheet_row)
            continue

        # Extract ATS from job URL if provided
        ats_result = None
        if job_url and _is_valid_url(job_url):
            ats_result = match_ats_pattern(job_url)
            if ats_result:
                print(f"       [ATS] {ats_result['platform']} / "
                      f"{ats_result['slug'][:50]}")
            else:
                print(f"       [INFO] No ATS pattern in URL — "
                      f"will detect on next --detect-ats run")

        platform = ats_result["platform"] if ats_result else None
        slug     = ats_result["slug"]     if ats_result else None

        # Add to prospective_companies
        conn = get_conn()
        try:
            existing = conn.execute(
                "SELECT id, status, ats_platform FROM prospective_companies "
                "WHERE company = ?", (company,)
            ).fetchone()

            if existing:
                # Update ATS if we now have it
                if ats_result and not existing["ats_platform"]:
                    conn.execute(
                        "UPDATE prospective_companies "
                        "SET ats_platform=?, ats_slug=?, ats_detected_at=? "
                        "WHERE company=?",
                        (platform, slug, datetime.utcnow(), company)
                    )
                    conn.commit()
                    print(f"       [OK] ATS updated for existing company")
                else:
                    print(f"       [SKIP] Already in pipeline "
                          f"(status={existing['status']})")
                    skipped += 1
                    rows_to_delete.append(sheet_row)
                    continue
            else:
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
                print(f"       [OK] Added to pipeline (status=active)")
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
                print(f"   [WARNING] Could not delete row {row_index}: {e}")

    print(f"\n{'='*55}")
    print(f"[OK] Sync complete — "
          f"Imported: {imported} | Skipped: {skipped}")
    print(f"     New companies will be detected on next --detect-ats run")