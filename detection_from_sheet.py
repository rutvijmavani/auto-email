"""
detect_from_sheet.py — Detect ATS from job URLs in Google Sheet

Flow:
  1. Read Google Sheet with columns:
     A: Company | B: Domain | C: Job URL | D: ATS Platform | E: ATS Slug | F: Status
  2. For each row with a Job URL but no ATS Platform:
     → Run match_ats_pattern(url) to extract platform + slug
     → If matched: write platform + slug back to sheet + set Status = "auto"
     → If not matched: set Status = "manual" (needs human review)
  3. Also update prospective_companies DB via --override

Usage:
  python detect_from_sheet.py              # process all undetected rows
  python detect_from_sheet.py --dry-run    # preview without writing
  python detect_from_sheet.py --apply-db  # also update pipeline DB

Sheet setup:
  Create a new Google Sheet named "ATS Detection"
  Share it with your service account email
  Add headers in row 1:
    A1: Company
    B1: Domain
    C1: Job URL
    D1: ATS Platform
    E1: ATS Slug
    F1: Status
    G1: Notes
"""

import sys
import json
import argparse

# ── Config ────────────────────────────────────────────────────────────────
SHEET_NAME      = "ATS Detection"
HEADER_ROW      = 1
COL_COMPANY     = 0   # A
COL_DOMAIN      = 1   # B
COL_JOB_URL     = 2   # C
COL_PLATFORM    = 3   # D
COL_SLUG        = 4   # E
COL_STATUS      = 5   # F
COL_NOTES       = 6   # G
# ──────────────────────────────────────────────────────────────────────────


def get_sheet():
    """Connect to Google Sheet."""
    import gspread
    from dotenv import load_dotenv
    import os

    load_dotenv()

    sheet_id = os.getenv("GOOGLE_SHEET_ID")
    if not sheet_id:
        raise ValueError("GOOGLE_SHEET_ID not set in .env")

    gc = gspread.service_account(filename="credentials.json") \
        if os.path.exists("credentials.json") \
        else gspread.oauth()

    try:
        sh = gc.open_by_key(sheet_id)
        # Try to find ATS Detection sheet, create if missing
        try:
            ws = sh.worksheet(SHEET_NAME)
        except gspread.WorksheetNotFound:
            ws = sh.add_worksheet(SHEET_NAME, rows=200, cols=10)
            ws.update("A1:G1", [[
                "Company", "Domain", "Job URL",
                "ATS Platform", "ATS Slug", "Status", "Notes"
            ]])
            _populate_unknown_companies(ws)
            print(f"[OK] Created '{SHEET_NAME}' sheet with {_count_unknowns()} companies")
        return ws
    except Exception as e:
        raise RuntimeError(f"Cannot open sheet: {e}")


def _count_unknowns():
    """Count unknown companies in DB."""
    from db.connection import get_conn
    conn = get_conn()
    count = conn.execute("""
        SELECT COUNT(*) FROM prospective_companies
        WHERE ats_platform IS NULL
        OR ats_platform IN ('unknown', 'unsupported')
    """).fetchone()[0]
    conn.close()
    return count


def _populate_unknown_companies(ws):
    """Pre-fill sheet with unknown companies from DB."""
    from db.connection import get_conn
    conn = get_conn()
    rows = conn.execute("""
        SELECT company, domain
        FROM prospective_companies
        WHERE ats_platform IS NULL
        OR ats_platform IN ('unknown', 'unsupported')
        ORDER BY company ASC
    """).fetchall()
    conn.close()

    if not rows:
        return

    data = [[r["company"], r["domain"] or "", "", "", "", "pending", ""]
            for r in rows]
    ws.update(f"A2:G{len(data)+1}", data)


def process_sheet(dry_run=False, apply_db=False):
    """
    Main processing loop.
    Reads sheet, parses URLs, writes back platform+slug.
    """
    from jobs.ats.patterns import match_ats_pattern

    ws       = get_sheet()
    all_rows = ws.get_all_values()

    if len(all_rows) <= HEADER_ROW:
        print("[INFO] Sheet is empty — populate with unknown companies first")
        _populate_unknown_companies(ws)
        print(f"[OK] Populated sheet with unknown companies")
        return

    processed = 0
    matched   = 0
    skipped   = 0

    updates = []  # batch updates

    for i, row in enumerate(all_rows[HEADER_ROW:], start=HEADER_ROW + 1):
        # Pad row to minimum length
        while len(row) < 7:
            row.append("")

        company  = row[COL_COMPANY].strip()
        job_url  = row[COL_JOB_URL].strip()
        platform = row[COL_PLATFORM].strip()
        status   = row[COL_STATUS].strip()

        if not company:
            continue

        # Skip if already processed
        if platform and status in ("auto", "manual_done", "override"):
            skipped += 1
            continue

        # Skip if no URL yet
        if not job_url:
            continue

        processed += 1

        # Try to detect ATS from URL
        result = match_ats_pattern(job_url)

        if result:
            p    = result["platform"]
            slug = result["slug"]
            note = f"Auto-detected from URL"
            matched += 1

            print(f"  [MATCH] {company:<35} {p:<15} {slug[:40]}")

            if not dry_run:
                updates.append({
                    "row":      i,
                    "platform": p,
                    "slug":     slug,
                    "status":   "auto",
                    "notes":    note,
                })

                if apply_db:
                    _apply_to_db(company, p, slug)
        else:
            print(f"  [NO MATCH] {company:<35} URL: {job_url[:60]}")
            if not dry_run:
                updates.append({
                    "row":      i,
                    "platform": "",
                    "slug":     "",
                    "status":   "manual",
                    "notes":    "Pattern not recognized — check URL format",
                })

    # Batch write back to sheet
    if updates and not dry_run:
        for u in updates:
            row_n = u["row"]
            ws.update(f"D{row_n}:G{row_n}", [[
                u["platform"], u["slug"], u["status"], u["notes"]
            ]])

    print()
    print(f"=== RESULTS ===")
    print(f"Processed:  {processed}")
    print(f"Matched:    {matched}")
    print(f"No match:   {processed - matched}")
    print(f"Skipped:    {skipped} (already done)")

    if dry_run:
        print("[DRY RUN] No changes written")
    elif apply_db:
        print("[OK] DB updated for matched companies")


def _apply_to_db(company, platform, slug):
    """Apply detected ATS to pipeline DB."""
    try:
        from jobs.ats_detector import override_ats
        override_ats(company, platform, slug)
        print(f"    [DB] {company} → {platform}")
    except Exception as e:
        print(f"    [DB ERROR] {company}: {e}")


def list_pending():
    """Show companies still needing a URL in the sheet."""
    ws       = get_sheet()
    all_rows = ws.get_all_values()

    pending = []
    for row in all_rows[HEADER_ROW:]:
        while len(row) < 7:
            row.append("")
        company = row[COL_COMPANY].strip()
        job_url = row[COL_JOB_URL].strip()
        status  = row[COL_STATUS].strip()
        if company and not job_url and status not in ("auto", "manual_done"):
            pending.append(company)

    print(f"\n{len(pending)} companies still need a Job URL:\n")
    for c in pending:
        print(f"  {c}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Detect ATS from job URLs in Google Sheet"
    )
    parser.add_argument("--dry-run",   action="store_true",
                        help="Preview without writing")
    parser.add_argument("--apply-db",  action="store_true",
                        help="Also update pipeline DB")
    parser.add_argument("--list",      action="store_true",
                        help="List companies still needing a URL")
    parser.add_argument("--populate",  action="store_true",
                        help="Re-populate sheet with unknown companies")
    args = parser.parse_args()

    if args.list:
        list_pending()
    elif args.populate:
        ws = get_sheet()
        _populate_unknown_companies(ws)
        print("[OK] Sheet populated")
    else:
        process_sheet(dry_run=args.dry_run, apply_db=args.apply_db)