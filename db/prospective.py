# db/prospective.py — Prospective company DB operations

import os
from datetime import datetime, timezone

from db.connection import get_conn

_SHEET_ID         = os.environ.get("GOOGLE_SHEET_ID", "")
_CREDENTIALS_FILE = os.path.join(os.path.dirname(__file__), "..", "credentials.json")
_SHEET_NAME       = "Prospective"
_SCOPES           = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]


def submit_to_prospective_sheet(
    company: str,
    career_page_url: str | None = None,
    job_url: str | None = None,
    domain: str | None = None,
    notes: str = "via H1B Discover",
) -> bool:
    """
    Append one row to the 'Prospective' Google Sheet tab so that
    prospective_form_sync.py picks it up on next run and runs full
    ATS detection (career page fingerprint, job URL pattern match, etc.).

    Column order matches the sheet exactly:
      Timestamp | Company Name | Job URL | Domain | Career Page URL |
      XML/Sitemap URL | Listing Curl | Detail Curl | Notes

    Returns True when the row was successfully appended, False on failure.
    Failures are logged but not raised — callers should check the return
    value and show an appropriate warning when False.
    """
    import logging
    import gspread
    from google.oauth2.service_account import Credentials

    _log = logging.getLogger(__name__)
    try:
        creds  = Credentials.from_service_account_file(_CREDENTIALS_FILE, scopes=_SCOPES)
        client = gspread.authorize(creds)
        # Set a finite read timeout so network hangs don't block indefinitely
        if hasattr(client, "session") and hasattr(client.session, "timeout"):
            client.session.timeout = (10, 30)
        sheet = client.open_by_key(_SHEET_ID)
        try:
            ws = sheet.worksheet(_SHEET_NAME)
        except gspread.WorksheetNotFound:
            ws = sheet.add_worksheet(_SHEET_NAME, rows=200, cols=9)
            ws.update("A1:I1", [[
                "Timestamp", "Company Name", "Job URL",
                "Domain", "Career Page URL", "XML/Sitemap URL",
                "Listing Curl", "Detail Curl", "Notes",
            ]])

        row = [
            datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),  # Timestamp
            company,                                            # Company Name
            job_url         or "",                              # Job URL
            domain          or "",                              # Domain
            career_page_url or "",                              # Career Page URL
            "",                                                 # XML/Sitemap URL
            "",                                                 # Listing Curl
            "",                                                 # Detail Curl
            notes,                                              # Notes
        ]
        ws.append_row(row, value_input_option="USER_ENTERED")
        return True
    except Exception as exc:
        _log.warning("submit_to_prospective_sheet failed for %r: %s", company, exc)
        return False


def _normalize_company(name):
    """
    Normalize a company name — strip whitespace and validate non-empty.
    Raises ValueError if the result is empty.
    All prospective functions pass company names through this helper
    so " Google " and "Google" resolve to the same canonical value.
    """
    if name is None:
        raise ValueError("Company name cannot be None")
    normalized = name.strip()
    if not normalized:
        raise ValueError("Company name cannot be empty or whitespace")
    return normalized


def add_prospective_company(company, priority=0, domain=None, platform=None, slug=None):
    """
    Add a company to the prospective list.
    Silently ignores duplicates (INSERT OR IGNORE).
    Returns True if newly inserted, False if already existed.
    Raises ValueError if company name is empty/whitespace.

    platform/slug: pass when already known (e.g. from H1B discovery pipeline)
    so the company is immediately monitorable without a separate --detect-ats run.
    When omitted, ats_platform stays NULL and get_detection_queue() will pick it
    up as Priority 1 (new, never detected).
    """
    company = _normalize_company(company)
    conn = get_conn()
    c = conn.cursor()
    # ats_detected_at stamped at INSERT — never NULL — prevents stale companies
    # from triggering automatic re-detection on every monitor run.
    #
    # ats_platform: NULL when unknown (get_detection_queue Priority 1),
    #               set here when already detected by caller.
    # ON CONFLICT(company) DO NOTHING replaces INSERT OR IGNORE (SQLite).
    c.execute("""
        INSERT INTO prospective_companies
            (company, priority, status, domain, ats_detected_at, ats_platform, ats_slug)
        VALUES (?, ?, 'pending', ?, CURRENT_TIMESTAMP, ?, ?)
        ON CONFLICT(company) DO NOTHING
    """, (company, priority, domain, platform, slug))
    conn.commit()
    inserted = c.rowcount > 0
    if not inserted:
        # Update domain/platform/slug if company already existed and values are provided.
        # Treat 'unknown'/'unsupported' as sentinels — replace them when a real platform
        # is supplied, same as NULL.  Slug empty-string is likewise treated as absent.
        # Each field: keep the existing value when it is already real; otherwise
        # upgrade to the incoming value — but only when the incoming value is
        # non-NULL/non-empty.  COALESCE(?, existing) means a NULL incoming arg
        # falls back to the stored value, so sentinels like "unknown" are
        # preserved rather than erased.
        c.execute("""
            UPDATE prospective_companies
            SET domain       = COALESCE(NULLIF(domain, ''),                                     COALESCE(?, domain)),
                ats_platform = COALESCE(NULLIF(NULLIF(ats_platform, 'unknown'), 'unsupported'), COALESCE(?, ats_platform)),
                ats_slug     = COALESCE(NULLIF(ats_slug, ''),                                   COALESCE(?, ats_slug))
            WHERE company = ?
        """, (domain, platform, slug, company))
        conn.commit()
    conn.close()
    return inserted


def get_pending_prospective(limit=None):
    """
    Return companies pending recruiter scraping from both prospective_companies
    and company_ats (is_monitored=TRUE, status='pending', company_name NOT NULL).

    Each row includes:
      company    — actual company name passed to CareerShift search
      update_key — identifier for mark_prospective_scraped/exhausted calls;
                   equals company for prospective_companies rows,
                   'ca:{id}' for company_ats rows
      priority   — ordering weight
      domain     — full domain hint for scraper email validation (may be None)
    """
    conn = get_conn()
    c = conn.cursor()
    query = """
        SELECT
            company     AS update_key,
            company,
            priority,
            domain,
            created_at
        FROM prospective_companies
        WHERE status = 'pending'

        UNION ALL

        SELECT
            'ca:' || id::text AS update_key,
            company_name      AS company,
            priority,
            domain,
            detected_at       AS created_at
        FROM company_ats
        WHERE is_monitored = TRUE
          AND status = 'pending'
          AND company_name IS NOT NULL

        ORDER BY priority DESC, created_at ASC
    """
    if limit:
        query += f" LIMIT {int(limit)}"
    c.execute(query)
    rows = [dict(r) for r in c.fetchall()]
    conn.close()
    return rows


def get_prospective_companies(status=None):
    """
    Return all prospective companies, optionally filtered by status.
    status: 'pending', 'scraped', 'converted', 'exhausted' or None for all.
    """
    conn = get_conn()
    c = conn.cursor()
    if status:
        c.execute("""
            SELECT * FROM prospective_companies
            WHERE status = ?
            ORDER BY priority DESC, created_at ASC
        """, (status,))
    else:
        c.execute("""
            SELECT * FROM prospective_companies
            ORDER BY priority DESC, created_at ASC
        """)
    rows = [dict(r) for r in c.fetchall()]
    conn.close()
    return rows


def mark_prospective_scraped(company_key):
    """
    Mark company as scraped — recruiters found.
    company_key is 'ca:{id}' for company_ats rows, company name otherwise.
    """
    if company_key.startswith("ca:"):
        id_ = int(company_key[3:])
        conn = get_conn()
        try:
            conn.execute("""
                UPDATE company_ats
                SET status = 'scraped', scraped_at = CURRENT_TIMESTAMP
                WHERE id = %s AND status = 'pending'
            """, (id_,))
            conn.commit()
        finally:
            conn.close()
        return
    company = _normalize_company(company_key)
    conn = get_conn()
    c = conn.cursor()
    c.execute("""
        UPDATE prospective_companies
        SET status = 'scraped', scraped_at = CURRENT_TIMESTAMP
        WHERE company = ? AND status = 'pending'
    """, (company,))
    conn.commit()
    conn.close()


def mark_prospective_exhausted(company_key):
    """
    Mark company as exhausted — no recruiters found.
    company_key is 'ca:{id}' for company_ats rows, company name otherwise.
    """
    if company_key.startswith("ca:"):
        id_ = int(company_key[3:])
        conn = get_conn()
        try:
            conn.execute("""
                UPDATE company_ats
                SET status = 'exhausted', scraped_at = CURRENT_TIMESTAMP
                WHERE id = %s AND status = 'pending'
            """, (id_,))
            conn.commit()
        finally:
            conn.close()
        return
    company = _normalize_company(company_key)
    conn = get_conn()
    c = conn.cursor()
    c.execute("""
        UPDATE prospective_companies
        SET status = 'exhausted', scraped_at = CURRENT_TIMESTAMP
        WHERE company = ? AND status = 'pending'
    """, (company,))
    conn.commit()
    conn.close()


def mark_prospective_converted(company):
    """
    Mark prospective company as converted — user applied and ran --add.
    Called when --add detects existing prospective entry for a company.
    """
    company = _normalize_company(company)
    conn = get_conn()
    c = conn.cursor()
    c.execute("""
        UPDATE prospective_companies
        SET status = 'converted', converted_at = CURRENT_TIMESTAMP
        WHERE company = ? AND status IN ('pending', 'scraped')
    """, (company,))
    conn.commit()
    conn.close()


def is_prospective(company):
    """
    Check if company exists in prospective list with status 'scraped'.
    Used by --add to detect if recruiters are already pre-scraped.
    Returns True if company is scraped and ready for outreach.
    """
    company = _normalize_company(company)
    conn = get_conn()
    c = conn.cursor()
    c.execute("""
        SELECT id FROM prospective_companies
        WHERE company = ? AND status = 'scraped'
    """, (company,))
    row = c.fetchone()
    conn.close()
    return row is not None


def get_prospective_status_summary():
    """
    Return count of companies per status for --prospects-status report.
    """
    conn = get_conn()
    c = conn.cursor()
    c.execute("""
        SELECT status, COUNT(*) as count
        FROM prospective_companies
        GROUP BY status
        ORDER BY status
    """)
    rows = {r["status"]: r["count"] for r in c.fetchall()}
    conn.close()
    return rows


def get_prospective_company(company):
    """Return single prospective company record or None."""
    company = _normalize_company(company)
    conn = get_conn()
    c = conn.cursor()
    c.execute("""
        SELECT * FROM prospective_companies WHERE company = ?
    """, (company,))
    row = c.fetchone()
    conn.close()
    return dict(row) if row else None

def get_domain_for_prospective(company_key):
    """
    Return the domain root for a company, or '' if not set.
    e.g. 'lucidmotors.com' → 'lucidmotors', 'snap.com' → 'snap'
    company_key is 'ca:{id}' for company_ats rows, company name otherwise.
    """
    if company_key.startswith("ca:"):
        id_ = int(company_key[3:])
        conn = get_conn()
        try:
            row = conn.execute(
                "SELECT domain FROM company_ats WHERE id = %s", (id_,)
            ).fetchone()
        finally:
            conn.close()
        if row and row["domain"]:
            return row["domain"].split(".")[0]
        return ""
    company = _normalize_company(company_key)
    conn = get_conn()
    c = conn.cursor()
    c.execute(
        "SELECT domain FROM prospective_companies WHERE company = ?",
        (company,)
    )
    row = c.fetchone()
    conn.close()
    if row and row["domain"]:
        return row["domain"].split(".")[0]
    return ""