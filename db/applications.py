# db/applications.py — Application table helpers

import sqlite3
from datetime import datetime

from db.connection import get_conn


def add_application(company, job_url, job_title=None, applied_date=None,
                    expected_domain=None, status_override=None):
    """
    Insert a new application.
    Returns (application_id, created) where created=True means newly inserted,
    created=False means the URL already existed in DB.
    status_override: set custom status (e.g. 'prospective') instead of default 'active'
    """
    conn = get_conn()
    c = conn.cursor()
    status = status_override or "active"
    try:
        c.execute("""
            INSERT INTO applications (company, job_url, job_title, applied_date,
                                      expected_domain, status)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (company, job_url, job_title,
              applied_date or datetime.now().strftime("%Y-%m-%d"),
              expected_domain, status))
        conn.commit()
        return c.lastrowid, True
    except sqlite3.IntegrityError as e:
        if "UNIQUE constraint failed: applications.job_url" not in str(e):
            raise
        c.execute("SELECT id, expected_domain FROM applications WHERE job_url = ?",
                  (job_url,))
        row = c.fetchone()
        if not row:
            return (None, False)
        # Backfill expected_domain if existing row has NULL and we have a value
        if expected_domain and not row["expected_domain"]:
            c.execute(
                "UPDATE applications SET expected_domain = ? WHERE id = ?",
                (expected_domain, row["id"])
            )
            conn.commit()
        return (row["id"], False)
    finally:
        conn.close()


def get_all_active_applications():
    conn = get_conn()
    c = conn.cursor()
    c.execute("""
        SELECT * FROM applications
        WHERE status = 'active'
        ORDER BY applied_date ASC
    """)
    rows = [dict(r) for r in c.fetchall()]
    conn.close()
    return rows


def get_application_by_id(application_id):
    conn = get_conn()
    c = conn.cursor()
    c.execute("SELECT * FROM applications WHERE id = ?", (application_id,))
    row = c.fetchone()
    conn.close()
    return dict(row) if row else None


def mark_application_exhausted(application_id):
    """Mark application as exhausted — no recruiters found after all validation."""
    conn = get_conn()
    c = conn.cursor()
    c.execute("""
        UPDATE applications
        SET status = 'exhausted', exhausted_at = CURRENT_TIMESTAMP
        WHERE id = ?
    """, (application_id,))
    conn.commit()
    conn.close()


def mark_applications_exhausted(application_ids):
    """
    Atomically mark multiple applications as exhausted in a single transaction.
    Prevents partial updates if one row fails mid-loop.
    """
    if not application_ids:
        return
    conn = get_conn()
    c = conn.cursor()
    try:
        c.executemany("""
            UPDATE applications
            SET status = 'exhausted', exhausted_at = CURRENT_TIMESTAMP
            WHERE id = ?
        """, [(app_id,) for app_id in application_ids])
        conn.commit()
    finally:
        conn.close()


def reactivate_application(company):
    """Reactivate exhausted application by company name."""
    conn = get_conn()
    c = conn.cursor()
    c.execute("""
        UPDATE applications
        SET status = 'active', exhausted_at = NULL
        WHERE company = ? AND status = 'exhausted'
    """, (company,))
    conn.commit()
    count = c.rowcount
    conn.close()
    return count


def convert_prospective_to_active(company, real_job_url, job_title=None,
                                   expected_domain=None):
    """
    Convert a prospective placeholder application to active when user applies.
    Updates the placeholder URL to the real job URL and marks status active.
    Returns app_id if converted, None if no prospective found or URL conflict.
    """
    conn = get_conn()
    c = conn.cursor()
    company = company.strip()
    placeholder_url = f"prospective://{company.lower().replace(' ', '-')}"
    c.execute("""
        SELECT id FROM applications
        WHERE company = ? AND job_url = ? AND status = 'prospective'
    """, (company, placeholder_url))
    row = c.fetchone()
    if not row:
        conn.close()
        return None
    try:
        c.execute("""
            UPDATE applications
            SET job_url = ?,
                job_title = COALESCE(?, job_title),
                expected_domain = COALESCE(?, expected_domain),
                status = 'active',
                applied_date = DATE('now')
            WHERE id = ?
        """, (real_job_url, job_title, expected_domain, row["id"]))
        conn.commit()
        app_id = row["id"]
        return app_id
    except sqlite3.IntegrityError:
        # real_job_url already exists in applications table
        conn.rollback()
        print(f"   [WARNING] Job URL already exists: {real_job_url}")
        return None
    finally:
        conn.close()


def update_application_expected_domain(application_id, expected_domain):
    """Update expected_domain for an existing application."""
    conn = get_conn()
    c = conn.cursor()
    c.execute("""
        UPDATE applications SET expected_domain = ?
        WHERE id = ?
    """, (expected_domain, application_id))
    conn.commit()
    conn.close()


def get_existing_domain_for_company(company):
    """
    Return the email domain root already stored in DB for this company.
    Used as reference when adding more recruiters (top-up scenario).
    Returns None if no existing active recruiters.

    Example:
      john@collective.com is in DB → returns "collective"
    """
    conn = get_conn()
    c = conn.cursor()
    c.execute("""
        SELECT r.email FROM recruiters r
        INNER JOIN application_recruiters ar ON ar.recruiter_id = r.id
        INNER JOIN applications a ON a.id = ar.application_id
        WHERE a.company = ? AND r.recruiter_status = 'active'
        ORDER BY r.id ASC
        LIMIT 1
    """, (company,))
    row = c.fetchone()
    conn.close()
    if row and row["email"] and "@" in row["email"]:
        domain = row["email"].split("@")[1]   # "collective.com"
        return domain.split(".")[0]            # "collective"
    return None