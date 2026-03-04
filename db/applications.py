# db/applications.py — Application table helpers

import sqlite3
from datetime import datetime

from db.connection import get_conn


def add_application(company, job_url, job_title=None, applied_date=None,
                    expected_domain=None):
    """
    Insert a new application.
    Returns (application_id, created) where created=True means newly inserted,
    created=False means the URL already existed in DB.
    """
    conn = get_conn()
    c = conn.cursor()
    try:
        c.execute("""
            INSERT INTO applications (company, job_url, job_title, applied_date,
                                      expected_domain)
            VALUES (?, ?, ?, ?, ?)
        """, (company, job_url, job_title,
              applied_date or datetime.now().strftime("%Y-%m-%d"),
              expected_domain))
        conn.commit()
        return c.lastrowid, True
    except sqlite3.IntegrityError as e:
        if "UNIQUE constraint failed: applications.job_url" not in str(e):
            raise
        c.execute("SELECT id FROM applications WHERE job_url = ?", (job_url,))
        row = c.fetchone()
        return (row["id"], False) if row else (None, False)
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
        LIMIT 1
    """, (company,))
    row = c.fetchone()
    conn.close()
    if row and row["email"] and "@" in row["email"]:
        domain = row["email"].split("@")[1]   # "collective.com"
        return domain.split(".")[0]            # "collective"
    return None