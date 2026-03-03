# db/applications.py — Application table helpers

import sqlite3
from datetime import datetime

from db.connection import get_conn


def add_application(company, job_url, job_title=None, applied_date=None):
    """
    Insert a new application.
    Returns (application_id, created) where created=True means newly inserted,
    created=False means the URL already existed in DB.
    """
    conn = get_conn()
    c = conn.cursor()
    try:
        c.execute("""
            INSERT INTO applications (company, job_url, job_title, applied_date)
            VALUES (?, ?, ?, ?)
        """, (company, job_url, job_title,
              applied_date or datetime.now().strftime("%Y-%m-%d")))
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