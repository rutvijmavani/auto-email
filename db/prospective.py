# db/prospective.py — Prospective company DB operations

from db.connection import get_conn


def add_prospective_company(company, priority=0):
    """
    Add a company to the prospective list.
    Silently ignores duplicates (INSERT OR IGNORE).
    Returns True if newly inserted, False if already existed.
    """
    conn = get_conn()
    c = conn.cursor()
    c.execute("""
        INSERT OR IGNORE INTO prospective_companies (company, priority, status)
        VALUES (?, ?, 'pending')
    """, (company.strip(), priority))
    conn.commit()
    inserted = c.rowcount > 0
    conn.close()
    return inserted


def get_pending_prospective(limit=None):
    """
    Return prospective companies with status = 'pending' (not yet scraped).
    Ordered by priority DESC, then created_at ASC (higher priority first,
    earlier added first within same priority).
    """
    conn = get_conn()
    c = conn.cursor()
    query = """
        SELECT id, company, priority
        FROM prospective_companies
        WHERE status = 'pending'
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


def mark_prospective_scraped(company):
    """Mark prospective company as scraped — recruiters found."""
    conn = get_conn()
    c = conn.cursor()
    c.execute("""
        UPDATE prospective_companies
        SET status = 'scraped', scraped_at = CURRENT_TIMESTAMP
        WHERE company = ? AND status = 'pending'
    """, (company,))
    conn.commit()
    conn.close()


def mark_prospective_exhausted(company):
    """Mark prospective company as exhausted — no recruiters found."""
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
    conn = get_conn()
    c = conn.cursor()
    c.execute("""
        SELECT * FROM prospective_companies WHERE company = ?
    """, (company,))
    row = c.fetchone()
    conn.close()
    return dict(row) if row else None