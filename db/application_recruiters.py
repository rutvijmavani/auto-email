# db/application_recruiters.py — Application-Recruiter join table helpers

from db.connection import get_conn, DAILY_LIMITS
from config import MAX_CONTACTS_HARD_CAP, MAX_RECRUITERS_PER_APPLICATION


def link_recruiter_to_application(application_id, recruiter_id):
    """
    Link a recruiter to an application.
    Enforces MAX_RECRUITERS_PER_APPLICATION cap at DB level.
    Returns True if linked, False if cap already reached.
    """
    conn = get_conn()
    c = conn.cursor()
    try:
        c.execute("""
            SELECT COUNT(*) as cnt FROM application_recruiters
            WHERE application_id = ?
        """, (application_id,))
        if c.fetchone()["cnt"] >= MAX_RECRUITERS_PER_APPLICATION:
            return False  # cap reached — silent skip
        c.execute("""
            INSERT OR IGNORE INTO application_recruiters (application_id, recruiter_id)
            VALUES (?, ?)
        """, (application_id, recruiter_id))
        conn.commit()
        return True
    finally:
        conn.close()


def get_recruiters_for_application(application_id):
    conn = get_conn()
    c = conn.cursor()
    c.execute("""
        SELECT r.* FROM recruiters r
        INNER JOIN application_recruiters ar ON ar.recruiter_id = r.id
        WHERE ar.application_id = ? AND r.recruiter_status = 'active'
    """, (application_id,))
    rows = [dict(r) for r in c.fetchall()]
    conn.close()
    return rows


def get_unique_companies_needing_scraping(min_recruiters=2):
    """Return unique company names with fewer than min_recruiters active recruiters."""
    conn = get_conn()
    c = conn.cursor()
    c.execute("""
        SELECT a.id, a.company, COUNT(r.id) as recruiter_count
        FROM applications a
        LEFT JOIN application_recruiters ar ON ar.application_id = a.id
        LEFT JOIN recruiters r ON r.id = ar.recruiter_id AND r.recruiter_status = 'active'
        WHERE a.status = 'active'
        GROUP BY a.id, a.company
        HAVING recruiter_count < ?
        ORDER BY a.applied_date ASC
    """, (min_recruiters,))
    rows = c.fetchall()
    conn.close()

    seen = set()
    unique = []
    for row in rows:
        if row["company"] not in seen:
            seen.add(row["company"])
            unique.append(row["company"])
    return unique


def get_companies_needing_more_recruiters():
    """
    Return companies with fewer than MAX_CONTACTS_HARD_CAP active recruiters,
    ordered by shortage then recency.
    """
    conn = get_conn()
    c = conn.cursor()
    c.execute("""
        SELECT
            a.company,
            MAX(a.applied_date) as latest_applied,
            COUNT(DISTINCT r.id) as recruiter_count,
            (? - COUNT(DISTINCT r.id)) as shortage
        FROM applications a
        LEFT JOIN application_recruiters ar ON ar.application_id = a.id
        LEFT JOIN recruiters r ON r.id = ar.recruiter_id
            AND r.recruiter_status = 'active'
        WHERE a.status = 'active'
        GROUP BY a.company
        HAVING COUNT(DISTINCT r.id) < ?
        ORDER BY shortage DESC, latest_applied DESC
    """, (MAX_CONTACTS_HARD_CAP, MAX_CONTACTS_HARD_CAP))
    rows = [dict(r) for r in c.fetchall()]
    conn.close()
    return rows


def link_top_recruiters_for_company(application_id, company):
    """
    Link best recruiters for a company to an application.
    Picks up to MAX_RECRUITERS_PER_APPLICATION recruiters,
    ordered by confidence (auto first) then oldest (created_at ASC).
    Used during prospective → active conversion in --add flow.
    Returns count of recruiters linked.
    """
    conn = get_conn()
    c = conn.cursor()
    c.execute("""
        SELECT id FROM recruiters
        WHERE company = ? AND recruiter_status = 'active'
        ORDER BY
            CASE confidence WHEN 'auto' THEN 0 ELSE 1 END ASC,
            created_at ASC
        LIMIT ?
    """, (company, MAX_RECRUITERS_PER_APPLICATION))
    recruiters = [row["id"] for row in c.fetchall()]
    conn.close()

    linked = 0
    for recruiter_id in recruiters:
        if link_recruiter_to_application(application_id, recruiter_id):
            linked += 1
    return linked