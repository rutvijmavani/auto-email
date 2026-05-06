# db/recruiters.py — Recruiter table helpers

import json
from datetime import datetime, timedelta

import psycopg2.errors
from db.connection import get_conn


def get_recruiters_by_company(company):
    conn = get_conn()
    c = conn.cursor()
    c.execute("""
        SELECT * FROM recruiters
        WHERE company = ? AND recruiter_status = 'active'
    """, (company,))
    rows = [dict(r) for r in c.fetchall()]
    conn.close()
    return rows


def add_recruiter(company, name, position, email, confidence):
    """Insert recruiter at company level. Returns new id or existing id."""
    conn = get_conn()
    c = conn.cursor()
    try:
        # RETURNING id replaces c.lastrowid (not available with psycopg2).
        c.execute("""
            INSERT INTO recruiters (company, name, position, email, confidence, verified_at)
            VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            RETURNING id
        """, (company, name, position, email, confidence))
        row = c.fetchone()
        conn.commit()
        return row["id"]
    except psycopg2.errors.UniqueViolation:
        conn.rollback()
        c.execute("SELECT id FROM recruiters WHERE email = ?", (email,))
        row = c.fetchone()
        return row["id"] if row else None
    finally:
        conn.close()


def update_recruiter(recruiter_id, name=None, position=None,
                     confidence=None, recruiter_status=None, email=None):
    conn = get_conn()
    c = conn.cursor()
    fields = []
    values = []
    if name is not None:
        fields.append("name = ?"); values.append(name)
    if position is not None:
        fields.append("position = ?"); values.append(position)
    if confidence is not None:
        fields.append("confidence = ?"); values.append(confidence)
    if email is not None:
        fields.append("email = ?"); values.append(email)
    if recruiter_status is not None:
        fields.append("recruiter_status = ?"); values.append(recruiter_status)
    fields.append("verified_at = CURRENT_TIMESTAMP")
    values.append(recruiter_id)
    try:
        c.execute(
            "UPDATE recruiters SET " + ", ".join(fields) + " WHERE id = ?",
            values
        )
        conn.commit()
        return True
    except psycopg2.errors.UniqueViolation:
        conn.rollback()
        # Email conflict — retry without email field to preserve other updates
        retry_fields = [f for f in fields if f != "email = ?"]
        retry_values = [v for f, v in zip(fields, values[:-1]) if f != "email = ?"]
        retry_values.append(recruiter_id)
        if len(retry_fields) > 1:  # more than just verified_at
            c.execute(
                "UPDATE recruiters SET " + ", ".join(retry_fields) + " WHERE id = ?",
                retry_values
            )
            conn.commit()
            print("[WARNING] update_recruiter: email already exists — applied non-email updates only")
            return True
        print("[WARNING] update_recruiter: email already exists — no other fields to update")
        return False
    finally:
        conn.close()


def get_recruiters_by_tier(days_tier1=30, days_tier2=60):
    """
    Return all active recruiters grouped by verification tier.
    tier1: verified < days_tier1 ago → trust
    tier2: verified days_tier1-days_tier2 ago → lightweight check
    tier3: verified > days_tier2 ago or never → full profile visit
    """
    conn = get_conn()
    c = conn.cursor()
    c.execute("""
        SELECT * FROM recruiters
        WHERE recruiter_status = 'active'
        ORDER BY verified_at ASC
    """)
    rows = [dict(r) for r in c.fetchall()]
    conn.close()

    tier1, tier2, tier3 = [], [], []
    now = datetime.now()

    for r in rows:
        if not r["verified_at"]:
            tier3.append(r)
            continue
        try:
            verified = datetime.fromisoformat(str(r["verified_at"]))
            days_ago = (now - verified).days
            if days_ago < days_tier1:
                tier1.append(r)
            elif days_ago < days_tier2:
                tier2.append(r)
            else:
                tier3.append(r)
        except (ValueError, TypeError):
            tier3.append(r)

    return {"tier1": tier1, "tier2": tier2, "tier3": tier3}


def mark_recruiter_inactive(recruiter_id, reason=""):
    """Mark a recruiter as inactive and cancel pending outreach."""
    conn = get_conn()
    c = conn.cursor()
    c.execute("""
        UPDATE recruiters
        SET recruiter_status = 'inactive', verified_at = CURRENT_TIMESTAMP
        WHERE id = ?
    """, (recruiter_id,))
    c.execute("""
        UPDATE outreach SET status = 'cancelled'
        WHERE recruiter_id = ? AND status = 'pending'
    """, (recruiter_id,))
    conn.commit()
    conn.close()
    if reason:
        print(f"[INFO] Recruiter id={recruiter_id} marked inactive: {reason}")


def recruiter_email_exists(email):
    """Returns recruiter id if exists, None otherwise."""
    conn = get_conn()
    c = conn.cursor()
    c.execute("SELECT id FROM recruiters WHERE email = ?", (email,))
    row = c.fetchone()
    conn.close()
    return row["id"] if row else None


def get_used_search_terms(company):
    """Return list of HR search terms already tried for a company."""
    conn = get_conn()
    c = conn.cursor()
    c.execute("""
        SELECT used_search_terms FROM recruiters
        WHERE company = ? AND used_search_terms IS NOT NULL
        LIMIT 1
    """, (company,))
    row = c.fetchone()
    conn.close()
    if row and row["used_search_terms"]:
        try:
            return json.loads(row["used_search_terms"])
        except (ValueError, TypeError):
            return []
    return []


def mark_search_term_used(company, term):
    """Add a search term to the used_search_terms list for a company."""
    used = get_used_search_terms(company)
    if term not in used:
        used.append(term)
    conn = get_conn()
    c = conn.cursor()
    c.execute("""
        UPDATE recruiters SET used_search_terms = ?
        WHERE company = ?
    """, (json.dumps(used), company))
    conn.commit()
    conn.close()


def update_company_last_scraped(company):
    """Update last_scraped_at for all recruiters at a company."""
    conn = get_conn()
    c = conn.cursor()
    c.execute("""
        UPDATE recruiters SET last_scraped_at = CURRENT_TIMESTAMP
        WHERE company = ?
    """, (company,))
    conn.commit()
    conn.close()


def get_existing_emails_for_company(company):
    """Return set of all emails already in DB for a company (any status)."""
    conn = get_conn()
    c = conn.cursor()
    c.execute(
        "SELECT email FROM recruiters WHERE company = ?",
        (company,)
    )
    rows = c.fetchall()
    conn.close()
    return {row["email"] for row in rows}
