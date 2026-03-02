# db/outreach.py — Outreach table helpers

from datetime import datetime, timedelta
from db.connection import get_conn
from db.recruiters import mark_recruiter_inactive


def schedule_outreach(recruiter_id, application_id, stage, scheduled_for):
    conn = get_conn()
    c = conn.cursor()
    c.execute("""
        INSERT INTO outreach (recruiter_id, application_id, stage, scheduled_for)
        VALUES (?, ?, ?, ?)
    """, (recruiter_id, application_id, stage, scheduled_for))
    conn.commit()
    oid = c.lastrowid
    conn.close()
    return oid


def schedule_next_outreach(recruiter_id, application_id):
    """
    Schedule the next stage after a sent email.
    Stage flow: initial → followup1 → followup2 → done
    Returns new outreach id or None if sequence complete.
    """
    conn = get_conn()
    c = conn.cursor()
    c.execute("""
        SELECT stage FROM outreach
        WHERE recruiter_id = ? AND application_id = ?
        AND status = 'sent'
        ORDER BY id DESC LIMIT 1
    """, (recruiter_id, application_id))
    row = c.fetchone()
    conn.close()

    if not row:
        return None

    next_stage_map = {
        "initial":   "followup1",
        "followup1": "followup2",
        "followup2": None,
    }
    next_stage = next_stage_map.get(row["stage"])

    if not next_stage:
        print(f"   [OK] Outreach sequence complete for recruiter_id={recruiter_id}")
        return None

    from config import SEND_INTERVAL_DAYS
    scheduled_for = (datetime.now() + timedelta(days=SEND_INTERVAL_DAYS)).strftime("%Y-%m-%d")
    oid = schedule_outreach(recruiter_id, application_id, next_stage, scheduled_for)
    print(f"   [INFO] Scheduled {next_stage} for {scheduled_for}")
    return oid


def get_pending_outreach():
    conn = get_conn()
    c = conn.cursor()
    c.execute("""
        SELECT o.*, r.name, r.email, r.company, a.job_url, a.job_title
        FROM outreach o
        JOIN recruiters r ON r.id = o.recruiter_id
        JOIN applications a ON a.id = o.application_id
        WHERE o.status = 'pending'
        AND o.scheduled_for <= DATE('now')
        AND o.replied = 0
        AND r.recruiter_status = 'active'
        ORDER BY o.scheduled_for ASC
    """)
    rows = [dict(r) for r in c.fetchall()]
    conn.close()
    return rows


def mark_outreach_sent(outreach_id):
    conn = get_conn()
    c = conn.cursor()
    c.execute("""
        UPDATE outreach
        SET status = 'sent', sent_at = CURRENT_TIMESTAMP
        WHERE id = ?
    """, (outreach_id,))
    conn.commit()
    conn.close()


def mark_outreach_failed(outreach_id):
    conn = get_conn()
    c = conn.cursor()
    c.execute("UPDATE outreach SET status = 'failed' WHERE id = ?", (outreach_id,))
    conn.commit()
    conn.close()


def mark_outreach_bounced(outreach_id, recruiter_id):
    """Mark outreach as bounced and deactivate the recruiter."""
    conn = get_conn()
    c = conn.cursor()
    c.execute("UPDATE outreach SET status = 'bounced' WHERE id = ?", (outreach_id,))
    conn.commit()
    conn.close()
    mark_recruiter_inactive(recruiter_id, reason="email bounced")


def has_pending_or_sent_outreach(recruiter_id, application_id):
    conn = get_conn()
    c = conn.cursor()
    c.execute("""
        SELECT id FROM outreach
        WHERE recruiter_id = ? AND application_id = ?
        AND status IN ('pending', 'sent')
    """, (recruiter_id, application_id))
    row = c.fetchone()
    conn.close()
    return row is not None