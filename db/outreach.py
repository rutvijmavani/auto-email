# db/outreach.py — Outreach table helpers

from datetime import datetime, timedelta
from db.connection import get_conn
from db.recruiters import mark_recruiter_inactive
from logger import get_logger

logger = get_logger(__name__)


def schedule_outreach(recruiter_id, application_id, stage, scheduled_for,
                      user_id: int = 1):
    conn = get_conn()
    c = conn.cursor()
    # RETURNING id replaces c.lastrowid (not available with psycopg2).
    c.execute("""
        INSERT INTO outreach (recruiter_id, application_id, stage, scheduled_for,
                              user_id)
        VALUES (?, ?, ?, ?, ?)
        RETURNING id
    """, (recruiter_id, application_id, stage, scheduled_for, user_id))
    row = c.fetchone()
    conn.commit()
    conn.close()
    logger.debug("schedule_outreach id=%d user_id=%d recruiter=%d stage=%s for=%s",
                 row["id"], user_id, recruiter_id, stage, scheduled_for)
    return row["id"]


def schedule_next_outreach(recruiter_id, application_id, user_id: int = 1):
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
        logger.info("schedule_next_outreach: sequence complete recruiter_id=%d", recruiter_id)
        return None

    from config import SEND_INTERVAL_DAYS
    scheduled_for = (datetime.now() + timedelta(days=SEND_INTERVAL_DAYS)).strftime("%Y-%m-%d")

    conn = get_conn()
    c = conn.cursor()
    try:
        # PostgreSQL uses standard transactions — BEGIN IMMEDIATE (SQLite-specific
        # write-ahead lock) is not needed. Autocommit is off; the SELECT + INSERT
        # below runs inside a single implicit transaction, which is sufficient.
        c.execute("""
            SELECT id FROM outreach
            WHERE recruiter_id = ? AND application_id = ?
            AND stage = ? AND status IN ('pending', 'sent')
            LIMIT 1
        """, (recruiter_id, application_id, next_stage))
        existing = c.fetchone()
        if existing:
            conn.commit()
            logger.info("schedule_next_outreach: %s already scheduled recruiter_id=%d",
                        next_stage, recruiter_id)
            return existing["id"]
        c.execute("""
            INSERT INTO outreach (recruiter_id, application_id, stage, scheduled_for,
                                  user_id)
            VALUES (?, ?, ?, ?, ?)
            RETURNING id
        """, (recruiter_id, application_id, next_stage, scheduled_for, user_id))
        row = c.fetchone()
        conn.commit()
        logger.info("schedule_next_outreach: scheduled %s for %s recruiter_id=%d",
                    next_stage, scheduled_for, recruiter_id)
        return row["id"]
    finally:
        conn.close()


def get_pending_outreach(user_id: int | None = None):
    conn = get_conn()
    c = conn.cursor()
    # CURRENT_DATE replaces DATE('now') (SQLite-specific).
    if user_id is not None:
        c.execute("""
            SELECT o.*, r.name, r.email, r.company, a.job_url, a.job_title
            FROM outreach o
            JOIN recruiters r ON r.id = o.recruiter_id
            JOIN applications a ON a.id = o.application_id
            WHERE o.status = 'pending'
            AND o.scheduled_for <= CURRENT_DATE
            AND o.replied = 0
            AND r.recruiter_status = 'active'
            AND o.user_id = ?
            ORDER BY o.scheduled_for ASC
        """, (user_id,))
    else:
        c.execute("""
            SELECT o.*, r.name, r.email, r.company, a.job_url, a.job_title
            FROM outreach o
            JOIN recruiters r ON r.id = o.recruiter_id
            JOIN applications a ON a.id = o.application_id
            WHERE o.status = 'pending'
            AND o.scheduled_for <= CURRENT_DATE
            AND o.replied = 0
            AND r.recruiter_status = 'active'
            ORDER BY o.scheduled_for ASC
        """)
    rows = [dict(r) for r in c.fetchall()]
    conn.close()
    logger.debug("get_pending_outreach user_id=%s → %d rows", user_id, len(rows))
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
    """Mark outreach as bounced and deactivate the recruiter atomically."""
    conn = get_conn()
    c = conn.cursor()
    try:
        c.execute("UPDATE outreach SET status = 'bounced' WHERE id = ?", (outreach_id,))
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
        logger.info("mark_outreach_bounced: recruiter id=%d marked inactive (email bounced)", recruiter_id)
    finally:
        conn.close()


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
