# db/pipeline_alerts.py — CRUD for pipeline_alerts table
#
# Tracks alerts to prevent duplicate emails.
# Critical alerts sent once per 24h per platform.

from datetime import datetime, timedelta
from db.connection import get_conn
from config import ALERT_DEDUP_HOURS


# Alert types
ALERT_RATE_LIMIT  = "rate_limit"
ALERT_UNREACHABLE = "unreachable"
ALERT_SLOW        = "slow_response"
ALERT_SERPER_LOW  = "serper_low"
ALERT_SERPER_DONE = "serper_exhausted"
ALERT_CRASH       = "crash"

# Severity
CRITICAL = "critical"
WARNING  = "warning"


def create_alert(alert_type, severity, platform=None,
                 value=None, threshold=None, message=None):
    """
    Create a new alert record.
    Returns alert id or None if duplicate within dedup window.
    """
    if has_recent_alert(alert_type, platform):
        return None  # already alerted recently

    conn = get_conn()
    try:
        cursor = conn.execute("""
            INSERT INTO pipeline_alerts
                (alert_type, severity, platform,
                 value, threshold, message)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (alert_type, severity, platform,
              value, threshold, message))
        conn.commit()
        return cursor.lastrowid
    finally:
        conn.close()


def has_recent_alert(alert_type, platform=None,
                     hours=None):
    """
    Check if same alert was sent recently.
    Prevents duplicate emails within dedup window.
    """
    hours   = hours or ALERT_DEDUP_HOURS
    cutoff  = (datetime.now() - timedelta(hours=hours)
               ).isoformat()
    conn    = get_conn()
    try:
        if platform:
            row = conn.execute("""
                SELECT id FROM pipeline_alerts
                WHERE alert_type = ?
                AND platform = ?
                AND notified = 1
                AND notified_at > ?
            """, (alert_type, platform, cutoff)).fetchone()
        else:
            row = conn.execute("""
                SELECT id FROM pipeline_alerts
                WHERE alert_type = ?
                AND notified = 1
                AND notified_at > ?
            """, (alert_type, cutoff)).fetchone()
        return row is not None
    finally:
        conn.close()


def mark_notified(alert_id):
    """Mark alert as sent."""
    conn = get_conn()
    try:
        conn.execute("""
            UPDATE pipeline_alerts
            SET notified = 1, notified_at = ?
            WHERE id = ?
        """, (datetime.now().isoformat(), alert_id))
        conn.commit()
    finally:
        conn.close()


def get_pending_warnings():
    """
    Get unnotified WARNING alerts for daily digest.
    Returns list of dicts.
    """
    conn = get_conn()
    try:
        rows = conn.execute("""
            SELECT * FROM pipeline_alerts
            WHERE severity = 'warning'
            AND notified = 0
            ORDER BY created_at DESC
        """).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def mark_warnings_sent(alert_ids):
    """Mark multiple warning alerts as sent."""
    if not alert_ids:
        return
    conn = get_conn()
    try:
        now = datetime.now().isoformat()
        placeholders = ",".join("?" * len(alert_ids))
        conn.execute(f"""
            UPDATE pipeline_alerts
            SET notified = 1, notified_at = ?
            WHERE id IN ({placeholders})
        """, [now] + list(alert_ids))
        conn.commit()
    finally:
        conn.close()