# db/serper_quota.py — Serper.dev API credit tracking
#
# Tracks total credit usage against 2500 free credits.
# Sends email alert when credits drop below 50.
# Serper credits don't reset daily — they're a one-time pool.

from db.connection import get_conn
from config import SERPER_TOTAL_LIMIT, SERPER_LOW_CREDIT_THRESHOLD


def get_serper_credits():
    """
    Get current Serper credit usage.
    Returns dict: {credits_used, credits_limit, credits_remaining}
    """
    conn = get_conn()
    try:
        row = conn.execute("""
            SELECT credits_used, credits_limit
            FROM serper_quota
            WHERE id = 1
        """).fetchone()

        if row:
            used  = row["credits_used"]
            limit = row["credits_limit"]
        else:
            used  = 0
            limit = SERPER_TOTAL_LIMIT

        return {
            "credits_used":      used,
            "credits_limit":     limit,
            "credits_remaining": max(0, limit - used),
        }
    finally:
        conn.close()


def increment_serper_credits(count=1):
    """
    Increment Serper credit usage by count.
    Sends low credit email alert if threshold crossed.
    Returns updated credits dict.
    """
    conn = get_conn()
    try:
        # Ensure row exists
        conn.execute("""
            INSERT OR IGNORE INTO serper_quota
                (id, credits_used, credits_limit)
            VALUES (1, 0, ?)
        """, (SERPER_TOTAL_LIMIT,))

        conn.execute("""
            UPDATE serper_quota
            SET credits_used   = credits_used + ?,
                last_updated   = CURRENT_TIMESTAMP
            WHERE id = 1
        """, (count,))

        conn.commit()
    finally:
        conn.close()

    credits = get_serper_credits()

    # Send low credit alert if threshold crossed
    _check_low_credit_alert(credits)

    return credits


def has_serper_credits(needed=2):
    """
    Check if enough credits remain.
    Default 2 = one company (Workday + Oracle queries).
    """
    credits = get_serper_credits()
    return credits["credits_remaining"] >= needed


def _check_low_credit_alert(credits):
    """
    Send email alert when credits drop below threshold.
    Only sends once per threshold crossing.
    """
    remaining = credits["credits_remaining"]

    if remaining > SERPER_LOW_CREDIT_THRESHOLD:
        return

    # Check if alert already sent for this level
    conn = get_conn()
    try:
        row = conn.execute("""
            SELECT low_credit_alert_sent
            FROM serper_quota WHERE id = 1
        """).fetchone()

        if row and row["low_credit_alert_sent"]:
            return  # Already sent

        # Mark alert as sent
        conn.execute("""
            UPDATE serper_quota
            SET low_credit_alert_sent = 1
            WHERE id = 1
        """)
        conn.commit()
    finally:
        conn.close()

    # Send email
    try:
        _send_low_credit_email(remaining)
    except Exception as e:
        print(f"[WARNING] Failed to send Serper low credit alert: {e}")


def _send_low_credit_email(remaining):
    """Send low credit alert email."""
    from outreach.report_templates.base import send_email

    subject = f"[Alert] Serper API — only {remaining} credits remaining"

    body = f"""
    <h2>Serper API Low Credit Alert</h2>
    <p>Your Serper.dev API credits are running low.</p>

    <table>
      <tr><td><strong>Credits remaining:</strong></td>
          <td>{remaining} / {SERPER_TOTAL_LIMIT}</td></tr>
      <tr><td><strong>Threshold:</strong></td>
          <td>{SERPER_LOW_CREDIT_THRESHOLD}</td></tr>
    </table>

    <h3>What Serper is used for:</h3>
    <p>Detecting Workday and Oracle HCM companies
       (~2 queries per company).</p>

    <h3>Options:</h3>
    <ol>
      <li>Buy more credits at serper.dev ($50 for 50k queries)</li>
      <li>Switch to Brave Search API ($3-5 per 1000 queries)</li>
      <li>Use SeleniumBase UC Mode (free, browser-based)</li>
    </ol>

    <p>Use <code>--monitor-status</code> to check current credit usage.</p>
    """

    send_email(subject, body)
    print(f"[ALERT] Serper low credit email sent ({remaining} remaining)")


def reset_low_credit_alert():
    """Reset low credit alert flag (after buying more credits)."""
    conn = get_conn()
    try:
        conn.execute("""
            UPDATE serper_quota
            SET low_credit_alert_sent = 0
            WHERE id = 1
        """)
        conn.commit()
    finally:
        conn.close()