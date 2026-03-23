# db/alerts.py — Quota health alerts and coverage stats

from datetime import datetime, timedelta
from db.connection import get_conn, DAILY_LIMITS
from config import (
    QUOTA_UNDERUTILIZED_THRESHOLD,
    QUOTA_ALERT_CONSECUTIVE_DAYS,
    MAX_CONTACTS_HARD_CAP,
)


# ─────────────────────────────────────────
# QUOTA HEALTH CHECK
# ─────────────────────────────────────────

def get_quota_history(quota_type, days=None):
    """
    Return last N days of quota records.
    quota_type: 'careershift' or 'gemini'
    """
    if days is None:
        days = QUOTA_ALERT_CONSECUTIVE_DAYS

    conn = get_conn()
    c = conn.cursor()

    if quota_type == "careershift":
        c.execute("""
            SELECT date, used, remaining, total_limit
            FROM careershift_quota
            ORDER BY date DESC
            LIMIT ?
        """, (days,))
    else:  # gemini
        c.execute("""
            SELECT date,
                   SUM(count) as used,
                   ? - SUM(count) as remaining,
                   ? as total_limit
            FROM model_usage
            GROUP BY date
            ORDER BY date DESC
            LIMIT ?
        """, (sum(DAILY_LIMITS.values()), sum(DAILY_LIMITS.values()), days))

    rows = [dict(r) for r in c.fetchall()]
    conn.close()
    return rows


def check_quota_health():
    """
    Check quota health for CareerShift and Gemini.
    Returns list of alert dicts, empty if healthy.
    """
    alerts = []
    total_gemini = sum(DAILY_LIMITS.values())

    for quota_type, total_limit in [("careershift", 50), ("gemini", total_gemini)]:
        history = get_quota_history(quota_type, QUOTA_ALERT_CONSECUTIVE_DAYS)

        if len(history) < QUOTA_ALERT_CONSECUTIVE_DAYS:
            continue

        underutilized = all(
            row["used"] < QUOTA_UNDERUTILIZED_THRESHOLD * row["total_limit"]
            for row in history
        )
        exhausted = all(row["remaining"] == 0 for row in history)

        if underutilized or exhausted:
            alert_type = "exhausted" if exhausted else "underutilized"
            avg_used = sum(r["used"] for r in history) / len(history)
            avg_remaining = sum(r["remaining"] for r in history) / len(history)

            if not _alert_already_sent(quota_type, alert_type):
                suggested = _calculate_suggested_cap(
                    alert_type, avg_used, total_limit, quota_type
                )
                alerts.append({
                    "alert_type":    alert_type,
                    "quota_type":    quota_type,
                    "start_date":    history[-1]["date"],
                    "end_date":      history[0]["date"],
                    "avg_used":      round(avg_used, 1),
                    "avg_remaining": round(avg_remaining, 1),
                    "total_limit":   total_limit,
                    "suggested_cap": suggested,
                    "history":       history,
                })

    return alerts


def _alert_already_sent(quota_type, alert_type):
    conn = get_conn()
    c = conn.cursor()
    cutoff = (datetime.now() - timedelta(days=QUOTA_ALERT_CONSECUTIVE_DAYS)).strftime("%Y-%m-%d")
    c.execute("""
        SELECT id FROM quota_alerts
        WHERE quota_type = ? AND alert_type = ?
        AND notified = 1
        AND created_at >= ?
    """, (quota_type, alert_type, cutoff))
    row = c.fetchone()
    conn.close()
    return row is not None


def _calculate_suggested_cap(alert_type, avg_used, total_limit, quota_type):
    if quota_type != "careershift":
        return None

    current_cap = MAX_CONTACTS_HARD_CAP

    if alert_type == "underutilized":
        utilization_rate = avg_used / total_limit if total_limit > 0 else 0
        if utilization_rate > 0:
            suggested = round(current_cap / utilization_rate)
            return min(suggested, 10)
        return current_cap + 2
    else:
        conn = get_conn()
        c = conn.cursor()
        c.execute("""
            SELECT AVG(used) AS avg_used
            FROM (
                SELECT used
                FROM careershift_quota
                ORDER BY date DESC
                LIMIT ?
            )
        """, (QUOTA_ALERT_CONSECUTIVE_DAYS,))
        row = c.fetchone()
        conn.close()
        avg_companies = (row["avg_used"] / current_cap) if row and row["avg_used"] else 1
        if avg_companies > 0:
            suggested = int(total_limit / avg_companies)
            return max(suggested, 1)
        return max(current_cap - 1, 1)


def save_quota_alert(alert):
    """Save alert to quota_alerts table. notified flag controlled by caller."""
    conn = get_conn()
    c = conn.cursor()
    notified = 1 if alert.get("notified", True) else 0
    c.execute("""
        INSERT INTO quota_alerts (
            alert_type, quota_type, start_date, end_date,
            avg_used, avg_remaining, suggested_cap, notified
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        alert["alert_type"],
        alert["quota_type"],
        alert["start_date"],
        alert["end_date"],
        alert["avg_used"],
        alert["avg_remaining"],
        alert.get("suggested_cap"),
        notified,
    ))
    conn.commit()
    conn.close()


# ─────────────────────────────────────────
# COVERAGE STATS
# ─────────────────────────────────────────

def save_coverage_stats(stats: dict):
    """
    Save daily pipeline performance metrics.
    Takes a dict — same pattern as save_monitor_stats().
    Uses INSERT OR REPLACE to handle re-runs on same day.

    Expected keys:
      total_applications, companies_attempted,
      auto_found, rejected_count, exhausted_count,
      metric1, metric2
    """
    conn = get_conn()
    c = conn.cursor()
    today = datetime.now().strftime("%Y-%m-%d")
    c.execute("""
        INSERT OR REPLACE INTO coverage_stats (
            date, total_applications, companies_attempted,
            auto_found, rejected_count, exhausted_count,
            metric1, metric2
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        today,
        stats.get("total_applications",  0),
        stats.get("companies_attempted", 0),
        stats.get("auto_found",          0),
        stats.get("rejected_count",      0),
        stats.get("exhausted_count",     0),
        stats.get("metric1"),
        stats.get("metric2"),
    ))
    conn.commit()
    conn.close()


def get_coverage_stats(days: int = 3) -> list:
    """Return last N days of coverage stats, newest first."""
    conn = get_conn()
    c = conn.cursor()
    c.execute("""
        SELECT * FROM coverage_stats
        ORDER BY date DESC
        LIMIT ?
    """, (days,))
    rows = [dict(r) for r in c.fetchall()]
    conn.close()
    return rows