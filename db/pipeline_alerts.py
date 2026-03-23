# db/pipeline_alerts.py — CRUD for pipeline_alerts table
#
# Tracks alerts to prevent duplicate emails.
# Critical alerts sent once per 24h per platform.
# Pipeline health alerts (metric1/metric2) checked after --find-only.

from datetime import datetime, timedelta
from db.connection import get_conn
from config import ALERT_DEDUP_HOURS


# ─────────────────────────────────────────
# ALERT TYPE CONSTANTS
# ─────────────────────────────────────────

# Existing alert types — used by base.py, do not rename
ALERT_RATE_LIMIT  = "rate_limit"
ALERT_UNREACHABLE = "unreachable"
ALERT_SLOW        = "slow_response"
ALERT_SERPER_LOW  = "serper_low"
ALERT_SERPER_DONE = "serper_exhausted"
ALERT_CRASH       = "crash"

# Pipeline performance alert types — used by check_pipeline_health()
ALERT_METRIC1_LOW   = "metric1_low"
ALERT_METRIC2_LOW   = "metric2_low"
ALERT_API_FAILURE   = "api_failure_rate"
ALERT_COVERAGE_DROP = "coverage_drop"

# Severity
CRITICAL = "critical"
WARNING  = "warning"


# ─────────────────────────────────────────
# CREATE / DEDUP
# ─────────────────────────────────────────

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


def has_recent_alert(alert_type, platform=None, hours=None):
    """
    Check if same alert was sent recently.
    Prevents duplicate emails within dedup window.
    """
    hours  = hours or ALERT_DEDUP_HOURS
    cutoff = (datetime.now() - timedelta(hours=hours)).isoformat()
    conn   = get_conn()
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


# ─────────────────────────────────────────
# MARK NOTIFIED
# ─────────────────────────────────────────

def mark_notified(alert_id):
    """Mark a single alert as sent."""
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


def mark_warnings_sent(alert_ids):
    """Mark multiple warning alerts as sent."""
    if not alert_ids:
        return
    conn = get_conn()
    try:
        now          = datetime.now().isoformat()
        placeholders = ",".join("?" * len(alert_ids))
        conn.execute(f"""
            UPDATE pipeline_alerts
            SET notified = 1, notified_at = ?
            WHERE id IN ({placeholders})
        """, [now, *alert_ids])
        conn.commit()
    finally:
        conn.close()


# ─────────────────────────────────────────
# QUERY
# ─────────────────────────────────────────

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


def get_unnotified_alerts():
    """
    Get all unnotified alerts regardless of severity.
    Used by pipeline.py to send pending alert emails
    after --find-only and --monitor-jobs runs.
    """
    conn = get_conn()
    try:
        rows = conn.execute("""
            SELECT * FROM pipeline_alerts
            WHERE notified = 0
            ORDER BY created_at ASC
        """).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


# ─────────────────────────────────────────
# PIPELINE HEALTH CHECK — metric1 + metric2
# ─────────────────────────────────────────

def check_pipeline_health():
    """
    Check if metric1 or metric2 have been below threshold
    for METRIC_ALERT_CONSECUTIVE_DAYS consecutive days.

    Returns list of alert dicts (same shape as check_quota_health).
    Empty list = healthy.

    Called by pipeline.py after --find-only completes,
    same pattern as run_quota_report().
    """
    from config import (
        METRIC1_ALERT_THRESHOLD,
        METRIC2_ALERT_THRESHOLD,
        METRIC_ALERT_CONSECUTIVE_DAYS,
    )
    from db.alerts import get_coverage_stats
    import logging
    logger = logging.getLogger(__name__)

    alerts = []
    days   = METRIC_ALERT_CONSECUTIVE_DAYS
    stats  = get_coverage_stats(days=days)

    # Need exactly N days of data to check streak
    if len(stats) < days:
        logger.debug(
            "check_pipeline_health: only %d/%d days of data — skipping",
            len(stats), days
        )
        return []

    # ── Metric 1 streak check ──
    metric1_values = [
        s["metric1"] for s in stats
        if s.get("metric1") is not None
    ]
    if len(metric1_values) == days:
        all_below = all(v < METRIC1_ALERT_THRESHOLD
                        for v in metric1_values)
        if all_below:
            avg      = sum(metric1_values) / len(metric1_values)
            logger.warning(
                "check_pipeline_health: metric1 below %.0f%% "
                "for %d days (avg=%.1f%%)",
                METRIC1_ALERT_THRESHOLD, days, avg
            )
            alert_id = create_alert(
                alert_type=ALERT_METRIC1_LOW,
                severity=CRITICAL,
                value=round(avg, 1),
                threshold=METRIC1_ALERT_THRESHOLD,
                message=(
                    f"Find-only performance below "
                    f"{METRIC1_ALERT_THRESHOLD}% for {days} "
                    f"consecutive days (avg {avg:.1f}%)"
                ),
            )
            if alert_id:
                alerts.append({
                    "alert_id":   alert_id,
                    "alert_type": ALERT_METRIC1_LOW,
                    "severity":   CRITICAL,
                    "value":      round(avg, 1),
                    "threshold":  METRIC1_ALERT_THRESHOLD,
                    "history":    stats,
                    "message": (
                        f"Find-only performance below "
                        f"{METRIC1_ALERT_THRESHOLD}% for {days} "
                        f"consecutive days (avg {avg:.1f}%)"
                    ),
                })

    # ── Metric 2 streak check ──
    metric2_values = [
        s["metric2"] for s in stats
        if s.get("metric2") is not None
    ]
    if len(metric2_values) == days:
        all_below = all(v < METRIC2_ALERT_THRESHOLD
                        for v in metric2_values)
        if all_below:
            avg      = sum(metric2_values) / len(metric2_values)
            logger.warning(
                "check_pipeline_health: metric2 below %.0f%% "
                "for %d days (avg=%.1f%%)",
                METRIC2_ALERT_THRESHOLD, days, avg
            )
            alert_id = create_alert(
                alert_type=ALERT_METRIC2_LOW,
                severity=CRITICAL,
                value=round(avg, 1),
                threshold=METRIC2_ALERT_THRESHOLD,
                message=(
                    f"Outreach coverage below "
                    f"{METRIC2_ALERT_THRESHOLD}% for {days} "
                    f"consecutive days (avg {avg:.1f}%)"
                ),
            )
            if alert_id:
                alerts.append({
                    "alert_id":   alert_id,
                    "alert_type": ALERT_METRIC2_LOW,
                    "severity":   CRITICAL,
                    "value":      round(avg, 1),
                    "threshold":  METRIC2_ALERT_THRESHOLD,
                    "history":    stats,
                    "message": (
                        f"Outreach coverage below "
                        f"{METRIC2_ALERT_THRESHOLD}% for {days} "
                        f"consecutive days (avg {avg:.1f}%)"
                    ),
                })

    return alerts


# ─────────────────────────────────────────
# API HEALTH CHECK — per-platform failure rate
# ─────────────────────────────────────────

def check_api_health() -> list:
    """
    Check if any platform's error rate has exceeded
    API_FAILURE_RATE_THRESHOLD for API_FAILURE_CONSECUTIVE_DAYS
    consecutive days.

    Returns list of alert dicts. Empty list = healthy.
    Called by pipeline.py after --monitor-jobs completes,
    same pattern as check_pipeline_health().
    """
    from config import (
        API_FAILURE_RATE_THRESHOLD,
        API_FAILURE_CONSECUTIVE_DAYS,
    )
    from db.api_health import get_health_summary
    import logging
    logger = logging.getLogger(__name__)

    alerts  = []
    days    = API_FAILURE_CONSECUTIVE_DAYS
    summary = get_health_summary(days=days)

    for row in summary:
        platform   = row["platform"]
        error_pct  = row.get("error_pct", 0) or 0
        total_reqs = row.get("total_requests", 0) or 0

        # Skip platforms with too few requests to be meaningful
        if total_reqs < days:
            continue

        threshold_pct = API_FAILURE_RATE_THRESHOLD * 100

        if error_pct >= threshold_pct:
            logger.warning(
                "check_api_health: platform=%s error_rate=%.1f%% "
                "threshold=%.1f%% over %d days",
                platform, error_pct, threshold_pct, days
            )
            alert_id = create_alert(
                alert_type=ALERT_API_FAILURE,
                severity=CRITICAL,
                platform=platform,
                value=round(error_pct, 1),
                threshold=threshold_pct,
                message=(
                    f"{platform} API error rate {error_pct:.1f}% "
                    f"over last {days} days "
                    f"(threshold {threshold_pct:.0f}%)"
                ),
            )
            if alert_id:
                alerts.append({
                    "alert_id":   alert_id,
                    "alert_type": ALERT_API_FAILURE,
                    "severity":   CRITICAL,
                    "platform":   platform,
                    "value":      round(error_pct, 1),
                    "threshold":  threshold_pct,
                    "message": (
                        f"{platform} API error rate {error_pct:.1f}% "
                        f"over last {days} days "
                        f"(threshold {threshold_pct:.0f}%)"
                    ),
                })

    return alerts