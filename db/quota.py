# db/quota.py — CareerShift and Gemini quota helpers

import time
from datetime import datetime
from db.connection import get_conn, DAILY_LIMITS, RPM_LIMITS
from collections import defaultdict
from logger import get_logger

logger = get_logger(__name__)


# ─────────────────────────────────────────
# RPM TRACKING (in-memory sliding window)
# ─────────────────────────────────────────

# { model: [timestamp, ...] } — resets on process restart (fine for 60s windows)
_rpm_timestamps = defaultdict(list)


def within_rpm(model):
    """Return True if model is within RPM limit using 60s sliding window."""
    limit = RPM_LIMITS.get(model)
    if not limit:
        return True
    now = time.time()
    _rpm_timestamps[model] = [
        t for t in _rpm_timestamps[model] if t > now - 60
    ]
    return len(_rpm_timestamps[model]) < limit


def _record_rpm(model):
    """Record a call timestamp for RPM tracking."""
    _rpm_timestamps[model].append(time.time())



# ─────────────────────────────────────────
# CAREERSHIFT QUOTA
# ─────────────────────────────────────────

def get_today_quota(user_id: int):
    conn = get_conn()
    c = conn.cursor()
    today = datetime.now().strftime("%Y-%m-%d")
    # Partial unique index: careershift_quota_user_date_key WHERE user_id IS NOT NULL
    c.execute("""
        INSERT INTO careershift_quota (user_id, date, total_limit, used, remaining)
        VALUES (?, ?, 50, 0, 50)
        ON CONFLICT(user_id, date) WHERE user_id IS NOT NULL DO NOTHING
    """, (user_id, today))
    conn.commit()
    c.execute("SELECT * FROM careershift_quota WHERE user_id = ? AND date = ?",
              (user_id, today))
    row = c.fetchone()
    result = dict(row)
    conn.close()
    return result


def increment_quota_used(count=1, user_id: int = 1):
    conn = get_conn()
    c = conn.cursor()
    today = datetime.now().strftime("%Y-%m-%d")
    # Ensure row exists before updating
    c.execute("""
        INSERT INTO careershift_quota (user_id, date, total_limit, used, remaining)
        VALUES (?, ?, 50, 0, 50)
        ON CONFLICT(user_id, date) WHERE user_id IS NOT NULL DO NOTHING
    """, (user_id, today))
    # GREATEST(0, remaining - ?) prevents negative remaining
    c.execute("""
        UPDATE careershift_quota
        SET used = used + ?,
            remaining = GREATEST(0, remaining - ?)
        WHERE user_id = ? AND date = ?
    """, (count, count, user_id, today))
    conn.commit()
    conn.close()
    logger.debug("increment_quota_used user_id=%d count=%d date=%s", user_id, count, today)


def get_remaining_quota(user_id: int = 1):
    quota = get_today_quota(user_id)
    return max(0, quota["remaining"])


# ─────────────────────────────────────────
# GEMINI QUOTA MANAGER
# ─────────────────────────────────────────

def _get_today():
    return datetime.now().strftime("%Y-%m-%d")


def can_call(model, user_id: int = 1, use_case: str = "email_content"):
    """Return True if model is within both daily and RPM limits for user_id/use_case."""
    conn = get_conn()
    c = conn.cursor()
    today = _get_today()
    c.execute(
        "SELECT count FROM model_usage "
        "WHERE model=? AND date=? AND use_case=? AND user_id=?",
        (model, today, use_case, user_id)
    )
    row = c.fetchone()
    current = row["count"] if row else 0
    limit = DAILY_LIMITS.get(model, 0)
    conn.close()

    if current >= limit:
        return False

    if not within_rpm(model):
        return False

    return True


def increment_usage(model, user_id: int = 1, use_case: str = "email_content"):
    """Record call — updates RPM sliding window and daily DB count."""
    _record_rpm(model)
    conn = get_conn()
    c = conn.cursor()
    today = _get_today()
    # Partial unique index: model_usage_per_user WHERE user_id IS NOT NULL
    c.execute("""
        INSERT INTO model_usage (model, date, count, user_id, use_case)
        VALUES (?, ?, 1, ?, ?)
        ON CONFLICT(model, date, use_case, user_id) WHERE user_id IS NOT NULL
        DO UPDATE SET count = model_usage.count + 1
    """, (model, today, user_id, use_case))
    conn.commit()
    conn.close()


def all_models_exhausted(user_id: int = 1):
    for model in DAILY_LIMITS:
        if can_call(model, user_id=user_id):
            return False
    return True