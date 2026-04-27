# db/quota.py — CareerShift and Gemini quota helpers

import time
from datetime import datetime
from db.connection import get_conn, DAILY_LIMITS ,RPM_LIMITS
from collections import defaultdict


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

def get_today_quota():
    conn = get_conn()
    c = conn.cursor()
    today = datetime.now().strftime("%Y-%m-%d")
    # Atomic upsert — no race condition between SELECT and INSERT
    c.execute("""
        INSERT INTO careershift_quota (date, total_limit, used, remaining)
        VALUES (?, 50, 0, 50)
        ON CONFLICT(date) DO NOTHING
    """, (today,))
    conn.commit()
    c.execute("SELECT * FROM careershift_quota WHERE date = ?", (today,))
    row = c.fetchone()
    result = dict(row)
    conn.close()
    return result


def increment_quota_used(count=1):
    conn = get_conn()
    c = conn.cursor()
    today = datetime.now().strftime("%Y-%m-%d")
    # Ensure row exists before updating
    c.execute("""
        INSERT INTO careershift_quota (date, total_limit, used, remaining)
        VALUES (?, 50, 0, 50)
        ON CONFLICT(date) DO NOTHING
    """, (today,))
    # MAX(0, remaining - ?) prevents negative remaining
    c.execute("""
        UPDATE careershift_quota
        SET used = used + ?,
            remaining = GREATEST(0, remaining - ?)
        WHERE date = ?
    """, (count, count, today))
    conn.commit()
    conn.close()


def get_remaining_quota():
    quota = get_today_quota()
    return max(0, quota["remaining"])


# ─────────────────────────────────────────
# GEMINI QUOTA MANAGER
# ─────────────────────────────────────────

def _get_today():
    return datetime.now().strftime("%Y-%m-%d")


def can_call(model):
    """Return True if model is within both daily and RPM limits."""
    conn = get_conn()
    c = conn.cursor()
    today = _get_today()
    c.execute(
        "SELECT count FROM model_usage WHERE model=? AND date=?",
        (model, today)
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


def increment_usage(model):
    """Record call — updates RPM sliding window and daily DB count."""
    _record_rpm(model)
    conn = get_conn()
    c = conn.cursor()
    today = _get_today()
    c.execute("""
        INSERT INTO model_usage (model, date, count)
        VALUES (?, ?, 1)
        ON CONFLICT(model, date)
        DO UPDATE SET count = count + 1
    """, (model, today))
    conn.commit()
    conn.close()


def all_models_exhausted():
    for model in DAILY_LIMITS:
        if can_call(model):
            return False
    return True