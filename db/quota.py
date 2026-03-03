# db/quota.py — CareerShift and Gemini quota helpers

from datetime import datetime
from db.connection import get_conn, DAILY_LIMITS


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
            remaining = MAX(0, remaining - ?)
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
    return current < limit


def increment_usage(model):
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