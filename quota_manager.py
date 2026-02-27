import sqlite3
from datetime import datetime, timedelta

DB = "quota.db"

DAILY_LIMITS = {
    "gemini-2.5-flash-lite": 20,
    "gemini-2.5-flash": 20
}


def _get_today():
    return datetime.now().strftime("%Y-%m-%d")


def _cleanup_old_records():
    conn = sqlite3.connect(DB)
    cursor = conn.cursor()

    cutoff = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")

    cursor.execute(
        "DELETE FROM model_usage WHERE date < ?",
        (cutoff,)
    )

    conn.commit()
    conn.close()


def _init_table():
    conn = sqlite3.connect(DB)
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS model_usage (
            model TEXT,
            date TEXT,
            count INTEGER,
            PRIMARY KEY (model, date)
        )
    """)

    conn.commit()
    conn.close()

    _cleanup_old_records()


def can_call(model):
    _init_table()

    conn = sqlite3.connect(DB)
    cursor = conn.cursor()

    today = _get_today()

    cursor.execute(
        "SELECT count FROM model_usage WHERE model=? AND date=?",
        (model, today)
    )
    row = cursor.fetchone()

    current = row[0] if row else 0
    limit = DAILY_LIMITS.get(model, 0)

    conn.close()
    return current < limit


def increment_usage(model):
    conn = sqlite3.connect(DB)
    cursor = conn.cursor()

    today = _get_today()

    cursor.execute("""
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