# db/connection.py — Database connection and shared constants

import sqlite3
import os

DB_FILE = os.path.join(os.path.dirname(__file__), "..", "data", "recruiter_pipeline.db")

DAILY_LIMITS = {
    "gemini-2.5-flash-lite": 20,
    "gemini-2.5-flash": 20,
    "gemma-4-31b-it":  1500,
}

RPM_LIMITS = {
    "gemini-2.5-flash-lite": 10,
    "gemini-2.5-flash":  5,
    "gemma-4-31b-it": 15,
}


def get_conn():
    """Returns a connection using current DB_FILE value (supports test overrides)."""
    import db.connection as _self
    conn = sqlite3.connect(_self.DB_FILE, timeout=10)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA busy_timeout=10000")
    conn.row_factory = sqlite3.Row
    return conn