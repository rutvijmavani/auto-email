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
    """Returns a connection using current DB_FILE value (supports test overrides).

    timeout=30: when a concurrent writer holds the lock, sqlite3 retries for
    up to 30 seconds before raising OperationalError.  The default (5 s) is
    too short when 20 _process_company threads write simultaneously even under
    WAL mode (WAL serialises concurrent writers, not concurrent readers).
    """
    import db.connection as _self
    conn = sqlite3.connect(_self.DB_FILE, timeout=30)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.row_factory = sqlite3.Row
    return conn