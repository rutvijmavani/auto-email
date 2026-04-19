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

    WAL mode is persistent in SQLite — once set it survives across connections.
    Setting it again on an already-WAL database while another connection is open
    raises OperationalError("database is locked").  We catch and ignore that
    error: if another connection is already open the DB is already in WAL mode.
    """
    import db.connection as _self
    conn = sqlite3.connect(_self.DB_FILE, timeout=30)
    try:
        conn.execute("PRAGMA journal_mode=WAL")
    except sqlite3.OperationalError:
        # DB is already in WAL mode (set by a prior connection in the same
        # process).  The mode change requires exclusive access, but since
        # WAL persists on disk the connection is still fully usable.
        pass
    conn.execute("PRAGMA foreign_keys=ON")
    conn.row_factory = sqlite3.Row
    return conn