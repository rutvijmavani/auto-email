# tests/conftest.py — Shared test utilities

import os
import gc
import sqlite3


def cleanup_db(db_path):
    """
    Safely close WAL connections and remove SQLite DB files.

    - Flushes WAL via PRAGMA wal_checkpoint(TRUNCATE)
    - Runs gc.collect() to release Python references
    - Removes .db, .db-wal, .db-shm files
    - Silently skips PermissionError (Windows file locks)
    - Re-raises unexpected IOErrors
    """
    try:
        conn = sqlite3.connect(db_path)
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        conn.close()
    except sqlite3.OperationalError:
        pass  # DB doesn't exist yet — fine

    gc.collect()

    for ext in ["", "-wal", "-shm"]:
        path = db_path + ext
        if os.path.exists(path):
            try:
                os.remove(path)
            except PermissionError:
                pass  # Windows file lock — skip, next setUp will overwrite
            except OSError as e:
                raise  # Unexpected IO error — surface it