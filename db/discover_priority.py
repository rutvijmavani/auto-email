# db/discover_priority.py — discover_priority table: the user's personal High/Maybe/Skip flag per FEIN
#
# Set from the Discover page while reviewing companies. No row = unreviewed. Writes never raise:
# they return True on success, False on a bad level or a DB error (logged).

from db.connection import get_conn
from logger import get_logger

logger = get_logger(__name__)

LEVEL_HIGH = "high"
LEVEL_MAYBE = "maybe"
LEVEL_SKIP = "skip"
LEVEL_UNREVIEWED = "unreviewed"   # filter value only — never stored
LEVELS = (LEVEL_HIGH, LEVEL_MAYBE, LEVEL_SKIP)


def set_level(fein, level):
    """Save (or change) the flag for a FEIN. Returns True when saved."""
    if level not in LEVELS or not fein:
        return False
    conn = get_conn()
    try:
        conn.execute("""
            INSERT INTO discover_priority (employer_fein, level, updated_at)
            VALUES (?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT (employer_fein) DO UPDATE
            SET level = EXCLUDED.level, updated_at = CURRENT_TIMESTAMP
        """, (str(fein), level))
        conn.commit()
        return True
    except Exception as e:
        conn.rollback()
        logger.warning("discover_priority.set_level(%s, %s) failed: %s", fein, level, e)
        return False
    finally:
        conn.close()


def clear_level(fein):
    """Remove the flag (back to unreviewed). Returns True when the statement ran."""
    if not fein:
        return False
    conn = get_conn()
    try:
        conn.execute("DELETE FROM discover_priority WHERE employer_fein = ?", (str(fein),))
        conn.commit()
        return True
    except Exception as e:
        conn.rollback()
        logger.warning("discover_priority.clear_level(%s) failed: %s", fein, e)
        return False
    finally:
        conn.close()


def get_level(fein):
    """The stored level for a FEIN, or None when unreviewed."""
    conn = get_conn()
    try:
        row = conn.execute(
            "SELECT level FROM discover_priority WHERE employer_fein = ?", (str(fein),)
        ).fetchone()
        return row["level"] if row else None
    finally:
        conn.close()
