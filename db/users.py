"""db/users.py — User record accessors."""

from logger import get_logger
from db.connection import get_conn

logger = get_logger(__name__)


def get_all_active_users() -> list:
    """Return all active users ordered by id ASC."""
    conn = get_conn()
    try:
        rows = conn.execute(
            "SELECT id, email, name, resume_path FROM users WHERE is_active = TRUE ORDER BY id"
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def get_user(user_id: int) -> dict | None:
    """Return full user row as dict, or None if not found."""
    conn = get_conn()
    try:
        row = conn.execute(
            "SELECT id, email, name, resume_path, is_active FROM users WHERE id = %s",
            (user_id,)
        ).fetchone()
        return dict(row) if row else None
    finally:
        conn.close()


def get_user_email(user_id: int) -> str:
    """
    Return the notification delivery address for user_id.

    This is the canonical address for quota alerts and digest emails.
    It is NOT necessarily the same as GMAIL_USER_{id}_EMAIL (the SMTP
    send-from address) — the two may differ when the user has an alias.
    """
    conn = get_conn()
    try:
        row = conn.execute(
            "SELECT email FROM users WHERE id = %s", (user_id,)
        ).fetchone()
        if not row:
            raise ValueError(f"get_user_email: no user found with id={user_id}")
        return row["email"]
    finally:
        conn.close()


def get_user_name(user_id: int) -> str:
    """Return display name for user_id — used in alert email subjects."""
    conn = get_conn()
    try:
        row = conn.execute(
            "SELECT name FROM users WHERE id = %s", (user_id,)
        ).fetchone()
        if not row:
            raise ValueError(f"get_user_name: no user found with id={user_id}")
        return row["name"]
    finally:
        conn.close()


def get_resume_path(user_id: int) -> str:
    """
    Return the resume filename (relative to repo root) for user_id.
    Used by email_sender.py to attach the correct resume at send time.
    Raises ValueError if user not found — prevents silently attaching
    the wrong resume to an outreach email.
    """
    conn = get_conn()
    try:
        row = conn.execute(
            "SELECT resume_path FROM users WHERE id = %s", (user_id,)
        ).fetchone()
        if not row:
            raise ValueError(f"get_resume_path: no user found with id={user_id}")
        return row["resume_path"]
    finally:
        conn.close()
