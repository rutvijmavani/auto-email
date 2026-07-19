"""
db/gmail_tokens.py — DB operations for gmail_tokens table.

One row per user. Stores encrypted OAuth refresh token and Gmail watch state.
All callers work with plaintext tokens — encryption/decryption is handled here.
"""

import os

from cryptography.fernet import Fernet

from db.connection import get_conn
from logger import get_logger

logger = get_logger(__name__)

_fernet: "Fernet | None" = None


def _get_fernet() -> Fernet:
    global _fernet
    if _fernet is None:
        key = os.environ.get("GMAIL_TOKEN_ENCRYPTION_KEY", "")
        if not key:
            raise RuntimeError("GMAIL_TOKEN_ENCRYPTION_KEY is not set")
        _fernet = Fernet(key.encode())
    return _fernet


def _encrypt(plaintext: str) -> str:
    return _get_fernet().encrypt(plaintext.encode()).decode()


def _decrypt(ciphertext: str) -> str:
    return _get_fernet().decrypt(ciphertext.encode()).decode()


def upsert_token(user_id: int, gmail_email: str, refresh_token: str) -> None:
    """Store (or replace) the OAuth refresh token for a user."""
    token_enc = _encrypt(refresh_token)
    with get_conn() as conn:
        conn.execute("""
            INSERT INTO gmail_tokens (user_id, gmail_email, refresh_token_enc, updated_at)
            VALUES (%s, %s, %s, NOW())
            ON CONFLICT (user_id) DO UPDATE
                SET gmail_email       = EXCLUDED.gmail_email,
                    refresh_token_enc = EXCLUDED.refresh_token_enc,
                    updated_at        = NOW()
        """, (user_id, gmail_email, token_enc))
    logger.info("upserted gmail token for user_id=%s email=%s", user_id, gmail_email)


def update_watch(user_id: int, watch_id: str, expires_at: str) -> None:
    """Store the Gmail watch ID and expiry after calling gmail.users.watch()."""
    with get_conn() as conn:
        conn.execute("""
            UPDATE gmail_tokens
            SET watch_id         = %s,
                watch_expires_at = %s,
                updated_at       = NOW()
            WHERE user_id = %s
        """, (watch_id, expires_at, user_id))


def update_history_id(user_id: int, history_id: str) -> None:
    """Update the last processed historyId after handling a Pub/Sub notification."""
    with get_conn() as conn:
        conn.execute("""
            UPDATE gmail_tokens
            SET last_history_id = %s,
                updated_at      = NOW()
            WHERE user_id = %s
        """, (history_id, user_id))


def get_token(user_id: int) -> "dict | None":
    """
    Return the token row for a user with refresh_token decrypted.
    Returns None if the user has no token stored.
    """
    with get_conn() as conn:
        row = conn.execute("""
            SELECT user_id, gmail_email, refresh_token_enc,
                   watch_id, watch_expires_at, last_history_id
            FROM gmail_tokens
            WHERE user_id = %s
        """, (user_id,)).fetchone()
    if row is None:
        return None
    result = dict(row)
    result["refresh_token"] = _decrypt(result.pop("refresh_token_enc"))
    return result


def get_token_by_email(gmail_email: str) -> "dict | None":
    """
    Return the token row for a user identified by their Gmail address.
    Used by email_processor to look up the right user from a Pub/Sub notification.
    """
    with get_conn() as conn:
        row = conn.execute("""
            SELECT user_id, gmail_email, refresh_token_enc,
                   watch_id, watch_expires_at, last_history_id
            FROM gmail_tokens
            WHERE gmail_email = %s
        """, (gmail_email,)).fetchone()
    if row is None:
        return None
    result = dict(row)
    result["refresh_token"] = _decrypt(result.pop("refresh_token_enc"))
    return result


def get_all_tokens() -> list:
    """
    Return all token rows (refresh tokens decrypted).
    Used by renew_gmail_watch.py to check expiry for all users.
    """
    with get_conn() as conn:
        rows = conn.execute("""
            SELECT user_id, gmail_email, refresh_token_enc,
                   watch_id, watch_expires_at, last_history_id
            FROM gmail_tokens
        """).fetchall()
    result = []
    for row in rows:
        r = dict(row)
        r["refresh_token"] = _decrypt(r.pop("refresh_token_enc"))
        result.append(r)
    return result
