"""
scripts/renew_gmail_watch.py — Daily Gmail watch renewal.

Gmail push notifications require an active watch on each user's inbox.
Watches expire after 7 days. This script runs daily at 2 AM (via cron) and
renews any watch expiring within the next 48 hours.

Each user is renewed independently based on their own watch_expires_at timestamp.
On renewal, also calls history.list from last_history_id to catch up on any
emails that arrived during a potential gap since the last processing cycle.

Usage:
  python scripts/renew_gmail_watch.py
"""

import os
import sys
from datetime import datetime, timedelta, timezone

import google.auth.exceptions
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from db.gmail_tokens import get_all_tokens, update_watch, update_history_id
from logger import get_logger, init_logging
from workers.redis_client import get_redis
from config import REDIS_EMAIL_PUSH

init_logging("renew_gmail_watch")
logger = get_logger(__name__)

_CLIENT_ID     = os.environ.get("GMAIL_CLIENT_ID", "")
_CLIENT_SECRET = os.environ.get("GMAIL_CLIENT_SECRET", "")
_PUBSUB_TOPIC  = os.environ.get("GMAIL_PUBSUB_TOPIC", "")
_RENEW_BEFORE_HOURS = 48  # renew watches expiring within this many hours


def _build_gmail_service(token_row: dict):
    creds = Credentials(
        token=None,
        refresh_token=token_row["refresh_token"],
        token_uri="https://oauth2.googleapis.com/token",
        client_id=_CLIENT_ID,
        client_secret=_CLIENT_SECRET,
    )
    creds.refresh(Request())
    return build("gmail", "v1", credentials=creds, cache_discovery=False)


def _catch_up_missed(gmail, user_id: int, last_history_id: "str | None") -> bool:
    """
    After renewing a watch, call history.list from last_history_id to enqueue
    any messages that arrived since the last processing cycle (gap coverage).
    Each found message_id is pushed to REDIS_EMAIL_PUSH as a minimal payload
    so email_processor picks it up normally.

    Returns True on success (or when there is nothing to catch up), False on
    any error so the caller can skip watch renewal rather than advancing the
    history cursor over un-processed messages.
    """
    if not last_history_id:
        logger.info("no last_history_id for user_id=%s — skipping gap catch-up", user_id)
        return True

    try:
        profile = gmail.users().getProfile(userId="me").execute()
    except HttpError as exc:
        logger.warning("getProfile failed during catch-up user_id=%s: %s", user_id, exc)
        return False

    email_address = profile["emailAddress"]

    import json
    message_count = 0
    new_history_id = last_history_id
    page_token = None

    while True:
        try:
            req = gmail.users().history().list(
                userId="me",
                startHistoryId=last_history_id,
                historyTypes=["messageAdded"],
                pageToken=page_token,
            )
            resp = req.execute()
        except HttpError as exc:
            logger.warning("history.list failed during catch-up user_id=%s: %s", user_id, exc)
            return False

        # Track the latest historyId seen across all pages
        page_history_id = resp.get("historyId")
        if page_history_id:
            new_history_id = page_history_id

        for record in resp.get("history", []):
            for added in record.get("messagesAdded", []):
                msg_id = added.get("message", {}).get("id")
                if not msg_id:
                    continue
                payload = json.dumps({
                    "email": email_address,
                    "history_id": new_history_id,
                    "msg_id": msg_id,
                })
                get_redis().lpush(REDIS_EMAIL_PUSH, payload)
                message_count += 1

        page_token = resp.get("nextPageToken")
        if not page_token:
            break

    if message_count:
        logger.info(
            "gap catch-up: enqueued %d message(s) for user_id=%s",
            message_count, user_id,
        )

    # Advance cursor only after all pages processed successfully
    try:
        update_history_id(user_id, new_history_id)
    except Exception as exc:
        logger.error("failed to update last_history_id user_id=%s: %s", user_id, exc)

    return True


def main() -> int:
    tokens = get_all_tokens()
    if not tokens:
        logger.info("no users have Gmail tokens — nothing to renew")
        return 0

    now = datetime.now(timezone.utc)
    cutoff = now + timedelta(hours=_RENEW_BEFORE_HOURS)
    renewed = 0
    errors  = 0

    for token_row in tokens:
        user_id = token_row["user_id"]
        gmail_email = token_row.get("gmail_email", "")
        expires_raw = token_row.get("watch_expires_at")

        # Parse expiry — stored as ISO string
        if expires_raw:
            try:
                if isinstance(expires_raw, str):
                    expires_at = datetime.fromisoformat(expires_raw.replace("Z", "+00:00"))
                else:
                    expires_at = expires_raw  # already a datetime object
                if expires_at.tzinfo is None:
                    expires_at = expires_at.replace(tzinfo=timezone.utc)
            except (ValueError, TypeError):
                expires_at = None
        else:
            expires_at = None

        if expires_at and expires_at > cutoff:
            logger.debug(
                "user_id=%s watch expires %s — no renewal needed",
                user_id, expires_at.date(),
            )
            continue

        logger.info(
            "renewing watch for user_id=%s email=%s (expires %s)",
            user_id, gmail_email, expires_at.date() if expires_at else "unknown",
        )

        try:
            gmail = _build_gmail_service(token_row)
        except google.auth.exceptions.RefreshError as exc:
            logger.error(
                "OAuth revoked for user_id=%s email=%s — "
                "user must re-authorize at /oauth/start?user_id=%s: %s",
                user_id, gmail_email, user_id, exc,
            )
            errors += 1
            continue
        except HttpError as exc:
            if "invalid_grant" in str(exc):
                logger.error(
                    "OAuth revoked for user_id=%s email=%s — "
                    "user must re-authorize at /oauth/start?user_id=%s",
                    user_id, gmail_email, user_id,
                )
                errors += 1
                continue
            logger.error("auth failed for user_id=%s: %s", user_id, exc, exc_info=True)
            errors += 1
            continue
        except Exception as exc:
            logger.error("auth failed for user_id=%s: %s", user_id, exc, exc_info=True)
            errors += 1
            continue

        # Catch up on any missed emails before renewing (gap coverage).
        # If catch-up fails, skip renewal: renewing would advance the history
        # cursor past un-processed messages, losing them permanently.
        if not _catch_up_missed(gmail, user_id, token_row.get("last_history_id")):
            logger.error(
                "catch-up failed for user_id=%s — skipping watch renewal to avoid losing messages",
                user_id,
            )
            errors += 1
            continue

        # Renew the watch
        try:
            watch = gmail.users().watch(
                userId="me",
                body={"topicName": _PUBSUB_TOPIC, "labelIds": ["INBOX"]},
            ).execute()
            expires_ms  = int(watch["expiration"])
            new_expires = datetime.fromtimestamp(expires_ms / 1000, tz=timezone.utc).isoformat()
            new_watch_id = str(watch["historyId"])
            update_watch(user_id, new_watch_id, new_expires)
            logger.info(
                "watch renewed for user_id=%s email=%s new_expires=%s",
                user_id, gmail_email, new_expires,
            )
            renewed += 1
        except Exception as exc:
            logger.error("watch renewal failed for user_id=%s: %s", user_id, exc, exc_info=True)
            errors += 1

    logger.info("watch renewal complete: renewed=%d errors=%d", renewed, errors)
    return 1 if errors else 0


if __name__ == "__main__":
    sys.exit(main())
