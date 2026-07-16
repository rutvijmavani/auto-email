import os
import smtplib
from email.message import EmailMessage

from logger import get_logger
from config import EMAIL, APP_PASSWORD, RESUME_PATH

logger = get_logger(__name__)


def send_email(to_email, body, company, subject=None, user_id=None, attach_resume=True):
    """
    Send an email.

    user_id=None  → operator path: uses EMAIL / APP_PASSWORD from config.
    user_id=int   → per-user path: reads GMAIL_USER_{user_id}_EMAIL and
                    GMAIL_USER_{user_id}_APP_PASSWORD; raises ValueError if
                    either is missing so a misconfigured user never silently
                    falls back to the operator account.

    attach_resume=False skips the PDF attachment (used for pipeline alert emails).
    """
    if user_id is None:
        from_email   = EMAIL
        app_password = APP_PASSWORD
    else:
        from_email   = os.getenv(f"GMAIL_USER_{user_id}_EMAIL")
        app_password = os.getenv(f"GMAIL_USER_{user_id}_APP_PASSWORD")
        if not from_email or not app_password:
            raise ValueError(
                f"Missing GMAIL_USER_{user_id}_EMAIL or GMAIL_USER_{user_id}_APP_PASSWORD — "
                f"set these env vars before sending as user_id={user_id}"
            )

    msg            = EmailMessage()
    msg["From"]    = from_email
    msg["To"]      = to_email
    msg["Subject"] = subject or f"{company} – Backend Engineer Interest"

    msg.set_content(body)

    if attach_resume:
        resume_path = _resume_path_for(user_id)
        if resume_path:
            try:
                with open(resume_path, "rb") as f:
                    msg.add_attachment(
                        f.read(),
                        maintype="application",
                        subtype="pdf",
                        filename=os.path.basename(resume_path),
                    )
            except FileNotFoundError:
                logger.warning("Resume not found at %r for user_id=%s — sending without attachment",
                               resume_path, user_id)

    try:
        with smtplib.SMTP("smtp.gmail.com", 587, timeout=30) as server:
            server.starttls()
            server.login(from_email, app_password)
            server.send_message(msg)

        logger.info("Email sent user_id=%s to=%s subject=%r", user_id, to_email, msg["Subject"])
        print(f"Sent email to {to_email} | Subject: {msg['Subject']}")

    except Exception as e:
        logger.error("Email send failed user_id=%s to=%s: %s", user_id, to_email, e)
        print("Email sending failed:", e)
        raise


def _resume_path_for(user_id) -> str | None:
    """
    Return the resume path for user_id.
    - None (operator path) → returns global RESUME_PATH.
    - int → queries the DB; returns the user's own path or None if unconfigured.
      Never falls back to RESUME_PATH so user 2 cannot receive user 1's resume.
    """
    if user_id is None:
        return RESUME_PATH
    try:
        from db.users import get_resume_path
        path = get_resume_path(user_id)
        if not path:
            logger.warning("No resume_path configured for user_id=%d — sending without attachment", user_id)
        return path or None
    except ValueError as exc:
        logger.warning("get_resume_path failed for user_id=%d (%s) — sending without attachment", user_id, exc)
        return None
