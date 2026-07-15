import os
import smtplib
from email.message import EmailMessage

from logger import get_logger
from config import EMAIL, APP_PASSWORD, RESUME_PATH

logger = get_logger(__name__)


def send_email(to_email, body, company, subject=None, user_id=1, attach_resume=True):
    """
    Send an email from user_id's Gmail account.

    Reads GMAIL_USER_{user_id}_EMAIL and GMAIL_USER_{user_id}_APP_PASSWORD;
    falls back to the operator EMAIL/APP_PASSWORD if the per-user vars are absent.

    attach_resume=False skips the PDF attachment (used for pipeline alert emails).
    """
    from_email   = os.getenv(f"GMAIL_USER_{user_id}_EMAIL")   or EMAIL
    app_password = os.getenv(f"GMAIL_USER_{user_id}_APP_PASSWORD") or APP_PASSWORD

    msg          = EmailMessage()
    msg["From"]  = from_email
    msg["To"]    = to_email
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
                logger.warning("Resume not found at %r for user_id=%d — sending without attachment",
                               resume_path, user_id)

    try:
        with smtplib.SMTP("smtp.gmail.com", 587, timeout=30) as server:
            server.starttls()
            server.login(from_email, app_password)
            server.send_message(msg)

        logger.info("Email sent user_id=%d to=%s subject=%r", user_id, to_email, msg["Subject"])
        print(f"Sent email to {to_email} | Subject: {msg['Subject']}")

    except Exception as e:
        logger.error("Email send failed user_id=%d to=%s: %s", user_id, to_email, e)
        print("Email sending failed:", e)
        raise


def _resume_path_for(user_id: int) -> str | None:
    """Return the resume path for user_id, falling back to the global RESUME_PATH."""
    try:
        from db.users import get_resume_path
        path = get_resume_path(user_id)
        return path or RESUME_PATH
    except Exception:
        return RESUME_PATH
