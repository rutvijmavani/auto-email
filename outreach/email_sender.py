import smtplib
from email.message import EmailMessage
from config import EMAIL, APP_PASSWORD, RESUME_PATH


def send_email(to_email, body, company, subject=None):

    msg = EmailMessage()
    msg["From"] = EMAIL
    msg["To"] = to_email

    # Smart fallback
    if subject:
        msg["Subject"] = subject
    else:
        msg["Subject"] = f"{company} – Backend Engineer Interest"

    msg.set_content(body)

    with open(RESUME_PATH, "rb") as f:
        msg.add_attachment(
            f.read(),
            maintype="application",
            subtype="pdf",
            filename="Resume.pdf"
        )

    try:
        with smtplib.SMTP("smtp.gmail.com", 587, timeout=30) as server:
            server.starttls()
            server.login(EMAIL, APP_PASSWORD)
            server.send_message(msg)

        print(f"Sent email to {to_email} | Subject: {msg['Subject']}")

    except Exception as e:
        print("Email sending failed:", e)