import csv
import time
import random
from datetime import datetime, timedelta

from config import SEND_INTERVAL_DAYS, MAX_EMAIL_COUNT
from template_engine import get_template, next_stage
from email_sender import send_email


def should_send(row):

    if row.get("replied", "NO") == "YES":
        return False

    if row.get("stage", "initial") == "stopped":
        return False

    # Email count check
    email_count = row.get("email_count", "0")

    try:
        email_count = int(email_count) if email_count else 0
    except ValueError:
        email_count = 0

    if email_count >= MAX_EMAIL_COUNT:
        return False

    # If never sent before → allow sending
    last_sent = row.get("last_email_sent", "")

    if not last_sent:
        return True

    # Date validation
    try:
        last_date = datetime.strptime(last_sent, "%Y-%m-%d")
    except ValueError:
        return False  # safer than forcing resend

    return datetime.now() - last_date > timedelta(days=SEND_INTERVAL_DAYS)


def process_leads(csv_file="recruiters.csv"):

    updated_rows = []

    with open(csv_file, newline="") as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames

        for row in reader:

            if should_send(row):

                template = get_template(
                    row.get("stage") or "initial",
                    row["name"],
                    row["company"]
                )

                if template:

                    send_email(
                        row["email"],
                        template,
                        row["company"]
                    )

                    email_count = row.get("email_count", "0")

                    try:
                        email_count = int(email_count) if email_count else 0
                    except ValueError:
                        email_count = 0

                    row["email_count"] = str(email_count + 1)
                    row["stage"] = next_stage(row.get("stage") or "initial")
                    row["last_email_sent"] = datetime.now().strftime("%Y-%m-%d")
                    if row.get("replied", "NO") != "YES":
                        row["replied"] = "NO"

                    time.sleep(random.randint(30, 90))

            updated_rows.append(row)

    with open(csv_file, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(updated_rows)