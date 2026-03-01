"""
outreach/outreach_engine.py — Schedules and sends recruiter outreach emails.

Send window logic:
  Before 9 AM   → wait until 9 AM then start
  9 AM - 11 AM  → send normally (preferred window)
  11 AM - 12 PM → grace period, continue sending if emails still pending
  After 12 PM   → hard cutoff, reschedule remaining for tomorrow 9 AM
"""

import time
import random
import smtplib
from datetime import datetime, timedelta

try:
    from zoneinfo import ZoneInfo
except ImportError:
    from backports.zoneinfo import ZoneInfo

from config import SEND_WINDOW_START, SEND_WINDOW_END, GRACE_PERIOD_HOURS, SEND_TIMEZONE
from outreach.template_engine import get_template
from outreach.email_sender import send_email
from db.quota_manager import all_models_exhausted
from db.db import (
    init_db,
    get_pending_outreach,
    mark_outreach_sent,
    mark_outreach_failed,
    mark_outreach_bounced,
    schedule_outreach,
    schedule_next_outreach,
    has_pending_or_sent_outreach,
    get_recruiters_for_application,
    get_all_active_applications,
)

HARD_CUTOFF_HOUR = SEND_WINDOW_END + GRACE_PERIOD_HOURS  # 12 PM


def _now():
    """Return current time in configured timezone."""
    return datetime.now(ZoneInfo(SEND_TIMEZONE))


def get_send_status():
    """
    Returns:
      'wait'    — before send window, sleep until 9 AM
      'send'    — within window or grace period, send emails
      'cutoff'  — past hard cutoff, reschedule remaining for tomorrow
    """
    hour = _now().hour

    if hour < SEND_WINDOW_START:
        return "wait"

    if hour < HARD_CUTOFF_HOUR:
        return "send"

    return "cutoff"


def wait_until_window():
    """Sleep until send window opens at SEND_WINDOW_START."""
    now = _now()
    target = now.replace(hour=SEND_WINDOW_START, minute=0, second=0, microsecond=0)

    if now >= target:
        return

    wait_seconds = (target - now).total_seconds()
    print(f"[INFO] Outside send window. Waiting {int(wait_seconds // 60)} min until {SEND_WINDOW_START}:00 AM ({SEND_TIMEZONE})...")
    time.sleep(wait_seconds)
    print(f"[OK] Send window open. Starting outreach...")


def reschedule_remaining(pending):
    """Push all remaining unsent emails to tomorrow."""
    tomorrow = (_now() + timedelta(days=1)).strftime("%Y-%m-%d")
    conn_module = __import__("db.db", fromlist=["get_conn"])
    conn = conn_module.get_conn()
    c = conn.cursor()

    ids = [row["id"] for row in pending]
    for oid in ids:
        c.execute("""
            UPDATE outreach SET scheduled_for = ? WHERE id = ?
        """, (tomorrow, oid))

    conn.commit()
    conn.close()
    print(f"[INFO] Rescheduled {len(ids)} email(s) for tomorrow ({tomorrow})")


def schedule_initial_outreach():
    """
    For every recruiter linked to an application with no outreach yet,
    schedule the initial email for today.
    """
    apps = get_all_active_applications()
    scheduled = 0

    for app in apps:
        recruiters = get_recruiters_for_application(app["id"])
        for recruiter in recruiters:
            if has_pending_or_sent_outreach(recruiter["id"], app["id"]):
                continue

            today = _now().strftime("%Y-%m-%d")
            schedule_outreach(
                recruiter_id=recruiter["id"],
                application_id=app["id"],
                stage="initial",
                scheduled_for=today,
            )
            print(f"   [INFO] Scheduled initial: {recruiter['name']} @ {recruiter['company']}")
            scheduled += 1

    if scheduled:
        print(f"[OK] Scheduled {scheduled} initial outreach email(s)")
    else:
        print("[OK] No new initial emails to schedule")


def process_outreach():
    """
    Send all pending outreach emails within the send window.
    Handles wait / send / cutoff states automatically.
    """
    # Check send window status
    status = get_send_status()

    if status == "wait":
        wait_until_window()
        status = "send"

    if status == "cutoff":
        pending = get_pending_outreach()
        if pending:
            print(f"[INFO] Past hard cutoff ({HARD_CUTOFF_HOUR}:00 {SEND_TIMEZONE}). Rescheduling {len(pending)} email(s) for tomorrow.")
            reschedule_remaining(pending)
        else:
            print("[INFO] No pending emails and outside send window.")
        return

    # status == "send"
    pending = get_pending_outreach()

    if not pending:
        print("[INFO] No pending outreach emails due today.")
        return

    print(f"[INFO] {len(pending)} outreach email(s) to send.")
    print(f"[INFO] Send window: {SEND_WINDOW_START}:00 AM - {HARD_CUTOFF_HOUR}:00 AM ({SEND_TIMEZONE})\n")

    sent_count = 0
    failed_count = 0

    for row in pending:

        # Re-check window before each email
        current_status = get_send_status()
        if current_status == "cutoff":
            remaining = [r for r in pending if r["id"] != row["id"]]
            if remaining:
                print(f"\n[INFO] Hard cutoff reached. Rescheduling {len(remaining)} remaining email(s).")
                reschedule_remaining(remaining)
            break

        recruiter_name  = row["name"]
        recruiter_email = row["email"]
        company         = row["company"]
        job_url         = row["job_url"]
        stage           = row["stage"]
        outreach_id     = row["id"]
        recruiter_id    = row["recruiter_id"]
        application_id  = row["application_id"]

        print(f"[INFO] [{stage}] {recruiter_name} @ {company} → {recruiter_email}")

        # Check AI cache — warn and skip if missing
        # NOTE: quota check is done AFTER template fetch so cached content
        # always sends even when AI quota is exhausted
        try:
            template = get_template(
                stage=stage,
                name=recruiter_name,
                company=company,
                job_url=job_url,
            )

            if not template:
                if job_url and all_models_exhausted():
                    print(f"   [WARNING] AI quota exhausted and no cached template for {company}. Skipping until tomorrow.")
                else:
                    print(f"   [WARNING] No AI content found for {company}. Run --find-only first. Skipping.")
                continue

            body, subject = template

            send_email(
                to_email=recruiter_email,
                body=body,
                company=company,
                subject=subject,
            )

        except smtplib.SMTPRecipientsRefused:
            print(f"   [ERROR] Hard bounce for {recruiter_email} — marking recruiter inactive")
            mark_outreach_bounced(outreach_id, recruiter_id)
            failed_count += 1
            time.sleep(random.randint(30, 90))
            continue

        except Exception as e:
            print(f"   [ERROR] Failed to send: {e}")
            mark_outreach_failed(outreach_id)
            failed_count += 1
            time.sleep(random.randint(30, 90))
            continue

        # Only reached if send succeeded
        mark_outreach_sent(outreach_id)
        sent_count += 1
        print(f"   [OK] Sent | Subject: {subject}")

        # Schedule next stage if no reply
        try:
            if not row["replied"]:
                schedule_next_outreach(recruiter_id, application_id)
        except Exception as e:
            print(f"   [WARNING] Could not schedule next outreach: {e}")

        # Human-like delay between emails
        time.sleep(random.randint(30, 90))

    print(f"\n[OK] Sent: {sent_count} | Failed: {failed_count}")


def run():
    init_db()
    print("[INFO] Scheduling initial outreach emails...")
    schedule_initial_outreach()
    print("\n[INFO] Processing pending outreach emails...")
    process_outreach()


if __name__ == "__main__":
    run()