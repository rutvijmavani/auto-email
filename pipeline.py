"""
pipeline.py — Master orchestrator for the recruiter outreach pipeline.

Daily schedule:
  Evening:   python pipeline.py --sync-forms    → pull form responses + scrape JD
  Night:     python pipeline.py --find-only     → find recruiters + generate AI content
  Morning:   python pipeline.py --outreach-only → send emails (9 AM - 11 AM window)

  Or all at once:
             python pipeline.py

CLI flags:
  --sync-forms    pull Google Form responses → insert into DB + scrape JD
  --add           add a single job application interactively
  --find-only     scrape CareerShift + generate AI content + quota health check
  --outreach-only schedule + send outreach emails
  --quota-report  check quota health and send alert email if needed
"""

import sys
from datetime import datetime

from db.db import init_db, add_application, get_remaining_quota


def add_job_interactively():
    """Add a new application, scrape JD, store in jobs table."""
    print("\n[ADD] Add New Job Application")
    print("-" * 40)
    company      = input("Company name: ").strip()
    job_url      = input("Job URL: ").strip()
    job_title    = input("Job title (optional): ").strip() or None
    applied_date = input("Applied date (YYYY-MM-DD) [default: today]: ").strip()

    if not applied_date:
        applied_date = datetime.now().strftime("%Y-%m-%d")

    if not company or not job_url:
        print("[ERROR] Company and Job URL are required.")
        return

    app_id, created = add_application(
        company=company,
        job_url=job_url,
        job_title=job_title,
        applied_date=applied_date,
    )

    if not app_id:
        print("[ERROR] Failed to add application.")
        return

    if not created:
        print("[WARNING] Job URL already exists in DB.")
        return

    print(f"[OK] Added: {company} | {job_url} (id={app_id})")

    print(f"[INFO] Scraping job description from {job_url}...")
    try:
        from jobs.job_fetcher import fetch_job_description
        result = fetch_job_description(job_url)
        if result:
            print(f"[OK] Job description cached for {company}")
        else:
            print(f"[WARNING] Could not scrape job description. Will retry during --find-only.")
    except Exception as e:
        print(f"[WARNING] JD scraping failed: {e}. Will retry during --find-only.")


def run_sync_forms():
    """Pull Google Form responses, insert into DB, scrape JDs, clean up sheet."""
    print("\n" + "=" * 55)
    print("[INFO] Syncing Google Form responses")
    print("=" * 55)
    from jobs.form_sync import run as sync_run
    sync_run()


def run_find_emails():
    """Scrape CareerShift for recruiters + generate AI content + quota health check."""
    print("\n" + "=" * 55)
    print("[INFO] STEP 1: Finding recruiter emails via CareerShift")
    print("=" * 55)
    from careershift.find_emails import run as find_run
    find_run()

    print("\n" + "=" * 55)
    print("[INFO] STEP 2: Generating AI email content")
    print("=" * 55)
    _generate_ai_content_for_all()

    print("\n" + "=" * 55)
    print("[INFO] STEP 3: Quota health check")
    print("=" * 55)
    run_quota_report(silent_if_healthy=True)


def _generate_ai_content_for_all():
    """
    Generate and cache AI email content for every active application.
    Uses leftover Gemini quota for applications missing cache.
    """
    from db.db import get_all_active_applications, get_applications_missing_ai_cache
    from jobs.job_fetcher import fetch_job_description
    from outreach.ai_full_personalizer import (
        generate_all_content,
        generate_all_content_without_jd,
        all_models_exhausted,
    )

    apps = get_all_active_applications()

    if not apps:
        print("[INFO] No active applications found.")
        return

    generated = 0
    skipped   = 0
    failed    = 0

    for app in apps:
        company   = app["company"]
        job_url   = app["job_url"]
        job_title = app["job_title"] or "Software Engineer"

        print(f"\n  [INFO] {company} | {job_title}")

        job_data = fetch_job_description(job_url)

        if not job_data:
            print(f"  [WARNING] No job description for {company}. Using role-based fallback.")
            result = generate_all_content_without_jd(company, job_title)
        else:
            if isinstance(job_data, dict):
                job_text  = job_data.get("job_text", "")
                job_title = job_data.get("job_title") or job_title
            else:
                job_text = job_data

            if not job_text:
                print(f"  [WARNING] Empty job description for {company}. Using role-based fallback.")
                result = generate_all_content_without_jd(company, job_title)
            else:
                result = generate_all_content(company, job_title, job_text)

        if result:
            print(f"  [OK] AI content ready for {company}")
            generated += 1
        else:
            print(f"  [WARNING] AI generation failed for {company} (quota exhausted?)")
            failed += 1

    # Leftover Gemini quota utilization
    if not all_models_exhausted():
        missing = get_applications_missing_ai_cache()
        if missing:
            print(f"\n[INFO] Using leftover Gemini quota for {len(missing)} application(s) missing cache...")
            for app in missing:
                if all_models_exhausted():
                    break
                company   = app["company"]
                job_title = app["job_title"] or "Software Engineer"
                job_url   = app["job_url"]
                job_data  = fetch_job_description(job_url)

                if isinstance(job_data, dict):
                    job_text = job_data.get("job_text", "")
                elif isinstance(job_data, str):
                    job_text = job_data
                else:
                    job_text = None

                result = (
                    generate_all_content(company, job_title, job_text)
                    if job_text
                    else generate_all_content_without_jd(company, job_title)
                )
                if result:
                    print(f"  [OK] Leftover quota used for: {company}")
                    generated += 1

    print(f"\n[OK] AI content — Generated: {generated} | Skipped (cached): {skipped} | Failed: {failed}")


def run_outreach():
    """Schedule and send outreach emails within the send window."""
    print("\n" + "=" * 55)
    print("[INFO] Sending outreach emails")
    print("=" * 55)
    from outreach.outreach_engine import run as outreach_run
    outreach_run()


def run_quota_report(silent_if_healthy=False):
    """
    Check quota health for CareerShift and Gemini.
    Sends alert email if conditions are met.
    Can be run standalone via --quota-report or automatically after --find-only.
    """
    from db.db import check_quota_health, save_quota_alert
    from outreach.email_sender import send_email
    from config import MAX_CONTACTS_HARD_CAP, QUOTA_ALERT_CONSECUTIVE_DAYS

    alerts = check_quota_health()

    if not alerts:
        if not silent_if_healthy:
            print("[OK] Quota health: no issues detected.")
        return

    # Build combined alert email
    subject = "Quota Alert - Action Required"
    body_parts = []

    for alert in alerts:
        quota_label = "CAREERSHIFT QUOTA" if alert["quota_type"] == "careershift" else "GEMINI QUOTA"
        alert_label = alert["alert_type"].upper()
        total       = alert["total_limit"]
        threshold   = int(total * 0.4) if alert["alert_type"] == "underutilized" else 0

        body_parts.append(f"{quota_label} - {alert_label} ({QUOTA_ALERT_CONSECUTIVE_DAYS} consecutive days)")
        body_parts.append("")

        for row in reversed(alert["history"]):
            pct = round((row["used"] / row["total_limit"]) * 100) if row["total_limit"] > 0 else 0
            body_parts.append(f"  {row['date']}: used {row['used']}/{row['total_limit']} ({pct}%)")

        body_parts.append("")
        body_parts.append(f"Average usage: {alert['avg_used']}/{total}")

        if alert["alert_type"] == "underutilized":
            if alert.get("suggested_cap"):
                body_parts.append(
                    f"Recommendation: Increase MAX_CONTACTS_HARD_CAP "
                    f"from {MAX_CONTACTS_HARD_CAP} to {alert['suggested_cap']}"
                )
            else:
                body_parts.append(f"Recommendation: Increase MAX_CONTACTS_HARD_CAP in config.py")
        else:
            if alert["quota_type"] == "careershift":
                if alert.get("suggested_cap"):
                    body_parts.append(
                        f"Recommendation: Decrease MAX_CONTACTS_HARD_CAP "
                        f"from {MAX_CONTACTS_HARD_CAP} to {alert['suggested_cap']}, "
                        f"or reduce daily job applications."
                    )
            else:
                body_parts.append(
                    "Recommendation: Reduce daily applications or upgrade Gemini plan."
                )

        body_parts.append("")
        body_parts.append("-" * 40)
        body_parts.append("")

    body = "\n".join(body_parts)

    # Multiple alerts in one email
    if len(alerts) > 1:
        subject = f"Quota Alert - Action Required ({len(alerts)} issues)"

    try:
        from config import EMAIL
        send_email(to_email=EMAIL, body=body, company="Pipeline", subject=subject)
        print(f"[INFO] Quota alert email sent: {subject}")
    except Exception as e:
        print(f"[WARNING] Could not send quota alert email: {e}")
        print(f"[INFO] Alert details:\n{body}")

    # Save all alerts to DB
    for alert in alerts:
        save_quota_alert(alert)


def main():
    init_db()
    args = sys.argv[1:]

    if "--sync-forms" in args:
        run_sync_forms()
        return

    if "--add" in args:
        add_job_interactively()
        return

    if "--find-only" in args:
        run_find_emails()
        return

    if "--outreach-only" in args:
        run_outreach()
        return

    if "--quota-report" in args:
        run_quota_report()
        return

    # Full pipeline
    print("[INFO] Starting Recruiter Outreach Pipeline")
    print(f"[INFO] {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print(f"[INFO] CareerShift quota remaining: {get_remaining_quota()}/50")

    add_job_interactively()
    run_find_emails()
    run_outreach()

    print("\n" + "=" * 55)
    print("[DONE] Pipeline complete!")
    print(f"[INFO] CareerShift quota remaining: {get_remaining_quota()}/50")


if __name__ == "__main__":
    main()