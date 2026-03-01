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
  --find-only     scrape CareerShift + generate AI content
  --outreach-only schedule + send outreach emails
"""

import sys
from datetime import datetime

from db.db import init_db, add_application, get_remaining_quota


def add_job_interactively():
    """Add a new application, scrape JD, store in jobs table."""
    print("\n[ADD] Add New Job Application")
    print("─" * 40)
    company     = input("Company name: ").strip()
    job_url     = input("Job URL: ").strip()
    job_title   = input("Job title (optional): ").strip() or None
    applied_date = input(f"Applied date (YYYY-MM-DD) [default: today]: ").strip()

    if not applied_date:
        applied_date = datetime.now().strftime("%Y-%m-%d")

    if not company or not job_url:
        print("[ERROR] Company and Job URL are required.")
        return

    # Insert application
    app_id = add_application(
        company=company,
        job_url=job_url,
        job_title=job_title,
        applied_date=applied_date,
    )

    if not app_id:
        print("[WARNING]  Job URL already exists in DB.")
        return

    print(f"[OK] Added: {company} | {job_url} (id={app_id})")

    # Scrape JD immediately and store in jobs table
    print(f"[INFO] Scraping job description from {job_url}...")
    try:
        from jobs.job_fetcher import fetch_job_description
        result = fetch_job_description(job_url)
        if result:
            print(f"[OK] Job description cached for {company}")
        else:
            print(f"[WARNING]  Could not scrape job description. Will retry during --find-only.")
    except Exception as e:
        print(f"[WARNING]  JD scraping failed: {e}. Will retry during --find-only.")


def run_sync_forms():
    """Pull Google Form responses, insert into DB, scrape JDs, clean up sheet."""
    print("\n" + "=" * 55)
    print("[INFO] Syncing Google Form responses")
    print("=" * 55)
    from jobs.form_sync import run as sync_run
    sync_run()


def run_find_emails():
    """Scrape CareerShift for recruiters + generate AI content for all applications."""
    print("\n" + "=" * 55)
    print("[INFO] STEP 1: Finding recruiter emails via CareerShift")
    print("=" * 55)
    from careershift.find_emails import run as find_run
    find_run()

    # Generate AI content for all applications after recruiters are linked
    print("\n" + "=" * 55)
    print("[INFO] STEP 2: Generating AI email content")
    print("=" * 55)
    _generate_ai_content_for_all()


def _generate_ai_content_for_all():
    """
    Generate and cache AI email content for every active application.
    Skips applications whose cache is already warm.
    """
    from db.db import get_all_active_applications
    from jobs.job_fetcher import fetch_job_description
    from outreach.ai_full_personalizer import generate_all_content, generate_all_content_without_jd

    apps = get_all_active_applications()

    if not apps:
        print("[INFO] No active applications found.")
        return

    generated = 0
    skipped = 0
    failed = 0

    for app in apps:
        company   = app["company"]
        job_url   = app["job_url"]
        job_title = app["job_title"] or "Software Engineer"

        print(f"\n  [INFO] {company} | {job_title}")

        # Fetch JD (from cache or scrape)
        job_data = fetch_job_description(job_url)

        if not job_data:
            print(f"  [WARNING] No job description available for {company}. Using role-based fallback.")
            result = generate_all_content_without_jd(company, job_title)
        else:
            # Handle both string and dict return from fetch_job_description
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
            print(f"  [WARNING]  AI generation failed for {company} (quota exhausted?)")
            failed += 1

    print(f"\n[OK] AI content — Generated: {generated} | Skipped (cached): {skipped} | Failed: {failed}")


def run_outreach():
    """Schedule and send outreach emails within the send window."""
    print("\n" + "=" * 55)
    print("[INFO] Sending outreach emails")
    print("=" * 55)
    from outreach.outreach_engine import run as outreach_run
    outreach_run()


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