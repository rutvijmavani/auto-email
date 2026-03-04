"""
pipeline.py — Master orchestrator for the recruiter outreach pipeline.

Daily schedule:
  Evening:   python pipeline.py --sync-forms    → pull form responses + scrape JD
  Night:     python pipeline.py --find-only     → find recruiters + generate AI content
  Weekly:    python pipeline.py --verify-only   → verify recruiters + report under-stocked
  Morning:   python pipeline.py --outreach-only → send emails (9 AM - 11 AM window)

  Or all at once:
             python pipeline.py

CLI flags:
  --sync-forms    pull Google Form responses → insert into DB + scrape JD
  --add           add a single job application interactively
  --find-only     scrape CareerShift + generate AI content + quota health check
  --verify-only   verify all active recruiters + report under-stocked companies
  --outreach-only schedule + send outreach emails
  --quota-report  check quota health and send alert email if needed
"""

import sys
from datetime import datetime

from db.db import init_db, add_application, get_remaining_quota,\
    update_application_expected_domain


def extract_expected_domain(job_url):
    """
    Extract expected company domain root from job URL.

    Examples:
      https://jobs.ashbyhq.com/collective/54259edc  → "collective"
      https://boards.greenhouse.io/collectiveinc/   → "collectiveinc"
      https://collective.com/careers/engineer       → "collective"
      https://jobs.lever.co/stripe/abc123           → "stripe"
    """
    try:
        from urllib.parse import urlparse
        parsed = urlparse(job_url)
        hostname = parsed.hostname or ""           # "jobs.ashbyhq.com"
        path     = parsed.path.strip("/")          # "collective/54259edc"

        # ATS platforms — extract company slug from path
        ats_hosts = [
            "jobs.ashbyhq.com", "boards.greenhouse.io", "job-boards.greenhouse.io",
            "jobs.lever.co", "boards.eu.greenhouse.io", "jobs.jobvite.com",
            "jobs.smartrecruiters.com", "apply.workable.com",
        ]
        for ats in ats_hosts:
            if ats in hostname:
                slug = path.split("/")[0].lower()
                if slug:
                    # Remove common suffixes from slug
                    import re
                    slug = re.sub(r'(jobs?|careers?|hiring)$', '', slug)
                    return slug.strip("-_") or None

        # Direct company domain — extract root
        # e.g. "collective.com" → "collective"
        # e.g. "careers.stripe.com" → "stripe"
        parts = hostname.split(".")
        if len(parts) >= 2:
            # Skip common subdomains
            skip = {"www", "jobs", "careers", "boards", "apply", "hire",
                    "talent", "recruiting", "work"}
            for part in parts[:-1]:  # exclude TLD
                if part not in skip:
                    return part.lower()

        return None
    except Exception:
        return None


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

    expected_domain = extract_expected_domain(job_url)

    app_id, created = add_application(
        company=company,
        job_url=job_url,
        job_title=job_title,
        applied_date=applied_date,
        expected_domain=expected_domain,
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


def run_verify_only():
    """
    Verify all active recruiters and report under-stocked companies.

    Steps:
      1. Run tiered verification for all active recruiters (free — cached profiles)
      2. Mark departed recruiters inactive + cancel their pending outreach
      3. Detect companies that dropped below MIN_RECRUITERS threshold
      4. Print summary report — under-stocked companies picked up by next --find-only
    """
    import random
    from playwright.sync_api import sync_playwright
    from dotenv import load_dotenv
    from careershift.constants import SESSION_FILE
    from careershift.utils import human_delay
    from careershift.verification import run_tiered_verification
    from db.db import (
        get_all_active_applications,
        get_unique_companies_needing_scraping,
        get_recruiters_by_company,
    )
    from config import MIN_RECRUITERS_PER_COMPANY

    load_dotenv()

    if not os.path.exists(SESSION_FILE):
        print("[ERROR] Session file not found. Run careershift/auth.py first.")
        return

    print("\n" + "=" * 55)
    print("[INFO] STEP 1: Recruiter Verification")
    print("=" * 55)

    applications = get_all_active_applications()
    if not applications:
        print("[INFO] No active applications found.")
        return

    USER_AGENTS = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:123.0) Gecko/20100101 Firefox/123.0",
    ]

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True, slow_mo=100)
        context = browser.new_context(
            storage_state=SESSION_FILE,
            user_agent=random.choice(USER_AGENTS),
            viewport={"width": random.randint(1280, 1920), "height": random.randint(768, 1080)},
        )
        page = context.new_page()

        # Verify session
        print("[INFO] Verifying CareerShift session...")
        page.goto("https://www.careershift.com/App/Dashboard/Overview",
                  wait_until="domcontentloaded", timeout=30000)
        human_delay(2.0, 4.0)

        if "login" in page.url.lower() or "signin" in page.url.lower():
            print("[ERROR] Session expired. Run careershift/auth.py again.")
            browser.close()
            return

        print("[OK] Session valid.\n")

        # Run tiered verification
        run_tiered_verification(page, applications)

        browser.close()

    # ── Step 2: Check for under-stocked companies ──
    print("\n" + "=" * 55)
    print("[INFO] STEP 2: Checking for under-stocked companies")
    print("=" * 55)

    under_stocked = get_unique_companies_needing_scraping(MIN_RECRUITERS_PER_COMPANY)

    if not under_stocked:
        print("[OK] All companies have enough active recruiters.")
    else:
        print(f"\n[WARNING] {len(under_stocked)} company/companies under-stocked after verification:")
        for company in under_stocked:
            recruiters = get_recruiters_by_company(company)
            active_count = len(recruiters)
            print(f"  - {company}: {active_count} active recruiter(s) "
                  f"(needs {MIN_RECRUITERS_PER_COMPANY - active_count} more)")
        print("\n[INFO] Run --find-only to top up under-stocked companies.")

    print("\n" + "=" * 55)
    print("[OK] Verification complete!")
    print("=" * 55)


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
        # Only mark as notified after successful send
        for alert in alerts:
            save_quota_alert(alert)
    except Exception as e:
        print(f"[WARNING] Could not send quota alert email: {e}")
        print(f"[INFO] Alert details:\n{body}")
        # Save without notified flag so future runs still attempt to send
        for alert in alerts:
            save_quota_alert({**alert, "notified": False})


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

    if "--verify-only" in args:
        run_verify_only()
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