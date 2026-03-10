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
  --find-only           scrape CareerShift + generate AI content + quota health check
  --verify-only         verify all active recruiters + report under-stocked companies
  --import-prospects    bulk import prospective companies from prospects.txt
  --prospects-status    show prospective pipeline status summary
  --monitor-jobs        scan all companies for new job postings + send PDF digest
  --detect-ats          auto-detect ATS for all undetected companies
  --detect-ats "Name"   force re-detect ATS for specific company
  --monitor-status      show job monitoring status summary
  --outreach-only       schedule + send outreach emails
  --quota-report        check quota health and send alert email if needed
"""

import os
import sys
import random
from datetime import datetime
from playwright.sync_api import sync_playwright
from dotenv import load_dotenv
from careershift.constants import SESSION_FILE
from careershift.utils import human_delay
from careershift.verification import run_tiered_verification
from config import MIN_RECRUITERS_PER_COMPANY

from db.db import (
    init_db, add_application, get_remaining_quota,
    update_application_expected_domain, get_application_by_id,
    convert_prospective_to_active, is_prospective, mark_prospective_converted,
    add_prospective_company, get_prospective_status_summary,
    get_prospective_companies,
    get_all_active_applications, get_unique_companies_needing_scraping,
    get_recruiters_by_company,
)


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

    # ── Prospective detection ──
    # Normalize company name so " Google ", "google", "Google" all match
    # the stored prospective entry regardless of how user typed it
    company_normalized = company.strip()
    if is_prospective(company_normalized):
        print(f"[INFO] '{company_normalized}' found in prospective pipeline — recruiters already pre-scraped!")
        converted_id = convert_prospective_to_active(
            company=company_normalized,
            real_job_url=job_url,
            job_title=job_title,
            expected_domain=expected_domain,
        )
        if converted_id:
            mark_prospective_converted(company_normalized)
            print(f"[OK] Converted prospective → active (id={converted_id})")
            print(f"[INFO] Outreach will be scheduled on next --outreach-only run.")
            return
        # Conversion failed (e.g. placeholder URL mismatch) → fall through to normal --add

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
        # URL already exists — backfill expected_domain only if currently NULL
        if expected_domain:
            existing = get_application_by_id(app_id)
            if existing and not existing.get("expected_domain"):
                update_application_expected_domain(app_id, expected_domain)
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
    from datetime import datetime
    from outreach.report_templates.find_report import build_find_report

    print("\n" + "=" * 55)
    print("[INFO] STEP 1: Finding recruiter emails via CareerShift")
    print("=" * 55)
    from careershift.find_emails import run as find_run
    find_stats = find_run()  # returns stats dict

    print("\n" + "=" * 55)
    print("[INFO] STEP 2: Generating AI email content")
    print("=" * 55)
    ai_stats = _generate_ai_content_for_all()

    print("\n" + "=" * 55)
    print("[INFO] STEP 3: Quota health check")
    print("=" * 55)
    run_quota_report(silent_if_healthy=True)

    # Send HTML report
    try:
        date_str = datetime.now().strftime("%B %-d, %Y")
    except ValueError:
        date_str = datetime.now().strftime("%B %d, %Y")

    report_stats = {
        "date":                 date_str,
        "quota_used":           find_stats.get("quota_used", 0) if find_stats else 0,
        "quota_total":          50,
        "companies":            find_stats.get("companies", []) if find_stats else [],
        "prospective_scraped":  find_stats.get("prospective_scraped", 0) if find_stats else 0,
        "prospective_exhausted":find_stats.get("prospective_exhausted", 0) if find_stats else 0,
        "ai_generated":         ai_stats.get("generated", 0) if ai_stats else 0,
        "ai_cached":            ai_stats.get("skipped", 0) if ai_stats else 0,
        "ai_failed":            ai_stats.get("failed", 0) if ai_stats else 0,
    }
    build_find_report(report_stats)


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
    return {"generated": generated, "skipped": skipped, "failed": failed}


def run_import_prospects(filepath="prospects.txt"):
    """
    Bulk import prospective companies from a text file.

    Formats supported:
      One column:   "Stripe"
      Two columns:  "Stripe,stripe.com"

    Lines starting with # and blank lines ignored.
    Domain column used for Phase 3a HTML redirect scan.
    """
    if not os.path.exists(filepath):
        print(f"[ERROR] File not found: {filepath}")
        print(f"[INFO] Format: one company per line, optional domain:")
        print(f"[INFO]   Stripe")
        print(f"[INFO]   Capital One,capitalone.com")
        return

    print(f"\n[INFO] Importing prospective companies from {filepath}...")

    added = 0
    skipped = 0
    with open(filepath, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue

            # Support "Company,domain.com" format
            parts  = line.split(",", 1)
            company = parts[0].strip()
            domain  = parts[1].strip() if len(parts) > 1 else None

            if not company:
                continue

            if add_prospective_company(company, domain=domain):
                domain_str = f" ({domain})" if domain else ""
                print(f"  [+] {company}{domain_str}")
                added += 1
            else:
                # Update domain if not set
                if domain:
                    add_prospective_company(company, domain=domain)
                skipped += 1

    print(f"\n[OK] Import complete — Added: {added} | Already existed: {skipped}")
    print(f"[INFO] Run --detect-ats --batch to start ATS detection.")


def run_prospects_status():
    """Show status summary of all prospective companies."""
    summary = get_prospective_status_summary()

    if not summary:
        print("\n[INFO] No prospective companies found.")
        print("[INFO] Import one with: python pipeline.py --import-prospects prospects.txt")
        return

    print("\n" + "=" * 55)
    print("[INFO] Prospective Companies Status")
    print("=" * 55)

    total = sum(summary.values())
    for status in ["pending", "scraped", "converted", "exhausted"]:
        count = summary.get(status, 0)
        label = {
            "pending":   "Pending (not yet scraped)",
            "scraped":   "Scraped (recruiters ready)",
            "converted": "Converted (applied)",
            "exhausted": "Exhausted (no data found)",
        }[status]
        print(f"  {label:<35} {count:>3}")

    print(f"  {'─'*39}")
    print(f"  {'Total':<35} {total:>3}")

    # Show scraped companies ready for outreach
    scraped = get_prospective_companies(status="scraped")
    if scraped:
        print(f"\n[OK] Ready for immediate outreach when you apply:")
        for p in scraped[:10]:
            print(f"  → {p['company']}")
        if len(scraped) > 10:
            print(f"  ... and {len(scraped) - 10} more")

    print("=" * 55)


def run_verify_only():
    """
    Verify all active recruiters and report under-stocked companies.

    Steps:
      1. Run tiered verification for all active recruiters (free — cached profiles)
      2. Mark departed recruiters inactive + cancel their pending outreach
      3. Detect companies that dropped below MIN_RECRUITERS threshold
      4. Print summary report — under-stocked companies picked up by next --find-only
    """
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

        # Run tiered verification — capture stats for report
        verify_stats = run_tiered_verification(page, applications) or {}

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

    # ── Send HTML report ──
    from outreach.report_templates.verify_report import build_verify_report
    from datetime import datetime
    try:
        date_str = datetime.now().strftime("%B %-d, %Y")
    except ValueError:
        date_str = datetime.now().strftime("%B %d, %Y")

    under_stocked_detail = []
    for company in under_stocked:
        recruiters = get_recruiters_by_company(company)
        active_count = len(recruiters)
        under_stocked_detail.append({
            "company":      company,
            "active_count": active_count,
            "needed":       max(0, MIN_RECRUITERS_PER_COMPANY - active_count),
        })

    build_verify_report({
        "date":           date_str,
        "tier1_count":    verify_stats.get("tier1_count",    0),
        "tier2_count":    verify_stats.get("tier2_count",    0),
        "tier2_verified": verify_stats.get("tier2_verified", 0),
        "tier3_count":    verify_stats.get("tier3_count",    0),
        "tier3_verified": verify_stats.get("tier3_verified", 0),
        "tier3_inactive": verify_stats.get("tier3_inactive", 0),
        "changes":        verify_stats.get("changes",        []),
        "under_stocked":  under_stocked_detail,
    })


def run_outreach():
    """Schedule and send outreach emails within the send window."""
    print("\n" + "=" * 55)
    print("[INFO] Sending outreach emails")
    print("=" * 55)
    from outreach.outreach_engine import run as outreach_run
    from outreach.report_templates.outreach_report import build_outreach_report
    from db.db import get_pending_outreach, get_conn
    from datetime import datetime

    stats = outreach_run()  # returns stats dict from outreach_engine

    # Send HTML report
    if stats:
        try:
            stats["date"] = datetime.now().strftime("%B %-d, %Y")
        except ValueError:
            stats["date"] = datetime.now().strftime("%B %d, %Y")
        build_outreach_report(stats)


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

    if "--import-prospects" in args:
        # Optional custom filepath: python pipeline.py --import-prospects mylist.txt
        filepath = next((a for a in args if not a.startswith("--")), "prospects.txt")
        run_import_prospects(filepath)
        return

    if "--prospects-status" in args:
        run_prospects_status()
        return

    if "--monitor-jobs" in args:
        from jobs.job_monitor import run as monitor_run
        monitor_run()
        return

    if "--detect-ats" in args:
        from jobs.job_monitor import run_detect_ats
        # Usage:
        #   --detect-ats                              (all pending)
        #   --detect-ats --batch                      (next batch, respects quota)
        #   --detect-ats "Stripe"                     (single company)
        #   --detect-ats "Capital One" --override workday capitalone
        non_flag_args     = [a for a in args if not a.startswith("--")]
        company           = non_flag_args[0] if len(non_flag_args) > 0 else None
        override_platform = non_flag_args[1] if len(non_flag_args) > 1 else None
        override_slug     = non_flag_args[2] if len(non_flag_args) > 2 else None
        batch             = "--batch" in args
        run_detect_ats(
            company=company,
            override_platform=override_platform,
            override_slug=override_slug,
            batch=batch,
        )
        return

    if "--monitor-status" in args:
        from jobs.job_monitor import run_monitor_status
        run_monitor_status()
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