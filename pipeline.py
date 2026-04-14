"""
pipeline.py — Master orchestrator for the recruiter outreach pipeline.

Daily schedule:
  Evening:   python pipeline.py --sync-forms       → pull form responses + scrape JD
  Evening:   python pipeline.py --sync-prospective → sync prospective company form
  Night:     python pipeline.py --find-only        → find recruiters + generate AI content
  Weekly:    python pipeline.py --verify-only      → verify recruiters + report under-stocked
  Morning:   python pipeline.py --outreach-only    → send emails (9 AM - 11 AM window)

  Or all at once:
             python pipeline.py

CLI flags:
  --sync-forms                pull Google Form responses → insert into DB + scrape JD
  --sync-prospective          pull prospective company form → detect ATS + store config
  --add                       add a single job application interactively
  --find-only                 scrape CareerShift + generate AI content + quota health check
  --verify-only               verify all active recruiters + report under-stocked companies
  --import-prospects          bulk import prospective companies from prospects.txt
  --prospects-status          show prospective pipeline status summary
  --monitor-jobs              scan all companies for new job postings + send PDF digest
  --detect-ats                auto-detect ATS for all undetected companies
  --detect-ats "Name"         force re-detect ATS for specific company
  --detect-ats "Co" --override <platform> <slug>
                              manually set ATS for a company (permanent)
  --detect-ats --batch        detect next batch only (respects Serper quota)
  --monitor-status            show job monitoring status summary
  --verify-filled             verify stale job URLs → mark filled positions
  --set-custom-ats "Co" --curl "curl '...'"
                              capture custom ATS config from DevTools curl
  --set-custom-ats "Co" --curl "curl '...'" --detail-curl "curl '...'"
                              capture listing + detail curl for full description fetch
  --diagnostics               show open custom ATS diagnostic issues
  --resolve-diagnostic <id>   mark a diagnostic as resolved by ID
  --resolve-diagnostic <id> "CompanyName"
                              resolve all open diagnostics for a company
  --weekly-summary            send Monday weekly summary email
  --outreach-only             schedule + send outreach emails
  --quota-report              check quota health + send alert email if needed
  --performance-report        check pipeline performance metrics + send alert if needed
  --reactivate "Name"         reactivate an exhausted application
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

from logger import get_logger
from db.db import (
    init_db, add_application, get_remaining_quota,
    update_application_expected_domain, get_application_by_id,
    convert_prospective_to_active, is_prospective, mark_prospective_converted,
    add_prospective_company, get_prospective_status_summary,
    get_prospective_companies,
    get_all_active_applications, get_unique_companies_needing_scraping,
    get_recruiters_by_company,
)

logger = get_logger(__name__)


def extract_expected_domain(job_url):
    """
    Extract expected company domain root from job URL.

    Examples:
      https://jobs.ashbyhq.com/collective/54259edc  → "collective"
      https://boards.greenhouse.io/collectiveinc/   → "collectiveinc"
      https://collective.com/careers/engineer       → "collective"
      https://jobs.lever.co/stripe/abc123           → "stripe"
      https://careers-schwab.icims.com/jobs/123     → "schwab"
      https://schwab.icims.com/jobs/123             → "schwab"
      https://jpmc.fa.oraclecloud.com/hcmUI/...     → "jpmc"
      https://capitalone.wd12.myworkdayjobs.com/... → "capitalone"
    """
    try:
        from urllib.parse import urlparse
        import re

        parsed   = urlparse(job_url)
        hostname = parsed.hostname or ""           # "jobs.ashbyhq.com"
        path     = parsed.path.strip("/")          # "collective/54259edc"

        # ── iCIMS — slug is in subdomain ──────────────────────────
        # careers-schwab.icims.com → "schwab"
        # schwab.icims.com         → "schwab"

        if hostname.endswith(".icims.com"):
            subdomain = hostname[: -len(".icims.com")]
            slug = re.sub(r"^careers-", "", subdomain)
            if slug and "." not in slug and slug not in {"careers", "jobs", "www"}:
                return slug.lower()
            return None

        # ── Oracle HCM — slug is first subdomain ──────────────────
        # jpmc.fa.oraclecloud.com → "jpmc"
        if hostname.endswith(".fa.oraclecloud.com"):
            slug = hostname[: -len(".fa.oraclecloud.com")]
            if slug and "." not in slug and slug not in {"fa", "www"}:
                return slug.lower()
            return None

        # ── Workday — slug is first subdomain before .wd{N} ───────
        # capitalone.wd12.myworkdayjobs.com → "capitalone"
        for suffix in (".myworkdayjobs.com", ".myworkdaysite.com"):
            if hostname.endswith(suffix):
                slug = hostname[: -len(suffix)].split(".")[0]
                if slug and slug not in {"www", "jobs", "careers"}:
                    return slug.lower()
                return None

        # ── Greenhouse embed — job-boards.greenhouse.io/embed/job_board?for=Databricks
        if hostname in ("job-boards.greenhouse.io", "boards.greenhouse.io"):
            from urllib.parse import parse_qs
            qs = parse_qs(parsed.query)
            if "for" in qs:
                return qs["for"][0].lower()
            # Fall through to normal path extraction
            slug = path.split("/")[0].lower()
            if slug and slug not in {"embed", "jobs", "careers"}:
                return slug
            return None

        # ── ATS platforms — extract company slug from path ─────────
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
                    slug = re.sub(r'(jobs?|careers?|hiring)$', '', slug)
                    return slug.strip("-_") or None

        # ── Direct company domain — extract root ───────────────────
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
        logger.info("Prospective conversion: %r found in pipeline", company_normalized)

        # Create real application directly — no placeholder to convert
        app_id, created = add_application(
            company=company_normalized,
            job_url=job_url,
            job_title=job_title,
            applied_date=applied_date,
            expected_domain=expected_domain,
        )

        if not app_id:
            logger.error("Prospective conversion: failed to create application for %r", company_normalized)
            print(f"[ERROR] Failed to create application for {company_normalized}.")
            return

        if not created:
            logger.warning("Prospective conversion: job URL already exists for %r (id=%s)",
                           company_normalized, app_id)
            print(f"[WARNING] Job URL already exists in DB (id={app_id}).")
            return

        # Link best recruiters (cap enforced inside link_top_recruiters_for_company)
        from db.db import link_top_recruiters_for_company
        linked = link_top_recruiters_for_company(app_id, company_normalized)

        mark_prospective_converted(company_normalized)
        logger.info("Prospective → active: %r (id=%s) linked=%d",
                    company_normalized, app_id, linked)
        print(f"[OK] Converted prospective → active (id={app_id})")
        print(f"[INFO] Linked {linked} recruiter(s) for {company_normalized}")
        print("[INFO] Outreach will be scheduled on next --outreach-only run.")
        return
        # Note: no fall-through needed — real application created directly

    logger.info("Adding application: company=%r url=%r title=%r date=%s",
                company, job_url, job_title, applied_date)
    app_id, created = add_application(
        company=company,
        job_url=job_url,
        job_title=job_title,
        applied_date=applied_date,
        expected_domain=expected_domain,
    )

    if not app_id:
        logger.error("Failed to insert application for %r", company)
        print("[ERROR] Failed to add application.")
        return

    if not created:
        # URL already exists — backfill expected_domain only if currently NULL
        if expected_domain:
            existing = get_application_by_id(app_id)
            if existing and not existing.get("expected_domain"):
                update_application_expected_domain(app_id, expected_domain)
        logger.warning("Job URL already exists in DB for %r (id=%s)", company, app_id)
        print("[WARNING] Job URL already exists in DB.")
        return

    logger.info("Application added: %r (id=%s)", company, app_id)
    print(f"[OK] Added: {company} | {job_url} (id={app_id})")

    print(f"[INFO] Scraping job description from {job_url}...")
    logger.debug("Scraping JD for %r from %s", company, job_url)
    try:
        from jobs.job_fetcher import fetch_job_description
        result = fetch_job_description(job_url)
        if result:
            logger.info("JD cached for %r", company)
            print(f"[OK] Job description cached for {company}")
        else:
            logger.warning("Could not scrape JD for %r — will retry during --find-only", company)
            print(f"[WARNING] Could not scrape job description. Will retry during --find-only.")
    except Exception as e:
        logger.error("JD scraping failed for %r: %s", company, e, exc_info=True)
        print(f"[WARNING] JD scraping failed: {e}. Will retry during --find-only.")


def run_sync_forms():
    """Pull Google Form responses, insert into DB, scrape JDs, clean up sheet."""
    logger.info("run_sync_forms called")
    print("\n" + "=" * 55)
    print("[INFO] Syncing Google Form responses")
    print("=" * 55)
    from jobs.form_sync import run as sync_run
    sync_run()

def run_sync_prospective():
    """Pull Google Form responses, insert into DB, scrape JDs, clean up sheet."""
    logger.info("run_sync_prospective called")
    print("\n" + "=" * 55)
    print("[INFO] Syncing Google Form responses")
    print("=" * 55)
    from jobs.prospective_form_sync import run as sync_prospective
    sync_prospective()

def run_find_emails():
    """Scrape CareerShift for recruiters + generate AI content + quota health check."""
    from datetime import datetime
    from outreach.report_templates.find_report import build_find_report

    logger.info("run_find_emails starting")
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

    print("\n" + "=" * 55)
    print("[INFO] STEP 4: Pipeline performance check")
    print("=" * 55)
    run_performance_report()
    run_pipeline_alert_report()

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
    logger.info("run_find_emails complete: quota_used=%d ai_generated=%d ai_failed=%d",
                report_stats["quota_used"], report_stats["ai_generated"],
                report_stats["ai_failed"])
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
        logger.info("No active applications found for AI content generation")
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
        logger.debug("Generating AI content for %r | %s", company, job_title)

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
            logger.info("AI content generated for %r", company)
            print(f"  [OK] AI content ready for {company}")
            generated += 1
        else:
            logger.warning("AI generation failed for %r (quota exhausted?)", company)
            print(f"  [WARNING] AI generation failed for {company} (quota exhausted?)")
            failed += 1

    # Leftover Gemini quota utilization
    if not all_models_exhausted():
        missing = get_applications_missing_ai_cache()
        if missing:
            logger.info("Using leftover Gemini quota for %d application(s) missing cache",
                        len(missing))
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
                    logger.info("Leftover quota used for: %r", company)
                    print(f"  [OK] Leftover quota used for: {company}")
                    generated += 1

    logger.info("AI content complete: generated=%d skipped=%d failed=%d",
                generated, skipped, failed)
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
        logger.error("Prospects file not found: %s", filepath)
        print(f"[ERROR] File not found: {filepath}")
        print(f"[INFO] Format: one company per line, optional domain:")
        print(f"[INFO]   Stripe")
        print(f"[INFO]   Capital One,capitalone.com")
        return

    logger.info("Importing prospective companies from: %s", filepath)
    print(f"\n[INFO] Importing prospective companies from {filepath}...")

    added   = 0
    skipped = 0
    with open(filepath, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue

            # Support "Company,domain.com" format
            parts   = line.split(",", 1)
            company = parts[0].strip()
            domain  = parts[1].strip() if len(parts) > 1 else None

            if not company:
                continue

            if add_prospective_company(company, domain=domain):
                domain_str = f" ({domain})" if domain else ""
                logger.debug("Imported prospective: %r domain=%s", company, domain)
                print(f"  [+] {company}{domain_str}")
                added += 1
            else:
                skipped += 1  # already exists, domain backfilled by add_prospective_company

    logger.info("Import complete: added=%d skipped=%d", added, skipped)
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
        logger.error("Session file not found: %s", SESSION_FILE)
        print("[ERROR] Session file not found. Run careershift/auth.py first.")
        return

    print("\n" + "=" * 55)
    print("[INFO] STEP 1: Recruiter Verification")
    print("=" * 55)

    applications = get_all_active_applications()
    if not applications:
        logger.info("No active applications found for verification")
        print("[INFO] No active applications found.")
        return

    logger.info("run_verify_only: %d applications", len(applications))

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
            logger.error("CareerShift session expired")
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
        logger.info("All companies have enough active recruiters")
        print("[OK] All companies have enough active recruiters.")
    else:
        logger.warning("%d company/companies under-stocked after verification: %s",
                       len(under_stocked), under_stocked)
        print(f"\n[WARNING] {len(under_stocked)} company/companies under-stocked after verification:")
        for company in under_stocked:
            recruiters   = get_recruiters_by_company(company)
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
        recruiters   = get_recruiters_by_company(company)
        active_count = len(recruiters)
        under_stocked_detail.append({
            "company":      company,
            "active_count": active_count,
            "needed":       max(0, MIN_RECRUITERS_PER_COMPANY - active_count),
        })

    logger.info("Verification report: t2_verified=%d t3_verified=%d t3_inactive=%d under_stocked=%d",
                verify_stats.get("tier2_verified", 0),
                verify_stats.get("tier3_verified", 0),
                verify_stats.get("tier3_inactive", 0),
                len(under_stocked_detail))

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
    logger.info("run_outreach starting")
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
        logger.info("run_outreach complete: %s", stats)
        build_outreach_report(stats)


def run_performance_report():
    """
    Show pipeline performance metrics (metric1 + metric2)
    for the last METRIC_ALERT_CONSECUTIVE_DAYS days.
    Sends an alert email if thresholds are breached.
    Same pattern as run_quota_report().
    """
    from db.alerts import get_coverage_stats
    from db.pipeline_alerts import check_pipeline_health, mark_notified
    from outreach.email_sender import send_email
    from config import (
        METRIC1_ALERT_THRESHOLD, METRIC2_ALERT_THRESHOLD,
        METRIC_ALERT_CONSECUTIVE_DAYS, EMAIL,
    )

    logger.info("run_performance_report starting")

    stats = get_coverage_stats(days=METRIC_ALERT_CONSECUTIVE_DAYS)

    print("\n" + "=" * 55)
    print("[INFO] Pipeline Performance Report")
    print("=" * 55)

    if not stats:
        print("[INFO] No coverage stats found yet.")
        print("[INFO] Stats are written at the end of each --find-only run.")
        logger.info("Performance report: no data yet")
        return

    for row in stats:
        m1 = f"{row['metric1']:.1f}%" if row.get("metric1") is not None else "N/A"
        m2 = f"{row['metric2']:.1f}%" if row.get("metric2") is not None else "N/A"
        print(f"  {row['date']}  "
              f"attempted={row['companies_attempted']}  "
              f"found={row['auto_found']}  "
              f"exhausted={row['exhausted_count']}  "
              f"metric1={m1}  metric2={m2}")

    print(f"\n  Thresholds:  metric1 >= {METRIC1_ALERT_THRESHOLD}%  "
          f"metric2 >= {METRIC2_ALERT_THRESHOLD}%")
    print("=" * 55)

    alerts = check_pipeline_health()

    if not alerts:
        logger.info("Performance report: no threshold breaches")
        print("[OK] Performance thresholds met — no alerts.")
        return

    # Filter out deduped alerts (those without alert_id)
    alerts = [a for a in alerts if a.get("alert_id") or a.get("id")]

    if not alerts:
        logger.info("Performance report: all alerts were deduped")
        print("[OK] All alerts were already notified — no action needed.")
        return

    logger.warning("Performance report: %d alert(s) detected", len(alerts))

    body_parts = []
    for alert in alerts:
        label = (
            "FIND-ONLY PERFORMANCE (Metric 1)"
            if alert["alert_type"] == "metric1_low"
            else "OUTREACH COVERAGE (Metric 2)"
        )
        body_parts.append(f"{label} - DEGRADED")
        body_parts.append("")
        for row in alert.get("history", []):
            m = row.get("metric1") if alert["alert_type"] == "metric1_low" \
                else row.get("metric2")
            val = f"{m:.1f}%" if m is not None else "N/A"
            body_parts.append(f"  {row['date']}: {val}")
        body_parts.append("")
        body_parts.append(
            f"Average: {alert['value']:.1f}% — "
            f"below {alert['threshold']:.0f}% threshold"
        )
        body_parts.append("")
        body_parts.append(
            "Recommendation: Review CareerShift search terms or "
            "manually reactivate exhausted applications:"
        )
        body_parts.append('  python pipeline.py --reactivate "CompanyName"')
        body_parts.append("")
        body_parts.append("-" * 40)
        body_parts.append("")

    body    = "\n".join(body_parts)
    subject = f"Pipeline Performance Alert — {len(alerts)} issue(s) detected"

    try:
        send_email(to_email=EMAIL, body=body,
                   company="Pipeline", subject=subject)
        # Only mark alerts as notified after successful email delivery
        logger.info("Performance alert email sent: %s", subject)
        print(f"[INFO] Alert email sent: {subject}")
        for alert in alerts:
            alert_id = alert.get("alert_id") or alert.get("id")
            if alert_id:
                mark_notified(alert_id)
    except Exception as e:
        logger.error("Could not send performance alert email: %s",
                     e, exc_info=True)
        print(f"[WARNING] Could not send alert email: {e}")
        print(f"[INFO] Alert details:\n{body}")
        # Do NOT mark_notified on failure — alerts will be retried on next run


def run_reactivate(company_name):
    """
    Reactivate an exhausted application by company name.
    Resets status to 'active' and clears exhausted_at.
    """
    from db.db import reactivate_application

    if not company_name:
        print("[ERROR] Company name required.")
        print('[INFO] Usage: python pipeline.py --reactivate "CompanyName"')
        return

    logger.info("run_reactivate: company=%r", company_name)
    result = reactivate_application(company_name)

    if result:
        logger.info("Reactivated application for %r", company_name)
        print(f"[OK] Reactivated: {company_name}")
        print("[INFO] Application will be included in next --find-only run.")
    else:
        logger.warning("Reactivate: no exhausted application found for %r",
                       company_name)
        print(f"[WARNING] No exhausted application found for '{company_name}'.")
        print("[INFO] Check company name spelling or use --add to add a new application.")


def run_pipeline_alert_report():
    """
    Send emails for any unnotified pipeline_alerts rows.
    Called after --find-only and --monitor-jobs.
    Handles both metric alerts (from check_pipeline_health)
    and API health alerts (from check_api_health).
    Same pattern as run_quota_report().
    """
    from db.pipeline_alerts import get_unnotified_alerts, mark_notified
    from outreach.email_sender import send_email
    from config import EMAIL

    alerts = get_unnotified_alerts()
    if not alerts:
        logger.debug("run_pipeline_alert_report: no unnotified alerts")
        return

    logger.warning("run_pipeline_alert_report: %d unnotified alert(s)",
                   len(alerts))

    critical = [a for a in alerts if a["severity"] == "critical"]
    warnings = [a for a in alerts if a["severity"] == "warning"]

    for group, label in [(critical, "CRITICAL"), (warnings, "WARNING")]:
        if not group:
            continue

        body_parts = [f"Pipeline Alert — {label}\n"]
        for alert in group:
            platform = f" [{alert['platform']}]" if alert.get("platform") else ""
            body_parts.append(f"  {alert['alert_type'].upper()}{platform}")
            body_parts.append(f"  {alert['message']}")
            body_parts.append(
                f"  Value: {alert['value']}  Threshold: {alert['threshold']}"
            )
            body_parts.append("")

        body    = "\n".join(body_parts)
        subject = f"Pipeline {label} Alert — {len(group)} issue(s) detected"

        try:
            send_email(to_email=EMAIL, body=body,
                       company="Pipeline", subject=subject)
            # Only mark alerts as notified after successful email delivery
            logger.info("Pipeline alert email sent: %s", subject)
            print(f"[INFO] Alert email sent: {subject}")
            for alert in group:
                mark_notified(alert["id"])
        except Exception as e:
            logger.error("Could not send pipeline alert email: %s",
                         e, exc_info=True)
            print(f"[WARNING] Could not send alert email: {e}")
            # Do NOT mark_notified on failure — alerts will be retried on next run


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
            logger.info("Quota health: no issues detected")
            print("[OK] Quota health: no issues detected.")
        return

    logger.warning("Quota alerts detected: %d issue(s)", len(alerts))

    # Build combined alert email
    subject    = "Quota Alert - Action Required"
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

    if len(alerts) > 1:
        subject = f"Quota Alert - Action Required ({len(alerts)} issues)"

    try:
        from config import EMAIL
        send_email(to_email=EMAIL, body=body, company="Pipeline", subject=subject)
        logger.info("Quota alert email sent: %s", subject)
        print(f"[INFO] Quota alert email sent: {subject}")
        for alert in alerts:
            save_quota_alert(alert)
    except Exception as e:
        logger.error("Could not send quota alert email: %s", e, exc_info=True)
        print(f"[WARNING] Could not send quota alert email: {e}")
        print(f"[INFO] Alert details:\n{body}")
        for alert in alerts:
            save_quota_alert({**alert, "notified": False})


def main():
    init_db()
    args = sys.argv[1:]

    logger.info("pipeline.py invoked with args: %s", args)

    if "--weekly-summary" in args:
        from outreach.report_templates.weekly_summary import (
            build_weekly_summary
        )
        build_weekly_summary()
        return

    if "--sync-forms" in args:
        run_sync_forms()
        return

    if "--sync-prospective" in args:
        run_sync_prospective()
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
        filepath = next((a for a in args if not a.startswith("--")), "prospects.txt")
        run_import_prospects(filepath)
        return

    if "--prospects-status" in args:
        run_prospects_status()
        return

    if "--monitor-jobs" in args:
        from jobs.job_monitor import run as monitor_run
        monitor_run()
        # Check API health after monitor run and send any alerts
        from db.pipeline_alerts import check_api_health
        check_api_health()
        run_pipeline_alert_report()
        return

    if "--detect-ats" in args:
        from jobs.job_monitor import run_detect_ats
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
    
    if "--diagnostics" in args:
        from jobs.job_monitor import run_diagnostics
        run_diagnostics()
        return
    
    if "--resolve-diagnostic" in args:
        from jobs.job_monitor import run_resolve_diagnostic

        # Parse explicit flags for clarity
        diagnostic_id = None
        company = None

        # Check for positional diagnostic id immediately following --resolve-diagnostic
        try:
            idx = args.index("--resolve-diagnostic")
            if idx + 1 < len(args):
                next_arg = args[idx + 1]
                # Check if next arg is a positive integer (not another flag)
                if not next_arg.startswith("--"):
                    try:
                        diagnostic_id = int(next_arg)
                    except ValueError:
                        pass  # fall through to flag parsing
        except (ValueError, IndexError):
            pass

        # Check for --diagnostic-id flag
        if "--diagnostic-id" in args:
            try:
                idx = args.index("--diagnostic-id")
                if idx + 1 < len(args):
                    diagnostic_id = int(args[idx + 1])
            except (ValueError, IndexError):
                print('[ERROR] --diagnostic-id requires an integer argument')
                return

        # Check for --company flag
        if "--company" in args:
            try:
                idx = args.index("--company")
                if idx + 1 < len(args):
                    company = args[idx + 1]
            except IndexError:
                print('[ERROR] --company requires a company name argument')
                return

        run_resolve_diagnostic(diagnostic_id=diagnostic_id, company=company)
        return

    if "--quota-report" in args:
        run_quota_report()
        return

    if "--performance-report" in args:
        run_performance_report()
        run_pipeline_alert_report()
        return

    if "--reactivate" in args:
        non_flag_args = [a for a in args if not a.startswith("--")]
        company_name  = non_flag_args[0] if non_flag_args else ""
        run_reactivate(company_name)
        return

    if "--verify-filled" in args:
        from jobs.fill_verifier import run as verify_filled_run
        verify_filled_run()
        return
    
    if "--set-custom-ats" in args:
        from jobs.set_custom_ats import run as run_set_custom_ats
        curl_idx      = args.index("--curl") if "--curl" in args else None
        detail_idx    = args.index("--detail-curl") if "--detail-curl" in args else None

        # Determine positions of flag values to exclude from non_flag_args
        excluded_positions = set()
        if curl_idx is not None and curl_idx + 1 < len(args):
            excluded_positions.add(curl_idx + 1)
        if detail_idx is not None and detail_idx + 1 < len(args):
            excluded_positions.add(detail_idx + 1)

        # Pick company as the first non-flag arg that's not a flag value
        non_flag_args = [
            args[i] for i in range(len(args))
            if not args[i].startswith("--") and i not in excluded_positions
        ]
        company       = non_flag_args[0] if non_flag_args else None
        curl_string   = args[curl_idx + 1] if curl_idx is not None and curl_idx + 1 < len(args) else None
        detail_curl   = args[detail_idx + 1] if detail_idx is not None and detail_idx + 1 < len(args) else None
        if not company or not curl_string:
            print('[ERROR] Usage: --set-custom-ats "Company" --curl "curl ..."')
            return
        run_set_custom_ats(company, curl_string, detail_curl=detail_curl)
        return

    # Full pipeline
    logger.info("Full pipeline run starting")
    print("[INFO] Starting Recruiter Outreach Pipeline")
    print(f"[INFO] {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print(f"[INFO] CareerShift quota remaining: {get_remaining_quota()}/50")

    add_job_interactively()
    run_find_emails()
    run_outreach()

    print("\n" + "=" * 55)
    print("[DONE] Pipeline complete!")
    print(f"[INFO] CareerShift quota remaining: {get_remaining_quota()}/50")
    logger.info("Full pipeline run complete")


if __name__ == "__main__":
    main()