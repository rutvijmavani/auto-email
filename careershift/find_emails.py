"""
careershift/find_emails.py — Orchestrates CareerShift recruiter scraping.

Steps:
  1. Verify existing recruiters (tiered verification)
  2. Scrape new companies needing recruiters
  3. Use leftover quota to top up under-stocked companies
"""

import os
import random
from playwright.sync_api import sync_playwright
from dotenv import load_dotenv

from logger import get_logger
from db.db import (
    init_db,
    get_all_active_applications,
    get_unique_companies_needing_scraping,
    get_companies_needing_more_recruiters,
    get_remaining_quota,
    recruiter_email_exists,
    add_recruiter,
    link_recruiter_to_application,
    mark_application_exhausted,
    mark_applications_exhausted,
    get_pending_prospective,
    mark_prospective_scraped,
    mark_prospective_exhausted,
    get_domain_for_prospective,
)
from careershift.constants import SESSION_FILE, MIN_RECRUITERS_PER_COMPANY
from careershift.utils import human_delay
from careershift.quota_manager import fetch_real_quota, calculate_distribution
from careershift.verification import run_tiered_verification
from careershift.scraper import scrape_company

logger = get_logger(__name__)

load_dotenv()

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:123.0) Gecko/20100101 Firefox/123.0",
]


def _get_apps_and_domain(applications, company):
    """
    Return (matching_apps, expected_domain) for a company.
    matching_apps: all active applications for this company.
    expected_domain: first non-empty expected_domain found, or "".
    Centralises repeated lookup used in Step 2 and Step 3.
    """
    matching_apps = [a for a in applications if a["company"] == company]
    expected_domain = next(
        (a.get("expected_domain") for a in matching_apps
         if a.get("expected_domain")),
        ""
    )
    return matching_apps, expected_domain


def _save_contacts(contacts, company, applications):
    """Save scraped contacts to DB and link to matching applications."""
    matching_apps = [a for a in applications if a["company"] == company]
    for contact in contacts:
        existing_id = recruiter_email_exists(contact["email"])
        if existing_id:
            recruiter_id = existing_id
            logger.debug("Recruiter already in DB: id=%s company=%r", recruiter_id, company)
            print(f"   [SKIP] Already in DB: {contact['email']} (id={recruiter_id})")
        else:
            recruiter_id = add_recruiter(
                company=company,
                name=contact["name"],
                position=contact["position"],
                email=contact["email"],
                confidence=contact["confidence"],
            )
            logger.info("Saved recruiter: id=%s company=%r", recruiter_id, company)
            print(f"   [DB] Saved: {contact['name']} | {contact['email']}")

        for app in matching_apps:
            link_recruiter_to_application(app["id"], recruiter_id)
            logger.debug("Linked recruiter id=%s to application id=%s (%s)",
                         recruiter_id, app["id"], app.get("job_title") or app.get("job_url"))
            print(f"   [INFO] Linked to application id={app['id']} ({app['job_title'] or app['job_url']})")


def _save_prospective_contacts(contacts, company):
    """
    Save prospective recruiter contacts to DB at company level only.
    No placeholder application created — recruiters are stored in the
    recruiters table and linked to a real application only when user
    applies via --add.
    Returns True if at least one contact was saved, False otherwise.
    """
    saved = 0
    for contact in contacts:
        existing_id = recruiter_email_exists(contact["email"])
        if existing_id:
            logger.debug("Prospective recruiter already in DB: company=%r", company)
            print(f"   [SKIP] Already in DB: {contact['email']}")
        else:
            recruiter_id = add_recruiter(
                company=company,
                name=contact["name"],
                position=contact["position"],
                email=contact["email"],
                confidence=contact["confidence"],
            )
            if recruiter_id:
                logger.info("Saved prospective recruiter: %s | %s (company=%r)",
                            contact["name"], contact["email"], company)
                print(f"   [DB] Prospective saved: {contact['name']} | {contact['email']}")
                saved += 1

    if not saved and not any(recruiter_email_exists(c["email"]) for c in contacts):
        logger.warning("_save_prospective_contacts: no contacts saved for %r", company)
        return False

    return True


def _check_pipeline_degraded():
    """
    Check if pipeline has been consistently unhealthy over last
    METRIC_ALERT_CONSECUTIVE_DAYS days using coverage_stats.

    Returns True if degraded (exhaustions should be blocked),
    False if healthy or insufficient history.
    Called once before Step 2 loop.
    """
    try:
        from db.alerts import get_coverage_stats
        from config import (
            METRIC1_ALERT_THRESHOLD,
            METRIC2_ALERT_THRESHOLD,
            METRIC_ALERT_CONSECUTIVE_DAYS,
        )

        recent = get_coverage_stats(days=METRIC_ALERT_CONSECUTIVE_DAYS)

        # Not enough history — allow exhaustions (safe default)
        if len(recent) < METRIC_ALERT_CONSECUTIVE_DAYS:
            logger.debug(
                "_check_pipeline_degraded: only %d/%d days history — allowing exhaustions",
                len(recent), METRIC_ALERT_CONSECUTIVE_DAYS,
            )
            return False

        # Verify dates are contiguous — same logic as check_pipeline_health.
        # If pipeline skipped a day, gaps should not trigger the guard.
        from datetime import date as _date_type
        try:
            dates = [_date_type.fromisoformat(s["date"]) for s in recent]
            for i in range(len(dates) - 1):
                if (dates[i] - dates[i + 1]).days != 1:
                    logger.debug(
                        "_check_pipeline_degraded: non-contiguous dates "
                        "%s to %s — allowing exhaustions",
                        dates[i], dates[i + 1],
                    )
                    return False
        except (ValueError, KeyError):
            return False

        # All-breach check — matches check_pipeline_health() logic exactly.
        # Pipeline is degraded only if EVERY day in the window breached threshold.
        m1_vals    = [s["metric1"] for s in recent if s.get("metric1") is not None]
        m2_vals    = [s["metric2"] for s in recent if s.get("metric2") is not None]
        m1_degraded = (
            len(m1_vals) == METRIC_ALERT_CONSECUTIVE_DAYS and
            all(v < METRIC1_ALERT_THRESHOLD for v in m1_vals)
        )
        m2_degraded = (
            len(m2_vals) == METRIC_ALERT_CONSECUTIVE_DAYS and
            all(v < METRIC2_ALERT_THRESHOLD for v in m2_vals)
        )
        degraded = m1_degraded or m2_degraded

        if degraded:
            logger.warning(
                "_check_pipeline_degraded: pipeline degraded — "
                "m1_degraded=%s m2_degraded=%s (window=%d days)",
                m1_degraded, m2_degraded, METRIC_ALERT_CONSECUTIVE_DAYS,
            )
        else:
            logger.debug(
                "_check_pipeline_degraded: pipeline healthy — "
                "m1_degraded=%s m2_degraded=%s",
                m1_degraded, m2_degraded,
            )

        return degraded

    except Exception as e:
        # On error — allow exhaustions (safe default, never block on check failure)
        logger.error("_check_pipeline_degraded check failed: %s", e, exc_info=True)
        return False


def run():
    logger.info("════════════════════════════════════════")
    logger.info("--find-only starting")

    if not os.path.exists(SESSION_FILE):
        logger.error("Session file not found: %s", SESSION_FILE)
        print("[ERROR] Session file not found. Run careershift/auth.py first.")
        return

    init_db()

    applications        = get_all_active_applications()
    pending_prospective = get_pending_prospective()

    logger.info("Active applications: %d  Pending prospective: %d",
                len(applications), len(pending_prospective))

    if not applications and not pending_prospective:
        logger.info("No active applications and no pending prospective companies — nothing to do")
        print("[INFO] No active applications and no pending prospective companies.")
        print("[INFO] Add an application: python pipeline.py --add")
        print("[INFO] Import prospects:   python pipeline.py --import-prospects prospects.txt")
        return

    # Stats tracking — initialized before playwright so accessible after
    _scrape_stats       = []
    _prospective_stats  = {"scraped": 0, "exhausted": 0}

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
            logger.error("CareerShift session expired — re-authentication required")
            print("[ERROR] Session expired. Run careershift/auth.py again.")
            browser.close()
            return

        print("[OK] Session valid.\n")

        # Fetch real remaining quota
        remaining = fetch_real_quota(page)
        logger.info("Quota remaining today: %d/50", remaining)
        print(f"[INFO] Quota remaining today: {remaining}/50\n")

        # ─────────────────────────────────────────
        # STEP 1: Tiered verification
        # ─────────────────────────────────────────
        if applications:
            print("=" * 55)
            print("[INFO] STEP 1: Verifying existing recruiters (tiered)...")
            logger.info("Step 1: tiered verification — %d applications", len(applications))
            run_tiered_verification(page, applications)
        else:
            logger.info("Step 1: skipped — no active applications")
            print("[INFO] STEP 1: Skipped — no active applications to verify.")

        # ─────────────────────────────────────────
        # STEP 2: Scrape new companies
        # ─────────────────────────────────────────
        companies_to_scrape = get_unique_companies_needing_scraping(MIN_RECRUITERS_PER_COMPANY) if applications else []
        logger.info("Step 2: %d companies need scraping", len(companies_to_scrape))

        # Check pipeline health BEFORE loop — gate exhaustions if degraded
        pipeline_is_degraded = _check_pipeline_degraded()
        if pipeline_is_degraded:
            print("[WARNING] Pipeline degraded over last N days — "
                  "exhaustions blocked (human review required)")

        if not companies_to_scrape:
            print("\n[OK] All applications have enough recruiters. No scraping needed.")
        elif remaining == 0:
            logger.warning("Step 2: %d companies need scraping but quota is 0",
                           len(companies_to_scrape))
            print(f"\n[WARNING] {len(companies_to_scrape)} companies need scraping but quota is 0.")
            print("    Run again tomorrow when quota resets.")
        else:
            print(f"\n{'='*55}")
            print(f"[INFO] STEP 2: Scraping {len(companies_to_scrape)} company/companies")
            print(f"[INFO] Quota: {remaining} credits / {len(companies_to_scrape)} companies")

            counts = calculate_distribution(remaining, len(companies_to_scrape))
            logger.info("Step 2: quota distribution=%s", counts)
            print(f"[INFO] Distribution: {counts}\n")

            for i, company in enumerate(companies_to_scrape):
                max_contacts = counts[i] if i < len(counts) else 0
                if max_contacts == 0:
                    logger.debug("Skipping %r — no quota remaining", company)
                    print(f"[SKIP] Skipping {company} — no quota remaining")
                    continue

                print(f"\n{'='*55}")
                print(f"[INFO] [{i+1}/{len(companies_to_scrape)}] {company} (max {max_contacts})")
                logger.info("Step 2 [%d/%d]: scraping %r (max_contacts=%d)",
                            i + 1, len(companies_to_scrape), company, max_contacts)

                matching_apps, expected_domain = _get_apps_and_domain(
                    applications, company
                )

                contacts = scrape_company(page, company, max_contacts,
                                          expected_domain)

                if contacts is None:
                    # None = weak signal → skip, retry tomorrow
                    logger.info("Step 2: %r — weak signal, skipping (retry tomorrow)", company)
                    print(f"   [INFO] Skipping {company} — weak signal, retry tomorrow")
                elif not contacts:
                    # [] = no recruiters found (CareerShift has no data or
                    # domain mismatch) — treat both as exhausted.
                    # Guard: if pipeline is degraded, block exhaustion and alert.
                    if pipeline_is_degraded:
                        logger.warning(
                            "Step 2: %r — no recruiters found but pipeline degraded "
                            "— blocking exhaustion, creating alert",
                            company,
                        )
                        print(f"   [WARNING] {company} — no recruiters found but "
                              f"pipeline degraded — NOT exhausting (human review needed)")
                        try:
                            from db.pipeline_alerts import (
                                create_alert, ALERT_EXHAUSTION_BLOCKED, CRITICAL,
                            )
                            create_alert(
                                alert_type=ALERT_EXHAUSTION_BLOCKED,
                                severity=CRITICAL,
                                platform=company,  # per-company dedup key
                                message=(
                                    f"Exhaustion blocked for '{company}': "
                                    f"no recruiters found but pipeline metrics "
                                    f"are below threshold — manual review required"
                                ),
                            )
                        except Exception as _ae:
                            logger.error("Failed to create exhaustion-blocked alert: %s", _ae)
                    else:
                        logger.info("Step 2: %r — no valid recruiters found, exhausting", company)
                        print(f"   [INFO] Exhausting {company} — no valid recruiters found")
                        mark_applications_exhausted([app["id"] for app in matching_apps])
                else:
                    logger.info("Step 2: %r — found %d contact(s)", company, len(contacts))
                    _save_contacts(contacts, company, applications)
                    _scrape_stats.append({
                        "name": company, "status": "found",
                        "count": len(contacts),
                    })

                # Track non-success statuses
                if contacts is None:
                    _scrape_stats.append({"name": company, "status": "skipped", "count": 0})
                elif contacts == []:
                    if pipeline_is_degraded:
                        # Record as blocked — not exhausted — so metrics are accurate
                        _scrape_stats.append({"name": company, "status": "blocked", "count": 0})
                    else:
                        _scrape_stats.append({"name": company, "status": "exhausted", "count": 0})

                human_delay(3.0, 7.0)

        # ─────────────────────────────────────────
        # STEP 3: Leftover quota utilization
        # ─────────────────────────────────────────
        remaining_after = get_remaining_quota()
        if remaining_after > 0:
            under_stocked = get_companies_needing_more_recruiters()
            under_stocked = [c for c in under_stocked
                             if c["company"] not in companies_to_scrape]

            if under_stocked:
                logger.info("Step 3: %d under-stocked companies, %d credits remaining",
                            len(under_stocked), remaining_after)
                print(f"\n{'='*55}")
                print(f"[INFO] STEP 3: Leftover quota utilization")
                print(f"[INFO] {remaining_after} credits remaining — topping up {len(under_stocked)} company/companies")

                for company_row in under_stocked:
                    if get_remaining_quota() == 0:
                        logger.info("Step 3: quota exhausted — stopping top-up")
                        break

                    company   = company_row["company"]
                    shortage  = company_row["shortage"]
                    max_extra = min(shortage, get_remaining_quota())

                    logger.info("Step 3: topping up %r (shortage=%d max_extra=%d)",
                                company, shortage, max_extra)
                    print(f"\n[INFO] {company} — needs {shortage} more recruiter(s), fetching {max_extra}")

                    matching_apps, expected_domain = _get_apps_and_domain(
                        applications, company
                    )

                    contacts = scrape_company(page, company, max_extra,
                                              expected_domain)

                    if contacts is None:
                        logger.info("Step 3: %r — weak signal, skipping", company)
                        print(f"   [INFO] Skipping {company} — weak signal")
                    elif contacts:
                        logger.info("Step 3: %r — found %d contact(s)", company, len(contacts))
                        _save_contacts(contacts, company, applications)

                    human_delay(3.0, 7.0)

        # ─────────────────────────────────────────
        # STEP 3 — Priority 2: Prospective companies
        # ─────────────────────────────────────────
        remaining_prospective = get_remaining_quota()
        if remaining_prospective > 0:
            pending = get_pending_prospective()
            if pending:
                logger.info("Step 3 (Priority 2): %d prospective companies, %d credits remaining",
                            len(pending), remaining_prospective)
                print(f"\n{'='*55}")
                print(f"[INFO] STEP 3 (Priority 2): Pre-scraping prospective companies")
                print(f"[INFO] {remaining_prospective} credits remaining — "
                      f"{len(pending)} prospective companies pending")

                for prospect in pending:
                    if get_remaining_quota() == 0:
                        logger.info("Step 3 (Priority 2): quota exhausted — stopping")
                        break

                    company   = prospect["company"]
                    max_extra = min(3, get_remaining_quota())

                    logger.info("Prospective scrape: %r (max_extra=%d)", company, max_extra)
                    print(f"\n[INFO] Prospective: {company} (max {max_extra})")

                    prospective_domain = get_domain_for_prospective(company)
                    contacts = scrape_company(page, company, max_extra, prospective_domain)
                    if contacts is None:
                        logger.info("Prospective %r — weak signal, skipping", company)
                        print(f"   [INFO] Skipping {company} — weak signal, retry tomorrow")
                    elif not contacts:
                        logger.info("Prospective %r — no contacts found, exhausting", company)
                        print(f"   [INFO] Exhausting prospective {company}")
                        mark_prospective_exhausted(company)
                    else:
                        logger.info("Prospective %r — found %d contact(s), saving",
                                    company, len(contacts))
                        saved = _save_prospective_contacts(contacts, company)
                        if saved:
                            mark_prospective_scraped(company)
                            _prospective_stats["scraped"] += 1
                            logger.info("Prospective %r — marked scraped", company)
                        else:
                            logger.warning("Prospective %r — could not persist contacts, not marking scraped",
                                           company)
                            print(f"   [WARNING] Could not persist contacts for {company} — not marking scraped")

                    if contacts == []:
                        _prospective_stats["exhausted"] += 1

                    human_delay(3.0, 7.0)

        browser.close()

    remaining_after = get_remaining_quota()
    quota_used      = 50 - remaining_after
    logger.info("--find-only complete: quota_used=%d remaining=%d "
                "prospective_scraped=%d prospective_exhausted=%d",
                quota_used, remaining_after,
                _prospective_stats.get("scraped", 0),
                _prospective_stats.get("exhausted", 0))
    print(f"\n{'='*55}")
    print(f"[OK] Done! Quota used: {quota_used}/50 | Remaining: {remaining_after}/50")

    # ─────────────────────────────────────────
    # COVERAGE STATS — metric1 + metric2
    # metric1: all scraped companies in Step 2 (pipeline run performance)
    # metric2: yesterday's applications with active recruiters (outreach coverage)
    # find-only runs at 1 AM EST so "yesterday" is the previous calendar day.
    # ─────────────────────────────────────────
    try:
        from datetime import date, timedelta
        from db.applications import get_applications_by_date
        from db.application_recruiters import get_sendable_count_for_date
        from db.alerts import save_coverage_stats

        yesterday      = (date.today() - timedelta(days=1)).strftime("%Y-%m-%d")
        yesterday_apps = get_applications_by_date(yesterday)
        total_apps     = len(yesterday_apps)

        # metric1 — find-only pipeline performance (all Step 2 companies)
        companies_attempted = len(_scrape_stats)
        auto_found          = sum(1 for s in _scrape_stats if s["status"] == "found")
        exhausted_count     = sum(1 for s in _scrape_stats if s["status"] == "exhausted")
        rejected_count      = 0  # reserved for future implementation

        metric1 = (
            round((auto_found / companies_attempted) * 100, 1)
            if companies_attempted > 0 else None
        )

        # metric2 — outreach coverage for yesterday's applications only
        sendable = get_sendable_count_for_date(yesterday)
        metric2  = (
            round((sendable / total_apps) * 100, 1)
            if total_apps > 0 else None
        )

        coverage_stats = {
            "total_applications":  total_apps,
            "companies_attempted": companies_attempted,
            "auto_found":          auto_found,
            "rejected_count":      rejected_count,
            "exhausted_count":     exhausted_count,
            "metric1":             metric1,
            "metric2":             metric2,
        }

        save_coverage_stats(coverage_stats)
        logger.info(
            "Coverage stats saved: date=today yesterday=%s "
            "total_apps=%d attempted=%d auto_found=%d "
            "exhausted=%d metric1=%s metric2=%s",
            yesterday, total_apps, companies_attempted,
            auto_found, exhausted_count, metric1, metric2,
        )
        print(f"[INFO] Coverage stats: metric1={metric1}% metric2={metric2}%")

    except Exception as e:
        # Never block the main pipeline on stats failure
        logger.error("Coverage stats save failed: %s", e, exc_info=True)

    logger.info("════ --find-only finished ════")

    return {
        "quota_used":            quota_used,
        "companies":             _scrape_stats,
        "prospective_scraped":   _prospective_stats.get("scraped", 0),
        "prospective_exhausted": _prospective_stats.get("exhausted", 0),
    }


if __name__ == "__main__":
    run()