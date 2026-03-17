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
    get_domain_for_prospective
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
            #logger.debug("Recruiter already in DB: %s (id=%s)", contact["email"], recruiter_id)
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
            #logger.info("Saved recruiter: %s | %s (company=%r)",
            #            contact["name"], contact["email"], company)
            logger.info("Saved recruiter id=%s for company=%r", recruiter_id, company)
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
            logger.debug("Prospective recruiter already in DB: %s", contact["email"])
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

                # Get all matching applications and expected_domain for this company
                matching_apps, expected_domain = _get_apps_and_domain(
                    applications, company
                )

                contacts = scrape_company(page, company, max_contacts,
                                          expected_domain)

                if contacts is None:
                    # None = skip (weak signal, retry tomorrow) — not exhausted
                    logger.info("Step 2: %r — weak signal, skipping (retry tomorrow)", company)
                    print(f"   [INFO] Skipping {company} — weak signal, retry tomorrow")
                elif not contacts:
                    # [] = exhaust — mark ALL matching applications exhausted atomically
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
                        # Save recruiters — only mark scraped if persistence succeeded
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
    logger.info("════ --find-only finished ════")

    return {
        "quota_used":            quota_used,
        "companies":             _scrape_stats,
        "prospective_scraped":   _prospective_stats.get("scraped", 0),
        "prospective_exhausted": _prospective_stats.get("exhausted", 0),
    }


if __name__ == "__main__":
    run()