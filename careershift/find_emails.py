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
)
from careershift.constants import SESSION_FILE, MIN_RECRUITERS_PER_COMPANY
from careershift.utils import human_delay
from careershift.quota_manager import fetch_real_quota, calculate_distribution
from careershift.verification import run_tiered_verification
from careershift.scraper import scrape_company

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
            print(f"   [SKIP] Already in DB: {contact['email']} (id={recruiter_id})")
        else:
            recruiter_id = add_recruiter(
                company=company,
                name=contact["name"],
                position=contact["position"],
                email=contact["email"],
                confidence=contact["confidence"],
            )
            print(f"   [DB] Saved: {contact['name']} | {contact['email']}")

        for app in matching_apps:
            link_recruiter_to_application(app["id"], recruiter_id)
            print(f"   [INFO] Linked to application id={app['id']} ({app['job_title'] or app['job_url']})")


def _save_prospective_contacts(contacts, company):
    """
    Save prospective recruiter contacts to DB under a placeholder application.
    When user later applies (--add), the placeholder converts to real application.
    Returns True if contacts were saved successfully, False otherwise.
    """
    from db.db import add_application

    # Create placeholder application for this prospective company
    placeholder_url = f"prospective://{company.lower().replace(' ', '-')}"
    app_id, _ = add_application(
        company=company,
        job_url=placeholder_url,
        job_title=None,
        status_override="prospective",
    )

    if not app_id:
        print(f"   [WARNING] Could not create placeholder for {company}")
        return False

    for contact in contacts:
        existing_id = recruiter_email_exists(contact["email"])
        if existing_id:
            recruiter_id = existing_id
            print(f"   [SKIP] Already in DB: {contact['email']}")
        else:
            recruiter_id = add_recruiter(
                company=company,
                name=contact["name"],
                position=contact["position"],
                email=contact["email"],
                confidence=contact["confidence"],
            )
            print(f"   [DB] Prospective saved: {contact['name']} | {contact['email']}")

        link_recruiter_to_application(app_id, recruiter_id)

    return True



def run():
    if not os.path.exists(SESSION_FILE):
        print("[ERROR] Session file not found. Run careershift/auth.py first.")
        return

    init_db()

    applications = get_all_active_applications()
    pending_prospective = get_pending_prospective()

    if not applications and not pending_prospective:
        print("[INFO] No active applications and no pending prospective companies.")
        print("[INFO] Add an application: python pipeline.py --add")
        print("[INFO] Import prospects:   python pipeline.py --import-prospects prospects.txt")
        return

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

        # Fetch real remaining quota
        remaining = fetch_real_quota(page)
        print(f"[INFO] Quota remaining today: {remaining}/50\n")

        # ─────────────────────────────────────────
        # STEP 1: Tiered verification
        # ─────────────────────────────────────────
        if applications:
            print("=" * 55)
            print("[INFO] STEP 1: Verifying existing recruiters (tiered)...")
            run_tiered_verification(page, applications)
        else:
            print("[INFO] STEP 1: Skipped — no active applications to verify.")

        # ─────────────────────────────────────────
        # STEP 2: Scrape new companies
        # ─────────────────────────────────────────
        companies_to_scrape = get_unique_companies_needing_scraping(MIN_RECRUITERS_PER_COMPANY) if applications else []

        if not companies_to_scrape:
            print("\n[OK] All applications have enough recruiters. No scraping needed.")
        elif remaining == 0:
            print(f"\n[WARNING] {len(companies_to_scrape)} companies need scraping but quota is 0.")
            print("    Run again tomorrow when quota resets.")
        else:
            print(f"\n{'='*55}")
            print(f"[INFO] STEP 2: Scraping {len(companies_to_scrape)} company/companies")
            print(f"[INFO] Quota: {remaining} credits / {len(companies_to_scrape)} companies")

            counts = calculate_distribution(remaining, len(companies_to_scrape))
            print(f"[INFO] Distribution: {counts}\n")

            for i, company in enumerate(companies_to_scrape):
                max_contacts = counts[i] if i < len(counts) else 0
                if max_contacts == 0:
                    print(f"[SKIP] Skipping {company} — no quota remaining")
                    continue

                print(f"\n{'='*55}")
                print(f"[INFO] [{i+1}/{len(companies_to_scrape)}] {company} (max {max_contacts})")

                # Get all matching applications and expected_domain for this company
                matching_apps, expected_domain = _get_apps_and_domain(
                    applications, company
                )

                contacts = scrape_company(page, company, max_contacts,
                                          expected_domain)

                if contacts is None:
                    # None = skip (weak signal, retry tomorrow) — not exhausted
                    print(f"   [INFO] Skipping {company} — weak signal, retry tomorrow")
                elif not contacts:
                    # [] = exhaust — mark ALL matching applications exhausted atomically
                    print(f"   [INFO] Exhausting {company} — no valid recruiters found")
                    mark_applications_exhausted([app["id"] for app in matching_apps])
                else:
                    _save_contacts(contacts, company, applications)

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
                print(f"\n{'='*55}")
                print(f"[INFO] STEP 3: Leftover quota utilization")
                print(f"[INFO] {remaining_after} credits remaining — topping up {len(under_stocked)} company/companies")

                for company_row in under_stocked:
                    if get_remaining_quota() == 0:
                        break

                    company   = company_row["company"]
                    shortage  = company_row["shortage"]
                    max_extra = min(shortage, get_remaining_quota())

                    print(f"\n[INFO] {company} — needs {shortage} more recruiter(s), fetching {max_extra}")

                    matching_apps, expected_domain = _get_apps_and_domain(
                        applications, company
                    )

                    contacts = scrape_company(page, company, max_extra,
                                              expected_domain)

                    if contacts is None:
                        print(f"   [INFO] Skipping {company} — weak signal")
                    elif contacts:
                        _save_contacts(contacts, company, applications)

                    human_delay(3.0, 7.0)

        # ─────────────────────────────────────────
        # STEP 3 — Priority 2: Prospective companies
        # ─────────────────────────────────────────
        remaining_prospective = get_remaining_quota()
        if remaining_prospective > 0:
            pending = get_pending_prospective()
            if pending:
                print(f"\n{'='*55}")
                print(f"[INFO] STEP 3 (Priority 2): Pre-scraping prospective companies")
                print(f"[INFO] {remaining_prospective} credits remaining — "
                      f"{len(pending)} prospective companies pending")

                for prospect in pending:
                    if get_remaining_quota() == 0:
                        break

                    company  = prospect["company"]
                    max_extra = min(3, get_remaining_quota())

                    print(f"\n[INFO] Prospective: {company} (max {max_extra})")

                    contacts = scrape_company(page, company, max_extra, "")

                    if contacts is None:
                        print(f"   [INFO] Skipping {company} — weak signal, retry tomorrow")
                    elif not contacts:
                        print(f"   [INFO] Exhausting prospective {company}")
                        mark_prospective_exhausted(company)
                    else:
                        # Save recruiters — only mark scraped if persistence succeeded
                        saved = _save_prospective_contacts(contacts, company)
                        if saved:
                            mark_prospective_scraped(company)
                        else:
                            print(f"   [WARNING] Could not persist contacts for {company} — not marking scraped")

                    human_delay(3.0, 7.0)

        browser.close()

    remaining_after = get_remaining_quota()
    print(f"\n{'='*55}")
    print(f"[OK] Done! Quota used: {50 - remaining_after}/50 | Remaining: {remaining_after}/50")


if __name__ == "__main__":
    run()