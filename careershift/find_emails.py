"""
careershift/find_emails.py — Orchestrates CareerShift recruiter scraping.

Multi-user two-phase design:
  Phase 1 (per user, in id ASC order):
    1. Verify existing recruiters found by this user's account
    2. Scrape new companies for this user's applications
  Phase 2 (per user, using leftover quota):
    3. Top-up under-stocked companies across all users
    4. Pre-scrape pending prospective companies
"""

import os
import random
from playwright.sync_api import sync_playwright
from dotenv import load_dotenv

from logger import get_logger
from db.db import (
    init_db,
    get_all_active_users,
    get_all_active_applications,
    get_unique_companies_needing_scraping,
    get_companies_needing_more_recruiters,
    get_remaining_quota,
    get_today_quota,
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
from careershift.constants import session_file_for_user, MIN_RECRUITERS_PER_COMPANY
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
    """
    matching_apps = [a for a in applications if a["company"] == company]
    expected_domain = next(
        (a.get("expected_domain") for a in matching_apps
         if a.get("expected_domain")),
        ""
    )
    return matching_apps, expected_domain


def _save_contacts(contacts, company, applications, user_id: int = 1):
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
                found_by_user_id=user_id,
            )
            logger.info("Saved recruiter: id=%s company=%r found_by_user_id=%d",
                        recruiter_id, company, user_id)
            print(f"   [DB] Saved: {contact['name']} | {contact['email']}")

        for app in matching_apps:
            link_recruiter_to_application(app["id"], recruiter_id)
            logger.debug("Linked recruiter id=%s to application id=%s (%s)",
                         recruiter_id, app["id"], app.get("job_title") or app.get("job_url"))
            print(f"   [INFO] Linked to application id={app['id']} ({app['job_title'] or app['job_url']})")


def _save_prospective_contacts(contacts, company, user_id: int = 1):
    """
    Save prospective recruiter contacts to DB at company level only.
    Returns True if at least one contact was saved or already exists.
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
                found_by_user_id=user_id,
            )
            if recruiter_id:
                logger.info("Saved prospective recruiter: %s | %s (company=%r found_by=%d)",
                            contact["name"], contact["email"], company, user_id)
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
    Called once before the scraping loop.
    """
    try:
        from db.alerts import get_coverage_stats
        from config import (
            METRIC1_ALERT_THRESHOLD,
            METRIC2_ALERT_THRESHOLD,
            METRIC_ALERT_CONSECUTIVE_DAYS,
        )

        recent = get_coverage_stats(days=METRIC_ALERT_CONSECUTIVE_DAYS, user_id=1)

        if len(recent) < METRIC_ALERT_CONSECUTIVE_DAYS:
            logger.debug(
                "_check_pipeline_degraded: only %d/%d days history — allowing exhaustions",
                len(recent), METRIC_ALERT_CONSECUTIVE_DAYS,
            )
            return False

        from datetime import date as _date_type
        try:
            dates = [_date_type.fromisoformat(s["date"]) for s in recent]
            for i in range(len(dates) - 1):
                if (dates[i] - dates[i + 1]).days != 1:
                    logger.debug(
                        "_check_pipeline_degraded: non-contiguous dates %s to %s — allowing exhaustions",
                        dates[i], dates[i + 1],
                    )
                    return False
        except (ValueError, KeyError):
            return False

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
        return degraded

    except Exception as e:
        logger.error("_check_pipeline_degraded check failed: %s", e, exc_info=True)
        return False


def _open_user_session(p, user_id: int, user_name: str):
    """
    Launch a Playwright browser and verify the CareerShift session for user_id.
    Returns (browser, page) on success, or (None, None) if session is missing/expired.
    """
    session_file = session_file_for_user(user_id)

    if not os.path.exists(session_file):
        logger.error("Session file missing for user_id=%d (%s): %s", user_id, user_name, session_file)
        print(f"[ERROR] Session file not found for user_id={user_id} ({user_name}): {session_file}")
        print(f"[ERROR] Run: python careershift/auth_njit.py --user-id {user_id}")
        return None, None

    browser = p.chromium.launch(headless=True, slow_mo=100)
    context = browser.new_context(
        storage_state=session_file,
        user_agent=random.choice(USER_AGENTS),
        viewport={"width": random.randint(1280, 1920), "height": random.randint(768, 1080)},
    )
    page = context.new_page()

    print(f"[INFO] Verifying CareerShift session for user_id={user_id} ({user_name})...")
    page.goto("https://www.careershift.com/App/Dashboard/Overview",
              wait_until="domcontentloaded", timeout=30000)
    human_delay(2.0, 4.0)

    if "login" in page.url.lower() or "signin" in page.url.lower():
        logger.error("Session expired for user_id=%d (%s) — skipping user", user_id, user_name)
        print(f"[ERROR] Session expired for user_id={user_id}. Re-run auth and scp session file.")
        browser.close()
        return None, None

    print(f"[OK] Session valid for user_id={user_id}.\n")
    return browser, page


def run():
    logger.info("════════════════════════════════════════")
    logger.info("--find-only starting")

    init_db()

    users = get_all_active_users()
    if not users:
        logger.error("No active users found in DB")
        print("[ERROR] No active users found. Run: python scripts/add_user.py --name ... --email ...")
        return

    logger.info("Active users: %d", len(users))

    pipeline_degraded = _check_pipeline_degraded()
    if pipeline_degraded:
        print("[WARNING] Pipeline degraded over last N days — exhaustions blocked (human review required)")

    all_scrape_stats      = []
    all_prospective_stats = {"scraped": 0, "exhausted": 0}

    with sync_playwright() as p:
        for user in users:
            user_id   = user["id"]
            user_name = user["name"]

            logger.info("════ Phase 1 — user_id=%d (%s) ════", user_id, user_name)
            print(f"\n{'='*55}")
            print(f"[INFO] Phase 1 — user_id={user_id} ({user_name})")

            browser, page = _open_user_session(p, user_id, user_name)
            if page is None:
                continue
            try:
                # Fetch real quota for this user
                remaining = fetch_real_quota(page, user_id)
                logger.info("user_id=%d quota: %d/50 remaining", user_id, remaining)
                print(f"[INFO] user_id={user_id} quota remaining: {remaining}/50\n")

                user_apps = get_all_active_applications(user_id=user_id)
                logger.info("user_id=%d active applications: %d", user_id, len(user_apps))

                # ─── STEP 1: Tiered verification for this user's recruiters ───
                if user_apps:
                    print("=" * 55)
                    print(f"[INFO] STEP 1 (user_id={user_id}): Tiered recruiter verification")
                    logger.info("Step 1 user_id=%d: verifying recruiters — %d applications",
                                user_id, len(user_apps))
                    run_tiered_verification(page, user_apps, found_by_user_id=user_id)
                else:
                    logger.info("Step 1 user_id=%d: skipped — no active applications", user_id)
                    print(f"[INFO] STEP 1 (user_id={user_id}): Skipped — no active applications.")

                # ─── STEP 2: Scrape companies for this user's applications ───
                companies_to_scrape = (
                    get_unique_companies_needing_scraping(MIN_RECRUITERS_PER_COMPANY, user_id=user_id)
                    if user_apps else []
                )
                logger.info("Step 2 user_id=%d: %d companies need scraping",
                            user_id, len(companies_to_scrape))

                scrape_stats = []
                if not companies_to_scrape:
                    print(f"\n[OK] user_id={user_id}: All applications have enough recruiters.")
                elif remaining == 0:
                    logger.warning("Step 2 user_id=%d: %d companies need scraping but quota is 0",
                                   user_id, len(companies_to_scrape))
                    print(f"\n[WARNING] user_id={user_id}: {len(companies_to_scrape)} companies need "
                          f"scraping but quota is 0. Run again tomorrow when quota resets.")
                else:
                    print(f"\n{'='*55}")
                    print(f"[INFO] STEP 2 (user_id={user_id}): Scraping {len(companies_to_scrape)} company/companies")
                    print(f"[INFO] Quota: {remaining} credits / {len(companies_to_scrape)} companies")

                    counts = calculate_distribution(remaining, len(companies_to_scrape))
                    logger.info("Step 2 user_id=%d distribution=%s", user_id, counts)
                    print(f"[INFO] Distribution: {counts}\n")

                    for i, company in enumerate(companies_to_scrape):
                        max_contacts = counts[i] if i < len(counts) else 0
                        if max_contacts == 0:
                            logger.debug("Skipping %r — no quota for user_id=%d", company, user_id)
                            print(f"[SKIP] Skipping {company} — no quota remaining")
                            continue

                        print(f"\n{'='*55}")
                        print(f"[INFO] [{i+1}/{len(companies_to_scrape)}] {company} (max {max_contacts})")
                        logger.info("Step 2 user_id=%d [%d/%d]: scraping %r (max_contacts=%d)",
                                    user_id, i + 1, len(companies_to_scrape), company, max_contacts)

                        matching_apps, expected_domain = _get_apps_and_domain(user_apps, company)
                        contacts = scrape_company(page, company, max_contacts, expected_domain,
                                                  user_id=user_id)

                        if contacts is None:
                            logger.info("Step 2 user_id=%d: %r — weak signal, skipping", user_id, company)
                            print(f"   [INFO] Skipping {company} — weak signal, retry tomorrow")
                            scrape_stats.append({"name": company, "status": "skipped", "count": 0})
                        elif not contacts:
                            if pipeline_degraded:
                                logger.warning(
                                    "Step 2 user_id=%d: %r — no recruiters but pipeline degraded "
                                    "— blocking exhaustion",
                                    user_id, company,
                                )
                                print(f"   [WARNING] {company} — no recruiters found but pipeline "
                                      f"degraded — NOT exhausting (human review needed)")
                                try:
                                    from db.pipeline_alerts import (
                                        create_alert, ALERT_EXHAUSTION_BLOCKED, CRITICAL,
                                    )
                                    create_alert(
                                        alert_type=ALERT_EXHAUSTION_BLOCKED,
                                        severity=CRITICAL,
                                        platform=company,
                                        message=(
                                            f"Exhaustion blocked for '{company}': "
                                            f"no recruiters found but pipeline metrics "
                                            f"are below threshold — manual review required"
                                        ),
                                    )
                                except Exception as _ae:
                                    logger.error("Failed to create exhaustion-blocked alert: %s", _ae)
                                scrape_stats.append({"name": company, "status": "blocked", "count": 0})
                            else:
                                logger.info("Step 2 user_id=%d: %r — no valid recruiters, exhausting",
                                            user_id, company)
                                print(f"   [INFO] Exhausting {company} — no valid recruiters found")
                                mark_applications_exhausted([app["id"] for app in matching_apps])
                                scrape_stats.append({"name": company, "status": "exhausted", "count": 0})
                        else:
                            logger.info("Step 2 user_id=%d: %r — found %d contact(s)",
                                        user_id, company, len(contacts))
                            _save_contacts(contacts, company, user_apps, user_id=user_id)
                            scrape_stats.append({
                                "name": company, "status": "found", "count": len(contacts),
                            })

                        human_delay(3.0, 7.0)

                all_scrape_stats.extend(scrape_stats)

                # ─── STEP 3 (Phase 2): Leftover quota — top-up + prospective ───
                remaining_after = get_remaining_quota(user_id=user_id)
                if remaining_after > 0:
                    # All active applications across users — needed for domain lookup + linking
                    all_applications = get_all_active_applications()

                    under_stocked = get_companies_needing_more_recruiters(user_id=user_id)
                    # Exclude companies already scrapped for this user in Step 2
                    under_stocked = [c for c in under_stocked
                                     if c["company"] not in companies_to_scrape]

                    if under_stocked:
                        logger.info("Step 3 user_id=%d: %d under-stocked companies, %d credits",
                                    user_id, len(under_stocked), remaining_after)
                        print(f"\n{'='*55}")
                        print(f"[INFO] STEP 3 (user_id={user_id}): Leftover quota — top-up")
                        print(f"[INFO] {remaining_after} credits remaining — "
                              f"topping up {len(under_stocked)} company/companies")

                        for company_row in under_stocked:
                            current_remaining = get_remaining_quota(user_id=user_id)
                            if current_remaining == 0:
                                logger.info("Step 3 user_id=%d: quota exhausted — stopping top-up", user_id)
                                break

                            company   = company_row["company"]
                            shortage  = company_row["shortage"]
                            max_extra = min(shortage, current_remaining)

                            logger.info("Step 3 user_id=%d: topping up %r (shortage=%d max_extra=%d)",
                                        user_id, company, shortage, max_extra)
                            print(f"\n[INFO] {company} — needs {shortage} more recruiter(s), fetching {max_extra}")

                            matching_apps, expected_domain = _get_apps_and_domain(all_applications, company)
                            contacts = scrape_company(page, company, max_extra, expected_domain,
                                                      user_id=user_id)

                            if contacts is None:
                                logger.info("Step 3 user_id=%d: %r — weak signal, skipping", user_id, company)
                                print(f"   [INFO] Skipping {company} — weak signal")
                            elif contacts:
                                logger.info("Step 3 user_id=%d: %r — found %d contact(s)",
                                            user_id, company, len(contacts))
                                _save_contacts(contacts, company, all_applications, user_id=user_id)

                            human_delay(3.0, 7.0)

                    # Prospective companies
                    remaining_prospective = get_remaining_quota(user_id=user_id)
                    if remaining_prospective > 0:
                        pending = get_pending_prospective()
                        if pending:
                            logger.info("Step 3 (Priority 2) user_id=%d: %d prospective companies, %d credits",
                                        user_id, len(pending), remaining_prospective)
                            print(f"\n{'='*55}")
                            print(f"[INFO] STEP 3 (Priority 2, user_id={user_id}): Pre-scraping prospective companies")
                            print(f"[INFO] {remaining_prospective} credits remaining — "
                                  f"{len(pending)} prospective companies pending")

                            for prospect in pending:
                                current_remaining = get_remaining_quota(user_id=user_id)
                                if current_remaining == 0:
                                    logger.info("Step 3 (Priority 2) user_id=%d: quota exhausted — stopping",
                                                user_id)
                                    break

                                company   = prospect["company"]
                                max_extra = min(3, current_remaining)

                                logger.info("Prospective scrape user_id=%d: %r (max_extra=%d)",
                                            user_id, company, max_extra)
                                print(f"\n[INFO] Prospective: {company} (max {max_extra})")

                                prospective_domain = get_domain_for_prospective(company)
                                contacts = scrape_company(page, company, max_extra, prospective_domain,
                                                          user_id=user_id)

                                if contacts is None:
                                    logger.info("Prospective %r — weak signal, skipping", company)
                                    print(f"   [INFO] Skipping {company} — weak signal, retry tomorrow")
                                elif not contacts:
                                    logger.info("Prospective %r — no contacts found, exhausting", company)
                                    print(f"   [INFO] Exhausting prospective {company}")
                                    mark_prospective_exhausted(company)
                                    all_prospective_stats["exhausted"] += 1
                                else:
                                    logger.info("Prospective %r — found %d contact(s), saving",
                                                company, len(contacts))
                                    saved = _save_prospective_contacts(contacts, company, user_id=user_id)
                                    if saved:
                                        mark_prospective_scraped(company)
                                        all_prospective_stats["scraped"] += 1
                                        logger.info("Prospective %r — marked scraped", company)
                                    else:
                                        logger.warning(
                                            "Prospective %r — could not persist contacts, not marking scraped",
                                            company,
                                        )
                                        print(f"   [WARNING] Could not persist contacts for {company} "
                                              f"— not marking scraped")

                                human_delay(3.0, 7.0)

            except Exception:
                logger.error("Error processing user_id=%d (%s)", user_id, user_name, exc_info=True)
            finally:
                browser.close()

    # ─── Summary ───
    total_quota_used = sum(
        get_today_quota(u["id"]).get("used", 0) for u in users
    )
    total_remaining = sum(get_remaining_quota(u["id"]) for u in users)

    logger.info(
        "--find-only complete: total_quota_used=%d total_remaining=%d "
        "prospective_scraped=%d prospective_exhausted=%d",
        total_quota_used, total_remaining,
        all_prospective_stats["scraped"],
        all_prospective_stats["exhausted"],
    )
    print(f"\n{'='*55}")
    print(f"[OK] Done! Total quota used: {total_quota_used} | Remaining: {total_remaining}")

    # ─── Coverage stats — metric1 + metric2 ───
    try:
        from datetime import date, timedelta
        from db.applications import get_applications_by_date
        from db.application_recruiters import get_sendable_count_for_date
        from db.alerts import save_coverage_stats

        yesterday      = (date.today() - timedelta(days=1)).strftime("%Y-%m-%d")
        yesterday_apps = get_applications_by_date(yesterday)   # all users
        total_apps     = len(yesterday_apps)

        companies_attempted = len(all_scrape_stats)
        auto_found          = sum(1 for s in all_scrape_stats if s["status"] == "found")
        exhausted_count     = sum(1 for s in all_scrape_stats if s["status"] == "exhausted")

        metric1 = (
            round((auto_found / companies_attempted) * 100, 1)
            if companies_attempted > 0 else None
        )

        sendable = get_sendable_count_for_date(yesterday)      # all users
        metric2  = (
            round((sendable / total_apps) * 100, 1)
            if total_apps > 0 else None
        )

        coverage_stats = {
            "total_applications":  total_apps,
            "companies_attempted": companies_attempted,
            "auto_found":          auto_found,
            "rejected_count":      0,
            "exhausted_count":     exhausted_count,
            "metric1":             metric1,
            "metric2":             metric2,
        }

        save_coverage_stats(coverage_stats, user_id=1)
        logger.info(
            "Coverage stats saved: yesterday=%s total_apps=%d attempted=%d "
            "auto_found=%d exhausted=%d metric1=%s metric2=%s",
            yesterday, total_apps, companies_attempted,
            auto_found, exhausted_count, metric1, metric2,
        )
        print(f"[INFO] Coverage stats: metric1={metric1}% metric2={metric2}%")

    except Exception as e:
        logger.error("Coverage stats save failed: %s", e, exc_info=True)

    logger.info("════ --find-only finished ════")

    return {
        "quota_used":            total_quota_used,
        "companies":             all_scrape_stats,
        "prospective_scraped":   all_prospective_stats["scraped"],
        "prospective_exhausted": all_prospective_stats["exhausted"],
    }


if __name__ == "__main__":
    run()
