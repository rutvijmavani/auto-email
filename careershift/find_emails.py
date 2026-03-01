"""
careershift/find_emails.py — Scrapes CareerShift for recruiter contacts.

Flow:
1. For each application, check if company already has >= 2 recruiters in DB
   → YES: validate (update verified_at), link to application, schedule outreach
   → NO:  add company to scraping list

2. Collect all unique companies needing scraping
   → distribute remaining quota: contacts = remaining_quota / unique_companies
   → scrape each company, save recruiters, link to all matching applications

3. Schedule outreach for all newly linked recruiters
"""

import os
import re
import time
import random
from playwright.sync_api import sync_playwright
from bs4 import BeautifulSoup
from dotenv import load_dotenv

from db.db import (
    init_db,
    get_conn,
    get_all_active_applications,
    get_recruiters_by_company,
    get_recruiters_by_tier,
    add_recruiter,
    update_recruiter,
    mark_recruiter_inactive,
    recruiter_email_exists,
    link_recruiter_to_application,
    get_unique_companies_needing_scraping,
    get_remaining_quota,
    increment_quota_used,
)
from datetime import datetime

load_dotenv()

SESSION_FILE = os.path.join(os.path.dirname(__file__), "..", "data", "careershift_session.json")
CAREERSHIFT_SEARCH_URL = "https://www.careershift.com/App/Contacts/Search"
MAX_CONTACTS_HARD_CAP = 3
MIN_RECRUITERS_PER_COMPANY = 2  # threshold — below this triggers scraping

HR_SEARCH_TERMS = ["Recruiter", "Talent Acquisition", "Human Resources", "People Operations", "HR"]

HR_KEYWORDS_STRONG = [
    "recruiter", "recruiting", "recruitment",
    "talent acquisition", "talent partner",
    "human resources", "hr manager", "hr director",
    "hr business partner", "hrbp", "hr generalist",
    "people operations", "people partner",
    "staffing", "head of people",
    "vp of people", "vp hr", "director of hr",
    "hr specialist", "hr coordinator",
]

HR_KEYWORDS_LOOSE = [
    "people", "hiring", "workforce", "culture",
    "talent", "onboarding", " hr", "human capital",
]

EXCLUDE_KEYWORDS = [
    "chief executive", "ceo", "chief technology", "cto",
    "chief operating", "coo", "chief financial", "cfo",
    "chief marketing", "cmo", "chief information", "cio",
    "chief people", "chief hr", "chief human resources",
    "founder", "co-founder", "president",
    "board member", "board of director",
    "managing partner", "general partner",
    "executive vice president", "evp",
    "senior vice president", "svp",
    "vice president", " vp ",
]


CAREERSHIFT_QUOTA_URL = "https://www.careershift.com/App/Settings/ResetPassword"

# Tiered verification thresholds (days)
TIER1_DAYS = 30   # trust — skip verification
TIER2_DAYS = 60   # lightweight search check
# > TIER2_DAYS  → full profile visit (free, cached)


def fetch_real_quota(page):
    """
    Fetch actual remaining quota from CareerShift Account Usage page.
    Navigates to Settings > Reset Password page which contains the quota table.
    Updates local DB to match real value.
    Returns remaining quota as integer.
    """
    try:
        page.goto(CAREERSHIFT_QUOTA_URL, wait_until="domcontentloaded", timeout=30000)
        human_delay(2.0, 3.0)

        html = page.content()
        soup = BeautifulSoup(html, "html.parser")

        # Find the Account Usage table with Contacts / Companies / Remaining columns
        tables = soup.find_all("table")
        for table in tables:
            headers = [th.get_text(strip=True).lower() for th in table.find_all("th")]
            if "remaining" in headers:
                rows = table.find_all("tr")
                for row in rows[1:]:  # skip header row
                    cols = [td.get_text(strip=True) for td in row.find_all("td")]
                    if cols:
                        remaining_idx = headers.index("remaining")
                        remaining = int(cols[remaining_idx])

                        # Sync local DB with real value
                        conn = get_conn()
                        c = conn.cursor()
                        today = datetime.now().strftime("%Y-%m-%d")
                        used = 50 - remaining
                        c.execute("""
                            INSERT INTO careershift_quota (date, total_limit, used, remaining)
                            VALUES (?, 50, ?, ?)
                            ON CONFLICT(date) DO UPDATE SET
                                used = excluded.used,
                                remaining = excluded.remaining
                        """, (today, used, remaining))
                        conn.commit()
                        conn.close()

                        print(f"[INFO] Real CareerShift quota — Remaining: {remaining}/50")
                        return remaining

        print("[WARNING] Could not parse quota from account usage page. Using local DB value.")
        return get_remaining_quota()

    except Exception as e:
        print(f"[WARNING] Could not fetch real quota: {e}. Using local DB value.")
        return get_remaining_quota()


def human_delay(min_sec=1.0, max_sec=3.0):
    time.sleep(random.uniform(min_sec, max_sec))


def verify_tier2_recruiter(page, recruiter):
    """
    Tier 2: Lightweight verification — search by company name and look for
    recruiter name in results. No profile visit needed.
    Returns True if recruiter found, False if missing (escalate to Tier 3).
    """
    company = recruiter["company"]
    name = recruiter["name"].strip().lower()

    try:
        ok = submit_search(page, company, hr_term=None, require_email=False)
        if not ok:
            return True  # search failed — assume still valid

        human_delay(2.0, 3.0)
        html = page.content()
        cards = parse_cards_from_html(html)

        for card_name, _, _, _ in cards:
            if name in card_name.strip().lower():
                update_recruiter(recruiter["id"])
                print(f"     [OK] Tier 2 verified: {recruiter['name']} still at {company}")
                return True

        print(f"     [WARNING] Tier 2: {recruiter['name']} not found in search results — escalating to Tier 3")
        return False

    except Exception as e:
        print(f"     [WARNING] Tier 2 check failed for {recruiter['name']}: {e} — assuming valid")
        return True


def verify_tier3_recruiter(page, recruiter):
    """
    Tier 3: Full profile visit — free since profile is cached.
    Checks company, title, and email are still current.
    Updates DB or marks inactive accordingly.
    """
    name = recruiter["name"]
    company = recruiter["company"]

    # Search for recruiter by name + company to get their detail URL
    try:
        ok = submit_search(page, company, hr_term=None, require_email=False)
        if not ok:
            update_recruiter(recruiter["id"])
            return

        human_delay(2.0, 3.0)
        html = page.content()
        cards = parse_cards_from_html(html)

        detail_url = None
        for card_name, position, url, _ in cards:
            if name.strip().lower() in card_name.strip().lower():
                detail_url = url
                break

        if not detail_url:
            print(f"     [INFO] Tier 3: {name} not found at {company} — marking inactive")
            mark_recruiter_inactive(recruiter["id"], reason="not found in company search")
            return

        # Visit profile (cached — free)
        human_delay(3.0, 6.0)
        page.goto(detail_url, wait_until="domcontentloaded", timeout=20000)
        human_delay(2.0, 4.0)

        html = page.content()
        soup = BeautifulSoup(html, "html.parser")

        # Check company still matches
        page_text = soup.get_text(separator=" ").lower()
        if company.lower() not in page_text:
            print(f"     [INFO] Tier 3: {name} no longer at {company} — marking inactive")
            mark_recruiter_inactive(recruiter["id"], reason="company mismatch on profile")
            return

        # Check for updated email
        new_email = extract_email(page)
        current_email = recruiter["email"]

        # Check for updated title
        h4s = soup.find_all("h4")
        new_position = h4s[1].get_text(strip=True) if len(h4s) >= 2 else None

        # Update fields if changed
        updates = {}
        if new_email and new_email != current_email:
            print(f"     [INFO] Email updated: {current_email} → {new_email}")
            updates["email"] = new_email
        if new_position and new_position != recruiter["position"]:
            print(f"     [INFO] Title updated: {recruiter['position']} → {new_position}")
            updates["position"] = new_position

        update_recruiter(recruiter["id"], **updates)
        print(f"     [OK] Tier 3 verified: {name} still active at {company}")

        page.goto(f"{CAREERSHIFT_SEARCH_URL}#contacts_search_results",
                  wait_until="domcontentloaded", timeout=20000)
        human_delay(2.0, 3.0)

    except Exception as e:
        print(f"     [WARNING] Tier 3 check failed for {name}: {e}")
        update_recruiter(recruiter["id"])  # update timestamp at minimum


def run_tiered_verification(page, applications):
    """
    Run tiered verification for all existing recruiters.

    Tier 1 (< 30 days):  skip — recently verified, trust as-is
    Tier 2 (30-60 days): lightweight search check — free, no profile visit
    Tier 3 (> 60 days):  full profile visit — free (cached), update all fields

    After verification, link all active recruiters to their applications.
    """
    tiers = get_recruiters_by_tier(TIER1_DAYS, TIER2_DAYS)

    tier1 = tiers["tier1"]
    tier2 = tiers["tier2"]
    tier3 = tiers["tier3"]

    print(f"\n[INFO] Recruiter verification tiers:")
    print(f"  Tier 1 (< {TIER1_DAYS} days, trust):        {len(tier1)} recruiter(s) — skipping")
    print(f"  Tier 2 ({TIER1_DAYS}-{TIER2_DAYS} days, search check): {len(tier2)} recruiter(s)")
    print(f"  Tier 3 (> {TIER2_DAYS} days, full visit):   {len(tier3)} recruiter(s)")

    # Tier 2: lightweight search check
    if tier2:
        print(f"\n[INFO] Running Tier 2 verification ({len(tier2)} recruiter(s))...")
        for recruiter in tier2:
            print(f"  Checking: {recruiter['name']} @ {recruiter['company']}")
            found = verify_tier2_recruiter(page, recruiter)
            if not found:
                # Escalate to Tier 3
                verify_tier3_recruiter(page, recruiter)
            human_delay(1.0, 2.0)

    # Tier 3: full profile visit
    if tier3:
        print(f"\n[INFO] Running Tier 3 verification ({len(tier3)} recruiter(s))...")
        for recruiter in tier3:
            print(f"  Verifying: {recruiter['name']} @ {recruiter['company']}")
            verify_tier3_recruiter(page, recruiter)
            human_delay(1.0, 2.0)

    # Link all still-active recruiters to their applications
    print(f"\n[INFO] Linking verified recruiters to applications...")
    for app in applications:
        existing = get_recruiters_by_company(app["company"])
        linked = 0
        for recruiter in existing:
            link_recruiter_to_application(app["id"], recruiter["id"])
            linked += 1
        if linked:
            print(f"  [OK] {app['company']}: linked {linked} recruiter(s) to application id={app['id']}")


def slow_type(element, text):
    for char in text:
        element.type(char)
        time.sleep(random.uniform(0.05, 0.18))


def classify_title(title):
    t = title.lower().strip()
    for kw in HR_KEYWORDS_STRONG:
        if kw in t:
            return "auto"
    for kw in HR_KEYWORDS_LOOSE:
        if kw in t:
            return "manual_review"
    return None


def is_excluded_title(title):
    t = f" {title.lower().strip()} "
    for kw in EXCLUDE_KEYWORDS:
        if kw in t:
            return True
    return False


def parse_cards_from_html(html):
    """Parse result cards. Returns list of (name, position, detail_url, has_email)."""
    soup = BeautifulSoup(html, "html.parser")
    cards = soup.find_all("li", attrs={"data-type": "contact"})
    results = []
    for card in cards:
        try:
            name_tag = card.find("h3", class_="title")
            name = name_tag.get_text(strip=True) if name_tag else ""
            h4s = card.find_all("h4")
            position = h4s[1].get_text(strip=True) if len(h4s) >= 2 else ""
            detail_link = card.find("a", href=re.compile(r"/App/Contacts/SearchDetails"))
            detail_url = ""
            if detail_link:
                href = detail_link.get("href", "")
                detail_url = "https://www.careershift.com" + href if href.startswith("/") else href
            has_email = card.find("span", class_="fa-envelope-o") is not None
            if name and position and detail_url:
                results.append((name, position, detail_url, has_email))
        except:
            continue
    return results


def extract_email(page):
    """Extract email from a CareerShift contact details page."""
    try:
        email_el = page.locator("a[href^='mailto:']").first
        email_el.wait_for(timeout=5000)
        raw = email_el.get_attribute("href") or ""
        email = raw.replace("mailto:", "").strip()
        if email:
            return email
    except:
        pass
    try:
        candidate = page.locator("span:has-text('@'), a:has-text('@'), p:has-text('@')").first
        candidate.wait_for(timeout=3000)
        text = candidate.inner_text().strip()
        if "@" in text and "linkedin" not in text.lower():
            return text
    except:
        pass
    try:
        content = page.content()
        matches = re.findall(r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}", content)
        for m in matches:
            if all(x not in m for x in ["careershift", "springshare", "linkedin",
                                          "google", "hubspot", "sentry"]):
                return m
    except:
        pass
    return None


def submit_search(page, company, hr_term=None, require_email=True):
    """Submit a CareerShift search with optional filters. Returns True if successful."""
    try:
        page.goto(CAREERSHIFT_SEARCH_URL, wait_until="domcontentloaded", timeout=30000)
        human_delay(2.0, 4.0)
    except Exception as e:
        print(f"   [ERROR] Could not load search page: {e}")
        return False

    try:
        company_input = page.locator("input[placeholder='Company Name']").first
        company_input.wait_for(timeout=5000)
        company_input.click()
        human_delay(0.3, 0.7)
        slow_type(company_input, company)
        human_delay(1.0, 2.0)
        try:
            suggestion = page.locator(
                "ul.autocomplete li, .autocomplete-suggestion, [class*='suggest'] li, [class*='dropdown'] li"
            ).first
            suggestion.wait_for(timeout=3000)
            human_delay(0.3, 0.6)
            suggestion.click()
            human_delay(0.3, 0.6)
        except:
            pass
    except Exception as e:
        print(f"   [WARNING]  Could not fill Company Name: {e}")
        return False

    if hr_term or require_email:
        try:
            page.evaluate("""
                () => {
                    const cb = document.querySelector('#advanced-search');
                    if (cb && !cb.checked) cb.click();
                }
            """)
            human_delay(0.8, 1.5)
            advanced_open = page.evaluate(
                "() => document.querySelector('#advanced-search')?.checked === true"
            )
            if not advanced_open:
                raise Exception("Advanced toggle did not open")
            print(f"   [OK] Advanced search opened")
        except Exception as e:
            print(f"   [WARNING]  Advanced search failed: {e} — aborting.")
            return False

        if hr_term:
            try:
                title_input = page.locator("input#Title").first
                title_input.wait_for(state="visible", timeout=5000)
                title_input.click()
                human_delay(0.2, 0.5)
                slow_type(title_input, hr_term)
                human_delay(0.5, 1.0)
                filled = title_input.input_value()
                if not filled.strip():
                    raise Exception("Job Title field empty after fill")
                print(f"   [OK] Job Title: '{filled}'")
            except Exception as e:
                print(f"   [WARNING]  Job Title failed: {e} — aborting.")
                return False

        if require_email:
            try:
                page.evaluate("""
                    () => {
                        const cb = document.querySelector('#RequireEmail');
                        if (cb && !cb.checked) cb.click();
                    }
                """)
                human_delay(0.3, 0.6)
                checked = page.evaluate(
                    "() => document.querySelector('#RequireEmail')?.checked === true"
                )
                if not checked:
                    raise Exception("RequireEmail did not get checked")
                print(f"   [OK] RequireEmail enabled")
            except Exception as e:
                print(f"   [WARNING]  RequireEmail failed: {e} — aborting.")
                return False

    try:
        search_btn = page.locator("button.search-button").first
        search_btn.wait_for(timeout=3000)
        human_delay(0.4, 0.9)
        search_btn.click()
        human_delay(2.5, 4.5)
    except Exception as e:
        print(f"   [WARNING]  Could not click search: {e}")
        return False

    return True


def visit_and_extract(page, detail_url, name, position, confidence, is_fallback=False):
    """Visit profile, extract email, update quota. Returns contact dict or None."""
    try:
        human_delay(4.0, 8.0)
        page.goto(detail_url, wait_until="domcontentloaded", timeout=20000)
        human_delay(3.0, 6.0)
        increment_quota_used(1)

        email = extract_email(page)
        if email:
            print(f"         [INFO] {email}")
            page.goto(f"{CAREERSHIFT_SEARCH_URL}#contacts_search_results",
                      wait_until="domcontentloaded", timeout=20000)
            human_delay(3.0, 5.0)
            return {
                "name": name,
                "position": position,
                "email": email,
                "confidence": "manual_review" if is_fallback else confidence,
            }
        else:
            print(f"         [SKIP]  No email — skipping {name}")
            page.goto(f"{CAREERSHIFT_SEARCH_URL}#contacts_search_results",
                      wait_until="domcontentloaded", timeout=20000)
            human_delay(2.0, 3.0)
            return None
    except Exception as e:
        print(f"         [WARNING]  Profile visit failed: {e}")
        return None


def scan_and_collect(page, max_contacts, exclude_senior=True, is_fallback=False):
    """Scan current result pages and collect up to max_contacts profiles."""
    found = []
    for batch in range(5):
        if len(found) >= max_contacts:
            break
        print(f"      [INFO] Page {batch + 1}...")
        human_delay(1.0, 2.0)
        html = page.content()
        cards = parse_cards_from_html(html)
        print(f"      [INFO] {len(cards)} card(s)")
        if not cards:
            break

        for name, position, detail_url, has_email in cards:
            if len(found) >= max_contacts:
                break
            confidence = classify_title(position)
            if not confidence:
                continue
            if exclude_senior and is_excluded_title(position):
                continue
            if not has_email:
                continue
            print(f"      [OK] {name} | {position} | {confidence}")
            contact = visit_and_extract(page, detail_url, name, position,
                                         confidence, is_fallback)
            if contact:
                found.append(contact)

        if len(found) >= max_contacts:
            break
        try:
            next_btn = page.locator("button.btnNext").first
            next_btn.wait_for(timeout=3000)
            if next_btn.is_visible() and next_btn.is_enabled():
                human_delay(0.5, 1.2)
                next_btn.click()
                human_delay(2.5, 4.0)
            else:
                break
        except:
            break

    return found


def scrape_company(page, company, max_contacts):
    """
    3-pass search for a company:
    Pass 1: HR title + RequireEmail + exclude senior titles
    Pass 2: HR title + RequireEmail + include senior titles
    Pass 3: No filters + exclude senior titles
    """
    found = []

    # Pass 1: filtered, no senior
    for hr_term in HR_SEARCH_TERMS:
        if len(found) >= max_contacts:
            break
        print(f"   [INFO] '{hr_term}'")
        if not submit_search(page, company, hr_term=hr_term, require_email=True):
            continue
        try:
            page.wait_for_selector("li[data-type='contact']", timeout=6000)
        except:
            continue
        found += scan_and_collect(page, max_contacts - len(found),
                                   exclude_senior=True, is_fallback=False)
        if len(found) >= MIN_RECRUITERS_PER_COMPANY:
            break
        human_delay(2.0, 4.0)

    # Pass 2: filtered, include senior
    if len(found) < MIN_RECRUITERS_PER_COMPANY:
        print(f"   [INFO] Senior titles fallback...")
        for hr_term in HR_SEARCH_TERMS:
            if len(found) >= max_contacts:
                break
            if not submit_search(page, company, hr_term=hr_term, require_email=True):
                continue
            try:
                page.wait_for_selector("li[data-type='contact']", timeout=6000)
            except:
                continue
            found += scan_and_collect(page, max_contacts - len(found),
                                       exclude_senior=False, is_fallback=True)
            if len(found) >= MIN_RECRUITERS_PER_COMPANY:
                break
            human_delay(2.0, 4.0)

    # Pass 3: unfiltered
    if len(found) < MIN_RECRUITERS_PER_COMPANY:
        print(f"   [INFO] Unfiltered fallback...")
        if submit_search(page, company, hr_term=None, require_email=False):
            try:
                page.wait_for_selector("li[data-type='contact']", timeout=6000)
                found += scan_and_collect(page, max_contacts - len(found),
                                           exclude_senior=True, is_fallback=True)
            except:
                pass

    return found


def calculate_distribution(remaining_quota, company_count):
    """
    Distribute quota fairly across companies, fully utilizing all credits.
    Returns list of per-company contact counts.
    """
    if company_count == 0 or remaining_quota == 0:
        return [0] * company_count

    base = remaining_quota // company_count
    extra = remaining_quota % company_count

    # Cap base at MAX_CONTACTS_HARD_CAP
    if base >= MAX_CONTACTS_HARD_CAP:
        # All companies get the cap, no extra needed
        return [MAX_CONTACTS_HARD_CAP] * company_count

    counts = []
    for i in range(company_count):
        if base == 0:
            # Not enough quota for all companies — first `extra` get 1, rest get 0
            counts.append(1 if i < extra else 0)
        else:
            # Give first `extra` companies one more than base
            if i < extra and base + 1 <= MAX_CONTACTS_HARD_CAP:
                counts.append(base + 1)
            else:
                counts.append(base)
    return counts


def run():
    if not os.path.exists(SESSION_FILE):
        print("[ERROR] Session file not found. Run careershift/auth.py first.")
        return

    init_db()

    applications = get_all_active_applications()
    if not applications:
        print("[INFO] No active applications found. Add one with: python pipeline.py --add")
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

        # Fetch real remaining quota from CareerShift settings page
        remaining = fetch_real_quota(page)
        print(f"[INFO] Quota remaining today: {remaining}/50\n")

        # ─────────────────────────────────────────
        # STEP 1: Tiered verification of existing recruiters
        # ─────────────────────────────────────────
        print("=" * 55)
        print("[INFO] STEP 1: Verifying existing recruiters (tiered)...")
        run_tiered_verification(page, applications)

        # ─────────────────────────────────────────
        # STEP 2: Scrape companies that need more recruiters
        # ─────────────────────────────────────────
        companies_to_scrape = get_unique_companies_needing_scraping(MIN_RECRUITERS_PER_COMPANY)

        if not companies_to_scrape:
            print("\n[OK] All applications have enough recruiters. No scraping needed.")
        elif remaining == 0:
            print(f"\n[WARNING]  {len(companies_to_scrape)} companies need scraping but quota is 0.")
            print("    Run again tomorrow when quota resets.")
        else:
            print(f"\n{'='*55}")
            print(f"[INFO] STEP 2: Scraping {len(companies_to_scrape)} unique company/companies")
            print(f"[INFO] Quota: {remaining} credits / {len(companies_to_scrape)} companies")

            counts = calculate_distribution(remaining, len(companies_to_scrape))
            print(f"[INFO] Distribution: {counts}\n")

            for i, company in enumerate(companies_to_scrape):
                max_contacts = counts[i] if i < len(counts) else 0
                if max_contacts == 0:
                    print(f"[SKIP]  Skipping {company} — no quota remaining")
                    continue

                print(f"\n{'='*55}")
                print(f"[INFO] [{i+1}/{len(companies_to_scrape)}] {company} (max {max_contacts})")

                contacts = scrape_company(page, company, max_contacts)

                # Save new recruiters and link to ALL matching applications
                matching_apps = [a for a in applications if a["company"] == company]

                for contact in contacts:
                    existing_id = recruiter_email_exists(contact["email"])
                    if existing_id:
                        recruiter_id = existing_id
                        print(f"   [SKIP]  Already in DB: {contact['email']} (id={recruiter_id})")
                    else:
                        recruiter_id = add_recruiter(
                            company=company,
                            name=contact["name"],
                            position=contact["position"],
                            email=contact["email"],
                            confidence=contact["confidence"],
                        )
                        print(f"   [DB] Saved: {contact['name']} | {contact['email']}")

                    # Link to all applications for this company
                    for app in matching_apps:
                        link_recruiter_to_application(app["id"], recruiter_id)
                        print(f"   [INFO] Linked to application id={app['id']} ({app['job_title'] or app['job_url']})")

                if not contacts:
                    print(f"   [ERROR] No contacts found for {company}")

                human_delay(3.0, 7.0)

        browser.close()

    remaining_after = get_remaining_quota()
    print(f"\n{'='*55}")
    print(f"[OK] Done! Quota used: {50 - remaining_after}/50 | Remaining: {remaining_after}/50")


if __name__ == "__main__":
    run()