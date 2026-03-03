# careershift/verification.py — Tiered recruiter verification

from bs4 import BeautifulSoup

from db.db import (
    get_recruiters_by_tier,
    get_recruiters_by_company,
    update_recruiter,
    mark_recruiter_inactive,
    link_recruiter_to_application,
)
from careershift.utils import human_delay
from careershift.search import submit_search, parse_cards_from_html, extract_email
from careershift.constants import CAREERSHIFT_SEARCH_URL, TIER1_DAYS, TIER2_DAYS


def verify_tier2_recruiter(page, recruiter):
    """
    Tier 2: Lightweight verification — search by company and look for name in cards.
    No profile visit needed. Returns True if found, False to escalate to Tier 3.
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

        print(f"     [WARNING] Tier 2: {recruiter['name']} not found — escalating to Tier 3")
        return False

    except Exception as e:
        print(f"     [WARNING] Tier 2 check failed for {recruiter['name']}: {e} — assuming valid")
        return True


def verify_tier3_recruiter(page, recruiter):
    """
    Tier 3: Full profile visit — free since profile is cached.
    Checks company, title, email still current. Updates DB or marks inactive.
    """
    name = recruiter["name"]
    company = recruiter["company"]

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

        human_delay(3.0, 6.0)
        page.goto(detail_url, wait_until="domcontentloaded", timeout=20000)
        human_delay(2.0, 4.0)

        html = page.content()
        soup = BeautifulSoup(html, "html.parser")

        page_text = soup.get_text(separator=" ").lower()
        if company.lower() not in page_text:
            print(f"     [INFO] Tier 3: {name} no longer at {company} — marking inactive")
            mark_recruiter_inactive(recruiter["id"], reason="company mismatch on profile")
            return

        new_email = extract_email(page)
        current_email = recruiter["email"]

        h4s = soup.find_all("h4")
        new_position = h4s[1].get_text(strip=True) if len(h4s) >= 2 else None

        updates = {}
        if new_email and new_email != current_email:
            print(f"     [INFO] Email updated: {current_email} -> {new_email}")
            updates["email"] = new_email
        if new_position and new_position != recruiter["position"]:
            print(f"     [INFO] Title updated: {recruiter['position']} -> {new_position}")
            updates["position"] = new_position

        update_recruiter(recruiter["id"], **updates)
        print(f"     [OK] Tier 3 verified: {name} still active at {company}")

        page.goto(f"{CAREERSHIFT_SEARCH_URL}#contacts_search_results",
                  wait_until="domcontentloaded", timeout=20000)
        human_delay(2.0, 3.0)

    except Exception as e:
        print(f"     [WARNING] Tier 3 check failed for {name}: {e}")
        update_recruiter(recruiter["id"])


def run_tiered_verification(page, applications):
    """
    Run tiered verification for all existing recruiters.
    After verification, link active recruiters to their applications.
    """
    tiers = get_recruiters_by_tier(TIER1_DAYS, TIER2_DAYS)

    tier1 = tiers["tier1"]
    tier2 = tiers["tier2"]
    tier3 = tiers["tier3"]

    print(f"\n[INFO] Recruiter verification tiers:")
    print(f"  Tier 1 (< {TIER1_DAYS} days, trust):        {len(tier1)} recruiter(s) — skipping")
    print(f"  Tier 2 ({TIER1_DAYS}-{TIER2_DAYS} days, search check): {len(tier2)} recruiter(s)")
    print(f"  Tier 3 (> {TIER2_DAYS} days, full visit):   {len(tier3)} recruiter(s)")

    if tier2:
        print(f"\n[INFO] Running Tier 2 verification ({len(tier2)} recruiter(s))...")
        for recruiter in tier2:
            print(f"  Checking: {recruiter['name']} @ {recruiter['company']}")
            found = verify_tier2_recruiter(page, recruiter)
            if not found:
                verify_tier3_recruiter(page, recruiter)
            human_delay(1.0, 2.0)

    if tier3:
        print(f"\n[INFO] Running Tier 3 verification ({len(tier3)} recruiter(s))...")
        for recruiter in tier3:
            print(f"  Verifying: {recruiter['name']} @ {recruiter['company']}")
            verify_tier3_recruiter(page, recruiter)
            human_delay(1.0, 2.0)

    print(f"\n[INFO] Linking verified recruiters to applications...")
    for app in applications:
        existing = get_recruiters_by_company(app["company"])
        linked = 0
        for recruiter in existing:
            link_recruiter_to_application(app["id"], recruiter["id"])
            linked += 1
        if linked:
            print(f"  [OK] {app['company']}: linked {linked} recruiter(s) to application id={app['id']}")