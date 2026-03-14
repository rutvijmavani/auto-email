# careershift/verification.py — Tiered recruiter verification

from bs4 import BeautifulSoup

from logger import get_logger
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

logger = get_logger(__name__)


def verify_tier2_recruiter(page, recruiter):
    """
    Tier 2: Lightweight verification — search by company and look for name in cards.
    No profile visit needed. Returns True if found, False to escalate to Tier 3.
    """
    company = recruiter["company"]
    name    = recruiter["name"].strip().lower()

    logger.debug("Tier 2 verify: %r @ %r", recruiter["name"], company)

    try:
        ok = submit_search(page, company, hr_term=None, require_email=False)
        if not ok:
            logger.warning("Tier 2: search failed for %r @ %r — assuming valid",
                           recruiter["name"], company)
            return True  # search failed — assume still valid

        human_delay(2.0, 3.0)
        html  = page.content()
        cards = parse_cards_from_html(html)

        for card_name, _, _, _, _ in cards:
            if name in card_name.strip().lower():
                update_recruiter(recruiter["id"])
                logger.info("Tier 2 verified: %r still at %r", recruiter["name"], company)
                print(f"     [OK] Tier 2 verified: {recruiter['name']} still at {company}")
                return True

        logger.info("Tier 2: %r not found at %r — escalating to Tier 3",
                    recruiter["name"], company)
        print(f"     [WARNING] Tier 2: {recruiter['name']} not found — escalating to Tier 3")
        return False

    except Exception as e:
        logger.warning("Tier 2 check failed for %r: %s — assuming valid",
                       recruiter["name"], e)
        print(f"     [WARNING] Tier 2 check failed for {recruiter['name']}: {e} — assuming valid")
        return True


def verify_tier3_recruiter(page, recruiter):
    """
    Tier 3: Full profile visit — free since profile is cached.
    Checks company, title, email still current. Updates DB or marks inactive.
    Returns True if recruiter was marked inactive, False otherwise.
    """
    name    = recruiter["name"]
    company = recruiter["company"]

    logger.debug("Tier 3 verify: %r @ %r", name, company)

    try:
        ok = submit_search(page, company, hr_term=None, require_email=False)
        if not ok:
            logger.warning("Tier 3: search failed for %r — marking as verified", name)
            update_recruiter(recruiter["id"])
            return False

        human_delay(2.0, 3.0)
        html       = page.content()
        cards      = parse_cards_from_html(html)
        detail_url = None

        for card_name, _, position, url, _ in cards:
            if name.strip().lower() in card_name.strip().lower():
                detail_url = url
                break

        if not detail_url:
            logger.info("Tier 3: %r not found at %r — marking inactive", name, company)
            print(f"     [INFO] Tier 3: {name} not found at {company} — marking inactive")
            mark_recruiter_inactive(recruiter["id"], reason="not found in company search")
            return True

        human_delay(3.0, 6.0)
        page.goto(detail_url, wait_until="domcontentloaded", timeout=20000)
        human_delay(2.0, 4.0)

        html  = page.content()
        soup  = BeautifulSoup(html, "html.parser")
        page_text = soup.get_text(separator=" ").lower()

        if company.lower() not in page_text:
            logger.info("Tier 3: %r no longer at %r — marking inactive", name, company)
            print(f"     [INFO] Tier 3: {name} no longer at {company} — marking inactive")
            mark_recruiter_inactive(recruiter["id"], reason="company mismatch on profile")
            return True

        new_email     = extract_email(page)
        current_email = recruiter["email"]

        h4s          = soup.find_all("h4")
        new_position = h4s[1].get_text(strip=True) if len(h4s) >= 2 else None

        updates = {}
        if new_email and new_email != current_email:
            logger.info("Tier 3: email updated for %r: %s → %s",
                        name, current_email, new_email)
            print(f"     [INFO] Email updated: {current_email} -> {new_email}")
            updates["email"] = new_email
        if new_position and new_position != recruiter["position"]:
            logger.info("Tier 3: position updated for %r: %r → %r",
                        name, recruiter["position"], new_position)
            print(f"     [INFO] Title updated: {recruiter['position']} -> {new_position}")
            updates["position"] = new_position

        update_recruiter(recruiter["id"], **updates)
        logger.info("Tier 3 verified: %r still active at %r", name, company)
        print(f"     [OK] Tier 3 verified: {name} still active at {company}")

        page.goto(f"{CAREERSHIFT_SEARCH_URL}#contacts_search_results",
                  wait_until="domcontentloaded", timeout=20000)
        human_delay(2.0, 3.0)
        return False

    except Exception as e:
        logger.warning("Tier 3 check failed for %r: %s", name, e)
        print(f"     [WARNING] Tier 3 check failed for {name}: {e}")
        update_recruiter(recruiter["id"])
        return False


def run_tiered_verification(page, applications):
    """
    Run tiered verification for all existing recruiters.
    After verification, link active recruiters to their applications.
    Returns stats dict with tier counts and changes.
    """
    tiers = get_recruiters_by_tier(TIER1_DAYS, TIER2_DAYS)

    tier1 = tiers["tier1"]
    tier2 = tiers["tier2"]
    tier3 = tiers["tier3"]

    logger.info("Tiered verification: tier1=%d tier2=%d tier3=%d",
                len(tier1), len(tier2), len(tier3))

    print(f"\n[INFO] Recruiter verification tiers:")
    print(f"  Tier 1 (< {TIER1_DAYS} days, trust):        {len(tier1)} recruiter(s) — skipping")
    print(f"  Tier 2 ({TIER1_DAYS}-{TIER2_DAYS} days, search check): {len(tier2)} recruiter(s)")
    print(f"  Tier 3 (> {TIER2_DAYS} days, full visit):   {len(tier3)} recruiter(s)")

    tier2_verified = 0
    tier3_verified = 0
    tier3_inactive = 0
    changes        = []

    if tier2:
        print(f"\n[INFO] Running Tier 2 verification ({len(tier2)} recruiter(s))...")
        for recruiter in tier2:
            print(f"  Checking: {recruiter['name']} @ {recruiter['company']}")
            found = verify_tier2_recruiter(page, recruiter)
            if found:
                tier2_verified += 1
            else:
                # Escalate to Tier 3
                marked_inactive = verify_tier3_recruiter(page, recruiter)
                if marked_inactive:
                    tier3_inactive += 1
                    logger.info("Recruiter marked inactive (escalated from T2): %r @ %r",
                                recruiter["name"], recruiter["company"])
                    changes.append({
                        "name":    recruiter["name"],
                        "company": recruiter["company"],
                        "action":  "marked inactive",
                    })
                else:
                    tier3_verified += 1
            human_delay(1.0, 2.0)

    if tier3:
        print(f"\n[INFO] Running Tier 3 verification ({len(tier3)} recruiter(s))...")
        for recruiter in tier3:
            print(f"  Verifying: {recruiter['name']} @ {recruiter['company']}")
            marked_inactive = verify_tier3_recruiter(page, recruiter)
            if marked_inactive:
                tier3_inactive += 1
                logger.info("Recruiter marked inactive (T3): %r @ %r",
                            recruiter["name"], recruiter["company"])
                changes.append({
                    "name":    recruiter["name"],
                    "company": recruiter["company"],
                    "action":  "marked inactive",
                })
            else:
                tier3_verified += 1
            human_delay(1.0, 2.0)

    print(f"\n[INFO] Linking verified recruiters to applications...")
    for app in applications:
        existing = get_recruiters_by_company(app["company"])
        linked   = 0
        for recruiter in existing:
            link_recruiter_to_application(app["id"], recruiter["id"])
            linked += 1
        if linked:
            logger.debug("Linked %d recruiter(s) to application id=%s (%s)",
                         linked, app["id"], app["company"])
            print(f"  [OK] {app['company']}: linked {linked} recruiter(s) to application id={app['id']}")

    logger.info("Verification complete: t2_verified=%d t3_verified=%d t3_inactive=%d changes=%d",
                tier2_verified, tier3_verified, tier3_inactive, len(changes))

    return {
        "tier1_count":    len(tier1),
        "tier2_count":    len(tier2),
        "tier2_verified": tier2_verified,
        "tier3_count":    len(tier3),
        "tier3_verified": tier3_verified,
        "tier3_inactive": tier3_inactive,
        "changes":        changes,
    }