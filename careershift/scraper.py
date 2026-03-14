# careershift/scraper.py — Profile visiting and company scraping with validation

import re
from collections import Counter

from logger import get_logger
from db.db import (
    increment_quota_used,
    get_remaining_quota,
    get_used_search_terms,
    mark_search_term_used,
    update_company_last_scraped,
    get_existing_domain_for_company,
)
from careershift.utils import human_delay
from careershift.search import (
    submit_search,
    parse_cards_from_html,
    extract_email,
    classify_title,
    is_excluded_title,
)
from careershift.constants import CAREERSHIFT_SEARCH_URL, HR_SEARCH_TERMS
from config import (
    MAX_CONTACTS_HARD_CAP,
    CAREERSHIFT_SAMPLE_SIZE,
    CAREERSHIFT_HIGH_CONFIDENCE,
    CAREERSHIFT_MEDIUM_CONFIDENCE,
    CAREERSHIFT_MAX_PROFILES,
)

logger = get_logger(__name__)

LEGAL_SUFFIXES = [
    "inc", "llc", "ltd", "corp", "co", "lp",
    "plc", "gmbh", "pte", "incorporated",
    "corporation", "limited",
]


# ─────────────────────────────────────────
# VALIDATION HELPERS
# ─────────────────────────────────────────

def normalize(company_name):
    """Lowercase, remove punctuation, strip whitespace. Does NOT remove suffixes."""
    name = company_name.lower()
    name = re.sub(r'[.,\-]', '', name)
    name = re.sub(r'\s+', ' ', name)
    return name.strip()


def is_suffix_variation(normalized_card, normalized_expected):
    """
    Returns True if card = expected + legal suffix only.
    e.g. "collective inc" = "collective" + "inc" → True
         "collective junction" → False
    """
    if not normalized_card.startswith(normalized_expected):
        return False
    remainder = normalized_card[len(normalized_expected):].strip()
    return remainder in LEGAL_SUFFIXES


def domain_matches_expected(email, reference_domain):
    """
    Check if email domain root matches reference domain.
    e.g. john@collective.com → root "collective" == "collective" → True
         jane@collective-la.com → root "collective-la" != "collective" → False
    """
    if not email or "@" not in email:
        return False
    domain = email.split("@")[1]       # "collective.com"
    root = domain.split(".")[0]        # "collective"
    return root == reference_domain


def analyze_buffer(buffer, expected_domain, existing_db_domain=None):
    """
    Analyze visited profiles for domain consistency before DB insertion.

    reference = existing DB domain (trusted) OR expected_domain (from job URL)
    Domain check ONLY triggered when domains conflict — not for consistent buffers.

    If no reference domain available → trust the buffer as-is (consistent signal
    is sufficient without a reference to compare against).

    Returns list of verified records to insert, empty list to exhaust/skip.
    """
    if not buffer:
        return []

    # DB domain takes priority — already verified, possibly used for outreach
    reference = existing_db_domain if existing_db_domain else expected_domain

    # No reference domain available — can't do domain validation
    # Trust buffer as-is: consistent or not, we have no baseline to compare against
    if not reference:
        logger.debug("analyze_buffer: no reference domain — trusting buffer (%d records)",
                     len(buffer))
        print(f"   [INFO] No reference domain available — trusting buffer as-is")
        return buffer

    domains = [entry["email"].split("@")[1] for entry in buffer]
    unique_domains = set(domains)

    # All same domain — consistent signal
    if len(unique_domains) == 1:
        buffer_root = domains[0].split(".")[0]
        if buffer_root == reference:
            # Consistent + matches reference → insert all
            logger.debug("analyze_buffer: consistent domain %r matches reference — accepting all",
                         buffer_root)
            return buffer
        else:
            # Consistent but conflicts with reference
            # → Trust DB/expected, discard buffer silently
            logger.warning("analyze_buffer: buffer domain %r conflicts with reference %r — discarding",
                           buffer_root, reference)
            print(f"   [INFO] Buffer domain '{buffer_root}' conflicts with "
                  f"reference '{reference}' — discarding buffer")
            return []

    # Mixed domains — use reference as tiebreaker
    matched = [r for r in buffer if domain_matches_expected(r["email"], reference)]

    if matched:
        discarded = len(buffer) - len(matched)
        if discarded:
            logger.debug("analyze_buffer: domain tiebreaker kept %d, discarded %d",
                         len(matched), discarded)
            print(f"   [INFO] Domain tiebreaker: kept {len(matched)}, "
                  f"discarded {discarded} non-matching record(s)")
        return matched

    # Nothing matches reference
    logger.warning("analyze_buffer: no records match reference domain %r — discarding all",
                   reference)
    print(f"   [INFO] No buffer records match reference domain '{reference}' "
          f"— discarding all")
    return []


# ─────────────────────────────────────────
# PROFILE VISITOR
# ─────────────────────────────────────────

def visit_and_extract(page, detail_url, name, position, confidence):
    """Visit profile, extract email, update quota. Returns contact dict or None."""
    try:
        human_delay(4.0, 8.0)
        page.goto(detail_url, wait_until="domcontentloaded", timeout=20000)
        human_delay(3.0, 6.0)
        increment_quota_used(1)

        email = extract_email(page)
        if email:
            logger.debug("Profile visit: extracted email=%s name=%r", email, name)
            print(f"         [INFO] {email}")
            page.goto(f"{CAREERSHIFT_SEARCH_URL}#contacts_search_results",
                      wait_until="domcontentloaded", timeout=20000)
            human_delay(3.0, 5.0)
            return {
                "name":       name,
                "position":   position,
                "email":      email,
                "confidence": confidence,
            }
        else:
            logger.debug("Profile visit: no email found for %r — skipping", name)
            print(f"         [SKIP] No email — skipping {name}")
            page.goto(f"{CAREERSHIFT_SEARCH_URL}#contacts_search_results",
                      wait_until="domcontentloaded", timeout=20000)
            human_delay(2.0, 3.0)
            return None
    except Exception as e:
        logger.warning("Profile visit failed for %r: %s", name, e)
        print(f"         [WARNING] Profile visit failed: {e}")
        return None


# ─────────────────────────────────────────
# MAIN SCRAPING FUNCTION
# ─────────────────────────────────────────

def scrape_company(page, company, max_contacts, expected_domain):
    """
    Find recruiters for a company using set-level validation.

    max_contacts:    from calculate_distribution — controls quota usage
    expected_domain: from application.expected_domain — domain tiebreaker
    """
    logger.info("scrape_company: company=%r max_contacts=%d expected_domain=%r",
                company, max_contacts, expected_domain)

    normalized_expected = normalize(company)

    # Tracks exact matches and suffix variations across all HR terms
    hashmap = {normalized_expected: 0}

    # Accumulates exact match profile cards across all HR terms
    # Deduplicated by detail_url to prevent same profile added multiple times
    all_exact_profiles = []
    seen_urls = set()

    # Total cards seen across all HR terms (for hashmap_confidence calculation)
    total_cards_seen = 0

    skip_remaining = False

    # ── Step 1: Card-level analysis across HR terms ──
    for hr_term in HR_SEARCH_TERMS:
        if skip_remaining:
            break

        print(f"   [INFO] Searching '{hr_term}' for {company}...")

        if not submit_search(page, company, hr_term=hr_term, require_email=True):
            continue

        try:
            page.wait_for_selector("li[data-type='contact']", timeout=6000)
        except Exception:
            logger.debug("No results for hr_term=%r company=%r", hr_term, company)
            print(f"   [INFO] No results for '{hr_term}'")
            continue

        html = page.content()
        cards = parse_cards_from_html(html)

        actual_count = len(cards)
        sample_size  = min(actual_count, CAREERSHIFT_SAMPLE_SIZE)

        if sample_size == 0:
            continue  # no results → try next term

        total_cards_seen += sample_size
        cnt = 0  # exact matches in this batch

        for card in cards[:sample_size]:  # only process sampled subset
            name, card_company, position, detail_url, has_email = card
            normalized_card = normalize(card_company)

            if normalized_card == normalized_expected:
                cnt += 1
                hashmap[normalized_expected] += 1
                if has_email and classify_title(position) and \
                   not is_excluded_title(position) and \
                   detail_url not in seen_urls:
                    all_exact_profiles.append({
                        "name":       name,
                        "position":   position,
                        "detail_url": detail_url,
                    })
                    seen_urls.add(detail_url)

            elif is_suffix_variation(normalized_card, normalized_expected):
                if normalized_card not in hashmap:
                    hashmap[normalized_card] = 0
                hashmap[normalized_card] += 1

            else:
                pass  # different company -> ignore

        confidence = (cnt / sample_size) * 100
        logger.debug("hr_term=%r company=%r confidence=%.0f%% (%d/%d exact)",
                     hr_term, company, confidence, cnt, sample_size)
        print(f"   [INFO] Confidence: {confidence:.0f}% "
              f"({cnt}/{sample_size} exact matches)")

        mark_search_term_used(company, hr_term)

        if confidence >= CAREERSHIFT_HIGH_CONFIDENCE:
            skip_remaining = True   # strong signal → skip remaining HR terms
            logger.info("High confidence (%.0f%%) for %r — skipping remaining HR terms",
                        confidence, company)
            print(f"   [OK] High confidence ({confidence:.0f}%) — "
                  f"skipping remaining HR terms")

        human_delay(2.0, 4.0)

    # ── After all HR terms ──
    if hashmap[normalized_expected] == 0:
        logger.info("No exact matches found for %r — exhausting", company)
        print(f"   [INFO] No exact matches found for {company} — exhausting")
        update_company_last_scraped(company)
        return []

    logger.debug("Total exact profiles found for %r: %d", company, len(all_exact_profiles))
    print(f"   [INFO] Total exact profiles found: {len(all_exact_profiles)}")

    # ── Step 2: Visit profiles ──
    # Respect quota allocation — never visit more than max_contacts
    visit_limit      = min(max_contacts, CAREERSHIFT_MAX_PROFILES)
    profiles_to_visit = all_exact_profiles[:visit_limit]

    logger.info("Visiting %d profile(s) for %r (limit=%d)",
                len(profiles_to_visit), company, visit_limit)
    print(f"   [INFO] Visiting {len(profiles_to_visit)} profile(s) "
          f"(limit={visit_limit})")

    buffer = []

    for profile in profiles_to_visit:
        # Stop if quota exhausted during visits
        if get_remaining_quota() == 0:
            logger.warning("Quota exhausted during profile visits for %r", company)
            print(f"   [INFO] Quota exhausted — stopping profile visits early")
            break

        confidence = classify_title(profile["position"]) or "auto"
        detail = visit_and_extract(
            page,
            profile["detail_url"],
            profile["name"],
            profile["position"],
            confidence,
        )

        if not detail:
            continue

        buffer.append({
            "name":       detail["name"],
            "position":   detail["position"],
            "email":      detail["email"],
            "company":    company,
            "confidence": detail["confidence"],
        })

    logger.debug("Buffer after visits for %r: %d record(s)", company, len(buffer))

    # ── Step 3: Get existing DB domain (top-up scenario) ──
    existing_db_domain = get_existing_domain_for_company(company)
    if existing_db_domain:
        logger.debug("Existing DB domain for %r: %r", company, existing_db_domain)
        print(f"   [INFO] Existing DB domain for {company}: '{existing_db_domain}'")
    else:
        logger.debug("No existing DB domain for %r — using expected_domain: %r",
                     company, expected_domain)
        print(f"   [INFO] No existing DB domain for {company} — using expected_domain: '{expected_domain}'")

    # ── Step 4: Analyze buffer ──

    # Special case: single profile visit (quota forced visit_limit = 1)
    if visit_limit == 1:
        if not buffer:
            logger.info("Single visit: no buffer for %r — exhausting", company)
            update_company_last_scraped(company)
            return []

        single = buffer[0]
        # Use total cards seen across all terms for accurate confidence
        effective_total    = max(total_cards_seen, 1)
        hashmap_confidence = (hashmap[normalized_expected] / effective_total) * 100

        reference = existing_db_domain if existing_db_domain else expected_domain

        logger.debug("Single visit: company=%r hashmap_confidence=%.0f%% reference=%r",
                     company, hashmap_confidence, reference)
        print(f"   [INFO] Single visit — hashmap confidence: "
              f"{hashmap_confidence:.0f}%")

        if hashmap_confidence >= CAREERSHIFT_MEDIUM_CONFIDENCE:
            if reference and domain_matches_expected(single["email"], reference):
                logger.info("Single visit validated for %r — domain matches reference", company)
                print(f"   [OK] Single record validated — domain matches reference")
                update_company_last_scraped(company)
                return [single]
            elif not reference:
                # No reference domain available — trust hashmap confidence alone
                logger.info("Single visit: no reference domain for %r — "
                            "trusting hashmap confidence", company)
                print(f"   [OK] Single record — no reference domain, "
                      f"trusting hashmap confidence")
                update_company_last_scraped(company)
                return [single]
            else:
                logger.warning("Single visit: domain mismatch for %r — exhausting", company)
                print(f"   [INFO] Single record domain mismatch — exhausting")
                update_company_last_scraped(company)
                return []
        else:
            # Weak signal — skip, retry tomorrow
            logger.info("Single visit: weak signal (%.0f%%) for %r — skipping",
                        hashmap_confidence, company)
            print(f"   [INFO] Weak hashmap signal ({hashmap_confidence:.0f}%) "
                  f"— skipping, retry tomorrow")
            update_company_last_scraped(company)
            return None  # None = skip (not exhaust)

    # Normal case: multiple profiles
    verified = analyze_buffer(buffer, expected_domain, existing_db_domain)

    logger.info("scrape_company done: %r → %d verified record(s)", company, len(verified) if verified else 0)
    update_company_last_scraped(company)
    return verified  # [] = exhaust, [records] = insert