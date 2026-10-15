"""
scripts/careershift_selftest.py — end-to-end CareerShift UI self-test (local, visible browser).

Walks the real search flow with a recruiter you ALREADY unlocked, so no quota is burned:
  login/session -> quota (before) -> search -> parse cards -> find the known recruiter
  -> open that (already unlocked) contact -> extract email -> compare -> quota (after)

Every stage saves a screenshot + page HTML to CAREERSHIFT_UI_CAPTURE_DIR (data/cs_ui) so
stale selectors can be fixed from the real UI. Never writes to the database.
Aborts before opening anything if the quota can't be read; flags any quota drop.

Usage:
  python scripts/careershift_selftest.py \
      --company "Genentech" --name "Celia Christine Yenchik" --email known@genentech.com
  (--email is optional; with it the extracted email is compared.)

Session file: data/careershift_session.json (local only).
Credentials (read from .env, never printed), first found wins:
  UNIVERSITY_EMAIL / UNIVERSITY_PASSWORD, NJIT_CAREERSHIFT_EMAIL / _PASSWORD
"""

import argparse
import os
import re
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from dotenv import load_dotenv
from playwright.sync_api import sync_playwright

from logger import get_logger, init_logging
from config import CAREERSHIFT_UI_CAPTURE_DIR, CAREERSHIFT_LOGIN_URL
from careershift.constants import (
    CAREERSHIFT_SEARCH_URL, CAREERSHIFT_QUOTA_URL, HR_SEARCH_TERMS, SESSION_FILE,
)
from careershift.search import submit_search, parse_cards_from_html, extract_email
from careershift.utils import human_delay

logger = get_logger(__name__)
load_dotenv()

_DASHBOARD = "https://www.careershift.com/App/Dashboard/Overview"


def _creds():
    pairs = [
        ("UNIVERSITY_EMAIL", "UNIVERSITY_PASSWORD"),
        ("NJIT_CAREERSHIFT_EMAIL", "NJIT_CAREERSHIFT_PASSWORD"),
    ]
    for e_key, p_key in pairs:
        if os.getenv(e_key) and os.getenv(p_key):
            return e_key, os.getenv(e_key), os.getenv(p_key)
    return None, None, None


def _capture(page, tag):
    os.makedirs(CAREERSHIFT_UI_CAPTURE_DIR, exist_ok=True)
    base = os.path.join(CAREERSHIFT_UI_CAPTURE_DIR, tag)
    try:
        page.screenshot(path=base + ".png", full_page=True)
        with open(base + ".html", "w", encoding="utf-8") as f:
            f.write(page.content())
        print(f"   [capture] {base}.png / .html  (url={page.url})")
    except Exception as e:
        print(f"   [capture-failed] {tag}: {e}")


def _norm_name(s):
    return re.sub(r"[^a-z]+", " ", (s or "").lower()).strip()


def _on_login_page(page):
    u = page.url.lower()
    return "login" in u or "signin" in u


def _is_logged_in(page):
    """Affirmative check: on the authenticated /App/ area of careershift.com with no password field shown."""
    if _on_login_page(page):
        return False
    u = page.url.lower()
    if "careershift.com/app/" not in u:
        return False
    try:
        return page.locator("input[type='password']").count() == 0
    except Exception:
        return False


def _read_quota(page):
    """Parse 'Remaining' from the account usage page. Returns int or None. No DB writes."""
    page.goto(CAREERSHIFT_QUOTA_URL, wait_until="domcontentloaded", timeout=30000)
    human_delay(2.0, 3.0)
    for table in page.locator("table").all():
        headers = [h.strip().lower() for h in table.locator("th").all_inner_texts()]
        if "remaining" in headers:
            idx = headers.index("remaining")
            for row in table.locator("tr").all()[1:]:
                cols = [c.strip() for c in row.locator("td").all_inner_texts()]
                if len(cols) > idx and cols[idx].isdigit():
                    return int(cols[idx])
    return None


def _try_form_login(page, email, password):
    """Best-effort fill of whatever email/password form is on the page. Returns fields found."""
    found = {"email": False, "password": False, "submit": False}
    try:
        e = page.locator("input[type='email'], input[id='Email'], input[name*='mail' i]").first
        e.wait_for(timeout=5000)
        e.fill(email)
        found["email"] = True
        p = page.locator("input[type='password']").first
        p.wait_for(timeout=5000)
        p.fill(password)
        found["password"] = True
        s = page.locator("button[type='submit'], input[type='submit'], button:has-text('Log in'), "
                         "button:has-text('Login'), button:has-text('Sign in')").first
        s.wait_for(timeout=3000)
        found["submit"] = True
        s.click()
        page.wait_for_load_state("networkidle")
    except Exception as e:
        print(f"   [login] form step stopped: {type(e).__name__}")
    return found


def _ensure_session(context, page, session_file):
    page.goto(_DASHBOARD, wait_until="domcontentloaded", timeout=30000)
    human_delay(1.5, 2.5)
    if _is_logged_in(page):
        print("[OK] Saved session is valid.")
        return True
    print("[INFO] Saved session missing/expired — logging in.")
    _capture(page, "01_login_page")
    env_key, email, password = _creds()
    page.goto(CAREERSHIFT_LOGIN_URL, wait_until="domcontentloaded", timeout=30000)
    human_delay(1.5, 2.5)
    if email:
        print(f"[INFO] Using credentials from {env_key} (value not shown).")
        found = _try_form_login(page, email, password)
        print(f"   [login] fields found: {found}")
        _capture(page, "02_after_form_login")
    else:
        print("[WARN] No credentials found in .env for this user.")
    page.goto(_DASHBOARD, wait_until="domcontentloaded", timeout=30000)
    if not _is_logged_in(page):
        print("\n[ACTION] Complete the login (SSO/MFA) in the browser window, wait for the")
        print("         CareerShift dashboard, then press Enter here.")
        input()
        page.goto(_DASHBOARD, wait_until="domcontentloaded", timeout=30000)
    if not _is_logged_in(page):
        _capture(page, "03_login_failed")
        return False
    os.makedirs(os.path.dirname(session_file), exist_ok=True)
    context.storage_state(path=session_file)
    print(f"[OK] Session saved to {os.path.normpath(session_file)}")
    return True


_GUIDED_STEPS = [
    ("10_contacts_menu", "Click 'Contacts' in the left sidebar so its sub-menu is open"),
    ("11_contact_search_form", "Open the contact SEARCH page (blank form, nothing typed)"),
    ("12_search_filled", "Type the company '{company}' (and a job title like Recruiter), but do NOT press search yet"),
    ("13_search_results", "Run the search and wait for the results list to appear"),
    ("14_results_scrolled", "Scroll the results so a few contact cards are visible (don't open any yet)"),
    ("14b_before_open", "Open the account dropdown / any credits area and note any counter, then close it. Capture = BEFORE state"),
    ("14c_contact_opened", "Open ONE contact (e.g. '{name}') and reveal the email. This may burn 1 credit (user-approved)"),
    ("14d_after_open", "Open the same account dropdown / credits area again. Capture = AFTER state; compare any counter with before"),
    ("15_profile_menu", "Click your name (top right) so the account dropdown is open — look for any credits/usage"),
    ("16_credits_or_usage", "If you can find ANY page showing credits / usage / limits, open it (otherwise skip)"),
]


def _guided_capture(page, args):
    """Human drives, script records: after each Enter, save screenshot + HTML of the current page."""
    print("\nFor each step: do it in the browser window, then press Enter here to capture.")
    print("Type 's' + Enter to skip a step. Open exactly ONE contact (step 14c) — it may burn one credit.\n")
    for tag, text in _GUIDED_STEPS:
        ans = input(f"STEP {tag}: {text.format(company=args.company, name=args.name)}\n   Enter=capture, s=skip > ")
        if ans.strip().lower() == "s":
            continue
        _capture(page, tag)
        if tag == "14c_contact_opened":
            try:
                print(f"   [extract_email on current page] -> {extract_email(page)}")
            except Exception as e:
                print(f"   [extract_email failed] {e}")
    print("\n[DONE] Send me everything in", CAREERSHIFT_UI_CAPTURE_DIR)


def run(args):
    session_file = SESSION_FILE
    results = []

    def stage(name, ok, detail=""):
        results.append((name, ok, detail))
        print(f"[{'PASS' if ok else 'FAIL'}] {name} {detail}")
        return ok

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False, slow_mo=250)
        try:
            kwargs = {"storage_state": session_file} if os.path.exists(session_file) else {}
            context = browser.new_context(**kwargs)
            page = context.new_page()

            if not stage("session/login", _ensure_session(context, page, session_file)):
                return results

            before = _read_quota(page)
            _capture(page, "04_quota_page")
            if not stage("quota readable (before)", before is not None, f"remaining={before}"):
                print("[INFO] Old quota page is gone (the UI moved to app.careershift.com).")
                print("[INFO] Switching to guided capture — no automated clicks, no quota risk.")
                _guided_capture(page, args)
                context.storage_state(path=session_file)
                return results

            # Search: first HR term that returns the target; stop at the first hit.
            target_name = _norm_name(args.name)
            target_card = None
            for term in ([args.term] if args.term else HR_SEARCH_TERMS):
                ok = submit_search(page, args.company, hr_term=term, require_email=True)
                term_tag = re.sub(r"\W+", "_", term)
                _capture(page, f"05_search_{term_tag}")
                if not stage(f"search '{term}' submitted", ok):
                    continue
                cards = parse_cards_from_html(page.content())
                stage(f"cards parsed for '{term}'", len(cards) > 0, f"count={len(cards)}")
                target_company = _norm_name(args.company)
                for card in cards:
                    card_company = _norm_name(card[1])
                    same_company = bool(card_company) and (
                        target_company in card_company or card_company in target_company)
                    # name alone is ambiguous across employers; the later --email comparison confirms the profile
                    if _norm_name(card[0]) == target_name and same_company:
                        target_card = card
                        break
                if target_card:
                    break

            if not stage("known recruiter found in results", target_card is not None,
                         f"name='{args.name}'"):
                print("   Hint: check 05_search_*.html — card markup (li[data-type=contact], h3.title,"
                      " h4, a[href*=SearchDetails]) may have changed.")
                return results

            name, company, position, detail_url, has_email = target_card
            print(f"   card: {name} | {company} | {position} | has_email_icon={has_email}")

            # Open the already-unlocked contact (free). Only this one profile, then stop.
            page.goto(detail_url, wait_until="domcontentloaded", timeout=30000)
            human_delay(2.0, 3.5)
            _capture(page, "06_contact_detail")
            email = extract_email(page)
            stage("email extracted", bool(email), f"email={email}")
            if args.email:
                stage("email matches DB", bool(email) and email.lower() == args.email.lower(),
                      f"expected={args.email}")

            after = _read_quota(page)
            stage("quota unchanged (no burn)", after == before, f"before={before} after={after}")
            context.storage_state(path=session_file)
        finally:
            if not args.no_pause:
                input("\nPress Enter to close the browser...")
            browser.close()
    return results


def main():
    init_logging("careershift_selftest")
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--company", required=True, help="Company as stored in recruiters.company")
    ap.add_argument("--name", required=True, help="Recruiter name of an ALREADY UNLOCKED contact")
    ap.add_argument("--email", help="Known email from the DB, to compare against")
    ap.add_argument("--term", help="Single job-title term to search (default: all HR_SEARCH_TERMS)")
    ap.add_argument("--no-pause", action="store_true", help="Close the browser without waiting")
    args = ap.parse_args()

    results = run(args)
    print("\n=== SUMMARY ===")
    for name, ok, detail in results:
        print(f"{'PASS' if ok else 'FAIL'}  {name}  {detail}")
    logger.info("selftest done: %d/%d passed", sum(1 for r in results if r[1]), len(results))
    sys.exit(0 if results and all(r[1] for r in results) else 1)


if __name__ == "__main__":
    main()
