"""
careershift/auth_njit.py — CareerShift authentication for NJIT members.

NJIT accesses CareerShift via:
  https://www.careershift.com/?sc=njit

Two login paths:
  Primary:  Direct email/password form (automated) — works when token is valid
  Fallback: NJIT Webauth SSO button (manual)       — use when token is expired

Usage:
  python careershift/auth_njit.py

Required .env variables:
  NJIT_CAREERSHIFT_EMAIL=your_careershift_email
  NJIT_CAREERSHIFT_PASSWORD=your_careershift_password

Session saved to: data/careershift_session.json
Valid for ~30 days — re-run when session expires.
"""

import os
from playwright.sync_api import sync_playwright
from dotenv import load_dotenv

load_dotenv()

EMAIL        = os.getenv("NJIT_CAREERSHIFT_EMAIL")
PASSWORD     = os.getenv("NJIT_CAREERSHIFT_PASSWORD")
SESSION_FILE = "./data/careershift_session.json"

CAREERSHIFT_NJIT_URL  = "https://www.careershift.com/?sc=njit"
CAREERSHIFT_DASHBOARD = "https://www.careershift.com/App/Dashboard/Overview"


def _is_on_login_page(page):
    url = page.url.lower()
    return "login" in url or "signin" in url or "account/login" in url


def _is_on_dashboard(page):
    return "careershift.com/app" in page.url.lower()


def login():
    if not EMAIL or not PASSWORD:
        print("[ERROR] NJIT_CAREERSHIFT_EMAIL or NJIT_CAREERSHIFT_PASSWORD not set in .env")
        print("        Add the following to your .env:")
        print("          NJIT_CAREERSHIFT_EMAIL=your_careershift_email")
        print("          NJIT_CAREERSHIFT_PASSWORD=your_careershift_password")
        return

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False, slow_mo=500)
        try:
            context = browser.new_context()
            page = context.new_page()

            # ── Step 1: Open CareerShift NJIT login page ──
            print("Opening CareerShift NJIT login page...")
            page.goto(CAREERSHIFT_NJIT_URL)
            page.wait_for_load_state("networkidle")
            print(f"Current URL: {page.url}")

            # ── Step 2: Attempt direct email/password login ──
            login_succeeded = False
            try:
                print("\n[INFO] Attempting direct email/password login...")
                email_input = page.locator("input[type='email'], input[id='Email']").first
                email_input.wait_for(timeout=5000)
                email_input.fill(EMAIL)

                password_input = page.locator("input[type='password'], input[id='Password']").first
                password_input.wait_for(timeout=3000)
                password_input.fill(PASSWORD)

                submit = page.locator("button:has-text('Login'), input[type='submit']").first
                submit.wait_for(timeout=3000)
                submit.click()
                page.wait_for_load_state("networkidle")
                print(f"Current URL after login: {page.url}")

                if _is_on_dashboard(page) or not _is_on_login_page(page):
                    login_succeeded = True
                    print("[OK] Direct login succeeded.")

            except Exception as e:
                print(f"[WARNING] Direct login failed: {e}")

            # ── Step 3: Fallback — manual SSO via Webauth ──
            if not login_succeeded:
                print("\n" + "=" * 55)
                print("[INFO] Direct login did not succeed.")
                print("[INFO] Your CareerShift token may be expired.")
                print()
                print("Please complete these steps in the browser:")
                print("  1. Click 'Log in with the Webauth Authentication")
                print("     Service (UCID)' button")
                print("  2. Enter your NJIT UCID credentials")
                print("  3. Complete MFA if prompted")
                print("  4. Wait until CareerShift dashboard loads")
                print()
                print("Once you see the CareerShift dashboard, press Enter.")
                print("=" * 55)
                input()

                # Re-check after manual SSO
                if _is_on_login_page(page):
                    print("[WARNING] Still on login page after manual SSO.")
                    print("          Navigate to the CareerShift dashboard manually.")
                    print("          Press Enter once you're on the dashboard.")
                    input()

            # ── Step 4: Navigate to dashboard to confirm session ──
            try:
                print("\n[INFO] Verifying session on dashboard...")
                page.goto(CAREERSHIFT_DASHBOARD,
                          wait_until="domcontentloaded", timeout=20000)
                page.wait_for_load_state("networkidle")
                print(f"Current URL: {page.url}")
            except Exception as e:
                print(f"[WARNING] Could not load dashboard: {e}")
                print("Press Enter to save session anyway, or Ctrl+C to cancel.")
                input()

            if _is_on_login_page(page):
                print("[ERROR] Session invalid — still redirecting to login.")
                print("        Check your credentials or try the SSO path.")
                return

            # ── Step 5: Save session ──
            os.makedirs(os.path.dirname(SESSION_FILE), exist_ok=True)
            context.storage_state(path=SESSION_FILE)
            print(f"\n[OK] Session saved to '{SESSION_FILE}'")
            print("[DONE] Future runs will reuse this session (~30 days).")

            input("\nPress Enter to close the browser...")

        finally:
            browser.close()


if __name__ == "__main__":
    login()