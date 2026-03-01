import os
from playwright.sync_api import sync_playwright
from dotenv import load_dotenv

load_dotenv()

EMAIL = os.getenv("UNIVERSITY_EMAIL")
PASSWORD = os.getenv("UNIVERSITY_PASSWORD")
SESSION_FILE = "./data/careershift_session.json"

SYMPLICITY_URL = "https://northeastern-csm.symplicity.com/students/"


def login():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False, slow_mo=500)
        try:
            context = browser.new_context()
            page = context.new_page()

            # --- Step 1: Open Symplicity portal ---
            print("Opening Northeastern Symplicity portal...")
            page.goto(SYMPLICITY_URL)
            page.wait_for_load_state("networkidle")
            print(f"Current URL: {page.url}")

            # --- Step 2: Auto-fill credentials ---
            try:
                print("Attempting to fill credentials...")
                email_input = page.locator(
                    "input[type='email'], input[name*='email'], input[name*='user'], input[id*='email']"
                ).first
                email_input.wait_for(timeout=5000)
                email_input.fill(EMAIL)

                password_input = page.locator("input[type='password']").first
                password_input.wait_for(timeout=3000)
                password_input.fill(PASSWORD)

                page.keyboard.press("Enter")
                page.wait_for_load_state("networkidle")
                print("[OK] Credentials filled successfully")
            except:
                print("[WARNING] Could not auto-fill credentials. Please login manually in the browser.")

            # --- Step 3: MFA ---
            print("\n" + "="*50)
            print("1. Complete MFA if prompted.")
            print("2. Manually click the CareerShift link inside the portal.")
            print("3. Once CareerShift starts loading in the NEW TAB, come back and press Enter.")
            print("="*50)
            input()

            # --- Step 4: Find the CareerShift tab ---
            print("Looking for CareerShift tab...")
            careershift_page = None

            all_pages = context.pages
            print(f"Found {len(all_pages)} open tab(s).")

            for p in all_pages:
                print(f"   - {p.url}")
                if "careershift.com" in p.url:
                    careershift_page = p
                    break

            if not careershift_page:
                print("[WARNING] CareerShift tab not detected yet. Waiting up to 15 seconds...")
                try:
                    with context.expect_page(timeout=15000) as new_page_info:
                        pass
                    careershift_page = new_page_info.value
                except:
                    all_pages = context.pages
                    if len(all_pages) > 1:
                        careershift_page = all_pages[-1]
                        print(f"[WARNING] Using last opened tab: {careershift_page.url}")
                    else:
                        print("[ERROR] Could not find CareerShift tab. Please make sure you clicked the link.")
                        input("Press Enter to exit...")
                        return

            # --- Step 5: Wait for CareerShift to fully load ---
            print(f"[OK] Found CareerShift tab: {careershift_page.url}")
            print("Waiting for CareerShift to finish loading...")
            try:
                careershift_page.wait_for_url("https://www.careershift.com/**", timeout=20000)
                careershift_page.wait_for_load_state("networkidle", timeout=15000)
            except:
                print(f"[WARNING] Still on: {careershift_page.url}")
                print("If CareerShift looks loaded in the browser, press Enter to save anyway.")
                input()

            # --- Step 6: Save session from the CareerShift tab ---
            print(f"Current URL: {careershift_page.url}")

            if not careershift_page.url.startswith("https://www.careershift.com"):
                print("[WARNING] Doesn't look like CareerShift is fully loaded.")
                print("Press Enter to save anyway, or Ctrl+C to cancel.")
                input()

            os.makedirs(os.path.dirname(SESSION_FILE), exist_ok=True)
            context.storage_state(path=SESSION_FILE)
            print(f"[OK] Session saved to '{SESSION_FILE}'")
            print("[DONE] Future runs will reuse this session.")

            input("\nPress Enter to close the browser...")

        finally:
            browser.close()


if __name__ == "__main__":
    login()