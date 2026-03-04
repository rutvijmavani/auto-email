# careershift/search.py — CareerShift search, card parsing, email extraction

import re
from bs4 import BeautifulSoup

from careershift.utils import human_delay, slow_type
from careershift.constants import (
    CAREERSHIFT_SEARCH_URL,
    HR_KEYWORDS_STRONG,
    HR_KEYWORDS_LOOSE,
    EXCLUDE_KEYWORDS,
)


def classify_title(title):
    """Classify HR title as auto, manual_review, or None (not HR)."""
    t = title.lower().strip()
    for kw in HR_KEYWORDS_STRONG:
        if kw in t:
            return "auto"
    for kw in HR_KEYWORDS_LOOSE:
        if kw in t:
            return "manual_review"
    return None


def is_excluded_title(title):
    """Return True if title matches senior/executive exclusion list."""
    t = f" {title.lower().strip()} "
    for kw in EXCLUDE_KEYWORDS:
        if kw in t:
            return True
    return False


def parse_cards_from_html(html):
    """
    Parse result cards from search results HTML.
    Returns list of (name, company, position, detail_url, has_email).
    """
    soup = BeautifulSoup(html, "html.parser")
    cards = soup.find_all("li", attrs={"data-type": "contact"})
    results = []
    for card in cards:
        try:
            name_tag = card.find("h3", class_="title")
            name = name_tag.get_text(strip=True) if name_tag else ""
            h4s = card.find_all("h4")
            # h4[0] = company name, h4[1] = position (title)
            company = h4s[0].get_text(strip=True) if len(h4s) >= 1 else ""
            position = h4s[1].get_text(strip=True) if len(h4s) >= 2 else ""
            detail_link = card.find("a", href=re.compile(r"/App/Contacts/SearchDetails"))
            detail_url = ""
            if detail_link:
                href = detail_link.get("href", "")
                detail_url = "https://www.careershift.com" + href if href.startswith("/") else href
            has_email = card.find("span", class_="fa-envelope-o") is not None
            if name and detail_url:
                results.append((name, company, position, detail_url, has_email))
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
        match = re.search(r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}", text)
        if match and "linkedin" not in match.group(0).lower():
            return match.group(0)
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
        print(f"   [WARNING] Could not fill Company Name: {e}")
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
            print(f"   [WARNING] Advanced search failed: {e} — aborting.")
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
                print(f"   [WARNING] Job Title failed: {e} — aborting.")
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
                print(f"   [WARNING] RequireEmail failed: {e} — aborting.")
                return False

    try:
        search_btn = page.locator("button.search-button").first
        search_btn.wait_for(timeout=3000)
        human_delay(0.4, 0.9)
        search_btn.click()
        human_delay(2.5, 4.5)
    except Exception as e:
        print(f"   [WARNING] Could not click search: {e}")
        return False

    return True