# careershift/quota_manager.py — CareerShift quota fetching and distribution

from datetime import datetime
from bs4 import BeautifulSoup

from logger import get_logger
from db.db import get_conn, get_remaining_quota, increment_quota_used
from careershift.utils import human_delay
from careershift.constants import CAREERSHIFT_QUOTA_URL
from config import MAX_CONTACTS_HARD_CAP

logger = get_logger(__name__)


def fetch_real_quota(page, user_id: int = 1):
    """
    Fetch actual remaining quota from CareerShift Account Usage page
    for the given user's session. Syncs local DB with real value.
    Returns remaining quota as integer.
    """
    try:
        page.goto(CAREERSHIFT_QUOTA_URL, wait_until="domcontentloaded", timeout=30000)
        human_delay(2.0, 3.0)

        html = page.content()
        soup = BeautifulSoup(html, "html.parser")

        tables = soup.find_all("table")
        for table in tables:
            headers = [th.get_text(strip=True).lower() for th in table.find_all("th")]
            if "remaining" in headers:
                rows = table.find_all("tr")
                for row in rows[1:]:
                    cols = [td.get_text(strip=True) for td in row.find_all("td")]
                    if cols:
                        remaining_idx = headers.index("remaining")
                        remaining = int(cols[remaining_idx])

                        conn = get_conn()
                        try:
                            c = conn.cursor()
                            today = datetime.now().strftime("%Y-%m-%d")
                            used = 50 - remaining
                            # Partial unique index: careershift_quota_user_date_key WHERE user_id IS NOT NULL
                            c.execute("""
                                INSERT INTO careershift_quota (user_id, date, total_limit, used, remaining)
                                VALUES (?, ?, 50, ?, ?)
                                ON CONFLICT(user_id, date) WHERE user_id IS NOT NULL DO UPDATE SET
                                    used = excluded.used,
                                    remaining = excluded.remaining
                            """, (user_id, today, used, remaining))
                            conn.commit()
                        finally:
                            conn.close()

                        logger.info("fetch_real_quota user_id=%d: remaining=%d/50", user_id, remaining)
                        print(f"[INFO] Real CareerShift quota user_id={user_id} — Remaining: {remaining}/50")
                        return remaining

        logger.warning("fetch_real_quota user_id=%d: could not parse quota page — using DB value", user_id)
        print("[WARNING] Could not parse quota from account usage page. Using local DB value.")
        return get_remaining_quota(user_id=user_id)

    except Exception as e:
        logger.warning("fetch_real_quota user_id=%d failed: %s — using DB value", user_id, e)
        print(f"[WARNING] Could not fetch real quota: {e}. Using local DB value.")
        return get_remaining_quota(user_id=user_id)


def calculate_distribution(remaining_quota, company_count):
    """
    Distribute quota fairly across companies.
    Returns list of per-company contact counts.
    """
    if company_count == 0 or remaining_quota == 0:
        return [0] * company_count

    base = remaining_quota // company_count
    extra = remaining_quota % company_count

    if base >= MAX_CONTACTS_HARD_CAP:
        return [MAX_CONTACTS_HARD_CAP] * company_count

    counts = []
    for i in range(company_count):
        if base == 0:
            counts.append(1 if i < extra else 0)
        else:
            if i < extra and base + 1 <= MAX_CONTACTS_HARD_CAP:
                counts.append(base + 1)
            else:
                counts.append(base)
    return counts
