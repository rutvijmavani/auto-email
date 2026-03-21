"""
jobs/fill_verifier.py — Verify and mark filled job postings.

For each job missing from API scan for VERIFY_FILLED_MISSING_DAYS+:
  → Make direct HTTP request to job URL
  → 404/410/gone → mark status='filled', clear description
  → 200/redirect → job still active, reset missing days counter

Runs as --verify-filled pipeline command.
Processes VERIFY_FILLED_BATCH_SIZE jobs per run to avoid flooding.
"""

import time
import requests
from datetime import datetime

from logger import get_logger, init_logging
from db.db import (
    init_db,
    get_stale_jobs,
    increment_missing_days,
    reset_missing_days,
    mark_job_filled,
    save_verify_filled_stats,
)
from config import (
    VERIFY_FILLED_BATCH_SIZE,
    VERIFY_FILLED_MISSING_DAYS,
)

logger = get_logger(__name__)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/122.0.0.0 Safari/537.36"
}

# Status codes that confirm job is gone
GONE_CODES = {404, 410, 301, 302}

# Redirect destinations that indicate job is gone
GONE_PATTERNS = [
    "/jobs",           # redirected to job listing page
    "/careers",        # redirected to careers page
    "?error",          # error page
    "not-found",       # not found page
    "expired",         # expired job page
]


def _is_job_gone(url):
    """
    Make HTTP request to job URL.
    Returns True if job is confirmed gone (404/410 or redirect to listing page).
    Returns False if job is still active (200 with valid content).
    Returns None if inconclusive (timeout, connection error).
    """
    try:
        r = requests.get(
            url,
            headers=HEADERS,
            timeout=10,
            allow_redirects=True,
        )
        # Hard 404/410 → definitely gone
        if r.status_code in {404, 410}:
            return True

        # Redirected to a generic page → likely gone
        if r.url != url:
            from urllib.parse import urlparse

            original = urlparse(url)
            final = urlparse(r.url)
            original_path = original.path.rstrip("/").lower()
            final_path = final.path.rstrip("/").lower()
            final_query = final.query.lower()

            redirected_to_terminal_page = (
                final_path in {"/jobs", "/careers"}
                or "not-found" in final_path
                or "expired" in final_path
                or final_query in {"error=true", "error=1", "error=404"}
            )

            if redirected_to_terminal_page and (
                final.netloc != original.netloc or final_path != original_path
            ):
                return True

        # 200 with content → still active
        if r.status_code == 200:
            return False

        # Other codes → inconclusive
        return None

    except requests.exceptions.Timeout:
        return "timeout"
    except requests.exceptions.ConnectionError:
        return "conn_error"
    except Exception as e:
        logger.debug("Error verifying %s: %s", url, e)
        return "exception"

    # Other status codes → inconclusive
    return f"status_{r.status_code}"


def run():
    """
    Main entry point for --verify-filled.
    Verifies stale jobs and marks confirmed filled ones.
    Returns stats dict.
    """
    init_logging("verify_filled")
    logger.info("════════════════════════════════════════")
    logger.info("--verify-filled starting")

    start_time = time.time() 

    init_db()

    stale_jobs = get_stale_jobs(VERIFY_FILLED_MISSING_DAYS)

    if not stale_jobs:
        logger.info("No stale jobs to verify")
        print("[INFO] No stale jobs to verify.")
        return {"verified": 0, "filled": 0, "active": 0, "inconclusive": 0}

    # Process up to VERIFY_FILLED_BATCH_SIZE jobs
    batch = stale_jobs[:VERIFY_FILLED_BATCH_SIZE]

    print(f"\n{'='*55}")
    print(f"[INFO] Verify Filled — {datetime.now().strftime('%B %d, %Y')}")
    print(f"[INFO] Stale jobs:  {len(stale_jobs)}")
    print(f"[INFO] Batch size:  {len(batch)}")
    print(f"{'='*55}\n")

    verified     = 0
    filled_count = 0
    active_count = 0
    inconclusive_timeout      = 0
    inconclusive_conn_error   = 0
    inconclusive_other_status = 0
    inconclusive_exception    = 0

    for i, job in enumerate(batch, 1):
        job_id  = job["id"]
        url     = job["job_url"]
        company = job["company"]
        title   = job["title"]
        missing = job["consecutive_missing_days"]

        print(f"[{i}/{len(batch)}] {company} | {title[:50]}")
        print(f"   Missing: {missing} days | {url[:60]}")

        result = _is_job_gone(url)
        verified += 1

        if result is True:
            mark_job_filled(job_id)
            filled_count += 1
            print(f"   [FILLED] Confirmed gone")
        elif result is False:
            reset_missing_days([job_id])
            active_count += 1
            print(f"   [ACTIVE] Still live — reset counter")
        else:
            # Breakdown by reason
            if result == "timeout":
                inconclusive_timeout += 1
                print(f"   [SKIP] Timeout")
            elif result == "conn_error":
                inconclusive_conn_error += 1
                print(f"   [SKIP] Connection error")
            elif result and result.startswith("status_"):
                inconclusive_other_status += 1
                print(f"   [SKIP] Unexpected status: {result.replace('status_', '')}")
            else:
                inconclusive_exception += 1
                print(f"   [SKIP] Exception")
            inconclusive += 1

        # Polite delay between requests
        time.sleep(1)

    remaining = len(stale_jobs) - len(batch)

    print(f"\n{'='*55}")
    print(f"[INFO] Verified: {verified} | "
          f"Filled: {filled_count} | "
          f"Active: {active_count} | "
          f"Inconclusive: {inconclusive}")
    if remaining > 0:
        print(f"[INFO] {remaining} stale jobs remaining — "
              f"will process in future runs")
    print(f"{'='*55}")

    duration = int(time.time() - start_time)

    final_stats = {
        "verified":                  verified,
        "filled":                    filled_count,
        "active":                    active_count,
        "inconclusive":              inconclusive,
        "inconclusive_timeout":      inconclusive_timeout,
        "inconclusive_conn_error":   inconclusive_conn_error,
        "inconclusive_other_status": inconclusive_other_status,
        "inconclusive_exception":    inconclusive_exception,
        "remaining":                 remaining,
        "run_duration_secs":         duration,
    }

    save_verify_filled_stats(final_stats)
    logger.info(
        "--verify-filled complete: verified=%d filled=%d "
        "active=%d inconclusive=%d remaining=%d duration=%ds",
        verified, filled_count, active_count,
        inconclusive, remaining, duration,
    )

    return final_stats