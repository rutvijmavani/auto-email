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
GONE_CODES = {404, 410}

# Redirect destinations that indicate job is gone
GONE_REDIRECT_PATHS = {"/jobs", "/careers", "/job-search", "/open-positions"}
GONE_REDIRECT_KEYWORDS = ["not-found", "expired", "no-longer", "position-filled",
                          "job-closed", "requisition-closed"]
GONE_REDIRECT_QUERIES  = {"error=true", "error=1", "error=404", "flow=error",
                          "status=closed", "status=filled"}

# ATS domains that return 403 when a job no longer exists (not when it's active).
# These platforms gate real job pages behind JS/cookie auth — a plain HTTP GET
# to a live job returns 200; a plain GET to a closed/removed job returns 403.
# So 403 from these domains is effectively the same signal as 404.
ATS_403_MEANS_GONE = {
    "boards.greenhouse.io",
    "job-boards.greenhouse.io",
    "jobs.ashbyhq.com",
    "jobs.lever.co",
    "hire.lever.co",
    "jobs.smartrecruiters.com",
    "jobs.jobvite.com",
}


def _is_job_gone(url):
    """
    Make HTTP request to job URL.

    Returns:
        True              — job confirmed gone (404/410, ATS-403, or terminal redirect)
        False             — job still active (200)
        "status_{code}"   — inconclusive (unexpected status code)
        "timeout"         — request timed out
        "conn_error"      — connection error
        "exception"       — other exception
    """
    from urllib.parse import urlparse

    try:
        r = requests.get(
            url,
            headers=HEADERS,
            timeout=10,
            allow_redirects=True,
        )

        # Hard 404/410 → definitely gone
        if r.status_code in GONE_CODES:
            return True

        # 403 from ATS platforms that return it specifically for removed jobs
        if r.status_code == 403:
            host = urlparse(r.url).netloc.lower().lstrip("www.")
            if any(host == d or host.endswith("." + d) for d in ATS_403_MEANS_GONE):
                logger.debug("403 from known ATS domain → treating as gone: %s", url)
                return True
            # Unknown domain 403 → inconclusive (might be bot-blocking a live job)
            return "status_403"

        # Redirect to a terminal/listing page → gone
        if r.url != url:
            original   = urlparse(url)
            final      = urlparse(r.url)
            final_path = final.path.rstrip("/").lower()
            final_q    = final.query.lower()

            terminal = (
                final_path in GONE_REDIRECT_PATHS
                or any(kw in final_path for kw in GONE_REDIRECT_KEYWORDS)
                or any(q in final_q    for q  in GONE_REDIRECT_QUERIES)
            )
            if terminal and (
                final.netloc != original.netloc
                or final_path != original.path.rstrip("/").lower()
            ):
                return True

        # 200 → still active
        if r.status_code == 200:
            return False

        # Everything else → inconclusive, but named so we can count per-code
        return f"status_{r.status_code}"

    except requests.exceptions.Timeout:
        return "timeout"
    except requests.exceptions.ConnectionError:
        return "conn_error"
    except Exception as e:
        logger.debug("Error verifying %s: %s", url, e)
        return "exception"



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
        duration = int(time.time() - start_time)
        final_stats = {
            "verified":                  0,
            "filled":                    0,
            "active":                    0,
            "inconclusive":              0,
            "inconclusive_timeout":      0,
            "inconclusive_conn_error":   0,
            "inconclusive_other_status": 0,
            "inconclusive_exception":    0,
            "status_code_breakdown":     {},
            "remaining":                 0,
            "run_duration_secs":         duration,
        }
        save_verify_filled_stats(final_stats)
        logger.info(
            "--verify-filled complete: verified=0 filled=0 active=0 "
            "inconclusive=0 remaining=0 duration=%ds",
            duration,
        )
        return final_stats

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
    inconclusive              = 0
    inconclusive_timeout      = 0
    inconclusive_conn_error   = 0
    inconclusive_other_status = 0
    inconclusive_exception    = 0
    status_code_breakdown     = {}   # {"403": 89, "429": 5, …}

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
                code = result.replace("status_", "")
                status_code_breakdown[code] = status_code_breakdown.get(code, 0) + 1
                print(f"   [SKIP] Unexpected status: {code}")
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
        "status_code_breakdown":     status_code_breakdown,
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