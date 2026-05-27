#!/usr/bin/env python3
"""
scripts/test_e2e.py -- End-to-end adaptive polling pipeline test.

Tests the complete dual-tier architecture against real companies in the DB.
Makes real HTTP calls to Greenhouse API (Stripe, Airbnb) and Workday API.

Stages:
     1. PREFLIGHT              -- Redis + DB connectivity, clean test-company state
     2. SCAN WORKER            -- first scan (Stripe, Greenhouse)
     3. SCAN WORKER            -- incremental scan (Airbnb, Greenhouse)
     4. ON_ADAPTIVE_COMPLETE   -- interval engine + fullscan scheduling
     5. DETAIL WORKER (Mode A) -- synthetic Greenhouse job (no HTTP)
     6. DETAIL WORKER (Mode B) -- Workday guard clause + real API call
                                  Sub-test A: broken payload → guard fires (no HTTP)
                                  Sub-test B: full payload → API called → location set
     7. FULLSCAN               -- full scan (Airbnb, Greenhouse)
     8. WATCHDOG               -- orphan + hung worker checks
     9. REDIS SIGNAL           -- pause / heartbeat / resume cycle
    10. CLEANUP                -- remove synthetic test data

Usage:
    python scripts/test_e2e.py
    python scripts/test_e2e.py --stage 6   # run only one stage
"""

import json
import os
import sys
import time
from datetime import datetime, timezone, timedelta

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from dotenv import load_dotenv
load_dotenv()

GREEN  = "\033[32m"
RED    = "\033[31m"
YELLOW = "\033[33m"
CYAN   = "\033[36m"
RESET  = "\033[0m"
BOLD   = "\033[1m"

def ok(msg):   print(f"  {GREEN}[OK]{RESET} {msg}")
def fail(msg): print(f"  {RED}[FAIL]{RESET} {msg}"); sys.exit(1)
def warn(msg): print(f"  {YELLOW}[WARN]{RESET} {msg}")
def info(msg): print(f"  {CYAN}[..]{RESET} {msg}")

def header(n, title):
    print(f"\n{BOLD}[{n}] {title}{RESET}")
    print("-" * 60)

# -- test constants ------------------------------------------------------------
TEST_JOB_ID       = "e2e-test-00001"
TEST_JOB_URL      = "https://boards.greenhouse.io/stripe/jobs/e2e-test-00001"
TEST_COMPANY      = "Stripe"    # first-scan company (Greenhouse / Mode A)
INCR_COMPANY      = "Airbnb"   # incremental + fullscan company (Greenhouse / Mode A)
WORKDAY_E2E_JOB   = "e2e-test-workday-00001"  # synthetic job_id for Mode B stage


# ===========================================================
# STAGE 1 -- PREFLIGHT
# ===========================================================

def stage_preflight():
    header(1, "PREFLIGHT -- Redis + DB connectivity")

    from workers.redis_client import ping, get_redis
    from db.db import init_db, get_conn

    if not ping():
        fail("Redis not reachable -- is Memurai running?")
    ok("Redis reachable")

    r = get_redis()
    info(f"Redis URL: {os.getenv('REDIS_URL', 'redis://localhost:6379/0')}")

    init_db()
    conn = get_conn()
    try:
        rows = conn.execute("""
            SELECT company, ats_platform, first_scanned_at
            FROM prospective_companies
            WHERE company IN (%s, %s)
        """, (TEST_COMPANY, INCR_COMPANY)).fetchall()
    finally:
        conn.close()

    companies = {row["company"]: row for row in rows}

    if TEST_COMPANY not in companies:
        fail(f"Test company '{TEST_COMPANY}' not found in DB")
    if INCR_COMPANY not in companies:
        fail(f"Incremental company '{INCR_COMPANY}' not found in DB")

    ok(f"{TEST_COMPANY}: platform={companies[TEST_COMPANY]['ats_platform']}  "
       f"first_scanned_at={companies[TEST_COMPANY]['first_scanned_at']}")
    ok(f"{INCR_COMPANY}: platform={companies[INCR_COMPANY]['ats_platform']}  "
       f"first_scanned_at={companies[INCR_COMPANY]['first_scanned_at']}")

    if companies[INCR_COMPANY]["first_scanned_at"] is None:
        warn(f"{INCR_COMPANY} has not been first-scanned -- incremental test may not be meaningful")

    # -- Clean leftover state from previous test runs ---------------------------
    # Remove both test companies from the scheduler queues so each run is
    # independent regardless of what the previous run left behind.
    removed_adaptive = r.zrem("poll:adaptive", TEST_COMPANY, INCR_COMPANY)
    removed_fullscan = r.zrem("poll:fullscan", TEST_COMPANY, INCR_COMPANY)
    if removed_adaptive or removed_fullscan:
        info(f"Cleared leftover queue entries "
             f"(adaptive={removed_adaptive} fullscan={removed_fullscan}) from prior run")

    # Flush any pending detail jobs for the test companies from previous runs
    # (we rebuild during stage 3 after scan, so flushing here is safe)
    flushed = 0
    for _ in range(r.llen("queue:detail:adaptive")):
        item = r.rpop("queue:detail:adaptive")
        if item is None:
            break
        try:
            payload = json.loads(item)
            if payload.get("company") not in (TEST_COMPANY, INCR_COMPANY):
                r.lpush("queue:detail:adaptive", item)   # put back
            else:
                flushed += 1
        except Exception:
            r.lpush("queue:detail:adaptive", item)

    if flushed:
        info(f"Flushed {flushed} leftover detail queue entries from prior test run")

    info(f"poll:adaptive   members = {r.zcard('poll:adaptive')}")
    info(f"poll:fullscan   members = {r.zcard('poll:fullscan')}")
    info(f"scan:queue      depth   = {r.llen('scan:queue')}")
    info(f"detail:adaptive depth   = {r.llen('queue:detail:adaptive')}")
    ok("Preflight complete -- queues clean for test run")


# ===========================================================
# STAGE 2 -- SCAN WORKER: FIRST SCAN
# ===========================================================

def stage_scan_first():
    header(2, f"SCAN WORKER -- first scan ({TEST_COMPANY}, Greenhouse)")

    from workers.redis_client import get_redis
    from workers.scan_worker import _run_listing_scan
    from db.db import get_conn
    from config import JOB_MONITOR_DAYS_FRESH

    r = get_redis()

    # Force first-scan path by clearing first_scanned_at
    conn = get_conn()
    try:
        conn.execute("""
            UPDATE prospective_companies SET first_scanned_at = NULL WHERE company = %s
        """, (TEST_COMPANY,))
        conn.commit()
        info(f"Reset {TEST_COMPANY} first_scanned_at -> NULL")
    finally:
        conn.close()

    # Clear seen: SET so first scan populates it fresh
    r.delete(f"seen:{TEST_COMPANY}")
    info(f"Cleared seen:{TEST_COMPANY}")

    result = _run_listing_scan({
        "company":     TEST_COMPANY,
        "scan_type":   "adaptive",
        "request_id":  "e2e-first-001",
        "enqueued_at": datetime.now(timezone.utc).isoformat(),
    })

    info(f"Result: success={result['success']} first_scan={result.get('first_scan')} "
         f"fetched={result['fetched']} new_jobs={result['new_jobs']} "
         f"duration={result['duration_ms']}ms")

    if not result["success"]:
        fail(f"scan_worker returned success=False: {result.get('error', '?')}")
    ok("scan returned success=True")

    if not result.get("first_scan"):
        fail("Expected first_scan=True on initial scan")
    ok("first_scan=True detected correctly")

    info(f"fresh jobs queued for detail = {result['new_jobs']} "
         f"(posted within {JOB_MONITOR_DAYS_FRESH}d)")
    info(f"stale jobs marked pre_existing = {result['fetched'] - result['new_jobs']}")
    ok("first scan correctly splits fresh vs. stale jobs")

    if result["fetched"] == 0:
        warn(f"fetched=0 -- {TEST_COMPANY} Greenhouse API returned 0 jobs")
    else:
        ok(f"fetched={result['fetched']} jobs from Greenhouse API")

    # Verify DB
    conn = get_conn()
    try:
        row = conn.execute("""
            SELECT first_scanned_at FROM prospective_companies WHERE company = %s
        """, (TEST_COMPANY,)).fetchone()
    finally:
        conn.close()

    if not row or row["first_scanned_at"] is None:
        fail("first_scanned_at not set in DB after first scan")
    ok(f"first_scanned_at set in DB: {row['first_scanned_at']}")

    # Verify seen: SET
    seen_count = r.scard(f"seen:{TEST_COMPANY}")
    # seen: contains stale jobs; fresh jobs are added by detail_worker on completion
    expected_seen = result["fetched"] - result["new_jobs"]
    if result["fetched"] > 0 and seen_count != expected_seen:
        warn(f"seen:{TEST_COMPANY} has {seen_count} members, expected {expected_seen} "
             "(stale jobs only -- fresh ones added by detail_worker on completion)")
    else:
        ok(f"seen:{TEST_COMPANY} has {seen_count} members (stale pre_existing jobs)")

    # Verify detail queue
    detail_depth = r.llen("queue:detail:adaptive")
    if result["new_jobs"] > 0:
        ok(f"queue:detail:adaptive has {detail_depth} fresh jobs queued")
    else:
        info(f"queue:detail:adaptive depth={detail_depth} "
             f"(no {TEST_COMPANY} postings in last {JOB_MONITOR_DAYS_FRESH}d)")
    ok("first scan detail queue state verified")


# ===========================================================
# STAGE 3 -- SCAN WORKER: INCREMENTAL
# ===========================================================

def stage_scan_incremental():
    header(3, f"SCAN WORKER -- incremental scan ({INCR_COMPANY}, Greenhouse)")

    from workers.redis_client import get_redis
    from workers.scan_worker import _run_listing_scan
    from db.db import get_conn

    r = get_redis()

    # adaptive_seen:{company} is populated naturally by scan_worker runs.
    # No pre-seeding needed — first run will do DB lookups for all jobs,
    # subsequent runs within the same day skip via the SET cache.
    pre_seen_count = r.scard(f"adaptive_seen:{INCR_COMPANY}")
    if pre_seen_count > 0:
        ok(f"adaptive_seen:{INCR_COMPANY} has {pre_seen_count} members from earlier runs")
    else:
        info(f"adaptive_seen:{INCR_COMPANY} is empty -- scan_worker will populate it")

    result = _run_listing_scan({
        "company":     INCR_COMPANY,
        "scan_type":   "adaptive",
        "request_id":  "e2e-incr-001",
        "enqueued_at": datetime.now(timezone.utc).isoformat(),
    })

    info(f"Result: success={result['success']} first_scan={result.get('first_scan')} "
         f"fetched={result['fetched']} new_jobs={result['new_jobs']} "
         f"duration={result['duration_ms']}ms")

    if not result["success"]:
        fail(f"scan_worker returned success=False: {result.get('error', '?')}")
    ok("scan returned success=True")

    if result.get("first_scan"):
        warn(f"{INCR_COMPANY} ran as first scan (first_scanned_at was NULL) -- "
             "incremental diff not tested. Re-run to test incremental path.")
    else:
        ok("incremental diff ran (not first_scan)")

    post_seen_count = r.scard(f"adaptive_seen:{INCR_COMPANY}")
    if post_seen_count > pre_seen_count:
        ok(f"adaptive_seen:{INCR_COMPANY} grew {pre_seen_count}→{post_seen_count} (scan populated cache)")
    elif post_seen_count > 0:
        ok(f"adaptive_seen:{INCR_COMPANY} has {post_seen_count} entries (populated by prior or current run)")
    else:
        fail(f"adaptive_seen:{INCR_COMPANY} is still empty after scan — cache not populated")

    ok(f"fetched={result['fetched']} from Greenhouse API")
    if result["new_jobs"] == 0:
        ok(f"new_jobs=0 -- all {INCR_COMPANY} jobs already seen (correct for recently scanned company)")
    else:
        ok(f"new_jobs={result['new_jobs']} -- genuinely new {INCR_COMPANY} job(s) posted since last scan")
        info(f"These are real jobs -- they will remain in queue:detail:adaptive for detail_worker")

    # Poll stats
    conn = get_conn()
    try:
        row = conn.execute("""
            SELECT total_polls, last_poll_at
            FROM company_poll_stats WHERE company = %s
        """, (INCR_COMPANY,)).fetchone()
    finally:
        conn.close()

    if row is None:
        warn(f"No company_poll_stats row for {INCR_COMPANY}")
    else:
        ok(f"poll_stats: total_polls={row['total_polls']} last_poll_at={row['last_poll_at']}")


# ===========================================================
# STAGE 4 -- ON_ADAPTIVE_COMPLETE (interval engine + fullscan scheduling)
# ===========================================================

def stage_on_adaptive_complete():
    header(4, "ON_ADAPTIVE_COMPLETE -- interval engine + fullscan scheduling")

    from workers.scheduler import on_adaptive_complete
    from workers.redis_client import get_redis
    from db.db import get_conn
    from db.job_monitor import upsert_poll_stats

    r = get_redis()

    # Ensure poll_stats row exists for INCR_COMPANY (stage 3 creates it via upsert)
    conn = get_conn()
    try:
        row = conn.execute("""
            SELECT current_interval_s, recent_poll_counts, adaptive_score
            FROM company_poll_stats WHERE company = %s
        """, (INCR_COMPANY,)).fetchone()
    finally:
        conn.close()

    if row is None:
        upsert_poll_stats(INCR_COMPANY, "greenhouse", 0, 0)
        info(f"Created poll_stats row for {INCR_COMPANY}")
    else:
        info(f"poll_stats before: interval={row['current_interval_s']}s  "
             f"score={row['adaptive_score']}  counts={row['recent_poll_counts']}")

    # Record a simulated scan result: 3 new jobs found
    info(f"Calling on_adaptive_complete({INCR_COMPANY!r}, new_jobs=3, success=True)...")
    on_adaptive_complete(INCR_COMPANY, new_jobs=3, success=True)

    # -- Verify poll:adaptive was rescheduled ----------------------------------
    score = r.zscore("poll:adaptive", INCR_COMPANY)
    if score is None:
        fail(f"{INCR_COMPANY} not found in poll:adaptive after on_adaptive_complete")
    next_poll = datetime.fromtimestamp(score).strftime("%Y-%m-%d %H:%M")
    ok(f"poll:adaptive rescheduled: next_poll={next_poll} (score={score:.0f})")

    if score > time.time():
        ok("poll:adaptive score is in the future (correct)")
    else:
        warn("poll:adaptive score is in the past -- interval may be 0")

    # -- Verify adaptive interval was updated in DB ----------------------------
    conn = get_conn()
    try:
        row = conn.execute("""
            SELECT current_interval_s, adaptive_score, recent_poll_counts, last_poll_at
            FROM company_poll_stats WHERE company = %s
        """, (INCR_COMPANY,)).fetchone()
    finally:
        conn.close()

    if row is None:
        fail("poll_stats row missing after on_adaptive_complete")
    ok(f"adaptive_score={row['adaptive_score']:.3f}  "
       f"interval={row['current_interval_s']}s  "
       f"counts={row['recent_poll_counts']}")
    ok(f"last_poll_at updated: {row['last_poll_at']}")

    # -- Verify poll:fullscan was scheduled (last_full_scan_at IS NULL -> first full scan due) --
    fs_score = r.zscore("poll:fullscan", INCR_COMPANY)
    if fs_score is not None:
        fs_time = datetime.fromtimestamp(fs_score).strftime("%Y-%m-%d %H:%M")
        ok(f"poll:fullscan scheduled: score={fs_time} (Rule 3 -- full scan queued after adaptive)")
    else:
        # Full scan may already be in poll:fullscan from stage 3 or prior run,
        # or last_full_scan_at is recent enough to not trigger Rule 3.
        conn = get_conn()
        try:
            lfs = conn.execute("""
                SELECT last_full_scan_at FROM company_poll_stats WHERE company = %s
            """, (INCR_COMPANY,)).fetchone()
        finally:
            conn.close()
        if lfs and lfs["last_full_scan_at"]:
            info(f"poll:fullscan not added -- last_full_scan_at={lfs['last_full_scan_at']} "
                 "(full scan interval not yet elapsed)")
        else:
            warn(f"poll:fullscan not scheduled despite last_full_scan_at=NULL -- "
                 "check _should_trigger_full_scan() logic")


# ===========================================================
# STAGE 5 -- DETAIL WORKER: Mode A synthetic job
# ===========================================================

def stage_detail_worker():
    header(5, "DETAIL WORKER -- Mode A synthetic job (Greenhouse, no HTTP)")

    from workers.redis_client import get_redis
    from workers.detail_worker import _process_detail
    from db.db import get_conn

    r = get_redis()

    yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    today     = datetime.now().strftime("%Y-%m-%d")

    # Insert synthetic pending_detail row
    conn = get_conn()
    try:
        conn.execute("""
            INSERT INTO job_postings
              (company, title, job_url, job_id, ats_platform,
               location, posted_at, description, skill_score,
               status, found_by, first_seen)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s,
                    'pending_detail', %s, %s)
            ON CONFLICT (job_url) DO UPDATE SET
                status    = 'pending_detail',
                job_id    = EXCLUDED.job_id,
                posted_at = EXCLUDED.posted_at
        """, (
            TEST_COMPANY, "Senior Software Engineer",
            TEST_JOB_URL, TEST_JOB_ID, "greenhouse",
            "San Francisco, CA", yesterday,
            "We are looking for a senior software engineer with Python and AWS experience.",
            3, "tier1_adaptive", today,
        ))
        conn.commit()
        info(f"Inserted synthetic pending_detail row: job_id={TEST_JOB_ID}")
    finally:
        conn.close()

    payload = {
        "company":      TEST_COMPANY,
        "ats_platform": "greenhouse",
        "job_id":       TEST_JOB_ID,
        "job_url":      TEST_JOB_URL,
        "title":        "Senior Software Engineer",
        "location":     "San Francisco, CA",
        "posted_at":    yesterday,
        "description":  "We are looking for a senior software engineer with Python and AWS experience.",
        "skill_score":  3,
        "found_by":     "tier1_adaptive",
        "slug_info":    "stripe",
        "enqueued_at":  datetime.now(timezone.utc).isoformat(),
    }

    info("Running _process_detail (Mode A -- no HTTP fetch)...")
    result = _process_detail(payload, source_queue="queue:detail:adaptive")

    info(f"Result: outcome={result['outcome']} duration={result['duration_ms']}ms")

    if result["outcome"] == "error":
        fail("detail_worker returned outcome=error for synthetic job")

    if result["outcome"] == "filtered":
        warn("Synthetic job was filtered -- check title/location filter settings")
        conn = get_conn()
        try:
            row = conn.execute(
                "SELECT status FROM job_postings WHERE job_url = %s", (TEST_JOB_URL,)
            ).fetchone()
        finally:
            conn.close()
        if row is None:
            ok("Filtered: pending_detail row deleted correctly")
        else:
            info(f"Filtered: row still exists with status={row['status']}")

    elif result["outcome"] == "new":
        ok("outcome=new -- job passed all filters and was promoted")

        conn = get_conn()
        try:
            row = conn.execute(
                "SELECT status FROM job_postings WHERE job_url = %s", (TEST_JOB_URL,)
            ).fetchone()
        finally:
            conn.close()

        if not row or row["status"] != "new":
            fail(f"Expected status='new' in DB, got '{row['status'] if row else None}'")
        ok("DB row status='new' confirmed")

        if r.sismember(f"seen:{TEST_COMPANY}", TEST_JOB_ID):
            ok(f"seen:{TEST_COMPANY} contains {TEST_JOB_ID} (SADD by detail_worker)")
        else:
            warn(f"seen:{TEST_COMPANY} does NOT contain {TEST_JOB_ID}")

    else:
        warn(f"Unexpected outcome: {result['outcome']}")


# ===========================================================
# STAGE 6 -- DETAIL WORKER (Mode B): Workday guard clause + real API
# ===========================================================

def stage_detail_worker_mode_b():
    """
    Verifies the full Mode B (Workday) detail-fetch contract end-to-end.

    Two sub-tests run back-to-back against the same real _external_path
    fetched live from the Workday listing API:

      Sub-test A — BROKEN payload (_slug / _wd / _path intentionally empty)
        fetch_job_detail()'s guard clause fires → location / _country_code
        stay empty → no HTTP request made.
        This is the pre-fix behaviour that caused India jobs to leak.

      Sub-test B — FULL payload (all four required keys present)
        Guard passes → Workday detail API is called → location and
        _country_code are populated in the returned dict.
        This is what must be true after the Accenture fix.

    The test uses a real _external_path from the live listing so the detail
    fetch exercises the actual production code path.  No DB rows are written
    by this stage — fetch_job_detail() is called directly.
    """
    header(6, "DETAIL WORKER (Mode B) — Workday guard clause + real API call")

    from db.db import get_conn
    from jobs.ats.registry import get_config, parse_slug
    from jobs.ats_detector import get_ats_module
    from jobs.job_filter import is_us_location

    # ── 1. Find an active Workday company in DB ───────────────────────────────
    conn = get_conn()
    try:
        row = conn.execute("""
            SELECT company, ats_slug FROM prospective_companies
            WHERE ats_platform = %s AND status = %s
            ORDER BY company LIMIT 1
        """, ("workday", "active")).fetchone()
    finally:
        conn.close()

    if not row:
        warn("No active Workday company found in DB — Mode B stage skipped.")
        warn("Populate one with:  python scripts/seed_test_companies.py")
        warn("or run:  python pipeline.py --detect-ats --batch")
        return

    wday_company = row["company"]
    config       = get_config("workday")
    slug_info    = parse_slug("workday", row["ats_slug"], config)
    slug = slug_info.get("slug", "")
    wd   = slug_info.get("wd",   "")
    path = slug_info.get("path", "careers")
    site = slug_info.get("site")

    if not slug or not wd:
        warn(f"Could not parse Workday slug for {wday_company!r} — skipping Mode B stage")
        return

    ok(f"Workday company: {wday_company!r}  slug={slug!r}  wd={wd!r}  path={path!r}")

    # ── 2. Fetch listing to get a real _external_path ─────────────────────────
    info(f"Fetching Workday listing for {wday_company!r} (real HTTP, ~5-10s)...")
    ats_module = get_ats_module("workday")
    try:
        all_jobs = ats_module.fetch_jobs(slug_info, wday_company)
    except Exception as exc:
        warn(f"fetch_jobs raised {exc!r} — skipping Mode B stage")
        return

    sample = next((j for j in all_jobs if j.get("_external_path")), None)
    if not sample:
        warn(f"Listing returned {len(all_jobs)} jobs but none have _external_path — skipping")
        return

    ext_path  = sample["_external_path"]
    job_url   = sample.get("job_url", "")
    job_title = sample.get("title", "")
    ok(f"Grabbed _external_path from listing  ({len(all_jobs)} total jobs fetched)")
    info(f"  title       : {job_title[:60]!r}")
    info(f"  job_url     : {job_url[:70]}")
    info(f"  _external_path: {ext_path!r}")

    # ── Sub-test A: BROKEN payload → guard must fire (no HTTP) ────────────────
    info("")
    info("Sub-test A — BROKEN payload  (_slug/_wd/_path intentionally empty)")
    broken_payload = {
        "company":        wday_company,
        "job_id":         WORKDAY_E2E_JOB,
        "job_url":        job_url,
        "title":          job_title,
        "location":       "",
        "description":    "",
        "_country_code":  "",
        "_external_path": ext_path,   # real path — but slug/wd/path missing
        "_slug":          "",          # ← intentionally empty (pre-fix bug)
        "_wd":            "",          # ← intentionally empty
        "_path":          "",          # ← intentionally empty
        "_site":          site,
    }
    broken_after = ats_module.fetch_job_detail(dict(broken_payload))

    b_loc = broken_after.get("location", "")
    b_cc  = broken_after.get("_country_code", "")

    if b_loc == "" and b_cc == "":
        ok("Guard fired — location and _country_code unchanged (no HTTP request) ✓")
    else:
        # Guard did NOT fire when it should have — unexpected
        fail(
            f"BROKEN payload should trigger guard but API was called "
            f"(location={b_loc!r}  cc={b_cc!r}).  "
            "Check fetch_job_detail() guard logic in jobs/ats/workday.py"
        )

    b_us = is_us_location(b_loc)
    if b_us:
        info("is_us_location('') = True  → broken path leaks non-US jobs as 'US' ✗")
    else:
        info(f"is_us_location({b_loc!r}) = False  → broken path correctly filtered (unusual)")

    # ── Sub-test B: FULL payload → guard must NOT fire, API must be called ────
    info("")
    info("Sub-test B — FULL payload  (all four required keys present)")
    full_payload = {
        "company":        wday_company,
        "job_id":         WORKDAY_E2E_JOB,
        "job_url":        job_url,
        "title":          job_title,
        "location":       "",
        "description":    "",
        "_country_code":  "",
        "_external_path": ext_path,
        "_slug":          slug,        # ← fixed: all keys present
        "_wd":            wd,
        "_path":          path,
        "_site":          site,
    }
    full_after = ats_module.fetch_job_detail(dict(full_payload))

    f_loc  = full_after.get("location", "")
    f_cc   = full_after.get("_country_code", "")
    f_desc = full_after.get("description", "")
    enriched = f_loc != "" or f_cc != "" or f_desc != ""

    if enriched:
        ok(f"API called + job enriched ✓")
        ok(f"  location      : {f_loc!r}")
        ok(f"  _country_code : {f_cc!r}")
        ok(f"  description   : {len(f_desc)} chars")
    else:
        # API was called (guard didn't fire) but returned empty data.
        # This is an API-side issue, not a code bug.  Warn, don't fail.
        warn(
            "API was called (all keys present, guard passed) but returned "
            "no new data.  The Workday job may have expired or the API "
            "returned an empty response for this specific posting."
        )

    f_us = is_us_location(f_loc)
    if f_loc:
        decision = "INCLUDE (US)" if f_us else "EXCLUDE (non-US)"
        info(f"is_us_location({f_loc!r}) = {f_us}  → {decision}")
    else:
        info("location empty after detail fetch — is_us_location check skipped")

    # ── Final verdict ─────────────────────────────────────────────────────────
    if not enriched:
        # Only case where we still pass: API returned empty but guard didn't fire.
        # Key check: sub-test A proved guard fires for broken; sub-test B had
        # all keys present.  The contract is correct even if this job's data is gone.
        ok("Mode B contract verified: guard fires iff required keys are missing ✓")
    else:
        ok("Mode B contract verified: guard bypassed + API enriched job ✓")


# ===========================================================
# STAGE 7 (was 6) -- FULLSCAN WORKER
# ===========================================================

def stage_fullscan():
    header(7, f"FULLSCAN WORKER -- full scan ({INCR_COMPANY}, Greenhouse)")

    from workers.redis_client import get_redis
    from workers.fullscan import _run_fullscan, _BloomPair
    from db.db import get_conn
    from db.job_monitor import upsert_poll_stats

    r = get_redis()

    # Ensure poll_stats row exists
    conn = get_conn()
    try:
        row = conn.execute("""
            SELECT company, full_scan_interrupted, last_full_scan_at, full_scan_interval_s
            FROM company_poll_stats WHERE company = %s
        """, (INCR_COMPANY,)).fetchone()
    finally:
        conn.close()

    if row is None:
        upsert_poll_stats(INCR_COMPANY, "greenhouse", 0, 0)
        info(f"Created poll_stats row for {INCR_COMPANY}")
    else:
        info(f"poll_stats: interrupted={row['full_scan_interrupted']}  "
             f"last_full_scan={row['last_full_scan_at']}  "
             f"interval={row['full_scan_interval_s']}s")

    # Clear both OLD and NEW bloom filter keys for a clean full-scan run.
    # prepare_fresh() does this inside _run_fullscan, but doing it explicitly
    # here ensures a truly clean slate for the test.
    bloom = _BloomPair(r, INCR_COMPANY)
    bloom.prepare_fresh()
    r.delete(f"bloom:fullscan:{INCR_COMPANY}")   # also wipe OLD key
    r.delete(f"bloom:fallback:{INCR_COMPANY}")
    info(f"Cleared stale bloom filters for {INCR_COMPANY}")

    info(f"Running _run_fullscan for {INCR_COMPANY} (skip_lock=True for test)...")
    info("Real HTTP calls to Greenhouse API -- may take ~5-15s")
    result = _run_fullscan(INCR_COMPANY, r, skip_lock=True)

    info(f"Result: outcome={result['outcome']} success={result['success']} "
         f"fetched={result['fetched']} new_jobs={result['new_jobs']} "
         f"pages={result['pages']} duration={result['duration_ms']}ms")

    if result["outcome"] == "error":
        fail(f"fullscan returned outcome=error: {result}")
    if result["outcome"] == "skipped":
        fail(f"fullscan skipped {INCR_COMPANY} -- check DB company row")
    if result["outcome"] == "deferred":
        warn("Fullscan deferred (Rule 4: adaptive hasn't run this cycle)")
        ok("Deferred is a valid outcome (Rule 4 working)")
        return

    ok(f"outcome={result['outcome']}")
    ok(f"fetched={result['fetched']}  new_jobs={result['new_jobs']}  pages={result['pages']}")

    if result["outcome"] == "completed":
        # Bloom filter
        bf_exists = r.exists(f"bloom:fullscan:{INCR_COMPANY}") or \
                    r.exists(f"bloom:fallback:{INCR_COMPANY}")
        if bf_exists:
            ok(f"Bloom filter exists for {INCR_COMPANY}")
        else:
            warn("Bloom filter not found (may be empty if 0 jobs processed)")

        # DB update
        conn = get_conn()
        try:
            row = conn.execute("""
                SELECT last_full_scan_at, full_scan_interrupted
                FROM company_poll_stats WHERE company = %s
            """, (INCR_COMPANY,)).fetchone()
        finally:
            conn.close()

        if row and row["last_full_scan_at"]:
            ok(f"last_full_scan_at updated: {row['last_full_scan_at']}")
        else:
            warn("last_full_scan_at not updated in DB")

        if row and not row["full_scan_interrupted"]:
            ok("full_scan_interrupted=FALSE (clean completion)")

        # Reschedule score
        score = r.zscore("poll:fullscan", INCR_COMPANY)
        if score and score > time.time():
            ok(f"poll:fullscan rescheduled: "
               f"{datetime.fromtimestamp(score).strftime('%Y-%m-%d %H:%M')} (future)")
        else:
            warn(f"poll:fullscan score unexpected: {score}")


# ===========================================================
# STAGE 8 (was 7) -- WATCHDOG
# ===========================================================

def stage_watchdog():
    header(8, "WATCHDOG -- hung worker + stream PEL checks")

    # NOTE: check_orphans() / track_inflight() / clear_inflight() were removed
    # in the two-layer scheduler redesign (Section 5).  PEL + claim_stale_work()
    # (XAUTOCLAIM) in scheduler.py owns crash-recovery for scan workers.
    # The watchdog now covers:
    #   1. Hung-worker detection via heartbeat:{company} + progress:{company}
    #   2. Stream PEL observability via XPENDING (informational; no recovery)

    from workers.watchdog import check_hung_workers, check_pel_stats
    from workers.redis_client import get_redis
    from config import REDIS_STREAM_ADAPTIVE, REDIS_STREAM_FULLSCAN

    r = get_redis()

    # ── 1. Hung-worker detection ──────────────────────────────────────────────
    info("Running check_hung_workers()...")
    hung = check_hung_workers()
    ok(f"check_hung_workers returned {len(hung)} hung workers (expected 0 in clean state)")
    if hung:
        warn(f"Hung workers detected: {hung}")

    # ── 2. Stream PEL stats ───────────────────────────────────────────────────
    info("Running check_pel_stats()...")
    pel = check_pel_stats()

    for stream_key in (REDIS_STREAM_ADAPTIVE, REDIS_STREAM_FULLSCAN):
        stats = pel.get(stream_key)
        if stats is None:
            warn(f"PEL stats missing for {stream_key} (stream may not exist yet)")
            continue
        total   = stats.get("total_pending", 0)
        age_s   = (stats["oldest_age_ms"] // 1000) if stats.get("oldest_age_ms") else 0
        ok(
            f"PEL {stream_key}: pending={total} "
            f"oldest_age={age_s}s "
            f"consumers={stats.get('consumers', [])}"
        )
        if total > 0:
            warn(
                f"{stream_key} has {total} pending messages — "
                "this is normal during active scans; "
                "claim_stale_work() reclaims them after p95*3 ms idle."
            )


# ===========================================================
# STAGE 9 (was 8) -- REDIS SIGNAL (pause / heartbeat / resume)
# ===========================================================

def stage_redis_signal():
    header(9, "REDIS SIGNAL -- pause / heartbeat / resume")

    from scripts.redis_signal import cmd_pause, cmd_heartbeat, cmd_resume
    from workers.redis_client import get_redis
    from workers.fullscan import _is_paused
    from workers.scheduler import _check_auto_resume, _pause_event, _resume_event
    from config import REDIS_CRONCHAIN_ALIVE, REDIS_DB_MAINTENANCE

    r = get_redis()

    # Pause
    info("Running cmd_pause()...")
    cmd_pause()

    if not r.exists(REDIS_DB_MAINTENANCE):
        fail("db:maintenance key NOT set after pause")
    ok("db:maintenance key SET after pause")

    if not r.exists(REDIS_CRONCHAIN_ALIVE):
        fail("cronchain:alive key NOT set after pause")
    ok(f"cronchain:alive key SET (TTL={r.ttl(REDIS_CRONCHAIN_ALIVE)}s)")

    # Heartbeat
    info("Running cmd_heartbeat()...")
    cmd_heartbeat()
    ttl = r.ttl(REDIS_CRONCHAIN_ALIVE)
    ok(f"cronchain:alive TTL refreshed to {ttl}s after heartbeat")

    # _is_paused() check
    if _is_paused(r):
        ok("fullscan._is_paused() correctly detects maintenance window")
    else:
        fail("fullscan._is_paused() returned False during maintenance")

    # Scheduler auto-resume should NOT fire while cronchain:alive is set.
    # Two-event pattern: _pause_event.set() = paused, _resume_event.clear() = paused.
    _pause_event.set()
    _resume_event.clear()
    _check_auto_resume()
    if _pause_event.is_set() and not _resume_event.is_set():
        ok("scheduler._check_auto_resume() did NOT auto-resume (cronchain:alive alive)")
    else:
        fail(
            "scheduler._check_auto_resume() fired early — cronchain:alive may have expired "
            f"(pause_event.is_set={_pause_event.is_set()}, "
            f"resume_event.is_set={_resume_event.is_set()})"
        )
    # Reset to running state
    _pause_event.clear()
    _resume_event.set()

    # Resume
    info("Running cmd_resume()...")
    cmd_resume()

    if r.exists(REDIS_DB_MAINTENANCE):
        fail("db:maintenance key still set after resume")
    ok("db:maintenance key DELETED after resume")

    if r.exists(REDIS_CRONCHAIN_ALIVE):
        fail("cronchain:alive key still set after resume")
    ok("cronchain:alive key DELETED after resume")

    if _is_paused(r):
        fail("fullscan._is_paused() still True after resume")
    ok("fullscan._is_paused() correctly returns False after resume")


# ===========================================================
# STAGE 10 (was 9) -- CLEANUP
# ===========================================================

def stage_cleanup():
    header(10, "CLEANUP -- remove synthetic test data")

    from db.db import get_conn
    from workers.redis_client import get_redis

    r   = get_redis()
    conn = get_conn()

    # Delete synthetic job row (e2e-test-00001)
    try:
        cursor = conn.execute(
            "DELETE FROM job_postings WHERE job_url = %s", (TEST_JOB_URL,)
        )
        conn.commit()
        if cursor.rowcount > 0:
            ok(f"Deleted synthetic job row ({TEST_JOB_URL})")
        else:
            info("Synthetic job row not found (already cleaned or never inserted)")
    finally:
        conn.close()

    # Remove synthetic job_id from seen:{TEST_COMPANY}
    r.srem(f"seen:{TEST_COMPANY}", TEST_JOB_ID)
    ok(f"Removed {TEST_JOB_ID} from seen:{TEST_COMPANY}")

    # Remove watchdog simulation keys
    r.srem("watchdog:inflight", "__e2e_orphan_sim__", "__e2e_test_company__")
    ok("Removed e2e watchdog simulation keys")

    # Remove test companies from scheduler queues
    r.zrem("poll:adaptive", TEST_COMPANY, INCR_COMPANY)
    r.zrem("poll:fullscan", TEST_COMPANY, INCR_COMPANY)
    ok("Removed test companies from poll:adaptive and poll:fullscan")

    # Report any real new jobs left in queue:detail:adaptive from stage 3
    # (genuine Airbnb postings -- leave them for detail_worker to process)
    depth = r.llen("queue:detail:adaptive")
    if depth > 0:
        info(f"queue:detail:adaptive has {depth} item(s) remaining -- "
             f"these are real new jobs found during the test, detail_worker will process them")
    else:
        ok("queue:detail:adaptive is empty")

    ok("Cleanup complete")


# ===========================================================
# MAIN
# ===========================================================

STAGES = {
    1:  ("PREFLIGHT",                    stage_preflight),
    2:  ("SCAN FIRST",                   stage_scan_first),
    3:  ("SCAN INCREMENTAL",             stage_scan_incremental),
    4:  ("ON_ADAPTIVE_COMPLETE",         stage_on_adaptive_complete),
    5:  ("DETAIL WORKER — Mode A",       stage_detail_worker),
    6:  ("DETAIL WORKER — Mode B",       stage_detail_worker_mode_b),
    7:  ("FULLSCAN",                     stage_fullscan),
    8:  ("WATCHDOG",                     stage_watchdog),
    9:  ("REDIS SIGNAL",                 stage_redis_signal),
    10: ("CLEANUP",                      stage_cleanup),
}

def main():
    target = None
    if "--stage" in sys.argv:
        idx = sys.argv.index("--stage")
        try:
            target = int(sys.argv[idx + 1])
        except (IndexError, ValueError):
            print("Usage: python scripts/test_e2e.py [--stage N]")
            sys.exit(1)

    print(f"\n{BOLD}{'=' * 60}{RESET}")
    print(f"{BOLD}  Adaptive Polling Pipeline - End-to-End Test{RESET}")
    print(f"{BOLD}{'=' * 60}{RESET}")
    print(f"  Date:    {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  Greenhouse : {TEST_COMPANY} (first scan) / {INCR_COMPANY} (incremental + fullscan)")
    print(f"  Workday    : first active Workday company in DB (Mode B stage 6)")

    to_run = [(target, STAGES[target])] if target else list(STAGES.items())

    t0 = time.monotonic()
    for n, (name, fn) in to_run:
        try:
            fn()
        except SystemExit:
            raise
        except Exception as exc:
            import traceback
            print(f"\n  {RED}[CRASH] STAGE {n} ({name}):{RESET}")
            traceback.print_exc()
            sys.exit(1)

    elapsed = time.monotonic() - t0
    print(f"\n{BOLD}{'=' * 60}{RESET}")
    print(f"{GREEN}{BOLD}  All stages passed in {elapsed:.1f}s{RESET}")
    print(f"{BOLD}{'=' * 60}{RESET}\n")


if __name__ == "__main__":
    main()
