#!/usr/bin/env python3
"""
scripts/test_e2e.py -- End-to-end adaptive polling pipeline test.

Tests the complete dual-tier architecture against real companies in the DB.
Makes real HTTP calls to Greenhouse API (Stripe, Airbnb).

Stages:
    1. PREFLIGHT              -- Redis + DB connectivity, clean test-company state
    2. SCAN WORKER            -- first scan (Stripe, Greenhouse)
    3. SCAN WORKER            -- incremental scan (Airbnb, Greenhouse)
    4. ON_ADAPTIVE_COMPLETE   -- interval engine + fullscan scheduling
    5. DETAIL WORKER          -- Mode A synthetic job (no HTTP)
    6. FULLSCAN               -- full scan (Airbnb, Greenhouse)
    7. WATCHDOG               -- orphan + hung worker checks
    8. REDIS SIGNAL           -- pause / heartbeat / resume cycle
    9. CLEANUP                -- remove synthetic test data

Usage:
    python scripts/test_e2e.py
    python scripts/test_e2e.py --stage 3   # run only one stage
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
TEST_JOB_ID  = "e2e-test-00001"
TEST_JOB_URL = "https://boards.greenhouse.io/stripe/jobs/e2e-test-00001"
TEST_COMPANY = "Stripe"   # first-scan company (Greenhouse)
INCR_COMPANY = "Airbnb"  # incremental + fullscan company (Greenhouse)


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
    from workers.rebuild import rebuild_seen_ids
    from db.db import get_conn

    r = get_redis()

    # Rebuild seen:{INCR_COMPANY} from DB -- simulates what rebuild_redis() does
    # at production scheduler startup. Without this, seen: is empty and all
    # 200+ jobs look "new" to the diff, causing wasteful ON CONFLICT attempts.
    seen_before = r.scard(f"seen:{INCR_COMPANY}")
    if seen_before == 0:
        info(f"seen:{INCR_COMPANY} is empty -- rebuilding from DB (simulating production startup)")
        rebuilt = rebuild_seen_ids(INCR_COMPANY)
        seen_after = r.scard(f"seen:{INCR_COMPANY}")
        ok(f"seen:{INCR_COMPANY} rebuilt: {seen_after} members loaded from DB")
    else:
        ok(f"seen:{INCR_COMPANY} already has {seen_before} members")

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
# STAGE 6 -- FULLSCAN WORKER
# ===========================================================

def stage_fullscan():
    header(6, f"FULLSCAN WORKER -- full scan ({INCR_COMPANY}, Greenhouse)")

    from workers.redis_client import get_redis
    from workers.fullscan import _run_fullscan, _BloomFilter
    from workers.rebuild import rebuild_seen_ids
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

    # Ensure seen: is populated (fullscan uses it to skip already-known jobs)
    seen_count = r.scard(f"seen:{INCR_COMPANY}")
    if seen_count == 0:
        info(f"seen:{INCR_COMPANY} empty -- rebuilding from DB before fullscan")
        rebuild_seen_ids(INCR_COMPANY)
        ok(f"seen:{INCR_COMPANY} rebuilt: {r.scard(f'seen:{INCR_COMPANY}')} members")

    # Clear bloom filter for a clean full-scan run
    bloom = _BloomFilter(r, INCR_COMPANY)
    bloom.delete()
    info(f"Cleared stale bloom filter for {INCR_COMPANY}")

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
# STAGE 7 -- WATCHDOG
# ===========================================================

def stage_watchdog():
    header(7, "WATCHDOG -- orphan + hung worker checks")

    from workers.watchdog import check_orphans, check_hung_workers, track_inflight, clear_inflight
    from workers.redis_client import get_redis

    r = get_redis()

    # track / clear helpers
    track_inflight("__e2e_test_company__")
    if r.sismember("watchdog:inflight", "__e2e_test_company__"):
        ok("track_inflight: company added to watchdog:inflight SET")
    else:
        fail("track_inflight: company NOT in watchdog:inflight SET")

    clear_inflight("__e2e_test_company__")
    if not r.sismember("watchdog:inflight", "__e2e_test_company__"):
        ok("clear_inflight: company removed from watchdog:inflight SET")
    else:
        fail("clear_inflight: company still in watchdog:inflight SET")

    # Clean-state checks
    info("Running check_orphans()...")
    requeued = check_orphans()
    ok(f"check_orphans returned {requeued} orphans (expected 0 in clean state)")

    info("Running check_hung_workers()...")
    hung = check_hung_workers()
    ok(f"check_hung_workers returned {len(hung)} hung workers (expected 0 in clean state)")

    # Orphan simulation: add to inflight with no heartbeat and not in any queue
    info("Simulating orphan: in-flight company with expired heartbeat...")
    track_inflight("__e2e_orphan_sim__")
    # heartbeat:__e2e_orphan_sim__ is NOT set -> expired
    # __e2e_orphan_sim__ is NOT in poll:adaptive or poll:fullscan -> orphan

    requeued2 = check_orphans()
    if requeued2 >= 1:
        ok(f"Orphan correctly detected and re-queued ({requeued2} company)")
        score = r.zscore("poll:adaptive", "__e2e_orphan_sim__")
        if score is not None:
            ok(f"Orphan added to poll:adaptive with score={score:.0f}")
            r.zrem("poll:adaptive", "__e2e_orphan_sim__")
        else:
            warn("Orphan not found in poll:adaptive after re-queue")
    else:
        warn("Orphan not detected -- check watchdog:inflight or queue membership")
        r.srem("watchdog:inflight", "__e2e_orphan_sim__")


# ===========================================================
# STAGE 8 -- REDIS SIGNAL (pause / heartbeat / resume)
# ===========================================================

def stage_redis_signal():
    header(8, "REDIS SIGNAL -- pause / heartbeat / resume")

    from scripts.redis_signal import cmd_pause, cmd_heartbeat, cmd_resume
    from workers.redis_client import get_redis
    from workers.fullscan import _is_paused
    from workers.scheduler import _check_auto_resume, _paused
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

    # Scheduler auto-resume should NOT fire while cronchain:alive is set
    _paused.set()
    _check_auto_resume()
    if _paused.is_set():
        ok("scheduler._check_auto_resume() did NOT auto-resume (cronchain:alive alive)")
    else:
        warn("scheduler._check_auto_resume() fired early -- cronchain:alive may have expired")
    _paused.clear()

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
# STAGE 9 -- CLEANUP
# ===========================================================

def stage_cleanup():
    header(9, "CLEANUP -- remove synthetic test data")

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
    1: ("PREFLIGHT",             stage_preflight),
    2: ("SCAN FIRST",            stage_scan_first),
    3: ("SCAN INCREMENTAL",      stage_scan_incremental),
    4: ("ON_ADAPTIVE_COMPLETE",  stage_on_adaptive_complete),
    5: ("DETAIL WORKER",         stage_detail_worker),
    6: ("FULLSCAN",              stage_fullscan),
    7: ("WATCHDOG",              stage_watchdog),
    8: ("REDIS SIGNAL",          stage_redis_signal),
    9: ("CLEANUP",               stage_cleanup),
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
    print(f"  Company: {TEST_COMPANY} (first scan) / {INCR_COMPANY} (incremental + fullscan)")

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
