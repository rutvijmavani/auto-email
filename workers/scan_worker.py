"""
workers/scan_worker.py — Adaptive Tier 1 listing scan worker (Phase 3).

Picks up scan payloads from SCAN_QUEUE via BLPOP (dispatched by
workers/scheduler.py), runs a listing-only fetch for the company,
diffs the returned job IDs against the Redis seen:{company} SET to find
only genuinely new IDs, then pushes them to queue:detail:adaptive for the
detail_worker to hydrate and filter.

─── Data flow ───────────────────────────────────────────────────────────────

    Scheduler → LPUSH scan:queue → [this worker] → LPUSH queue:detail:adaptive
                                                  → LPUSH scan:results

1. Scheduler: ZPOPMIN poll:adaptive → enqueue to scan:queue
2. scan_worker: BLPOP scan:queue
   a. Fetch listing via ats_module.fetch_jobs() (IDs + titles + metadata)
   b. Diff against seen:{company} Redis SET
   c. First scan: SADD all, save pre_existing rows, mark first_scanned_at
   d. Otherwise: for each new ID → INSERT pending_detail + LPUSH to detail queue
3. detail_worker: BRPOP queue:detail:adaptive → fetch detail → filter → save 'new'
4. scheduler.result_consumer_loop: BRPOP scan:results → on_adaptive_complete()

─── Scan payload ────────────────────────────────────────────────────────────

    {
        "company":     "Stripe",
        "scan_type":   "adaptive",
        "enqueued_at": "2026-04-27T07:00:00+00:00",
        "request_id":  "adp-1745728800"
    }

─── Result payload ──────────────────────────────────────────────────────────

    {
        "company":      "Stripe",
        "scan_type":    "adaptive",
        "request_id":   "adp-1745728800",
        "success":      true,
        "new_jobs":     3,
        "fetched":      47,
        "duration_ms":  1240,
        "worker_id":    "DESKTOP-ABC:12345",
        "completed_at": "2026-04-27T07:00:01+00:00"
    }

─── First-scan bootstrap ────────────────────────────────────────────────────

    When a company's first_scanned_at IS NULL, all jobs returned by the
    listing are "new" from the API's perspective but should be treated as
    pre_existing (they existed before we started monitoring). The worker:

      1. SADDs every returned job_id to seen:{company}
      2. Saves a minimal pre_existing row to DB (for Redis rebuild on restart)
      3. Marks first_scanned_at so subsequent scans do the incremental diff
      4. Returns new_jobs=0 (correct — no new jobs on first scan)

─── Usage ───────────────────────────────────────────────────────────────────

    python -m workers.scan_worker          # run forever
    python -m workers.scan_worker --once   # process one job then exit

    # Push a test job manually:
    python -c "
    import json, redis
    r = redis.from_url('redis://localhost:6379/0', decode_responses=True)
    r.lpush('scan:queue', json.dumps({
        'company': 'Airbnb',
        'scan_type': 'adaptive',
        'enqueued_at': '2026-04-27T07:00:00+00:00',
        'request_id': 'test-001',
    }))
    print('Job pushed')
    "
    python -m workers.scan_worker --once
"""

import json
import os
import socket
import sys
import time
from datetime import datetime, timezone

from logger import get_logger
from config import (
    SCAN_QUEUE,
    RESULT_CHANNEL,
    WORKER_BLOCK_SECS,
    REDIS_DETAIL_ADAPTIVE,
    REDIS_POLL_ADAPTIVE,
    DETAIL_QUEUE_MAX_ADAPTIVE,
    JOB_MONITOR_DAYS_FRESH,
    REDIS_BACKOFF_PREFIX,
    WORKER_BACKOFF_BASE_S,
    WORKER_BACKOFF_CAP_S,
    WORKER_BACKOFF_GIVEUP_S,
)
from workers.redis_client import get_redis, ping
from workers.scheduler import set_heartbeat, clear_heartbeat, set_progress
from workers.http_client import set_request_context
from workers.paginator import estimate_scan_depth
from jobs.ats_detector import get_ats_module
from jobs.ats.registry import get_config, parse_slug, should_fetch_detail
from jobs.job_filter import filter_jobs, filter_jobs_title_only
from db.db import init_db
from db.job_monitor import (
    get_company_row,
    upsert_poll_stats,
    save_pending_detail,
    save_pre_existing_listing,
    mark_first_scan_complete,
)

logger = get_logger(__name__)

WORKER_ID = f"{socket.gethostname()}:{os.getpid()}"

# Platform-specific semaphores from job_monitor are not used here — each
# adaptive scan_worker processes one company sequentially, so there is no
# within-worker concurrency pressure on the same ATS domain. Cross-worker
# concurrency is governed by scheduler dispatch rate (1 company per tick).


# ─────────────────────────────────────────
# EXPONENTIAL BACKOFF HELPER
# ─────────────────────────────────────────

def _get_backoff_delay(r, company: str, op_type: str) -> int:
    """
    Return the next exponential backoff delay for a company/operation and
    increment its retry counter.

    Delay schedule (WORKER_BACKOFF_BASE_S = 300):
        retry 0 (1st failure) → 300s
        retry 1               → 600s
        retry 2               → 1200s
        retry 3               → 2400s
        retry 4               → 3600s  (WORKER_BACKOFF_CAP_S)
        retry 5+              → 86400s (WORKER_BACKOFF_GIVEUP_S — skip today)

    The counter auto-expires after 86400s so every company starts fresh at
    the next cycle with no explicit reset at record_cycle_start().

    Args:
        r:        Redis client
        company:  company name (key component)
        op_type:  "scan" | "detail" | "fullscan"

    Returns:
        Delay in seconds to add to now before re-queueing.
    """
    key   = f"{REDIS_BACKOFF_PREFIX}:{op_type}:{company}"
    count = r.incr(key)                    # atomically increment; creates key at 1
    if count == 1:
        r.expire(key, 86400)               # first failure → set 24h TTL

    retry_count = count - 1               # 0-based retry index
    if retry_count >= 5:
        return WORKER_BACKOFF_GIVEUP_S    # give up for today

    return min(WORKER_BACKOFF_BASE_S * (2 ** retry_count), WORKER_BACKOFF_CAP_S)


# ─────────────────────────────────────────
# LISTING SCAN — core logic
# ─────────────────────────────────────────

def _run_listing_scan(payload: dict, shutdown_event=None) -> dict:
    """
    Perform one adaptive listing scan for a company.

    Fetches the job listing (IDs + metadata), diffs against the Redis
    seen:{company} SET, and pushes truly new jobs to queue:detail:adaptive.

    Returns a result dict (same schema as the old _run_one) — always
    well-formed. Never raises; exceptions become success=False.

    Args:
        payload:        decoded job dict from SCAN_QUEUE
        shutdown_event: multiprocessing.Event — if set mid-scan, the worker
                        discards partial results, re-queues the company with
                        exponential backoff, and returns success=False.
                        Checked after fetch_jobs() returns (boundary between
                        HTTP work and DB writes). Phase 7 will add per-page
                        checks once ATS modules expose page-level control.

    Returns:
        result dict with keys: company, scan_type, request_id, success,
        new_jobs, fetched, duration_ms, worker_id, completed_at
        (and optionally "error", "first_scan").
    """
    company    = payload.get("company", "")
    scan_type  = payload.get("scan_type", "adaptive")
    request_id = payload.get("request_id", "")

    start_mono = time.monotonic()
    r = get_redis()

    result: dict = {
        "company":      company,
        "scan_type":    scan_type,
        "request_id":   request_id,
        "success":      False,
        "new_jobs":     0,
        "fetched":      0,
        "duration_ms":  0,
        "worker_id":    WORKER_ID,
        "completed_at": datetime.now(timezone.utc).isoformat(),
    }

    if not company:
        logger.warning("scan_worker: empty company name in payload — skipping")
        return result

    try:
        # ── 1. Company metadata ───────────────────────────────────────────────
        company_row = get_company_row(company)
        if not company_row:
            logger.warning(
                "scan_worker [%s]: company %r not found in DB",
                request_id, company,
            )
            return result

        platform = company_row.get("ats_platform", "unknown")
        slug     = company_row.get("ats_slug")

        if platform == "unknown" or not slug:
            logger.warning(
                "scan_worker [%s]: %r has unknown ATS — skipping",
                request_id, company,
            )
            return result

        # ── 2. ATS module + config ────────────────────────────────────────────
        config     = get_config(platform)
        ats_module = get_ats_module(platform)
        if not ats_module:
            logger.error(
                "scan_worker [%s]: no ATS module for platform=%s company=%r",
                request_id, platform, company,
            )
            return result

        slug_info = parse_slug(platform, slug, config)
        if platform == "custom" and not isinstance(slug_info, dict):
            logger.error(
                "scan_worker [%s]: invalid custom slug JSON for %r",
                request_id, company,
            )
            return result

        # ── 3. Heartbeat ──────────────────────────────────────────────────────
        set_heartbeat(company)
        set_progress(company, "fetching_listing")

        logger.info(
            "scan_worker [%s] starting | company=%r platform=%s",
            request_id, company, platform,
        )

        # ── 4. Fetch listing ──────────────────────────────────────────────────
        # fetch_jobs() handles pagination internally and returns a flat list.
        # Phase 7 will refactor ATS modules to expose page-level control so
        # the paginator can trigger per-page shutdown checks.
        #
        # Phase 10 — api_health context tagging:
        # Set the thread-local request context so every ats_get() call made
        # inside fetch_jobs() writes the correct context to api_health.
        # Priority: canary (from payload) > backoff (active retry key) > normal.
        _payload_ctx = payload.get("context", "normal")
        if _payload_ctx == "canary":
            _scan_ctx = "canary"
        elif r.exists(f"{REDIS_BACKOFF_PREFIX}:scan:{company}"):
            _scan_ctx = "backoff"
        else:
            _scan_ctx = "normal"

        set_request_context(_scan_ctx)
        try:
            raw_jobs = ats_module.fetch_jobs(slug_info, company)
        finally:
            set_request_context("normal")   # always reset, even on exception

        # ── Shutdown checkpoint (post-fetch, pre-DB-write) ────────────────────
        # If the scheduler removed this worker due to errors while fetch_jobs()
        # was in-flight, discard partial results and re-queue with backoff.
        # This is the earliest safe point: HTTP work is done, no DB writes yet.
        if shutdown_event is not None and shutdown_event.is_set():
            delay = _get_backoff_delay(r, company, "scan")
            r.zadd(REDIS_POLL_ADAPTIVE, {company: time.time() + delay})
            logger.info(
                "scan_worker [%s]: shutdown mid-scan, re-queuing %r with +%ds backoff",
                request_id, company, delay,
            )
            duration_ms = int((time.monotonic() - start_mono) * 1000)
            result["duration_ms"] = duration_ms
            result["error"]       = "shutdown_mid_scan"
            clear_heartbeat(company)
            return result

        set_progress(company, "processing_results")

        # Drop entries missing job_url (uncheckable) or job_id (cannot dedup)
        valid_jobs = [
            j for j in raw_jobs
            if j.get("job_url") and j.get("job_id")
        ]
        dropped = len(raw_jobs) - len(valid_jobs)
        if dropped:
            logger.debug(
                "scan_worker [%s]: dropped %d jobs missing job_url/job_id",
                request_id, dropped,
            )

        result["fetched"] = len(valid_jobs)

        # ── 5. First-scan bootstrap ───────────────────────────────────────────
        is_first_scan = company_row.get("first_scanned_at") is None

        if is_first_scan:
            logger.info(
                "scan_worker [%s]: FIRST SCAN for %r — %d jobs "
                "(fresh ones will be queued, stale marked pre_existing)",
                request_id, company, len(valid_jobs),
            )
            fresh_count = _handle_first_scan(
                r, company, platform, valid_jobs,
                slug_info=slug_info,
                config=config,
                request_id=request_id,
            )
            mark_first_scan_complete(company)
            duration_ms = int((time.monotonic() - start_mono) * 1000)
            result.update({
                "success":     True,
                "new_jobs":    fresh_count,
                "duration_ms": duration_ms,
                "first_scan":  True,
                "completed_at": datetime.now(timezone.utc).isoformat(),
            })
            logger.info(
                "scan_worker [%s]: FIRST SCAN done | company=%r "
                "fetched=%d fresh_queued=%d pre_existing=%d",
                request_id, company,
                len(valid_jobs), fresh_count, len(valid_jobs) - fresh_count,
            )
            clear_heartbeat(company)
            return result

        # ── 6. Incremental diff ───────────────────────────────────────────────
        seen_key = f"seen:{company}"
        seen_ids = r.smembers(seen_key)   # returns set of strings

        # In-cycle dedup set — catches identical job_ids on multiple pages
        # when fetch_jobs() returns paginated data that has some overlap.
        cycle_seen: set = set()

        # Apply listing-level title filter to avoid queueing obviously
        # non-matching jobs (saves detail_worker effort):
        listing_filter = config.get("listing_filter", "full")
        if listing_filter == "title_only":
            title_matched = filter_jobs_title_only(valid_jobs)
        else:
            title_matched = filter_jobs(valid_jobs)

        new_count  = 0
        seen_count = 0

        # Backpressure: check detail queue depth before pushing
        queue_depth = r.llen(REDIS_DETAIL_ADAPTIVE)
        if queue_depth > DETAIL_QUEUE_MAX_ADAPTIVE:
            logger.warning(
                "scan_worker [%s]: detail queue backpressure "
                "(depth=%d > max=%d) — scan will proceed but "
                "new jobs may be delayed",
                request_id, queue_depth, DETAIL_QUEUE_MAX_ADAPTIVE,
            )

        for job in title_matched:
            job_id = job.get("job_id")
            if not job_id:
                continue

            # In-cycle dedup
            if job_id in cycle_seen:
                continue
            cycle_seen.add(job_id)

            # Already known in Redis
            if job_id in seen_ids:
                seen_count += 1
                continue

            # ── Genuinely new job ─────────────────────────────────────────────
            # Insert pending_detail row so it survives Redis restart
            inserted = save_pending_detail(company, platform, job)

            if inserted:
                # Push to detail queue for full hydration + filtering
                detail_payload = _build_detail_payload(
                    company, platform, job, slug_info,
                    request_id=request_id,
                    found_by="tier1_adaptive",
                )
                r.lpush(REDIS_DETAIL_ADAPTIVE, json.dumps(detail_payload))
                new_count += 1

            # Note: SADD to seen:{company} is done by detail_worker after
            # full detail is fetched and filters applied (per architecture
            # doc Section 17). This means a second adaptive poll before
            # detail completes could re-detect the same IDs — safe because
            # save_pending_detail uses ON CONFLICT DO NOTHING and the DB
            # remains consistent.

        # Log paginator efficiency (Phase 7 prep — full-page analysis)
        depth_stats = estimate_scan_depth(
            total_fetched=len(valid_jobs),
            new_found=new_count,
            early_exit=False,   # Phase 7 will set this based on actual exit
        )
        logger.info(
            "scan_worker [%s] done | company=%r platform=%s "
            "fetched=%d new=%d seen=%d waste=%.0f%%",
            request_id, company, platform,
            depth_stats["total_fetched"],
            depth_stats["new_found"],
            seen_count,
            depth_stats["waste_ratio"] * 100,
        )

        # ── 7. Update poll stats ──────────────────────────────────────────────
        duration_ms = int((time.monotonic() - start_mono) * 1000)
        upsert_poll_stats(company, platform, new_count, duration_ms)

        result.update({
            "success":      True,
            "new_jobs":     new_count,
            "duration_ms":  duration_ms,
            "completed_at": datetime.now(timezone.utc).isoformat(),
        })

    except Exception as exc:
        duration_ms = int((time.monotonic() - start_mono) * 1000)
        result["duration_ms"] = duration_ms
        result["error"]       = str(exc)
        logger.error(
            "scan_worker [%s] error for %r: %s",
            request_id, company, exc, exc_info=True,
        )

    clear_heartbeat(company)
    return result


# ─────────────────────────────────────────
# FIRST-SCAN HELPER
# ─────────────────────────────────────────

def _is_fresh_at_listing(job: dict) -> bool:
    """
    Return True if a job's posted_at is within JOB_MONITOR_DAYS_FRESH days.

    Used during first-scan bootstrap to decide whether a job should be
    queued for detail fetch (fresh) or treated as pre-existing (stale).

    Conservative: if posted_at is missing or unparseable → False (pre-existing).
    """
    from datetime import datetime, timezone, timedelta
    posted = job.get("posted_at")
    if not posted:
        return False

    try:
        if isinstance(posted, datetime):
            dt = posted
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
        else:
            # ISO string — handle both offset-aware and naive
            s = str(posted).strip()
            if s.endswith("Z"):
                s = s[:-1] + "+00:00"
            dt = datetime.fromisoformat(s)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
    except (ValueError, TypeError, AttributeError):
        return False

    cutoff = datetime.now(timezone.utc) - timedelta(days=JOB_MONITOR_DAYS_FRESH)
    return dt >= cutoff


def _handle_first_scan(
    r,
    company: str,
    platform: str,
    jobs: list,
    slug_info=None,
    config: dict = None,
    request_id: str = "",
) -> int:
    """
    Bootstrap a company's first scan with fresh/stale split.

    Fresh jobs (posted_at within JOB_MONITOR_DAYS_FRESH days):
        - Apply listing-level title filter
        - Pass  → INSERT pending_detail + LPUSH queue:detail:adaptive
        - Fail filter → treat as pre-existing (SADD + save_pre_existing)
        - detail_worker will SADD fresh jobs to seen:{company} on completion.

    Stale jobs (posted_at older than freshness window, or date missing):
        - SADD to seen:{company} immediately (bulk pipeline)
        - INSERT as pre_existing to DB (for rebuild_seen_ids on restart)

    Returns:
        Count of fresh jobs pushed to the detail queue.
    """
    seen_key       = f"seen:{company}"
    config         = config or {}
    listing_filter = config.get("listing_filter", "full")

    fresh_queued    = 0
    preexisting_ids: list = []

    for job in jobs:
        job_id  = job.get("job_id")
        job_url = job.get("job_url")
        if not job_id or not job_url:
            continue

        if _is_fresh_at_listing(job):
            # ── Fresh job: apply title filter, then queue for detail ──────────
            if listing_filter == "title_only":
                title_passed = filter_jobs_title_only([job])
            else:
                title_passed = filter_jobs([job])

            if title_passed:
                inserted = save_pending_detail(
                    company, platform, job, found_by="first_scan_fresh"
                )
                if inserted:
                    detail_payload = _build_detail_payload(
                        company, platform, job, slug_info,
                        request_id=request_id,
                        found_by="first_scan_fresh",
                    )
                    r.lpush(REDIS_DETAIL_ADAPTIVE, json.dumps(detail_payload))
                    fresh_queued += 1
                    # NOTE: SADD to seen:{company} is done by detail_worker
                    # after full detail + filters pass (Section 17).
            else:
                # Filtered by title → treat as pre-existing
                preexisting_ids.append(job_id)
                save_pre_existing_listing(company, platform, job)
        else:
            # ── Stale job: mark pre-existing immediately ──────────────────────
            preexisting_ids.append(job_id)
            save_pre_existing_listing(company, platform, job)

    # Bulk SADD all pre-existing IDs in one pipeline
    if preexisting_ids:
        pipe = r.pipeline()
        for i in range(0, len(preexisting_ids), 500):
            pipe.sadd(seen_key, *preexisting_ids[i:i + 500])
        pipe.execute()

    logger.debug(
        "_handle_first_scan [%s]: company=%r fresh_queued=%d pre_existing=%d",
        request_id, company, fresh_queued, len(preexisting_ids),
    )
    return fresh_queued


# ─────────────────────────────────────────
# DETAIL QUEUE PAYLOAD BUILDER
# ─────────────────────────────────────────

def _build_detail_payload(
    company: str,
    platform: str,
    job: dict,
    slug_info,
    request_id: str = "",
    found_by: str = "tier1_adaptive",
) -> dict:
    """
    Build the payload pushed to queue:detail:adaptive.

    Includes all listing-level data plus the slug_info needed by
    detail_worker to call fetch_job_detail() for Mode B platforms.

    The slug_info is JSON-serialized (it is either a str or a dict —
    both are JSON-serializable).
    """
    payload = {
        "company":     company,
        "ats_platform": platform,
        "job_id":      job.get("job_id"),
        "job_url":     job.get("job_url", ""),
        "title":       job.get("title", ""),
        "location":    job.get("location", ""),
        "posted_at":   (
            job["posted_at"].isoformat()
            if hasattr(job.get("posted_at"), "isoformat")
            else job.get("posted_at")
        ),
        "description":  job.get("description", ""),
        "content_hash": job.get("content_hash"),
        "skill_score":  job.get("skill_score", 0),
        "found_by":     found_by,
        "request_id":   request_id,
        "enqueued_at":  datetime.now(timezone.utc).isoformat(),
        # slug_info is stored for Mode B platforms that need fetch_job_detail()
        "slug_info": slug_info if isinstance(slug_info, (str, dict, type(None))) else str(slug_info),
    }

    # Forward platform-specific keys required by fetch_job_detail()
    PLATFORM_DETAIL_KEYS = {
        "workday":         ["_external_path"],
        "icims":           ["_base_url", "_feed_type"],
        "jobvite":         ["_slug"],
        "taleo":           ["_contest_no"],
        "smartrecruiters": ["_company_slug"],
        "sitemap":         ["_feed_type"],
        "avature":         [],
        "phenom":          [],
        "talentbrew":      [],
        "custom":          [],
        "eightfold":       [],
    }

    for key in PLATFORM_DETAIL_KEYS.get(platform, []):
        if job.get(key) is not None:
            payload[key] = job[key]

    # Country code available at listing level (Workday, SmartRecruiters, etc.)
    if job.get("_country_code"):
        payload["_country_code"] = job["_country_code"]

    return payload


# ─────────────────────────────────────────
# MAIN LOOP
# ─────────────────────────────────────────

def run_worker(once: bool = False, shutdown_event=None) -> None:
    """
    Main adaptive scan worker loop.

    BLPOPs payloads from SCAN_QUEUE (dispatched by scheduler.adaptive_loop),
    runs _run_listing_scan(), and LPUSHes the result onto RESULT_CHANNEL for
    the scheduler's result_consumer_loop → on_adaptive_complete().

    Args:
        once:           if True, process at most one job then exit.
        shutdown_event: multiprocessing.Event set by the scheduler when this
                        worker should stop after finishing its current job.
                        Checked at two points:
                          1. After BLPOP returns — before scan starts.
                             Company is re-queued with exponential backoff.
                          2. Inside _run_listing_scan() after fetch_jobs()
                             returns — before any DB writes are committed.
    """
    init_db()

    if not ping():
        redis_url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
        logger.error("scan_worker: Redis not reachable at %s — aborting", redis_url)
        print(f"[scan_worker] ERROR: Redis unreachable ({redis_url}). "
              "Is Memurai/Redis running?")
        sys.exit(1)

    r = get_redis()

    logger.info("scan_worker started | worker_id=%s queue=%s once=%s",
                WORKER_ID, SCAN_QUEUE, once)
    print(f"[scan_worker] Ready — worker={WORKER_ID}")
    print(f"[scan_worker] Listening on {SCAN_QUEUE!r}  "
          f"(results → {RESULT_CHANNEL!r})")
    if once:
        print("[scan_worker] --once mode: processing one job then exiting")
    else:
        print("[scan_worker] Press Ctrl+C to stop\n")

    while True:
        try:
            item = r.blpop(SCAN_QUEUE, timeout=WORKER_BLOCK_SECS)

            if item is None:
                # BLPOP timed out — check shutdown before looping
                if shutdown_event is not None and shutdown_event.is_set():
                    logger.info("scan_worker: shutdown event set (idle) — exiting")
                    break
                if once:
                    logger.info("scan_worker: --once, queue empty — exiting")
                    print("[scan_worker] Queue empty — exiting (--once)")
                    break
                continue

            _, raw = item

            # ── Shutdown checkpoint 1: after BLPOP, before scan starts ────────
            # Earliest safe point — company was just popped from SCAN_QUEUE.
            # Re-queue with exponential backoff so the platform gets breathing
            # room before the next attempt.
            if shutdown_event is not None and shutdown_event.is_set():
                try:
                    company = json.loads(raw).get("company", "")
                except Exception:
                    company = ""
                if company:
                    delay = _get_backoff_delay(r, company, "scan")
                    r.zadd(REDIS_POLL_ADAPTIVE, {company: time.time() + delay})
                    logger.info(
                        "scan_worker: shutdown (pre-scan), re-queuing %r "
                        "with +%ds backoff",
                        company, delay,
                    )
                break

            try:
                payload = json.loads(raw)
            except json.JSONDecodeError as exc:
                logger.error(
                    "scan_worker: bad JSON payload: %s | raw=%r",
                    exc, raw[:200],
                )
                if once:
                    break
                continue

            # Pass shutdown_event so the scan can self-abort mid-fetch
            result = _run_listing_scan(payload, shutdown_event=shutdown_event)

            # Publish completion event → scheduler.result_consumer_loop
            r.lpush(RESULT_CHANNEL, json.dumps(result))

            status = "OK" if result["success"] else "FAIL"
            first  = " [first-scan]" if result.get("first_scan") else ""
            print(f"  [{status}] {result['company']}{first} — "
                  f"{result['fetched']} fetched, {result['new_jobs']} new "
                  f"({result['duration_ms']}ms)")

            if once:
                break

        except KeyboardInterrupt:
            logger.info("scan_worker: KeyboardInterrupt — shutting down")
            print("\n[scan_worker] Shutting down.")
            break

        except Exception as exc:
            logger.error(
                "scan_worker: unexpected loop error: %s", exc, exc_info=True,
            )
            if once:
                break
            time.sleep(1)

    # Flush any pending api_health writes
    try:
        from db.api_health import flush as flush_api_health
        flush_api_health()
    except Exception:
        pass

    logger.info("scan_worker shutdown | worker_id=%s", WORKER_ID)


# ─────────────────────────────────────────
# CLI
# ─────────────────────────────────────────

if __name__ == "__main__":
    once = "--once" in sys.argv
    run_worker(once=once)
