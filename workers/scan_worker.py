"""
workers/scan_worker.py — Adaptive Tier 1 listing scan worker (Phase 3).

Picks up scan payloads from SCAN_QUEUE via BLPOP (dispatched by
workers/scheduler.py), runs a listing-only fetch for the company, and
pushes genuinely new job IDs to queue:detail:adaptive for the detail_worker
to hydrate and filter.

─── Data flow ───────────────────────────────────────────────────────────────

    Scheduler: ZRANGEBYSCORE poll:adaptive → XADD stream:adaptive → ZREM
    [this worker]: XREADGROUP stream:adaptive → on_adaptive_complete() → XACK
                                              → LPUSH queue:detail:adaptive

1. Scheduler adaptive_loop():
       ZRANGEBYSCORE poll:adaptive → XADD stream:adaptive → ZREM
       (non-destructive: crash between XADD and ZREM = harmless duplicate)
2. scan_worker: XREADGROUP stream:adaptive BLOCK 500ms
   a. Fetch listing via ats_module.fetch_jobs() (IDs + titles + metadata)
   b. Bloom filter early exit check (sorted platforms, Phase 7+)
   c. For each job ID: check adaptive_seen:{company} SET → skip if seen today
   d. DB lookup for unseen IDs → queue new ones for detail fetch
   e. Add all processed IDs to adaptive_seen:{company} SET
   f. First scan: save pre_existing rows, mark first_scanned_at
   g. Call on_adaptive_complete() inline (reschedule + WARMING check)
   h. XACK stream message (remove from PEL)
3. detail_worker: BRPOP queue:detail:adaptive → fetch detail → filter → save 'new'
4. Crash recovery: XAUTOCLAIM (scheduler) reclaims PEL messages after p95×3ms

─── Stream message fields ───────────────────────────────────────────────────

    company      = "Stripe"
    scan_type    = "adaptive"
    dc_key       = "greenhouse"
    context      = "normal" | "canary" | "backoff"
    enqueued_at  = "2026-04-27T07:00:00+00:00"
    request_id   = "adp-1745728800"

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

      1. SADDs every returned job_id to adaptive_seen:{company} SET
      2. Saves a minimal pre_existing row to DB (source of truth)
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
import copy
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
    REDIS_ADAPTIVE_SEEN_PREFIX,
    ADAPTIVE_SEEN_TTL,
    # Stream-based delivery (two-layer scheduler redesign)
    REDIS_STREAM_ADAPTIVE,
    STREAM_CONSUMER_GROUP,
    STREAM_BLOCK_MS,
)
from workers.redis_client import get_redis, ping
from workers.heartbeat import Heartbeat
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

WORKER_ID      = f"{socket.gethostname()}:{os.getpid()}"
_CONSUMER_NAME = f"worker-{socket.gethostname()}-{os.getpid()}"

# Platform-specific semaphores from job_monitor are not used here — each
# adaptive scan_worker processes one company sequentially, so there is no
# within-worker concurrency pressure on the same ATS domain. Cross-worker
# concurrency is governed by scheduler dispatch rate (1 company per tick).


# ─────────────────────────────────────────
# STREAM CONSUMER GROUP INIT
# ─────────────────────────────────────────

def _ensure_consumer_group(r) -> None:
    """
    Ensure the consumer group exists for stream:adaptive (idempotent).

    Uses id='$' so only NEW messages are delivered — never replays history.
    BUSYGROUP means the group already exists; MKSTREAM creates the stream if
    it doesn't exist yet (safe to call repeatedly at worker startup).
    """
    try:
        r.xgroup_create(
            REDIS_STREAM_ADAPTIVE, STREAM_CONSUMER_GROUP,
            id="$", mkstream=True,
        )
    except Exception as exc:
        if "BUSYGROUP" not in str(exc):
            logger.warning("scan_worker: xgroup_create error: %s", exc)
            raise


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

    Fetches the job listing (IDs + metadata), checks adaptive_seen:{company}
    and DB for new jobs, and pushes them to queue:detail:adaptive.

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
        _slug_info_before = copy.deepcopy(slug_info) if isinstance(slug_info, dict) else None
        try:
            raw_jobs = ats_module.fetch_jobs(slug_info, company)
        finally:
            set_request_context("normal")   # always reset, even on exception

        # ── Persist slug_info mutations made in-place by ATS modules ──────────
        # e.g. talentbrew auto-detects the live tenant_id from the sitemap and
        # updates slug_info["tenant_id"] in-place.  Without this write-back the
        # corrected value is lost when the process exits and every future scan
        # re-discovers the same mismatch.
        if _slug_info_before is not None and slug_info != _slug_info_before:
            from db.connection import get_conn as _get_conn
            _conn = None
            try:
                _conn = _get_conn()
                _conn.execute(
                    "UPDATE prospective_companies SET ats_slug = ? WHERE company = ?",
                    (json.dumps(slug_info), company),
                )
                _conn.commit()
                logger.info(
                    "scan_worker [%s]: persisted updated slug_info for %r "
                    "(changed keys: %s)",
                    request_id, company,
                    sorted(k for k in slug_info if slug_info.get(k) != _slug_info_before.get(k)),
                )
            except Exception as _slug_exc:
                logger.warning(
                    "scan_worker [%s]: failed to persist updated slug_info "
                    "for %r: %s",
                    request_id, company, _slug_exc,
                )
            finally:
                if _conn is not None:
                    _conn.close()

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
        adaptive_seen_key = f"{REDIS_ADAPTIVE_SEEN_PREFIX}:{company}"

        # In-cycle dedup — catches identical job_ids on multiple pages
        # when fetch_jobs() returns paginated data that has some overlap.
        cycle_seen: set = set()

        # Apply listing-level title filter to avoid queueing obviously
        # non-matching jobs (saves detail_worker effort):
        listing_filter = config.get("listing_filter", "full")
        if listing_filter == "title_only":
            title_matched = filter_jobs_title_only(valid_jobs)
        else:
            title_matched = filter_jobs(valid_jobs)

        new_count        = 0
        adaptive_skipped = 0

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

            # Layer 1: in-cycle dedup (pagination overlap within this fetch)
            if job_id in cycle_seen:
                continue
            cycle_seen.add(job_id)

            # Layer 2: adaptive_seen cache — already processed in an earlier
            # adaptive scan today (DB lookup already done, outcome recorded).
            # Skip entirely — no DB round-trip needed.
            if r.sismember(adaptive_seen_key, job_id):
                adaptive_skipped += 1
                continue

            # Layer 3: DB check — source of truth.
            # save_pending_detail returns True only if the row did not exist.
            inserted = save_pending_detail(company, platform, job)

            if inserted:
                # Genuinely new job — push to detail queue for full hydration
                detail_payload = _build_detail_payload(
                    company, platform, job, slug_info,
                    request_id=request_id,
                    found_by="tier1_adaptive",
                )
                r.lpush(REDIS_DETAIL_ADAPTIVE, json.dumps(detail_payload))
                new_count += 1

            # Mark as processed in adaptive_seen regardless of outcome so
            # subsequent adaptive scans today skip the DB lookup entirely.
            r.sadd(adaptive_seen_key, job_id)

        # Refresh adaptive_seen TTL after each adaptive scan so it stays alive
        # until the next full scan (which will DEL it explicitly).
        r.expire(adaptive_seen_key, ADAPTIVE_SEEN_TTL)

        # Log paginator efficiency (Phase 7 prep — full-page analysis)
        depth_stats = estimate_scan_depth(
            total_fetched=len(valid_jobs),
            new_found=new_count,
            early_exit=False,   # Phase 7 will set this based on actual exit
        )
        logger.info(
            "scan_worker [%s] done | company=%r platform=%s "
            "fetched=%d new=%d adaptive_skipped=%d waste=%.0f%%",
            request_id, company, platform,
            depth_stats["total_fetched"],
            depth_stats["new_found"],
            adaptive_skipped,
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
        - Fail filter → treat as pre-existing (save_pre_existing + adaptive_seen)

    Stale jobs (posted_at older than freshness window, or date missing):
        - INSERT as pre_existing to DB (source of truth)
        - SADD to adaptive_seen:{company} so subsequent adaptive scans today
          skip the DB lookup for these already-processed IDs

    Returns:
        Count of fresh jobs pushed to the detail queue.
    """
    adaptive_seen_key = f"{REDIS_ADAPTIVE_SEEN_PREFIX}:{company}"
    config            = config or {}
    listing_filter    = config.get("listing_filter", "full")

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
                # Always mark in adaptive_seen — repeat adaptive scans today
                # must not re-check this job regardless of whether it was a
                # new DB insert (inserted=True) or a duplicate (inserted=False).
                preexisting_ids.append(job_id)
            else:
                # Filtered by title → treat as pre-existing
                preexisting_ids.append(job_id)
                save_pre_existing_listing(company, platform, job)
        else:
            # ── Stale job: mark pre-existing immediately ──────────────────────
            preexisting_ids.append(job_id)
            save_pre_existing_listing(company, platform, job)

    # Bulk SADD all processed IDs to adaptive_seen (saves DB lookups today)
    if preexisting_ids:
        pipe = r.pipeline()
        for i in range(0, len(preexisting_ids), 500):
            pipe.sadd(adaptive_seen_key, *preexisting_ids[i:i + 500])
        pipe.execute()
        r.expire(adaptive_seen_key, ADAPTIVE_SEEN_TTL)

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

    # Forward platform-specific keys required by fetch_job_detail().
    #
    # Rule: forward EVERY key that fetch_job_detail() reads from the job dict.
    # Omitting a required key causes a silent fail — the guard clause returns
    # the original job unchanged with empty location / description.
    #
    # Workday:  _slug + _wd + _path + _external_path are ALL checked by the
    #           guard: if not all([slug, wd, path, external_path]): return job
    #           _site is optional (myworkdaysite.com tenants).
    #
    # Taleo:    _base_url + _contest_no are BOTH checked by the guard:
    #           if not base_url or not contest_no: return job
    #           _section defaults to "ex" but some tenants use a different
    #           section code — forward it to preserve the per-tenant value.
    #
    # iCIMS:    guard is only `if not job_url` (job_url is in the base
    #           payload). _base_url and _feed_type are forwarded as extras;
    #           _base_url doubles as the should_fetch_detail() gate key.
    #
    # Jobvite:  fetch_job_detail only reads job_url (base payload). _slug is
    #           forwarded solely because should_fetch_detail() gates on it.
    #
    # SmartRecruiters: guard checks job_id (base) + _company_slug. ✓
    # Sitemap:  _feed_type gate: if _feed_type=="xml" return job (skip). ✓
    # Avature/Phenom/TalentBrew/Eightfold: guard is only `if not job_url`. ✓
    PLATFORM_DETAIL_KEYS = {
        "workday":         ["_external_path", "_slug", "_wd", "_path", "_site"],
        "icims":           ["_base_url", "_feed_type"],
        "jobvite":         ["_slug"],
        "taleo":           ["_base_url", "_contest_no", "_section"],
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

def run_worker(once: bool = False, shutdown_event=None,
               skip_init_db: bool = False) -> None:
    """
    Main adaptive scan worker loop — stream-based delivery (Section 5 redesign).

    Reads scan payloads from stream:adaptive via XREADGROUP (crash-safe):
        1. XREADGROUP COUNT 1 BLOCK 500ms — get next undelivered message
        2. Check shutdown / pause before starting scan
        3. Run _run_listing_scan() → result dict
        4. Call on_adaptive_complete() inline (replaces result_consumer_loop)
        5. XACK — remove from PEL, mark work complete

    If the worker dies between step 3 and step 5, the message stays in the PEL.
    The scheduler's claim_stale_work() (XAUTOCLAIM with p95×3 idle timeout)
    reclaims it and retries, up to MAX_STREAM_REDELIVERIES times before
    dead-lettering to poll:adaptive with exponential backoff.

    Shutdown:
        shutdown_event fired between steps: stop after current job without XACK
        (message stays in PEL for XAUTOCLAIM reclaim — no data loss).
        shutdown_event fired during _run_listing_scan: post-fetch checkpoint
        in _run_listing_scan re-queues with backoff then returns success=False.
        We still XACK in this case because re-queuing to poll:adaptive means
        the scheduler will re-dispatch without relying on PEL reclaim.

    Args:
        once:           if True, process at most one job then exit.
        shutdown_event: multiprocessing.Event — set by scheduler to request stop.
        skip_init_db:   if True, skip init_db() (parent process already did it).
    """
    if not skip_init_db:
        init_db()

    # ── Startup validation (Redis + PostgreSQL + required config) ────────────
    from workers.startup import validate_startup
    validate_startup("scan_worker",
                     check_redis=True,
                     check_db=not skip_init_db,   # DB already checked by caller if skip_init_db
                     check_config=True)

    r = get_redis()
    _ensure_consumer_group(r)

    logger.info(
        "scan_worker started | worker_id=%s consumer=%s stream=%s once=%s",
        WORKER_ID, _CONSUMER_NAME, REDIS_STREAM_ADAPTIVE, once,
    )
    print(f"[scan_worker] Ready — worker={WORKER_ID}")
    print(f"[scan_worker] Consuming from {REDIS_STREAM_ADAPTIVE!r} "
          f"group={STREAM_CONSUMER_GROUP!r}")
    if once:
        print("[scan_worker] --once mode: processing one job then exiting")
    else:
        print("[scan_worker] Press Ctrl+C to stop\n")

    # ── Background heartbeat ─────────────────────────────────────────────────
    # Daemon thread writes worker:alive:scan_worker every 10s, independent of
    # how long each listing scan takes (Workday scans can exceed 60s).
    # daemon=True means the thread dies with the process — no ghost heartbeats.
    _hw = {"count": 0}
    _hb = Heartbeat(r, "scan_worker", lambda: _hw["count"]).start()

    while True:
        try:
            # ── Shutdown check (idle) ─────────────────────────────────────────
            if shutdown_event is not None and shutdown_event.is_set():
                logger.info("scan_worker: shutdown event set (idle) — exiting")
                break

            # ── XREADGROUP: block up to STREAM_BLOCK_MS for next message ──────
            # Short block (500ms) so we check shutdown_event ~2× per second.
            # id=">" = only new undelivered messages (not PEL retries).
            stream_result = r.xreadgroup(
                STREAM_CONSUMER_GROUP,
                _CONSUMER_NAME,
                {REDIS_STREAM_ADAPTIVE: ">"},
                count=1,
                block=STREAM_BLOCK_MS,
            )

            if not stream_result:
                # Timeout — no messages available
                if once:
                    logger.info("scan_worker: --once, stream empty — exiting")
                    print("[scan_worker] Stream empty — exiting (--once)")
                    break
                continue

            # Unpack: [(stream_name, [(msg_id, fields_dict), ...])]
            _stream_name, messages = stream_result[0]
            msg_id, fields = messages[0]

            company      = fields.get("company", "")
            dc_key       = fields.get("dc_key", "unknown")
            scan_context = fields.get("context", "normal")
            enqueued_at  = fields.get("enqueued_at", "")
            request_id   = fields.get("request_id", f"adp-{int(time.time())}")

            # ── Shutdown checkpoint (after XREADGROUP, before scan) ───────────
            # Message is in PEL — don't XACK. scheduler's XAUTOCLAIM will
            # reclaim it after idle_ms and retry. No data loss.
            if shutdown_event is not None and shutdown_event.is_set():
                logger.info(
                    "scan_worker: shutdown (pre-scan) for company=%r — "
                    "leaving in PEL for XAUTOCLAIM reclaim",
                    company,
                )
                break

            payload = {
                "company":     company,
                "scan_type":   "adaptive",
                "request_id":  request_id,
                "dc_key":      dc_key,
                "context":     scan_context,
                "enqueued_at": enqueued_at,
            }

            # ── Run listing scan ──────────────────────────────────────────────
            result = _run_listing_scan(payload, shutdown_event=shutdown_event)

            # ── Inline completion handler (replaces result_consumer_loop) ─────
            # Guards:
            #   • Empty/missing company → malformed message; skip OAC + XACK
            #     so it stays in PEL for XAUTOCLAIM (prevents DB corruption).
            #   • shutdown_mid_scan / requeued → work was re-queued elsewhere;
            #     leave in PEL so the requeued entry is the canonical one.
            # Normal path: call OAC then XACK. If OAC raises, leave in PEL
            # (OAC is idempotent — XAUTOCLAIM retry is safe).
            if not company:
                logger.warning(
                    "scan_worker: stream message %r has empty company field — "
                    "leaving in PEL for XAUTOCLAIM reclaim",
                    msg_id,
                )
            elif result.get("error") == "shutdown_mid_scan" or result.get("requeued"):
                logger.info(
                    "scan_worker: %r result=%s — skipping OAC, leaving in PEL",
                    company, result.get("error") or "requeued",
                )
            else:
                try:
                    from workers.scheduler import on_adaptive_complete
                    on_adaptive_complete(
                        company,
                        result.get("new_jobs", 0),
                        success=result.get("success", False),
                    )
                    # ── XACK: remove from PEL (work complete) ────────────────
                    # Only XACK on success — if on_adaptive_complete raises, the
                    # message stays in PEL for XAUTOCLAIM to retry (idempotent).
                    r.xack(REDIS_STREAM_ADAPTIVE, STREAM_CONSUMER_GROUP, msg_id)
                except Exception as oac_exc:
                    logger.error(
                        "scan_worker: on_adaptive_complete failed for %r: %s — "
                        "leaving in PEL for XAUTOCLAIM retry",
                        company, oac_exc, exc_info=True,
                    )

            _hw["count"] += 1
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
            _hb.stop()
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
