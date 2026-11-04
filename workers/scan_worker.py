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
   f. First scan (first_scanned_at NULL): route to poll:fullscan, no scan here
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

─── First scan = fullscan ───────────────────────────────────────────────────

    When a company's first_scanned_at IS NULL the worker does not scan: it
    ZADDs the company to poll:fullscan (NX) and returns new_jobs=0 with
    routed_to_fullscan=True. The fullscan path (old bloom empty, date gate,
    title/location filter) handles the first board and sets first_scanned_at.

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
import redis.exceptions
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
    REDIS_POLL_FULLSCAN,
    REDIS_BACKOFF_PREFIX,
    WORKER_BACKOFF_BASE_S,
    WORKER_BACKOFF_CAP_S,
    WORKER_BACKOFF_GIVEUP_S,
    REDIS_ADAPTIVE_SEEN_PREFIX,
    ADAPTIVE_SEEN_TTL,
    ADAPTIVE_EARLY_EXIT_ENABLED,
    # Stream-based delivery (two-layer scheduler redesign)
    REDIS_STREAM_ADAPTIVE,
    STREAM_CONSUMER_GROUP,
    STREAM_BLOCK_MS,
    REDIS_INFLIGHT_PREFIX,
)
from workers.redis_client import get_redis, ping
from workers.heartbeat import BusyMeter, Heartbeat
from workers.queue_stamp import count_arrival
from workers import progress
from workers.scheduler import set_heartbeat, clear_heartbeat, set_progress
from workers.http_client import set_request_context, CeilingExceeded
from workers.paginator import EarlyExitGate, estimate_scan_depth, make_seen_check
from jobs.ats_detector import get_ats_module
from jobs.ats.registry import accepts_stop_check, get_config, parse_slug, should_fetch_detail
from jobs.ats.avature import IncompleteSearchError
from jobs.ats.fetch_report import STOP_EARLY_EXIT, STOP_REQUEST_FAILED, report_of
from jobs.job_filter import filter_jobs, filter_jobs_title_only, passes_listing_date
from db.db import init_db
from db.job_monitor import (
    get_company_row,
    is_company_row_active,
    upsert_poll_stats,
    save_pending_detail,
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

    return backoff_delay_for(count - 1)   # 0-based retry index


def backoff_delay_for(retry_count: int) -> int:
    """Backoff delay (s) for a 0-based retry index; retry 5+ gives up for today."""
    if retry_count >= 5:
        return WORKER_BACKOFF_GIVEUP_S

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

        # Disabled (or marked stale) after it was queued: flag it so the main
        # loop skips on_adaptive_complete (which would reschedule it forever).
        if not is_company_row_active(company_row):
            logger.info(
                "scan_worker [%s]: %r no longer monitored (disabled/stale) — skipping",
                request_id, company,
            )
            result["disabled"] = True
            return result

        # display_name is the real company name written to job_postings.company.
        # For ca:{id} routing keys the row now carries company_name from company_ats;
        # for prospective_companies rows company_name is absent, fall back to company.
        display_name = company_row.get("company_name") or company

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
        _fetch_complete = True

        # Early-exit gate: a page whose every job is already handled (old fullscan bloom OR this
        # cycle's adaptive_seen) counts toward a stop; any unseen job resets the count. No old bloom
        # (never fullscanned, or expired) means nothing to compare against, so no early exit.
        adaptive_seen_key = f"{REDIS_ADAPTIVE_SEEN_PREFIX}:{company}"
        _gate = None
        if (ADAPTIVE_EARLY_EXIT_ENABLED and accepts_stop_check(ats_module)
                and company_row.get("first_scanned_at") is not None):
            from workers.fullscan import _BloomPair
            _bloom = _BloomPair(r, company)
            if _bloom.old_present():
                _gate = EarlyExitGate(make_seen_check(
                    _bloom.old_exists,
                    lambda jid: bool(r.sismember(adaptive_seen_key, jid)),
                ))
        try:
            if _gate is not None:
                raw_jobs = ats_module.fetch_jobs(slug_info, company, stop_check=_gate)
            else:
                raw_jobs = ats_module.fetch_jobs(slug_info, company)
        except CeilingExceeded:
            # Platform at concurrency ceiling — reschedule via poll:adaptive
            # with a 30s delay. Leave stream message in PEL (no XACK) so
            # XAUTOCLAIM reclaims it if this worker dies before the ZADD.
            # result["requeued"] = True triggers the "leave in PEL" path in
            # the main loop (line 944 check).
            r.zadd("poll:adaptive", {company: time.time() + 30})
            result["requeued"] = True
            logger.debug(
                "scan_worker [%s]: CeilingExceeded for %r — "
                "rescheduled poll:adaptive +30s, leaving in PEL",
                request_id, company,
            )
            return result
        except IncompleteSearchError as exc:
            if not exc.stubs:
                raise
            logger.warning(
                "scan_worker [%s]: avature partial fetch for %r — %d stubs "
                "(HTTP failure mid-pagination)",
                request_id, company, len(exc.stubs),
            )
            raw_jobs = exc.stubs
            _fetch_complete = False
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
                if company.startswith("ca:"):
                    try:
                        _ca_id = int(company[3:])
                    except ValueError:
                        raise ValueError(f"malformed ca: key: {company!r}")
                    _conn.execute(
                        "UPDATE company_ats SET slug = %s WHERE id = %s",
                        (json.dumps(slug_info), _ca_id),
                    )
                else:
                    _conn.execute(
                        "UPDATE prospective_companies SET ats_slug = %s WHERE company = %s",
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

        # Completeness verdict of this fetch (jobs/ats/fetch_report.py) — recorded, and an
        # incomplete fetch must not mark a first scan done.
        _report = report_of(raw_jobs)
        if not _fetch_complete:
            _report.complete = False
            _report.stop_reason = STOP_REQUEST_FAILED
            _report.note = _report.note or "avature partial fetch"
        elif _report.complete is False:
            _fetch_complete = False
        # Not recorded to scan_health: the completeness verdict comes from fullscan only, and adaptive
        # scans run constantly — their rows would feed nothing.

        # ── 5. Never-fullscanned company → fullscan ───────────────────────────
        # A first scan IS a fullscan (old bloom empty, so every listing job goes
        # through date + title gates and is queued). Adaptive does no bootstrap
        # of its own: hand the company to the fullscan queue and stop.
        if company_row.get("first_scanned_at") is None:
            r.zadd(REDIS_POLL_FULLSCAN, {company: time.time()}, nx=True)
            logger.info(
                "scan_worker [%s]: %r never fullscanned — routed to fullscan "
                "(adaptive fetched %d jobs, none processed)",
                request_id, company, len(valid_jobs),
            )
            result.update({
                "success":      True,
                "new_jobs":     0,
                "duration_ms":  int((time.monotonic() - start_mono) * 1000),
                "routed_to_fullscan": True,
                "completed_at": datetime.now(timezone.utc).isoformat(),
            })
            clear_heartbeat(company)
            return result

        # ── 6. Incremental diff ───────────────────────────────────────────────
        early_exited = _report.stop_reason == STOP_EARLY_EXIT

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

        matched_ids      = {j.get("job_id") for j in title_matched}
        new_count        = 0
        adaptive_skipped = 0
        date_skipped     = 0
        filter_skipped   = 0

        for job in valid_jobs:
            job_id = job.get("job_id")

            # Layer 1: in-cycle dedup (pagination overlap within this fetch)
            if job_id in cycle_seen:
                continue
            cycle_seen.add(job_id)

            # Layer 2: adaptive_seen — handled by an earlier adaptive scan
            # since the last fullscan. Skip, no DB round-trip.
            if r.sismember(adaptive_seen_key, job_id):
                adaptive_skipped += 1
                continue

            # Layer 3: date gate (platforms whose listing date is the posting
            # date only), then title/location filter. Rejected jobs are marked
            # handled so they are not re-evaluated every scan.
            if not passes_listing_date(job, platform):
                r.sadd(adaptive_seen_key, job_id)
                date_skipped += 1
                continue
            if job_id not in matched_ids:
                r.sadd(adaptive_seen_key, job_id)
                filter_skipped += 1
                continue

            # Layer 4: DB check — validate payload first to avoid orphaned DB
            # records (a job inserted but never queued can never be re-queued).
            # A DB error raises out of save_pending_detail and aborts the scan;
            # a payload failure leaves the ID unmarked so it retries.
            try:
                detail_payload = _build_detail_payload(
                    display_name, platform, job, slug_info,
                    request_id=request_id,
                    found_by="tier1_adaptive",
                )
            except ValueError:
                logger.error(
                    "scan_worker: _build_detail_payload raised — job skipped "
                    "to avoid orphaned DB record. "
                    "company=%r platform=%s job_id=%s",
                    company, platform, job.get("job_id"),
                    exc_info=True,
                )
            else:
                if save_pending_detail(display_name, platform, job,
                                       detail_payload=detail_payload):
                    # Save only: the detail drainer releases the held row to the
                    # queue at the bucket rate (pushed_at NULL = held).
                    count_arrival(r, REDIS_DETAIL_ADAPTIVE)
                    new_count += 1
                # New or already known (DB decided): handled either way.
                r.sadd(adaptive_seen_key, job_id)

        # Refresh adaptive_seen TTL after each adaptive scan so it stays alive
        # until the next full scan (which will DEL it explicitly).
        r.expire(adaptive_seen_key, ADAPTIVE_SEEN_TTL)

        # Log paginator efficiency
        depth_stats = estimate_scan_depth(
            total_fetched=len(valid_jobs),
            new_found=new_count,
            early_exit=early_exited,
        )
        logger.info(
            "scan_worker [%s] done | company=%r platform=%s "
            "fetched=%d new=%d adaptive_skipped=%d date_skipped=%d "
            "filter_skipped=%d waste=%.0f%% early_exit=%s pages=%d",
            request_id, company, platform,
            depth_stats["total_fetched"],
            depth_stats["new_found"],
            adaptive_skipped, date_skipped, filter_skipped,
            depth_stats["waste_ratio"] * 100,
            early_exited, _report.pages,
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
        "paradox":         [],   # host is derived from job_url
    }

    for key in PLATFORM_DETAIL_KEYS.get(platform, []):
        # Use truthiness (not `is not None`) so that empty strings are treated as
        # absent.  fetch_job_detail() guard clauses use `not all([...])` which
        # treats empty strings as missing — forwarding "" would silently trigger
        # the guard and return the job unenriched.
        if job.get(key):
            payload[key] = job[key]

    # Country code available at listing level (Workday, SmartRecruiters, etc.)
    if job.get("_country_code"):
        payload["_country_code"] = job["_country_code"]

    # Validate that keys required by fetch_job_detail()'s guard clauses made it
    # into the payload.  A missing required key causes the guard to return the
    # job unchanged with no HTTP request — raising here produces a stack trace
    # (via exc_info=True at the caller) instead of a silent bad-enrichment.
    _REQUIRED = {
        "workday":         ["_external_path", "_slug", "_wd", "_path"],
        "taleo":           ["_base_url", "_contest_no"],
        "smartrecruiters": ["_company_slug"],
        "icims":           ["_base_url"],
        "jobvite":         ["_slug"],
    }
    _missing_required = [k for k in _REQUIRED.get(platform, []) if not payload.get(k)]
    if _missing_required:
        raise ValueError(
            f"detail payload missing required keys for {company!r}/{platform} "
            f"job_id={job.get('job_id')} missing={_missing_required} "
            f"raw_underscore_keys={[k for k in job if k.startswith('_')]}"
        )

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
    from workers.sentry_init import init_sentry
    if not init_sentry():
        logger.warning("scan_worker: Sentry not initialized — SENTRY_DSN absent or invalid")

    # ── Startup validation (Redis + PostgreSQL + required config) ────────────
    # Run before init_db so config/connectivity issues are caught before any
    # schema initialization work.
    from workers.startup import validate_startup
    validate_startup("scan_worker",
                     check_redis=True,
                     check_db=True,
                     check_config=True)

    if not skip_init_db:
        init_db()

    r = get_redis()
    _ensure_consumer_group(r)

    # Signal manager that this worker is now online (reduces pending_spawns counter)
    try:
        _ps_key = "manager:pool:scan:pending_spawns"
        if r.exists(_ps_key):
            if r.decr(_ps_key) < 0:
                r.set(_ps_key, 0)
    except Exception:
        pass

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
    # Busy time for the manager's utilization signal rides in the heartbeat:
    # _busy.begin()/end() bracket each listing scan.
    _hw = {"count": 0}
    _busy = BusyMeter()
    _hb = Heartbeat(r, "scan_worker", lambda: _hw["count"], busy_meter=_busy).start()
    # Progress reports (worker:cur:{consumer}): checkpoints installed at the
    # transport layer, so no ATS module needs to know about them.
    progress.bind(r, _CONSUMER_NAME)
    progress.install()

    while True:
        try:
            progress.end()   # idempotent: clears the previous message on every path
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
            progress.begin(msg_id, dc_key)

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
            _busy.begin()
            try:
                result = _run_listing_scan(payload, shutdown_event=shutdown_event)
            finally:
                _busy.end()

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
            elif result.get("disabled"):
                # Company was disabled after dispatch: finish the delivery without
                # on_adaptive_complete (it would reschedule the company). Release the
                # per-DC inflight slot the dispatcher claimed; the scheduler's
                # reconcile sweep clears the rest of its state.
                try:
                    r.zrem(f"{REDIS_INFLIGHT_PREFIX}:{dc_key}", company)
                    r.xack(REDIS_STREAM_ADAPTIVE, STREAM_CONSUMER_GROUP, msg_id)
                except Exception as dis_exc:
                    logger.error(
                        "scan_worker: could not close delivery for disabled %r: %s — "
                        "leaving in PEL",
                        company, dis_exc, exc_info=True,
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

            progress.end()
            _hw["count"] += 1
            status = "OK" if result["success"] else "FAIL"
            first  = " [first-scan]" if result.get("first_scan") else ""
            logger.info(
                "[%s] %s%s — %d fetched, %d new (%dms)",
                status, result["company"], first,
                result["fetched"], result["new_jobs"], result["duration_ms"],
            )

            if once:
                break

        except KeyboardInterrupt:
            logger.info("scan_worker: KeyboardInterrupt — shutting down")
            print("\n[scan_worker] Shutting down.")
            break

        except redis.exceptions.TimeoutError:
            logger.warning("scan_worker: Redis socket timeout (system load?) — retrying")
            if once:
                break
            continue
        except Exception as exc:
            logger.error(
                "scan_worker: unexpected loop error: %s", exc, exc_info=True,
            )
            if once:
                break
            time.sleep(1)

    # Stop heartbeat on ALL exit paths (break, KeyboardInterrupt, --once, shutdown event)
    _hb.stop()

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
    from logger import init_logging
    init_logging("scan_worker")
    once = "--once" in sys.argv
    run_worker(once=once)
