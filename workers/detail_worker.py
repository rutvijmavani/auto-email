"""
workers/detail_worker.py — Two-tier detail fetch worker (Phase 3).

Drains the detail queues, fetches full job data for Mode B platforms, applies
all filters, and promotes jobs from status='pending_detail' to status='new'
(or deletes them if they fail filters).

─── Two-tier queue priority ─────────────────────────────────────────────────

    queue:detail:adaptive   (high priority — from Tier 1 listing scan)
    queue:detail:fullscan   (low priority  — from Tier 2 full scan)

Workers drain the adaptive queue FIRST. Redis BRPOP with a list of keys
handles this automatically: it pops from the first non-empty key, so
queue:detail:adaptive always takes priority over queue:detail:fullscan.

─── Per-platform behavior ───────────────────────────────────────────────────

    Mode A (detail_needed=False):
        Greenhouse, Lever, Ashby, SmartRecruiters, Oracle HCM, etc.
        All data (title, location, description) is already in the listing
        payload. detail_worker just applies filters and saves.

    Mode B (detail_needed=True):
        Workday, iCIMS, Eightfold, Taleo, Jobvite, etc.
        Location and/or description only on individual detail page.
        detail_worker calls fetch_job_detail() before filtering.

─── Filter pipeline ─────────────────────────────────────────────────────────

    1. Title filter         (always — listing_filter drove this at scan time,
                             but detail_worker re-applies for consistency)
    2. Country code gate    (Workday alpha-2, SmartRecruiters alpha-2)
    3. Location filter      (is_us_location() text parsing)
    4. Freshness gate       (is_fresh() — checks first_published / createdAt)

    Jobs passing all filters → status='new' → appear in next digest.
    Jobs failing any filter  → pending_detail row deleted from DB.

─── Deduplication note ──────────────────────────────────────────────────────

    detail_worker does NOT write to any Redis dedup structure.
    Deduplication is handled upstream:
      - adaptive_seen:{company} SET: written by scan_worker before enqueue.
      - bloom:fullscan:{company}: written by fullscan worker on completion.
    detail_worker's sole responsibility is: fetch detail, filter, persist.

─── Usage ───────────────────────────────────────────────────────────────────

    python -m workers.detail_worker          # run forever
    python -m workers.detail_worker --once   # process one job then exit

    # Inspect the adaptive detail queue depth:
    python -c "
    from workers.redis_client import get_redis
    r = get_redis()
    print('adaptive queue:', r.llen('queue:detail:adaptive'))
    print('fullscan queue:', r.llen('queue:detail:fullscan'))
    "
"""

import json
import os
import socket
import sys
import time
from datetime import datetime, timezone
from typing import Optional

from logger import get_logger
from config import (
    REDIS_DETAIL_ADAPTIVE,
    REDIS_DETAIL_FULLSCAN,
    WORKER_BLOCK_SECS,
    REDIS_BACKOFF_PREFIX,
)
from workers.redis_client import get_redis
from workers.heartbeat import Heartbeat
from workers.http_client import set_request_context
from jobs.ats_detector import get_ats_module
from jobs.ats.registry import get_config, parse_slug, should_fetch_detail
from jobs.job_filter import (
    filter_jobs, filter_jobs_title_only, is_us_location, is_fresh,
)
from urllib.parse import urlparse, parse_qs
from db.db import init_db
from db.job_monitor import (
    complete_pending_detail,
    delete_pending_detail,
)

logger = get_logger(__name__)

WORKER_ID = f"{socket.gethostname()}:{os.getpid()}"

# ── At-least-once delivery via LMOVE processing lists ────────────────────────
# BRPOP is destructive: if the worker dies after pop but before the DB write,
# the job is lost from Redis.  Instead we LMOVE the item into a per-worker
# "inflight" list atomically before we touch it, then LREM it afterwards.
#
# Per-PID inflight key layout (N = os.getpid() of this worker process):
#   queue:detail:adaptive              ← source (LPUSH by scan_worker/fullscan)
#   queue:detail:adaptive:inflight:N   ← this worker's in-progress jobs
#   queue:detail:fullscan
#   queue:detail:fullscan:inflight:N
#
# Using per-PID keys prevents _recover_stuck_jobs() from draining a live
# peer's active items on startup (Bug 1 fix).  Recovery scans for ALL
# :inflight:* keys, checks the heartbeat of each PID, and only drains keys
# whose owner has no active heartbeat (confirmed dead).
#
# _INFLIGHT_ADAPTIVE / _INFLIGHT_FULLSCAN / _INFLIGHT_KEY are intentionally
# left empty here — they are set by run_worker() using the child process's
# own PID (os.getpid() after fork) before the main loop starts.

_INFLIGHT_ADAPTIVE: str = ""   # set in run_worker()
_INFLIGHT_FULLSCAN: str = ""   # set in run_worker()
_INFLIGHT_KEY: dict     = {}   # set in run_worker()

# ── Atomic shutdown requeue (Bug 3 fix) ───────────────────────────────────────
# Replaces the previous two-call sequence (LREM then LPUSH) which had a crash
# window where the job could be lost if the process died between the two calls.
# Lua runs atomically on the Redis server — either both ops complete or neither.
#
# KEYS[1] = inflight_key   KEYS[2] = source_queue   ARGV[1] = raw payload
#
# RPUSH (not LPUSH): producers use LPUSH (head/left); consumers pop from the
# tail/right with LMOVE "RIGHT".  RPUSH places the requeued job at the tail so
# it is consumed next — giving it high priority rather than sending it to the
# back of the line.
_ATOMIC_REQUEUE_LUA = """
local removed = redis.call('LREM', KEYS[1], 1, ARGV[1])
if removed > 0 then
    redis.call('RPUSH', KEYS[2], ARGV[1])
end
return removed
"""

# Atomic peek-and-pop for recovery drain.
# Checks that the expected item is still the rightmost element, then
# pops it from the inflight list and optionally pushes it to the source
# queue — all in one round-trip so no live worker can steal the item
# between the pop and the discard decision.
#
# KEYS[1] = inflight_key   KEYS[2] = source_key
# ARGV[1] = expected item  ARGV[2] = '1' recover (push to source), '0' discard
_ATOMIC_DRAIN_LUA = """
local tip = redis.call('LINDEX', KEYS[1], -1)
if not tip or tip ~= ARGV[1] then
    return 0
end
redis.call('RPOP', KEYS[1])
if ARGV[2] == '1' then
    redis.call('RPUSH', KEYS[2], ARGV[1])
end
return 1
"""


_MAX_DETAIL_RETRIES  = 5
_RETRY_KEY_PREFIX    = "detail:retry:"
_RETRY_KEY_TTL       = 86400 * 7   # 7 days — auto-expires if job never comes back


def _recover_stuck_jobs(r, own_token: str) -> None:
    """
    On worker startup: scan for per-worker inflight keys belonging to dead workers
    and drain their items back to the source queues.

    Safety contract:
      • own_token is always skipped — this worker has no heartbeat yet so it
        would look "dead" from the outside; never touch your own key.
      • A peer whose heartbeat key (worker:alive:detail_worker:{pid}) is still
        present is considered alive — skip its key entirely.
      • Only drain keys whose owning worker has NO heartbeat (confirmed dead).

    Token format: "{hostname}:{pid}" — guards against PID reuse across hosts
    when multiple machines share the same Redis.  Legacy keys that used a bare
    "{pid}" suffix are still handled: the PID is always the last colon component.

    Drain is atomic per item via _ATOMIC_DRAIN_LUA:
      LINDEX (peek) → retry check → Lua(RPOP + optional RPUSH / discard)
    This prevents live workers from stealing over-retry items during recovery.
    """
    for queue_key, source_key in [
        (REDIS_DETAIL_ADAPTIVE, REDIS_DETAIL_ADAPTIVE),
        (REDIS_DETAIL_FULLSCAN, REDIS_DETAIL_FULLSCAN),
    ]:
        _prefix  = f"{queue_key}:inflight:"
        pattern  = f"{_prefix}*"
        cursor   = 0
        while True:
            cursor, raw_keys = r.scan(cursor, match=pattern, count=100)
            for raw_key in raw_keys:
                key = raw_key.decode() if isinstance(raw_key, bytes) else raw_key

                if not key.startswith(_prefix):
                    logger.warning(
                        "detail_worker: unexpected inflight key format %r — skipping",
                        key,
                    )
                    continue

                # Full worker token (supports "hostname:pid" and legacy "pid")
                peer_token = key[len(_prefix):]

                # Never touch our own in-flight key
                if peer_token == own_token:
                    continue

                # Skip peers with a live heartbeat.
                # Heartbeat key is host-qualified ({hostname}:{pid}) to prevent
                # false skips when two hosts share the same PID.
                hb_key = f"worker:alive:detail_worker:{peer_token}"
                if r.exists(hb_key):
                    logger.debug(
                        "detail_worker: peer token=%s heartbeat present — "
                        "skipping inflight recovery for %s",
                        peer_token, key,
                    )
                    continue

                # Peer is dead — drain atomically so no live worker can steal
                # an over-retry item between the move and the discard decision.
                # Algorithm per item:
                #   1. LINDEX (peek rightmost, non-destructive)
                #   2. Increment retry counter, decide recover vs discard
                #   3. Lua: RPOP from inflight + RPUSH to source (or discard)
                #      Returns 0 if item changed (concurrent drain) → re-loop.
                recovered = 0
                discarded = 0
                while True:
                    raw_peek = r.lindex(key, -1)
                    if raw_peek is None:
                        break
                    raw_peek = raw_peek.decode() if isinstance(raw_peek, bytes) else raw_peek

                    # Parse job_id/company for retry key
                    try:
                        _payload = json.loads(raw_peek)
                        _job_id  = _payload.get("job_id", "")
                        _company = _payload.get("company", "")
                    except Exception:
                        _job_id = _company = ""

                    # Recovery from a dead peer's inflight is not a retry attempt —
                    # the job was never actually processed.  Always requeue so the
                    # retry budget is only charged in the main run_worker loop when
                    # _process_detail() actually fails with retryable=True.
                    should_recover = True

                    # Atomically pop from inflight; push to source only if recovering.
                    # If another drain worker already consumed this item, Lua returns 0
                    # and we re-loop to pick up the next item.
                    _moved = r.eval(
                        _ATOMIC_DRAIN_LUA, 2, key, source_key,
                        raw_peek, "1",   # always recover (mode=1 → RPUSH to source)
                    )
                    if not _moved:
                        # Concurrent drain took the item — re-loop to get next
                        continue

                    recovered += 1

                if recovered:
                    logger.warning(
                        "detail_worker: recovered %d stuck job(s) from "
                        "dead peer token=%s: %s -> %s",
                        recovered, peer_token, key, source_key,
                    )

            if cursor == 0:
                break


def _pop_with_inflight(r, timeout: float) -> Optional[tuple]:
    """
    Priority-aware pop with at-least-once guarantee via inflight list.

    1. Try non-blocking LMOVE from adaptive first (high priority).
    2. If adaptive is empty, try non-blocking LMOVE from fullscan.
    3. If both empty, sleep briefly and retry until timeout.

    Returns (source_queue_key, raw_payload) or None on timeout.

    LMOVE is atomic: the item is either in the source list or the inflight
    list — never in neither.  The worker deletes it from inflight only AFTER
    a successful DB write (LREM in the main loop).
    """
    deadline = time.monotonic() + timeout
    poll_interval = 0.2   # seconds between empty-queue polls

    while time.monotonic() < deadline:
        # ── Adaptive first (high priority) ───────────────────────────────────
        raw = r.lmove(REDIS_DETAIL_ADAPTIVE, _INFLIGHT_ADAPTIVE, "RIGHT", "LEFT")
        if raw is not None:
            return (REDIS_DETAIL_ADAPTIVE, raw)

        # ── Fullscan fallback (low priority) ─────────────────────────────────
        raw = r.lmove(REDIS_DETAIL_FULLSCAN, _INFLIGHT_FULLSCAN, "RIGHT", "LEFT")
        if raw is not None:
            return (REDIS_DETAIL_FULLSCAN, raw)

        # ── Both empty — wait briefly before next poll ────────────────────────
        remaining = deadline - time.monotonic()
        time.sleep(min(poll_interval, max(0, remaining)))

    return None


# Keys that fetch_job_detail() checks in its guard clause before making any
# HTTP request.  If any of these are absent from the queue payload, the
# function returns the original job dict silently — no API call is made,
# no location or description is filled, and the job may pass location
# filters with empty strings.
#
# Used by the pre-flight audit in _process_detail() to log a WARNING before
# the call so the gap is visible in logs even when the job ends up "new".
#
# Keep in sync with each ATS module's fetch_job_detail() guard clause.
_REQUIRED_DETAIL_KEYS: dict = {
    # if not all([slug, wd, path, external_path]): return job
    "workday":         ["_slug", "_wd", "_path", "_external_path"],
    # if not base_url or not contest_no: return job
    "taleo":           ["_base_url", "_contest_no"],
    # if not job_id or not company_slug: return job
    "smartrecruiters": ["_company_slug"],
    # guard is `if not job_url` — but _base_url doubles as the
    # should_fetch_detail() gate key so we still check it here
    "icims":           ["_base_url"],
    # guard is `if not job_url`; _slug is only the gate key
    "jobvite":         ["_slug"],
}

# Per-ATS semaphores for detail fetches — each detail_worker makes one
# HTTP call per job, so semaphores throttle cross-worker pressure on
# the same ATS domain. Import lazily to avoid circular imports.
_PLATFORM_SEMAPHORES: dict = {}
_DEFAULT_CONCURRENCY = 5


def _get_semaphore(platform: str):
    """Return (or lazily create) the per-platform Semaphore."""
    if not _PLATFORM_SEMAPHORES:
        import threading
        from config import MONITOR_PLATFORM_CONCURRENCY
        for plat, limit in MONITOR_PLATFORM_CONCURRENCY.items():
            _PLATFORM_SEMAPHORES[plat] = threading.Semaphore(limit)
        _PLATFORM_SEMAPHORES["_default"] = threading.Semaphore(
            _DEFAULT_CONCURRENCY
        )
    return _PLATFORM_SEMAPHORES.get(platform,
                                    _PLATFORM_SEMAPHORES["_default"])


# ─────────────────────────────────────────
# CORE: process one detail job
# ─────────────────────────────────────────

def _process_detail(payload: dict, source_queue: str) -> dict:
    """
    Process one detail fetch job from the queue.

    Steps:
        1. Extract job metadata from payload
        2. Call fetch_job_detail() for Mode B platforms
        3. Apply full filter pipeline (title, location, freshness)
        4. Promote to status='new' or delete pending_detail row

    Returns a compact result dict for logging.
    Never raises — all exceptions are caught and logged.

    Args:
        payload:      Decoded job dict from the detail queue.
        source_queue: Which queue was popped (for logging).

    Returns:
        dict with keys: company, job_id, outcome (new/filtered/error),
        platform, duration_ms.
    """
    company     = payload.get("company", "")
    job_id      = payload.get("job_id", "")
    platform    = payload.get("ats_platform", "")
    found_by    = payload.get("found_by", "tier1_adaptive")
    slug_info   = payload.get("slug_info")

    start_mono  = time.monotonic()

    result = {
        "company":     company,
        "job_id":      job_id,
        "platform":    platform,
        "outcome":     "error",
        "duration_ms": 0,
        # retryable=True means leave the Redis item in inflight so
        # _recover_stuck_jobs restores it for retry on the next worker start.
        # Defaults False — most errors (filtered, fetch failure, bad payload)
        # have already cleaned up the DB row and should not be retried.
        "retryable":   False,
    }

    if not company or not job_id:
        logger.warning("detail_worker: missing company or job_id in payload — skipping")
        return result

    try:
        # ── 1. Config + module ────────────────────────────────────────────────
        config     = get_config(platform)
        ats_module = get_ats_module(platform)

        if not ats_module:
            logger.error(
                "detail_worker: no ATS module for platform=%s company=%r job_id=%s",
                platform, company, job_id,
            )
            try:
                delete_pending_detail(company, job_id)
            except Exception as _del_err:
                logger.error(
                    "detail_worker: delete_pending_detail failed for %s: %s",
                    job_id, _del_err,
                )
            return result

        # slug_info comes serialized in the queue payload (str or dict)
        if slug_info is None and payload.get("ats_slug"):
            # Fallback: re-parse from ats_slug if passed directly
            slug_info = parse_slug(platform, payload["ats_slug"], config)

        # ── 2. Detail fetch (Mode B) ──────────────────────────────────────────
        # Merge listing payload into a job dict for fetch_job_detail()
        job = dict(payload)   # listing-level data already in payload

        detail_attempted = should_fetch_detail(job, platform, config, slug_info)

        if detail_attempted:
            # ── Pre-flight key audit ──────────────────────────────────────────
            # Each fetch_job_detail() has a guard clause that returns the
            # original job dict unchanged if required keys are absent.  That
            # silent return is indistinguishable from a successful fetch in
            # downstream code — log a WARNING here so we catch payload gaps
            # before they produce empty-location jobs in the digest.
            #
            # Keys that, if missing, cause fetch_job_detail to return immediately
            # WITHOUT making an HTTP request (derived from each module's guard):
            #   workday:         not all([_slug, _wd, _path, _external_path])
            #   taleo:           not _base_url or not _contest_no
            #   smartrecruiters: not job_id or not _company_slug
            #   icims:           not job_url  (job_url always in payload — safety)
            #   jobvite:         not job_url  (same; _slug is only gate key)
            _missing = [
                k for k in _REQUIRED_DETAIL_KEYS.get(platform, [])
                if not job.get(k)
            ]
            if _missing:
                logger.warning(
                    "detail_worker: MISSING required keys — fetch_job_detail "
                    "guard will fire with NO HTTP request made. "
                    "platform=%s company=%r job_id=%s missing_keys=%s "
                    "payload_underscore_keys=%s",
                    platform, company, job_id, _missing,
                    [k for k in job if k.startswith("_") and job.get(k)],
                )
            else:
                logger.debug(
                    "detail_worker: detail fetch starting "
                    "platform=%s company=%r job_id=%s",
                    platform, company, job_id,
                )

            # Snapshot fields that fetch_job_detail() should enrich so we can
            # detect a silent no-op return after the call completes.
            _snap_loc  = job.get("location", "")
            _snap_cc   = job.get("_country_code", "")
            _snap_desc = job.get("description", "")

            # Phase 10 — api_health context tagging:
            # Tag all ats_get() calls inside fetch_job_detail() with the
            # correct context so backoff retries don't pollute the baseline.
            # detail_worker has no canary concept — only normal vs backoff.
            r = get_redis()
            _detail_ctx = (
                "backoff"
                if r.exists(f"{REDIS_BACKOFF_PREFIX}:detail:{company}")
                else "normal"
            )
            set_request_context(_detail_ctx)
            sem = _get_semaphore(platform)
            with sem:
                try:
                    if platform == "custom":
                        job = ats_module.fetch_job_detail(job, slug_info)
                    else:
                        job = ats_module.fetch_job_detail(job)
                except Exception as exc:
                    logger.error(
                        "detail_worker: fetch_job_detail failed "
                        "platform=%s company=%r job_id=%s: %s",
                        platform, company, job_id, exc, exc_info=True,
                    )
                    result["duration_ms"] = int(
                        (time.monotonic() - start_mono) * 1000
                    )
                    result["outcome"] = "error"
                    # Default all unknown exceptions to retryable so transient
                    # issues (rate limits, unexpected ATS responses, parser
                    # hiccups) get a retry rather than permanent deletion.
                    # The bounded retry cap (_MAX_DETAIL_RETRIES) prevents
                    # poisoned jobs from looping forever.
                    result["retryable"] = True
                    return result
                finally:
                    set_request_context("normal")   # always reset

            # ── Post-fetch enrichment audit ───────────────────────────────────
            # If none of the key fields changed, fetch_job_detail silently
            # returned the original job without making an HTTP request (guard
            # fired) OR the API was called but returned empty data.  Either
            # way, log at WARNING so it shows up in scheduler_{date}.log.
            _enriched = (
                job.get("description",   "") != _snap_desc
                or job.get("location",   "") != _snap_loc
                or job.get("_country_code", "") != _snap_cc
            )
            if _enriched:
                logger.debug(
                    "detail_worker: fetch_job_detail enriched job — "
                    "location=%r cc=%r desc_chars=%d "
                    "platform=%s company=%r job_id=%s",
                    job.get("location"), job.get("_country_code"),
                    len(job.get("description") or ""),
                    platform, company, job_id,
                )
            else:
                logger.warning(
                    "detail_worker: fetch_job_detail returned NO new data "
                    "(location/cc/description unchanged). "
                    "Guard may have fired or API returned empty. "
                    "platform=%s company=%r job_id=%s "
                    "location=%r cc=%r",
                    platform, company, job_id,
                    job.get("location"), job.get("_country_code"),
                )

        # ── 3. Filter pipeline ────────────────────────────────────────────────

        # 3a. Country code gate (Workday alpha-2, SmartRecruiters alpha-2, etc.)
        country_src  = config.get("country_source", "text")
        country_code = (job.get("_country_code") or "").upper()

        if country_src == "alpha2" and country_code:
            if country_code != "US":
                logger.debug(
                    "detail_worker: non-US (alpha2=%s) %r | %s",
                    country_code, company, job.get("title"),
                )
                _finish(job_id, company, job, platform,
                        outcome="filtered", found_by=found_by)
                result["outcome"]     = "filtered"
                result["duration_ms"] = int(
                    (time.monotonic() - start_mono) * 1000
                )
                return result

        # 3b. Full filter (title + location) — also re-applies title for safety
        listing_filter = config.get("listing_filter", "full")
        if listing_filter == "title_only":
            passed = filter_jobs_title_only([job])
        else:
            passed = filter_jobs([job])

        if not passed:
            logger.debug(
                "detail_worker: filtered out %r | %s | %s",
                company, job.get("title"), job.get("location"),
            )
            _finish(job_id, company, job, platform,
                    outcome="filtered", found_by=found_by)
            result["outcome"]     = "filtered"
            result["duration_ms"] = int(
                (time.monotonic() - start_mono) * 1000
            )
            return result

        # Re-check location for title_only platforms (detail has real location)
        if listing_filter == "title_only":
            # Refresh country_code — detail fetch may have set/changed it
            country_code = (job.get("_country_code") or "").upper()

            # ── alpha-2 gate (after refresh) ──────────────────────────────
            if country_code and country_code != "US":
                logger.debug(
                    "detail_worker: non-US (alpha2 from detail) %r | %s",
                    company, job.get("title"),
                )
                _finish(job_id, company, job, platform,
                        outcome="filtered", found_by=found_by)
                result["outcome"]     = "filtered"
                result["duration_ms"] = int(
                    (time.monotonic() - start_mono) * 1000
                )
                return result

            # ── Location text check ───────────────────────────────────────
            location = job.get("location", "")

            # Fallback: if location is still empty after detail fetch, try to
            # extract a city from the job URL.  Workday embeds city in the
            # URL path: /job/Hyderabad/Application-Developer_ATCI-...
            # is_us_location("Hyderabad") → False (geonamescache: India-only,
            # Signal 6), whereas is_us_location("") → True by design.
            # This is a best-effort improvement; if URL extraction also yields
            # nothing we fall through to is_us_location("") = True to avoid
            # false negatives on platforms that don't embed city in the URL.
            if not location.strip():
                url_city = _extract_city_from_url(job.get("job_url", ""))
                if url_city:
                    location = url_city
                    job["location"] = url_city   # visible in logs / DB
                    logger.debug(
                        "detail_worker: location empty after detail fetch, "
                        "extracted from URL: %r — platform=%s company=%r | %s",
                        url_city, platform, company, job.get("title"),
                    )

            if not is_us_location(location):
                logger.debug(
                    "detail_worker: non-US location (text) %r | %s | %s",
                    company, job.get("title"), location,
                )
                _finish(job_id, company, job, platform,
                        outcome="filtered", found_by=found_by)
                result["outcome"]     = "filtered"
                result["duration_ms"] = int(
                    (time.monotonic() - start_mono) * 1000
                )
                return result

        # 3c. Freshness gate
        # Greenhouse exception: first_published is reliable; always check.
        # Other platforms: use is_fresh() which handles their date fields.
        if platform != "greenhouse" and not is_fresh(job, platform):
            logger.debug(
                "detail_worker: stale job %r | %s | posted=%s",
                company, job.get("title"), job.get("posted_at"),
            )
            _finish(job_id, company, job, platform,
                    outcome="filtered", found_by=found_by)
            result["outcome"]     = "filtered"
            result["duration_ms"] = int(
                (time.monotonic() - start_mono) * 1000
            )
            return result

        # ── 4. Save as 'new' ──────────────────────────────────────────────────
        job["found_by"]     = found_by
        job["skill_score"]  = passed[0].get("skill_score", 0) if passed else 0

        updated = complete_pending_detail(
            company=company,
            job_id=job_id,
            job=job,
            status="new",
        )

        if not updated:
            # Row may have been deleted (cleanup) or never inserted.
            # Log as warning and continue — data is not lost because
            # detail was fetched; we just can't update the status.
            logger.warning(
                "detail_worker: no pending_detail row found for "
                "company=%r job_id=%s — may have been cleaned up",
                company, job_id,
            )

        result["outcome"]     = "new"
        result["duration_ms"] = int((time.monotonic() - start_mono) * 1000)

        logger.info(
            "detail_worker: NEW job | company=%r | %s | %s",
            company, job.get("title"), job.get("location"),
        )

    except Exception as exc:
        result["duration_ms"] = int((time.monotonic() - start_mono) * 1000)
        result["outcome"]     = "error"
        # The DB row (status='pending_detail') has NOT been cleaned up by this
        # path — _finish() was never called.  Mark as retryable so the caller
        # leaves this item in inflight; _recover_stuck_jobs() on the next
        # worker startup will restore it for a retry attempt.
        result["retryable"]   = True
        logger.error(
            "detail_worker: unhandled error company=%r job_id=%s: %s",
            company, job_id, exc, exc_info=True,
        )

    return result


def _extract_city_from_url(job_url: str) -> str:
    """
    Try to extract a city name from a job URL path.

    Handles common ATS URL patterns:
      Workday:  /.../job/Hyderabad/Job-Title_R-12345
      Taleo:    /...?location=Bengaluru
      Fallback: scan path segments for plausible city-like tokens

    Returns a cleaned city string, or "" if nothing usable is found.
    The caller uses the result with is_us_location() which can correctly
    reject non-US cities via geonamescache (Signal 6).
    """
    if not job_url:
        return ""
    try:
        parsed   = urlparse(job_url)
        # Query param: ?location=CityName (Taleo and others)
        qs = parse_qs(parsed.query)
        for key in ("location", "city", "loc"):
            vals = qs.get(key, [])
            if vals and vals[0].strip():
                return vals[0].strip()

        # Path: /job/CityName/... or /jobs/CityName/...
        parts = [p for p in parsed.path.split("/") if p]
        for i, part in enumerate(parts):
            if part.lower() in ("job", "jobs") and i + 1 < len(parts):
                candidate = parts[i + 1]
                # Skip obvious non-city segments: job IDs (R-12345, JR123),
                # purely numeric IDs, or segments with underscores (job titles)
                if (candidate
                        and not candidate.startswith(("R-", "JR", "req", "REQ"))
                        and not candidate[:1].isdigit()
                        and "_" not in candidate):
                    # Convert URL slug to readable form: "New-York" → "New York"
                    city = candidate.replace("-", " ").strip()
                    # Reject overly long segments — likely a job title, not a city
                    if city and len(city) <= 30:
                        return city
    except Exception:
        pass
    return ""


def _finish(
    job_id: str,
    company: str,
    job: dict,
    platform: str,
    outcome: str,
    found_by: str,
) -> None:
    """
    Clean up after a job fails filters.

    Deletes the pending_detail row (it would never be promoted to 'new').
    Dedup tracking is handled by adaptive_seen:{company} (written by
    scan_worker before enqueue) — no Redis write needed here.
    """
    delete_pending_detail(company, job_id)


# ─────────────────────────────────────────
# MAIN LOOP
# ─────────────────────────────────────────

def run_worker(once: bool = False, shutdown_event=None,
               skip_init_db: bool = False) -> None:
    """
    Main detail fetch worker loop.

    Pops payloads from queue:detail:adaptive and queue:detail:fullscan using
    _pop_with_inflight(), which atomically moves each item into a per-worker
    inflight list (inflight:detail_worker:{hostname}:{pid}) before processing.

    On shutdown, _ATOMIC_REQUEUE_LUA moves any in-progress item back to the
    front of its source queue so it is not lost.  On startup,
    _recover_stuck_jobs() scans peer inflight keys and requeues items from
    workers whose heartbeat has expired.

    Args:
        once:           if True, process at most one job then exit.
        shutdown_event: multiprocessing.Event set by the scheduler when this
                        worker should stop. Checked after each pop attempt.
        skip_init_db:   if True, skip the init_db() call (used when the
                        scheduler parent process already ran it before fork).
    """
    from workers.sentry_init import init_sentry
    init_sentry()

    # ── Startup validation (Redis + PostgreSQL + required config) ────────────
    # Must run before init_db() so infrastructure failures produce a clear
    # STARTUP FAILED message rather than an opaque DB initialisation error.
    from workers.startup import validate_startup
    validate_startup("detail_worker",
                     check_redis=True,
                     check_db=True,
                     check_config=True)

    if not skip_init_db:
        init_db()

    r = get_redis()

    # ── Set per-PID inflight key names ───────────────────────────────────────
    # Must use os.getpid() HERE (inside the child process after fork) so each
    # worker process gets its own inflight namespace.  Module-level constants
    # were intentionally left empty to prevent accidental shared-key usage
    # when the scheduler imports this module before spawning workers.
    global _INFLIGHT_ADAPTIVE, _INFLIGHT_FULLSCAN, _INFLIGHT_KEY
    own_pid   = os.getpid()
    # Include hostname so PID reuse on a different host cannot accidentally match
    # a live peer's inflight key when multiple machines share the same Redis.
    own_token          = f"{socket.gethostname()}:{own_pid}"
    _INFLIGHT_ADAPTIVE = f"{REDIS_DETAIL_ADAPTIVE}:inflight:{own_token}"
    _INFLIGHT_FULLSCAN = f"{REDIS_DETAIL_FULLSCAN}:inflight:{own_token}"
    _INFLIGHT_KEY      = {
        REDIS_DETAIL_ADAPTIVE: _INFLIGHT_ADAPTIVE,
        REDIS_DETAIL_FULLSCAN: _INFLIGHT_FULLSCAN,
    }

    # ── Recover any jobs left in inflight lists from dead peer workers ────────
    _recover_stuck_jobs(r, own_token)

    logger.info(
        "detail_worker started | worker_id=%s adaptive=%s fullscan=%s once=%s",
        WORKER_ID, REDIS_DETAIL_ADAPTIVE, REDIS_DETAIL_FULLSCAN, once,
    )
    print(f"[detail_worker] Ready — worker={WORKER_ID}")
    print(f"[detail_worker] Draining: {REDIS_DETAIL_ADAPTIVE!r} (priority) "
          f"then {REDIS_DETAIL_FULLSCAN!r}")
    if once:
        print("[detail_worker] --once mode: processing one job then exiting")
    else:
        print("[detail_worker] Press Ctrl+C to stop\n")

    # ── Background heartbeat ─────────────────────────────────────────────────
    # Daemon thread writes worker:alive:detail_worker every 10s.
    # Detail fetches are usually fast (1–5s) but the thread approach keeps the
    # pattern consistent and future-proof if heavier fetches are added.
    # daemon=True means the thread dies with the process — no ghost heartbeats.
    _hw = {"count": 0}
    _hb = Heartbeat(r, "detail_worker", lambda: _hw["count"]).start()

    # Periodic peer-recovery — also runs at startup (line above).
    # A peer can die at any time, not just before we start.  Check every
    # _PEER_RECOVERY_INTERVAL_S so stranded inflight jobs are reclaimed quickly
    # without adding meaningful overhead (SCAN returns empty in the common case).
    _PEER_RECOVERY_INTERVAL_S = 300   # 5 minutes
    _last_peer_recovery = time.monotonic()

    while True:
        try:
            # ── Periodic dead-peer recovery ───────────────────────────────────
            _now_mono = time.monotonic()
            if _now_mono - _last_peer_recovery >= _PEER_RECOVERY_INTERVAL_S:
                try:
                    _recover_stuck_jobs(r, own_token)
                except Exception as _rec_exc:
                    logger.warning(
                        "detail_worker: periodic peer recovery failed: %s", _rec_exc
                    )
                _last_peer_recovery = _now_mono

            # ── At-least-once pop via LMOVE inflight list ─────────────────────
            # Moves item atomically: source_queue → inflight list.
            # The item is acknowledged (LREM'd) only after a successful DB write.
            # On crash, _recover_stuck_jobs() restores it on next startup.
            item = _pop_with_inflight(r, timeout=WORKER_BLOCK_SECS)

            if item is None:
                # Poll timeout — check shutdown before looping
                if shutdown_event is not None and shutdown_event.is_set():
                    logger.info("detail_worker: shutdown event set (idle) — exiting")
                    break
                if once:
                    logger.info("detail_worker: --once, both queues empty — exiting")
                    print("[detail_worker] Both queues empty — exiting (--once)")
                    break
                continue

            source_queue, raw = item
            inflight_key = _INFLIGHT_KEY[source_queue]

            # ── Shutdown checkpoint: item is already in inflight list ─────────
            # Atomically move our specific job back to the front of the source
            # queue via Lua script (_ATOMIC_REQUEUE_LUA).  A single Redis eval()
            # is atomic — no crash window between LREM and LPUSH.
            #
            # We use LREM-by-value (not LMOVE-from-end) because each worker now
            # has its own per-PID inflight key, so there is only ever one item
            # in this key at a time.  LREM is semantically clearer and safe.
            if shutdown_event is not None and shutdown_event.is_set():
                r.eval(_ATOMIC_REQUEUE_LUA, 2, inflight_key, source_queue, raw)
                logger.info(
                    "detail_worker: shutdown (pre-process), atomically returned "
                    "job to %s — exiting",
                    source_queue,
                )
                break

            try:
                payload = json.loads(raw)
            except json.JSONDecodeError as exc:
                # Malformed payload — can never succeed, remove from inflight
                logger.error(
                    "detail_worker: bad JSON in %s: %s | raw=%r",
                    source_queue, exc, raw[:200],
                )
                r.lrem(inflight_key, 1, raw)   # discard — retrying won't help
                if once:
                    break
                continue

            result = _process_detail(payload, source_queue)
            _hw["count"] += 1

            # ── Acknowledge or retain in inflight ─────────────────────────────
            # retryable=True means _process_detail hit an unexpected exception
            # WITHOUT cleaning up the DB row — leave the item in inflight so
            # _recover_stuck_jobs() restores it for a retry on next startup.
            #
            # retryable=False (default) covers all other paths: success (new),
            # filtered, and known-permanent errors where _finish() already
            # deleted the pending_detail DB row.  These are acknowledged.
            if result.get("retryable"):
                # Charge the retry budget here — not in recovery — so only real
                # processing failures count against the limit.
                _r_job_id  = result.get("job_id", "")
                _r_company = result.get("company", "")
                _r_discard = False
                if _r_job_id:
                    _rkey = (
                        f"{_RETRY_KEY_PREFIX}{_r_company}:{_r_job_id}"
                        if _r_company else
                        f"{_RETRY_KEY_PREFIX}{_r_job_id}"
                    )
                    try:
                        _attempt = int(r.incr(_rkey))
                        r.expire(_rkey, _RETRY_KEY_TTL)
                        if _attempt > _MAX_DETAIL_RETRIES:
                            logger.error(
                                "detail_worker: job_id=%s company=%r exceeded "
                                "%d retries — discarding permanently",
                                _r_job_id, _r_company, _MAX_DETAIL_RETRIES,
                            )
                            r.lrem(inflight_key, 1, raw)
                            try:
                                delete_pending_detail(_r_company, _r_job_id)
                            except Exception as _dp_err:
                                logger.error(
                                    "detail_worker: delete_pending_detail "
                                    "failed for %s: %s", _r_job_id, _dp_err,
                                )
                            _r_discard = True
                    except Exception as _cnt_err:
                        logger.warning(
                            "detail_worker: retry counter failed for %s: %s "
                            "— allowing retry",
                            _r_job_id, _cnt_err,
                        )
                if _r_discard:
                    if once:
                        break
                    continue

                logger.warning(
                    "detail_worker: transient error — leaving job_id=%s "
                    "company=%r in inflight; exiting so _recover_stuck_jobs() "
                    "can reclaim it on the next startup",
                    _r_job_id, _r_company,
                )
                # Stop the heartbeat daemon BEFORE deleting the key so the
                # thread cannot recreate it after deletion (race window).
                _hb.stop()
                # Delete our heartbeat key immediately so a respawned worker
                # that starts before the TTL expires can still reclaim this
                # inflight item via _recover_stuck_jobs() without waiting 30s.
                try:
                    r.delete(f"worker:alive:detail_worker:{os.getpid()}")
                except Exception as _hb_del_err:
                    logger.error(
                        "detail_worker: failed to delete heartbeat key: %s",
                        _hb_del_err,
                    )
                break
            else:
                try:
                    r.lrem(inflight_key, 1, raw)
                except Exception as _lrem_err:
                    # If ack fails the item stays in inflight indefinitely while
                    # this worker is alive (recover_stuck_jobs skips live PIDs).
                    # Safest: exit so the next startup can reclaim the item.
                    logger.error(
                        "detail_worker: lrem failed for job_id=%s — "
                        "leaving in inflight; exiting for recovery: %s",
                        payload.get("job_id"), _lrem_err,
                    )
                    _hb.stop()
                    break

            tier    = "T1" if source_queue == REDIS_DETAIL_ADAPTIVE else "T2"
            outcome = result["outcome"]
            icon    = {"new": "[NEW]", "filtered": "[skip]", "error": "[ERR]"}.get(
                outcome, "[?]"
            )
            print(f"  {icon} [{tier}] {result['company']} "
                  f"{result.get('job_id', '?')} ({result['duration_ms']}ms)")

            if once:
                break

        except KeyboardInterrupt:
            logger.info("detail_worker: KeyboardInterrupt — shutting down")
            print("\n[detail_worker] Shutting down.")
            break

        except Exception as exc:
            logger.error(
                "detail_worker: unexpected loop error: %s", exc, exc_info=True,
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

    logger.info("detail_worker shutdown | worker_id=%s", WORKER_ID)


# ─────────────────────────────────────────
# CLI
# ─────────────────────────────────────────

if __name__ == "__main__":
    once = "--once" in sys.argv
    run_worker(once=once)
