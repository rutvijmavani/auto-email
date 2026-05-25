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
from workers.redis_client import get_redis, ping
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
                    # Delete the pending_detail row so it does not linger as a
                    # zombie — rebuild_detail_queue() re-queues ALL pending_detail
                    # rows on restart, and without slug_info the rebuilt payload
                    # would skip the actual fetch and promote with empty location.
                    _finish(job_id, company, job, platform,
                            outcome="error", found_by=found_by)
                    result["duration_ms"] = int(
                        (time.monotonic() - start_mono) * 1000
                    )
                    result["outcome"] = "error"
                    return result
                finally:
                    set_request_context("normal")   # always reset

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

    BRPOPs payloads from [queue:detail:adaptive, queue:detail:fullscan].
    Redis BRPOP with a list of keys pops from the first non-empty key —
    adaptive queue is listed first so it always has priority.

    Args:
        once:           if True, process at most one job then exit.
        shutdown_event: multiprocessing.Event set by the scheduler when this
                        worker should stop. Checked after BRPOP returns.
                        If set, the payload is pushed back to the front of the
                        source queue (LPUSH) so it is not lost — detail jobs
                        do not use exponential backoff since the issue is
                        worker count, not a platform error for this job.
        skip_init_db:   if True, skip the init_db() call (used when the
                        scheduler parent process already ran it before fork).
    """
    if not skip_init_db:
        init_db()

    if not ping():
        redis_url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
        logger.error("detail_worker: Redis not reachable at %s — aborting", redis_url)
        print(f"[detail_worker] ERROR: Redis unreachable ({redis_url})")
        sys.exit(1)

    r = get_redis()

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

    while True:
        try:
            # BRPOP pops from the first non-empty key → adaptive has priority
            item = r.brpop(
                [REDIS_DETAIL_ADAPTIVE, REDIS_DETAIL_FULLSCAN],
                timeout=WORKER_BLOCK_SECS,
            )

            if item is None:
                # BRPOP timed out — check shutdown before looping
                if shutdown_event is not None and shutdown_event.is_set():
                    logger.info("detail_worker: shutdown event set (idle) — exiting")
                    break
                if once:
                    logger.info("detail_worker: --once, both queues empty — exiting")
                    print("[detail_worker] Both queues empty — exiting (--once)")
                    break
                continue

            source_queue, raw = item

            # ── Shutdown checkpoint: after BRPOP, before processing ───────────
            # Push the payload back to the front of its source queue (LPUSH so
            # it is picked up next) and exit.  No backoff here — the job itself
            # is fine, the worker count is just being reduced.
            if shutdown_event is not None and shutdown_event.is_set():
                r.lpush(source_queue, raw)
                logger.info(
                    "detail_worker: shutdown (pre-process), returned job "
                    "to %s — exiting",
                    source_queue,
                )
                break

            try:
                payload = json.loads(raw)
            except json.JSONDecodeError as exc:
                logger.error(
                    "detail_worker: bad JSON in %s: %s | raw=%r",
                    source_queue, exc, raw[:200],
                )
                if once:
                    break
                continue

            result = _process_detail(payload, source_queue)

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
