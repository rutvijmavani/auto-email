"""
workers/fullscan.py — Full scan worker (Phase 6).

Reads companies from the poll:fullscan ZSET (score-ascending = most-overdue
first) and performs a comprehensive scan of every job page, using a per-company
Bloom filter to efficiently skip job IDs already seen during adaptive polling.

─── Architecture ─────────────────────────────────────────────────────────────

    scheduler._schedule_full_scan()
        → ZADD poll:fullscan {company: now + 300}   ← Rule 3 buffer
    [this worker]
        → ZPOPMIN poll:fullscan  (score-ascending = most overdue first)
        → Rule 4 pre-check: if last_poll_at < cycle_start → adaptive first
        → SET fullscan:lock:{company} NX EX 3600     ← exclusive lock
        → Resume from checkpoint if full_scan_interrupted=TRUE
        → Page-by-page: Bloom filter → save pending_detail → detail queue
        → On pause: checkpoint + re-queue immediately (score=now-1)
        → On complete: refresh Bloom filter TTL + update DB + reschedule

─── Two-tier detail queue ────────────────────────────────────────────────────

    Full scan jobs → queue:detail:fullscan   (low priority, Tier 2)
    detail_worker drains queue:detail:adaptive FIRST (BRPOP priority)

─── Bloom filter ─────────────────────────────────────────────────────────────

    bloom:fullscan:{company}   — RedisBloom BF.* (if module available)
    bloom:fallback:{company}   — Regular Redis SET (automatic fallback)
    TTL = 36h (FULLSCAN_BLOOM_TTL)

    The Bloom filter tracks job IDs queued during the current full scan cycle
    so that: (a) intra-scan duplicate IDs are skipped, and (b) a resumed
    scan after pause can skip already-processed jobs without re-fetching them.

    On a fresh full scan: the filter is deleted and re-created from scratch.
    On a checkpoint resume: the existing filter is reused — jobs already in
    it were queued before the pause, so they are correctly skipped.

─── Rule 4 (adaptive-first) ──────────────────────────────────────────────────

    If company_poll_stats.last_poll_at < cycle:start (Redis):
        The company hasn't had its adaptive poll this cycle yet.
        → ZADD poll:adaptive with score=now (trigger immediately)
        → ZADD poll:fullscan with score=now+900 (retry in 15 minutes)
        → Skip this full scan run

─── Pause / resume ───────────────────────────────────────────────────────────

    Pause is detected by polling r.exists(db:maintenance) between page chunks.
    On pipeline:pause channel message (set by nightly cron via redis_signal.py):
        1. Finish current page chunk
        2. Write checkpoint: full_scan_interrupted=TRUE, interrupted_at_page=N
        3. ZADD poll:fullscan score=now-1  (immediately re-due on resume)
        4. Release lock + clear heartbeat

    On resume: fullscan dispatch picks up the company again (score <= now),
    restores the Bloom filter state, and continues processing — jobs already
    in the Bloom filter are skipped, ensuring no duplicate queue pushes.

─── Sorted vs. non-sorted platforms ─────────────────────────────────────────

    SORTED (Greenhouse, Lever, Ashby, SmartRecruiters, Eightfold, Oracle HCM):
        Results are newest-first. Use should_continue_paginating() with 80%/2-page
        early exit — even in full scan mode, we stop when we hit the "old zone".

    NON-SORTED (Workday, iCIMS, Taleo, ADP, etc.):
        No ordering guarantee. All pages are fetched unconditionally.
        Note: Phase 7 will add true page-by-page streaming; until then
        fetch_jobs() returns all pages at once and we process in chunks.

─── Usage ────────────────────────────────────────────────────────────────────

    python -m workers.fullscan          # run forever
    python -m workers.fullscan --once   # process one company then exit
    python -m workers.fullscan --skip-lock  # bypass lock (dev/debug only)

─── Architecture doc reference ───────────────────────────────────────────────

    Section 9  — Full scan (Tier 2)
    Section 11 — Smart early exit (sorted platforms only)
    Section 15 — Redis key reference (bloom:fullscan:*, fullscan:lock:*)
    Section 18 — Checkpoint / resume on pause
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
    REDIS_POLL_ADAPTIVE,
    REDIS_POLL_FULLSCAN,
    REDIS_DETAIL_FULLSCAN,
    REDIS_CYCLE_START,
    REDIS_DB_MAINTENANCE,
    WORKER_BLOCK_SECS,
    SCHEDULER_FULL_SCAN_LOCK_TTL,
    SCHEDULER_FULL_SCAN_INTERVAL_S,
    SCHEDULER_FULL_SCAN_BUFFER_S,
    SCHEDULER_HEARTBEAT_TTL,
    FULLSCAN_BLOOM_TTL,
    FULLSCAN_BLOOM_ERROR_RATE,
    DETAIL_QUEUE_MAX_FULLSCAN,
)
from workers.redis_client import get_redis, ping
from workers.scheduler import set_heartbeat, clear_heartbeat, set_progress
from workers.paginator import should_continue_paginating, estimate_scan_depth
from jobs.ats_detector import get_ats_module
from jobs.ats.registry import get_config, parse_slug, should_fetch_detail
from jobs.job_filter import filter_jobs, filter_jobs_title_only
from db.db import init_db, get_conn
from db.job_monitor import (
    get_company_row,
    save_pending_detail,
)

logger = get_logger(__name__)

WORKER_ID = f"{socket.gethostname()}:{os.getpid()}"

# How many jobs to process per "page chunk" before checking for pause.
# Phase 7 will replace this with actual per-page HTTP fetching.
FULLSCAN_CHUNK_SIZE = 50

# Estimated capacity for BF.RESERVE (jobs per company full scan)
FULLSCAN_BLOOM_CAPACITY = 10_000

# Seconds to wait between pause-polling loops
FULLSCAN_PAUSE_POLL_SECS = 10

# Re-queue delay when adaptive-first pre-check fires (Rule 4)
FULLSCAN_ADAPTIVE_FIRST_DELAY_S = 900   # 15 minutes


# ─────────────────────────────────────────
# BLOOM FILTER ABSTRACTION
# ─────────────────────────────────────────

class _BloomFilter:
    """
    Thin wrapper around RedisBloom BF.* commands with automatic Redis SET fallback.

    If the RedisBloom module is available:  uses BF.ADD / BF.EXISTS
    Otherwise:                              falls back to SADD / SISMEMBER
                                            on a TTL'd key (bloom:fallback:{company})

    The fallback SET is a true exact-match structure (no false positives,
    but higher memory than a real Bloom filter). For typical fullscan sizes
    (≤ 10,000 job IDs), the memory difference is negligible.
    """

    def __init__(self, r, company: str):
        self.r           = r
        self._bf_key     = f"bloom:fullscan:{company}"
        self._fb_key     = f"bloom:fallback:{company}"
        self._use_bf: Optional[bool] = None   # None = not probed yet

    def _probe(self) -> bool:
        """Detect RedisBloom availability on first call."""
        if self._use_bf is not None:
            return self._use_bf
        try:
            self.r.execute_command("BF.EXISTS", self._bf_key, "_probe_")
            self._use_bf = True
        except Exception:
            self._use_bf = False
            logger.debug(
                "bloom: RedisBloom unavailable — using SET fallback "
                "(%s → %s)", self._bf_key, self._fb_key,
            )
        return self._use_bf

    def exists(self, job_id: str) -> bool:
        """Return True if job_id is (probably) in the filter."""
        if self._probe():
            try:
                return bool(
                    self.r.execute_command("BF.EXISTS", self._bf_key, job_id)
                )
            except Exception:
                self._use_bf = False
        return bool(self.r.sismember(self._fb_key, job_id))

    def add(self, job_id: str) -> None:
        """Add job_id to the filter and refresh its TTL."""
        if self._probe():
            try:
                self.r.execute_command("BF.ADD", self._bf_key, job_id)
                self.r.expire(self._bf_key, FULLSCAN_BLOOM_TTL)
                return
            except Exception:
                self._use_bf = False
        self.r.sadd(self._fb_key, job_id)
        self.r.expire(self._fb_key, FULLSCAN_BLOOM_TTL)

    def initialize(self) -> None:
        """
        Pre-create the Bloom filter with estimated capacity.
        Safe to call on resume (BF.RESERVE fails gracefully if key exists).
        """
        if self._probe():
            try:
                self.r.execute_command(
                    "BF.RESERVE",
                    self._bf_key,
                    str(FULLSCAN_BLOOM_ERROR_RATE),
                    str(FULLSCAN_BLOOM_CAPACITY),
                )
                self.r.expire(self._bf_key, FULLSCAN_BLOOM_TTL)
                return
            except Exception:
                pass   # key may already exist; that's fine on resume
        # Fallback SET: just ensure TTL is set
        self.r.expire(self._fb_key, FULLSCAN_BLOOM_TTL)

    def delete(self) -> None:
        """Delete the filter completely (fresh scan — not a resume)."""
        self.r.delete(self._bf_key)
        self.r.delete(self._fb_key)

    def extend_ttl(self) -> None:
        """Refresh TTL on both the BF key and its fallback."""
        self.r.expire(self._bf_key, FULLSCAN_BLOOM_TTL)
        self.r.expire(self._fb_key, FULLSCAN_BLOOM_TTL)


# ─────────────────────────────────────────
# REDIS HELPERS
# ─────────────────────────────────────────

def _is_paused(r) -> bool:
    """
    Return True if the nightly maintenance window is active.

    Checks db:maintenance key — set by redis_signal.py cmd_pause()
    and cleared by cmd_resume(). No TTL (must be explicitly cleared).
    """
    return bool(r.exists(REDIS_DB_MAINTENANCE))


def _acquire_lock(company: str, r) -> bool:
    """
    Acquire exclusive full-scan lock for company.

    Uses SET NX EX to prevent two fullscan workers from scanning the same
    company simultaneously (possible if worker restarts or two processes run).

    Returns True if lock acquired, False if already held.
    """
    key = f"fullscan:lock:{company}"
    return bool(
        r.set(key, WORKER_ID, nx=True, ex=SCHEDULER_FULL_SCAN_LOCK_TTL)
    )


def _release_lock(company: str, r) -> None:
    """Release the fullscan lock (only if we hold it)."""
    key = f"fullscan:lock:{company}"
    # Only delete if our worker_id holds the lock (safe-delete).
    # r.get() may return bytes (redis-py without decode_responses) or str —
    # normalise to str before comparing with the WORKER_ID str constant.
    current = r.get(key)
    if isinstance(current, (bytes, bytearray)):
        current = current.decode()
    if current == WORKER_ID:
        r.delete(key)


def _defer_adaptive_first(company: str, r) -> None:
    """
    Rule 4: adaptive poll hasn't happened this cycle yet.

    Trigger adaptive immediately, re-queue fullscan in 15 minutes.
    """
    now = time.time()
    r.zadd(REDIS_POLL_ADAPTIVE, {company: now})           # trigger now
    r.zadd(REDIS_POLL_FULLSCAN, {company: now + FULLSCAN_ADAPTIVE_FIRST_DELAY_S})
    logger.info(
        "fullscan [%s]: Rule 4 — adaptive-first; "
        "re-scheduling fullscan in %ds",
        company, FULLSCAN_ADAPTIVE_FIRST_DELAY_S,
    )


def _get_cycle_start(r) -> Optional[float]:
    """Return current cycle:start Unix timestamp, or None if not set."""
    val = r.get(REDIS_CYCLE_START)
    return float(val) if val else None


# ─────────────────────────────────────────
# DB HELPERS (fullscan-specific)
# ─────────────────────────────────────────

def _get_fullscan_state(company: str) -> dict:
    """
    Fetch fullscan-relevant columns from company_poll_stats.

    Returns dict with keys:
        full_scan_interrupted   bool
        interrupted_at_page     int | None
        full_scan_interval_s    int
        last_poll_at            datetime | None   (for Rule 4 check)
        last_full_scan_at       datetime | None
    Returns empty dict if company not in company_poll_stats yet.
    """
    conn = get_conn()
    try:
        row = conn.execute("""
            SELECT full_scan_interrupted, interrupted_at_page,
                   full_scan_interval_s, last_poll_at, last_full_scan_at
            FROM company_poll_stats
            WHERE company = %s
        """, (company,)).fetchone()
        if not row:
            return {}
        return {
            "full_scan_interrupted": bool(row["full_scan_interrupted"]),
            "interrupted_at_page":   row["interrupted_at_page"],
            "full_scan_interval_s":  row["full_scan_interval_s"] or SCHEDULER_FULL_SCAN_INTERVAL_S,
            "last_poll_at":          row["last_poll_at"],
            "last_full_scan_at":     row["last_full_scan_at"],
        }
    finally:
        conn.close()


def _write_checkpoint(company: str, page_num: int) -> None:
    """
    Persist a pause checkpoint to company_poll_stats.

    Marks full_scan_interrupted=TRUE and records the current page so we
    have accurate stats on resume. The Bloom filter is the real resume
    state — this checkpoint is for DB integrity and observability.
    """
    conn = get_conn()
    try:
        conn.execute("""
            UPDATE company_poll_stats SET
                full_scan_interrupted = TRUE,
                interrupted_at_page   = %s,
                interrupted_at        = NOW(),
                updated_at            = NOW()
            WHERE company = %s
        """, (page_num, company))
        conn.commit()
    finally:
        conn.close()


def _complete_fullscan_db(
    company: str,
    platform: str,
    new_jobs: int,
    interval_s: int,
) -> None:
    """
    Update company_poll_stats on successful full scan completion.

    Sets last_full_scan_at, next_full_scan_at, clears interrupted flag,
    and increments total_new_jobs.
    """
    conn = get_conn()
    try:
        conn.execute("""
            UPDATE company_poll_stats SET
                last_full_scan_at     = NOW(),
                next_full_scan_at     = NOW() + (%s * INTERVAL '1 second'),
                full_scan_interrupted = FALSE,
                interrupted_at_page   = NULL,
                interrupted_at        = NULL,
                total_new_jobs        = total_new_jobs + %s,
                updated_at            = NOW()
            WHERE company = %s
        """, (interval_s, new_jobs, company))
        conn.commit()
    except Exception as exc:
        logger.error(
            "fullscan: _complete_fullscan_db failed for %r: %s",
            company, exc, exc_info=True,
        )
    finally:
        conn.close()


# ─────────────────────────────────────────
# DETAIL QUEUE PAYLOAD BUILDER
# ─────────────────────────────────────────

def _build_detail_payload(
    company: str,
    platform: str,
    job: dict,
    slug_info,
) -> dict:
    """
    Build the payload pushed to queue:detail:fullscan.

    Mirrors scan_worker._build_detail_payload() but marks found_by as
    'tier2_fullscan' so detail_worker and the digest can attribute the
    discovery correctly.
    """
    payload = {
        "company":      company,
        "ats_platform": platform,
        "job_id":       job.get("job_id"),
        "job_url":      job.get("job_url", ""),
        "title":        job.get("title", ""),
        "location":     job.get("location", ""),
        "posted_at":    (
            job["posted_at"].isoformat()
            if hasattr(job.get("posted_at"), "isoformat")
            else job.get("posted_at")
        ),
        "description":  job.get("description", ""),
        "content_hash": job.get("content_hash"),
        "skill_score":  job.get("skill_score", 0),
        "found_by":     "tier2_fullscan",
        "enqueued_at":  datetime.now(timezone.utc).isoformat(),
        "slug_info":    (
            slug_info
            if isinstance(slug_info, (str, dict, type(None)))
            else str(slug_info)
        ),
    }

    # Forward platform-specific detail keys (same map as scan_worker)
    PLATFORM_DETAIL_KEYS = {
        "workday":         ["_external_path"],
        "icims":           ["_base_url", "_feed_type"],
        "jobvite":         ["_slug"],
        "taleo":           ["_contest_no"],
        "smartrecruiters": ["_company_slug"],
        "sitemap":         ["_feed_type"],
    }
    for key in PLATFORM_DETAIL_KEYS.get(platform, []):
        if job.get(key) is not None:
            payload[key] = job[key]

    if job.get("_country_code"):
        payload["_country_code"] = job["_country_code"]

    return payload


# ─────────────────────────────────────────
# CORE: run one full scan
# ─────────────────────────────────────────

def _run_fullscan(company: str, r, skip_lock: bool = False) -> dict:
    """
    Perform one complete full scan for a company.

    Fetches every job page, compares against the per-company Bloom filter
    and the seen:{company} SET, and pushes new job IDs to queue:detail:fullscan.

    Returns a result dict summarising the outcome. Never raises — all
    exceptions are caught and logged.

    Args:
        company:   Company name (must match prospective_companies.company).
        r:         Redis connection.
        skip_lock: If True, bypass the exclusivity lock (dev/debug only).

    Returns:
        dict with keys: company, success, new_jobs, fetched, pages,
        duration_ms, outcome (completed/paused/error/deferred/skipped),
        worker_id, completed_at.
    """
    start_mono = time.monotonic()

    result: dict = {
        "company":      company,
        "success":      False,
        "new_jobs":     0,
        "fetched":      0,
        "pages":        0,
        "duration_ms":  0,
        "outcome":      "error",
        "worker_id":    WORKER_ID,
        "completed_at": datetime.now(timezone.utc).isoformat(),
    }

    # ── 1. Company metadata ───────────────────────────────────────────────────
    company_row = get_company_row(company)
    if not company_row:
        logger.warning("fullscan [%s]: company not found in DB — skipping", company)
        result["outcome"] = "skipped"
        return result

    platform = company_row.get("ats_platform", "unknown")
    slug     = company_row.get("ats_slug")

    if platform == "unknown" or not slug:
        logger.warning(
            "fullscan [%s]: unknown ATS or missing slug — skipping", company
        )
        result["outcome"] = "skipped"
        return result

    # ── 2. ATS module + config ────────────────────────────────────────────────
    config     = get_config(platform)
    ats_module = get_ats_module(platform)
    if not ats_module:
        logger.error(
            "fullscan [%s]: no ATS module for platform=%s", company, platform
        )
        return result

    slug_info = parse_slug(platform, slug, config)

    # ── 3. Rule 4 — adaptive-first check ─────────────────────────────────────
    # If this company hasn't had its adaptive poll this cycle, trigger adaptive
    # first and defer the full scan by 15 minutes.
    cycle_start = _get_cycle_start(r)
    if cycle_start is not None:
        fs_state  = _get_fullscan_state(company)
        last_poll = fs_state.get("last_poll_at")

        if last_poll is not None:
            last_poll_ts = (
                last_poll.timestamp()
                if hasattr(last_poll, "timestamp")
                else float(last_poll)
            )
            if last_poll_ts < cycle_start:
                _defer_adaptive_first(company, r)
                result["outcome"] = "deferred"
                return result
    else:
        # cycle:start not set → --monitor-jobs hasn't run yet today.
        # Proceed with full scan anyway (best-effort).
        fs_state = _get_fullscan_state(company)

    # ── 4. Exclusive lock ─────────────────────────────────────────────────────
    if not skip_lock:
        if not _acquire_lock(company, r):
            logger.info(
                "fullscan [%s]: lock held by another worker — skipping",
                company,
            )
            result["outcome"] = "skipped"
            return result

    try:
        # ── 5. Heartbeat + progress ───────────────────────────────────────────
        set_heartbeat(company)
        set_progress(company, "fullscan_start")

        is_resume    = fs_state.get("full_scan_interrupted", False)
        interval_s   = fs_state.get("full_scan_interval_s", SCHEDULER_FULL_SCAN_INTERVAL_S)
        sorted_plat  = config.get("sorted_by_recency", False)

        logger.info(
            "fullscan [%s] starting | platform=%s sorted=%s resume=%s",
            company, platform, sorted_plat, is_resume,
        )

        # ── 6. Bloom filter setup ─────────────────────────────────────────────
        bloom = _BloomFilter(r, company)
        if is_resume:
            # Resume: reuse existing Bloom filter — it tracks what was already
            # queued before the pause. Extend TTL so it doesn't expire mid-run.
            bloom.initialize()   # no-op if BF already exists; sets TTL on SET
            bloom.extend_ttl()
            logger.info(
                "fullscan [%s]: resuming from page=%s (bloom filter preserved)",
                company, fs_state.get("interrupted_at_page"),
            )
        else:
            # Fresh scan: delete any stale filter from a previous cycle.
            bloom.delete()
            bloom.initialize()

        # ── 7. Fetch all jobs ─────────────────────────────────────────────────
        # Phase 7 will replace this with page-by-page streaming so we can
        # honour pause signals between HTTP requests. Until then, we fetch
        # all pages in one call and process in FULLSCAN_CHUNK_SIZE chunks.
        set_progress(company, "fullscan_fetching")
        raw_jobs = ats_module.fetch_jobs(slug_info, company)
        set_progress(company, "fullscan_processing")

        valid_jobs = [
            j for j in raw_jobs
            if j.get("job_url") and j.get("job_id")
        ]
        dropped = len(raw_jobs) - len(valid_jobs)
        if dropped:
            logger.debug(
                "fullscan [%s]: dropped %d jobs missing job_url/job_id",
                company, dropped,
            )

        result["fetched"] = len(valid_jobs)

        # ── 8. Apply listing-level title filter (same as scan_worker) ─────────
        listing_filter = config.get("listing_filter", "full")
        if listing_filter == "title_only":
            title_matched = filter_jobs_title_only(valid_jobs)
        else:
            title_matched = filter_jobs(valid_jobs)

        # ── 9. Intra-scan dedup set (catches duplicates across pages) ─────────
        cycle_seen: set = set()

        # Load seen:{company} once for the whole scan.
        seen_key = f"seen:{company}"
        seen_ids = r.smembers(seen_key)

        # ── 10. Page-chunk processing loop ───────────────────────────────────
        new_count    = 0
        page_num     = 0
        paused       = False
        early_exit   = False
        overlap_pages = 0

        # Backpressure: check detail queue depth before starting
        queue_depth = r.llen(REDIS_DETAIL_FULLSCAN)
        if queue_depth > DETAIL_QUEUE_MAX_FULLSCAN:
            logger.warning(
                "fullscan [%s]: detail queue backpressure "
                "(depth=%d > max=%d) — pausing until queue drains",
                company, queue_depth, DETAIL_QUEUE_MAX_FULLSCAN,
            )
            # Wait until queue has headroom (up to 5 minutes)
            waited = 0
            while (r.llen(REDIS_DETAIL_FULLSCAN) > DETAIL_QUEUE_MAX_FULLSCAN
                   and waited < 300):
                time.sleep(10)
                waited += 10
                if _is_paused(r):
                    break

        # Chunk the flat job list to simulate page-level pause checks.
        # Phase 7 will replace this outer loop with actual paginated HTTP fetches.
        for chunk_start in range(0, max(len(title_matched), 1), FULLSCAN_CHUNK_SIZE):
            chunk = title_matched[chunk_start:chunk_start + FULLSCAN_CHUNK_SIZE]
            if not chunk:
                break

            # ── Pause check (between chunks) ─────────────────────────────────
            if _is_paused(r):
                logger.info(
                    "fullscan [%s]: maintenance pause detected at page=%d",
                    company, page_num,
                )
                paused = True
                break

            # ── Smart early exit for SORTED platforms ─────────────────────────
            if sorted_plat:
                should_go_on, overlap_pages = should_continue_paginating(
                    chunk, seen_ids, overlap_pages, sorted_by_recency=True,
                )
                if not should_go_on:
                    early_exit = True
                    logger.debug(
                        "fullscan [%s]: early exit at page=%d "
                        "(80%%/2-page threshold)",
                        company, page_num,
                    )
                    break

            # ── Process jobs in this chunk ────────────────────────────────────
            for job in chunk:
                job_id = job.get("job_id")
                if not job_id:
                    continue

                # Intra-scan dedup
                if job_id in cycle_seen:
                    continue
                cycle_seen.add(job_id)

                # Skip if already in Bloom filter (queued in this scan cycle)
                if bloom.exists(job_id):
                    continue

                # Skip if already in seen:{company} (known from adaptive polling)
                if job_id in seen_ids:
                    continue

                # ── Genuinely new job for fullscan ────────────────────────────
                inserted = save_pending_detail(
                    company, platform, job, found_by="tier2_fullscan"
                )

                if inserted:
                    detail_payload = _build_detail_payload(
                        company, platform, job, slug_info,
                    )
                    r.lpush(REDIS_DETAIL_FULLSCAN, json.dumps(detail_payload))
                    new_count += 1

                # Add to Bloom filter regardless of whether DB insert succeeded
                # (ON CONFLICT DO NOTHING means it was already there — still
                # should be marked as seen in this scan cycle).
                bloom.add(job_id)

            set_progress(company, f"fullscan_page_{page_num}")
            page_num += 1

        result["pages"]    = page_num
        result["new_jobs"] = new_count

        # ── 11. Depth/waste stats ─────────────────────────────────────────────
        depth_stats = estimate_scan_depth(
            total_fetched=len(valid_jobs),
            new_found=new_count,
            early_exit=early_exit,
        )
        logger.info(
            "fullscan [%s] done | platform=%s fetched=%d new=%d "
            "pages=%d waste=%.0f%% early_exit=%s paused=%s",
            company, platform,
            depth_stats["total_fetched"],
            depth_stats["new_found"],
            page_num,
            depth_stats["waste_ratio"] * 100,
            early_exit,
            paused,
        )

        # ── 12. Pause checkpoint OR completion ────────────────────────────────
        now = time.time()

        if paused:
            # Write checkpoint and re-queue with score=now-1 (immediately due)
            _write_checkpoint(company, page_num)
            r.zadd(REDIS_POLL_FULLSCAN, {company: now - 1})

            result["outcome"]     = "paused"
            result["success"]     = True
            result["duration_ms"] = int((time.monotonic() - start_mono) * 1000)
            result["completed_at"] = datetime.now(timezone.utc).isoformat()

            logger.info(
                "fullscan [%s]: checkpointed at page=%d — "
                "re-queued with score=now-1 (immediately due on resume)",
                company, page_num,
            )

        else:
            # ── Successful completion ─────────────────────────────────────────
            # Extend Bloom filter TTL (it will be reused for dedup if
            # scan runs again within 36h, e.g. if interval < BLOOM_TTL).
            bloom.extend_ttl()

            # Update DB: last_full_scan_at, next_full_scan_at, clear interrupted
            _complete_fullscan_db(company, platform, new_count, interval_s)

            # Reschedule in poll:fullscan ZSET
            next_scan_at = now + interval_s
            r.zadd(REDIS_POLL_FULLSCAN, {company: next_scan_at})

            result["outcome"]     = "completed"
            result["success"]     = True
            result["duration_ms"] = int((time.monotonic() - start_mono) * 1000)
            result["completed_at"] = datetime.now(timezone.utc).isoformat()

            logger.info(
                "fullscan [%s]: completed — new=%d next_scan_in=%dh",
                company, new_count, interval_s // 3600,
            )

    except Exception as exc:
        result["duration_ms"] = int((time.monotonic() - start_mono) * 1000)
        result["outcome"]     = "error"
        logger.error(
            "fullscan [%s] unhandled error: %s", company, exc, exc_info=True,
        )
        # Re-queue with 1h delay to avoid tight crash loops
        r.zadd(REDIS_POLL_FULLSCAN, {company: time.time() + 3600})

    finally:
        clear_heartbeat(company)
        if not skip_lock:
            _release_lock(company, r)

    return result


# ─────────────────────────────────────────
# MAIN DISPATCH LOOP
# ─────────────────────────────────────────

def run_worker(once: bool = False, skip_lock: bool = False) -> None:
    """
    Main fullscan worker loop.

    Polls poll:fullscan ZSET for companies whose score <= now (i.e. overdue).
    Processes one company per iteration (full scans are expensive — no
    parallel dispatch here). Respects maintenance pauses.

    Args:
        once:      If True, process at most one company then exit.
        skip_lock: If True, bypass the exclusivity lock (dev/debug only).
    """
    init_db()

    if not ping():
        redis_url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
        logger.error("fullscan: Redis not reachable at %s — aborting", redis_url)
        print(f"[fullscan] ERROR: Redis unreachable ({redis_url})")
        sys.exit(1)

    r = get_redis()

    logger.info("fullscan worker started | worker_id=%s once=%s skip_lock=%s",
                WORKER_ID, once, skip_lock)
    print(f"[fullscan] Ready — worker={WORKER_ID}")
    print(f"[fullscan] Polling {REDIS_POLL_FULLSCAN!r} (score-ascending)")
    if once:
        print("[fullscan] --once mode: process one company then exit")
    elif skip_lock:
        print("[fullscan] --skip-lock mode (dev only)")
    else:
        print("[fullscan] Press Ctrl+C to stop\n")

    processed = 0

    while True:
        try:
            # ── Pause check ───────────────────────────────────────────────────
            if _is_paused(r):
                logger.debug("fullscan: maintenance window active — sleeping %ds",
                             FULLSCAN_PAUSE_POLL_SECS)
                time.sleep(FULLSCAN_PAUSE_POLL_SECS)
                continue

            # ── Pop next due company (score <= now) ───────────────────────────
            now = time.time()
            # ZPOPMIN returns [(member, score), ...] — take 1 entry
            due = r.zrangebyscore(
                REDIS_POLL_FULLSCAN, "-inf", now,
                start=0, num=1, withscores=True,
            )

            if not due:
                if once and processed == 0:
                    print("[fullscan] No companies due — exiting (--once)")
                    break
                if once:
                    break
                # Nothing due — sleep briefly and retry
                time.sleep(WORKER_BLOCK_SECS)
                continue

            company, score = due[0]

            # Atomic pop — prevent double processing if multiple workers run
            removed = r.zrem(REDIS_POLL_FULLSCAN, company)
            if not removed:
                # Another worker beat us to this company
                continue

            result    = _run_fullscan(company, r, skip_lock=skip_lock)
            processed += 1

            outcome = result["outcome"]
            icon    = {
                "completed": "[DONE]",
                "paused":    "[PAUSE]",
                "deferred":  "[DEFER]",
                "skipped":   "[SKIP]",
                "error":     "[ERR]",
            }.get(outcome, "[?]")

            print(
                f"  {icon} {company} — "
                f"{result['new_jobs']} new / {result['fetched']} fetched "
                f"({result['pages']} pages, {result['duration_ms']}ms)"
            )

            if once:
                break

        except KeyboardInterrupt:
            logger.info("fullscan: KeyboardInterrupt — shutting down")
            print("\n[fullscan] Shutting down.")
            break

        except Exception as exc:
            logger.error(
                "fullscan: unexpected loop error: %s", exc, exc_info=True,
            )
            if once:
                break
            time.sleep(5)

    logger.info("fullscan worker shutdown | processed=%d worker_id=%s",
                processed, WORKER_ID)


# ─────────────────────────────────────────
# CLI
# ─────────────────────────────────────────

if __name__ == "__main__":
    once      = "--once"      in sys.argv
    skip_lock = "--skip-lock" in sys.argv
    run_worker(once=once, skip_lock=skip_lock)
