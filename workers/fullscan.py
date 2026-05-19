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

─── Bloom filter pair ────────────────────────────────────────────────────────

    OLD bloom  bloom:fullscan:{company}      — authoritative (last completed scan)
    NEW bloom  bloom:fullscan:new:{company}  — being built this cycle
    Fallback   bloom:fallback:{company}      — Redis SET if RedisBloom unavailable
    TTL = 36h (FULLSCAN_BLOOM_TTL)

    OLD bloom: read-only during the scan.  Used to skip DB checks for jobs
    already known from the last cycle (speedup), and read by adaptive scan
    workers for early-exit overlap detection (Section 11).

    NEW bloom: ALL currently fetched job IDs are added, regardless of whether
    they were in the OLD bloom or are genuinely new.  Closed/filled jobs fall
    out naturally — they are no longer fetched so they never enter NEW bloom.

    On completion: NEW bloom is promoted to OLD (DEL old, RENAME new → old).
    On resume:     both keys are preserved; NEW bloom provides intra-scan dedup
                   so jobs queued before the pause are not re-queued.

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
    REDIS_ADAPTIVE_SEEN_PREFIX,
    ADAPTIVE_SEEN_TTL,
    # Stream-based delivery (two-layer scheduler redesign)
    REDIS_STREAM_FULLSCAN,
    STREAM_CONSUMER_GROUP,
    STREAM_BLOCK_MS,
    WARMING_POLLS_COUNT,
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

WORKER_ID      = f"{socket.gethostname()}:{os.getpid()}"
_CONSUMER_NAME = f"worker-{socket.gethostname()}-{os.getpid()}"

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

class _BloomPair:
    """
    Two-key Bloom filter pair for per-company full scan (Section 9 / 13).

    OLD key (bloom:fullscan:{company}):
        Authoritative state from the last *completed* scan.  Read-only during
        the current scan — used to skip DB checks for already-known jobs
        (speedup) and read by adaptive scan for early exit (Section 11).

    NEW key (bloom:fullscan:new:{company}):
        Being built fresh during the current scan.  ALL currently active job
        IDs are added to it regardless of whether they were known.  On scan
        completion it is promoted to OLD so closed/filled jobs fall out
        naturally (they were not fetched → not in NEW → absent next cycle).

    Resume behaviour (interrupted scan):
        OLD key: still present — continues to serve as DB-skip reference.
        NEW key: partially built — new_exists() used for intra-scan dedup so
                 jobs processed before the pause are not re-queued.

    RedisBloom / SET fallback:
        Uses BF.* commands when available; falls back to SADD/SISMEMBER on
        TTL'd keys (bloom:fallback:{company} / bloom:fallback:new:{company}).
        The fallback is exact-match (no false positives) but uses more memory.
    """

    # NEW bloom needs a slightly longer TTL than OLD so it survives slow scans.
    _NEW_TTL = FULLSCAN_BLOOM_TTL + 3600   # 37h

    def __init__(self, r, company: str):
        self.r        = r
        self._old_bf  = f"bloom:fullscan:{company}"
        self._new_bf  = f"bloom:fullscan:new:{company}"
        self._old_fb  = f"bloom:fallback:{company}"
        self._new_fb  = f"bloom:fallback:new:{company}"
        self._use_bf: Optional[bool] = None

    # ── RedisBloom probe ──────────────────────────────────────────────────────

    def _probe(self) -> bool:
        """Detect RedisBloom availability (cached after first call)."""
        if self._use_bf is not None:
            return self._use_bf
        try:
            self.r.execute_command("BF.EXISTS", self._old_bf, "_probe_")
            self._use_bf = True
        except Exception:
            self._use_bf = False
            logger.debug("bloom: RedisBloom unavailable — using SET fallback")
        return self._use_bf

    # ── OLD bloom reads ───────────────────────────────────────────────────────

    def old_exists(self, job_id: str) -> bool:
        """True if job_id was in the last completed full scan (old bloom)."""
        if self._probe():
            try:
                return bool(
                    self.r.execute_command("BF.EXISTS", self._old_bf, job_id)
                )
            except Exception:
                self._use_bf = False
        return bool(self.r.sismember(self._old_fb, job_id))

    def old_exists_fn(self):
        """Return a callable suitable for paginator.should_continue_paginating."""
        return self.old_exists

    # ── NEW bloom reads + writes ──────────────────────────────────────────────

    def new_exists(self, job_id: str) -> bool:
        """True if job_id was already processed in this scan cycle (new bloom)."""
        if self._probe():
            try:
                return bool(
                    self.r.execute_command("BF.EXISTS", self._new_bf, job_id)
                )
            except Exception:
                self._use_bf = False
        return bool(self.r.sismember(self._new_fb, job_id))

    def new_add(self, job_id: str) -> None:
        """Add job_id to the NEW bloom (call for EVERY fetched job ID)."""
        if self._probe():
            try:
                self.r.execute_command("BF.ADD", self._new_bf, job_id)
                return
            except Exception:
                self._use_bf = False
        self.r.sadd(self._new_fb, job_id)

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    def prepare_fresh(self) -> None:
        """
        Prepare for a FRESH full scan (not a resume).

        Keeps OLD key as-is (DB-skip reference for this scan cycle).
        Wipes NEW key and pre-creates it for the current scan.
        """
        self.r.delete(self._new_bf)
        self.r.delete(self._new_fb)
        if self._probe():
            try:
                self.r.execute_command(
                    "BF.RESERVE",
                    self._new_bf,
                    str(FULLSCAN_BLOOM_ERROR_RATE),
                    str(FULLSCAN_BLOOM_CAPACITY),
                )
            except Exception:
                pass  # BF.RESERVE failed — BF.ADD will auto-create
        self.r.expire(self._new_bf, self._NEW_TTL)
        self.r.expire(self._new_fb, self._NEW_TTL)

    def prepare_resume(self) -> None:
        """
        Prepare for a RESUMED scan (after pause checkpoint).

        Both OLD and NEW keys are still present; extend TTL so they don't
        expire during a long-running resumed scan.
        """
        self.r.expire(self._old_bf, FULLSCAN_BLOOM_TTL)
        self.r.expire(self._old_fb, FULLSCAN_BLOOM_TTL)
        self.r.expire(self._new_bf, self._NEW_TTL)
        self.r.expire(self._new_fb, self._NEW_TTL)
        logger.debug(
            "bloom: resume — OLD=%s NEW=%s TTLs extended",
            self._old_bf, self._new_bf,
        )

    def finalize(self) -> None:
        """
        Promote NEW bloom → OLD on scan completion.

        DEL old keys, RENAME new keys to old keys, set authoritative TTL.
        The promoted OLD bloom now represents the complete current board state
        and will be read by adaptive scans for early exit until the next
        full scan cycle rebuilds it.
        """
        self.r.delete(self._old_bf)
        self.r.delete(self._old_fb)
        try:
            self.r.rename(self._new_bf, self._old_bf)
            self.r.expire(self._old_bf, FULLSCAN_BLOOM_TTL)
        except Exception:
            pass  # new_bf may not exist if scan found 0 jobs
        try:
            self.r.rename(self._new_fb, self._old_fb)
            self.r.expire(self._old_fb, FULLSCAN_BLOOM_TTL)
        except Exception:
            pass


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
# STREAM CONSUMER GROUP INIT
# ─────────────────────────────────────────

def _ensure_consumer_group(r) -> None:
    """
    Ensure the consumer group exists for stream:fullscan (idempotent).

    Uses id='$' so only NEW messages are delivered — never replays history.
    BUSYGROUP means the group already exists; safe to ignore.
    MKSTREAM creates the stream key if it doesn't exist yet.
    """
    try:
        r.xgroup_create(
            REDIS_STREAM_FULLSCAN, STREAM_CONSUMER_GROUP,
            id="$", mkstream=True,
        )
    except Exception as exc:
        if "BUSYGROUP" not in str(exc):
            logger.warning("fullscan: xgroup_create error: %s", exc)


# ─────────────────────────────────────────
# WARMING BOOTSTRAP (first full scan)
# ─────────────────────────────────────────

def _bootstrap_warming_adaptive(company: str, r) -> None:
    """
    Bootstrap a brand-new company into the WARMING lifecycle.

    Called after the FIRST successful full scan completes
    (detected by last_poll_at IS NULL before the scan).

    Steps:
        1. Read initial_slot_offset_s from DB (set at registration time).
           Falls back to slot_offset(company_id) for legacy rows without it.
        2. Compute first_poll_at = today_midnight_eastern + initial_slot_offset_s.
           If that slot has already passed today, push to tomorrow's slot.
        3. Write warming_polls_remaining = WARMING_POLLS_COUNT to DB.
        4. ZADD company to poll:adaptive at first_poll_at.

    This replaces the old "ZADD with ADAPTIVE_DEFAULT_INTERVAL at now" logic so
    new companies are spread deterministically across the day instead of all
    clustering at the moment their first full scan finishes.
    """
    import pytz
    from datetime import datetime as _dt
    from workers.slot import slot_offset

    eastern = pytz.timezone("America/New_York")
    now_eastern = _dt.now(eastern)
    today_midnight = now_eastern.replace(hour=0, minute=0, second=0, microsecond=0)
    today_midnight_ts = today_midnight.timestamp()
    now_ts = time.time()

    # Fetch initial_slot_offset_s from DB
    conn = get_conn()
    try:
        row = conn.execute("""
            SELECT id, initial_slot_offset_s
            FROM company_poll_stats
            WHERE company = %s
        """, (company,)).fetchone()
    finally:
        conn.close()

    if row and row["initial_slot_offset_s"] is not None:
        offset_s = int(row["initial_slot_offset_s"])
    elif row:
        # Legacy row — derive offset from company_poll_stats.id
        offset_s = slot_offset(row["id"])
        logger.debug(
            "_bootstrap_warming_adaptive: %r has no initial_slot_offset_s — "
            "using slot_offset(id=%d) = %ds",
            company, row["id"], offset_s,
        )
    else:
        offset_s = slot_offset(company)   # fallback: hash of company name

    first_poll_at = today_midnight_ts + offset_s
    if first_poll_at <= now_ts:
        first_poll_at += 86400   # push to tomorrow's slot

    first_poll_dt = _dt.fromtimestamp(first_poll_at)

    conn = get_conn()
    try:
        conn.execute("""
            UPDATE company_poll_stats
            SET warming_polls_remaining = %s,
                next_poll_at            = %s,
                updated_at              = CURRENT_TIMESTAMP
            WHERE company = %s
        """, (WARMING_POLLS_COUNT, first_poll_dt, company))
        conn.commit()
    finally:
        conn.close()

    r.zadd(REDIS_POLL_ADAPTIVE, {company: first_poll_at})

    logger.info(
        "fullscan: WARMING bootstrap for %r — "
        "warming_polls=%d first_poll_at=%s (offset=%ds, in %.1fh)",
        company, WARMING_POLLS_COUNT,
        first_poll_dt.strftime("%H:%M"),
        offset_s,
        (first_poll_at - now_ts) / 3600,
    )


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

    Fetches every job page, uses the Bloom filter pair for DB-check speedup,
    and pushes genuinely new job IDs to queue:detail:fullscan.

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

        # ── 6. Bloom filter + adaptive_seen setup ────────────────────────────────
        bloom = _BloomPair(r, company)
        adaptive_seen_key = f"{REDIS_ADAPTIVE_SEEN_PREFIX}:{company}"

        if is_resume:
            # Resume: OLD bloom (last completed scan) and NEW bloom (partially
            # built before pause) are both still present. Extend TTLs.
            bloom.prepare_resume()
            logger.info(
                "fullscan [%s]: resuming from page=%s "
                "(old+new bloom preserved)",
                company, fs_state.get("interrupted_at_page"),
            )
        else:
            # Fresh scan: keep OLD bloom as DB-skip reference, wipe NEW bloom,
            # and clear today's adaptive_seen cache.
            bloom.prepare_fresh()
            r.delete(adaptive_seen_key)
            logger.info(
                "fullscan [%s]: fresh scan — NEW bloom cleared, "
                "adaptive_seen cleared",
                company,
            )

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

        # ── 9. Intra-scan dedup (Python set — one scan run fits in memory) ───────
        cycle_seen: set = set()

        # ── 10. Page-chunk processing loop ───────────────────────────────────
        new_count     = 0
        page_num      = 0
        paused        = False
        early_exit    = False
        overlap_pages = 0

        # Backpressure: check detail queue depth before starting
        queue_depth = r.llen(REDIS_DETAIL_FULLSCAN)
        if queue_depth > DETAIL_QUEUE_MAX_FULLSCAN:
            logger.warning(
                "fullscan [%s]: detail queue backpressure "
                "(depth=%d > max=%d) — pausing until queue drains",
                company, queue_depth, DETAIL_QUEUE_MAX_FULLSCAN,
            )
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
            # Uses OLD bloom (last completed scan's board state) — contains ALL
            # jobs that were active then, so the 80% threshold is meaningful.
            if sorted_plat:
                should_go_on, overlap_pages = should_continue_paginating(
                    chunk, bloom.old_exists_fn(), overlap_pages,
                    sorted_by_recency=True,
                )
                if not should_go_on:
                    early_exit = True
                    logger.debug(
                        "fullscan [%s]: early exit at page=%d "
                        "(80%%/2-page bloom threshold)",
                        company, page_num,
                    )
                    break

            # ── Process jobs in this chunk ────────────────────────────────────
            for job in chunk:
                job_id = job.get("job_id")
                if not job_id:
                    continue

                # Layer 1: intra-scan dedup (Python set — catches pagination overlap)
                if job_id in cycle_seen:
                    continue
                cycle_seen.add(job_id)

                # Layer 2 (resume): already queued before pause → skip re-queuing
                if bloom.new_exists(job_id):
                    continue

                # Layer 3: OLD bloom — known from last completed scan.
                # Skip DB check (speedup); still add to NEW bloom below.
                if bloom.old_exists(job_id):
                    bloom.new_add(job_id)   # ALL active jobs go into NEW bloom
                    continue

                # Layer 4: DB check — source of truth for new/known decision.
                inserted = save_pending_detail(
                    company, platform, job, found_by="tier2_fullscan"
                )
                if inserted:
                    detail_payload = _build_detail_payload(
                        company, platform, job, slug_info,
                    )
                    r.lpush(REDIS_DETAIL_FULLSCAN, json.dumps(detail_payload))
                    new_count += 1

                # ALL fetched jobs go into NEW bloom regardless of outcome —
                # this ensures closed/filled jobs fall out naturally next cycle.
                bloom.new_add(job_id)

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
            # Capture whether this is the first full scan BEFORE updating DB
            # (last_poll_at IS NULL = company has never had an adaptive scan).
            is_first_fullscan = (fs_state.get("last_poll_at") is None)

            # Promote NEW bloom → OLD (DEL old, RENAME new → old, set TTL).
            # The promoted OLD bloom now reflects the complete current board
            # state and will be read by adaptive scans for early exit.
            bloom.finalize()

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

            # ── Bootstrap new companies into WARMING adaptive cycle ───────────
            # is_first_fullscan = last_poll_at was NULL before this scan ran.
            # Use the deterministic slot-offset approach so companies spread
            # across the day instead of all bootstrapping at midnight+0.
            if is_first_fullscan:
                try:
                    _bootstrap_warming_adaptive(company, r)
                except Exception as bw_exc:
                    # Non-fatal — fall back to immediate ZADD so company isn't lost
                    logger.error(
                        "fullscan [%s]: _bootstrap_warming_adaptive failed: %s — "
                        "falling back to immediate ZADD",
                        company, bw_exc,
                    )
                    from config import ADAPTIVE_DEFAULT_INTERVAL
                    r.zadd(REDIS_POLL_ADAPTIVE, {company: now + ADAPTIVE_DEFAULT_INTERVAL})

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

def run_worker(once: bool = False, skip_lock: bool = False,
               skip_init_db: bool = False) -> None:
    """
    Main fullscan worker loop — stream-based delivery (Section 9 redesign).

    Reads full-scan payloads from stream:fullscan via XREADGROUP (crash-safe):
        1. XREADGROUP COUNT 1 BLOCK 500ms — get next undelivered message
        2. Check maintenance pause (db:maintenance key)
        3. Run _run_fullscan() — Bloom filters, checkpoint/resume, detail queue
        4. XACK — remove from PEL, mark work complete

    _run_fullscan() handles all scheduling internally:
        - On success: ZADD poll:fullscan at next_scan_at
                      _bootstrap_warming_adaptive() for first-scan companies
        - On pause:   _write_checkpoint() + ZADD poll:fullscan at score=now-1
        - On error:   ZADD poll:fullscan with 1h retry

    If the worker dies between steps 3 and 4, the stream message stays in the
    PEL.  The scheduler's claim_stale_work() (XAUTOCLAIM with p95×3 idle
    timeout) reclaims it.  _run_fullscan()'s lock + interrupted_at_page
    checkpoint ensures the resumed scan doesn't duplicate work.

    Args:
        once:          If True, process at most one company then exit.
        skip_lock:     If True, bypass exclusivity lock (dev/debug only).
        skip_init_db:  If True, skip init_db() (parent process already did it).
    """
    if not skip_init_db:
        init_db()

    if not ping():
        redis_url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
        logger.error("fullscan: Redis not reachable at %s — aborting", redis_url)
        print(f"[fullscan] ERROR: Redis unreachable ({redis_url})")
        sys.exit(1)

    r = get_redis()
    _ensure_consumer_group(r)

    logger.info(
        "fullscan worker started | worker_id=%s consumer=%s stream=%s "
        "once=%s skip_lock=%s",
        WORKER_ID, _CONSUMER_NAME, REDIS_STREAM_FULLSCAN, once, skip_lock,
    )
    print(f"[fullscan] Ready — worker={WORKER_ID}")
    print(f"[fullscan] Consuming from {REDIS_STREAM_FULLSCAN!r} "
          f"group={STREAM_CONSUMER_GROUP!r}")
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
            # Full scans are long (minutes); polling db:maintenance every 10s
            # is fine (cheaper than adding pub/sub to an already-complex module).
            if _is_paused(r):
                logger.debug("fullscan: maintenance window active — sleeping %ds",
                             FULLSCAN_PAUSE_POLL_SECS)
                time.sleep(FULLSCAN_PAUSE_POLL_SECS)
                continue

            # ── XREADGROUP: block up to STREAM_BLOCK_MS for next message ──────
            stream_result = r.xreadgroup(
                STREAM_CONSUMER_GROUP,
                _CONSUMER_NAME,
                {REDIS_STREAM_FULLSCAN: ">"},
                count=1,
                block=STREAM_BLOCK_MS,
            )

            if not stream_result:
                if once and processed == 0:
                    print("[fullscan] Stream empty — exiting (--once)")
                    break
                if once:
                    break
                continue

            _stream_name, messages = stream_result[0]
            msg_id, fields = messages[0]

            company = fields.get("company", "")
            if not company:
                logger.warning("fullscan: received stream message with no company — XACK and skip")
                r.xack(REDIS_STREAM_FULLSCAN, STREAM_CONSUMER_GROUP, msg_id)
                continue

            # ── Run full scan ─────────────────────────────────────────────────
            result    = _run_fullscan(company, r, skip_lock=skip_lock)
            processed += 1

            # ── XACK: remove from PEL (work complete) ────────────────────────
            # _run_fullscan() handles all rescheduling internally before we get
            # here, so XACK unconditionally marks this delivery as done.
            r.xack(REDIS_STREAM_FULLSCAN, STREAM_CONSUMER_GROUP, msg_id)

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
