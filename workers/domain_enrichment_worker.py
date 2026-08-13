"""
workers/domain_enrichment_worker.py — Domain enrichment worker for H1B pipeline.

Reads from Redis ZSET domain_enrichment_queue (score = petition_count, highest first).
For each company FEIN:
  1. Resolves assigned_domain → public_domain (HTTP redirect / root fallback / CT log)
  2. Phase 3: probe career paths → careers_url
  3. Phase 6: scan careers_url for ATS platform + slug (bonus)
  4. Writes results to fein_domain_map (and company_ats if ATS found)
  5. Pushes to discovery_queue if petition_count > 0

Worker exits cleanly when queue is empty — not a perpetual daemon.
Started by:
  - fuzzy_match_uscis_dol.py   (after bulk queue population)
  - staleness_checker cron      (>90 days stale companies)
  - API endpoint                (on-demand user-triggered re-enrichment)

Usage:
  python -m workers.domain_enrichment_worker
  python -m workers.domain_enrichment_worker --once
"""

import json
import os
import sys
import time

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from config import (
    DOMAIN_ENRICHMENT_DELAYED,
    DOMAIN_ENRICHMENT_DLQ,
    DOMAIN_ENRICHMENT_QUEUE,
    DISCOVERY_QUEUE,
    ENRICHMENT_HEARTBEAT_S,
    ENRICHMENT_MAX_RETRIES,
    REDIS_DB_MAINTENANCE,
)
from db.connection import get_conn
from jobs.career_page import detect_via_career_page
from jobs.public_domain import discover_public_domain
from logger import get_logger, init_logging
from workers.heartbeat import Heartbeat
from workers.redis_client import get_redis

log = get_logger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# Phase 3 import — discover_careers_url lives in scripts/discover_h1b_ats.py
# ─────────────────────────────────────────────────────────────────────────────

try:
    from scripts.discover_h1b_ats import discover_careers_url as _discover_careers_url
    _PHASE3_AVAILABLE = True
except Exception as _e:
    log.warning("Phase 3 unavailable (import failed): %s — skipping career path probe", _e)
    _PHASE3_AVAILABLE = False


def _phase3(website_url: str) -> "tuple[str|None, str|None, str|None]":
    if not _PHASE3_AVAILABLE:
        return None, None, None
    try:
        return _discover_careers_url(website_url)
    except Exception as e:
        log.warning("Phase 3 error for %s: %s", website_url, e)
        return None, None, None


# ─────────────────────────────────────────────────────────────────────────────
# Maintenance window
# ─────────────────────────────────────────────────────────────────────────────

def _is_maintenance(r) -> bool:
    try:
        return bool(r.exists(REDIS_DB_MAINTENANCE))
    except Exception as exc:
        log.warning("Redis maintenance check failed (%s) — assuming not in maintenance", exc)
        return False


# ─────────────────────────────────────────────────────────────────────────────
# Delayed queue — certspotter 429 re-queue with not_before timestamp
# ─────────────────────────────────────────────────────────────────────────────

def _requeue_delayed(r, fein: str, petition_count: int, delay_s: int) -> None:
    """Push company to delayed ZSET scored by not_before timestamp."""
    payload = json.dumps({"fein": fein, "petition_count": petition_count})
    not_before = time.time() + delay_s
    r.zadd(DOMAIN_ENRICHMENT_DELAYED, {payload: not_before})
    log.info("re-queued %s to delayed queue — retry in %ds", fein, delay_s)


def _flush_delayed(r) -> int:
    """Move delayed items that are now ready into the main enrichment queue. Returns count moved."""
    now = time.time()
    items = r.zrangebyscore(DOMAIN_ENRICHMENT_DELAYED, "-inf", now, withscores=False)
    if not items:
        return 0
    moved = 0
    for item in items:
        try:
            data = json.loads(item)
            r.zadd(DOMAIN_ENRICHMENT_QUEUE, {data["fein"]: data["petition_count"]})
            r.zrem(DOMAIN_ENRICHMENT_DELAYED, item)
            moved += 1
        except Exception as e:
            log.warning("Failed to flush delayed item %s: %s", item, e)
    if moved:
        log.info("Flushed %d delayed items to enrichment queue", moved)
    return moved


# ─────────────────────────────────────────────────────────────────────────────
# Retry tracking
# ─────────────────────────────────────────────────────────────────────────────

_RETRY_KEY_PREFIX = "enrichment:retry:"
_RETRY_TTL_S      = 86400 * 7  # 7 days


def _get_retry_count(r, fein: str) -> int:
    return int(r.get(f"{_RETRY_KEY_PREFIX}{fein}") or 0)


def _incr_retry(r, fein: str) -> int:
    key = f"{_RETRY_KEY_PREFIX}{fein}"
    count = r.incr(key)
    r.expire(key, _RETRY_TTL_S)
    return count


def _clear_retry(r, fein: str) -> None:
    r.delete(f"{_RETRY_KEY_PREFIX}{fein}")


# ─────────────────────────────────────────────────────────────────────────────
# DLQ
# ─────────────────────────────────────────────────────────────────────────────

def _move_to_dlq(r, fein: str, error_reason: str, retry_count: int) -> None:
    payload = json.dumps({
        "fein":         fein,
        "error_reason": error_reason,
        "retry_count":  retry_count,
        "failed_at":    time.time(),
    })
    r.lpush(DOMAIN_ENRICHMENT_DLQ, payload)
    log.error("DLQ: fein=%s reason=%s retries=%d", fein, error_reason, retry_count)


# ─────────────────────────────────────────────────────────────────────────────
# DB helpers
# ─────────────────────────────────────────────────────────────────────────────

def _load_company(conn, fein: str) -> "dict | None":
    row = conn.execute("""
        SELECT
            f.employer_fein,
            f.assigned_domain,
            f.careers_url,
            e.employer_name,
            COALESCE(u.petition_count, 0) AS petition_count
        FROM fein_domain_map f
        JOIN dol_h1b_employers e USING (employer_fein)
        LEFT JOIN (
            SELECT employer_fein, COUNT(*) AS petition_count
            FROM uscis_dol_fuzzy_map um
            JOIN dol_h1b_employers dh ON dh.employer_fein = um.dol_fein
            GROUP BY dh.employer_fein
        ) u ON u.employer_fein = f.employer_fein
        WHERE f.employer_fein = %s
    """, (fein,)).fetchone()
    if not row:
        return None
    return dict(zip(
        ["fein", "assigned_domain", "careers_url", "employer_name", "petition_count"],
        row,
    ))


def _write_domain(conn, fein: str, public_domain: "str|None", method: str) -> None:
    conn.execute("""
        UPDATE fein_domain_map
        SET public_domain        = %s,
            public_domain_method = %s,
            last_enriched_at     = NOW(),
            updated_at           = NOW()
        WHERE employer_fein = %s
    """, (public_domain, method, fein))


def _write_metric(conn, fein: str, trigger: str,
                  public_domain_method: "str|None", public_domain: "str|None",
                  careers_source: "str|None", careers_url: "str|None",
                  ats_source: "str|None", ats_platform: "str|None", ats_slug: "str|None",
                  duration_ms: int) -> None:
    conn.execute("""
        INSERT INTO h1b_enrichment_metrics
            (employer_fein, worker, trigger,
             public_domain_method, public_domain,
             careers_source, careers_url,
             ats_source, ats_platform, ats_slug,
             duration_ms)
        VALUES (%s, 'domain_enrichment', %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, (fein, trigger, public_domain_method, public_domain,
          careers_source, careers_url, ats_source, ats_platform, ats_slug, duration_ms))


def _write_careers(conn, fein: str, careers_url: str) -> None:
    conn.execute("""
        UPDATE fein_domain_map
        SET careers_url = %s,
            updated_at  = NOW()
        WHERE employer_fein = %s
          AND (careers_url IS NULL OR careers_url = '')
    """, (careers_url, fein))


def _write_ats(conn, fein: str, domain: str, company_name: str,
               platform: str, slug: str, petition_count: int) -> None:
    conn.execute("""
        INSERT INTO company_ats
            (employer_fein, domain, company_name, platform, slug, source, priority)
        VALUES (%s, %s, %s, %s, %s, 'enrichment', %s)
        ON CONFLICT (domain, platform) DO UPDATE SET
            slug         = EXCLUDED.slug,
            employer_fein = COALESCE(EXCLUDED.employer_fein, company_ats.employer_fein),
            source       = EXCLUDED.source,
            detected_at  = NOW()
    """, (fein, domain, company_name, platform, slug, petition_count))


def _push_to_discovery(r, fein: str, petition_count: int) -> None:
    member = json.dumps({"fein": fein, "trigger": "enrichment"})
    r.zadd(DISCOVERY_QUEUE, {member: petition_count})
    log.debug("pushed %s to discovery_queue (petition_count=%d)", fein, petition_count)


# ─────────────────────────────────────────────────────────────────────────────
# Per-company processing
# ─────────────────────────────────────────────────────────────────────────────

def _process_company(r, fein: str, petition_count: int, trigger: str = "enrichment") -> bool:
    """
    Run full enrichment for one company.
    Returns True on success (or permanent skip), False on transient error.
    """
    conn = get_conn()
    t_start = time.time()
    try:
        company = _load_company(conn, fein)
        if not company:
            log.warning("fein=%s not found in fein_domain_map — skipping", fein)
            return True

        assigned = company["assigned_domain"]
        if not assigned:
            log.warning("fein=%s has no assigned_domain — skipping", fein)
            return True

        employer_name  = company["employer_name"]
        existing_careers = company["careers_url"]

        log.info("enriching fein=%s domain=%s name=%r", fein, assigned, employer_name)

        # ── Step 1: public domain resolution ──────────────────────────────────
        public_domain, method, retry_after = discover_public_domain(assigned)

        if retry_after is not None:
            # Certspotter quota exhausted — re-queue with delay, don't count as retry
            log.info("fein=%s certspotter quota — re-queuing in %ds", fein, retry_after)
            _requeue_delayed(r, fein, petition_count, retry_after)
            return True

        probe_domain = public_domain or (assigned if method == "same_domain" else assigned)
        website_url  = f"https://www.{probe_domain}"

        _write_domain(conn, fein, public_domain, method)
        conn.commit()
        log.info("fein=%s public_domain=%s method=%s", fein, public_domain, method)

        # ── Step 2: Phase 3 — career path probe ───────────────────────────────
        careers_url = existing_careers  # don't overwrite an existing good URL
        p3_platform = p3_slug = None

        if not careers_url:
            careers_url, p3_platform, p3_slug = _phase3(website_url)
            if careers_url:
                _write_careers(conn, fein, careers_url)
                conn.commit()
                log.info("fein=%s careers_url=%s (phase3)", fein, careers_url)

        # ── Step 3: Phase 6 — career page ATS scan ────────────────────────────
        try:
            p6_result = detect_via_career_page(
                employer_name, probe_domain, careers_url=careers_url or None,
            )
        except Exception as e:
            log.warning("Phase 6 error for fein=%s: %s", fein, e)
            p6_result = None

        p6_platform = None
        p6_slug     = None
        if p6_result:
            p6_careers  = p6_result.get("careers_url")
            p6_platform = p6_result.get("platform")
            p6_slug     = p6_result.get("slug")

            if p6_careers and not existing_careers:
                _write_careers(conn, fein, p6_careers)
                careers_url = p6_careers
                log.info("fein=%s careers_url=%s (phase6)", fein, p6_careers)

            if p6_platform and p6_slug:
                _write_ats(conn, fein, probe_domain, employer_name,
                           p6_platform, p6_slug, petition_count)
                log.info("fein=%s ATS detected: %s slug=%s (phase6)", fein, p6_platform, p6_slug)

        # Use Phase 3 ATS whenever Phase 6 found no platform (even if p6_result is present)
        if not p6_platform and p3_platform and p3_slug:
            _write_ats(conn, fein, probe_domain, employer_name,
                       p3_platform, p3_slug, petition_count)
            log.info("fein=%s ATS detected: %s slug=%s (phase3)", fein, p3_platform, p3_slug)

        conn.commit()

        # ── Step 4: push to discovery_queue ───────────────────────────────────
        if petition_count > 0:
            _push_to_discovery(r, fein, petition_count)

        # ── Metrics — reflect only persisted ATS data ─────────────────────────
        ats_source   = None
        ats_platform = None
        ats_slug     = None
        if p6_platform and p6_slug:
            ats_source   = "phase6"
            ats_platform = p6_platform
            ats_slug     = p6_slug
        elif p3_platform and p3_slug:
            ats_source   = "phase3"
            ats_platform = p3_platform
            ats_slug     = p3_slug

        careers_source = None
        final_careers  = careers_url
        if final_careers and not existing_careers:
            if p6_result and p6_result.get("careers_url") == final_careers:
                careers_source = "phase6"
            else:
                careers_source = "phase3"

        duration_ms = int((time.time() - t_start) * 1000)
        try:
            _write_metric(conn, fein, trigger,
                          method, public_domain,
                          careers_source, final_careers,
                          ats_source, ats_platform, ats_slug,
                          duration_ms)
            conn.commit()
        except Exception as me:
            log.warning("metric write failed for fein=%s: %s", fein, me)
            try:
                conn.rollback()
            except Exception:
                pass

        _clear_retry(r, fein)
        return True

    except Exception as exc:
        log.error("unexpected error enriching fein=%s: %s", fein, exc, exc_info=True)
        try:
            conn.rollback()
        except Exception:
            pass
        return False
    finally:
        conn.close()


# ─────────────────────────────────────────────────────────────────────────────
# Main loop
# ─────────────────────────────────────────────────────────────────────────────

def run_worker(once: bool = False) -> None:
    r = get_redis()
    processed = {"n": 0}
    hb = Heartbeat(r, "domain_enrichment_worker",
                   lambda: processed["n"], interval_s=ENRICHMENT_HEARTBEAT_S).start()

    log.info("domain-enrichment-worker started")

    try:
        while True:
            while _is_maintenance(r):
                log.info("Maintenance window active — pausing 30s")
                time.sleep(30)

            # Move any delayed items that are now ready
            _flush_delayed(r)

            # Pop highest petition_count company
            result = r.zpopmax(DOMAIN_ENRICHMENT_QUEUE, count=1)
            if not result:
                log.info("Enrichment queue empty — exiting")
                break

            fein, score = result[0]
            petition_count = int(score)

            retry_count = _get_retry_count(r, fein)
            if retry_count >= ENRICHMENT_MAX_RETRIES:
                _move_to_dlq(r, fein, "max_retries_exceeded", retry_count)
                _clear_retry(r, fein)
                continue

            success = _process_company(r, fein, petition_count)
            processed["n"] += 1

            if not success:
                count = _incr_retry(r, fein)
                if count >= ENRICHMENT_MAX_RETRIES:
                    _move_to_dlq(r, fein, "processing_error", count)
                    _clear_retry(r, fein)
                else:
                    # Re-queue for retry — same petition_count score
                    r.zadd(DOMAIN_ENRICHMENT_QUEUE, {fein: petition_count})
                    log.warning("fein=%s re-queued for retry (%d/%d)",
                                fein, count, ENRICHMENT_MAX_RETRIES)

            if once:
                break

    finally:
        hb.stop()

    log.info("domain-enrichment-worker stopped — processed %d companies", processed["n"])


if __name__ == "__main__":
    init_logging("domain_enrichment_worker")
    once = "--once" in sys.argv
    run_worker(once=once)
