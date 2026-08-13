"""
workers/discover_h1b_ats_worker.py — ATS discovery worker for H1B pipeline.

Reads from Redis ZSET discovery_queue (score = petition_count, highest first).
For each company FEIN runs the full discovery pipeline:
  1. Phase 1: Google Knowledge Graph → canonical name + Freebase MID
               (always runs on first pass — kg_checked=False, even if careers_url set)
  2. Wikidata SPARQL P646+P10311+P856 → jobs_url, website, glassdoor_id, crunchbase_id
  3. Phase 3: 19-pattern career URL probe (if no jobs_url)
  4. Phase 4: Brave search fallback (if Phase 3 misses — consumes Brave quota)
  5. Phase 6: career_page.py deep scan (if platform still unknown)
  6. Phase 7: career_detector BFS with Chrome impersonation (last resort)
  Writes results to h1b_ats_discovery + company_ats.
  Sets kg_checked=True in fein_domain_map after Phase 1 runs.

Re-detection triggers (from job_fetcher / admin):
  When {"fein":..., "trigger":"re_detection"} arrives, the ATS-already-set
  guard is skipped and the full pipeline re-runs regardless.

Worker exits cleanly when queue is empty — not a perpetual daemon.
Started by:
  - domain_enrichment_worker (pushes top petition_count companies after enrichment)
  - staleness_checker cron   (>30 days since last_discovered_at)
  - API endpoint             (on-demand re-detection)

Usage:
  python -m workers.discover_h1b_ats_worker
  python -m workers.discover_h1b_ats_worker --once
"""

import json
import os
import sys
import time

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from config import (
    DISCOVERY_DLQ,
    DISCOVERY_HEARTBEAT_S,
    DISCOVERY_MAX_RETRIES,
    DISCOVERY_QUEUE,
    REDIS_DB_MAINTENANCE,
)
from db.connection import get_conn
from logger import get_logger, init_logging
from workers.heartbeat import Heartbeat
from workers.redis_client import get_redis

log = get_logger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# Lazy imports — heavy dependencies loaded once on first use
# ─────────────────────────────────────────────────────────────────────────────

_discover_ats = None


def _get_discover_ats():
    global _discover_ats
    if _discover_ats is None:
        try:
            import scripts.discover_h1b_ats as m
            _discover_ats = m
        except Exception as e:
            raise RuntimeError(f"discover_h1b_ats import failed: {e}") from e
    return _discover_ats


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
# Retry tracking
# ─────────────────────────────────────────────────────────────────────────────

_RETRY_KEY_PREFIX = "discovery:retry:"
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
    r.lpush(DISCOVERY_DLQ, payload)
    log.error("DLQ: fein=%s reason=%s retries=%d", fein, error_reason, retry_count)


# ─────────────────────────────────────────────────────────────────────────────
# DB helpers
# ─────────────────────────────────────────────────────────────────────────────

def _load_company(conn, fein: str) -> "dict | None":
    """Load company data from fein_domain_map + dol_h1b_employers + petition count."""
    row = conn.execute("""
        SELECT
            f.employer_fein,
            f.assigned_domain,
            f.public_domain,
            f.careers_url,
            f.kg_checked,
            e.employer_name,
            COALESCE(u.petition_count, 0) AS petition_count
        FROM fein_domain_map f
        JOIN dol_h1b_employers e USING (employer_fein)
        LEFT JOIN (
            SELECT dh.employer_fein, COUNT(*) AS petition_count
            FROM uscis_dol_fuzzy_map um
            JOIN dol_h1b_employers dh ON dh.employer_fein = um.dol_fein
            GROUP BY dh.employer_fein
        ) u ON u.employer_fein = f.employer_fein
        WHERE f.employer_fein = %s
    """, (fein,)).fetchone()
    if not row:
        return None
    return dict(zip(
        ["fein", "assigned_domain", "public_domain", "careers_url",
         "kg_checked", "employer_name", "petition_count"],
        row,
    ))


def _write_metric(conn, fein: str, trigger: str,
                  careers_source: "str|None", careers_url: "str|None",
                  ats_source: "str|None", ats_platform: "str|None", ats_slug: "str|None",
                  duration_ms: int) -> None:
    conn.execute("""
        INSERT INTO h1b_enrichment_metrics
            (employer_fein, worker, trigger,
             careers_source, careers_url,
             ats_source, ats_platform, ats_slug,
             duration_ms)
        VALUES (%s, 'discovery', %s, %s, %s, %s, %s, %s, %s)
    """, (fein, trigger, careers_source, careers_url,
          ats_source, ats_platform, ats_slug, duration_ms))


def _mark_kg_checked(conn, fein: str) -> None:
    conn.execute("""
        UPDATE fein_domain_map
        SET kg_checked = TRUE, updated_at = NOW()
        WHERE employer_fein = %s
    """, (fein,))


def _write_last_discovered(conn, fein: str) -> None:
    conn.execute("""
        UPDATE fein_domain_map
        SET last_discovered_at = NOW(), updated_at = NOW()
        WHERE employer_fein = %s
    """, (fein,))


# ─────────────────────────────────────────────────────────────────────────────
# Per-company processing
# ─────────────────────────────────────────────────────────────────────────────

def _process_company(fein: str, petition_count: int, trigger: str) -> bool:
    """
    Run full ATS discovery for one company.
    Returns True on success (or permanent skip), False on transient error.
    trigger values: 'enrichment' | 're_detection' | 'staleness' | 'manual'
    """
    conn = get_conn()
    t_start = time.time()
    try:
        m = _get_discover_ats()
        company = _load_company(conn, fein)
        if not company:
            log.warning("fein=%s not found in fein_domain_map — skipping", fein)
            return True

        employer_name = company["employer_name"]
        kg_checked    = company["kg_checked"]
        careers_url   = company["careers_url"]

        # Use public_domain if available; fall back to assigned_domain
        probe_domain = company["public_domain"] or company["assigned_domain"]
        if not probe_domain:
            log.warning("fein=%s has no domain — skipping", fein)
            return True

        log.info("discovering fein=%s domain=%s name=%r trigger=%s",
                 fein, probe_domain, employer_name, trigger)

        # Build emp dict matching process_employer() expectations
        emp = {
            "employer_fein":   fein,
            "employer_name":   employer_name,
            "assigned_domain": probe_domain,
            "total_approvals": petition_count,
        }

        # Check existing discovery row — needed for KG MID cache + re-detection guard
        existing_row = m.get_discovery_row(fein, conn)
        already_has_ats = (
            existing_row
            and existing_row.get("detected_platform")
            and existing_row.get("detected_slug")
        )

        # Skip re-discovery if ATS already detected, UNLESS trigger forces re-detection
        if already_has_ats and trigger not in ("re_detection", "manual"):
            log.info("fein=%s ATS already detected (%s/%s) — skipping (trigger=%s)",
                     fein, existing_row["detected_platform"],
                     existing_row.get("detected_slug"), trigger)
            _write_last_discovered(conn, fein)
            conn.commit()
            return True

        # Run full discovery pipeline via process_employer().
        # kg_checked=False → KG always runs on first pass (even if careers_url set).
        # pass skip_brave=False so Phase 4 (Brave) runs — this worker is the right place.
        if not kg_checked:
            # First pass: always run KG (prefetched=None forces inline KG call)
            result = m.process_employer(
                emp, conn, dry_run=False, force=False,
                prefetched=None, skip_brave=False,
            )
            _mark_kg_checked(conn, fein)
        else:
            # Subsequent pass: KG MID already cached in h1b_ats_discovery.
            # process_employer reads it via get_discovery_row → cached_mid path.
            # Use force=True on re_detection trigger so the recency guard is bypassed.
            force = trigger in ("re_detection", "manual")
            result = m.process_employer(
                emp, conn, dry_run=False, force=force,
                prefetched=None, skip_brave=False,
            )

        _write_last_discovered(conn, fein)
        conn.commit()

        # ── Metrics ───────────────────────────────────────────────────────────
        det_platform = result.get("detected_platform")
        det_slug     = result.get("detected_slug")
        res_careers  = result.get("careers_url")

        # Determine ats_source from the result — process_employer logs the phase
        # but doesn't return it directly. We infer from what was available:
        # jobs_url (P10311) → phase1_kg; otherwise check phase order.
        if result.get("jobs_url") and det_platform:
            ats_src = "phase1_kg"
        elif det_platform:
            # Discovery worker always runs Phase 7 last — can't distinguish
            # phase6 vs phase7 from result alone. Use 'discovery' as fallback;
            # per-phase attribution is tracked in enrichment worker where we
            # control each phase individually.
            ats_src = "discovery"
        else:
            ats_src = None

        careers_src = "discovery" if res_careers and not company.get("careers_url") else None

        duration_ms = int((time.time() - t_start) * 1000)
        try:
            _write_metric(conn, fein, trigger,
                          careers_src, res_careers,
                          ats_src, det_platform, det_slug,
                          duration_ms)
            conn.commit()
        except Exception as me:
            log.warning("metric write failed for fein=%s: %s", fein, me)
            try:
                conn.rollback()
            except Exception:
                pass

        log.info(
            "fein=%s done: careers=%s platform=%s slug=%s",
            fein, res_careers, det_platform, det_slug,
        )
        return True

    except Exception as exc:
        log.error("unexpected error discovering fein=%s: %s", fein, exc, exc_info=True)
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
    hb = Heartbeat(r, "discover_h1b_ats_worker",
                   lambda: processed["n"], interval_s=DISCOVERY_HEARTBEAT_S).start()

    log.info("discover-h1b-ats-worker started")

    try:
        while True:
            while _is_maintenance(r):
                log.info("Maintenance window active — pausing 30s")
                time.sleep(30)

            # Pop highest petition_count company
            result = r.zpopmax(DISCOVERY_QUEUE, count=1)
            if not result:
                log.info("Discovery queue empty — exiting")
                break

            raw_member, score = result[0]
            petition_count = int(score)

            # Member is JSON: {"fein": "...", "trigger": "..."}
            try:
                data    = json.loads(raw_member)
                fein    = data["fein"]
                trigger = data.get("trigger", "enrichment")
            except Exception as e:
                log.error("Malformed discovery queue member %r: %s — sending to DLQ", raw_member, e)
                r.lpush(DISCOVERY_DLQ, raw_member)
                continue

            retry_count = _get_retry_count(r, fein)
            if retry_count >= DISCOVERY_MAX_RETRIES:
                _move_to_dlq(r, fein, "max_retries_exceeded", retry_count)
                continue

            success = _process_company(fein, petition_count, trigger)
            processed["n"] += 1

            if not success:
                count = _incr_retry(r, fein)
                if count >= DISCOVERY_MAX_RETRIES:
                    _move_to_dlq(r, fein, "processing_error", count)
                else:
                    # Re-queue for retry — preserve original trigger and score
                    member = json.dumps({"fein": fein, "trigger": trigger})
                    r.zadd(DISCOVERY_QUEUE, {member: petition_count})
                    log.warning("fein=%s re-queued for retry (%d/%d)",
                                fein, count, DISCOVERY_MAX_RETRIES)
            else:
                _clear_retry(r, fein)

            if once:
                break

    finally:
        hb.stop()

    log.info("discover-h1b-ats-worker stopped — processed %d companies", processed["n"])


if __name__ == "__main__":
    init_logging("discover_h1b_ats_worker")
    once = "--once" in sys.argv
    run_worker(once=once)
