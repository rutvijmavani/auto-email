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
  - fuzzy_match_uscis_dol.py   (after bulk queue population; never-enriched companies only)
  - staleness_checker cron      (enriched more than ENRICH_STALENESS_DAYS ago, default 90)
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
    DISCOVERY_BATCH,
    DISCOVERY_REDETECT,
    ENRICHMENT_BATCH,
    ENRICHMENT_DELAYED,
    ENRICHMENT_DLQ,
    ENRICHMENT_HEARTBEAT_S,
    ENRICHMENT_INFLIGHT,
    ENRICHMENT_MAX_RETRIES,
    ENRICHMENT_ON_DEMAND,
    HEAD_CHECK_CACHE_PREFIX,
    REDIS_DB_MAINTENANCE,
    STALENESS_DISCOVERY_MIN_PETITIONS,
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


def _phase3(website_url: str) -> "tuple[str|None, str|None, str|None, int|None]":
    """4th element: careers_url_last_status (docs/discovery-pipeline-hardening.md Part 2)
    — most block-like HTTP status seen across the probe, or None on a hit / network error."""
    if not _PHASE3_AVAILABLE:
        log.debug("phase3 unavailable (import-time failure) — skipping for %s", website_url)
        return None, None, None, None
    try:
        return _discover_careers_url(website_url)
    except Exception as e:
        # Re-raise (after logging) rather than returning an empty-success shape:
        # a transient failure here must propagate to the outer except Exception
        # in _process_company() so last_enriched_at is not advanced and retry
        # state is not cleared. Empty results are reserved for confirmed misses.
        log.warning("Phase 3 error for %s: %s", website_url, e)
        raise


# ─────────────────────────────────────────────────────────────────────────────
# Maintenance window
# ─────────────────────────────────────────────────────────────────────────────

def _is_maintenance(r) -> bool:
    try:
        return bool(r.exists(REDIS_DB_MAINTENANCE))
    except Exception as exc:
        log.warning("Redis maintenance check failed (%s) — assuming not in maintenance", exc)
        return False


# Atomically pops the highest-scoring member from a ZSET (KEYS[1]) and writes
# it to the inflight ZSET (KEYS[2]) with the same score.
_POP_ZSET_TO_INFLIGHT_LUA = """
local res = redis.call('ZPOPMAX', KEYS[1], 1)
if #res == 0 then return {} end
redis.call('ZADD', KEYS[2], tonumber(res[2]), res[1])
return {res[1], res[2]}
"""

# Atomically pops from a LIST (KEYS[1]) and writes to the inflight ZSET (KEYS[2])
# with score=0. petition_count is carried in the JSON payload, not the score.
_POP_LIST_TO_INFLIGHT_LUA = """
local res = redis.call('LPOP', KEYS[1])
if res == nil or res == false then return {} end
redis.call('ZADD', KEYS[2], 0, res)
return {res, '0'}
"""

# ─────────────────────────────────────────────────────────────────────────────
# Delayed queue — certspotter 429 re-queue with not_before timestamp
# ─────────────────────────────────────────────────────────────────────────────

def _requeue_delayed(r, fein: str, petition_count: int, delay_s: int,
                     trigger: str = "delayed_retry", source=None, tier: str = "batch") -> None:
    """Push company to enrichment:delayed ZSET scored by not_before timestamp."""
    payload = json.dumps({"fein": fein, "petition_count": petition_count,
                          "trigger": trigger, "source": source, "tier": tier})
    not_before = time.time() + delay_s
    r.zadd(ENRICHMENT_DELAYED, {payload: not_before})
    log.info("re-queued %s to enrichment:delayed — retry in %ds", fein, delay_s)


def _flush_delayed(r) -> int:
    """Promote enrichment:delayed items that are now ready into enrichment:batch. Returns count moved."""
    now = time.time()
    items = r.zrangebyscore(ENRICHMENT_DELAYED, "-inf", now, withscores=False)
    if not items:
        return 0
    moved = 0
    for item in items:
        try:
            data    = json.loads(item)
            fein    = data["fein"]
            trigger = data.get("trigger", "delayed_retry")
            pc      = data["petition_count"]
            tier    = data.get("tier", "batch")
            member  = json.dumps({"fein": fein, "trigger": trigger, "source": data.get("source"), "tier": tier})
            if tier == "on_demand":
                r.lpush(ENRICHMENT_ON_DEMAND, member)
            else:
                r.zadd(ENRICHMENT_BATCH, {member: pc}, gt=True)
            r.zrem(ENRICHMENT_DELAYED, item)
            moved += 1
        except (json.JSONDecodeError, KeyError, TypeError) as e:
            log.warning("Failed to flush delayed item %r: %s — sending to DLQ", item, e)
            r.lpush(ENRICHMENT_DLQ, json.dumps({
                "fein": "MALFORMED", "error_reason": "malformed_payload",
                "last_error": str(e), "raw": repr(item), "failed_at": time.time(),
            }))
            r.zrem(ENRICHMENT_DELAYED, item)
        except Exception as e:
            log.warning("delayed flush: Redis error for %r — will retry next cycle (%s)", item, e)
    if moved:
        log.info("Flushed %d delayed items to enrichment:batch", moved)
    return moved


# ─────────────────────────────────────────────────────────────────────────────
# Retry tracking
# ─────────────────────────────────────────────────────────────────────────────

_RETRY_KEY_PREFIX = "enrichment:retry:"
_RETRY_TTL_S      = 86400 * 7  # 7 days


def _retry_key(fein: str, trigger: str, source, tier=None) -> str:
    # fein alone is not a unique queue-item identity — different trigger/source/tier
    # combinations for the same company are independent retry sequences.
    return f"{_RETRY_KEY_PREFIX}{fein}:{trigger}:{source or ''}:{tier or ''}"


def _get_retry_count(r, fein: str, trigger: str, source=None, tier=None) -> int:
    return int(r.get(_retry_key(fein, trigger, source, tier)) or 0)


def _incr_retry(r, fein: str, trigger: str, source=None, tier=None) -> int:
    key = _retry_key(fein, trigger, source, tier)
    count = r.incr(key)
    r.expire(key, _RETRY_TTL_S)
    return count


def _clear_retry(r, fein: str, trigger: str, source=None, tier=None) -> None:
    r.delete(_retry_key(fein, trigger, source, tier))


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
    r.lpush(ENRICHMENT_DLQ, payload)
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
            f.public_domain,
            f.public_domain_method,
            f.public_domain_host,
            COALESCE(f.public_domain_retry_count, 0) AS public_domain_retry_count,
            e.employer_name,
            COALESCE(u.petition_count, 0) AS petition_count
        FROM fein_domain_map f
        JOIN dol_h1b_employers e USING (employer_fein)
        LEFT JOIN uscis_petition_counts u ON u.employer_fein = f.employer_fein
        WHERE f.employer_fein = %s
    """, (fein,)).fetchone()
    if not row:
        return None
    return dict(row)


def _write_domain(conn, fein: str, public_domain: "str|None", method: str,
                  last_status: "int|None", prev_retry_count: int,
                  host: "str|None" = None) -> None:
    # host: exact host that answered (e.g. www.example.com), stored in
    # public_domain_host and used ONLY to fetch the website. Invariant enforced here:
    # registrable root(host) must equal public_domain, else host is stored as NULL.
    # Does NOT touch last_enriched_at — Phase 3/6 haven't run yet at this point,
    # and if either later raises, the attempt is a failure that must not look
    # like a completed enrichment cycle to staleness-based re-detection.
    # last_enriched_at is advanced once, in _process_company, only after both
    # phases have completed without raising.
    #
    # public_domain_last_status/public_domain_retry_count (Part 1 gate fix):
    # a clean 2xx resolution always clears both (NULL / 0). A failed resolution
    # records the inconclusive status seen and increments the retry counter only
    # for transient (429/503) statuses — the staleness_checker pd-retry pass reads
    # this counter to decide when to stop plain-retrying and escalate to the relay
    # queue (Part 3) instead. Non-transient statuses (403/404/etc.) record the
    # status but leave the counter alone — those are relay-eligible immediately,
    # not part of the plain-retry loop.
    #
    # public_domain_last_attempt_at: the retry-age gate staleness_checker's Pass 1c
    # actually reads. Deliberately separate from updated_at, which is touched by
    # every write to this row (careers_url resolution, mobile relay guard clears,
    # etc.) and would otherwise silently reset the pd-retry backoff clock on writes
    # unrelated to a retry attempt. Cleared on a clean resolution (nothing left to
    # gate); set only on an actual failed attempt.
    if public_domain is not None:
        if host:
            from jobs.public_domain import _root
            if _root(host) != _root(public_domain):
                log.warning("fein=%s host %s root != public_domain %s — host not stored",
                            fein, host, public_domain)
                host = None
        conn.execute("""
            UPDATE fein_domain_map
            SET public_domain              = %s,
                public_domain_host          = %s,
                public_domain_method        = %s,
                public_domain_last_status   = NULL,
                public_domain_retry_count   = 0,
                public_domain_last_attempt_at = NULL,
                updated_at                  = NOW()
            WHERE employer_fein = %s
        """, (public_domain, host, method, fein))
    else:
        # Resolution failed — preserve any previously stored domain.
        new_retry_count = prev_retry_count + 1 if last_status in (429, 503) else prev_retry_count
        conn.execute("""
            UPDATE fein_domain_map
            SET public_domain_last_status = %s,
                public_domain_retry_count  = %s,
                public_domain_last_attempt_at = NOW(),
                updated_at                 = NOW()
            WHERE employer_fein = %s
        """, (last_status, new_retry_count, fein))


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


def _write_careers(conn, fein: str, careers_url: str, source: str) -> None:
    """UPDATE fein_domain_map.careers_url (no commit). Callers must call
    _invalidate_head_check_cache(r, fein) after the commit."""
    conn.execute("""
        UPDATE fein_domain_map
        SET careers_url    = %s,
            careers_source = %s,
            careers_url_last_status = NULL,
            updated_at     = NOW()
        WHERE employer_fein = %s
    """, (careers_url, source, fein))


def _write_careers_last_status(conn, fein: str, last_status: "int | None") -> None:
    """UPDATE-only counterpart to _write_careers() for the miss/block case
    (docs/discovery-pipeline-hardening.md Part 2): persists the most block-like
    status Phase 3 saw so a future staleness/relay pass can gate on it, without
    touching careers_url/careers_source. No commit — caller commits."""
    conn.execute("""
        UPDATE fein_domain_map
        SET careers_url_last_status = %s,
            updated_at = NOW()
        WHERE employer_fein = %s
    """, (last_status, fein))


def _invalidate_head_check_cache(r, fein: str) -> None:
    """Drop head_check:{fein} so the next HEAD check probes the freshly written
    careers_url instead of replaying a cached verdict for the old (dead) one.
    Best-effort: the DB write is already committed and the cache TTL bounds staleness."""
    try:
        r.delete(f"{HEAD_CHECK_CACHE_PREFIX}{fein}")
    except Exception as exc:
        log.warning("head_check cache invalidation failed fein=%s: %s", fein, exc)


def _write_ats(conn, fein: str, domain: str, company_name: str,
               platform: str, slug: str, petition_count: int) -> int:
    """Insert or update a company_ats row. Returns rowcount (0 if blocked by review guard)."""
    cur = conn.execute("""
        INSERT INTO company_ats
            (employer_fein, domain, company_name, platform, slug, source, priority)
        VALUES (%s, %s, %s, %s, %s, 'enrichment', %s)
        ON CONFLICT (domain, platform) DO UPDATE SET
            slug          = EXCLUDED.slug,
            employer_fein = COALESCE(company_ats.employer_fein, EXCLUDED.employer_fein),
            source        = EXCLUDED.source,
            detected_at   = NOW()
        WHERE company_ats.reviewed_at IS NULL AND company_ats.is_monitored = FALSE
    """, (fein, domain, company_name, platform, slug, petition_count))
    return cur.rowcount


def _write_discovery_ats(conn, fein: str, employer_name: str,
                          platform: str, slug: str, source: str) -> None:
    """Mirror-write a company_ats detection into h1b_ats_discovery (docs/discovery-
    pipeline-hardening.md Part 5.2) — keeps the two ATS-tracking systems from
    silently diverging when enrichment (this worker) finds a platform/slug that
    discover_h1b_ats.py's own pass hasn't seen yet, or has seen differently.

    Ports the same platform-change guard scripts/discover_h1b_ats.py::upsert_discovery
    already applies: a slug never survives under a different platform than the one
    it was detected with — if this write's platform differs from what's on record,
    the new slug replaces it outright (even NULL); otherwise COALESCE keeps whichever
    slug is non-NULL. No commit — caller commits (same convention as _write_ats)."""
    conn.execute("""
        INSERT INTO h1b_ats_discovery
            (employer_fein, employer_name, detected_platform, detected_slug,
             ats_source, last_checked)
        VALUES (%s, %s, %s, %s, %s, NOW())
        ON CONFLICT (employer_fein) DO UPDATE SET
            employer_name     = COALESCE(EXCLUDED.employer_name, h1b_ats_discovery.employer_name),
            detected_platform = COALESCE(EXCLUDED.detected_platform, h1b_ats_discovery.detected_platform),
            detected_slug     = CASE
                WHEN EXCLUDED.detected_platform IS NOT NULL
                     AND EXCLUDED.detected_platform IS DISTINCT FROM h1b_ats_discovery.detected_platform
                THEN EXCLUDED.detected_slug
                ELSE COALESCE(EXCLUDED.detected_slug, h1b_ats_discovery.detected_slug)
            END,
            ats_source        = COALESCE(EXCLUDED.ats_source, h1b_ats_discovery.ats_source),
            last_checked      = NOW()
    """, (fein, employer_name, platform, slug, source))


def _push_to_discovery(r, fein: str, petition_count: int, source: "str | None" = None,
                       trigger: str = "enrichment") -> None:
    member = json.dumps({"fein": fein, "trigger": trigger, "source": source})
    if trigger == "redetect":
        r.zadd(DISCOVERY_REDETECT, {member: petition_count}, gt=True)
        log.debug("pushed %s to discovery:redetect (petition_count=%d)", fein, petition_count)
    else:
        r.zadd(DISCOVERY_BATCH, {member: petition_count}, gt=True)
        log.debug("pushed %s to discovery:batch (trigger=%s petition_count=%d)",
                  fein, trigger, petition_count)


# ─────────────────────────────────────────────────────────────────────────────
# Per-company processing
# ─────────────────────────────────────────────────────────────────────────────

def _process_company(r, fein: str, petition_count: int, trigger: str = "enrichment",
                     source: "str | None" = None, tier: str = "batch") -> bool:
    """
    Run full enrichment for one company.
    Returns True on success (or permanent skip), False on transient error.
    source: forwarded from queue payload ("company_ats"|"prospective"|None).
    tier: forwarded from queue payload ("on_demand"|"batch") — preserved across delayed retries.
    """
    conn = None
    t_start = time.time()
    try:
        conn = get_conn()
        company = _load_company(conn, fein)
        if not company:
            log.warning("fein=%s trigger=%s not found in fein_domain_map — permanent skip (LCA not yet ingested?)", fein, trigger)
            try:
                conn.execute(
                    "INSERT INTO fein_domain_map (employer_fein, last_enriched_at)"
                    " VALUES (%s, NOW())"
                    " ON CONFLICT (employer_fein) DO UPDATE SET last_enriched_at = NOW()",
                    (fein,),
                )
                conn.commit()
            except Exception as _placeholder_exc:
                log.warning("fein=%s: placeholder insert into fein_domain_map failed: %s", fein, _placeholder_exc)
            return True

        assigned = company["assigned_domain"]
        if not assigned:
            log.warning("fein=%s trigger=%s assigned_domain is NULL — permanent skip (no email domain in LCA data)", fein, trigger)
            conn.execute(
                "UPDATE fein_domain_map SET last_enriched_at = NOW() WHERE employer_fein = %s",
                (fein,),
            )
            conn.commit()
            return True

        employer_name      = company["employer_name"]
        existing_careers   = company["careers_url"]
        stored_public      = company["public_domain"]
        db_petition_count  = company["petition_count"]
        prev_retry_count   = company["public_domain_retry_count"]

        log.info("enriching fein=%s domain=%s name=%r", fein, assigned, employer_name)

        # End the read transaction opened by _load_company before the DNS/HTTP/Certspotter
        # work in discover_public_domain — otherwise it holds AccessShare locks on
        # fein_domain_map/dol_h1b_employers for the whole resolution and queues init_db's
        # ALTER (and every later reader) behind it.
        conn.commit()

        # ── Step 1: public domain resolution ──────────────────────────────────
        public_domain, method, retry_after, last_status, pd_host = discover_public_domain(assigned)

        if retry_after is not None:
            # Certspotter quota exhausted — re-queue with delay, don't count as retry
            log.info("fein=%s certspotter quota — re-queuing in %ds", fein, retry_after)
            _requeue_delayed(r, fein, petition_count, retry_after, trigger, source=source, tier=tier)
            return True

        # When resolution fails, fall back to the previously stored public_domain so
        # downstream phases probe the best-known domain rather than the raw assigned one.
        effective_public = public_domain or stored_public
        probe_domain     = effective_public or assigned
        # Fetch uses the exact host that answered; identity (ATS/KG) keeps the root.
        # Fallback chain: host -> public_domain -> assigned.
        fetch_host       = (pd_host if public_domain else company.get("public_domain_host")) or probe_domain
        website_url      = f"https://{fetch_host}"

        # _write_domain persists public_domain/method when resolution succeeded, or just
        # records last_status/retry_count on failure (preserves existing stored domain).
        # It does not advance last_enriched_at — see its docstring comment.
        _write_domain(conn, fein, public_domain, method, last_status, prev_retry_count, pd_host)
        conn.commit()
        log.info("fein=%s public_domain=%s method=%s last_status=%s (effective=%s)",
                 fein, public_domain, method, last_status, effective_public)

        # ── Step 2: Phase 3 — career path probe (always runs) ────────────────
        # Routing upstream (entry check + head_check) guarantees we only arrive
        # here when careers_url is missing or dead, so no guard needed.
        careers_url = None
        _careers_source_this_run = None
        p3_platform = p3_slug = None

        careers_url, p3_platform, p3_slug, p3_last_status = _phase3(website_url)
        if careers_url:
            _careers_source_this_run = "phase3"
            _write_careers(conn, fein, careers_url, source="phase3")
            conn.commit()
            _invalidate_head_check_cache(r, fein)
            log.info("fein=%s careers_url=%s (phase3)", fein, careers_url)
        elif p3_last_status is not None:
            # Block-like status (403/429/503) or first-seen miss (404) — persist so
            # a future relay pass (Part 3) can gate on it, mirroring discover_h1b_ats.py.
            _write_careers_last_status(conn, fein, p3_last_status)
            conn.commit()
            log.info("fein=%s careers_url_last_status=%s (phase3)", fein, p3_last_status)

        # ── Step 3: Phase 6 — career page ATS scan (only if Phase 3 found nothing) ──
        p6_platform = None
        p6_slug     = None
        p6_written  = False
        if not p3_platform:
            try:
                p6_result = detect_via_career_page(
                    employer_name, probe_domain, careers_url=careers_url,
                )
            except Exception as e:
                # Re-raise (after logging) rather than treating this as a
                # confirmed miss — see _phase3()'s comment for the rationale.
                log.warning("Phase 6 error for fein=%s: %s", fein, e)
                raise

            if p6_result:
                p6_careers  = p6_result.get("careers_url")
                p6_platform = p6_result.get("platform")
                p6_slug     = p6_result.get("slug")

                if p6_careers:
                    _careers_source_this_run = "phase6"
                    _write_careers(conn, fein, p6_careers, source="phase6")
                    careers_url = p6_careers
                    log.info("fein=%s careers_url=%s (phase6)", fein, p6_careers)

                if p6_platform and p6_slug:
                    p6_written = bool(_write_ats(conn, fein, probe_domain, employer_name,
                                                  p6_platform, p6_slug, db_petition_count))
                    _write_discovery_ats(conn, fein, employer_name,
                                          p6_platform, p6_slug, source="enrichment_phase6")
                    log.info("fein=%s ATS detected: %s slug=%s (phase6)", fein, p6_platform, p6_slug)

        # Use Phase 3 ATS whenever Phase 6 found no platform
        p3_written = False
        if not p6_platform and p3_platform and p3_slug:
            p3_written = bool(_write_ats(conn, fein, probe_domain, employer_name,
                                          p3_platform, p3_slug, db_petition_count))
            _write_discovery_ats(conn, fein, employer_name,
                                  p3_platform, p3_slug, source="enrichment_phase3")
            log.info("fein=%s ATS detected: %s slug=%s (phase3)", fein, p3_platform, p3_slug)

        # Phase 3 and Phase 6 both completed without raising — this is a genuinely
        # completed enrichment cycle, so advance the staleness timestamp now (not
        # earlier in _write_domain, which runs before either phase).
        conn.execute(
            "UPDATE fein_domain_map SET last_enriched_at = NOW() WHERE employer_fein = %s",
            (fein,),
        )
        conn.commit()
        if _careers_source_this_run == "phase6":
            _invalidate_head_check_cache(r, fein)

        # ── Step 4: push to discovery (skip on_demand — loop stops here) ─────
        # redetect items are already-monitored companies whose ATS went silent —
        # they already cleared a monitoring bar once, so the min-petition floor
        # (meant to keep low-value new companies out of discovery) doesn't apply.
        if trigger != "on_demand" and (
                trigger == "redetect" or db_petition_count >= STALENESS_DISCOVERY_MIN_PETITIONS):
            _push_to_discovery(r, fein, db_petition_count, source=source, trigger=trigger)

        # ── Metrics — reflect only persisted ATS data ─────────────────────────
        ats_source   = None
        ats_platform = None
        ats_slug     = None
        if p6_written:
            ats_source   = "phase6"
            ats_platform = p6_platform
            ats_slug     = p6_slug
        elif p3_written:
            ats_source   = "phase3"
            ats_platform = p3_platform
            ats_slug     = p3_slug

        final_careers  = careers_url
        careers_source = _careers_source_this_run if final_careers else None

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

        _clear_retry(r, fein, trigger, source=source, tier=tier)
        return True

    except Exception as exc:
        log.error("unexpected error enriching fein=%s: %s", fein, exc, exc_info=True)
        if conn:
            try:
                conn.rollback()
            except Exception:
                pass
        return False
    finally:
        if conn:
            conn.close()


# ─────────────────────────────────────────────────────────────────────────────
# Inflight crash recovery
# ─────────────────────────────────────────────────────────────────────────────

def _reclaim_inflight(r, inflight_key: str) -> None:
    """Re-queue any FEINs left in the per-instance inflight ZSET from a prior crash."""
    items = r.zrange(inflight_key, 0, -1, withscores=True)
    if not items:
        return
    log.warning("reclaiming %d inflight FEINs from %s", len(items), inflight_key)
    for raw_member, score in items:
        member = raw_member.decode() if isinstance(raw_member, bytes) else raw_member
        try:
            parsed = json.loads(member)
            fein = parsed["fein"]
            # petition_count from payload (on_demand items have score=0 in inflight).
            pc   = parsed.get("petition_count", int(score)) or int(score)
            tier = parsed.get("tier")
        except Exception:
            fein = member.strip()
            pc   = int(score)
            tier = None
        if tier == "on_demand" or (tier is None and score == 0):
            r.lpush(ENRICHMENT_ON_DEMAND, member)
            log.info("reclaimed inflight fein=%s -> enrichment:on_demand", fein)
        else:
            r.zadd(ENRICHMENT_BATCH, {member: pc}, gt=True)
            log.info("reclaimed inflight fein=%s pc=%d -> enrichment:batch", fein, pc)
        r.zrem(inflight_key, raw_member)


# ─────────────────────────────────────────────────────────────────────────────
# Main loop
# ─────────────────────────────────────────────────────────────────────────────

def run_worker(once: bool = False) -> None:
    r = get_redis()
    processed = {"n": 0}
    _instance = os.environ.get("WORKER_INSTANCE", "")
    _hb_name  = f"domain_enrichment_worker@{_instance}" if _instance else "domain_enrichment_worker"
    hb = Heartbeat(r, _hb_name,
                   lambda: processed["n"], interval_s=ENRICHMENT_HEARTBEAT_S).start()

    _inflight_key = f"{ENRICHMENT_INFLIGHT}:{_instance}" if _instance else ENRICHMENT_INFLIGHT

    log.info("domain-enrichment-worker started (instance=%r inflight=%s)", _instance, _inflight_key)
    _reclaim_inflight(r, _inflight_key)

    _pop_list_to_inflight = r.register_script(_POP_LIST_TO_INFLIGHT_LUA)
    _pop_zset_to_inflight = r.register_script(_POP_ZSET_TO_INFLIGHT_LUA)
    _MAINTENANCE_MAX_S = 4 * 3600  # exit if stuck in maintenance for 4+ hours

    try:
        while True:
            _maint_start = None
            while _is_maintenance(r):
                if _maint_start is None:
                    _maint_start = time.monotonic()
                elapsed = time.monotonic() - _maint_start
                if elapsed > _MAINTENANCE_MAX_S:
                    log.error("Maintenance window exceeded %dh — exiting to allow restart",
                              _MAINTENANCE_MAX_S // 3600)
                    sys.exit(1)
                log.info("Maintenance window active — pausing 30s (%.0fm elapsed)", elapsed / 60)
                time.sleep(30)

            # Promote any delayed items that are now ready → enrichment:batch
            _flush_delayed(r)

            # Pop on_demand LIST first (priority), fall back to batch ZSET
            _pop_result = _pop_list_to_inflight(keys=[ENRICHMENT_ON_DEMAND, _inflight_key])
            _tier = "on_demand"
            if not _pop_result:
                _pop_result = _pop_zset_to_inflight(keys=[ENRICHMENT_BATCH, _inflight_key])
                _tier = "batch"

            if not _pop_result:
                earliest = r.zrange(ENRICHMENT_DELAYED, 0, 0, withscores=True)
                if not earliest:
                    # Guard against producer-enqueue race: re-flush and re-check once.
                    _flush_delayed(r)
                    if r.llen(ENRICHMENT_ON_DEMAND) == 0 and r.zcard(ENRICHMENT_BATCH) == 0:
                        log.info("Enrichment queues empty — exiting")
                        break
                    time.sleep(1)
                    continue
                if once:
                    log.info("Enrichment queues empty (--once); %d delayed item(s) — exiting",
                             r.zcard(ENRICHMENT_DELAYED))
                    break
                _, next_ts = earliest[0]
                wait_s = min(30.0, max(1.0, next_ts - time.time()))
                log.info("Enrichment queues empty; %d delayed item(s) — sleeping %.0fs",
                         r.zcard(ENRICHMENT_DELAYED), wait_s)
                time.sleep(wait_s)
                continue

            raw_member = _pop_result[0]  # str (decode_responses=True) — already in inflight

            # Parse fein + trigger + source + petition_count from JSON payload
            try:
                data           = json.loads(raw_member)
                fein           = data["fein"]
                trigger        = data.get("trigger", "enrichment")
                source         = data.get("source")
                # ZSET items carry score; LIST items carry petition_count in payload
                petition_count = int(data.get("petition_count", 0)) or int(float(_pop_result[1]))
            except (json.JSONDecodeError, KeyError, TypeError, ValueError):
                raw_str = raw_member.decode() if isinstance(raw_member, bytes) else raw_member
                if raw_str.strip().lstrip("-").isdigit():
                    fein           = raw_str.strip()
                    trigger        = "enrichment"
                    source         = None
                    petition_count = int(float(_pop_result[1]))
                else:
                    log.error("malformed queue member %r — sending to DLQ", raw_member)
                    r.lpush(ENRICHMENT_DLQ, json.dumps({
                        "fein": "MALFORMED", "error_reason": "malformed_member",
                        "raw": repr(raw_member), "failed_at": time.time(),
                    }))
                    r.zrem(_inflight_key, raw_member)
                    continue

            retry_count = _get_retry_count(r, fein, trigger, source=source, tier=_tier)
            if retry_count >= ENRICHMENT_MAX_RETRIES:
                _move_to_dlq(r, fein, "max_retries_exceeded", retry_count)
                _clear_retry(r, fein, trigger, source=source, tier=_tier)
                r.zrem(_inflight_key, raw_member)
                continue

            success = _process_company(r, fein, petition_count, trigger=trigger, source=source, tier=_tier)
            processed["n"] += 1

            if not success:
                count = _incr_retry(r, fein, trigger, source=source, tier=_tier)
                if count >= ENRICHMENT_MAX_RETRIES:
                    _move_to_dlq(r, fein, "processing_error", count)
                    _clear_retry(r, fein, trigger, source=source, tier=_tier)
                else:
                    delay_s = 30 * (4 ** (count - 1))  # 30s → 120s → 480s
                    _requeue_delayed(r, fein, petition_count, delay_s, trigger, source=source, tier=_tier)
                    log.warning("fein=%s retry %d/%d in %ds",
                                fein, count, ENRICHMENT_MAX_RETRIES, delay_s)
            else:
                _clear_retry(r, fein, trigger, source=source, tier=_tier)

            r.zrem(_inflight_key, raw_member)

            if once:
                break

    finally:
        hb.stop()

    log.info("domain-enrichment-worker stopped — processed %d companies", processed["n"])


if __name__ == "__main__":
    init_logging("domain_enrichment_worker")
    once = "--once" in sys.argv
    run_worker(once=once)
