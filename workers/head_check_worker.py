"""
workers/head_check_worker.py — HEAD check worker for H1B pipeline.

Sits between producers (api.py, staleness_checker, job_monitor) and the
enrichment/discovery workers. Verifies whether a known careers_url is still
alive before deciding where to route the company next.

Pop order (priority): HEAD_CHECK_ON_DEMAND (api.py) first, HEAD_CHECK_BATCH second.
Both are Redis LISTs — BLPOP handles strict priority ordering natively.

For each company:
  1. Check Redis cache head_check:{fein} (TTL = HEAD_CHECK_CACHE_TTL_S / 6h default).
     Cache hit → use stored result, skip HTTP. Prevents redundant HEAD requests when
     the same FEIN is pushed twice within the TTL window.
  2. Cache miss → HTTP HEAD on careers_url, follow redirects.
  3. Classify result into 6 cases and route:
       Case 1: redirect to same domain, careers path → write new URL, → discovery
       Case 2: redirect to known ATS domain         → write new URL, → discovery
       Case 3: redirect to same domain, homepage    → → enrichment
       Case 4: redirect to unrelated 3rd party      → → enrichment
       Case 5: clean 200 (URL healthy)              → → discovery (redetect/staleness only)
       Case 6: timeout / connection error           → → enrichment
  4. on_demand trigger: STOP at Cases 1, 2, 5 (no discovery push).
     redetect trigger:  Cases 1, 2, 5 → discovery:redetect.
     staleness trigger: Cases 1, 2, 5 → discovery:batch.

Worker exits cleanly when both queues are empty.

Usage:
  python -m workers.head_check_worker
  python -m workers.head_check_worker --once
"""

import json
import os
import sys
import time
from urllib.parse import urlparse

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import requests

from config import (
    CONNECT_TIMEOUT,
    DISCOVERY_BATCH,
    DISCOVERY_REDETECT,
    ENRICHMENT_BATCH,
    ENRICHMENT_DLQ,
    ENRICHMENT_ON_DEMAND,
    FETCH_TIMEOUT,
    HEAD_CHECK_BATCH,
    HEAD_CHECK_CACHE_TTL_S,
    HEAD_CHECK_DLQ,
    HEAD_CHECK_HEARTBEAT_S,
    HEAD_CHECK_MAX_RETRIES,
    HEAD_CHECK_ON_DEMAND,
    REDIS_DB_MAINTENANCE,
    STALENESS_DISCOVERY_MIN_PETITIONS,
    WORKER_BLOCK_SECS,
)
from db.connection import get_conn
from logger import get_logger, init_logging
from workers.heartbeat import Heartbeat
from workers.redis_client import get_redis

log = get_logger(__name__)

# Registered domains of known ATS platforms — imported from discover_h1b_ats to stay in sync.
try:
    from scripts.discover_h1b_ats import _KNOWN_ATS_DOMAINS, _root_domain
    _tldextract_available = True
except Exception as _import_err:
    log.warning("discover_h1b_ats import failed (%s) — using fallback ATS domain set", _import_err)
    _KNOWN_ATS_DOMAINS = {
        "myworkdayjobs.com", "greenhouse.io", "lever.co", "ashbyhq.com",
        "icims.com", "smartrecruiters.com", "jobvite.com", "taleo.net",
        "successfactors.com", "oraclecloud.com", "brassring.com",
        "eightfold.ai", "phenompeople.com", "jobscore.com",
    }
    _tldextract_available = False

    import tldextract as _tldextract_mod
    _tldextract_inst = _tldextract_mod.TLDExtract(suffix_list_urls=())

    def _root_domain(url: str) -> str:
        if "://" not in url:
            url = "https://" + url
        host = urlparse(url).hostname or ""
        ext = _tldextract_inst.extract(host)
        return ext.registered_domain or host

# Path keywords that indicate a careers/jobs page (not a homepage).
_CAREERS_PATH_KEYWORDS = ("/career", "/job", "/work", "/hiring", "/talent", "/recruit")


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
# Redis cache helpers  (head_check:{fein})
# ─────────────────────────────────────────────────────────────────────────────

_CACHE_PREFIX = "head_check:"


def _cache_get(r, fein: str) -> "dict | None":
    raw = r.get(f"{_CACHE_PREFIX}{fein}")
    if not raw:
        return None
    try:
        return json.loads(raw)
    except Exception:
        return None


def _cache_set(r, fein: str, result: dict) -> None:
    try:
        r.setex(f"{_CACHE_PREFIX}{fein}", HEAD_CHECK_CACHE_TTL_S, json.dumps(result))
    except Exception as exc:
        log.warning("head_check cache write failed fein=%s: %s", fein, exc)


def _cache_delete(r, fein: str) -> None:
    try:
        r.delete(f"{_CACHE_PREFIX}{fein}")
    except Exception as exc:
        log.warning("head_check cache delete failed fein=%s: %s", fein, exc)


# ─────────────────────────────────────────────────────────────────────────────
# Retry tracking
# ─────────────────────────────────────────────────────────────────────────────

_RETRY_KEY_PREFIX = "head_check:retry:"
_RETRY_TTL_S      = 86400 * 3  # 3 days


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
    r.lpush(HEAD_CHECK_DLQ, payload)
    log.error("DLQ: fein=%s reason=%s retries=%d", fein, error_reason, retry_count)


# ─────────────────────────────────────────────────────────────────────────────
# HTTP HEAD + classification
# ─────────────────────────────────────────────────────────────────────────────

def _http_head(url: str) -> "tuple[requests.Response | None, Exception | None]":
    try:
        resp = requests.head(
            url,
            allow_redirects=True,
            timeout=(CONNECT_TIMEOUT, FETCH_TIMEOUT),
            headers={"User-Agent": "Mozilla/5.0 (compatible; H1BPipeline/1.0)"},
        )
        return resp, None
    except Exception as exc:
        return None, exc


def _classify(original_url: str, resp: "requests.Response | None") -> "tuple[str, str | None]":
    """
    Classify the HEAD response into one of 6 cases.
    Returns (case_label, final_url_or_None).

    case_label values:
      "careers_redirect"  → Case 1: same domain, careers path
      "ats_redirect"      → Case 2: known ATS domain
      "homepage_redirect" → Case 3: same domain, homepage/unknown path
      "unknown_redirect"  → Case 4: unrelated 3rd party
      "ok"                → Case 5: clean 200, URL healthy
      "dead"              → Case 6: error / non-2xx / timeout
    """
    if resp is None:
        return "dead", None

    status = resp.status_code
    final_url = resp.url

    if status not in (200, 301, 302, 303, 307, 308):
        return "dead", None

    redirected = bool(resp.history)
    if not redirected and status == 200:
        return "ok", final_url

    orig_root  = _root_domain(original_url)
    final_root = _root_domain(final_url)

    if final_root == orig_root:
        path = urlparse(final_url).path.lower()
        if any(kw in path for kw in _CAREERS_PATH_KEYWORDS):
            return "careers_redirect", final_url
        return "homepage_redirect", None

    if final_root in _KNOWN_ATS_DOMAINS:
        return "ats_redirect", final_url

    return "unknown_redirect", None


# ─────────────────────────────────────────────────────────────────────────────
# DB helpers
# ─────────────────────────────────────────────────────────────────────────────

def _load_careers_url(conn, fein: str) -> "str | None":
    row = conn.execute(
        "SELECT careers_url FROM fein_domain_map WHERE employer_fein = %s",
        (fein,),
    ).fetchone()
    return row["careers_url"] if row else None


def _write_careers(conn, fein: str, careers_url: str) -> None:
    conn.execute("""
        UPDATE fein_domain_map
        SET careers_url    = %s,
            careers_source = 'head_check',
            updated_at     = NOW()
        WHERE employer_fein = %s
    """, (careers_url, fein))


# ─────────────────────────────────────────────────────────────────────────────
# Routing helpers
# ─────────────────────────────────────────────────────────────────────────────

def _push_enrichment(r, fein: str, petition_count: int, trigger: str,
                     source: "str | None", tier: str) -> None:
    member = json.dumps({"fein": fein, "trigger": trigger, "source": source})
    if tier == "on_demand":
        r.lpush(ENRICHMENT_ON_DEMAND, member)
        log.debug("head_check: fein=%s → enrichment:on_demand trigger=%s", fein, trigger)
    else:
        r.zadd(ENRICHMENT_BATCH, {member: petition_count}, gt=True)
        log.debug("head_check: fein=%s → enrichment:batch trigger=%s", fein, trigger)


def _push_discovery(r, fein: str, petition_count: int, trigger: str,
                    source: "str | None") -> None:
    member = json.dumps({"fein": fein, "trigger": trigger, "source": source})
    if trigger == "redetect":
        r.zadd(DISCOVERY_REDETECT, {member: petition_count}, gt=True)
        log.debug("head_check: fein=%s → discovery:redetect petition_count=%d", fein, petition_count)
    else:
        r.zadd(DISCOVERY_BATCH, {member: petition_count}, gt=True)
        log.debug("head_check: fein=%s → discovery:batch trigger=%s petition_count=%d",
                  fein, trigger, petition_count)


# ─────────────────────────────────────────────────────────────────────────────
# Per-company processing
# ─────────────────────────────────────────────────────────────────────────────

def _process_company(r, fein: str, petition_count: int, trigger: str,
                     source: "str | None", tier: str) -> bool:
    """
    Run HEAD check for one company. Returns True on success, False on transient error.

    tier: "on_demand" | "batch" — determines which enrichment lane to use for Cases 3,4,6.
    """
    conn = None
    try:
        conn = get_conn()
        careers_url = _load_careers_url(conn, fein)
        if not careers_url:
            # careers_url vanished since the producer pushed this item — push to enrichment.
            log.info("head_check: fein=%s careers_url NULL in DB — routing to enrichment", fein)
            _push_enrichment(r, fein, petition_count, trigger, source, tier)
            return True

        # Check Redis cache first
        cached = _cache_get(r, fein)
        if cached and cached.get("careers_url") == careers_url:
            case_label = cached["case"]
            final_url  = cached.get("final_url")
            log.debug("head_check: fein=%s cache HIT case=%s", fein, case_label)
        else:
            # Cache miss or URL changed — do HTTP HEAD
            resp, exc = _http_head(careers_url)
            if exc:
                log.info("head_check: fein=%s HEAD failed: %s", fein, exc)
            case_label, final_url = _classify(careers_url, resp)
            _cache_set(r, fein, {
                "case":       case_label,
                "final_url":  final_url,
                "careers_url": careers_url,
            })
            log.info("head_check: fein=%s case=%s final_url=%s trigger=%s",
                     fein, case_label, final_url, trigger)

        # ── Route based on case ───────────────────────────────────────────────
        if case_label in ("careers_redirect", "ats_redirect"):
            # Case 1 / 2 — redirect found a valid URL; update DB and invalidate cache
            _write_careers(conn, fein, final_url)
            conn.commit()
            _cache_delete(r, fein)
            if trigger == "on_demand":
                log.info("head_check: fein=%s Case 1/2 on_demand → STOP", fein)
            else:
                _push_discovery(r, fein, petition_count, trigger, source)

        elif case_label == "ok":
            # Case 5 — URL still healthy; no DB write needed
            if trigger == "on_demand":
                log.info("head_check: fein=%s Case 5 on_demand → STOP", fein)
            else:
                _push_discovery(r, fein, petition_count, trigger, source)

        else:
            # Cases 3, 4, 6 — URL dead or homepage; send to enrichment
            _push_enrichment(r, fein, petition_count, trigger, source, tier)

        _clear_retry(r, fein)
        return True

    except Exception as exc:
        log.error("head_check: unexpected error fein=%s: %s", fein, exc, exc_info=True)
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
# Main loop
# ─────────────────────────────────────────────────────────────────────────────

def run_worker(once: bool = False) -> None:
    r = get_redis()
    processed = {"n": 0}
    _instance = os.environ.get("WORKER_INSTANCE", "")
    _hb_name  = f"head_check_worker@{_instance}" if _instance else "head_check_worker"
    hb = Heartbeat(r, _hb_name,
                   lambda: processed["n"], interval_s=HEAD_CHECK_HEARTBEAT_S).start()

    log.info("head-check-worker started (instance=%r)", _instance)

    _MAINTENANCE_MAX_S = 4 * 3600

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
                    return
                log.info("Maintenance window active — pausing 30s (%.0fm elapsed)", elapsed / 60)
                time.sleep(30)

            # BLPOP checks on_demand first (priority), falls back to batch.
            # Returns (list_key, value) or None on timeout.
            result = r.blpop([HEAD_CHECK_ON_DEMAND, HEAD_CHECK_BATCH],
                             timeout=WORKER_BLOCK_SECS)

            if result is None:
                # Timeout — check if both queues are genuinely empty.
                if r.llen(HEAD_CHECK_ON_DEMAND) == 0 and r.llen(HEAD_CHECK_BATCH) == 0:
                    log.info("head_check: both queues empty — exiting")
                    break
                continue

            queue_key, raw_member = result
            tier = "on_demand" if queue_key in (
                HEAD_CHECK_ON_DEMAND,
                HEAD_CHECK_ON_DEMAND.encode(),
            ) else "batch"

            # Parse payload
            try:
                data           = json.loads(raw_member)
                fein           = data["fein"]
                trigger        = data.get("trigger", "staleness")
                source         = data.get("source")
                petition_count = int(data.get("petition_count", 0))
            except (json.JSONDecodeError, KeyError, TypeError):
                raw_str = raw_member.decode() if isinstance(raw_member, bytes) else raw_member
                log.error("head_check: malformed member %r — sending to DLQ", raw_str)
                r.lpush(HEAD_CHECK_DLQ, json.dumps({
                    "fein": "MALFORMED", "error_reason": "malformed_member",
                    "raw": repr(raw_str), "failed_at": time.time(),
                }))
                continue

            retry_count = _get_retry_count(r, fein)
            if retry_count >= HEAD_CHECK_MAX_RETRIES:
                _move_to_dlq(r, fein, "max_retries_exceeded", retry_count)
                _clear_retry(r, fein)
                continue

            success = _process_company(r, fein, petition_count, trigger, source, tier)
            processed["n"] += 1

            if not success:
                count = _incr_retry(r, fein)
                if count >= HEAD_CHECK_MAX_RETRIES:
                    _move_to_dlq(r, fein, "processing_error", count)
                    _clear_retry(r, fein)
                else:
                    # Re-push to same queue tier for retry (RPUSH so it goes to back of LINE)
                    member = json.dumps({
                        "fein": fein, "trigger": trigger, "source": source,
                        "petition_count": petition_count,
                    })
                    if tier == "on_demand":
                        r.rpush(HEAD_CHECK_ON_DEMAND, member)
                    else:
                        r.rpush(HEAD_CHECK_BATCH, member)
                    log.warning("head_check: fein=%s retry %d/%d", fein, count, HEAD_CHECK_MAX_RETRIES)

            if once:
                break

    finally:
        hb.stop()

    log.info("head-check-worker stopped — processed %d companies", processed["n"])


if __name__ == "__main__":
    init_logging("head_check_worker")
    once = "--once" in sys.argv
    run_worker(once=once)
