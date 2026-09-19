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
import socket
import sys
import time
from urllib.parse import urlparse, urljoin

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import requests

from jobs.http_safe import is_private_host as _is_private_host, make_safe_session as _make_safe_session

from config import (
    CONNECT_TIMEOUT,
    DISCOVERY_BATCH,
    DISCOVERY_REDETECT,
    ENRICHMENT_BATCH,
    ENRICHMENT_DLQ,
    ENRICHMENT_ON_DEMAND,
    FETCH_TIMEOUT,
    HEAD_CHECK_BATCH,
    HEAD_CHECK_CACHE_PREFIX,
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
        ext = _tldextract_inst(host)
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

_CACHE_PREFIX = HEAD_CHECK_CACHE_PREFIX


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


# Release the staleness_checker enqueue-dedup guard (scripts/staleness_checker.py) once
# an item reaches a terminal outcome. Only staleness_checker's HEAD_CHECK_BATCH producer
# ever sets this key — deleting it for items that came from elsewhere (api.py on_demand,
# job_monitor.py) is a harmless no-op. Kept until here (not cleared on transient retry)
# so a repeated staleness_checker run can't re-enqueue a company that's still pending.
_ENQUEUE_GUARD_PREFIX = "head_check:enqueue_guard:"


def _clear_enqueue_guard(r, fein: str, trigger: str, source=None) -> None:
    r.delete(f"{_ENQUEUE_GUARD_PREFIX}{HEAD_CHECK_BATCH}:{fein}:{trigger}:{source or ''}")


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

_MAX_REDIRECTS = 10

def _http_head(url: str) -> "tuple[requests.Response | None, Exception | None, str]":
    """Return (response, exception, logical_url).

    logical_url is the last URL we requested before the SSRFAdapter may have
    rewritten the host to an IP literal — _classify must use it, not resp.url,
    so that HTTP→IP rewrites are not misidentified as redirects.
    """
    headers = {"User-Agent": "Mozilla/5.0 (compatible; H1BPipeline/1.0)"}
    current_url = url
    _sess = _make_safe_session()
    try:
        for _ in range(_MAX_REDIRECTS):
            logical_url = current_url
            resp = _sess.head(
                current_url,
                allow_redirects=False,
                timeout=(CONNECT_TIMEOUT, FETCH_TIMEOUT),
                headers=headers,
            )
            if resp.status_code not in (301, 302, 303, 307, 308):
                return resp, None, logical_url
            location = resp.headers.get("Location", "")
            if not location:
                return None, None, logical_url
            next_url = urljoin(current_url, location)
            if not _is_safe_url(next_url):
                return None, None, logical_url
            current_url = next_url
        # Redirect chain exhausted without reaching a terminal response — dead, not
        # a 3xx that _classify() could mistake for a completed redirect.
        return None, None, logical_url
    except Exception as exc:
        return None, exc, url


def _is_safe_url(url: str) -> bool:
    """Return True if url has a public http/https scheme and a non-private hostname."""
    from urllib.parse import urlparse
    parsed = urlparse(url)
    if parsed.scheme not in ("http", "https"):
        return False
    host = parsed.hostname or ""
    return bool(host) and not _is_private_host(host)

def _classify(
    original_url: str,
    resp: "requests.Response | None",
    logical_url: "str | None" = None,
) -> "tuple[str, str | None]":
    """
    Classify the HEAD response into one of 6 cases.
    Returns (case_label, final_url_or_None).

    logical_url: the pre-SSRFAdapter hostname URL captured by _http_head before
    the request is sent.  SSRFAdapter rewrites resp.url to an IP literal for
    HTTP requests, so resp.url cannot be compared against original_url directly.
    Pass logical_url to use the hostname URL as final_url instead.

    case_label values:
      "careers_redirect"  -> Case 1: same domain, careers path
      "ats_redirect"      -> Case 2: known ATS domain
      "homepage_redirect" -> Case 3: same domain, homepage/unknown path
      "unknown_redirect"  -> Case 4: unrelated 3rd party
      "ok"                -> Case 5: clean 200, URL healthy
      "dead"              -> Case 6: error / non-2xx / timeout
    """
    if resp is None:
        return "dead", None

    status = resp.status_code
    # Use the caller-supplied logical URL (hostname) to avoid comparing against
    # the IP-literal URL that SSRFAdapter writes into resp.url for HTTP requests.
    final_url = logical_url if logical_url is not None else resp.url

    if status in (403, 405):
        # 403 Forbidden / 405 Method Not Allowed — server is alive but blocks HEAD.
        # Treat as inconclusive: route same as Case 5 (ok) to avoid false dead-url detection.
        return "ok", final_url
    if status not in (200, 301, 302, 303, 307, 308):
        return "dead", None

    redirected = bool(resp.history) or (final_url != original_url)
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
            careers_url_verified_at = NOW(),
            updated_at     = NOW()
        WHERE employer_fein = %s
    """, (careers_url, fein))


def _mark_verified(conn, fein: str) -> None:
    conn.execute("""
        UPDATE fein_domain_map
        SET careers_url_verified_at = NOW()
        WHERE employer_fein = %s
    """, (fein,))


# ─────────────────────────────────────────────────────────────────────────────
# Routing helpers
# ─────────────────────────────────────────────────────────────────────────────

def _push_enrichment(r, fein: str, petition_count: int, trigger: str,
                     source: "str | None", tier: str) -> None:
    member = json.dumps({"fein": fein, "trigger": trigger, "source": source, "tier": tier})
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
            _clear_retry(r, fein, trigger, source=source, tier=tier)
            _clear_enqueue_guard(r, fein, trigger, source=source)
            return True

        # Validate careers_url is a public URL before requesting
        if not _is_safe_url(careers_url):
            log.warning("head_check: fein=%s careers_url %r is not a safe public URL -- routing to enrichment", fein, careers_url)
            _push_enrichment(r, fein, petition_count, trigger, source, tier)
            _clear_retry(r, fein, trigger, source=source, tier=tier)
            _clear_enqueue_guard(r, fein, trigger, source=source)
            return True

        # Check Redis cache first
        cached = _cache_get(r, fein)
        if cached and cached.get("careers_url") == careers_url:
            case_label = cached["case"]
            final_url  = cached.get("final_url")
            log.debug("head_check: fein=%s cache HIT case=%s", fein, case_label)
        else:
            # Cache miss or URL changed — do HTTP HEAD
            resp, exc, logical_url = _http_head(careers_url)
            if exc:
                log.info("head_check: fein=%s HEAD failed: %s", fein, exc)
            case_label, final_url = _classify(careers_url, resp, logical_url)
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
            # Case 5 — URL still healthy; record verification timestamp
            _mark_verified(conn, fein)
            conn.commit()
            if trigger == "on_demand":
                log.info("head_check: fein=%s Case 5 on_demand → STOP", fein)
            else:
                _push_discovery(r, fein, petition_count, trigger, source)

        else:
            # Cases 3, 4, 6 — URL dead or homepage; send to enrichment
            _push_enrichment(r, fein, petition_count, trigger, source, tier)

        _clear_retry(r, fein, trigger, source=source, tier=tier)
        _clear_enqueue_guard(r, fein, trigger, source=source)
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

def _reclaim_inflight(r, own_ondemand_key: str, own_batch_key: str) -> None:
    """On startup, push any items left in ANY prior instance's inflight lists back to their source queues.

    Scans all head_check:inflight:instance:*:{on_demand,batch} keys so that items from a crashed
    instance (different PID → different key) are recovered even when WORKER_INSTANCE is not set.
    Each key's tier suffix (":on_demand" / ":batch") determines which source queue to restore to —
    items are raw payload strings (no wrapper), matching what LMOVE/BLMOVE write into these lists.
    """
    cursor = 0
    all_keys: list = []
    while True:
        cursor, keys = r.scan(cursor, match="head_check:inflight:instance:*", count=50)
        all_keys.extend(k.decode() if isinstance(k, bytes) else k for k in keys)
        if cursor == 0:
            break

    for own_key in (own_ondemand_key, own_batch_key):
        if own_key not in all_keys:
            all_keys.append(own_key)

    for key in all_keys:
        if key.endswith(":on_demand"):
            tier_suffix, target_queue = "on_demand", HEAD_CHECK_ON_DEMAND
        elif key.endswith(":batch"):
            tier_suffix, target_queue = "batch", HEAD_CHECK_BATCH
        else:
            log.warning("head_check: skipping inflight key %s — unrecognized tier suffix", key)
            continue

        is_own = key in (own_ondemand_key, own_batch_key)
        if not is_own:
            # Skip keys whose worker is still alive (heartbeat present).
            # suffix is "{hostname}:{pid}" (no-instance mode) or a bare instance number.
            suffix = key[len("head_check:inflight:instance:"):-len(f":{tier_suffix}")]
            if ":" in suffix:
                # No-instance mode: suffix == "hostname:pid" → direct heartbeat key.
                if r.exists(f"worker:alive:head_check_worker:{suffix}"):
                    log.debug("head_check: skipping inflight key %s — worker still alive", key)
                    continue
            else:
                # Instance mode: scan for any alive heartbeat for this instance number.
                _cursor, hb_keys = 0, []
                while True:
                    _cursor, _batch = r.scan(_cursor, match=f"worker:alive:head_check_worker@{suffix}:*", count=10)
                    hb_keys.extend(_batch)
                    if _cursor == 0:
                        break
                if hb_keys:
                    log.debug("head_check: skipping inflight key %s — worker still alive", key)
                    continue
        # own_inflight keys always reclaim (this process's own heartbeat is already
        # alive at this point, so the alive-check above would otherwise skip it forever,
        # stranding any items left over from a prior crash that reused the same key).
        reclaimed = 0
        while True:
            # Atomic move: a crash between separate RPOP + RPUSH would drop the item.
            raw = r.lmove(key, target_queue, "RIGHT", "RIGHT")
            if raw is None:
                break
            reclaimed += 1
        if reclaimed:
            log.warning("head_check: reclaimed %d inflight items from orphaned key %s", reclaimed, key)
        r.delete(key)


def _pop_with_inflight(r, own_ondemand_key: str, own_batch_key: str, timeout: float):
    """
    Priority-aware atomic pop with at-least-once guarantee via inflight lists.

    1. Try non-blocking LMOVE from on_demand first (priority).
    2. If on_demand is empty, block on BLMOVE from batch for up to 1s, then
       re-check on_demand. Avoids a busy-poll loop while still picking up
       on_demand items within ~1s of enqueue.

    Returns (queue_key, tier, raw_member) or None on timeout.

    LMOVE/BLMOVE are atomic — the item is either in the source queue or the
    inflight list, never in neither (unlike the prior BLPOP + separate LPUSH,
    which had a gap where a crash between the two calls silently dropped the
    item). Direction LEFT→LEFT replicates the original BLPOP (left-pop) +
    LPUSH (left-insert) ordering.
    """
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        raw = r.lmove(HEAD_CHECK_ON_DEMAND, own_ondemand_key, "LEFT", "LEFT")
        if raw is not None:
            return (HEAD_CHECK_ON_DEMAND, "on_demand", raw)

        remaining = deadline - time.monotonic()
        if remaining <= 0:
            break
        wait_s = min(1.0, remaining)
        raw = r.blmove(HEAD_CHECK_BATCH, own_batch_key, wait_s, "LEFT", "LEFT")
        if raw is not None:
            return (HEAD_CHECK_BATCH, "batch", raw)
    return None


def run_worker(once: bool = False) -> None:
    r = get_redis()
    processed = {"n": 0}
    _instance = os.environ.get("WORKER_INSTANCE", "")
    _hb_name  = f"head_check_worker@{_instance}" if _instance else "head_check_worker"
    hb = Heartbeat(r, _hb_name,
                   lambda: processed["n"], interval_s=HEAD_CHECK_HEARTBEAT_S).start()

    _inflight_suffix       = _instance if _instance else f"{socket.gethostname()}:{os.getpid()}"
    _own_inflight_ondemand = f"head_check:inflight:instance:{_inflight_suffix}:on_demand"
    _own_inflight_batch    = f"head_check:inflight:instance:{_inflight_suffix}:batch"
    _reclaim_inflight(r, _own_inflight_ondemand, _own_inflight_batch)

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
                    sys.exit(1)
                log.info("Maintenance window active — pausing 30s (%.0fm elapsed)", elapsed / 60)
                time.sleep(30)

            # Atomic pop: item lands in the tier's own inflight list the instant it
            # leaves its source queue — never in neither (see _pop_with_inflight).
            result = _pop_with_inflight(r, _own_inflight_ondemand, _own_inflight_batch,
                                        timeout=WORKER_BLOCK_SECS)

            if result is None:
                # Timeout — check if both queues are genuinely empty.
                if r.llen(HEAD_CHECK_ON_DEMAND) == 0 and r.llen(HEAD_CHECK_BATCH) == 0:
                    log.info("head_check: both queues empty — exiting")
                    break
                continue

            _, tier, raw_member = result
            _member_str = raw_member.decode() if isinstance(raw_member, bytes) else raw_member
            _own_inflight_key = _own_inflight_ondemand if tier == "on_demand" else _own_inflight_batch

            # Parse payload
            try:
                data           = json.loads(raw_member)
                fein           = data["fein"]
                trigger        = data.get("trigger", "staleness")
                source         = data.get("source")
                petition_count = int(data.get("petition_count", 0))
            except (json.JSONDecodeError, KeyError, TypeError, ValueError):
                log.error("head_check: malformed member %r — sending to DLQ", _member_str)
                r.lpush(HEAD_CHECK_DLQ, json.dumps({
                    "fein": "MALFORMED", "error_reason": "malformed_member",
                    "raw": repr(_member_str), "failed_at": time.time(),
                }))
                r.lrem(_own_inflight_key, 1, _member_str)
                continue

            retry_count = _get_retry_count(r, fein, trigger, source=source, tier=tier)
            if retry_count >= HEAD_CHECK_MAX_RETRIES:
                _move_to_dlq(r, fein, "max_retries_exceeded", retry_count)
                _clear_retry(r, fein, trigger, source=source, tier=tier)
                _clear_enqueue_guard(r, fein, trigger, source=source)
                r.lrem(_own_inflight_key, 1, _member_str)
                continue

            try:
                success = _process_company(r, fein, petition_count, trigger, source, tier)
            finally:
                r.lrem(_own_inflight_key, 1, _member_str)
            processed["n"] += 1

            if not success:
                count = _incr_retry(r, fein, trigger, source=source, tier=tier)
                if count >= HEAD_CHECK_MAX_RETRIES:
                    _move_to_dlq(r, fein, "processing_error", count)
                    _clear_retry(r, fein, trigger, source=source, tier=tier)
                    _clear_enqueue_guard(r, fein, trigger, source=source)
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
