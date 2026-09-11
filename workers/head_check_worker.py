"""
workers/head_check_worker.py â€” HEAD check worker for H1B pipeline.

Sits between producers (api.py, staleness_checker, job_monitor) and the
enrichment/discovery workers. Verifies whether a known careers_url is still
alive before deciding where to route the company next.

Pop order (priority): HEAD_CHECK_ON_DEMAND (api.py) first, HEAD_CHECK_BATCH second.
Both are Redis LISTs â€” BLPOP handles strict priority ordering natively.

For each company:
  1. Check Redis cache head_check:{fein} (TTL = HEAD_CHECK_CACHE_TTL_S / 6h default).
     Cache hit â†’ use stored result, skip HTTP. Prevents redundant HEAD requests when
     the same FEIN is pushed twice within the TTL window.
  2. Cache miss â†’ HTTP HEAD on careers_url, follow redirects.
  3. Classify result into 6 cases and route:
       Case 1: redirect to same domain, careers path â†’ write new URL, â†’ discovery
       Case 2: redirect to known ATS domain         â†’ write new URL, â†’ discovery
       Case 3: redirect to same domain, homepage    â†’ â†’ enrichment
       Case 4: redirect to unrelated 3rd party      â†’ â†’ enrichment
       Case 5: clean 200 (URL healthy)              â†’ â†’ discovery (redetect/staleness only)
       Case 6: timeout / connection error           â†’ â†’ enrichment
  4. on_demand trigger: STOP at Cases 1, 2, 5 (no discovery push).
     redetect trigger:  Cases 1, 2, 5 â†’ discovery:redetect.
     staleness trigger: Cases 1, 2, 5 â†’ discovery:batch.

Worker exits cleanly when both queues are empty.

Usage:
  python -m workers.head_check_worker
  python -m workers.head_check_worker --once
"""

import json
import os
import sys
import time
from urllib.parse import urlparse, urljoin

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import requests

from jobs.http_safe import is_private_host as _is_private_host

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
    HEAD_CHECK_INFLIGHT,
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

# Registered domains of known ATS platforms â€” imported from discover_h1b_ats to stay in sync.
try:
    from scripts.discover_h1b_ats import _KNOWN_ATS_DOMAINS, _root_domain
    _tldextract_available = True
except Exception as _import_err:
    log.warning("discover_h1b_ats import failed (%s) â€” using fallback ATS domain set", _import_err)
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


# â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
# Maintenance window
# â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

def _is_maintenance(r) -> bool:
    try:
        return bool(r.exists(REDIS_DB_MAINTENANCE))
    except Exception as exc:
        log.warning("Redis maintenance check failed (%s) â€” assuming not in maintenance", exc)
        return False


# â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
# Redis cache helpers  (head_check:{fein})
# â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

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


# â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
# Retry tracking
# â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

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


# â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
# DLQ
# â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

def _move_to_dlq(r, fein: str, error_reason: str, retry_count: int) -> None:
    payload = json.dumps({
        "fein":         fein,
        "error_reason": error_reason,
        "retry_count":  retry_count,
        "failed_at":    time.time(),
    })
    r.lpush(HEAD_CHECK_DLQ, payload)
    log.error("DLQ: fein=%s reason=%s retries=%d", fein, error_reason, retry_count)


# â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
# HTTP HEAD + classification
# â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

_MAX_REDIRECTS = 10

def _http_head(url: str) -> "tuple[requests.Response | None, Exception | None]":
    headers = {"User-Agent": "Mozilla/5.0 (compatible; H1BPipeline/1.0)"}
    current_url = url
    try:
        for _ in range(_MAX_REDIRECTS):
            resp = requests.head(
                current_url,
                allow_redirects=False,
                timeout=(CONNECT_TIMEOUT, FETCH_TIMEOUT),
                headers=headers,
            )
            if resp.status_code not in (301, 302, 303, 307, 308):
                return resp, None
            location = resp.headers.get("Location", "")
            if not location:
                return resp, None
            next_url = urljoin(current_url, location)
            if not _is_safe_url(next_url):
                return resp, None
            current_url = next_url
        return resp, None
    except Exception as exc:
        return None, exc


def _is_safe_url(url: str) -> bool:
    """Return True if url has a public http/https scheme and a non-private hostname."""
    from urllib.parse import urlparse
    parsed = urlparse(url)
    if parsed.scheme not in ("http", "https"):
        return False
    host = parsed.hostname or ""
    return bool(host) and not _is_private_host(host)

def _classify(original_url: str, resp: "requests.Response | None") -> "tuple[str, str | None]":
    """
    Classify the HEAD response into one of 6 cases.
    Returns (case_label, final_url_or_None).

    case_label values:
      "careers_redirect"  â†’ Case 1: same domain, careers path
      "ats_redirect"      â†’ Case 2: known ATS domain
      "homepage_redirect" â†’ Case 3: same domain, homepage/unknown path
      "unknown_redirect"  â†’ Case 4: unrelated 3rd party
      "ok"                â†’ Case 5: clean 200, URL healthy
      "dead"              â†’ Case 6: error / non-2xx / timeout
    """
    if resp is None:
        return "dead", None

    status = resp.status_code
    final_url = resp.url

    if status in (403, 405):
        # 403 Forbidden / 405 Method Not Allowed — server is alive but blocks HEAD.
        # Treat as inconclusive: route same as Case 5 (ok) to avoid false dead-url detection.
        return "ok", final_url
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


# â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
# DB helpers
# â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

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


# â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
# Routing helpers
# â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

def _push_enrichment(r, fein: str, petition_count: int, trigger: str,
                     source: "str | None", tier: str) -> None:
    member = json.dumps({"fein": fein, "trigger": trigger, "source": source})
    if tier == "on_demand":
        r.lpush(ENRICHMENT_ON_DEMAND, member)
        log.debug("head_check: fein=%s â†’ enrichment:on_demand trigger=%s", fein, trigger)
    else:
        r.zadd(ENRICHMENT_BATCH, {member: petition_count}, gt=True)
        log.debug("head_check: fein=%s â†’ enrichment:batch trigger=%s", fein, trigger)


def _push_discovery(r, fein: str, petition_count: int, trigger: str,
                    source: "str | None") -> None:
    member = json.dumps({"fein": fein, "trigger": trigger, "source": source})
    if trigger == "redetect":
        r.zadd(DISCOVERY_REDETECT, {member: petition_count}, gt=True)
        log.debug("head_check: fein=%s â†’ discovery:redetect petition_count=%d", fein, petition_count)
    else:
        r.zadd(DISCOVERY_BATCH, {member: petition_count}, gt=True)
        log.debug("head_check: fein=%s â†’ discovery:batch trigger=%s petition_count=%d",
                  fein, trigger, petition_count)


# â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
# Per-company processing
# â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

def _process_company(r, fein: str, petition_count: int, trigger: str,
                     source: "str | None", tier: str) -> bool:
    """
    Run HEAD check for one company. Returns True on success, False on transient error.

    tier: "on_demand" | "batch" â€” determines which enrichment lane to use for Cases 3,4,6.
    """
    conn = None
    try:
        conn = get_conn()
        careers_url = _load_careers_url(conn, fein)
        if not careers_url:
            # careers_url vanished since the producer pushed this item â€” push to enrichment.
            log.info("head_check: fein=%s careers_url NULL in DB â€” routing to enrichment", fein)
            _push_enrichment(r, fein, petition_count, trigger, source, tier)
            return True

        # Validate careers_url is a public URL before requesting
        if not _is_safe_url(careers_url):
            log.warning("head_check: fein=%s careers_url %r is not a safe public URL -- routing to enrichment", fein, careers_url)
            _push_enrichment(r, fein, petition_count, trigger, source, tier)
            return True

        # Check Redis cache first
        cached = _cache_get(r, fein)
        if cached and cached.get("careers_url") == careers_url:
            case_label = cached["case"]
            final_url  = cached.get("final_url")
            log.debug("head_check: fein=%s cache HIT case=%s", fein, case_label)
        else:
            # Cache miss or URL changed â€” do HTTP HEAD
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

        # â”€â”€ Route based on case â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
        if case_label in ("careers_redirect", "ats_redirect"):
            # Case 1 / 2 â€” redirect found a valid URL; update DB and invalidate cache
            _write_careers(conn, fein, final_url)
            conn.commit()
            _cache_delete(r, fein)
            if trigger == "on_demand":
                log.info("head_check: fein=%s Case 1/2 on_demand â†’ STOP", fein)
            else:
                _push_discovery(r, fein, petition_count, trigger, source)

        elif case_label == "ok":
            # Case 5 â€” URL still healthy; no DB write needed
            if trigger == "on_demand":
                log.info("head_check: fein=%s Case 5 on_demand â†’ STOP", fein)
            else:
                _push_discovery(r, fein, petition_count, trigger, source)

        else:
            # Cases 3, 4, 6 â€” URL dead or homepage; send to enrichment
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


# â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
# Main loop
# â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

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
                    log.error("Maintenance window exceeded %dh â€” exiting to allow restart",
                              _MAINTENANCE_MAX_S // 3600)
                    return
                log.info("Maintenance window active â€” pausing 30s (%.0fm elapsed)", elapsed / 60)
                time.sleep(30)

            # BLPOP checks on_demand first (priority), falls back to batch.
            # Returns (list_key, value) or None on timeout.
            result = r.blpop([HEAD_CHECK_ON_DEMAND, HEAD_CHECK_BATCH],
                             timeout=WORKER_BLOCK_SECS)

            if result is None:
                # Timeout â€” check if both queues are genuinely empty.
                if r.llen(HEAD_CHECK_ON_DEMAND) == 0 and r.llen(HEAD_CHECK_BATCH) == 0:
                    log.info("head_check: both queues empty â€” exiting")
                    break
                continue

            queue_key, raw_member = result
            tier = "on_demand" if queue_key in (
                HEAD_CHECK_INFLIGHT,
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
                log.error("head_check: malformed member %r â€” sending to DLQ", raw_str)
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

            r.incr(HEAD_CHECK_INFLIGHT)
            try:
                success = _process_company(r, fein, petition_count, trigger, source, tier)
            finally:
                r.decr(HEAD_CHECK_INFLIGHT)
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

    log.info("head-check-worker stopped â€” processed %d companies", processed["n"])


if __name__ == "__main__":
    init_logging("head_check_worker")
    once = "--once" in sys.argv
    run_worker(once=once)
