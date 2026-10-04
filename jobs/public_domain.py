"""
jobs/public_domain.py — Public domain resolution for H1B pipeline enrichment.

Resolves internal/email domains (e.g. fmr.com, jpmchase.com) to the company's
real public-facing website domain (e.g. fidelity.com, jpmorgan.com).

Three-step algorithm:
  1. HTTP redirect follow    — jpmchase.com → jpmorgan.com
  2. Root-domain fallback   — ny.email.gs.com → gs.com → goldmansachs.com
  3. CT log (certspotter)   — fmr.com → fidelity.com via cert SANs
     Fallback: crt.sh if certspotter unavailable.

Returns (public_domain, method, retry_after, last_status, host) where retry_after is non-None
only on certspotter 429 — caller should re-queue the company with that delay, and host is the
exact host that answered (stored in fein_domain_map.public_domain_host, used only to fetch the
site). Every probe (_probe_host) fetches bare + www, classifies the final response with the
Rule 1 vendor-signature classifier (jobs/pd_classify.py) and only a 2xx / challenge page confirms;
parked, junk-landing and platform-template results store nothing; a 403 is kept as the pd only in
relay_mode (home relay drain worker). Fetches use a Chrome-impersonated curl_cffi session by default
(docs/discovery-pipeline-hardening.md Part 2); pass session= to inject a different one.
"""

import ipaddress
import json
import socket
import threading
import time
from urllib.parse import urljoin, urlparse

import requests
from jobs.http_safe import (
    make_safe_session as _make_safe_session,
    make_safe_curl_session as _make_safe_curl_session,
)
import tldextract
_tldextract = tldextract.TLDExtract(suffix_list_urls=(), cache_dir=None)
import urllib3

_urllib3_no_ssl_warn = urllib3.exceptions.InsecureRequestWarning

from config import (
    CERTSPOTTER_API_KEY, PD_BODY_MAX_BYTES, PD_SNIPPET_CHARS, PD_PROBE_RECORD_ENABLED,
    CF_WORKER_MAX_HOPS,
)
from logger import get_logger
from db.external_api_health import record_external_request, get_day_request_count
from db.pd_probe import record_probe
from jobs.pd_classify import (
    PLATFORM_REASON_PREFIX, apply_outcome_rules, classify, describe, platform_no_site,
)

try:
    from config import CF_WORKER_URL, CF_WORKER_SECRET, CF_WORKER_DAILY_LIMIT
except ImportError:
    CF_WORKER_URL = ""
    CF_WORKER_SECRET = ""
    CF_WORKER_DAILY_LIMIT = 0

log = get_logger(__name__)


def _is_private_ip_literal(host: str) -> bool:
    """Return True if host is an IP address literal in a private/loopback/link-local range."""
    try:
        addr = ipaddress.ip_address(host)
        return addr.is_private or addr.is_loopback or addr.is_link_local
    except ValueError:
        return False  # hostname, not an IP literal


def _is_public_host(host: str) -> bool:
    """Return True if host resolves only to globally-routable addresses.

    Blocks loopback, link-local, RFC1918, CGNAT (100.64/10), and cloud metadata
    (169.254.169.254) — same set as api.py/_is_private_host.  Returns False on
    DNS failure (fail-closed).
    """
    if not host:
        return False
    try:
        infos = socket.getaddrinfo(host, None)
        if not infos:
            return False
        for info in infos:
            addr = ipaddress.ip_address(info[4][0])
            if (
                addr.is_loopback or addr.is_link_local or addr.is_private
                or addr.is_reserved or addr.is_unspecified or addr.is_multicast
                or (isinstance(addr, ipaddress.IPv4Address)
                    and addr in ipaddress.IPv4Network("100.64.0.0/10"))
            ):
                return False
        return True
    except Exception:
        return False

# Root domains belonging to cloud / email / CDN providers — never a real company domain
GENERIC_ROOTS = {
    "outlook.com", "hotmail.com", "gmail.com", "yahoo.com",
    "pphosted.com", "mimecast.com", "proofpoint.com", "messagelabs.com",
    "cloudflare.com", "fastly.com", "akamai.com", "amazonaws.com",
    "azure.com", "cloudfront.net", "googleusercontent.com",
    "office365.com", "microsoft.com", "googlehosted.com",
}

# Domains belonging to bot-protection / DDoS-mitigation vendors.
# When a redirect chain ends on one of these, the origin domain is the real company domain.
_CHALLENGE_DOMAINS = frozenset({
    "perfdrive.com",     # PerfDrive / Shape Security (F5)
    "imperva.com",       # Imperva
    "incapsula.com",     # Imperva legacy brand
    "sucuri.net",        # Sucuri (GoDaddy)
    "radwarecloud.com",  # Radware Bot Manager
    "reblaze.com",       # Reblaze
    "perimeterx.net",   # PerimeterX (HUMAN Security)
    "ddos-guard.net",    # DDoS-Guard
    "datadome.co",       # DataDome
    "kasada.io",         # Kasada
})

# Header-based challenge detection (x-iinfo, x-sucuri-id, cf-mitigated, ...) lives in
# jobs/pd_classify.py::challenge_reason — vendor-presence headers only count when the response
# is not a full page.

_safe_session = _make_safe_session()

# Chrome-impersonated fetching (docs/discovery-pipeline-hardening.md Part 2) —
# this is the DEFAULT session for the redirect/web-liveness probes below (_probe_host,
# _redirect_domain). CT-log queries (_ct_certspotter, _ct_crtsh) keep using plain `requests` above —
# those hit certspotter/crt.sh directly, not the company's own WAF-fronted site, so Chrome
# impersonation buys nothing there.
#
# session= params on _probe_host/_redirect_domain/discover_public_domain let Part 3's mobile
# relay worker pass an identical function call with a SOCKS5-proxied session instead of
# this module default — same code path, different egress IP.
#
# curl_cffi sessions are not documented as thread-safe (no internal locking around the
# underlying libcurl easy handle), so a single import-time session shared across threads
# risks corrupted/interleaved requests if any caller ever runs these probes concurrently
# (e.g. from a ThreadPoolExecutor). Lazily creating one session per thread, cached on a
# threading.local, keeps the "session=None → use the module default" call sites unchanged
# while making that default safe under concurrency; single-threaded callers (today's only
# callers) pay one extra session construction on first use, same as before.
_thread_local = threading.local()


def _get_default_curl_session():
    sess = getattr(_thread_local, "session", None)
    if sess is None:
        sess = _make_safe_curl_session()
        _thread_local.session = sess
    return sess


def _fetch_via_worker(url: str) -> "dict | None":
    """Proxy a single GET through the Cloudflare probe Worker — fallback tier used when
    the direct curl_cffi attempt to `url` comes back inconclusive (403/429/503).

    Shares the same daily cap and atomic external_api_health tracking (service="cf_worker")
    as jobs/ats/career_detector.py::_fetch_via_worker and
    scripts/discover_h1b_ats.py::_fetch_via_worker — all three count against one shared
    daily total. The shared limit is NOT raised for this addition (locked 2026-09-27).

    Returns a res-shaped dict {status, headers, body, final_url, cookies, error_type} so
    jobs/pd_classify.classify can judge the Worker's response exactly like a direct fetch —
    status is the WORKER's read of the target site, which may itself still be non-2xx.
    Requests the same bounded body as the direct tier (PD_BODY_MAX_BYTES) and a hop cap
    (CF_WORKER_MAX_HOPS). The redeployed Worker also returns response headers, Set-Cookie
    names across the redirect chain and a `truncated` flag; an OLD Worker omits them and
    we fall back to empty headers/cookies (body/path/title signatures still work).
    A body that hit the cap is marked oversize (x-scan-too-large, body "") — a page that big
    is a real site, mirroring _fetch_chain. Returns None on any failure of the call to the
    worker itself (quota exhausted, no config, network error, bad worker response).

    Also double-writes an identical, purely-additive "pd_cf_worker" entry alongside
    every "cf_worker" write below (docs/discovery-pipeline-hardening.md Part 4) — for
    phase×origin reporting only. The unlabeled "cf_worker" write itself is never
    renamed or altered; CF_WORKER_DAILY_LIMIT's quota gate (get_day_request_count
    ("cf_worker") above) keeps reading only that one.
    """
    if not CF_WORKER_URL or not CF_WORKER_SECRET:
        return None
    if get_day_request_count("cf_worker") >= CF_WORKER_DAILY_LIMIT:
        log.debug("public_domain: CF Worker daily quota reached, skipping %s", url)
        return None
    _t0 = time.time()
    try:
        resp = requests.post(
            CF_WORKER_URL,
            json={"url": url, "max_bytes": PD_BODY_MAX_BYTES, "max_hops": CF_WORKER_MAX_HOPS},
            headers={"Authorization": f"Bearer {CF_WORKER_SECRET}"},
            timeout=30,
        )
        _ms = int((time.time() - _t0) * 1000)
    except Exception as exc:
        record_external_request("cf_worker", 0, int((time.time() - _t0) * 1000))
        record_external_request("pd_cf_worker", 0, int((time.time() - _t0) * 1000))
        log.debug("public_domain: CF Worker call failed for %s: %s", url, exc)
        return None

    if resp.status_code != 200:
        record_external_request("cf_worker", resp.status_code, _ms)
        record_external_request("pd_cf_worker", resp.status_code, _ms)
        log.debug("public_domain: CF Worker HTTP %s for %s (worker call itself failed)",
                  resp.status_code, url)
        return None

    try:
        data = resp.json()
    except Exception as exc:
        record_external_request("cf_worker", 0, _ms)
        record_external_request("pd_cf_worker", 0, _ms)
        log.debug("public_domain: CF Worker invalid JSON for %s: %s", url, exc)
        return None

    target_status = data.get("status") or 0
    # Sentinel 1 for a target-site error, matching career_detector's convention: it lands
    # in the generic other_err bucket without triggering the "key rejected" 401/403 alert.
    _pd_status = 200 if 200 <= target_status < 300 else 1
    record_external_request("cf_worker", _pd_status, _ms)
    record_external_request("pd_cf_worker", _pd_status, _ms)
    final_url = data.get("final_url") or url
    log.debug("public_domain: CF Worker %s → %s (status=%s)", url, final_url, target_status)

    body = data.get("body") or ""
    raw_headers = data.get("headers")
    headers = ({str(k).lower(): str(v) for k, v in raw_headers.items()}
               if isinstance(raw_headers, dict) else {})
    raw_cookies = data.get("cookies")
    cookies = ({str(c) for c in raw_cookies} if isinstance(raw_cookies, list) else set())
    # New Worker says so explicitly; an old one is inferred from the body hitting the cap.
    truncated = data.get("truncated")
    if truncated is None:
        truncated = len(body.encode("utf-8", "ignore")) >= PD_BODY_MAX_BYTES
    if truncated:
        headers["x-scan-too-large"] = "1"
        body = ""
    return {"status": target_status, "headers": headers, "body": body, "final_url": final_url,
            "cookies": cookies, "error_type": ""}


_REDIRECT_TIMEOUT    = 8
_WEB_TIMEOUT         = 6
_CT_TIMEOUT          = 20
_CRTSH_TIMEOUT       = 30
_CT_MAX_BYTES        = 20 * 1024 * 1024  # 20 MiB — guard against oversized CT responses
_CT_PROBE_BUDGET_S   = 60

# Module-level certspotter backoff — avoid hammering after a 429
_certspotter_retry_after: float = 0.0


def _bounded_json(r, max_bytes: int = _CT_MAX_BYTES):
    """Read a streaming response body up to max_bytes, then JSON-parse.
    Raises ValueError when the body exceeds the limit.
    Always closes the response, including when the size limit is exceeded."""
    chunks = []
    received = 0
    try:
        for chunk in r.iter_content(chunk_size=65536):
            received += len(chunk)
            if received > max_bytes:
                raise ValueError(f"CT response body exceeds {max_bytes} bytes")
            chunks.append(chunk)
        return json.loads(b"".join(chunks))
    finally:
        r.close()


def _root(u: str) -> str:
    if "://" not in u:
        u = "https://" + u
    h = urlparse(u).hostname or ""
    ext = _tldextract(h)
    return ext.registered_domain or h


_REDIRECT_MAX_HOPS   = 8
_REDIRECT_CODES      = frozenset((301, 302, 303, 307, 308))
# Total wall-clock budget per _redirect_domain call across all schemes/hops.
# Worst case without a budget: _REDIRECT_MAX_HOPS × _REDIRECT_TIMEOUT × 2 schemes = 128 s.
_REDIRECT_BUDGET_S   = 20


def _read_body(r) -> "tuple[str, bool]":
    """Bounded read of a streaming response body -> (text, too_large).

    A body larger than PD_BODY_MAX_BYTES is dropped (too_large=True): a page that big is a real
    site, never a parked stub, and the caller marks it with the x-scan-too-large header
    jobs/pd_classify.py treats as "full page". A failed read keeps whatever status/headers we have.
    """
    chunks, received = [], 0
    try:
        for chunk in r.iter_content(chunk_size=8192):
            received += len(chunk)
            if received > PD_BODY_MAX_BYTES:
                return "", True
            chunks.append(chunk)
    except Exception:
        pass
    return b"".join(chunks).decode("utf-8", "ignore"), False


def _cookie_names(r) -> set:
    try:
        return {str(k) for k in r.cookies.keys()}
    except Exception:
        return set()


def _err_kind_of(exc: Exception) -> str:
    name = type(exc).__name__.lower()
    msg = str(exc).lower()
    if "timeout" in name or "timeout" in msg:
        return "timeout"
    if "connect" in name or "connection" in msg:
        return "conn_err"
    return "error"


def _fetch_chain(host: str, scheme: str, sess, deadline: float) -> dict:
    """Follow redirects from scheme://host and read the FINAL response's bounded body.

    Returns the response dict documented in jobs/pd_classify.py (status None + error_type when no
    HTTP response was obtained). Redirects are followed manually so every intermediate hop is
    validated as a publicly-routable address before connecting (prevents SSRF via redirect chain).
    verify=False is applied only after an SSL error on a hop; SSL-unverified hops may only stay on
    the same registrable root.
    """
    res = {"status": None, "headers": {}, "body": "", "final_url": "", "cookies": set(),
           "error_type": "no_response"}
    current = f"{scheme}://{host}"
    cookies: set = set()
    for _ in range(_REDIRECT_MAX_HOPS):
        if time.monotonic() > deadline:
            res["error_type"] = "budget"
            return res
        hop_host = urlparse(current).hostname or ""
        if not hop_host or not _is_public_host(hop_host):
            log.debug("_fetch_chain: non-public host in chain: %s", hop_host)
            res["error_type"] = "non_public_host"
            return res
        verify_off = False
        try:
            r = sess.get(current, allow_redirects=False, timeout=_REDIRECT_TIMEOUT, stream=True)
        except Exception as fetch_exc:
            # requests raises requests.exceptions.SSLError; curl_cffi raises its own SSL-flavored
            # error class — duck-type on the exception name/message rather than importing
            # curl_cffi's error types, since sess may be either kind.
            if not ("ssl" in type(fetch_exc).__name__.lower() or "ssl" in str(fetch_exc).lower()):
                res["error_type"] = _err_kind_of(fetch_exc)
                return res
            try:
                import warnings
                with warnings.catch_warnings():
                    warnings.simplefilter("ignore", _urllib3_no_ssl_warn)
                    r = sess.get(current, allow_redirects=False,
                                 timeout=_REDIRECT_TIMEOUT, verify=False, stream=True)
                verify_off = True
            except Exception:
                log.debug("_fetch_chain: SSL error for %s — no response", current)
                res["error_type"] = "ssl_error"
                return res
        cookies |= _cookie_names(r)
        loc = r.headers.get("Location", "") if r.status_code in _REDIRECT_CODES else ""
        if loc:
            if verify_off:
                next_host = urlparse(urljoin(current, loc)).hostname or ""
                if _root(next_host) != _root(hop_host):
                    log.debug("_fetch_chain: cross-root redirect via verify=False — aborting")
                    r.close()
                    res["error_type"] = "ssl_cross_root"
                    return res
            r.close()
            current = urljoin(current, loc)
            continue
        headers = dict(r.headers)
        body, too_large = _read_body(r)
        if too_large:
            headers["x-scan-too-large"] = "1"
        status = r.status_code
        r.close()
        return {"status": status, "headers": headers, "body": body, "final_url": current,
                "cookies": cookies, "error_type": ""}
    log.debug("_fetch_chain: hop limit reached for %s — rejecting chain", host)
    res["error_type"] = "hop_limit"
    return res


def _www_conflict(host: str, platform: str, sess, deadline: float) -> str:
    """Platform "no site" templates are only soft evidence on one host variant (scan v4: some domains
    show one on the bare host yet serve a real site on www). Check the other variant:
      ""            — it shows the same template, or doesn't exist (DNS fail / refused): template stands
      "www_differs" — it answers with anything else (a real site wins)
    """
    alt = host[4:] if host.startswith("www.") else "www." + host
    last = None
    for scheme in ("https", "http"):
        last = _fetch_chain(alt, scheme, sess, deadline)
        if last["status"] is not None:
            break
    if last["status"] is None:
        return "" if last["error_type"] in ("non_public_host", "conn_err") else "www_differs"
    return "" if platform_no_site(last) == platform else "www_differs"


def _final_host(url: str) -> str:
    return (urlparse(url).hostname or "").lower() if url else ""


def _probe_host(host: str, session=None, relay_mode: bool = False) -> dict:
    """Fetch host (https then http, redirects followed), classify the final response, and return:

      confirmed   bool — host answered as a real company site and `root` is its public domain
      root        registrable domain of the final URL ("" when nothing confirmed)
      final_host  exact host that answered (only meaningful when confirmed)
      status      last HTTP status seen (Worker's read replaces it when the Worker ran), or None
      verdict/reason  jobs/pd_classify verdict (ok | parked | challenge | blocked | inconclusive | error)
      cross_domain, resolved_by (oci | worker | relay), final_url

    Confirmation policy (user-locked 2026-10-04): only a 2xx confirms; a challenge page also confirms
    (a live bot-protected site — the origin is the real domain, as before). A 403 never decides on its
    own: direct -> CF Worker (skipped in relay_mode) -> and only when the home relay (relay_mode=True)
    ALSO sees 403 is it kept as the public domain. 429/503 and every other non-2xx store nothing
    (retried later). Parked / junk-landing / platform-template results store nothing.

    Each probe is also written to pd_probe_observation (evidence log, never read by the pipeline).
    """
    sess = session if session is not None else _get_default_curl_session()
    deadline = time.monotonic() + _REDIRECT_BUDGET_S
    host_root = _root(host)
    res = {"status": None, "headers": {}, "body": "", "final_url": "", "cookies": set(),
           "error_type": "no_response"}
    scheme_used = ""
    for scheme in ("https", "http"):
        t0 = time.time()
        res = _fetch_chain(host, scheme, sess, deadline)
        scheme_used = scheme
        # One "pd_oci" external_api_health entry per scheme attempt (Part 4) — status 0 = no HTTP response.
        record_external_request("pd_oci", res["status"] or 0, int((time.time() - t0) * 1000),
                                error_kind=(res["error_type"] if res["status"] is None
                                            and res["error_type"] in ("timeout", "conn_err") else None))
        if res["status"] is not None or res["error_type"] == "budget":
            break

    final_url = res["final_url"]
    final_root = _root(final_url) if final_url else ""
    platform = platform_no_site(res)
    if platform:
        res["www_conflict"] = _www_conflict(host, platform, sess, deadline)
    verdict, reason = classify(res, final_root, _CHALLENGE_DOMAINS)
    verdict, reason, cross = apply_outcome_rules(verdict, reason, host_root, final_root)

    resolved_by = "relay" if relay_mode else "oci"
    status = res["status"]
    worker_obs: dict = {}
    # CF-Worker tier: only on a 403 / retry-later 429/503 from the direct tier, never in relay mode
    # (the Worker already had its turn in the first pass; the relay is the last word).
    if not relay_mode and (verdict == "blocked" or reason.startswith("retry_later:")):
        worker_result = _fetch_via_worker(final_url)
        if worker_result:
            w_status, w_final_url = worker_result["status"], worker_result["final_url"]
            worker_obs = {"worker_status": w_status}
            if 200 <= w_status < 300:
                # A Worker 2xx is classified like a direct one (body/title/path/cookie signatures) —
                # it no longer confirms blindly: parked/template -> nothing stored, challenge/ok -> confirmed.
                w_root = _root(w_final_url)
                w_platform = platform_no_site(worker_result)
                if w_platform:
                    worker_result["www_conflict"] = _www_conflict(host, w_platform, sess, deadline)
                w_verdict, w_reason = classify(worker_result, w_root, _CHALLENGE_DOMAINS)
                w_verdict, w_reason, w_cross = apply_outcome_rules(w_verdict, w_reason, host_root, w_root)
                worker_obs.update({"worker_verdict": w_verdict, "worker_res": worker_result})
                log.info("_probe_host: CF Worker read %s via %s (status=%s) -> %s/%s",
                         host, w_final_url, w_status, w_verdict, w_reason)
                final_url, final_root, resolved_by = w_final_url, w_root, "worker"
                verdict, reason, cross = w_verdict, w_reason, w_cross
                status = w_status
            else:
                # Non-2xx Worker answer: the decision is unchanged, but its verdict + body fingerprint
                # are still evidence (e.g. a WAF block page seen from Cloudflare too).
                worker_obs.update({"worker_verdict": classify(worker_result, _root(w_final_url),
                                                              _CHALLENGE_DOMAINS)[0],
                                   "worker_res": worker_result})
                status = w_status or status

    confirmed, root, out_host = False, "", ""
    if verdict == "ok":
        confirmed, root, out_host = True, final_root, _final_host(final_url)
    elif verdict == "challenge":
        # Live, bot-protected site: the origin domain is real (a vendor-domain landing says nothing
        # about where the company's site is, so the origin host itself is the answer).
        confirmed, root = True, host_root
        out_host = _final_host(final_url) if final_root == host_root else host
    elif verdict == "blocked" and relay_mode:
        # 403 from the home relay too — user decision: keep it as the public domain.
        confirmed, root, out_host = True, final_root, _final_host(final_url)
        reason = f"{reason}:relay_confirmed"

    result = {"confirmed": confirmed, "root": root, "final_host": out_host, "status": status,
              "verdict": verdict, "reason": reason, "cross_domain": cross,
              "resolved_by": resolved_by, "final_url": final_url}
    if PD_PROBE_RECORD_ENABLED:
        _record_probe_result(host, res, result, scheme_used, relay_mode, worker_obs)
    return result


def _record_probe_result(host: str, res: dict, result: dict, scheme: str, relay_mode: bool,
                         worker_obs: dict) -> None:
    """Best-effort write to pd_probe_observation — never raises, never gates resolution.

    Each tier owns its own column group and writes ONLY that group (db/pd_probe.py skips absent keys):
      OCI    status..error_type + body_hash/title/cookies — the VM's direct fetch (non-relay probes)
      worker worker_*  — the CF Worker's response (non-relay probes; set to NULL when the Worker did not
                         run, so a stale answer from an earlier probe never sits beside fresh OCI data)
      relay  relay_*   — the home relay's response (relay-mode probes only; OCI/worker columns untouched)
    """
    try:
        obs = {
            "final_verdict": result["verdict"], "final_reason": result["reason"],
            "resolved_by": result["resolved_by"], "final_host": result["final_host"] or _final_host(result["final_url"]),
            "cross_domain": result["cross_domain"],
        }
        d = describe(res, PD_SNIPPET_CHARS)
        if relay_mode:
            obs.update({"relay_status": res.get("status"), "relay_verdict": result["verdict"],
                        "relay_body_len": d["body_len"], "relay_title": d["title"],
                        "relay_body_hash": d["body_hash"],
                        "relay_cookie_names": ",".join(sorted(res.get("cookies") or ()))})
        else:
            lh = {str(k).lower(): v for k, v in (res.get("headers") or {}).items()}
            obs.update({
                "status": res.get("status"), "final_url": res.get("final_url"), "server": lh.get("server"),
                "fetch_via": scheme, "body_len": d["body_len"], "ext_refs": d["ext_refs"],
                "body_hash": d["body_hash"], "title": d["title"], "snippet": d["snippet"],
                "cookie_names": ",".join(sorted(res.get("cookies") or ())),
                "header_names": ",".join(sorted(lh)), "error_type": res.get("error_type") or None,
                "impersonate": getattr(_get_default_curl_session(), "impersonate", None),
                "worker_status": worker_obs.get("worker_status"),
                "worker_verdict": worker_obs.get("worker_verdict"),
                "worker_body_len": None, "worker_title": None,
                "worker_body_hash": None, "worker_cookie_names": None,
            })
            wres = worker_obs.get("worker_res")
            if wres is not None:
                wd = describe(wres, PD_SNIPPET_CHARS)
                obs.update({"worker_body_len": wd["body_len"], "worker_title": wd["title"],
                            "worker_body_hash": wd["body_hash"],
                            "worker_cookie_names": ",".join(sorted(wres.get("cookies") or ()))})
        record_probe(host, obs)
    except Exception as exc:
        log.debug("pd_probe: could not build observation for %s: %s", host, exc)


def _redirect_domain(host: str, session=None, relay_mode: bool = False) -> "tuple[str | None, int | None]":
    """Legacy-shaped view of _probe_host, used by scripts/pd_gate_sample_recheck.py. Returns (domain, last_status):
      ("", None)       — confirmed, same root as host
      (str, None)       — confirmed, different root (redirect found)
      (None, status)    — inconclusive non-2xx (status seen); caller must cascade
      (None, None)      — no response / parked / junk landing / platform template — nothing to store
    """
    return _legacy_view(_probe_host(host, session=session, relay_mode=relay_mode), host)


def _legacy_view(r: dict, host: str) -> "tuple[str | None, int | None]":
    if r["confirmed"]:
        return (r["root"] if r["root"] != _root(host) else ""), None
    if r["verdict"] in ("blocked", "inconclusive") and not r["reason"].startswith(
            ("junk_landing", PLATFORM_REASON_PREFIX)):
        return None, r["status"]
    return None, None


def _pd_host(public_domain: str, r: dict) -> "str | None":
    """Host to store next to public_domain: the exact host that answered, only when its registrable
    root matches public_domain (invariant — a mismatched pair is never stored)."""
    h = r.get("final_host") or ""
    return h if h and _root(h) == _root(public_domain) else None


def _probe_candidate(root: str, session=None, relay_mode: bool = False) -> "dict | None":
    """Confirm a CT-log candidate domain with the same classifier as every other probe.

    Returns the _probe_host result when the candidate is a confirmed real site that stays on its own
    registrable domain, else None. Stricter than the old status<400 check: a parked / 3xx-only /
    non-2xx candidate is rejected, and a candidate that redirects to another domain is skipped
    rather than silently swapping in a domain the CT lookup never proposed.
    """
    if root in _CHALLENGE_DOMAINS or not _is_public_host(root):
        return None
    r = _probe_host(root, session=session, relay_mode=relay_mode)
    if r["confirmed"] and r["root"] == _root(root):
        return r
    return None


def _ct_certspotter(domain: str) -> "tuple[list[str], int | None]":
    """
    Query SSLmate certspotter for all certs issued under domain.
    Extracts and ranks root domains found across all certificate SANs.

    Returns (candidates, retry_after_seconds_or_None).
    retry_after is non-None on HTTP 429 — caller re-queues with that delay.
    """
    global _certspotter_retry_after

    if time.time() < _certspotter_retry_after:
        wait = max(1, int(_certspotter_retry_after - time.time()))
        log.debug("certspotter in-process backoff: %ds remaining", wait)
        return [], wait

    headers = {"User-Agent": "python-h1b-discovery/1.0"}
    if CERTSPOTTER_API_KEY:
        headers["Authorization"] = f"Bearer {CERTSPOTTER_API_KEY}"

    _t0 = time.time()
    try:
        r = requests.get(
            "https://api.certspotter.com/v1/issuances",
            params={"domain": domain, "include_subdomains": "true", "expand": "dns_names"},
            headers=headers,
            timeout=_CT_TIMEOUT,
            stream=True,
        )
        response_ms = int((time.time() - _t0) * 1000)
        if r.status_code == 429:
            _ra = r.headers.get("Retry-After", "3600")
            try:
                retry_after = int(_ra)
            except (ValueError, TypeError):
                retry_after = 3600  # HTTP-date or unparseable — safe fallback
            retry_after = max(1, min(retry_after, 3600))
            _certspotter_retry_after = time.time() + retry_after
            log.warning("certspotter 429 for %s — retry after %ds", domain, retry_after)
            record_external_request("certspotter", 429, response_ms, backoff_s=retry_after)
            r.close()
            return [], retry_after

        if r.status_code != 200:
            log.warning("certspotter HTTP %d for %s", r.status_code, domain)
            record_external_request("certspotter", r.status_code, response_ms)
            r.close()
            return [], None

        certs = _bounded_json(r)
        roots: dict[str, int] = {}
        for cert in certs:
            for d in cert.get("dns_names", []):
                d = d.removeprefix("*.")
                if d == domain:
                    continue
                root = _root(d)
                if root and root not in GENERIC_ROOTS:
                    roots[root] = roots.get(root, 0) + 1

        top = sorted(roots, key=lambda x: (-roots[x], x))[:10]
        log.debug("certspotter: %d certs for %s → top roots: %s", len(certs), domain, top)
        record_external_request("certspotter", 200, response_ms)
        return top, None

    except Exception as e:
        log.warning("certspotter error for %s: %s", domain, e)
        record_external_request("certspotter", 0, int((time.time() - _t0) * 1000))
        return [], None


def _ct_crtsh(domain: str) -> list[str]:
    """crt.sh fallback — slower, sometimes unavailable."""
    _t0 = time.time()
    try:
        r = requests.get(
            "https://crt.sh/",
            params={"q": domain, "output": "json"},
            timeout=_CRTSH_TIMEOUT,
            headers={"User-Agent": "python-h1b-discovery/1.0"},
            stream=True,
        )
        response_ms = int((time.time() - _t0) * 1000)
        if r.status_code != 200:
            log.warning("crt.sh HTTP %d for %s", r.status_code, domain)
            record_external_request("crtsh", r.status_code, response_ms)
            r.close()
            return []

        roots: dict[str, int] = {}
        for cert in _bounded_json(r):
            for d in cert.get("name_value", "").replace("\n", ",").split(","):
                d = d.strip().removeprefix("*.")
                if not d or d == domain:
                    continue
                root = _root(d)
                if root and root not in GENERIC_ROOTS:
                    roots[root] = roots.get(root, 0) + 1

        top = sorted(roots, key=lambda x: (-roots[x], x))[:10]
        log.debug("crt.sh: top roots for %s: %s", domain, top)
        record_external_request("crtsh", 200, response_ms)
        return top

    except Exception as e:
        log.warning("crt.sh error for %s: %s", domain, e)
        record_external_request("crtsh", 0, int((time.time() - _t0) * 1000))
        return []


def _ct_domains(domain: str) -> "tuple[list[str], int | None, str]":
    """certspotter primary, crt.sh fallback. Returns (candidates, retry_after_or_None, source)."""
    candidates, retry_after = _ct_certspotter(domain)
    if candidates:
        return candidates, None, "certspotter"
    if retry_after is not None:
        # certspotter in backoff — still try crt.sh before propagating quota error
        fallback = _ct_crtsh(domain)
        if fallback:
            return fallback, None, "crtsh"
        return [], retry_after, "certspotter"
    candidates = _ct_crtsh(domain)
    return candidates, None, "crtsh"


def discover_public_domain(assigned_domain: str, session=None, relay_mode: bool = False,
                           ) -> "tuple[str | None, str, int | None, int | None, str | None]":
    """
    Resolve an internal/email domain to the company's real public domain.

    Returns (public_domain, method, retry_after, last_status, host):
      public_domain — resolved registrable domain, or None if unresolvable
      method        — 'http_redirect' | 'root_fallback' | 'certspotter' |
                      'crtsh' | 'same_domain' | 'ct_quota' | 'no_signal'
      retry_after   — seconds before re-queuing (certspotter 429), else None
      last_status   — HTTP status seen on the last INCONCLUSIVE attempt, else None
                      (persisted into fein_domain_map.public_domain_last_status)
      host          — exact host that answered (e.g. www.example.com) when its root
                      equals public_domain, else None (persisted into
                      fein_domain_map.public_domain_host; used only to FETCH the site)

    relay_mode — True only from the home-relay drain worker: a 403 there is the final
    escalation tier and is accepted as the public domain (see _probe_host).
    session — optional explicit session (mobile relay egress); identical logic.
    """
    domain = (assigned_domain or "").lower().strip()
    if not domain:
        return None, "no_signal", None, None, None

    if _is_private_ip_literal(domain):
        log.warning("public_domain: rejecting private address %s", domain)
        return None, "no_signal", None, None, None

    last_status: "int | None" = None

    # Step 1 — probe the full domain (bare + www)
    r = _probe_host(domain, session=session, relay_mode=relay_mode)
    redir, status = _legacy_view(r, domain)
    if status is not None:
        last_status = status
    if redir is None:
        log.debug("DNS fail or inconclusive status for %s — trying root fallback", domain)
    elif redir == "":
        # "Already public" only for root domains and www-prefixed subdomains; a deeper
        # subdomain (ny.email.gs.com) falls through to the CT log.
        sub = _tldextract(domain).subdomain
        if not sub or sub == "www":
            log.debug("%s already resolves publicly", domain)
            return domain, "same_domain", None, None, _pd_host(domain, r)
        log.debug("%s resolves within its root but has subdomain — continuing", domain)
    elif redir not in GENERIC_ROOTS:
        log.info("public_domain: %s → %s (http_redirect)", domain, redir)
        return redir, "http_redirect", None, None, _pd_host(redir, r)
    else:
        log.debug("public_domain: %s → %s (generic root — skipping)", domain, redir)

    # Step 2 — registered-domain fallback
    root_try = _tldextract(domain).registered_domain
    if root_try and root_try != domain:
        r2 = _probe_host(root_try, session=session, relay_mode=relay_mode)
        redir, status = _legacy_view(r2, root_try)
        if status is not None:
            last_status = status
        if redir is None:
            pass
        elif redir == "":
            if root_try not in GENERIC_ROOTS:
                log.info("public_domain: %s → %s (root_fallback)", domain, root_try)
                return root_try, "root_fallback", None, None, _pd_host(root_try, r2)
        elif redir not in GENERIC_ROOTS:
            log.info("public_domain: %s → %s (root_fallback)", domain, redir)
            return redir, "root_fallback", None, None, _pd_host(redir, r2)
        else:
            log.debug("public_domain: %s → %s (generic root — skipping)", domain, redir)

    # Step 3 — CT log (certspotter → crt.sh fallback)
    log.debug("querying CT logs for %s", domain)
    candidates, retry_after, ct_source = _ct_domains(domain)

    if retry_after is not None:
        return None, "ct_quota", retry_after, None, None

    _ct_budget_start = time.time()
    for candidate in candidates[:10]:
        if time.time() - _ct_budget_start > _CT_PROBE_BUDGET_S:
            log.debug("CT probe budget exhausted for %s — stopping early", domain)
            break
        rc = _probe_candidate(candidate, session=session, relay_mode=relay_mode)
        if rc:
            log.info("public_domain: %s → %s (%s)", domain, candidate, ct_source)
            return candidate, ct_source, None, None, _pd_host(candidate, rc)

    log.debug("no public domain signal for %s", domain)
    return None, "no_signal", None, last_status, None
