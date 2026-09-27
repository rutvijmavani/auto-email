"""
jobs/public_domain.py — Public domain resolution for H1B pipeline enrichment.

Resolves internal/email domains (e.g. fmr.com, jpmchase.com) to the company's
real public-facing website domain (e.g. fidelity.com, jpmorgan.com).

Three-step algorithm:
  1. HTTP redirect follow    — jpmchase.com → jpmorgan.com
  2. Root-domain fallback   — ny.email.gs.com → gs.com → goldmansachs.com
  3. CT log (certspotter)   — fmr.com → fidelity.com via cert SANs
     Fallback: crt.sh if certspotter unavailable.

Returns (public_domain, method, retry_after, last_status) where retry_after is non-None
only on certspotter 429 — caller should re-queue the company with that delay. Fetches use
a Chrome-impersonated curl_cffi session by default (docs/discovery-pipeline-hardening.md
Part 2); pass session= to inject a different one (e.g. Part 3's mobile relay worker).
"""

import ipaddress
import json
import socket
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

from config import CERTSPOTTER_API_KEY
from logger import get_logger
from db.external_api_health import record_external_request, get_day_request_count

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

# Response headers that identify a bot-protection challenge page served from the company's
# own domain (e.g. an Imperva inline challenge stays on company.com but sets x-iinfo).
# Cloudflare is NOT included here: cf-ray is present on every Cloudflare-proxied response
# (challenge or not), so its mere presence isn't a challenge signal — see cf-mitigated below.
_CHALLENGE_HEADERS = frozenset({
    "x-iinfo",             # Imperva (inline mode — served from company domain)
    "x-sucuri-id",         # Sucuri (inline mode)
    "x-px-access-denied",  # PerimeterX
})


_safe_session = _make_safe_session()

# Chrome-impersonated fetching (docs/discovery-pipeline-hardening.md Part 2) —
# this is the DEFAULT session for the redirect/web-liveness probes below (_redirect_domain,
# _has_web). CT-log queries (_ct_certspotter, _ct_crtsh) keep using plain `requests` above —
# those hit certspotter/crt.sh directly, not the company's own WAF-fronted site, so Chrome
# impersonation buys nothing there.
#
# session= params on _redirect_domain/_has_web/discover_public_domain let Part 3's mobile
# relay worker (not yet built) pass an identical function call with a SOCKS5-proxied
# session instead of this module default — same code path, different egress IP.
_default_curl_session = _make_safe_curl_session()


def _fetch_via_worker(url: str) -> "tuple[str, int] | None":
    """Proxy a single GET through the Cloudflare probe Worker — fallback tier used when
    the direct curl_cffi attempt to `url` comes back inconclusive (403/429/503).

    Shares the same daily cap and atomic external_api_health tracking (service="cf_worker")
    as jobs/ats/career_detector.py::_fetch_via_worker and
    scripts/discover_h1b_ats.py::_fetch_via_worker — all three count against one shared
    daily total. The shared limit is NOT raised for this addition (locked 2026-09-27).

    Returns (final_url, status) — status is the WORKER's read of the target site, which
    may itself still be non-2xx. Returns None on any failure of the call to the worker
    itself (quota exhausted, no config, network error, bad worker response).

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
            json={"url": url, "max_bytes": 4096},
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
    return final_url, target_status


def _is_challenge_response(headers: dict) -> bool:
    """Return True if response headers indicate a bot-protection challenge page."""
    lower_headers = {k.lower(): v for k, v in headers.items()}
    if _CHALLENGE_HEADERS & lower_headers.keys():
        return True
    # Cloudflare only sets cf-mitigated: challenge when it actually served a challenge —
    # cf-ray alone just means the response passed through Cloudflare's proxy.
    return (lower_headers.get("cf-mitigated", "") or "").strip().lower() == "challenge"

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


def _redirect_domain(host: str, session=None) -> "tuple[str | None, int | None]":
    """
    Follow HTTP redirects on host. Returns (domain, last_status):
      ("", None)        — final response is 2xx and same root as host → confirmed public
      (str, None)        — final response is 2xx and root differs from host → redirect found
      (None, status)     — final response is not 2xx (status is the code seen) → inconclusive,
                            never a confirmation and never a rejection; caller must cascade
      (None, None)        — connection error / DNS fail / redirect chain leads to a private
                            host / no HTTP response obtained at all

    Only an actual 2xx response, after following redirects to their final hop, confirms
    a domain (docs/discovery-pipeline-hardening.md Part 1) — a non-2xx final response
    (e.g. a 403 from an IP-reputation/WAF block) must never be treated as "already public"
    or as a genuine redirect target; it cascades to the next resolution step instead.

    Redirects are followed manually so every intermediate hop is validated as a
    publicly-routable address before connecting (prevents SSRF via redirect chain).
    verify=False is applied only when the initial HTTPS attempt raises an SSL error —
    company domains frequently have self-signed or expired certs; we only use the
    final URL's domain name, never the response body.

    session — Chrome-impersonated curl_cffi session by default (docs/discovery-pipeline-
    hardening.md Part 2); pass an explicit session (e.g. Part 3's SOCKS5-proxied relay
    session) to fetch through a different egress path with identical logic.

    Records one "pd_oci" external_api_health entry per scheme attempt (Part 4) — the
    CF-Worker fallback tier above keeps recording its own separate "pd_cf_worker" entry
    inside _fetch_via_worker(), unchanged.
    """
    sess = session if session is not None else _default_curl_session
    _budget_deadline = time.monotonic() + _REDIRECT_BUDGET_S
    for scheme in ("https", "http"):
        current = f"{scheme}://{host}"
        final_status: "int | None" = None
        _scheme_t0 = time.time()
        try:
            _last_headers: dict = {}
            for _ in range(_REDIRECT_MAX_HOPS):
                if time.monotonic() > _budget_deadline:
                    log.debug("_redirect_domain: budget exceeded for %s — aborting", host)
                    return None, None
                hop_host = urlparse(current).hostname or ""
                if not hop_host or not _is_public_host(hop_host):
                    log.debug("_redirect_domain: non-public host in chain: %s", hop_host)
                    current = None
                    break
                try:
                    r = sess.get(current, allow_redirects=False,
                                 timeout=_REDIRECT_TIMEOUT, stream=True)
                except Exception as _fetch_exc:
                    # requests raises requests.exceptions.SSLError; curl_cffi raises its own
                    # SSL-flavored error class — duck-type on the exception name/message
                    # rather than importing curl_cffi's error types, since sess may be
                    # either kind (plain make_safe_session() fallback or curl_cffi).
                    _is_ssl_err = ("ssl" in type(_fetch_exc).__name__.lower()
                                   or "ssl" in str(_fetch_exc).lower())
                    if not _is_ssl_err:
                        raise
                    try:
                        import warnings
                        with warnings.catch_warnings():
                            warnings.simplefilter("ignore", _urllib3_no_ssl_warn)
                            r = sess.get(current, allow_redirects=False,
                                         timeout=_REDIRECT_TIMEOUT, verify=False, stream=True)
                    except Exception:
                        log.debug("_redirect_domain: SSL error for %s — no redirect signal", current)
                        current = None
                        break
                    # SSL-unverified: cross-root redirects are untrusted — only same-root hops allowed.
                    if r.status_code in _REDIRECT_CODES:
                        _loc = r.headers.get("Location", "")
                        if _loc:
                            _next_host = urlparse(urljoin(current, _loc)).hostname or ""
                            if _root(_next_host) != _root(urlparse(current).hostname or ""):
                                log.debug("_redirect_domain: cross-root redirect via verify=False — aborting")
                                _last_headers = dict(r.headers)
                                r.close()
                                current = None
                                break
                _last_headers = dict(r.headers)
                final_status = r.status_code
                r.close()
                if r.status_code not in _REDIRECT_CODES:
                    break  # current is the final URL
                loc = r.headers.get("Location", "")
                if not loc:
                    break
                current = urljoin(current, loc)
            else:
                # All hops consumed while still in redirect chain — last Location is unverified.
                log.debug("_redirect_domain: hop limit reached for %s — rejecting chain", host)
                current = None

            if current is None:
                # Records one "pd_oci" entry per scheme attempt (docs/discovery-pipeline-
                # hardening.md Part 4) — status 0 when no HTTP response was ever obtained
                # for this scheme (non-public hop, unrecoverable SSL error, hop limit).
                record_external_request("pd_oci", final_status or 0, int((time.time() - _scheme_t0) * 1000))
                continue  # try next scheme
            final = _root(current)
            # If the chain landed on a bot-protection vendor domain, the origin is the real domain.
            if final in _CHALLENGE_DOMAINS or _is_challenge_response(_last_headers):
                log.debug("_redirect_domain: challenge page detected (%s) — origin %s is real domain",
                          final, host)
                record_external_request("pd_oci", final_status or 0, int((time.time() - _scheme_t0) * 1000))
                return "", None
            # Only an actual 2xx confirms — any other final status (403/404/5xx/etc.) is
            # inconclusive, never a confirmation and never a rejection (Part 1 gate fix).
            if final_status is None or not (200 <= final_status < 300):
                # CF-Worker fallback tier (Part 2) — a block-like status might just mean
                # the OCI/local egress IP is blocked, not that the domain is genuinely
                # unreachable. One extra read through the Worker's IP is near-free against
                # the shared daily quota; only tried on the block-like codes, not every
                # inconclusive status (e.g. never on a 404, which is a real answer).
                if final_status in (403, 429, 503):
                    worker_result = _fetch_via_worker(current)
                    if worker_result:
                        w_final_url, w_status = worker_result
                        if 200 <= w_status < 300:
                            w_root = _root(w_final_url)
                            log.info("_redirect_domain: CF Worker confirmed %s via %s (status=%s)",
                                     host, w_final_url, w_status)
                            return (w_root if w_root != _root(host) else ""), None
                        final_status = w_status or final_status
                log.debug("_redirect_domain: final status for %s is %s (not 2xx) — inconclusive",
                          host, final_status)
                record_external_request("pd_oci", final_status or 0, int((time.time() - _scheme_t0) * 1000))
                return None, final_status
            record_external_request("pd_oci", final_status or 0, int((time.time() - _scheme_t0) * 1000))
            return (final if final != _root(host) else ""), None
        except Exception as _exc:
            _err_name = type(_exc).__name__.lower()
            _err_kind = "timeout" if "timeout" in _err_name or "timeout" in str(_exc).lower() else (
                "conn_err" if "connect" in _err_name or "connection" in str(_exc).lower() else None
            )
            record_external_request("pd_oci", 0, int((time.time() - _scheme_t0) * 1000), error_kind=_err_kind)
            log.debug("_redirect_domain: scheme probe failed for %r (%s): %s", host, scheme, _exc)
            continue
    return None, None


def _has_web(root: str, session=None) -> bool:
    """Return True if root domain serves a non-error HTTP response (status < 400).

    Validates that root resolves only to public addresses before connecting.
    Redirects are not followed — a 3xx response (< 400) still means the domain
    is live and serving HTTP, which is all the caller cares about. A 4xx/5xx no
    longer counts (Part 1 gate fix) — a 403 from an IP-reputation/WAF block is
    not evidence the candidate domain is the company's real site.

    session — Chrome-impersonated curl_cffi session by default (Part 2); see
    _redirect_domain's docstring for the session-injection rationale.

    Records one "pd_oci" external_api_health entry per scheme attempt (Part 4) —
    same convention as _redirect_domain above.
    """
    if not _is_public_host(root):
        return False
    if root in _CHALLENGE_DOMAINS:
        return False
    sess = session if session is not None else _default_curl_session
    for scheme in ("https", "http"):
        url = f"{scheme}://{root}"
        _t0 = time.time()
        try:
            r = sess.get(url, timeout=_WEB_TIMEOUT, allow_redirects=False, stream=True)
            status = r.status_code
            r.close()
            record_external_request("pd_oci", status, int((time.time() - _t0) * 1000))
            if status < 400:
                return True
        except Exception as _exc:
            _err_name = type(_exc).__name__.lower()
            _err_kind = "timeout" if "timeout" in _err_name or "timeout" in str(_exc).lower() else (
                "conn_err" if "connect" in _err_name or "connection" in str(_exc).lower() else None
            )
            record_external_request("pd_oci", 0, int((time.time() - _t0) * 1000), error_kind=_err_kind)
    return False


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


def discover_public_domain(assigned_domain: str, session=None) -> "tuple[str | None, str, int | None, int | None]":
    """
    Resolve an internal/email domain to the company's real public domain.

    Returns (public_domain, method, retry_after, last_status):
      public_domain — resolved domain string, or None if unresolvable
      method        — 'http_redirect' | 'root_fallback' | 'certspotter' |
                      'crtsh' | 'same_domain' | 'ct_quota' | 'no_signal'
      retry_after   — seconds before re-queuing (certspotter 429), else None
      last_status   — numeric HTTP status seen on the last INCONCLUSIVE confirmation
                      attempt (Part 1 gate fix), or None on a clean 2xx / non-HTTP
                      failure / successful resolution. Caller persists this into
                      fein_domain_map.public_domain_last_status.

    session — Chrome-impersonated curl_cffi session by default (docs/discovery-pipeline-
    hardening.md Part 2). Pass an explicit session (Part 3's mobile relay worker) to
    resolve through a different egress path with identical logic — every internal
    _redirect_domain/_has_web call below is threaded with this same session.
    """
    domain = (assigned_domain or "").lower().strip()
    if not domain:
        return None, "no_signal", None, None

    if _is_private_ip_literal(domain):
        log.warning("public_domain: rejecting private address %s", domain)
        return None, "no_signal", None, None

    last_status: "int | None" = None

    # Step 1 — HTTP redirect on full domain
    redir, status = _redirect_domain(domain, session=session)
    if status is not None:
        last_status = status
    if redir is None:
        log.debug("DNS fail or inconclusive status for %s — trying root fallback", domain)
    elif redir == "":
        # Accept "already public" for root domains and www-prefixed subdomains.
        # A subdomain like ny.email.gs.com resolves within the same root (gs.com),
        # but the real public site may be at goldmansachs.com — fall through to CT log.
        # www is a standard public alias, not a meaningful subdomain.
        sub = _tldextract(domain).subdomain
        if not sub or sub == "www":
            log.debug("%s already resolves publicly", domain)
            return domain, "same_domain", None, None
        log.debug("%s resolves within its root but has subdomain — continuing", domain)
    elif redir not in GENERIC_ROOTS:
        log.info("public_domain: %s → %s (http_redirect)", domain, redir)
        return redir, "http_redirect", None, None
    else:
        log.debug("public_domain: %s → %s (generic root — skipping)", domain, redir)

    # Step 2 — Root-domain fallback (strip subdomain prefix via PSL)
    ext      = _tldextract(domain)
    root_try = ext.registered_domain
    if root_try and root_try != domain:
        redir, status = _redirect_domain(root_try, session=session)
        if status is not None:
            last_status = status
        if redir is None:
            pass
        elif redir == "":
            if root_try not in GENERIC_ROOTS:
                log.info("public_domain: %s → %s (root_fallback)", domain, root_try)
                return root_try, "root_fallback", None, None
        elif redir not in GENERIC_ROOTS:
            log.info("public_domain: %s → %s (root_fallback)", domain, redir)
            return redir, "root_fallback", None, None
        else:
            log.debug("public_domain: %s → %s (generic root — skipping)", domain, redir)

    # Step 3 — CT log (certspotter → crt.sh fallback)
    log.debug("querying CT logs for %s", domain)
    candidates, retry_after, ct_source = _ct_domains(domain)

    if retry_after is not None:
        return None, "ct_quota", retry_after, None

    _ct_budget_start = time.time()
    for candidate in candidates[:10]:
        if time.time() - _ct_budget_start > _CT_PROBE_BUDGET_S:
            log.debug("CT probe budget exhausted for %s — stopping early", domain)
            break
        if _has_web(candidate, session=session):
            log.info("public_domain: %s → %s (%s)", domain, candidate, ct_source)
            return candidate, ct_source, None, None

    log.debug("no public domain signal for %s", domain)
    return None, "no_signal", None, last_status



