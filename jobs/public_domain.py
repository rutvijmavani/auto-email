"""
jobs/public_domain.py — Public domain resolution for H1B pipeline enrichment.

Resolves internal/email domains (e.g. fmr.com, jpmchase.com) to the company's
real public-facing website domain (e.g. fidelity.com, jpmorgan.com).

Three-step algorithm:
  1. HTTP redirect follow    — jpmchase.com → jpmorgan.com
  2. Root-domain fallback   — ny.email.gs.com → gs.com → goldmansachs.com
  3. CT log (certspotter)   — fmr.com → fidelity.com via cert SANs
     Fallback: crt.sh if certspotter unavailable.

Returns (public_domain, method, retry_after) where retry_after is non-None
only on certspotter 429 — caller should re-queue the company with that delay.
"""

import ipaddress
import json
import socket
import time
from urllib.parse import urljoin, urlparse

import requests
import tldextract
_tldextract = tldextract.TLDExtract(suffix_list_urls=())
import urllib3

_urllib3_no_ssl_warn = urllib3.exceptions.InsecureRequestWarning

from config import CERTSPOTTER_API_KEY
from logger import get_logger

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
# own domain (e.g. Cloudflare JS challenge stays on company.com but sets cf-ray).
_CHALLENGE_HEADERS = frozenset({
    "cf-ray",              # Cloudflare
    "x-iinfo",             # Imperva (inline mode — served from company domain)
    "x-sucuri-id",         # Sucuri (inline mode)
    "x-px-access-denied",  # PerimeterX
})


def _is_challenge_response(headers: dict) -> bool:
    """Return True if response headers indicate a bot-protection challenge page."""
    lower_keys = {k.lower() for k in headers}
    return bool(_CHALLENGE_HEADERS & lower_keys)

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
    ext = _tldextract.extract(h)
    return ext.registered_domain or h


_REDIRECT_MAX_HOPS   = 8
_REDIRECT_CODES      = frozenset((301, 302, 303, 307, 308))
# Total wall-clock budget per _redirect_domain call across all schemes/hops.
# Worst case without a budget: _REDIRECT_MAX_HOPS × _REDIRECT_TIMEOUT × 2 schemes = 128 s.
_REDIRECT_BUDGET_S   = 20


def _redirect_domain(host: str) -> "str | None":
    """
    Follow HTTP redirects on host. Returns:
      str  — root domain of final URL differs from host → redirect found
      ""   — final URL has same root as host → already public
      None — connection error / DNS fail / redirect chain leads to a private host

    Redirects are followed manually so every intermediate hop is validated as a
    publicly-routable address before connecting (prevents SSRF via redirect chain).
    verify=False is applied only when the initial HTTPS attempt raises SSLError —
    company domains frequently have self-signed or expired certs; we only use the
    final URL's domain name, never the response body.
    """
    _budget_deadline = time.monotonic() + _REDIRECT_BUDGET_S
    for scheme in ("https", "http"):
        current = f"{scheme}://{host}"
        try:
            _last_headers: dict = {}
            for _ in range(_REDIRECT_MAX_HOPS):
                if time.monotonic() > _budget_deadline:
                    log.debug("_redirect_domain: budget exceeded for %s — aborting", host)
                    return None
                hop_host = urlparse(current).hostname or ""
                if not hop_host or not _is_public_host(hop_host):
                    log.debug("_redirect_domain: non-public host in chain: %s", hop_host)
                    current = None
                    break
                try:
                    r = requests.get(current, allow_redirects=False,
                                     timeout=_REDIRECT_TIMEOUT, stream=True)
                except requests.exceptions.SSLError:
                    try:
                        import warnings
                        with warnings.catch_warnings():
                            warnings.simplefilter("ignore", _urllib3_no_ssl_warn)
                            r = requests.get(current, allow_redirects=False,
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
                continue  # try next scheme
            final = _root(current)
            # If the chain landed on a bot-protection vendor domain, the origin is the real domain.
            if final in _CHALLENGE_DOMAINS or _is_challenge_response(_last_headers):
                log.debug("_redirect_domain: challenge page detected (%s) — origin %s is real domain",
                          final, host)
                return ""
            return final if final != _root(host) else ""
        except Exception as _exc:
            log.debug("_redirect_domain: scheme probe failed for %r (%s): %s", host, scheme, _exc)
            continue
    return None


def _has_web(root: str) -> bool:
    """Return True if root domain serves any HTTP response (status < 500).

    Validates that root resolves only to public addresses before connecting.
    Redirects are not followed — a 3xx response (< 500) still means the domain
    is live and serving HTTP, which is all the caller cares about.
    """
    if not _is_public_host(root):
        return False
    if root in _CHALLENGE_DOMAINS:
        return False
    for scheme in ("https", "http"):
        url = f"{scheme}://{root}"
        try:
            r = requests.get(url, timeout=_WEB_TIMEOUT, allow_redirects=False, stream=True)
            status = r.status_code
            r.close()
            if status < 500:
                return True
        except Exception:
            pass
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

    try:
        r = requests.get(
            "https://api.certspotter.com/v1/issuances",
            params={"domain": domain, "include_subdomains": "true", "expand": "dns_names"},
            headers=headers,
            timeout=_CT_TIMEOUT,
            stream=True,
        )
        if r.status_code == 429:
            _ra = r.headers.get("Retry-After", "3600")
            try:
                retry_after = int(_ra)
            except (ValueError, TypeError):
                retry_after = 3600  # HTTP-date or unparseable — safe fallback
            retry_after = max(1, min(retry_after, 3600))
            _certspotter_retry_after = time.time() + retry_after
            log.warning("certspotter 429 for %s — retry after %ds", domain, retry_after)
            r.close()
            return [], retry_after

        if r.status_code != 200:
            log.warning("certspotter HTTP %d for %s", r.status_code, domain)
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
        return top, None

    except Exception as e:
        log.warning("certspotter error for %s: %s", domain, e)
        return [], None


def _ct_crtsh(domain: str) -> list[str]:
    """crt.sh fallback — slower, sometimes unavailable."""
    try:
        r = requests.get(
            "https://crt.sh/",
            params={"q": domain, "output": "json"},
            timeout=_CRTSH_TIMEOUT,
            headers={"User-Agent": "python-h1b-discovery/1.0"},
            stream=True,
        )
        if r.status_code != 200:
            log.warning("crt.sh HTTP %d for %s", r.status_code, domain)
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
        return top

    except Exception as e:
        log.warning("crt.sh error for %s: %s", domain, e)
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


def discover_public_domain(assigned_domain: str) -> "tuple[str | None, str, int | None]":
    """
    Resolve an internal/email domain to the company's real public domain.

    Returns (public_domain, method, retry_after):
      public_domain — resolved domain string, or None if unresolvable
      method        — 'http_redirect' | 'root_fallback' | 'certspotter' |
                      'crtsh' | 'same_domain' | 'ct_quota' | 'no_signal'
      retry_after   — seconds before re-queuing (certspotter 429), else None
    """
    domain = (assigned_domain or "").lower().strip()
    if not domain:
        return None, "no_signal", None

    if _is_private_ip_literal(domain):
        log.warning("public_domain: rejecting private address %s", domain)
        return None, "no_signal", None

    # Step 1 — HTTP redirect on full domain
    redir = _redirect_domain(domain)
    if redir is None:
        log.debug("DNS fail for %s — trying root fallback", domain)
    elif redir == "":
        # Accept "already public" for root domains and www-prefixed subdomains.
        # A subdomain like ny.email.gs.com resolves within the same root (gs.com),
        # but the real public site may be at goldmansachs.com — fall through to CT log.
        # www is a standard public alias, not a meaningful subdomain.
        sub = _tldextract.extract(domain).subdomain
        if not sub or sub == "www":
            log.debug("%s already resolves publicly", domain)
            return domain, "same_domain", None
        log.debug("%s resolves within its root but has subdomain — continuing", domain)
    elif redir not in GENERIC_ROOTS:
        log.info("public_domain: %s → %s (http_redirect)", domain, redir)
        return redir, "http_redirect", None
    else:
        log.debug("public_domain: %s → %s (generic root — skipping)", domain, redir)

    # Step 2 — Root-domain fallback (strip subdomain prefix via PSL)
    ext      = _tldextract.extract(domain)
    root_try = ext.registered_domain
    if root_try and root_try != domain:
        redir = _redirect_domain(root_try)
        if redir is None:
            pass
        elif redir == "":
            if root_try not in GENERIC_ROOTS:
                log.info("public_domain: %s → %s (root_fallback)", domain, root_try)
                return root_try, "root_fallback", None
        elif redir not in GENERIC_ROOTS:
            log.info("public_domain: %s → %s (root_fallback)", domain, redir)
            return redir, "root_fallback", None
        else:
            log.debug("public_domain: %s → %s (generic root — skipping)", domain, redir)

    # Step 3 — CT log (certspotter → crt.sh fallback)
    log.debug("querying CT logs for %s", domain)
    candidates, retry_after, ct_source = _ct_domains(domain)

    if retry_after is not None:
        return None, "ct_quota", retry_after

    _ct_budget_start = time.time()
    for candidate in candidates[:10]:
        if time.time() - _ct_budget_start > _CT_PROBE_BUDGET_S:
            log.debug("CT probe budget exhausted for %s — stopping early", domain)
            break
        if _has_web(candidate):
            log.info("public_domain: %s → %s (%s)", domain, candidate, ct_source)
            return candidate, ct_source, None

    log.debug("no public domain signal for %s", domain)
    return None, "no_signal", None
