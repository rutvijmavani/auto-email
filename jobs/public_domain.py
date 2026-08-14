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
import time
from urllib.parse import urlparse

import requests
import tldextract as _tldextract
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

# Root domains belonging to cloud / email / CDN providers — never a real company domain
GENERIC_ROOTS = {
    "outlook.com", "hotmail.com", "gmail.com", "yahoo.com",
    "pphosted.com", "mimecast.com", "proofpoint.com", "messagelabs.com",
    "cloudflare.com", "fastly.com", "akamai.com", "amazonaws.com",
    "azure.com", "cloudfront.net", "googleusercontent.com",
    "office365.com", "microsoft.com", "googlehosted.com",
}

_REDIRECT_TIMEOUT = 8
_WEB_TIMEOUT      = 6
_CT_TIMEOUT       = 20
_CRTSH_TIMEOUT    = 30

# Module-level certspotter backoff — avoid hammering after a 429
_certspotter_retry_after: float = 0.0


def _root(u: str) -> str:
    if "://" not in u:
        u = "https://" + u
    h = urlparse(u).hostname or ""
    ext = _tldextract.extract(h)
    return ext.registered_domain or h


def _redirect_domain(host: str) -> "str | None":
    """
    Follow HTTP redirects on host. Returns:
      str  — root domain of final URL differs from host → redirect found
      ""   — final URL has same root as host → already public
      None — connection error / DNS fail
    """
    for scheme in ("https", "http"):
        url = f"{scheme}://{host}"
        try:
            r = requests.get(url, allow_redirects=True, timeout=_REDIRECT_TIMEOUT)
            final = _root(r.url)
            return final if final != _root(host) else ""
        except requests.exceptions.SSLError:
            # verify=False is intentional: company domains frequently have self-signed or
            # expired certs. We only use the redirect destination's domain name, never
            # the response body, so cert validity doesn't affect correctness. This
            # function is only called on domains already in our internal database.
            try:
                import warnings
                with warnings.catch_warnings():
                    warnings.simplefilter("ignore", _urllib3_no_ssl_warn)
                    r2 = requests.get(url, allow_redirects=True, timeout=_REDIRECT_TIMEOUT, verify=False)
                final = _root(r2.url)
                return final if final != _root(host) else ""
            except Exception:
                log.debug("_redirect_domain: SSL error for %s — no redirect signal", url)
                continue
        except Exception:
            continue
    return None


def _has_web(root: str) -> bool:
    """Return True if root domain serves any HTTP response (status < 500)."""
    for scheme in ("https", "http"):
        url = f"{scheme}://{root}"
        try:
            r = requests.get(url, timeout=_WEB_TIMEOUT, allow_redirects=True)
            if r.status_code < 500:
                return True
        except requests.exceptions.SSLError:
            # Same verify=False rationale as _redirect_domain: internal DB domains only,
            # we only check response status, not content.
            log.debug("_has_web: SSL error for %s — retrying without TLS verify", url)
            try:
                import warnings
                with warnings.catch_warnings():
                    warnings.simplefilter("ignore", _urllib3_no_ssl_warn)
                    r = requests.get(url, timeout=_WEB_TIMEOUT, allow_redirects=True, verify=False)
                if r.status_code < 500:
                    return True
            except Exception:
                pass
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
        )
        if r.status_code == 429:
            _ra = r.headers.get("Retry-After", "3600")
            try:
                retry_after = int(_ra)
            except (ValueError, TypeError):
                retry_after = 3600  # HTTP-date or unparseable — safe fallback
            _certspotter_retry_after = time.time() + retry_after
            log.warning("certspotter 429 for %s — retry after %ds", domain, retry_after)
            return [], retry_after

        if r.status_code != 200:
            log.warning("certspotter HTTP %d for %s", r.status_code, domain)
            return [], None

        certs = r.json()
        roots: dict[str, int] = {}
        for cert in certs:
            for d in cert.get("dns_names", []):
                d = d.lstrip("*.")
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
            f"https://crt.sh/?q={domain}&output=json",
            timeout=_CRTSH_TIMEOUT,
            headers={"User-Agent": "python-h1b-discovery/1.0"},
        )
        if r.status_code != 200:
            log.warning("crt.sh HTTP %d for %s", r.status_code, domain)
            return []

        roots: dict[str, int] = {}
        for cert in r.json():
            for d in cert.get("name_value", "").replace("\n", ",").split(","):
                d = d.strip().lstrip("*.")
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
    domain = assigned_domain.lower().strip()

    if _is_private_ip_literal(domain):
        log.warning("public_domain: rejecting private address %s", domain)
        return None, "no_signal", None

    # Step 1 — HTTP redirect on full domain
    redir = _redirect_domain(domain)
    if redir is None:
        log.debug("DNS fail for %s — trying root fallback", domain)
    elif redir == "":
        # Only accept "already public" for root domains. A subdomain like
        # ny.email.gs.com resolves within the same root (gs.com), but the
        # real public site may be at goldmansachs.com — fall through to CT log.
        if not _tldextract.extract(domain).subdomain:
            log.debug("%s already resolves publicly", domain)
            return None, "same_domain", None
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

    for candidate in candidates:
        if _has_web(candidate):
            log.info("public_domain: %s → %s (%s)", domain, candidate, ct_source)
            return candidate, ct_source, None

    log.debug("no public domain signal for %s", domain)
    return None, "no_signal", None
