"""
jobs/http_safe.py — SSRF-safe HTTP session for pipeline outbound requests.

Provides make_safe_session() which returns a requests.Session with SSRFAdapter
mounted on both http:// and https://, closing the DNS-rebinding TOCTOU gap.

Usage:
    from jobs.http_safe import make_safe_session, is_private_host

    _session = make_safe_session()      # module-level; reuse across calls
    resp = _session.get(url, ...)
"""

import ipaddress
import socket
from urllib.parse import urlparse, urlunparse

import requests
from requests.adapters import HTTPAdapter

# Chrome-impersonated fetching (docs/discovery-pipeline-hardening.md Part 2) —
# curl_cffi matches Chrome's TLS fingerprint (JA3) + HTTP/2, which plain urllib3
# does not, so WAF/bot-management vendors (Cloudflare, Akamai, PerimeterX) that
# fingerprint the TLS handshake stop distinguishing us from a real browser.
# curl_cffi has no clean CURLOPT_RESOLVE/resolve= kwarg for DNS pinning (checked
# against the installed 0.15.0: not present in Session.request's signature), so
# unlike SSRFAdapter above there is no resolve-then-pin adapter for it. Callers
# must pre-flight-validate each host/hop with is_private_host() before every
# request/redirect hop instead — same accepted TOCTOU-gap precedent already in
# use in jobs/ats/career_detector.py (all URLs originate from our own DB, no
# untrusted user input enters these fetches).
try:
    from curl_cffi.requests import Session as _CurlSession
    _CURL_AVAILABLE = True
except ImportError:
    _CURL_AVAILABLE = False

_PRIVATE_NETS = [
    ipaddress.ip_network(r) for r in (
        "10.0.0.0/8", "172.16.0.0/12", "192.168.0.0/16",
        "127.0.0.0/8", "169.254.0.0/16", "0.0.0.0/8",
        "100.64.0.0/10",   # CGNAT / shared address space (RFC 6598)
        "192.0.0.0/24",    # IETF protocol assignments (RFC 5736)
        "198.18.0.0/15",   # benchmarking (RFC 2544)
        "224.0.0.0/4",     # multicast
        "240.0.0.0/4",     # reserved
        "::/128",                              # unspecified address
        "::1/128", "fc00::/7", "fe80::/10",  # loopback, ULA, link-local
        "ff00::/8",        # IPv6 multicast
        "64:ff9b::/96",    # IPv4-mapped / NAT64
    )
]


def is_private_host(host: str) -> bool:
    """Return True when ANY address getaddrinfo returns is private/loopback (fail closed)."""
    try:
        results = socket.getaddrinfo(host, None)
        if not results:
            return True
        for _family, _type, _proto, _canon, sockaddr in results:
            addr = ipaddress.ip_address(sockaddr[0])
            if any(addr in net for net in _PRIVATE_NETS):
                return True
            mapped = getattr(addr, "ipv4_mapped", None)
            if mapped and any(mapped in net for net in _PRIVATE_NETS):
                return True
        return False
    except Exception:
        return True  # treat unresolvable as private (fail closed)


class SSRFAdapter(HTTPAdapter):
    """Closes the DNS-rebinding TOCTOU gap for outbound HTTP/HTTPS requests.

    The gap: a validation check calling getaddrinfo once, then requests/urllib3
    calling getaddrinfo again at connect time. A DNS server with TTL=0 can
    return different IPs on successive queries, slipping a private IP through.

    Fix — HTTP: resolve once, validate ALL returned IPs, rewrite the URL hostname
    to the resolved IP so urllib3 re-resolves IP→IP (no-op), eliminating the race.
    Fix — HTTPS: resolve + validate all IPs, keep the original hostname so TLS SNI
    and certificate validation are unaffected. DNS-rebinding on HTTPS requires the
    attacker to also hold a valid cert for the public domain — practically infeasible.

    Raises requests.exceptions.ConnectionError on any SSRF risk.
    """

    def send(self, request, *args, **kwargs):
        parsed = urlparse(request.url)
        host   = parsed.hostname or ""
        try:
            explicit_port = parsed.port
        except ValueError as exc:
            raise requests.exceptions.ConnectionError(
                f"SSRF: invalid port in URL: {exc}"
            ) from exc
        port = explicit_port or (443 if parsed.scheme == "https" else 80)

        if not host:
            raise requests.exceptions.ConnectionError("SSRF: empty hostname")

        try:
            addrs = socket.getaddrinfo(host, port, type=socket.SOCK_STREAM)
        except OSError as exc:
            raise requests.exceptions.ConnectionError(
                f"SSRF: DNS resolution failed for {host!r}: {exc}"
            ) from exc

        if not addrs:
            raise requests.exceptions.ConnectionError(f"SSRF: no DNS results for {host!r}")

        safe_ip = None
        for _fam, _typ, _prt, _can, sockaddr in addrs:
            ip = ipaddress.ip_address(sockaddr[0])
            if any(ip in net for net in _PRIVATE_NETS):
                raise requests.exceptions.ConnectionError(f"SSRF: {host!r} resolves to private {ip}")
            mapped = getattr(ip, "ipv4_mapped", None)
            if mapped and any(mapped in net for net in _PRIVATE_NETS):
                raise requests.exceptions.ConnectionError(
                    f"SSRF: {host!r} resolves to IPv4-mapped private {ip}"
                )
            if safe_ip is None:
                safe_ip = sockaddr[0]

        if parsed.scheme == "http":
            # Rewrite URL to the resolved IP so urllib3 won't re-resolve.
            # Host header preserves virtual-hosting / HTTP/1.1 semantics.
            logical_url = request.url
            ip_host = f"[{safe_ip}]" if ":" in safe_ip else safe_ip
            netloc  = f"{ip_host}:{explicit_port}" if explicit_port else ip_host
            request.url = urlunparse(parsed._replace(netloc=netloc))
            # Include port in Host header only when non-default (RFC 7230 §5.4).
            _default_port = 80
            host_header = f"{host}:{explicit_port}" if explicit_port and explicit_port != _default_port else host
            request.headers["Host"] = host_header
            response = super().send(request, *args, **kwargs)
            # requests sets Response.url from the (IP-rewritten) request.url —
            # restore the logical hostname so downstream consumers (e.g.
            # career_page._fetch_and_scan's final_url) persist/compare domains,
            # not connection IPs.
            response.url = logical_url
            return response
        else:
            # HTTPS: do not carry over a Host header from a previous HTTP hop.
            request.headers.pop("Host", None)

        return super().send(request, *args, **kwargs)


def make_safe_session() -> requests.Session:
    """Return a new requests.Session with SSRFAdapter mounted on http:// and https://."""
    session = requests.Session()
    session.mount("http://",  SSRFAdapter())
    session.mount("https://", SSRFAdapter())
    return session


def make_safe_curl_session():
    """Return a curl_cffi Session impersonating Chrome 124 (docs/discovery-pipeline-
    hardening.md Part 2), falling back to make_safe_session() when curl_cffi isn't
    installed.

    No SSRFAdapter-style DNS pinning here — see the note above on _CURL_AVAILABLE.
    Callers must call is_private_host() on every host/hop before connecting.
    """
    if _CURL_AVAILABLE:
        return _CurlSession(impersonate="chrome124")
    return make_safe_session()


def make_relay_curl_session(proxy_host: str, proxy_port: int):
    """Return a curl_cffi Session identical to make_safe_curl_session() but routed
    through a local SOCKS5 proxy (docs/discovery-pipeline-hardening.md Part 3 —
    the WireGuard-tunneled home-PC relay, scripts/mobile_relay_socks5.py).

    Requires curl_cffi — the mobile relay drain worker is the only caller and
    curl_cffi is already a hard dependency of the rest of the discovery pipeline
    it shares a process with, so no requests-based fallback is provided here.
    socks5h (not socks5) so DNS resolution also happens at the proxy end, on the
    home PC's network — not on the OCI VM, which is the whole point of the relay.
    """
    if not _CURL_AVAILABLE:
        raise RuntimeError("make_relay_curl_session requires curl_cffi, which is not installed")
    proxy_url = f"socks5h://{proxy_host}:{proxy_port}"
    return _CurlSession(impersonate="chrome124", proxies={"http": proxy_url, "https": proxy_url})


class ResponseTooLarge(Exception):
    """Raised by read_bounded_text when a response body exceeds the byte limit."""


def read_bounded_text(resp, max_bytes=None) -> str:
    """
    Read a streamed response (requests or curl_cffi, fetched with stream=True) and
    return its decoded text, raising ResponseTooLarge as soon as the body exceeds
    max_bytes (declared Content-Length is checked first). Always closes resp.
    """
    limit = max_bytes if max_bytes is not None else _default_max_bytes()
    try:
        declared = resp.headers.get("Content-Length")
        if declared and declared.isdigit() and int(declared) > limit:
            raise ResponseTooLarge(f"Content-Length {declared} > {limit}")
        buf = bytearray()
        for chunk in resp.iter_content(chunk_size=65536):
            buf.extend(chunk)
            if len(buf) > limit:
                raise ResponseTooLarge(f"body exceeds {limit} bytes")
    finally:
        resp.close()
    try:
        return bytes(buf).decode(getattr(resp, "encoding", None) or "utf-8", errors="replace")
    except LookupError:
        return bytes(buf).decode("utf-8", errors="replace")


def _default_max_bytes() -> int:
    try:
        from config import HTTP_FETCH_MAX_BYTES
        return HTTP_FETCH_MAX_BYTES
    except Exception:
        return 10 * 1024 * 1024

