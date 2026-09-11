"""
jobs/http_safe.py â€” SSRF-safe HTTP session for pipeline outbound requests.

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

    Fix â€” HTTP: resolve once, validate ALL returned IPs, rewrite the URL hostname
    to the resolved IP so urllib3 re-resolves IPâ†’IP (no-op), eliminating the race.
    Fix â€” HTTPS: resolve + validate all IPs, keep the original hostname so TLS SNI
    and certificate validation are unaffected. DNS-rebinding on HTTPS requires the
    attacker to also hold a valid cert for the public domain â€” practically infeasible.

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
            ip_host = f"[{safe_ip}]" if ":" in safe_ip else safe_ip
            netloc  = f"{ip_host}:{explicit_port}" if explicit_port else ip_host
            request.url = urlunparse(parsed._replace(netloc=netloc))
            # Include port in Host header only when non-default (RFC 7230 Â§5.4).
            _default_port = 80
            host_header = f"{host}:{explicit_port}" if explicit_port and explicit_port != _default_port else host
            request.headers["Host"] = host_header

        return super().send(request, *args, **kwargs)


def make_safe_session() -> requests.Session:
    """Return a new requests.Session with SSRFAdapter mounted on http:// and https://."""
    session = requests.Session()
    session.mount("http://",  SSRFAdapter())
    session.mount("https://", SSRFAdapter())
    return session

