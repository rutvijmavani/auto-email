"""
tests/test_http_safe.py — SSRFAdapter unit tests.

SSRFAdapter.send() — HTTP path:
  · rewrites URL hostname to resolved IP (eliminates DNS-rebinding race)
  · sets Host header with virtual-hosting name
  · Host header omits port for default port 80
  · Host header includes port for non-default port

SSRFAdapter.send() — HTTPS path:
  · does NOT rewrite URL (keeps TLS SNI / cert validation intact)
  · strips any stale Host header (regression: HTTP→HTTPS redirect hop
    must not carry over the Host set for the HTTP leg — Pass 64 fix)
  · blocks private-IP targets

SSRFAdapter.send() — SSRF blocking:
  · rejects loopback addresses (127.0.0.1)
  · rejects RFC-1918 addresses (10.x, 172.16.x, 192.168.x)
  · rejects empty hostname
  · rejects invalid port in URL
  · rejects IPv4-mapped IPv6 private addresses (::ffff:127.0.0.1)
  · raises requests.exceptions.ConnectionError on DNS failure

make_safe_session():
  · mounts SSRFAdapter on both http:// and https://
"""
import socket
import unittest
from unittest.mock import MagicMock, patch

import requests
from requests import PreparedRequest

from jobs.http_safe import SSRFAdapter, is_private_host, make_safe_session


def _fake_getaddrinfo(ip_str, family=socket.AF_INET):
    """Return a getaddrinfo-shaped list for the given IP."""
    return [(family, socket.SOCK_STREAM, 0, "", (ip_str, 0))]


def _make_request(url, extra_headers=None):
    req = PreparedRequest()
    req.prepare_url(url, {})
    req.prepare_headers(extra_headers or {})
    req.prepare_body(None, None)
    req.prepare_method("GET")
    return req


class TestSSRFAdapterHTTP(unittest.TestCase):
    def _send(self, url, ip="1.2.3.4", extra_headers=None):
        adapter = SSRFAdapter()
        req = _make_request(url, extra_headers)
        with patch("jobs.http_safe.socket.getaddrinfo", return_value=_fake_getaddrinfo(ip)):
            with patch.object(type(adapter).__mro__[1], "send", return_value=MagicMock()) as mock_super:
                adapter.send(req)
                return req, mock_super

    def test_http_url_rewritten_to_ip(self):
        req, _ = self._send("http://example.com/path")
        self.assertIn("1.2.3.4", req.url)
        self.assertNotIn("example.com", req.url)

    def test_http_host_header_set(self):
        req, _ = self._send("http://example.com/path")
        self.assertEqual(req.headers["Host"], "example.com")

    def test_http_host_header_includes_nondefault_port(self):
        req, _ = self._send("http://example.com:8080/path")
        self.assertEqual(req.headers["Host"], "example.com:8080")

    def test_http_host_header_omits_default_port_80(self):
        req, _ = self._send("http://example.com:80/path")
        self.assertEqual(req.headers["Host"], "example.com")


class TestSSRFAdapterHTTPS(unittest.TestCase):
    def _send(self, url, ip="1.2.3.4", extra_headers=None):
        adapter = SSRFAdapter()
        req = _make_request(url, extra_headers)
        with patch("jobs.http_safe.socket.getaddrinfo", return_value=_fake_getaddrinfo(ip)):
            with patch.object(type(adapter).__mro__[1], "send", return_value=MagicMock()):
                adapter.send(req)
                return req

    def test_https_url_not_rewritten(self):
        req = self._send("https://example.com/path")
        self.assertIn("example.com", req.url)
        self.assertNotIn("1.2.3.4", req.url)

    def test_https_strips_stale_host_header_from_http_hop(self):
        # Regression test for Pass 64: HTTP→HTTPS redirect must not carry over
        # the Host header that SSRFAdapter set for the HTTP leg.
        req = self._send(
            "https://example.com/path",
            extra_headers={"Host": "192.168.1.1"},  # stale header from HTTP hop
        )
        self.assertNotIn("Host", req.headers)

    def test_https_no_spurious_host_header_added(self):
        req = self._send("https://example.com/path")
        self.assertNotIn("Host", req.headers)


class TestSSRFAdapterBlocking(unittest.TestCase):
    def _assert_blocked(self, url, ip):
        adapter = SSRFAdapter()
        req = _make_request(url)
        with patch(
            "jobs.http_safe.socket.getaddrinfo",
            return_value=_fake_getaddrinfo(ip),
        ):
            with self.assertRaises(requests.exceptions.ConnectionError):
                adapter.send(req)

    def test_blocks_loopback(self):
        self._assert_blocked("http://internal.example.com/", "127.0.0.1")

    def test_blocks_rfc1918_10(self):
        self._assert_blocked("http://internal.example.com/", "10.0.0.1")

    def test_blocks_rfc1918_172(self):
        self._assert_blocked("http://internal.example.com/", "172.16.0.1")

    def test_blocks_rfc1918_192(self):
        self._assert_blocked("http://internal.example.com/", "192.168.1.1")

    def test_blocks_ipv4_mapped_ipv6_loopback(self):
        adapter = SSRFAdapter()
        req = _make_request("http://internal.example.com/")
        mapped = [(socket.AF_INET6, socket.SOCK_STREAM, 0, "", ("::ffff:127.0.0.1", 0))]
        with patch("jobs.http_safe.socket.getaddrinfo", return_value=mapped):
            with self.assertRaises(requests.exceptions.ConnectionError):
                adapter.send(req)

    def test_blocks_empty_hostname(self):
        adapter = SSRFAdapter()
        req = PreparedRequest()
        req.url = "http://"
        req.headers = {}
        req.body = None
        req.method = "GET"
        with self.assertRaises(requests.exceptions.ConnectionError):
            adapter.send(req)

    def test_blocks_dns_failure(self):
        adapter = SSRFAdapter()
        req = _make_request("http://no-such-host.invalid/")
        with patch("jobs.http_safe.socket.getaddrinfo", side_effect=OSError("NXDOMAIN")):
            with self.assertRaises(requests.exceptions.ConnectionError):
                adapter.send(req)

    def test_blocks_invalid_port(self):
        adapter = SSRFAdapter()
        req = PreparedRequest()
        req.url = "http://example.com:notaport/"
        req.headers = {}
        req.body = None
        req.method = "GET"
        with self.assertRaises(requests.exceptions.ConnectionError):
            adapter.send(req)


class TestIsPrivateHost(unittest.TestCase):
    def test_loopback_is_private(self):
        with patch("jobs.http_safe.socket.getaddrinfo", return_value=_fake_getaddrinfo("127.0.0.1")):
            self.assertTrue(is_private_host("localhost"))

    def test_public_ip_is_not_private(self):
        with patch("jobs.http_safe.socket.getaddrinfo", return_value=_fake_getaddrinfo("8.8.8.8")):
            self.assertFalse(is_private_host("dns.google"))

    def test_unresolvable_treated_as_private(self):
        with patch("jobs.http_safe.socket.getaddrinfo", side_effect=OSError):
            self.assertTrue(is_private_host("no-such-host.invalid"))


class TestMakeSafeSession(unittest.TestCase):
    def test_adapters_mounted(self):
        session = make_safe_session()
        self.assertIsInstance(session.get_adapter("http://x"), SSRFAdapter)
        self.assertIsInstance(session.get_adapter("https://x"), SSRFAdapter)


if __name__ == "__main__":
    unittest.main()
