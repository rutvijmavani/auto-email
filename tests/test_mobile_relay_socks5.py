"""
tests/test_mobile_relay_socks5.py
─────────────────────────────────────────────────────────────────────────────
scripts/mobile_relay_socks5.py — SOCKS5 CONNECT handshake with a domain-name
destination (ATYP 0x03), the form curl's socks5h:// sends.

Regression: the destination was decoded with .decode("idna", errors="replace"),
which raises UnicodeError ("Unsupported error handling: replace") — every
domain-name CONNECT was dropped before any reply (curl error 97), so the relay
never forwarded a single request. Uses loopback only; no real network.
"""

import asyncio
import os
import struct
import sys
import unittest
from unittest.mock import patch

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from scripts import mobile_relay_socks5 as relay


async def _socks_connect(proxy_port: int, domain_bytes: bytes, dst_port: int):
    reader, writer = await asyncio.open_connection("127.0.0.1", proxy_port)
    writer.write(struct.pack("!BBB", 5, 1, 0))
    await writer.drain()
    greeting = await reader.readexactly(2)
    writer.write(struct.pack("!BBBBB", 5, 1, 0, 3, len(domain_bytes)) + domain_bytes
                 + struct.pack("!H", dst_port))
    await writer.drain()
    return reader, writer, greeting


class TestDomainNameConnect(unittest.TestCase):
    def test_domain_connect_is_relayed(self):
        async def run():
            async def echo(r, w):
                w.write(await r.read(5))
                await w.drain()
                w.close()

            target = await asyncio.start_server(echo, "127.0.0.1", 0)
            target_port = target.sockets[0].getsockname()[1]
            proxy = await asyncio.start_server(relay._handle_client, "127.0.0.1", 0)
            proxy_port = proxy.sockets[0].getsockname()[1]
            try:
                with patch.object(relay, "_first_global_ip", return_value="127.0.0.1"):
                    reader, writer, greeting = await _socks_connect(
                        proxy_port, b"example.test", target_port)
                    reply = await reader.readexactly(10)
                    writer.write(b"hello")
                    await writer.drain()
                    echoed = await reader.readexactly(5)
                    writer.close()
                return greeting, reply[1], echoed
            finally:
                proxy.close()
                target.close()

        greeting, rep, echoed = asyncio.run(run())
        self.assertEqual(greeting, bytes([5, 0]))
        self.assertEqual(rep, 0x00)
        self.assertEqual(echoed, b"hello")

    def test_non_ascii_domain_is_refused_with_reply_not_dropped(self):
        async def run():
            proxy = await asyncio.start_server(relay._handle_client, "127.0.0.1", 0)
            proxy_port = proxy.sockets[0].getsockname()[1]
            try:
                reader, writer, _ = await _socks_connect(proxy_port, "bücher.test".encode("utf-8"), 80)
                reply = await reader.readexactly(10)
                writer.close()
                return reply[1]
            finally:
                proxy.close()

        self.assertEqual(asyncio.run(run()), 0x08)


if __name__ == "__main__":
    unittest.main()
