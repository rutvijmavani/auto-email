# scripts/mobile_relay_socks5.py — Minimal SOCKS5 relay for the Mobile
# Relay Queue (docs/discovery-pipeline-hardening.md, Part 3).
#
# Runs on the home PC, bound ONLY to the WireGuard tunnel interface
# (10.10.0.2 by default), never on 0.0.0.0 — so it is unreachable from the
# home LAN or the public internet, only from the OCI VM over the tunnel.
#
# Pure-stdlib asyncio implementation (RFC 1928): no-auth SOCKS5, CONNECT
# command only (BIND / UDP ASSOCIATE are refused — nothing in this
# pipeline needs them). IPv4 and domain-name address types are supported.
# Outbound connections use the machine's normal default route (the home
# ISP connection), which is the entire point — this lets the VM's
# requests exit through a residential IP for domains that block
# datacenter ranges.
#
# Usage:
#   python scripts/mobile_relay_socks5.py
#   python scripts/mobile_relay_socks5.py --host 10.10.0.2 --port 1080
#
# Runs in the foreground; Ctrl+C to stop. Not installed as a service —
# start it only when you want the mobile-relay fallback available.

import argparse
import asyncio
import ipaddress
import os
import socket
import struct
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from logger import get_logger, init_logging

log = get_logger(__name__)

SOCKS_VERSION = 0x05

# Same private/loopback/link-local/reserved/CGNAT exclusion set as
# jobs/public_domain.py::_is_public_host and jobs/http_safe.py's SSRFAdapter —
# kept as a self-contained stdlib check here (no project-module import) since
# this relay is meant to run standalone on the home PC with minimal deps.
_CGNAT_NET = ipaddress.IPv4Network("100.64.0.0/10")


def _is_global_addr(addr: "ipaddress.IPv4Address | ipaddress.IPv6Address") -> bool:
    """Return True only for a globally-routable address — rejects loopback,
    link-local, RFC1918/ULA, CGNAT, reserved, unspecified, and multicast."""
    if (addr.is_loopback or addr.is_link_local or addr.is_private
            or addr.is_reserved or addr.is_unspecified or addr.is_multicast):
        return False
    if isinstance(addr, ipaddress.IPv4Address) and addr in _CGNAT_NET:
        return False
    return True


def _first_global_ip(host: str) -> "str | None":
    """Resolve host (or accept it as an IP literal) and return the first
    globally-routable address found, or None if host is an IP literal that
    isn't global, or every resolved address is non-global (fail-closed on
    DNS failure too). Run in a thread executor by the caller — getaddrinfo
    is blocking.

    Resolving here and connecting to the returned IP literal (never to the
    original hostname again) closes the DNS-rebinding TOCTOU gap: a second,
    independent lookup at connect time could return a different, private
    address for a TTL=0 attacker-controlled domain.
    """
    try:
        addr = ipaddress.ip_address(host)
        return host if _is_global_addr(addr) else None
    except ValueError:
        pass  # not an IP literal — resolve as a domain name
    try:
        infos = socket.getaddrinfo(host, None)
    except OSError:
        return None
    for info in infos:
        try:
            addr = ipaddress.ip_address(info[4][0])
        except ValueError:
            continue
        if _is_global_addr(addr):
            return str(addr)
    return None

# Address types (RFC 1928 §5)
ATYP_IPV4 = 0x01
ATYP_DOMAIN = 0x03
ATYP_IPV6 = 0x04

CMD_CONNECT = 0x01

RELAY_BUF_SIZE = 65536


async def _pipe(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
    """Copy bytes from reader to writer until EOF, then half-close."""
    try:
        while True:
            data = await reader.read(RELAY_BUF_SIZE)
            if not data:
                break
            writer.write(data)
            await writer.drain()
    except (ConnectionResetError, BrokenPipeError, OSError):
        pass
    finally:
        try:
            writer.write_eof()
        except (OSError, RuntimeError):
            pass


async def _handle_client(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
    peer = writer.get_extra_info("peername")
    try:
        await _negotiate_and_relay(reader, writer, peer)
    except (asyncio.IncompleteReadError, ConnectionResetError, OSError) as e:
        log.debug("Client %s dropped mid-negotiation: %s", peer, e)
    except Exception:
        log.exception("Unhandled error serving client %s", peer)
    finally:
        try:
            writer.close()
        except OSError:
            pass


async def _negotiate_and_relay(reader, writer, peer) -> None:
    # --- Greeting: VER, NMETHODS, METHODS[] ---
    ver, nmethods = struct.unpack("!BB", await reader.readexactly(2))
    if ver != SOCKS_VERSION:
        log.debug("Rejecting non-SOCKS5 client %s (ver=%d)", peer, ver)
        writer.close()
        return
    await reader.readexactly(nmethods)  # we only support no-auth; ignore offered methods

    # No-auth only.
    writer.write(struct.pack("!BB", SOCKS_VERSION, 0x00))
    await writer.drain()

    # --- Request: VER, CMD, RSV, ATYP, DST.ADDR, DST.PORT ---
    ver, cmd, _rsv, atyp = struct.unpack("!BBBB", await reader.readexactly(4))
    if ver != SOCKS_VERSION:
        writer.close()
        return

    if atyp == ATYP_IPV4:
        addr_bytes = await reader.readexactly(4)
        dst_addr = ".".join(str(b) for b in addr_bytes)
    elif atyp == ATYP_DOMAIN:
        (length,) = struct.unpack("!B", await reader.readexactly(1))
        # ASCII only: socks5h clients (curl) send an IDN already punycoded. The "idna"
        # codec rejects errors="replace" with UnicodeError, which used to drop every
        # domain-name CONNECT (curl error 97 "connection to proxy closed") before any
        # reply was written — so the relay never forwarded a single socks5h request.
        try:
            dst_addr = (await reader.readexactly(length)).decode("ascii")
        except UnicodeDecodeError:
            await _send_reply(writer, 0x08)  # address type not supported
            writer.close()
            return
    elif atyp == ATYP_IPV6:
        addr_bytes = await reader.readexactly(16)
        dst_addr = ":".join(f"{addr_bytes[i]:02x}{addr_bytes[i+1]:02x}" for i in range(0, 16, 2))
    else:
        await _send_reply(writer, 0x08)  # address type not supported
        writer.close()
        return

    (dst_port,) = struct.unpack("!H", await reader.readexactly(2))

    if cmd != CMD_CONNECT:
        log.debug("Refusing unsupported SOCKS5 command %d from %s", cmd, peer)
        await _send_reply(writer, 0x07)  # command not supported
        writer.close()
        return

    loop = asyncio.get_running_loop()
    vetted_ip = await loop.run_in_executor(None, _first_global_ip, dst_addr)
    if vetted_ip is None:
        log.warning("Rejecting non-global destination %s:%d (client %s)", dst_addr, dst_port, peer)
        await _send_reply(writer, 0x02)  # connection not allowed by ruleset
        writer.close()
        return

    try:
        target_reader, target_writer = await asyncio.wait_for(
            asyncio.open_connection(vetted_ip, dst_port), timeout=15
        )
    except asyncio.TimeoutError:
        log.warning("Connect timeout to %s:%d (client %s)", dst_addr, dst_port, peer)
        await _send_reply(writer, 0x04)  # host unreachable
        writer.close()
        return
    except OSError as e:
        log.warning("Connect failed to %s:%d (client %s): %s", dst_addr, dst_port, peer, e)
        await _send_reply(writer, 0x05)  # connection refused
        writer.close()
        return

    bind_host, bind_port = target_writer.get_extra_info("sockname")[:2]
    await _send_reply(writer, 0x00, bind_host, bind_port)
    log.info("Relaying %s -> %s:%d", peer, dst_addr, dst_port)

    await asyncio.gather(
        _pipe(reader, target_writer),
        _pipe(target_reader, writer),
        return_exceptions=True,
    )
    try:
        target_writer.close()
    except OSError:
        pass


async def _send_reply(writer, rep: int, bind_addr: str = "0.0.0.0", bind_port: int = 0) -> None:
    try:
        addr_bytes = bytes(int(o) for o in bind_addr.split("."))
        atyp = ATYP_IPV4
    except ValueError:
        addr_bytes = bind_addr.encode("idna")
        atyp = ATYP_DOMAIN
        addr_bytes = struct.pack("!B", len(addr_bytes)) + addr_bytes
    header = struct.pack("!BBBB", SOCKS_VERSION, rep, 0x00, atyp)
    if atyp == ATYP_IPV4:
        writer.write(header + addr_bytes + struct.pack("!H", bind_port))
    else:
        writer.write(header + addr_bytes + struct.pack("!H", bind_port))
    await writer.drain()


async def _run(host: str, port: int) -> None:
    server = await asyncio.start_server(_handle_client, host=host, port=port)
    addrs = ", ".join(str(s.getsockname()) for s in server.sockets)
    log.info("Mobile relay SOCKS5 server listening on %s", addrs)
    async with server:
        await server.serve_forever()


def main():
    init_logging("mobile_relay_socks5")

    parser = argparse.ArgumentParser(
        description="Minimal SOCKS5 relay for the mobile relay queue (home PC side)"
    )
    parser.add_argument("--host", default="10.10.0.2",
                        help="Interface to bind to (default: 10.10.0.2, the WireGuard tunnel IP — never use 0.0.0.0)")
    parser.add_argument("--port", type=int, default=1080,
                        help="Port to listen on (default: 1080)")
    args = parser.parse_args()

    if args.host in ("0.0.0.0", "::"):
        parser.error("Refusing to bind to 0.0.0.0 — this proxy must stay private to the WireGuard tunnel interface only.")

    try:
        asyncio.run(_run(args.host, args.port))
    except KeyboardInterrupt:
        log.info("Mobile relay SOCKS5 server stopped (Ctrl+C)")


if __name__ == "__main__":
    main()
