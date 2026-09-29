# scripts/mobile_relay_watcher.py — on-demand launcher for the mobile relay
# (docs/discovery-pipeline-hardening.md Part 3, on-demand-start follow-up 2026-09-28).
#
# Runs on the home PC as an auto-start Windows service (registered via NSSM),
# bound ONLY to the WireGuard tunnel interface (10.10.0.2 by default) — same
# bind restriction as scripts/mobile_relay_socks5.py, never 0.0.0.0.
#
# Exposes exactly one control action: a bare "START\n" line. workers/manager.py
# (the VM side, which already knows the mobile-relay queue depth locally) sends
# this whenever the queue has backlog but the relay port isn't reachable yet.
# On receiving it, this watcher:
#   - spawns scripts/mobile_relay_socks5.py as a subprocess if it isn't already
#     running (or has died since the last trigger — self-healing restart)
#   - resets an idle-stop timer; the relay subprocess is killed after
#     MOBILE_RELAY_IDLE_STOP_S seconds with no new START trigger, so the actual
#     SOCKS5 relay (the thing exposing this PC's residential IP as egress for
#     the VM) only runs while there's real queued work, not 24/7.
#
# No auth token on the control channel: the tunnel interface itself is the
# trust boundary (only the WireGuard peer, i.e. the VM, can reach 10.10.0.2 at
# all) — identical trust model to the relay's own no-auth SOCKS5 listener.
#
# Usage:
#   python scripts/mobile_relay_watcher.py
#   python scripts/mobile_relay_watcher.py --host 10.10.0.2 --port 1081
#
# Intended to run continuously (NSSM service = restart-on-crash + start-at-boot).

import argparse
import asyncio
import ipaddress
import os
import subprocess
import sys
import time

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from logger import get_logger, init_logging

log = get_logger(__name__)

_RELAY_SCRIPT = os.path.join(os.path.dirname(__file__), "mobile_relay_socks5.py")


class _RelayLauncher:
    """Owns the relay subprocess lifecycle: start-on-trigger, self-heal if it
    died, idle-stop after a quiet period. Single instance, no locking needed —
    everything runs on one asyncio event loop thread."""

    def __init__(self, relay_host: str, relay_port: int, idle_stop_s: int):
        self._relay_host = relay_host
        self._relay_port = relay_port
        self._idle_stop_s = idle_stop_s
        self._proc: "subprocess.Popen | None" = None
        self._last_trigger = 0.0

    def _is_running(self) -> bool:
        return self._proc is not None and self._proc.poll() is None

    def on_trigger(self) -> None:
        self._last_trigger = time.monotonic()
        if self._is_running():
            log.debug("relay already running (pid=%d) — trigger noted, no restart needed", self._proc.pid)
            return
        log.info("starting relay subprocess: %s --host %s --port %d",
                  _RELAY_SCRIPT, self._relay_host, self._relay_port)
        self._proc = subprocess.Popen(
            [sys.executable, _RELAY_SCRIPT, "--host", self._relay_host, "--port", str(self._relay_port)],
        )

    async def idle_watch_loop(self) -> None:
        """Runs forever alongside the control server; stops the relay subprocess
        once MOBILE_RELAY_IDLE_STOP_S has passed since the last START trigger."""
        while True:
            await asyncio.sleep(10)
            if not self._is_running():
                continue
            idle_for = time.monotonic() - self._last_trigger
            if idle_for >= self._idle_stop_s:
                log.info("relay idle for %.0fs (>= %ds) — stopping subprocess (pid=%d)",
                          idle_for, self._idle_stop_s, self._proc.pid)
                self._proc.terminate()
                try:
                    self._proc.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    log.warning("relay subprocess did not exit in time — killing (pid=%d)", self._proc.pid)
                    self._proc.kill()
                self._proc = None


async def _handle_client(reader: asyncio.StreamReader, writer: asyncio.StreamWriter,
                          launcher: _RelayLauncher) -> None:
    peer = writer.get_extra_info("peername")
    try:
        line = await asyncio.wait_for(reader.readline(), timeout=5)
        cmd = line.strip().upper()
        if cmd == b"START":
            launcher.on_trigger()
            writer.write(b"OK\n")
        else:
            log.debug("Unrecognized control command %r from %s — ignoring", cmd, peer)
            writer.write(b"UNKNOWN\n")
        await writer.drain()
    except (asyncio.IncompleteReadError, asyncio.TimeoutError, ConnectionResetError, OSError) as e:
        log.debug("Control client %s dropped: %s", peer, e)
    finally:
        try:
            writer.close()
        except OSError:
            pass


async def _run(host: str, port: int, relay_host: str, relay_port: int, idle_stop_s: int) -> None:
    launcher = _RelayLauncher(relay_host, relay_port, idle_stop_s)
    server = await asyncio.start_server(
        lambda r, w: _handle_client(r, w, launcher), host=host, port=port,
    )
    addrs = ", ".join(str(s.getsockname()) for s in server.sockets)
    log.info("Mobile relay watcher listening on %s (idle-stop=%ds, relay=%s:%d)",
              addrs, idle_stop_s, relay_host, relay_port)
    async with server:
        await asyncio.gather(server.serve_forever(), launcher.idle_watch_loop())


def main():
    init_logging("mobile_relay_watcher")

    parser = argparse.ArgumentParser(
        description="On-demand launcher for the mobile relay SOCKS5 proxy (home PC side)"
    )
    parser.add_argument("--host", default="10.10.0.2",
                        help="Interface to bind to (default: 10.10.0.2, the WireGuard tunnel IP — never use 0.0.0.0)")
    parser.add_argument("--port", type=int, default=1081,
                        help="Control port to listen on (default: 1081)")
    parser.add_argument("--relay-host", default="10.10.0.2",
                        help="Host to pass to mobile_relay_socks5.py --host (default: 10.10.0.2)")
    parser.add_argument("--relay-port", type=int, default=1080,
                        help="Port to pass to mobile_relay_socks5.py --port (default: 1080)")
    parser.add_argument("--idle-stop-seconds", type=int, default=300,
                        help="Stop the relay subprocess after this long with no new START trigger (default: 300)")
    args = parser.parse_args()

    if args.host in ("0.0.0.0", "::"):
        parser.error("Refusing to bind to 0.0.0.0 — this control channel must stay private to the WireGuard tunnel interface only.")

    try:
        asyncio.run(_run(args.host, args.port, args.relay_host, args.relay_port, args.idle_stop_seconds))
    except KeyboardInterrupt:
        log.info("Mobile relay watcher stopped (Ctrl+C)")


if __name__ == "__main__":
    main()
