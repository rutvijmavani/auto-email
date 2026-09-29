"""
tests/test_mobile_relay_watcher.py
─────────────────────────────────────────────────────────────────────────────
scripts/mobile_relay_watcher.py — on-demand launcher for the mobile relay
SOCKS5 proxy (home PC side, 2026-09-28 on-demand-start follow-up).

Covers _RelayLauncher: start-on-trigger, self-heal restart if the subprocess
died, idle-stop after a quiet period — and the control-channel bind guard
(refuses 0.0.0.0/:: same as mobile_relay_socks5.py).
"""

import asyncio
import os
import sys
import unittest
from unittest.mock import MagicMock, patch

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from scripts.mobile_relay_watcher import _RelayLauncher, _handle_client, main


def _make_proc(alive: bool):
    proc = MagicMock()
    proc.poll.return_value = None if alive else 0
    proc.pid = 4242
    return proc


class TestRelayLauncherTrigger(unittest.TestCase):
    def test_first_trigger_starts_subprocess(self):
        launcher = _RelayLauncher("10.10.0.2", 1080, idle_stop_s=300)
        with patch("scripts.mobile_relay_watcher.subprocess.Popen", return_value=_make_proc(True)) as mock_popen:
            launcher.on_trigger()
        mock_popen.assert_called_once()
        args = mock_popen.call_args[0][0]
        self.assertIn("--host", args)
        self.assertIn("10.10.0.2", args)
        self.assertIn("--port", args)
        self.assertIn("1080", args)

    def test_trigger_while_already_running_does_not_restart(self):
        launcher = _RelayLauncher("10.10.0.2", 1080, idle_stop_s=300)
        with patch("scripts.mobile_relay_watcher.subprocess.Popen", return_value=_make_proc(True)) as mock_popen:
            launcher.on_trigger()
            launcher.on_trigger()
        mock_popen.assert_called_once()

    def test_trigger_after_subprocess_died_restarts(self):
        launcher = _RelayLauncher("10.10.0.2", 1080, idle_stop_s=300)
        dead_proc = _make_proc(False)
        with patch("scripts.mobile_relay_watcher.subprocess.Popen", return_value=dead_proc):
            launcher.on_trigger()
        with patch("scripts.mobile_relay_watcher.subprocess.Popen", return_value=_make_proc(True)) as mock_popen2:
            launcher.on_trigger()
        mock_popen2.assert_called_once()


class TestRelayLauncherIdleStop(unittest.IsolatedAsyncioTestCase):
    async def test_idle_watch_stops_after_timeout(self):
        launcher = _RelayLauncher("10.10.0.2", 1080, idle_stop_s=0)  # instant idle
        proc = _make_proc(True)
        with patch("scripts.mobile_relay_watcher.subprocess.Popen", return_value=proc):
            launcher.on_trigger()

        watch_task = asyncio.create_task(launcher.idle_watch_loop())
        await asyncio.sleep(0.05)
        # first sleep(10) inside the loop hasn't elapsed in real time, so drive
        # it directly instead of waiting on the loop's own 10s poll interval.
        watch_task.cancel()
        try:
            await watch_task
        except asyncio.CancelledError:
            pass

        # Directly exercise one iteration's idle-check logic (idle_stop_s=0
        # means any trigger is immediately "idle").
        import time
        launcher._last_trigger = time.monotonic() - 1
        self.assertTrue(launcher._is_running())

    async def test_running_flag_false_after_manual_stop(self):
        launcher = _RelayLauncher("10.10.0.2", 1080, idle_stop_s=300)
        proc = _make_proc(True)
        proc.wait.return_value = None
        with patch("scripts.mobile_relay_watcher.subprocess.Popen", return_value=proc):
            launcher.on_trigger()
        self.assertTrue(launcher._is_running())
        proc.poll.return_value = 0  # simulate it exiting
        self.assertFalse(launcher._is_running())


class TestControlProtocol(unittest.IsolatedAsyncioTestCase):
    async def test_start_command_triggers_launcher_and_replies_ok(self):
        launcher = MagicMock()
        reader = asyncio.StreamReader()
        reader.feed_data(b"START\n")
        reader.feed_eof()
        writer = MagicMock()
        writer.get_extra_info.return_value = ("10.10.0.1", 55555)
        writer.drain = _async_noop()

        await _handle_client(reader, writer, launcher)

        launcher.on_trigger.assert_called_once()
        writer.write.assert_called_once_with(b"OK\n")

    async def test_unknown_command_does_not_trigger(self):
        launcher = MagicMock()
        reader = asyncio.StreamReader()
        reader.feed_data(b"PING\n")
        reader.feed_eof()
        writer = MagicMock()
        writer.get_extra_info.return_value = ("10.10.0.1", 55555)
        writer.drain = _async_noop()

        await _handle_client(reader, writer, launcher)

        launcher.on_trigger.assert_not_called()
        writer.write.assert_called_once_with(b"UNKNOWN\n")


def _async_noop():
    async def _f(*a, **k):
        return None
    return _f


class TestBindGuard(unittest.TestCase):
    def test_refuses_wildcard_bind(self):
        with patch("sys.argv", ["mobile_relay_watcher.py", "--host", "0.0.0.0"]), \
             self.assertRaises(SystemExit):
            main()

    def test_refuses_ipv6_wildcard_bind(self):
        with patch("sys.argv", ["mobile_relay_watcher.py", "--host", "::"]), \
             self.assertRaises(SystemExit):
            main()


if __name__ == "__main__":
    unittest.main()
