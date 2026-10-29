"""
workers/heartbeat.py — Background heartbeat writer for long-running workers.

Problem solved
──────────────
Workers (scan, detail, fullscan) do heavy work inside their main loop.
Writing the heartbeat at the top of the loop creates a gap equal to the
job duration — a 60s Workday scan means the heartbeat key is absent for
~30s mid-scan, which the watchdog could misread as a dead worker.

Solution
────────
A daemon thread writes the heartbeat key every interval_s seconds,
completely independent of what the main thread is doing.  Because the
thread is daemon=True, it is hard-tied to the process:

    Process ALIVE  → thread running → key refreshed every interval_s
    Process EXITS  → OS kills all daemon threads → key TTL runs out
                     → watchdog detects dead worker correctly

This eliminates false positives (alive worker looks dead mid-scan) AND
false negatives (dead worker appears alive via a stale key) at the same
time.

Usage
─────
    _hw = {"count": 0}              # mutable dict — both threads can read/write
    hb  = Heartbeat(r, "scan_worker", lambda: _hw["count"]).start()

    while True:
        ... do work ...
        _hw["count"] += 1           # GIL-safe int increment

    hb.stop()                       # clean shutdown — unblocks wait() immediately
"""

import json
import os
import socket
import threading
import time

from logger import get_logger

logger = get_logger(__name__)

_HOSTNAME = socket.gethostname()   # resolved once at import time; stable for process lifetime


class BusyMeter:
    """
    Cumulative busy-time odometer for one worker, read by the heartbeat thread.

    The main loop brackets each job with begin() / end(); the heartbeat thread
    calls snapshot().  total_ms and since are kept together under one lock so a
    snapshot can never mix the two (a job ending between two separate reads
    would otherwise be counted twice or not at all).

    total_ms  grows only when a job ends and is never reset.
    since     is the wall-clock start of the job in progress, None when idle —
              it lets the manager count a long job (a multi-hour scan) while it
              is still running instead of only after it ends.
    Both use time.time(), the same clock as the heartbeat's ts.
    """

    def __init__(self) -> None:
        self._lock     = threading.Lock()
        self._total_ms = 0.0
        self._since    = None

    def begin(self) -> None:
        with self._lock:
            self._since = time.time()

    def end(self) -> None:
        with self._lock:
            if self._since is not None:
                self._total_ms += max(time.time() - self._since, 0.0) * 1000.0
                self._since = None

    def snapshot(self) -> tuple:
        """Return (total_ms, since_or_None) as one consistent pair."""
        with self._lock:
            return int(self._total_ms), self._since


class Heartbeat:
    """
    Daemon thread that writes worker:alive:{worker_type}:{hostname}:{pid}
    every interval_s seconds while the worker process is alive.

    TTL is set to 3x interval_s so two consecutive missed writes (Redis
    blip, GIL stall) are tolerated before the key expires.  The watchdog's
    dead threshold should be set to at least 4x interval_s so there is
    always a gap between key expiry and alert.

    Args:
        r:            Redis client (same connection used by the worker)
        worker_type:  Key suffix — "scan_worker", "detail_worker",
                      "fullscan_worker"
        get_count:    Callable that returns the current processed-job count.
                      Called on every write for the heartbeat payload.
                      Use a lambda over a mutable container so the thread
                      always sees the latest value, e.g.
                          _hw = {"count": 0}
                          lambda: _hw["count"]
        interval_s:   Seconds between writes (default 10).
                      fullscan_worker should use 60 — scans take minutes.
        busy_meter:   Optional BusyMeter.  When given, every write carries
                      busy_ms (cumulative) and busy_since (job start or null)
                      so the manager can derive utilization from heartbeats.
    """

    def __init__(
        self,
        r,
        worker_type: str,
        get_count,
        *,
        interval_s: int = 10,
        busy_meter=None,
    ) -> None:
        if interval_s <= 0:
            raise ValueError(
                f"Heartbeat interval_s must be > 0, got {interval_s!r}. "
                "Zero or negative values cause a busy-spin that hammers Redis."
            )
        self._r           = r
        self._worker_type = worker_type
        self._get_count   = get_count
        self._busy_meter  = busy_meter
        self._interval_s  = interval_s
        self._ttl_s       = interval_s * 3   # 3× safety margin
        self._stop        = threading.Event()
        self._thread      = threading.Thread(
            target  = self._loop,
            name    = f"{worker_type}_heartbeat",
            daemon  = True,   # dies automatically when the process exits —
                              # no ghost heartbeats from a dead worker
        )

    # ── public API ────────────────────────────────────────────────────────────

    def start(self) -> "Heartbeat":
        """Start the heartbeat thread.  Returns self for chaining."""
        self._write()           # immediate first write — watchdog sees it right away
        self._thread.start()
        logger.debug(
            "heartbeat: started for %r (interval=%ds ttl=%ds)",
            self._worker_type, self._interval_s, self._ttl_s,
        )
        return self

    def stop(self) -> None:
        """Signal the thread to stop, wait for it to exit, then delete the key.

        The join ensures the background thread cannot recreate the key after we
        delete it (possible if stop() signals the event while _write() is about
        to run on the next loop iteration).
        """
        self._stop.set()        # unblocks _stop.wait() on the next sleep boundary
        if self._thread.ident is not None:                 # only join if started
            self._thread.join(timeout=self._interval_s + 2)
            if self._thread.is_alive():
                logger.warning(
                    "heartbeat: thread for %r did not exit within timeout — "
                    "key will expire via TTL",
                    self._worker_type,
                )
                # Thread still running — skip explicit delete so the thread can't
                # recreate the key after we remove it.  TTL handles cleanup.
                return
        try:
            self._r.delete(f"worker:alive:{self._worker_type}:{_HOSTNAME}:{os.getpid()}")
        except Exception as _del_err:
            logger.debug(
                "heartbeat: cleanup delete failed for %s:%s:%s — TTL will expire: %s",
                self._worker_type, _HOSTNAME, os.getpid(), _del_err,
            )

    # ── internals ─────────────────────────────────────────────────────────────

    def _write(self) -> None:
        """Write one heartbeat key to Redis.  Swallows all exceptions."""
        try:
            payload = {
                "pid":       os.getpid(),
                "ts":        time.time(),
                "processed": self._get_count(),
            }
            if self._busy_meter is not None:
                payload["busy_ms"], payload["busy_since"] = self._busy_meter.snapshot()
            self._r.set(
                f"worker:alive:{self._worker_type}:{_HOSTNAME}:{os.getpid()}",
                json.dumps(payload),
                ex=self._ttl_s,
            )
        except Exception as exc:
            # Redis blip — keep going; TTL gives 3× the interval as buffer
            logger.debug(
                "heartbeat: write failed for %r: %s", self._worker_type, exc
            )

    def _loop(self) -> None:
        """
        Main thread body.

        Uses Event.wait(timeout) instead of time.sleep() so stop() unblocks
        this immediately rather than waiting up to interval_s seconds.
        """
        while not self._stop.wait(self._interval_s):
            self._write()
        logger.debug("heartbeat: stopped for %r", self._worker_type)
