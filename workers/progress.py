"""
workers/progress.py — worker progress reports for the stream consumers.

While a scan_worker / fullscan worker holds a stream message it publishes
    worker:cur:{consumer} = {"msg_id", "alive_until"}
synchronously at every checkpoint.  The scheduler uses it to tell a long scan
(now <= alive_until) from a stalled one (now > alive_until) and from a worker that
dropped the message (msg_id differs).  No ATS module is touched: the checkpoints
are installed once per worker process at the transport layer.

Checkpoints
    requests.Session.send            every requests-based HTTP call (base.py helpers
                                     and the direct users)
    curl_cffi.requests.Session.request   adp / paradox / custom_career
    time.sleep                       a sleep of s seconds declares now + s + allowance
    begin()/touch()                  stream read and the set_progress phase markers

Deadline after a checkpoint:
    HTTP call in flight   now + its own timeout (connect + read); a timeout of None
                          (or a None read) falls back to the allowance
    between calls         now + allowance
    allowance             max(longest silent gap seen for this dc_key x
                          PROGRESS_GAP_FACTOR, PROGRESS_COLD_GAP_S)
The silent gap (time outside HTTP calls and sleeps) is measured at each checkpoint
and its high-water mark is kept in the progress:gap hash.  Progress reporting must
never break a scan, so every Redis error is swallowed.
"""

import json
import time
import threading
from typing import Optional

from config import (
    PROGRESS_GAP_FACTOR,
    PROGRESS_COLD_GAP_S,
    REDIS_PROGRESS_GAP_HASH,
    REDIS_WORKER_CUR_PREFIX,
)
from logger import get_logger

logger = get_logger(__name__)

_lock = threading.Lock()
_state: dict = {
    "r":        None,    # Redis client
    "consumer": None,    # worker:cur key suffix
    "msg_id":   None,    # None = no message held → checkpoints are no-ops
    "gap_key":  None,    # dc_key the gap high-water mark is kept under
    "gap_hw":   0.0,     # cached high-water mark for gap_key (s)
    "last_end": 0.0,     # when the last checkpoint ended (monotonic)
}
_installed = False
_orig_sleep = time.sleep


def bind(r, consumer: str) -> None:
    """Attach the worker's Redis client and consumer name (call once at startup)."""
    _state["r"] = r
    _state["consumer"] = consumer


def _allowance() -> float:
    return max(_state["gap_hw"] * PROGRESS_GAP_FACTOR, float(PROGRESS_COLD_GAP_S))


def _publish(deadline: float) -> None:
    r = _state["r"]
    if r is None or _state["msg_id"] is None:
        return
    try:
        ttl = int(max(deadline - time.time(), 0) + _allowance()) + 1
        r.set(
            f"{REDIS_WORKER_CUR_PREFIX}:{_state['consumer']}",
            json.dumps({"msg_id": _state["msg_id"], "alive_until": deadline}),
            ex=ttl,
        )
    except Exception as exc:
        logger.debug("progress: publish failed: %s", exc)


def begin(msg_id: str, gap_key: str) -> None:
    """Called right after XREADGROUP returns a message, before any work."""
    r = _state["r"]
    hw = 0.0
    if r is not None:
        try:
            raw = r.hget(REDIS_PROGRESS_GAP_HASH, gap_key)
            hw = float(raw) if raw else 0.0
        except Exception:
            hw = 0.0
    with _lock:
        _state.update(msg_id=msg_id, gap_key=gap_key, gap_hw=hw,
                      last_end=time.monotonic())
    _publish(time.time() + _allowance())


def end() -> None:
    """The message is finished (or abandoned): stop reporting.  Idempotent."""
    with _lock:
        held = _state["msg_id"] is not None
        _state["msg_id"] = None
    if held and _state["r"] is not None:
        try:
            _state["r"].delete(f"{REDIS_WORKER_CUR_PREFIX}:{_state['consumer']}")
        except Exception:
            pass


def read_cur(r, consumer: str) -> Optional[dict]:
    """The consumer's current progress report, or None (idle / no report)."""
    try:
        raw = r.get(f"{REDIS_WORKER_CUR_PREFIX}:{consumer}")
        cur = json.loads(raw) if raw else None
        return cur if isinstance(cur, dict) else None
    except Exception:
        return None


def is_stalled(r, consumer: str, now: Optional[float] = None) -> bool:
    """True when the consumer holds a message whose reported deadline has passed."""
    cur = read_cur(r, consumer)
    if not cur:
        return False
    return (now if now is not None else time.time()) > float(cur.get("alive_until", 0))


def terminate(r, consumer: str, host: str, pid: int, local_host: str) -> None:
    """Drop a stalled worker's report and kill it when it runs on this host."""
    import os
    import signal
    try:
        r.delete(f"{REDIS_WORKER_CUR_PREFIX}:{consumer}")
    except Exception:
        pass
    if host != local_host:
        logger.warning("progress: stalled worker %s is on another host — not killed", consumer)
        return
    try:
        os.kill(pid, getattr(signal, "SIGKILL", signal.SIGTERM))
        logger.warning("progress: killed stalled worker %s", consumer)
    except OSError as exc:
        logger.debug("progress: kill %s failed: %s", consumer, exc)


def _learn_gap(now_m: float) -> None:
    """Record the silent gap that just ended; raise the high-water mark if beaten."""
    gap = now_m - _state["last_end"]
    if gap > _state["gap_hw"]:
        _state["gap_hw"] = gap
        r = _state["r"]
        if r is not None and _state["gap_key"]:
            try:
                cur = r.hget(REDIS_PROGRESS_GAP_HASH, _state["gap_key"])
                if not cur or gap > float(cur):
                    r.hset(REDIS_PROGRESS_GAP_HASH, _state["gap_key"], round(gap, 3))
            except Exception:
                pass


def touch() -> None:
    """Phase marker / explicit checkpoint: a silent gap ends here, a new one starts."""
    if _state["msg_id"] is None:
        return
    now_m = time.monotonic()
    _learn_gap(now_m)
    _state["last_end"] = now_m
    _publish(time.time() + _allowance())


def _timeout_bound(timeout) -> Optional[float]:
    """Seconds an HTTP call may stay silent, from its own timeout; None = unbounded."""
    if timeout is None:
        return None
    if isinstance(timeout, (tuple, list)):
        if any(t is None for t in timeout):
            return None
        return float(sum(timeout))
    try:
        return float(timeout) * 2      # connect + read, each bounded by the same value
    except (TypeError, ValueError):
        return None


class _Span:
    """Context for a call that is silent by nature (HTTP request, sleep)."""

    def __init__(self, bound: Optional[float]):
        self.bound = bound

    def __enter__(self):
        if _state["msg_id"] is None:
            return self
        _learn_gap(time.monotonic())
        wait = self.bound if self.bound is not None else _allowance()
        _publish(time.time() + wait)
        return self

    def __exit__(self, *exc):
        if _state["msg_id"] is not None:
            _state["last_end"] = time.monotonic()
            _publish(time.time() + _allowance())
        return False


def _sleep(seconds):
    try:
        bound = float(seconds) + _allowance()
    except (TypeError, ValueError):
        bound = None
    with _Span(bound):
        _orig_sleep(seconds)


def install() -> None:
    """Install the transport checkpoints once per worker process (idempotent)."""
    global _installed
    if _installed:
        return
    _installed = True

    time.sleep = _sleep

    try:
        import requests

        _orig_send = requests.Session.send

        def _send(self, request, **kwargs):
            with _Span(_timeout_bound(kwargs.get("timeout"))):
                return _orig_send(self, request, **kwargs)

        requests.Session.send = _send
    except Exception as exc:
        logger.warning("progress: requests hook not installed: %s", exc)

    try:
        from curl_cffi import requests as cffi_requests

        _orig_request = cffi_requests.Session.request

        def _request(self, method, url, *args, **kwargs):
            with _Span(_timeout_bound(kwargs.get("timeout"))):
                return _orig_request(self, method, url, *args, **kwargs)

        cffi_requests.Session.request = _request
    except ImportError:
        pass
    except Exception as exc:
        logger.warning("progress: curl_cffi hook not installed: %s", exc)
