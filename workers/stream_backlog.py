"""
workers/stream_backlog.py — backlog signal and safe trimming for the scan streams.

The scheduler dispatches a due company by XADD to stream:adaptive / stream:fullscan
and then ZREMs it from the poll ZSET, so the backlog moves out of the ZSET into the
stream.  Workers read with id ">" (undelivered entries only), so "waiting for a
worker" is exactly the entries after the consumer group's last-delivered-id.

    stream_backlog(r, key, group, now)   -> (waiting_count, oldest_wait_s | None)
    trim_delivered(r, key, group)        -> entries deleted

Wait is measured from the entry ID (Redis time of the XADD = the moment the entry
became available to a worker), so scheduler downtime before dispatch never inflates
it.  Entries in the PEL are not counted as work in progress: XAUTOCLAIM hands them
to the scheduler's consumer and workers never re-read them (">"), so they are
orphans until dead-lettered.

trim_delivered replaces XADD MAXLEN: a count cap deletes the oldest entries whether
or not a worker took them, and the company is already gone from the ZSET.  Only
entries older than min(last-delivered-id, oldest pending id) are deleted.
"""

import time

from logger import get_logger

logger = get_logger(__name__)



def _s(v):
    return v.decode() if isinstance(v, bytes) else v


def _id_parts(entry_id: str):
    ms, _, seq = _s(entry_id).partition("-")
    return int(ms), int(seq or 0)


def _group_info(r, stream_key: str, group: str):
    """Return the XINFO GROUPS dict for group, or None when it does not exist."""
    for g in r.xinfo_groups(stream_key):
        if _s(g.get("name", b"")) == group:
            return g
    return None


def stream_backlog(r, stream_key: str, group: str, now_ts: float = None):
    """
    Return (waiting_count, oldest_wait_s) for entries no worker has taken.

    waiting_count  XINFO GROUPS lag (O(1)); when lag is unavailable (Redis returns
                   nil after some deletes) the exact count of the entries after
                   last-delivered-id, logged because it reads them all.
    oldest_wait_s  now - XADD time of the first entry after last-delivered-id;
                   None when nothing is waiting.
    Raises on a Redis error or a missing group so the caller decides what unknown
    means (the manager treats it as "no stream signal", not as zero backlog).
    """
    if now_ts is None:
        now_ts = time.time()
    g = _group_info(r, stream_key, group)
    if g is None:
        raise RuntimeError(f"consumer group {group!r} missing on {stream_key!r}")
    last_id = _s(g.get("last-delivered-id", "0-0"))

    first = r.xrange(stream_key, min=f"({last_id}", max="+", count=1)
    if not first:
        return 0, None
    oldest_wait_s = max(0.0, now_ts - _id_parts(first[0][0])[0] / 1000.0)

    lag = g.get("lag")
    if lag is not None:
        return max(int(lag), 1), oldest_wait_s
    waiting = len(r.xrange(stream_key, min=f"({last_id}", max="+"))
    logger.info("stream_backlog: lag unavailable on %r — counted %d entries by XRANGE",
                stream_key, waiting)
    return waiting, oldest_wait_s


def waiting_companies(r, stream_key: str, group: str) -> list:
    """
    Company names of the entries no worker has taken (same entries stream_backlog
    counts).  Reads them all, no cap; raises on a Redis error or a missing group.
    """
    g = _group_info(r, stream_key, group)
    if g is None:
        raise RuntimeError(f"consumer group {group!r} missing on {stream_key!r}")
    last_id = _s(g.get("last-delivered-id", "0-0"))
    entries = r.xrange(stream_key, min=f"({last_id}", max="+")
    return [_s(fields.get("company") or fields.get(b"company", b""))
            for _, fields in entries
            if fields.get("company") or fields.get(b"company")]


def trim_delivered(r, stream_key: str, group: str) -> int:
    """
    Delete entries every consumer is done with; return how many were deleted.

    Keeps everything at or after min(last-delivered-id, oldest pending id), so no
    waiting or pending entry is ever removed.  A pending entry holds the trim point
    back only while its owner is actively scanning it; claim_stale_work reaps every
    other pending entry within two ticks.
    """
    g = _group_info(r, stream_key, group)
    if g is None:
        return 0
    keep_from = _s(g.get("last-delivered-id", "0-0"))
    if keep_from == "0-0":
        return 0

    pending = r.xpending(stream_key, group)
    oldest_pending = pending.get("min") if isinstance(pending, dict) else None
    if oldest_pending:
        oldest_pending = _s(oldest_pending)
        if _id_parts(oldest_pending) < _id_parts(keep_from):
            keep_from = oldest_pending

    return int(r.xtrim(stream_key, minid=keep_from, approximate=False) or 0)
