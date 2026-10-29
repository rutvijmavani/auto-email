"""
workers/queue_stamp.py — queue-wait stamp for detail queue payloads.

Two timestamps travel with a detail payload:

    enqueued_at   build / discovery time (set by _build_detail_payload and
                  persisted in job_postings.detail_payload).  Never changes.
    queued_at     time the job was last PUSHED to a detail queue by a producer
                  (scan_worker, fullscan, rebuild).  Never persisted to the DB,
                  so a rebuild always adds a fresh one.

Requeue / recovery / retry paths in detail_worker move the raw payload string
back to the consuming end WITHOUT rewriting it, so they keep their original
queued_at (honest total wait, tail of the list stays the oldest).

Neither stamp drives a scaling decision; the wait derived here is for display,
alerts and the legacy delay input only.
"""

import json
from datetime import datetime, timezone


def stamp_queued_at(payload: dict) -> dict:
    """Return a copy of payload with queued_at = now (UTC isoformat)."""
    return {**payload, "queued_at": datetime.now(timezone.utc).isoformat()}


def count_arrival(r, queue_key: str, source: str = "organic") -> None:
    """
    INCR signal:arrivals:{queue_key}:{source} after a producer LPUSH.

    source is "organic" (scan_worker, fullscan) or "rebuild" (rebuild.py), so
    the manager's inflow rate counts real arrivals and never a rebuild burst.
    The counter is a signal only: a failure here must never break a push.
    """
    try:
        r.incr(f"signal:arrivals:{queue_key}:{source}")
    except Exception:
        pass


def payload_wait_s(raw, now_ts: float) -> "float | None":
    """
    Seconds the payload in raw (JSON str/bytes) has waited, or None if unknown.

    Uses queued_at.  Payloads pushed before queued_at existed fall back to
    enqueued_at (transition only; they drain out of the queue).  A missing or
    unparseable stamp returns None so callers can tell "unknown" from "0".
    """
    if not raw:
        return None
    try:
        payload = json.loads(raw)
        stamp = payload.get("queued_at") or payload.get("enqueued_at")
        if not stamp:
            return None
        return max(0.0, now_ts - datetime.fromisoformat(stamp).timestamp())
    except Exception:
        return None
