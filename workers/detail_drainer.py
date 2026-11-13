"""
workers/detail_drainer.py — the detail admission gate (token bucket + drainer).

Design: docs/detail-pool-ceiling-th-design.md sections 3.7 and 9, step 7.

A pending_detail row with job_postings.pushed_at IS NULL is HELD: saved but not in
the Redis detail queue.  This process is the only writer of the queue for held
rows.  It owns one token bucket for the whole detail pool:

    tick     = s / n                 seconds per token (one job per tick)
    capacity = n x D / s             most tokens the bucket holds

n, s and D are written by the manager each cycle to BUCKET_PARAMS_KEY (step 8).
Tokens are built from elapsed time and kept in TOKENS_KEY, so a restart does not
reset the bucket.  Every wake the drainer releases min(whole tokens, held rows),
oldest first by (created_at, id): it sets pushed_at FIRST and commits, THEN pushes
with a fresh queued_at stamp.  A crash between the two leaves a row with pushed_at
set that rebuild_detail_queue re-pushes; a job is never lost, and a duplicate
queue entry is harmless (complete_pending_detail only matches pending_detail).

It also publishes held_count / held_oldest_age_s (HELD_KEY) for the manager's
lender gate and shortfall alert, and a heartbeat for the watchdog.

With no params key it releases nothing (held rows keep waiting; the held figures
and the heartbeat still update so the stall is visible).
"""

import json
import math
import os
import socket
import time
from typing import Optional

from config import (
    DRAIN_HEARTBEAT_TTL_S,
    DRAIN_MIN_WAKE_S,
    REDIS_DETAIL_ADAPTIVE,
    REDIS_DETAIL_FULLSCAN,
)
from db.connection import get_conn
from logger import get_logger, init_logging
from workers.detail_payload import build_detail_push
from workers.redis_client import get_redis

logger = get_logger(__name__)

BUCKET_PARAMS_KEY = "manager:detail_bucket"     # manager -> drainer: {"n", "s", "d"}
TOKENS_KEY        = "drainer:detail:tokens"     # {"tokens", "ts"}
HELD_KEY          = "drainer:detail:held"       # {"count", "oldest_age_s", "ts"}
HEARTBEAT_PREFIX  = "worker:alive:detail_drainer"
_HOSTNAME         = socket.gethostname()
_HELD_TTL_S       = 3600

_HELD_FILTER = "status = 'pending_detail' AND pushed_at IS NULL"


# ── pure helpers ─────────────────────────────────────────────────────────────

def tick_s(n: float, s: float) -> float:
    """Seconds per token: the pool serves n jobs at s seconds each."""
    return s / n


def capacity(n: float, s: float, d: float) -> float:
    """Most tokens the bucket holds (n x D / s), at least one so a burst can start."""
    return max(1.0, n * d / s)


def wake_k(tick: float) -> int:
    """Tokens per wake: k = ceil(DRAIN_MIN_WAKE_S / tick), at least 1."""
    return max(1, math.ceil(DRAIN_MIN_WAKE_S / tick))


def refill(tokens: float, elapsed_s: float, tick: float, cap: float) -> float:
    """Tokens after elapsed_s of refill at one per tick, capped at capacity.

    Tokens already above a reduced capacity are kept, never cut (a lower ceiling
    only stops further refill); they are spent by later releases.
    """
    return max(tokens, min(cap, tokens + max(0.0, elapsed_s) / tick))


# ── Redis state ──────────────────────────────────────────────────────────────

def read_params(r) -> Optional[dict]:
    """Manager-published {"n", "s", "d"} as floats, or None when absent/unusable."""
    raw = r.get(BUCKET_PARAMS_KEY)
    if not raw:
        return None
    try:
        p = json.loads(raw)
        n, s, d = float(p["n"]), float(p["s"]), float(p["d"])
    except (ValueError, TypeError, KeyError):
        return None
    if not (n > 0 and s > 0 and d > 0 and all(map(math.isfinite, (n, s, d)))):
        return None
    return {"n": n, "s": s, "d": d}


def _queued_and_inflight(r) -> int:
    """Jobs in the detail queues plus the workers' in-progress lists."""
    total = r.llen(REDIS_DETAIL_ADAPTIVE) + r.llen(REDIS_DETAIL_FULLSCAN)
    for queue in (REDIS_DETAIL_ADAPTIVE, REDIS_DETAIL_FULLSCAN):
        cursor = 0
        while True:
            cursor, keys = r.scan(cursor, match=f"{queue}:inflight:*", count=100)
            total += sum(r.llen(k) for k in keys)
            if cursor == 0:
                break
    return total


def load_tokens(r, cap: float, now: float) -> float:
    """
    Stored token count refilled up to now is done by the caller; this returns the
    stored count, or on a first start / Redis wipe max(0, capacity - jobs already
    queued or in flight) so the pool is not flooded on top of an existing queue.
    """
    raw = r.get(TOKENS_KEY)
    if raw:
        try:
            return float(json.loads(raw)["tokens"])
        except (ValueError, TypeError, KeyError):
            pass
    return max(0.0, cap - _queued_and_inflight(r))


def _stored_ts(r, now: float) -> float:
    raw = r.get(TOKENS_KEY)
    try:
        return float(json.loads(raw)["ts"]) if raw else now
    except (ValueError, TypeError, KeyError):
        return now


# ── database ─────────────────────────────────────────────────────────────────

def held_figures(conn) -> tuple:
    """
    (held row count, age in seconds of the oldest held row, id of that row) from
    Postgres.  The id lets the watchdog tell a draining backlog (the oldest row
    changes) from a stuck drainer (it does not).
    """
    row = conn.execute(f"""
        SELECT COUNT(*) AS c,
               EXTRACT(EPOCH FROM (NOW() - MIN(created_at))) AS age,
               (SELECT id FROM job_postings WHERE {_HELD_FILTER}
                ORDER BY created_at, id LIMIT 1) AS oldest_id
        FROM job_postings WHERE {_HELD_FILTER}
    """).fetchone()
    count = int(row["c"] or 0)
    if not count:
        return 0, 0.0, None
    return count, (float(row["age"]) if row["age"] is not None else 0.0), row["oldest_id"]


def release_held(r, conn, limit: int) -> int:
    """
    Release up to `limit` held rows, oldest first.  Marks pushed_at and commits
    BEFORE any push.  Returns the number of jobs pushed (rows that cannot be
    rebuilt are marked released but not pushed and cost no token).
    """
    if limit <= 0:
        return 0
    rows = conn.execute(f"""
        SELECT id, company, job_url, job_id, ats_platform, detail_payload, found_by
        FROM job_postings WHERE {_HELD_FILTER}
        ORDER BY created_at, id LIMIT ?
    """, (limit,)).fetchall()
    if not rows:
        return 0

    claimed = {c["id"] for c in conn.execute(f"""
        UPDATE job_postings SET pushed_at = NOW()
        WHERE id = ANY(?) AND {_HELD_FILTER}
        RETURNING id
    """, ([row["id"] for row in rows],)).fetchall()}
    conn.commit()

    pushed = 0
    unpushed = [row["id"] for row in rows if row["id"] in claimed]   # still to push, in order
    for row in rows:
        if row["id"] not in claimed:
            continue            # completed or released by someone else meanwhile
        push = build_detail_push(row)
        if push is None:
            logger.info("drainer: %s %s released but not pushed (no rebuildable payload)",
                        row["company"], row["job_id"])
            unpushed.remove(row["id"])
            continue
        queue, payload = push
        try:
            r.lpush(queue, payload)
        except Exception:
            # The claim was committed before the push: give this row and every later claimed
            # row back to the held pool so a Redis failure does not lose them.
            try:
                conn.execute("UPDATE job_postings SET pushed_at = NULL WHERE id = ANY(?)",
                             (unpushed,))
                conn.commit()
            except Exception as reset_exc:
                logger.error("drainer: could not re-hold %d rows after a failed push: %s",
                             len(unpushed), reset_exc)
            raise
        unpushed.remove(row["id"])
        pushed += 1
        logger.info("drainer: released %s %s -> %s", row["company"], row["job_id"], queue)
    return pushed


# ── one wake ─────────────────────────────────────────────────────────────────

def drain_once(r, now: Optional[float] = None) -> dict:
    """
    One wake: refill the bucket, release what the tokens allow, publish the held
    figures.  Returns {released, held, tokens, wake_s} (wake_s None without params).
    """
    now = time.time() if now is None else now
    params = read_params(r)
    conn = get_conn()
    try:
        held, age, oldest_id = held_figures(conn)
        r.set(HELD_KEY, json.dumps({"count": held, "oldest_age_s": age,
                                    "oldest_id": oldest_id, "ts": now}),
              ex=_HELD_TTL_S)
        if params is None:
            return {"released": 0, "held": held, "tokens": None, "wake_s": None}

        tick = tick_s(params["n"], params["s"])
        cap = capacity(params["n"], params["s"], params["d"])
        k = wake_k(tick)
        stored = load_tokens(r, cap, now)
        tokens = refill(stored, now - _stored_ts(r, now), tick, cap)

        released = release_held(r, conn, min(int(tokens), held))
        tokens -= released
        r.set(TOKENS_KEY, json.dumps({"tokens": tokens, "ts": now}))
        return {"released": released, "held": held - released, "tokens": tokens,
                "wake_s": k * tick}
    finally:
        conn.close()


def _heartbeat(r, cycles: int, wake_s: float) -> None:
    try:
        r.set(f"{HEARTBEAT_PREFIX}:{_HOSTNAME}:{os.getpid()}",
              json.dumps({"pid": os.getpid(), "ts": time.time(), "cycles": cycles}),
              ex=int(max(DRAIN_HEARTBEAT_TTL_S, 3 * wake_s)))
    except Exception:
        pass


def run_drainer() -> None:
    """Main loop: wake every k x tick (DRAIN_MIN_WAKE_S when no params yet)."""
    r = get_redis()
    logger.info("drainer: starting (min wake %.2fs)", DRAIN_MIN_WAKE_S)
    cycles = 0
    wake_s = DRAIN_MIN_WAKE_S
    while True:
        try:
            result = drain_once(r)
            wake_s = result["wake_s"] or DRAIN_MIN_WAKE_S
            cycles += 1
            if result["released"]:
                logger.info("drainer: released %d, held %d, tokens %.1f",
                            result["released"], result["held"], result["tokens"])
            elif result["wake_s"] is None and result["held"]:
                logger.warning("drainer: no bucket params from manager, %d jobs held",
                               result["held"])
        except Exception as exc:
            logger.error("drainer: wake failed: %s", exc, exc_info=True)
            wake_s = DRAIN_MIN_WAKE_S
        _heartbeat(r, cycles, wake_s)
        # A long wake (slow pool) must not leave the heartbeat older than the
        # watchdog's stale threshold: sleep in slices of at most a third of the TTL.
        # Tokens refill by elapsed time, so extra wakes release nothing early.
        time.sleep(min(wake_s, DRAIN_HEARTBEAT_TTL_S / 3))


if __name__ == "__main__":
    init_logging("detail_drainer")
    run_drainer()
