"""
scripts/mobile_relay_drain_worker.py — third-tier IP-reputation-block fallback
(docs/discovery-pipeline-hardening.md Part 3).

When scripts/discover_h1b_ats.py's Phase 3 probe comes back block-like (403/429/503),
it defers Brave/Phase 4 and leaves careers_url_last_status persisted instead of
guessing further from the OCI VM's IP. The post-cascade push in discover_h1b_ats.py
then lands that company in MOBILE_RELAY_QUEUE. This worker drains that queue and
re-attempts the identical Phase 3-5 fetch run, but through a curl_cffi session
proxied over a WireGuard tunnel to a SOCKS5 relay on the user's home PC
(scripts/mobile_relay_socks5.py) — a different egress IP the target site hasn't
seen fail yet.

Single instance only (worker_control.MOBILE_RELAY_WORKERS) — this is a narrow
fallback path, not a primary throughput pool. workers/manager.py starts it (0→1)
only when the tunnel is reachable and MOBILE_RELAY_QUEUE is non-empty, and stops
it (1→0) as soon as either condition is no longer true.

Usage:
  python -m scripts.mobile_relay_drain_worker
  python -m scripts.mobile_relay_drain_worker --once
"""

import json
import os
import sys
import time

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from config import (
    MOBILE_RELAY_DLQ,
    MOBILE_RELAY_INFLIGHT,
    MOBILE_RELAY_MAX_RETRIES,
    MOBILE_RELAY_PROXY_HOST,
    MOBILE_RELAY_PROXY_PORT,
    MOBILE_RELAY_QUEUE,
    REDIS_DB_MAINTENANCE,
)
from db.connection import get_conn
from db.external_api_health import record_external_request
from logger import get_logger, init_logging
from workers.heartbeat import Heartbeat
from workers.redis_client import get_redis

log = get_logger(__name__)

# Lua: atomically pop highest-score member from KEYS[1] (ZSET queue)
# and add it to KEYS[2] (inflight ZSET) with the same score.
# Returns {member, score} or {} when the queue is empty.
# (identical to workers/discover_h1b_ats_worker.py's _POP_ZSET_TO_INFLIGHT_LUA)
_POP_ZSET_TO_INFLIGHT_LUA = """
local res = redis.call('ZPOPMAX', KEYS[1], 1)
if #res == 0 then return {} end
redis.call('ZADD', KEYS[2], tonumber(res[2]), res[1])
return {res[1], res[2]}
"""

_RETRY_KEY_PREFIX = "mobile_relay:retry:"
_RETRY_TTL_S      = 86400 * 7  # 7 days — mirrors discovery worker's retry TTL


# ─────────────────────────────────────────────────────────────────────────────
# Lazy imports — heavy dependencies loaded once on first use
# ─────────────────────────────────────────────────────────────────────────────

_discover_ats = None


def _get_discover_ats():
    global _discover_ats
    if _discover_ats is None:
        try:
            import scripts.discover_h1b_ats as m
            _discover_ats = m
        except Exception as e:
            raise RuntimeError(f"discover_h1b_ats import failed: {e}") from e
    return _discover_ats


# ─────────────────────────────────────────────────────────────────────────────
# Maintenance window
# ─────────────────────────────────────────────────────────────────────────────

def _is_maintenance(r) -> bool:
    try:
        return bool(r.exists(REDIS_DB_MAINTENANCE))
    except Exception as exc:
        log.warning("Redis maintenance check failed (%s) — assuming not in maintenance", exc)
        return False


# ─────────────────────────────────────────────────────────────────────────────
# Retry tracking
# ─────────────────────────────────────────────────────────────────────────────

def _retry_key(fein: str) -> str:
    return f"{_RETRY_KEY_PREFIX}{fein}"


def _get_retry_count(r, fein: str) -> int:
    return int(r.get(_retry_key(fein)) or 0)


def _incr_retry(r, fein: str) -> int:
    key = _retry_key(fein)
    count = r.incr(key)
    r.expire(key, _RETRY_TTL_S)
    return count


def _clear_retry(r, fein: str) -> None:
    r.delete(_retry_key(fein))


def _move_to_dlq(r, fein: str, error_reason: str, retry_count: int) -> None:
    payload = json.dumps({
        "fein":         fein,
        "error_reason": error_reason,
        "retry_count":  retry_count,
        "failed_at":    time.time(),
    })
    r.lpush(MOBILE_RELAY_DLQ, payload)
    log.error("DLQ: fein=%s reason=%s retries=%d", fein, error_reason, retry_count)


# ─────────────────────────────────────────────────────────────────────────────
# DB helpers
# ─────────────────────────────────────────────────────────────────────────────

def _load_company(conn, fein: str) -> "dict | None":
    row = conn.execute("""
        SELECT
            f.employer_fein,
            f.assigned_domain,
            f.public_domain,
            f.careers_url,
            f.careers_source,
            e.employer_name,
            COALESCE(u.petition_count, 0) AS petition_count
        FROM fein_domain_map f
        JOIN dol_h1b_employers e USING (employer_fein)
        LEFT JOIN uscis_petition_counts u ON u.employer_fein = f.employer_fein
        WHERE f.employer_fein = %s
    """, (fein,)).fetchone()
    if not row:
        return None
    return dict(row)


def _clear_careers_url_last_status(conn, fein: str) -> None:
    """Clear the block-like status flag once this fein has run (successfully or not)
    through the relay — leaving it set would keep re-deferring Brave on the direct
    (OCI) path forever even after the relay has already had its shot."""
    conn.execute("""
        UPDATE fein_domain_map
        SET careers_url_last_status = NULL, updated_at = NOW()
        WHERE employer_fein = %s
    """, (fein,))


# ─────────────────────────────────────────────────────────────────────────────
# Per-company processing
# ─────────────────────────────────────────────────────────────────────────────

def _process_relay_item(fein: str, petition_count: int) -> bool:
    """
    Re-run the Phase 3-5 fetch for one company through the mobile relay session.
    Returns True on success (or permanent skip), False on transient error (retry).
    """
    conn = None
    t_start = time.time()
    try:
        conn = get_conn()
        m = _get_discover_ats()
        company = _load_company(conn, fein)
        if not company:
            log.warning("fein=%s not found in fein_domain_map — skipping", fein)
            return True

        employer_name = company["employer_name"]
        probe_domain  = company["public_domain"] or company["assigned_domain"]
        if not probe_domain:
            log.warning("fein=%s has no domain — skipping", fein)
            return True

        log.info("relay discovery fein=%s domain=%s name=%r", fein, probe_domain, employer_name)

        emp = {
            "employer_fein":   fein,
            "employer_name":   employer_name,
            "assigned_domain": probe_domain,
            "total_approvals": petition_count,
        }

        from jobs.http_safe import make_relay_curl_session
        relay_session = make_relay_curl_session(MOBILE_RELAY_PROXY_HOST, MOBILE_RELAY_PROXY_PORT)
        try:
            result = m.process_employer(
                emp, conn, dry_run=False, force=True,
                prefetched=None, skip_brave=False,
                known_careers_url=None, known_careers_source=None,
                skip_phase6=True,  # Phase 6 doesn't route through `session` — no relay benefit, skip it
                session=relay_session,
            )
        finally:
            relay_session.close()

        if not isinstance(result, dict):
            log.error("fein=%s: process_employer returned %s — treating as failure",
                      fein, type(result).__name__)
            record_external_request("mobile_relay", 0, int((time.time() - t_start) * 1000),
                                     error_kind="bad_result")
            return False

        det_platform = result.get("detected_platform")
        res_careers  = result.get("careers_url")
        duration_ms  = int((time.time() - t_start) * 1000)

        record_external_request("mobile_relay", 200 if res_careers else 404, duration_ms)

        _clear_careers_url_last_status(conn, fein)
        conn.commit()

        log.info("fein=%s relay done: careers=%s platform=%s slug=%s",
                 fein, res_careers, det_platform, result.get("detected_slug"))
        return True

    except Exception as exc:
        log.error("unexpected error in relay discovery fein=%s: %s", fein, exc, exc_info=True)
        try:
            record_external_request("mobile_relay", 0, int((time.time() - t_start) * 1000),
                                     error_kind=type(exc).__name__)
        except Exception:
            pass
        if conn:
            try:
                conn.rollback()
            except Exception:
                pass
        return False
    finally:
        if conn:
            conn.close()


# ─────────────────────────────────────────────────────────────────────────────
# Inflight crash recovery
# ─────────────────────────────────────────────────────────────────────────────

def _reclaim_inflight(r, inflight_key: str) -> None:
    """Re-queue any FEINs left in this instance's inflight ZSET from a prior crash."""
    items = r.zrange(inflight_key, 0, -1, withscores=True)
    if not items:
        return
    log.warning("reclaiming %d inflight FEINs from prior run (key=%s)", len(items), inflight_key)
    for raw_member, score in items:
        r.zadd(MOBILE_RELAY_QUEUE, {raw_member: int(score)}, gt=True)
        r.zrem(inflight_key, raw_member)
        log.info("reclaimed inflight member=%s score=%d", raw_member, int(score))


# ─────────────────────────────────────────────────────────────────────────────
# Main loop
# ─────────────────────────────────────────────────────────────────────────────

def run_worker(once: bool = False) -> None:
    r = get_redis()
    processed = {"n": 0}
    _instance = os.environ.get("WORKER_INSTANCE", "")
    _hb_name  = f"mobile_relay_drain_worker@{_instance}" if _instance else "mobile_relay_drain_worker"
    hb = Heartbeat(r, _hb_name, lambda: processed["n"], interval_s=30).start()

    # Single-instance worker (worker_control.MOBILE_RELAY_WORKERS has exactly one
    # unit) but keep the per-instance key convention for consistency/future-proofing.
    _inflight_key = f"{MOBILE_RELAY_INFLIGHT}:{_instance}" if _instance else MOBILE_RELAY_INFLIGHT

    log.info("mobile-relay-drain-worker started (instance=%r inflight=%s)", _instance, _inflight_key)
    _reclaim_inflight(r, _inflight_key)
    _pop_to_inflight = r.register_script(_POP_ZSET_TO_INFLIGHT_LUA)

    _MAINTENANCE_MAX_S = 4 * 3600

    try:
        while True:
            _maint_start = None
            while _is_maintenance(r):
                if _maint_start is None:
                    _maint_start = time.monotonic()
                elapsed = time.monotonic() - _maint_start
                if elapsed > _MAINTENANCE_MAX_S:
                    log.error("Maintenance window exceeded %dh — exiting to allow restart",
                              _MAINTENANCE_MAX_S // 3600)
                    sys.exit(1)
                log.info("Maintenance window active — pausing 30s (%.0fm elapsed)", elapsed / 60)
                time.sleep(30)

            _pop_result = _pop_to_inflight(keys=[MOBILE_RELAY_QUEUE, _inflight_key])
            if not _pop_result:
                if r.zcard(MOBILE_RELAY_QUEUE) == 0:
                    log.info("Mobile relay queue empty — exiting")
                    break
                # Another worker instance raced us — brief sleep, avoid tight polling.
                time.sleep(1)
                continue

            raw_member     = _pop_result[0]
            petition_count = int(float(_pop_result[1]))

            try:
                data = json.loads(raw_member)
                fein = data["fein"]
            except (json.JSONDecodeError, KeyError, TypeError):
                bare = raw_member.strip() if isinstance(raw_member, str) else raw_member.decode(errors="replace").strip()
                if bare.isdigit():
                    fein = bare
                    log.debug("Legacy bare-FEIN member %r", bare)
                else:
                    _dlq_payload = json.dumps({
                        "fein": "MALFORMED", "error_reason": "malformed_member",
                        "raw": repr(raw_member), "failed_at": time.time(),
                    })
                    log.error("Malformed relay queue member %r — sending to DLQ", raw_member)
                    r.lpush(MOBILE_RELAY_DLQ, _dlq_payload)
                    r.zrem(_inflight_key, raw_member)
                    continue

            retry_count = _get_retry_count(r, fein)
            if retry_count >= MOBILE_RELAY_MAX_RETRIES:
                _move_to_dlq(r, fein, "max_retries_exceeded", retry_count)
                _clear_retry(r, fein)
                r.zrem(_inflight_key, raw_member)
                continue

            success = _process_relay_item(fein, petition_count)
            processed["n"] += 1

            if not success:
                count = _incr_retry(r, fein)
                if count >= MOBILE_RELAY_MAX_RETRIES:
                    _move_to_dlq(r, fein, "processing_error", count)
                    _clear_retry(r, fein)
                else:
                    # Re-queue immediately at the same priority — unlike the discovery
                    # worker's exponential backoff, a relay failure is far more likely
                    # to be "tunnel dropped mid-run" (manager stops this worker as soon
                    # as the tunnel is unreachable, so a retry basically never runs hot).
                    r.zadd(MOBILE_RELAY_QUEUE, {raw_member: petition_count}, gt=True)
                    log.warning("fein=%s retry %d/%d — requeued", fein, count, MOBILE_RELAY_MAX_RETRIES)
            else:
                _clear_retry(r, fein)

            r.zrem(_inflight_key, raw_member)

            if once:
                break

    finally:
        hb.stop()

    log.info("mobile-relay-drain-worker stopped — processed %d companies", processed["n"])


if __name__ == "__main__":
    init_logging("mobile_relay_drain_worker")
    once = "--once" in sys.argv
    run_worker(once=once)
