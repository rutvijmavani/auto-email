"""
scripts/mobile_relay_drain_worker.py — third-tier IP-reputation-block fallback
(docs/discovery-pipeline-hardening.md Part 3).

scripts/discover_h1b_ats.py pushes a company here, at the very end of its full
Phase 1/3/4/5/6/7 cascade, whenever the CURRENT fein_domain_map row still shows
public_domain and/or careers_url unresolved (see _push_mobile_relay there). This
worker drains that queue and re-attempts whichever of the two is still missing,
through a curl_cffi session proxied over a WireGuard tunnel to a SOCKS5 relay on
the user's home PC (scripts/mobile_relay_socks5.py) — a different egress IP the
target site hasn't seen fail yet.

Phase 4 (Brave) is deliberately skipped on every attempt except the last one
right before an item is permanently dropped — it's a search-API call, not a
target-site fetch, so a different egress IP doesn't help it; quota is spent only
at that last-resort point. Phase 6 (career_page scan) runs on every attempt like
Phase 3, since it is a target-site fetch the relay IP can help with.

Single instance only (worker_control.MOBILE_RELAY_WORKERS) — this is a narrow
fallback path, not a primary throughput pool. workers/manager.py starts it (0→1)
only when the tunnel is reachable and MOBILE_RELAY_QUEUE+MOBILE_RELAY_INFLIGHT
combined depth is non-zero, and stops it (1→0) as soon as either condition is no
longer true.

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
    MOBILE_RELAY_GUARD_PREFIX,
    MOBILE_RELAY_INFLIGHT,
    MOBILE_RELAY_MAX_ATTEMPTS,
    MOBILE_RELAY_PROBE_TIMEOUT_S,
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

# Lua: atomically pop the LOWEST-score member (oldest — FIFO) from KEYS[1] (ZSET
# queue) and add it to KEYS[2] (inflight ZSET) with the same score.
# Returns {member, score} or {} when the queue is empty.
_POP_ZSET_TO_INFLIGHT_LUA = """
local res = redis.call('ZPOPMIN', KEYS[1], 1)
if #res == 0 then return {} end
redis.call('ZADD', KEYS[2], tonumber(res[2]), res[1])
return {res[1], res[2]}
"""


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
# Reachability
# ─────────────────────────────────────────────────────────────────────────────

def _probe_mobile_relay_reachable(timeout_s: float = MOBILE_RELAY_PROBE_TIMEOUT_S) -> bool:
    """TCP-connect probe to the home PC's WireGuard-tunneled SOCKS5 relay — same
    check workers/manager.py::_probe_mobile_relay_reachable uses to decide whether
    to autoscale this pool up/down. Duplicated here (not imported from manager.py,
    to avoid pulling in the whole manager module) so the worker itself can bail out
    of a mid-run item the instant the tunnel drops, rather than burning a full
    connect-timeout (and an incremented attempt) against a dead proxy."""
    import socket
    try:
        with socket.create_connection((MOBILE_RELAY_PROXY_HOST, MOBILE_RELAY_PROXY_PORT), timeout=timeout_s):
            return True
    except OSError:
        return False


def _clear_relay_guard(r, fein: str) -> None:
    """Clear the SET NX push-dedup guard (scripts/discover_h1b_ats.py::_push_mobile_relay)
    once this fein's queued item is actually dropped or removed — resolved, or
    permanently exhausted after MOBILE_RELAY_MAX_ATTEMPTS. Must NOT be called when the
    item is merely requeued for another attempt; the guard's job is to keep the fein
    from being double-enqueued while still genuinely queued/inflight."""
    try:
        r.delete(f"{MOBILE_RELAY_GUARD_PREFIX}{fein}")
    except Exception as exc:
        log.warning("fein=%s: relay guard clear failed (%s) — will self-expire via TTL", fein, exc)


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
# DB helpers
# ─────────────────────────────────────────────────────────────────────────────

def _load_company(conn, fein: str) -> "dict | None":
    row = conn.execute("""
        SELECT
            f.employer_fein,
            f.assigned_domain,
            f.public_domain,
            f.public_domain_host,
            COALESCE(f.public_domain_retry_count, 0) AS public_domain_retry_count,
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
# Careers-URL resolution — Phase 3 → Phase 6 → Phase 7, called directly
# ─────────────────────────────────────────────────────────────────────────────

def _resolve_careers_via_relay(conn, m, fein: str, website_url: str, company_name: str,
                                total_approvals: int, run_brave: bool, session,
                                public_domain: "str | None" = None) -> dict:
    """Phase 3 → Phase 6 → Phase 7 career-discovery chain over the relay session —
    the exact same functions scripts.discover_h1b_ats.process_employer calls for
    those phases (discover_careers_url, jobs.career_page.detect_via_career_page,
    jobs.ats.career_detector.detect_company), called directly instead of going
    through process_employer itself, since that function's KG/SPARQL/canonical-name
    resolution and company_ats upsert don't belong in this relay re-fetch-only
    context (docs/discovery-pipeline-hardening.md Part 3).

    Phase 4 (Brave) is deliberately NOT part of this chain — it's a search-API
    call, not a target-site fetch, so a different egress IP can't help it. It only
    runs, once, directly, right here, when run_brave is True (the final attempt
    before permanent drop) and careers_url is still unresolved after Phase 3/6/7 —
    matching the doc's "before the permanent drop specifically... call Phase 4
    once, directly, right here".

    Persists exactly like process_employer's own direct-OCI persist block: a
    careers_url hit UPSERTs fein_domain_map; a block-like miss (403/429/503, only
    possible from Phase 3) updates careers_url_last_status only; a resolved
    platform+slug upserts company_ats.
    """
    careers_url = detected_platform = detected_slug = careers_source = ats_source = None
    careers_url_last_status = None

    website_url = m._resolve_website_redirect(website_url, session)

    # Phase 3: 19-pattern probe
    try:
        careers_url, detected_platform, detected_slug, careers_url_last_status = \
            m.discover_careers_url(website_url, session)
        if careers_url:
            careers_source = "phase3"
        if detected_platform:
            ats_source = "phase3"
    except Exception as e:
        log.warning("fein=%s: relay Phase 3 probe failed: %s", fein, e)

    # Phase 4: Brave — last-resort only, right before permanent drop (see docstring)
    if not careers_url and run_brave:
        try:
            search_name = m.strip_legal_suffixes(company_name) or company_name
            brave_url = m.brave_career_search(search_name, website_url=website_url,
                                              anchor_domain=public_domain)
            if brave_url:
                careers_url    = brave_url
                careers_source = "phase4"
                log.info("fein=%s: relay Phase 4 (Brave) found: %s", fein, brave_url)
                try:
                    html, _final, _ = m._fetch_html(brave_url, session)
                    _why = m._brave_landing_rejected(_final, company_name, public_domain)
                    if _why:
                        log.warning("fein=%s: relay Brave result %s landed on rejected %s (%s) — dropping",
                                    fein, brave_url, _final, _why)
                        careers_url = careers_source = None
                    elif html:
                        detected_platform, detected_slug = m._find_ats_in_html(html)
                        if detected_platform:
                            ats_source = "phase5"
                except Exception as e:
                    log.warning("fein=%s: relay Phase 4 HTML fingerprint failed: %s", fein, e)
        except Exception as e:
            log.warning("fein=%s: relay Phase 4 (Brave) failed: %s", fein, e)

    # Phase 6: career_page.py — 3-layer deep scan
    if not detected_platform:
        try:
            from jobs.career_page import detect_via_career_page
            _domain = m._root_domain(website_url)
            _seed = careers_url if careers_source in {"phase3"} else None
            _cp = detect_via_career_page(company_name, _domain, careers_url=_seed, session=session)
            if _cp:
                if _cp.get("platform"):
                    detected_platform = _cp["platform"]
                    detected_slug     = _cp.get("slug")
                    ats_source        = "phase6"
                if _cp.get("careers_url"):
                    careers_url    = _cp["careers_url"]
                    careers_source = "phase6"
        except Exception as e:
            log.warning("fein=%s: relay Phase 6 (career_page) failed: %s", fein, e)

    # Phase 7: career_detector.py — Chrome-impersonation BFS, last resort
    if not detected_platform:
        try:
            from jobs.ats.career_detector import detect_company
            _domain = m._root_domain(website_url)
            _seed = careers_url if careers_source in {"phase3", "phase6"} else None
            _results = detect_company(_domain, session=session, seed_url=_seed)
            if _results:
                _best = next((r for r in _results if r.get("slug")), _results[0])
                detected_platform = _best["platform"]
                _best_slug = _best.get("slug") or ""
                if _best_slug:
                    detected_slug = _best_slug
                ats_source = "phase7"
                if not careers_url:
                    _src = _best.get("source_url")
                    if _src:
                        careers_url    = _src
                        careers_source = "phase7"
        except Exception as e:
            log.warning("fein=%s: relay Phase 7 (career_detector) failed: %s", fein, e)

    # Same all-phase vendor/challenge/aggregator host guard as process_employer.
    if careers_url:
        _why = m.blocked_reason(careers_url, company_name)
        if _why:
            log.warning("fein=%s: relay dropping careers_url %s (%s, source=%s)",
                        fein, careers_url, _why, careers_source)
            if m.ats_derived_from_careers_source(ats_source, careers_source):
                detected_platform = detected_slug = ats_source = None
            careers_url = careers_source = None

    # Persist — same shape as process_employer's own direct-OCI persist block
    # (scripts/discover_h1b_ats.py, end of process_employer).
    if careers_url:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO fein_domain_map (employer_fein, careers_url, careers_source, careers_url_last_status, updated_at)
            VALUES (%s, %s, %s, NULL, NOW())
            ON CONFLICT (employer_fein) DO UPDATE
                SET careers_url    = EXCLUDED.careers_url,
                    careers_source = EXCLUDED.careers_source,
                    careers_url_last_status = NULL,
                    careers_url_verified_at = CASE
                        WHEN fein_domain_map.careers_url IS DISTINCT FROM EXCLUDED.careers_url
                        THEN NULL
                        ELSE fein_domain_map.careers_url_verified_at
                    END,
                    updated_at     = NOW()
        """, (fein, careers_url, careers_source))
        conn.commit()
    elif careers_url_last_status is not None:
        cur = conn.cursor()
        cur.execute("""
            UPDATE fein_domain_map
            SET careers_url_last_status = %s, updated_at = NOW()
            WHERE employer_fein = %s
        """, (careers_url_last_status, fein))
        conn.commit()

    if detected_platform and detected_slug and website_url:
        domain = m._root_domain(website_url)
        if domain:
            m._upsert_company_ats(
                conn, fein=fein, domain=domain, company_name=company_name,
                platform=detected_platform, slug=detected_slug,
                priority=total_approvals,
            )
            log.info("fein=%s: relay company_ats upserted: %s / %s / %s",
                      fein, domain, detected_platform, detected_slug)

    return {
        "careers_url": careers_url,
        "detected_platform": detected_platform,
        "detected_slug": detected_slug,
        "careers_url_last_status": careers_url_last_status,
    }


# ─────────────────────────────────────────────────────────────────────────────
# Per-company processing
# ─────────────────────────────────────────────────────────────────────────────

def _process_relay_item(fein: str, run_brave: bool) -> "bool | None":
    """
    Resolve whichever of public_domain / careers_url the current row is still
    missing, through the mobile relay session. public_domain uses the exact same
    function the direct-OCI path calls (jobs.public_domain.discover_public_domain).
    careers_url calls Phase 3/6/7 (discover_careers_url / detect_via_career_page /
    career_detector.detect_company) directly — see _resolve_careers_via_relay —
    rather than the full process_employer() orchestration, whose KG/SPARQL/
    canonical-name resolution and company_ats upsert don't belong in a relay
    re-fetch-only context. Per the doc: "No separate 'quick check' step and no
    separate 'from scratch' step" — same underlying fetch functions, just handed
    a relay-backed session.

    run_brave (True only on the final attempt before permanent drop): when True,
    Phase 4 (Brave) is allowed to run if careers_url is still unresolved after
    Phase 3/6/7. Every other attempt skips it entirely.

    Returns:
      True  — attempt completed without error, and the row is now fully resolved
              (or was already fully resolved when re-checked) — caller can drop.
      False — attempt completed without error, but something is still unresolved
              and this was not the final (run_brave) attempt — caller retries.
      None  — an exception occurred — caller treats this as a failed attempt too
              (same retry/drop accounting as False, logged separately).
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
        assigned_domain = company["assigned_domain"]
        pd_missing = company["public_domain"] is None
        careers_missing = company["careers_url"] is None

        if not pd_missing and not careers_missing:
            log.info("fein=%s already fully resolved — skipping", fein)
            return True

        if not assigned_domain:
            log.warning("fein=%s has no assigned_domain — cannot relay, skipping", fein)
            return True

        log.info("relay discovery fein=%s domain=%s name=%r pd_missing=%s careers_missing=%s "
                  "run_brave=%s", fein, assigned_domain, employer_name, pd_missing,
                  careers_missing, run_brave)

        from jobs.http_safe import make_relay_curl_session
        relay_session = make_relay_curl_session(MOBILE_RELAY_PROXY_HOST, MOBILE_RELAY_PROXY_PORT)
        try:
            if pd_missing:
                from jobs.public_domain import discover_public_domain_gated
                from workers.domain_enrichment_worker import _write_domain
                # relay_mode: final escalation tier — a 403 here is accepted as the pd.
                # Cross-domain redirects still pass the employer-name gate (held in pd_redirect_review).
                public_domain, method, retry_after, last_status, pd_host = discover_public_domain_gated(
                    conn, fein, employer_name, assigned_domain, source="relay",
                    session=relay_session, relay_mode=True,
                )
                _write_domain(conn, fein, public_domain, method, last_status,
                              company["public_domain_retry_count"], pd_host)
                conn.commit()
                _pd_status = last_status or (200 if public_domain else 0)
                _pd_ms = int((time.time() - t_start) * 1000)
                record_external_request("mobile_relay", _pd_status, _pd_ms)
                # Phase×origin metrics (docs/discovery-pipeline-hardening.md Part 4) —
                # second, additive write under the pd-specific label, alongside the
                # relay-wide "mobile_relay" write above; never read by any quota gate.
                record_external_request("pd_relay", _pd_status, _pd_ms)
                if public_domain:
                    company["public_domain"] = public_domain
                    company["public_domain_host"] = pd_host
                    pd_missing = False

            det_platform = det_slug = res_careers = None
            if careers_missing:
                probe_domain = company["public_domain"] or assigned_domain
                # Fetch uses the stored host; fallback host -> public_domain -> assigned.
                website_url = "https://" + (company.get("public_domain_host") or probe_domain)
                try:
                    result = _resolve_careers_via_relay(
                        conn, m, fein, website_url, employer_name,
                        int(company["petition_count"] or 0), run_brave, relay_session,
                        public_domain=company["public_domain"],
                    )
                except Exception as e:
                    log.error("fein=%s: careers relay resolution failed: %s", fein, e, exc_info=True)
                    _err_ms = int((time.time() - t_start) * 1000)
                    record_external_request("mobile_relay", 0, _err_ms, error_kind=type(e).__name__)
                    # Phase×origin metrics (Part 4) — second, additive write, see above.
                    record_external_request("career_relay", 0, _err_ms, error_kind=type(e).__name__)
                    return None

                det_platform = result.get("detected_platform")
                det_slug     = result.get("detected_slug")
                res_careers  = result.get("careers_url")
                careers_missing = res_careers is None
                _career_status = 200 if res_careers else 404
                _career_ms = int((time.time() - t_start) * 1000)
                record_external_request("mobile_relay", _career_status, _career_ms)
                # Phase×origin metrics (Part 4) — second, additive write, see above.
                record_external_request("career_relay", _career_status, _career_ms)
        finally:
            relay_session.close()

        fully_resolved = not pd_missing and not careers_missing
        if fully_resolved or run_brave:
            # Relay has had its full shot (resolved, or this was the last attempt) —
            # stop deferring Phase 4 on the direct (OCI) path for this fein forever.
            _clear_careers_url_last_status(conn, fein)
            conn.commit()

        log.info("fein=%s relay attempt done: pd_missing=%s careers=%s platform=%s slug=%s",
                 fein, pd_missing, res_careers, det_platform, det_slug)
        return fully_resolved

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
        return None
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
        r.zadd(MOBILE_RELAY_QUEUE, {raw_member: score}, gt=True)
        r.zrem(inflight_key, raw_member)
        log.info("reclaimed inflight member=%s score=%s", raw_member, score)


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

            raw_member = _pop_result[0]

            try:
                data     = json.loads(raw_member)
                fein     = data["fein"]
                attempts = int(data.get("attempts", 0))
            except (json.JSONDecodeError, KeyError, TypeError):
                bare = raw_member.strip() if isinstance(raw_member, str) else raw_member.decode(errors="replace").strip()
                if bare.isdigit():
                    fein, attempts = bare, 0
                    log.debug("Legacy bare-FEIN member %r", bare)
                else:
                    log.error("Malformed relay queue member %r — dropping", raw_member)
                    r.zrem(_inflight_key, raw_member)
                    continue

            # Cheap reachability check before spending a real attempt: if the tunnel
            # has dropped since this item was popped, restore it to the queue exactly
            # as popped (original payload, original FIFO score) — no attempts penalty,
            # since the resolution itself was never even tried — and stop the loop so
            # this instance doesn't spin tight-polling a dead proxy (manager.py's
            # autoscaler will stop the unit once it independently observes the tunnel
            # is unreachable).
            if not _probe_mobile_relay_reachable():
                _orig_score = _pop_result[1]
                r.zadd(MOBILE_RELAY_QUEUE, {raw_member: _orig_score})
                r.zrem(_inflight_key, raw_member)
                log.warning("fein=%s: relay unreachable — restored to queue unchanged, exiting", fein)
                break

            is_final_attempt = (attempts + 1) >= MOBILE_RELAY_MAX_ATTEMPTS
            outcome = _process_relay_item(fein, run_brave=is_final_attempt)
            processed["n"] += 1

            if outcome is True:
                log.info("fein=%s relay: fully resolved — done", fein)
                _clear_relay_guard(r, fein)
            elif is_final_attempt:
                log.warning("fein=%s relay: attempt %d/%d exhausted — permanently dropping",
                            fein, attempts + 1, MOBILE_RELAY_MAX_ATTEMPTS)
                _clear_relay_guard(r, fein)
            else:
                # Still unresolved (outcome False) or errored (outcome None) — requeue with
                # a fresh FIFO timestamp and attempts incremented. Unlike the discovery
                # worker's exponential backoff, a relay failure is far more likely to be
                # "tunnel dropped mid-run" (manager stops this worker as soon as the tunnel
                # is unreachable), so an immediate requeue at the back of the FIFO is enough.
                new_payload = json.dumps({"fein": fein, "attempts": attempts + 1})
                r.zadd(MOBILE_RELAY_QUEUE, {new_payload: time.time()})
                log.warning("fein=%s relay attempt %d/%d incomplete — requeued",
                            fein, attempts + 1, MOBILE_RELAY_MAX_ATTEMPTS)

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
