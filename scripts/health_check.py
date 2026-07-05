#!/usr/bin/env python3
"""
scripts/health_check.py — Instant pipeline health status (no email, no side-effects).

Prints a color-coded status table for every pipeline component and exits.
Exit code: 0 if all checks pass, 1 if any ERROR/CRITICAL found.

Usage:
    cd /home/opc/mail
    source venv/bin/activate
    python scripts/health_check.py

    # Or via module:
    python -m workers.watchdog --status

What is checked:
  ─ Infrastructure  : Redis (reachable, version, memory, last RDB save)
                      PostgreSQL (reachable, job count)
  ─ Worker liveness : scheduler, scan_worker, detail_worker, fullscan_worker
                      via worker:alive:{type} heartbeat keys
  ─ Queue health    : poll:adaptive (ZSET), poll:fullscan (ZSET)
                      queue:detail:adaptive (LIST), queue:detail:fullscan (LIST)
                      stream:adaptive PEL, stream:fullscan PEL
  ─ Bloom filters   : bloom:fullscan:* key count
  ─ Coverage        : companies not scanned in last 26h
  ─ Stuck jobs      : pending_detail records > 1h old

Exit code is 0 if everything is OK or WARNING-only; 1 if any ERROR or CRITICAL.
"""

import json
import logging
import os
import sys
import time
from datetime import datetime

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# Bootstrap — ensure project root is on sys.path
# ─────────────────────────────────────────────────────────────────────────────
_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

# ANSI colors for terminal output
_GREEN  = "\033[92m"
_YELLOW = "\033[93m"
_RED    = "\033[91m"
_CYAN   = "\033[96m"
_BOLD   = "\033[1m"
_RESET  = "\033[0m"
_GREY   = "\033[90m"

# Disable colors if not a TTY (piped output)
if not sys.stdout.isatty():
    _GREEN = _YELLOW = _RED = _CYAN = _BOLD = _RESET = _GREY = ""


def _c(text: str, *codes: str) -> str:
    """Wrap text in ANSI codes."""
    return "".join(codes) + text + _RESET


def _sym(level: str) -> str:
    """Return a colored status symbol."""
    return {
        "OK":       _c("✓", _GREEN),
        "WARNING":  _c("!", _YELLOW),
        "DEGRADED": _c("~", _YELLOW),
        "ERROR":    _c("✗", _RED, _BOLD),
        "CRITICAL": _c("✗", _RED, _BOLD),
    }.get(level, "?")


def _section(title: str) -> None:
    print(f"\n  {_c(title, _CYAN, _BOLD)}")
    print(f"  {'─' * 60}")


def _row(level: str, label: str, detail: str) -> None:
    sym     = _sym(level)
    lbl_pad = f"{label:<32}"
    print(f"  [{sym}] {lbl_pad} {detail}")


# ─────────────────────────────────────────────────────────────────────────────
# CHECKS
# ─────────────────────────────────────────────────────────────────────────────

_INFLIGHT_STALE_S = 7200   # 2-hour in-flight TTL (matches job_monitor.py)


def run_health_check() -> int:
    """
    Run all checks and print a report.
    Returns exit code: 0 = OK/WARNING, 1 = ERROR/CRITICAL.
    """
    from config import (
        REDIS_POLL_ADAPTIVE, REDIS_POLL_FULLSCAN,
        REDIS_DETAIL_ADAPTIVE, REDIS_DETAIL_FULLSCAN,
        REDIS_STREAM_ADAPTIVE, REDIS_STREAM_FULLSCAN,
        STREAM_CONSUMER_GROUP, REDIS_URL,
    )
    import redis as _redis_hc_lib

    now     = time.time()
    errors  = 0
    warnings = 0

    DSEP = "═" * 70
    print(f"\n{_c(DSEP, _BOLD)}")
    print(f"  {_c('PIPELINE HEALTH CHECK', _BOLD)}   "
          f"{_c(datetime.now().strftime('%Y-%m-%d %H:%M:%S'), _GREY)}")
    print(f"{_c(DSEP, _BOLD)}")

    # ── INFRASTRUCTURE ────────────────────────────────────────────────────────
    _section("INFRASTRUCTURE")

    # Redis — use a health-check-specific client with bounded timeouts so the
    # check never hangs indefinitely if Redis stops responding mid-operation.
    try:
        r = _redis_hc_lib.from_url(
            REDIS_URL,
            socket_timeout=5,
            socket_connect_timeout=3,
            decode_responses=True,
        )
        r_ok = r.ping()
    except Exception:
        r_ok = False
        r = None

    if not r_ok:
        _row("ERROR", "Redis", "UNREACHABLE — all workers likely stopped")
        errors += 1
        print(f"\n  {_c('Cannot continue — Redis required for all checks', _RED)}\n")
        return 1

    try:
        info     = r.info("server")
        mem_info = r.info("memory")
        version  = info.get("redis_version", "?")
        mem      = mem_info.get("used_memory_human", "?")
        _row("OK", "Redis", f"v{version}  memory={mem}")
    except Exception as exc:
        _row("WARNING", "Redis", f"Connected but info failed: {exc}")
        warnings += 1

    try:
        from workers.watchdog import check_redis_persistence, Issue as _WdgIssue
        for _pi in check_redis_persistence(r):
            _row(_pi.level, _pi.category, _pi.message)
            if _pi.level in ("ERROR", "CRITICAL"):
                errors += 1
            elif _pi.level == "WARNING":
                warnings += 1
    except Exception as exc:
        # Fallback: direct RDB-save check if watchdog module is unavailable
        try:
            persist = r.info("persistence")
            last_s  = persist.get("rdb_last_save_time", 0) or r.lastsave()
            if isinstance(last_s, int) and last_s > 0:
                age_min = (now - last_s) / 60
                if age_min > 30:
                    _row("WARNING", "Redis RDB save",
                         f"Last save {age_min:.0f} min ago — data loss window is large")
                    warnings += 1
                else:
                    _row("OK", "Redis RDB save", f"Last save {age_min:.0f} min ago")
        except Exception as exc2:
            _row("WARNING", "Redis persistence", f"Persistence check failed: {exc2}")
            warnings += 1

    # PostgreSQL
    try:
        from db.db import get_conn
        conn = get_conn()
        try:
            row  = conn.execute("SELECT COUNT(*) AS cnt FROM job_postings").fetchone()
            jobs = row["cnt"] if row else 0
            pend = conn.execute(
                "SELECT COUNT(*) AS cnt FROM job_postings WHERE status='pending_detail'"
            ).fetchone()
            _row("OK", "PostgreSQL", f"{jobs:,} total jobs  {pend['cnt']} pending_detail")
        finally:
            conn.close()
    except Exception as exc:
        _row("ERROR", "PostgreSQL", f"UNREACHABLE: {exc}")
        errors += 1

    # ── WORKER LIVENESS ───────────────────────────────────────────────────────
    _section("WORKER LIVENESS")

    # ── Scheduler — single heartbeat key ─────────────────────────────────────
    raw = r.get("worker:alive:scheduler")
    if raw is None:
        _row("ERROR", "scheduler", "DEAD — heartbeat key missing")
        errors += 1
    else:
        try:
            d     = json.loads(raw)
            age_s = now - d.get("ts", now)
            status = (
                f"pid={d.get('pid','?')}  "
                f"dispatched={d.get('dispatched',0)}  "
                f"heartbeat {age_s:.0f}s ago"
            )
            # Scheduler writes this key every SCHEDULER_TICK_SECS (~1 s) with
            # ex=15.  The TTL is 15 s, so the key expires before age_s can
            # reach 20 — making "age_s > 20" unreachable dead code.
            # Use 5 s instead: reachable within the 15 s TTL window, and a
            # meaningful signal that the scheduler main loop is delayed.
            if age_s > 5:
                _row("WARNING", "scheduler", status + "  (STALE)")
                warnings += 1
            else:
                _row("OK", "scheduler", status)
        except Exception:
            _row("WARNING", "scheduler", "alive but heartbeat payload unparseable")
            warnings += 1

    # ── Worker pools — from scheduler:health + per-PID keys ──────────────────
    health_raw = r.get("scheduler:health")
    if health_raw is None:
        _row("WARNING", "worker pools", "scheduler:health missing — pool state unknown")
        warnings += 1
    else:
        try:
            health = json.loads(health_raw)
            pool   = health.get("pool", {})

            for ptype, label_suffix in [
                ("scan",     "scan_worker"),
                ("detail",   "detail_worker"),
                ("fullscan", "fullscan_worker"),
            ]:
                info   = pool.get(ptype, {})
                alive  = info.get("alive", 0)
                consec = info.get("consecutive_deaths", 0)
                total  = info.get("total_replacements", 0)

                # Collect per-PID details
                pid_details = []
                try:
                    cursor = 0
                    while True:
                        cursor, keys = r.scan(
                            cursor,
                            match=f"worker:alive:{label_suffix}:*",
                            count=50,
                        )
                        for key in keys:
                            kraw = r.get(key)
                            if kraw:
                                kd = json.loads(kraw)
                                pid_details.append(
                                    f"pid={kd.get('pid','?')} "
                                    f"proc={kd.get('processed',0)}"
                                )
                        if cursor == 0:
                            break
                except Exception as _scan_err:
                    logger.debug("health_check: Redis scan/parse failed: %s", _scan_err)

                pid_str = f"  [{' | '.join(pid_details)}]" if pid_details else ""
                base    = (
                    f"{alive} alive{pid_str}  "
                    f"total_replacements={total}"
                )

                if alive == 0:
                    _row("ERROR", label_suffix, f"{base}  no live workers")
                    errors += 1
                elif consec >= 5:
                    _row("ERROR", label_suffix,
                         f"{base}  consecutive_rapid_deaths={consec}")
                    errors += 1
                elif consec >= 3:
                    _row("WARNING", label_suffix,
                         f"{base}  consecutive_rapid_deaths={consec}")
                    warnings += 1
                else:
                    note = (
                        f"  ({consec} recent death(s) — replacing)"
                        if consec > 0 else ""
                    )
                    _row("OK", label_suffix, f"{base}{note}")

        except Exception as exc:
            _row("WARNING", "worker pools",
                 f"Could not parse scheduler:health: {exc}")
            warnings += 1

    # ── QUEUE HEALTH ──────────────────────────────────────────────────────────
    # Delegate to the watchdog's check_queue_health() so both tools agree on
    # health status.  The watchdog uses velocity-based stall detection (trend
    # across cycles stored in Redis); this CLI reads the same saved snapshot
    # and runs the same logic, ensuring no "false ERROR/OK" discrepancy.
    _section("QUEUE HEALTH")

    try:
        from workers.watchdog import check_queue_health, Issue
        _wdg_queue_issues = check_queue_health(r, persist_snapshot=False)
        for _issue in _wdg_queue_issues:
            _lbl = _issue.category.replace("queue:", "").replace("stream:", "stream:")
            _row(_issue.level, _lbl, _issue.message)
            if _issue.level in ("ERROR", "CRITICAL"):
                errors += 1
            elif _issue.level == "WARNING":
                warnings += 1
    except Exception as _exc:
        # Fallback to static counts if the watchdog module is unavailable
        _row("WARNING", "queue health", f"Could not run watchdog checks: {_exc}")
        warnings += 1

    # ── BLOOM FILTERS ─────────────────────────────────────────────────────────
    _section("BLOOM FILTERS")

    try:
        bloom_count = fallback_count = 0
        cursor = 0
        while True:
            cursor, keys = r.scan(cursor, match="bloom:fullscan:*", count=200)
            bloom_count += len(keys)
            if cursor == 0:
                break
        cursor = 0
        while True:
            cursor, keys = r.scan(cursor, match="bloom:fallback:*", count=200)
            fallback_count += len(keys)
            if cursor == 0:
                break

        total_bloom = bloom_count + fallback_count
        if total_bloom == 0:
            _row("WARNING", "bloom filters",
                 "No bloom:fullscan:* keys found — Redis may have restarted without saving")
            warnings += 1
        else:
            _row("OK", "bloom filters",
                 f"~{total_bloom} keys  (RedisBloom={bloom_count}  fallback={fallback_count})")
    except Exception as _bloom_err:
        _row("DEGRADED", "bloom filters", f"Redis scan error: {_bloom_err}")
        warnings += 1

    # ── COVERAGE ──────────────────────────────────────────────────────────────
    _section("COVERAGE (last 26h)")

    try:
        from db.db import get_conn
        from config import REDIS_INFLIGHT_FULLSCAN

        # Companies mid-scan are not missed — exclude them from the stale count
        # so the health check matches the contract used by the monitor layer.
        inflight_names: set = set()
        try:
            _stale_cutoff = now - _INFLIGHT_STALE_S
            _raw_inflight = r.zrangebyscore(
                REDIS_INFLIGHT_FULLSCAN, _stale_cutoff, "+inf"
            )
            inflight_names = {
                (c.decode() if isinstance(c, bytes) else c)
                for c in (_raw_inflight or [])
            }
        except Exception as _inf_err:
            logger.debug("health_check: inflight ZSET unavailable: %s", _inf_err)

        conn = get_conn()
        try:
            total = conn.execute("SELECT COUNT(*) AS c FROM company_poll_stats").fetchone()["c"]

            # Fetch all stale companies so we can filter out inflight in Python
            _stale_rows = conn.execute("""
                SELECT company, last_full_scan_at
                FROM company_poll_stats
                WHERE last_full_scan_at IS NULL
                   OR last_full_scan_at < NOW() - INTERVAL '26 hours'
                ORDER BY last_full_scan_at ASC NULLS FIRST
            """).fetchall()

            stuck = conn.execute("""
                SELECT COUNT(*) AS c FROM job_postings
                WHERE status = 'pending_detail'
                  AND created_at < NOW() - INTERVAL '1 hour'
            """).fetchone()["c"]
        finally:
            conn.close()

        # Exclude actively-scanning companies from the missed count
        _stale_set    = {r2["company"] for r2 in _stale_rows}
        _effective_missed = _stale_set - inflight_names
        missed        = len(_effective_missed)
        missed_names  = [r2 for r2 in _stale_rows if r2["company"] in _effective_missed][:3]

        scanned = total - missed
        pct     = scanned / total * 100 if total else 0

        if total == 0:
            _row("WARNING", "companies scanned", "No companies in company_poll_stats")
            warnings += 1
        elif missed / total > 0.25:
            _row("ERROR", "companies scanned",
                 f"{scanned}/{total} ({pct:.0f}%)  {missed} missed — throughput issue")
            errors += 1
        elif missed > 0:
            _row("WARNING", "companies scanned",
                 f"{scanned}/{total} ({pct:.0f}%)  {missed} missed")
            warnings += 1
        else:
            _row("OK", "companies scanned", f"{scanned}/{total} (100%)")

        if missed_names:
            names_str = ", ".join(
                r2["company"] + (" [never]" if r2["last_full_scan_at"] is None else "")
                for r2 in missed_names
            )
            print(f"  {_c('  Missed (worst):', _GREY)} {names_str}")

        if stuck > 10:
            _row("WARNING", "pending_detail >1h", f"{stuck} jobs stuck")
            warnings += 1
        elif stuck > 0:
            _row("OK", "pending_detail >1h", f"{stuck} (minor)")
        else:
            _row("OK", "pending_detail >1h", "0 stuck")

    except Exception as exc:
        _row("WARNING", "coverage", f"DB query failed: {exc}")
        warnings += 1

    # ── HUNG WORKERS ─────────────────────────────────────────────────────────
    try:
        hung = []
        cursor = 0
        while True:
            cursor, keys = r.scan(cursor, match="heartbeat:*", count=100)
            for key in keys:
                ks = key.decode() if isinstance(key, bytes) else key
                company = ks.split(":", 1)[1]
                if not r.exists(f"progress:{company}"):
                    hung.append(company)
            if cursor == 0:
                break

        _section("HUNG WORKERS  (heartbeat alive, no progress update)")
        if hung:
            _row("WARNING", "hung workers",
                 f"{len(hung)}: {', '.join(hung[:5])}{'...' if len(hung) > 5 else ''}")
            warnings += 1
        else:
            _row("OK", "hung workers", "none detected")
    except Exception as _hung_err:
        _section("HUNG WORKERS  (heartbeat alive, no progress update)")
        _row("DEGRADED", "hung workers", f"Redis scan error: {_hung_err}")
        warnings += 1

    # ── SUMMARY ───────────────────────────────────────────────────────────────
    print(f"\n  {'─' * 60}")
    verdict = (
        _c("ALL OK ✓",     _GREEN, _BOLD) if errors == 0 and warnings == 0 else
        _c("DEGRADED ⚠",   _YELLOW, _BOLD) if errors == 0 else
        _c("UNHEALTHY ✗",  _RED, _BOLD)
    )
    print(f"  VERDICT: {verdict}   "
          f"{_c(f'{errors} errors', _RED if errors else _GREY)}  "
          f"{_c(f'{warnings} warnings', _YELLOW if warnings else _GREY)}")
    print(f"{_c(DSEP, _BOLD)}\n")

    try:
        r.close()
    except Exception:
        pass

    return 1 if errors > 0 else 0


if __name__ == "__main__":
    sys.exit(run_health_check())
