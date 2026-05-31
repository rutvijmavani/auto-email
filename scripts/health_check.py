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
import os
import sys
import time
from datetime import datetime

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

def run_health_check() -> int:
    """
    Run all checks and print a report.
    Returns exit code: 0 = OK/WARNING, 1 = ERROR/CRITICAL.
    """
    from config import (
        REDIS_POLL_ADAPTIVE, REDIS_POLL_FULLSCAN,
        REDIS_DETAIL_ADAPTIVE, REDIS_DETAIL_FULLSCAN,
        REDIS_STREAM_ADAPTIVE, REDIS_STREAM_FULLSCAN,
        STREAM_CONSUMER_GROUP,
    )
    from workers.redis_client import get_redis, ping

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

    # Redis
    r_ok = ping()
    if not r_ok:
        _row("ERROR", "Redis", "UNREACHABLE — all workers likely stopped")
        errors += 1
        print(f"\n  {_c('Cannot continue — Redis required for all checks', _RED)}\n")
        return 1

    r = get_redis()

    try:
        info    = r.info("server")
        version = info.get("redis_version", "?")
        mem     = info.get("used_memory_human", "?")
        _row("OK", "Redis", f"v{version}  memory={mem}")
    except Exception as exc:
        _row("WARNING", "Redis", f"Connected but info failed: {exc}")
        warnings += 1

    try:
        persist = r.info("persistence")
        last_s  = persist.get("rdb_last_bgsave_time_sec", 0) or r.lastsave()
        if isinstance(last_s, int) and last_s > 0:
            age_min = (now - last_s) / 60
            if age_min > 30:
                _row("WARNING", "Redis RDB save", f"Last save {age_min:.0f} min ago — data loss window is large")
                warnings += 1
            else:
                _row("OK", "Redis RDB save", f"Last save {age_min:.0f} min ago")
    except Exception:
        pass

    # PostgreSQL
    try:
        from db.db import init_db, get_conn
        init_db()
        conn = get_conn()
        row  = conn.execute("SELECT COUNT(*) AS cnt FROM job_postings").fetchone()
        jobs = row["cnt"] if row else 0
        pend = conn.execute(
            "SELECT COUNT(*) AS cnt FROM job_postings WHERE status='pending_detail'"
        ).fetchone()
        conn.close()
        _row("OK", "PostgreSQL", f"{jobs:,} total jobs  {pend['cnt']} pending_detail")
    except Exception as exc:
        _row("ERROR", "PostgreSQL", f"UNREACHABLE: {exc}")
        errors += 1

    # ── WORKER LIVENESS ───────────────────────────────────────────────────────
    _section("WORKER LIVENESS  (via worker:alive:* heartbeat keys)")

    workers = [
        ("scheduler",       15,   "worker:alive:scheduler"),
        ("scan_worker",     45,   "worker:alive:scan_worker"),
        ("detail_worker",   45,   "worker:alive:detail_worker"),
        ("fullscan_worker", 1900, "worker:alive:fullscan_worker"),
    ]

    for label, threshold, key in workers:
        raw = r.get(key)
        if raw is None:
            _row("ERROR", label, "DEAD — heartbeat key missing")
            errors += 1
        else:
            try:
                d       = json.loads(raw)
                age_s   = now - d.get("ts", now)
                pid     = d.get("pid", "?")
                proc    = d.get("processed", 0)
                extra   = d.get("dispatched", proc)  # scheduler uses 'dispatched'
                _row("OK", label,
                     f"pid={pid}  processed={extra}  "
                     f"heartbeat {age_s:.0f}s ago")
            except Exception:
                _row("OK", label, "alive (heartbeat key present)")

    # ── QUEUE HEALTH ──────────────────────────────────────────────────────────
    _section("QUEUE HEALTH")

    # poll:adaptive
    adp_total   = r.zcard(REDIS_POLL_ADAPTIVE)
    adp_overdue = r.zcount(REDIS_POLL_ADAPTIVE, "-inf", now - 1800)
    if adp_total == 0:
        _row("ERROR", "poll:adaptive (ZSET)", "EMPTY — rebuild needed")
        errors += 1
    elif adp_overdue > 10:
        _row("ERROR", "poll:adaptive (ZSET)",
             f"{adp_total} total  {adp_overdue} overdue >30min")
        errors += 1
    elif adp_overdue > 3:
        _row("WARNING", "poll:adaptive (ZSET)",
             f"{adp_total} total  {adp_overdue} overdue >30min")
        warnings += 1
    else:
        _row("OK", "poll:adaptive (ZSET)", f"{adp_total} companies  {adp_overdue} overdue")

    # poll:fullscan
    fs_total   = r.zcard(REDIS_POLL_FULLSCAN)
    fs_overdue = r.zcount(REDIS_POLL_FULLSCAN, "-inf", now - 7200)
    if fs_total == 0:
        _row("WARNING", "poll:fullscan (ZSET)",
             "EMPTY — normal after rebuild, alert if > 1h")
        warnings += 1
    elif fs_overdue > 5:
        _row("ERROR", "poll:fullscan (ZSET)",
             f"{fs_total} total  {fs_overdue} overdue >2h")
        errors += 1
    else:
        _row("OK", "poll:fullscan (ZSET)", f"{fs_total} companies  {fs_overdue} overdue")

    # detail queues
    for key, label in [(REDIS_DETAIL_ADAPTIVE, "detail:adaptive (LIST)"),
                       (REDIS_DETAIL_FULLSCAN,  "detail:fullscan (LIST)")]:
        depth = r.llen(key)
        if depth > 500:
            _row("ERROR", label, f"depth={depth:,} — CRITICAL backlog")
            errors += 1
        elif depth > 100:
            _row("WARNING", label, f"depth={depth:,} — elevated")
            warnings += 1
        else:
            _row("OK", label, f"depth={depth}")

    # Stream PEL
    for stream_key, label in [(REDIS_STREAM_ADAPTIVE, "stream:adaptive (PEL)"),
                               (REDIS_STREAM_FULLSCAN,  "stream:fullscan (PEL)")]:
        try:
            summary = r.xpending(stream_key, STREAM_CONSUMER_GROUP)
            total   = summary.get("pending", 0) if summary else 0
            if total == 0:
                _row("OK", label, "0 pending entries")
            else:
                entries  = r.xpending_range(
                    stream_key, STREAM_CONSUMER_GROUP, min="-", max="+", count=1,
                )
                oldest_ms = entries[0].get("time_since_delivered", 0) if entries else 0
                oldest_s  = oldest_ms // 1000
                if oldest_s > 1800:
                    _row("ERROR", label,
                         f"{total} pending  oldest={oldest_s//60}min — XAUTOCLAIM may be stuck")
                    errors += 1
                elif oldest_s > 600:
                    _row("WARNING", label,
                         f"{total} pending  oldest={oldest_s//60}min")
                    warnings += 1
                else:
                    _row("OK", label, f"{total} pending  oldest={oldest_s}s")
        except Exception as exc:
            _row("WARNING", label, f"Could not query: {exc}")
            warnings += 1

    # ── BLOOM FILTERS ─────────────────────────────────────────────────────────
    _section("BLOOM FILTERS")

    bloom_count = fallback_count = 0
    cursor = 0
    for _ in range(10):
        cursor, keys = r.scan(cursor, match="bloom:fullscan:*", count=200)
        bloom_count += len(keys)
        if cursor == 0:
            break
    cursor = 0
    for _ in range(10):
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

    # ── COVERAGE ──────────────────────────────────────────────────────────────
    _section("COVERAGE (last 26h)")

    try:
        from db.db import get_conn
        conn   = get_conn()
        total  = conn.execute("SELECT COUNT(*) AS c FROM company_poll_stats").fetchone()["c"]
        missed = conn.execute("""
            SELECT COUNT(*) AS c FROM company_poll_stats
            WHERE last_full_scan_at IS NULL
               OR last_full_scan_at < NOW() - INTERVAL '26 hours'
        """).fetchone()["c"]
        stuck  = conn.execute("""
            SELECT COUNT(*) AS c FROM job_postings
            WHERE status = 'pending_detail'
              AND created_at < NOW() - INTERVAL '1 hour'
        """).fetchone()["c"]
        # Show the worst 3 missed companies
        missed_names = conn.execute("""
            SELECT company, last_full_scan_at
            FROM company_poll_stats
            WHERE last_full_scan_at IS NULL
               OR last_full_scan_at < NOW() - INTERVAL '26 hours'
            ORDER BY last_full_scan_at ASC NULLS FIRST
            LIMIT 3
        """).fetchall()
        conn.close()

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

    return 1 if errors > 0 else 0


if __name__ == "__main__":
    sys.exit(run_health_check())
