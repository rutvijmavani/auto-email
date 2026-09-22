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
  ─ Worker liveness : scheduler, scan_worker, detail_worker, fullscan_worker,
                      h1b_llm_worker, email_processor
                      via worker:alive:{type} heartbeat keys
  ─ Queue health    : poll:adaptive (ZSET), poll:fullscan (ZSET)
                      queue:detail:adaptive (LIST), queue:detail:fullscan (LIST)
                      stream:adaptive PEL, stream:fullscan PEL
                      queue:email:push (LIST depth), llm:h1b:disambiguate (stream depth)
  ─ ATS pipeline    : head_check / domain_enrichment / discover_h1b_ats workers
                      (live instances, processed total, oldest heartbeat) and
                      their queues (queued, delayed, in-flight, DLQ); WARN on
                      DLQ > ATS_HEALTH_DLQ_WARN or pending work with no workers
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

from config import INFLIGHT_FULLSCAN_STALE_S
try:
    from workers.watchdog import (
        HEARTBEAT_DEAD_AFTER as _HEARTBEAT_DEAD_AFTER,
        WARN_DEATHS as _WARN_DEATHS,
        ERR_DEATHS  as _ERR_DEATHS,
    )
except Exception:
    _HEARTBEAT_DEAD_AFTER = {"scheduler": 20}   # fallback matches watchdog default
    _WARN_DEATHS = 3
    _ERR_DEATHS  = 5

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

_INFLIGHT_STALE_S = INFLIGHT_FULLSCAN_STALE_S   # imported from config; matches job_monitor.py


def _ats_lanes() -> list:
    """ATS pipeline lanes: worker type + the Redis structures each lane drains.

    Queue tuples are (label, key, kind) with kind "list" (LLEN) or "zset" (ZCARD).
    inflight is (glob pattern, kind); zero workers with items queued/in-flight/delayed
    means the manager has not (yet) scaled the pool up.
    """
    import config as _cfg
    return [
        {
            "name": "head-check", "worker": "head_check_worker",
            "heartbeat_s": _cfg.HEAD_CHECK_HEARTBEAT_S,
            "queues": [("on_demand", _cfg.HEAD_CHECK_ON_DEMAND, "list"),
                       ("batch", _cfg.HEAD_CHECK_BATCH, "list")],
            "delayed": None,
            "inflight": ("head_check:inflight:instance:*", "list"),
            "dlq": _cfg.HEAD_CHECK_DLQ,
        },
        {
            "name": "enrichment", "worker": "domain_enrichment_worker",
            "heartbeat_s": _cfg.ENRICHMENT_HEARTBEAT_S,
            "queues": [("on_demand", _cfg.ENRICHMENT_ON_DEMAND, "list"),
                       ("batch", _cfg.ENRICHMENT_BATCH, "zset")],
            "delayed": _cfg.ENRICHMENT_DELAYED,
            "inflight": (f"{_cfg.ENRICHMENT_INFLIGHT}*", "zset"),
            "dlq": _cfg.ENRICHMENT_DLQ,
        },
        {
            "name": "discovery", "worker": "discover_h1b_ats_worker",
            "heartbeat_s": _cfg.DISCOVERY_HEARTBEAT_S,
            "queues": [("redetect", _cfg.DISCOVERY_REDETECT, "zset"),
                       ("batch", _cfg.DISCOVERY_BATCH, "zset")],
            "delayed": _cfg.DISCOVERY_DELAYED,
            "inflight": (f"{_cfg.DISCOVERY_INFLIGHT}*", "zset"),
            "dlq": _cfg.DISCOVERY_DLQ,
        },
    ]


def _depth(r, key: str, kind: str) -> int:
    return int((r.llen(key) if kind == "list" else r.zcard(key)) or 0)


def check_ats_lane(r, lane: dict, now: float, dlq_warn: int) -> list:
    """Return [(level, label, detail), ...] for one ATS lane: a worker row and a queue row.

    Rules: idle (zero workers, everything empty) is normal -> OK. WARNING when the DLQ exceeds
    dlq_warn, when work is queued/delayed/in-flight but no worker is alive, or when a heartbeat
    key is present but older than 2x the heartbeat interval.
    """
    wtype = lane["worker"]
    stale_after = 2 * lane["heartbeat_s"]

    ages, processed, stale = [], 0, 0
    seen = set()
    for pattern in (f"worker:alive:{wtype}:*", f"worker:alive:{wtype}@*"):
        for key in r.scan_iter(pattern, count=50):
            if key in seen:
                continue
            seen.add(key)
            try:
                d = json.loads(r.get(key) or "{}")
                age = now - float(d.get("ts", now))
                processed += int(d.get("processed", 0) or 0)
            except Exception:
                continue
            ages.append(age)
            if age > stale_after:
                stale += 1
    live = len(ages) - stale

    tiers = [(lbl, _depth(r, key, kind)) for lbl, key, kind in lane["queues"]]
    queued = sum(n for _, n in tiers)
    delayed = _depth(r, lane["delayed"], "zset") if lane["delayed"] else 0
    pattern, ikind = lane["inflight"]
    inflight = sum(_depth(r, k, ikind) for k in set(r.scan_iter(pattern, count=50)))
    dlq = _depth(r, lane["dlq"], "list")

    rows = []
    oldest = f"  oldest heartbeat {max(ages):.0f}s ago" if ages else ""
    w_detail = f"{live} live  processed={processed}{oldest}"
    backlog = queued + delayed + inflight
    if stale:
        rows.append(("WARNING", wtype, f"{w_detail}  ({stale} STALE)"))
    elif live == 0 and backlog > 0:
        rows.append(("WARNING", wtype,
                     f"{w_detail}  no live workers but {backlog} item(s) pending "
                     f"— manager not scaling up?"))
    elif live == 0:
        rows.append(("OK", wtype, f"{w_detail}  idle (manager scales on demand)"))
    else:
        rows.append(("OK", wtype, w_detail))

    q_detail = (f"queued={queued} ({', '.join(f'{l}={n}' for l, n in tiers)})  "
                f"delayed={delayed}  in-flight={inflight}  dlq={dlq}")
    if dlq > dlq_warn:
        rows.append(("WARNING", f"{lane['name']} queue",
                     f"{q_detail}  DLQ above {dlq_warn}"))
    else:
        rows.append(("OK", f"{lane['name']} queue", q_detail))
    return rows


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
            aof_on  = persist.get("aof_enabled") in (1, "1", True)
            last_s  = persist.get("rdb_last_save_time", 0) or r.lastsave()
            # Normalize: some Redis client versions return a datetime object
            if hasattr(last_s, "timestamp"):
                last_s = int(last_s.timestamp())
            if isinstance(last_s, int) and last_s > 0:
                age_min = (now - last_s) / 60
                if age_min > 30 and not aof_on:
                    # Only warn about stale RDB when AOF is off — with AOF the
                    # data-loss window is ~1 s regardless of last snapshot age.
                    _row("WARNING", "Redis RDB save",
                         f"Last save {age_min:.0f} min ago — data loss window is large")
                    warnings += 1
                else:
                    suffix = " [AOF active — data safe]" if aof_on else ""
                    _row("OK", "Redis RDB save", f"Last save {age_min:.0f} min ago{suffix}")
            else:
                _row("WARNING", "Redis RDB save",
                     "No RDB save recorded — Redis has not persisted data yet")
                warnings += 1
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

    # ── SENTRY ────────────────────────────────────────────────────────────────
    try:
        import sentry_sdk as _sentry_sdk_hc
        _sentry_installed = True
    except ImportError:
        _sentry_installed = False

    if not _sentry_installed:
        _row("WARNING", "Sentry", "sentry-sdk not installed — run: pip install sentry-sdk")
        warnings += 1
    else:
        _dotenv_failed = False
        try:
            from dotenv import load_dotenv as _load_dotenv
        except ImportError:
            print("  [WARNING] health_check: python-dotenv not installed — .env not loaded", flush=True)
            _dotenv_failed = True
            warnings += 1
            _load_dotenv = None
        if _load_dotenv is not None:
            try:
                from pathlib import Path as _Path
                _load_dotenv(_Path(_ROOT) / ".env")
            except Exception as _dotenv_err:
                print(f"  [WARNING] health_check: .env load failed: {_dotenv_err}", flush=True)
                _dotenv_failed = True
                warnings += 1
        _dsn = os.environ.get("SENTRY_DSN", "").strip()

        if not _dsn:
            _row("WARNING", "Sentry", "SENTRY_DSN not set in .env — exception capture disabled")
            warnings += 1
        elif _dotenv_failed:
            _row("WARNING", "Sentry", "configured (SENTRY_DSN found in environment but .env load failed)")
        else:
            _row("OK", "Sentry", "configured")

    # ── WORKER LIVENESS ───────────────────────────────────────────────────────
    _section("WORKER LIVENESS")

    # ── Scheduler — per-loop heartbeat keys ──────────────────────────────────
    # Each scheduler loop writes its own key (ex=30s).  Check them independently
    # so a hung loop is visible even while the other loop keeps the process alive.
    try:
        _sched_loop_raws = {
            "adaptive": r.get("worker:alive:scheduler:adaptive"),
            "fullscan": r.get("worker:alive:scheduler:fullscan"),
        }
    except Exception as _hb_redis_err:
        _row("WARNING", "scheduler:heartbeats", f"Redis read failed: {_hb_redis_err}")
        warnings += 1
        _sched_loop_raws = {}
    for _loop_name, _raw in _sched_loop_raws.items():
        _label = f"scheduler:{_loop_name}"
        if _raw is None:
            _row("ERROR", _label, "DEAD — heartbeat key missing")
            errors += 1
        else:
            try:
                d     = json.loads(_raw)
                age_s = now - d.get("ts", now)
                status = (
                    f"pid={d.get('pid','?')}  "
                    f"dispatched={d.get('dispatched',0)}  "
                    f"heartbeat {age_s:.0f}s ago"
                )
                # Keys are written with ex=30s.  Dead-after from watchdog constant.
                if age_s > _HEARTBEAT_DEAD_AFTER["scheduler"]:
                    _row("ERROR", _label, status + "  (STALE)")
                    errors += 1
                else:
                    _row("OK", _label, status)
            except Exception:
                _row("WARNING", _label, "alive but heartbeat payload unparseable")
                warnings += 1

    # ── Manager heartbeat ─────────────────────────────────────────────────────
    try:
        mgr_keys = []
        cursor = 0
        while True:
            cursor, keys = r.scan(cursor, match="worker:alive:manager:*", count=50)
            mgr_keys.extend(keys)
            if cursor == 0:
                break
        if not mgr_keys:
            _row("ERROR", "manager", "DEAD — heartbeat key missing (recruiter-manager.service down?)")
            errors += 1
        else:
            raw = r.get(mgr_keys[0])
            d   = json.loads(raw) if raw else {}
            age_s = time.time() - float(d.get("ts", 0))
            status = f"pid={d.get('pid','?')}  cycles={d.get('cycles',0)}  heartbeat {age_s:.0f}s ago"
            if age_s > 180:
                _row("ERROR", "manager", status + "  (STALE)")
                errors += 1
            else:
                _row("OK", "manager", status)
    except Exception as _mgr_err:
        _row("WARNING", "manager", f"heartbeat check failed: {_mgr_err}")
        warnings += 1

    # ── h1b_llm_worker heartbeat ──────────────────────────────────────────────
    try:
        _h1b_keys = []
        cursor = 0
        while True:
            cursor, keys = r.scan(cursor, match="worker:alive:h1b_llm_worker:*", count=50)
            _h1b_keys.extend(keys)
            if cursor == 0:
                break
        if not _h1b_keys:
            _row("WARNING", "h1b_llm_worker",
                 "heartbeat key missing — worker not running (H1B LLM disambiguation paused)")
            warnings += 1
        else:
            _h1b_raw = r.get(_h1b_keys[0])
            _h1b_d   = json.loads(_h1b_raw) if _h1b_raw else {}
            _h1b_age = now - float(_h1b_d.get("ts", now))
            _h1b_dead_after = _HEARTBEAT_DEAD_AFTER.get("h1b_llm_worker", 60)
            _h1b_status = (
                f"pid={_h1b_d.get('pid','?')}  processed={_h1b_d.get('processed',0)}  "
                f"heartbeat {_h1b_age:.0f}s ago"
            )
            if _h1b_age > _h1b_dead_after:
                _row("WARNING", "h1b_llm_worker", _h1b_status + "  (STALE)")
                warnings += 1
            else:
                _row("OK", "h1b_llm_worker", _h1b_status)
    except Exception as _h1b_err:
        _row("WARNING", "h1b_llm_worker", f"heartbeat check failed: {_h1b_err}")
        warnings += 1

    # ── email_processor heartbeat ─────────────────────────────────────────────
    try:
        _ep_keys = []
        cursor = 0
        while True:
            cursor, keys = r.scan(cursor, match="worker:alive:email_processor:*", count=50)
            _ep_keys.extend(keys)
            if cursor == 0:
                break
        if not _ep_keys:
            _row("WARNING", "email_processor",
                 "heartbeat key missing — worker not running (email classification paused)")
            warnings += 1
        else:
            _ep_raw = r.get(_ep_keys[0])
            _ep_d   = json.loads(_ep_raw) if _ep_raw else None
            _ep_ts  = _ep_d.get("ts") if isinstance(_ep_d, dict) else None
            if _ep_ts is None:
                _row("WARNING", "email_processor",
                     "heartbeat key present but payload missing or has no ts — DEATH")
                warnings += 1
            else:
                _ep_age = now - float(_ep_ts)
                _ep_dead_after = _HEARTBEAT_DEAD_AFTER.get("email_processor", 60)
                _ep_status = (
                    f"pid={_ep_d.get('pid','?')}  processed={_ep_d.get('processed',0)}  "
                    f"heartbeat {_ep_age:.0f}s ago"
                )
                if _ep_age > _ep_dead_after:
                    _row("WARNING", "email_processor", _ep_status + "  (STALE)")
                    warnings += 1
                else:
                    _row("OK", "email_processor", _ep_status)
    except Exception as _ep_err:
        _row("WARNING", "email_processor", f"heartbeat check failed: {_ep_err}")
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
                elif consec >= _ERR_DEATHS:
                    _row("ERROR", label_suffix,
                         f"{base}  consecutive_rapid_deaths={consec}")
                    errors += 1
                elif consec >= _WARN_DEATHS:
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
            _lbl = _issue.category.replace("queue:", "").replace("stream:", "")
            _row(_issue.level, _lbl, _issue.message)
            if _issue.level in ("ERROR", "CRITICAL"):
                errors += 1
            elif _issue.level == "WARNING":
                warnings += 1
    except Exception as _exc:
        # Fallback to static counts if the watchdog module is unavailable
        _row("WARNING", "queue health", f"Could not run watchdog checks: {_exc}")
        warnings += 1

    # ── Ancillary queue depths (not velocity-tracked, just depth) ─────────────
    try:
        from config import REDIS_EMAIL_PUSH, H1B_DISAMBIG_STREAM
        _email_depth = int(r.llen(REDIS_EMAIL_PUSH) or 0)
        _email_msg   = f"depth={_email_depth}" + (" — idle" if _email_depth == 0 else "")
        if _email_depth > 50:
            _row("WARNING", "email:push", f"{_email_msg}  (backlog building — is email-processor running?)")
            warnings += 1
        else:
            _row("OK", "email:push", _email_msg)

        _h1b_lag = _h1b_pel = None
        try:
            for _grp in (r.xinfo_groups(H1B_DISAMBIG_STREAM) or []):
                if (isinstance(_grp, dict) and
                        _grp.get("name") in ("h1b-llm-workers", b"h1b-llm-workers")):
                    _h1b_lag = int(_grp.get("lag") or 0)
                    _h1b_pel = int(_grp.get("pending") or 0)
                    break
        except Exception:
            pass  # stream may not exist yet
        if _h1b_lag is None or _h1b_pel is None:
            _row("WARNING", "llm:h1b:disambiguate",
                 "metrics unavailable — stream or consumer group not found yet")
            warnings += 1
        else:
            _h1b_idle = _h1b_lag == 0 and _h1b_pel == 0
            _h1b_msg  = (f"lag={_h1b_lag}  pel={_h1b_pel}" +
                         (" — idle" if _h1b_idle else ""))
            _row("OK", "llm:h1b:disambiguate", _h1b_msg)
    except Exception as _qdepth_err:
        _row("WARNING", "queue depths", f"Could not read ancillary queue depths: {_qdepth_err}")
        warnings += 1

    # ── ATS PIPELINE (head-check / enrichment / discovery) ────────────────────
    _section("ATS PIPELINE  (workers + queues)")
    try:
        from config import ATS_HEALTH_DLQ_WARN
        for _lane in _ats_lanes():
            try:
                for _lvl, _lbl, _msg in check_ats_lane(r, _lane, now, ATS_HEALTH_DLQ_WARN):
                    _row(_lvl, _lbl, _msg)
                    if _lvl in ("ERROR", "CRITICAL"):
                        errors += 1
                    elif _lvl == "WARNING":
                        warnings += 1
            except Exception as _lane_err:
                _row("WARNING", _lane["name"], f"check failed: {_lane_err}")
                warnings += 1
    except Exception as _ats_err:
        _row("WARNING", "ats pipeline", f"Could not run ATS pipeline checks: {_ats_err}")
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

    # ── H1B PIPELINE METRICS ─────────────────────────────────────────────────
    _section("H1B PIPELINE METRICS  (last 7 days)")
    conn = None
    try:
        from db.connection import get_conn as _get_conn
        conn = _get_conn()

        # Public domain breakdown (enrichment worker) — newest run per fein only
        pd_rows = conn.execute("""
            SELECT public_domain_method, COUNT(*) AS n
            FROM (
                SELECT DISTINCT ON (employer_fein) public_domain_method
                FROM h1b_enrichment_metrics
                WHERE worker = 'domain_enrichment'
                  AND run_at > NOW() - INTERVAL '7 days'
                ORDER BY employer_fein, run_at DESC
            ) sub
            GROUP BY public_domain_method
            ORDER BY n DESC
        """).fetchall()

        # Career URL breakdown — newest run per fein that has a non-null
        # careers_source (NOT NULL filtered *inside* the subquery so the
        # DISTINCT ON pick isn't a null-source row from a later run; same fix
        # shape as pd_rows above). Each company counted exactly once, under
        # its most recent known source — avoids double-counting a company
        # under two sources when careers_source changed within the window.
        cu_rows = conn.execute("""
            SELECT careers_source, COUNT(*) AS n
            FROM (
                SELECT DISTINCT ON (employer_fein) careers_source
                FROM h1b_enrichment_metrics
                WHERE run_at > NOW() - INTERVAL '7 days'
                  AND careers_source IS NOT NULL
                ORDER BY employer_fein, run_at DESC
            ) sub
            GROUP BY careers_source
            ORDER BY n DESC
        """).fetchall()

        # ATS detection breakdown — same approach.
        ats_rows = conn.execute("""
            SELECT ats_source, COUNT(*) AS n
            FROM (
                SELECT DISTINCT ON (employer_fein) ats_source
                FROM h1b_enrichment_metrics
                WHERE run_at > NOW() - INTERVAL '7 days'
                  AND ats_source IS NOT NULL
                ORDER BY employer_fein, run_at DESC
            ) sub
            GROUP BY ats_source
            ORDER BY n DESC
        """).fetchall()

        # Totals
        pd_total = sum(r["n"] for r in pd_rows)

        # Attempted / Found / Missed denominator model (docs/enrichment_discovery_design.md
        # §11 "Attempted / Found / Missed", agreed 2026-09-22). "attempted" is identical for
        # all three metrics — it's pd_total's population (every FEIN that entered the pipeline
        # via a domain_enrichment run this window), since a FEIN that doesn't resolve in
        # enrichment's Phase 3/6 is guaranteed to get a discovery Phase 1/7 pass too (§9
        # enrichment→discovery forward). "found" is current live state, read once — not an
        # incrementally-tracked counter — so it doesn't matter whether enrichment or discovery
        # (or which of ATS's two attempt phases, 6 or 7) ultimately resolved it.
        cu_total = ats_total = 0
        if pd_total:
            live_rows = conn.execute("""
                SELECT
                    (SELECT COUNT(*) FROM fein_domain_map f
                      WHERE f.careers_url IS NOT NULL
                        AND f.employer_fein IN (
                            SELECT DISTINCT employer_fein FROM h1b_enrichment_metrics
                            WHERE worker = 'domain_enrichment' AND run_at > NOW() - INTERVAL '7 days'
                        )) AS cu_found,
                    (SELECT COUNT(DISTINCT c.employer_fein) FROM company_ats c
                      WHERE c.employer_fein IN (
                            SELECT DISTINCT employer_fein FROM h1b_enrichment_metrics
                            WHERE worker = 'domain_enrichment' AND run_at > NOW() - INTERVAL '7 days'
                        )) AS ats_found
            """).fetchone()
            cu_total  = live_rows["cu_found"]
            ats_total = live_rows["ats_found"]

        if pd_total == 0 and not cu_rows and not ats_rows:
            _row("WARNING", "h1b metrics", "no data yet — workers haven't run")
            warnings += 1
        else:
            def _breakdown(rows, total):
                if not total:
                    return "no data"
                return "  ".join(
                    f"{list(r.values())[0] or '?'} {list(r.values())[1] / total * 100:.0f}%"
                    for r in rows
                )

            # Public domain — NULL method (worker crashed before writing) counts as unresolved
            if pd_total == 0:
                _row("WARNING", "public domain", "no data in last 7 days — enrichment worker may not have run")
                warnings += 1
            else:
                no_signal   = next((r["n"] for r in pd_rows if r["public_domain_method"] == "no_signal"), 0)
                null_method = next((r["n"] for r in pd_rows if r["public_domain_method"] is None), 0)
                pd_found    = pd_total - no_signal - null_method
                pd_detail = _breakdown(pd_rows, pd_total)
                if (no_signal + null_method) / pd_total > 0.15:
                    _row("WARNING", "public domain",
                         f"{pd_found}/{pd_total} resolved  {pd_detail}")
                    warnings += 1
                else:
                    _row("OK", "public domain",
                         f"{pd_found}/{pd_total} resolved  {pd_detail}")

            # Career URL — attempted population = pd_total; found = live fein_domain_map state
            if pd_total == 0:
                _row("WARNING", "career URL", "no data in last 7 days")
                warnings += 1
            elif cu_total == 0:
                _row("WARNING", "career URL", f"0/{pd_total} found in last 7 days")
                warnings += 1
            else:
                cu_detail = _breakdown(cu_rows, sum(r["n"] for r in cu_rows))
                _row("OK", "career URL",
                     f"{cu_total}/{pd_total} found  {cu_detail}")

            # ATS detection — attempted population = pd_total; found = live company_ats state
            if pd_total == 0:
                _row("WARNING", "ATS detected", "no data in last 7 days")
                warnings += 1
            elif ats_total == 0:
                _row("WARNING", "ATS detected", f"0/{pd_total} found in last 7 days")
                warnings += 1
            else:
                ats_detail = _breakdown(ats_rows, sum(r["n"] for r in ats_rows))
                _row("OK", "ATS detected",
                     f"{ats_total}/{pd_total} found  {ats_detail}")

    except Exception as exc:
        _row("WARNING", "h1b metrics", f"DB query failed: {exc}")
        warnings += 1
    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass

    # ── HUNG WORKERS ─────────────────────────────────────────────────────────
    _section("HUNG WORKERS  (heartbeat alive, no progress update)")
    try:
        from workers.watchdog import check_hung_workers
        _hung_issues = check_hung_workers(r)
        if _hung_issues:
            for _hi in _hung_issues:
                _row(_hi.level, "hung workers", _hi.message)
                warnings += 1
        else:
            _row("OK", "hung workers", "none detected")
    except Exception as _hung_err:
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
