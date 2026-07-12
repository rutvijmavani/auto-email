"""
workers/watchdog.py — Pipeline health watchdog with self-healing and email alerts.

─── Behaviour overview ───────────────────────────────────────────────────────

Every WATCHDOG_INTERVAL_S (5 min):

  1. Run all health checks (Redis, PostgreSQL, workers, queues, bloom, coverage)
  2. For each ERROR/CRITICAL issue found:
       a. Is this NEW (wasn't broken last cycle)?
              → attempt self-heal (restart worker or rebuild queue)
              → send "Attempting auto-heal" email
       b. Was a heal attempted last cycle — did it WORK?
              → send "✅ Auto-healed" email
       c. Heal attempted N times, still broken?
              → send "🆘 ESCALATION — manual intervention required" email
              → stop retrying (don't spam restarts)
  3. For each issue that RESOLVED on its own (no heal needed):
       → send "✅ Resolved" email

─── Self-healing actions ─────────────────────────────────────────────────────

  Under systemd (production):
    worker:scheduler         → sudo systemctl restart recruiter-scheduler
    worker:scan_worker       → sudo systemctl restart recruiter-scheduler
    worker:detail_worker     → sudo systemctl restart recruiter-scheduler
    worker:fullscan_worker   → sudo systemctl restart recruiter-scheduler
    queue:poll:adaptive      → python pipeline.py --rebuild  (foreground)
    queue:poll:fullscan      → python pipeline.py --rebuild  (foreground)

  Without systemd (dev / cron):
    worker:* → spawned as detached background process (start_new_session=True)
    queue:*  → same --rebuild foreground command

  Individual workers are not separate systemd units — they are children of
  recruiter-scheduler.  Restarting the scheduler unit (which restarts in 30s) is
  the cleanest way to recreate the full managed pool.

─── Escalation ───────────────────────────────────────────────────────────────

  After HEAL_MAX_ATTEMPTS (3) failed restarts within HEAL_ATTEMPT_WINDOW (30 min),
  the watchdog sends an escalation email and stops retrying.  The escalation
  flag expires after 24h, at which point the watchdog will try again if the
  issue persists.

─── Running ──────────────────────────────────────────────────────────────────

  python -m workers.watchdog              # run forever (checks every 5 min)
  python -m workers.watchdog --once       # single check cycle then exit (cron)
  python -m workers.watchdog --status     # print status table, no email, no heal
  python -m workers.watchdog --no-heal    # alerts only, no auto-restart

  Add to crontab for zero-downtime coverage:
    */5 * * * * cd /home/opc/mail && source venv/bin/activate && \
               python -m workers.watchdog --once >> /tmp/watchdog.log 2>&1

─── Architecture doc reference ───────────────────────────────────────────────

  Section 5  — Two-layer scheduler redesign (Redis Streams + PEL)
  Section 15 — Redis key reference
  Section 18 — Resilience: Worker failures
  pending_work.md Phase 3 — Reliability Layer
"""

import html as _html_lib
import json
import os
import shutil
import smtplib
import subprocess
import sys
import time
import logging
from datetime import datetime
from pathlib import Path
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import Optional

from workers.redis_client import get_redis, ping
from config import (
    REDIS_STREAM_ADAPTIVE,
    REDIS_STREAM_FULLSCAN,
    REDIS_POLL_ADAPTIVE,
    REDIS_POLL_FULLSCAN,
    REDIS_DETAIL_ADAPTIVE,
    REDIS_DETAIL_FULLSCAN,
    STREAM_CONSUMER_GROUP,
    EMAIL,
    APP_PASSWORD,
)

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────
# PATHS — derived at import time so every
# subprocess uses the same venv
# ─────────────────────────────────────────

_PROJECT_ROOT    = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_PYTHON_EXE      = sys.executable   # same interpreter / venv as this watchdog
# Use a project-owned logs directory rather than world-writable /tmp to avoid
# symlink attacks on shared hosts.  Directory is created if it doesn't exist.
_LOG_DIR         = os.path.join(_PROJECT_ROOT, "logs")
os.makedirs(_LOG_DIR, exist_ok=True)

# When running under systemd the scheduler unit manages all workers.
# Prefer `sudo systemctl restart recruiter-scheduler` over spawning a raw process —
# systemd tracks PID, handles ordering, and writes to journald.
# Fall back to direct subprocess spawn when systemd is not available (dev / cron).
_SYSTEMCTL = shutil.which("systemctl") or "/usr/bin/systemctl"

def _systemd_available() -> bool:
    """Check systemd availability at call time, not import time.

    Evaluating once at import risks a False result during the boot race window
    where the watchdog starts before /run/systemd/system is mounted. A stale
    False would cause the watchdog to spawn detached subprocesses (ghost workers)
    for its entire lifetime even after systemd is fully initialised.
    """
    return os.path.isfile(_SYSTEMCTL) and os.path.isdir("/run/systemd/system")

# Keep the module-level name for backward-compat with _print_status / logging.
_SYSTEMD_AVAILABLE = _systemd_available()

# Managed systemd unit names — used for is-active checks and restarts
_UNIT_SCHEDULER = "recruiter-scheduler"
_UNIT_WATCHDOG  = "recruiter-watchdog"


# ─────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────

WATCHDOG_INTERVAL_S   = 300     # 5 min between checks
ALERT_COOLDOWN_S      = 3600    # 1 h — don't repeat same alert type within this
HEAL_MAX_ATTEMPTS     = 3       # max auto-restarts before escalation
HEAL_ATTEMPT_WINDOW   = 1800    # 30 min — attempt counter TTL
HEAL_COOLDOWN_S       = 150     # min gap between two heal attempts (avoids rapid loop)
ESCALATION_COOLDOWN   = 86400   # 24 h — re-escalation window
RESOLVE_COOLDOWN_S    = 7200    # 2 h — min gap between "RESOLVED" emails

# Worker heartbeat absence threshold (seconds) before watchdog considers worker dead
HEARTBEAT_DEAD_AFTER = {
    "scheduler":       20,
    "scan_worker":     45,
    "detail_worker":   45,
    "fullscan_worker": 1900,   # scans can legitimately take 30 min
}

# Consecutive rapid-death thresholds for worker pool health.
# Shared with scripts/health_check.py so both tools agree on severity.
WARN_DEATHS = 3
ERR_DEATHS  = 5

# Queue health thresholds
#
# Poll queues — velocity/delta tracking across consecutive watchdog cycles.
# We ask "is the queue making forward progress?" not "how many are overdue?".
# Absolute overdue counts are meaningless without fleet-size context AND
# don't distinguish a healthy-but-busy queue from a stalled one.
# State is persisted in Redis between cycles so deltas survive watchdog restarts.
WATCHDOG_SNAPSHOT_KEY = "watchdog:queue_snapshot"
WATCHDOG_SNAPSHOT_TTL = WATCHDOG_INTERVAL_S * 2   # expires if watchdog skips a cycle

# Detail queue depth — absolute count is correct here: it is a throughput
# metric, not a fleet-size metric.  500 backed-up jobs means the same lag
# regardless of how many companies are registered.
DETAIL_QUEUE_WARN  = 100
DETAIL_QUEUE_ALERT = 500

PEL_WARN_AGE_MS         = 10 * 60 * 1000
PEL_ALERT_AGE_MS        = 30 * 60 * 1000
COVERAGE_MISS_ALERT_PCT = 0.25


# ─────────────────────────────────────────
# SELF-HEAL COMMAND TABLE
# ─────────────────────────────────────────

def _get_heal_action(alert_type: str) -> Optional[dict]:
    """
    Return the heal action for an alert type, or None if not auto-healable.

    Returns a dict:
        cmd_args    : list[str]  — argv for subprocess.Popen
        log_file    : str        — path to append stdout/stderr
        description : str        — human-readable description for the email
        foreground  : bool       — if True, run synchronously (default False)

    Systemd mode vs subprocess mode
    ────────────────────────────────
    When systemd is detected (_SYSTEMD_AVAILABLE=True):
      • scheduler dead  → `sudo systemctl restart recruiter-scheduler`
                          systemd owns the unit; it will spawn all workers
                          and record PID / logs in journald.
      • individual worker dead (but scheduler alive) — these workers are
        children of the scheduler process, NOT separate systemd units.
        A dead child means the scheduler's pool had an exception.
        Restarting the scheduler via systemd is the cleanest fix since it
        re-creates the whole managed pool cleanly.
        We restart recruiter-scheduler (idempotent — if scheduler is already
        alive, restarting it briefly is still safe and ensures a fresh pool).

    Without systemd (dev machine, cron mode):
      • Workers are spawned directly as detached background processes.
      • Uses start_new_session=True so they survive watchdog exit.
    """
    if _systemd_available():
        # ── Systemd path — prefer managed restarts ────────────────────────
        # All workers (scan, detail, fullscan) are children of recruiter-scheduler.
        # A dead individual worker means the scheduler pool lost a child;
        # restarting the scheduler unit recreates the full managed pool.
        #
        # IMPORTANT: If the service is in "failed" state (StartLimitBurst hit),
        # `systemctl restart` alone is blocked.  We must `reset-failed` first.
        # We do this as a shell string executed via bash so both commands run
        # atomically in one subprocess call, with the exact _SYSTEMCTL path
        # that matches the sudoers NOPASSWD grant.
        #
        # Sudoers grants:
        #   opc ALL=(ALL) NOPASSWD: <_SYSTEMCTL> restart recruiter-scheduler
        #   opc ALL=(ALL) NOPASSWD: <_SYSTEMCTL> reset-failed recruiter-scheduler
        def _systemd_restart_cmd(unit: str) -> list:
            """
            Return argv for a safe systemd restart that handles failed state.
            Uses bash -c so reset-failed + restart run as one call.
            Both systemctl invocations use _SYSTEMCTL (exact path for sudoers).
            """
            return [
                "bash", "-c",
                f"sudo {_SYSTEMCTL} reset-failed {unit} 2>/dev/null || true; "
                f"sudo {_SYSTEMCTL} restart {unit}",
            ]

        worker_via_systemd = {
            "cmd_args":    _systemd_restart_cmd(_UNIT_SCHEDULER),
            "log_file":    f"{_LOG_DIR}/systemctl_restart.log",
            "description": f"Restarted {_UNIT_SCHEDULER} via systemd (respawns all workers)",
            "foreground":  True,
        }
        actions: dict = {
            "worker_scan_worker":     worker_via_systemd,
            "worker_detail_worker":   worker_via_systemd,
            "worker_fullscan_worker": worker_via_systemd,
            "worker_scheduler": {
                "cmd_args":    _systemd_restart_cmd(_UNIT_SCHEDULER),
                "log_file":    f"{_LOG_DIR}/systemctl_restart.log",
                "description": f"Restarted {_UNIT_SCHEDULER} via systemd",
                "foreground":  True,
            },
            # systemd_service_* alert types from check_systemd_services()
            f"systemd_{_UNIT_SCHEDULER}": {
                "cmd_args":    _systemd_restart_cmd(_UNIT_SCHEDULER),
                "log_file":    f"{_LOG_DIR}/systemctl_restart.log",
                "description": f"Restarted {_UNIT_SCHEDULER} via systemd (service was inactive/failed)",
                "foreground":  True,
            },
            # Queue empty → rebuild.  Queue stall → restart workers (rebuild
            # won't help if the queue has entries but workers aren't draining it).
            "queue_poll_adaptive_empty": {
                "cmd_args":    [_PYTHON_EXE, "pipeline.py", "--rebuild"],
                "log_file":    f"{_LOG_DIR}/rebuild.log",
                "description": "Ran pipeline.py --rebuild (poll:adaptive was empty)",
                "foreground":  True,
                "timeout_s":   300,  # rebuild can take several minutes
            },
            "queue_poll_adaptive_stall": worker_via_systemd,
            "queue_poll_fullscan_empty": {
                "cmd_args":    [_PYTHON_EXE, "pipeline.py", "--rebuild"],
                "log_file":    f"{_LOG_DIR}/rebuild.log",
                "description": "Ran pipeline.py --rebuild (poll:fullscan was empty)",
                "foreground":  True,
                "timeout_s":   300,
            },
            "queue_poll_fullscan_stall": worker_via_systemd,
            # Detail queues — depth growing/stalled at CRITICAL level.
            # Restart the scheduler (which manages detail_worker pool) via systemd.
            "queue_detail_adaptive": worker_via_systemd,
            "queue_detail_fullscan": worker_via_systemd,
            # Orphaned PEL entries — consumer died before XAUTOCLAIM reclaimed the
            # message.  Restarting the scheduler re-triggers adaptive_loop which
            # runs XAUTOCLAIM and also respawns the workers.
            "stream_adaptive_pel": worker_via_systemd,
            "stream_fullscan_pel": worker_via_systemd,
        }
    else:
        # ── Subprocess path — dev / cron mode ────────────────────────────
        # Workers are spawned as detached background processes with
        # start_new_session=True so they survive watchdog exit.
        actions = {
            "worker_scan_worker": {
                "cmd_args":    [_PYTHON_EXE, "-m", "workers.scan_worker"],
                "log_file":    f"{_LOG_DIR}/scan_worker.log",
                "description": "Restarted scan_worker process (subprocess)",
            },
            "worker_detail_worker": {
                "cmd_args":    [_PYTHON_EXE, "-m", "workers.detail_worker"],
                "log_file":    f"{_LOG_DIR}/detail_worker.log",
                "description": "Restarted detail_worker process (subprocess)",
            },
            "worker_fullscan_worker": {
                "cmd_args":    [_PYTHON_EXE, "-m", "workers.fullscan"],
                "log_file":    f"{_LOG_DIR}/fullscan_worker.log",
                "description": "Restarted fullscan worker process (subprocess)",
            },
            "worker_scheduler": {
                "cmd_args":    [_PYTHON_EXE, "pipeline.py", "--scheduler"],
                "log_file":    f"{_LOG_DIR}/scheduler.log",
                "description": "Restarted scheduler process (subprocess)",
            },
            "queue_poll_adaptive_empty": {
                "cmd_args":    [_PYTHON_EXE, "pipeline.py", "--rebuild"],
                "log_file":    f"{_LOG_DIR}/rebuild.log",
                "description": "Ran pipeline.py --rebuild (poll:adaptive was empty)",
                "foreground":  True,
                "timeout_s":   300,
            },
            "queue_poll_adaptive_stall": {
                "cmd_args":    [_PYTHON_EXE, "pipeline.py", "--scheduler"],
                "log_file":    f"{_LOG_DIR}/scheduler.log",
                "description": "Restarted scheduler (poll:adaptive stalled)",
            },
            "queue_poll_fullscan_empty": {
                "cmd_args":    [_PYTHON_EXE, "pipeline.py", "--rebuild"],
                "log_file":    f"{_LOG_DIR}/rebuild.log",
                "description": "Ran pipeline.py --rebuild (poll:fullscan was empty)",
                "foreground":  True,
                "timeout_s":   300,
            },
            "queue_poll_fullscan_stall": {
                "cmd_args":    [_PYTHON_EXE, "pipeline.py", "--scheduler"],
                "log_file":    f"{_LOG_DIR}/scheduler.log",
                "description": "Restarted scheduler (poll:fullscan stalled)",
            },
            # Detail queues — depth growing/stalled at CRITICAL level.
            "queue_detail_adaptive": {
                "cmd_args":    [_PYTHON_EXE, "-m", "workers.detail_worker"],
                "log_file":    f"{_LOG_DIR}/detail_worker.log",
                "description": "Restarted detail_worker (detail:adaptive critically elevated)",
            },
            "queue_detail_fullscan": {
                "cmd_args":    [_PYTHON_EXE, "-m", "workers.detail_worker"],
                "log_file":    f"{_LOG_DIR}/detail_worker.log",
                "description": "Restarted detail_worker (detail:fullscan critically elevated)",
            },
        }
    return actions.get(alert_type)


# ─────────────────────────────────────────
# REDIS STATE KEYS
# ─────────────────────────────────────────
# watchdog:alert:{type}           — cooldown flag after alert sent (1h TTL)
# watchdog:heal_count:{type}      — consecutive heal attempts (30min TTL)
# watchdog:heal_last:{type}       — timestamp of last heal attempt
# watchdog:heal_pid:{type}        — PID of spawned process (if applicable)
# watchdog:escalated:{type}       — escalation already sent (24h TTL)
# watchdog:resolved:{type}        — "resolved" email already sent (2h TTL)
# watchdog:active_issues          — JSON list of alert_types from last cycle

def _rkey(prefix: str, alert_type: str) -> str:
    return f"watchdog:{prefix}:{alert_type}"


# ─────────────────────────────────────────
# EMAIL
# ─────────────────────────────────────────

def _send_email(subject: str, body_html: str, *, dedup_key: str,
                dedup_ttl: int, r) -> bool:
    """
    Send an HTML email with Redis-based deduplication.

    Returns True if sent, False if suppressed by the cooldown window.
    """
    if not EMAIL or not APP_PASSWORD:
        logger.error("watchdog: EMAIL or APP_PASSWORD not configured — cannot send")
        return False

    # Reserve the dedup key atomically (NX) BEFORE sending so concurrent
    # watchdog processes cannot both pass the check and send duplicate emails.
    # If another process already set the key, suppress immediately.
    # On send failure we delete the key so the alert can be retried.
    _reserved = None
    try:
        _reserved = r.set(dedup_key, "1", ex=dedup_ttl, nx=True)
        if _reserved is None:
            logger.debug("watchdog: suppressing email (cooldown) key=%r", dedup_key)
            return False
    except Exception as exc:
        # Redis unavailable — proceed optimistically (single-instance dev mode)
        logger.warning("watchdog: dedup reserve failed, proceeding with send: %s", exc)

    try:
        now_str  = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        msg      = MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"]    = EMAIL
        msg["To"]      = EMAIL

        full_html = f"""<!DOCTYPE html>
<html><head><meta charset="UTF-8"></head>
<body style="margin:0;padding:0;background:#f0f2f5;
             font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Arial,sans-serif;">
  <table width="100%" cellpadding="0" cellspacing="0">
    <tr><td align="center" style="padding:24px 0;">
      <table width="600" cellpadding="0" cellspacing="0">
        <tr>
          <td style="background:#0f172a;border-radius:10px 10px 0 0;padding:20px 28px;">
            <span style="color:#ffffff;font-size:16px;font-weight:700;">
              {subject}
            </span>
            <span style="color:#94a3b8;font-size:12px;float:right;padding-top:3px;">
              {now_str}
            </span>
          </td>
        </tr>
        <tr>
          <td style="background:#ffffff;padding:24px 28px;
                     border:1px solid #e2e8f0;border-top:none;">
            {body_html}
          </td>
        </tr>
        <tr>
          <td style="background:#f8fafc;border-radius:0 0 10px 10px;
                     border:1px solid #e2e8f0;border-top:none;
                     padding:12px 28px;text-align:center;">
            <span style="color:#64748b;font-size:11px;">
              Recruiter Pipeline Watchdog &nbsp;·&nbsp; {now_str}
              &nbsp;·&nbsp; To check now: <code>python scripts/health_check.py</code>
            </span>
          </td>
        </tr>
      </table>
    </td></tr>
  </table>
</body></html>"""

        msg.attach(MIMEText(full_html, "html"))

        with smtplib.SMTP("smtp.gmail.com", 587, timeout=30) as srv:
            srv.starttls()
            srv.login(EMAIL, APP_PASSWORD)
            srv.send_message(msg)

        logger.info("watchdog: email sent — subject=%r", subject)
        return True

    except Exception as exc:
        logger.error("watchdog: email send failed: %s", exc)
        # Release the reserved dedup key so the alert can be retried next cycle.
        if _reserved is not None:
            try:
                r.delete(dedup_key)
            except Exception:
                pass
        return False


def _html_issue_table(issues: list) -> str:
    """Build an HTML table of issues for email bodies."""
    rows = ""
    for issue in issues:
        color = {"CRITICAL": "#ef4444", "ERROR": "#ef4444",
                 "WARNING": "#f59e0b", "OK": "#22c55e"}.get(issue.level, "#64748b")
        _esc_cat = _html_lib.escape(issue.category)
        _esc_msg = _html_lib.escape(issue.message)
        fix_html = (
            f'<div style="margin-top:4px;font-family:monospace;font-size:12px;'
            f'color:#475569;background:#f1f5f9;padding:4px 8px;border-radius:4px;">'
            f'Fix: {_html_lib.escape(issue.fix)}</div>'
        ) if issue.fix else ""

        rows += f"""<tr>
          <td style="padding:8px 12px;vertical-align:top;white-space:nowrap;">
            <span style="color:{color};font-weight:700;font-size:12px;">
              {issue.emoji()} {issue.level}
            </span>
          </td>
          <td style="padding:8px 12px;vertical-align:top;">
            <span style="font-weight:600;color:#1e293b;font-size:13px;">
              {_esc_cat}
            </span>
            <div style="color:#475569;font-size:13px;margin-top:2px;">
              {_esc_msg}
            </div>{fix_html}
          </td>
        </tr>"""

    return (
        f'<table width="100%" style="border:1px solid #e2e8f0;'
        f'border-radius:8px;border-collapse:collapse;">'
        f'<thead><tr style="background:#f8fafc;">'
        f'<th style="text-align:left;padding:8px 12px;color:#64748b;'
        f'font-size:11px;font-weight:700;text-transform:uppercase;">Level</th>'
        f'<th style="text-align:left;padding:8px 12px;color:#64748b;'
        f'font-size:11px;font-weight:700;text-transform:uppercase;">Detail</th>'
        f'</tr></thead><tbody>{rows}</tbody></table>'
    )


# ─────────────────────────────────────────
# HEALTH CHECKS
# ─────────────────────────────────────────

class Issue:
    CRITICAL = "CRITICAL"
    ERROR    = "ERROR"
    WARNING  = "WARNING"
    OK       = "OK"

    def __init__(self, level: str, category: str, message: str,
                 fix: str = "", alert_type: str = ""):
        self.level      = level
        self.category   = category
        self.message    = message
        self.fix        = fix
        # alert_type is the dedup/heal key — derived from category if not given
        self.alert_type = alert_type or category.replace(":", "_").replace(" ", "_")

    def is_alertable(self) -> bool:
        return self.level in (self.CRITICAL, self.ERROR)

    def emoji(self) -> str:
        return {"CRITICAL": "🔴", "ERROR": "🔴",
                "WARNING": "🟡", "OK": "🟢"}.get(self.level, "⚪")

    def __str__(self) -> str:
        s = f"[{self.level}] {self.category}: {self.message}"
        return s + (f"\n  Fix: {self.fix}" if self.fix else "")


def check_redis() -> tuple:
    try:
        r       = get_redis()
        info    = r.info("server")
        version = info.get("redis_version", "?")
        mem     = info.get("used_memory_human", "?")
        return True, f"v{version} mem={mem}"
    except Exception as exc:
        return False, str(exc)


def check_postgres() -> tuple:
    try:
        from db.db import get_conn
        conn = get_conn()
        try:
            row = conn.execute("SELECT COUNT(*) AS cnt FROM job_postings").fetchone()
        finally:
            conn.close()
        return True, f"{row['cnt']:,} jobs in DB"
    except Exception as exc:
        return False, str(exc)


def check_worker_heartbeats(r) -> list:
    """
    Check worker health via two independent signals:

    1. worker:alive:scheduler  — fast detection (TTL=15s, written every ~1s).
       Scheduler dead → key expires → ERROR immediately.

    2. scheduler:health  — rich pool state published by the scheduler on every
       pool event (death, respawn, scale up/down).  TTL=10min — also expires
       if scheduler dies, giving the watchdog a second confirmation signal.
       Contains per-type alive counts and consecutive_deaths counters, which
       let the watchdog see "scheduler is alive but struggling to keep workers
       up" — a situation the heartbeat key alone cannot detect.

    Per-worker per-PID heartbeat keys (worker:alive:{type}:{pid}) are written
    by individual workers and used for observability/display only.  All alerting
    decisions come from the two keys above.

    Responsibility split:
      Scheduler  — owns worker lifecycle (spawn / replace / scale).
                   Publishes scheduler:health so the watchdog can see inside.
      Watchdog   — monitors the scheduler and escalates when it can't recover.
                   Never tries to manage individual worker processes directly.
    """
    issues  = []
    now     = time.time()
    FIX_CMD = "sudo systemctl restart recruiter-scheduler"

    # ── 1. Scheduler per-loop heartbeats ─────────────────────────────────────
    # Each scheduler loop (adaptive_loop, fullscan_loop) writes its own key.
    # Checking them independently means a hung loop is detected even while the
    # other loop keeps the process alive — the old single shared key masked
    # one-loop hangs because the surviving loop kept refreshing it.
    dead_after = HEARTBEAT_DEAD_AFTER["scheduler"]

    _loop_raws = {
        "adaptive": r.get("worker:alive:scheduler:adaptive"),
        "fullscan": r.get("worker:alive:scheduler:fullscan"),
    }

    for loop_name, raw in _loop_raws.items():
        category   = f"worker:scheduler:{loop_name}"
        alert_type = f"worker_scheduler_{loop_name}"
        if raw is None:
            issues.append(Issue(
                Issue.ERROR,
                category,
                f"scheduler {loop_name}_loop heartbeat MISSING — loop is dead or never started",
                FIX_CMD,
                alert_type=alert_type,
            ))
        else:
            try:
                d   = json.loads(raw)
                age = now - d.get("ts", now)
                if age > dead_after:
                    issues.append(Issue(
                        Issue.ERROR,
                        category,
                        f"scheduler {loop_name}_loop heartbeat STALE — last write {age:.0f}s ago "
                        f"(threshold {dead_after}s). Loop may be hung.",
                        FIX_CMD,
                        alert_type=alert_type,
                    ))
                else:
                    issues.append(Issue(Issue.OK, category,
                        f"alive pid={d.get('pid','?')} dispatched={d.get('dispatched',0)} "
                        f"heartbeat {age:.0f}s ago"))
            except Exception:
                issues.append(Issue(Issue.OK, category,
                    "alive (heartbeat key present)"))

    # If BOTH loop keys are missing the scheduler process itself is down —
    # workers are also dead, so no point reading scheduler:health.
    if all(v is None for v in _loop_raws.values()):
        return issues

    # ── 2. Pool health from scheduler:health ─────────────────────────────────
    _HEALTH_MISS_KEY       = _rkey("pool_health_miss_count", "worker_pool_health")
    _HEALTH_MISS_THRESHOLD = 3   # consecutive misses before escalating to ERROR
    health_raw   = r.get("scheduler:health")

    if health_raw is None:
        # Key expired — scheduler stopped publishing (likely just started or
        # is about to die; the faster heartbeat check above will catch a death).
        try:
            miss_count = int(r.incr(_HEALTH_MISS_KEY))
            r.expire(_HEALTH_MISS_KEY, 3600)
        except Exception:
            miss_count = 1
        severity = Issue.ERROR if miss_count >= _HEALTH_MISS_THRESHOLD else Issue.WARNING
        issues.append(Issue(
            severity,
            "worker:pool_health",
            f"scheduler:health key missing for {miss_count} consecutive cycle(s) — "
            "pool state unknown (scheduler may have just started or died)",
            alert_type="worker_pool_health",
        ))
        return issues
    # Key present — reset consecutive miss counter
    try:
        r.delete(_HEALTH_MISS_KEY)
    except Exception:
        pass

    try:
        health   = json.loads(health_raw)
        pool     = health.get("pool", {})
        pool_age = now - health.get("ts", now)

        for ptype, label_suffix in [
            ("scan",     "scan_worker"),
            ("detail",   "detail_worker"),
            ("fullscan", "fullscan_worker"),
        ]:
            info   = pool.get(ptype, {})
            alive  = info.get("alive", 0)
            consec = info.get("consecutive_deaths", 0)
            total  = info.get("total_replacements", 0)
            label  = f"worker:{label_suffix}"

            # ── Collect live PIDs for display (best-effort, non-alerting) ─────
            try:
                live_pids = []
                cursor = 0
                while True:
                    cursor, keys = r.scan(
                        cursor, match=f"worker:alive:{label_suffix}:*", count=50
                    )
                    for k in keys:
                        kraw = r.get(k)
                        if kraw:
                            kd = json.loads(kraw)
                            live_pids.append(str(kd.get("pid", "?")))
                    if cursor == 0:
                        break
            except Exception:
                live_pids = []

            pid_str    = f"pids=[{','.join(live_pids)}]" if live_pids else ""
            detail_str = (
                f"{alive} alive  {pid_str}  "
                f"total_replacements={total}  pool_age={pool_age:.0f}s"
            ).strip()

            if consec >= ERR_DEATHS:
                issues.append(Issue(
                    Issue.ERROR,
                    label,
                    f"{detail_str}  consecutive_rapid_deaths={consec} "
                    "— workers failing on startup; pool cannot stabilize",
                    FIX_CMD,
                    alert_type=f"worker_{label_suffix}",
                ))
            elif consec >= WARN_DEATHS:
                issues.append(Issue(
                    Issue.WARNING,
                    label,
                    f"{detail_str}  consecutive_rapid_deaths={consec} "
                    "— scheduler struggling to keep workers up",
                    FIX_CMD,
                    alert_type=f"worker_{label_suffix}",
                ))
            else:
                note = (
                    f"  ({consec} recent death(s) — scheduler replacing)"
                    if consec > 0 else ""
                )
                issues.append(Issue(Issue.OK, label,
                    f"{detail_str}{note}"))

    except Exception as exc:
        issues.append(Issue(Issue.WARNING, "worker:pool_health",
            f"Could not parse scheduler:health: {exc}"))

    return issues


# ─────────────────────────────────────────
# QUEUE HEALTH — HELPERS
# ─────────────────────────────────────────

def _worker_processed(r, worker_type: str) -> Optional[int]:
    """
    Sum 'processed' counters across all alive instances of a worker type.

    With per-PID heartbeat keys (worker:alive:{type}:{pid}), multiple workers
    of the same type each maintain their own counter.  Summing gives the total
    jobs processed by the pool — used by check_queue_stall to detect stalled
    throughput across worker cycles.
    """
    try:
        total  = 0
        found  = False
        cursor = 0
        while True:
            cursor, keys = r.scan(
                cursor, match=f"worker:alive:{worker_type}:*", count=100
            )
            for key in keys:
                raw = r.get(key)
                if raw:
                    total += int(json.loads(raw).get("processed", 0))
                    found  = True
            if cursor == 0:
                break
        return total if found else None
    except Exception:
        return None


def _zset_head(r, key: str):
    """(company_str, score_float) for the lowest-scored ZSET entry, or (None, None)."""
    try:
        results = r.zrange(key, 0, 0, withscores=True)
        if results:
            c = results[0][0]
            return (c.decode() if isinstance(c, bytes) else c), float(results[0][1])
    except Exception:
        pass
    return None, None


def _fullscan_lock_active(r) -> bool:
    """True if any fullscan:lock:* key is present — a scan is running right now."""
    try:
        cursor = 0
        while True:
            cursor, keys = r.scan(cursor, match="fullscan:lock:*", count=10)
            if keys:
                return True
            if cursor == 0:
                break
        return False
    except Exception:
        return False


def _trend(delta: int) -> str:
    """↓ shrinking (good), → stable, ↑ growing (bad)."""
    return "↓" if delta < 0 else ("↑" if delta > 0 else "→")


def _stall_count(
    cur_overdue: int, prev_overdue: int,
    cur_head_c:  Optional[str],   cur_head_s:  Optional[float],
    prev_head_c: Optional[str],   prev_head_s: Optional[float],
    cur_proc:    Optional[int],   prev_proc:   Optional[int],
) -> tuple:
    """
    Return (stall_signals, valid_signals).

    Three independent signals each contribute 1 to stall_signals when they
    indicate no forward progress was made since the last watchdog cycle:

      1. Overdue count not shrinking  — always valid when overdue > 0
      2. Queue head unchanged         — valid when both current + prev head exist
      3. Processed count unchanged    — valid when both current + prev proc exist

    Caller interprets:
      stall >= 3 (of 3 valid) → ERROR  — all signals agree: stalled
      stall >= 2 (of ≥2 valid) → WARNING — likely stalling
      stall < 2                → OK    — still making progress
    """
    stall = 0
    valid = 0

    # Signal 1: overdue not shrinking
    if cur_overdue > 0:
        valid += 1
        if cur_overdue >= prev_overdue:
            stall += 1

    # Signal 2: queue head unchanged (same company, same score)
    if cur_head_c is not None and prev_head_c is not None:
        valid += 1
        same = (
            cur_head_c == prev_head_c and
            cur_head_s is not None and prev_head_s is not None and
            abs(cur_head_s - prev_head_s) < 1.0
        )
        if same:
            stall += 1

    # Signal 3: worker processed count unchanged
    if cur_proc is not None and prev_proc is not None:
        valid += 1
        if cur_proc == prev_proc:
            stall += 1

    return stall, valid


def _consumer_pid(consumer_name) -> Optional[int]:
    """
    Extract the PID from a consumer name of the form 'worker-{hostname}-{pid}'.

    Returns None if the name doesn't match the expected format.
    """
    try:
        if isinstance(consumer_name, bytes):
            consumer_name = consumer_name.decode()
        return int(consumer_name.rsplit("-", 1)[-1])
    except (ValueError, AttributeError, UnicodeDecodeError):
        return None


def _heartbeat_pid(r, worker_type: str) -> Optional[int]:
    """
    Return any live PID for this worker type, or None if no heartbeat keys exist.

    With per-PID keys (worker:alive:{type}:{pid}), we scan for any key matching
    the worker type prefix and return the first PID found.  Used as a fallback
    in _check_pel_health; the primary check there is _consumer_pid_alive().
    """
    try:
        cursor = 0
        while True:
            cursor, keys = r.scan(
                cursor, match=f"worker:alive:{worker_type}:*", count=10
            )
            for key in keys:
                raw = r.get(key)
                if raw:
                    pid = json.loads(raw).get("pid")
                    return int(pid) if pid else None
            if cursor == 0:
                break
        return None
    except Exception:
        return None


def _consumer_pid_alive(r, worker_type: str, consumer_name) -> bool:
    """
    Return True if a live heartbeat key exists for this stream consumer.

    Standard worker consumer format: "worker-{hostname}-{pid}"
    Heartbeat key format:            "worker:alive:{type}:{hostname}:{pid}"
    Both include the hostname so PID reuse across machines cannot produce a false positive.

    Scheduler consumer format: "scheduler-{hostname}-{pid}"
    Scheduler heartbeat keys:  "worker:alive:scheduler:adaptive" and
                               "worker:alive:scheduler:fullscan" (no hostname:pid in key).
    The scheduler PID is stored in the heartbeat value — matched against the consumer PID
    to distinguish a restarted scheduler (new PID) from the original consumer (old PID).
    """
    if not consumer_name:
        return False
    try:
        if isinstance(consumer_name, bytes):
            consumer_name = consumer_name.decode()

        # Scheduler consumer names use "scheduler-{hostname}-{pid}" but heartbeat keys
        # are "worker:alive:scheduler:adaptive/fullscan" (no hostname:pid in the key).
        if consumer_name.startswith("scheduler-"):
            pid_str = consumer_name.rsplit("-", 1)[-1]
            int(pid_str)  # validate
            for hb_key in ("worker:alive:scheduler:adaptive",
                           "worker:alive:scheduler:fullscan"):
                raw = r.get(hb_key)
                if raw:
                    try:
                        hb_pid = str(json.loads(raw).get("pid", ""))
                        if hb_pid == pid_str:
                            return True
                    except Exception:
                        pass
            return False

        # Standard format: "worker-{hostname}-{pid}"
        name, _, pid_str = consumer_name.rpartition("-")
        hostname = name.removeprefix("worker-")
        int(pid_str)   # validate — raises ValueError if not a number
        return bool(r.exists(f"worker:alive:{worker_type}:{hostname}:{pid_str}"))
    except Exception:
        return False


def _check_pel_health(r, issues: list) -> None:
    """
    PEL liveness checks for stream:adaptive and stream:fullscan.

    The core question is NOT "how old is the oldest PEL entry?" but
    "is the worker that owns the oldest PEL entry still alive?"

    Why this matters
    ────────────────
    A fullscan legitimately takes 20–30 minutes.  A time-only threshold
    (old approach: >10 min = WARNING, >30 min = ERROR) fires constantly
    on a perfectly healthy fullscan_worker mid-scan.

    The fix: cross-reference the PEL entry's consumer name (which embeds
    the worker's PID) against the live heartbeat key's PID field:

      Same PID → worker is alive and owns this job → OK, no matter the age
      Different / missing PID → consumer died → entry is orphaned

    Time thresholds (PEL_WARN_AGE_MS / PEL_ALERT_AGE_MS) only apply to
    orphaned entries — when we already know the consumer is dead and
    XAUTOCLAIM should have reclaimed the entry but hasn't yet.
    In that context, ">10 min orphaned" is meaningful and actionable.
    """
    for stream_key, label, atype, worker_type in [
        (REDIS_STREAM_ADAPTIVE, "stream:adaptive PEL",
         "stream_adaptive_pel",  "scan_worker"),
        (REDIS_STREAM_FULLSCAN, "stream:fullscan PEL",
         "stream_fullscan_pel",  "fullscan_worker"),
    ]:
        try:
            summary = r.xpending(stream_key, STREAM_CONSUMER_GROUP)
            total   = summary.get("pending", 0) if summary else 0
            if total == 0:
                issues.append(Issue(Issue.OK, label, "0 pending entries"))
                continue

            # Paginate through all PEL entries in batches so orphans beyond
            # position 10 are not silently ignored.
            _BATCH = 100
            worst_ms      = 0
            worst_orphan  = None
            alive_entries = []
            first_entry   = None
            _min_id       = "-"
            while True:
                batch = r.xpending_range(stream_key, STREAM_CONSUMER_GROUP,
                                         min=_min_id, max="+", count=_BATCH)
                if not batch:
                    break
                if first_entry is None:
                    first_entry = batch[0]
                for entry in batch:
                    e_consumer = entry.get("consumer", "")
                    if isinstance(e_consumer, bytes):
                        e_consumer = e_consumer.decode()
                    e_pid   = _consumer_pid(e_consumer)
                    e_alive = _consumer_pid_alive(r, worker_type, e_consumer)
                    e_ms    = entry.get("time_since_delivered", 0)
                    if e_alive:
                        alive_entries.append((e_consumer, e_pid, e_ms))
                    else:
                        if e_ms > worst_ms:
                            worst_ms     = e_ms
                            worst_orphan = (e_consumer, e_pid, e_ms)
                if len(batch) < _BATCH:
                    break
                last_id = batch[-1].get("message_id", b"")
                if isinstance(last_id, bytes):
                    last_id = last_id.decode()
                _min_id = f"({last_id}"

            if first_entry is None:
                issues.append(Issue(Issue.OK, label, f"{total} pending (no detail)"))
                continue

            oldest_ms = first_entry.get("time_since_delivered", 0)
            age_str = (
                f"{oldest_ms // 60000:.0f}min"
                if oldest_ms >= 60000 else f"{oldest_ms // 1000:.0f}s"
            )

            if worst_orphan is None:
                # All PEL entries have alive consumers.
                _rep_consumer, _rep_pid, _ = alive_entries[0] if alive_entries else ("?", None, 0)
                issues.append(Issue(Issue.OK, label,
                    f"{total} pending  oldest={age_str}  "
                    f"consumer={_rep_consumer} alive (pid={_rep_pid}) — in progress"))
            else:
                consumer_name, c_pid, _ = worst_orphan
                worst_age_str = (
                    f"{worst_ms // 60000:.0f}min"
                    if worst_ms >= 60000 else f"{worst_ms // 1000:.0f}s"
                )
                # Consumer is dead — entry is orphaned.  XAUTOCLAIM should
                # reclaim it on the next scheduler tick.  Time thresholds
                # now make sense: we know the consumer is gone.
                orphan_note = (
                    f"consumer={consumer_name} DEAD "
                    f"(pid={c_pid} — no heartbeat key)  worst_age={worst_age_str}"
                )
                if worst_ms > PEL_ALERT_AGE_MS:
                    issues.append(Issue(Issue.ERROR, label,
                        f"{total} pending  oldest={age_str}  {orphan_note}  "
                        "— XAUTOCLAIM may be stuck; restart scheduler",
                        "python pipeline.py --scheduler",
                        alert_type=atype,
                    ))
                elif worst_ms > PEL_WARN_AGE_MS:
                    issues.append(Issue(Issue.WARNING, label,
                        f"{total} pending  oldest={age_str}  {orphan_note}  "
                        "— awaiting XAUTOCLAIM reclaim"))
                else:
                    issues.append(Issue(Issue.OK, label,
                        f"{total} pending  oldest={age_str}  {orphan_note}  "
                        "— XAUTOCLAIM will reclaim shortly"))

        except Exception as exc:
            issues.append(Issue(Issue.WARNING, label, f"Query failed: {exc}"))


# ─────────────────────────────────────────
# QUEUE HEALTH — MAIN CHECK
# ─────────────────────────────────────────

def check_queue_health(r, persist_snapshot: bool = True) -> list:
    """
    Detect queue stalls via velocity/delta tracking across watchdog cycles.

    Approach
    ────────
    Instead of asking "how many companies are overdue?" (which doesn't scale
    and can't distinguish a busy-but-healthy queue from a stalled one), we
    ask three questions between consecutive 5-minute cycles:

      1. Did the queue head change?
         ZRANGE poll:X 0 0 WITHSCORES — if the front-of-queue company or its
         score changed, at least one job was picked up since last cycle.

      2. Is the overdue count shrinking?
         ZCOUNT poll:X -inf now — if fewer companies are overdue now than
         last cycle, workers are draining faster than companies become due.

      3. Did the worker's processed count change?
         Read from the worker heartbeat key (written by the background
         daemon thread every 10–60 s). An unchanged counter means the
         worker picked up nothing since last cycle.

    All three signals stalling → ERROR + auto-restart.
    Two signals stalling       → WARNING, watch for next cycle.
    One or zero                → OK (queue is moving, even if behind).

    Fullscan exoneration
    ────────────────────
    A fullscan legitimately takes 20–30 minutes.  Between two 5-minute
    watchdog cycles the processed count won't change and the queue head
    won't move — but the worker is perfectly healthy.  The fullscan:lock:*
    key is set while any scan is in progress.  If the lock is active,
    all stall signals are suppressed for poll:fullscan regardless of state.

    Persistence
    ───────────
    State is stored in Redis under WATCHDOG_SNAPSHOT_KEY (TTL = 2× interval).
    If the watchdog restarts or skips a cycle, the snapshot expires and the
    first run after the gap is treated as a baseline cycle (no alarms).
    """
    issues = []
    now    = time.time()

    # ── 1. Collect current state ──────────────────────────────────────────────
    adp_total        = r.zcard(REDIS_POLL_ADAPTIVE)
    adp_overdue      = r.zcount(REDIS_POLL_ADAPTIVE, "-inf", now)
    adp_head_c, adp_head_s = _zset_head(r, REDIS_POLL_ADAPTIVE)

    fs_total         = r.zcard(REDIS_POLL_FULLSCAN)
    fs_overdue       = r.zcount(REDIS_POLL_FULLSCAN, "-inf", now)
    fs_head_c, fs_head_s = _zset_head(r, REDIS_POLL_FULLSCAN)
    fs_lock          = _fullscan_lock_active(r)

    detail_adp_depth = r.llen(REDIS_DETAIL_ADAPTIVE)
    detail_fs_depth  = r.llen(REDIS_DETAIL_FULLSCAN)

    scan_proc        = _worker_processed(r, "scan_worker")
    fs_proc          = _worker_processed(r, "fullscan_worker")
    detail_proc      = _worker_processed(r, "detail_worker")

    # ── 2. Load previous snapshot ─────────────────────────────────────────────
    snap = None
    try:
        raw = r.get(WATCHDOG_SNAPSHOT_KEY)
        if raw:
            snap = json.loads(raw)
    except Exception:
        pass

    # ── 3. Persist current state for next cycle ───────────────────────────────
    # Skip when called from --status / health_check.py so manual checks do not
    # overwrite the snapshot the continuous watchdog uses for delta analysis.
    if persist_snapshot:
        try:
            r.set(WATCHDOG_SNAPSHOT_KEY, json.dumps({
                "ts":               now,
                "adp_total":        adp_total,
                "adp_overdue":      adp_overdue,
                "adp_head_c":       adp_head_c,
                "adp_head_s":       adp_head_s,
                "fs_total":         fs_total,
                "fs_overdue":       fs_overdue,
                "fs_head_c":        fs_head_c,
                "fs_head_s":        fs_head_s,
                "detail_adp_depth": detail_adp_depth,
                "detail_fs_depth":  detail_fs_depth,
                "scan_proc":        scan_proc,
                "fs_proc":          fs_proc,
                "detail_proc":      detail_proc,
            }), ex=WATCHDOG_SNAPSHOT_TTL)
        except Exception:
            pass   # Redis write failure — don't crash; next cycle will try again

    # ── 4. No prior snapshot → baseline cycle, no delta analysis ─────────────
    if snap is None:
        # Even on the first cycle, an empty poll queue is unambiguously wrong —
        # alert immediately rather than masking it as "baseline OK".
        if adp_total == 0:
            issues.append(Issue(Issue.ERROR, "queue:poll:adaptive",
                "EMPTY on baseline — no companies scheduled; Redis may have been wiped.",
                "python pipeline.py --rebuild",
                alert_type="queue_poll_adaptive_empty",
            ))
        else:
            issues.append(Issue(Issue.OK, "queue:poll:adaptive",
                f"baseline — {adp_total} scheduled  {adp_overdue} overdue "
                "(velocity tracking starts next cycle)"))
        if fs_total == 0:
            issues.append(Issue(Issue.ERROR, "queue:poll:fullscan",
                "EMPTY on baseline — no fullscan companies scheduled.",
                "python pipeline.py --rebuild",
                alert_type="queue_poll_fullscan_empty",
            ))
        else:
            issues.append(Issue(Issue.OK, "queue:poll:fullscan",
                f"baseline — {fs_total} scheduled  {fs_overdue} overdue"
                + (" [lock active]" if fs_lock else "")))
        for label, depth in [
            ("queue:detail:adaptive", detail_adp_depth),
            ("queue:detail:fullscan", detail_fs_depth),
        ]:
            if depth > DETAIL_QUEUE_ALERT:
                issues.append(Issue(Issue.ERROR, label,
                    f"depth={depth:,} (baseline) — CRITICAL threshold exceeded",
                    "python -m workers.detail_worker"))
            elif depth > DETAIL_QUEUE_WARN:
                issues.append(Issue(Issue.WARNING, label,
                    f"depth={depth:,} (baseline) — elevated, delta tracking starts next cycle"))
            else:
                issues.append(Issue(Issue.OK, label, f"depth={depth} (baseline)"))
        _check_pel_health(r, issues)
        return issues

    # ── 5. Extract previous snapshot values ───────────────────────────────────
    prev_adp_overdue = snap.get("adp_overdue", 0)
    prev_adp_head_c  = snap.get("adp_head_c")
    prev_adp_head_s  = snap.get("adp_head_s")
    prev_fs_total    = snap.get("fs_total")   # None means unknown (first real cycle)
    prev_fs_overdue  = snap.get("fs_overdue", 0)
    prev_fs_head_c   = snap.get("fs_head_c")
    prev_fs_head_s   = snap.get("fs_head_s")
    prev_detail_adp  = snap.get("detail_adp_depth", 0)
    prev_detail_fs   = snap.get("detail_fs_depth", 0)
    prev_scan_proc   = snap.get("scan_proc")
    prev_fs_proc     = snap.get("fs_proc")
    prev_detail_proc = snap.get("detail_proc")

    # ── 6. poll:adaptive ─────────────────────────────────────────────────────
    adp_delta = adp_overdue - prev_adp_overdue

    if adp_total == 0:
        issues.append(Issue(Issue.ERROR, "queue:poll:adaptive",
            "EMPTY — no companies scheduled. Scheduler crashed or Redis was wiped.",
            "python pipeline.py --rebuild",
            alert_type="queue_poll_adaptive_empty",
        ))
    elif adp_overdue == 0:
        issues.append(Issue(Issue.OK, "queue:poll:adaptive",
            f"{adp_total} scheduled  0 overdue — all on schedule"))
    else:
        stall, valid = _stall_count(
            adp_overdue, prev_adp_overdue,
            adp_head_c,  adp_head_s,
            prev_adp_head_c, prev_adp_head_s,
            scan_proc,   prev_scan_proc,
        )
        proc_delta = (scan_proc - prev_scan_proc) if (scan_proc is not None and prev_scan_proc is not None) else None
        summary = (
            f"{adp_total} total  {adp_overdue} overdue {_trend(adp_delta)}({adp_delta:+d})  "
            f"head={adp_head_c or '?'}  "
            + (f"scan_worker +{proc_delta} processed" if proc_delta is not None else "scan_worker processed=unknown")
        )
        if stall >= 3 and valid >= 3:
            issues.append(Issue(Issue.ERROR, "queue:poll:adaptive",
                f"STALL — {summary}  ({stall}/{valid} signals agree: no progress since last cycle)",
                "sudo systemctl restart recruiter-scheduler",
                alert_type="queue_poll_adaptive_stall",
            ))
        elif stall >= 2 and valid >= 2:
            issues.append(Issue(Issue.WARNING, "queue:poll:adaptive",
                f"DEGRADED — {summary}  ({stall}/{valid} signals: likely stalling)"))
        else:
            issues.append(Issue(Issue.OK, "queue:poll:adaptive", summary))

    # ── 7. poll:fullscan ──────────────────────────────────────────────────────
    fs_delta = fs_overdue - prev_fs_overdue

    if fs_total == 0:
        # Escalate to ERROR if the queue was also empty last cycle (persistent
        # empty is not normal — only the immediate post-rebuild cycle should be).
        _fs_was_empty = (prev_fs_total is not None and prev_fs_total == 0)
        issues.append(Issue(
            Issue.ERROR if _fs_was_empty else Issue.WARNING,
            "queue:poll:fullscan",
            ("EMPTY (2nd consecutive cycle) — rebuild required"
             if _fs_was_empty
             else "EMPTY — normal right after rebuild; alert if persists"),
            "python pipeline.py --rebuild",
            alert_type="queue_poll_fullscan_empty",
        ))
    elif fs_overdue == 0:
        issues.append(Issue(Issue.OK, "queue:poll:fullscan",
            f"{fs_total} scheduled  0 overdue"
            + (" [lock active — scan in progress]" if fs_lock else "")))
    elif fs_lock:
        # A scan is actively running — queue will not move until it completes.
        # This is the normal state for a healthy fullscan_worker mid-scan.
        issues.append(Issue(Issue.OK, "queue:poll:fullscan",
            f"{fs_total} total  {fs_overdue} overdue {_trend(fs_delta)}({fs_delta:+d})  "
            f"[lock active — scan in progress, no stall check]"))
    else:
        stall, valid = _stall_count(
            fs_overdue, prev_fs_overdue,
            fs_head_c,  fs_head_s,
            prev_fs_head_c, prev_fs_head_s,
            fs_proc,    prev_fs_proc,
        )
        proc_delta = (fs_proc - prev_fs_proc) if (fs_proc is not None and prev_fs_proc is not None) else None
        summary = (
            f"{fs_total} total  {fs_overdue} overdue {_trend(fs_delta)}({fs_delta:+d})  "
            f"head={fs_head_c or '?'}  "
            + (f"fullscan_worker +{proc_delta} scans" if proc_delta is not None else "fullscan_worker scans=unknown")
        )
        if stall >= 3 and valid >= 3:
            issues.append(Issue(Issue.ERROR, "queue:poll:fullscan",
                f"STALL — {summary}  ({stall}/{valid} signals agree: no progress since last cycle)",
                "sudo systemctl restart recruiter-scheduler",
                alert_type="queue_poll_fullscan_stall",
            ))
        elif stall >= 2 and valid >= 2:
            issues.append(Issue(Issue.WARNING, "queue:poll:fullscan",
                f"DEGRADED — {summary}  ({stall}/{valid} signals: likely stalling)"))
        else:
            issues.append(Issue(Issue.OK, "queue:poll:fullscan", summary))

    # ── 8. Detail queues ──────────────────────────────────────────────────────
    detail_proc_delta = (
        (detail_proc - prev_detail_proc)
        if detail_proc is not None and prev_detail_proc is not None
        else None
    )
    proc_note = (
        f"  detail_worker +{detail_proc_delta} jobs"
        if detail_proc_delta is not None else ""
    )

    for label, atype, depth, prev_depth in [
        ("queue:detail:adaptive", "queue_detail_adaptive",
         detail_adp_depth, prev_detail_adp),
        ("queue:detail:fullscan", "queue_detail_fullscan",
         detail_fs_depth,  prev_detail_fs),
    ]:
        delta   = depth - prev_depth
        # Use only this queue's own depth delta to determine if it is draining.
        # The pool-wide detail_proc_delta is not queue-specific: workers processing
        # adaptive jobs would make a growing fullscan queue falsely appear as draining.
        draining = delta < 0

        if depth == 0:
            issues.append(Issue(Issue.OK, label, f"depth=0 — idle{proc_note}"))
        elif draining:
            # Queue is shrinking or worker confirmed processing
            if depth > DETAIL_QUEUE_ALERT:
                issues.append(Issue(Issue.WARNING, label,
                    f"depth={depth:,} {_trend(delta)}({delta:+d}) — "
                    f"draining but critically elevated{proc_note}"))
            elif depth > DETAIL_QUEUE_WARN:
                issues.append(Issue(Issue.WARNING, label,
                    f"depth={depth:,} {_trend(delta)}({delta:+d}) — "
                    f"draining but elevated{proc_note}"))
            else:
                issues.append(Issue(Issue.OK, label,
                    f"depth={depth:,} {_trend(delta)}({delta:+d}) draining{proc_note}"))
        else:
            # Not draining — stalled or growing
            direction = "growing" if delta > 0 else "stalled"
            if depth > DETAIL_QUEUE_ALERT:
                issues.append(Issue(Issue.ERROR, label,
                    f"depth={depth:,} {_trend(delta)}({delta:+d}) — "
                    f"{direction} at CRITICAL level{proc_note}",
                    "python -m workers.detail_worker",
                    alert_type=atype,
                ))
            elif depth > DETAIL_QUEUE_WARN:
                issues.append(Issue(Issue.WARNING, label,
                    f"depth={depth:,} {_trend(delta)}({delta:+d}) — "
                    f"{direction} at elevated level{proc_note}"))
            else:
                issues.append(Issue(Issue.OK, label,
                    f"depth={depth:,} {_trend(delta)}({delta:+d}) — "
                    f"small backlog, {direction}{proc_note}"))

    # ── 9. PEL checks (unchanged) ─────────────────────────────────────────────
    _check_pel_health(r, issues)
    return issues


def check_bloom_health(r) -> list:
    bloom = fallback = 0
    cursor = 0
    for _ in range(10):
        cursor, keys = r.scan(cursor, match="bloom:fullscan:*", count=200)
        bloom += len(keys)
        if cursor == 0:
            break
    cursor = 0
    for _ in range(10):
        cursor, keys = r.scan(cursor, match="bloom:fallback:*", count=200)
        fallback += len(keys)
        if cursor == 0:
            break

    if bloom + fallback == 0:
        return [Issue(Issue.WARNING, "bloom_filters",
            "No bloom filter keys found — Redis may have restarted and lost data. "
            "Fullscans will rebuild filters from scratch (no jobs will be missed).")]
    return [Issue(Issue.OK, "bloom_filters",
        f"~{bloom+fallback} keys (RedisBloom={bloom} fallback={fallback})")]


def check_coverage(r) -> list:
    issues = []
    try:
        from db.db import get_conn
        conn = get_conn()
        try:
            total  = conn.execute(
                "SELECT COUNT(*) AS c FROM company_poll_stats").fetchone()["c"]
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
        finally:
            conn.close()

        if total == 0:
            issues.append(Issue(Issue.WARNING, "coverage",
                "No companies in company_poll_stats"))
        elif total > 0 and missed / total > COVERAGE_MISS_ALERT_PCT:
            issues.append(Issue(Issue.ERROR, "coverage",
                f"{missed}/{total} companies ({missed/total:.0%}) missed fullscan "
                f"in last 26h — fullscan_worker bottleneck or throughput issue",
                "Check fullscan_worker logs. Phase 2 thundering-herd fix addresses this.",
                alert_type="coverage_miss",
            ))
        else:
            scanned = total - missed
            issues.append(Issue(Issue.OK, "coverage",
                f"{scanned}/{total} companies scanned in last 26h "
                f"({missed} missed)"))

        if stuck > 10:
            issues.append(Issue(Issue.WARNING, "pending_detail",
                f"{stuck} jobs stuck as pending_detail >1h"))
        elif stuck == 0:
            issues.append(Issue(Issue.OK, "pending_detail", "0 stuck"))

    except Exception as exc:
        issues.append(Issue(Issue.WARNING, "coverage", f"DB query failed: {exc}"))
    return issues


def check_redis_persistence(r) -> list:
    try:
        info     = r.info("persistence")
        aof_on   = info.get("aof_enabled") in (1, "1", True)
        last_s   = info.get("rdb_last_save_time", 0) or r.lastsave()
        if isinstance(last_s, int) and last_s > 0:
            age_min = (time.time() - last_s) / 60
            if age_min > 30 and not aof_on:
                # Only warn about stale RDB when AOF is disabled.  With AOF
                # (appendfsync everysec) the data-loss window is ~1 second
                # regardless of how long ago the last RDB snapshot ran.
                return [Issue(Issue.WARNING, "redis_persistence",
                    f"Last RDB save {age_min:.0f} min ago — "
                    "data loss window is large. Consider enabling AOF.")]
            suffix = " [AOF active — data safe]" if aof_on else ""
            return [Issue(Issue.OK, "redis_persistence",
                f"Last RDB save {age_min:.0f} min ago{suffix}")]
    except Exception as exc:
        return [Issue(Issue.WARNING, "redis_persistence", f"Could not check: {exc}")]
    return []


def check_hung_workers(r) -> list:
    hung = []
    cursor = 0
    while True:
        cursor, keys = r.scan(cursor, match="heartbeat:*", count=100)
        for key in keys:
            ks      = key.decode() if isinstance(key, bytes) else key
            company = ks.split(":", 1)[1]
            if not r.exists(f"progress:{company}"):
                hung.append(company)
        if cursor == 0:
            break
    if hung:
        return [Issue(Issue.WARNING, "hung_workers",
            f"{len(hung)} scan(s) may be hung: "
            f"{', '.join(hung[:5])}{'...' if len(hung) > 5 else ''}")]
    return []


def check_systemd_services() -> list:
    """
    Directly query systemd for the state of both pipeline units.

    This catches failures that heartbeat checks miss:
      - Service in "failed" state (StartLimitBurst exhausted by systemd)
      - Service never started (inactive / dead on boot)
      - Service manually stopped via `systemctl stop`

    These states can persist even if a heartbeat key is still in Redis with
    remaining TTL from before the crash.

    Only runs when _SYSTEMD_AVAILABLE=True.  Returns empty list otherwise
    (dev machines, cron mode) so this never blocks a non-systemd deploy.

    Heal action: systemd_recruiter-scheduler → reset-failed + restart via systemctl
    (The watchdog unit itself restarting is handled by systemd automatically.)
    """
    if not _systemd_available():
        return []

    issues = []
    # Check scheduler — this is the one we can and should auto-heal
    # Check watchdog itself for informational awareness (we can't restart
    # ourselves from inside, but the OnFailure alert will fire if we're dead)
    for unit, healable in [
        (_UNIT_SCHEDULER, True),
        (_UNIT_WATCHDOG,  False),   # we're the watchdog — can't self-heal
    ]:
        try:
            result = subprocess.run(
                [_SYSTEMCTL, "is-active", unit],
                capture_output=True, text=True, timeout=5,
            )
            state = result.stdout.strip()   # "active" | "inactive" | "failed" | "activating" ...

            if state == "active":
                issues.append(Issue(Issue.OK, f"systemd:{unit}",
                    "service is active (systemd managed)"))
            elif state == "activating":
                # Briefly starting — not an error, just note it
                issues.append(Issue(Issue.OK, f"systemd:{unit}",
                    "service is activating (starting up)"))
            elif state == "failed":
                msg = (
                    f"systemd service is in FAILED state — "
                    f"StartLimitBurst may have been hit. "
                    f"Run: sudo {_SYSTEMCTL} status {unit}"
                )
                fix = (
                    f"sudo {_SYSTEMCTL} reset-failed {unit} && "
                    f"sudo {_SYSTEMCTL} start {unit}"
                )
                atype = f"systemd_{unit}"
                if healable:
                    issues.append(Issue(Issue.ERROR, f"systemd:{unit}", msg, fix,
                                        alert_type=atype))
                else:
                    issues.append(Issue(Issue.CRITICAL, f"systemd:{unit}", msg, fix,
                                        alert_type=atype))
            else:
                # inactive / unknown / deactivating
                msg = (
                    f"systemd service state is '{state}' (expected 'active'). "
                    f"Run: sudo {_SYSTEMCTL} status {unit}"
                )
                fix = f"sudo {_SYSTEMCTL} start {unit}"
                atype = f"systemd_{unit}"
                level = Issue.ERROR if healable else Issue.CRITICAL
                issues.append(Issue(level, f"systemd:{unit}", msg, fix,
                                    alert_type=atype))
        except subprocess.TimeoutExpired:
            issues.append(Issue(Issue.WARNING, f"systemd:{unit}",
                "systemctl is-active timed out — systemd may be overloaded"))
        except Exception as exc:
            issues.append(Issue(Issue.WARNING, f"systemd:{unit}",
                f"Could not query service state: {exc}"))

    return issues


def _run_all_checks(r, persist_snapshot: bool = True) -> list:
    issues = []
    # ── systemd service state (first — most direct signal) ────────────────────
    # check_systemd_services() is a no-op on non-systemd systems.
    # It catches "failed" / "inactive" service states that heartbeat checks
    # can miss during the heartbeat TTL grace period after a crash.
    issues.extend(check_systemd_services())
    issues.extend(check_worker_heartbeats(r))
    issues.extend(check_queue_health(r, persist_snapshot=persist_snapshot))
    issues.extend(check_bloom_health(r))
    issues.extend(check_coverage(r))
    issues.extend(check_hung_workers(r))
    issues.extend(check_redis_persistence(r))
    return issues


# ─────────────────────────────────────────
# GHOST WORKER DETECTION + CLEANUP
# ─────────────────────────────────────────

# Worker subprocess command patterns spawned by the non-systemd self-heal path.
# Any process matching these patterns whose PPID is not the scheduler's PID is
# a ghost competing with the managed pool for the same Redis queues.
_GHOST_WORKER_PATTERNS = (
    "-m workers.detail_worker",
    "-m workers.scan_worker",
    "-m workers.fullscan",
)


def _get_scheduler_pid(r) -> int | None:
    """
    Return the confirmed live scheduler PID, or None if identity cannot be verified.

    Validation steps:
      1. Read PID from the Redis heartbeat key.
      2. Confirm the process is alive (os.kill(pid, 0)).
      3. Confirm its cmdline matches the scheduler entrypoint (-m workers.scheduler).
      4. Fallback: if cmdline check fails, confirm via systemd MainPID for the
         recruiter-scheduler unit (handles compiled or aliased entrypoints).

    Returns None — skipping the ghost-worker kill pass entirely — if any
    step cannot confirm the scheduler's identity.
    """
    try:
        hb = r.get("worker:alive:scheduler:adaptive")
        if not hb:
            return None
        pid = json.loads(hb).get("pid")
        if not isinstance(pid, int) or pid <= 0:
            return None

        # Step 2: confirm process is alive
        try:
            os.kill(pid, 0)
        except ProcessLookupError:
            logger.debug(
                "_get_scheduler_pid: heartbeat PID %d is no longer alive — skipping",
                pid,
            )
            return None
        except PermissionError:
            pass  # process exists; we cannot signal it but it is alive

        # Step 3: confirm cmdline identifies the scheduler entrypoint
        try:
            cmdline = (
                Path(f"/proc/{pid}/cmdline")
                .read_bytes()
                .replace(b"\x00", b" ")
                .decode(errors="replace")
            )
            if "-m workers.scheduler" in cmdline:
                return pid
        except FileNotFoundError:
            return None  # exited between kill(0) and cmdline read

        # Step 4: cmdline check inconclusive — confirm via systemd MainPID
        if _systemd_available():
            try:
                out = subprocess.run(
                    [_SYSTEMCTL, "show", "-p", "MainPID", "--value",
                     _UNIT_SCHEDULER],
                    capture_output=True, text=True, timeout=3,
                ).stdout.strip()
                if out and int(out) == pid:
                    return pid
            except (subprocess.SubprocessError, ValueError, OSError):
                pass

        logger.debug(
            "_get_scheduler_pid: PID %d could not be confirmed as the scheduler "
            "— skipping ghost-worker kill pass",
            pid,
        )
        return None

    except Exception as exc:
        logger.debug("_get_scheduler_pid: failed to read scheduler heartbeat: %s", exc)
    return None


def _kill_ghost_workers(r) -> None:
    """
    Detect and kill worker processes not parented to the scheduler.

    Ghost workers arise when the watchdog's non-systemd self-heal path
    (subprocess.Popen with start_new_session=True) spawns detached worker
    subprocesses that survive after the server transitions to systemd management.
    They compete with the scheduler's managed pool for the same Redis queues,
    causing duplicate job processing and inflated queue metrics.

    Detection: scan /proc for processes matching a worker command pattern whose
    PPID is not the scheduler's PID. No psutil dependency — /proc is portable
    across all Linux distros.

    Action: SIGTERM → 10s grace period → SIGKILL any survivors.
    Sends one alert email per kill batch (Redis-deduped, 1h cooldown).
    Skipped entirely when the scheduler PID is unknown (avoids false kills).

    ⚠  On systemd-managed hosts this will also terminate matching worker
    processes started manually for debugging (e.g. `python -m workers.scan_worker`)
    if they are not children of the scheduler PID.  Before running debug workers
    alongside a live scheduler, either start them as children of the scheduler
    (via `--spawn-worker` if available) or temporarily set `self_heal=False` in
    the watchdog config to suppress ghost-worker detection.
    """
    import signal as _signal

    scheduler_pid = _get_scheduler_pid(r)
    if not scheduler_pid:
        # Can't determine the legitimate scheduler PID — skip to avoid
        # accidentally killing a valid worker during scheduler startup.
        return

    ghosts: list[int] = []
    own_pid = os.getpid()

    try:
        for pid_dir in Path("/proc").iterdir():
            if not pid_dir.name.isdigit():
                continue
            pid = int(pid_dir.name)
            if pid == own_pid:
                continue
            try:
                cmdline = (
                    pid_dir / "cmdline"
                ).read_bytes().replace(b"\x00", b" ").decode(errors="replace")
                if not any(pat in cmdline for pat in _GHOST_WORKER_PATTERNS):
                    continue
                status_text = (pid_dir / "status").read_text()
                ppid_line   = next(
                    line for line in status_text.splitlines() if line.startswith("PPid:")
                )
                ppid = int(ppid_line.split()[1])
                if ppid != scheduler_pid:
                    ghosts.append(pid)
            except (FileNotFoundError, PermissionError, StopIteration, ValueError, UnicodeDecodeError):
                continue
    except Exception as exc:
        logger.warning("watchdog: ghost worker scan failed: %s", exc)
        return

    if not ghosts:
        return

    logger.warning(
        "watchdog: %d ghost worker(s) detected (not children of scheduler PID %d) — PIDs %s",
        len(ghosts), scheduler_pid, ghosts,
    )

    killed: list[int] = []
    for pid in ghosts:
        # Revalidate immediately before SIGTERM — the process may have exited or
        # been adopted by the scheduler between the ghost scan and now.
        try:
            _pre_cmdline = (
                Path(f"/proc/{pid}/cmdline")
                .read_bytes()
                .replace(b"\x00", b" ")
                .decode(errors="replace")
            )
            if not any(pat in _pre_cmdline for pat in _GHOST_WORKER_PATTERNS):
                logger.debug(
                    "watchdog: PID %d cmdline changed before SIGTERM — skipping (PID may have been reused): %r",
                    pid, _pre_cmdline[:120],
                )
                continue
            _pre_status = Path(f"/proc/{pid}/status").read_text()
            _pre_ppid   = int(
                next(
                    line for line in _pre_status.splitlines()
                    if line.startswith("PPid:")
                ).split()[1]
            )
            if _pre_ppid == scheduler_pid:
                logger.debug(
                    "watchdog: PID %d was adopted by scheduler before SIGTERM — skipping",
                    pid,
                )
                continue
        except (FileNotFoundError, StopIteration, ValueError):
            continue  # Process exited between scan and revalidation
        try:
            os.kill(pid, _signal.SIGTERM)
            killed.append(pid)
            logger.info("watchdog: SIGTERM → ghost PID %d", pid)
        except ProcessLookupError:
            pass  # Already dead between revalidation and kill
        except PermissionError:
            logger.warning("watchdog: no permission to kill ghost PID %d", pid)

    if not killed:
        return

    # Grace period — let workers flush Redis cleanup (requeue inflight jobs etc.)
    time.sleep(10)

    for pid in killed:
        try:
            os.kill(pid, 0)  # Raises ProcessLookupError if already gone
        except ProcessLookupError:
            continue  # Exited cleanly after SIGTERM
        # Re-read cmdline: PID may have been reused by a different process
        # in the 10-second grace window between SIGTERM and SIGKILL.
        try:
            _recheck = (
                Path(f"/proc/{pid}/cmdline")
                .read_bytes()
                .replace(b"\x00", b" ")
                .decode(errors="replace")
            )
        except FileNotFoundError:
            continue  # process exited between kill(0) and cmdline read
        if not any(pat in _recheck for pat in _GHOST_WORKER_PATTERNS):
            logger.debug(
                "watchdog: PID %d cmdline changed after SIGTERM — skipping SIGKILL "
                "(PID may have been reused): %r",
                pid, _recheck[:120],
            )
            continue
        # Re-read PPID: if the process was adopted by the scheduler between
        # SIGTERM and now it is no longer a ghost and must not be killed.
        try:
            _status_recheck = Path(f"/proc/{pid}/status").read_text()
            _ppid_recheck   = int(
                next(
                    line for line in _status_recheck.splitlines()
                    if line.startswith("PPid:")
                ).split()[1]
            )
            if _ppid_recheck == scheduler_pid:
                logger.debug(
                    "watchdog: PID %d is now a child of scheduler (PPID=%d) — skipping SIGKILL",
                    pid, scheduler_pid,
                )
                continue
        except (FileNotFoundError, StopIteration, ValueError):
            continue  # process exited; nothing to kill
        try:
            os.kill(pid, _signal.SIGKILL)
            logger.warning("watchdog: ghost PID %d ignored SIGTERM — SIGKILLed", pid)
        except ProcessLookupError:
            pass  # Exited between re-check and SIGKILL

    pid_list = ", ".join(str(p) for p in killed)
    _send_email(
        f"⚠ Ghost workers detected and killed ({len(killed)} PID{'s' if len(killed) > 1 else ''})",
        f"""
        <p style="color:#b45309;font-size:16px;font-weight:700;">
          ⚠ Ghost worker processes were detected and terminated automatically.
        </p>
        <p style="color:#374151;">
          These processes were running worker code but were
          <strong>not children of the scheduler</strong> (PID {scheduler_pid}).
          They were likely spawned by the watchdog's non-systemd self-heal path
          before <code>install-systemd.sh</code> was configured, and survived
          the transition to systemd management.
        </p>
        <p style="color:#374151;">
          <strong>PIDs terminated:</strong> <code>{pid_list}</code><br>
          <strong>Action:</strong> SIGTERM → 10s grace → SIGKILL for survivors.
        </p>
        <p style="color:#16a34a;">
          The scheduler's managed worker pool is unaffected.
          No manual intervention needed.
        </p>
        <p style="color:#64748b;font-size:12px;">
          To prevent recurrence ensure <code>install-systemd.sh</code> has been
          run and both <code>recruiter-scheduler</code> and
          <code>recruiter-watchdog</code> are active systemd units.
        </p>""",
        dedup_key="watchdog:ghost_workers_killed",
        dedup_ttl=3600,
        r=r,
    )


# ─────────────────────────────────────────
# SELF-HEALING
# ─────────────────────────────────────────

def _heal_attempt_count(alert_type: str, r) -> int:
    return int(r.get(_rkey("heal_count", alert_type)) or 0)


def _can_heal(alert_type: str, r) -> bool:
    """
    True if we should attempt a self-heal for this alert type.

    Blocked if:
      - No heal action is defined for this alert type
      - Already escalated (and escalation hasn't expired)
      - Heal was attempted too recently (< HEAL_COOLDOWN_S ago)
      - Attempt count >= HEAL_MAX_ATTEMPTS
    """
    if _get_heal_action(alert_type) is None:
        return False
    if r.exists(_rkey("escalated", alert_type)):
        return False
    last_ts_raw = r.get(_rkey("heal_last", alert_type))
    if last_ts_raw:
        last_ts = float(last_ts_raw)
        if time.time() - last_ts < HEAL_COOLDOWN_S:
            return False
    if _heal_attempt_count(alert_type, r) >= HEAL_MAX_ATTEMPTS:
        return False
    return True


def _attempt_heal(issue: Issue, r) -> bool:
    """
    Attempt to self-heal an issue by running the defined heal action.

    Workers are spawned as detached background processes (new session group)
    so they survive beyond this watchdog invocation.

    Foreground actions (e.g. --rebuild) are run synchronously with a timeout.

    Returns True if the heal was successfully initiated.
    """
    action = _get_heal_action(issue.alert_type)
    if action is None:
        return False

    log_path = action["log_file"]
    cmd_args  = action["cmd_args"]
    is_fg    = action.get("foreground", False)

    mode = "systemd" if _systemd_available() else "subprocess"
    logger.info(
        "watchdog: attempting self-heal for %r [%s] — %s",
        issue.alert_type, mode, action["description"],
    )

    # ── Atomic exclusive lock — prevent concurrent watchdog instances from
    # both healing the same issue simultaneously (service + cron overlap).
    # NX ensures only the first caller proceeds; EX auto-expires if we crash.
    _heal_lock_key = _rkey("heal_lock", issue.alert_type)
    _heal_lock_val = f"{os.getpid()}:{time.time()}"
    try:
        _lock_acquired = r.set(_heal_lock_key, _heal_lock_val,
                               nx=True, ex=HEAL_COOLDOWN_S)
    except Exception:
        _lock_acquired = True   # Redis error — optimistically proceed (single-instance)
    if not _lock_acquired:
        logger.info(
            "watchdog: heal lock already held for %r — "
            "another watchdog instance is handling this, skipping",
            issue.alert_type,
        )
        return False

    # Record the attempt BEFORE running the subprocess so a timeout or spawn
    # error still counts toward the cooldown / max-attempt limits.  Without
    # this, a consistently timing-out command would retry indefinitely.
    count_key = _rkey("heal_count", issue.alert_type)
    try:
        r.incr(count_key)
        r.expire(count_key, HEAL_ATTEMPT_WINDOW)
        r.set(_rkey("heal_last", issue.alert_type), str(time.time()),
              ex=HEAL_ATTEMPT_WINDOW)
    except Exception as _re:
        logger.warning("watchdog: could not record heal attempt in Redis: %s", _re)

    try:
        with open(log_path, "a") as log_fh:
            ts_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            log_fh.write(f"\n[{ts_str}] watchdog auto-heal: {' '.join(cmd_args)}\n")
            log_fh.flush()

            if is_fg:
                # Synchronous — use per-action timeout (default 60 s);
                # rebuild actions need more time so they specify timeout_s=300.
                _timeout = action.get("timeout_s", 60)
                result = subprocess.run(
                    cmd_args,
                    cwd=_PROJECT_ROOT,
                    stdout=log_fh,
                    stderr=subprocess.STDOUT,
                    timeout=_timeout,
                )
                success = (result.returncode == 0)
                if not success:
                    logger.warning(
                        "watchdog: foreground heal failed for %r (exit=%d)",
                        issue.alert_type, result.returncode,
                    )
                    return False
            else:
                # Background — detach from watchdog's process group.
                # log_fh is inherited by the child; the with block closes it
                # in the parent after Popen returns (even if Popen raises).
                subprocess.Popen(
                    cmd_args,
                    cwd=_PROJECT_ROOT,
                    stdout=log_fh,
                    stderr=subprocess.STDOUT,
                    start_new_session=True,   # detach — survives watchdog exit
                )

        logger.info(
            "watchdog: heal initiated for %r (attempt %d/%d) log=%s",
            issue.alert_type,
            _heal_attempt_count(issue.alert_type, r),
            HEAL_MAX_ATTEMPTS,
            log_path,
        )
        return True

    except Exception as exc:
        logger.error(
            "watchdog: self-heal command failed for %r: %s",
            issue.alert_type, exc,
        )
        return False


# ─────────────────────────────────────────
# ALERT DISPATCH
# ─────────────────────────────────────────

def _process_issues(issues: list, r, self_heal: bool = True) -> None:
    """
    For every alertable issue, determine what action to take and fire appropriate emails.

    State machine per alert_type:

        NEW ISSUE (not in active_issues last cycle):
            → if healable: attempt_heal + send "Attempting auto-heal" email
            → if not healable: send "Issue detected" alert email

        PERSISTING ISSUE (was active last cycle, still active):
            → if heal was attempted: check if it worked
                • worked (heartbeat appeared / queue refilled): → "✅ Auto-healed" email
                  (detected by the issue no longer being ERROR in this cycle — handled
                  in RESOLVED section below since it won't be in alertable list)
                • still broken + under attempt limit: → attempt_heal again
                • still broken + over attempt limit: → "🆘 ESCALATION" email
            → if no heal: resend alert (subject to ALERT_COOLDOWN_S)

        RESOLVED (was active last cycle, now OK):
            → send "✅ Resolved" email
    """
    # Load last cycle's active alert_types from Redis
    raw_last    = r.get("watchdog:active_issues")
    last_active = set(json.loads(raw_last)) if raw_last else set()

    # Current active alert_types
    current_alertable = {i.alert_type for i in issues if i.is_alertable()}

    # Persist current set for next cycle
    r.set("watchdog:active_issues", json.dumps(list(current_alertable)), ex=86400)

    # ── RESOLVED: was broken last cycle, is OK now ────────────────────────────
    # Only treat as resolved if the alert_type is completely absent (or at OK
    # level) this cycle.  A downgrade from ERROR → WARNING must NOT fire a
    # "resolved" email because the issue is still present — just at lower severity.
    current_all_types = {i.alert_type for i in issues if i.level != "OK"}
    for alert_type in last_active - current_alertable:
        if alert_type in current_all_types:
            # Downgraded to WARNING — still present, not yet resolved
            continue
        # Check if this was actually a heal success (heal_last key still in window)
        was_being_healed = r.exists(_rkey("heal_last", alert_type))
        if was_being_healed:
            subject = f"✅ Auto-healed: {alert_type.replace('_', ' ').title()}"
            body    = f"""
            <p style="color:#16a34a;font-size:16px;font-weight:700;">
              ✅ Self-healing was successful
            </p>
            <p style="color:#374151;">
              <strong>{alert_type.replace('_', ' ').title()}</strong> recovered after
              automatic intervention. No manual action needed.
            </p>
            <p style="color:#64748b;font-size:13px;">
              Attempt count: {_heal_attempt_count(alert_type, r)}/{HEAL_MAX_ATTEMPTS}
            </p>"""
        else:
            subject = f"✅ Resolved: {alert_type.replace('_', ' ').title()}"
            body    = f"""
            <p style="color:#16a34a;font-size:16px;font-weight:700;">
              ✅ Issue resolved
            </p>
            <p style="color:#374151;">
              <strong>{alert_type.replace('_', ' ').title()}</strong> is now healthy.
            </p>"""

        _send_email(subject, body,
                    dedup_key=_rkey("resolved", alert_type),
                    dedup_ttl=RESOLVE_COOLDOWN_S, r=r)

        # Clean up heal state for this alert type
        r.delete(_rkey("heal_count", alert_type))
        r.delete(_rkey("heal_last", alert_type))
        r.delete(_rkey("escalated", alert_type))

    # ── NEW / PERSISTING ISSUES ───────────────────────────────────────────────
    multi_issue_body_needed = []

    # If the scheduler itself is dead, DO NOT also spawn individual workers.
    # The revived scheduler will spawn its own managed pool — spawning standalone
    # workers simultaneously would create duplicates (harmless but wasteful).
    # Exception: if scheduler was already healing last cycle (i.e. watchdog already
    # restarted it) but workers are STILL missing, those workers may be orphaned
    # from before the crash — heal them directly.
    scheduler_dead_this_cycle = any(
        i.alert_type == "worker_scheduler" and i.is_alertable()
        for i in issues
    )
    scheduler_being_healed_already = r.exists(_rkey("heal_last", "worker_scheduler"))
    # Skip individual worker heals only on the FIRST cycle of scheduler failure.
    # If scheduler was already restarted (heal_last exists) but workers are still
    # missing, heal them directly (scheduler spawn may have failed or taken its pool down).
    skip_worker_heals = scheduler_dead_this_cycle and not scheduler_being_healed_already

    _WORKER_ALERT_TYPES = {
        "worker_scan_worker", "worker_detail_worker", "worker_fullscan_worker",
    }

    for issue in issues:
        if not issue.is_alertable():
            continue

        atype       = issue.alert_type
        is_new      = atype not in last_active
        was_healing = r.exists(_rkey("heal_last", atype))
        attempts    = _heal_attempt_count(atype, r)
        escalated   = r.exists(_rkey("escalated", atype))

        if escalated:
            # Already escalated — do nothing until escalation TTL expires
            continue

        # ── Escalation check (persisting issue, exhausted attempts) ───────────
        if not is_new and was_healing and attempts >= HEAL_MAX_ATTEMPTS:
            subj = f"🆘 ESCALATION — {issue.category} still broken after {HEAL_MAX_ATTEMPTS} auto-restarts"
            body = f"""
            <p style="color:#ef4444;font-weight:700;font-size:16px;">
              🆘 Manual intervention required
            </p>
            <p style="color:#374151;">
              <strong>{issue.category}</strong> could not be automatically recovered
              after <strong>{HEAL_MAX_ATTEMPTS} restart attempts</strong>.
            </p>
            <p style="color:#374151;"><strong>Issue:</strong> {_html_lib.escape(issue.message)}</p>
            {"<p style='font-family:monospace;background:#fef2f2;padding:10px;border-radius:6px;color:#991b1b;'><strong>Manual fix:</strong><br>" + _html_lib.escape(issue.fix) + "</p>" if issue.fix else ""}
            <p style="color:#64748b;font-size:12px;">
              Auto-heal will retry after {ESCALATION_COOLDOWN // 3600}h.
              Worker restart logs: <code>{_LOG_DIR}/{atype.replace('worker_','')}.log</code>
            </p>"""
            sent = _send_email(subj, body,
                               dedup_key=_rkey("escalated", atype),
                               dedup_ttl=ESCALATION_COOLDOWN, r=r)
            if sent:
                logger.warning(
                    "watchdog: ESCALATION sent for %r after %d failed heal attempts",
                    atype, attempts,
                )
            continue

        # ── Skip individual worker heals when scheduler just died ────────────
        # The scheduler is their parent and will spawn its own pool on restart.
        # We only heal them directly if the scheduler has already been restarted
        # (heal_last exists) but workers are still absent.
        if skip_worker_heals and atype in _WORKER_ALERT_TYPES:
            logger.info(
                "watchdog: skipping standalone heal for %r — scheduler is also "
                "dead (will be spawned by revived scheduler)", atype,
            )
            # Still send an informational alert so you know workers are down
            subj = f"⚠ Pipeline Alert: {issue.category} — {issue.level}"
            body = f"""
            <p style="color:#f59e0b;font-weight:700;">{issue.emoji()} {issue.level}: {issue.category}</p>
            <p style="color:#374151;">{_html_lib.escape(issue.message)}</p>
            <p style="color:#64748b;font-size:13px;">
              ℹ Scheduler is also dead — healing scheduler first.
              Workers will be spawned automatically by the revived scheduler.
            </p>"""
            _send_email(subj, body,
                        dedup_key=_rkey("alert", atype),
                        dedup_ttl=ALERT_COOLDOWN_S, r=r)
            multi_issue_body_needed.append(issue)
            continue

        # ── Attempt self-heal (new issue or retry within attempt budget) ──────
        if self_heal and _can_heal(atype, r):
            heal_ok = _attempt_heal(issue, r)
            if heal_ok:
                attempt_n = _heal_attempt_count(atype, r)
                heal_action = _get_heal_action(atype)
                subj = f"⚠ Pipeline Issue — Auto-heal Attempted: {issue.category}"
                body = f"""
                <p style="color:#f59e0b;font-weight:700;font-size:15px;">
                  ⚠ Issue detected — attempting automatic recovery
                </p>
                <p style="color:#374151;">
                  <strong>{_html_lib.escape(issue.category)}</strong>: {_html_lib.escape(issue.message)}
                </p>
                <div style="background:#fefce8;border:1px solid #fde68a;
                            border-radius:6px;padding:12px;margin:12px 0;">
                  <strong style="color:#92400e;">🔧 Auto-heal action taken
                    (attempt {attempt_n}/{HEAL_MAX_ATTEMPTS}):</strong><br>
                  <span style="color:#78350f;">
                    {heal_action['description']}
                  </span><br>
                  <span style="font-family:monospace;color:#92400e;font-size:12px;">
                    $ {' '.join(heal_action['cmd_args'])}
                  </span><br>
                  <span style="color:#78350f;font-size:12px;">
                    Log: {heal_action['log_file']}
                  </span>
                </div>
                <p style="color:#64748b;font-size:12px;">
                  Next check in {WATCHDOG_INTERVAL_S // 60} min — you'll receive
                  "✅ Auto-healed" if recovery succeeded, or another attempt will run.
                  After {HEAL_MAX_ATTEMPTS} failed attempts you'll receive an
                  escalation requiring manual intervention.
                </p>"""
                _send_email(subj, body,
                            dedup_key=_rkey("alert", atype),
                            dedup_ttl=ALERT_COOLDOWN_S, r=r)
                multi_issue_body_needed.append(issue)
                continue

        # ── No heal available (or heal skipped) — plain alert ─────────────────
        subj = f"⚠ Pipeline Alert: {issue.category} — {issue.level}"
        body = f"""
        <p style="color:#ef4444;font-weight:700;">{issue.emoji()} {issue.level}: {issue.category}</p>
        <p style="color:#374151;">{_html_lib.escape(issue.message)}</p>
        {"<p style='font-family:monospace;background:#f1f5f9;padding:8px;border-radius:4px;color:#475569;font-size:13px;'><strong>Fix:</strong> " + _html_lib.escape(issue.fix) + "</p>" if issue.fix else ""}
        <p style="color:#64748b;font-size:12px;">
          Auto-heal is not configured for this issue type.
          Manual intervention required.
        </p>"""
        _send_email(subj, body,
                    dedup_key=_rkey("alert", atype),
                    dedup_ttl=ALERT_COOLDOWN_S, r=r)
        multi_issue_body_needed.append(issue)

    # ── Multi-issue summary (if 3+ issues in one cycle) ───────────────────────
    if len(multi_issue_body_needed) >= 3:
        alertable_all = [i for i in issues if i.is_alertable()]
        _send_email(
            f"⚠ Pipeline Alert: {len(alertable_all)} issues detected simultaneously",
            '<p style="color:#ef4444;font-weight:700;">Multiple issues detected:</p>'
            + _html_issue_table(alertable_all),
            dedup_key="watchdog:alert:summary_multi",
            dedup_ttl=ALERT_COOLDOWN_S,
            r=r,
        )


# ─────────────────────────────────────────
# STATUS PRINT (--status / --once console output)
# ─────────────────────────────────────────

def _print_status(r_ok, r_info, pg_ok, pg_info, issues: list) -> None:
    SEP  = "─" * 70
    DSEP = "═" * 70
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    print(f"\n{DSEP}")
    print(f"  PIPELINE HEALTH CHECK   {now_str}")
    print(DSEP)

    def sym(lvl):
        return {"OK": "✓", "WARNING": "!", "ERROR": "✗", "CRITICAL": "✗"}.get(lvl, "?")

    print("\n  INFRASTRUCTURE")
    print(f"  {SEP}")
    print(f"  [{'✓' if r_ok else '✗'}] Redis       {r_info}")
    print(f"  [{'✓' if pg_ok else '✗'}] PostgreSQL  {pg_info}")

    groups = [
        ("WORKER LIVENESS",  lambda i: i.category.startswith("worker:")),
        ("QUEUE HEALTH",     lambda i: i.category.startswith("queue:") or
                                       i.category.startswith("stream:")),
        ("OTHER",            lambda i: not i.category.startswith("worker:") and
                                       not i.category.startswith("queue:") and
                                       not i.category.startswith("stream:")),
    ]
    for title, pred in groups:
        group = [i for i in issues if pred(i)]
        if group:
            print(f"\n  {title}")
            print(f"  {SEP}")
            for i in group:
                print(f"  [{sym(i.level)}] {i.category:<32} {i.message[:52]}")

    errors   = sum(1 for i in issues if i.level in ("ERROR", "CRITICAL"))
    warnings = sum(1 for i in issues if i.level == "WARNING")
    oks      = sum(1 for i in issues if i.level == "OK")
    verdict  = "ALL OK" if errors == 0 and warnings == 0 else \
               "DEGRADED" if errors == 0 else "UNHEALTHY"
    print(f"\n  {SEP}")
    print(f"  VERDICT: {verdict}   ({oks} ok  {warnings} warnings  {errors} errors)")
    print(f"{DSEP}\n")


# ─────────────────────────────────────────
# MAIN LOOP
# ─────────────────────────────────────────

def run_watchdog(once: bool = False,
                 status_only: bool = False,
                 self_heal: bool = True) -> None:
    """
    Main watchdog entry point.

    Args:
        once:        run one check cycle then exit (for cron).
        status_only: print status table and exit — no emails, no healing.
        self_heal:   if False, alerts only (no auto-restart commands).
    """
    from workers.sentry_init import init_sentry
    init_sentry()

    # ── Infrastructure check first ────────────────────────────────────────────
    r_ok, r_info = check_redis()
    if not r_ok:
        print(f"\n[watchdog] 🔴 CRITICAL: Redis UNREACHABLE — {r_info}")
        logger.error("watchdog: Redis unreachable: %s", r_info)
        if not status_only:
            _try_redis_down_email(r_info)
        sys.exit(1)

    r = get_redis()

    pg_ok, pg_info = check_postgres()
    if not pg_ok and not status_only:
        _send_email(
            "🔴 CRITICAL: Pipeline PostgreSQL is DOWN",
            f'<p style="color:#ef4444;font-weight:700;">PostgreSQL UNREACHABLE</p>'
            f'<p style="color:#374151;">{pg_info}</p>'
            f'<p style="font-family:monospace;background:#fef2f2;padding:8px;'
            f'border-radius:4px;">Fix: check PostgreSQL service and DB config</p>',
            dedup_key="watchdog:alert:postgresql_down",
            dedup_ttl=ALERT_COOLDOWN_S,
            r=r,
        )

    if status_only:
        issues = _run_all_checks(r, persist_snapshot=False)
        _print_status(r_ok, r_info, pg_ok, pg_info, issues)
        return

    mode_str = "status-only" if status_only else (
        f"self-heal={'on' if self_heal else 'off'}"
    )
    logger.info("watchdog started (once=%s %s)", once, mode_str)
    print(f"[watchdog] Running — interval={WATCHDOG_INTERVAL_S}s  "
          f"self_heal={self_heal}  once={once}")

    # In-process dedup for mid-loop Redis-down emails: avoid an email every
    # WATCHDOG_INTERVAL_S seconds during a sustained Redis outage.
    _last_redis_down_email_ts: float = 0.0

    while True:
        try:
            if self_heal and _systemd_available():
                _kill_ghost_workers(r)
            issues = _run_all_checks(r)

            errors   = sum(1 for i in issues if i.level in ("ERROR", "CRITICAL"))
            warnings = sum(1 for i in issues if i.level == "WARNING")
            ts       = datetime.now().strftime("%H:%M:%S")

            if errors or warnings:
                print(f"[watchdog {ts}] ⚠  errors={errors} warnings={warnings}")
                for i in issues:
                    if i.level in ("ERROR", "CRITICAL", "WARNING"):
                        print(f"  {i.emoji()} {i}")
            else:
                print(f"[watchdog {ts}] ✓  all {len(issues)} checks passed")

            _process_issues(issues, r, self_heal=self_heal)

            if once:
                break

            time.sleep(WATCHDOG_INTERVAL_S)

        except KeyboardInterrupt:
            print("\n[watchdog] Stopping.")
            break
        except Exception as exc:
            logger.error("watchdog: loop error: %s", exc, exc_info=True)
            # If Redis went down mid-loop the operator would be blind — all
            # dedup keys are in Redis and can't be written.  Send a last-resort
            # email (no Redis dedup) at most once per ALERT_COOLDOWN_S to avoid
            # an email storm during a sustained outage.
            try:
                _r_ok, _r_info = check_redis()
                if not _r_ok:
                    if time.time() - _last_redis_down_email_ts > ALERT_COOLDOWN_S:
                        _try_redis_down_email(_r_info)
                        _last_redis_down_email_ts = time.time()
            except Exception:
                pass
            if once:
                import sys as _sys
                _sys.exit(1)
            time.sleep(WATCHDOG_INTERVAL_S)

    logger.info("watchdog shutdown")


def _try_redis_down_email(r_info: str) -> None:
    """Last-resort email when Redis is down (can't use Redis for dedup)."""
    try:
        if not EMAIL or not APP_PASSWORD:
            return
        msg = MIMEMultipart("alternative")
        msg["Subject"] = "🔴 CRITICAL: Pipeline Redis is DOWN"
        msg["From"]    = EMAIL
        msg["To"]      = EMAIL
        msg.attach(MIMEText(
            f"Redis is unreachable. All pipeline workers have likely stopped.\n\n"
            f"Error: {r_info}\n\n"
            f"Fix: check Redis service on the server.\n"
            f"     sudo systemctl status redis  (or memurai on Windows)\n"
            f"     sudo systemctl restart redis",
            "plain",
        ))
        with smtplib.SMTP("smtp.gmail.com", 587, timeout=30) as srv:
            srv.starttls()
            srv.login(EMAIL, APP_PASSWORD)
            srv.send_message(msg)
    except Exception:
        pass


# ─────────────────────────────────────────
# CLI
# ─────────────────────────────────────────

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )
    once        = "--once"    in sys.argv
    status_only = "--status"  in sys.argv
    no_heal     = "--no-heal" in sys.argv
    run_watchdog(once=once, status_only=status_only, self_heal=not no_heal)
