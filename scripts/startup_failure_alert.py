"""
scripts/startup_failure_alert.py — Startup failure alert for systemd OnFailure=.

Called automatically by recruiter-pipeline-alert@<service>.service when a pipeline
service (recruiter-scheduler or recruiter-watchdog) hits its StartLimitBurst — i.e. it
crashed and restarted so many times so fast that systemd gave up on it.

Usage (called by systemd, not directly):
    python scripts/startup_failure_alert.py recruiter-scheduler
    python scripts/startup_failure_alert.py recruiter-watchdog

This is a one-shot script: send one email and exit.
Redis dedup key prevents duplicate emails if systemd re-triggers it.

Trigger condition:
    StartLimitBurst=5 + StartLimitIntervalSec=300s in the service unit means:
    "if the service dies and restarts 5 times in 5 minutes, stop trying and
     call OnFailure=".  This is a genuine repeated-crash scenario, not a
     one-off glitch — always worth a human looking at it.
"""

import html
import os
import smtplib
import sys
import time
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

# Add project root to sys.path so we can import config
_SCRIPT_DIR  = os.path.dirname(os.path.abspath(__file__))
_PROJECT_DIR = os.path.dirname(_SCRIPT_DIR)
sys.path.insert(0, _PROJECT_DIR)

from config import EMAIL, APP_PASSWORD  # noqa: E402 — after sys.path fix

# ─────────────────────────────────────────
# Dedup via a flag file — Redis may be
# unavailable if the service is crashing
# ─────────────────────────────────────────
# Store dedup flag files under the project's run/ directory, not /tmp.
# /tmp is world-writable: a local attacker could pre-create the flag file to
# suppress alerts.  A project-owned directory with mode 700 is not accessible
# to other users.
_RUN_DIR            = os.path.join(_PROJECT_DIR, "run")
_FLAG_FILE_TEMPLATE = os.path.join(_RUN_DIR, "startup_failure_alert_{service}.flag")
_DEDUP_WINDOW_S     = 3600   # suppress duplicate alerts for 1 hour

# Allowlist of services this script may act on.  The service name is used in
# file paths and subprocess arguments; reject anything not on this list to
# prevent path-traversal or unintended journal queries.
_VALID_SERVICES = frozenset({
    "recruiter-scheduler",
    "recruiter-watchdog",
})


def _validate_service(service: str) -> None:
    """Raise ValueError if service is not in the known-good allowlist."""
    if service not in _VALID_SERVICES:
        raise ValueError(
            f"Unknown service {service!r} — must be one of {sorted(_VALID_SERVICES)}"
        )


def _already_sent(service: str) -> bool:
    _validate_service(service)
    flag = _FLAG_FILE_TEMPLATE.format(service=service.replace("-", "_"))
    if not os.path.exists(flag):
        return False
    age = time.time() - os.path.getmtime(flag)
    return age < _DEDUP_WINDOW_S


def _mark_sent(service: str) -> None:
    _validate_service(service)
    os.makedirs(_RUN_DIR, mode=0o700, exist_ok=True)
    # Enforce permissions even if the directory already existed (exist_ok=True
    # silently ignores the mode argument when the dir is pre-created).
    os.chmod(_RUN_DIR, 0o700)
    flag = _FLAG_FILE_TEMPLATE.format(service=service.replace("-", "_"))
    with open(flag, "w") as fh:
        fh.write(datetime.now().isoformat())


# ─────────────────────────────────────────
# JOURNAL SNIPPET
# try to pull the last N log lines via
# journalctl so the email is self-contained
# ─────────────────────────────────────────

def _get_journal_tail(service: str, lines: int = 30) -> str:
    """Return the last `lines` journal entries for `service` as plain text."""
    _validate_service(service)
    try:
        import subprocess
        result = subprocess.run(
            ["/usr/bin/journalctl", "-u", service, "-n", str(lines), "--no-pager",
             "--output=short-precise"],
            capture_output=True, text=True, timeout=10,
        )
        if result.returncode == 0 and result.stdout.strip():
            return result.stdout.strip()
    except Exception:
        pass
    return f"(could not retrieve journal — run: journalctl -u {service} -n 50)"


# ─────────────────────────────────────────
# EMAIL
# ─────────────────────────────────────────

_SERVICE_DISPLAY = {
    "recruiter-scheduler": "Scheduler (main pipeline process)",
    "recruiter-watchdog":  "Watchdog (health monitor)",
}

_DIAGNOSE_HINTS = {
    "recruiter-scheduler": [
        "Check Redis is running: <code>systemctl status redis</code>",
        "Check PostgreSQL is running: <code>systemctl status postgresql</code>",
        "Check .env file exists and is readable: <code>ls -la /home/opc/mail/.env</code>",
        "Check for Python errors: <code>journalctl -u recruiter-scheduler -n 50</code>",
        "Try starting manually: <code>cd /home/opc/mail && source venv/bin/activate "
        "&amp;&amp; python pipeline.py --scheduler</code>",
        "Re-enable after fixing: <code>sudo systemctl reset-failed recruiter-scheduler "
        "&amp;&amp; sudo systemctl start recruiter-scheduler</code>",
    ],
    "recruiter-watchdog": [
        "Check Redis is running: <code>systemctl status redis</code>",
        "Check .env file exists and is readable: <code>ls -la /home/opc/mail/.env</code>",
        "Check for Python errors: <code>journalctl -u recruiter-watchdog -n 50</code>",
        "Try starting manually: <code>cd /home/opc/mail && source venv/bin/activate "
        "&amp;&amp; python -m workers.watchdog</code>",
        "Re-enable after fixing: <code>sudo systemctl reset-failed recruiter-watchdog "
        "&amp;&amp; sudo systemctl start recruiter-watchdog</code>",
    ],
}


def send_startup_failure_alert(service: str) -> None:
    _validate_service(service)   # validate before any I/O so invalid service fails fast

    if not EMAIL or not APP_PASSWORD:
        print(f"[startup-alert] EMAIL or APP_PASSWORD not set — cannot send alert for {service}")
        sys.exit(1)   # non-zero so systemd knows alerting is misconfigured

    if _already_sent(service):
        print(f"[startup-alert] Duplicate suppressed for {service} (within {_DEDUP_WINDOW_S//60}min window)")
        sys.exit(0)

    now_str      = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    display_name = _SERVICE_DISPLAY.get(service, service)
    hints        = _DIAGNOSE_HINTS.get(service, [
        f"Check logs: <code>journalctl -u {service} -n 50</code>",
        f"Re-enable: <code>sudo systemctl reset-failed {service} && "
        f"sudo systemctl start {service}</code>",
    ])

    journal_text = _get_journal_tail(service, lines=30)
    hints_html   = "".join(
        f'<li style="margin:4px 0;">{h}</li>' for h in hints
    )

    body_html = f"""
<p style="color:#ef4444;font-size:18px;font-weight:700;margin:0 0 16px;">
  🆘 Manual intervention required
</p>

<div style="background:#fef2f2;border:1px solid #fca5a5;border-radius:8px;
            padding:16px;margin-bottom:16px;">
  <table style="width:100%;border-collapse:collapse;">
    <tr>
      <td style="color:#7f1d1d;font-weight:700;padding:4px 0;width:140px;">Service</td>
      <td style="color:#1e293b;">{service} — {display_name}</td>
    </tr>
    <tr>
      <td style="color:#7f1d1d;font-weight:700;padding:4px 0;">Time</td>
      <td style="color:#1e293b;">{now_str}</td>
    </tr>
    <tr>
      <td style="color:#7f1d1d;font-weight:700;padding:4px 0;">What happened</td>
      <td style="color:#1e293b;">
        The service crashed and was restarted 5&nbsp;times within 5&nbsp;minutes.
        systemd has stopped retrying — the service is now in
        <strong>failed</strong> state.
      </td>
    </tr>
  </table>
</div>

<p style="font-weight:700;color:#1e293b;margin:16px 0 8px;">
  Diagnosis steps:
</p>
<ol style="color:#374151;margin:0;padding-left:20px;line-height:1.8;">
  {hints_html}
</ol>

<p style="font-weight:700;color:#1e293b;margin:20px 0 8px;">
  Last {min(30, len(journal_text.splitlines()))} journal lines:
</p>
<pre style="background:#0f172a;color:#e2e8f0;padding:14px;border-radius:6px;
            font-size:11px;line-height:1.5;overflow:auto;
            white-space:pre-wrap;word-break:break-all;">{html.escape(journal_text)}</pre>

<p style="color:#64748b;font-size:12px;margin-top:16px;">
  ⚠ The watchdog's self-healing (5-minute restart loop) cannot help here —
  this alert fires specifically when the service is crashing too fast for any
  automated recovery to succeed.  Manual fix is required.
</p>"""

    subject = f"🆘 Pipeline FAILED: {service} — repeated startup crashes, manual fix needed"

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"]    = EMAIL
    msg["To"]      = EMAIL

    full_html = f"""<!DOCTYPE html>
<html><head><meta charset="UTF-8"></head>
<body style="margin:0;padding:0;background:#f0f2f5;
             font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Arial,sans-serif;">
  <table width="100%" cellpadding="0" cellspacing="0">
    <tr><td align="center" style="padding:24px 0;">
      <table width="640" cellpadding="0" cellspacing="0">
        <tr>
          <td style="background:#7f1d1d;border-radius:10px 10px 0 0;padding:20px 28px;">
            <span style="color:#ffffff;font-size:16px;font-weight:700;">
              {subject}
            </span>
            <span style="color:#fca5a5;font-size:12px;float:right;padding-top:3px;">
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
              Recruiter Pipeline — Startup Failure Alert &nbsp;·&nbsp; {now_str}
            </span>
          </td>
        </tr>
      </table>
    </td></tr>
  </table>
</body></html>"""

    msg.attach(MIMEText(full_html, "html"))

    try:
        with smtplib.SMTP("smtp.gmail.com", 587, timeout=30) as srv:
            srv.starttls()
            srv.login(EMAIL, APP_PASSWORD)
            srv.send_message(msg)
        _mark_sent(service)
        print(f"[startup-alert] Alert sent for {service}")
    except Exception as exc:
        print(f"[startup-alert] Failed to send email: {exc}", file=sys.stderr)
        sys.exit(1)


# ─────────────────────────────────────────
# ENTRY POINT
# ─────────────────────────────────────────

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python scripts/startup_failure_alert.py <service-name>",
              file=sys.stderr)
        print("  e.g. python scripts/startup_failure_alert.py recruiter-scheduler",
              file=sys.stderr)
        sys.exit(1)

    service_name = sys.argv[1]
    # Normalise: systemd passes %n which may include .service suffix
    if service_name.endswith(".service"):
        service_name = service_name[: -len(".service")]

    send_startup_failure_alert(service_name)
