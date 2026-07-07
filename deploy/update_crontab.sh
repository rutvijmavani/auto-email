#!/bin/bash
# deploy/update_crontab.sh — Apply crontab-only fixes without running full setup_cron.sh.
#
# Changes applied:
#   1. MONITOR RETRY  — 9 AM fallback cron that runs --monitor-jobs only if
#                       the 7 AM run did not complete successfully (checks exit=0).
#   2. LOG MONITOR    — Every-15-minute cron that scans log files for new ERRORs
#                       and sends batched email alerts (Redis-deduped per fingerprint).
#   3. KEEP-ALIVE     — Changed from "every 4 days at noon" to "every 4 hours"
#                       so Oracle never sees a 10+ hour idle window overnight.
#
# Run as opc (no sudo needed — edits the opc crontab):
#   bash deploy/update_crontab.sh

set -euo pipefail

# Guard: must run as opc — root or any other user would edit the wrong crontab.
if [[ "${EUID:-$(id -u)}" -eq 0 ]]; then
    echo "[ERROR] Do not run this script as root / with sudo."
    echo "        Run as the opc user directly: bash deploy/update_crontab.sh"
    exit 1
fi
if [[ "$(whoami)" != "opc" ]]; then
    echo "[ERROR] This script must be run as the 'opc' user (current user: $(whoami))."
    echo "        Switch to opc first: sudo su - opc"
    exit 1
fi

echo "► Updating crontab (monitor retry + keep-alive frequency)..."

# ── Capture existing crontab ──────────────────────────────────────────────────
EXISTING=$(crontab -l 2>/dev/null || true)

# ── Remove only the entries being updated, preserving everything else ─────────
# Both the marker path and the legacy path use the same surgical grep-v list so
# the `0 7 run_monitor.sh` anchor is always preserved for awk injection.
# The marker path additionally strips the BEGIN/END marker lines themselves
# (setup_cron.sh will re-add them on the next full setup run).
_STRIP_PATTERNS=(
    "monitor_.*log.*run_monitor"
    "scripts/log_monitor\.py"
    "hashlib.*range(100000)"
)

CLEANED="$EXISTING"
for _pat in "${_STRIP_PATTERNS[@]}"; do
    CLEANED=$(echo "$CLEANED" | grep -v "$_pat" || true)
done

# Strip per-block markers inserted by this script on previous runs so the
# blocks can be replaced cleanly without accumulating comment headers.
for _marker in "MONITOR RETRY" "LOG MONITOR" "KEEP-ALIVE"; do
    CLEANED=$(echo "$CLEANED" \
      | sed "/# === BEGIN ${_marker} ===/,/# === END ${_marker} ===/d" \
      || true)
done

# Strip RECRUITER PIPELINE markers if present (re-added by setup_cron.sh later).
if echo "$EXISTING" | grep -q "# === BEGIN RECRUITER PIPELINE ==="; then
    CLEANED=$(echo "$CLEANED" \
      | grep -v "# === BEGIN RECRUITER PIPELINE ===" \
      | grep -v "# === END RECRUITER PIPELINE ===" \
      || true)
fi

# ── Build the new crontab ─────────────────────────────────────────────────────
# We inject the retry guard immediately after the 7 AM monitor line and append
# the updated keep-alive at the end.
# The awk pattern matches on the two essential components (time "0 7" and script
# name "run_monitor.sh") rather than the exact full-line format, so minor spacing
# variations in the existing crontab still trigger the insertion.

NEW_CRON=$(echo "$CLEANED" | awk '
/^0 7[[:space:]].*run_monitor\.sh/ {
    print
    print ""
    print "# === BEGIN MONITOR RETRY ==="
    print "# ─────────────────────────────────────────"
    print "# MONITOR RETRY — 9 AM fallback"
    print "# Runs only if the 7 AM job did not complete successfully."
    print "# Checks for exit=0 in today'\''s monitor log (written by run_monitor.sh"
    print "# after --monitor-jobs finishes); if absent, runs the monitor now so the"
    print "# digest arrives at most 2 hours late rather than not at all."
    print "# ─────────────────────────────────────────"
    print "0 9 * * * grep -q '\''exit=0'\'' /home/opc/mail/logs/monitor_$(date +\\%Y-\\%m-\\%d).log 2>/dev/null || pgrep -f '\''bash /home/opc/mail/run_monitor\\.sh'\'' > /dev/null 2>&1 || /home/opc/mail/run_monitor.sh"
    print "# === END MONITOR RETRY ==="
    next
}
{ print }
')

NEW_CRON="${NEW_CRON}

# === BEGIN LOG MONITOR ===
# ─────────────────────────────────────────
# LOG MONITOR — every 15 minutes
# Scans log files for new ERRORs / tracebacks since the last byte offset.
# Sends an immediate alert email per new error fingerprint (Redis-deduped
# so each unique issue emails at most once per hour).  A separate 3-day
# digest collects all seen fingerprints.
# State (offsets, inodes) tracked in data/log_monitor_state.json.
# ─────────────────────────────────────────
*/15 * * * * /home/opc/mail/venv/bin/python /home/opc/mail/scripts/log_monitor.py >> /home/opc/mail/logs/log_monitor_\$(date +\%Y-\%m-\%d).log 2>&1; find /home/opc/mail/logs -name 'log_monitor_*.log' -mtime +14 -delete
# === END LOG MONITOR ===

# === BEGIN KEEP-ALIVE ===
# ─────────────────────────────────────────
# KEEP-ALIVE — every 4 hours (Oracle idle protection)
# Prevents Oracle Cloud from suspending the VM overnight.
# Previously ran once every 4 days at noon; the VM was being suspended during
# the ~10-hour overnight window which caused the 7 AM monitor cron to be
# missed.  Running every 4 hours (00:00, 04:00, 08:00, 12:00, 16:00, 20:00)
# ensures no idle window exceeds 4 hours.
# ─────────────────────────────────────────
0 */4 * * * python3 -c \"import hashlib; [hashlib.sha256(str(i).encode()).hexdigest() for i in range(100000)]\" >> /dev/null 2>&1
# === END KEEP-ALIVE ==="

# ── Validate required sections before installing ──────────────────────────────
_cron_ok=1
if ! echo "$NEW_CRON" | grep -q "0 9.*run_monitor\.sh"; then
    echo "  [WARN] 9 AM retry job is missing from the new crontab template."
    echo "         Expected format: 0 9 * * * .../run_monitor.sh"
    _cron_ok=0
fi
if ! echo "$NEW_CRON" | grep -q "log_monitor\.py"; then
    echo "  [WARN] LOG MONITOR section is missing from the new crontab template."
    _cron_ok=0
fi
if ! echo "$NEW_CRON" | grep -q "hashlib.*range(100000)"; then
    echo "  [WARN] KEEP-ALIVE section is missing from the new crontab template."
    _cron_ok=0
fi
if [[ "$_cron_ok" -eq 0 ]]; then
    echo "  Aborting — crontab was NOT changed."
    exit 1
fi

# ── Install ───────────────────────────────────────────────────────────────────
echo "$NEW_CRON" | crontab -
echo "  [OK] Crontab updated"

# ── Verify ───────────────────────────────────────────────────────────────────
echo ""
echo "► Current crontab (monitor + keep-alive + log-monitor sections):"
crontab -l | grep -A6 -E "^0 7|MONITOR RETRY|LOG MONITOR|KEEP-ALIVE|hashlib|log_monitor"
echo ""
echo "Done. No service restarts needed — cron changes take effect immediately."
