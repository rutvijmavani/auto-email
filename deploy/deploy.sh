#!/bin/bash
# deploy/deploy.sh — Zero-downtime code deployment for the mail pipeline.
#
# Run this every time you push new code to the server.
# systemd handles the restart — workers come back automatically.
#
# Usage:
#   bash deploy/deploy.sh                  # pull + restart + health check
#   bash deploy/deploy.sh --skip-health    # pull + restart only (skip wait)
#   bash deploy/deploy.sh --no-restart     # pull only (manual restart later)
#
# Requirements:
#   - Run as opc (NOT sudo — systemctl restart is granted via sudoers)
#   - Git remote 'origin' pointing to your repo
#   - systemd units already installed (run install-systemd.sh once first)

set -euo pipefail

DEPLOY_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$DEPLOY_DIR")"
SKIP_HEALTH=false
NO_RESTART=false

for arg in "$@"; do
    case "$arg" in
        --skip-health) SKIP_HEALTH=true ;;
        --no-restart)  NO_RESTART=true  ;;
    esac
done

echo "════════════════════════════════════════════════════════════"
echo "  Mail Pipeline — code deployment"
echo "  Project : $PROJECT_DIR"
echo "  Time    : $(date '+%Y-%m-%d %H:%M:%S')"
echo "════════════════════════════════════════════════════════════"

# ── Sanity: must NOT be root ──────────────────────────────────────────────────
if [[ $EUID -eq 0 ]]; then
    echo "[ERROR] Do NOT run deploy.sh as root / sudo."
    echo "        Run as the service user (opc): bash deploy/deploy.sh"
    exit 1
fi

# ── Pull latest code ──────────────────────────────────────────────────────────
echo ""
echo "► Pulling latest code from origin..."
cd "$PROJECT_DIR"

# Show what's incoming before we pull
CURRENT_SHA=$(git rev-parse --short HEAD)
git fetch origin

# Count commits we're about to pull in
BEHIND=$(git rev-list --count HEAD..origin/$(git rev-parse --abbrev-ref HEAD) 2>/dev/null || echo "?")
echo "  Current commit : $CURRENT_SHA"
echo "  Commits behind : $BEHIND"

if [[ "$BEHIND" == "0" ]]; then
    echo "  Already up to date — skipping pull."
    if $NO_RESTART; then
        echo ""
        echo "════════════════════════════════════════════════════════════"
        echo "  Nothing to do."
        echo "════════════════════════════════════════════════════════════"
        exit 0
    fi
else
    git pull origin "$(git rev-parse --abbrev-ref HEAD)"
    NEW_SHA=$(git rev-parse --short HEAD)
    echo "  Updated to : $NEW_SHA"
    echo ""
    echo "► Recent changes:"
    git log --oneline -5
fi

# ── Install / update Python dependencies ─────────────────────────────────────
echo ""
echo "► Updating Python dependencies..."
if [[ -f "$PROJECT_DIR/requirements.txt" ]]; then
    "$PROJECT_DIR/venv/bin/pip" install -q -r "$PROJECT_DIR/requirements.txt"
    echo "  Dependencies up to date."
else
    echo "  [SKIP] No requirements.txt found."
fi

if $NO_RESTART; then
    echo ""
    echo "════════════════════════════════════════════════════════════"
    echo "  Code updated. Restart skipped (--no-restart)."
    echo "  To restart manually: sudo systemctl restart recruiter-scheduler recruiter-watchdog"
    echo "════════════════════════════════════════════════════════════"
    exit 0
fi

# ── Restart systemd services ──────────────────────────────────────────────────
# Restarting recruiter-scheduler is enough — it spawns all workers (scan_worker,
# detail_worker, fullscan_worker) via multiprocessing.  The watchdog gets
# restarted separately so it picks up any watchdog.py changes.
echo ""
echo "► Restarting services..."

sudo systemctl restart recruiter-scheduler
echo "  Restarted: recruiter-scheduler"

sudo systemctl restart recruiter-watchdog
echo "  Restarted: recruiter-watchdog"

echo ""
echo "  Waiting for services to come up..."
sleep 5

# ── Quick status check ────────────────────────────────────────────────────────
echo ""
echo "► Service status:"
_svc_fail=0
for svc in recruiter-scheduler recruiter-watchdog; do
    status=$(systemctl is-active "$svc" 2>/dev/null || echo "unknown")
    pid=$(systemctl show -p MainPID --value "$svc" 2>/dev/null || echo "?")
    if [[ "$status" == "active" ]]; then
        echo "  ✓ $svc: $status (pid=$pid)"
    else
        echo "  ✗ $svc: $status (pid=$pid)  ← PROBLEM"
        _svc_fail=1
    fi
done
if [[ $_svc_fail -ne 0 ]]; then
    echo ""
    echo "  [ERROR] One or more services failed to start — check logs:"
    echo "          journalctl -u recruiter-scheduler -n 50"
    echo "          journalctl -u recruiter-watchdog  -n 20"
    exit 1
fi

# ── Health check ─────────────────────────────────────────────────────────────
if $SKIP_HEALTH; then
    echo ""
    echo "  Health check skipped (--skip-health)."
else
    echo ""
    echo "► Running health check (waiting 15s for worker heartbeats)..."
    sleep 15
    "$PROJECT_DIR/venv/bin/python" scripts/health_check.py || {
        echo ""
        echo "  [WARN] Health check reported issues — check logs:"
        echo "         journalctl -u recruiter-scheduler -n 50"
        echo "         journalctl -u recruiter-watchdog  -n 20"
    }
fi

echo ""
echo "════════════════════════════════════════════════════════════"
echo "  Deployment complete!"
echo ""
echo "  Useful commands:"
echo "    journalctl -u recruiter-scheduler -f      # live scheduler logs"
echo "    journalctl -u recruiter-watchdog  -f      # live watchdog logs"
echo "    bash deploy/deploy.sh                # deploy again"
echo "════════════════════════════════════════════════════════════"
