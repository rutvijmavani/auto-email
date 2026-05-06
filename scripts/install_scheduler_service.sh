#!/bin/bash
# scripts/install_scheduler_service.sh
#
# Creates and enables a systemd service for the adaptive polling scheduler.
# The scheduler is a long-running process (not a cron job) — systemd keeps
# it alive across reboots and restarts it automatically on crash.
#
# Usage (run once on the server as opc):
#   chmod +x scripts/install_scheduler_service.sh
#   ./scripts/install_scheduler_service.sh

set -e

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
VENV="$PROJECT_DIR/venv"
SERVICE_NAME="recruiter-scheduler"
SERVICE_FILE="/etc/systemd/system/${SERVICE_NAME}.service"
USER="opc"

echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo " Installing adaptive scheduler systemd service"
echo " Project: $PROJECT_DIR"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""

# ─────────────────────────────────────────
# 1. Verify Redis is running
# ─────────────────────────────────────────
echo "[1/4] Checking Redis..."
if ! systemctl is-active --quiet redis 2>/dev/null && \
   ! systemctl is-active --quiet redis-server 2>/dev/null; then
    echo "[WARN] Redis does not appear to be running."
    echo "       Install it first: sudo dnf install -y redis && sudo systemctl enable --now redis"
    echo "       Then re-run this script."
    exit 1
fi
echo "[OK]  Redis is running"
echo ""

# ─────────────────────────────────────────
# 2. Write the systemd unit file
# ─────────────────────────────────────────
echo "[2/4] Writing $SERVICE_FILE..."

sudo tee "$SERVICE_FILE" > /dev/null <<EOF
[Unit]
Description=Recruiter Pipeline — Adaptive Polling Scheduler
Documentation=file://$PROJECT_DIR/docs/adaptive-polling-architecture.md
After=network.target postgresql.service redis.service
Wants=postgresql.service redis.service

[Service]
Type=simple
User=$USER
WorkingDirectory=$PROJECT_DIR
Environment=PYTHONPATH=$PROJECT_DIR
EnvironmentFile=$PROJECT_DIR/.env

ExecStart=$VENV/bin/python -m workers.scheduler
Restart=on-failure
RestartSec=15s

# Give workers 30s to finish their current scan before force-killing
TimeoutStopSec=30s
KillMode=mixed
KillSignal=SIGINT

# Logging — view with: journalctl -u $SERVICE_NAME -f
StandardOutput=journal
StandardError=journal
SyslogIdentifier=$SERVICE_NAME

[Install]
WantedBy=multi-user.target
EOF

echo "[OK]  Service file written"
echo ""

# ─────────────────────────────────────────
# 3. Check workers/__main__.py exists
# ─────────────────────────────────────────
echo "[3/4] Checking workers package entry point..."

MAIN_FILE="$PROJECT_DIR/workers/__main__.py"
if [ ! -f "$MAIN_FILE" ]; then
    cat > "$MAIN_FILE" <<'PY'
"""workers/__main__.py — Entry point for: python -m workers.scheduler"""
from workers.scheduler import run_scheduler
import sys

if __name__ == "__main__":
    skip = "--skip-rebuild" in sys.argv
    run_scheduler(skip_rebuild=skip)
PY
    echo "[OK]  Created workers/__main__.py"
else
    echo "[OK]  workers/__main__.py already exists"
fi
echo ""

# ─────────────────────────────────────────
# 4. Enable and start
# ─────────────────────────────────────────
echo "[4/4] Enabling and starting service..."

sudo systemctl daemon-reload
sudo systemctl enable "$SERVICE_NAME"
sudo systemctl start  "$SERVICE_NAME"

sleep 3   # give it a moment to start

STATUS=$(systemctl is-active "$SERVICE_NAME")
if [ "$STATUS" = "active" ]; then
    echo "[OK]  $SERVICE_NAME is running"
else
    echo "[WARN] Service status: $STATUS"
    echo "       Check logs: journalctl -u $SERVICE_NAME -n 50 --no-pager"
fi

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo " Done! Useful commands:"
echo ""
echo "  View live logs:"
echo "    journalctl -u $SERVICE_NAME -f"
echo ""
echo "  Check status:"
echo "    systemctl status $SERVICE_NAME"
echo ""
echo "  Restart after a code deploy:"
echo "    sudo systemctl restart $SERVICE_NAME"
echo ""
echo "  Stop:"
echo "    sudo systemctl stop $SERVICE_NAME"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
