#!/bin/bash
# deploy/first_time_setup.sh — One-time server setup for the mail pipeline.
#
# Run this ONCE after your first git clone on a new server.
# After this, use deploy/deploy.sh for every code update.
#
# What it does (in order):
#   1. Checks all prerequisites (venv, .env, Redis, Python)
#   2. Runs deploy/install-systemd.sh
#        → Stops any old nohup/cron processes
#        → Installs and enables recruiter-scheduler + recruiter-watchdog systemd units
#        → Adds the sudoers rule so watchdog can self-heal
#        → Starts both services, then immediately stops them
#   3. Runs deploy/configure-redis.sh
#        → Enables AOF persistence (saves every ~1 second)
#        → Sets auto-rewrite thresholds (file never grows unbounded)
#        → Persists settings to redis.conf
#   3b. Starts recruiter-scheduler + recruiter-watchdog (Redis now durable)
#   4. Waits for worker heartbeats to appear in Redis
#   5. Runs scripts/health_check.py — must exit 0 before setup is complete
#
# Usage:
#   sudo bash deploy/first_time_setup.sh
#
# Requirements:
#   - Run with sudo (both sub-scripts require root)
#   - Python venv already created at <project>/venv
#   - .env file already created with all credentials
#   - Redis must be running before this script is called

set -euo pipefail

DEPLOY_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$DEPLOY_DIR")"
SERVICE_USER="${SUDO_USER:-opc}"
PYTHON="$PROJECT_DIR/venv/bin/python"

# Reject root as the service user — same guard as install-systemd.sh.
# SERVICE_USER=root happens when SUDO_USER is explicitly set to root or when
# the script is invoked directly as root without sudo (empty SUDO_USER and
# no fallback matches root).
if [[ "$SERVICE_USER" == "root" ]]; then
    echo "[ERROR] SERVICE_USER resolved to 'root'."
    echo "        Run with sudo as a non-root user: sudo bash deploy/first_time_setup.sh"
    echo "        Or export SUDO_USER=opc before running."
    exit 1
fi

echo "════════════════════════════════════════════════════════════"
echo "  Mail Pipeline — first-time server setup"
echo "  Project : $PROJECT_DIR"
echo "  User    : $SERVICE_USER"
echo "  Time    : $(date '+%Y-%m-%d %H:%M:%S')"
echo "════════════════════════════════════════════════════════════"

# ── Must be run with sudo ─────────────────────────────────────────────────────
if [[ $EUID -ne 0 ]]; then
    echo ""
    echo "[ERROR] This script must be run with sudo:"
    echo "        sudo bash deploy/first_time_setup.sh"
    exit 1
fi

# ─────────────────────────────────────────
# STEP 0 — PREREQUISITE CHECKS
# ─────────────────────────────────────────
echo ""
echo "════════════════════════════════════════════════════════════"
echo "  STEP 0 — Checking prerequisites"
echo "════════════════════════════════════════════════════════════"

ERRORS=0

# Python venv
if [[ -f "$PYTHON" ]]; then
    PYTHON_VERSION=$("$PYTHON" --version 2>&1)
    echo "  ✓ Python venv found: $PYTHON_VERSION"
else
    echo "  ✗ Python venv not found at $PROJECT_DIR/venv"
    echo "    Fix: cd $PROJECT_DIR && python3 -m venv venv"
    echo "         source venv/bin/activate && pip install -r requirements.txt"
    ERRORS=$((ERRORS + 1))
fi

# .env file
if [[ -f "$PROJECT_DIR/.env" ]]; then
    echo "  ✓ .env file found"
else
    echo "  ✗ .env file not found at $PROJECT_DIR/.env"
    echo "    Fix: create .env with all required credentials (see deployment.md)"
    ERRORS=$((ERRORS + 1))
fi

# Redis reachable
if redis-cli ping > /dev/null 2>&1; then
    REDIS_VERSION=$(redis-cli INFO server 2>/dev/null | grep redis_version | cut -d: -f2 | tr -d '[:space:]')
    echo "  ✓ Redis is running (version $REDIS_VERSION)"
else
    echo "  ✗ Redis is not reachable (redis-cli ping failed)"
    echo "    Fix: sudo systemctl start redis"
    ERRORS=$((ERRORS + 1))
fi

# PostgreSQL (via Python)
if sudo -u "$SERVICE_USER" bash -c 'cd "$1" && source venv/bin/activate && python -c "from db.connection import get_conn; get_conn().close(); print(\"ok\")"' _ "$PROJECT_DIR" > /dev/null 2>&1; then
    echo "  ✓ PostgreSQL is reachable"
else
    echo "  ✗ PostgreSQL is not reachable"
    echo "    Fix: check DB_HOST / DB_PORT / DB_NAME in .env"
    ERRORS=$((ERRORS + 1))
fi

# Required sub-scripts exist
for script in install-systemd.sh configure-redis.sh; do
    if [[ -f "$DEPLOY_DIR/$script" ]]; then
        echo "  ✓ $script found"
    else
        echo "  ✗ $DEPLOY_DIR/$script not found"
        ERRORS=$((ERRORS + 1))
    fi
done

# health_check.py
if [[ -f "$PROJECT_DIR/scripts/health_check.py" ]]; then
    echo "  ✓ scripts/health_check.py found"
else
    echo "  ✗ scripts/health_check.py not found"
    ERRORS=$((ERRORS + 1))
fi

if [[ $ERRORS -gt 0 ]]; then
    echo ""
    echo "[ERROR] $ERRORS prerequisite(s) failed — fix the above issues and re-run."
    exit 1
fi

echo ""
echo "  All prerequisites met. Proceeding with setup."

# ─────────────────────────────────────────
# STEP 1 — SYSTEMD SERVICES (install only, services started after Redis AOF)
# ─────────────────────────────────────────
echo ""
echo "════════════════════════════════════════════════════════════"
echo "  STEP 1 — Installing systemd service units"
echo "════════════════════════════════════════════════════════════"

bash "$DEPLOY_DIR/install-systemd.sh"

echo "  ✓ Step 1 complete — units installed and enabled, services NOT started yet"
echo "    (services start in Step 2b after Redis AOF durability is configured)"

# ─────────────────────────────────────────
# STEP 2 — REDIS AOF PERSISTENCE
# ─────────────────────────────────────────
echo ""
echo "════════════════════════════════════════════════════════════"
echo "  STEP 2 — Configuring Redis AOF persistence"
echo "════════════════════════════════════════════════════════════"

bash "$DEPLOY_DIR/configure-redis.sh"

echo ""
echo "  ✓ Step 2 complete — Redis AOF persistence configured"

# ─────────────────────────────────────────
# STEP 2b — START SERVICES (Redis now durable)
# ─────────────────────────────────────────
echo ""
echo "════════════════════════════════════════════════════════════"
echo "  STEP 2b — Starting services (Redis AOF now active)"
echo "════════════════════════════════════════════════════════════"

sudo systemctl start recruiter-scheduler recruiter-watchdog
echo "  ✓ recruiter-scheduler and recruiter-watchdog started"

# ─────────────────────────────────────────
# STEP 3 — WAIT FOR WORKER HEARTBEATS
# ─────────────────────────────────────────
echo ""
echo "════════════════════════════════════════════════════════════"
echo "  STEP 3 — Waiting for all worker heartbeats"
echo "════════════════════════════════════════════════════════════"
echo ""
echo "  Workers need ~20 seconds to start and write their first heartbeat."

WORKERS=("scheduler" "scan_worker" "detail_worker" "fullscan_worker")
WAIT_MAX=60    # seconds to wait before giving up
INTERVAL=5
ELAPSED=0

while true; do
    MISSING=()
    for worker in "${WORKERS[@]}"; do
        # scheduler writes per-loop keys (adaptive + fullscan); consider it alive
        # when at least one loop key is present.
        if [[ "$worker" == "scheduler" ]]; then
            _adp=$(redis-cli GET "worker:alive:scheduler:adaptive" 2>/dev/null)
            _fsc=$(redis-cli GET "worker:alive:scheduler:fullscan" 2>/dev/null)
            if [[ -z "$_adp" && -z "$_fsc" ]]; then
                MISSING+=("$worker")
            fi
        else
            # Heartbeat keys are per-PID: worker:alive:{type}:{pid}
            if [[ -z "$(redis-cli --scan --pattern "worker:alive:${worker}:*" 2>/dev/null | head -1)" ]]; then
                MISSING+=("$worker")
            fi
        fi
    done

    if [[ ${#MISSING[@]} -eq 0 ]]; then
        echo "  ✓ All worker heartbeats present"
        break
    fi

    if [[ $ELAPSED -ge $WAIT_MAX ]]; then
        echo "  [WARN] Timed out after ${WAIT_MAX}s — missing heartbeats: ${MISSING[*]}"
        echo "         Health check below will show the exact problem."
        break
    fi

    echo "  Waiting... (${ELAPSED}s elapsed, missing: ${MISSING[*]})"
    sleep $INTERVAL
    ELAPSED=$((ELAPSED + INTERVAL))
done

# ─────────────────────────────────────────
# STEP 4 — HEALTH CHECK
# ─────────────────────────────────────────
echo ""
echo "════════════════════════════════════════════════════════════"
echo "  STEP 4 — Running full health check"
echo "════════════════════════════════════════════════════════════"
echo ""

if sudo -u "$SERVICE_USER" bash -c 'cd "$1" && source venv/bin/activate && python scripts/health_check.py' _ "$PROJECT_DIR"; then
    HEALTH_OK=true
else
    HEALTH_OK=false
fi

# ─────────────────────────────────────────
# SUMMARY
# ─────────────────────────────────────────
echo ""
echo "════════════════════════════════════════════════════════════"
echo "  Setup complete!"
echo ""

if $HEALTH_OK; then
    echo "  ✓ Health check passed — pipeline is running and healthy"
else
    echo "  ⚠ Health check reported issues (see above)"
    echo "    Most likely cause: workers still starting up."
    echo "    Wait 30 seconds and re-run:"
    echo "      python scripts/health_check.py"
fi

echo ""
echo "  What was configured:"
echo "    ✓ recruiter-scheduler.service — starts on boot, auto-restarts on crash"
echo "    ✓ recruiter-watchdog.service  — continuously monitors pipeline, restarts dead workers automatically"
echo "    ✓ Redis AOF persistence  — max 1s data loss on crash (was ~5 min)"
echo "    ✓ AOF auto-rewrite       — file never grows unbounded"
echo "    ✓ Sudoers rule           — watchdog can restart scheduler without password"
echo ""
echo "  Everyday commands:"
echo "    bash deploy/deploy.sh              # deploy new code after every git push"
echo "    python scripts/health_check.py    # instant system status snapshot"
echo "    journalctl -u recruiter-scheduler -f   # live scheduler + worker logs"
echo "    journalctl -u recruiter-watchdog  -f   # live watchdog logs"
echo ""
echo "  This script does not need to be run again unless you set up a"
echo "  brand-new server. For code updates, use deploy/deploy.sh."
echo "════════════════════════════════════════════════════════════"

# Propagate health check failure so calling CI/CD systems see a non-zero exit
# code when setup completes but workers are not yet healthy.
$HEALTH_OK || exit 1
