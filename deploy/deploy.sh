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

# Detect detached HEAD before capturing PREVIOUS_SHA — _BRANCH must be defined
# before the ERR trap fires so _rollback_to_previous can use it safely under set -u.
_BRANCH=$(git rev-parse --abbrev-ref HEAD)
if [[ "$_BRANCH" == "HEAD" ]]; then
    echo "[ERROR] Repository is in detached HEAD state — cannot pull."
    echo "  Run: git checkout main   (or the appropriate branch)"
    exit 1
fi

# Capture the current SHA before any update so we can roll back if needed.
PREVIOUS_SHA=$(git rev-parse HEAD)

# ── Shared rollback helper ────────────────────────────────────────────────────
_rollback_to_previous() {
    echo "  [ROLLBACK] Reverting to $PREVIOUS_SHA..."
    git reset --hard HEAD 2>/dev/null || true
    local _checkout_ok=true
    if ! git checkout -B "$_BRANCH" "$PREVIOUS_SHA"; then
        echo "  [FATAL] git rollback failed — code state unknown"
        _checkout_ok=false
    fi

    # pip install and unit sync require known-good code — skip if checkout failed
    if $_checkout_ok; then
        "$PROJECT_DIR/venv/bin/pip" install -q -r "$PROJECT_DIR/requirements.txt" \
            || echo "  [FATAL] pip rollback failed"
        sudo /usr/local/bin/install-pipeline-units || echo "  [WARN] unit sync failed"
        sudo systemctl daemon-reload || true
    else
        echo "  [WARN] Skipping pip install and unit sync — code state unknown"
    fi

    # Always attempt service restarts: a running service on stale code is better
    # than no service at all
    sudo systemctl restart recruiter-scheduler \
        || echo "  [WARN] could not restart recruiter-scheduler"
    sudo systemctl restart recruiter-watchdog \
        || echo "  [WARN] could not restart recruiter-watchdog"
    sudo systemctl restart pipeline-api \
        || echo "  [WARN] could not restart pipeline-api"
    sudo systemctl restart recruiter-manager \
        || echo "  [WARN] could not restart recruiter-manager"
    sudo systemctl restart email-processor \
        || echo "  [WARN] could not restart email-processor"

    # Post-rollback verification
    sleep 3
    local _all_active=true
    for svc in recruiter-scheduler recruiter-watchdog pipeline-api recruiter-manager email-processor; do
        if systemctl is-active --quiet "$svc"; then
            echo "  [OK] $svc is active after rollback"
        else
            echo "  [WARN] $svc failed to start after rollback — manual intervention required"
            journalctl -u "$svc" -n 20 --no-pager || true
            _all_active=false
        fi
    done
    echo "  [ROLLBACK] Check logs:"
    echo "             journalctl -u recruiter-scheduler -n 50"
    echo "             journalctl -u recruiter-watchdog  -n 20"
    echo "             journalctl -u pipeline-api        -n 20"
    $_all_active || return 1
}

# Trap ERR during the pull/install phase so a partial update auto-rolls back.
# Disabled after restarts begin (explicit rollback calls take over from there).
_pull_err() {
    local _exit=$?
    echo ""
    echo "  [ERROR] Pull/install step failed (exit $_exit) — rolling back to $PREVIOUS_SHA"
    _rollback_to_previous
    exit $_exit
}

CURRENT_SHA=$(git rev-parse --short HEAD)
git fetch origin

# Count commits we're about to pull in
BEHIND=$(git rev-list --count "HEAD..origin/${_BRANCH}" 2>/dev/null || echo "?")

# Arm ERR trap only AFTER the read-only fetch+count steps so a transient
# network failure during fetch doesn't trigger a rollback of unchanged code.
trap '_pull_err' ERR
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
    git pull origin "${_BRANCH}"
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

# Pull/install complete — hand off to explicit rollback calls for remaining steps.
trap - ERR

if $NO_RESTART; then
    echo ""
    echo "════════════════════════════════════════════════════════════"
    echo "  Code updated. Restart skipped (--no-restart)."
    echo "  To restart manually: sudo systemctl restart recruiter-scheduler recruiter-watchdog pipeline-api"
    echo "════════════════════════════════════════════════════════════"
    exit 0
fi

# ── Sync systemd unit files ───────────────────────────────────────────────────
# Unit file changes (OnFailure=, ordering, env vars) are only picked up after
# the unit files on disk are refreshed and systemd reloads its unit graph.
# Run this before restart so the updated definitions are always active.
echo ""
echo "► Syncing systemd unit files..."
# NOTE: install-pipeline-units reads from the root-owned staging directory
# (/usr/local/share/mail-pipeline/systemd/).  If you changed a .service file
# in this commit, you MUST re-stage it first:
#   sudo bash deploy/install-systemd.sh
# (one-time per structural unit change; deploy.sh does not auto-restage)
if [[ ! -x /usr/local/bin/install-pipeline-units ]]; then
    echo "[ERROR] /usr/local/bin/install-pipeline-units not found."
    echo "        Run 'sudo bash deploy/install-systemd.sh' once to provision it."
    _rollback_to_previous; exit 1
fi
# Guard: verify staging is current before installing — same check as deploy.yml.
# If a unit file changed in this commit and install-systemd.sh was not re-run,
# install-pipeline-units would silently install the stale staged copy.
_stale_units=0
for _unit in "recruiter-scheduler.service" "recruiter-watchdog.service" "recruiter-pipeline-alert@.service" "pipeline-api.service"; do
    _src="$DEPLOY_DIR/systemd/$_unit"
    _staged="/usr/local/share/mail-pipeline/systemd/$_unit"
    if [[ ! -f "$_staged" ]]; then
        echo "[ERROR] Staging file missing: $_staged — run: sudo bash deploy/install-systemd.sh"
        _stale_units=1
    elif ! diff -q "$_src" "$_staged" > /dev/null 2>&1; then
        echo "[ERROR] Staged unit out of date: $_unit (deploy/systemd/ vs staging)"
        echo "        Run: sudo bash deploy/install-systemd.sh"
        _stale_units=1
    fi
done
if [[ "$_stale_units" -eq 1 ]]; then
    echo "[ERROR] Systemd unit staging is stale — rolling back"
    _rollback_to_previous; exit 1
fi
sudo /usr/local/bin/install-pipeline-units || {
    echo "  [ERROR] Unit sync failed — rolling back"
    _rollback_to_previous; exit 1
}
echo "  systemd unit files installed"
sudo systemctl daemon-reload || {
    echo "  [ERROR] daemon-reload failed — rolling back"
    _rollback_to_previous; exit 1
}
echo "  systemd daemon reloaded"

# ── Restart systemd services ──────────────────────────────────────────────────
# Restarting recruiter-scheduler is enough — it spawns all workers (scan_worker,
# detail_worker, fullscan_worker) via multiprocessing.  The watchdog gets
# restarted separately so it picks up any watchdog.py changes.
echo ""
echo "► Restarting services..."

sudo systemctl restart recruiter-scheduler || {
    echo "  [ERROR] recruiter-scheduler failed to restart — rolling back"
    _rollback_to_previous; exit 1
}
echo "  Restarted: recruiter-scheduler"

sudo systemctl restart recruiter-watchdog || {
    echo "  [ERROR] recruiter-watchdog failed to restart — rolling back"
    _rollback_to_previous; exit 1
}

sudo systemctl restart pipeline-api || {
    echo "  [ERROR] pipeline-api failed to restart — rolling back"
    _rollback_to_previous; exit 1
}

sudo systemctl restart recruiter-manager || {
    echo "  [ERROR] recruiter-manager failed to restart — rolling back"
    _rollback_to_previous; exit 1
}

sudo systemctl restart email-processor || {
    echo "  [ERROR] email-processor failed to restart — rolling back"
    _rollback_to_previous; exit 1
}

echo ""
echo "  Waiting for services to come up..."
sleep 5

# ── Quick status check ────────────────────────────────────────────────────────
echo ""
echo "► Service status:"
_svc_fail=0
for svc in recruiter-scheduler recruiter-watchdog pipeline-api recruiter-manager email-processor; do
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
    echo "  [ERROR] One or more services failed to start"
    _rollback_to_previous
    exit 1
fi

# ── Health check ─────────────────────────────────────────────────────────────
if $SKIP_HEALTH; then
    echo ""
    echo "  Health check skipped (--skip-health)."
else
    echo ""
    echo "► Running health check (waiting 70s for worker heartbeats)..."
    sleep 70
    "$PROJECT_DIR/venv/bin/python" scripts/health_check.py || {
        echo ""
        echo "  [WARN] Health check reported issues — rolling back to $PREVIOUS_SHA"
        _rollback_to_previous
        exit 1
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
