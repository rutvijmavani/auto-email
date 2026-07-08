#!/bin/bash
# deploy/install-systemd.sh — One-time systemd setup for the pipeline.
#
# Run this ONCE on the server after initial deployment.
# After this, use deploy/deploy.sh for every code update.
#
# Requirements:
#   - Run as a non-root user with sudo (e.g. opc, or set SUDO_USER=opc)
#   - Python venv at <project>/venv  (PROJECT_DIR derived from script location)
#   - .env file at <project>/.env    (same PROJECT_DIR)

set -euo pipefail

DEPLOY_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$DEPLOY_DIR")"
SERVICE_USER="${SUDO_USER:-opc}"

# Reject root as the service user — pipeline processes must not run as root.
# This happens when the script is invoked directly as root (not via sudo <user>),
# making SUDO_USER empty so the fallback "opc" applies, or when SUDO_USER=root.
if [[ "$SERVICE_USER" == "root" ]]; then
    echo "[ERROR] SERVICE_USER resolved to 'root'."
    echo "        Run with sudo as a non-root user: sudo bash deploy/install-systemd.sh"
    echo "        Or export SUDO_USER=opc before running."
    exit 1
fi

echo "════════════════════════════════════════════════════════════"
echo "  Mail Pipeline — systemd setup"
echo "  Project : $PROJECT_DIR"
echo "  User    : $SERVICE_USER"
echo "════════════════════════════════════════════════════════════"

# ── Sanity checks ─────────────────────────────────────────────────────────────
if [[ $EUID -ne 0 ]]; then
    echo "[ERROR] Run this script with sudo: sudo bash deploy/install-systemd.sh"
    exit 1
fi

if ! id "$SERVICE_USER" &>/dev/null; then
    echo "[ERROR] Service user '$SERVICE_USER' does not exist on this host."
    echo "        Create the user or set SUDO_USER to the correct account."
    exit 1
fi

if [[ ! -f "$PROJECT_DIR/.env" ]]; then
    echo "[ERROR] .env file not found at $PROJECT_DIR/.env"
    echo "        Create it with your secrets before continuing."
    exit 1
fi

if [[ ! -f "$PROJECT_DIR/venv/bin/python" ]]; then
    echo "[ERROR] venv not found at $PROJECT_DIR/venv"
    echo "        Run: cd $PROJECT_DIR && python3 -m venv venv && source venv/bin/activate && pip install -r requirements.txt"
    exit 1
fi

# ── Remove watchdog cron entry (replaced by recruiter-watchdog.service) ───────────
# The watchdog was previously run via cron (--once every 5 min) as a temporary
# measure.  Now that it runs continuously under systemd, the cron entry must be
# removed to avoid both running in parallel (doubled emails + doubled heals).
#
# IMPORTANT: this script runs as root (EUID=0).  The old watchdog cron entry
# lives in the SERVICE_USER (opc) crontab — not root's.  Always pass -u so we
# read and write the correct user's crontab.
echo ""
echo "► Removing watchdog cron entry from $SERVICE_USER crontab (now managed by systemd)..."
if crontab -u "$SERVICE_USER" -l 2>/dev/null | grep -qE "workers\.watchdog|watchdog.*--once"; then
    # Under set -o pipefail, grep -v exits 1 when ALL lines are filtered (empty
    # crontab result).  Wrap in a subshell with || true so the pipeline always
    # succeeds and crontab receives stdin even if every line was removed.
    (crontab -u "$SERVICE_USER" -l 2>/dev/null \
        | grep -v "workers.watchdog" \
        | grep -v "watchdog.*--once" \
        || true) | crontab -u "$SERVICE_USER" -
    echo "  Removed watchdog cron entry from $SERVICE_USER crontab"
else
    echo "  (no watchdog cron entry found in $SERVICE_USER crontab — nothing to remove)"
fi

# ── Stop existing nohup processes (if any) ───────────────────────────────────
echo ""
echo "► Stopping any existing nohup worker processes..."
pkill -u "$SERVICE_USER" -f "pipeline.py --scheduler" 2>/dev/null && echo "  Stopped scheduler" || echo "  (no scheduler running)"
pkill -u "$SERVICE_USER" -f "workers.scan_worker"     2>/dev/null && echo "  Stopped scan_worker" || true
pkill -u "$SERVICE_USER" -f "workers.detail_worker"   2>/dev/null && echo "  Stopped detail_worker" || true
pkill -u "$SERVICE_USER" -f "workers.fullscan"        2>/dev/null && echo "  Stopped fullscan" || true
pkill -u "$SERVICE_USER" -f "workers.watchdog"        2>/dev/null && echo "  Stopped watchdog" || true
sleep 2

# ── Fix .env permissions (must not be world-readable — contains secrets) ─────
echo ""
echo "► Securing .env file permissions..."
chown "$SERVICE_USER:$SERVICE_USER" "$PROJECT_DIR/.env"
chmod 600 "$PROJECT_DIR/.env"
echo "  .env permissions: 600 (owner read-only)"

# ── Copy unit templates to a root-owned staging directory ─────────────────────
# The wrapper reads from this directory at deploy time.  Keeping the templates
# in a root-owned location (not the project tree, which is SERVICE_USER-writable)
# prevents a privilege-escalation path where the service user writes a malicious
# unit file and then triggers the root wrapper to install it.
#
# Update these staging copies every time install-systemd.sh is re-run so that
# structural changes to unit files (e.g. new directives) are picked up without
# requiring a separate manual step.
echo ""
echo "► Staging unit templates to root-owned location..."
UNIT_STAGING_DIR="/usr/local/share/mail-pipeline/systemd"
mkdir -p "$UNIT_STAGING_DIR"
for unit in recruiter-scheduler.service recruiter-watchdog.service "recruiter-pipeline-alert@.service"; do
    src="$DEPLOY_DIR/systemd/$unit"
    if [[ ! -f "$src" ]]; then
        echo "[ERROR] Unit file not found: $src"
        echo "        Ensure deploy/systemd/$unit exists before running this script."
        exit 1
    fi
    cp "$src" "$UNIT_STAGING_DIR/$unit"
    chown root:root "$UNIT_STAGING_DIR/$unit"
    chmod 644 "$UNIT_STAGING_DIR/$unit"
    echo "  Staged: $UNIT_STAGING_DIR/$unit"
done
chown root:root "$UNIT_STAGING_DIR"
chmod 755 "$UNIT_STAGING_DIR"

# ── Create root-owned unit-install wrapper ────────────────────────────────────
# Reads from the root-owned staging directory (not the project tree).
# Granting sudo access to this wrapper instead of "sudo tee" eliminates the
# stdin-injection risk AND the writable-source-tree privilege-escalation risk.
echo ""
echo "► Creating unit-install wrapper..."
UNIT_INSTALL_BIN="/usr/local/bin/install-pipeline-units"
cat > "$UNIT_INSTALL_BIN" << WRAPPER_EOF
#!/bin/bash
# Root-owned unit installer — reads from root-owned staging dir.
# Do NOT grant NOPASSWD tee on /etc/systemd/system/ — use this wrapper instead.
set -euo pipefail
SERVICE_USER="${SERVICE_USER}"
PROJECT_DIR="${PROJECT_DIR}"
SRC_DIR="/usr/local/share/mail-pipeline/systemd"
DST_DIR="/etc/systemd/system"
ALLOWED_UNITS=(
    "recruiter-scheduler.service"
    "recruiter-watchdog.service"
    "recruiter-pipeline-alert@.service"
)
for unit in "\${ALLOWED_UNITS[@]}"; do
    src="\$SRC_DIR/\$unit"
    [[ -f "\$src" ]] || continue
    sed "s|User=opc|User=\$SERVICE_USER|g; s|Group=opc|Group=\$SERVICE_USER|g; s|/home/opc/mail|\$PROJECT_DIR|g" \
        "\$src" > "\$DST_DIR/\$unit"
    echo "  Installed: \$DST_DIR/\$unit"
done
WRAPPER_EOF
chmod 755 "$UNIT_INSTALL_BIN"
chown root:root "$UNIT_INSTALL_BIN"
echo "  Wrapper installed: $UNIT_INSTALL_BIN"

# ── Add sudoers rule so watchdog can restart the scheduler ───────────────────
# The watchdog needs to run: sudo systemctl restart recruiter-scheduler
# We grant ONLY the minimum commands needed — no blanket sudo.
#
# IMPORTANT: The sudoers rule must use the exact resolved path of systemctl
# (no symlinks) so it matches what the watchdog's subprocess call resolves to.
# workers/watchdog.py uses: _SYSTEMCTL = shutil.which("systemctl")
# This script detects the same path and writes it into sudoers so they match.
echo ""
echo "► Adding sudoers rule for watchdog self-healing..."
SYSTEMCTL_BIN="$(which systemctl 2>/dev/null || true)"
if [[ -z "$SYSTEMCTL_BIN" ]]; then
    echo "[ERROR] systemctl not found in PATH — cannot create sudoers rule"
    echo "        Ensure systemd is installed and systemctl is on PATH."
    exit 1
fi
echo "  systemctl resolved to: $SYSTEMCTL_BIN"

SUDOERS_FILE="/etc/sudoers.d/mail-pipeline"
# Write to a temp file first, validate with visudo, then atomically install.
# Writing directly to SUDOERS_FILE and validating afterward leaves a window
# where a bad rule is live and can break sudo system-wide.
_SUDOERS_TMP=$(mktemp /tmp/mail-pipeline-sudoers.XXXXXX)
cat > "$_SUDOERS_TMP" << EOF
# Allow opc user to restart/query pipeline services without password.
# Required by workers/watchdog.py self-healing and deploy workflow.
# Path = $(which systemctl) → resolved to $SYSTEMCTL_BIN
# Watchdog commands:
$SERVICE_USER ALL=(root) NOPASSWD: $SYSTEMCTL_BIN reset-failed recruiter-scheduler
$SERVICE_USER ALL=(root) NOPASSWD: $SYSTEMCTL_BIN reset-failed recruiter-watchdog
$SERVICE_USER ALL=(root) NOPASSWD: $SYSTEMCTL_BIN restart recruiter-scheduler
$SERVICE_USER ALL=(root) NOPASSWD: $SYSTEMCTL_BIN restart recruiter-watchdog
$SERVICE_USER ALL=(root) NOPASSWD: $SYSTEMCTL_BIN is-active recruiter-scheduler
$SERVICE_USER ALL=(root) NOPASSWD: $SYSTEMCTL_BIN is-active recruiter-watchdog
# Deploy-time unit sync — uses root-owned wrapper (not tee) to prevent stdin injection:
$SERVICE_USER ALL=(root) NOPASSWD: $UNIT_INSTALL_BIN
$SERVICE_USER ALL=(root) NOPASSWD: $SYSTEMCTL_BIN daemon-reload
EOF
chmod 440 "$_SUDOERS_TMP"

if visudo -c -f "$_SUDOERS_TMP" 2>/dev/null; then
    mv "$_SUDOERS_TMP" "$SUDOERS_FILE"
    chmod 440 "$SUDOERS_FILE"
    echo "  Sudoers rule validated and installed at $SUDOERS_FILE"
else
    echo "  [ERROR] sudoers syntax check failed — temp file discarded (sudo unchanged)"
    rm -f "$_SUDOERS_TMP"
    exit 1
fi

# ── Install systemd unit files ────────────────────────────────────────────────
# The wrapper (already created above) reads from the root-owned staging dir and
# applies the opc→SERVICE_USER/PROJECT_DIR substitutions itself — no need to
# duplicate that logic here.
echo ""
echo "► Installing systemd unit files..."
"$UNIT_INSTALL_BIN"

# ── Reload systemd + enable (+ optionally start) ─────────────────────────────
# Services do NOT start by default.  Redis AOF durability must be configured
# before starting the pipeline (see deploy/configure-redis.sh and
# first_time_setup.sh).  Pass --start as $1 to start services immediately
# (re-installs and restarts only — not appropriate for first-time setups).
_START="${1:-}"

echo ""
echo "► Enabling services (daemon-reload + enable)..."
systemctl daemon-reload

systemctl enable recruiter-scheduler
systemctl enable recruiter-watchdog

if [[ "$_START" == "--start" ]]; then
    echo "► Starting services..."
    systemctl start recruiter-scheduler
    echo "  Started recruiter-scheduler"
    sleep 5   # give scheduler time to spawn workers and write heartbeats

    systemctl start recruiter-watchdog
    echo "  Started recruiter-watchdog"
    sleep 3
else
    echo "  Services NOT started (default — Redis AOF must be configured first)."
    echo "  To start immediately after install: sudo bash deploy/install-systemd.sh --start"
    echo "  For first-time setup: use deploy/first_time_setup.sh instead."
fi

# ── Verify ───────────────────────────────────────────────────────────────────
# Skip status probe and health check unless --start was given: services were
# not started so they will report "inactive", which is not an error condition.
if [[ "$_START" != "--start" ]]; then
    echo ""
    echo "  Skipping service status probe (services not started)."
    echo "  The caller is responsible for starting services when ready."
else
    echo ""
    echo "► Service status:"
    for svc in recruiter-scheduler recruiter-watchdog; do
        status=$(systemctl is-active "$svc" 2>/dev/null || echo "unknown")
        pid=$(systemctl show -p MainPID --value "$svc" 2>/dev/null || echo "?")
        echo "  $svc: $status (pid=$pid)"
    done

    echo ""
    echo "► Running health check..."
    sleep 8   # wait for worker heartbeats to appear in Redis
    # Run as the service user (not root) so file paths resolve correctly.
    # Health check failure is a WARNING here, not an abort — workers may still be
    # starting up.  set -euo pipefail would otherwise terminate the whole install
    # script on a transient unhealthy state that resolves in seconds.
    if sudo -u "$SERVICE_USER" bash -c 'cd "$1" && source venv/bin/activate && python scripts/health_check.py' _ "$PROJECT_DIR"; then
        echo "  ✓ Health check passed"
    else
        echo ""
        echo "  [WARN] Health check reported issues — this is often normal right after startup."
        echo "         Workers may still be initialising.  Wait 30 s and re-run:"
        echo "           python scripts/health_check.py"
        echo "         If issues persist, check:"
        echo "           journalctl -u recruiter-scheduler -n 50"
    fi
fi

echo ""
echo "════════════════════════════════════════════════════════════"
echo "  Setup complete!"
echo ""
echo "  Useful commands:"
echo "    journalctl -u recruiter-scheduler -f      # live scheduler logs"
echo "    journalctl -u recruiter-watchdog -f        # live watchdog logs"
echo "    systemctl status recruiter-scheduler       # service status + last 10 lines"
echo "    bash deploy/deploy.sh                 # deploy new code + restart"
echo "════════════════════════════════════════════════════════════"
