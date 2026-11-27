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

# ── Sanity: must NOT be root (checked before any file is created, so root leaves no root-owned artifacts) ──
if [[ $EUID -eq 0 ]]; then
    echo "[ERROR] Do NOT run deploy.sh as root / sudo."
    echo "        Run as the service user (opc): bash deploy/deploy.sh"
    exit 1
fi

# One deploy at a time: concurrent runs would race on the pull, setup_cron.sh, the wrapper
# regeneration and the data/.cron_regen_pending flag. The lock is held until this shell exits.
mkdir -p "$PROJECT_DIR/data"
exec 9>"$PROJECT_DIR/data/.deploy.lock"
if ! flock -n 9; then
    echo "[ERROR] Another deploy is already running (lock: $PROJECT_DIR/data/.deploy.lock) — aborting."
    exit 1
fi

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

# ── The user's own crontab lines ──────────────────────────────────────────────
# Rollback runs PREVIOUS_SHA's setup_cron.sh, and an older one strips every run_*.sh entry, the
# user's own included. These two helpers carry the lines the pipeline does not own across that run.
# Their filters are those of setup_cron.sh's merge: keep them in step.
_user_cron_lines() {   # $1 = crontab text
    local lines="$1"
    if grep -qF '# === BEGIN RECRUITER PIPELINE ===' <<<"$lines"; then
        # unmatched BEGIN: setup_cron.sh aborts without writing, so there is nothing to carry over
        grep -qF '# === END RECRUITER PIPELINE ===' <<<"$lines" || return 0
        lines=$(sed '/# === BEGIN RECRUITER PIPELINE ===/,/# === END RECRUITER PIPELINE ===/d' <<<"$lines")
    fi
    { grep -Ev '/home/opc/mail/run_(sync|nightly|monday|monthly|outreach|monitor|detect|weekly_summary|enrich|renew_watch|verify_filled|sync_dol_lca)\.sh([[:space:]]|$|[;&|<>)"'"'"'])' <<<"$lines" \
        | grep -v 'scripts/log_monitor\.py' \
        | grep -v 'hashlib.*range(100000)' \
        | sed '/^[[:space:]]*$/d'; } || true   # grep exits 1 when no line is left
}

_restore_user_cron() {   # $1 = the user's lines, saved before the old setup_cron.sh ran
    local saved="$1" current outside line merged missing=false
    [[ -n "$saved" ]] || return 0
    if current=$(crontab -l 2>/dev/null); then
        # a line counts as present only outside the pipeline block
        outside=$(sed '/# === BEGIN RECRUITER PIPELINE ===/,/# === END RECRUITER PIPELINE ===/d' <<<"$current")
        while IFS= read -r line; do
            grep -qxF -- "$line" <<<"$outside" || missing=true
        done <<<"$saved"
        $missing || return 0
        # the saved lines first, in their own order, then every other line installed now (the pipeline
        # block included): nothing that is in the crontab is dropped
        merged=$(SAVED="$saved" awk '
            BEGIN { n = split(ENVIRON["SAVED"], s, "\n"); for (i = 1; i <= n; i++) { print s[i]; have[s[i]]++ } }
            /# === BEGIN RECRUITER PIPELINE ===/ { block = 1 }
            !block && have[$0] > 0 { have[$0]--; next }
            { print }
            /# === END RECRUITER PIPELINE ===/ { block = 0 }
        ' <<<"$current") && printf '%s\n' "$merged" | crontab - && return 0
    fi
    echo "  [WARN] could not restore your own crontab lines — check 'crontab -l' and add back any of these:"
    printf '%s\n' "$saved"
    return 1
}

_restore_saved_user_cron() {   # restores the lines backed up on disk; the backup goes only once they are back
    [[ -f "$_CRON_USER_BACKUP" ]] || return 0
    local saved
    saved=$(cat "$_CRON_USER_BACKUP") || return 1
    _restore_user_cron "$saved" || return 1
    rm -f "$_CRON_USER_BACKUP"
}

# ── Shared rollback helper ────────────────────────────────────────────────────
_rollback_to_previous() {
    echo "  [ROLLBACK] Reverting to $PREVIOUS_SHA..."
    # wrappers regenerated in this deploy are pending again until they are restored below, so a
    # rollback that fails or is interrupted leaves the flag for the next deploy
    if ${_CRON_REGENERATED:-false}; then
        touch "$_CRON_PENDING_FLAG" || echo "  [WARN] could not set $_CRON_PENDING_FLAG"
    fi
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
        # wrappers/crontab regenerated from the new setup_cron.sh must go back with the code
        if ${_CRON_REGENERATED:-false}; then
            local _user_cron _cron_now _setup_ok=true
            # without a copy of the user's lines on disk the old setup_cron.sh must not run; the flag stays.
            # A backup left by an earlier rollback is restored first, never overwritten.
            if ! _restore_saved_user_cron; then
                echo "  [WARN] an earlier crontab backup is not restored ($_CRON_USER_BACKUP) — cron wrappers not restored (the next deploy retries)"
            elif ! _cron_now=$(crontab -l); then
                echo "  [WARN] crontab -l failed — cron wrappers not restored; re-run setup_cron.sh manually (the next deploy retries)"
            else
                _user_cron=$(_user_cron_lines "$_cron_now")
                if [[ -n "$_user_cron" ]] \
                    && ! { printf '%s\n' "$_user_cron" > "$_CRON_USER_BACKUP.tmp" \
                           && mv "$_CRON_USER_BACKUP.tmp" "$_CRON_USER_BACKUP"; }; then
                    echo "  [WARN] could not save your crontab lines to $_CRON_USER_BACKUP — cron wrappers not restored (the next deploy retries)"
                else
                    if ! bash "$PROJECT_DIR/setup_cron.sh"; then
                        _setup_ok=false
                        echo "  [WARN] cron wrapper restore failed — re-run setup_cron.sh manually (the next deploy retries)"
                    fi
                    # the user's lines go back even when setup_cron.sh failed part-way
                    if _restore_saved_user_cron && $_setup_ok; then
                        rm -f "$_CRON_PENDING_FLAG"
                    fi
                fi
            fi
        fi
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
    sudo systemctl restart recruiter-detail-drainer \
        || echo "  [WARN] could not restart recruiter-detail-drainer"
    sudo systemctl restart email-processor \
        || echo "  [WARN] could not restart email-processor"
    sudo systemctl restart streamlit-frontend \
        || echo "  [WARN] could not restart streamlit-frontend"
    sudo systemctl reset-failed h1b-llm-worker 2>/dev/null || true
    sudo systemctl restart h1b-llm-worker \
        || echo "  [WARN] could not restart h1b-llm-worker"

    # Post-rollback verification
    sleep 3
    local _all_active=true
    for svc in recruiter-scheduler recruiter-watchdog pipeline-api recruiter-manager recruiter-detail-drainer email-processor streamlit-frontend; do
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

_CRON_PENDING_FLAG="$PROJECT_DIR/data/.cron_regen_pending"
_CRON_USER_BACKUP="$PROJECT_DIR/data/.cron_user_lines"   # present = a rollback did not put these back
_CRON_REGENERATED=false
if ! _restore_saved_user_cron; then
    echo "  [WARN] crontab backup kept in $_CRON_USER_BACKUP — the next deploy retries"
fi
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
    # a pending cron regeneration (flag file) must still run below, even with nothing to pull
    if $NO_RESTART && [[ ! -f "$_CRON_PENDING_FLAG" ]]; then
        echo ""
        echo "════════════════════════════════════════════════════════════"
        echo "  Nothing to do."
        echo "════════════════════════════════════════════════════════════"
        exit 0
    fi
else
    # The run_*.sh wrappers are generated from setup_cron.sh and are not in git, so a changed
    # setup_cron.sh does not reach them on its own (the VM ran a stale run_monthly.sh for a week).
    # Persisted in a flag file so a failed regeneration (or an aborted deploy) is retried by the next deploy,
    # even when that one pulls nothing; cleared only after setup_cron.sh succeeds.
    # Set BEFORE the pull and cleared only once the pulled commits are known to leave setup_cron.sh alone:
    # the pull can bring commits pushed since the fetch above, and a deploy interrupted during or right
    # after it would leave nothing to detect next time.
    _cron_was_pending=false
    if [[ -f "$_CRON_PENDING_FLAG" ]]; then _cron_was_pending=true; fi
    mkdir -p "$PROJECT_DIR/data"
    touch "$_CRON_PENDING_FLAG"
    git pull origin "${_BRANCH}"
    if ! $_cron_was_pending && git diff --quiet "$PREVIOUS_SHA" HEAD -- setup_cron.sh; then
        rm -f "$_CRON_PENDING_FLAG"
    fi
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

# ── Cron wrapper self-heal ────────────────────────────────────────────────────
# The run_*.sh wrappers are generated by setup_cron.sh and are not in git. They
# vanished from the VM once (found 2026-10-10) and every cron job silently
# failed for ~2 months. If the crontab names a wrapper that is missing or not
# executable, regenerate them. Only fires in that case, never touches non-pipeline
# crontab entries (setup_cron.sh preserves them), and is non-fatal for the deploy.
echo ""
echo "► Checking cron wrappers..."
_missing_wrappers=""
_cron_wrappers=""
_crontab_rc=0
_crontab_out=$(crontab -l 2>&1) || _crontab_rc=$?
if [[ $_crontab_rc -ne 0 ]]; then
    # "crontab -l" fails both when no crontab exists and when it cannot be read; either way
    # the wrapper check below would see an empty list, so say so instead of reporting all-clear.
    echo "  [WARN] crontab -l failed (exit $_crontab_rc: ${_crontab_out:-no output}) — cannot verify cron wrappers"
else
    _cron_wrappers=$(printf '%s\n' "$_crontab_out" | grep -vE '^[[:space:]]*#' | grep -oE "/[^ '\"]*/run_[a-z_]+\.sh" | sort -u || true)
fi
for _w in $_cron_wrappers; do
    # Only wrappers setup_cron.sh generates (same dir, same name) — a user's own run_custom.sh
    # must not trigger a regeneration.
    [[ "$(dirname "$_w")" == "$PROJECT_DIR" ]] || continue
    grep -qF "\"\$PROJECT_DIR/$(basename "$_w")\"" "$PROJECT_DIR/setup_cron.sh" || continue
    if [[ ! -x "$_w" ]]; then
        _missing_wrappers="$_missing_wrappers $_w"
    fi
done
if [[ $_crontab_rc -ne 0 ]]; then
    :   # already warned above; deploy continues (self-heal is non-fatal)
elif [[ -n "$_missing_wrappers" ]] || [[ -f "$_CRON_PENDING_FLAG" ]]; then
    if [[ -n "$_missing_wrappers" ]]; then
        echo "  [WARN] Cron wrapper(s) missing or not executable:$_missing_wrappers"
        echo "  [WARN] Something deleted them — regenerating via setup_cron.sh"
    else
        echo "  setup_cron.sh changed in this deploy — regenerating wrappers"
    fi
    # set before the call: a setup_cron.sh that fails part-way may already have replaced some wrappers
    _CRON_REGENERATED=true   # a later rollback re-runs the old setup_cron.sh
    if bash "$PROJECT_DIR/setup_cron.sh"; then
        rm -f "$_CRON_PENDING_FLAG"
        echo "  Cron wrappers regenerated."
    else
        echo "  [WARN] setup_cron.sh failed — cron jobs may not run; fix manually"
    fi
else
    echo "  All cron wrappers present."
fi

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
for _wrapper in /usr/local/bin/install-pipeline-units /usr/local/bin/sync-pipeline-staging; do
    if [[ ! -x "$_wrapper" ]]; then
        echo "[ERROR] $_wrapper not found."
        echo "        Run 'sudo bash deploy/install-systemd.sh' once to provision it."
        _rollback_to_previous; exit 1
    fi
done
# The sudoers rules are generated per staged unit by install-systemd.sh, so a host
# provisioned before recruiter-detail-drainer existed has no rule for it yet and the
# restart below would fail (or prompt).  Detect that now and say what to run.
_SYSTEMCTL_BIN="$(command -v systemctl)"
if ! sudo -n -l "$_SYSTEMCTL_BIN" restart recruiter-detail-drainer >/dev/null 2>&1; then
    echo "[ERROR] no sudoers rule to restart recruiter-detail-drainer."
    echo "        Run 'sudo bash deploy/install-systemd.sh' once to add it, then redeploy."
    _rollback_to_previous; exit 1
fi
sudo /usr/local/bin/sync-pipeline-staging || {
    echo "  [ERROR] Staging sync failed — rolling back"
    _rollback_to_previous; exit 1
}
sudo /usr/local/bin/install-pipeline-units || {
    echo "  [ERROR] Unit install failed — rolling back"
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

sudo systemctl restart recruiter-detail-drainer || {
    echo "  [ERROR] recruiter-detail-drainer failed to restart — rolling back"
    _rollback_to_previous; exit 1
}

sudo systemctl restart email-processor || {
    echo "  [ERROR] email-processor failed to restart — rolling back"
    _rollback_to_previous; exit 1
}

sudo systemctl restart streamlit-frontend || {
    echo "  [ERROR] streamlit-frontend failed to restart — rolling back"
    _rollback_to_previous; exit 1
}

# h1b-llm-worker: reset-failed first — StartLimitBurst may have been hit if
# the worker crash-looped before deploy.  Non-critical batch worker; a restart
# failure here does not trigger a rollback.
sudo systemctl reset-failed h1b-llm-worker 2>/dev/null || true
if sudo systemctl restart h1b-llm-worker; then
    echo "  Restarted: h1b-llm-worker"
else
    echo "  [WARN] h1b-llm-worker failed to restart — non-critical, check manually"
fi

echo ""
echo "  Waiting for services to come up..."
sleep 5

# ── Quick status check ────────────────────────────────────────────────────────
echo ""
echo "► Service status:"
_svc_fail=0
for svc in recruiter-scheduler recruiter-watchdog pipeline-api recruiter-manager recruiter-detail-drainer email-processor streamlit-frontend; do
    status=$(systemctl is-active "$svc" 2>/dev/null || echo "unknown")
    pid=$(systemctl show -p MainPID --value "$svc" 2>/dev/null || echo "?")
    if [[ "$status" == "active" ]]; then
        echo "  ✓ $svc: $status (pid=$pid)"
    else
        echo "  ✗ $svc: $status (pid=$pid)  ← PROBLEM"
        _svc_fail=1
    fi
done
# h1b-llm-worker: batch worker — warn only, does not block deployment
_h1b_status=$(systemctl is-active "h1b-llm-worker" 2>/dev/null)
_h1b_status=${_h1b_status:-unknown}
_h1b_pid=$(systemctl show -p MainPID --value "h1b-llm-worker" 2>/dev/null || echo "?")
if [[ "$_h1b_status" == "active" ]]; then
    echo "  ✓ h1b-llm-worker: $_h1b_status (pid=$_h1b_pid)"
else
    echo "  ! h1b-llm-worker: $_h1b_status  ← WARN (batch worker — non-critical)"
fi
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
