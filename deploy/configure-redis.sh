#!/bin/bash
# deploy/configure-redis.sh — Enable Redis AOF persistence (one-time setup).
#
# Reduces the data-loss window from ~5 minutes (RDB snapshot) to ~1 second
# (AOF fsync every second).  This protects detail queue entries that haven't
# yet been flushed to a PostgreSQL `pending_detail` row.
#
# Safe to re-run — it checks before changing anything.
#
# Run ONCE on the server after initial setup:
#   sudo bash deploy/configure-redis.sh
#
# Requirements: Redis running, redis-cli available, sudo access.

set -euo pipefail

REDIS_CLI="${REDIS_CLI:-redis-cli}"
REDIS_CONF_CANDIDATES=(
    /etc/redis/redis.conf
    /etc/redis.conf
    /usr/local/etc/redis.conf
)

echo "════════════════════════════════════════════════════════════"
echo "  Mail Pipeline — Redis AOF persistence setup"
echo "════════════════════════════════════════════════════════════"

# ── Must be root ──────────────────────────────────────────────────────────────
if [[ $EUID -ne 0 ]]; then
    echo "[ERROR] Run with sudo: sudo bash deploy/configure-redis.sh"
    exit 1
fi

# ── Check Redis is reachable ──────────────────────────────────────────────────
if ! $REDIS_CLI ping > /dev/null 2>&1; then
    echo "[ERROR] Cannot reach Redis (redis-cli ping failed)."
    echo "        Is Redis running?  sudo systemctl status redis"
    exit 1
fi

REDIS_VERSION=$($REDIS_CLI INFO server 2>/dev/null | grep redis_version | cut -d: -f2 | tr -d '[:space:]')
echo ""
echo "► Redis version: $REDIS_VERSION"

# ── Apply CONFIG SET immediately (takes effect without restart) ───────────────
echo ""
echo "► Applying AOF config via CONFIG SET (live — no restart needed)..."

current_aof=$($REDIS_CLI CONFIG GET appendonly            | tail -1)
current_fsync=$($REDIS_CLI CONFIG GET appendfsync          | tail -1)
current_rwpct=$($REDIS_CLI CONFIG GET auto-aof-rewrite-percentage | tail -1)
current_rwmin=$($REDIS_CLI CONFIG GET auto-aof-rewrite-min-size   | tail -1)

echo "  Current appendonly                 : $current_aof"
echo "  Current appendfsync                : $current_fsync"
echo "  Current auto-aof-rewrite-percentage: $current_rwpct"
echo "  Current auto-aof-rewrite-min-size  : $current_rwmin"

# Check all four settings — a partial previous run may have set only
# appendonly+appendfsync but skipped the rewrite thresholds.
AOF_CHANGED=0
AOF_NEWLY_ENABLED=0   # set only when appendonly transitions no → yes

if [[ "$current_aof"   == "yes"     &&
      "$current_fsync" == "everysec" &&
      "$current_rwpct" == "100"      &&
      # Redis < 7 returns bytes ("67108864"); Redis 7+ returns "64mb".
      # Accept either form so the idempotence check works on all versions.
      ( "$current_rwmin" == "64mb" || "$current_rwmin" == "67108864" ) ]]; then
    echo "  ✓ AOF already fully configured — nothing to change."
else
    AOF_CHANGED=1
    [[ "$current_aof" != "yes" ]] && AOF_NEWLY_ENABLED=1

    $REDIS_CLI CONFIG SET appendonly yes
    echo "  Set appendonly                     = yes"

    $REDIS_CLI CONFIG SET appendfsync everysec
    echo "  Set appendfsync                    = everysec"

    # AOF rewrite (compaction): Redis rewrites the AOF to just the minimal
    # commands needed to recreate the current in-memory state, discarding
    # all historical intermediate writes.  Without this the file grows
    # indefinitely — e.g. 1,440 heartbeat writes/hour collapse to 4 lines.
    # Trigger: when file doubles vs its size after the last rewrite (100%)
    # and is at least 64 MB.  These are the Redis defaults but we set them
    # explicitly so the config is self-documenting and version-independent.
    $REDIS_CLI CONFIG SET auto-aof-rewrite-percentage 100
    echo "  Set auto-aof-rewrite-percentage    = 100"

    $REDIS_CLI CONFIG SET auto-aof-rewrite-min-size 64mb
    echo "  Set auto-aof-rewrite-min-size      = 64mb"
fi

# ── Persist to redis.conf so it survives a Redis restart ─────────────────────
echo ""
echo "► Persisting to redis.conf..."

REDIS_CONF=""
_conf_patched=0
for candidate in "${REDIS_CONF_CANDIDATES[@]}"; do
    if [[ -f "$candidate" ]]; then
        REDIS_CONF="$candidate"
        break
    fi
done

if [[ -z "$REDIS_CONF" ]]; then
    echo ""
    echo "  [ERROR] Could not find redis.conf in standard locations:"
    for c in "${REDIS_CONF_CANDIDATES[@]}"; do
        echo "         $c"
    done
    echo ""
    echo "  The CONFIG SET above is LIVE but will be lost on Redis restart."
    echo "  Redis AOF durability is NOT persistent until redis.conf is updated."
    echo "  Find your redis.conf and add these lines manually, then restart Redis:"
    echo ""
    echo "      appendonly      yes"
    echo "      appendfsync     everysec"
    echo "      auto-aof-rewrite-percentage 100"
    echo "      auto-aof-rewrite-min-size   64mb"
    echo ""
    echo "  Then: sudo systemctl restart redis"
    echo ""
    echo "  To skip this check (not recommended): re-run with SKIP_CONF_CHECK=1"
    if [[ "${SKIP_CONF_CHECK:-0}" != "1" ]]; then
        exit 1
    fi
else
    echo "  Found: $REDIS_CONF"
    # Back up before editing
    cp "$REDIS_CONF" "${REDIS_CONF}.bak.$(date +%Y%m%d_%H%M%S)"
    echo "  Backup: ${REDIS_CONF}.bak.$(date +%Y%m%d_%H%M%S)"

    # Patch each directive — update in-place if present, append if missing
    _set_redis_conf() {
        local key="$1"
        local val="$2"
        local conf="$3"

        if grep -qE "^[[:space:]]*${key}[[:space:]]" "$conf"; then
            # Replace the active directive only — do NOT match commented examples
            # (the old pattern used [[:space:]#]* which turned "# appendonly no"
            # into a second active directive whenever the real one was present).
            sed -i "s|^[[:space:]]*${key}[[:space:]].*|${key} ${val}|" "$conf"
            echo "  Updated: $key $val"
        else
            # Append at end of file
            echo "" >> "$conf"
            echo "${key} ${val}" >> "$conf"
            echo "  Added:   $key $val"
        fi
    }

    _set_redis_conf appendonly      yes          "$REDIS_CONF"
    _set_redis_conf appendfsync     everysec     "$REDIS_CONF"
    _set_redis_conf auto-aof-rewrite-percentage 100    "$REDIS_CONF"
    _set_redis_conf auto-aof-rewrite-min-size   64mb   "$REDIS_CONF"

    echo ""
    echo "  Config saved. Reloading Redis config..."
    if ! $REDIS_CLI CONFIG REWRITE > /dev/null 2>&1; then
        echo "  [WARN] CONFIG REWRITE failed — redis.conf may be read-only or Redis lacks write permission."
        echo "         The live CONFIG SET is active, but redis.conf was not updated by CONFIG REWRITE."
        echo "         The file edits above already wrote the directives directly; this warning is non-fatal."
    fi
    _conf_patched=1
fi

# ── Trigger AOF rewrite only when appendonly was newly enabled ────────────────
# BGREWRITEAOF creates the initial .aof file after appendonly is turned on.
# Skipped when only threshold settings changed (appendfsync / rewrite limits)
# because the existing .aof file is already current in that case, and an
# unnecessary rewrite wastes CPU on a large dataset.
if [[ "$AOF_NEWLY_ENABLED" -eq 1 ]]; then
    echo ""
    echo "► Triggering initial AOF rewrite (BGREWRITEAOF)..."
    # CONFIG SET appendonly yes can start an automatic rewrite; check before
    # issuing a second BGREWRITEAOF to avoid the BUSY error.
    _aof_rw_active=$($REDIS_CLI INFO persistence 2>/dev/null \
        | grep -E "^aof_rewrite_in_progress:|^aof_rewrite_scheduled:" \
        | awk -F: '{s+=$2} END {print s+0}')
    if [[ "${_aof_rw_active:-0}" -gt 0 ]]; then
        echo "  AOF rewrite already in progress — skipping BGREWRITEAOF."
    else
        $REDIS_CLI BGREWRITEAOF
        echo "  AOF rewrite started in background."
    fi
    sleep 2
else
    echo ""
    if [[ "$AOF_CHANGED" -eq 1 ]]; then
        echo "► Skipping BGREWRITEAOF — appendonly was already enabled; only threshold settings changed."
    else
        echo "► Skipping BGREWRITEAOF — AOF was already fully configured."
    fi
fi

# ── Verify ────────────────────────────────────────────────────────────────────
echo ""
echo "► Verification:"
aof_enabled=$($REDIS_CLI CONFIG GET appendonly                    | tail -1)
aof_fsync=$($REDIS_CLI CONFIG GET appendfsync                     | tail -1)
aof_rwpct=$($REDIS_CLI CONFIG GET auto-aof-rewrite-percentage     | tail -1)
aof_rwmin=$($REDIS_CLI CONFIG GET auto-aof-rewrite-min-size       | tail -1)
aof_file=$($REDIS_CLI INFO persistence | grep aof_filename | cut -d: -f2 | tr -d '[:space:]' || echo "(unknown)")

echo "  appendonly                    : $aof_enabled  (want: yes)"
echo "  appendfsync                   : $aof_fsync   (want: everysec)"
echo "  auto-aof-rewrite-percentage   : $aof_rwpct  (want: 100)"
echo "  auto-aof-rewrite-min-size     : $aof_rwmin  (want: 64mb)"
echo "  AOF file                      : $aof_file"

if [[ "$aof_enabled" == "yes"     &&
      "$aof_fsync"   == "everysec" &&
      "$aof_rwpct"   == "100"      &&
      ( "$aof_rwmin" == "64mb" || "$aof_rwmin" == "67108864" ) ]]; then
    echo ""
    if [[ "$_conf_patched" -eq 0 ]]; then
        echo "  ✓ AOF settings are LIVE (active in Redis process)"
        echo "  [WARN] redis.conf was NOT updated (SKIP_CONF_CHECK=1) — settings will revert on Redis restart"
    else
        echo "  ✓ AOF persistence is ACTIVE"
        echo "  ✓ redis.conf updated — settings survive Redis restart"
    fi
    echo "  ✓ Data-loss window: ~1 second (AOF fsync everysec)"
    echo "  ✓ AOF compaction:   auto-rewrite when file doubles (≥64 MB)"
else
    echo ""
    echo "  [ERROR] AOF settings did not apply correctly — check Redis logs."
    exit 1
fi

echo ""
echo "════════════════════════════════════════════════════════════"
echo "  Redis AOF setup complete!"
echo ""
echo "  What changed:"
echo "    Before: RDB snapshots every ~5 min → up to 5 min of data loss"
echo "            on crash (detail queue entries may be lost)"
echo "    After:  AOF fsync every 1 second → max 1 second of data loss"
echo ""
echo "  AOF file size is controlled by automatic rewriting (compaction):"
echo "    Redis rewrites the AOF when it doubles vs its last-rewrite size"
echo "    AND is ≥64 MB.  Rewriting collapses history into just the minimal"
echo "    commands to recreate current state — e.g. 1,440 heartbeat writes"
echo "    per hour collapse to 4 lines.  File never exceeds ~2× live data."
echo "    Trigger manually any time: redis-cli BGREWRITEAOF"
echo ""
echo "  The detail queue also has a PostgreSQL fallback:"
echo "    pending_detail rows survive any Redis crash and are rebuilt"
echo "    automatically by the scheduler (pipeline.py --rebuild)."
echo ""
echo "  Useful commands:"
echo "    redis-cli INFO persistence     # AOF status + last save time"
echo "    redis-cli DEBUG SLEEP 0        # test Redis is responsive"
echo "════════════════════════════════════════════════════════════"
