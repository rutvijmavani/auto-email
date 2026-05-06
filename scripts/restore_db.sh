#!/bin/bash
# scripts/restore_db.sh — Restore recruiter_pipeline from a backup.
#
# Handles two backup formats:
#   *.dump — PostgreSQL custom-format dump (pg_dump -Fc); restored with pg_restore
#   *.db   — SQLite snapshot; restored via migrate_sqlite_to_postgres.py --force
#
# Usage:
#   cd /home/opc/mail
#   chmod +x scripts/restore_db.sh
#   ./scripts/restore_db.sh
#
# Or to skip the interactive prompt and restore a specific file:
#   BACKUP_FILE=/mnt/backups/recruiter_pipeline_2026-05-05_01-00.dump \
#     ./scripts/restore_db.sh

set -e

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ENV_FILE="$PROJECT_DIR/.env"
BACKUP_DIR="/mnt/backups"
VENV="$PROJECT_DIR/venv"

# ─────────────────────────────────────────
# 0. Sanity checks
# ─────────────────────────────────────────
if [ ! -d "$BACKUP_DIR" ] || ! mountpoint -q "$BACKUP_DIR"; then
    echo "[ERROR] $BACKUP_DIR is not mounted."
    echo "        Run: sudo mount /dev/sdb /mnt/backups"
    exit 1
fi

if [ ! -f "$ENV_FILE" ]; then
    echo "[ERROR] .env not found at $ENV_FILE"
    exit 1
fi

# ─────────────────────────────────────────
# 1. List available backups
# ─────────────────────────────────────────
echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo " Available backups in $BACKUP_DIR"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""

# Collect .dump and .db backups, newest first
mapfile -t BACKUPS < <(
    ls -t "$BACKUP_DIR"/recruiter_pipeline_*.dump \
       "$BACKUP_DIR"/recruiter_pipeline_*.db 2>/dev/null | head -20
)

if [ ${#BACKUPS[@]} -eq 0 ]; then
    echo "[ERROR] No backups found in $BACKUP_DIR"
    echo "        Pattern: recruiter_pipeline_*.dump  or  recruiter_pipeline_*.db"
    exit 1
fi

# Print numbered list
for i in "${!BACKUPS[@]}"; do
    f="${BACKUPS[$i]}"
    fname=$(basename "$f")
    ext="${fname##*.}"
    size=$(du -sh "$f" 2>/dev/null | cut -f1)
    mtime=$(date -r "$f" "+%Y-%m-%d %H:%M" 2>/dev/null || stat -c "%y" "$f" | cut -d' ' -f1-2)
    if [ "$ext" = "dump" ]; then
        label="[PostgreSQL]"
    else
        label="[SQLite]    "
    fi
    printf "  %2d)  %s  %-45s  %s  (%s)\n" $((i+1)) "$label" "$fname" "$mtime" "$size"
done

echo ""

# ─────────────────────────────────────────
# 2. Select backup
# ─────────────────────────────────────────
if [ -n "${BACKUP_FILE:-}" ]; then
    # Non-interactive: caller provided the file
    SELECTED="$BACKUP_FILE"
    if [ ! -f "$SELECTED" ]; then
        echo "[ERROR] BACKUP_FILE not found: $SELECTED"
        exit 1
    fi
    echo "[INFO] Using: $SELECTED"
else
    read -rp "Enter number of backup to restore (or q to quit): " choice
    if [[ "$choice" == "q" || "$choice" == "Q" ]]; then
        echo "Aborted."
        exit 0
    fi
    if ! [[ "$choice" =~ ^[0-9]+$ ]] || [ "$choice" -lt 1 ] || [ "$choice" -gt ${#BACKUPS[@]} ]; then
        echo "[ERROR] Invalid selection."
        exit 1
    fi
    SELECTED="${BACKUPS[$((choice-1))]}"
fi

FNAME=$(basename "$SELECTED")
EXT="${FNAME##*.}"

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo " Restoring: $FNAME"
echo " Format   : $EXT"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""
echo "  ⚠  WARNING: This will OVERWRITE the current database."
echo "     All existing data will be replaced."
echo ""
read -rp "  Type 'yes' to confirm: " confirm
if [ "$confirm" != "yes" ]; then
    echo "Aborted."
    exit 0
fi
echo ""

# ─────────────────────────────────────────
# 3. Parse DATABASE_URL from .env
# ─────────────────────────────────────────
source "$VENV/bin/activate"
cd "$PROJECT_DIR"

# Let Python parse the URL so we handle encoded passwords correctly
read -r PG_HOST PG_PORT PG_USER PG_PASS PG_DB < <(python3 - <<'PY'
import os, sys
from urllib.parse import urlparse
from dotenv import load_dotenv
load_dotenv()
url = os.environ.get("DATABASE_URL", "")
p   = urlparse(url)
print(
    p.hostname or "localhost",
    str(p.port or 5432),
    p.username or "pipeline_user",
    p.password or "",
    (p.path or "/recruiter_pipeline").lstrip("/"),
)
PY
)

export PGPASSWORD="$PG_PASS"

echo "[INFO] Target: postgresql://$PG_USER@$PG_HOST:$PG_PORT/$PG_DB"
echo ""

# ─────────────────────────────────────────
# 4. Execute restore
# ─────────────────────────────────────────
if [ "$EXT" = "dump" ]; then
    # ── PostgreSQL custom-format restore ──────────────────────────────────────
    echo "[RESTORE] Running pg_restore..."

    pg_restore \
        -h "$PG_HOST" \
        -p "$PG_PORT" \
        -U "$PG_USER" \
        -d "$PG_DB" \
        --clean \
        --if-exists \
        --no-owner \
        --no-privileges \
        -Fc \
        "$SELECTED"

    echo "[OK]  pg_restore complete"

elif [ "$EXT" = "db" ]; then
    # ── SQLite backup → PostgreSQL migration ──────────────────────────────────
    echo "[RESTORE] Migrating SQLite backup into PostgreSQL (--force)..."
    echo "          This re-runs the full schema init and overwrites all tables."
    echo ""

    python scripts/migrate_sqlite_to_postgres.py \
        --sqlite "$SELECTED" \
        --force

else
    echo "[ERROR] Unknown backup format: .$EXT"
    exit 1
fi

unset PGPASSWORD

# ─────────────────────────────────────────
# 5. Verify
# ─────────────────────────────────────────
echo ""
echo "[INFO] Verifying restore..."
echo ""

python3 - <<'PY'
import sys
sys.path.insert(0, ".")
from dotenv import load_dotenv
load_dotenv()
from db.connection import get_conn

tables = [
    "applications", "recruiters", "prospective_companies",
    "job_postings", "outreach", "application_recruiters",
]
conn = get_conn()
print("  Table                        Rows")
print("  " + "─" * 40)
total = 0
for t in tables:
    try:
        row = conn.execute(f"SELECT COUNT(*) AS cnt FROM {t}").fetchone()
        n   = row["cnt"]
        print(f"  {t:<30} {n:>6,}")
        total += n
    except Exception as e:
        print(f"  {t:<30}  ERROR: {e}")
conn.close()
print("  " + "─" * 40)
print(f"  {'TOTAL':<30} {total:>6,}")
PY

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo " Restore complete ✓"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
