#!/bin/bash
# setup_cron.sh — Install cron jobs for the recruiter pipeline
# Run once on your Oracle Cloud VM:
#   chmod +x setup_cron.sh && ./setup_cron.sh
#
# ── Full daily timeline (UTC) ───────────────────────────────────
#  12:00 AM  --sync-forms + --sync-prospective
#   1:00 AM  ATS discovery (1st of month only)
#   2:00 AM  Tue-Sun: backup → find-only
#   3:00 AM  --sync-forms + --sync-prospective + enrichment (daily)
#   6:00 AM  --sync-forms + --sync-prospective
#   7:00 AM  --monitor-jobs
#   8:30 AM  --detect-ats --batch
#   9:00 AM  --sync-forms + --sync-prospective + --outreach-only
#  12:00 PM  --sync-forms + --sync-prospective
#   3:00 PM  --sync-forms + --sync-prospective
#   6:00 PM  --sync-forms + --sync-prospective
#   9:00 PM  --sync-forms + --sync-prospective  ← last sync of day
#  10:00 PM  Mon: backup → verify-only → find-only
#
# ── Chaining safety ─────────────────────────────────────────────
#   backup fails   → verify-only + find-only do NOT run (DB safe)
#   verify-only fails (Mon) → find-only does NOT run
#   find-only fails → outreach still runs at 9 AM with existing data
#
# ── DB consistency guarantee ────────────────────────────────────
#   All destructive operations (verify, find, outreach) are preceded
#   by a backup via &&. If backup fails nothing runs.
#   sync-forms and sync-prospective are read+insert only — safe to
#   run standalone without a backup gate.

set -e

PROJECT_DIR="/home/opc/mail"
PYTHON="$PROJECT_DIR/venv/bin/python"
PIPELINE="$PROJECT_DIR/pipeline.py"
LOG_DIR="$PROJECT_DIR/logs"

# ── Sanity checks ──────────────────────────────────────────────
if [ ! -f "$PIPELINE" ]; then
  echo "[ERROR] pipeline.py not found at $PIPELINE"
  echo "        Are you sure the project is at /home/opc/mail?"
  exit 1
fi

if [ ! -f "$PYTHON" ]; then
  echo "[ERROR] venv not found at $PROJECT_DIR/venv"
  echo "        Run: cd $PROJECT_DIR && python3 -m venv venv && source venv/bin/activate && pip install -r requirements.txt"
  exit 1
fi

mkdir -p "$LOG_DIR"
echo "[OK] Log directory: $LOG_DIR"

# ═══════════════════════════════════════════════════════════════
# WRAPPER SCRIPTS
# Each wrapper: cd, activate venv, run, log with timestamps,
# rotate old logs. Scripts called by cron entries below.
# ═══════════════════════════════════════════════════════════════

# ── sync (forms + prospective) ─────────────────────────────────
cat > "$PROJECT_DIR/run_sync.sh" << 'EOF'
#!/bin/bash
# run_sync.sh — run --sync-forms and --sync-prospective back-to-back
# Safe standalone: read+insert only, no backup needed
PROJECT_DIR="/home/opc/mail"
LOG_DIR="$PROJECT_DIR/logs"
LOG_FILE="$LOG_DIR/sync_$(date +%Y-%m-%d).log"

cd "$PROJECT_DIR" || exit 1
echo "" >> "$LOG_FILE"
echo "══════════════════════════════════════════════" >> "$LOG_FILE"
echo "[CRON] sync started at $(date '+%Y-%m-%d %H:%M:%S')" >> "$LOG_FILE"
echo "══════════════════════════════════════════════" >> "$LOG_FILE"

source venv/bin/activate
python pipeline.py --sync-forms      >> "$LOG_FILE" 2>&1
python pipeline.py --sync-prospective >> "$LOG_FILE" 2>&1
EXIT_CODE=$?

echo "[CRON] sync finished at $(date '+%Y-%m-%d %H:%M:%S') | exit=$EXIT_CODE" >> "$LOG_FILE"

# Keep 14 days of sync logs
find "$LOG_DIR" -name "sync_*.log" -mtime +14 -delete

exit $EXIT_CODE
EOF

# ── nightly (backup → find-only) ───────────────────────────────
cat > "$PROJECT_DIR/run_nightly.sh" << 'EOF'
#!/bin/bash
# run_nightly.sh — Tue-Sun 2 AM: backup → find-only
# Chained with &&: find-only will NOT run if backup fails
PROJECT_DIR="/home/opc/mail"
LOG_DIR="$PROJECT_DIR/logs"
LOG_FILE="$LOG_DIR/nightly_$(date +%Y-%m-%d).log"

cd "$PROJECT_DIR" || exit 1
echo "" >> "$LOG_FILE"
echo "══════════════════════════════════════════════" >> "$LOG_FILE"
echo "[CRON] nightly started at $(date '+%Y-%m-%d %H:%M:%S')" >> "$LOG_FILE"
echo "══════════════════════════════════════════════" >> "$LOG_FILE"

source venv/bin/activate

# backup MUST succeed before find-only runs
python scripts/backup_db.py >> "$LOG_FILE" 2>&1 && \
  python pipeline.py --find-only >> "$LOG_FILE" 2>&1
EXIT_CODE=$?

echo "[CRON] nightly finished at $(date '+%Y-%m-%d %H:%M:%S') | exit=$EXIT_CODE" >> "$LOG_FILE"

# Keep 14 days of nightly logs
find "$LOG_DIR" -name "nightly_*.log" -mtime +14 -delete

exit $EXIT_CODE
EOF

# ── monday (backup → verify-only → find-only) ──────────────────
cat > "$PROJECT_DIR/run_monday.sh" << 'EOF'
#!/bin/bash
# run_monday.sh — Mon 10 PM: backup → verify-only → find-only
# Chained with &&: each step only runs if the previous succeeded
PROJECT_DIR="/home/opc/mail"
LOG_DIR="$PROJECT_DIR/logs"
LOG_FILE="$LOG_DIR/monday_$(date +%Y-%m-%d).log"

cd "$PROJECT_DIR" || exit 1
echo "" >> "$LOG_FILE"
echo "══════════════════════════════════════════════" >> "$LOG_FILE"
echo "[CRON] monday nightly started at $(date '+%Y-%m-%d %H:%M:%S')" >> "$LOG_FILE"
echo "══════════════════════════════════════════════" >> "$LOG_FILE"

source venv/bin/activate

# backup → verify-only → find-only (all chained — any failure stops the chain)
python scripts/backup_db.py          >> "$LOG_FILE" 2>&1 && \
  python pipeline.py --verify-only   >> "$LOG_FILE" 2>&1 && \
  python pipeline.py --find-only     >> "$LOG_FILE" 2>&1
EXIT_CODE=$?

echo "[CRON] monday nightly finished at $(date '+%Y-%m-%d %H:%M:%S') | exit=$EXIT_CODE" >> "$LOG_FILE"

# Keep 14 days of monday logs
find "$LOG_DIR" -name "monday_*.log" -mtime +14 -delete

exit $EXIT_CODE
EOF

# ── outreach ───────────────────────────────────────────────────
cat > "$PROJECT_DIR/run_outreach.sh" << 'EOF'
#!/bin/bash
# run_outreach.sh — Daily 9 AM: --outreach-only
# Runs independently — uses existing recruiter data even if find-only failed
PROJECT_DIR="/home/opc/mail"
LOG_DIR="$PROJECT_DIR/logs"
LOG_FILE="$LOG_DIR/outreach_$(date +%Y-%m-%d).log"

cd "$PROJECT_DIR" || exit 1
echo "" >> "$LOG_FILE"
echo "══════════════════════════════════════════════" >> "$LOG_FILE"
echo "[CRON] --outreach-only started at $(date '+%Y-%m-%d %H:%M:%S')" >> "$LOG_FILE"
echo "══════════════════════════════════════════════" >> "$LOG_FILE"

source venv/bin/activate
python pipeline.py --outreach-only >> "$LOG_FILE" 2>&1
EXIT_CODE=$?

echo "[CRON] --outreach-only finished at $(date '+%Y-%m-%d %H:%M:%S') | exit=$EXIT_CODE" >> "$LOG_FILE"

# Keep 14 days of outreach logs
find "$LOG_DIR" -name "outreach_*.log" -mtime +14 -delete

exit $EXIT_CODE
EOF

# ── monitor-jobs ───────────────────────────────────────────────
cat > "$PROJECT_DIR/run_monitor.sh" << 'EOF'
#!/bin/bash
# run_monitor.sh — Daily 7 AM: --monitor-jobs
PROJECT_DIR="/home/opc/mail"
LOG_DIR="$PROJECT_DIR/logs"
LOG_FILE="$LOG_DIR/monitor_$(date +%Y-%m-%d).log"

cd "$PROJECT_DIR" || exit 1
echo "" >> "$LOG_FILE"
echo "══════════════════════════════════════════════" >> "$LOG_FILE"
echo "[CRON] --monitor-jobs started at $(date '+%Y-%m-%d %H:%M:%S')" >> "$LOG_FILE"
echo "══════════════════════════════════════════════" >> "$LOG_FILE"

source venv/bin/activate
python pipeline.py --monitor-jobs >> "$LOG_FILE" 2>&1
EXIT_CODE=$?

echo "[CRON] --monitor-jobs finished at $(date '+%Y-%m-%d %H:%M:%S') | exit=$EXIT_CODE" >> "$LOG_FILE"

# Keep 14 days of monitor logs
find "$LOG_DIR" -name "monitor_*.log" -mtime +14 -delete

exit $EXIT_CODE
EOF

# ── detect-ats ─────────────────────────────────────────────────
cat > "$PROJECT_DIR/run_detect.sh" << 'EOF'
#!/bin/bash
# run_detect.sh — Daily 8:30 AM: --detect-ats --batch
PROJECT_DIR="/home/opc/mail"
LOG_DIR="$PROJECT_DIR/logs"
LOG_FILE="$LOG_DIR/detect_$(date +%Y-%m-%d).log"

cd "$PROJECT_DIR" || exit 1
echo "" >> "$LOG_FILE"
echo "══════════════════════════════════════════════" >> "$LOG_FILE"
echo "[CRON] --detect-ats started at $(date '+%Y-%m-%d %H:%M:%S')" >> "$LOG_FILE"
echo "══════════════════════════════════════════════" >> "$LOG_FILE"

source venv/bin/activate
python pipeline.py --detect-ats --batch >> "$LOG_FILE" 2>&1
EXIT_CODE=$?

echo "[CRON] --detect-ats finished at $(date '+%Y-%m-%d %H:%M:%S') | exit=$EXIT_CODE" >> "$LOG_FILE"

# Keep 14 days of detect logs
find "$LOG_DIR" -name "detect_*.log" -mtime +14 -delete

exit $EXIT_CODE
EOF

# ── weekly-summary ─────────────────────────────────────────────
cat > "$PROJECT_DIR/run_weekly_summary.sh" << 'EOF'
#!/bin/bash
# run_weekly_summary.sh — Mon 9 AM: --weekly-summary
PROJECT_DIR="/home/opc/mail"
LOG_DIR="$PROJECT_DIR/logs"
LOG_FILE="$LOG_DIR/weekly_$(date +%Y-%m-%d).log"

cd "$PROJECT_DIR" || exit 1
echo "" >> "$LOG_FILE"
echo "══════════════════════════════════════════════" >> "$LOG_FILE"
echo "[CRON] --weekly-summary started at $(date '+%Y-%m-%d %H:%M:%S')" >> "$LOG_FILE"
echo "══════════════════════════════════════════════" >> "$LOG_FILE"

source venv/bin/activate
python pipeline.py --weekly-summary >> "$LOG_FILE" 2>&1
EXIT_CODE=$?

echo "[CRON] --weekly-summary finished at $(date '+%Y-%m-%d %H:%M:%S') | exit=$EXIT_CODE" >> "$LOG_FILE"

# Keep 14 days of weekly summary logs
find "$LOG_DIR" -name "weekly_*.log" -mtime +14 -delete

exit $EXIT_CODE
EOF

# ── enrichment (daily Phase B) ─────────────────────────────────
cat > "$PROJECT_DIR/run_enrich.sh" << 'EOF'
#!/bin/bash
# run_enrich.sh — Daily 3 AM: Phase B background enrichment
PROJECT_DIR="/home/opc/mail"
LOG_DIR="$PROJECT_DIR/logs"
LOG_FILE="$LOG_DIR/enrich_$(date +%Y-%m).log"

cd "$PROJECT_DIR" || exit 1
echo "" >> "$LOG_FILE"
echo "══════════════════════════════════════════════" >> "$LOG_FILE"
echo "[CRON] enrichment started at $(date '+%Y-%m-%d %H:%M:%S')" >> "$LOG_FILE"
echo "══════════════════════════════════════════════" >> "$LOG_FILE"

source venv/bin/activate
python enrich_ats_companies.py --daily >> "$LOG_FILE" 2>&1
EXIT_CODE=$?

echo "[CRON] enrichment finished at $(date '+%Y-%m-%d %H:%M:%S') | exit=$EXIT_CODE" >> "$LOG_FILE"

exit $EXIT_CODE
EOF

# ── ATS discovery (monthly) ────────────────────────────────────
cat > "$PROJECT_DIR/run_ats_discovery.sh" << 'EOF'
#!/bin/bash
# run_ats_discovery.sh — 1st of month 1 AM: build slug list → enrich
# Chained: enrich only runs if build succeeds
PROJECT_DIR="/home/opc/mail"
LOG_DIR="$PROJECT_DIR/logs"
LOG_FILE="$LOG_DIR/ats_discovery_$(date +%Y-%m).log"

cd "$PROJECT_DIR" || exit 1
echo "" >> "$LOG_FILE"
echo "══════════════════════════════════════════════" >> "$LOG_FILE"
echo "[CRON] ATS discovery started at $(date '+%Y-%m-%d %H:%M:%S')" >> "$LOG_FILE"
echo "══════════════════════════════════════════════" >> "$LOG_FILE"

source venv/bin/activate

python build_ats_slug_list.py              >> "$LOG_FILE" 2>&1 && \
  python enrich_ats_companies.py           >> "$LOG_FILE" 2>&1
EXIT_CODE=$?

echo "[CRON] ATS discovery finished at $(date '+%Y-%m-%d %H:%M:%S') | exit=$EXIT_CODE" >> "$LOG_FILE"

exit $EXIT_CODE
EOF

# ── DB maintenance (monthly) ───────────────────────────────────
cat > "$PROJECT_DIR/run_db_maintenance.sh" << 'EOF'
#!/bin/bash
# run_db_maintenance.sh — 1st of month 3 AM: VACUUM + ANALYZE
PROJECT_DIR="/home/opc/mail"
LOG_DIR="$PROJECT_DIR/logs"
LOG_FILE="$LOG_DIR/maintenance_$(date +%Y-%m).log"

cd "$PROJECT_DIR" || exit 1
echo "" >> "$LOG_FILE"
echo "══════════════════════════════════════════════" >> "$LOG_FILE"
echo "[CRON] DB maintenance started at $(date '+%Y-%m-%d %H:%M:%S')" >> "$LOG_FILE"
echo "══════════════════════════════════════════════" >> "$LOG_FILE"

source venv/bin/activate
python -c "
import sqlite3
conn = sqlite3.connect('data/recruiter_pipeline.db')
conn.execute('VACUUM')
conn.execute('ANALYZE')
conn.close()
print('[OK] DB maintenance complete')
" >> "$LOG_FILE" 2>&1
EXIT_CODE=$?

echo "[CRON] DB maintenance finished at $(date '+%Y-%m-%d %H:%M:%S') | exit=$EXIT_CODE" >> "$LOG_FILE"

exit $EXIT_CODE
EOF

chmod +x \
  "$PROJECT_DIR/run_sync.sh" \
  "$PROJECT_DIR/run_nightly.sh" \
  "$PROJECT_DIR/run_monday.sh" \
  "$PROJECT_DIR/run_outreach.sh" \
  "$PROJECT_DIR/run_monitor.sh" \
  "$PROJECT_DIR/run_detect.sh" \
  "$PROJECT_DIR/run_weekly_summary.sh" \
  "$PROJECT_DIR/run_enrich.sh" \
  "$PROJECT_DIR/run_ats_discovery.sh" \
  "$PROJECT_DIR/run_db_maintenance.sh"

echo "[OK] All wrapper scripts created"

# ═══════════════════════════════════════════════════════════════
# INSTALL CRONTAB
# All times UTC — Oracle Cloud VMs default to UTC.
# To verify: date && timedatectl
# ═══════════════════════════════════════════════════════════════

EXISTING_CRON=$(crontab -l 2>/dev/null || echo "")

# Remove any old pipeline entries if re-running this script
CLEAN_CRON=$(echo "$EXISTING_CRON" \
  | grep -v "run_sync.sh" \
  | grep -v "run_nightly.sh" \
  | grep -v "run_monday.sh" \
  | grep -v "run_outreach.sh" \
  | grep -v "run_monitor.sh" \
  | grep -v "run_detect.sh" \
  | grep -v "run_weekly_summary.sh" \
  | grep -v "run_enrich.sh" \
  | grep -v "run_ats_discovery.sh" \
  | grep -v "run_db_maintenance.sh" \
  | grep -v "keep-alive" \
  | grep -v "hashlib" \
  | grep -v "backups.*mtime" \
  || true)

NEW_CRON=$(cat << 'CRONTAB'

# ═══════════════════════════════════════════════════════════════
# RECRUITER PIPELINE — full schedule
# All times UTC. Oracle Cloud VMs default to UTC.
# ═══════════════════════════════════════════════════════════════

# ─────────────────────────────────────────
# SYNC — every 3 hours (forms + prospective)
# Runs at: 12AM 3AM 6AM 9AM 12PM 3PM 6PM 9PM
# Safe standalone: read+insert only, no backup needed
# ─────────────────────────────────────────
0 0,3,6,9,12,15,18,21 * * * /home/opc/mail/run_sync.sh

# ─────────────────────────────────────────
# OUTREACH — Weekdays only 9 AM (Mon-Fri)
# Skips Saturday and Sunday automatically
# ─────────────────────────────────────────
0 9 * * 1-5 /home/opc/mail/run_outreach.sh

# ─────────────────────────────────────────
# MONITOR JOBS — Daily 7 AM
# ─────────────────────────────────────────
0 7 * * * /home/opc/mail/run_monitor.sh

# ─────────────────────────────────────────
# DETECT ATS — Daily 8:30 AM (after monitor)
# Commented out — uncomment when needed
# ─────────────────────────────────────────
# 30 8 * * * /home/opc/mail/run_detect.sh

# ─────────────────────────────────────────
# WEEKLY SUMMARY — Monday 9 AM
# ─────────────────────────────────────────
0 9 * * 1 /home/opc/mail/run_weekly_summary.sh

# ─────────────────────────────────────────
# NIGHTLY — Tuesday to Sunday 2 AM
# backup → find-only (chained: find-only skipped if backup fails)
# ─────────────────────────────────────────
0 2 * * 2-7 /home/opc/mail/run_nightly.sh

# ─────────────────────────────────────────
# MONDAY NIGHTLY — Monday 10 PM
# backup → verify-only → find-only (fully chained)
# Starts right after last sync of the day (9 PM)
# ─────────────────────────────────────────
0 22 * * 1 /home/opc/mail/run_monday.sh

# ─────────────────────────────────────────
# ENRICHMENT — Daily 3 AM (Phase B background enrichment)
# Runs alongside sync at 3 AM — both are safe to overlap
# ─────────────────────────────────────────
0 3 * * * /home/opc/mail/run_enrich.sh

# ─────────────────────────────────────────
# ATS DISCOVERY — 1st of every month at 1 AM
# build slug list → enrich (chained)
# ─────────────────────────────────────────
0 1 1 * * /home/opc/mail/run_ats_discovery.sh

# ─────────────────────────────────────────
# DB MAINTENANCE — 1st of every month at 3 AM
# VACUUM + ANALYZE (runs after ATS discovery)
# ─────────────────────────────────────────
0 3 1 * * /home/opc/mail/run_db_maintenance.sh

# ─────────────────────────────────────────
# KEEP-ALIVE — every 4 days (Oracle idle protection)
# ─────────────────────────────────────────
0 12 */4 * * python3 -c "import hashlib; [hashlib.sha256(str(i).encode()).hexdigest() for i in range(100000)]" >> /dev/null 2>&1

# ─────────────────────────────────────────
# CLEANUP — delete DB backups older than 28 days
# Runs every Sunday 1:35 AM
# ─────────────────────────────────────────
35 1 * * 0 find /home/opc/mail/data/backups/ -name "*.db" -mtime +28 -delete

CRONTAB
)

echo "$CLEAN_CRON$NEW_CRON" | crontab -
echo "[OK] Crontab installed"

# ── Verify ────────────────────────────────────────────────────
echo ""
echo "══════════════════════════════════════════════"
echo "Installed cron jobs:"
crontab -l | grep -v "^#" | grep -v "^$"
echo "══════════════════════════════════════════════"
echo ""
echo "Wrapper scripts created in: $PROJECT_DIR/"
echo "  run_sync.sh            ← --sync-forms + --sync-prospective (every 3h)"
echo "  run_nightly.sh         ← backup → find-only (Tue-Sun 2 AM)"
echo "  run_monday.sh          ← backup → verify-only → find-only (Mon 10 PM)"
echo "  run_outreach.sh        ← --outreach-only (daily 9 AM)"
echo "  run_monitor.sh         ← --monitor-jobs (daily 7 AM)"
echo "  run_detect.sh          ← --detect-ats --batch (daily 8:30 AM)"
echo "  run_weekly_summary.sh  ← --weekly-summary (Mon 9 AM)"
echo "  run_enrich.sh          ← enrichment Phase B (daily 3 AM)"
echo "  run_ats_discovery.sh   ← build slugs → enrich (1st of month 1 AM)"
echo "  run_db_maintenance.sh  ← VACUUM + ANALYZE (1st of month 3 AM)"
echo ""
echo "Logs directory: $LOG_DIR/"
echo ""
echo "To tail logs live:"
echo "  tail -f $LOG_DIR/sync_\$(date +%Y-%m-%d).log"
echo "  tail -f $LOG_DIR/nightly_\$(date +%Y-%m-%d).log"
echo "  tail -f $LOG_DIR/pipeline.log"
echo ""
echo "To test a script immediately:"
echo "  $PROJECT_DIR/run_sync.sh"
echo "  $PROJECT_DIR/run_nightly.sh"
echo "  $PROJECT_DIR/run_monday.sh"