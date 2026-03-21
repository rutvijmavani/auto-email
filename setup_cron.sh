#!/bin/bash
# setup_cron.sh — Install cron jobs for the recruiter pipeline
# Run once on your Oracle Cloud VM:
#   chmod +x setup_cron.sh && ./setup_cron.sh
#
# ── Full daily timeline (UTC) ───────────────────────────────────
#
#  Daytime (every day):
#   7:00 AM  --monitor-jobs
#   9:00 AM  --outreach-only (Mon-Fri only)
#   9:00 AM  --weekly-summary (Mon only)
#   9:00 AM, 12PM, 3PM, 6PM, 9PM  → sync-forms + sync-prospective
#
#  Nightly chain at 1:00 AM:
#   Tue-Sun: sync → backup → find-only → verify-filled
#   Monday:  sync → backup → verify-only → find-only → verify-filled
#
#  Monthly chain at 1:00 AM (1st of month):
#   sync → backup → find-only → build-slugs → enrich → VACUUM → verify-filled
#   (replaces both nightly and monthly jobs on the 1st)
#
#  Enrichment at 3:00 AM (standalone, 18hr background job):
#   enrich_ats_companies.py --daily
#
# ── DB consistency guarantee ────────────────────────────────────
#   All nightly chains start with sync then backup.
#   If backup fails, everything after stops.
#   VACUUM only runs at end of monthly chain — DB guaranteed quiet.
#   Daytime syncs are read+insert only — safe without backup gate.
#   Enrichment runs independently at 3AM — writes to ats_discovery.db only.

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
# ═══════════════════════════════════════════════════════════════

# ── daytime sync (forms + prospective) ────────────────────────
cat > "$PROJECT_DIR/run_sync.sh" << 'EOF'
#!/bin/bash
# run_sync.sh — daytime sync: --sync-forms + --sync-prospective
# Safe standalone: read+insert only, no backup needed.
# Runs at: 9AM 12PM 3PM 6PM 9PM daily.
PROJECT_DIR="/home/opc/mail"
LOG_DIR="$PROJECT_DIR/logs"
LOG_FILE="$LOG_DIR/sync_$(date +%Y-%m-%d).log"

cd "$PROJECT_DIR" || exit 1
echo "" >> "$LOG_FILE"
echo "══════════════════════════════════════════════" >> "$LOG_FILE"
echo "[CRON] sync started at $(date '+%Y-%m-%d %H:%M:%S')" >> "$LOG_FILE"
echo "══════════════════════════════════════════════" >> "$LOG_FILE"

source venv/bin/activate
python pipeline.py --sync-forms       >> "$LOG_FILE" 2>&1 && \
python pipeline.py --sync-prospective >> "$LOG_FILE" 2>&1
EXIT_CODE=$?

echo "[CRON] sync finished at $(date '+%Y-%m-%d %H:%M:%S') | exit=$EXIT_CODE" >> "$LOG_FILE"
find "$LOG_DIR" -name "sync_*.log" -mtime +14 -delete
exit $EXIT_CODE
EOF

# ── nightly chain Tue-Sun (sync → backup → find-only → verify-filled) ─
cat > "$PROJECT_DIR/run_nightly.sh" << 'EOF'
#!/bin/bash
# run_nightly.sh — Tue-Sun 1 AM nightly chain:
#   sync → backup → find-only → verify-filled
# Each step only runs if the previous succeeded (&&).
# Backup failure stops the chain — DB stays safe.
# Guard: exits on the 1st of the month — run_monthly.sh handles it.
PROJECT_DIR="/home/opc/mail"
LOG_DIR="$PROJECT_DIR/logs"
LOG_FILE="$LOG_DIR/nightly_$(date +%Y-%m-%d).log"

if [ "$(date +%d)" = "01" ]; then
  echo "[SKIP] 1st of month — run_monthly.sh handles today" >> "$LOG_FILE"
  exit 0
fi

cd "$PROJECT_DIR" || exit 1
echo "" >> "$LOG_FILE"
echo "══════════════════════════════════════════════" >> "$LOG_FILE"
echo "[CRON] nightly started at $(date '+%Y-%m-%d %H:%M:%S')" >> "$LOG_FILE"
echo "══════════════════════════════════════════════" >> "$LOG_FILE"

source venv/bin/activate

echo "[STEP 1] sync at $(date '+%H:%M:%S')" >> "$LOG_FILE"
python pipeline.py --sync-forms       >> "$LOG_FILE" 2>&1 && \
python pipeline.py --sync-prospective >> "$LOG_FILE" 2>&1 && \

echo "[STEP 2] backup at $(date '+%H:%M:%S')" >> "$LOG_FILE" && \
python scripts/backup_db.py           >> "$LOG_FILE" 2>&1 && \

echo "[STEP 3] find-only at $(date '+%H:%M:%S')" >> "$LOG_FILE" && \
python pipeline.py --find-only        >> "$LOG_FILE" 2>&1 && \

echo "[STEP 4] verify-filled at $(date '+%H:%M:%S')" >> "$LOG_FILE" && \
python pipeline.py --verify-filled    >> "$LOG_FILE" 2>&1

EXIT_CODE=$?
echo "[CRON] nightly finished at $(date '+%Y-%m-%d %H:%M:%S') | exit=$EXIT_CODE" >> "$LOG_FILE"
find "$LOG_DIR" -name "nightly_*.log" -mtime +14 -delete
exit $EXIT_CODE
EOF

# ── monday nightly chain (sync → backup → verify → find-only → verify-filled) ─
cat > "$PROJECT_DIR/run_monday.sh" << 'EOF'
#!/bin/bash
# run_monday.sh — Monday 1 AM nightly chain:
#   sync → backup → verify-only → find-only → verify-filled
# Each step only runs if the previous succeeded (&&).
# Guard: exits on the 1st of the month — run_monthly.sh handles it.
PROJECT_DIR="/home/opc/mail"
LOG_DIR="$PROJECT_DIR/logs"
LOG_FILE="$LOG_DIR/monday_$(date +%Y-%m-%d).log"

if [ "$(date +%d)" = "01" ]; then
  echo "[SKIP] 1st of month — run_monthly.sh handles today" >> "$LOG_FILE"
  exit 0
fi

cd "$PROJECT_DIR" || exit 1
echo "" >> "$LOG_FILE"
echo "══════════════════════════════════════════════" >> "$LOG_FILE"
echo "[CRON] monday nightly started at $(date '+%Y-%m-%d %H:%M:%S')" >> "$LOG_FILE"
echo "══════════════════════════════════════════════" >> "$LOG_FILE"

source venv/bin/activate

echo "[STEP 1] sync at $(date '+%H:%M:%S')" >> "$LOG_FILE"
python pipeline.py --sync-forms       >> "$LOG_FILE" 2>&1 && \
python pipeline.py --sync-prospective >> "$LOG_FILE" 2>&1 && \

echo "[STEP 2] backup at $(date '+%H:%M:%S')" >> "$LOG_FILE" && \
python scripts/backup_db.py           >> "$LOG_FILE" 2>&1 && \

echo "[STEP 3] verify-only at $(date '+%H:%M:%S')" >> "$LOG_FILE" && \
python pipeline.py --verify-only      >> "$LOG_FILE" 2>&1 && \

echo "[STEP 4] find-only at $(date '+%H:%M:%S')" >> "$LOG_FILE" && \
python pipeline.py --find-only        >> "$LOG_FILE" 2>&1 && \

echo "[STEP 5] verify-filled at $(date '+%H:%M:%S')" >> "$LOG_FILE" && \
python pipeline.py --verify-filled    >> "$LOG_FILE" 2>&1

EXIT_CODE=$?
echo "[CRON] monday nightly finished at $(date '+%Y-%m-%d %H:%M:%S') | exit=$EXIT_CODE" >> "$LOG_FILE"
find "$LOG_DIR" -name "monday_*.log" -mtime +14 -delete
exit $EXIT_CODE
EOF

# ── monthly chain (1st of month) ───────────────────────────────
cat > "$PROJECT_DIR/run_monthly.sh" << 'EOF'
#!/bin/bash
# run_monthly.sh — 1st of every month at 1 AM
# Fully sequential chain — replaces both nightly and monthly jobs:
#   sync → backup → find-only → build-slugs → enrich → VACUUM → verify-filled
# Each step only runs if the previous succeeded (&&).
# VACUUM runs second-to-last — DB guaranteed quiet at that point.
PROJECT_DIR="/home/opc/mail"
LOG_DIR="$PROJECT_DIR/logs"
LOG_FILE="$LOG_DIR/monthly_$(date +%Y-%m).log"

cd "$PROJECT_DIR" || exit 1
echo "" >> "$LOG_FILE"
echo "══════════════════════════════════════════════" >> "$LOG_FILE"
echo "[CRON] monthly started at $(date '+%Y-%m-%d %H:%M:%S')" >> "$LOG_FILE"
echo "══════════════════════════════════════════════" >> "$LOG_FILE"

source venv/bin/activate

echo "[STEP 1] sync at $(date '+%H:%M:%S')" >> "$LOG_FILE"
python pipeline.py --sync-forms        >> "$LOG_FILE" 2>&1 && \
python pipeline.py --sync-prospective  >> "$LOG_FILE" 2>&1 && \

echo "[STEP 2] backup at $(date '+%H:%M:%S')" >> "$LOG_FILE" && \
python scripts/backup_db.py            >> "$LOG_FILE" 2>&1 && \

echo "[STEP 3] find-only at $(date '+%H:%M:%S')" >> "$LOG_FILE" && \
python pipeline.py --find-only         >> "$LOG_FILE" 2>&1 && \

echo "[STEP 4] ATS slug discovery at $(date '+%H:%M:%S')" >> "$LOG_FILE" && \
python build_ats_slug_list.py          >> "$LOG_FILE" 2>&1 && \

echo "[STEP 5] enrich at $(date '+%H:%M:%S')" >> "$LOG_FILE" && \
python enrich_ats_companies.py         >> "$LOG_FILE" 2>&1 && \

echo "[STEP 6] VACUUM + ANALYZE at $(date '+%H:%M:%S')" >> "$LOG_FILE" && \
python -c "
import sqlite3
conn = sqlite3.connect('data/recruiter_pipeline.db')
conn.execute('VACUUM')
conn.execute('ANALYZE')
conn.close()
print('[OK] DB maintenance complete')
" >> "$LOG_FILE" 2>&1 && \

echo "[STEP 7] verify-filled at $(date '+%H:%M:%S')" >> "$LOG_FILE" && \
python pipeline.py --verify-filled     >> "$LOG_FILE" 2>&1

EXIT_CODE=$?
echo "[CRON] monthly finished at $(date '+%Y-%m-%d %H:%M:%S') | exit=$EXIT_CODE" >> "$LOG_FILE"
exit $EXIT_CODE
EOF

# ── outreach (Mon-Fri 9 AM) ────────────────────────────────────
cat > "$PROJECT_DIR/run_outreach.sh" << 'EOF'
#!/bin/bash
# run_outreach.sh — Mon-Fri 9 AM: --outreach-only
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
find "$LOG_DIR" -name "outreach_*.log" -mtime +14 -delete
exit $EXIT_CODE
EOF

# ── monitor-jobs (daily 7 AM) ──────────────────────────────────
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
find "$LOG_DIR" -name "monitor_*.log" -mtime +14 -delete
exit $EXIT_CODE
EOF

# ── detect-ats (currently disabled) ───────────────────────────
cat > "$PROJECT_DIR/run_detect.sh" << 'EOF'
#!/bin/bash
# run_detect.sh — --detect-ats --batch (disabled — run manually)
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
find "$LOG_DIR" -name "detect_*.log" -mtime +14 -delete
exit $EXIT_CODE
EOF

# ── weekly-summary (Mon 9 AM) ──────────────────────────────────
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
find "$LOG_DIR" -name "weekly_*.log" -mtime +14 -delete
exit $EXIT_CODE
EOF

# ── daily enrichment (3 AM) ────────────────────────────────────
cat > "$PROJECT_DIR/run_enrich.sh" << 'EOF'
#!/bin/bash
# run_enrich.sh — Daily 3 AM: Phase B background enrichment
# Runs independently — enrichment is read+write on ats_discovery.db only,
# no conflict with recruiter_pipeline.db nightly chain (starts at 1 AM).
PROJECT_DIR="/home/opc/mail"
LOG_DIR="$PROJECT_DIR/logs"
LOG_FILE="$LOG_DIR/enrich_$(date +%Y-%m).log"

cd "$PROJECT_DIR" || exit 1
echo "" >> "$LOG_FILE"
echo "══════════════════════════════════════════════" >> "$LOG_FILE"
echo "[CRON] enrichment started at $(date '+%Y-%m-%d %H:%M:%S')" >> "$LOG_FILE"
echo "══════════════════════════════════════════════" >> "$LOG_FILE"

source venv/bin/activate

if [ "$(date +%d)" = "01" ]; then
  echo "[SKIP] 1st of month — run_monthly.sh handles enrichment today" >> "$LOG_FILE"
  exit 0
fi

python enrich_ats_companies.py --daily >> "$LOG_FILE" 2>&1
EXIT_CODE=$?

echo "[CRON] enrichment finished at $(date '+%Y-%m-%d %H:%M:%S') | exit=$EXIT_CODE" >> "$LOG_FILE"
exit $EXIT_CODE
EOF

# ── verify-filled (manual use / standalone) ────────────────────
cat > "$PROJECT_DIR/run_verify_filled.sh" << 'EOF'
#!/bin/bash
# run_verify_filled.sh — --verify-filled
# NOT in crontab — runs as part of nightly/monday/monthly chains.
# Use this script to run manually when needed:
#   /home/opc/mail/run_verify_filled.sh
PROJECT_DIR="/home/opc/mail"
LOG_DIR="$PROJECT_DIR/logs"
LOG_FILE="$LOG_DIR/verify_filled_$(date +%Y-%m-%d).log"

cd "$PROJECT_DIR" || exit 1
echo "" >> "$LOG_FILE"
echo "══════════════════════════════════════════════" >> "$LOG_FILE"
echo "[CRON] --verify-filled started at $(date '+%Y-%m-%d %H:%M:%S')" >> "$LOG_FILE"
echo "══════════════════════════════════════════════" >> "$LOG_FILE"

source venv/bin/activate
python pipeline.py --verify-filled >> "$LOG_FILE" 2>&1
EXIT_CODE=$?

echo "[CRON] --verify-filled finished at $(date '+%Y-%m-%d %H:%M:%S') | exit=$EXIT_CODE" >> "$LOG_FILE"
find "$LOG_DIR" -name "verify_filled_*.log" -mtime +14 -delete
exit $EXIT_CODE
EOF

chmod +x \
  "$PROJECT_DIR/run_sync.sh" \
  "$PROJECT_DIR/run_nightly.sh" \
  "$PROJECT_DIR/run_monday.sh" \
  "$PROJECT_DIR/run_monthly.sh" \
  "$PROJECT_DIR/run_outreach.sh" \
  "$PROJECT_DIR/run_monitor.sh" \
  "$PROJECT_DIR/run_detect.sh" \
  "$PROJECT_DIR/run_weekly_summary.sh" \
  "$PROJECT_DIR/run_enrich.sh" \
  "$PROJECT_DIR/run_verify_filled.sh"

echo "[OK] All wrapper scripts created"

# ═══════════════════════════════════════════════════════════════
# INSTALL CRONTAB
# All times America/New_York (EDT, -0400).
# To verify: date && timedatectl
# ═══════════════════════════════════════════════════════════════

EXISTING_CRON=$(crontab -l 2>/dev/null || echo "")

CLEAN_CRON=$(echo "$EXISTING_CRON" \
  | grep -v "run_sync.sh" \
  | grep -v "run_nightly.sh" \
  | grep -v "run_monday.sh" \
  | grep -v "run_monthly.sh" \
  | grep -v "run_outreach.sh" \
  | grep -v "run_monitor.sh" \
  | grep -v "run_detect.sh" \
  | grep -v "run_weekly_summary.sh" \
  | grep -v "run_enrich.sh" \
  | grep -v "run_verify_filled.sh" \
  | grep -v "run_ats_discovery.sh" \
  | grep -v "run_db_maintenance.sh" \
  | grep -v "keep-alive" \
  | grep -v "hashlib" \
  | grep -v "backups.*mtime" \
  | grep -v "RECRUITER PIPELINE" \
  | grep -v "All times UTC" \
  || true)

NEW_CRON=$(cat << 'CRONTAB'

# ═══════════════════════════════════════════════════════════════
# RECRUITER PIPELINE — full schedule
# All times America/New_York (EDT, -0400).
# ═══════════════════════════════════════════════════════════════

# ─────────────────────────────────────────
# DAYTIME SYNC — 9AM 12PM 3PM 6PM 9PM daily
# ─────────────────────────────────────────
0 9,12,15,18,21 * * * /home/opc/mail/run_sync.sh

# ─────────────────────────────────────────
# MONITOR JOBS — Daily 7 AM
# ─────────────────────────────────────────
0 7 * * * /home/opc/mail/run_monitor.sh

# ─────────────────────────────────────────
# OUTREACH — Mon-Fri 9 AM only
# ─────────────────────────────────────────
0 9 * * 1-5 /home/opc/mail/run_outreach.sh

# ─────────────────────────────────────────
# WEEKLY SUMMARY — Monday 9 AM
# ─────────────────────────────────────────
0 9 * * 1 /home/opc/mail/run_weekly_summary.sh

# ─────────────────────────────────────────
# NIGHTLY CHAIN — Tuesday to Sunday 1 AM
# sync → backup → find-only → verify-filled
# ─────────────────────────────────────────
0 1 * * 2-7 /home/opc/mail/run_nightly.sh

# ─────────────────────────────────────────
# MONDAY NIGHTLY CHAIN — Monday 1 AM
# sync → backup → verify-only → find-only → verify-filled
# ─────────────────────────────────────────
0 1 * * 1 /home/opc/mail/run_monday.sh

# ─────────────────────────────────────────
# MONTHLY CHAIN — 1st of every month at 1 AM
# sync → backup → find-only → build-slugs → enrich → VACUUM → verify-filled
# ─────────────────────────────────────────
0 1 1 * * /home/opc/mail/run_monthly.sh

# ─────────────────────────────────────────
# DAILY ENRICHMENT — 3 AM (standalone, 18hr background job)
# Writes to ats_discovery.db only — no conflict with nightly chain
# ─────────────────────────────────────────
0 3 * * * /home/opc/mail/run_enrich.sh

# ─────────────────────────────────────────
# DETECT ATS — currently disabled
# Uncomment when needed:
# 30 8 * * * /home/opc/mail/run_detect.sh
# ─────────────────────────────────────────

# ─────────────────────────────────────────
# KEEP-ALIVE — every 4 days (Oracle idle protection)
# ─────────────────────────────────────────
0 12 */4 * * python3 -c "import hashlib; [hashlib.sha256(str(i).encode()).hexdigest() for i in range(100000)]" >> /dev/null 2>&1

CRONTAB
)

echo "$NEW_CRON" | crontab -
echo "[OK] Crontab installed"

# ── Verify ────────────────────────────────────────────────────
echo ""
echo "══════════════════════════════════════════════"
echo "Installed cron jobs:"
crontab -l | grep -v "^#" | grep -v "^$"
echo "══════════════════════════════════════════════"
echo ""
echo "Wrapper scripts in: $PROJECT_DIR/"
echo "  run_sync.sh            ← sync-forms + sync-prospective (9AM 12PM 3PM 6PM 9PM)"
echo "  run_nightly.sh         ← sync→backup→find-only→verify-filled (Tue-Sun 1AM)"
echo "  run_monday.sh          ← sync→backup→verify-only→find-only→verify-filled (Mon 1AM)"
echo "  run_monthly.sh         ← sync→backup→find→slugs→enrich→VACUUM→verify-filled (1st 1AM)"
echo "  run_outreach.sh        ← --outreach-only (Mon-Fri 9AM)"
echo "  run_monitor.sh         ← --monitor-jobs (daily 7AM)"
echo "  run_detect.sh          ← --detect-ats --batch (disabled — run manually)"
echo "  run_weekly_summary.sh  ← --weekly-summary (Mon 9AM)"
echo "  run_enrich.sh          ← enrichment Phase B (daily 3AM, standalone)"
echo "  run_verify_filled.sh   ← --verify-filled (manual use only)"
echo ""
echo "Logs: $LOG_DIR/"
echo ""
echo "To tail logs live:"
echo "  tail -f $LOG_DIR/nightly_\$(date +%Y-%m-%d).log"
echo "  tail -f $LOG_DIR/pipeline.log"
echo ""
echo "To test immediately:"
echo "  $PROJECT_DIR/run_sync.sh"
echo "  $PROJECT_DIR/run_nightly.sh"
echo "  $PROJECT_DIR/run_monday.sh"
echo "  $PROJECT_DIR/run_monthly.sh"