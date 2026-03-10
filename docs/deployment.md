# Deployment Guide

## Overview

This document covers deployment options, cloud setup, logging strategy,
and operational maintenance for the Recruiter Outreach Pipeline.

---

## Deployment Options

### Option 1 — Local Machine (current)

```
Pros:
  → No cost
  → Full control
  → Works fine for manual use

Cons:
  → Machine must be on and awake at send time
  → Manual effort to run commands daily
  → If laptop sleeps/restarts → outreach missed
  → Not reliable for consistent daily sending
```

**Reliability: ~70%** — misses runs when machine is off/sleeping.

---

### Option 2 — Windows Task Scheduler (free, local)

Automate daily runs without any cloud cost:

```powershell
# Daily 9:00 AM — send outreach emails
schtasks /create /tn "RecruiterOutreach" ^
  /tr "python C:\Downloads\mail\pipeline.py --outreach-only" ^
  /sc daily /st 09:00

# Weekly Monday 10:00 PM — verify recruiters
schtasks /create /tn "RecruiterVerify" ^
  /tr "python C:\Downloads\mail\pipeline.py --verify-only" ^
  /sc weekly /d MON /st 22:00
```

**Wake-on-Timer** (run even when laptop is sleeping):
```
Task Scheduler → Task Properties:
  General tab   → "Run whether user is logged in or not" ✓
  Conditions tab → "Wake the computer to run this task" ✓

Requirements for wake-on-timer to work:
  ✓ Laptop plugged in (not on battery)
  ✓ Fast Startup disabled in Windows power settings
  ✓ BIOS supports wake timers
  ✓ Machine in SLEEP (not Hibernate or Shutdown)
```

**Reliability: ~80%** — still misses runs on hibernate/shutdown.

---

### Option 3 — Oracle Cloud Free Tier VM (recommended)

**Most reliable option at zero cost.**

```
Service:  Oracle Cloud Infrastructure (OCI)
Tier:     Always Free (not just 12 months)
Shape:    VM.Standard.A1.Flex (ARM/Ampere)
Specs:    4 OCPUs, 24 GB RAM, 47 GB storage
          (free allowance: 3,000 OCPU-hours/month)
Cost:     $0 forever
```

**Sign up:** https://www.oracle.com/cloud/free/

```
Requirements:
  → Credit card for identity verification only
  → NOT charged — purely for verification
  → $0 forever on always-free resources
```

**Reliability: 99.9%** — always on, never misses a run.

---

### Option 4 — DigitalOcean Droplet ($4/month)

```
Specs:    512 MB RAM, 10 GB SSD
Cost:     $4/month (~$48/year)
Uptime:   99.99% SLA

Advantage over Oracle:
  → Never terminated for inactivity
  → Simpler, more predictable
  → Better support

When to choose:
  → If Oracle free tier feels risky
  → If Oracle changes free tier policy
  → Worth $48/year for reliability during job search
```

---

## Oracle Cloud VM Setup

### Step 1 — Create account
```
1. Go to https://www.oracle.com/cloud/free/
2. Sign up with email + credit card (not charged)
3. Choose home region closest to you
4. Select "Always Free" tier
```

### Step 2 — Create VM instance
```
OCI Console → Compute → Instances → Create Instance

Settings:
  Name:    recruiter-pipeline
  Image:   Ubuntu 22.04 (ARM-compatible)
  Shape:   VM.Standard.A1.Flex (Always Free)
  OCPUs:   2
  RAM:     12 GB
  Storage: 47 GB boot volume

Networking:
  Create new VCN (or use existing)
  Assign public IP: Yes

SSH Key:
  Generate new key pair
  Download private key (.pem file)
  Keep it safe — needed for SSH access
```

### Step 3 — Connect to VM
```bash
# From your local machine
ssh -i /path/to/private-key.pem ubuntu@<your-vm-ip>
```

### Step 4 — Install dependencies
```bash
# Update system
sudo apt update && sudo apt upgrade -y

# No swap needed — A1.Flex has 12 GB RAM (Playwright uses ~600 MB)

# Install Python 3.11
sudo apt install python3.11 python3.11-pip python3-venv -y

# Install Playwright system dependencies
sudo apt install -y \
  libglib2.0-0 libnss3 libnspr4 libdbus-1-3 \
  libatk1.0-0 libatk-bridge2.0-0 libcups2 \
  libdrm2 libxkbcommon0 libxcomposite1 \
  libxdamage1 libxfixes3 libxrandr2 \
  libgbm1 libasound2

# Create project directory
mkdir -p /home/ubuntu/mail
cd /home/ubuntu/mail

# Clone your repo
git clone https://github.com/yourusername/auto-email.git .

# Create virtual environment
python3 -m venv venv
source venv/bin/activate

# Install Python dependencies
pip install -r requirements.txt

# Install Playwright browsers
playwright install chromium

# Verify AWS + Athena access
python -c "import boto3; print('[OK] boto3')"
python -c "import pyathena; print('[OK] pyathena')"
```

### Step 5 — Configure environment
```bash
# Create .env file
nano /home/ubuntu/mail/.env

# Add your credentials:
GMAIL_EMAIL=your@gmail.com
GMAIL_APP_PASSWORD=your-app-password
GOOGLE_SHEET_ID=your-sheet-id
GEMINI_API_KEY=your-api-key
SERPER_API_KEY=your-serper-key       # serper.dev (2500 free credits)

# AWS Athena (ATS discovery)
AWS_ACCESS_KEY_ID=your-key
AWS_SECRET_ACCESS_KEY=your-secret
AWS_REGION=us-east-1
ATHENA_DATABASE=ccindex
ATHENA_TABLE=ccindex
ATHENA_S3_OUTPUT=s3://your-bucket/athena-results/

# Brave Search API (ATS discovery fallback)
BRAVE_API_KEY=your-key               # api.search.brave.com (1000 free/month)

# Create data directories
mkdir -p /home/ubuntu/mail/data
mkdir -p /home/ubuntu/mail/data/backups
mkdir -p /home/ubuntu/mail/logs
```

### Step 6 — CareerShift session
```bash
# Run auth once interactively to create session file
cd /home/ubuntu/mail
source venv/bin/activate
python careershift/auth.py

# Session saved to: data/careershift_session.json
# Valid for ~30 days — re-run auth.py when it expires
```

### Step 7 — Set up cron jobs
```bash
# Open crontab editor
crontab -e

# Add these lines:

# Daily 9:00 AM — send outreach emails
0 9 * * * cd /home/ubuntu/mail && source venv/bin/activate && python pipeline.py --outreach-only >> logs/outreach_$(date +\%Y-\%m-\%d).log 2>&1

# Weekly Monday 10:00 PM — verify recruiters
0 22 * * 1 cd /home/ubuntu/mail && source venv/bin/activate && python pipeline.py --verify-only >> logs/verify_$(date +\%Y-\%m-\%d).log 2>&1

# Keep-alive every 4 days (prevents Oracle idle reclamation)
0 12 */4 * * python3 -c "import hashlib; [hashlib.sha256(str(i).encode()).hexdigest() for i in range(100000)]" >> /dev/null 2>&1

# Weekly Sunday midnight — backup SQLite DB
0 0 * * 0 cp /home/ubuntu/mail/data/recruiter_pipeline.db /home/ubuntu/mail/data/backups/recruiter_pipeline_$(date +\%Y\%m\%d).db

# Delete DB backups older than 28 days
5 0 * * 0 find /home/ubuntu/mail/data/backups/ -name "*.db" -mtime +28 -delete
```

### Step 8 — Deploy code updates
```bash
# When you push new code to GitHub, pull on VM:
cd /home/ubuntu/mail
git pull origin main

# Restart any running processes if needed
```

---

## Oracle Idle Reclamation — What You Need to Know

### The risk
```
Oracle monitors free tier VMs for idle usage:
  → If CPU < 10% for 7 consecutive days
  → Oracle sends warning email
  → If still idle after warning
  → VM TERMINATED (data lost!)
```

### Why your VM won't be idle
```
Your daily cron jobs prevent this:
  Daily  9 AM: --outreach-only → Playwright + SMTP = CPU spikes
  Weekly Mon:  --verify-only   → Playwright + CareerShift = CPU spikes
  Every 4 days: keep-alive script = CPU activity

Playwright launching Chromium = significant CPU usage
Oracle's monitors will see regular activity
Risk of reclamation: LOW
```

### Protection measures
```
1. Keep-alive cron (already in Step 7 above)
   → Runs every 4 days
   → Generates CPU activity
   → Belt-and-suspenders protection

2. Enable Oracle email notifications
   Oracle Console → Account → Preferences → Notifications
   → Get warned before any termination
   → Gives you time to react

3. Weekly DB backup (already in Step 7 above)
   → Even if VM terminated → data recoverable
   → Keep last 4 weekly backups

4. Code always on GitHub (you're already doing this ✓)
   → VM terminated → git clone → 30 min to restore
   → Only SQLite DB needs separate backup
```

---

## SQLite vs Oracle Database

**Short answer: Keep SQLite. Don't migrate.**

```
Your data scale:
  ~50-200 applications
  ~100-400 recruiters
  ~300-1000 outreach records
  Single user, single process

SQLite advantages for this use case:
  ✓ Zero configuration
  ✓ File-based — backup = copy the file
  ✓ No network latency
  ✓ Runs anywhere
  ✓ Already working perfectly
  ✓ Portable — move between machines easily

Oracle DB advantages (none apply here):
  → Multiple servers writing simultaneously → not your case
  → Millions of rows → not your case
  → Multiple users → not your case
  → High availability → not your case

Recommended stack:
  Oracle VM (free compute) + SQLite (local file)
  → Best of both worlds
  → No migration needed
  → Fast, reliable, simple
```

---

## Logging Strategy

### Phase 1 — Email reports (implement now)
```
HTML email sent after every pipeline run:
  --outreach-only → Outreach Report email
  --find-only     → Find Report email
  --verify-only   → Verification Report email

Gives visibility on healthy runs.
```

### Phase 2 — File logging (implement during Oracle migration)
```
Python logging module routes output to both:
  → Terminal (for interactive runs)
  → Log file (for cron job runs)

Log files:
  logs/outreach_2026-03-04.log
  logs/find_2026-03-04.log
  logs/verify_2026-03-04.log

Log levels:
  INFO     → normal operations
  WARNING  → non-fatal issues (quota low, JD missing)
  ERROR    → failures (SMTP failed, session expired)
  CRITICAL → pipeline cannot continue

Retention: 30 days (auto-delete older files)
```

### Why both are needed
```
Email report = what happened (summary for healthy runs)
Log file     = why it happened (forensics for failed runs)

If pipeline crashes before email sends:
  → Log file captures everything
  → Check logs to understand what went wrong
```

### Phase 3 — Optional future
```
Structured JSON logs for easier parsing
Log aggregation if needed
```

---

## Storage & Compute Requirements

### Storage (Oracle free tier: 47 GB)

```
OS + Ubuntu base:              ~3.0 GB
Python packages + Chromium:    ~0.8 GB
Project code:                  ~0.005 GB
SQLite DB (6 months):          ~0.023 GB
ats_discovery.db:              ~0.008 GB
ats_archive.csv.gz:            ~0.001 GB
DB backups (28 daily × 23 MB): ~0.644 GB
PDF digests (30 day retention):~0.008 GB
Log files (30 day retention):  ~0.010 GB
Athena CSV (2 day retention):  ~0.010 GB
─────────────────────────────────────────
Total used:                    ~4.5 GB
Available:                     ~42.5 GB (90% free)
```

Storage is not a concern. The pipeline uses ~9.6%
of available storage even after 6 months.

### Compute (Oracle free tier: A1.Flex — 2 OCPU + 12 GB RAM)

```
Pipeline         RAM Peak   Duration    Risk
─────────────────────────────────────────────────
--monitor-jobs   ~250 MB    5 min       None ✓
--outreach-only  ~80 MB     5 min       None ✓
--sync-forms     ~50 MB     30 sec      None ✓
--find-only      ~600 MB    30-40 min   None ✓
--verify-only    ~600 MB    10-15 min   None ✓
DB backup        ~80 MB     5 sec       None ✓
build_ats_slug   ~200 MB    5 min       None ✓
```

All pipelines run sequentially — never in parallel.
Peak RAM: ~600 MB out of 12 GB available (5% usage).
No swap file needed.

### No Swap Needed (A1.Flex)

```
A1.Flex has 12 GB RAM → no swap file needed.
Playwright uses ~600 MB → trivial on 12 GB.

Verify Playwright works on ARM after setup:
  playwright install chromium
  python -c "
  from playwright.sync_api import sync_playwright
  print('[OK] Playwright ARM working')
  "
```

---

## Recommended Deployment Schedule

### Key rules
```
--sync-forms and --add: run only between 12 AM and 10 PM
  → No form syncing after 10 PM
  → DB is quiet after 10 PM
  → Safe for overnight backup and processing

Nightly jobs chain with && operator:
  → Each step only runs if previous step succeeded
  → Backup always runs FIRST (clean pre-run snapshot)
  → If backup fails → nothing else runs → data safe
  → If verify-only fails → find-only does not run
```

### Complete cron schedule
```bash
# ─────────────────────────────────────────
# SYNC — every 3 hours, stops at 10 PM
# Runs at: 12AM 3AM 6AM 9AM 12PM 3PM 6PM 9PM
# ─────────────────────────────────────────
0 0,3,6,9,12,15,18,21 * * * cd /home/ubuntu/mail &&   source venv/bin/activate &&   python pipeline.py --sync-forms   >> logs/sync_6--.log 2>&1

# ─────────────────────────────────────────
# NIGHTLY — Tuesday to Sunday 2 AM
# backup → find-only
# ─────────────────────────────────────────
0 2 * * 2-7 cd /home/ubuntu/mail &&   source venv/bin/activate &&   python scripts/backup_db.py &&   python pipeline.py --find-only   >> logs/nightly_6--.log 2>&1

# ─────────────────────────────────────────
# MONDAY NIGHTLY — 10 PM
# backup → verify-only → find-only
# Starts right after last sync-forms of the day
# ─────────────────────────────────────────
0 22 * * 1 cd /home/ubuntu/mail &&   source venv/bin/activate &&   python scripts/backup_db.py &&   python pipeline.py --verify-only &&   python pipeline.py --find-only   >> logs/monday_6--.log 2>&1

# ─────────────────────────────────────────
# OUTREACH — Daily 9 AM
# ─────────────────────────────────────────
0 9 * * * cd /home/ubuntu/mail &&   source venv/bin/activate &&   python pipeline.py --outreach-only   >> logs/outreach_6--.log 2>&1

# ─────────────────────────────────────────
# ATS DISCOVERY — 1st of every month at 1 AM
# build slug list → enrich company names
# ─────────────────────────────────────────
0 1 1 * * cd /home/ubuntu/mail && source venv/bin/activate && python build_ats_slug_list.py >> logs/ats_discovery_$(date +\%Y-\%m).log 2>&1 && python enrich_ats_companies.py >> logs/ats_discovery_$(date +\%Y-\%m).log 2>&1

# ─────────────────────────────────────────
# KEEP-ALIVE — every 4 days (Oracle idle protection)
# ─────────────────────────────────────────
0 12 */4 * * python3 -c "import hashlib;   [hashlib.sha256(str(i).encode()).hexdigest()   for i in range(100000)]" >> /dev/null 2>&1

# ─────────────────────────────────────────
# CLEANUP — delete backups older than 28 days
# Runs every Sunday 1:35 AM
# ─────────────────────────────────────────
35 1 * * 0 find /home/ubuntu/mail/data/backups/   -name "*.db" -mtime +28 -delete
```

### Full daily timeline
```
12:00 AM: --sync-forms
 3:00 AM: --sync-forms
 6:00 AM: --sync-forms
 9:00 AM: --sync-forms + --outreach-only
12:00 PM: --sync-forms
 3:00 PM: --sync-forms
 6:00 PM: --sync-forms
 9:00 PM: --sync-forms  ← last sync of the day (DB quiet after this)

Monday only:
10:00 PM: backup → verify-only → find-only (chained)
          ← starts right after last sync

Tuesday-Sunday:
 2:00 AM: backup → find-only (chained)

 9:00 AM: --outreach-only ← emails sent with overnight fresh data
```

### Safety with && chaining
```
backup_db.py fails:
  → verify-only and find-only do NOT run
  → DB unchanged and safe
  → Email report notifies you

verify-only fails (Monday):
  → find-only does NOT run
  → Backup already completed safely
  → Investigate before next run

find-only fails:
  → Backup already completed safely
  → Outreach still runs at 9 AM with existing data
  → Email report notifies you
```

### Quick reference
```
Just applied to new jobs?     → fill Google Form (sync runs automatically)
Regular morning sending?      → --outreach-only (automated 9 AM)
Overnight processing?         → backup + --find-only (automated 2 AM)
Weekly freshness check?       → backup + --verify-only + --find-only (Mon 10 PM)
Check quota health manually?  → --quota-report
Reactivate exhausted company? → --reactivate "CompanyName"
Import prospective companies? → --import-prospects prospects.txt
Check prospective status?     → --prospects-status
Monitor jobs + send digest?   → --monitor-jobs (automated 8 AM)
View digest in terminal?      → --jobs-digest
Detect ATS for all companies? → --detect-ats --batch  (10/run, uses 4-phase detection)
```

---

## Maintenance Checklist

### Monthly
```
□ Re-run careershift/auth.py if session expired
  (session valid ~30 days)
□ Check log files for recurring errors
□ Verify DB backup files exist
□ Check Oracle Console for any warnings
□ Run DB maintenance (VACUUM + ANALYZE)
□ Run MSCK REPAIR TABLE (auto — handled by build_ats_slug_list.py)
□ Check Athena cost log: cat data/athena_costs.json
□ Check Brave quota: cat data/brave_quota.json
□ Check ATS discovery DB: python pipeline.py --monitor-status
```

**DB maintenance cron (runs 3 AM on 1st of every month):**
```bash
# Add to crontab
0 3 1 * * cd /home/ubuntu/mail &&   source venv/bin/activate &&   python -c "
import sqlite3
conn = sqlite3.connect('data/recruiter_pipeline.db')
conn.execute('VACUUM')
conn.execute('ANALYZE')
conn.close()
print('[OK] DB maintenance complete')
" >> logs/maintenance_$(date +\%Y-\%m).log 2>&1
```

What each does:
```
VACUUM:
  → Reclaims disk space from deleted/archived rows
  → Defragments DB file
  → Keeps DB size lean over time
  → Safe to run anytime (read-only during VACUUM)

ANALYZE:
  → Updates query planner statistics
  → Ensures indexes are used efficiently
  → Critical as job_postings table grows
  → Takes < 1 second on our DB size
```

### Quarterly
```
□ Review recruiter data quality
□ Check pipeline metrics (Metric 1 + Metric 2)
□ Update CareerShift credentials if changed
□ Pull latest code from GitHub: git pull origin main
```

### When things go wrong
```
Outreach not sending:
  → Check logs/outreach_YYYY-MM-DD.log
  → Verify GMAIL_APP_PASSWORD still valid
  → Check send window config (9 AM - 11 AM)

CareerShift session expired:
  → SSH into VM
  → python careershift/auth.py
  → Re-authenticate interactively

VM not responding:
  → Oracle Console → Compute → Instances
  → Check instance state
  → Reboot if needed
  → Restore from DB backup if terminated
```

---

## Database Strategy & Long-term Scalability

### SQLite is sufficient for this application

```
Single user (you):
  → SQLite designed for this use case ✓
  → No concurrent write conflicts ✓
  → All pipelines run sequentially ✓

Expected DB size over time:
  1 year:   ~50 MB
  2 years:  ~100 MB
  5 years:  ~200 MB
  → Well within SQLite comfort zone (< 1 GB)

Query performance with proper indexes:
  200,000 job_postings rows:
    Without index: ~200ms (table scan)
    With index:    ~1ms (index scan) ✓
  
  All critical queries use indexed columns:
    → status + first_seen (job monitoring)
    → content_hash (deduplication)
    → company + recruiter_status (outreach)
```

### What keeps the DB lean

```
Retention policies (automatic):
  → job descriptions cleared on expiry
  → Old dismissed jobs deleted after 30 days
  → Old outreach records deleted per retention config
  → Old AI cache deleted after 21 days
  → PDF digests deleted after 30 days

Monthly VACUUM:
  → Reclaims space from deleted rows
  → Prevents DB file from growing stale
  → Keeps file size proportional to active data
```

### When to consider migrating to PostgreSQL

```
Migrate only if:
  □ You share this with multiple users simultaneously
    (SQLite write lock becomes bottleneck)
  □ DB consistently exceeds 500 MB
    (unlikely with retention policies)
  □ You productize this for others
    (need proper multi-tenancy)
  □ Query latency becomes noticeable
    (index + VACUUM should prevent this)

Migration is straightforward when needed:
  → sqlite3 dump → PostgreSQL import
  → Well-documented process
  → All SQL in this app is standard SQL
    (no SQLite-specific syntax)
  → Estimated migration time: ~2-4 hours
```

### DB health monitoring

```
Add to weekly checklist:
  □ Check DB file size:
    ls -lh data/recruiter_pipeline.db

  □ Check largest tables:
    python -c "
    import sqlite3
    conn = sqlite3.connect('data/recruiter_pipeline.db')
    for table in ['job_postings', 'outreach', 'recruiters',
                  'applications', 'ai_cache']:
        count = conn.execute(
            f'SELECT COUNT(*) FROM {table}'
        ).fetchone()[0]
        print(f'{table}: {count} rows')
    conn.close()
    "

  □ Alert if DB > 500 MB (early warning)
```

---

## First Deployment Checklist

```
□ 1. Oracle VM created + SSH access working
□ 2. Dependencies installed (pip install -r requirements.txt)
□ 3. .env file created with all credentials
□ 4. AWS Athena table created (one-time, already done)
□ 5. Boto3 + pyathena working (python -c "import boto3, pyathena")
□ 6. prospects.txt uploaded with domains
□ 7. Import prospects:
     python pipeline.py --import-prospects prospects.txt
□ 8. Bootstrap ATS discovery:
     python build_ats_slug_list.py --backfill  (Lever one-time)
     python build_ats_slug_list.py             (all platforms)
     python enrich_ats_companies.py --test     (verify enrichment)
     python enrich_ats_companies.py            (full enrichment)
□ 9. Run ATS detection:
     python pipeline.py --detect-ats --batch
□ 10. First job monitoring run:
      python pipeline.py --monitor-jobs
□ 11. Verify digest email received
□ 12. Set up all cron jobs (Step 7)
□ 13. Verify cron running:
      crontab -l
      grep CRON /var/log/syslog | tail -20
```

---

## Backup & Recovery

### What to backup
```
Critical (must backup):
  data/recruiter_pipeline.db    → all your pipeline data
  data/careershift_session.json → CareerShift login session
  .env                          → all credentials

Safe on GitHub (no backup needed):
  All .py files
  All docs
  config.py
  requirements.txt
```

### Backup script — scripts/backup_db.py
```python
import sqlite3
import os
from datetime import datetime

SRC_DB     = "/home/ubuntu/mail/data/recruiter_pipeline.db"
BACKUP_DIR = "/home/ubuntu/mail/data/backups"

def backup():
    os.makedirs(BACKUP_DIR, exist_ok=True)
    date_str  = datetime.now().strftime("%Y%m%d_%H%M")
    dest      = os.path.join(BACKUP_DIR,
                             f"recruiter_pipeline_{date_str}.db")

    src_conn  = sqlite3.connect(SRC_DB)
    dest_conn = sqlite3.connect(dest)

    # SQLite online backup API
    # Safe even while DB is being written to
    src_conn.backup(dest_conn)

    src_conn.close()
    dest_conn.close()

    size_kb = os.path.getsize(dest) // 1024
    print(f"[OK] Backup saved: {dest} ({size_kb} KB)")

if __name__ == "__main__":
    backup()
```

**Why SQLite online backup API instead of cp:**
```
Plain cp on live SQLite DB:
  → Copies file mid-write → corrupted backup
  → NEVER use cp on a live SQLite DB

sqlite3.backup():
  → Uses SQLite built-in online backup API
  → Atomic and consistent snapshot
  → Safe while DB is being written to
  → Even if --sync-forms ran seconds before
```

### Backup cadence
```
Runs: nightly as first step in chained job
  Tue-Sun 2:00 AM:  before --find-only
  Monday 10:00 PM:  before --verify-only and --find-only

DB is always quiet at backup time:
  → --sync-forms stops at 10 PM
  → 4+ hour gap before any nightly backup
  → Zero conflict risk

Retention: 28 days (daily backups = 28 files kept)
Location:  data/backups/recruiter_pipeline_YYYYMMDD_HHMM.db
```

### Recovery procedure (VM terminated)
```
1. Create new Oracle VM (Step 2 above)
2. Install dependencies (Step 4 above)
3. Clone repo: git clone https://github.com/you/auto-email.git
4. Restore .env file (keep a local copy on your laptop)
5. Copy latest DB backup → data/recruiter_pipeline.db
6. Run: python careershift/auth.py (re-authenticate)
7. Set up cron jobs (Step 7 above)
8. Resume normal operation

Total recovery time: ~30-45 minutes
Max data loss:       24 hours (daily backup cadence)
```