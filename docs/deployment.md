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
Specs:    4 OCPUs, 24 GB RAM, 47 GB boot storage
          (free allowance: 3,000 OCPU-hours/month)
Block:    Up to 200 GB block storage (Always Free)
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
mkdir -p /home/opc/mail
cd /home/opc/mail

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
nano /home/opc/mail/.env

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

# Create data and log directories
mkdir -p /home/opc/mail/data
mkdir -p /home/opc/mail/logs
mkdir -p /home/opc/mail/scripts
```

### Step 6 — Attach and mount block storage (50 GB)

Oracle Always Free includes up to 200 GB of block storage. We use a 50 GB volume
mounted at `/mnt/backups` exclusively for DB backups.

```
OCI Console → Storage → Block Volumes → Create Block Volume
  Name:        pipeline-backups
  Size:        50 GB
  Compartment: same as your VM

OCI Console → Compute → Instances → your VM
  → Attached Block Volumes → Attach Volume
  → Select pipeline-backups
  → Access: Read/Write
  → Note the device path shown (typically /dev/sdb)
```

Then on the VM (run once only — formatting destroys existing data):

```bash
# Confirm device name matches what OCI console showed
lsblk

# Format (one-time only)
sudo mkfs.ext4 /dev/sdb

# Create mount point
sudo mkdir -p /mnt/backups

# Mount
sudo mount /dev/sdb /mnt/backups

# Persist across reboots
echo "/dev/sdb /mnt/backups ext4 defaults,_netdev 0 2" | sudo tee -a /etc/fstab

# Set permissions
sudo chown opc:opc /mnt/backups

# Verify
df -h /mnt/backups
```

### Step 7 — CareerShift session
```bash
# Run auth once interactively to create session file
cd /home/opc/mail
source venv/bin/activate
python careershift/auth.py

# Session saved to: data/careershift_session.json
# Valid for ~30 days — re-run auth.py when it expires
```

### Step 8 — Set up cron jobs
```bash
cd /home/opc/mail
chmod +x setup_cron.sh && ./setup_cron.sh
```

This creates all wrapper scripts and installs the full crontab automatically.
See the **Recommended Deployment Schedule** section for the complete timeline.

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
1. Keep-alive cron (already installed by setup_cron.sh)
   → Runs every 4 days at 12 PM
   → Generates CPU activity
   → Belt-and-suspenders protection

2. Enable Oracle email notifications
   Oracle Console → Account → Preferences → Notifications
   → Get warned before any termination
   → Gives you time to react

3. Nightly DB backup to block storage (already configured)
   → Even if VM terminated → data recoverable from /mnt/backups
   → 7 daily backups retained (both DBs)

4. Code always on GitHub (you're already doing this ✓)
   → VM terminated → git clone → 30 min to restore
   → Only SQLite DBs need separate backup
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
  logs/nightly_YYYY-MM-DD.log
  logs/monday_YYYY-MM-DD.log
  logs/monthly_YYYY-MM.log
  logs/outreach_YYYY-MM-DD.log
  logs/monitor_YYYY-MM-DD.log
  logs/sync_YYYY-MM-DD.log
  logs/weekly_YYYY-MM-DD.log
  logs/enrich_YYYY-MM.log
  logs/detect_YYYY-MM-DD.log

Log levels:
  INFO     → normal operations
  WARNING  → non-fatal issues (quota low, JD missing)
  ERROR    → failures (SMTP failed, session expired)
  CRITICAL → pipeline cannot continue

Retention: 14 days (auto-delete older files, per wrapper scripts)
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

### Storage

#### Boot volume (Oracle free tier: ~47 GB)

```
OS + Ubuntu base:              ~3.0 GB
Python packages + Chromium:    ~0.8 GB
Project code:                  ~0.005 GB
SQLite DBs (6 months):         ~0.050 GB
PDF digests (30 day retention):~0.008 GB
Log files (14 day retention):  ~0.010 GB
Athena CSV (2 day retention):  ~0.010 GB
─────────────────────────────────────────
Total used:                    ~3.9 GB
Available:                     ~43.1 GB (91% free)
```

#### Block volume (50 GB at /mnt/backups)

```
Both DBs currently:            ~12 MB combined
7 daily backups × 12 MB:       ~85 MB
─────────────────────────────────────────
Total used:                    <0.1 GB
Available:                     ~49.9 GB (99.8% free)
```

Storage is not a concern on either volume.

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

---

## Recommended Deployment Schedule

### Key rules
```
--sync-forms and --add: run only between 9 AM and 9 PM
  → Five syncs per day at 9AM 12PM 3PM 6PM 9PM
  → DB is quiet after 9 PM
  → Safe for overnight backup and nightly chain

Nightly chains use && operator:
  → Each step only runs if previous step succeeded
  → Backup always runs FIRST (clean pre-run snapshot)
  → If backup fails → nothing else runs → data safe
  → If verify-only fails (Monday) → find-only does not run

Guard on 1st of month:
  → run_nightly.sh and run_monday.sh exit early on the 1st
  → run_monthly.sh handles the 1st exclusively
```

### Wrapper scripts (created by setup_cron.sh)

```
/home/opc/mail/
  run_sync.sh            ← --sync-forms + --sync-prospective (9AM 12PM 3PM 6PM 9PM)
  run_nightly.sh         ← sync → backup → find-only (Tue-Sun 1AM)
  run_monday.sh          ← sync → backup → verify-only → find-only (Mon 1AM)
  run_monthly.sh         ← sync → backup → find-only → build-slugs → enrich → VACUUM (1st 1AM)
  run_outreach.sh        ← --outreach-only (Mon-Fri 9AM)
  run_monitor.sh         ← --monitor-jobs (daily 7AM)
  run_weekly_summary.sh  ← --weekly-summary (Mon 9AM)
  run_enrich.sh          ← enrichment Phase B (daily 3AM, skips 1st)
  run_detect.sh          ← --detect-ats --batch (disabled — run manually)
```

### Complete cron schedule (installed by setup_cron.sh)

```bash
# ─────────────────────────────────────────
# DAYTIME SYNC — 9AM 12PM 3PM 6PM 9PM daily
# Safe standalone: read+insert only, no backup needed
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
# sync → backup → find-only
# Guard inside script exits early on the 1st of month
# ─────────────────────────────────────────
0 1 * * 2-7 /home/opc/mail/run_nightly.sh

# ─────────────────────────────────────────
# MONDAY NIGHTLY CHAIN — Monday 1 AM
# sync → backup → verify-only → find-only
# Guard inside script exits early on the 1st of month
# ─────────────────────────────────────────
0 1 * * 1 /home/opc/mail/run_monday.sh

# ─────────────────────────────────────────
# MONTHLY CHAIN — 1st of every month at 1 AM
# sync → backup → find-only → build-slugs → enrich → VACUUM
# ─────────────────────────────────────────
0 1 1 * * /home/opc/mail/run_monthly.sh

# ─────────────────────────────────────────
# DAILY ENRICHMENT — 3 AM (skips 1st — run_monthly.sh handles it)
# ─────────────────────────────────────────
0 3 * * * /home/opc/mail/run_enrich.sh

# ─────────────────────────────────────────
# KEEP-ALIVE — every 4 days (Oracle idle protection)
# ─────────────────────────────────────────
0 12 */4 * * python3 -c "import hashlib; [hashlib.sha256(str(i).encode()).hexdigest() for i in range(100000)]" >> /dev/null 2>&1

# ─────────────────────────────────────────
# DETECT ATS — currently disabled
# Uncomment in setup_cron.sh when needed:
# 30 8 * * * /home/opc/mail/run_detect.sh
# ─────────────────────────────────────────
```

### Full daily timeline
```
 7:00 AM: --monitor-jobs
 9:00 AM: --sync-forms + --sync-prospective
 9:00 AM: --outreach-only (Mon-Fri only)
 9:00 AM: --weekly-summary (Mon only)
12:00 PM: --sync-forms + --sync-prospective
 3:00 AM: enrichment Phase B
 3:00 PM: --sync-forms + --sync-prospective
 6:00 PM: --sync-forms + --sync-prospective
 9:00 PM: --sync-forms + --sync-prospective  ← last sync of the day

 1:00 AM (Tue-Sun): sync → backup → find-only (chained)
 1:00 AM (Mon):     sync → backup → verify-only → find-only (chained)
 1:00 AM (1st):     sync → backup → find-only → build-slugs → enrich → VACUUM
```

### Safety with && chaining
```
backup_db.py fails:
  → verify-only and find-only do NOT run
  → DB unchanged and safe

verify-only fails (Monday):
  → find-only does NOT run
  → Backup already completed safely

find-only fails:
  → Backup already completed safely
  → Outreach still runs at 9 AM with existing data

1st of month:
  → run_nightly.sh and run_monday.sh detect date and exit early
  → run_monthly.sh takes over exclusively
```

### Quick reference
```
Just applied to new jobs?     → fill Google Form (sync runs automatically)
Regular morning sending?      → --outreach-only (automated 9 AM)
Overnight processing?         → backup + --find-only (automated 1 AM)
Weekly freshness check?       → backup + --verify-only + --find-only (Mon 1 AM)
Check quota health manually?  → --quota-report
Reactivate exhausted company? → --reactivate "CompanyName"
Import prospective companies? → --import-prospects prospects.txt
Check prospective status?     → --prospects-status
Monitor jobs + send digest?   → --monitor-jobs (automated 7 AM)
View digest in terminal?      → --jobs-digest
Detect ATS for all companies? → --detect-ats --batch
Send weekly summary now?      → --weekly-summary
Run priority enrichment?      → python enrich_ats_companies.py --priority
Run daily enrichment?         → python enrich_ats_companies.py --daily
```

---

## Backup & Recovery

### What to backup
```
Critical (must backup):
  data/recruiter_pipeline.db    → all pipeline data
  data/ats_discovery.db         → ATS enrichment data
  data/careershift_session.json → CareerShift login session
  .env                          → all credentials

Safe on GitHub (no backup needed):
  All .py files
  All docs
  config.py
  requirements.txt
```

### Backup script — scripts/backup_db.py

Both DBs are backed up using SQLite's native backup API which guarantees
a consistent snapshot even if a write is in progress.

```
Source DBs:
  /home/opc/mail/data/recruiter_pipeline.db
  /home/opc/mail/data/ats_discovery.db

Destination:
  /mnt/backups/recruiter_pipeline_YYYY-MM-DD_HH-MM.db
  /mnt/backups/ats_discovery_YYYY-MM-DD_HH-MM.db

Retention: 7 days (enforced automatically on every backup run)
```

**Why SQLite backup API instead of cp:**
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

**Why block storage instead of local data/backups/:**
```
Boot volume failure → local backups lost too
Block volume is independent storage:
  → Survives boot volume failure
  → Survives VM termination (volume persists)
  → 50 GB dedicated — no competition with OS/logs
  → Re-attachable to a new VM on recovery
```

**Retention:**
```
Handled inside backup_db.py on every run — no separate cron needed.
7 days × 2 DBs × ~12 MB = ~170 MB total (negligible on 50 GB volume)
```

### Backup cadence
```
Runs: nightly as first step in each chained job
  Mon 1:00 AM:  before --verify-only and --find-only
  Tue-Sun 1 AM: before --find-only
  1st of month: before --find-only (monthly chain)

DB is always quiet at backup time:
  → Last --sync-forms runs at 9 PM
  → 4-hour gap before any nightly backup at 1 AM
  → Zero conflict risk
```

### Recovery procedure (VM terminated)
```
1. Create new Oracle VM (Step 2 above)
2. Attach existing block volume (pipeline-backups) to new VM
   OCI Console → Storage → Block Volumes → pipeline-backups
   → Attach to new instance
3. Mount block volume:
   sudo mkdir -p /mnt/backups
   sudo mount /dev/sdb /mnt/backups
4. Install dependencies (Step 4 above)
5. Clone repo: git clone https://github.com/you/auto-email.git
6. Restore .env file (keep a local copy on your laptop)
7. Copy latest DB backup from block volume:
   cp /mnt/backups/recruiter_pipeline_<latest>.db /home/opc/mail/data/recruiter_pipeline.db
   cp /mnt/backups/ats_discovery_<latest>.db /home/opc/mail/data/ats_discovery.db
8. Run: python careershift/auth.py (re-authenticate)
9. Run: chmod +x setup_cron.sh && ./setup_cron.sh
10. Resume normal operation

Total recovery time: ~30-45 minutes
Max data loss:       24 hours (daily backup cadence)
```

---

## Maintenance Checklist

### Monthly
```
□ Re-run careershift/auth.py if session expired
  (session valid ~30 days)
□ Check log files for recurring errors
□ Verify DB backup files exist on block storage:
  ls -lh /mnt/backups/
□ Check Oracle Console for any warnings
□ VACUUM + ANALYZE runs automatically on 1st via run_monthly.sh
□ Run MSCK REPAIR TABLE (auto — handled by build_ats_slug_list.py)
□ Check Athena cost log: cat data/athena_costs.json
□ Check Brave quota: cat data/brave_quota.json
□ Check ATS discovery DB: python pipeline.py --monitor-status
```

### Quarterly
```
□ Review recruiter data quality
□ Check pipeline metrics (Metric 1 + Metric 2)
□ Update CareerShift credentials if changed
□ Pull latest code from GitHub: git pull origin main
□ Check block volume usage: df -h /mnt/backups
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

Backup failed (nightly chain stopped):
  → Check if block volume is mounted: df -h /mnt/backups
  → If not mounted: sudo mount /dev/sdb /mnt/backups
  → Re-run manually: python scripts/backup_db.py
  → Check /etc/fstab entry is correct for auto-mount on reboot

VM not responding:
  → Oracle Console → Compute → Instances
  → Check instance state
  → Reboot if needed
  → Block volume (pipeline-backups) persists independently
  → Restore from /mnt/backups after reattaching volume
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

Monthly VACUUM (run_monthly.sh — 1st of every month):
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
    ls -lh data/recruiter_pipeline.db data/ats_discovery.db

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

  □ Check backup volume usage:
    df -h /mnt/backups
    ls -lh /mnt/backups/

  □ Alert if DB > 500 MB (early warning)
```

---

## First Deployment Checklist

```
□ 1.  Oracle VM created + SSH access working
□ 2.  Block volume (50 GB) created, attached, formatted, and mounted at /mnt/backups
□ 3.  Dependencies installed (pip install -r requirements.txt)
□ 4.  .env file created with all credentials
□ 5.  AWS Athena table created (one-time, already done)
□ 6.  Boto3 + pyathena working (python -c "import boto3, pyathena")
□ 7.  prospects.txt uploaded with domains
□ 8.  Import prospects:
       python pipeline.py --import-prospects prospects.txt
□ 9.  Bootstrap ATS discovery:
       python build_ats_slug_list.py --backfill  (Lever one-time)
       python build_ats_slug_list.py             (all platforms)
       python enrich_ats_companies.py --test     (verify enrichment)
       python enrich_ats_companies.py            (full enrichment)
□ 10. Run ATS detection:
       python pipeline.py --detect-ats --batch
□ 11. First job monitoring run:
       python pipeline.py --monitor-jobs
□ 12. Verify digest email received
□ 13. Set up all cron jobs:
       chmod +x setup_cron.sh && ./setup_cron.sh
□ 14. Verify cron running:
       crontab -l
       grep CRON /var/log/syslog | tail -20
□ 15. Test backup manually:
       python scripts/backup_db.py
       ls -lh /mnt/backups/
```