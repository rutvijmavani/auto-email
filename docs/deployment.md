# Deployment Guide

## Overview

This document covers deployment options, cloud setup, logging strategy,
and operational maintenance for the Recruiter Outreach Pipeline.

---

## Deployment Options

### Option 1 — Local Machine (current)

```text
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
```text
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

```text
Service:  Oracle Cloud Infrastructure (OCI)
Tier:     Always Free (not just 12 months)
Shape:    VM.Standard.A1.Flex (ARM/Ampere)
Specs:    4 OCPUs, 24 GB RAM, 47 GB boot storage
          (free allowance: 3,000 OCPU-hours/month)
Block:    Up to 200 GB block storage (Always Free)
Cost:     $0 forever
```

**Sign up:** https://www.oracle.com/cloud/free/

```text
Requirements:
  → Credit card for identity verification only
  → NOT charged — purely for verification
  → $0 forever on always-free resources
```

**Reliability: 99.9%** — always on, never misses a run.

---

### Option 4 — DigitalOcean Droplet ($4/month)

```text
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
```text
1. Go to https://www.oracle.com/cloud/free/
2. Sign up with email + credit card (not charged)
3. Choose home region closest to you
4. Select "Always Free" tier
```

### Step 2 — Create VM instance
```text
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

```text
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
```text
Oracle monitors free tier VMs for idle usage:
  → If CPU < 10% for 7 consecutive days
  → Oracle sends warning email
  → If still idle after warning
  → VM TERMINATED (data lost!)
```

### Why your VM won't be idle
```text
Your daily cron jobs prevent this:
  Daily  9 AM: --outreach-only → Playwright + SMTP = CPU spikes
  Weekly Mon:  --verify-only   → Playwright + CareerShift = CPU spikes
  Every 4 hours: keep-alive script = CPU activity

Playwright launching Chromium = significant CPU usage
Oracle's monitors will see regular activity
Risk of reclamation: LOW
```

### Protection measures
```text
1. Keep-alive cron (already installed by setup_cron.sh)
   → Runs every 4 hours
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

```text
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
```text
HTML email sent after every pipeline run:
  --outreach-only → Outreach Report email
  --find-only     → Find Report email
  --verify-only   → Verification Report email

Gives visibility on healthy runs.
```

### Phase 2 — File logging (implement during Oracle migration)

#### Two-layer log architecture

Every pipeline command produces **two kinds of log output**:

```text
Layer 1 — Shell wrapper logs  (stdout redirect in run_*.sh)
  Captures: shell echo messages ([CRON], [STEP N], [REDIS])
            + any Python print() output
  Written by: run_nightly.sh, run_monitor.sh, etc. via >> "$LOG_FILE"
  Examples:
    logs/nightly_YYYY-MM-DD.log
    logs/monitor_YYYY-MM-DD.log
    logs/monthly_YYYY-MM.log

Layer 2 — Python logger logs  (logger.py FileHandler)
  Captures: all logger.debug/info/warning/error/critical calls
            inside every Python module
  Written by: init_logging() in each pipeline entry point
  Examples:
    logs/monitor_YYYY-MM-DD.log    ← when --monitor-jobs calls init_logging("monitor")
    logs/pipeline_YYYY-MM-DD.log   ← catch-all alongside every command

Note: under cron the console handler is suppressed (stdout is not a TTY),
so Python logger output goes ONLY to the file handlers, not into the
shell wrapper's redirect.  Both layers capture different things.
```

#### Log files — daily commands (one file per calendar day, 14-day retention)

```text
logs/monitor_YYYY-MM-DD.log          --monitor-jobs
logs/outreach_YYYY-MM-DD.log         --outreach-only
logs/sync_YYYY-MM-DD.log             --sync-forms / --sync-prospective
logs/nightly_YYYY-MM-DD.log          nightly chain (shell layer)
logs/monday_YYYY-MM-DD.log           Monday chain (shell layer)
logs/weekly_YYYY-MM-DD.log           --weekly-summary
logs/detect_YYYY-MM-DD.log           --detect-ats
logs/verify_filled_YYYY-MM-DD.log    --verify-filled
logs/enrich_ats_companies_YYYY-MM-DD.log  enrich_ats_companies.py --daily
logs/scheduler_YYYY-MM-DD.log        scheduler worker (long-running)
logs/pipeline_YYYY-MM-DD.log         catch-all — every command writes here
```

#### Log files — monthly commands (one file per calendar month, 35-day retention)

```text
logs/monthly_YYYY-MM.log             run_monthly.sh chain (shell layer)
logs/enrich_YYYY-MM.log              run_enrich.sh (shell layer)
logs/build_ats_slug_list_YYYY-MM.log build_ats_slug_list.py

35-day retention = 1 month + 4-day buffer so the next month's log is
always written before the previous month's is deleted.
```

#### Log levels

```text
DEBUG    → step-by-step tracing (default, verbose)
INFO     → normal operations and summaries
WARNING  → non-fatal issues (API guard fired, key missing, quota low)
ERROR    → failures (SMTP failed, API returned 5xx)
CRITICAL → pipeline cannot continue
```

#### Retention — how it works

```text
Configured in:  config.py
  LOG_RETENTION_DAILY_DAYS   = 14
  LOG_RETENTION_MONTHLY_DAYS = 35

Enforced by:  _cleanup_old_logs() in logger.py
  Runs once per process at startup (inside init_logging()).
  Uses mtime (last-written time) to classify and delete files:

  Pattern               Retention   Examples
  ────────────────────────────────────────────────────────────
  *_YYYY-MM.log         35 days     monthly_2026-05.log
  *_YYYY-MM-DD.log      14 days     monitor_2026-05-26.log
  (except build_ats_slug_list_* → 35 days, monthly-run command)
  *.log.YYYY-MM-DD      14 days     scheduler_X.log.2026-05-10
                                    (TimedRotatingFileHandler backups)
  *.log (no date)       14 days     scheduler.log, fullscan.log
  (only deleted once process stops writing — mtime ages naturally)

Shell wrapper scripts (run_*.sh) also run find … -mtime +14 -delete
for their own log files as a belt-and-suspenders measure.

No separate cron job needed — cleanup is self-contained.
```

#### Viewing logs — utils/view_logs.py

```bash
# Today's logs across all commands
python utils/view_logs.py

# Live tail today's catch-all (pipeline_YYYY-MM-DD.log)
python utils/view_logs.py --tail

# Live tail a specific command's log
python utils/view_logs.py --tail --cmd monitor

# Show only warnings and errors
python utils/view_logs.py --errors

# Filter by company name
python utils/view_logs.py --company "Accenture"

# Last 2 hours only
python utils/view_logs.py --since 2h

# Specific date
python utils/view_logs.py --date 2026-05-23

# Summary (count per log level)
python utils/view_logs.py --summary

# Quick manual tail (no viewer)
tail -f ~/mail/logs/pipeline_$(date +%Y-%m-%d).log
tail -f ~/mail/logs/monitor_$(date +%Y-%m-%d).log
```

#### Viewing logs — systemd journal (scheduler and watchdog)

The scheduler and watchdog workers are managed by systemd and log to the systemd
journal in addition to the file logs above. The journal is often the fastest way
to see what is happening right now:

```bash
# Live log stream for the scheduler (Ctrl+C to stop)
journalctl -u recruiter-scheduler -f

# Live log stream for the watchdog
journalctl -u recruiter-watchdog -f

# Last 100 lines of scheduler logs
journalctl -u recruiter-scheduler -n 100

# Last 100 lines of watchdog logs
journalctl -u recruiter-watchdog -n 100

# Logs from the last hour for both services
journalctl -u recruiter-scheduler -u recruiter-watchdog --since "1 hour ago"

# All logs since last boot
journalctl -u recruiter-scheduler -b

# Logs between two timestamps
journalctl -u recruiter-scheduler --since "2026-05-29 01:00" --until "2026-05-29 02:00"
```

The journal and the file logs capture the same Python logger output. Use the
journal for quick interactive checks and live tailing; use file logs (via
`view_logs.py`) for searching by company name, filtering by log level, or
looking at a specific past date.

#### Migration note — old pipeline.log

```text
Before this logging overhaul, a single undated pipeline.log was used
as the catch-all.  TimedRotatingFileHandler created dated backups
(pipeline.log.2026-04-26 etc.) only when a long-running process
(the scheduler) survived past midnight.

After deploying:
  → pipeline.log stops being written to
  → pipeline.log and all pipeline.log.* backups age out within 14 days
  → pipeline_YYYY-MM-DD.log (one per day) replaces it going forward

To clean up the existing backlog immediately after deploy:
  find ~/mail/logs/ -name "*_????-??-??.log"  -mtime +14 -delete
  find ~/mail/logs/ -name "*_????-??.log"     -mtime +35 -delete
  find ~/mail/logs/ -name "*.log.????-??-??"  -mtime +14 -delete
```

### Why both layers are needed
```text
Email report  = what happened (summary for healthy runs)
Shell log     = chain-level trace ([STEP 1], [STEP 2], exit codes)
Python log    = why it happened (module-level forensics for failures)

If pipeline crashes before email sends:
  → Python log captures the last thing that ran
  → Shell log shows which step failed and exit code
  → Check both to understand what went wrong
```

### Phase 3 — Optional future
```text
Structured JSON logs for easier parsing
Log aggregation if needed
```

---

## Storage & Compute Requirements

### Storage

#### Boot volume (Oracle free tier: ~47 GB)

```text
OS + Ubuntu base:              ~3.0 GB
Python packages + Chromium:    ~0.8 GB
Project code:                  ~0.005 GB
SQLite DBs (6 months):         ~0.050 GB
PDF digests (30 day retention):~0.008 GB
Log files (14/35 day retention):~0.010 GB
Athena CSV (2 day retention):  ~0.010 GB
─────────────────────────────────────────
Total used:                    ~3.9 GB
Available:                     ~43.1 GB (91% free)
```

#### Block volume (50 GB at /mnt/backups)

```text
Both DBs currently:            ~12 MB combined
7 daily backups × 12 MB:       ~85 MB
─────────────────────────────────────────
Total used:                    <0.1 GB
Available:                     ~49.9 GB (99.8% free)
```

Storage is not a concern on either volume.

### Compute (Oracle free tier: A1.Flex — 2 OCPU + 12 GB RAM)

```text
Pipeline           RAM Peak   Duration      Risk
───────────────────────────────────────────────────
--monitor-jobs     ~250 MB    20-30 min     None ✓
--verify-filled    ~80 MB     3-5 min       None ✓
--outreach-only    ~80 MB     5 min         None ✓
--sync-forms       ~50 MB     30 sec        None ✓
--find-only        ~600 MB    30-40 min     None ✓
--verify-only      ~600 MB    10-15 min     None ✓
DB backup          ~80 MB     5 sec         None ✓
build_ats_slug     ~200 MB    5 min         None ✓
```

All pipelines run sequentially — never in parallel.
Peak RAM: ~600 MB out of 12 GB available (5% usage).
No swap file needed.

---

## Recommended Deployment Schedule

### Key rules
```text
--sync-forms and --add: run only between 9 AM and 9 PM
  → Five syncs per day at 9AM 12PM 3PM 6PM 9PM
  → DB is quiet after 9 PM
  → Safe for overnight backup and nightly chain

Nightly chains use && operator:
  → Each step only runs if previous step succeeded
  → Backup always runs FIRST (clean pre-run snapshot)
  → If backup fails → nothing else runs → data safe
  → If verify-only fails (Monday) → find-only does not run
  → If find-only fails → verify-filled does not run

Guard on 1st of month:
  → run_nightly.sh and run_monday.sh exit early on the 1st
  → run_monthly.sh handles the 1st exclusively

Enrichment runs independently:
  → run_enrich.sh fires at 3 AM separately
  → Writes to ats_discovery.db only — no conflict with
    recruiter_pipeline.db nightly chain (finishes by ~2 AM)
  → Takes 18+ hours — designed as a long-running background job
```

### Wrapper scripts (created by setup_cron.sh)

```text
/home/opc/mail/
  run_sync.sh            ← --sync-forms + --sync-prospective (9AM 12PM 3PM 6PM 9PM)
  run_nightly.sh         ← sync → backup → find-only → verify-filled (Tue-Sun 1AM)
  run_monday.sh          ← sync → backup → verify-only → find-only → verify-filled (Mon 1AM)
  run_monthly.sh         ← sync → backup → find-only → build-slugs → enrich → VACUUM → verify-filled (1st 1AM)
  run_outreach.sh        ← --outreach-only (Mon-Fri 9AM)
  run_monitor.sh         ← --monitor-jobs (daily 7AM)
  run_weekly_summary.sh  ← --weekly-summary (Mon 9AM)
  run_enrich.sh          ← enrichment Phase B (daily 3AM, standalone 18hr job, skips 1st)
  run_detect.sh          ← --detect-ats --batch (disabled — run manually)
  run_verify_filled.sh   ← --verify-filled (manual use only — runs automatically in nightly chains)
```

### Complete cron schedule (installed by setup_cron.sh)

```bash
CRON_TZ=America/New_York

# ─────────────────────────────────────────
# DAYTIME SYNC — 9AM 12PM 3PM 6PM 9PM daily
# Safe standalone: read+insert only, no backup needed
# ─────────────────────────────────────────
0 9,12,15,18,21 * * * /home/opc/mail/run_sync.sh

# ─────────────────────────────────────────
# MONITOR JOBS — Daily 7 AM
# 9 AM retry guard in case VM was suspended overnight and 7 AM was missed
# ─────────────────────────────────────────
0 7 * * * /home/opc/mail/run_monitor.sh
0 9 * * * /bin/bash -c 'f=/home/opc/mail/logs/monitor_$(date +\%Y-\%m-\%d).log; [ ! -f "$f" ] && /home/opc/mail/run_monitor.sh'

# ─────────────────────────────────────────
# OUTREACH — Mon-Fri 9 AM only
# Skips Saturday and Sunday automatically
# ─────────────────────────────────────────
0 9 * * 1-5 /home/opc/mail/run_outreach.sh

# ─────────────────────────────────────────
# WEEKLY SUMMARY — Monday 9 AM
# ─────────────────────────────────────────
0 9 * * 1 /home/opc/mail/run_weekly_summary.sh

# ─────────────────────────────────────────
# NIGHTLY CHAIN — Tuesday to Sunday 1 AM
# sync → backup → find-only → verify-filled
# Guard inside script exits early on the 1st of month
# ─────────────────────────────────────────
0 1 * * 2-7 /home/opc/mail/run_nightly.sh

# ─────────────────────────────────────────
# MONDAY NIGHTLY CHAIN — Monday 1 AM
# sync → backup → verify-only → find-only → verify-filled
# Guard inside script exits early on the 1st of month
# ─────────────────────────────────────────
0 1 * * 1 /home/opc/mail/run_monday.sh

# ─────────────────────────────────────────
# MONTHLY CHAIN — 1st of every month at 1 AM
# sync → backup → find-only → build-slugs → enrich → VACUUM → verify-filled
# Replaces nightly chain on the 1st
# ─────────────────────────────────────────
0 1 1 * * /home/opc/mail/run_monthly.sh

# ─────────────────────────────────────────
# DAILY ENRICHMENT — 3 AM (standalone, 18hr background job)
# Writes to ats_discovery.db only — no conflict with nightly chain
# Skips 1st of month — run_monthly.sh handles enrichment that day
# ─────────────────────────────────────────
0 3 * * * /home/opc/mail/run_enrich.sh

# ─────────────────────────────────────────
# LOG MONITOR — every 15 minutes
# Scans log files from last byte offset, emails on new errors (Redis-deduped)
# ─────────────────────────────────────────
*/15 * * * * /home/opc/mail/venv/bin/python /home/opc/mail/scripts/log_monitor.py >> /home/opc/mail/logs/log_monitor.log 2>&1

# ─────────────────────────────────────────
# KEEP-ALIVE — every 4 hours (Oracle idle protection)
# Changed from every 4 days — closes the overnight suspension gap
# ─────────────────────────────────────────
0 */4 * * * python3 -c "import hashlib; [hashlib.sha256(str(i).encode()).hexdigest() for i in range(100000)]" >> /dev/null 2>&1

# ─────────────────────────────────────────
# DETECT ATS — currently disabled
# Uncomment in setup_cron.sh when needed:
# 30 8 * * * /home/opc/mail/run_detect.sh
# ─────────────────────────────────────────
```

**Note on `CRON_TZ=America/New_York`:** This line at the top of the crontab tells cron to interpret all times as Eastern Time regardless of the server's system timezone. Oracle Cloud VMs default to UTC — without this setting, "1 AM" would actually fire at 1 AM UTC (which is 9 PM or 10 PM Eastern depending on DST). `setup_cron.sh` sets this automatically so you never need to think about it.

### Full daily timeline (America/New_York)
```text
 7:00 AM: --monitor-jobs (tracks URL presence, increments missing counters)
 9:00 AM: --sync-forms + --sync-prospective
 9:00 AM: --outreach-only (Mon-Fri only)
 9:00 AM: --weekly-summary (Mon only)
12:00 PM: --sync-forms + --sync-prospective
 3:00 AM: enrichment Phase B (standalone, runs all day in background)
 3:00 PM: --sync-forms + --sync-prospective
 6:00 PM: --sync-forms + --sync-prospective
 9:00 PM: --sync-forms + --sync-prospective  ← last sync of the day

 1:00 AM (Tue-Sun): sync → backup → find-only → verify-filled (chained)
 1:00 AM (Mon):     sync → backup → verify-only → find-only → verify-filled (chained)
 1:00 AM (1st):     sync → backup → find-only → build-slugs → enrich → VACUUM → verify-filled

Every 15 min:  log_monitor.py (scans logs, emails on new errors, Redis-deduped)
Every 4 hours: keep-alive (Oracle idle protection — was every 4 days)
```

### Safety with && chaining
```text
backup_db.py fails:
  → verify-only, find-only, and verify-filled do NOT run
  → DB unchanged and safe

verify-only fails (Monday):
  → find-only does NOT run
  → verify-filled does NOT run
  → Backup already completed safely

find-only fails:
  → verify-filled does NOT run
  → Backup already completed safely
  → Outreach still runs at 9 AM with existing data

verify-filled fails:
  → Backup and find-only already completed safely
  → Stale jobs will be retried the next nightly run
  → No data loss — only cleanup is delayed by one day

1st of month:
  → run_nightly.sh and run_monday.sh detect date and exit early
  → run_monthly.sh takes over exclusively
```

### Quick reference
```text
Just applied to new jobs?          → fill Google Form (sync runs automatically)
Regular morning sending?           → --outreach-only (automated 9 AM)
Overnight processing?              → backup + --find-only → verify-filled (automated 1 AM)
Weekly freshness check?            → backup + --verify-only + --find-only (Mon 1 AM)
Check quota health manually?       → --quota-report
Reactivate exhausted company?      → --reactivate "CompanyName"
Import prospective companies?      → --import-prospects prospects.txt
Check prospective status?          → --prospects-status
Monitor jobs + send digest?        → --monitor-jobs (automated 7 AM)
View digest in terminal?           → --jobs-digest
Detect ATS for all companies?      → --detect-ats --batch
Send weekly summary now?           → --weekly-summary
Run verify-filled manually?        → python pipeline.py --verify-filled
Run priority enrichment?           → python enrich_ats_companies.py --priority
Run daily enrichment?              → python enrich_ats_companies.py --daily
```

---

## Backup & Recovery

### What to backup
```text
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

```text
Source DBs:
  /home/opc/mail/data/recruiter_pipeline.db
  /home/opc/mail/data/ats_discovery.db

Destination:
  /mnt/backups/recruiter_pipeline_YYYY-MM-DD_HH-MM.db
  /mnt/backups/ats_discovery_YYYY-MM-DD_HH-MM.db

Retention: 7 days (enforced automatically on every backup run)
```

**Why SQLite backup API instead of cp:**
```text
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
```text
Boot volume failure → local backups lost too
Block volume is independent storage:
  → Survives boot volume failure
  → Survives VM termination (volume persists)
  → 50 GB dedicated — no competition with OS/logs
  → Re-attachable to a new VM on recovery
```

**Retention:**
```text
Handled inside backup_db.py on every run — no separate cron needed.
7 days × 2 DBs × ~12 MB = ~170 MB total (negligible on 50 GB volume)
```

### Backup cadence
```text
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
```text
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
```text
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
□ Review verify_filled_stats for high inconclusive rates:
  SELECT date, verified, filled, active, inconclusive,
         inconclusive_timeout, inconclusive_other_status, remaining
  FROM verify_filled_stats ORDER BY date DESC LIMIT 7;
```

### Quarterly
```text
□ Review recruiter data quality
□ Check pipeline metrics (Metric 1 + Metric 2)
□ Update CareerShift credentials if changed
□ Pull latest code from GitHub: git pull origin main
□ Check block volume usage: df -h /mnt/backups
```

### When things go wrong
```text
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

Verify-filled not cleaning up stale jobs:
  → Check logs/verify_filled_YYYY-MM-DD.log
  → High inconclusive_timeout → ATS may be blocking requests
  → High inconclusive_other_status (403) → ATS bot detection triggered
  → High remaining → increase VERIFY_FILLED_BATCH_SIZE in config.py
  → Check stale job count manually:
    SELECT COUNT(*) FROM job_postings WHERE consecutive_missing_days >= 3;

Cron jobs not firing at expected times:
  → Verify CRON_TZ is set: crontab -l | head -3
  → Should show: CRON_TZ=America/New_York
  → If missing: re-run bash setup_cron.sh
  → Verify system timezone: timedatectl | grep "Time zone"

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

```text
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

```text
Retention policies (automatic):
  → job descriptions cleared on expiry or when position filled
  → filled job rows deleted after 7 days (verify-filled pipeline)
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

```text
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

```text
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

  □ Check verify-filled stats:
    python -c "
    import sqlite3
    conn = sqlite3.connect('data/recruiter_pipeline.db')
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        'SELECT date, verified, filled, active, inconclusive, remaining '
        'FROM verify_filled_stats ORDER BY date DESC LIMIT 7'
    ).fetchall()
    for r in rows:
        print(dict(r))
    conn.close()
    "

  □ Alert if DB > 500 MB (early warning)
```

---

## Scheduler Workers

The continuous adaptive scheduler runs as three long-lived background processes alongside the existing pipeline cron jobs.

### Starting the workers

```bash
# 1. Start the scheduler (adaptive + fullscan dispatch loops, runs forever)
#    rebuild_redis() runs automatically on startup — no manual pre-step needed.
python -m workers.scheduler

# 2. Start fullscan worker(s) in separate terminal(s) / processes
python -m workers.fullscan
```

### How thundering herd is prevented automatically

Thundering herd = all companies scheduled at the same time, overwhelming the fullscan worker and causing most to miss the 7 AM digest. Two independent mechanisms prevent this.

---

**Layer 1 — `rebuild_redis()` at startup**

Runs automatically every time the scheduler starts. Classifies every company against the **7 AM cycle boundary**:

| Situation | `next_poll_at` in DB | What happens |
|---|---|---|
| Fresh deploy / long outage | Days old (before last 7 AM) | **Recovery spread** — redistributes load evenly across a dynamic window based on `STARTUP_AVG_SCAN_TIME_S × company count` |
| Normal restart (brief downtime) | Within today's cycle (≥ last 7 AM) | **DB timestamps restored** — existing even distribution is preserved as-is |
| Brand new company | NULL | **Full scan first**, then `slot_offset(company_id)` bootstraps first adaptive poll at a deterministic daily slot |

No manual pre-step required. Just start the workers.

---

**Layer 2 — `_pick_schedule_time()` on every reschedule**

Every time a company is rescheduled after a scan, the new time is chosen by the gap-detection algorithm rather than `now + interval` or random jitter:

```text
window = interval × 20%  (e.g. 4.8 h on a 24 h fullscan cycle)
fetch all existing scheduled times within the window from Redis
find the largest gap between them
return the midpoint of that gap
```

The algorithm guarantees maximum separation from the nearest scheduled neighbours regardless of fleet size. Random jitter spreads companies slowly (one random step per cycle); gap-detection converges to an even distribution in 2–3 cycles even from a full cluster.

**Fullscan-specific: digest deadline guard**

For `poll:fullscan`, the algorithm also skips gap midpoints where the predicted scan duration (`avg_fullscan_duration_s` per-company EMA) would push completion past 7 AM ET. This ensures no scan is scheduled so late it cannot finish before the daily digest fires.

```text
Skip gap midpoint if: midpoint + avg_fullscan_duration_s ≥ next 7 AM ET
```

`avg_fullscan_duration_s` is updated after every successful scan using an exponential moving average (α=0.3). Default is 30 s until the first scan completes for a company.

---

**Layer 3 — `inflight:fullscan` ZSET**

When `--monitor-jobs` runs at 7 AM, it checks which companies the fullscan worker missed. Without inflight tracking, a company 20 minutes into a 30-minute scan would appear "missed" and trigger a redundant fallback HTTP fetch. The `inflight:fullscan` ZSET tracks actively-scanning companies and excludes them from the missed list.

### Worker flags

```bash
# Adaptive scheduler
python -m workers.scheduler            # run forever (production)

# Fullscan worker
python -m workers.fullscan             # run forever (production)
python -m workers.fullscan --once      # process one company then exit (smoke-test)
python -m workers.fullscan --skip-lock # bypass exclusivity lock (dev only)
```

### reschedule_on_deploy.py — manual escape hatch only

`rebuild_redis()` handles thundering herd prevention automatically on every
startup. `reschedule_on_deploy.py` is only needed in exceptional cases where
Redis scores are corrupted or clustered **while the scheduler is already
running** (so a restart is not possible or practical):

```bash
python scripts/reschedule_on_deploy.py --dry-run        # preview — no writes
python scripts/reschedule_on_deploy.py                  # both queues
python scripts/reschedule_on_deploy.py --adaptive-only  # skip poll:fullscan
python scripts/reschedule_on_deploy.py --fullscan-only  # skip poll:adaptive
```

### Troubleshooting scheduler issues

```text
Workers idle — no companies being dispatched:
  → Check poll:adaptive and poll:fullscan are populated:
    redis-cli ZCOUNT poll:adaptive -inf +inf
    redis-cli ZCOUNT poll:fullscan -inf +inf
  → If empty: run rebuild_redis() or re-import prospects
  → If all scores are far in the future: restart the scheduler —
    rebuild_redis() will reclassify and spread automatically

All companies firing at once (thundering herd on restart):
  → rebuild_redis() handles this automatically via the 7 AM cycle boundary.
    STALE companies (next_poll_at before last 7 AM) get recovery spread.
    CURRENT companies (within today's cycle) use DB timestamps directly.
  → Even without a restart, _pick_schedule_time() spreads companies on every
    reschedule — a cluster converges to even distribution within 2–3 cycles.
  → If still clustered after restart: use reschedule_on_deploy.py as a
    manual override (scores corrupted while scheduler was running).

Fullscan worker never calls bootstrap for new company:
  → Check detail queue depth (backpressure):
    redis-cli LLEN queue:detail:fullscan
  → If above DETAIL_QUEUE_MAX_FULLSCAN threshold, worker waits for drain

WARMING companies not advancing to STABLE:
  → Check warming_polls_remaining in company_poll_stats
  → WARMING decrements on each on_adaptive_complete() success
  → If stuck at 3: verify adaptive worker is running and polling the company
```

---

## Reliability Layer — Automatic Monitoring & Recovery

### What it is and why it matters

Before Phase 3, if the scheduler or a worker crashed, someone had to notice the problem, SSH into the server, and manually restart the process. This meant jobs could go unscanned for hours.

Phase 3 adds a self-managing safety net: the system monitors itself, restarts itself when something goes wrong, and emails you only when it truly cannot fix the problem on its own. In normal operation, you should not need to intervene at all.

Think of it as two layers of protection:

| Layer | What it is | How fast | What it handles |
|---|---|---|---|
| **systemd** | The OS service manager — like a supervisor who immediately calls a replacement when someone doesn't show up for work | 10–30 seconds | Process crashes — scheduler or watchdog dies unexpectedly |
| **Watchdog** | A smart monitor that checks the whole system every 5 minutes and tries to fix problems it finds | 5 minutes | Hung workers, empty queues, Redis issues, stuck jobs |

Neither layer requires any manual action on your part for day-to-day issues.

---

### The two systemd services

Two services run continuously in the background and start automatically whenever the server boots:

**`recruiter-scheduler.service`** — runs the main scheduler process. The scheduler also spawns all scan, detail, and fullscan workers as child processes. If the scheduler crashes for any reason, systemd restarts it within 30 seconds — and all workers come back up with it.

**`recruiter-watchdog.service`** — runs the watchdog monitor. If the watchdog itself crashes, systemd restarts it within 10 seconds.

There is also **`recruiter-pipeline-alert@.service`**, which is triggered automatically when a service crashes too many times in a short window (5 crashes within 5 minutes). It sends you an email with the last 30 lines of logs so you can diagnose the problem without needing to SSH in.

---

### What the watchdog monitors — and what it does about it

Every 5 minutes, the watchdog runs seven checks. For each one: what it detects, why it works that way, what alert fires, whether it fixes itself, and exactly what to do if it lands in your inbox.

---

## 1 · systemd service states

**What it checks:**

The watchdog calls `systemctl is-active recruiter-scheduler` (and `recruiter-watchdog`) as a subprocess and reads the single-word output:

- `active` → healthy, nothing to do
- `activating` → starting up, skip this cycle
- `failed` → crashed 5 times within 5 minutes; systemd gave up retrying. Service is frozen — **will not restart on its own**
- `inactive` / anything else → stopped or never started

**Why this check exists separately from heartbeats:**

A worker heartbeat key in Redis has a TTL of 15–45 seconds. If the scheduler just crashed, that key is still alive in Redis for up to 45 more seconds — during that window the heartbeat check says "healthy" while the process is dead. `systemctl is-active` reads the true OS-level state immediately, before the heartbeat key expires. It is the faster, more authoritative signal.

**Why `failed` is different from `inactive`:**

`failed` is a terminal state — systemd has stopped retrying. And `systemctl restart` is silently blocked in `failed` state (does nothing, no error). You must clear the failure counter first with `reset-failed`, then restart.

**Auto-heal:**

```bash
sudo systemctl reset-failed recruiter-scheduler
sudo systemctl restart recruiter-scheduler
```

Both commands run atomically in one `bash -c` call. `reset-failed` is safe to run unconditionally — it's a no-op if the service wasn't in `failed` state — so the watchdog doesn't need to branch on `failed` vs `inactive`.

**The watchdog's own blind spot:**

The watchdog cannot restart itself. If `recruiter-watchdog.service` enters `failed` state, systemd fires the `OnFailure=recruiter-pipeline-alert@.service` template — a one-shot unit that emails the last 30 journal lines so you can diagnose without SSH.

**Your action:**

- `✅ Auto-healed` email arrives → nothing to do
- `🆘 ESCALATION` email arrives → see Manual recovery section below

---

## 2 · Worker heartbeats

**What it checks:**

Each worker runs a background daemon thread that writes `worker:alive:{type}:{pid}` to Redis on a fixed interval, independent of what the main thread is doing:

```text
scheduler:       writes every ~1s  TTL=15s   dead after 20s
scan_worker:     writes every 10s  TTL=30s   dead after 45s
detail_worker:   writes every 10s  TTL=30s   dead after 45s
fullscan_worker: writes every 60s  TTL=180s  dead after 1,900s
```

TTL = 3× write interval, so two consecutive missed writes are tolerated before the key disappears.

**Why a daemon thread — not a loop-top write:**

Earlier versions wrote the heartbeat at the top of the main loop. A single Workday full scan taking 60–90 seconds meant the key (TTL=30s) expired mid-scan — the watchdog falsely declared the worker dead. A daemon thread writes continuously regardless of job duration.

Daemon threads are hard-tied to their process: when the process exits for any reason (crash, SIGKILL, clean shutdown), the OS terminates all daemon threads immediately. The key's TTL then expires and the watchdog correctly detects the dead worker. No ghost heartbeats from a dead process are possible.

**Two alert triggers:**

1. **Key MISSING** — TTL ran out. At least 2 consecutive writes were missed. Worker has almost certainly crashed.
2. **Key PRESENT but STALE** — key is still in Redis but the `ts` field inside is older than the threshold. Worker may be alive at the OS level but internally deadlocked (daemon thread blocked on a Redis write or lock). Process hasn't exited so TTL hasn't run, but nothing is processing.

Both trigger ERROR and the same auto-heal.

**Auto-heal:**

```bash
sudo systemctl restart recruiter-scheduler
```

All workers (scan, detail, fullscan) are child processes of the scheduler — not independent systemd units. Restarting the scheduler recreates the entire managed pool. Spawning an individual worker directly while the scheduler is alive would create an unmanaged orphan that the scheduler would duplicate on its next liveness check.

**Your action:**

- `✅ Auto-healed` → nothing to do
- Worker keeps dying (repeated alerts) → check logs: `journalctl -u recruiter-scheduler -n 100`
- `🆘 ESCALATION` → see Manual recovery section

---

## 3 · Queue depths

**What it checks:**

Four queues:
- `poll:adaptive` (ZSET) — companies scored by next-scan-due timestamp
- `poll:fullscan` (ZSET) — same, for full scans
- `queue:detail:adaptive` (LIST) — job IDs waiting for detail-page fetch
- `queue:detail:fullscan` (LIST) — same, for fullscan-discovered jobs

**Why not a simple overdue count?**

An absolute count ("more than 10 overdue") doesn't scale — 10 out of 139 companies is 7% (a problem); 10 out of 1,000 is 1% (noise). A percentage ratio scales but still can't distinguish a healthy-but-busy queue from a stalled one. A queue can have 50 companies overdue and be perfectly healthy (workers processing flat-out), or 3 overdue and be completely dead (workers crashed). A snapshot can't tell these apart.

**How it actually works — velocity tracking:**

The watchdog saves a state snapshot to Redis at the end of every cycle. On the next cycle it compares three signals:

```text
Signal 1 — Overdue count delta:      shrinking ↓ = draining, stable/growing → ↑ = problem
Signal 2 — Queue head (company+score): changed = job was picked up; same = nothing moved
Signal 3 — Worker processed count:   increased = worker completed jobs; same = worker idle
```

```text
3/3 signals stalling → ERROR   (all agree: nothing moved → auto-restart)
2/3 signals stalling → WARNING (likely stalling, watch next cycle)
0-1 signals stalling → OK      (making progress, even if running behind)
```

Requiring multiple signals prevents false alarms from natural variance — a brief Redis blip might stall one signal while the others show movement.

**Fullscan special case — the lock exoneration:**

A Workday full scan legitimately takes 20–30 minutes. During that time the processed count won't change and the queue head won't move — but the worker is healthy. The `fullscan:lock:{company}` key is written when a scan starts and cleared when it finishes. If the watchdog finds any `fullscan:lock:*` key, it suppresses all stall signals for `poll:fullscan` — the worker is provably mid-scan, not dead.

**Alert triggers and auto-heal:**

```text
poll:adaptive EMPTY  → ERROR   → auto-heal: python pipeline.py --rebuild
poll:adaptive STALL  → ERROR   → auto-heal: restart recruiter-scheduler
poll:fullscan EMPTY  → WARNING (normal briefly after rebuild, ignored if transient)
poll:fullscan STALL  → ERROR   → auto-heal: restart recruiter-scheduler
detail queue depth >500, not draining → ERROR  (no auto-heal)
detail queue depth >100, not draining → WARNING (no auto-heal)
```

Detail queues have no auto-heal because a growing detail queue usually means the workers are overloaded — restarting doesn't help if they're alive and processing. The scheduler's slow-throughput check adds workers automatically when depth stays elevated for two consecutive 30-minute cycles.

**Your action:**

- Poll queue alerts → none unless escalated (auto-heal fires first)
- Detail queue alerts → check if detail_worker is alive:
  ```bash
  redis-cli --scan --pattern "worker:alive:detail_worker:*"
  ```
  Missing or stale → `sudo systemctl restart recruiter-scheduler`
  Alive but queue growing → workers overloaded, check logs: `journalctl -u recruiter-scheduler -n 100`

---

## 4 · Stuck jobs (Stream PEL age)

**What it checks:**

`scan_worker` and `fullscan_worker` read jobs from Redis Streams (`stream:adaptive`, `stream:fullscan`) via `XREADGROUP`. When a worker reads a job, Redis places it in the **PEL** (Pending Entry List) — a per-consumer ledger of "claimed but not yet acknowledged" jobs. The job stays in the PEL until the worker calls `XACK` after completing it. A crashed worker leaves the job orphaned in the PEL.

> `detail_worker` is **not** involved here. It uses Redis LISTs with its own at-least-once mechanism: `LMOVE` atomically moves a job to an inflight list before processing; `LREM` removes it only after the DB write succeeds. If it crashes mid-job, the job stays in the inflight list and is requeued on next startup. No PEL.

**Why not just use time thresholds:**

The old approach was: oldest PEL entry >10 min = WARNING, >30 min = ERROR. This fires constantly on a healthy `fullscan_worker` mid-scan (legitimately 20–30 minutes). A time threshold alone cannot distinguish "job in progress" from "job orphaned."

**How it actually works — consumer liveness:**

Each PEL entry records the consumer name, which embeds the worker's PID (e.g. `worker-myhost-18432`). The watchdog reads the current live heartbeat PID for that worker type and compares:

```text
Consumer: worker-myhost-18432
Heartbeat PID: 18432  → same  → worker is alive, job is in progress → OK (no alarm, ever)
Heartbeat PID: 19001  → different → worker 18432 is dead → entry is orphaned
Heartbeat PID: missing  → worker is dead → entry is orphaned
```

Time thresholds only apply once the consumer is **confirmed dead**:

```text
Orphaned entry < 10 min  → OK      (XAUTOCLAIM will reclaim it shortly)
Orphaned entry > 10 min  → WARNING (XAUTOCLAIM should have caught this by now)
Orphaned entry > 30 min  → ERROR   (XAUTOCLAIM itself may be stuck)
```

**Auto-heal:** None. The scheduler's `XAUTOCLAIM` loop runs every second and automatically reclaims orphaned PEL entries, re-delivering them to the next available worker. This is fully automatic and requires no intervention.

**Your action:**

- WARNING → wait one cycle; XAUTOCLAIM will handle it
- ERROR → `sudo systemctl restart recruiter-scheduler` (XAUTOCLAIM is stuck; restarting the scheduler clears it and re-registers all consumers on the first tick)

---

## 5 · Bloom filter presence

**What it checks:**

Scans Redis for all `bloom:fullscan:*` and `bloom:fallback:*` keys. If the total count is zero, all Bloom filter state is gone.

**What Bloom filters do:**

Every completed full scan builds a Redis Bloom filter (`bloom:fullscan:{company}`) containing all job IDs seen on that company's board. On the next scan, each fetched job ID is checked against the filter first — if it's already known, the DB check is skipped entirely. Without these filters, every full scan would compare tens of thousands of job IDs against PostgreSQL on every cycle.

**What zero keys means:**

Redis was wiped — either `FLUSHALL` was called, or Redis restarted with persistence disabled (AOF/RDB was off). All deduplication state is lost.

**Auto-heal:** None. This fires as WARNING, not ERROR.

**What actually happens:**

The next full scan per company runs as a cold start. It fetches all jobs and checks each against PostgreSQL. No duplicate rows are created — the DB has a `UNIQUE` constraint on job ID. The Bloom filters rebuild automatically as each scan completes. The only cost is extra DB traffic for one scan cycle per company.

**Your action:**

Find out why Redis lost data:
- Redis restarted with no persistence → run `sudo bash deploy/configure-redis.sh` (enables AOF, saves every ~1 second — prevents this from ever happening again)
- Someone ran `FLUSHALL` → restore from last backup if needed; filters will rebuild on their own

---

## 6 · Company scan coverage

**What it checks:**

Two queries run against PostgreSQL:

```sql
-- Companies that missed a full scan in the last 26 hours
SELECT COUNT(*) FROM company_poll_stats
WHERE last_full_scan_at IS NULL
   OR last_full_scan_at < NOW() - INTERVAL '26 hours';
```

If `missed / total > 25%` → ERROR.

The window is 26 hours (not 24) to give a 2-hour buffer for companies finishing just before the 7 AM digest. The threshold is 25% (not 0%) because minor misses during ramp-up or brief ATS outages are expected and not actionable.

A second check looks for stuck detail jobs:

```sql
SELECT COUNT(*) FROM job_postings
WHERE status = 'pending_detail'
  AND created_at < NOW() - INTERVAL '1 hour';
```

More than 10 stuck rows → WARNING. These are jobs scan_worker queued but detail_worker never picked up.

**Auto-heal:** None for either check. Alert only.

**Your action:**

Coverage alert:
- Check if fullscan_worker heartbeat is alive (that check usually fires too and auto-heals)
- If alive but slow → likely a thundering-herd problem; `_pick_schedule_time()` gap algorithm (Phase 2, now implemented) spreads companies automatically over 2–3 cycles — no manual action needed
- If a specific ATS is down → scheduler puts that platform in outage mode automatically; wait for it to recover

Stuck `pending_detail` alert:
- Check detail_worker heartbeat: `redis-cli --scan --pattern "worker:alive:detail_worker:*"`
- If missing → `sudo systemctl restart recruiter-scheduler`
- If alive but queue growing → detail workers overloaded; check logs

---

## 7 · Redis persistence

**What it checks:**

```python
info = r.info("persistence")
last_save = info.get("rdb_last_save_time", 0)  # Unix epoch of last successful RDB save
age_minutes = (time.time() - last_save) / 60
```

> **Note:** `rdb_last_bgsave_time_sec` is the *duration* of the last bgsave (e.g. 3 seconds), not an epoch timestamp. Using it as an epoch produces an age of ~28 million minutes and always shows WARNING. The correct field is `rdb_last_save_time`.

If `age_minutes > 30` → WARNING.

**Why this matters:**

Redis is an in-memory database. Everything — poll queues, heartbeat keys, Bloom filters, stream entries — lives in RAM. By default Redis saves a full RDB snapshot every 5 minutes. If Redis crashes between snapshots, you lose up to 5 minutes of state. Workers would recover on next startup (rebuild_redis() restores from PostgreSQL) but you lose that window's scan results and queue state.

The watchdog fires at 30 minutes to catch a broken snapshot process (disk full, permissions issue) before the gap becomes catastrophic.

**Auto-heal:** None. This requires a one-time configuration change to the Redis server itself.

**Your action — run once, fix permanently:**

```bash
sudo bash deploy/configure-redis.sh
```

What it does:
1. Switches Redis to AOF (Append-Only File) mode — every write is appended to disk within 1 second
2. Sets `appendfsync everysec` — max data-loss window shrinks from ~5 minutes to ~1 second
3. Sets `auto-aof-rewrite-percentage 100` and `auto-aof-rewrite-min-size 64mb` — automatic AOF compaction
4. Patches `redis.conf` so settings survive a Redis restart
5. Triggers initial `BGREWRITEAOF` to compact the file immediately

After running this, the 30-minute RDB check is permanently green — AOF writes continuously so the last-save timestamp is always recent.

**AOF file size — automatic compaction:**

The AOF grows as operations are appended. Redis handles this automatically via background rewriting: when the file doubles its post-rewrite size AND is at least 64 MB, Redis forks a background process that rewrites the file to just the minimal commands needed to recreate current state. Example: 1,440 heartbeat writes per hour collapse to 4 lines. The file never grows more than ~2× the actual live data size. No manual maintenance needed.

---

### Escalation path

```text
Problem detected
  → Attempt auto-fix (if auto-healable — see above)
  → Email: "⚠ Pipeline Issue — Auto-heal Attempted (1/3)"
  → Wait 5 minutes

  → Fixed? Email: "✅ Auto-healed"                      — no action needed
  → Resolved on its own? Email: "✅ Resolved"           — no action needed
  → Still broken, attempt 2 → try again
  → Still broken, attempt 3 → try again
  → Still broken after 3 attempts?
    → Email: "🆘 ESCALATION — manual intervention required"
    → Auto-heal paused for 24 hours — you must fix manually
```

Deduplication prevents repeated emails: once an alert fires for a given issue type, it is suppressed for 1 hour so you do not receive the same email repeatedly between attempts.

The watchdog can call `systemctl restart` without a password prompt because `install-systemd.sh` adds a single narrowly-scoped rule to `/etc/sudoers.d/` — it grants exactly this one command, nothing else.

---

### Alert emails — what each one means and what to do

| Email subject | Meaning | Action needed? |
|---|---|---|
| `⚠ Pipeline Issue — Auto-heal Attempted` | Something broke, watchdog is trying to fix it | None — wait and see |
| `✅ Auto-healed` | Watchdog fixed it successfully | None |
| `✅ Resolved` | Issue resolved on its own (e.g., Redis briefly unreachable) | None |
| `🆘 ESCALATION — manual intervention required` | Watchdog tried 3 times and failed | SSH in and fix manually (see below) |
| `🆘 Pipeline FAILED: recruiter-scheduler — repeated startup crashes` | Service crashed 5 times in 5 minutes — systemd stopped trying | Check the log lines in the email body to diagnose; SSH in if needed |
| `🔴 CRITICAL: Pipeline Redis is DOWN` | Redis is unreachable — everything has stopped | Check Redis service on server: `sudo systemctl status redis` |

---

### Day-to-day health check

Run this any time to see the current state of every component in one table:

```bash
python scripts/health_check.py
```

Exit code 0 means everything is healthy. Exit code 1 means something needs attention. The output is color-coded: green = healthy, amber = warning, red = problem.

To check service status from the command line:

```bash
# Quick status of both services
sudo systemctl status recruiter-scheduler recruiter-watchdog

# Is the scheduler running?
sudo systemctl is-active recruiter-scheduler

# Is the watchdog running?
sudo systemctl is-active recruiter-watchdog
```

To watch live logs:

```bash
# Live scheduler logs (Ctrl+C to stop)
journalctl -u recruiter-scheduler -f

# Live watchdog logs
journalctl -u recruiter-watchdog -f

# Last 100 lines of scheduler logs
journalctl -u recruiter-scheduler -n 100

# Logs from the last hour
journalctl -u recruiter-scheduler --since "1 hour ago"
```

---

### Deploying new code

After every `git push`, run this one command on the server:

```bash
bash deploy/deploy.sh
```

What it does, in order:
1. `git pull` — downloads the latest code
2. `pip install -r requirements.txt` — picks up any new Python dependencies
3. `sudo systemctl restart recruiter-scheduler recruiter-watchdog` — restarts both services with the new code
4. Waits 15 seconds for worker heartbeats to appear in Redis
5. Runs `scripts/health_check.py` to confirm everything came up healthy

If `health_check.py` reports a problem, check the logs:

```bash
journalctl -u recruiter-scheduler -n 50
```

---

### June 2026 — Session fixes deploy steps

The following changes were made in this session and require specific deploy actions.
Run these **once** on the server after `git pull`.

#### What changed

| File | Change | Needs restart? |
|---|---|---|
| `workers/scheduler.py` | XAUTOCLAIM `None` msg_id guard (crash fix) | ✅ Yes |
| `db/pipeline_alerts.py` | `fromisoformat` TypeError fix | ❌ Auto on next run |
| `scripts/log_monitor.py` | New proactive log scanner (dynamic TTL) | ❌ Via cron |
| `workers/sentry_init.py` | New Sentry dedup module (dynamic TTL, no re-appear) | ✅ Yes (+ pip + .env) |
| `pipeline.py` + 5 workers | `init_sentry()` wired in at each entry point | ✅ Yes |
| Crontab | 9 AM retry guard + keep-alive every 4h + log_monitor/15m | ❌ Via update_crontab.sh |

#### Step-by-step

```bash
# ── 1. Pull latest code ───────────────────────────────────────────────────────
cd /home/opc/mail
git pull

# ── 2. Apply crontab changes (idempotent — safe to run multiple times) ────────
bash deploy/update_crontab.sh

# Verify the three new entries are present:
crontab -l | grep -E "(retry|log_monitor|keep-alive|\*/4)"

# ── 3. Install Sentry SDK (one-time) ─────────────────────────────────────────
/home/opc/mail/venv/bin/pip install sentry-sdk

# ── 4. Add SENTRY_DSN to .env (one-time — get DSN from sentry.io free account)
#       Skip this step if you haven't set up a Sentry project yet.
#       init_sentry() is a no-op when SENTRY_DSN is absent — safe to skip.
echo "SENTRY_DSN=https://xxx@oYYY.ingest.sentry.io/ZZZ" >> /home/opc/mail/.env

# ── 5. Restart services (picks up scheduler.py fix + all sentry_init wiring) ──
sudo systemctl restart recruiter-scheduler
sudo systemctl restart recruiter-watchdog

# ── 6. Verify services came back up ──────────────────────────────────────────
systemctl is-active recruiter-scheduler recruiter-watchdog
# Both should print "active"

# ── 7. Smoke test log monitor ─────────────────────────────────────────────────
/home/opc/mail/venv/bin/python /home/opc/mail/scripts/log_monitor.py
# Should print "[log_monitor] clean — no issues found" if logs are quiet

# Check that the state file was created (byte offsets per log file):
cat /home/opc/mail/data/log_monitor_state.json | python3 -m json.tool | head -20

# ── 8. Full health check ──────────────────────────────────────────────────────
/home/opc/mail/venv/bin/python scripts/health_check.py
```

#### What takes effect automatically (no action needed)

- `db/pipeline_alerts.py` fix — active on next `run_monitor.sh` execution
- `scripts/log_monitor.py` — active on next 15-minute cron tick after step 2
- Dynamic TTL in both `sentry_init.py` and `log_monitor.py` — active immediately
  after restart (step 5); Redis `ts:{fp}` keys build up over the first few
  occurrences of each error, then the TTL adapts automatically

---

### CodeRabbit Review (June 2026) — deploy steps

Security fixes, bug fixes, and architectural hardening from CodeRabbit review.
Run these **once** on the server after `git pull`.

#### What changed

| File | Change | Needs restart? |
|---|---|---|
| `workers/sentry_init.py` | Atomic `SET NX` — fixes race where two concurrent workers could both forward the same new error to Sentry | ✅ Yes |
| `scripts/health_check.py` | Silent `pass` on Redis RDB check replaced with `WARNING` row; scheduler heartbeat parse failure no longer reported as `OK` | ❌ Auto on next run |
| `scripts/log_monitor.py` | E741 variable rename (`l` → `line`) — linting fix only | ❌ Auto |
| `jobs/job_monitor.py` | `zrangebyscore` lower bound changed from `-inf` to `now − 7200` — stops stale killed-worker entries permanently excluding companies from missed-jobs check | ❌ Auto |
| `workers/startup.py` | Redis URL credentials masked before printing to stderr/journal; `SELECT COUNT(*)` startup check replaced with `SELECT 1 … LIMIT 1` (no full-table scan) | ✅ Yes |
| `workers/scan_worker.py` | `_hb.stop()` moved to after the loop — now called on all exit paths, not just `KeyboardInterrupt` | ✅ Yes |
| `workers/detail_worker.py` | **Three architectural fixes:** (1) per-PID inflight keys prevent restarting worker from stealing live peer's job; (2) atomic Lua script for shutdown requeue eliminates crash-loss window; (3) `retryable` flag leaves unexpected-error jobs in inflight for recovery | ✅ Yes |
| `workers/scheduler.py` | `_worker_spawn_times` entry cleaned up in `_shrink_pool()` — prevents unbounded dict growth | ✅ Yes |
| `workers/fullscan.py` | Updated EMA passed to `_pick_schedule_time()` — deadline guard now reflects the scan that just completed, not the stale pre-scan value | ✅ Yes |
| `workers/watchdog.py` | File handle in `_attempt_heal` wrapped in `with` block — closes on `Popen` exception too | ✅ Yes |
| `scripts/startup_failure_alert.py` | Service name validated against allowlist before use in file paths and subprocess args | ❌ Via systemd |
| `.github/workflows/deploy.yml` | `systemctl cat` check before restart — masks only "unit not found" errors, fails for real restart failures | ❌ CI only |
| `tests/` | E701/E702 linting fixes across 3 test files | ❌ Tests only |

#### Step-by-step

```bash
# ── 1. Pull latest code ───────────────────────────────────────────────────────
cd /home/opc/mail
git pull

# ── 2. Restart services ───────────────────────────────────────────────────────
# All source-code fixes take effect on restart.
sudo systemctl restart recruiter-scheduler
sudo systemctl restart recruiter-watchdog

# ── 3. Verify services came back up ──────────────────────────────────────────
systemctl is-active recruiter-scheduler recruiter-watchdog
# Both should print "active"

# ── 4. Full health check ──────────────────────────────────────────────────────
/home/opc/mail/venv/bin/python scripts/health_check.py
# Redis RDB save should now appear in report (was silently missing before)
# Scheduler heartbeat parse errors now show as WARNING instead of OK
```

#### Verifying the detail_worker per-PID inflight keys

After restart you can confirm each worker uses its own inflight key:

```bash
# List all detail inflight keys (should be one per live worker PID)
redis-cli --scan --pattern "queue:detail:*:inflight:*"

# Each key should have exactly 0 or 1 items (the currently-in-progress job)
redis-cli LLEN "queue:detail:adaptive:inflight:<pid>"
```

---

### Manual recovery (when the watchdog escalates)

If you receive a `🆘 ESCALATION` email, SSH into the server and run:

```bash
# 1. Check what state each service is in
sudo systemctl status recruiter-scheduler recruiter-watchdog

# 2. Check the last logs for the failed service
journalctl -u recruiter-scheduler -n 50

# 3. Reset the failed state and restart
sudo systemctl reset-failed recruiter-scheduler
sudo systemctl restart recruiter-scheduler

# 4. Verify it came back up
sudo systemctl is-active recruiter-scheduler

# 5. Run a full health check
python scripts/health_check.py
```

If the scheduler is up but workers are missing:

```bash
# Check worker heartbeats in Redis
redis-cli --scan --pattern "worker:alive:*"

# If queues are empty, rebuild from PostgreSQL
python pipeline.py --rebuild
```

If Redis is the problem:

```bash
sudo systemctl status redis
sudo systemctl restart redis
```

---

### Who manages what — the complete picture

| Component | How it starts | What restarts it if it crashes | Time to recovery |
|---|---|---|---|
| scheduler | systemd on boot | systemd (recruiter-scheduler.service) | ~30 seconds |
| scan_worker pool | scheduler (child process) | scheduler restart via systemd | ~35 seconds |
| detail_worker pool | scheduler (child process) | scheduler restart via systemd | ~35 seconds |
| fullscan_worker | scheduler (child process) | scheduler restart via systemd | ~35 seconds |
| watchdog | systemd on boot | systemd (recruiter-watchdog.service) | ~10 seconds |

---

### Server setup (one-time only)

The scripts in `deploy/` handle all server-side setup. Run these once when setting up a new server:

**`deploy/install-systemd.sh`** (run with `sudo`) — does the full systemd setup:
1. Removes any old watchdog cron entry to prevent double-running
2. Stops any existing nohup processes
3. Sets `.env` file permissions to 600 (read-only by the owner — keeps credentials secure)
4. Adds the sudoers rule so the watchdog can call `systemctl restart` without a password prompt
5. Installs and enables the unit files, starts both services
6. Runs `health_check.py` to confirm everything is running

**`deploy/configure-redis.sh`** (run with `sudo`) — enables Redis AOF (Append-Only File) persistence and configures automatic AOF rewriting. By default Redis saves its in-memory data to disk every 5 minutes (RDB). AOF mode appends every operation to disk within 1 second, reducing the crash data-loss window from ~5 minutes to ~1 second. AOF rewriting (auto-triggered when the file doubles in size and is ≥64 MB) compacts the file back down by collapsing all intermediate writes into just the current state — keeping file size proportional to live data rather than write history.

---

## First Deployment Checklist

```text
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
□ 14. Verify crontab has CRON_TZ and correct schedule:
       crontab -l | head -5
       grep CRON /var/log/syslog | tail -20
□ 15. Test backup manually:
       python scripts/backup_db.py
       ls -lh /mnt/backups/
□ 16. Test verify-filled manually (after first monitor run):
       python pipeline.py --verify-filled
□ 17. Run one-time server setup (systemd services + Redis AOF persistence):
       sudo bash deploy/first_time_setup.sh
       # This is the only command you need to run once. It does, in order:
       #   1. Checks all prerequisites (venv, .env, Redis, PostgreSQL)
       #   2. Installs recruiter-scheduler + recruiter-watchdog as systemd services
       #      (starts on boot, auto-restarts on crash)
       #   3. Adds the sudoers rule so the watchdog can self-heal
       #   4. Enables Redis AOF persistence (max 1s data loss, was ~5 min)
       #   5. Configures AOF auto-rewrite (file never grows unbounded)
       #   6. Waits for all worker heartbeats to appear
       #   7. Runs health_check.py to confirm everything is healthy
       #
       # After this script completes successfully, use deploy/deploy.sh
       # for every subsequent code update — never run first_time_setup.sh again.
□ 18. (Emergency only) Redistribute ZSET scores — SKIP on normal deploys.
       # Use ONLY if scores are corrupted while the scheduler is already
       # running and a restart is not possible. rebuild_redis() handles all
       # normal cases (fresh deploy, long outage, brief restart) automatically.
       python scripts/reschedule_on_deploy.py --dry-run  # preview
       python scripts/reschedule_on_deploy.py            # apply
```
