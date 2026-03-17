#!/usr/bin/env python3
"""
scripts/backup_db.py — SQLite backup to block storage (/mnt/backups)

Backs up both:
  - data/recruiter_pipeline.db
  - data/ats_discovery.db

Uses SQLite's native backup API for guaranteed consistent snapshots.
Enforces 28-day retention on /mnt/backups — deletes older backups automatically.

Called by: run_nightly.sh, run_monday.sh, run_monthly.sh
Exit codes: 0 = success, 1 = failure (stops the nightly chain)
"""

import os
import sqlite3
import sys
from datetime import datetime, timedelta
from pathlib import Path

# ─────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────
PROJECT_DIR  = Path("/home/opc/mail")
BACKUP_DIR   = Path("/mnt/backups")
RETENTION_DAYS = 7

DBS_TO_BACKUP = [
    PROJECT_DIR / "data" / "recruiter_pipeline.db",
    PROJECT_DIR / "data" / "ats_discovery.db",
]


# ─────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────

def backup_db(src: Path, dest: Path) -> None:
    """
    Copy src → dest using SQLite's backup API.
    Guarantees a consistent snapshot even if a write is in progress.
    Raises on any error.
    """
    src_conn  = sqlite3.connect(str(src))
    dest_conn = sqlite3.connect(str(dest))
    try:
        src_conn.backup(dest_conn)
    finally:
        dest_conn.close()
        src_conn.close()


def enforce_retention(backup_dir: Path, db_stem: str, retention_days: int) -> None:
    """
    Delete backups for a given DB stem older than retention_days.
    Matches files like: recruiter_pipeline_2026-03-01_01-00.db
    """
    cutoff = datetime.now() - timedelta(days=retention_days)
    pattern = f"{db_stem}_*.db"
    deleted = 0
    for f in backup_dir.glob(pattern):
        if f.stat().st_mtime < cutoff.timestamp():
            f.unlink()
            deleted += 1
            print(f"[RETENTION] Deleted old backup: {f.name}")
    if deleted == 0:
        print(f"[RETENTION] No old backups to delete for {db_stem}")


# ─────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────

def run():
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M")
    errors    = []

    # Sanity check — block storage must be mounted
    if not BACKUP_DIR.exists() or not os.path.ismount(BACKUP_DIR):
        print(f"[ERROR] Backup directory {BACKUP_DIR} is not mounted.")
        print(f"        Run: sudo mount /dev/sdb /mnt/backups")
        sys.exit(1)

    print(f"[INFO] Backup started at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"[INFO] Destination: {BACKUP_DIR}")
    print(f"[INFO] Retention: {RETENTION_DAYS} days")
    print()

    for src in DBS_TO_BACKUP:
        stem = src.stem  # e.g. "recruiter_pipeline"

        if not src.exists():
            print(f"[SKIP] {src.name} not found — skipping")
            continue

        dest_name = f"{stem}_{timestamp}.db"
        dest      = BACKUP_DIR / dest_name

        print(f"[INFO] Backing up {src.name} → {dest_name}")
        try:
            backup_db(src, dest)
            size_mb = dest.stat().st_size / (1024 * 1024)
            print(f"[OK]   {dest_name} ({size_mb:.2f} MB)")
        except Exception as e:
            print(f"[ERROR] Failed to back up {src.name}: {e}")
            errors.append(src.name)
            continue

        # Enforce retention for this DB
        enforce_retention(BACKUP_DIR, stem, RETENTION_DAYS)
        print()

    # Disk usage summary
    try:
        stat  = os.statvfs(BACKUP_DIR)
        total = stat.f_blocks * stat.f_frsize / (1024 ** 3)
        free  = stat.f_bfree  * stat.f_frsize / (1024 ** 3)
        used  = total - free
        print(f"[INFO] Block storage: {used:.2f}GB used / {total:.2f}GB total "
              f"({free:.2f}GB free)")
    except Exception:
        pass

    if errors:
        print(f"\n[ERROR] Backup failed for: {', '.join(errors)}")
        sys.exit(1)

    print(f"\n[OK] All backups complete.")


if __name__ == "__main__":
    run()