#!/usr/bin/env python3
"""
scripts/backup_db.py — PostgreSQL backup to block storage (/mnt/backups)

Uses pg_dump to create compressed SQL dumps.
Enforces 7-day retention on /mnt/backups — deletes older backups automatically.

Called by: run_nightly.sh, run_monday.sh, run_monthly.sh
Exit codes: 0 = success, 1 = failure (stops the nightly chain)

Legacy SQLite backup (data/ats_discovery.db) is still backed up for reference
until ats_discovery data is fully migrated or no longer needed.
"""

import os
import sqlite3
import subprocess
import sys
from datetime import datetime, timedelta
from pathlib import Path

# ─────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────
PROJECT_DIR    = Path("/home/opc/mail")
BACKUP_DIR     = Path("/mnt/backups")
RETENTION_DAYS = 7

# PostgreSQL connection params (read from .env / environment)
PG_DB   = os.environ.get("PG_DB",   "recruiter_pipeline")
PG_USER = os.environ.get("PG_USER", "pipeline_user")
PG_HOST = os.environ.get("PG_HOST", "localhost")
PG_PORT = os.environ.get("PG_PORT", "5432")

# Legacy SQLite DBs still on disk (keep backing up until fully retired)
SQLITE_DBS = [
    PROJECT_DIR / "data" / "ats_discovery.db",
]


# ─────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────

def backup_postgres(dest: Path) -> None:
    """
    Dump PostgreSQL database to a gzip-compressed SQL file using pg_dump.
    Uses PGPASSWORD env var to pass the password without a .pgpass file.
    """
    from dotenv import load_dotenv
    load_dotenv(PROJECT_DIR / ".env")

    # Parse DATABASE_URL for password if set
    db_url = os.environ.get("DATABASE_URL", "")
    pg_pass = ""
    if ":" in db_url and "@" in db_url:
        # postgresql://user:pass@host/db
        try:
            userinfo = db_url.split("//")[1].split("@")[0]
            pg_pass  = userinfo.split(":")[1] if ":" in userinfo else ""
        except Exception:
            pass

    env = os.environ.copy()
    env["PGPASSWORD"] = pg_pass

    cmd = [
        "pg_dump",
        "-h", PG_HOST,
        "-p", PG_PORT,
        "-U", PG_USER,
        "-Fc",            # custom format (compressed, parallel-restore capable)
        "-f", str(dest),
        PG_DB,
    ]
    result = subprocess.run(cmd, env=env, capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(f"pg_dump failed: {result.stderr.strip()}")


def backup_sqlite(src: Path, dest: Path) -> None:
    """
    Copy src → dest using SQLite's backup API.
    Guarantees a consistent snapshot even if a write is in progress.
    """
    src_conn  = sqlite3.connect(str(src))
    dest_conn = sqlite3.connect(str(dest))
    try:
        src_conn.backup(dest_conn)
    finally:
        dest_conn.close()
        src_conn.close()


def enforce_retention(backup_dir: Path, stem: str, retention_days: int,
                      suffix: str = ".dump") -> None:
    """
    Delete backups for a given stem older than retention_days.
    Matches: {stem}_YYYY-MM-DD_HH-MM{suffix}
    """
    cutoff  = datetime.now() - timedelta(days=retention_days)
    pattern = f"{stem}_*{suffix}"
    deleted = 0
    for f in backup_dir.glob(pattern):
        if f.stat().st_mtime < cutoff.timestamp():
            f.unlink()
            deleted += 1
            print(f"[RETENTION] Deleted old backup: {f.name}")
    if deleted == 0:
        print(f"[RETENTION] No old backups to delete for {stem}")


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

    # ── PostgreSQL backup ──────────────────────────────────────────────────
    pg_dest = BACKUP_DIR / f"recruiter_pipeline_{timestamp}.dump"
    print(f"[INFO] Backing up PostgreSQL '{PG_DB}' → {pg_dest.name}")
    try:
        backup_postgres(pg_dest)
        size_mb = pg_dest.stat().st_size / (1024 * 1024)
        print(f"[OK]   {pg_dest.name} ({size_mb:.2f} MB)")
    except Exception as e:
        print(f"[ERROR] PostgreSQL backup failed: {e}")
        errors.append(PG_DB)

    enforce_retention(BACKUP_DIR, "recruiter_pipeline", RETENTION_DAYS, ".dump")
    print()

    # ── Legacy SQLite backups ──────────────────────────────────────────────
    for src in SQLITE_DBS:
        stem = src.stem
        if not src.exists():
            print(f"[SKIP] {src.name} not found — skipping")
            continue

        dest_name = f"{stem}_{timestamp}.db"
        dest      = BACKUP_DIR / dest_name
        print(f"[INFO] Backing up {src.name} → {dest_name}")
        try:
            backup_sqlite(src, dest)
            size_mb = dest.stat().st_size / (1024 * 1024)
            print(f"[OK]   {dest_name} ({size_mb:.2f} MB)")
        except Exception as e:
            print(f"[ERROR] Failed to back up {src.name}: {e}")
            errors.append(src.name)
            continue

        enforce_retention(BACKUP_DIR, stem, RETENTION_DAYS, ".db")
        print()

    # ── Disk usage summary ─────────────────────────────────────────────────
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
