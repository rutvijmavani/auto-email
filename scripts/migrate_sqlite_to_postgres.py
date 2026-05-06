#!/usr/bin/env python3
"""
scripts/migrate_sqlite_to_postgres.py — One-shot SQLite → PostgreSQL migration.

Usage (run ON the Oracle VM after PostgreSQL is installed):
    cd /home/opc/mail
    source venv/bin/activate
    python scripts/migrate_sqlite_to_postgres.py [--sqlite data/recruiter_pipeline.db]

What this does:
  1. Connects to PostgreSQL (via DATABASE_URL in .env)
  2. Runs init_db() to create the full schema
  3. For each table, reads all rows from SQLite and bulk-inserts into PostgreSQL
  4. Resets all BIGSERIAL sequences to max(id)+1 so future inserts work
  5. Prints a per-table summary

Safety guarantees:
  - Idempotent: skips tables that already have rows (use --force to overwrite)
  - Never drops or truncates existing PostgreSQL data unless --force is given
  - SQLite DB is opened read-only (no writes)
  - Any table-level error is printed and skipped (does not abort the run)

Tables migrated (in FK-safe order):
  applications, recruiters, application_recruiters, outreach,
  careershift_quota, ai_cache, jobs, model_usage, quota_alerts,
  coverage_stats, prospective_companies, custom_ats_diagnostics,
  job_postings, monitor_stats, google_api_quota, serper_quota,
  api_health, worker_scaling_events, pipeline_alerts,
  verify_filled_stats, custom_ats_inspection, seen_job_ids,
  company_poll_stats, adaptive_poll_metrics, company_config
"""

import argparse
import os
import sqlite3
import sys
from pathlib import Path

# ─────────────────────────────────────────
# ORDER matters: parent tables before child tables (FK constraints)
# ─────────────────────────────────────────
TABLE_ORDER = [
    "applications",
    "recruiters",
    "application_recruiters",
    "outreach",
    "careershift_quota",
    "ai_cache",
    "jobs",
    "model_usage",
    "quota_alerts",
    "coverage_stats",
    "prospective_companies",
    "custom_ats_diagnostics",
    "job_postings",
    "monitor_stats",
    "google_api_quota",
    "serper_quota",
    "api_health",
    "worker_scaling_events",
    "pipeline_alerts",
    "verify_filled_stats",
    "custom_ats_inspection",
    "seen_job_ids",
    "company_poll_stats",
    "adaptive_poll_metrics",
    "company_config",
]

# Tables that use BIGSERIAL primary key (need sequence reset after bulk insert)
BIGSERIAL_TABLES = {
    "applications", "recruiters", "application_recruiters", "outreach",
    "careershift_quota", "ai_cache", "quota_alerts", "coverage_stats",
    "prospective_companies", "custom_ats_diagnostics", "job_postings",
    "monitor_stats", "api_health", "worker_scaling_events",
    "pipeline_alerts", "verify_filled_stats", "custom_ats_inspection",
    "seen_job_ids", "company_poll_stats", "adaptive_poll_metrics",
    "company_config",
}

# model_usage uses (model, date) composite PK — no sequence.
# jobs uses TEXT url_hash as PK — no sequence.
# google_api_quota uses DATE as PK — no sequence.
# serper_quota uses INTEGER id DEFAULT 1 — no sequence, single row.


def get_sqlite_tables(sqlite_conn):
    """Return set of table names in the SQLite DB."""
    c = sqlite_conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
    return {row[0] for row in c.fetchall()}


def get_sqlite_columns(sqlite_conn, table):
    """Return list of column names in a SQLite table."""
    c = sqlite_conn.execute(f"PRAGMA table_info({table})")
    return [row[1] for row in c.fetchall()]


def get_pg_columns(pg_conn, table):
    """Return list of column names in a PostgreSQL table."""
    c = pg_conn.execute("""
        SELECT column_name FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = %s
        ORDER BY ordinal_position
    """, (table,))
    return [row["column_name"] for row in c.fetchall()]


def reset_sequence(pg_conn, table):
    """Reset a BIGSERIAL sequence to max(id)+1 so next INSERT gets a fresh id."""
    try:
        row = pg_conn.execute(f"SELECT MAX(id) AS m FROM {table}").fetchone()
        max_id = row["m"] if row and row["m"] is not None else 0
        if max_id > 0:
            seq_name = f"{table}_id_seq"
            pg_conn.execute(f"SELECT setval('{seq_name}', %s)", (max_id,))
            pg_conn.commit()
    except Exception as e:
        print(f"    [WARN] Could not reset sequence for {table}: {e}")
        try:
            pg_conn.rollback()
        except Exception:
            pass


def migrate_table(table, sqlite_conn, pg_conn, force=False):
    """
    Migrate one table from SQLite to PostgreSQL.
    Returns (rows_migrated, skipped_reason) tuple.
    """
    # ── Check if table exists in SQLite ──────────────────────────────────────
    sqlite_tables = get_sqlite_tables(sqlite_conn)
    if table not in sqlite_tables:
        return 0, "not in SQLite (new table — skip)"

    # ── Check if PostgreSQL table already has data ────────────────────────────
    try:
        row = pg_conn.execute(f"SELECT COUNT(*) AS cnt FROM {table}").fetchone()
        pg_count = row["cnt"] if row else 0
    except Exception as e:
        return 0, f"PG table missing or error: {e}"

    if pg_count > 0 and not force:
        return 0, f"already has {pg_count} rows (use --force to overwrite)"

    if pg_count > 0 and force:
        try:
            pg_conn.execute(f"TRUNCATE TABLE {table} RESTART IDENTITY CASCADE")
            pg_conn.commit()
        except Exception as e:
            pg_conn.rollback()
            return 0, f"TRUNCATE failed: {e}"

    # ── Compute shared column set ─────────────────────────────────────────────
    sqlite_cols = get_sqlite_columns(sqlite_conn, table)
    pg_cols     = get_pg_columns(pg_conn, table)

    # Only migrate columns that exist in BOTH schemas
    shared = [c for c in sqlite_cols if c in pg_cols]
    if not shared:
        return 0, "no shared columns"

    # ── Read from SQLite ──────────────────────────────────────────────────────
    sqlite_conn.row_factory = sqlite3.Row
    rows = sqlite_conn.execute(
        f"SELECT {', '.join(shared)} FROM {table}"
    ).fetchall()

    if not rows:
        return 0, "SQLite table is empty"

    # ── Bulk insert into PostgreSQL ───────────────────────────────────────────
    col_list    = ", ".join(shared)
    placeholders = ", ".join(["%s"] * len(shared))
    insert_sql  = (
        f"INSERT INTO {table} ({col_list}) VALUES ({placeholders}) "
        f"ON CONFLICT DO NOTHING"
    )

    inserted = 0
    BATCH = 500
    batch = []

    for row in rows:
        values = []
        for col in shared:
            val = row[col]
            # SQLite stores BLOB as bytes — PostgreSQL BYTEA also accepts bytes
            # No conversion needed; psycopg2 handles it.
            values.append(val)
        batch.append(tuple(values))

        if len(batch) >= BATCH:
            try:
                pg_conn.executemany(insert_sql, batch)
                pg_conn.commit()
                inserted += len(batch)
            except Exception as e:
                pg_conn.rollback()
                return inserted, f"batch insert error after {inserted} rows: {e}"
            batch = []

    if batch:
        try:
            pg_conn.executemany(insert_sql, batch)
            pg_conn.commit()
            inserted += len(batch)
        except Exception as e:
            pg_conn.rollback()
            return inserted, f"final batch error after {inserted} rows: {e}"

    return inserted, None


def run(sqlite_path: Path, force: bool):
    # ── Locate SQLite DB ──────────────────────────────────────────────────────
    if not sqlite_path.exists():
        print(f"[ERROR] SQLite database not found: {sqlite_path}")
        sys.exit(1)

    print(f"[INFO] SQLite source : {sqlite_path}")
    print(f"[INFO] Force mode    : {force}")

    # ── Load .env for DATABASE_URL ────────────────────────────────────────────
    from dotenv import load_dotenv
    load_dotenv()

    db_url = os.environ.get("DATABASE_URL", "postgresql://localhost/recruiter_pipeline")
    print(f"[INFO] PostgreSQL DSN: {db_url}")
    print()

    # ── Open SQLite (read-only) ───────────────────────────────────────────────
    sqlite_conn = sqlite3.connect(f"file:{sqlite_path}?mode=ro", uri=True)
    sqlite_conn.row_factory = None  # reset — we'll set per-table

    # ── Init PostgreSQL schema ────────────────────────────────────────────────
    print("[INFO] Initialising PostgreSQL schema (init_db)...")
    sys.path.insert(0, str(Path(__file__).parent.parent))
    from db.schema import init_db
    init_db()
    print("[OK]  Schema ready.")
    print()

    # ── Get PostgreSQL connection ─────────────────────────────────────────────
    from db.connection import get_conn
    pg_conn = get_conn()

    # ── Migrate tables ────────────────────────────────────────────────────────
    total_rows   = 0
    migrated_tbl = 0
    skipped_tbl  = 0

    for table in TABLE_ORDER:
        print(f"  [{table}]", end=" ", flush=True)
        count, reason = migrate_table(table, sqlite_conn, pg_conn, force=force)

        if reason and count == 0:
            print(f"SKIP — {reason}")
            skipped_tbl += 1
        elif reason:
            print(f"PARTIAL {count} rows then ERROR: {reason}")
            skipped_tbl += 1
        else:
            # Reset sequence for BIGSERIAL tables
            if table in BIGSERIAL_TABLES and count > 0:
                reset_sequence(pg_conn, table)
            print(f"OK  {count:,} rows")
            migrated_tbl += 1
            total_rows   += count

    # ── Summary ───────────────────────────────────────────────────────────────
    print()
    print("━" * 50)
    print(f"[DONE] Migrated {migrated_tbl} tables, {total_rows:,} total rows")
    print(f"       Skipped  {skipped_tbl} tables")
    print()

    pg_conn.close()
    sqlite_conn.close()

    if total_rows > 0:
        print("[OK] Migration complete. PostgreSQL is ready.")
    else:
        print("[INFO] No rows migrated (all tables were empty or already populated).")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Migrate recruiter_pipeline SQLite → PostgreSQL"
    )
    parser.add_argument(
        "--sqlite",
        default="data/recruiter_pipeline.db",
        help="Path to SQLite database file (default: data/recruiter_pipeline.db)",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Truncate and re-migrate tables that already have PostgreSQL data",
    )
    args = parser.parse_args()

    run(Path(args.sqlite), force=args.force)
