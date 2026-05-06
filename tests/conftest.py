# tests/conftest.py — Shared test utilities

import os
import gc


# ---------------------------------------------------------------------------
# Tables to truncate between tests (dependents first; CASCADE handles the rest).
# Must match the actual PostgreSQL schema in db/schema.py.
# ---------------------------------------------------------------------------
_TRUNCATE_TABLES = [
    # Dependent tables first (foreign keys into applications / recruiters)
    "application_recruiters",
    "outreach",
    # Core pipeline tables
    "applications",
    "recruiters",
    # Quota / cache tables
    "careershift_quota",
    "ai_cache",
    "jobs",
    "model_usage",
    "quota_alerts",
    # Job monitoring
    "job_postings",
    "seen_job_ids",
    "monitor_stats",
    "coverage_stats",
    "verify_filled_stats",
    # Prospective / ATS
    "prospective_companies",
    "custom_ats_diagnostics",
    "custom_ats_inspection",
    # Adaptive polling
    "company_poll_stats",
    "company_config",
    "adaptive_poll_metrics",
    "worker_scaling_events",
    # Alerts / health
    "pipeline_alerts",
    "api_health",
    # Quota singletons (low risk of interference but clear them too)
    "serper_quota",
    "google_api_quota",
]


def cleanup_db(db_path=None):
    """
    Truncate all application tables in the test PostgreSQL database so each
    test starts with a clean slate.

    The ``db_path`` argument is kept for backward compatibility with callers
    that pass a SQLite file path; it is silently ignored.

    Behaviour:
    - Connects via ``db.connection.get_conn()`` (honours DATABASE_URL).
    - Issues TRUNCATE … RESTART IDENTITY CASCADE for every table in
      ``_TRUNCATE_TABLES``.  Tables that don't exist yet are skipped.
    - Rolls back and continues if any single TRUNCATE fails (e.g. the table
      hasn't been created in this environment).
    - Never raises — cleanup failures must not mask real test failures.
    """
    try:
        from db.connection import get_conn
        conn = get_conn()
        try:
            for table in _TRUNCATE_TABLES:
                try:
                    conn.execute(
                        f"TRUNCATE TABLE {table} RESTART IDENTITY CASCADE"
                    )
                    conn.commit()
                except Exception:
                    try:
                        conn.rollback()
                    except Exception:
                        pass
        finally:
            conn.close()
    except Exception:
        pass  # Never let cleanup failures hide real test failures


# ---------------------------------------------------------------------------
# Legacy SQLite helper — kept so any remaining direct callers don't crash.
# It is a no-op when the target file doesn't exist.
# ---------------------------------------------------------------------------
def cleanup_sqlite_files(db_path):
    """Remove SQLite .db / .db-wal / .db-shm files (legacy, rarely needed)."""
    gc.collect()
    for ext in ["", "-wal", "-shm"]:
        path = db_path + ext
        if os.path.exists(path):
            try:
                os.remove(path)
            except PermissionError:
                pass
            except OSError:
                raise
