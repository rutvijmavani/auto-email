"""
db/custom_ats_inspection.py — Inspection storage for custom ATS companies.

One row per company. Overwritten on every successful page-1 fetch.
Gives you a permanent window into exactly what the API returned,
what was auto-detected, and lets you override field_map manually
without touching ats_slug or re-running --sync-prospective.

Columns:
  company              — company name (UNIQUE — one row per company)
  listing_url          — the API endpoint used
  format               — detected response format (json/html/graphql/xml...)
  array_path           — dot-path where jobs array was found (e.g. "jobs")
  total_jobs           — total job count used for pagination
  total_field          — which field we picked for total ("hits", "count"...)
  all_numeric_fields   — JSON of ALL root-level numeric fields for verification
  page_size            — effective page size used
  pagination           — full pagination config JSON
  session_strategy     — auth strategy used (cookie_only/csrf_token/bearer...)
  first_job_raw        — full raw first job dict as returned by API (JSON)
  field_map            — auto-detected field map (JSON)
  field_map_override   — manual override (JSON) — NULL = use auto-detected
                         Set this to fix wrong field_map without re-syncing.
  sample_normalized    — first job after normalization (JSON) for quick check
  last_updated         — when this row was last written
"""

import json
from datetime import datetime
from db.connection import get_conn



# ─────────────────────────────────────────
# WRITE
# ─────────────────────────────────────────

def save_inspection(company, listing_url, fmt, array_path,
                    total_jobs, total_field, all_numeric_fields,
                    page_size, pagination, session_strategy,
                    first_job_raw, field_map, sample_normalized):
    """
    Upsert inspection row for a company.
    Called after page 1 fetch + structure detection in custom_career.py.

    Does NOT overwrite field_map_override — manual overrides persist
    across runs until explicitly cleared.

    Args:
        company             — company name
        listing_url         — API endpoint URL
        fmt                 — response format string
        array_path          — dot-path to jobs array
        total_jobs          — total job count from API
        total_field         — field name used for total
        all_numeric_fields  — dict of all root-level numeric fields
        page_size           — effective page size
        pagination          — pagination config dict
        session_strategy    — auth strategy string
        first_job_raw       — raw first job dict
        field_map           — detected field map dict
        sample_normalized   — normalized first job dict

    Returns True on success, False on failure.
    """
    conn = None
    try:
        conn = get_conn()

        # Serialize dicts/lists to JSON strings
        def _j(v):
            if v is None:
                return None
            if isinstance(v, str):
                return v
            return json.dumps(v, ensure_ascii=False, default=str)

        # field_map_override is intentionally NOT listed in DO UPDATE SET —
        # manual overrides persist across runs until explicitly cleared.
        conn.execute("""
            INSERT INTO custom_ats_inspection
                (company, listing_url, format, array_path,
                 total_jobs, total_field, all_numeric_fields,
                 page_size, pagination, session_strategy,
                 first_job_raw, field_map, sample_normalized,
                 last_updated)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(company) DO UPDATE SET
                listing_url        = EXCLUDED.listing_url,
                format             = EXCLUDED.format,
                array_path         = EXCLUDED.array_path,
                total_jobs         = EXCLUDED.total_jobs,
                total_field        = EXCLUDED.total_field,
                all_numeric_fields = EXCLUDED.all_numeric_fields,
                page_size          = EXCLUDED.page_size,
                pagination         = EXCLUDED.pagination,
                session_strategy   = EXCLUDED.session_strategy,
                first_job_raw      = EXCLUDED.first_job_raw,
                field_map          = EXCLUDED.field_map,
                sample_normalized  = EXCLUDED.sample_normalized,
                last_updated       = EXCLUDED.last_updated
        """, (
            company,
            listing_url,
            fmt,
            array_path,
            total_jobs,
            total_field,
            _j(all_numeric_fields),
            page_size,
            _j(pagination),
            session_strategy,
            _j(first_job_raw),
            _j(field_map),
            _j(sample_normalized),
            datetime.utcnow().isoformat(),
        ))
        conn.commit()
        return True
    except Exception as e:
        import logging
        logging.getLogger(__name__).warning(
            "custom_ats_inspection: save failed for %r: %s", company, e
        )
        return False
    finally:
        if conn:
            conn.close()


# ─────────────────────────────────────────
# READ
# ─────────────────────────────────────────

def get_inspection(company):
    """
    Return inspection row for a company.
    Returns dict or None if not found.
    """
    conn = None
    try:
        conn = get_conn()
        row  = conn.execute("""
            SELECT * FROM custom_ats_inspection
            WHERE company = ?
        """, (company,)).fetchone()
        return dict(row) if row else None
    except Exception:
        return None
    finally:
        if conn:
            conn.close()


def get_field_map_override(company):
    """
    Return the manual field_map_override for a company if set.
    Returns parsed dict or None.
    Called by custom_career.py before using auto-detected field_map.
    """
    conn = None
    try:
        conn = get_conn()
        row  = conn.execute("""
            SELECT field_map_override
            FROM custom_ats_inspection
            WHERE company = ?
        """, (company,)).fetchone()
        if not row or not row["field_map_override"]:
            return None
        return json.loads(row["field_map_override"])
    except Exception:
        return None
    finally:
        if conn:
            conn.close()


def get_all_inspections():
    """
    Return all inspection rows ordered by last_updated DESC.
    Used by --diagnostics / --monitor-status for overview.
    """
    conn = None
    try:
        conn = get_conn()
        rows = conn.execute("""
            SELECT company, listing_url, format, array_path,
                   total_jobs, total_field, page_size,
                   session_strategy, field_map, field_map_override,
                   last_updated
            FROM custom_ats_inspection
            ORDER BY last_updated DESC
        """).fetchall()
        return [dict(r) for r in rows]
    except Exception:
        return []
    finally:
        if conn:
            conn.close()


# ─────────────────────────────────────────
# OVERRIDE MANAGEMENT
# ─────────────────────────────────────────

def set_field_map_override(company, field_map):
    """
    Set a manual field_map override for a company.

    Example:
        set_field_map_override("Amazon", {
            "title":    "title",
            "job_url":  "job_path",
            "location": "location",
            "posted_at":"posted_date",
            "job_id":   "id_icims",
        })

    Returns True on success, False on failure.
    """
    conn = None
    try:
        conn = get_conn()
        # cursor.rowcount replaces SELECT changes() (SQLite-specific).
        cursor = conn.execute("""
            UPDATE custom_ats_inspection
            SET field_map_override = ?
            WHERE company = ?
        """, (json.dumps(field_map), company))
        conn.commit()
        if cursor.rowcount == 0:
            import logging
            logging.getLogger(__name__).warning(
                "custom_ats_inspection: no row found for %r — "
                "run --monitor-jobs first to create inspection row",
                company
            )
            return False
        return True
    except Exception as e:
        import logging
        logging.getLogger(__name__).warning(
            "custom_ats_inspection: set_override failed for %r: %s",
            company, e
        )
        return False
    finally:
        if conn:
            conn.close()


def clear_field_map_override(company):
    """
    Clear manual field_map override — revert to auto-detected.
    Returns True on success.
    """
    conn = None
    try:
        conn = get_conn()
        conn.execute("""
            UPDATE custom_ats_inspection
            SET field_map_override = NULL
            WHERE company = ?
        """, (company,))
        conn.commit()
        return True
    except Exception:
        return False
    finally:
        if conn:
            conn.close()

