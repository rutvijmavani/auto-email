"""
db/custom_ats_diagnostics.py — Storage and retrieval for custom ATS diagnostics.

Every time custom_career.py hits an unknown pattern, auth failure,
or structure detection failure, a row is written here.

This gives you a permanent record of:
  - What broke and when
  - What the raw response looked like
  - What pattern needs to be implemented
  - The original curl for local replay

Table: custom_ats_diagnostics
Columns:
  id            — auto PK
  company       — company name
  step          — where it failed:
                  session_warm | auth_error | listing_fetch |
                  structure_detect | detail_fetch | pagination |
                  unknown_pattern
  severity      — blocked | degraded | unknown_pattern
  pattern_hint  — short machine-readable hint:
                  auth_failed_after_cookie_only
                  auth_failed_after_csrf_token
                  career_page_403
                  career_page_timeout
                  no_array_found
                  format_unknown
                  no_description_field
                  html_no_selector_match
                  pagination_loop
                  graphql_doc_id_changed
                  new_auth_header_detected
                  job_id_not_found
  raw_response  — first 5KB of raw bytes (listing) or 2KB (detail)
  notes         — human-readable explanation
  resolved      — 0=open, 1=resolved
  created_at    — timestamp
"""

from datetime import datetime
from db.connection import get_conn


# ─────────────────────────────────────────
# SEVERITY CONSTANTS
# ─────────────────────────────────────────

BLOCKED         = "blocked"          # can't fetch any jobs
DEGRADED        = "degraded"         # fetching but missing fields
UNKNOWN_PATTERN = "unknown_pattern"  # new pattern seen, needs code

# ─────────────────────────────────────────
# STEP CONSTANTS
# ─────────────────────────────────────────

STEP_SESSION_WARM     = "session_warm"
STEP_AUTH_ERROR       = "auth_error"
STEP_LISTING_FETCH    = "listing_fetch"
STEP_STRUCTURE_DETECT = "structure_detect"
STEP_DETAIL_FETCH     = "detail_fetch"
STEP_PAGINATION       = "pagination"
STEP_UNKNOWN_PATTERN  = "unknown_pattern"


# ─────────────────────────────────────────
# WRITE
# ─────────────────────────────────────────

def flag_diagnostic(company, step, severity, pattern_hint=None,
                    raw_response=None, notes=None):
    """
    Write a diagnostic row for a custom ATS failure or unknown pattern.

    Args:
        company      -- company name
        step         -- which step failed (use STEP_* constants)
        severity     -- BLOCKED / DEGRADED / UNKNOWN_PATTERN
        pattern_hint -- short machine-readable hint string
        raw_response -- raw bytes or string (truncated to 5KB)
        notes        -- human-readable explanation

    Returns inserted row id, or None on failure.
    """
    # Truncate raw response
    if isinstance(raw_response, bytes):
        try:
            raw_str = raw_response.decode("utf-8", errors="replace")
        except Exception:
            raw_str = repr(raw_response[:200])
    else:
        raw_str = str(raw_response) if raw_response else None

    # Cap at 5KB for listing, 2KB for detail
    max_len = 2048 if step == STEP_DETAIL_FETCH else 5120
    if raw_str and len(raw_str) > max_len:
        raw_str = raw_str[:max_len] + f"\n... [truncated at {max_len} chars]"

    conn = None
    try:
        conn = get_conn()
        # ON CONFLICT DO NOTHING replaces INSERT OR IGNORE (SQLite).
        # The partial unique index on (company, step, COALESCE(pattern_hint,''))
        # WHERE resolved=0 is defined in db/schema.py and backs this constraint.
        # RETURNING id replaces cursor.lastrowid (not available on psycopg2).
        cursor = conn.execute("""
            INSERT INTO custom_ats_diagnostics
              (company, step, severity, pattern_hint, raw_response, notes)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT DO NOTHING
            RETURNING id
        """, (company, step, severity, pattern_hint, raw_str, notes))
        row = cursor.fetchone()
        conn.commit()
        if row is None:
            return None
        return row["id"]
    except Exception as e:
        # Never let diagnostics crash the main pipeline
        import logging
        logging.getLogger(__name__).warning(
            "custom_ats_diagnostics: write failed for %r: %s",
            company, e
        )
        return None
    finally:
        if conn:
            conn.close()


def has_open_diagnostic(company, step=None, pattern_hint=None):
    """
    Check if an open diagnostic already exists for this company.
    Prevents duplicate rows on every daily run.

    Returns True if an unresolved diagnostic exists.
    """
    conn = None
    try:
        conn = get_conn()
        # ILIKE replaces COLLATE NOCASE (SQLite) for case-insensitive comparison.
        if step and pattern_hint:
            row = conn.execute("""
                SELECT id FROM custom_ats_diagnostics
                WHERE company ILIKE %s AND step = %s
                  AND pattern_hint = %s AND resolved = 0
                LIMIT 1
            """, (company, step, pattern_hint)).fetchone()
        elif step:
            row = conn.execute("""
                SELECT id FROM custom_ats_diagnostics
                WHERE company ILIKE %s AND step = %s AND resolved = 0
                LIMIT 1
            """, (company, step)).fetchone()
        else:
            row = conn.execute("""
                SELECT id FROM custom_ats_diagnostics
                WHERE company ILIKE %s AND resolved = 0
                LIMIT 1
            """, (company,)).fetchone()
        return row is not None
    except Exception:
        return False
    finally:
        if conn:
            conn.close()


def flag_diagnostic_once(company, step, severity, pattern_hint=None,
                         raw_response=None, notes=None):
    """
    Write diagnostic only if no open row exists for this
    company+step+pattern_hint combination.
    Prevents flooding the table on repeated daily runs.

    Race-safe: flag_diagnostic uses ON CONFLICT DO NOTHING backed by a partial
    unique index on (company, step, COALESCE(pattern_hint, '')) WHERE resolved=0,
    so concurrent calls cannot produce duplicate rows.

    Returns row_id if a new row was written, None if already exists or failed.
    """
    return flag_diagnostic(
        company, step, severity, pattern_hint, raw_response, notes
    )


# ─────────────────────────────────────────
# READ
# ─────────────────────────────────────────

def get_open_diagnostics(company=None, severity=None, limit=50):
    """
    Return open (unresolved) diagnostic rows.

    Args:
        company  -- filter by company (None = all)
        severity -- filter by severity (None = all)
        limit    -- max rows to return
    """
    conn = None
    try:
        conn = get_conn()
        conditions = ["resolved = 0"]
        params     = []

        if company:
            conditions.append("company ILIKE %s")
            params.append(company)
        if severity:
            conditions.append("severity = %s")
            params.append(severity)

        where = " AND ".join(conditions)
        rows  = conn.execute(f"""
            SELECT id, company, step, severity, pattern_hint,
                   raw_response, notes, created_at
            FROM custom_ats_diagnostics
            WHERE {where}
            ORDER BY
                CASE severity
                    WHEN 'blocked'         THEN 1
                    WHEN 'unknown_pattern' THEN 2
                    WHEN 'degraded'        THEN 3
                    ELSE 4
                END,
                created_at DESC
            LIMIT %s
        """, params + [limit]).fetchall()
        return [dict(r) for r in rows]
    except Exception:
        return []
    finally:
        if conn:
            conn.close()


def get_diagnostic_summary():
    """
    Return counts grouped by severity for --diagnostics display.
    """
    conn = None
    try:
        conn = get_conn()
        rows = conn.execute("""
            SELECT
                severity,
                COUNT(*) as count,
                COUNT(DISTINCT company) as companies
            FROM custom_ats_diagnostics
            WHERE resolved = 0
            GROUP BY severity
            ORDER BY
                CASE severity
                    WHEN 'blocked'         THEN 1
                    WHEN 'unknown_pattern' THEN 2
                    WHEN 'degraded'        THEN 3
                    ELSE 4
                END
        """).fetchall()
        return [dict(r) for r in rows]
    except Exception:
        return []
    finally:
        if conn:
            conn.close()


def get_raw_curl_for_company(company):
    """
    Return stored raw curl strings for a company.
    Used by --diagnostics to show replay instructions.
    """
    conn = None
    try:
        conn = get_conn()
        row  = conn.execute("""
            SELECT listing_curl_raw, detail_curl_raw
            FROM prospective_companies
            WHERE company ILIKE %s
        """, (company,)).fetchone()
        return dict(row) if row else {}
    except Exception:
        return {}
    finally:
        if conn:
            conn.close()


# ─────────────────────────────────────────
# RESOLVE
# ─────────────────────────────────────────

def resolve_diagnostic(diagnostic_id):
    """Mark a diagnostic row as resolved."""
    conn = None
    try:
        conn = get_conn()
        cursor = conn.execute("""
            UPDATE custom_ats_diagnostics
            SET resolved = 1, resolved_at = NOW()
            WHERE id = %s AND resolved = 0
        """, (diagnostic_id,))
        conn.commit()
        # cursor.rowcount replaces SELECT changes() (SQLite-specific).
        return cursor.rowcount > 0
    except Exception:
        return False
    finally:
        if conn:
            conn.close()


def resolve_all_for_company(company):
    """Mark all open diagnostics for a company as resolved."""
    conn = None
    try:
        conn = get_conn()
        cursor = conn.execute("""
            UPDATE custom_ats_diagnostics
            SET resolved = 1, resolved_at = NOW()
            WHERE company ILIKE %s AND resolved = 0
        """, (company,))
        conn.commit()
        return cursor.rowcount
    except Exception:
        return 0
    finally:
        if conn:
            conn.close()
