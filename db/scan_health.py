# db/scan_health.py — CRUD for the scan_health table (one completeness verdict per company fetch)
#
# The verdict itself is built by jobs/ats/fetch_report.py (FetchReport); this module only persists it.
# Write pattern: synchronous, retry-once, NEVER raises — a failed health write must not fail a scan
# (same pattern as db/external_api_health.py::record_external_request).

from logger import get_logger
from db import company_health
from db.connection import get_conn

logger = get_logger(__name__)

SOURCE_SCAN     = "scan"       # workers/scan_worker.py
SOURCE_FULLSCAN = "fullscan"   # workers/fullscan.py
SOURCE_MONITOR  = "monitor"    # jobs/job_monitor.py


def record_scan(company_key, platform, source, report, raw_count=0, valid_count=0,
                matched_count=None, duration_ms=0, error=None, feed_status=None):
    """
    Persist one fetch's completeness verdict. `report` is a FetchReport (jobs/ats/fetch_report.py).
    Returns True when written, False when the write failed (logged, never raised).
    """
    row = (
        str(company_key), platform, source, report.stop_reason, report.complete,
        bool(report.server_short), report.pages, report.fetched_distinct, report.unusable,
        report.reported_total, report.total_source,
        raw_count, valid_count, matched_count, int(duration_ms or 0),
        (str(error)[:500] if error else None), (report.note or None), feed_status,
    )

    def _do_write():
        conn = get_conn()
        try:
            conn.execute("""
                INSERT INTO scan_health (
                    company_key, platform, source, stop_reason, complete,
                    server_short, pages, fetched_distinct, unusable,
                    reported_total, total_source,
                    raw_count, valid_count, matched_count, duration_ms,
                    error, note, feed_status
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, row)
            conn.commit()
        except Exception:
            try:
                conn.rollback()
            except Exception:
                pass
            raise
        finally:
            conn.close()

    written = False
    for attempt in range(2):
        try:
            _do_write()
            written = True
            break
        except Exception as exc:
            if attempt == 0:
                continue   # transient connection loss — retry once
            logger.warning("scan_health: record_scan failed (company=%r source=%r): %s",
                           company_key, source, exc)
    if not written:
        return False
    if source == SOURCE_FULLSCAN:
        company_health.refresh(company_key)   # only fullscan rows feed the health verdict; never raises
    return True


def annotate_latest(company_key, source, feed_status):
    """Attach a Jobvite feed-cross-check verdict to the company's most recent row from `source`."""
    conn = get_conn()
    try:
        conn.execute("""
            UPDATE scan_health SET feed_status = ?
            WHERE id = (
                SELECT id FROM scan_health
                WHERE company_key = ? AND source = ?
                ORDER BY scanned_at DESC, id DESC LIMIT 1
            )
        """, (feed_status, str(company_key), source))
        conn.commit()
    except Exception as exc:
        try:
            conn.rollback()
        except Exception:
            pass
        logger.warning("scan_health: annotate_latest failed (company=%r): %s", company_key, exc)
    finally:
        conn.close()


def get_recent(company_key, limit=20):
    """Latest scan_health rows for one company, newest first."""
    conn = get_conn()
    try:
        rows = conn.execute("""
            SELECT * FROM scan_health WHERE company_key = ?
            ORDER BY scanned_at DESC, id DESC LIMIT ?
        """, (str(company_key), limit)).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()
