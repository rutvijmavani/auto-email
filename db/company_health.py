# db/company_health.py — company_health table: the latest health verdict per company
#
# Fed only by fullscan rows of scan_health (jobs/health_verdict.py does the judging). refresh() is
# called by db/scan_health.py::record_scan after every fullscan row; it never raises.
# The daily digest reads digest_snapshot() and calls mark_reported() once the email has gone out.

from collections import defaultdict

from config import HEALTH_BASELINE_SCANS, HEALTH_MIN_HISTORY, HEALTH_MIN_PEERS
from db.connection import get_conn
from jobs.health_verdict import (
    STATE_FAILING, STATE_OK, STATE_SUSPECT, evaluate, is_good, peer_relative_spread,
)
from logger import get_logger

logger = get_logger(__name__)

_SOURCE_FULLSCAN = "fullscan"   # mirrors db.scan_health.SOURCE_FULLSCAN (not imported: avoids a cycle)


def _own_rows(conn, company_key):
    """This company's fullscan rows, newest first — enough for the baseline plus a run of bad scans."""
    rows = conn.execute("""
        SELECT platform, stop_reason, complete, fetched_distinct, reported_total, error, scanned_at
        FROM scan_health WHERE company_key = ? AND source = ?
        ORDER BY scanned_at DESC, id DESC LIMIT ?
    """, (str(company_key), _SOURCE_FULLSCAN, HEALTH_BASELINE_SCANS * 3)).fetchall()
    return [dict(r) for r in rows]


def _peer_spread(conn, company_key, platform):
    """Cold-start band from the other companies on the same platform; None when too few peers qualify."""
    if not platform:
        return None
    rows = conn.execute("""
        SELECT company_key, fetched_distinct FROM (
            SELECT company_key, fetched_distinct,
                   ROW_NUMBER() OVER (PARTITION BY company_key ORDER BY scanned_at DESC, id DESC) AS rn
            FROM scan_health
            WHERE source = ? AND platform = ? AND company_key <> ?
              AND error IS NULL AND complete IS NOT FALSE
        ) t
        WHERE rn <= ?
        ORDER BY company_key, rn
    """, (_SOURCE_FULLSCAN, platform, str(company_key), HEALTH_BASELINE_SCANS)).fetchall()
    per_company = defaultdict(list)
    for r in rows:
        per_company[r["company_key"]].append(int(r["fetched_distinct"] or 0))
    return peer_relative_spread(list(per_company.values()), HEALTH_MIN_PEERS)


def refresh(company_key):
    """Recompute and upsert this company's verdict. Returns the Verdict, or None when it failed."""
    try:
        conn = get_conn()
        try:
            rows = _own_rows(conn, company_key)
            if not rows:
                return None
            good_before_latest = sum(1 for r in rows[1:] if is_good(r))
            peer = (_peer_spread(conn, company_key, rows[0].get("platform"))
                    if good_before_latest < HEALTH_MIN_HISTORY else None)
            v = evaluate(rows, peer)
            conn.execute("""
                INSERT INTO company_health (company_key, platform, state, reason, last_scan_at,
                                            count, baseline, spread, flag_line, band_from)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (company_key) DO UPDATE SET
                    platform     = EXCLUDED.platform,
                    since        = CASE WHEN company_health.state <> EXCLUDED.state
                                        THEN CURRENT_TIMESTAMP ELSE company_health.since END,
                    expected_state = CASE WHEN company_health.expected_state = EXCLUDED.state
                                          THEN company_health.expected_state END,
                    expected_at    = CASE WHEN company_health.expected_state = EXCLUDED.state
                                          THEN company_health.expected_at END,
                    state        = EXCLUDED.state,
                    reason       = EXCLUDED.reason,
                    last_scan_at = EXCLUDED.last_scan_at,
                    count        = EXCLUDED.count,
                    baseline     = EXCLUDED.baseline,
                    spread       = EXCLUDED.spread,
                    flag_line    = EXCLUDED.flag_line,
                    band_from    = EXCLUDED.band_from
            """, (str(company_key), rows[0].get("platform"), v.state, v.reason,
                  rows[0].get("scanned_at"), v.count, v.median, v.spread, v.flag_line, v.band_from))
            conn.commit()
            return v
        except Exception:
            try:
                conn.rollback()
            except Exception:
                pass
            raise
        finally:
            conn.close()
    except Exception as exc:
        logger.warning("company_health: refresh failed (company=%r): %s", company_key, exc)
        return None


def digest_snapshot():
    """
    What the next digest should say, as dict of lists of row dicts:
      new       — non-ok now, and not what the last digest reported
      still     — non-ok now, same state the last digest reported
      recovered — ok now, last digest reported suspect/failing
    Failing sorts before suspect, oldest `since` first.
    A company the user marked expected in its current state is left out (mark_expected); a change of
    state clears the mark, so a new kind of failure is reported again.
    """
    conn = get_conn()
    try:
        rows = [dict(r) for r in conn.execute("""
            SELECT * FROM company_health
            WHERE (state <> ? OR COALESCE(reported_state, ?) <> ?)
              AND expected_state IS DISTINCT FROM state
            ORDER BY CASE state WHEN ? THEN 0 WHEN ? THEN 1 ELSE 2 END, since
        """, (STATE_OK, STATE_OK, STATE_OK, STATE_FAILING, STATE_SUSPECT)).fetchall()]
    finally:
        conn.close()
    snap = {"new": [], "still": [], "recovered": []}
    for r in rows:
        reported = r.get("reported_state") or STATE_OK
        if r["state"] == STATE_OK:
            snap["recovered"].append(r)
        elif reported == r["state"]:
            snap["still"].append(r)
        else:
            snap["new"].append(r)
    return snap


def mark_expected(company_key):
    """
    The user judged the company's current non-ok state to be real (a board that shrank, a page that
    moved): keep the verdict but stop reporting it. Cleared automatically by refresh() the moment the
    state changes. Returns True when a non-ok row was marked.
    """
    conn = get_conn()
    try:
        cur = conn.execute("""
            UPDATE company_health SET expected_state = state, expected_at = CURRENT_TIMESTAMP
            WHERE company_key = ? AND state <> ?
        """, (str(company_key), STATE_OK))
        conn.commit()
        return cur.rowcount > 0
    except Exception as exc:
        try:
            conn.rollback()
        except Exception:
            pass
        logger.warning("company_health: mark_expected failed (company=%r): %s", company_key, exc)
        return False
    finally:
        conn.close()


def clear_expected(company_key):
    """Undo mark_expected — the company is reported again from the next digest."""
    conn = get_conn()
    try:
        conn.execute("UPDATE company_health SET expected_state = NULL, expected_at = NULL "
                     "WHERE company_key = ?", (str(company_key),))
        conn.commit()
    except Exception as exc:
        try:
            conn.rollback()
        except Exception:
            pass
        logger.warning("company_health: clear_expected failed (company=%r): %s", company_key, exc)
    finally:
        conn.close()


def mark_reported(pairs=None):
    """
    Record that the digest just sent told the user a company's state.
    `pairs` = the (company_key, state) pairs the digest actually contained: only a row whose current key
    AND state still match a sent pair is stamped, so a row that changed after the snapshot was taken is
    left for the next digest. With no pairs every company's current state is stamped.
    """
    conn = get_conn()
    try:
        if pairs is None:
            conn.execute("""
                UPDATE company_health SET reported_state = state, reported_at = CURRENT_TIMESTAMP
                WHERE reported_state IS DISTINCT FROM state
            """)
        else:
            for key, state in pairs:
                conn.execute("""
                    UPDATE company_health SET reported_state = state, reported_at = CURRENT_TIMESTAMP
                    WHERE company_key = ? AND state = ? AND reported_state IS DISTINCT FROM state
                """, (str(key), state))
        conn.commit()
    except Exception as exc:
        try:
            conn.rollback()
        except Exception:
            pass
        logger.warning("company_health: mark_reported failed: %s", exc)
    finally:
        conn.close()
