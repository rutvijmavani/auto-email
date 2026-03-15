# db/ats_companies.py — CRUD operations for ats_discovery.db

import csv
import gzip
import json
import os
from datetime import datetime
from db.schema_discovery import get_discovery_conn

ARCHIVE_PATH = os.path.join("data", "ats_archive.csv.gz")
ARCHIVE_FIELDS = [
    "platform", "slug", "company_name", "website",
    "crawl_source", "last_seen_crawl", "source",
    "first_seen", "archived_at",
]


# ─────────────────────────────────────────
# SCANNED CRAWLS TRACKING
# ─────────────────────────────────────────

def get_scanned_crawls():
    """Return set of crawl IDs already processed by Athena."""
    conn = get_discovery_conn()
    try:
        rows = conn.execute(
            "SELECT crawl_id FROM scanned_crawls"
        ).fetchall()
        return {r["crawl_id"] for r in rows}
    finally:
        conn.close()


def mark_crawl_scanned(crawl_id, slugs_found=0,
                       slugs_new=0, query_type="athena"):
    """Mark a crawl as processed."""
    conn = get_discovery_conn()
    try:
        conn.execute("""
            INSERT OR REPLACE INTO scanned_crawls
                (crawl_id, scanned_at, slugs_found,
                 slugs_new, query_type)
            VALUES (?, ?, ?, ?, ?)
        """, (crawl_id, datetime.now().isoformat(),
              slugs_found, slugs_new, query_type))
        conn.commit()
    finally:
        conn.close()


def get_unscanned_crawls(window_crawls):
    """
    Return crawls in window that haven't been scanned yet.
    Only these need Athena queries.

    Args:
        window_crawls: list of crawl IDs in sliding window
                       e.g. ["CC-MAIN-2026-08", "CC-MAIN-2026-04", ...]

    Returns:
        list of unscanned crawl IDs (usually just 1 — the newest)
    """
    scanned = get_scanned_crawls()
    return [c for c in window_crawls if c not in scanned]


# ─────────────────────────────────────────
# PHASE 1 LOOKUP
# ─────────────────────────────────────────

def find_company(company_name, platform=None):
    """
    Phase 1 lookup: find ATS slug for a company by name.
    Uses deterministic keyword matching — no fuzzy matching.

    Returns {platform, slug, company_name} or None.
    """
    from jobs.ats_verifier import _all_keywords_present

    conn = get_discovery_conn()
    try:
        if platform:
            rows = conn.execute("""
                SELECT platform, slug, company_name,
                       website, job_count
                FROM ats_companies
                WHERE platform = ?
                AND is_active = 1
                AND company_name IS NOT NULL
                AND company_name != ''
            """, (platform,)).fetchall()
        else:
            rows = conn.execute("""
                SELECT platform, slug, company_name,
                       website, job_count
                FROM ats_companies
                WHERE is_active = 1
                AND company_name IS NOT NULL
                AND company_name != ''
            """).fetchall()

        for row in rows:
            if _all_keywords_present(company_name,
                                     row["company_name"]):
                return {
                    "platform":     row["platform"],
                    "slug":         row["slug"],
                    "company_name": row["company_name"],
                    "website":      row["website"],
                    "job_count":    row["job_count"],
                }
        return None

    finally:
        conn.close()


def find_by_slug(platform, slug):
    """Find a specific platform+slug combination."""
    conn = get_discovery_conn()
    try:
        row = conn.execute("""
            SELECT * FROM ats_companies
            WHERE platform = ? AND slug = ?
        """, (platform, slug)).fetchone()
        return dict(row) if row else None
    finally:
        conn.close()


# ─────────────────────────────────────────
# INSERT / UPDATE
# ─────────────────────────────────────────

def upsert_company(platform, slug, company_name=None,
                   website=None, job_count=None,
                   crawl_source=None, source="crawl",
                   is_active=None,
                   only_set_name_if_missing=False):
    """Insert or update a company slug."""
    conn = get_discovery_conn()
    try:
        conn.execute("BEGIN IMMEDIATE")

        # name_missing check — only when only_set_name_if_missing is True
        name_missing = True
        if only_set_name_if_missing and company_name is not None:
            row = conn.execute(
                "SELECT company_name FROM ats_companies "
                "WHERE platform=? AND slug=?",
                (platform, slug)
            ).fetchone()
            name_missing = (
                row is None or
                not row["company_name"] or
                row["company_name"].strip() == ""
            )

        conn.execute("""
            INSERT OR IGNORE INTO ats_companies
                (platform, slug, crawl_source,
                 source, last_seen_crawl)
            VALUES (?, ?, ?, ?, ?)
        """, (platform, slug, crawl_source,
              source, crawl_source))

        updates = []
        params  = []

        if company_name is not None:
            write_name = (not only_set_name_if_missing) or name_missing
            if write_name:
                updates.append("company_name = ?")
                params.append(company_name)
                updates.append("is_enriched = 1")

        if website is not None:
            updates.append("website = ?")
            params.append(website)

        if job_count is not None:
            updates.append("job_count = ?")
            params.append(job_count)

        if crawl_source is not None:
            updates.append("last_seen_crawl = ?")
            params.append(crawl_source)

        # Persist source so detection/backfill rows are never
        # accidentally downgraded to 'crawl' by subsequent updates
        if source is not None:
            updates.append("source = ?")
            params.append(source)

        # is_active — only touch when explicitly requested
        if is_active is True:
            updates.append("is_active = 1")
        elif is_active is False:
            updates.append("is_active = 0")

        updates.append("last_verified = ?")
        params.append(datetime.now().isoformat())

        if updates:
            params.extend([platform, slug])
            conn.execute(
                f"UPDATE ats_companies "
                f"SET {', '.join(updates)} "
                f"WHERE platform = ? AND slug = ?",
                params
            )

        conn.commit()

    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def bulk_insert_slugs(platform, slugs, crawl_source):
    """
    Bulk insert slugs from Athena results.
    INSERT OR IGNORE — never overwrites existing data.
    Updates last_seen_crawl for existing rows.

    Returns count of newly inserted rows.
    """
    if not slugs:
        return 0

    conn  = get_discovery_conn()
    added = 0
    try:
        for slug in slugs:
            result = conn.execute("""
                INSERT OR IGNORE INTO ats_companies
                    (platform, slug, crawl_source,
                     last_seen_crawl, source)
                VALUES (?, ?, ?, ?, 'crawl')
            """, (platform, str(slug),
                  crawl_source, crawl_source))

            if result.rowcount > 0:
                added += 1
            else:
                # Update last_seen_crawl for existing rows
                conn.execute("""
                    UPDATE ats_companies
                    SET last_seen_crawl = ?
                    WHERE platform = ? AND slug = ?
                """, (crawl_source, platform, str(slug)))

        conn.commit()
        return added

    finally:
        conn.close()


def mark_from_detection(platform, slug,
                        company_name=None,
                        website=None, job_count=None):
    """
    Self-populate DB from successful pipeline detection.
    Called by ats_detector.py on every successful hit.
    source='detection' → never deleted by sliding window.
    """
    upsert_company(
        platform=platform,
        slug=slug,
        company_name=company_name,
        website=website,
        job_count=job_count,
        source="detection",
    )


def mark_inactive(platform, slug):
    """Mark slug inactive (API returned 404)."""
    conn = get_discovery_conn()
    try:
        conn.execute("""
            UPDATE ats_companies
            SET is_active = 0, last_verified = ?
            WHERE platform = ? AND slug = ?
        """, (datetime.now().isoformat(), platform, slug))
        conn.commit()
    finally:
        conn.close()


def delete_company(platform, slug):
    """
    Permanently delete a slug from ats_companies.
    Called when ATS API returns 404 during enrichment.
    404 = definitively not on this platform → no value keeping it.
    """
    conn = get_discovery_conn()
    try:
        conn.execute("""
            DELETE FROM ats_companies
            WHERE platform = ? AND slug = ?
        """, (platform, slug))
        conn.commit()
    finally:
        conn.close()


# ─────────────────────────────────────────
# SLIDING WINDOW CLEANUP + ARCHIVE
# ─────────────────────────────────────────

def get_stale_slugs(keep_crawls):
    """
    Get slugs not seen in recent crawls.
    Only returns source='crawl' rows — detection/manual
    entries are never subject to sliding window cleanup.

    Args:
        keep_crawls: list of crawl IDs to keep

    Returns:
        list of row dicts to archive then delete
    """
    if not keep_crawls:
        return []

    placeholders = ",".join("?" * len(keep_crawls))
    conn = get_discovery_conn()
    try:
        rows = conn.execute(f"""
            SELECT platform, slug, company_name,
                   website, crawl_source,
                   last_seen_crawl, source, first_seen
            FROM ats_companies
            WHERE last_seen_crawl NOT IN ({placeholders})
            AND source = 'crawl'
        """, keep_crawls).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def archive_stale_slugs(stale_rows):
    """
    Append stale slugs to compressed archive before deletion.
    data/ats_archive.csv.gz — never queried by pipeline.
    Append-mode gzip — grows ~800KB/year.

    Recovery:
      gunzip -c data/ats_archive.csv.gz | head -20
    """
    if not stale_rows:
        return 0

    file_exists = os.path.exists(ARCHIVE_PATH)
    os.makedirs("data", exist_ok=True)

    archived_at = datetime.now().isoformat()
    written     = 0

    with gzip.open(ARCHIVE_PATH, "at", newline="",
                   encoding="utf-8") as f:
        writer = csv.DictWriter(
            f, fieldnames=ARCHIVE_FIELDS,
            extrasaction="ignore"
        )
        if not file_exists:
            writer.writeheader()
        for row in stale_rows:
            row["archived_at"] = archived_at
            writer.writerow(row)
            written += 1

    return written


def remove_stale_crawls(keep_crawls):
    """
    Archive then delete slugs not in sliding window.

    Flow:
      1. Find stale slugs (source='crawl' only)
      2. Archive to ats_archive.csv.gz
      3. Delete from ats_companies
      4. Return count deleted

    detection/manual/backfill sources never deleted.
    """
    stale_rows = get_stale_slugs(keep_crawls)
    if not stale_rows:
        return 0

    # Archive before deleting
    archived = archive_stale_slugs(stale_rows)

    # Delete from DB
    placeholders = ",".join("?" * len(keep_crawls))
    conn = get_discovery_conn()
    try:
        result = conn.execute(f"""
            DELETE FROM ats_companies
            WHERE last_seen_crawl NOT IN ({placeholders})
            AND source = 'crawl'
        """, keep_crawls)
        conn.commit()
        deleted = result.rowcount
    finally:
        conn.close()

    return deleted, archived


# ─────────────────────────────────────────
# ENRICHMENT QUERIES
# ─────────────────────────────────────────

def get_unenriched(platform=None, limit=500):
    """Get slugs not yet enriched with company name."""
    conn = get_discovery_conn()
    try:
        if platform:
            rows = conn.execute("""
                SELECT platform, slug
                FROM ats_companies
                WHERE is_enriched = 0
                AND is_active = 1
                AND platform = ?
                ORDER BY created_at ASC
                LIMIT ?
            """, (platform, limit)).fetchall()
        else:
            rows = conn.execute("""
                SELECT platform, slug
                FROM ats_companies
                WHERE is_enriched = 0
                AND is_active = 1
                ORDER BY platform ASC, created_at ASC
                LIMIT ?
            """, (limit,)).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


# ─────────────────────────────────────────
# STATS
# ─────────────────────────────────────────

def get_stats():
    """Get summary stats for --monitor-status display."""
    conn = get_discovery_conn()
    try:
        rows = conn.execute("""
            SELECT
                platform,
                COUNT(*) as total,
                SUM(CASE WHEN is_enriched = 1
                    THEN 1 ELSE 0 END) as enriched,
                SUM(CASE WHEN is_active = 0
                    THEN 1 ELSE 0 END) as inactive,
                SUM(CASE WHEN source = 'detection'
                    THEN 1 ELSE 0 END) as detected
            FROM ats_companies
            GROUP BY platform
            ORDER BY platform
        """).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def get_total_count():
    """Get total active company count."""
    conn = get_discovery_conn()
    try:
        return conn.execute(
            "SELECT COUNT(*) FROM ats_companies "
            "WHERE is_active = 1"
        ).fetchone()[0]
    finally:
        conn.close()


def get_cache_hit_stats():
    """
    Get Phase 1 cache hit ratio stats.
    Monitor this — if ratio degrades, re-import archive.
    """
    conn = get_discovery_conn()
    try:
        total = conn.execute(
            "SELECT COUNT(*) FROM ats_companies "
            "WHERE is_active = 1"
        ).fetchone()[0]

        enriched = conn.execute(
            "SELECT COUNT(*) FROM ats_companies "
            "WHERE is_active = 1 AND is_enriched = 1"
        ).fetchone()[0]

        by_source = conn.execute("""
            SELECT source, COUNT(*) as count
            FROM ats_companies
            WHERE is_active = 1
            GROUP BY source
        """).fetchall()

        scanned = conn.execute(
            "SELECT COUNT(*) FROM scanned_crawls"
        ).fetchone()[0]

        archive_size = 0
        if os.path.exists(ARCHIVE_PATH):
            archive_size = os.path.getsize(ARCHIVE_PATH)

        return {
            "total":        total,
            "enriched":     enriched,
            "enriched_pct": round(enriched / total * 100, 1)
                            if total else 0,
            "by_source":    {r["source"]: r["count"]
                             for r in by_source},
            "crawls_scanned": scanned,
            "archive_size_kb": archive_size // 1024,
        }
    finally:
        conn.close()