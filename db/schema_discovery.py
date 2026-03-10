# db/schema_discovery.py — ats_discovery.db schema
#
# Separate database for ATS company discovery data.
# Completely independent from recruiter_pipeline.db.
#
# Freshness strategy:
#   Discovery:  Monthly Athena query → INSERT OR IGNORE
#               Only queries NEW crawls not yet scanned
#   Cleanup:    Sliding window → archive then delete
#               slugs not seen in last 3 crawls
#   Archive:    data/ats_archive.csv.gz → historical
#               compressed, never queried by pipeline
#   Self-heal:  consecutive_empty_days >= 14
#               → re-detection for monitored companies

import os
import sqlite3

DISCOVERY_DB = os.path.join("data", "ats_discovery.db")


def get_discovery_conn():
    """Get connection to ats_discovery.db."""
    os.makedirs("data", exist_ok=True)
    conn = sqlite3.connect(DISCOVERY_DB)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_discovery_db():
    """Initialize ats_discovery.db schema."""
    conn = get_discovery_conn()
    try:
        # Main ATS companies table
        conn.execute("""
            CREATE TABLE IF NOT EXISTS ats_companies (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,

                -- Identity
                platform        TEXT NOT NULL,
                slug            TEXT NOT NULL,

                -- Company metadata (from enrichment)
                company_name    TEXT,
                website         TEXT,
                job_count       INTEGER DEFAULT 0,

                -- Discovery metadata
                crawl_source    TEXT,       -- first crawl that found this slug
                first_seen      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                last_verified   TIMESTAMP,  -- when ATS API was last called
                last_seen_crawl TEXT,       -- most recent CC crawl containing slug

                -- Status
                is_active       INTEGER DEFAULT 1,  -- 0 if 404 on verify
                is_enriched     INTEGER DEFAULT 0,  -- 0 = name not fetched yet
                source          TEXT DEFAULT 'crawl',
                                -- crawl / detection / manual / backfill

                created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

                UNIQUE(platform, slug)
            )
        """)

        # Track which crawls have been scanned
        # Prevents re-querying Athena for already-processed crawls
        conn.execute("""
            CREATE TABLE IF NOT EXISTS scanned_crawls (
                crawl_id        TEXT PRIMARY KEY,
                scanned_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                slugs_found     INTEGER DEFAULT 0,
                slugs_new       INTEGER DEFAULT 0,
                query_type      TEXT DEFAULT 'athena'
                                -- athena / backfill
            )
        """)

        # Indexes for fast Phase 1 lookups
        conn.execute("""
            CREATE INDEX IF NOT EXISTS
            idx_ats_companies_platform_name
            ON ats_companies(platform, company_name)
        """)

        conn.execute("""
            CREATE INDEX IF NOT EXISTS
            idx_ats_companies_platform_slug
            ON ats_companies(platform, slug)
        """)

        conn.execute("""
            CREATE INDEX IF NOT EXISTS
            idx_ats_companies_active
            ON ats_companies(is_active, platform)
        """)

        conn.execute("""
            CREATE INDEX IF NOT EXISTS
            idx_ats_companies_enriched
            ON ats_companies(is_enriched)
        """)

        conn.execute("""
            CREATE INDEX IF NOT EXISTS
            idx_ats_companies_last_seen
            ON ats_companies(last_seen_crawl, source)
        """)

        conn.commit()

    finally:
        conn.close()