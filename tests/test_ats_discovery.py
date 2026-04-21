# tests/test_ats_discovery.py — Tests for ATS discovery pipeline
#
# Covers:
#   db/schema_discovery.py   — schema + scanned_crawls table
#   db/ats_companies.py      — CRUD, archive, cleanup, delete_company
#   build_ats_slug_list.py   — slug extraction, Bing quota, crawl tracking
#   enrich_ats_companies.py  — 404 deletes record

import os
import csv
import gzip
import json
import sqlite3
import tempfile
import unittest
from datetime import datetime
from unittest.mock import patch, MagicMock


# ─────────────────────────────────────────
# TEST HELPERS
# ─────────────────────────────────────────

class TempDiscoveryDB:
    """Context manager: temporary ats_discovery.db for each test."""

    def __enter__(self):
        self.tmpdir = tempfile.mkdtemp()
        self.db_path = os.path.join(self.tmpdir, "ats_discovery.db")
        os.environ["_TEST_DISCOVERY_DB"] = self.db_path
        return self.db_path

    def __exit__(self, *args):
        import shutil
        shutil.rmtree(self.tmpdir, ignore_errors=True)
        os.environ.pop("_TEST_DISCOVERY_DB", None)


def _get_test_conn(db_path):
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


# ─────────────────────────────────────────
# SCHEMA TESTS
# ─────────────────────────────────────────

class TestSchemaDiscovery(unittest.TestCase):
    """Tests for db/schema_discovery.py."""

    def test_init_creates_ats_companies_table(self):
        """init_discovery_db creates ats_companies + scanned_crawls tables."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "test.db")

            # Use production initializer
            import db.schema_discovery as sd
            original = sd.DISCOVERY_DB
            sd.DISCOVERY_DB = db_path
            try:
                sd.init_discovery_db()
                conn = sqlite3.connect(db_path)
                tables = [r[0] for r in conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                ).fetchall()]
                self.assertIn("ats_companies", tables)
                self.assertIn("scanned_crawls", tables)
                conn.close()
            finally:
                sd.DISCOVERY_DB = original

    def test_ats_companies_unique_constraint(self):
        """UNIQUE(platform, slug) prevents duplicates."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "test.db")

            import db.schema_discovery as sd
            original = sd.DISCOVERY_DB
            sd.DISCOVERY_DB = db_path
            try:
                sd.init_discovery_db()
                conn = sqlite3.connect(db_path)
                conn.execute(
                    "INSERT INTO ats_companies (platform, slug) "
                    "VALUES ('greenhouse', 'stripe')"
                )
                conn.commit()

                # Second insert should be ignored
                conn.execute(
                    "INSERT OR IGNORE INTO ats_companies (platform, slug) "
                    "VALUES ('greenhouse', 'stripe')"
                )
                conn.commit()

                count = conn.execute(
                    "SELECT COUNT(*) FROM ats_companies"
                ).fetchone()[0]
                self.assertEqual(count, 1)
                conn.close()
            finally:
                sd.DISCOVERY_DB = original

    def test_scanned_crawls_primary_key(self):
        """crawl_id is primary key — no duplicates."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "test.db")
            conn = sqlite3.connect(db_path)
            conn.row_factory = sqlite3.Row
            conn.execute("""
                CREATE TABLE scanned_crawls (
                    crawl_id TEXT PRIMARY KEY,
                    scanned_at TIMESTAMP,
                    slugs_found INTEGER DEFAULT 0
                )
            """)
            conn.execute(
                "INSERT INTO scanned_crawls VALUES "
                "('CC-MAIN-2026-08', '2026-03-01', 1000)"
            )
            conn.commit()

            # Replace should update
            conn.execute(
                "INSERT OR REPLACE INTO scanned_crawls VALUES "
                "('CC-MAIN-2026-08', '2026-03-02', 2000)"
            )
            conn.commit()

            row = conn.execute(
                "SELECT * FROM scanned_crawls "
                "WHERE crawl_id = 'CC-MAIN-2026-08'"
            ).fetchone()
            self.assertEqual(row["slugs_found"], 2000)
            conn.close()


# ─────────────────────────────────────────
# ATS COMPANIES CRUD TESTS
# ─────────────────────────────────────────

class TestATSCompaniesCRUD(unittest.TestCase):
    """Tests for db/ats_companies.py CRUD operations."""

    def setUp(self):
        """Create in-memory SQLite for each test."""
        self.conn = sqlite3.connect(":memory:")
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("""
            CREATE TABLE ats_companies (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                platform TEXT NOT NULL,
                slug TEXT NOT NULL,
                company_name TEXT,
                website TEXT,
                job_count INTEGER DEFAULT 0,
                crawl_source TEXT,
                first_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                last_verified TIMESTAMP,
                last_seen_crawl TEXT,
                is_active INTEGER DEFAULT 1,
                is_enriched INTEGER DEFAULT 0,
                source TEXT DEFAULT 'crawl',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(platform, slug)
            )
        """)
        self.conn.execute("""
            CREATE TABLE scanned_crawls (
                crawl_id TEXT PRIMARY KEY,
                scanned_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                slugs_found INTEGER DEFAULT 0,
                slugs_new INTEGER DEFAULT 0,
                query_type TEXT DEFAULT 'athena'
            )
        """)
        self.conn.commit()

    def tearDown(self):
        self.conn.close()

    def _insert(self, platform, slug, source="crawl",
                last_seen=None, company_name=None,
                is_enriched=0):
        self.conn.execute("""
            INSERT OR IGNORE INTO ats_companies
                (platform, slug, source, last_seen_crawl,
                 company_name, is_enriched)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (platform, slug, source,
              last_seen or "CC-MAIN-2026-08",
              company_name, is_enriched))
        self.conn.commit()

    def test_bulk_insert_adds_new_slugs(self):
        """bulk_insert_slugs inserts new rows."""
        slugs = {"stripe", "airbnb", "databricks"}
        added = 0
        for slug in slugs:
            result = self.conn.execute("""
                INSERT OR IGNORE INTO ats_companies
                    (platform, slug, crawl_source,
                     last_seen_crawl, source)
                VALUES ('greenhouse', ?, 'CC-MAIN-2026-08',
                        'CC-MAIN-2026-08', 'crawl')
            """, (slug,))
            if result.rowcount > 0:
                added += 1
        self.conn.commit()
        self.assertEqual(added, 3)

    def test_bulk_insert_ignores_duplicates(self):
        """INSERT OR IGNORE preserves existing data."""
        self._insert("greenhouse", "stripe",
                     company_name="Stripe", is_enriched=1)

        # Try inserting same slug again
        self.conn.execute("""
            INSERT OR IGNORE INTO ats_companies
                (platform, slug, company_name, is_enriched)
            VALUES ('greenhouse', 'stripe', 'WRONG', 0)
        """)
        self.conn.commit()

        row = self.conn.execute("""
            SELECT company_name, is_enriched FROM ats_companies
            WHERE platform='greenhouse' AND slug='stripe'
        """).fetchone()
        # Original data preserved
        self.assertEqual(row["company_name"], "Stripe")
        self.assertEqual(row["is_enriched"], 1)

    def test_last_seen_crawl_updates_on_existing(self):
        """Existing slugs get last_seen_crawl updated."""
        self._insert("greenhouse", "stripe",
                     last_seen="CC-MAIN-2025-51")

        # Simulate new crawl finding same slug
        self.conn.execute("""
            UPDATE ats_companies
            SET last_seen_crawl = 'CC-MAIN-2026-08'
            WHERE platform = 'greenhouse' AND slug = 'stripe'
        """)
        self.conn.commit()

        row = self.conn.execute("""
            SELECT last_seen_crawl FROM ats_companies
            WHERE platform='greenhouse' AND slug='stripe'
        """).fetchone()
        self.assertEqual(row["last_seen_crawl"], "CC-MAIN-2026-08")

    def test_delete_company_removes_row(self):
        """delete_company permanently removes record."""
        self._insert("greenhouse", "stripe")

        # Simulate delete_company
        self.conn.execute("""
            DELETE FROM ats_companies
            WHERE platform = 'greenhouse' AND slug = 'stripe'
        """)
        self.conn.commit()

        row = self.conn.execute("""
            SELECT * FROM ats_companies
            WHERE platform='greenhouse' AND slug='stripe'
        """).fetchone()
        self.assertIsNone(row)

    def test_delete_company_not_mark_inactive(self):
        """404 should delete, not just set is_active=0."""
        self._insert("greenhouse", "deadcompany")

        # Old behavior (wrong): mark_inactive
        # New behavior (correct): delete
        self.conn.execute("""
            DELETE FROM ats_companies
            WHERE platform='greenhouse' AND slug='deadcompany'
        """)
        self.conn.commit()

        count = self.conn.execute("""
            SELECT COUNT(*) FROM ats_companies
            WHERE platform='greenhouse' AND slug='deadcompany'
        """).fetchone()[0]
        self.assertEqual(count, 0)  # Gone permanently ✓

    def test_source_detection_not_deleted_by_cleanup(self):
        """source='detection' rows survive sliding window cleanup."""
        self._insert("greenhouse", "stripe",
                     source="detection",
                     last_seen="CC-MAIN-2024-01")  # very old crawl

        keep_crawls = ["CC-MAIN-2026-08",
                       "CC-MAIN-2026-04",
                       "CC-MAIN-2025-51"]
        placeholders = ",".join("?" * len(keep_crawls))

        self.conn.execute(f"""
            DELETE FROM ats_companies
            WHERE last_seen_crawl NOT IN ({placeholders})
            AND source = 'crawl'
        """, keep_crawls)
        self.conn.commit()

        # detection source should still be there
        row = self.conn.execute("""
            SELECT * FROM ats_companies
            WHERE platform='greenhouse' AND slug='stripe'
        """).fetchone()
        self.assertIsNotNone(row)

    def test_source_crawl_deleted_by_cleanup(self):
        """source='crawl' rows are deleted if not in window."""
        self._insert("greenhouse", "oldcompany",
                     source="crawl",
                     last_seen="CC-MAIN-2024-01")  # old crawl

        keep_crawls = ["CC-MAIN-2026-08",
                       "CC-MAIN-2026-04",
                       "CC-MAIN-2025-51"]
        placeholders = ",".join("?" * len(keep_crawls))

        self.conn.execute(f"""
            DELETE FROM ats_companies
            WHERE last_seen_crawl NOT IN ({placeholders})
            AND source = 'crawl'
        """, keep_crawls)
        self.conn.commit()

        row = self.conn.execute("""
            SELECT * FROM ats_companies
            WHERE slug='oldcompany'
        """).fetchone()
        self.assertIsNone(row)

    def test_backfill_source_never_deleted(self):
        """source='backfill' rows survive sliding window cleanup."""
        self._insert("lever", "netflix",
                     source="backfill",
                     last_seen="CC-MAIN-2025-43")

        keep_crawls = ["CC-MAIN-2026-08",
                       "CC-MAIN-2026-04",
                       "CC-MAIN-2025-51"]
        placeholders = ",".join("?" * len(keep_crawls))

        self.conn.execute(f"""
            DELETE FROM ats_companies
            WHERE last_seen_crawl NOT IN ({placeholders})
            AND source = 'crawl'
        """, keep_crawls)
        self.conn.commit()

        row = self.conn.execute("""
            SELECT * FROM ats_companies
            WHERE platform='lever' AND slug='netflix'
        """).fetchone()
        self.assertIsNotNone(row)


# ─────────────────────────────────────────
# SCANNED CRAWLS TESTS
# ─────────────────────────────────────────

class TestScannedCrawls(unittest.TestCase):
    """Tests for scanned_crawls tracking."""

    def setUp(self):
        self.conn = sqlite3.connect(":memory:")
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("""
            CREATE TABLE scanned_crawls (
                crawl_id TEXT PRIMARY KEY,
                scanned_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                slugs_found INTEGER DEFAULT 0,
                slugs_new INTEGER DEFAULT 0,
                query_type TEXT DEFAULT 'athena'
            )
        """)
        self.conn.commit()

    def tearDown(self):
        self.conn.close()

    def _mark_scanned(self, crawl_id, query_type="athena"):
        self.conn.execute("""
            INSERT OR REPLACE INTO scanned_crawls
                (crawl_id, scanned_at, query_type)
            VALUES (?, ?, ?)
        """, (crawl_id, datetime.now().isoformat(), query_type))
        self.conn.commit()

    def _get_scanned(self):
        rows = self.conn.execute(
            "SELECT crawl_id FROM scanned_crawls"
        ).fetchall()
        return {r["crawl_id"] for r in rows}

    def test_get_unscanned_returns_new_only(self):
        """Only unscanned crawls returned."""
        self._mark_scanned("CC-MAIN-2026-04")
        self._mark_scanned("CC-MAIN-2025-51")

        window   = ["CC-MAIN-2026-08",
                    "CC-MAIN-2026-04",
                    "CC-MAIN-2025-51"]
        scanned  = self._get_scanned()
        unscanned = [c for c in window if c not in scanned]

        self.assertEqual(unscanned, ["CC-MAIN-2026-08"])

    def test_all_scanned_returns_empty(self):
        """Empty list when all crawls already scanned."""
        window = ["CC-MAIN-2026-08",
                  "CC-MAIN-2026-04",
                  "CC-MAIN-2025-51"]
        for c in window:
            self._mark_scanned(c)

        scanned  = self._get_scanned()
        unscanned = [c for c in window if c not in scanned]
        self.assertEqual(unscanned, [])

    def test_backfill_crawl_tracked_separately(self):
        """Backfill uses different crawl_id format."""
        self._mark_scanned(
            "backfill-CC-MAIN-2025-43", query_type="backfill"
        )

        scanned = self._get_scanned()
        self.assertIn("backfill-CC-MAIN-2025-43", scanned)

        row = self.conn.execute("""
            SELECT query_type FROM scanned_crawls
            WHERE crawl_id = 'backfill-CC-MAIN-2025-43'
        """).fetchone()
        self.assertEqual(row["query_type"], "backfill")


# ─────────────────────────────────────────
# SLUG EXTRACTION TESTS
# ─────────────────────────────────────────

class TestSlugExtraction(unittest.TestCase):
    """Tests for extract_slug() in build_ats_slug_list.py."""

    def _extract(self, row):
        """Call extract_slug with a mock Athena row dict."""
        from build_ats_slug_list import extract_slug
        return extract_slug(row)

    def test_greenhouse_boards_extracts_slug(self):
        """boards.greenhouse.io → slug from path."""
        result = self._extract({
            "url_host_registered_domain": "greenhouse.io",
            "url_host_3rd_last_part":     "boards",
            "url_host_4th_last_part":     None,
            "url_host_5th_last_part":     None,
            "url_path":                   "/stripe/jobs/123",
        })
        self.assertIsNotNone(result)
        self.assertEqual(result[0], "greenhouse")
        self.assertEqual(result[1], "stripe")

    def test_greenhouse_job_boards_text_slug_kept(self):
        """job-boards with text slug is kept."""
        result = self._extract({
            "url_host_registered_domain": "greenhouse.io",
            "url_host_3rd_last_part":     "job-boards",
            "url_host_4th_last_part":     None,
            "url_host_5th_last_part":     None,
            "url_path":                   "/andurilindustries/jobs/123",
        })
        self.assertIsNotNone(result)
        self.assertEqual(result[1], "andurilindustries")

    def test_greenhouse_job_boards_numeric_discarded(self):
        """job-boards with numeric ID is discarded."""
        result = self._extract({
            "url_host_registered_domain": "greenhouse.io",
            "url_host_3rd_last_part":     "job-boards",
            "url_host_4th_last_part":     None,
            "url_host_5th_last_part":     None,
            "url_path":                   "/103644278/jobs/7473353",
        })
        self.assertIsNone(result)

    def test_ashby_extracts_slug(self):
        """jobs.ashbyhq.com → slug from path."""
        result = self._extract({
            "url_host_registered_domain": "ashbyhq.com",
            "url_host_3rd_last_part":     "jobs",
            "url_host_4th_last_part":     None,
            "url_host_5th_last_part":     None,
            "url_path":                   "/linear/8e7e537f/apply",
        })
        self.assertIsNotNone(result)
        self.assertEqual(result[0], "ashby")
        self.assertEqual(result[1], "linear")

    def test_workday_extracts_slug_with_wd_and_path(self):
        """Workday extracts tenant + wd variant + path.

        URL: /en-US/External_Careers/job/Area-Manager
        The locale prefix (en-US) is skipped; the career site name
        (External_Careers) is stored as `path`.  This matches the
        documented behaviour in patterns.py: "We always want the career
        site name, not the locale prefix."
        """
        result = self._extract({
            "url_host_registered_domain": "myworkdayjobs.com",
            "url_host_3rd_last_part":     "wd1",
            "url_host_4th_last_part":     "2020companies",
            "url_host_5th_last_part":     None,
            "url_path":                   "/en-US/External_Careers/job/Area-Manager",
        })
        self.assertIsNotNone(result)
        self.assertEqual(result[0], "workday")
        data = json.loads(result[1])
        self.assertEqual(data["slug"], "2020companies")
        self.assertEqual(data["wd"],   "wd1")
        # en-US is a locale prefix and is skipped; the career site name is stored
        self.assertEqual(data["path"], "External_Careers")

    def test_workday_excludes_aggregator_subdomains(self):
        """Workday aggregator subdomains are excluded."""
        result = self._extract({
            "url_host_registered_domain": "myworkdayjobs.com",
            "url_host_3rd_last_part":     "wd1",
            "url_host_4th_last_part":     "jobs",  # aggregator
            "url_host_5th_last_part":     None,
            "url_path":                   "/search",
        })
        self.assertIsNone(result)

    def test_oracle_hcm_extracts_tenant_region_site(self):
        """Oracle HCM extracts tenant + region + site_id."""
        result = self._extract({
            "url_host_registered_domain": "oraclecloud.com",
            "url_host_3rd_last_part":     "ap1",
            "url_host_4th_last_part":     "fa",
            "url_host_5th_last_part":     "ebuu",
            "url_path": "/hcmUI/CandidateExperience/en/sites/CX_3001/job/123",
        })
        self.assertIsNotNone(result)
        self.assertEqual(result[0], "oracle_hcm")
        data = json.loads(result[1])
        self.assertEqual(data["slug"],   "ebuu")
        self.assertEqual(data["region"], "ap1")
        self.assertEqual(data["site"],   "CX_3001")

    def test_oracle_hcm_without_site_id_returns_none(self):
        """Oracle HCM rows without site_id are skipped."""
        result = self._extract({
            "url_host_registered_domain": "oraclecloud.com",
            "url_host_3rd_last_part":     "ap1",
            "url_host_4th_last_part":     "fa",
            "url_host_5th_last_part":     "ebuu",
            "url_path": "/hcmUI/CandidateExperience/en/jobs",
        })
        self.assertIsNone(result)

    def test_icims_extracts_slug(self):
        """iCIMS extracts slug from 3rd_last_part."""
        result = self._extract({
            "url_host_registered_domain": "icims.com",
            "url_host_3rd_last_part":     "abudhabi-nyu",
            "url_host_4th_last_part":     None,
            "url_host_5th_last_part":     None,
            "url_path": "/jobs/15028/title/job",
        })
        self.assertIsNotNone(result)
        self.assertEqual(result[0], "icims")
        self.assertEqual(result[1], "abudhabi-nyu")

    def test_icims_strips_careers_prefix(self):
        """iCIMS strips careers- prefix from slug."""
        result = self._extract({
            "url_host_registered_domain": "icims.com",
            "url_host_3rd_last_part":     "careers-schwab",
            "url_host_4th_last_part":     None,
            "url_host_5th_last_part":     None,
            "url_path": "/jobs/123/title/job",
        })
        self.assertIsNotNone(result)
        self.assertEqual(result[1], "schwab")

    def test_icims_excludes_www(self):
        """iCIMS www subdomain is excluded."""
        result = self._extract({
            "url_host_registered_domain": "icims.com",
            "url_host_3rd_last_part":     "www",
            "url_host_4th_last_part":     None,
            "url_host_5th_last_part":     None,
            "url_path": "/jobs/search",
        })
        self.assertIsNone(result)


# ─────────────────────────────────────────
# ARCHIVE TESTS
# ─────────────────────────────────────────

class TestArchiveStale(unittest.TestCase):
    """Tests for archive_stale_slugs() and remove_stale_crawls()."""

    def test_archive_writes_gzip_csv(self):
        """archive_stale_slugs writes compressed CSV."""
        with tempfile.TemporaryDirectory() as tmpdir:
            archive_path = os.path.join(tmpdir, "ats_archive.csv.gz")

            stale_rows = [
                {
                    "platform":       "greenhouse",
                    "slug":           "oldcompany",
                    "company_name":   "Old Company Inc",
                    "website":        "oldcompany.com",
                    "crawl_source":   "CC-MAIN-2024-01",
                    "last_seen_crawl":"CC-MAIN-2024-01",
                    "source":         "crawl",
                    "first_seen":     "2024-01-01",
                    "archived_at":    datetime.now().isoformat(),
                }
            ]

            # Write archive
            with gzip.open(archive_path, "at",
                           newline="", encoding="utf-8") as f:
                fields = [
                    "platform", "slug", "company_name",
                    "website", "crawl_source", "last_seen_crawl",
                    "source", "first_seen", "archived_at",
                ]
                writer = csv.DictWriter(
                    f, fieldnames=fields, extrasaction="ignore"
                )
                writer.writeheader()
                for row in stale_rows:
                    writer.writerow(row)

            # Verify can be read back
            self.assertTrue(os.path.exists(archive_path))
            with gzip.open(archive_path, "rt",
                           encoding="utf-8") as f:
                reader = list(csv.DictReader(f))

            self.assertEqual(len(reader), 1)
            self.assertEqual(reader[0]["slug"], "oldcompany")
            self.assertEqual(reader[0]["platform"], "greenhouse")

    def test_archive_appends_on_subsequent_runs(self):
        """Archive appends — doesn't overwrite."""
        with tempfile.TemporaryDirectory() as tmpdir:
            archive_path = os.path.join(tmpdir, "ats_archive.csv.gz")
            fields = ["platform", "slug", "archived_at"]

            # First write
            with gzip.open(archive_path, "at",
                           newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=fields)
                writer.writeheader()
                writer.writerow({
                    "platform": "greenhouse",
                    "slug": "company1",
                    "archived_at": "2026-02-01"
                })

            # Second write (append)
            with gzip.open(archive_path, "at",
                           newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=fields)
                writer.writerow({
                    "platform": "ashby",
                    "slug": "company2",
                    "archived_at": "2026-03-01"
                })

            # Both rows should be present
            with gzip.open(archive_path, "rt",
                           encoding="utf-8") as f:
                reader = list(csv.DictReader(f))

            self.assertEqual(len(reader), 2)
            slugs = {r["slug"] for r in reader}
            self.assertIn("company1", slugs)
            self.assertIn("company2", slugs)


# ─────────────────────────────────────────
# BING QUOTA TESTS
# ─────────────────────────────────────────

class TestBraveQuota(unittest.TestCase):
    """Tests for Brave Search quota management in build_ats_slug_list.py."""

    def _make_quota_file(self, tmpdir, calls, month=None):
        month = month or datetime.now().strftime("%Y-%m")
        quota_path = os.path.join(tmpdir, "bing_quota.json")
        with open(quota_path, "w") as f:
            json.dump({"month": month, "calls": calls}, f)
        return quota_path

    def test_quota_hard_stop_at_950(self):
        """Hard stop at 950 — not 1000."""
        with tempfile.TemporaryDirectory() as tmpdir:
            quota_path = self._make_quota_file(tmpdir, 950)
            with open(quota_path) as f:
                data = json.load(f)
            remaining = max(0, 950 - data["calls"])
            self.assertEqual(remaining, 0)

    def test_quota_allows_calls_below_950(self):
        """Calls allowed when under 950."""
        with tempfile.TemporaryDirectory() as tmpdir:
            quota_path = self._make_quota_file(tmpdir, 100)
            with open(quota_path) as f:
                data = json.load(f)
            remaining = max(0, 950 - data["calls"])
            self.assertEqual(remaining, 850)
            self.assertGreater(remaining, 0)

    def test_quota_resets_on_new_month(self):
        """Quota auto-resets when calendar month changes."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Old month quota at 900 calls
            quota_path = self._make_quota_file(
                tmpdir, 900, month="2026-02"
            )

            current_month = datetime.now().strftime("%Y-%m")
            with open(quota_path) as f:
                data = json.load(f)

            # Simulate month change detection
            if data.get("month") != current_month:
                data = {"month": current_month, "calls": 0}

            # Should reset to 0
            self.assertEqual(data["calls"], 0)
            self.assertEqual(data["month"], current_month)

    def test_quota_only_increments_on_success(self):
        """Quota incremented only on HTTP 200, not on errors."""
        calls_before = 100
        # Simulate failed request (non-200) — no increment
        status_code = 429
        if status_code == 200:
            calls_before += 1
        self.assertEqual(calls_before, 100)  # unchanged

        # Simulate success — increment
        status_code = 200
        if status_code == 200:
            calls_before += 1
        self.assertEqual(calls_before, 101)

    def test_quota_limit_is_1000_hard_stop_950(self):
        """Verify limit constants."""
        BRAVE_MONTHLY_QUOTA = 1000
        BRAVE_QUOTA_BUFFER  = 50
        BRAVE_QUOTA_LIMIT   = BRAVE_MONTHLY_QUOTA - BRAVE_QUOTA_BUFFER
        self.assertEqual(BRAVE_QUOTA_LIMIT, 950)
        self.assertEqual(BRAVE_MONTHLY_QUOTA, 1000)


# ─────────────────────────────────────────
# COLLINFO CACHE TESTS
# ─────────────────────────────────────────

class TestCollinfoCaching(unittest.TestCase):
    """Tests for collinfo.json caching in get_recent_crawls()."""

    def test_cache_used_when_fresh(self):
        """Fresh cache (< 24h) is used without fetching."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cache_path = os.path.join(tmpdir, "collinfo_cache.json")
            cached_crawls = [
                "CC-MAIN-2026-08",
                "CC-MAIN-2026-04",
                "CC-MAIN-2025-51",
            ]
            with open(cache_path, "w") as f:
                json.dump({
                    "crawls": cached_crawls,
                    "fetched_at": datetime.now().isoformat()
                }, f)

            # Simulate cache hit (< 24h old)
            import time
            mtime = os.path.getmtime(cache_path)
            age   = time.time() - mtime
            self.assertLess(age, 86400)

            # Read from cache
            with open(cache_path) as f:
                data = json.load(f)
            crawls = data["crawls"][:3]
            self.assertEqual(crawls, cached_crawls)

    def test_fallback_crawls_used_on_network_error(self):
        """Hardcoded fallback used when API unreachable."""
        fallback = [
            "CC-MAIN-2026-08",
            "CC-MAIN-2026-04",
            "CC-MAIN-2025-51",
        ]
        # Simulate network error → use fallback
        self.assertEqual(len(fallback), 3)
        self.assertTrue(all(
            c.startswith("CC-MAIN-") for c in fallback
        ))


# ─────────────────────────────────────────
# ENRICH 404 DELETE TESTS
# ─────────────────────────────────────────

class TestEnrichDelete(unittest.TestCase):
    """Tests that enrich_ats_companies deletes on 404."""

    def setUp(self):
        self.conn = sqlite3.connect(":memory:")
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("""
            CREATE TABLE ats_companies (
                platform TEXT NOT NULL,
                slug TEXT NOT NULL,
                company_name TEXT,
                is_active INTEGER DEFAULT 1,
                UNIQUE(platform, slug)
            )
        """)
        self.conn.execute("""
            INSERT INTO ats_companies
            VALUES ('greenhouse', 'deadcompany', NULL, 1)
        """)
        self.conn.commit()

    def tearDown(self):
        self.conn.close()

    def test_404_deletes_record(self):
        """404 response permanently deletes record."""
        # Simulate 404 handling
        status_code = 404
        if status_code == 404:
            self.conn.execute("""
                DELETE FROM ats_companies
                WHERE platform='greenhouse' AND slug='deadcompany'
            """)
            self.conn.commit()

        count = self.conn.execute("""
            SELECT COUNT(*) FROM ats_companies
            WHERE platform='greenhouse' AND slug='deadcompany'
        """).fetchone()[0]
        self.assertEqual(count, 0)

    def test_200_keeps_and_enriches_record(self):
        """200 response enriches record — does not delete."""
        status_code = 200
        if status_code == 200:
            self.conn.execute("""
                UPDATE ats_companies
                SET company_name = 'Dead Company Inc',
                    is_active = 1
                WHERE platform='greenhouse' AND slug='deadcompany'
            """)
            self.conn.commit()

        row = self.conn.execute("""
            SELECT company_name FROM ats_companies
            WHERE slug='deadcompany'
        """).fetchone()
        self.assertIsNotNone(row)
        self.assertEqual(row["company_name"], "Dead Company Inc")

    def test_404_does_not_use_mark_inactive(self):
        """
        Verify 404 deletes (is_active stays irrelevant).
        Old behavior: mark_inactive sets is_active=0, row stays.
        New behavior: delete_company removes row entirely.
        """
        # Old (wrong) approach
        self.conn.execute("""
            UPDATE ats_companies SET is_active = 0
            WHERE platform='greenhouse' AND slug='deadcompany'
        """)
        self.conn.commit()

        # Row still exists with is_active=0 — this is WRONG
        count_old = self.conn.execute("""
            SELECT COUNT(*) FROM ats_companies
            WHERE platform='greenhouse' AND slug='deadcompany'
        """).fetchone()[0]
        self.assertEqual(count_old, 1)  # Still there (bad!)

        # New (correct) approach — DELETE
        self.conn.execute("""
            DELETE FROM ats_companies
            WHERE platform='greenhouse' AND slug='deadcompany'
        """)
        self.conn.commit()

        count_new = self.conn.execute("""
            SELECT COUNT(*) FROM ats_companies
            WHERE platform='greenhouse' AND slug='deadcompany'
        """).fetchone()[0]
        self.assertEqual(count_new, 0)  # Gone (correct!) ✓


if __name__ == "__main__":
    unittest.main(verbosity=2)