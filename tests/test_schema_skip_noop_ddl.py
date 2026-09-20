"""
tests/test_schema_skip_noop_ddl.py — db.schema._SkipNoopColumnDDL

init_db() wraps its cursor so single-column ADD COLUMN IF NOT EXISTS /
DROP COLUMN IF EXISTS statements that the catalog shows are no-ops are skipped
(they would otherwise take an AccessExclusiveLock on the table and deadlock
against readers that join tables in the opposite order). Anything else must
reach the real cursor untouched.
"""

import os
import sys
import unittest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from db.schema import _SkipNoopColumnDDL


class _FakeCursor:
    """Records executed SQL; answers the catalog probe from a {table: {cols}} dict."""

    def __init__(self, catalog):
        self.catalog = catalog
        self.executed = []      # non-probe statements only
        self._row = None
        self.rowcount = 0

    def execute(self, sql, params=None):
        if "information_schema.columns" in sql:
            column, table = params
            cols = self.catalog.get(table, set())
            self._row = {"n_cols": len(cols), "has_col": 1 if column in cols else 0}
        else:
            self.executed.append((sql, params))
        return self

    def fetchone(self):
        return self._row


def _wrap(catalog):
    fake = _FakeCursor(catalog)
    return _SkipNoopColumnDDL(fake), fake


class TestSkipNoopColumnDDL(unittest.TestCase):

    def test_add_column_skipped_when_present(self):
        c, fake = _wrap({"fein_domain_map": {"kg_checked"}})
        c.execute("ALTER TABLE fein_domain_map ADD COLUMN IF NOT EXISTS kg_checked BOOLEAN NOT NULL DEFAULT FALSE")
        self.assertEqual(fake.executed, [])

    def test_add_column_runs_when_missing(self):
        c, fake = _wrap({"fein_domain_map": {"employer_fein"}})
        sql = "ALTER TABLE fein_domain_map ADD COLUMN IF NOT EXISTS kg_checked BOOLEAN"
        c.execute(sql)
        self.assertEqual(fake.executed, [(sql, None)])

    def test_multiline_statement_matched(self):
        c, fake = _wrap({"uscis_dol_fuzzy_map": {"candidates_json"}})
        c.execute("""
            ALTER TABLE uscis_dol_fuzzy_map
            ADD COLUMN IF NOT EXISTS candidates_json JSONB
        """)
        self.assertEqual(fake.executed, [])

    def test_case_insensitive(self):
        c, fake = _wrap({"dol_h1b_employers": {"poc_email_domain"}})
        c.execute("alter table DOL_H1B_EMPLOYERS add column if not exists POC_EMAIL_DOMAIN text")
        self.assertEqual(fake.executed, [])

    def test_multi_column_statement_runs_unchanged(self):
        c, fake = _wrap({"model_usage": {"user_id", "use_case"}})
        sql = """
            ALTER TABLE model_usage
              ADD COLUMN IF NOT EXISTS user_id INT,
              ADD COLUMN IF NOT EXISTS use_case TEXT
        """
        c.execute(sql)
        self.assertEqual(fake.executed, [(sql, None)])

    def test_drop_column_skipped_when_absent(self):
        c, fake = _wrap({"h1b_ats_discovery": {"employer_fein"}})
        c.execute("ALTER TABLE h1b_ats_discovery DROP COLUMN IF EXISTS careers_url")
        self.assertEqual(fake.executed, [])

    def test_drop_column_runs_when_present(self):
        c, fake = _wrap({"h1b_ats_discovery": {"employer_fein", "careers_url"}})
        sql = "ALTER TABLE h1b_ats_discovery DROP COLUMN IF EXISTS careers_url"
        c.execute(sql)
        self.assertEqual(fake.executed, [(sql, None)])

    def test_drop_column_on_missing_table_still_runs(self):
        # Original ALTER would raise for a missing table; skipping would hide that.
        c, fake = _wrap({})
        sql = "ALTER TABLE no_such_table DROP COLUMN IF EXISTS x"
        c.execute(sql)
        self.assertEqual(fake.executed, [(sql, None)])

    def test_other_statements_and_params_pass_through(self):
        c, fake = _wrap({})
        c.execute("SELECT pg_advisory_xact_lock(7387641)")
        c.execute("INSERT INTO t (a) VALUES (%s)", (1,))
        c.execute("ALTER TABLE applications DROP CONSTRAINT IF EXISTS applications_job_url_key")
        self.assertEqual(len(fake.executed), 3)

    def test_non_execute_attributes_delegate(self):
        c, fake = _wrap({})
        fake.rowcount = 7
        self.assertEqual(c.rowcount, 7)


if __name__ == "__main__":
    unittest.main()
