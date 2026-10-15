"""Tests for db/discover_priority.py — High/Maybe/Skip flag per FEIN."""
import os
import unittest

os.environ.setdefault("TEST_MODE", "1")

import db.db as db_module
from tests.conftest import cleanup_db
from db import discover_priority as dp


class TestDiscoverPriority(unittest.TestCase):
    def setUp(self):
        cleanup_db()
        db_module.init_db()

    def test_set_get_change_clear(self):
        self.assertIsNone(dp.get_level("12-345"))
        self.assertTrue(dp.set_level("12-345", dp.LEVEL_HIGH))
        self.assertEqual(dp.get_level("12-345"), "high")
        self.assertTrue(dp.set_level("12-345", dp.LEVEL_SKIP))
        self.assertEqual(dp.get_level("12-345"), "skip")
        self.assertTrue(dp.clear_level("12-345"))
        self.assertIsNone(dp.get_level("12-345"))

    def test_rejects_bad_level_and_empty_fein(self):
        self.assertFalse(dp.set_level("12-345", "unreviewed"))
        self.assertFalse(dp.set_level("12-345", "bogus"))
        self.assertFalse(dp.set_level("", dp.LEVEL_HIGH))
        self.assertFalse(dp.clear_level(""))
        self.assertIsNone(dp.get_level("12-345"))


if __name__ == "__main__":
    unittest.main()
