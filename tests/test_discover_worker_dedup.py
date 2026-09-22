"""
tests/test_discover_worker_dedup.py
─────────────────────────────────────────────────────────────────────────────
Covers the discover-worker phase-dedup fix (docs/enrichment_discovery_design.md
§4 "Processing Steps", agreed 2026-09-22):

  scripts/discover_h1b_ats.process_employer(known_careers_url=..., skip_phase6=...)
    · known_careers_url trusted as-is — Phase 3 (probe) and Phase 4 (Brave) never run
    · known_careers_url wins over a KG-found jobs_url (never overwritten)
    · skip_phase6=True skips Phase 6 (career_page scan) unconditionally
    · Phase 7 (career_detector BFS) still runs when platform is still unknown
    · a known_careers_url that itself matches a known ATS pattern short-circuits
      Phase 7 too (platform already known — no BFS needed)

This is a design/code-drift fix: domain_enrichment_worker always runs Phase 3, and
always runs Phase 6 unless Phase 3 found the ATS — so by the time a FEIN reaches
discover_h1b_ats_worker with already_has_ats=False, Phase 3/6 are guaranteed to have
already run and failed against the identical domain. Redoing them here is 100%
duplicate, wasted network work.
"""

import os
import sys
import unittest
from unittest.mock import MagicMock, patch

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from scripts.discover_h1b_ats import process_employer


def _emp():
    return {
        "employer_fein":   "12-3456789",
        "employer_name":   "Acme Corp",
        "assigned_domain": "acme.com",
        "total_approvals": 5,
    }


def _prefetched(jobs_url=None):
    """No KG jobs_url by default — isolates the known_careers_url priority logic."""
    return {
        "canonical_name":   "Acme Corp",
        "canonical_source": "regex",
        "website_url":      "https://acme.com",
        "kg_mid":           None,
        "wikidata_qid":     None,
        "jobs_url":         jobs_url,
        "glassdoor_id":     None,
        "crunchbase_id":    None,
    }


class TestKnownCareersUrlSkipsPhase3And4(unittest.TestCase):
    """known_careers_url set → Phase 3 probe and Phase 4 Brave never run."""

    @patch("scripts.discover_h1b_ats._upsert_company_ats")
    @patch("scripts.discover_h1b_ats.upsert_discovery")
    @patch("jobs.ats.career_detector.detect_company")
    @patch("scripts.discover_h1b_ats.brave_career_search")
    @patch("scripts.discover_h1b_ats.discover_careers_url")
    @patch("scripts.discover_h1b_ats._is_recently_checked", return_value=None)
    def test_probe_and_brave_never_called(
        self, _recent, mock_probe, mock_brave, mock_phase7, mock_upsert, mock_ats_upsert,
    ):
        mock_phase7.return_value = []  # Phase 7 still runs (no platform known yet)
        conn = MagicMock()

        result = process_employer(
            _emp(), conn, dry_run=False, force=True,
            prefetched=_prefetched(),
            known_careers_url="https://acme.com/careers",
            known_careers_source="phase3",
            skip_phase6=True,
        )

        mock_probe.assert_not_called()
        mock_brave.assert_not_called()
        self.assertEqual(result["careers_url"], "https://acme.com/careers")
        self.assertEqual(result["careers_source"], "phase3")

    @patch("scripts.discover_h1b_ats._upsert_company_ats")
    @patch("scripts.discover_h1b_ats.upsert_discovery")
    @patch("jobs.ats.career_detector.detect_company")
    @patch("scripts.discover_h1b_ats._is_recently_checked", return_value=None)
    def test_phase6_skipped_when_skip_phase6_true(
        self, _recent, mock_phase7, mock_upsert, mock_ats_upsert,
    ):
        mock_phase7.return_value = []
        conn = MagicMock()

        with patch("jobs.career_page.detect_via_career_page") as mock_phase6:
            process_employer(
                _emp(), conn, dry_run=False, force=True,
                prefetched=_prefetched(),
                known_careers_url="https://acme.com/careers",
                known_careers_source="phase3",
                skip_phase6=True,
            )
            mock_phase6.assert_not_called()

    @patch("scripts.discover_h1b_ats._upsert_company_ats")
    @patch("scripts.discover_h1b_ats.upsert_discovery")
    @patch("jobs.ats.career_detector.detect_company")
    @patch("scripts.discover_h1b_ats._is_recently_checked", return_value=None)
    def test_phase7_still_runs_when_platform_unknown(
        self, _recent, mock_phase7, mock_upsert, mock_ats_upsert,
    ):
        """Phase 7 is the only genuinely new work on the normal path — must still run."""
        mock_phase7.return_value = []
        conn = MagicMock()

        process_employer(
            _emp(), conn, dry_run=False, force=True,
            prefetched=_prefetched(),
            known_careers_url="https://acme.com/careers",  # plain URL, no ATS pattern hit
            known_careers_source="phase3",
            skip_phase6=True,
        )

        mock_phase7.assert_called_once()
        # Seeded with the known careers_url, per priority order
        self.assertEqual(mock_phase7.call_args.kwargs.get("seed_url"), "https://acme.com/careers")


class TestKnownCareersUrlOutranksKg(unittest.TestCase):
    """known_careers_url must never be overwritten by KG's jobs_url."""

    @patch("scripts.discover_h1b_ats._upsert_company_ats")
    @patch("scripts.discover_h1b_ats.upsert_discovery")
    @patch("jobs.ats.career_detector.detect_company")
    @patch("scripts.discover_h1b_ats._is_recently_checked", return_value=None)
    def test_kg_jobs_url_does_not_overwrite(
        self, _recent, mock_phase7, mock_upsert, mock_ats_upsert,
    ):
        mock_phase7.return_value = []
        conn = MagicMock()

        result = process_employer(
            _emp(), conn, dry_run=False, force=True,
            prefetched=_prefetched(jobs_url="https://stale-kg-url.example.com/jobs"),
            known_careers_url="https://acme.com/careers",
            known_careers_source="phase3",
            skip_phase6=True,
        )

        self.assertEqual(result["careers_url"], "https://acme.com/careers")
        self.assertNotEqual(result["careers_url"], "https://stale-kg-url.example.com/jobs")


class TestKnownCareersUrlAtsPatternShortCircuitsPhase7(unittest.TestCase):
    """A known_careers_url that itself matches a known ATS pattern needs no BFS."""

    @patch("scripts.discover_h1b_ats._upsert_company_ats")
    @patch("scripts.discover_h1b_ats.upsert_discovery")
    @patch("jobs.ats.career_detector.detect_company")
    @patch("scripts.discover_h1b_ats._is_recently_checked", return_value=None)
    def test_ats_pattern_match_skips_phase7(
        self, _recent, mock_phase7, mock_upsert, mock_ats_upsert,
    ):
        conn = MagicMock()

        result = process_employer(
            _emp(), conn, dry_run=False, force=True,
            prefetched=_prefetched(),
            known_careers_url="https://boards.greenhouse.io/acme/jobs",
            known_careers_source="phase6",
            skip_phase6=True,
        )

        mock_phase7.assert_not_called()
        self.assertEqual(result["detected_platform"], "greenhouse")
        self.assertEqual(result["ats_source"], "phase6")


if __name__ == "__main__":
    unittest.main()
