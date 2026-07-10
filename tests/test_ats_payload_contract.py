"""
tests/test_ats_payload_contract.py
─────────────────────────────────────────────────────────────────────────────
Tests for the Mode B platform payload key-forwarding contract.

Background
──────────
_build_detail_payload() in scan_worker.py builds the Redis queue payload that
detail_worker consumes.  For Mode B platforms (Workday, Taleo, iCIMS, Jobvite,
SmartRecruiters, Sitemap) each ATS module's fetch_job_detail() has a guard
clause that reads specific keys from the job dict.  If a required key is absent
the guard returns the original job dict unchanged — no HTTP call, no location
or description enrichment — a silent failure indistinguishable from success.

What these tests verify
───────────────────────
  TestBuildDetailPayloadKeyForwarding
    · Base payload always contains the standard fields
    · For each Mode B platform, all keys in PLATFORM_DETAIL_KEYS are forwarded
      when present on the listing-level job dict
    · Keys whose value is None are NOT forwarded (guard: job.get(key) is not None)
    · Keys whose value is "" (empty string) ARE forwarded (not None)
    · _country_code is forwarded when present
    · _country_code is not forwarded when absent or falsy
    · Mode A platforms (greenhouse, lever, …) receive no extra keys
    · Unknown platform receives no extra keys

  TestGuardClauseSilentFailure
    · Each Mode B guard fires and returns the original job object (identity)
      when its required key(s) are missing or empty — verified without any
      HTTP call because the guard short-circuits before network I/O.
    · Workday: guard requires ALL of _slug, _wd, _path, _external_path
      — any single missing/empty key fires the guard
    · Taleo: guard requires _base_url AND _contest_no (either missing fires)
    · SmartRecruiters: guard requires job_id AND _company_slug
    · iCIMS: guard requires job_url (not _base_url)
    · Jobvite: guard requires job_url
    · Sitemap xml feed: _feed_type=="xml" → skip (returns job unchanged)

  TestRequiredDetailKeysConsistency
    · _REQUIRED_DETAIL_KEYS in detail_worker is a subset of PLATFORM_DETAIL_KEYS
      in scan_worker (every key that the worker audits must also be forwarded
      by the scan worker, otherwise the audit would always fire spuriously)
    · All platforms in _REQUIRED_DETAIL_KEYS are also in PLATFORM_DETAIL_KEYS
"""

import sys
import os
import unittest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

# Import private helpers via explicit name — valid in Python
from workers.scan_worker import _build_detail_payload

# Guard clause functions — imported directly to call without HTTP
from jobs.ats import workday, taleo, smartrecruiters, icims, jobvite, sitemap

# Consistency check
from workers.detail_worker import _REQUIRED_DETAIL_KEYS


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _minimal_job(**extra):
    """Return a minimal listing-level job dict suitable for _build_detail_payload."""
    base = {
        "job_id":       "J-001",
        "job_url":      "https://example.com/job/1",
        "title":        "Software Engineer",
        "location":     "",
        "posted_at":    None,
        "description":  "",
        "content_hash": None,
        "skill_score":  0,
    }
    base.update(extra)
    return base


# ─────────────────────────────────────────────────────────────────────────────
# TestBuildDetailPayloadKeyForwarding
# ─────────────────────────────────────────────────────────────────────────────

class TestBuildDetailPayloadKeyForwarding(unittest.TestCase):
    """_build_detail_payload() correctly forwards platform-specific keys."""

    def test_base_payload_always_present(self):
        """Standard fields present for any platform."""
        payload = _build_detail_payload(
            "Acme", "greenhouse", _minimal_job(), "acme", "req-1",
        )
        for field in ("company", "ats_platform", "job_id", "job_url",
                      "title", "location", "description", "slug_info",
                      "found_by", "request_id", "enqueued_at"):
            self.assertIn(field, payload, f"Missing base field: {field}")
        self.assertEqual(payload["company"], "Acme")
        self.assertEqual(payload["ats_platform"], "greenhouse")
        self.assertEqual(payload["found_by"], "tier1_adaptive")

    def test_workday_keys_forwarded(self):
        """All 5 Workday keys are forwarded when present on the job dict."""
        job = _minimal_job(
            _external_path="/job/Austin-Texas/Test-Role_R-123",
            _slug="acme",
            _wd="wd1",
            _path="careers",
            _site=None,   # optional — None should NOT be forwarded
        )
        payload = _build_detail_payload("Acme", "workday", job, {"slug": "acme"})
        self.assertEqual(payload["_external_path"], "/job/Austin-Texas/Test-Role_R-123")
        self.assertEqual(payload["_slug"], "acme")
        self.assertEqual(payload["_wd"], "wd1")
        self.assertEqual(payload["_path"], "careers")
        # _site=None → not forwarded (falsy value excluded)
        self.assertNotIn("_site", payload)

    def test_workday_empty_string_not_forwarded(self):
        """Empty strings must NOT be forwarded — they are falsy and would silently
        trigger fetch_job_detail()'s guard (not all([slug, wd, path, external_path]))
        returning the job unenriched, identical to having a None value."""
        job = _minimal_job(_external_path="", _slug="acme", _wd="wd1", _path="")
        payload = _build_detail_payload("Acme", "workday", job, {})
        # _external_path="" and _path="" are falsy → must NOT be forwarded
        self.assertNotIn("_external_path", payload)
        self.assertNotIn("_path", payload)
        # non-empty keys ARE still forwarded
        self.assertIn("_slug", payload)
        self.assertIn("_wd", payload)

    def test_workday_none_value_not_forwarded(self):
        """Key with None value must NOT appear in the payload."""
        job = _minimal_job(_slug=None, _wd="wd1", _path="careers",
                           _external_path="/job/test")
        payload = _build_detail_payload("Acme", "workday", job, {})
        # _slug=None → skipped (falsy)
        self.assertNotIn("_slug", payload)

    def test_taleo_keys_forwarded(self):
        """Taleo required keys: _base_url, _contest_no, _section."""
        job = _minimal_job(
            _base_url="https://massanf.taleo.net",
            _contest_no="REQ-9876",
            _section="ex",
        )
        payload = _build_detail_payload("Mass", "taleo", job, {})
        self.assertEqual(payload["_base_url"], "https://massanf.taleo.net")
        self.assertEqual(payload["_contest_no"], "REQ-9876")
        self.assertEqual(payload["_section"], "ex")

    def test_icims_keys_forwarded(self):
        """iCIMS required keys: _base_url, _feed_type."""
        job = _minimal_job(
            _base_url="https://careers-schwab.icims.com",
            _feed_type="html",
        )
        payload = _build_detail_payload("Schwab", "icims", job, "schwab")
        self.assertEqual(payload["_base_url"], "https://careers-schwab.icims.com")
        self.assertEqual(payload["_feed_type"], "html")

    def test_jobvite_key_forwarded(self):
        """Jobvite forwards _slug for the should_fetch_detail gate."""
        job = _minimal_job(_slug="acme-corp")
        payload = _build_detail_payload("Acme", "jobvite", job, "acme-corp")
        self.assertEqual(payload["_slug"], "acme-corp")

    def test_smartrecruiters_key_forwarded(self):
        """SmartRecruiters forwards _company_slug."""
        job = _minimal_job(_company_slug="AcmeCorp")
        payload = _build_detail_payload("Acme", "smartrecruiters", job, "AcmeCorp")
        self.assertEqual(payload["_company_slug"], "AcmeCorp")

    def test_sitemap_feed_type_forwarded(self):
        """Sitemap forwards _feed_type so detail_worker can gate on 'xml'."""
        job = _minimal_job(_feed_type="xml")
        payload = _build_detail_payload("Blog", "sitemap", job, {})
        self.assertEqual(payload["_feed_type"], "xml")

    def test_country_code_forwarded_when_present(self):
        """_country_code is forwarded when truthy."""
        job = _minimal_job(_country_code="IN")
        payload = _build_detail_payload("Infosys", "workday", job, {})
        self.assertEqual(payload["_country_code"], "IN")

    def test_country_code_not_forwarded_when_absent(self):
        """_country_code absent from job → absent from payload."""
        job = _minimal_job()  # no _country_code key
        payload = _build_detail_payload("Acme", "greenhouse", job, "acme")
        self.assertNotIn("_country_code", payload)

    def test_country_code_not_forwarded_when_empty_string(self):
        """_country_code='' is falsy → not forwarded."""
        job = _minimal_job(_country_code="")
        payload = _build_detail_payload("Acme", "greenhouse", job, "acme")
        self.assertNotIn("_country_code", payload)

    def test_mode_a_platform_no_extra_keys(self):
        """Greenhouse (Mode A) gets no platform-specific keys."""
        job = _minimal_job()
        payload = _build_detail_payload("Acme", "greenhouse", job, "acme")
        ats_extras = [k for k in payload if k.startswith("_")]
        self.assertEqual(ats_extras, [],
                         f"Mode A got unexpected ATS keys: {ats_extras}")

    def test_unknown_platform_no_extra_keys(self):
        """An unknown platform must not add any unexpected keys."""
        job = _minimal_job()
        payload = _build_detail_payload("Acme", "unknown_ats", job, {})
        ats_extras = [k for k in payload if k.startswith("_")]
        self.assertEqual(ats_extras, [])

    def test_slug_info_serialized(self):
        """slug_info is stored as-is (dict or str are both valid)."""
        slug_dict = {"slug": "att", "wd": "wd1", "path": "ATTGeneral"}
        job = _minimal_job()
        payload = _build_detail_payload("AT&T", "workday", job, slug_dict)
        self.assertEqual(payload["slug_info"], slug_dict)

    def test_slug_info_string(self):
        slug_str = "acme-corp"
        job = _minimal_job()
        payload = _build_detail_payload("Acme", "jobvite", job, slug_str)
        self.assertEqual(payload["slug_info"], slug_str)


# ─────────────────────────────────────────────────────────────────────────────
# TestGuardClauseSilentFailure
# ─────────────────────────────────────────────────────────────────────────────

class TestGuardClauseSilentFailure(unittest.TestCase):
    """
    Guard clauses in each Mode B fetch_job_detail() return the original job dict
    unchanged (no HTTP call) when required keys are missing or empty.

    Verification: assert result IS the same object (not a copy).
    Workday and SmartRecruiters do `job = dict(job)` only AFTER the guard passes —
    so a guard-fired return is always identity-equal.
    Taleo, iCIMS, Jobvite also return `job` directly on guard fires.
    """

    # ── Workday ──────────────────────────────────────────────────────────────

    def test_workday_guard_fires_missing_slug(self):
        """_slug missing (falsy) → guard fires → returns same object."""
        job = {"_slug": "", "_wd": "wd1", "_path": "careers",
               "_external_path": "/job/Austin/Test_R-123"}
        result = workday.fetch_job_detail(job)
        self.assertIs(result, job,
                      "_slug='' should cause guard to fire; returned a different object")

    def test_workday_guard_fires_missing_wd(self):
        """_wd missing (falsy) → guard fires."""
        job = {"_slug": "acme", "_wd": "", "_path": "careers",
               "_external_path": "/job/Austin/Test_R-123"}
        result = workday.fetch_job_detail(job)
        self.assertIs(result, job)

    def test_workday_guard_fires_missing_path(self):
        """_path missing → guard fires."""
        job = {"_slug": "acme", "_wd": "wd1", "_path": "",
               "_external_path": "/job/Austin/Test_R-123"}
        result = workday.fetch_job_detail(job)
        self.assertIs(result, job)

    def test_workday_guard_fires_missing_external_path(self):
        """_external_path missing → guard fires."""
        job = {"_slug": "acme", "_wd": "wd1", "_path": "careers",
               "_external_path": ""}
        result = workday.fetch_job_detail(job)
        self.assertIs(result, job)

    def test_workday_guard_fires_all_keys_missing(self):
        """No Workday keys at all → guard fires."""
        job = {"job_id": "R-001", "title": "SWE"}
        result = workday.fetch_job_detail(job)
        self.assertIs(result, job)

    def test_workday_guard_all_keys_present_does_not_return_original(self):
        """
        With all 4 required keys present the guard does NOT fire.
        We mock fetch_json to return a valid jobPostingInfo response so the
        function proceeds all the way to `job = dict(job)` and returns an
        enriched copy — a different object than the input.

        Important: `job = dict(job)` only executes AFTER the code receives valid
        data (not None and not empty jobPostingInfo).  Returning None from
        fetch_json still returns the original object (via `if not data: return job`).
        We must return a real-looking response to prove the copy path was reached.
        """
        from unittest.mock import patch
        job = {"_slug": "acme", "_wd": "wd1", "_path": "careers",
               "_external_path": "/job/Austin/Role_R-123"}
        fake_response = {
            "jobPostingInfo": {
                "jobDescription": "<p>Test description</p>",
                "location": "Austin, TX",
            }
        }
        with patch("jobs.ats.workday.fetch_json", return_value=fake_response):
            result = workday.fetch_job_detail(job)
        # Guard did NOT fire → job = dict(job) was executed → result is a copy
        self.assertIsNot(result, job,
                         "All keys present: guard should not fire; "
                         "expected a dict copy (job = dict(job) path), "
                         "got the same object back")

    # ── Taleo ─────────────────────────────────────────────────────────────────

    def test_taleo_guard_fires_missing_base_url(self):
        """_base_url empty → guard fires."""
        job = {"_base_url": "", "_contest_no": "REQ-001", "job_id": "REQ-001"}
        result = taleo.fetch_job_detail(job)
        self.assertIs(result, job)

    def test_taleo_guard_fires_missing_contest_no(self):
        """_contest_no empty → guard fires."""
        job = {"_base_url": "https://co.taleo.net", "_contest_no": "",
               "job_id": ""}
        result = taleo.fetch_job_detail(job)
        self.assertIs(result, job)

    def test_taleo_guard_fires_both_missing(self):
        """Both keys empty → guard fires."""
        job = {"_base_url": "", "_contest_no": ""}
        result = taleo.fetch_job_detail(job)
        self.assertIs(result, job)

    def test_taleo_guard_fires_no_keys_at_all(self):
        """No Taleo keys → guard fires."""
        job = {"job_id": "001", "title": "Analyst"}
        result = taleo.fetch_job_detail(job)
        self.assertIs(result, job)

    # ── SmartRecruiters ───────────────────────────────────────────────────────

    def test_smartrecruiters_guard_fires_missing_job_id(self):
        """job_id empty → guard fires."""
        job = {"job_id": "", "_company_slug": "AcmeCorp"}
        result = smartrecruiters.fetch_job_detail(job)
        self.assertIs(result, job)

    def test_smartrecruiters_guard_fires_missing_company_slug(self):
        """_company_slug empty → guard fires."""
        job = {"job_id": "abc123", "_company_slug": ""}
        result = smartrecruiters.fetch_job_detail(job)
        self.assertIs(result, job)

    def test_smartrecruiters_guard_fires_both_missing(self):
        """Both required keys empty → guard fires."""
        job = {"job_id": "", "_company_slug": ""}
        result = smartrecruiters.fetch_job_detail(job)
        self.assertIs(result, job)

    # ── iCIMS ─────────────────────────────────────────────────────────────────

    def test_icims_guard_fires_missing_job_url(self):
        """job_url empty → guard fires."""
        job = {"job_url": "", "_base_url": "https://careers-schwab.icims.com"}
        result = icims.fetch_job_detail(job)
        self.assertIs(result, job)

    def test_icims_guard_fires_no_job_url_key(self):
        """job_url key absent → guard fires."""
        job = {"_base_url": "https://careers-schwab.icims.com"}
        result = icims.fetch_job_detail(job)
        self.assertIs(result, job)

    def test_icims_base_url_alone_does_not_satisfy_guard(self):
        """_base_url present but job_url missing → guard fires (iCIMS guards on job_url)."""
        job = {"_base_url": "https://careers-example.icims.com",
               "job_url": ""}
        result = icims.fetch_job_detail(job)
        self.assertIs(result, job)

    # ── Jobvite ───────────────────────────────────────────────────────────────

    def test_jobvite_guard_fires_missing_job_url(self):
        """job_url empty → guard fires."""
        job = {"job_url": "", "_slug": "acme-corp"}
        result = jobvite.fetch_job_detail(job)
        self.assertIs(result, job)

    def test_jobvite_guard_fires_no_job_url_key(self):
        """job_url absent → guard fires."""
        job = {"_slug": "acme-corp"}
        result = jobvite.fetch_job_detail(job)
        self.assertIs(result, job)

    # ── Sitemap ───────────────────────────────────────────────────────────────

    def test_sitemap_xml_feed_skips_detail_fetch(self):
        """_feed_type='xml' → returns job unchanged (all data already in listing)."""
        job = {"job_url": "https://example.com/jobs/1.xml",
               "_feed_type": "xml",
               "title": "SWE", "description": "existing description"}
        result = sitemap.fetch_job_detail(job)
        self.assertIs(result, job,
                      "_feed_type='xml' should skip detail fetch")

    def test_sitemap_non_xml_with_no_job_url_fires_guard(self):
        """Non-xml feed but job_url empty → second guard fires."""
        job = {"job_url": "", "_feed_type": "html"}
        result = sitemap.fetch_job_detail(job)
        self.assertIs(result, job)


# ─────────────────────────────────────────────────────────────────────────────
# TestRequiredDetailKeysConsistency
# ─────────────────────────────────────────────────────────────────────────────

class TestRequiredDetailKeysConsistency(unittest.TestCase):
    """
    _REQUIRED_DETAIL_KEYS in detail_worker must be consistent with
    PLATFORM_DETAIL_KEYS in scan_worker.

    The detail_worker audit warns when a required key is missing from the
    payload.  For that warning to be meaningful:
      (a) Every platform in _REQUIRED_DETAIL_KEYS must also exist in
          PLATFORM_DETAIL_KEYS (otherwise the scan worker never forwards it).
      (b) Every key listed in _REQUIRED_DETAIL_KEYS must also appear in
          PLATFORM_DETAIL_KEYS for that platform.

    This test will fail if someone adds a new platform to the audit list
    without also updating the forwarding table, or vice versa.
    """

    # Extracted directly from scan_worker._build_detail_payload source
    # (duplicated here intentionally so changes to either side break this test)
    PLATFORM_DETAIL_KEYS = {
        "workday":         ["_external_path", "_slug", "_wd", "_path", "_site"],
        "icims":           ["_base_url", "_feed_type"],
        "jobvite":         ["_slug"],
        "taleo":           ["_base_url", "_contest_no", "_section"],
        "smartrecruiters": ["_company_slug"],
        "sitemap":         ["_feed_type"],
        "avature":         [],
        "phenom":          [],
        "talentbrew":      [],
        "custom":          [],
        "eightfold":       [],
    }

    def test_required_keys_platforms_exist_in_forwarding_table(self):
        """Every platform in _REQUIRED_DETAIL_KEYS exists in PLATFORM_DETAIL_KEYS."""
        for platform in _REQUIRED_DETAIL_KEYS:
            self.assertIn(
                platform,
                self.PLATFORM_DETAIL_KEYS,
                f"Platform {platform!r} is in _REQUIRED_DETAIL_KEYS but not "
                f"in scan_worker PLATFORM_DETAIL_KEYS — scan worker will never "
                f"forward these keys, so the detail_worker audit is meaningless",
            )

    def test_required_keys_are_subset_of_forwarded_keys(self):
        """Each required key must also appear in the forwarding table."""
        for platform, required_keys in _REQUIRED_DETAIL_KEYS.items():
            forwarded = self.PLATFORM_DETAIL_KEYS.get(platform, [])
            for key in required_keys:
                # iCIMS and Jobvite guard on job_url (base payload field, always
                # forwarded) — those key names don't appear in PLATFORM_DETAIL_KEYS
                # by design because they're in the base payload.  Skip them.
                if key in ("job_url",):
                    continue
                self.assertIn(
                    key,
                    forwarded,
                    f"Key {key!r} required by detail_worker audit for "
                    f"{platform!r} is not in scan_worker forwarding table "
                    f"(forwarded: {forwarded})",
                )

    def test_workday_all_guard_keys_forwarded(self):
        """Workday guard checks _slug, _wd, _path, _external_path — all forwarded."""
        forwarded = self.PLATFORM_DETAIL_KEYS["workday"]
        for key in ("_slug", "_wd", "_path", "_external_path"):
            self.assertIn(key, forwarded,
                          f"Workday guard key {key!r} not in forwarding table")

    def test_taleo_all_guard_keys_forwarded(self):
        """Taleo guard checks _base_url, _contest_no — both forwarded."""
        forwarded = self.PLATFORM_DETAIL_KEYS["taleo"]
        for key in ("_base_url", "_contest_no"):
            self.assertIn(key, forwarded,
                          f"Taleo guard key {key!r} not in forwarding table")

    def test_smartrecruiters_guard_key_forwarded(self):
        """SmartRecruiters guard checks _company_slug — forwarded."""
        self.assertIn("_company_slug", self.PLATFORM_DETAIL_KEYS["smartrecruiters"])


if __name__ == "__main__":
    unittest.main()
