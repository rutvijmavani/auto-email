"""Override-time slug validation: a hand-written slug missing keys its platform needs is refused."""
import json
import unittest
from unittest import mock

from db.company_ats_admin import validate_platform_slug
from jobs.ats.registry import ATS_REGISTRY, SLUG_REQUIRED_KEYS


FULL = {
    "workday":        {"slug": "acme", "wd": "wd5"},
    "oracle_hcm":     {"slug": "acme"},
    "successfactors": {"slug": "acme", "dc": "dc4", "region": "eu"},
    "avature":        {"base": "https://acme.avature.net"},
    "phenom":         {"base": "https://jobs.acme.com"},
    "talentbrew":     {"base": "https://jobs.acme.com"},
    "sitemap":        {"url": "https://acme.com/sitemap.xml"},
    "taleo":          {"company": "acme"},
    "eightfold":      {"slug": "acme"},
    "paycom":         {"host": "www.paycomonline.net", "key": "abc"},
    "google":         {"any": "thing"},
}


class TestRequiredKeysTable(unittest.TestCase):
    def test_every_json_platform_but_custom_has_an_entry(self):
        json_platforms = {p for p, c in ATS_REGISTRY.items()
                          if c.get("slug_type") == "json" and p != "custom"}
        self.assertEqual(json_platforms, set(SLUG_REQUIRED_KEYS))

    def test_full_fixture_covers_table(self):
        self.assertEqual(set(FULL), set(SLUG_REQUIRED_KEYS))


class TestValidatePlatformSlug(unittest.TestCase):
    def test_full_slug_accepted_for_every_json_platform(self):
        for platform, obj in FULL.items():
            slug, err = validate_platform_slug(platform, json.dumps(obj))
            self.assertIsNone(err, platform)
            self.assertIsNotNone(slug)

    def test_bare_slug_rejected_for_every_json_platform(self):
        for platform in FULL:
            _, err = validate_platform_slug(platform, "acme")
            self.assertIsNotNone(err, platform)

    def test_each_missing_key_rejected(self):
        for platform, keys in SLUG_REQUIRED_KEYS.items():
            for key in keys:
                obj = {k: v for k, v in FULL[platform].items() if k != key}
                _, err = validate_platform_slug(platform, json.dumps(obj))
                self.assertIn(key, err or "", f"{platform} missing {key}")

    def test_google_accepts_empty_object(self):
        slug, err = validate_platform_slug("google", "{}")
        self.assertIsNone(err)

    def test_string_platform_unchanged(self):
        slug, err = validate_platform_slug("greenhouse", "acme")
        self.assertEqual((slug, err), ("acme", None))


class TestOverrideAtsRefuses(unittest.TestCase):
    def test_incomplete_slug_writes_nothing(self):
        from jobs import ats_detector
        with mock.patch.object(ats_detector, "get_conn") as gc:
            ok = ats_detector.override_ats("Acme", "workday", "acme")
        self.assertFalse(ok)
        gc.assert_not_called()

    def test_complete_slug_written(self):
        from jobs import ats_detector
        with mock.patch.object(ats_detector, "get_conn") as gc:
            ok = ats_detector.override_ats("Acme", "workday", '{"slug":"acme","wd":"wd5"}')
        self.assertTrue(ok)
        gc.return_value.execute.assert_called_once()

    def test_string_platform_bare_slug_written(self):
        from jobs import ats_detector
        with mock.patch.object(ats_detector, "get_conn") as gc:
            ok = ats_detector.override_ats("Acme", "greenhouse", "acme")
        self.assertTrue(ok)


if __name__ == "__main__":
    unittest.main()
