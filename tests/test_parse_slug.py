"""parse_slug must give every scan path the same slug, including for manual-override blobs."""
import json
from unittest.mock import patch

import pytest

from jobs import job_monitor
from jobs.ats import registry, smartrecruiters
from jobs.ats.registry import ATS_REGISTRY, get_config, parse_slug

STRING_PLATFORMS = sorted(p for p, c in ATS_REGISTRY.items() if c.get("slug_type") != "json")
JSON_PLATFORMS = sorted(p for p, c in ATS_REGISTRY.items() if c.get("slug_type") == "json")


def _override_blob(platform, slug):
    """Exactly what jobs.ats_detector.override_ats stores for a bare slug."""
    return json.dumps({"_manual": True, "_platform": platform, "slug": slug})


@pytest.mark.parametrize("platform", STRING_PLATFORMS)
def test_manual_blob_is_unwrapped_for_every_string_platform(platform):
    assert parse_slug(platform, _override_blob(platform, "acme"), get_config(platform)) == "acme"


@pytest.mark.parametrize("platform", STRING_PLATFORMS)
def test_plain_slug_is_unchanged(platform):
    assert parse_slug(platform, "acme", get_config(platform)) == "acme"


@pytest.mark.parametrize("raw", ["{not json", '{"_manual": true}', '{"slug": ""}', '{"slug": 5}', "[1]"])
def test_unusable_blob_is_passed_through(raw):
    assert parse_slug("greenhouse", raw, get_config("greenhouse")) == raw


@pytest.mark.parametrize("platform", STRING_PLATFORMS)
def test_job_monitor_parses_the_same_as_registry(platform):
    raw = _override_blob(platform, "acme")
    cfg = get_config(platform)
    assert job_monitor._parse_slug(platform, raw, cfg) == parse_slug(platform, raw, cfg)


@pytest.mark.parametrize("platform", JSON_PLATFORMS)
def test_json_platforms_still_return_the_dict(platform):
    raw = json.dumps({"slug": "acme", "_manual": True})
    out = parse_slug(platform, raw, get_config(platform))
    assert isinstance(out, dict) and out["slug"] == "acme"


def test_smartrecruiters_url_gets_the_bare_slug_from_a_manual_blob():
    seen = []
    def fake(url, **kw):
        seen.append(url)
        return {"content": [], "totalFound": 0}
    slug = parse_slug("smartrecruiters", _override_blob("smartrecruiters", "westerndigital"),
                      get_config("smartrecruiters"))
    with patch.object(smartrecruiters, "fetch_json", side_effect=fake):
        smartrecruiters.fetch_jobs(slug, "Western Digital")
    assert seen and all("/companies/westerndigital/" in u for u in seen)
