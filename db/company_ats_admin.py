"""
Manual maintenance of company_ats rows from the Discover page.

company_ats is the source of truth for companies shown on Discover. These helpers
let a human correct a row's platform/slug (edit in place), apply a pasted job URL
via the known URL patterns, and queue a worker re-detection.

Rules (agreed 2026-10-12):
  * An edit overwrites platform/slug in place on the row being viewed — no new row,
    no stale marker. stale_since and consecutive_empty_days are reset so the monitor
    scans the row again; reviewed_at is set so a worker redetect keeps the fix.
  * `custom` is never written from here — custom companies go through the
    prospective form.
  * UNIQUE(domain, platform) collisions are reported, never overwritten.
  * A pasted job URL that matches no known pattern writes nothing.
"""
import json

from config import DISCOVERY_REDETECT, ENRICHMENT_ON_DEMAND
from db.connection import get_conn
from jobs.ats.patterns import match_ats_pattern
from jobs.ats.registry import ATS_REGISTRY, SLUG_REQUIRED_KEYS

# Platforms that cannot be written from the Discover page.
_EXCLUDED_PLATFORMS = frozenset({"custom"})


def editable_platforms() -> list:
    """Registry platforms offered in the edit form (custom excluded)."""
    return sorted(p for p in ATS_REGISTRY if p not in _EXCLUDED_PLATFORMS)


def validate_platform_slug(platform: str, slug: str) -> "tuple[str | None, str | None]":
    """Return (clean_slug, error). Exactly one of the two is None.

    string platforms: any non-empty text.
    json platforms:   must parse to a JSON object holding every key the platform's
                      fetch_jobs needs (registry.SLUG_REQUIRED_KEYS) — _parse_slug
                      silently swaps a malformed JSON slug for defaults and a slug
                      missing a key scans nothing, so reject both here instead.
    """
    platform = (platform or "").strip()
    if platform in _EXCLUDED_PLATFORMS:
        return None, "Custom ATS is not supported here — add it through the prospective form."
    cfg = ATS_REGISTRY.get(platform)
    if not cfg:
        return None, f"Unsupported platform {platform!r}."
    slug = (slug or "").strip()
    if not slug:
        return None, "Slug is required."
    if cfg.get("slug_type") == "json":
        try:
            obj = json.loads(slug)
        except (json.JSONDecodeError, TypeError):
            return None, f"{platform} slug must be a JSON object, e.g. {{\"slug\": \"acme\"}}."
        required = SLUG_REQUIRED_KEYS.get(platform, ())
        if not isinstance(obj, dict):
            return None, f"{platform} slug must be a JSON object."
        missing = [k for k in required if not obj.get(k)]
        if missing:
            # fetch_jobs silently returns [] (or raises) without these — a row saved like
            # that would look enabled but never scan.
            return None, (f"{platform} slug JSON needs "
                          + ", ".join(f'"{k}"' for k in required)
                          + f" (missing: {', '.join(missing)}).")
    return slug, None


def match_job_url(job_url: str) -> "tuple[dict | None, str | None]":
    """Match a pasted job URL against the known patterns. Returns (hit, error)."""
    url = (job_url or "").strip()
    if not url.startswith(("http://", "https://")):
        return None, "Paste a full URL starting with https://"
    hit = match_ats_pattern(url)
    if not hit:
        return None, "That URL does not match any known ATS pattern — nothing was changed."
    _, err = validate_platform_slug(hit["platform"], hit["slug"])
    if err:
        return None, f"Matched {hit['platform']} but it cannot be monitored from here: {err}"
    return hit, None


def update_company_ats_row(ca_id: int, platform: str, slug: str,
                           enable: bool = False, conn=None) -> "tuple[bool, str]":
    """Overwrite platform/slug on one company_ats row in place.

    Returns (ok, message). Writes nothing on any validation or collision failure.
    enable=True also flips is_monitored on (the live registration sweep then
    picks up a never-scanned row within minutes).
    """
    clean, err = validate_platform_slug(platform, slug)
    if err:
        return False, err
    own = conn is None
    conn = conn or get_conn()
    try:
        cur = conn.cursor()
        cur.execute("SELECT domain, platform FROM company_ats WHERE id = %s", (ca_id,))
        row = cur.fetchone()
        if not row:
            return False, "Row no longer exists."
        row = dict(row)
        platform = platform.strip()
        if platform != row["platform"]:
            cur.execute(
                "SELECT 1 FROM company_ats WHERE domain = %s AND platform = %s AND id <> %s",
                (row["domain"], platform, ca_id),
            )
            if cur.fetchone():
                return False, f"This domain already has a {platform} row — edit that one instead."
        cur.execute(
            """
            UPDATE company_ats
            SET platform               = %s,
                slug                   = %s,
                stale_since            = NULL,
                consecutive_empty_days = 0,
                reviewed_at            = NOW(),
                is_monitored           = CASE WHEN %s THEN TRUE ELSE is_monitored END,
                source                 = 'manual'
            WHERE id = %s
            """,
            (platform, clean, enable, ca_id),
        )
        conn.commit()
        return True, f"Saved {platform} · {clean}"
    finally:
        if own:
            conn.close()


def insert_company_ats_row(fein: "str | None", domain: str, company_name: str,
                           platform: str, slug: str, enable: bool = False,
                           conn=None) -> "tuple[bool, str]":
    """Insert a manually confirmed row (used when a pasted job URL finds an ATS for a
    company that has no company_ats row yet). Reports a (domain, platform) collision
    instead of overwriting."""
    clean, err = validate_platform_slug(platform, slug)
    if err:
        return False, err
    domain = (domain or "").strip().lower()
    if not domain:
        return False, "No domain known for this company — cannot create a row."
    own = conn is None
    conn = conn or get_conn()
    try:
        cur = conn.cursor()
        platform = platform.strip()
        cur.execute("SELECT 1 FROM company_ats WHERE domain = %s AND platform = %s",
                    (domain, platform))
        if cur.fetchone():
            return False, f"This domain already has a {platform} row — edit that one instead."
        priority = 0
        if fein:
            cur.execute("SELECT petition_count FROM uscis_petition_counts "
                        "WHERE employer_fein = %s", (fein,))
            prow = cur.fetchone()
            priority = int(dict(prow)["petition_count"] or 0) if prow else 0
        cur.execute(
            """
            INSERT INTO company_ats
                (employer_fein, domain, company_name, platform, slug, source,
                 priority, is_monitored, reviewed_at)
            VALUES (%s, %s, %s, %s, %s, 'manual', %s, %s, NOW())
            """,
            (fein, domain, company_name, platform, clean, priority, enable),
        )
        conn.commit()
        return True, f"Added {platform} · {clean}"
    finally:
        if own:
            conn.close()


def queue_redetect(fein: str, r=None) -> "tuple[bool, str]":
    """Queue a forced ATS re-detection for a FEIN.

    Pushes to ENRICHMENT_ON_DEMAND (refreshes domain/careers URL) and to
    DISCOVERY_REDETECT with trigger "redetect" + source "company_ats" — the discover
    worker's _process_company forces a full re-probe for those values.
    """
    if not fein:
        return False, "This row has no FEIN — use a pasted job URL or edit the fields."
    if r is None:
        from workers.redis_client import get_redis
        r = get_redis()
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT COALESCE(SUM(petition_count), 0) AS n FROM uscis_petition_counts "
            "WHERE employer_fein = %s", (fein,),
        )
        row = cur.fetchone()
        score = int(dict(row)["n"]) if row else 0
    except Exception:
        score = 0
    finally:
        conn.close()
    r.lpush(ENRICHMENT_ON_DEMAND, json.dumps(
        {"fein": fein, "trigger": "on_demand", "source": None, "tier": "on_demand"}))
    r.zadd(DISCOVERY_REDETECT, {json.dumps(
        {"fein": fein, "trigger": "redetect", "source": "company_ats"}): score})
    return True, "Re-detection queued — refresh in a few minutes to see suggestions."
