# jobs/ats_detector.py — ATS detection orchestrator
#
# Detection flow (4 phases):
#
#   Phase 1: Sitemap lookup (FREE, instant)
#     boards.greenhouse.io/sitemap.xml
#     jobs.lever.co/sitemap.xml
#     jobs.ashbyhq.com/sitemap.xml
#     → Covers ~60% of companies
#
#   Phase 2: ATS API name probe (FREE, ~50ms)
#     boards-api.greenhouse.io/v1/boards/{slug}
#     api.lever.co/v0/postings/{slug}
#     jobs.ashbyhq.com/api/posting-api/job-board/{slug}
#     api.smartrecruiters.com/v1/companies/{slug}
#     → Covers additional ~15%
#
#   Phase 3a: HTML + redirect scan (FREE, ~100ms)
#     GET company.com/careers allow_redirects=True
#     → Covers additional ~10%
#
#   Phase 3b: Serper API (2500 free credits)
#     "{company} site:myworkdayjobs.com"
#     "{company} site:fa.oraclecloud.com"
#     → Covers remaining ~10%
#
#   Phase 4: Unknown → store as unknown
#     Amazon/Apple/Google/Meta (custom ATS)
#     → ~5%

import json
import re
from db.connection import get_conn
from jobs.ats import greenhouse, lever, ashby, smartrecruiters, workday
from jobs.ats import oracle_hcm, icims
from config import (
    ATS_STATUS_DETECTED,
    ATS_STATUS_UNSUPPORTED,
    ATS_STATUS_UNKNOWN,
    ATS_STATUS_MANUAL,
    ATS_KEYWORD_STOP_WORDS,
    ATS_DATE_RELIABILITY,
    JOB_MONITOR_REDETECT_DAYS,
    KNOWN_CUSTOM_ATS,
)


class QuotaExhaustedException(Exception):
    """Raised when Serper API credits are exhausted."""
    pass


# Platform registry — used by job_monitor for fetching jobs
ATS_REGISTRY = {
    "greenhouse":      greenhouse,
    "lever":           lever,
    "ashby":           ashby,
    "smartrecruiters": smartrecruiters,
    "workday":         workday,
    "oracle_hcm":      oracle_hcm,
    "icims":           icims,
}

ATS_STATUS_CUSTOM = "custom"


# ─────────────────────────────────────────
# KEYWORD EXTRACTION
# ─────────────────────────────────────────

def _get_keywords(company):
    """
    Extract significant keywords from company name.
    "Capital One"     -> ["capital", "one"]
    "JPMorgan Chase"  -> ["jpmorgan", "chase"]
    "AT&T"            -> ["at"]
    """
    name     = company.lower().strip()
    name     = re.sub(r"[^a-z0-9\s]", " ", name)
    tokens   = name.split()
    keywords = [
        t for t in tokens
        if t not in ATS_KEYWORD_STOP_WORDS and len(t) >= 2
    ]
    return keywords if keywords else [re.sub(r"[^a-z0-9]", "", name)]


# ─────────────────────────────────────────
# MAIN DETECTION
# ─────────────────────────────────────────

def detect_ats(company, domain=None, page=None, sb=None):
    """
    Detect ATS for a company using 4-phase approach.

    Args:
        company: company name (e.g. "Capital One")
        domain:  company domain for Phase 3a (e.g. "capitalone.com")
        page:    unused (kept for backward compatibility)
        sb:      unused (kept for backward compatibility)

    Returns dict:
    {
        company, status, platform, slug,
        ats_platform, ats_slug  (legacy keys)
    }
    """
    print(f"   [INFO] Detecting ATS for {company}...")

    # Phase 1: Sitemap lookup
    print("   [P1] Sitemap lookup...")
    from jobs.ats_sitemap import detect_via_sitemap
    result = detect_via_sitemap(company)
    if result:
        print(f"   [P1 HIT] {company} -> "
              f"{result['platform']} / {result['slug']}")
        return _store_and_return(company, result)

    # Phase 2: ATS API name probe
    print("   [P2] API name probe...")
    from jobs.ats_verifier import detect_via_api
    result = detect_via_api(company)
    if result:
        print(f"   [P2 HIT] {company} -> "
              f"{result['platform']} / {result['slug']}")
        return _store_and_return(company, result)

    # Phase 3a: HTML + redirect scan
    # Also runs Oracle HCM career page detection (documented pipeline)
    if domain:
        print(f"   [P3a] HTML redirect scan ({domain})...")

        # Oracle HCM: follow career page → extract oraclecloud URL → verify
        from jobs.ats.oracle_hcm import detect as oracle_detect
        oracle_slug = oracle_detect(company, domain)
        if oracle_slug:
            import json as _json
            result = {
                "platform": "oracle_hcm",
                "slug":     _json.dumps(oracle_slug),
            }
            print(f"   [P3a HIT] {company} -> oracle_hcm / {result['slug']}")
            return _store_and_return(company, result)

        from jobs.career_page import detect_via_career_page
        result = detect_via_career_page(company, domain)
        if result:
            print(f"   [P3a HIT] {company} -> "
                  f"{result['platform']} / {result['slug']}")
            return _store_and_return(company, result)

    # Phase 3b: Serper API (Workday + Oracle only)
    # Skip for companies known to use fully custom ATS
    if company in KNOWN_CUSTOM_ATS:
        print(f"   [CUSTOM] {company} uses custom ATS — skipping Serper")
        _store_detection(company, ATS_STATUS_CUSTOM, None)
        return {
            "company":      company,
            "status":       ATS_STATUS_CUSTOM,
            "platform":     ATS_STATUS_CUSTOM,
            "slug":         None,
            "ats_platform": ATS_STATUS_CUSTOM,
            "ats_slug":     None,
        }

    print("   [P3b] Serper API search...")
    from jobs.serper import detect_via_serper, SERPER_EXHAUSTED
    serper_result = detect_via_serper(company)

    if serper_result is SERPER_EXHAUSTED:
        print(f"   [WARNING] Serper credits exhausted — "
              f"storing as unknown, retry when credits available")
        raise QuotaExhaustedException("Serper credits exhausted")

    if serper_result:
        print(f"   [P3b HIT] {company} -> "
              f"{serper_result['platform']} / {serper_result['slug']}")
        return _store_and_return(company, serper_result)

    # Phase 4: Unknown
    print(f"   [UNKNOWN] {company} — no ATS found in any phase")
    _store_detection(company, ATS_STATUS_UNKNOWN, None)

    return {
        "company":      company,
        "status":       ATS_STATUS_UNKNOWN,
        "platform":     ATS_STATUS_UNKNOWN,
        "slug":         None,
        "ats_platform": ATS_STATUS_UNKNOWN,
        "ats_slug":     None,
    }


def _store_and_return(company, result):
    """
    Store detection result in:
    1. prospective_companies table (main pipeline DB)
    2. ats_discovery.db (self-populating reference data)
    """
    platform = result["platform"]
    slug     = result["slug"]

    if platform in ATS_REGISTRY:
        status = ATS_STATUS_DETECTED
        label  = "[OK]"
    else:
        status = ATS_STATUS_UNSUPPORTED
        label  = "[UNSUPPORTED]"
        print(f"   [INFO] {company} uses {platform} "
              f"(not yet supported — stored for future use)")

    # Store in main pipeline DB
    _store_detection(company, platform, slug)

    # Self-populate ats_discovery.db
    # This makes future Phase 1 lookups instant (no Serper needed)
    try:
        from db.ats_companies import mark_from_detection
        from db.schema_discovery import init_discovery_db
        init_discovery_db()

        # For Workday/Oracle: DB stores plain tenant slug not full JSON
        # e.g. {"slug":"nvidia","wd":"wd5"} → store as "nvidia"
        # so P1 slug lookup finds it correctly
        discovery_slug = slug
        if platform in ("workday", "oracle_hcm") and slug:
            try:
                parsed = json.loads(slug)
                discovery_slug = parsed.get("slug", slug)
            except (ValueError, TypeError):
                pass  # already plain string

        mark_from_detection(
            platform=platform,
            slug=discovery_slug,
            company_name=company,
        )
    except Exception as e:
        logger.error(
            "Failed to self-populate ats_discovery.db "
            "for %s/%s (%s): %s",
            platform, slug, company, e, exc_info=True
        )  # best-effort — never blocks detection

    print(f"   {label} {company} -> {platform} (slug: {slug})")

    return {
        "company":      company,
        "status":       status,
        "platform":     platform,
        "slug":         slug,
        "ats_platform": platform,
        "ats_slug":     slug,
    }


# ─────────────────────────────────────────
# SCORING (kept for legacy API buffer tests)
# ─────────────────────────────────────────

def _score_response(jobs, company):
    """Score API response confidence. Used by legacy tests."""
    import math
    from config import ATS_SAMPLE_SIZE

    job_count = len(jobs)
    if job_count == 0:
        return {"confidence": 40, "job_count": 0, "final_score": 0}

    keywords = _get_keywords(company)
    sample   = jobs[:ATS_SAMPLE_SIZE]
    matches  = 0

    for job in sample:
        text = " ".join(filter(None, [
            job.get("absolute_url",  ""),
            job.get("hostedUrl",     ""),
            job.get("jobUrl",        ""),
            job.get("externalUrl",   ""),
            job.get("title",         ""),
            job.get("text",          ""),
            job.get("name",          ""),
            job.get("_company_name", ""),
        ])).lower()

        if all(kw in text for kw in keywords):
            matches += 1

    confidence  = int((matches / len(sample)) * 100)
    final_score = round(confidence * math.log10(job_count + 1), 2)

    return {
        "confidence":  confidence,
        "job_count":   job_count,
        "final_score": final_score,
    }


def _tie_break(candidates):
    """Tie-break by date reliability."""
    def rank(e):
        try:
            return ATS_DATE_RELIABILITY.index(e["platform"])
        except ValueError:
            return len(ATS_DATE_RELIABILITY)
    return min(candidates, key=rank)


def _classify(buffer):
    """Classify buffer entries. Kept for legacy tests."""
    from config import (
        ATS_MIN_CONFIDENCE, ATS_DETECTION_THRESHOLD,
        ATS_CLOSE_CALL_GAP, ATS_STATUS_CLOSE_CALL,
    )

    if not buffer:
        return {
            "status": ATS_STATUS_UNKNOWN,
            "winner": None, "runner_up": None, "best_attempt": None,
        }

    viable = [
        e for e in buffer
        if e["confidence"] >= ATS_MIN_CONFIDENCE or e["job_count"] == 0
    ]

    if not viable:
        best = max(buffer, key=lambda x: x["final_score"])
        return {
            "status": ATS_STATUS_UNKNOWN,
            "winner": None, "runner_up": None, "best_attempt": best,
        }

    viable_sorted = sorted(
        viable,
        key=lambda x: (
            x["final_score"],
            -(ATS_DATE_RELIABILITY.index(x["platform"])
              if x["platform"] in ATS_DATE_RELIABILITY else 99),
        ),
        reverse=True,
    )

    winner    = viable_sorted[0]
    runner_up = viable_sorted[1] if len(viable_sorted) > 1 else None

    if (winner["job_count"] > 0 and
            winner["final_score"] < ATS_DETECTION_THRESHOLD):
        best = max(buffer, key=lambda x: x["final_score"])
        return {
            "status": ATS_STATUS_UNKNOWN,
            "winner": None, "runner_up": None, "best_attempt": best,
        }

    if (runner_up and runner_up["final_score"] > 0
            and winner["final_score"] > 0):
        gap_pct = (
            (winner["final_score"] - runner_up["final_score"])
            / winner["final_score"]
        ) * 100
        if gap_pct <= ATS_CLOSE_CALL_GAP:
            winner = _tie_break([winner, runner_up])
            runner_up = next(
                (e for e in viable_sorted if e != winner), None
            )
            return {
                "status":       ATS_STATUS_CLOSE_CALL,
                "winner":       winner,
                "runner_up":    runner_up,
                "best_attempt": None,
            }

    return {
        "status":       ATS_STATUS_DETECTED,
        "winner":       winner,
        "runner_up":    runner_up,
        "best_attempt": None,
    }


# ─────────────────────────────────────────
# DB OPERATIONS
# ─────────────────────────────────────────

def _store_detection(company, platform, slug):
    """Store ATS detection result. Resets consecutive_empty_days."""
    conn = get_conn()
    try:
        conn.execute("""
            UPDATE prospective_companies
            SET ats_platform           = ?,
                ats_slug               = ?,
                ats_detected_at        = CURRENT_TIMESTAMP,
                consecutive_empty_days = 0
            WHERE company = ?
        """, (platform, slug, company))
        conn.commit()
    finally:
        conn.close()


def override_ats(company, platform, slug):
    """
    Manually override ATS detection.
    Manual overrides are never auto-re-detected.
    """
    try:
        slug_data = json.loads(slug) if slug and slug.startswith("{") else {}
        slug_data["_manual"]   = True
        slug_data["_platform"] = platform
        if not slug_data.get("slug"):
            slug_data["slug"] = slug
        slug_str = json.dumps(slug_data)
    except Exception:
        slug_str = slug

    conn = get_conn()
    try:
        conn.execute("""
            UPDATE prospective_companies
            SET ats_platform           = ?,
                ats_slug               = ?,
                ats_detected_at        = CURRENT_TIMESTAMP,
                consecutive_empty_days = 0
            WHERE company = ?
        """, (platform, slug_str, company))
        conn.commit()
        print(f"[OK] {company} -> manually set to {platform} "
              f"(slug: {slug_str})")
    finally:
        conn.close()


def needs_redetection(company_row, redetect_days=14):
    """
    Check if company needs ATS re-detection.

    Never re-detects:
      → platform = "manual" (user override)
      → _manual flag in slug JSON
      → platform = "unsupported" (we know the ATS)

    Always re-detects:
      → platform is None/unknown/custom
      → ats_detected_at is None
      → consecutive_empty_days >= redetect_days
    """
    platform    = company_row.get("ats_platform", ATS_STATUS_UNKNOWN)
    slug        = company_row.get("ats_slug")
    empty_days  = company_row.get("consecutive_empty_days", 0) or 0
    detected_at = company_row.get("ats_detected_at")

    # Manual overrides never re-detected
    if platform == ATS_STATUS_MANUAL:
        return False
    if slug:
        try:
            slug_data = json.loads(slug)
            if slug_data.get("_manual"):
                return False
        except (ValueError, TypeError):
            pass

    # Unsupported but detected — never re-detect
    if platform == ATS_STATUS_UNSUPPORTED:
        return False

    # Unknown or custom — always retry
    if not platform or platform in (ATS_STATUS_UNKNOWN, "custom"):
        return True

    if not slug:
        return True
    if detected_at is None:
        return True
    if empty_days >= redetect_days:
        return True

    return False


def get_ats_module(platform):
    """Return ATS module for given platform name."""
    return ATS_REGISTRY.get(platform)