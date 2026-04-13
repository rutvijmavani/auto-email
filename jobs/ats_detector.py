"""
jobs/ats_detector.py — ATS detection orchestrator.

Detection flow (4 phases):
  Phase 1: Sitemap lookup       (FREE, instant)
  Phase 2: ATS API name probe   (FREE, ~50ms)
  Phase 3a: HTML + redirect     (FREE, ~100ms)
  Phase 3b: Serper API          (2500 free credits)
  Phase 4: Unknown → store as unknown

custom platform:
  Companies with ats_platform='custom' and a valid ats_slug
  (curl captured via Google Form) are fully monitorable.
  They are NOT re-detected — their slug contains everything needed.
  Only custom companies with NO slug need re-detection.
"""

import json
import logging
import re
from db.connection import get_conn

from logger import get_logger
logger = get_logger(__name__)

from jobs.ats import greenhouse, lever, ashby, smartrecruiters, workday
from jobs.ats import oracle_hcm, icims, jobvite, avature
from jobs.ats import phenom, talentbrew, sitemap, successfactors, google
from jobs.ats import custom_career   # ← universal custom ATS engine
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


# ─────────────────────────────────────────
# PLATFORM REGISTRY
# ─────────────────────────────────────────

ATS_REGISTRY = {
    "greenhouse":      greenhouse,
    "lever":           lever,
    "ashby":           ashby,
    "smartrecruiters": smartrecruiters,
    "workday":         workday,
    "oracle_hcm":      oracle_hcm,
    "icims":           icims,
    "jobvite":         jobvite,
    "avature":         avature,
    "phenom":          phenom,
    "talentbrew":      talentbrew,
    "sitemap":         sitemap,
    "successfactors":  successfactors,
    "google":          google,
    "apple":           sitemap,       # Apple uses sitemap-based scraping
    "custom":          custom_career, # Universal custom ATS engine
}

ATS_STATUS_CUSTOM = "custom"


# ─────────────────────────────────────────
# KEYWORD EXTRACTION
# ─────────────────────────────────────────

def _get_keywords(company):
    """
    Extract significant keywords from company name.
    "Capital One"    -> ["capital", "one"]
    "JPMorgan Chase" -> ["jpmorgan", "chase"]
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
    """Detect ATS for a company using 4-phase approach."""
    logger.info("━━━ ATS detection: company=%r domain=%r", company, domain)
    print(f"   [INFO] Detecting ATS for {company}...")

    # Phase 1: Sitemap lookup
    print("   [P1] Sitemap lookup...")
    from jobs.ats_sitemap import detect_via_sitemap
    result = detect_via_sitemap(company)
    if result:
        logger.info("[P1 HIT] %r → %s / %s",
                    company, result["platform"], result["slug"])
        print(f"   [P1 HIT] {company} -> "
              f"{result['platform']} / {result['slug']}")
        return _store_and_return(company, result)

    # Phase 2: ATS API name probe
    print("   [P2] API name probe...")
    from jobs.ats_verifier import detect_via_api
    result = detect_via_api(company)
    if result:
        logger.info("[P2 HIT] %r → %s / %s",
                    company, result["platform"], result["slug"])
        print(f"   [P2 HIT] {company} -> "
              f"{result['platform']} / {result['slug']}")
        return _store_and_return(company, result)

    # Phase 3a: HTML + redirect scan
    if domain:
        print(f"   [P3a] HTML redirect scan ({domain})...")
        from jobs.career_page import detect_via_career_page
        result = detect_via_career_page(company, domain)
        if result:
            logger.info("[P3a HIT] %r → %s / %s",
                        company, result["platform"], result["slug"])
            print(f"   [P3a HIT] {company} -> "
                  f"{result['platform']} / {result['slug']}")
            return _store_and_return(company, result)
    else:
        logger.warning("[P3a SKIP] No domain for %r", company)

    # Phase 3b: Serper API
    if company in KNOWN_CUSTOM_ATS:
        logger.info("[CUSTOM] %r uses custom ATS — skipping Serper", company)
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
        logger.warning("[P3b] Serper credits exhausted for %r", company)
        print("[WARNING] Serper credits exhausted")
        raise QuotaExhaustedException("Serper credits exhausted")

    if serper_result:
        logger.info("[P3b HIT] %r → %s / %s",
                    company, serper_result["platform"], serper_result["slug"])
        print(f"   [P3b HIT] {company} -> "
              f"{serper_result['platform']} / {serper_result['slug']}")
        return _store_and_return(company, serper_result)

    # Phase 4: Unknown
    logger.warning("[UNKNOWN] %r — no ATS found", company)
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
    """Store detection result and self-populate ats_discovery.db."""
    platform = result["platform"]
    slug     = result["slug"]

    if platform in ATS_REGISTRY:
        status = ATS_STATUS_DETECTED
        label  = "[OK]"
    else:
        status = ATS_STATUS_UNSUPPORTED
        label  = "[UNSUPPORTED]"
        print(f"   [INFO] {company} uses {platform} "
              f"(not yet supported)")

    _store_detection(company, platform, slug)

    try:
        from db.ats_companies import mark_from_detection
        from db.schema_discovery import init_discovery_db
        init_discovery_db()
        mark_from_detection(platform=platform, slug=slug,
                            company_name=company)
    except Exception as e:
        logger.error(
            "Failed to self-populate ats_discovery.db: %s", e,
            exc_info=True
        )

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
# NEEDS REDETECTION
# ─────────────────────────────────────────

def needs_redetection(company_row, redetect_days=14):
    """
    Check if company needs ATS re-detection.

    Rules:
      manual                          → False (permanent override)
      unsupported                     → False (out of scope)
      custom + valid slug (url field) → False (curl captured, ready)
      custom + no slug / invalid slug → True  (needs curl capture)
      unknown / no platform           → True
      no slug                         → True
      never detected                  → True
      14+ consecutive empty days      → True
    """
    platform    = company_row.get("ats_platform", ATS_STATUS_UNKNOWN)
    slug        = company_row.get("ats_slug")
    empty_days  = company_row.get("consecutive_empty_days", 0) or 0
    detected_at = company_row.get("ats_detected_at")
    company     = company_row.get("company", "?")

    # Manual override — never re-detect
    if platform == ATS_STATUS_MANUAL:
        logger.debug("needs_redetection: %r → False (manual)", company)
        return False

    # Check _manual flag in slug JSON
    if slug:
        try:
            slug_data = json.loads(slug)
            if slug_data.get("_manual"):
                logger.debug(
                    "needs_redetection: %r → False (_manual flag)",
                    company
                )
                return False
        except (ValueError, TypeError):
            pass

    # Unsupported — never re-detect
    if platform == ATS_STATUS_UNSUPPORTED:
        logger.debug(
            "needs_redetection: %r → False (unsupported)", company
        )
        return False

    # Custom platform — check if curl has been captured
    if platform == "custom":
        if slug:
            try:
                slug_data = json.loads(slug)
                if slug_data.get("url"):
                    # Valid curl config present — monitorable, no re-detection
                    logger.debug(
                        "needs_redetection: %r → False "
                        "(custom with valid slug)", company
                    )
                    return False
            except (ValueError, TypeError):
                pass
        # custom but no valid slug — needs curl to be captured
        logger.debug(
            "needs_redetection: %r → True "
            "(custom but no valid slug — needs curl capture)", company
        )
        return True

    # Unknown or missing
    if not platform or platform == ATS_STATUS_UNKNOWN:
        logger.debug(
            "needs_redetection: %r → True (platform=%s)",
            company, platform
        )
        return True

    if not slug:
        logger.debug("needs_redetection: %r → True (no slug)", company)
        return True

    if detected_at is None:
        logger.debug(
            "needs_redetection: %r → True (never detected)", company
        )
        return True

    if empty_days >= redetect_days:
        logger.debug(
            "needs_redetection: %r → True (empty_days=%d)",
            company, empty_days
        )
        return True

    logger.debug(
        "needs_redetection: %r → False (platform=%s empty_days=%d)",
        company, platform, empty_days
    )
    return False


# ─────────────────────────────────────────
# SCORING (legacy — kept for tests)
# ─────────────────────────────────────────

def _score_response(jobs, company):
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
    def rank(e):
        try:
            return ATS_DATE_RELIABILITY.index(e["platform"])
        except ValueError:
            return len(ATS_DATE_RELIABILITY)
    return min(candidates, key=rank)


def _classify(buffer):
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
            winner    = _tie_break([winner, runner_up])
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
    logger.debug("DB write: company=%r platform=%s", company, platform)
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
    """Manually override ATS detection. Never auto-re-detected."""
    logger.info("Manual override: %r → %s", company, platform)
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
        print(f"[OK] {company} -> manually set to {platform}")
    finally:
        conn.close()


def get_ats_module(platform):
    """Return ATS module for given platform name."""
    module = ATS_REGISTRY.get(platform)
    if not module:
        logger.warning("No ATS module for platform=%s", platform)
    return module