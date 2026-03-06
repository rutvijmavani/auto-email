# jobs/ats_detector.py — ATS detection via Google search
#
# Philosophy: Google already knows the correct ATS URL for every company.
# We search site-specific queries, extract and validate the URL, then store.
# If Google finds nothing → store as "custom" (out of scope for now).
#
# Simple. Reliable. No API guessing. No slug combinations.

import json
import re
from db.connection import get_conn
from jobs.ats import greenhouse, lever, ashby, smartrecruiters, workday
from jobs.ats import oracle_hcm
from config import (
    ATS_STATUS_DETECTED,
    ATS_STATUS_UNKNOWN,
    ATS_STATUS_MANUAL,
    ATS_KEYWORD_STOP_WORDS,
    ATS_DATE_RELIABILITY,
    JOB_MONITOR_REDETECT_DAYS,
)

# Platform registry — used by job_monitor for fetching jobs
ATS_REGISTRY = {
    "greenhouse":      greenhouse,
    "lever":           lever,
    "ashby":           ashby,
    "smartrecruiters": smartrecruiters,
    "workday":         workday,
    "oracle_hcm":      oracle_hcm,
}

# Status for companies using custom/unsupported ATS
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
    name   = company.lower().strip()
    name   = re.sub(r"[^a-z0-9\s]", " ", name)
    tokens = name.split()
    keywords = [
        t for t in tokens
        if t not in ATS_KEYWORD_STOP_WORDS and len(t) >= 2
    ]
    return keywords if keywords else [re.sub(r"[^a-z0-9]", "", name)]


# ─────────────────────────────────────────
# MAIN DETECTION
# ─────────────────────────────────────────

def detect_ats(company, page):
    """
    Detect ATS for a company via Google search.

    Args:
        company: company name string
        page:    Playwright page object (required)

    Returns dict:
    {
      company, status, platform, slug
    }

    status values:
      "detected" — found via Google ✓
      "custom"   — not found, uses custom ATS (out of scope)
    """
    print(f"   [INFO] Detecting ATS for {company}...")

    from jobs.google_detector import detect_via_google
    result = detect_via_google(company, page)

    if result:
        platform = result["platform"]
        slug     = result["slug"]
        _store_detection(company, platform, slug)

        print(
            f"   [OK] {company} -> {platform} "
            f"(slug: {slug})"
        )
        return {
            "company":  company,
            "status":   ATS_STATUS_DETECTED,
            "platform": platform,
            "slug":     slug,
        }

    # Not found on any supported ATS
    _store_detection(company, ATS_STATUS_CUSTOM, None)
    print(f"   [CUSTOM] {company} — not found on supported ATS platforms")

    return {
        "company":  company,
        "status":   ATS_STATUS_CUSTOM,
        "platform": ATS_STATUS_CUSTOM,
        "slug":     None,
    }


# ─────────────────────────────────────────
# SCORING (used by API verification layer)
# ─────────────────────────────────────────

def _score_response(jobs, company):
    """
    Score how well an API response matches the expected company.
    Used by API verification after Google detection.

    Formula: confidence% x log10(job_count + 1)
    """
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


# ─────────────────────────────────────────
# CLASSIFY / TIE-BREAK (kept for API fallback)
# ─────────────────────────────────────────

def _tie_break(candidates):
    """Tie-break by date reliability."""
    def rank(e):
        try:
            return ATS_DATE_RELIABILITY.index(e["platform"])
        except ValueError:
            return len(ATS_DATE_RELIABILITY)
    return min(candidates, key=rank)


def _classify(buffer):
    """Classify buffer entries into detected/close_call/unknown."""
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

    if winner["job_count"] > 0 and        winner["final_score"] < ATS_DETECTION_THRESHOLD:
        best = max(buffer, key=lambda x: x["final_score"])
        return {
            "status": ATS_STATUS_UNKNOWN,
            "winner": None, "runner_up": None, "best_attempt": best,
        }

    if runner_up and runner_up["final_score"] > 0        and winner["final_score"] > 0:
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
    """
    Store ATS detection result.
    Resets consecutive_empty_days to 0 after any detection attempt.
    """
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
    Called by --detect-ats "Company" --override platform slug.
    Manual overrides are never auto-re-detected.
    """
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
        print(f"[OK] {company} -> manually set to {platform} "
              f"(slug: {slug})")
    finally:
        conn.close()


def needs_redetection(company_row, redetect_days=14):
    """
    Check if company needs ATS re-detection.

    Triggers:
      1. ats_platform unknown/None/custom
      2. ats_slug None/empty (unless custom)
      3. ats_detected_at is None (never detected)
      4. consecutive_empty_days >= redetect_days

    Never re-detects:
      → ats_platform = "manual" (user override)
    """
    platform    = company_row.get("ats_platform", ATS_STATUS_UNKNOWN)
    slug        = company_row.get("ats_slug")
    empty_days  = company_row.get("consecutive_empty_days", 0) or 0
    detected_at = company_row.get("ats_detected_at")

    # Manual overrides never re-detected
    if platform == ATS_STATUS_MANUAL:
        return False
    # Custom ATS — retry after threshold (might have switched)
    if not platform or platform in (ATS_STATUS_UNKNOWN, ATS_STATUS_CUSTOM):
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