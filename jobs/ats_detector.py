# jobs/ats_detector.py — ATS detection with confidence scoring buffer
#
# Philosophy: Never trust first match. Try ALL platforms x ALL slugs,
# score every response, pick the best one with enough confidence.
# Same buffer approach as recruiter domain validation.

import json
import math
import re
from db.connection import get_conn
from jobs.ats import greenhouse, lever, ashby, smartrecruiters, workday
from config import (
    ATS_DETECTION_THRESHOLD,
    ATS_MIN_CONFIDENCE,
    ATS_CLOSE_CALL_GAP,
    ATS_SAMPLE_SIZE,
    ATS_DATE_RELIABILITY,
    ATS_STATUS_DETECTED,
    ATS_STATUS_CLOSE_CALL,
    ATS_STATUS_UNKNOWN,
    ATS_STATUS_MANUAL,
    ATS_KEYWORD_STOP_WORDS,
)

# Platform registry
ATS_REGISTRY = {
    "greenhouse":      greenhouse,
    "lever":           lever,
    "ashby":           ashby,
    "smartrecruiters": smartrecruiters,
    "workday":         workday,
}


# ─────────────────────────────────────────
# KEYWORD EXTRACTION
# ─────────────────────────────────────────

def _get_keywords(company):
    """
    Extract significant keywords from company name.
    "Capital One"        -> ["capital", "one"]
    "JPMorgan Chase"     -> ["jpmorgan", "chase"]
    "AT&T"               -> ["att"]
    "Palo Alto Networks" -> ["palo", "alto"]
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
# RESPONSE SCORING
# ─────────────────────────────────────────

def _score_response(jobs, company):
    """
    Score how well an API response matches the expected company.

    Formula: confidence% x log10(job_count + 1)

    confidence% = (matching_jobs / sampled_jobs) x 100
    A job "matches" if ALL company keywords appear
    in its combined title + URL text.

    Empty jobs (valid ATS, hiring freeze):
      confidence = 50 (neutral)
      final_score = 0 (log10(1) = 0)

    Returns: {confidence, job_count, final_score}
    """
    job_count = len(jobs)

    if job_count == 0:
        return {"confidence": 50, "job_count": 0, "final_score": 0}

    keywords = _get_keywords(company)
    sample   = jobs[:ATS_SAMPLE_SIZE]
    matches  = 0

    for job in sample:
        text = " ".join(filter(None, [
            job.get("absolute_url", ""),
            job.get("hostedUrl",    ""),
            job.get("jobUrl",       ""),
            job.get("externalUrl",  ""),
            job.get("title",        ""),
            job.get("text",         ""),
            job.get("name",         ""),
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
# BUFFER COLLECTION
# ─────────────────────────────────────────

def _try_workday_slug(slug, wd):
    """Try a single Workday slug + wd variant. Returns (slug_info, jobs)."""
    from jobs.ats.base import fetch_json
    from jobs.ats.workday import BASE_URL
    url  = BASE_URL.format(slug=slug, wd=wd)
    data = fetch_json(url, params={"limit": 20, "offset": 0})
    if not data:
        return None, None
    jobs = data.get("jobPostings", [])
    if not isinstance(jobs, list):
        return None, None
    return {"slug": slug, "wd": wd}, jobs


def _try_all_platforms(company):
    """
    Try every ATS platform x every slug variant.
    Collect ALL valid responses into a buffer.

    Returns list of dicts:
    [{platform, slug, jobs, confidence, job_count, final_score}, ...]
    """
    from jobs.ats.base import slugify
    from jobs.ats.workday import WD_VARIANTS

    buffer        = []
    slug_variants = slugify(company)
    seen_slugs    = set()  # prevent re-trying same slug on same platform

    for platform_name in ATS_DATE_RELIABILITY:
        module = ATS_REGISTRY[platform_name]

        for slug in slug_variants:
            key = f"{platform_name}|{slug}"
            if key in seen_slugs:
                continue
            seen_slugs.add(key)

            try:
                if platform_name == "workday":
                    for wd in WD_VARIANTS:
                        wd_key = f"workday|{slug}|{wd}"
                        if wd_key in seen_slugs:
                            continue
                        seen_slugs.add(wd_key)

                        slug_info, jobs = _try_workday_slug(slug, wd)
                        if slug_info is None:
                            continue

                        score    = _score_response(jobs, company)
                        slug_str = json.dumps({"slug": slug, "wd": wd})
                        buffer.append({
                            "platform":    platform_name,
                            "slug":        slug_str,
                            "jobs":        jobs,
                            **score,
                        })
                        # Stop trying wd variants if high confidence found
                        if score["confidence"] >= ATS_MIN_CONFIDENCE:
                            break
                else:
                    slug_result, jobs = module.detect(slug)
                    if slug_result is None:
                        continue

                    # For Workday returned via detect() — shouldn't happen
                    # but handle gracefully
                    if isinstance(slug_result, dict):
                        slug_str = json.dumps(slug_result)
                    else:
                        slug_str = slug_result

                    score = _score_response(jobs, company)
                    buffer.append({
                        "platform":    platform_name,
                        "slug":        slug_str,
                        "jobs":        jobs,
                        **score,
                    })

            except Exception as e:
                print(f"      [WARNING] {platform_name}/{slug}: {e}")
                continue

    return buffer


# ─────────────────────────────────────────
# TIE BREAKING
# ─────────────────────────────────────────

def _tie_break(candidates):
    """
    Break tie between equally-scored candidates.
    Prefers most reliable date field:
      ashby > lever > workday > smartrecruiters > greenhouse
    """
    def reliability_rank(entry):
        try:
            return ATS_DATE_RELIABILITY.index(entry["platform"])
        except ValueError:
            return len(ATS_DATE_RELIABILITY)

    return min(candidates, key=reliability_rank)


# ─────────────────────────────────────────
# CLASSIFICATION
# ─────────────────────────────────────────

def _classify(buffer):
    """
    Classify buffer into: detected / close_call / unknown.

    Returns dict:
    {
      status:       "detected" | "close_call" | "unknown"
      winner:       entry | None
      runner_up:    entry | None
      best_attempt: entry | None  (for unknown case)
    }
    """
    if not buffer:
        return {
            "status":       ATS_STATUS_UNKNOWN,
            "winner":       None,
            "runner_up":    None,
            "best_attempt": None,
        }

    # Viable = meets min confidence OR empty (valid ATS structure)
    viable = [
        e for e in buffer
        if e["confidence"] >= ATS_MIN_CONFIDENCE or e["job_count"] == 0
    ]

    if not viable:
        best = max(buffer, key=lambda x: x["final_score"])
        return {
            "status":       ATS_STATUS_UNKNOWN,
            "winner":       None,
            "runner_up":    None,
            "best_attempt": best,
        }

    # Sort by final_score DESC, tie-break by reliability rank
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

    # Winner must meet score threshold if it has actual jobs
    if winner["job_count"] > 0 and        winner["final_score"] < ATS_DETECTION_THRESHOLD:
        best = max(buffer, key=lambda x: x["final_score"])
        return {
            "status":       ATS_STATUS_UNKNOWN,
            "winner":       None,
            "runner_up":    None,
            "best_attempt": best,
        }

    # Check for close call between top two
    if runner_up and runner_up["final_score"] > 0 and winner["final_score"] > 0:
        gap_pct = (
            (winner["final_score"] - runner_up["final_score"])
            / winner["final_score"]
        ) * 100

        if gap_pct <= ATS_CLOSE_CALL_GAP:
            # Tie-break by date reliability
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
# MAIN DETECTION
# ─────────────────────────────────────────

def detect_ats(company):
    """
    Detect ATS for a company using full confidence scoring buffer.

    Returns dict:
    {
      company, status, platform, slug,
      confidence, job_count,
      runner_up, best_attempt
    }
    """
    print(f"   [INFO] Detecting ATS for {company}...")

    buffer = _try_all_platforms(company)
    result = _classify(buffer)
    status = result["status"]
    winner = result.get("winner")

    if status in (ATS_STATUS_DETECTED, ATS_STATUS_CLOSE_CALL) and winner:
        _store_detection(
            company, winner["platform"], winner["slug"], status
        )
        label = "[OK]" if status == ATS_STATUS_DETECTED else "[CLOSE CALL]"
        print(
            f"   {label} {company} -> {winner['platform']} "
            f"(slug: {winner['slug']}) | "
            f"conf: {winner['confidence']}% | "
            f"jobs: {winner['job_count']}"
        )
    else:
        _store_detection(company, ATS_STATUS_UNKNOWN, None, ATS_STATUS_UNKNOWN)
        best = result.get("best_attempt")
        if best:
            print(
                f"   [UNKNOWN] {company} — best attempt: "
                f"{best['platform']} {best['slug']} "
                f"({best['confidence']}% conf, {best['job_count']} jobs)"
            )
        else:
            print(f"   [UNKNOWN] {company} — no viable ATS found")

    return {
        "company":      company,
        "status":       status,
        "platform":     winner["platform"] if winner else None,
        "slug":         winner["slug"]     if winner else None,
        "confidence":   winner["confidence"] if winner else 0,
        "job_count":    winner["job_count"]  if winner else 0,
        "runner_up":    result.get("runner_up"),
        "best_attempt": result.get("best_attempt"),
    }


# ─────────────────────────────────────────
# DB OPERATIONS
# ─────────────────────────────────────────

def _store_detection(company, platform, slug, status):
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
    Manually override ATS detection. Called by --override flag.
    Marks platform as 'manual' so it is never auto-re-detected.
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
        print(f"[OK] {company} -> manually set to {platform} (slug: {slug})")
    finally:
        conn.close()


def needs_redetection(company_row, redetect_days=14):
    """
    Check if company needs ATS re-detection.

    Triggers:
      1. ats_platform unknown/None/missing
      2. ats_slug None/empty
      3. ats_detected_at is None (never detected)
      4. consecutive_empty_days >= redetect_days
    Note: manual overrides are never re-detected.
    """
    platform    = company_row.get("ats_platform", ATS_STATUS_UNKNOWN)
    slug        = company_row.get("ats_slug")
    empty_days  = company_row.get("consecutive_empty_days", 0) or 0
    detected_at = company_row.get("ats_detected_at")

    # Manual overrides are never re-detected
    if platform == ATS_STATUS_MANUAL:
        return False
    if not platform or platform == ATS_STATUS_UNKNOWN:
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