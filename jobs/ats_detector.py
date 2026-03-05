# jobs/ats_detector.py — Auto-detect ATS platform per company
# Tries all supported ATS in order, stores result in DB

import json
from db.connection import get_conn
from jobs.ats import greenhouse, lever, ashby, smartrecruiters, workday


ATS_ORDER = [
    ("greenhouse",      greenhouse),
    ("lever",           lever),
    ("ashby",           ashby),
    ("smartrecruiters", smartrecruiters),
    ("workday",         workday),
]


def detect_ats(company):
    """
    Auto-detect ATS platform for a company.
    Tries all platforms in order, stores result in DB.

    Returns dict:
      {"ats_platform": "greenhouse", "ats_slug": "stripe"}
      or {"ats_platform": "unknown", "ats_slug": None}
    """
    print(f"   [INFO] Detecting ATS for {company}...")

    for platform_name, module in ATS_ORDER:
        try:
            slug_info, jobs = module.detect(company)
            if slug_info is None:
                continue

            # For Workday, slug_info is a dict
            if isinstance(slug_info, dict):
                slug_str = json.dumps(slug_info)
            else:
                slug_str = slug_info

            # Valid detection — store in DB
            _store_detection(company, platform_name, slug_str)

            if jobs == []:
                print(f"   [OK] {company} → {platform_name} "
                      f"(slug: {slug_str}) — no jobs currently")
            else:
                print(f"   [OK] {company} → {platform_name} "
                      f"(slug: {slug_str}) — {len(jobs)} jobs")

            return {
                "ats_platform": platform_name,
                "ats_slug":     slug_str,
            }

        except Exception as e:
            print(f"   [WARNING] {platform_name} detection "
                  f"failed for {company}: {e}")
            continue

    # No ATS detected
    _store_detection(company, "unknown", None)
    print(f"   [WARNING] {company} — ATS not detected")
    return {"ats_platform": "unknown", "ats_slug": None}


def _store_detection(company, platform, slug):
    """
    Store ATS detection result in prospective_companies table.
    Always resets consecutive_empty_days to 0 after successful
    detection — prevents repeated re-detection for companies
    genuinely on a hiring freeze (0 jobs but correct ATS).
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


def needs_redetection(company_row, redetect_days=14):
    """
    Check if company needs ATS re-detection.

    Triggers:
      1. ats_platform = 'unknown' (retry weekly via consecutive empty days)
      2. consecutive_empty_days >= redetect_days
      3. ats_detected_at is NULL (never detected)
    """
    platform = company_row.get("ats_platform", "unknown")
    slug = company_row.get("ats_slug")
    empty_days = company_row.get("consecutive_empty_days", 0) or 0

    if not platform or platform == "unknown":
        return True
    if not slug:
        return True
    if empty_days >= redetect_days:
        return True
    return False


def get_ats_module(platform):
    """Return ATS module for given platform name."""
    modules = {
        "greenhouse":      greenhouse,
        "lever":           lever,
        "ashby":           ashby,
        "smartrecruiters": smartrecruiters,
        "workday":         workday,
    }
    return modules.get(platform)