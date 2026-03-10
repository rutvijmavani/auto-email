# jobs/ats_sitemap.py — Phase 1: ats_discovery.db lookup
#
# Queries ats_discovery.db for known company slugs.
# Falls through to Phase 2 on miss.
#
# Two lookup strategies:
#   1. Name match: company_name column verified against target company
#   2. Slug match: try slug variants, check if in DB
#
# DB self-populates from:
#   - build_ats_slug_list.py (Common Crawl, monthly)
#   - Successful Phase 2/3 detections (daily)
#   - enrich_ats_companies.py (API enrichment)

import os
from jobs.ats.base import slugify
from jobs.ats.patterns import validate_slug_for_company


def detect_via_sitemap(company):
    """
    Phase 1: Look up company in ats_discovery.db.

    Strategy 1: Search by company_name (enriched rows only)
    Strategy 2: Try slug variants, find in DB by slug

    Args:
        company: company name (e.g. "Stripe")

    Returns:
        {platform, slug} if found
        None             if not found or DB empty
    """
    # Check if discovery DB exists
    if not _db_exists():
        return None

    try:
        from db.ats_companies import find_company, find_by_slug

        # Strategy 1: Name-based lookup (most accurate)
        result = find_company(company)
        if result:
            return {
                "platform": result["platform"],
                "slug":     result["slug"],
            }

        # Strategy 2: Slug-based lookup
        candidates = slugify(company)
        platforms  = ["greenhouse", "lever", "ashby",
                      "smartrecruiters", "icims",
                      "workday", "oracle_hcm"]

        for platform in platforms:
            for slug in candidates:
                row = find_by_slug(platform, slug)
                if row and row.get("is_active", 1):
                    if validate_slug_for_company(slug, company):
                        return {
                            "platform": platform,
                            "slug":     slug,
                        }

        return None

    except Exception:
        # DB access failure — fall through to Phase 2
        return None


def clear_sitemap_cache():
    """No-op — kept for backward compatibility."""
    print("[INFO] ats_discovery.db used instead of cache files")


def _db_exists():
    """Check if ats_discovery.db exists and has data."""
    from db.schema_discovery import DISCOVERY_DB
    if not os.path.exists(DISCOVERY_DB):
        return False
    # Check it has at least one row
    try:
        from db.ats_companies import get_total_count
        return get_total_count() > 0
    except Exception:
        return False