import logging

logger = logging.getLogger(__name__)

from jobs.ats_verifier import _name_matches

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



def _enrich_slug_on_demand(platform, slug):
    """
    Enrich a single slug on-demand during P1 detection.
    Called when slug is found in DB but company_name is NULL.

    Returns:
        str  — company name if enrichment succeeded
        ""   — enrichment returned no name (ambiguous)
        None — enrichment failed (404/error → slug invalid)
    """
    try:
        from enrich_ats_companies import ENRICHERS
        enricher = ENRICHERS.get(platform)
        if not enricher:
            return ""  # no enricher → can't validate → accept

        data, status = enricher(slug)

        if status == "inactive":
            # 404 → slug genuinely doesn't exist → delete from DB
            from db.ats_companies import delete_company
            delete_company(platform, slug)
            return None

        if status == "skip":
            # Enricher skipped — can't validate → reject
            return None

        if status == "ok" and data:
            name = data.get("company_name", "")
            # Save enrichment to DB regardless of whether name matches
            # The slug IS valid — just for a different company
            from db.ats_companies import upsert_company
            upsert_company(
                platform=platform,
                slug=slug,
                company_name=name,
                website=data.get("website"),
                job_count=data.get("job_count"),
            )
            return name  # caller decides if name matches target

        return ""  # error/skip → can't validate → accept

    except Exception as e:
        logger.debug(
            "On-demand enrichment failed for %s/%s: %s",
            platform, slug, e
        )
        return ""  # enrichment error → can't validate → accept


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

        # Strategy 2: Slug-based lookup with company name validation
        candidates = slugify(company)
        platforms  = ["greenhouse", "lever", "ashby",
                      "smartrecruiters", "icims",
                      "workday", "oracle_hcm"]

        for platform in platforms:
            for slug in candidates:
                row = find_by_slug(platform, slug)
                if not row or not row.get("is_active", 1):
                    continue

                # Validate slug matches our target company
                if not validate_slug_for_company(slug, company):
                    continue

                # Validate company name before accepting slug match
                db_name   = row.get("company_name", "")
                is_enrich = row.get("is_enriched", 0)

                if is_enrich and db_name:
                    # Already enriched — validate name matches
                    if not _name_matches(db_name, company, slug):
                        logger.debug(
                            "P1 slug match rejected (enriched): "
                            "slug=%s db_name=%s company=%s",
                            slug, db_name, company
                        )
                        continue
                else:
                    # Not enriched — enrich on-demand before accepting
                    # This prevents wrong matches for unenriched slugs
                    # e.g. "capital" could be Capital One OR Capital Group
                    enriched_name = _enrich_slug_on_demand(
                        platform, slug
                    )
                    if enriched_name:
                        if not _name_matches(enriched_name, company, slug):
                            logger.debug(
                                "P1 slug match rejected (on-demand): "
                                "slug=%s enriched=%s company=%s",
                                slug, enriched_name, company
                            )
                            continue
                    # If enrichment fails (404/error) → skip slug
                    elif enriched_name is None:
                        logger.debug(
                            "P1 slug enrichment failed: slug=%s "
                            "platform=%s — skipping",
                            slug, platform
                        )
                        continue
                    # enriched_name == "" means API returned no name
                    # → fall through and accept (can't validate)

                return {
                    "platform": platform,
                    "slug":     slug,
                }

        return None

    except Exception as e:
        logger.debug(
            "DB access failed, falling through to Phase 2: %s",
            e, exc_info=True
        )
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
    except Exception as e:
        logger.error(
            "Failed to check discovery DB in _db_exists: %s",
            e, exc_info=True
        )
        return False