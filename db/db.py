"""
db/db.py — Single entry point for all database operations.

All functions are imported from submodules.
External code continues to import from db.db unchanged.

Submodules:
    db.connection             → get_conn, DB_FILE, DAILY_LIMITS
    db.schema                 → init_db
    db.applications           → application helpers
    db.recruiters             → recruiter helpers
    db.application_recruiters → join table helpers
    db.outreach               → outreach helpers
    db.cache                  → ai_cache and job cache helpers
    db.quota                  → careershift and gemini quota helpers
    db.alerts                 → quota health alerts and coverage stats
"""

# ─────────────────────────────────────────
# CONNECTION
# ─────────────────────────────────────────
from db.connection import get_conn, DB_FILE, DAILY_LIMITS

# ─────────────────────────────────────────
# SCHEMA
# ─────────────────────────────────────────
from db.schema import init_db

# ─────────────────────────────────────────
# APPLICATIONS
# ─────────────────────────────────────────
from db.applications import (
    add_application,
    get_all_active_applications,
    get_application_by_id,
    mark_application_exhausted,
    mark_applications_exhausted,
    reactivate_application,
    update_application_expected_domain,
    get_existing_domain_for_company,
    convert_prospective_to_active,
)

# ─────────────────────────────────────────
# RECRUITERS
# ─────────────────────────────────────────
from db.recruiters import (
    get_recruiters_by_company,
    add_recruiter,
    update_recruiter,
    get_recruiters_by_tier,
    mark_recruiter_inactive,
    recruiter_email_exists,
    get_used_search_terms,
    mark_search_term_used,
    update_company_last_scraped,
)

# ─────────────────────────────────────────
# APPLICATION RECRUITERS
# ─────────────────────────────────────────
from db.application_recruiters import (
    link_recruiter_to_application,
    get_recruiters_for_application,
    get_unique_companies_needing_scraping,
    get_companies_needing_more_recruiters,
)

# ─────────────────────────────────────────
# OUTREACH
# ─────────────────────────────────────────
from db.outreach import (
    schedule_outreach,
    schedule_next_outreach,
    get_pending_outreach,
    mark_outreach_sent,
    mark_outreach_failed,
    mark_outreach_bounced,
    has_pending_or_sent_outreach,
)

# ─────────────────────────────────────────
# CACHE
# ─────────────────────────────────────────
from db.cache import (
    get_ai_cache,
    save_ai_cache,
    get_applications_missing_ai_cache,
    save_job,
    get_job,
    delete_job,
    init_job_cache,
)

# ─────────────────────────────────────────
# QUOTA
# ─────────────────────────────────────────
from db.quota import (
    get_today_quota,
    increment_quota_used,
    get_remaining_quota,
    can_call,
    increment_usage,
    all_models_exhausted,
)

# ─────────────────────────────────────────
# ALERTS & COVERAGE STATS
# ─────────────────────────────────────────
from db.alerts import (
    get_quota_history,
    check_quota_health,
    save_quota_alert,
    save_coverage_stats,
    get_coverage_stats,
    _calculate_suggested_cap,
    _alert_already_sent,
)

# ─────────────────────────────────────────
# PROSPECTIVE COMPANIES
# ─────────────────────────────────────────
from db.prospective import (
    add_prospective_company,
    get_pending_prospective,
    get_prospective_companies,
    mark_prospective_scraped,
    mark_prospective_exhausted,
    mark_prospective_converted,
    is_prospective,
    get_prospective_status_summary,
    get_prospective_company,
)

# ─────────────────────────────────────────
# SCHEMA CLEANUP (used by tests directly)
# ─────────────────────────────────────────
from db.schema import (
    _cleanup_outreach,
    _cleanup_careershift_quota,
    _cleanup_quota_alerts,
    _cleanup_expired_ai_cache,
    _cleanup_expired_jobs,
    _cleanup_old_model_usage,
)


if __name__ == "__main__":
    init_db()