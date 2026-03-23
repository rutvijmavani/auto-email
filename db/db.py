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
    db.api_health             → per-platform ATS API health tracking
    db.pipeline_alerts        → pipeline threshold breach alerts
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
    get_applications_by_date,
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
    get_existing_emails_for_company,
)

# ─────────────────────────────────────────
# APPLICATION RECRUITERS
# ─────────────────────────────────────────
from db.application_recruiters import (
    link_recruiter_to_application,
    get_recruiters_for_application,
    get_unique_companies_needing_scraping,
    get_companies_needing_more_recruiters,
    link_top_recruiters_for_company,
    get_sendable_count_for_date,
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
# API HEALTH
# ─────────────────────────────────────────
from db.api_health import (
    record_request,
    get_platform_stats,
    get_health_summary,
    get_todays_stats,
    get_run_429_rate,
)

# ─────────────────────────────────────────
# PIPELINE ALERTS
# ─────────────────────────────────────────
from db.pipeline_alerts import (
    create_alert,
    has_recent_alert,
    mark_notified,
    mark_warnings_sent,
    get_pending_warnings,
    get_unnotified_alerts,
    check_pipeline_health,
    check_api_health,
    ALERT_RATE_LIMIT,
    ALERT_UNREACHABLE,
    ALERT_SLOW,
    ALERT_SERPER_LOW,
    ALERT_SERPER_DONE,
    ALERT_CRASH,
    ALERT_METRIC1_LOW,
    ALERT_METRIC2_LOW,
    ALERT_API_FAILURE,
    ALERT_COVERAGE_DROP,
    CRITICAL,
    WARNING,
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
    get_domain_for_prospective,
)

# ─────────────────────────────────────────
# JOB MONITORING
# ─────────────────────────────────────────
from db.serper_quota import (
    get_serper_credits,
    increment_serper_credits,
    has_serper_credits,
    reset_low_credit_alert,
)

from db.job_monitor import (
    get_detection_queue,
    get_detection_queue_stats,
    job_url_exists,
    job_hash_exists,
    save_job_posting,
    get_new_postings_for_digest,
    mark_first_scan_complete,
    update_company_check,
    get_all_monitored_companies,
    get_monitorable_companies,
    save_monitor_stats,
    get_monitor_stats,
    get_pipeline_reliability,
    mark_postings_digested,
    get_stale_jobs,
    increment_missing_days,
    reset_missing_days,
    mark_job_filled,
    get_tracked_urls_for_company,
    reactivate_job,
    save_verify_filled_stats,
    # NOTE: save_coverage_stats, get_coverage_stats, save_pipeline_alert,
    # save_api_health, get_unnotified_alerts, mark_alert_notified
    # have been removed from db.job_monitor — they now live in
    # db.alerts, db.pipeline_alerts, and db.api_health respectively.
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
    _cleanup_job_postings,
    _cleanup_closed_application_recruiters,
    _cleanup_auto_close_applications,
    _cleanup_monitor_stats,
)


if __name__ == "__main__":
    init_db()