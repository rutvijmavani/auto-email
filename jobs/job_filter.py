# jobs/job_filter.py — Filter and score jobs for relevance

import re
import hashlib
from datetime import datetime, timezone
from config import (
    TARGET_JOB_TITLES, TARGET_SENIORITY, TARGET_SKILLS,
    USA_LOCATION_KEYWORDS, EXCLUDE_LOCATIONS,
    JOB_MONITOR_DAYS_FRESH,
)


def normalize_text(text):
    """Lowercase, strip accents, remove extra whitespace."""
    if not text:
        return ""
    # Basic accent removal
    replacements = {
        "é": "e", "è": "e", "ê": "e", "ë": "e",
        "à": "a", "â": "a", "ä": "a",
        "ü": "u", "ú": "u", "û": "u",
        "ó": "o", "ô": "o", "ö": "o",
        "ñ": "n", "ç": "c",
    }
    text = text.lower()
    for accented, plain in replacements.items():
        text = text.replace(accented, plain)
    return " ".join(text.split())


def matches_title(title):
    """
    Check if job title matches target keywords.
    Broad match — partial keyword in title counts.
    Returns True if any keyword found.
    """
    if not title:
        return False
    normalized = normalize_text(title)
    return any(kw in normalized for kw in TARGET_JOB_TITLES)


def is_us_location(location):
    """
    Returns True if location is in the USA.
    Includes Remote (assume US-remote).
    Excludes known non-US locations.
    """
    if not location:
        return True  # assume US if not specified

    loc = normalize_text(location)

    # Explicit exclude
    if any(ex in loc for ex in EXCLUDE_LOCATIONS):
        return False

    # Explicit include
    if any(us in loc for us in USA_LOCATION_KEYWORDS):
        return True

    # US state abbreviations (e.g. NY, CA, TX, WA)
    if re.search(r'\b[A-Z]{2}\b', location):
        return True

    # Default: include if unclear
    return True


def score_job(job):
    """
    Calculate relevance score for a job.

    Scoring:
      +10 base (title matched)
      +5  seniority match
      +2  per skill matched in description
      +5  posted today
      +3  posted yesterday
      +1  posted 2-3 days ago

    Returns integer score.
    """
    score = 10  # base — title already matched

    title = normalize_text(job.get("title", ""))
    description = normalize_text(job.get("description", ""))
    combined = title + " " + description

    # Seniority bonus
    if any(s in title for s in TARGET_SENIORITY):
        score += 5

    # Skills bonus
    for skill in TARGET_SKILLS:
        if skill.lower() in combined:
            score += 2

    # Freshness bonus
    posted_at = job.get("posted_at")
    if posted_at:
        try:
            if isinstance(posted_at, str):
                posted_at = datetime.fromisoformat(
                    posted_at.replace("Z", "+00:00")
                )
            # Make timezone-aware comparison
            now = datetime.now(timezone.utc)
            if posted_at.tzinfo is None:
                posted_at = posted_at.replace(tzinfo=timezone.utc)
            days_old = (now - posted_at).days
            if days_old == 0:
                score += 5
            elif days_old == 1:
                score += 3
            elif days_old <= 3:
                score += 1
        except (ValueError, TypeError, AttributeError):
            pass

    return score


def make_content_hash(company, title, location):
    """
    Create SHA256 hash of company + normalized_title + location.
    Used as secondary deduplication key.
    Handles same job reposted with different URL.
    """
    if not company or not title:
        return None
    normalized = (
        normalize_text(company) + "|" +
        normalize_text(title) + "|" +
        normalize_text(location or "")+ "|" +
        normalize_text(job_id or "")
    )
    return hashlib.sha256(normalized.encode()).hexdigest()


def filter_jobs(jobs):
    """
    Filter list of raw job dicts for relevance.
    Returns list of jobs that passed ALL filters,
    each augmented with skill_score and content_hash.

    Filters applied:
      1. Title match (hard filter)
      2. US location (hard filter)
    """
    results = []
    for job in jobs:
        title    = job.get("title", "")
        location = job.get("location", "")

        # Hard filter 1: title must match
        if not matches_title(title):
            continue

        # Hard filter 2: US location only
        if not is_us_location(location):
            continue

        # Augment with score and hash
        job["skill_score"]   = score_job(job)
        job["content_hash"]  = make_content_hash(
            job.get("company", ""), title, location, job.get("job_id", "")
        )

        results.append(job)

    return results


def is_fresh(job, ats_platform, days_fresh=None):
    """
    Check if job is genuinely fresh based on posted_at.
    Greenhouse updated_at is unreliable — always returns True.
    Returns True if job is within freshness window.
    """
    # Greenhouse date unreliable — always treat as fresh
    # Freshness handled by first_seen + content_hash instead
    if ats_platform == "greenhouse":
        return True

    if days_fresh is None:
        days_fresh = JOB_MONITOR_DAYS_FRESH

    posted_at = job.get("posted_at")
    if not posted_at:
        return True  # no date → trust first_seen

    try:
        if isinstance(posted_at, str):
            posted_at = datetime.fromisoformat(
                posted_at.replace("Z", "+00:00")
            )
        now = datetime.now(timezone.utc)
        if posted_at.tzinfo is None:
            posted_at = posted_at.replace(tzinfo=timezone.utc)
        days_old = (now - posted_at).days
        return days_old <= days_fresh
    except (ValueError, TypeError, AttributeError):
        return True  # parse error → trust first_seen