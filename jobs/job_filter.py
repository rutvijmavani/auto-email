# jobs/job_filter.py — Filter and score jobs for relevance

import re
import csv
import hashlib
import logging
from pathlib import Path
from datetime import datetime, timezone

import pycountry
import geonamescache

from config import (
    TARGET_JOB_TITLES, TARGET_SENIORITY, TARGET_SKILLS,
    JOB_MONITOR_DAYS_FRESH,
)

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# Location data — loaded once at import, zero runtime cost
# ─────────────────────────────────────────────────────────────────────────────

_DATA_DIR = Path(__file__).parent.parent / "data"


def _load_simplemap_cities(csv_path: Path) -> set[str]:
    """
    Load SimpleMaps US cities CSV — city names only.
    Reads 'city' and 'city_ascii' columns (columns 1 & 2).
    State codes/names come from geonamescache (cleaner, authoritative).
    Returns set of lowercase city name strings.
    """
    city_names: set[str] = set()

    if not csv_path.exists():
        logger.warning("SimpleMaps CSV not found at %s — city lookup disabled", csv_path)
        return city_names

    with open(csv_path, encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            city     = (row.get("city")      or "").strip().lower()
            city_asc = (row.get("city_ascii") or "").strip().lower()
            if city:
                city_names.add(city)
            if city_asc:
                city_names.add(city_asc)

    logger.debug("SimpleMaps loaded: %d US city names", len(city_names))
    return city_names


_SIMPLEMAP_CITIES: set[str] = _load_simplemap_cities(_DATA_DIR / "uscities.csv")

# geonamescache — state codes/names + global city→countrycode lookup
_gc = geonamescache.GeonamesCache()

_US_STATE_CODES: set[str] = {k.lower() for k in _gc.get_us_states().keys()}
_US_STATE_NAMES: set[str] = {v["name"].lower() for v in _gc.get_us_states().values()}

_CITY_COUNTRY: dict[str, str] = {
    c["name"].lower(): c["countrycode"]
    for c in _gc.get_cities().values()
}

# ISO alpha-3 → alpha-2 mapping  e.g. "ind" → "IN", "usa" → "US"
_ALPHA3_TO_ALPHA2: dict[str, str] = {
    c.alpha_3.lower(): c.alpha_2
    for c in pycountry.countries
    if hasattr(c, "alpha_3")
}
_NON_US_ALPHA3: set[str] = {k for k, v in _ALPHA3_TO_ALPHA2.items() if v != "US"}

# Non-US country names and common names, minus any that collide with US state names
# (e.g. "Georgia" is both a country and a US state — keep it as US)
_NON_US_COUNTRY_WORDS: set[str] = set()
for _c in pycountry.countries:
    if _c.alpha_2 == "US":
        continue
    _NON_US_COUNTRY_WORDS.add(_c.name.lower())
    if hasattr(_c, "common_name"):
        _NON_US_COUNTRY_WORDS.add(_c.common_name.lower())
_NON_US_COUNTRY_WORDS -= _US_STATE_NAMES   # remove Georgia, Jordan, etc.

_US_EXPLICIT = {"united states", "usa", "u.s.a.", "u.s.", "america"}
_US_REMOTE   = {"remote", "work from home", "wfh", "anywhere"}


# ─────────────────────────────────────────────────────────────────────────────
# Location helpers
# ─────────────────────────────────────────────────────────────────────────────

def _normalize_location(loc: str) -> str:
    """
    Collapse any location encoding into a clean space-separated lowercase string.

    Handles (non-exhaustive):
        IND.Chennai          → "ind chennai"
        US-CA-Menlo Park     → "us ca menlo park"
        Seattle (WA)         → "seattle wa"
        PUNE 05              → "pune"
        Burlington Massachusetts → "burlington massachusetts"
        USA Remote Worksite  → "usa remote worksite"
        Menlo Park, CA; New York, NY → "menlo park ca new york ny"
    """
    # ISO alpha-3 dot pattern:  IND.Chennai → IND Chennai
    loc = re.sub(r'\b([A-Za-z]{3})\.', r'\1 ', loc)

    # ISO subdivision hyphen:   US-CA-Menlo Park → US CA Menlo Park
    loc = re.sub(r'\b([A-Za-z]{2,3})-([A-Za-z]{2})-', r'\1 \2 ', loc)

    # Parentheses:              Seattle (WA) → Seattle  WA
    loc = re.sub(r'[()]', ' ', loc)

    # Pure digit tokens:        PUNE 05 → PUNE
    loc = re.sub(r'\b\d+\b', ' ', loc)

    # Remaining punctuation → spaces
    loc = re.sub(r'[,;|/\-–—_.]+', ' ', loc)

    return ' '.join(loc.lower().split())


def _ngrams(tokens: list[str], max_n: int = 3) -> list[str]:
    """
    Generate all 1-, 2-, and 3-word phrases from a token list.
    Used to match multi-word city/state names regardless of format.

    Example: ["burlington", "massachusetts"]
        → ["burlington", "massachusetts", "burlington massachusetts"]
    """
    result = []
    n_tokens = len(tokens)
    for n in range(1, min(max_n, n_tokens) + 1):
        for i in range(n_tokens - n + 1):
            result.append(" ".join(tokens[i: i + n]))
    return result


def is_us_location(location: str) -> bool:
    """
    Signal-scan approach — does not try to parse location structure.
    Scans the entire string for US / non-US signals regardless of format.

    Signal priority (first match wins):
        1. Explicit US keyword           → True
        2. ISO alpha-3 non-US code       → False   (IND, GBR, CAN …)
        3. Non-US country name           → False   (India, Germany …)
        4. US state code or full name    → True    (CA, NY, California …)
        5. SimpleMaps US city lookup     → True    (San Francisco, Austin …)
        6. geonamescache city→country    → True/False
        7. Default                       → True    (preserve false-positive tolerance)
    """
    if not location or not location.strip():
        return True   # no location → assume US

    clean  = _normalize_location(location)
    tokens = clean.split()
    if not tokens:
        return True

    phrases = _ngrams(tokens, max_n=3)

    # ── Signal 1: Explicit US keywords ───────────────────────────────────
    if any(p in _US_EXPLICIT for p in phrases):
        return True
    if any(p in _US_REMOTE for p in phrases):
        return True

    # ── Signal 2: ISO alpha-3 non-US country code ─────────────────────────
    # Catches "IND", "GBR", "CAN", "DEU" etc. regardless of surrounding chars
    for tok in tokens:
        if len(tok) == 3:
            if tok == "usa":
                return True
            if tok in _NON_US_ALPHA3:
                return False

    # ── Signal 3: Non-US country name ────────────────────────────────────
    if any(p in _NON_US_COUNTRY_WORDS for p in phrases):
        return False

    # ── Signal 4: US state code or full name ─────────────────────────────
    if any(tok in _US_STATE_CODES for tok in tokens):
        return True
    if any(p in _US_STATE_NAMES for p in phrases):
        return True

    # ── Signal 5: SimpleMaps US city lookup ──────────────────────────────
    if any(p in _SIMPLEMAP_CITIES for p in phrases):
        return True

    # ── Signal 6: geonamescache global city → countrycode ────────────────
    found_us     = False
    found_non_us = False
    for p in phrases:
        cc = _CITY_COUNTRY.get(p)
        if cc == "US":
            found_us = True
        elif cc is not None:
            found_non_us = True

    if found_us:
        return True
    if found_non_us and not found_us:
        return False

    # ── Signal 7: Default ─────────────────────────────────────────────────
    return True   # genuinely ambiguous → allow (false-positive tolerance)


# ─────────────────────────────────────────────────────────────────────────────
# Text helpers
# ─────────────────────────────────────────────────────────────────────────────

def normalize_text(text):
    """Lowercase, strip accents, remove extra whitespace."""
    if not text:
        return ""
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


# ─────────────────────────────────────────────────────────────────────────────
# Title + scoring
# ─────────────────────────────────────────────────────────────────────────────

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


def score_job(job):
    """
    Calculate relevance score for a job.

    Scoring:
      +10 base (title already matched)
      +5  seniority match
      +2  per skill matched in description
      +5  posted today
      +3  posted yesterday
      +1  posted 2-3 days ago

    Returns integer score.
    """
    score = 10  # base — title already matched

    title       = normalize_text(job.get("title", ""))
    description = normalize_text(job.get("description", ""))
    combined    = title + " " + description

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


# ─────────────────────────────────────────────────────────────────────────────
# Hashing
# ─────────────────────────────────────────────────────────────────────────────

def make_content_hash(company, title, location, job_id=""):
    """
    Create SHA256 hash for deduplication.
    Uses job_id from ATS API when available for uniqueness.
    Returns a single hash string (or None if company/title missing).
    Format: company|title|location|job_id
    """
    if not company or not title:
        return None
    normalized = (
        normalize_text(company) + "|" +
        normalize_text(title)   + "|" +
        normalize_text(location or "") + "|" +
        normalize_text(job_id or "")
    )
    return hashlib.sha256(normalized.encode()).hexdigest()


def make_legacy_content_hash(company, title, location):
    """
    Legacy hash format (company|title|location) without job_id.
    Used during rollout to match existing DB rows saved before
    job_id was added to the hash.
    Returns None if company/title missing.
    """
    if not company or not title:
        return None
    normalized = (
        normalize_text(company) + "|" +
        normalize_text(title)   + "|" +
        normalize_text(location or "")
    )
    return hashlib.sha256(normalized.encode()).hexdigest()


# ─────────────────────────────────────────────────────────────────────────────
# Main filter
# ─────────────────────────────────────────────────────────────────────────────

def filter_jobs(jobs):
    """
    Filter list of raw job dicts for relevance.
    Returns list of jobs that passed ALL filters,
    each augmented with skill_score and content_hash.

    Filters applied:
      1. Title match   (hard filter)
      2. US location   (hard filter — signal-scan approach)
    """
    results = []
    for job in jobs:
        title    = job.get("title", "")
        location = job.get("location", "")

        if not matches_title(title):
            continue

        if not is_us_location(location):
            continue

        job["skill_score"]         = score_job(job)
        job["content_hash"]        = make_content_hash(
            job.get("company", ""), title, location,
            job.get("job_id", "")
        )
        job["content_hash_legacy"] = make_legacy_content_hash(
            job.get("company", ""), title, location
        )

        results.append(job)

    return results


# ─────────────────────────────────────────────────────────────────────────────
# Freshness
# ─────────────────────────────────────────────────────────────────────────────

def is_fresh(job, ats_platform, days_fresh=None):
    """
    Check if job is genuinely fresh based on posted_at.
    Greenhouse updated_at is unreliable — always returns True.
    Returns True if job is within freshness window.
    """
    if days_fresh is None:
        days_fresh = JOB_MONITOR_DAYS_FRESH

    posted_at = job.get("posted_at")
    if not posted_at:
        return True   # no date → trust first_seen

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
        return True   # parse error → trust first_seen
