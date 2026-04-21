# jobs/job_filter.py — Filter and score jobs for relevance

import re
import csv
import hashlib
import logging
from functools import lru_cache
from pathlib import Path
from datetime import datetime, timezone

import pycountry
import geonamescache

from config import (
    TARGET_JOB_TITLES, TARGET_SENIORITY, TARGET_SKILLS,
    JOB_MONITOR_DAYS_FRESH,
)

logger = logging.getLogger(__name__)

_DATA_DIR = Path(__file__).parent.parent / "data"

# Dotted forms ("u.s.a.", "u.s.") are stripped by _normalize_location before
# matching — store the post-normalization space-separated forms instead.
_US_EXPLICIT = {"united states", "usa", "u s a", "america"}

# Intentionally NOT checked in Signal 1 — placed after country/city checks
# (Signal 7) so an explicit non-US signal wins over a generic remote keyword.
# e.g. "Remote - India" → India rejected at Signal 4 before remote fires.
_US_REMOTE = {"remote", "work from home", "wfh", "anywhere"}

# Well-known non-US abbreviations used in job postings that pycountry's full
# country name list does not cover.  "uk" is the most common — pycountry stores
# "United Kingdom of Great Britain and Northern Ireland", not "uk".
# Checked in Signal 4 so "Remote (UK)" → False (not True via Signal 7 remote).
#
# "england" intentionally excluded from this set: Signal 4.5 handles it with
# the required "new england" exclusion so "Boston, New England" isn't rejected.
_NON_US_ALIASES = frozenset({"uk", "emea"})


# ─────────────────────────────────────────────────────────────────────────────
# Lazy-loaded location data (computed once on first use, cached forever)
# ─────────────────────────────────────────────────────────────────────────────

def _load_simplemap_cities(csv_path: Path) -> frozenset:
    """
    Load SimpleMaps US cities CSV — city names only.
    Reads 'city' and 'city_ascii' columns.
    State codes/names come from geonamescache (cleaner, authoritative).
    Returns frozenset of lowercase city name strings.
    """
    city_names: set = set()

    if not csv_path.exists():
        logger.warning(
            "SimpleMaps CSV not found at %s — US city lookup disabled", csv_path
        )
        return frozenset()

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
    return frozenset(city_names)


@lru_cache(maxsize=1)
def _get_simplemap_cities() -> frozenset:
    return _load_simplemap_cities(_DATA_DIR / "uscities.csv")


@lru_cache(maxsize=1)
def _get_state_and_city_data():
    """
    Load geonamescache once and return three structures:
      state_codes  — frozenset of lowercase 2-letter US state codes
      state_names  — frozenset of lowercase full US state names
      city_country — dict mapping city name → frozenset of country codes
                     (set preserves ambiguous cities, e.g. Cambridge → {"US","GB"})
    """
    gc = geonamescache.GeonamesCache()

    state_codes = frozenset(k.lower() for k in gc.get_us_states().keys())
    state_names = frozenset(
        v["name"].lower() for v in gc.get_us_states().values()
    )

    # Build city → {countrycodes} — do NOT overwrite duplicates
    tmp: dict = {}
    for c in gc.get_cities().values():
        tmp.setdefault(c["name"].lower(), set()).add(c["countrycode"])
    city_country = {k: frozenset(v) for k, v in tmp.items()}

    return state_codes, state_names, city_country


@lru_cache(maxsize=1)
def _get_pycountry_data():
    """
    Load pycountry once and return:
      non_us_alpha3        — frozenset of lowercase ISO alpha-3 codes for non-US countries
      non_us_country_words — frozenset of lowercase country names/common-names,
                             minus US state names to avoid collisions (Georgia etc.)
      non_us_alpha2        — frozenset of lowercase ISO alpha-2 codes for non-US countries
                             (used by Signal 2.5 for unambiguous 2-letter codes like IE, GB)
    """
    alpha3_map = {
        c.alpha_3.lower(): c.alpha_2
        for c in pycountry.countries
        if hasattr(c, "alpha_3")
    }
    non_us_alpha3 = frozenset(k for k, v in alpha3_map.items() if v != "US")

    non_us_alpha2 = frozenset(
        c.alpha_2.lower() for c in pycountry.countries if c.alpha_2 != "US"
    )

    # State names must be excluded BEFORE building non_us_country_words
    _, state_names, _ = _get_state_and_city_data()

    words: set = set()
    for c in pycountry.countries:
        if c.alpha_2 == "US":
            continue
        words.add(c.name.lower())
        if hasattr(c, "common_name"):
            words.add(c.common_name.lower())
    words -= state_names   # remove "georgia", "jordan" etc.

    return non_us_alpha3, frozenset(words), non_us_alpha2


# ─────────────────────────────────────────────────────────────────────────────
# Location helpers
# ─────────────────────────────────────────────────────────────────────────────

def _normalize_location(loc: str) -> str:
    """
    Collapse any location encoding into a clean space-separated lowercase string.

    Handles (non-exhaustive):
        IND.Chennai              → "ind chennai"
        US-CA-Menlo Park         → "us ca menlo park"
        Seattle (WA)             → "seattle wa"
        PUNE 05                  → "pune"
        Burlington Massachusetts → "burlington massachusetts"
        USA Remote Worksite      → "usa remote worksite"
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

    # Remaining punctuation → spaces (dots included — removes "u.s.a." dots)
    loc = re.sub(r'[,;|/\-–—_.]+', ' ', loc)

    return ' '.join(loc.lower().split())


def _ngrams(tokens: list, max_n: int = 3) -> list:
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
        1.   Explicit US keyword           → True   (usa, united states, u s a …)
        2.   ISO alpha-3 non-US code       → False  (IND, GBR, CAN … 3-letter uppercase-gated)
        2.5. ISO alpha-2 non-US code       → False  (IE, GB, SG … 2-letter uppercase-gated;
                                                     skipped when code conflicts with a US
                                                     state code — Signal 3 handles those)
        3.   US state code or full name    → True   (CA, NY, California … runs BEFORE
                                                     country-name check so "Jordan, UT"
                                                     is accepted on state code "ut";
                                                     state codes are skipped when a non-US
                                                     country word follows them, e.g.
                                                     "Delhi, IN, India" → "IN" skipped)
        4.   Non-US country name           → False  (India, Germany, Canada …)
        4.5. UK constituent nations        → False  (England, Scotland, Wales,
                                                     Northern Ireland; "New England" excluded)
        5.   SimpleMaps US city lookup     → True   (San Francisco, Austin …)
        6.   geonamescache city → country  → True/False  (set-based; prefers US when
                                                          city exists in multiple countries)
        7.   Remote / work-from-home       → True   (last resort; placed here so
                                                     "Remote - India" is rejected at
                                                     Signal 4 before this fires)
        8.   Default                       → True   (preserve false-positive tolerance)
    """
    if not location or not location.strip():
        return True   # no location → assume US

    # ── Uppercase alpha-3 / alpha-2 gate ─────────────────────────────────
    # Capture ALL-CAPS alpha tokens from the ORIGINAL string before
    # normalization.  Only tokens that were uppercase in the original are
    # treated as ISO country codes, preventing common English words ("can",
    # "per", "in", "and") from being misread as codes.
    orig_alpha_tokens = re.split(r'[^A-Za-z]+', location)

    orig_upper_alpha3: set = {
        tok.lower()
        for tok in orig_alpha_tokens
        if len(tok) == 3 and tok.isupper() and tok.isalpha()
    }

    # 2-letter ALL-CAPS tokens (potential ISO alpha-2 country codes).
    # e.g. "IE" for Ireland, "GB" for UK — used in Signal 2.5.
    orig_upper_alpha2: set = {
        tok.lower()
        for tok in orig_alpha_tokens
        if len(tok) == 2 and tok.isupper() and tok.isalpha()
    }

    clean  = _normalize_location(location)
    tokens = clean.split()
    if not tokens:
        return True

    phrases = _ngrams(tokens, max_n=3)

    # Load lazy data (computed once, cached via lru_cache)
    state_codes, state_names, city_country         = _get_state_and_city_data()
    non_us_alpha3, non_us_country_words, non_us_alpha2 = _get_pycountry_data()
    simplemap_cities                               = _get_simplemap_cities()

    # ── Signal 1: Explicit US keywords ───────────────────────────────────
    if any(p in _US_EXPLICIT for p in phrases):
        return True

    # ── Signal 2: ISO alpha-3 non-US country code ─────────────────────────
    # Only fires for tokens that were ALL-CAPS (3-letter) in the original.
    for tok in tokens:
        if len(tok) == 3 and tok in orig_upper_alpha3:
            if tok == "usa":
                return True
            if tok in non_us_alpha3:
                return False

    # ── Signal 2.5: ISO alpha-2 non-US country code ───────────────────────
    # Catches "Dublin, IE", "London, GB", "Singapore, SG" etc. where the
    # ATS passes a 2-letter ALL-CAPS country code.
    # Skips codes that conflict with US state codes (e.g. "IN" = Indiana /
    # India, "DE" = Delaware / Germany) — Signal 3's positional check handles
    # those ambiguous cases.
    for tok_lower in orig_upper_alpha2:
        if tok_lower in state_codes:
            continue   # ambiguous with US state — let Signal 3 decide
        if tok_lower in non_us_alpha2:
            return False

    # ── Signal 3: US state code or full name ─────────────────────────────
    # Full state names (California, Texas …) are unambiguous — match directly.
    if any(p in state_names for p in phrases):
        return True

    # 2-letter state codes are ambiguous when a non-US country word follows.
    # "Jordan, UT"     → "ut" fires (nothing non-US after) → True  ✓
    # "Delhi, IN, India" → "in" fires but "india" follows  → skip  ✓
    # Falls through to Signal 4 which then rejects "india".
    for tok in tokens:
        if tok not in state_codes:
            continue
        match = re.search(r'\b' + re.escape(tok) + r'\b', clean)
        if not match:
            continue
        after         = clean[match.end():]
        after_tokens  = after.split()
        after_phrases = _ngrams(after_tokens, max_n=3) if after_tokens else []
        if any(p in non_us_country_words for p in after_phrases):
            continue   # country suffix after state code → non-US context
        if any(p in _NON_US_ALIASES for p in after_phrases):
            continue
        return True

    # ── Signal 4: Non-US country name ────────────────────────────────────
    if any(p in non_us_country_words for p in phrases):
        return False
    if any(p in _NON_US_ALIASES for p in phrases):
        return False

    # ── Signal 4.5: UK constituent nations ───────────────────────────────
    # "England", "Scotland", "Wales", "Northern Ireland" are not sovereign
    # countries in pycountry and are intentionally absent from _NON_US_ALIASES
    # (to avoid false rejection of "New England").  Catch them explicitly here.
    # "new england" exclusion: the phrase check fires on "england" alone but
    # is guarded against the 2-gram "new england" being present first.
    if "england" in phrases and "new england" not in phrases:
        return False
    if any(p in {"scotland", "wales", "northern ireland"} for p in phrases):
        return False

    # ── Signal 5: SimpleMaps US city lookup ──────────────────────────────
    # Cross-check with city_country before accepting a SimpleMaps hit.
    # SimpleMaps includes small US cities (London, KY; Camden, NJ; Paris, TX)
    # whose names are shared with globally-famous non-US cities.  If
    # geonamescache knows the name exclusively as non-US (e.g. "london" →
    # {"GB","CA"}, "paris" → {"FR","CA",...}) we skip S5 and let S6 resolve
    # it — which will correctly return False.
    # Only return True from S5 when:
    #   • city_country has no entry for the name (US-only per SimpleMaps), OR
    #   • city_country includes "US" (city genuinely exists in both countries)
    for p in phrases:
        if p in simplemap_cities:
            cc_set = city_country.get(p)
            if cc_set is None or "US" in cc_set:
                return True
            # geonamescache knows this name as non-US only → skip, fall to S6

    # ── Signal 6: geonamescache global city → country codes ──────────────
    # city_country maps name → frozenset of country codes.
    # Prefer US: if "US" is in the set the city exists in the US regardless
    # of other countries sharing the same name (e.g. Cambridge → {US, GB}).
    found_us     = False
    found_non_us = False
    for p in phrases:
        cc_set = city_country.get(p)
        if cc_set:
            if "US" in cc_set:
                found_us = True
            else:
                found_non_us = True

    if found_us:
        return True
    if found_non_us:
        return False

    # ── Signal 7: Remote / work-from-home ────────────────────────────────
    # Intentionally last positive signal so a non-US country name in the
    # same string (Signal 4) wins first, e.g. "Remote - India" → False.
    if any(p in _US_REMOTE for p in phrases):
        return True

    # ── Signal 8: Default ─────────────────────────────────────────────────
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


def filter_jobs_title_only(jobs):
    """
    Apply title filter only — no location check.

    Used for platforms (e.g. Workday) where the listing-stage location is
    too vague to filter on reliably ("2 Locations", bare city without country,
    "London" that might map to Kentucky or the UK).  The caller is responsible
    for applying is_us_location() after fetching the detail endpoint which
    gives the full, precise location.

    Adds skill_score and content_hash (keyed on listing location) so the job
    dict is ready for save_job_posting() after the detail fetch updates the
    location field.
    """
    results = []
    for job in jobs:
        title    = job.get("title", "")
        location = job.get("location", "")

        if not matches_title(title):
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
