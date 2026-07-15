"""
workers/adaptive.py — Adaptive interval engine (Phase 5).

Implements the 5-poll weighted queue, asymmetric smoothing, score-tiered
MAX_INTERVAL cap, and full interval update flow described in Section 6
of the architecture doc.

All functions are pure — no Redis, no DB.  Called by the scheduler after
each scan completes (on_adaptive_complete).

Key design decisions (from doc):
  - Recency-biased weights [0.10, 0.15, 0.20, 0.25, 0.30] (oldest -> newest)
  - Asymmetric smoothing: reactivation is IMMEDIATE, dormancy is GRADUAL
  - Per-poll contribution capped at 10 (prevents one mass-hiring event
    from locking a company into max frequency for 5 polls)
  - score=None (< ADAPTIVE_MIN_POLLS history) or score=0 (no recent activity)
    -> default 12h interval; does not participate in percentile ranking

Band thresholds (low / moderate / active) are calibrated daily by
recalibrate_band_thresholds() in scheduler.py.  DEFAULT_THRESHOLDS is the
cold-start fallback (used before the first calibration run).  The thresholds
are passed in as a dict so every function here stays pure and testable.
"""

import json
from typing import Optional

from logger import get_logger

import math

from config import (
    ADAPTIVE_WEIGHTS,
    ADAPTIVE_MIN_POLLS,
    ADAPTIVE_CAP_PER_POLL,
    ADAPTIVE_SMOOTHING,
    ADAPTIVE_MIN_INTERVAL,
    ADAPTIVE_DEFAULT_INTERVAL,
    ADAPTIVE_MAX_INTERVAL_ACTIVE,
    ADAPTIVE_MAX_INTERVAL_DORMANT,
    ADAPTIVE_BAND_TOP_PCT,
    ADAPTIVE_BAND_ACTIVE_PCT,
    ADAPTIVE_BAND_MODERATE_PCT,
    ADAPTIVE_WINSORIZE_PCT,
    ADAPTIVE_MIN_COMPANIES_CALIBRATE,
)

logger = get_logger(__name__)


# ─────────────────────────────────────────
# BAND THRESHOLDS
# ─────────────────────────────────────────

# Cold-start / fallback thresholds used before recalibrate_band_thresholds()
# has run.  Values match the original hardcoded ADAPTIVE_BANDS so behaviour
# is unchanged until real data is available.
#
# Key semantics:
#   score = None  -> 12h  (insufficient poll history)
#   score = 0.0   -> 12h  (no new jobs in any recent poll — truly dormant)
#   score > 0     -> ranked:
#       score < thresholds["low"]      ->  9h
#       score < thresholds["moderate"] ->  6h
#       score < thresholds["active"]   ->  4h
#       score >= thresholds["active"]  ->  3h (ADAPTIVE_MIN_INTERVAL)
DEFAULT_THRESHOLDS: dict = {
    "low":      1.5,   # matches original band boundary
    "moderate": 3.5,
    "active":   6.0,
}


# ─────────────────────────────────────────
# SCORE COMPUTATION
# ─────────────────────────────────────────

def compute_score(recent_poll_counts: list) -> Optional[float]:
    """
    Compute weighted average of recent_poll_counts (5-poll rolling window).

    Returns None if fewer than ADAPTIVE_MIN_POLLS polls -- caller uses
    default interval instead of trusting an unreliable early score.

    Args:
        recent_poll_counts: list of int, up to 5 elements, oldest first.
                            Each value = new_jobs found in that poll (capped at 10).

    Returns:
        Weighted score (float >= 0), or None if not enough history.

    Examples:
        [0, 0, 2, 3, 1] -> recency-weighted avg ~= 1.65
        [5, 0, 0, 0, 0] -> 0.50  (burst fading -- correctly low)
        [0, 0, 0, 0, 5] -> 1.50  (burst just happened -- correctly higher)
    """
    if len(recent_poll_counts) < ADAPTIVE_MIN_POLLS:
        return None

    counts     = recent_poll_counts[-5:]
    weights    = ADAPTIVE_WEIGHTS[-len(counts):]
    weight_sum = sum(weights)

    return sum(c * w for c, w in zip(counts, weights)) / weight_sum


# ─────────────────────────────────────────
# BAND LOOKUP  (uses dynamic thresholds)
# ─────────────────────────────────────────

def band_lookup(score: Optional[float],
                thresholds: Optional[dict] = None) -> int:
    """
    Convert a score to a poll interval in seconds.

    score=None or score=0.0: no history / no activity -> 12h (ADAPTIVE_DEFAULT_INTERVAL).
    score > 0: compare against thresholds dict for 9h/6h/4h/3h assignment.

    Args:
        score:      output of compute_score(), or None.
        thresholds: dict with keys "low", "moderate", "active" from
                    recalibrate_band_thresholds(), or None to use DEFAULT_THRESHOLDS.

    Returns:
        Interval in seconds.
    """
    # No history yet, or company has had zero new jobs across entire window
    if score is None or score == 0.0:
        return ADAPTIVE_DEFAULT_INTERVAL   # 12h

    t = thresholds if thresholds is not None else DEFAULT_THRESHOLDS

    if score < t["low"]:
        return 9 * 3600
    elif score < t["moderate"]:
        return 6 * 3600
    elif score < t["active"]:
        return 4 * 3600
    return ADAPTIVE_MIN_INTERVAL           # 3h


# ─────────────────────────────────────────
# MAX_INTERVAL CAP  (uses dynamic thresholds)
# ─────────────────────────────────────────

def get_max_interval(score: Optional[float],
                     thresholds: Optional[dict] = None) -> int:
    """
    Return the score-tiered MAX_INTERVAL cap.

    Companies at or above the "moderate" percentile threshold are capped at 6h
    (ADAPTIVE_MAX_INTERVAL_ACTIVE); all others at 12h (ADAPTIVE_MAX_INTERVAL_DORMANT).

    Using thresholds["moderate"] (P75) as the cap boundary means the top 25%
    of active companies get the tighter cap, keeping them responsive.

    Args:
        score:      current adaptive score (or None).
        thresholds: live band thresholds dict (or None for defaults).

    Returns:
        Max interval in seconds.
    """
    t = thresholds if thresholds is not None else DEFAULT_THRESHOLDS
    if score is not None and score > 0.0 and score >= t["moderate"]:
        return ADAPTIVE_MAX_INTERVAL_ACTIVE    # 6h — moderate+ companies
    return ADAPTIVE_MAX_INTERVAL_DORMANT       # 12h


# ─────────────────────────────────────────
# ASYMMETRIC SMOOTHING
# ─────────────────────────────────────────

def compute_next_interval(computed: int, current_interval: int) -> int:
    """
    Apply asymmetric smoothing to the computed interval.

    Reactivation (interval dropping):
        React immediately -- no smoothing.  A dormant company suddenly posting
        jobs should be polled more frequently right away, not gradually.

    Dormancy (interval rising):
        Apply gradual EMA decay: 0.7 * computed + 0.3 * current.
        Prevents stopping polling after one empty day.

    Args:
        computed:         interval from band_lookup() (seconds)
        current_interval: company's current interval_s from DB

    Returns:
        Smoothed interval in seconds (int).
    """
    if computed < current_interval:
        # Getting MORE active -> react immediately
        return computed
    else:
        # Going DORMANT -> change gradually
        smoothed = ADAPTIVE_SMOOTHING * computed + (1 - ADAPTIVE_SMOOTHING) * current_interval
        return int(smoothed)


# ─────────────────────────────────────────
# ROLLING WINDOW UPDATE
# ─────────────────────────────────────────

def push_poll_result(recent_poll_counts: list, new_jobs: int) -> list:
    """
    Append new_jobs to the rolling window, capped at ADAPTIVE_CAP_PER_POLL.
    Pops the oldest entry if window exceeds 5 elements.

    Args:
        recent_poll_counts: current rolling window (list of int)
        new_jobs:           jobs found in this poll

    Returns:
        Updated rolling window (new list, does not mutate input).
    """
    counts = list(recent_poll_counts)
    counts.append(min(new_jobs, ADAPTIVE_CAP_PER_POLL))
    if len(counts) > 5:
        counts.pop(0)
    return counts


# ─────────────────────────────────────────
# RANK-BASED THRESHOLD BUILDER  (pure)
# ─────────────────────────────────────────

def build_thresholds_from_scores(scores: list) -> Optional[dict]:
    """
    Compute rank-based band thresholds with Winsorization.

    Algorithm (all steps pure — no Redis, no DB):
        1. Filter to score > 0  (dormant companies never participate in ranking)
        2. Return None if fewer than ADAPTIVE_MIN_COMPANIES_CALIBRATE  (caller
           falls back to DEFAULT_THRESHOLDS)
        3. Winsorize the top ADAPTIVE_WINSORIZE_PCT of scores: cap any value
           above the (1 - WINSORIZE_PCT) quantile at that quantile's value.
           Prevents one extreme mass-hiring outlier from pushing all boundaries
           upward and unintentionally demoting other companies.
        4. Sort descending; compute the score at each rank boundary:
               top 10%              -> active   threshold  (3h / 4h boundary)
               next 15% (ranks 2–3) -> moderate threshold  (4h / 6h boundary)
               next 25% (ranks 4–6) -> low      threshold  (6h / 9h boundary)
        5. Guarantee strict ordering  active > moderate > low > 0
           (collapses only possible when many scores are identical; nudge apart)

    Tie-promotion is automatic: band_lookup uses >= comparisons, so any company
    whose score exactly equals a boundary value is promoted to the better band.

    Args:
        scores: list of float, raw adaptive_score values queried from DB.
                May contain 0.0 values — they are filtered out in step 1.

    Returns:
        dict with keys "low", "moderate", "active", or None if too few data.

    Example  (10 active companies):
        Raw:        [0.2, 0.3, 0.4, 0.5, 0.7, 0.9, 1.2, 1.4, 1.8, 8.0]

        Winsorize top 5% (1 company): 8.0 → 1.8
        Winsorized desc: [1.8, 1.8, 1.4, 1.2, 0.9, 0.7, 0.5, 0.4, 0.3, 0.2]

        n_3h = ceil(10 * 0.10) = 1  → active   = winsorized_desc[0]     = 1.8
        n_4h = ceil(10 * 0.15) = 2  → moderate = winsorized_desc[1+2-1]
                                                = winsorized_desc[2]     = 1.4
        n_6h = ceil(10 * 0.25) = 3  → low      = winsorized_desc[1+2+3-1]
                                                = winsorized_desc[5]     = 0.7

        Stored: active=1.8, moderate=1.4, low=0.7

        At runtime (actual scores used, not winsorized):
            Company Z  score=8.0  -> >= 1.8 -> 3h  (spike reacts immediately)
            Company 9  score=1.8  -> >= 1.8 -> 3h  (tie-promoted)
            Company 8  score=1.4  -> >= 1.4 -> 4h
            Company 7  score=1.2  -> >= 0.7 -> 6h
            ...
            score=0              -> 12h  (excluded, not ranked)
    """
    active = [s for s in scores if s > 0.0]
    if len(active) < ADAPTIVE_MIN_COMPANIES_CALIBRATE:
        return None

    n = len(active)

    # ── Step 1: Winsorize ────────────────────────────────────────────────────
    # Replace the top WINSORIZE_PCT of scores with the value at the cap index.
    # We sort ascending to find the cap value, then apply it to all scores.
    #
    # n_outliers = number of top-ranked scores to cap (always at least 1 so
    # the formula works for small portfolios, e.g. n=10, WINSORIZE_PCT=0.05).
    # cap_idx is the index of the highest *kept* value in sorted_asc; anything
    # above it (the top n_outliers entries) is clamped down to cap_val.
    #
    # Using int(n * 0.95) would give cap_idx = n-1 when n ≤ 20, making cap_val
    # the maximum itself — no scores would ever be clipped.  math.ceil ensures
    # at least one outlier is always captured even for small portfolios.
    sorted_asc = sorted(active)
    n_outliers = max(1, math.ceil(n * ADAPTIVE_WINSORIZE_PCT))
    cap_idx    = max(0, n - n_outliers - 1)
    cap_val    = sorted_asc[cap_idx]
    winsorized = [min(s, cap_val) for s in active]

    # ── Step 2: Sort descending ──────────────────────────────────────────────
    desc = sorted(winsorized, reverse=True)

    # ── Step 3: Rank boundaries ──────────────────────────────────────────────
    # Use math.ceil so that e.g. 10 * 0.10 = 1 and 10 * 0.15 = 2 exactly.
    # For tiny portfolios (N=5) ceil ensures each band gets at least 1 slot.
    n_3h = max(1, math.ceil(n * ADAPTIVE_BAND_TOP_PCT))       # slots for 3h
    n_4h = max(1, math.ceil(n * ADAPTIVE_BAND_ACTIVE_PCT))    # slots for 4h
    n_6h = max(1, math.ceil(n * ADAPTIVE_BAND_MODERATE_PCT))  # slots for 6h

    # The threshold is the LOWEST score in each band — any actual score at or
    # above this value gets that band (or better, via tie-promotion).
    idx_active   = n_3h - 1
    idx_moderate = min(n_3h + n_4h - 1,           n - 1)
    idx_low      = min(n_3h + n_4h + n_6h - 1,    n - 1)

    active_thr   = desc[idx_active]
    moderate_thr = desc[idx_moderate]
    low_thr      = desc[idx_low]

    # ── Step 4: Guarantee strict ordering ────────────────────────────────────
    # Ties in scores can cause boundaries to collapse.  Nudge them apart by a
    # negligible epsilon so band_lookup comparisons are always unambiguous.
    eps          = 1e-9
    moderate_thr = min(moderate_thr, active_thr   - eps)
    low_thr      = min(low_thr,      moderate_thr - eps)
    low_thr      = max(low_thr, eps)   # must stay > 0

    return {
        "active":   active_thr,
        "moderate": moderate_thr,
        "low":      low_thr,
    }


# ─────────────────────────────────────────
# FULL UPDATE FLOW
# ─────────────────────────────────────────

def update_poll_interval(
    recent_poll_counts: list,
    current_interval_s: int,
    new_jobs: int,
    thresholds: Optional[dict] = None,
) -> dict:
    """
    Full interval update flow (Section 6 of architecture doc).

    Steps:
        1. Push new_jobs into rolling window
        2. Compute score from window
        3. Band lookup  (uses dynamic thresholds if supplied)
        4. Asymmetric smoothing
        5. Apply MAX_INTERVAL cap  (also threshold-aware)
        6. Return updated state dict

    Args:
        recent_poll_counts: current 5-poll rolling window
        current_interval_s: current poll interval in seconds
        new_jobs:           jobs found in this poll
        thresholds:         live band thresholds from get_band_thresholds()
                            (pass None to use DEFAULT_THRESHOLDS -- cold start
                            or unit tests that don't have Redis)

    Returns:
        dict with keys:
            recent_poll_counts  -- updated rolling window (list)
            adaptive_score      -- computed score (float, 0.0 if no history)
            current_interval_s  -- new interval in seconds
    """
    # Step 1: update rolling window
    counts = push_poll_result(recent_poll_counts, new_jobs)

    # Step 2: compute score
    score = compute_score(counts)

    # Step 3: band lookup (threshold-aware)
    computed = band_lookup(score, thresholds)

    # Step 4: asymmetric smoothing
    smoothed = compute_next_interval(computed, current_interval_s)

    # Step 5: cap (threshold-aware: moderate+ companies get tighter cap)
    max_interval   = get_max_interval(score, thresholds)
    final_interval = min(smoothed, max_interval)

    logger.debug(
        "adaptive: new_jobs=%d score=%s computed=%ds smoothed=%ds final=%ds "
        "thresholds=%s",
        new_jobs,
        f"{score:.3f}" if score is not None else "None",
        computed, smoothed, final_interval,
        thresholds or "defaults",
    )

    return {
        "recent_poll_counts": counts,
        "adaptive_score":     score if score is not None else 0.0,
        "current_interval_s": final_interval,
    }


# ─────────────────────────────────────────
# JSON SERIALIZATION HELPERS
# ─────────────────────────────────────────

def load_poll_counts(json_str: Optional[str]) -> list:
    """
    Deserialize recent_poll_counts from the DB (stored as JSON string).
    Returns [] on any parse error.
    """
    if not json_str:
        return []
    try:
        val = json.loads(json_str)
        if isinstance(val, list):
            return [int(x) for x in val]
        return []
    except (json.JSONDecodeError, TypeError, ValueError):
        return []


def dump_poll_counts(counts: list) -> str:
    """Serialize recent_poll_counts to JSON string for DB storage."""
    return json.dumps(counts)
