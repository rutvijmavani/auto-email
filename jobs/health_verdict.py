# jobs/health_verdict.py — pure verdict logic: is a company's latest fullscan healthy?
#
# Input is the company's fullscan scan_health rows (newest first), nothing else — no DB, no clock.
# Objective signals (a failed/incomplete fetch, an error, an empty board after a non-empty one) need
# no history. The count check is learned from the company's own history: the latest count is suspect
# when it falls HEALTH_Z spreads below the median of its recent good fullscans, where the spread is
# the larger of its own MAD and the Poisson floor sqrt(median). A company with too little history
# borrows the band from its platform peers (see peer_relative_spread); with no peers the count
# check is skipped. server_short (Taleo) and complete=None are never treated as bad.

import math
from dataclasses import dataclass
from statistics import median
from typing import Optional

from config import (
    HEALTH_BASELINE_SCANS, HEALTH_FAILING_CONSECUTIVE, HEALTH_MIN_HISTORY,
    HEALTH_PEER_PERCENTILE, HEALTH_Z,
)

STATE_OK      = "ok"
STATE_SUSPECT = "suspect"
STATE_FAILING = "failing"

MAD_TO_SIGMA = 1.4826   # MAD of a normal distribution × this = its standard deviation


@dataclass
class Verdict:
    state:     str = STATE_OK
    reason:    str = ""
    count:     Optional[int] = None     # latest fetched_distinct
    median:    Optional[float] = None   # baseline median (None when no baseline was used)
    spread:    Optional[float] = None   # s used for the band
    flag_line: Optional[float] = None   # median - z*s; a count below this is suspect
    band_from: Optional[str] = None     # 'own' | 'peers' | None


def is_good(row: dict) -> bool:
    """A scan that read the board properly: no error and not flagged incomplete."""
    return not row.get("error") and row.get("complete") is not False


def _count(row: dict) -> int:
    return int(row.get("fetched_distinct") or 0)


def mad(values) -> float:
    """Median absolute deviation."""
    m = median(values)
    return median([abs(v - m) for v in values])


def spread(values) -> float:
    """Band width s: the company's own robust spread, never below the Poisson floor sqrt(median)."""
    return max(MAD_TO_SIGMA * mad(values), math.sqrt(median(values)))


def relative_spread(values) -> float:
    """Own spread as a share of the median (for the peer pool); 0 when the median is 0."""
    m = median(values)
    return (MAD_TO_SIGMA * mad(values) / m) if m else 0.0


def percentile(values, pct: float) -> float:
    """Linear-interpolated percentile (pct in 0..100) of a non-empty list."""
    xs = sorted(values)
    k = (len(xs) - 1) * pct / 100.0
    lo, hi = math.floor(k), math.ceil(k)
    return xs[lo] + (xs[hi] - xs[lo]) * (k - lo)


def peer_relative_spread(peer_values_lists, min_peers: int) -> Optional[float]:
    """
    Cold-start band: HEALTH_PEER_PERCENTILE of the relative spreads of peers that have enough
    history of their own. None when fewer than min_peers qualify.
    """
    rels = [relative_spread(v) for v in peer_values_lists if len(v) >= HEALTH_MIN_HISTORY]
    if len(rels) < min_peers:
        return None
    return percentile(rels, HEALTH_PEER_PERCENTILE)


def _consecutive_bad(rows) -> int:
    n = 0
    for row in rows:
        if is_good(row):
            break
        n += 1
    return n


def evaluate(rows, peer_rel_spread: Optional[float] = None) -> Verdict:
    """
    rows: this company's fullscan scan_health rows as dicts, NEWEST FIRST.
    peer_rel_spread: platform peers' relative spread (peer_relative_spread), or None.
    """
    if not rows:
        return Verdict(reason="no fullscan yet")
    latest = rows[0]
    count = _count(latest)

    bad = _consecutive_bad(rows)
    if bad:
        what = (f"scan error: {str(latest.get('error'))[:120]}" if latest.get("error")
                else f"incomplete fetch ({latest.get('stop_reason')}, "
                     f"{count} of {latest.get('reported_total') or '?'})")
        state = STATE_FAILING if bad >= HEALTH_FAILING_CONSECUTIVE else STATE_SUSPECT
        return Verdict(state=state, count=count,
                       reason=f"{what}; {bad} bad fullscan{'s' if bad > 1 else ''} in a row")

    history = [_count(r) for r in rows[1:] if is_good(r)][:HEALTH_BASELINE_SCANS]
    if not history:
        return Verdict(count=count, reason="first good fullscan — no baseline yet")

    m = median(history)
    if count == 0 and m > 0:
        return Verdict(state=STATE_FAILING, count=count, median=m, band_from=None,
                       reason=f"empty board (0 jobs) after a median of {m:g}")

    if len(history) >= HEALTH_MIN_HISTORY:
        s, band_from = spread(history), "own"
    elif peer_rel_spread is not None:
        s, band_from = max(peer_rel_spread * m, math.sqrt(m)), "peers"
    else:
        return Verdict(count=count, median=m,
                       reason=f"{len(history)} good fullscan(s) — too little history and no peers")

    line = m - HEALTH_Z * s
    if count < line:
        return Verdict(state=STATE_SUSPECT, count=count, median=m, spread=s,
                       flag_line=line, band_from=band_from,
                       reason=f"{count} jobs, below {line:.0f} (median {m:g}, band {band_from})")
    return Verdict(count=count, median=m, spread=s, flag_line=line, band_from=band_from,
                   reason=f"{count} jobs, above {line:.0f} (median {m:g}, band {band_from})")
