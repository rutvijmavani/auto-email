"""
workers/detail_ceiling.py — nightly detail pool ceiling from arrival history.

Pure sizing logic for docs/detail-pool-ceiling-th-design.md (3.2-3.5, 9.2); the
manager supplies the data and stores the result.

  L(p) = A_p x s / (T x 60 x u)      workers needed to clear the covered slice
  C    = ceil(L(p))                  detail ceiling; p is the highest coverage in
                                     the ladder p99 -> p75 whose C fits C_avail
  D    = A_p x s / C                 design delay at the covered level (seconds)

A = jobs arriving in any T-minute sliding window of the per-minute history,
s = seconds of worker time per job, u = target utilisation.
"""

import math
from datetime import timedelta
from typing import List, Optional


def dense_series(rows) -> List[int]:
    """
    Per-minute arrivals with the gaps filled by 0, from rows of (minute, arrivals)
    sorted by minute.  A minute with no row had no recorded arrivals (a quiet
    minute is written as 0; a missing one means the manager was down).
    """
    series: List[int] = []
    prev = None
    for minute, arrivals in rows:
        if prev is not None:
            gap = int((minute - prev) / timedelta(minutes=1)) - 1
            if gap > 0:
                series.extend([0] * gap)
        series.append(int(arrivals))
        prev = minute
    return series


def window_sums(series: List[int], width: int) -> List[int]:
    """Arrivals in every full `width`-minute sliding window (empty if too short)."""
    if width <= 0 or len(series) < width:
        return []
    total = sum(series[:width])
    sums = [total]
    for i in range(width, len(series)):
        total += series[i] - series[i - width]
        sums.append(total)
    return sums


def percentile(sorted_values: List[float], pct: float) -> float:
    """Linear-interpolation percentile (same as SQL PERCENTILE_CONT); pct in 0..100."""
    if not sorted_values:
        raise ValueError("percentile of nothing")
    pos = (len(sorted_values) - 1) * pct / 100.0
    lo = math.floor(pos)
    hi = math.ceil(pos)
    if lo == hi:
        return float(sorted_values[lo])
    return sorted_values[lo] + (sorted_values[hi] - sorted_values[lo]) * (pos - lo)


def drain_minutes(series: List[int], workers: int, s: float) -> float:
    """
    Longest time (minutes) the biggest backlog of the history needs to drain
    when `workers` work flat out at s seconds per job: B grows by the minute's
    arrivals and shrinks by 60 x workers / s jobs per minute, never below 0.
    """
    per_min = 60.0 * workers / s
    backlog = worst = 0.0
    for a in series:
        backlog = max(0.0, backlog + a - per_min)
        worst = max(worst, backlog)
    return worst / per_min


def plan_detail_ceiling(
    series: List[int], s: float, *, u: float, window_min: int, start_pct: int,
    floor_pct: int, max_wait_min: float, others: int, total: int, cap: int,
    step: int, floor_workers: int,
) -> Optional[dict]:
    """
    Detail ceiling for tonight, or None when the history is shorter than one window.

    others = seats the scan and fullscan ceilings take (sized first); total = seats
    in force; cap = most seats Postgres can host; step = seats per raise.
    If C(start_pct) does not fit total - others, the seat total is raised toward
    cap first, and only then is the coverage level stepped down one point at a
    time to the first level that fits.  Below floor_pct the ceiling is simply
    what is left and eff_pct is None (critical).

    Returns {ceil, total, eff_pct, a_p, d_s, d_start_s, drain_min, c_avail,
    need_start, alerts}; alerts are log lines (email is deferred).
    """
    sums = window_sums(series, window_min)
    if not sums:
        return None
    ordered = sorted(sums)
    denom = window_min * 60.0 * u

    def a_at(pct: float) -> float:
        return percentile(ordered, pct)

    def need(pct: float) -> int:
        return max(floor_workers, math.ceil(a_at(pct) * s / denom))

    need_start = need(start_pct)
    while step > 0 and total < cap and total - others < need_start:
        total = min(total + step, cap)
    c_avail = max(floor_workers, total - others)

    eff_pct = next((p for p in range(start_pct, floor_pct - 1, -1)
                    if need(p) <= c_avail), None)
    ceil_ = need(eff_pct) if eff_pct is not None else c_avail
    a_p = a_at(eff_pct if eff_pct is not None else floor_pct)
    d_s = a_p * s / ceil_
    d_start_s = a_at(start_pct) * s / ceil_
    drain = drain_minutes(series, ceil_, s)

    alerts = []
    if eff_pct is None:
        alerts.append(
            f"CRITICAL detail ceiling {ceil_} covers less than p{floor_pct} "
            f"(p{start_pct} needs {need_start}, seats left {c_avail})")
    elif eff_pct < start_pct:
        alerts.append(
            f"detail ceiling {ceil_} covers p{eff_pct}, below p{start_pct} "
            f"(needs {need_start}, seats left {c_avail})")
    if d_s > max_wait_min * 60:
        alerts.append(
            f"detail design delay {d_s / 60:.0f} min exceeds the longest "
            f"acceptable wait {max_wait_min:.0f} min")
    if drain > max_wait_min:
        alerts.append(
            f"detail biggest backlog needs {drain:.0f} min to drain at ceiling "
            f"{ceil_}, longer than {max_wait_min:.0f} min")
    return {
        "ceil": ceil_, "total": total, "eff_pct": eff_pct, "a_p": a_p,
        "d_s": d_s, "d_start_s": d_start_s, "drain_min": drain,
        "c_avail": c_avail, "need_start": need_start, "alerts": alerts,
    }
