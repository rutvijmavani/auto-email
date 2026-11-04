"""
workers/paginator.py — early-exit gate for adaptive listing scans.

An adaptive scan only needs the front of a listing: the jobs it has not handled yet sit
near the top, everything behind them was handled by the last fullscan (old bloom) or an
earlier adaptive scan (adaptive_seen). The gate decides, page by page, when the rest of
the board is not worth fetching.

The rule (one rule for every platform, no sorted/unsorted flag):

    a page where EVERY job is already seen   -> counter + 1
    a page with ANY job not seen             -> counter reset to 0
    counter reaches PAGINATOR_CONFIRM_PAGES  -> stop

It was chosen from live runs on SmartRecruiters, Oracle HCM, Workday, Taleo, Jibe and
Eightfold: on every newest-first board it found 100% of the jobs posted since the last
scan; a looser "80% of the page" rule missed 10 of 11 new jobs on a shuffled Jibe board.
On a shuffled board the strict rule simply reads (nearly) every page, which is safe.
Fullscan is the safety net for a job the gate skipped (fullscan never passes a gate).

Feeders call the gate after each page with that page's normalized jobs and stop paging
when it returns True (reporting STOP_EARLY_EXIT). Feeders that fetch the whole board in
one response ignore it.
"""

from typing import Callable, Optional

from config import PAGINATOR_CONFIRM_PAGES


class EarlyExitGate:
    """
    Stateful per-fetch gate. Call it with each page's jobs; it returns True once the scan
    may stop. Not thread-safe: one gate per fetch (Workday builds one per facet slice via
    fresh()).
    """

    def __init__(self, is_seen: Callable[[str], bool],
                 confirm_pages: int = PAGINATOR_CONFIRM_PAGES,
                 id_key: str = "job_id"):
        self._is_seen = is_seen
        self.confirm_pages = max(1, int(confirm_pages))
        self._id_key = id_key
        self.streak = 0          # consecutive fully-seen pages so far
        self.pages = 0           # pages judged
        self.stopped = False     # True once the gate told the feeder to stop

    def fresh(self) -> "EarlyExitGate":
        """A new gate with the same seen-check and thresholds and a zeroed counter."""
        return EarlyExitGate(self._is_seen, self.confirm_pages, self._id_key)

    def __call__(self, page_jobs) -> bool:
        """Judge one page. True = stop paging now."""
        ids = [j.get(self._id_key) for j in (page_jobs or []) if j.get(self._id_key)]
        if not ids:
            # Empty page or no identifiable jobs: nothing to judge. The feeder's own
            # end-of-results handling applies; the counter is left alone.
            return False
        self.pages += 1
        if all(self._is_seen(i) for i in ids):
            self.streak += 1
        else:
            self.streak = 0
        if self.streak >= self.confirm_pages:
            self.stopped = True
        return self.stopped


def make_seen_check(*checks: Optional[Callable[[str], bool]]) -> Callable[[str], bool]:
    """A job is seen when any of the given checks says so (old bloom OR adaptive_seen)."""
    live = [c for c in checks if c is not None]

    def _seen(job_id: str) -> bool:
        return any(c(job_id) for c in live)

    return _seen


def estimate_scan_depth(
    total_fetched: int,
    new_found: int,
    early_exit: bool,
) -> dict:
    """
    Summarise scan efficiency for logging and observability metrics.

    Args:
        total_fetched: Total jobs fetched across all pages.
        new_found:     Genuinely new job IDs detected.
        early_exit:    Whether the gate stopped the fetch early.

    Returns:
        Dict with waste_ratio and efficiency stats.
    """
    waste_ratio = (total_fetched - new_found) / max(total_fetched, 1)
    return {
        "total_fetched": total_fetched,
        "new_found":     new_found,
        "wasted":        total_fetched - new_found,
        "waste_ratio":   round(waste_ratio, 3),
        "early_exit":    early_exit,
    }
