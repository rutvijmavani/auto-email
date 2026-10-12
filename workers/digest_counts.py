# workers/digest_counts.py
# Per-company job counts from completed full scans, for the daily digest.
#
# A full scan fetches a company's whole board once per cycle, so its counts are
# the reliable "jobs fetched / matched" numbers.  Adaptive scans are left out:
# they are partial and repeat many times a day.
#
# One hash field per company, overwritten by that company's latest completed
# full scan — a re-run or resumed scan can never double count.  The hash is
# deleted by record_cycle_start() once the digest has been sent.

import logging

from config import REDIS_DIGEST_FULLSCAN_COUNTS, DIGEST_FULLSCAN_COUNTS_TTL_S

logger = logging.getLogger(__name__)


def record_fullscan_counts(r, company: str, fetched: int, matched: int) -> None:
    """Store `company`'s latest full-scan counts. Never raises."""
    try:
        r.hset(REDIS_DIGEST_FULLSCAN_COUNTS, company, f"{int(fetched)}:{int(matched)}")
        r.expire(REDIS_DIGEST_FULLSCAN_COUNTS, DIGEST_FULLSCAN_COUNTS_TTL_S)
    except Exception as exc:
        logger.warning("digest_counts: could not record counts for %r: %s", company, exc)


def read_fullscan_totals(r, exclude=()) -> dict:
    """Sum the stored counts, skipping companies in `exclude`.

    Returns {"fetched": int, "matched": int, "companies": int}; zeros when the
    hash is empty or Redis is unavailable.
    """
    totals = {"fetched": 0, "matched": 0, "companies": 0}
    if r is None:
        return totals
    try:
        entries = r.hgetall(REDIS_DIGEST_FULLSCAN_COUNTS) or {}
    except Exception as exc:
        logger.warning("digest_counts: could not read counts: %s", exc)
        return totals
    for company, value in entries.items():
        if isinstance(company, bytes):
            company = company.decode()
        if isinstance(value, bytes):
            value = value.decode()
        if company in exclude:
            continue
        try:
            fetched, matched = (int(x) for x in str(value).split(":", 1))
        except ValueError:
            continue
        totals["fetched"]   += fetched
        totals["matched"]   += matched
        totals["companies"] += 1
    return totals


def reset_fullscan_counts(r) -> None:
    """Start a new digest window. Never raises."""
    try:
        r.delete(REDIS_DIGEST_FULLSCAN_COUNTS)
    except Exception as exc:
        logger.warning("digest_counts: could not reset counts: %s", exc)
