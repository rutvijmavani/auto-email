# workers/digest_counts.py
# Per-company job counts from completed full scans, for the daily digest.
#
# A full scan fetches a company's whole board once per cycle, so its counts are
# the reliable "jobs fetched / matched" numbers.  Adaptive scans are left out:
# they are partial and repeat many times a day.
#
# One hash field per company, overwritten by that company's latest completed
# full scan — a re-run or resumed scan can never double count.  Once the digest
# has been sent, record_cycle_start() removes only the entries the digest read
# (compare-and-delete), so counts written meanwhile carry into the next digest.

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


def read_fullscan_counts(r) -> dict:
    """Return the raw stored counts as {company: "fetched:matched"} (decoded).

    Empty when the hash is empty or Redis is unavailable.  The digest reads
    this once and passes the same snapshot to sum_fullscan_counts() and, after
    the digest is sent, to reset_fullscan_counts().
    """
    if r is None:
        return {}
    try:
        entries = r.hgetall(REDIS_DIGEST_FULLSCAN_COUNTS) or {}
    except Exception as exc:
        logger.warning("digest_counts: could not read counts: %s", exc)
        return {}
    out = {}
    for company, value in entries.items():
        if isinstance(company, bytes):
            company = company.decode()
        if isinstance(value, bytes):
            value = value.decode()
        out[company] = str(value)
    return out


def sum_fullscan_counts(entries: dict, exclude=(), include=None) -> dict:
    """Sum `entries`, skipping companies in `exclude`.

    When `include` is given, only companies in it are counted (e.g. the
    currently monitorable set, so a disabled company's leftover count is not
    added to the digest).

    Returns {"fetched": int, "matched": int, "companies": int}.
    """
    totals = {"fetched": 0, "matched": 0, "companies": 0}
    for company, value in entries.items():
        if company in exclude:
            continue
        if include is not None and company not in include:
            continue
        try:
            fetched, matched = (int(x) for x in str(value).split(":", 1))
        except ValueError:
            continue
        totals["fetched"]   += fetched
        totals["matched"]   += matched
        totals["companies"] += 1
    return totals


def read_fullscan_totals(r, exclude=(), include=None) -> dict:
    """Read and sum the stored counts; zeros when empty or Redis is unavailable."""
    return sum_fullscan_counts(read_fullscan_counts(r), exclude, include)


# Delete each field only if it still holds the value the digest read, so a
# count a full scan wrote after the digest read it survives for the next digest.
_DELETE_IF_UNCHANGED_LUA = """
for i = 1, #ARGV, 2 do
    if redis.call('HGET', KEYS[1], ARGV[i]) == ARGV[i + 1] then
        redis.call('HDEL', KEYS[1], ARGV[i])
    end
end
return 1
"""


def reset_fullscan_counts(r, snapshot=None) -> None:
    """Start a new digest window. Never raises.

    With `snapshot` (the dict read_fullscan_counts() returned for the digest),
    only entries still equal to that snapshot are removed; entries written or
    changed since are kept.  Without it the whole hash is deleted.
    """
    try:
        if snapshot is None:
            r.delete(REDIS_DIGEST_FULLSCAN_COUNTS)
        elif snapshot:
            args = []
            for company, value in snapshot.items():
                args.extend((company, value))
            r.eval(_DELETE_IF_UNCHANGED_LUA, 1, REDIS_DIGEST_FULLSCAN_COUNTS, *args)
    except Exception as exc:
        logger.warning("digest_counts: could not reset counts: %s", exc)
