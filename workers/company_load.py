"""
workers/company_load.py — per-company scan cost table for the scan / fullscan pools.

The manager sizes scan and fullscan from work, not from a count of companies
(docs/scan-fullscan-sizing-plan.md).  For every company it needs:

    d  how long one run takes (the EMA the DB keeps: avg_scan_duration_s /
       avg_fullscan_duration_s, never the raw last duration)
    T  how long after a run finishes the next one is due (current_interval_s /
       full_scan_interval_s; the next run is scheduled at completion + T)

so a company keeps d / (d + T) of one worker busy.  The table holds "d,T" per
company plus the running total of d / (d + T) over all companies:

    manager:company_load:{pool}          hash  company -> "d,T"
    manager:company_load:{pool}:total    float sum of d / (d + T)

update()   called after every run finishes; changes the total by the difference,
           atomically, and does nothing while no table exists (a partial table
           built from events alone would read as a complete one).
rebuild()  builds both keys from company_poll_stats, swaps them in atomically and
           gives them COMPANY_LOAD_TTL_S.  Run by the nightly recompute and by
           steady_load() whenever the table is missing.
"""

import math

from config import COMPANY_LOAD_TTL_S, SCHEDULER_FULL_SCAN_INTERVAL_S
from logger import get_logger

logger = get_logger(__name__)

# pool -> (average-duration column, interval column) in company_poll_stats
_COLUMNS = {
    "scan":     ("avg_scan_duration_s",     "current_interval_s"),
    "fullscan": ("avg_fullscan_duration_s", "full_scan_interval_s"),
}

# company_poll_stats.*_interval_s DEFAULT 86400 (db/schema.py); a NULL interval
# counts as that default instead of dropping the company from the total.
_DEFAULT_INTERVAL_S = SCHEDULER_FULL_SCAN_INTERVAL_S

_REBUILD_CHUNK = 1000

_UPDATE_LUA = """
if redis.call('EXISTS', KEYS[2]) == 0 then
    return 0
end
local function load(v)
    local d, t = string.match(v, '^([^,]+),(.+)$')
    d = tonumber(d)
    t = tonumber(t)
    if d == nil or t == nil or d + t <= 0 then
        return 0
    end
    return d / (d + t)
end
local old = redis.call('HGET', KEYS[1], ARGV[1])
local old_load = 0
if old then
    old_load = load(old)
end
local new_value = ARGV[2] .. ',' .. ARGV[3]
redis.call('HSET', KEYS[1], ARGV[1], new_value)
redis.call('INCRBYFLOAT', KEYS[2], load(new_value) - old_load)
return 1
"""


def _hash_key(pool: str) -> str:
    return f"manager:company_load:{pool}"


def _total_key(pool: str) -> str:
    return f"manager:company_load:{pool}:total"


def load_of(d: float, t: float) -> float:
    """Share of one worker a company keeps busy: d / (d + T)."""
    if d is None or t is None or d < 0 or t < 0 or d + t <= 0:
        return 0.0
    return d / (d + t)


def backlog_term(durations: list, horizon_s: float) -> float:
    """
    Workers needed to clear the late companies within the horizon: each company
    counts min(d / horizon, 1) because one run can use at most one worker.
    """
    if horizon_s <= 0:
        return float(len(durations))
    return sum(min(max(d, 0.0) / horizon_s, 1.0) for d in durations)


def update(r, pool: str, company: str, d, t) -> None:
    """
    Store a company's new (d, T) after a run finished and move the pool total by
    the difference.  Never raises: a scan completion must not fail on the table.
    """
    if d is None or t is None:
        return
    try:
        r.eval(_UPDATE_LUA, 2, _hash_key(pool), _total_key(pool),
               company, repr(float(d)), repr(float(t)))
    except Exception as exc:
        logger.warning("company_load: update failed [%s] %r: %s", pool, company, exc)


def rebuild(r, pool: str, default_d: float) -> int:
    """
    Build the table from company_poll_stats and swap it in atomically.  A NULL
    average takes default_d (the pool's P75: unknown is not zero), a NULL interval
    the schema default.  Returns the number of companies.  Raises on a DB or Redis
    error so the caller decides what to do; the old table stays untouched then.
    """
    from db.db import get_conn

    d_col, t_col = _COLUMNS[pool]
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(f"SELECT company, {d_col} AS d, {t_col} AS t FROM company_poll_stats")  # noqa: S608
        rows = cur.fetchall()

    entries: dict = {}
    total = 0.0
    for row in rows:
        d = float(row["d"]) if row["d"] is not None else float(default_d)
        t = float(row["t"]) if row["t"] is not None else float(_DEFAULT_INTERVAL_S)
        entries[row["company"]] = f"{d!r},{t!r}"
        total += load_of(d, t)

    hash_key, total_key = _hash_key(pool), _total_key(pool)
    tmp_hash, tmp_total = f"{hash_key}:building", f"{total_key}:building"
    r.delete(tmp_hash, tmp_total)
    items = list(entries.items())
    for i in range(0, len(items), _REBUILD_CHUNK):
        r.hset(tmp_hash, mapping=dict(items[i:i + _REBUILD_CHUNK]))
    r.set(tmp_total, repr(total))

    pipe = r.pipeline(transaction=True)
    if items:
        pipe.rename(tmp_hash, hash_key)
        pipe.expire(hash_key, COMPANY_LOAD_TTL_S)
    else:
        pipe.delete(hash_key)
    pipe.rename(tmp_total, total_key)
    pipe.expire(total_key, COMPANY_LOAD_TTL_S)
    pipe.execute()

    logger.info("company_load: rebuilt [%s] companies=%d steady_load=%.3f workers",
                pool, len(items), total)
    return len(items)


def steady_load(r, pool: str, default_d: float) -> "float | None":
    """
    Sum of d / (d + T) over all companies of the pool, in workers.  A missing
    table is rebuilt from the DB on the spot.  None when it cannot be read or
    rebuilt (the caller keeps its last value; unknown is never reported as 0).
    """
    try:
        raw = r.get(_total_key(pool))
        if raw is None:
            logger.warning("company_load: table [%s] missing — rebuilding from DB", pool)
            rebuild(r, pool, default_d)
            raw = r.get(_total_key(pool))
        return max(0.0, float(raw))
    except Exception as exc:
        logger.error("company_load: steady load [%s] unavailable: %s", pool, exc)
        return None


def entries(r, pool: str, names: list, default_d: float) -> list:
    """
    (d, T, in_table) of each named company.  A company without a usable entry gets
    (default_d, None, False): it is not part of the stored total, so nothing may be
    subtracted for it (see steady_excluding).
    """
    if not names:
        return []
    out = []
    for value in r.hmget(_hash_key(pool), names):
        d = t = None
        if value:
            try:
                raw_d, raw_t = value.split(",", 1)
                d, t = float(raw_d), float(raw_t)
            except (ValueError, TypeError):
                d = t = None
        if d is not None and t is not None and math.isfinite(d) and math.isfinite(t):
            out.append((d, t, True))
        else:
            out.append((float(default_d), None, False))
    return out


def steady_excluding(total: float, rows: list) -> float:
    """
    The pool's steady load without the given companies: total minus d / (d + T) of
    each row that is in the table, clamped at 0.
    """
    rest = float(total)
    for d, t, in_table in rows:
        if in_table:
            rest -= load_of(d, t)
    return max(0.0, rest)


def durations(r, pool: str, names: list, default_d: float) -> list:
    """
    d of each named company; a company without an entry (new, or the table was
    just rebuilt without it) takes default_d.
    """
    if not names:
        return []
    out = []
    values = r.hmget(_hash_key(pool), names)
    for value in values:
        d = None
        if value:
            try:
                d = float(value.split(",", 1)[0])
            except (ValueError, TypeError):
                d = None
        out.append(d if d is not None and math.isfinite(d) else float(default_d))
    return out
