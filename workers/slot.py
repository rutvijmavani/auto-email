"""
workers/slot.py — Deterministic per-company slot-offset utility (Section 25).

Used by the two-layer scheduler to spread companies evenly across a 24-hour
window so adaptive polls don't cluster at the same time (thundering herd).

──────────────────────────────────────────────────────────────────────────────
Why MD5 instead of Python's built-in hash()?

Python's hash() is identity for small integers:
    hash(1) == 1,  hash(2) == 2,  hash(3) == 3 …

If we used hash(batch_position) % 86400 for a batch of 10 new companies
(batch_position = 1 … 10), ALL of them would land within the first 10 seconds
of midnight, completely defeating the spread.  MD5 produces a uniform
hex digest regardless of the numeric magnitude of the input.
──────────────────────────────────────────────────────────────────────────────

Usage:

    from workers.slot import slot_offset

    # At registration time — store in DB so it survives restarts:
    offset_s = slot_offset(batch_position)   # batch_position = 1, 2, 3 …
    db.execute("UPDATE company_poll_stats SET initial_slot_offset_s = %s …", offset_s)

    # At first adaptive-poll scheduling (on_fullscan_complete for new company):
    offset_s = company.initial_slot_offset_s   # read back from DB
    first_poll_at = today_midnight_eastern + offset_s
    if first_poll_at <= time.time():
        first_poll_at += 86400             # push to tomorrow's slot

Example distribution for batch_position 1–5:
    slot_offset(1) → 27 291 s  (07:34 AM)
    slot_offset(2) → 68 104 s  (18:55 PM)
    slot_offset(3) → 41 840 s  (11:37 AM)
    slot_offset(4) → 14 523 s  (04:02 AM)
    slot_offset(5) → 55 217 s  (15:20 PM)
"""

import hashlib


def slot_offset(identifier) -> int:
    """
    Return a deterministic, well-distributed slot offset in [0, 86400).

    Uses MD5 (not Python's built-in hash()) to guarantee:
    - Platform-independent results (CPython, PyPy, etc.)
    - Uniform distribution regardless of the numeric value of identifier
    - Stable output across process restarts (no PYTHONHASHSEED randomisation)

    The same identifier always produces the same offset, so the schedule
    is reproducible from DB data alone without any Redis state.

    Args:
        identifier: any value — converted to str before hashing.
                    Typically batch_position (int 1, 2, 3 …) at registration,
                    or company_id (int) for recurring polls after first scan.

    Returns:
        int in [0, 86400) — seconds from midnight for this company's daily slot.
    """
    digest = hashlib.md5(str(identifier).encode()).hexdigest()
    return int(digest, 16) % 86400
