"""
workers/detail_payload.py — build the Redis push for one pending_detail row.

Shared by rebuild_detail_queue (restart recovery) and the detail drainer, so both
choose the same queue and payload for a row (docs/detail-pool-ceiling-th-design.md,
steps 3 and 7).

The queue follows the row's found_by: fullscan finds go to the fullscan queue,
everything else (adaptive scan, first scan, unknown) to the adaptive queue, the
same as the live push sites in scan_worker and fullscan.
"""

import json
from typing import Optional, Tuple

from config import REDIS_DETAIL_ADAPTIVE, REDIS_DETAIL_FULLSCAN
from workers.queue_stamp import stamp_queued_at

FULLSCAN_FOUND_BY = "tier2_fullscan"

# Platforms whose detail payloads require ephemeral keys extracted from the
# listing-scan API response (_slug, _wd, _path, _base_url, etc.). job_postings
# only stores job_url/job_id — a payload cannot be reconstructed for these when the
# row has no stored detail_payload. Their pending_detail rows are re-discovered on
# the next adaptive/fullscan cycle.  Keep in sync with _REQUIRED_DETAIL_KEYS in
# workers/detail_worker.py.
SKIP_PLATFORMS_WITHOUT_PAYLOAD = frozenset({
    "workday", "taleo", "smartrecruiters", "icims", "jobvite",
})


def queue_for_found_by(found_by: Optional[str]) -> str:
    """Detail queue a row belongs to: fullscan finds -> fullscan, all others -> adaptive."""
    return REDIS_DETAIL_FULLSCAN if found_by == FULLSCAN_FOUND_BY else REDIS_DETAIL_ADAPTIVE


def build_detail_push(row) -> Optional[Tuple[str, str]]:
    """
    Return (queue, payload_json) for a pending_detail row, or None when the row
    cannot be rebuilt (no stored payload and a platform that needs ephemeral keys).

    row needs: company, job_url, job_id, ats_platform, detail_payload, found_by.
    queued_at is never persisted, so the push adds a fresh one (enqueued_at stays).
    """
    found_by = row["found_by"]
    queue = queue_for_found_by(found_by)

    stored = row["detail_payload"]
    if isinstance(stored, str):
        try:
            stored = json.loads(stored)
        except Exception:
            stored = None

    if isinstance(stored, dict):
        # Full payload persisted at scan time: it already carries every
        # platform-specific key and the real found_by.
        return queue, json.dumps(stamp_queued_at(stored))
    if row["detail_payload"]:
        # Stored but unparsable: push the raw value as it was (unchanged behaviour).
        return queue, row["detail_payload"]
    if row["ats_platform"] in SKIP_PLATFORMS_WITHOUT_PAYLOAD:
        return None

    # Old row for a platform whose minimal payload is sufficient.
    return queue, json.dumps(stamp_queued_at({
        "company":      row["company"],
        "job_url":      row["job_url"],
        "job_id":       row["job_id"],
        "ats_platform": row["ats_platform"],
        "found_by":     found_by or "rebuild",
    }))
