# db/pd_probe.py — upsert of the latest public-domain probe result per domain.
#
# Backs pd_probe_observation (db/schema.py). Records the verdict from jobs/pd_classify.py plus the
# evidence behind it (status, content fingerprint, per-tier results) so parked/challenge verdicts can be
# re-checked and Rule 2/3 candidate mining (scripts/pd_candidates.py) can run on live data.
#
# Best-effort by design: this is observability, never a gate. A failed write is logged and swallowed so
# it can never break public-domain resolution. Same synchronous retry-once pattern as
# db/external_api_health.py::record_external_request.

from db.connection import get_conn
from logger import get_logger

log = get_logger(__name__)

# Columns taken straight from the observation dict (everything except the key and the managed
# timestamps / change-tracking columns).
_FIELDS = (
    "employer_fein", "final_verdict", "final_reason", "resolved_by", "final_host", "cross_domain",
    "status", "final_url", "server", "fetch_via", "body_len", "ext_refs", "body_hash", "title",
    "snippet", "cookie_names", "header_names", "error_type",
    "worker_status", "worker_verdict", "worker_body_len", "worker_title",
    "relay_status", "relay_verdict", "relay_body_len", "relay_title",
    "impersonate",
)

_SQL = f"""
    INSERT INTO pd_probe_observation (domain, {", ".join(_FIELDS)})
    VALUES (?, {", ".join("?" for _ in _FIELDS)})
    ON CONFLICT (domain) DO UPDATE SET
        prev_verdict       = CASE WHEN pd_probe_observation.final_verdict IS DISTINCT FROM EXCLUDED.final_verdict
                                  THEN pd_probe_observation.final_verdict
                                  ELSE pd_probe_observation.prev_verdict END,
        verdict_changed_at = CASE WHEN pd_probe_observation.final_verdict IS DISTINCT FROM EXCLUDED.final_verdict
                                  THEN NOW()
                                  ELSE pd_probe_observation.verdict_changed_at END,
        probed_at          = NOW(),
        {", ".join(f"{f} = EXCLUDED.{f}" for f in _FIELDS)}
"""


def record_probe(domain: str, obs: dict) -> bool:
    """Upsert one probe observation. Returns True if written, False if skipped or failed (never raises).

    obs may omit any field in _FIELDS (stored as NULL) but must carry final_verdict.
    """
    if not domain or not obs.get("final_verdict"):
        return False
    params = (domain.lower().strip(),) + tuple(obs.get(f) for f in _FIELDS)
    for attempt in range(2):
        conn = None
        try:
            conn = get_conn()
            conn.execute(_SQL, params)
            conn.commit()
            return True
        except Exception as exc:
            if conn is not None:
                try:
                    conn.rollback()
                except Exception:
                    pass
            if attempt == 1:
                log.warning("pd_probe: could not record %s: %s", domain, exc)
        finally:
            if conn is not None:
                try:
                    conn.close()
                except Exception:
                    pass
    return False
