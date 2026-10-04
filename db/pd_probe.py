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
# timestamps / change-tracking columns). Per tier (jobs/public_domain.py::_record_probe_result):
#   shared decision : employer_fein .. cross_domain      (written by every probe)
#   OCI (direct)    : status .. error_type + impersonate (written by non-relay probes)
#   worker          : worker_*                           (written by non-relay probes)
#   relay           : relay_*                            (written by relay-mode probes only)
_FIELDS = (
    "employer_fein", "final_verdict", "final_reason", "resolved_by", "final_host", "cross_domain",
    "status", "final_url", "server", "fetch_via", "body_len", "ext_refs", "body_hash", "title",
    "snippet", "cookie_names", "header_names", "error_type",
    "worker_status", "worker_verdict", "worker_body_len", "worker_title",
    "worker_body_hash", "worker_cookie_names",
    "relay_status", "relay_verdict", "relay_body_len", "relay_title",
    "relay_body_hash", "relay_cookie_names",
    "impersonate",
)


def _build_sql(fields: tuple) -> str:
    return f"""
    INSERT INTO pd_probe_observation (domain, {", ".join(fields)})
    VALUES (?, {", ".join("?" for _ in fields)})
    ON CONFLICT (domain) DO UPDATE SET
        prev_verdict       = CASE WHEN pd_probe_observation.final_verdict IS DISTINCT FROM EXCLUDED.final_verdict
                                  THEN pd_probe_observation.final_verdict
                                  ELSE pd_probe_observation.prev_verdict END,
        verdict_changed_at = CASE WHEN pd_probe_observation.final_verdict IS DISTINCT FROM EXCLUDED.final_verdict
                                  THEN NOW()
                                  ELSE pd_probe_observation.verdict_changed_at END,
        probed_at          = NOW(),
        {", ".join(f"{f} = EXCLUDED.{f}" for f in fields)}
"""


def record_probe(domain: str, obs: dict) -> bool:
    """Upsert one probe observation. Returns True if written, False if skipped or failed (never raises).

    Only the columns whose keys are PRESENT in obs are written; a key that is absent leaves the stored
    value untouched (so a relay probe never wipes the OCI/worker evidence and vice versa). To clear a
    column, pass the key with value None. obs must carry final_verdict.
    """
    if not domain or not obs.get("final_verdict"):
        return False
    fields = tuple(f for f in _FIELDS if f in obs)
    params = (domain.lower().strip(),) + tuple(obs[f] for f in fields)
    sql = _build_sql(fields)
    for attempt in range(2):
        conn = None
        try:
            conn = get_conn()
            conn.execute(sql, params)
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
