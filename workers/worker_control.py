"""
workers/worker_control.py — Shared helper for starting and stopping systemd worker units.

Used by:
  - scripts/staleness_checker.py     (after pushing stale feins to queues)
  - scripts/fuzzy_match_uscis_dol.py (after bulk-queuing enrichment feins)
  - workers/manager.py               (autoscaling — start/stop based on queue depth)

Keeps the sudo systemctl start/stop logic, timeout, and warning handling in one place
so all callers stay in sync.
"""

import subprocess

from logger import get_logger

log = get_logger(__name__)

ENRICHMENT_WORKERS = ("domain-enrichment-worker@1", "domain-enrichment-worker@2")
DISCOVERY_WORKERS  = ("discover-h1b-ats-worker@1",  "discover-h1b-ats-worker@2")

# Allowlist also includes plain unit names (no instance suffix) so callers
# that pass the template unit don't get silently skipped.
_KNOWN_UNITS = frozenset(ENRICHMENT_WORKERS + DISCOVERY_WORKERS + (
    "domain-enrichment-worker",
    "discover-h1b-ats-worker",
))


def stop_workers(*units: str, dry_run: bool = False) -> None:
    """Stop one or more systemd units via `sudo systemctl stop`.

    Failures are logged as warnings; the caller is never interrupted.
    """
    for unit in units:
        if unit not in _KNOWN_UNITS:
            log.warning("stop_workers: unknown unit %r — skipping", unit)
            continue
        if dry_run:
            log.info("[dry-run] would stop %s", unit)
            continue
        try:
            res = subprocess.run(
                ["sudo", "-n", "systemctl", "stop", unit],
                check=False,
                timeout=10,
                capture_output=True,
            )
            if res.returncode == 0:
                log.info("stopped %s", unit)
            else:
                log.warning("systemctl stop %s rc=%d: %s", unit, res.returncode,
                            res.stderr.decode(errors="replace").strip())
        except Exception as exc:
            log.warning("could not stop %s: %s", unit, exc)


def start_workers(*units: str, dry_run: bool = False) -> None:
    """Start one or more systemd units via `sudo systemctl start`.

    systemctl start is a no-op when the unit is already active, so calling
    this after populating a queue is always safe — it only starts idle workers.
    Failures are logged as warnings; the caller is never interrupted.
    """
    for unit in units:
        if unit not in _KNOWN_UNITS:
            log.warning("start_workers: unknown unit %r — skipping", unit)
            continue
        if dry_run:
            log.info("[dry-run] would start %s", unit)
            continue
        try:
            res = subprocess.run(
                ["sudo", "-n", "systemctl", "start", "--no-block", unit],
                check=False,
                timeout=10,
                capture_output=True,
            )
            if res.returncode == 0:
                log.info("started %s", unit)
            else:
                log.warning("systemctl start %s rc=%d: %s", unit, res.returncode,
                            res.stderr.decode(errors="replace").strip())
        except Exception as exc:
            log.warning("could not start %s: %s", unit, exc)
