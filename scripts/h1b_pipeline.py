"""
scripts/h1b_pipeline.py — Central orchestrator for the H1B ATS discovery pipeline.

Runs daily via systemd timer (h1b-pipeline.timer). Chains four steps in order:

  Step 1: sync_dol_lca.py          — auto-download new DOL LCA quarterly files
  Step 2: process_uscis_h1b.py     — ingest USCIS petition CSV (if file dropped)
  Step 3: fuzzy_match_uscis_dol.py — match USCIS rows to DOL employers; ambiguous
                                     rows pushed to h1b_llm_worker async (no wait)
  Step 4: discover_h1b_ats.py      — probe DISCOVER_BATCH_SIZE new companies:
                                     KG → SPARQL → 19-path probe → career_page.py
                                     → career_detector.py → h1b_ats_discovery +
                                     company_ats

Step 2 is skipped unless a CSV/XLSX is present in USCIS_DROP_DIR/pending/.
After ingestion the file is moved to USCIS_DROP_DIR/processed/.

Step 3.5 (re-detect stale): queries company_ats for entries with
consecutive_empty_days >= DISCOVER_REDETECT_EMPTY_DAYS and re-runs
discover_h1b_ats.py --fein for each before the main batch (Step 4).

Usage:
    python scripts/h1b_pipeline.py               # normal daily run
    python scripts/h1b_pipeline.py --dry-run     # print commands, don't execute
    python scripts/h1b_pipeline.py --skip-sync   # skip Step 1 (DOL sync)
    python scripts/h1b_pipeline.py --only-discover  # jump straight to Step 4
"""

import argparse
import os
import shutil
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from config import DISCOVER_BATCH_SIZE, DISCOVER_REDETECT_EMPTY_DAYS
from db.connection import get_conn
from logger import get_logger, init_logging

log = get_logger(__name__)

PROJECT_DIR  = Path(__file__).resolve().parent.parent
PYTHON       = sys.executable
SCRIPTS_DIR  = PROJECT_DIR / "scripts"
USCIS_DROP_DIR = PROJECT_DIR / "uscis_drops"


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _run(cmd: list[str], dry_run: bool, step_name: str) -> bool:
    """Run a subprocess command. Returns True on success, False on failure."""
    log.info("▶ %s: %s", step_name, " ".join(str(c) for c in cmd))
    if dry_run:
        return True
    result = subprocess.run(cmd, cwd=str(PROJECT_DIR))
    if result.returncode != 0:
        log.error("✗ %s failed (exit %d)", step_name, result.returncode)
        return False
    log.info("✓ %s complete", step_name)
    return True


def _find_uscis_file() -> Path | None:
    """Return the first CSV/XLSX in uscis_drops/pending/, or None."""
    pending = USCIS_DROP_DIR / "pending"
    pending.mkdir(parents=True, exist_ok=True)
    for ext in ("*.csv", "*.xlsx", "*.xls"):
        matches = sorted(pending.glob(ext))
        if matches:
            return matches[0]
    return None


def _mark_uscis_processed(path: Path) -> None:
    processed = USCIS_DROP_DIR / "processed"
    processed.mkdir(parents=True, exist_ok=True)
    dest = processed / path.name
    shutil.move(str(path), str(dest))
    log.info("  USCIS file moved → %s", dest)


def _stale_feins() -> list[str]:
    """FEINs in company_ats with consecutive_empty_days >= threshold."""
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT DISTINCT employer_fein
            FROM company_ats
            WHERE is_monitored = TRUE
              AND employer_fein IS NOT NULL
              AND consecutive_empty_days >= %s
        """, (DISCOVER_REDETECT_EMPTY_DAYS,))
        return [row["employer_fein"] for row in cur.fetchall()]
    finally:
        conn.close()


# ─────────────────────────────────────────────────────────────────────────────
# Pipeline steps
# ─────────────────────────────────────────────────────────────────────────────

def step1_sync_dol(dry_run: bool) -> bool:
    ok = _run(
        [PYTHON, str(SCRIPTS_DIR / "sync_dol_lca.py")],
        dry_run, "Step 1a: DOL LCA sync",
    )
    if not ok:
        return False
    return _run(
        [PYTHON, str(SCRIPTS_DIR / "build_email_patterns.py")],
        dry_run, "Step 1b: rebuild email patterns",
    )


def step2_uscis(dry_run: bool) -> bool:
    uscis_file = _find_uscis_file()
    if not uscis_file:
        log.info("Step 2: no USCIS file in %s/pending/ — skipping", USCIS_DROP_DIR)
        return True
    log.info("Step 2: USCIS file found: %s", uscis_file.name)
    ok = _run(
        [PYTHON, str(SCRIPTS_DIR / "process_uscis_h1b.py"), "--file", str(uscis_file)],
        dry_run, "Step 2: USCIS ingest",
    )
    if ok and not dry_run:
        _mark_uscis_processed(uscis_file)
    return ok


def step3_fuzzy_match(dry_run: bool) -> bool:
    return _run(
        [PYTHON, str(SCRIPTS_DIR / "fuzzy_match_uscis_dol.py")],
        dry_run, "Step 3: fuzzy match USCIS → DOL",
    )


def step3_5_redetect_stale(dry_run: bool) -> bool:
    """Re-run discovery on company_ats entries going stale in job monitoring."""
    feins = [] if dry_run else _stale_feins()
    if not feins:
        log.info("Step 3.5: no stale company_ats entries — skipping")
        return True
    log.info("Step 3.5: re-detecting %d stale company_ats entries", len(feins))
    for fein in feins:
        ok = _run(
            [PYTHON, str(SCRIPTS_DIR / "discover_h1b_ats.py"), "--fein", fein, "--force"],
            dry_run, f"Step 3.5: re-detect FEIN={fein}",
        )
        if not ok:
            log.warning("  re-detect failed for FEIN=%s — continuing", fein)
    return True


def step4_discover(dry_run: bool) -> bool:
    return _run(
        [PYTHON, str(SCRIPTS_DIR / "discover_h1b_ats.py"), "--top", str(DISCOVER_BATCH_SIZE)],
        dry_run, f"Step 4: discover ATS (batch={DISCOVER_BATCH_SIZE})",
    )


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

def main() -> None:
    init_logging("h1b_pipeline")

    parser = argparse.ArgumentParser(description="H1B ATS discovery pipeline orchestrator")
    parser.add_argument("--dry-run",       action="store_true", help="Print commands without executing")
    parser.add_argument("--skip-sync",     action="store_true", help="Skip Step 1 (DOL LCA sync)")
    parser.add_argument("--skip-fuzzy",    action="store_true", help="Skip Step 3 (fuzzy match)")
    parser.add_argument("--only-discover", action="store_true", help="Run Step 4 only")
    args = parser.parse_args()

    if args.dry_run:
        log.info("DRY RUN — no commands will be executed")

    steps = [
        (not args.skip_sync and not args.only_discover, step1_sync_dol),
        (not args.only_discover,                        step2_uscis),
        (not args.skip_fuzzy and not args.only_discover, step3_fuzzy_match),
        (not args.only_discover,                         step3_5_redetect_stale),
        (True,                                           step4_discover),
    ]

    for should_run, step_fn in steps:
        if not should_run:
            continue
        if not step_fn(args.dry_run):
            log.error("Pipeline aborted at %s", step_fn.__name__)
            sys.exit(1)

    log.info("Pipeline complete.")


if __name__ == "__main__":
    main()
