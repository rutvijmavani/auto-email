# logger.py — Centralized logging for the recruiter pipeline
#
# Usage in any module:
#   from logger import get_logger
#   logger = get_logger(__name__)
#   logger.debug("Checking slug: %s", slug)
#   logger.info("Detection complete for %s", company)
#   logger.warning("Serper credits low: %d remaining", credits)
#   logger.error("API fetch failed for %s: %s", company, e)
#
# Log files written to:
#   logs/monitor_YYYY-MM-DD.log   → --monitor-jobs runs
#   logs/detect_YYYY-MM-DD.log    → --detect-ats runs
#   logs/sync_YYYY-MM-DD.log      → --sync-forms runs
#   logs/find_YYYY-MM-DD.log      → --find-only runs
#   logs/pipeline.log             → always-on catch-all (all commands)
#
# During testing: DEBUG level — every step logged
# Format: 2026-03-12 09:14:32.401 | DEBUG | jobs.serper | Slug rejected: ...

import logging
import logging.handlers
import os
import sys
from datetime import datetime
from pathlib import Path

# ─────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────

LOG_DIR   = Path(__file__).parent / "logs"
LOG_LEVEL = logging.DEBUG   # verbose during testing — change to INFO for prod

LOG_FORMAT  = "%(asctime)s.%(msecs)03d | %(levelname)-8s | %(name)s | %(message)s"
DATE_FORMAT = "%Y-%m-%d %H:%M:%S"

# Map CLI flag → log filename prefix
# Pipeline sets this via init_logging(command="monitor") at startup
_active_command: str = "pipeline"

# Module-level guard — init_logging() called only once per process
_initialized: bool = False


# ─────────────────────────────────────────
# PUBLIC API
# ─────────────────────────────────────────

def init_logging(command: str = "pipeline") -> None:
    """
    Call once at the top of each pipeline entry point.

    Args:
        command: short name matching the CLI flag, e.g.
                 "monitor"  → logs/monitor_YYYY-MM-DD.log
                 "detect"   → logs/detect_YYYY-MM-DD.log
                 "sync"     → logs/sync_YYYY-MM-DD.log
                 "find"     → logs/find_YYYY-MM-DD.log
    """
    global _active_command, _initialized
    if _initialized:
        return

    _active_command = command
    _initialized    = True

    LOG_DIR.mkdir(parents=True, exist_ok=True)

    root = logging.getLogger()
    root.setLevel(LOG_LEVEL)

    # Remove any handlers added by third-party libs before we configure
    root.handlers.clear()

    formatter = logging.Formatter(LOG_FORMAT, datefmt=DATE_FORMAT)

    # ── 1. Console handler ──────────────────────────────────────────
    # Only active when running interactively (TTY present).
    # Under cron, stdout is NOT a TTY, so we skip this handler.
    # Previously cron redirected stdout to the same file the
    # TimedRotatingFileHandler already writes to — causing every
    # log line to appear twice.  Skipping the console handler under
    # cron eliminates the duplication while keeping the file handlers.
    if sys.stdout.isatty():
        console = logging.StreamHandler(sys.stdout)
        console.setLevel(LOG_LEVEL)
        console.setFormatter(formatter)
        root.addHandler(console)

    # ── 2. Command-specific rotating file ──────────────────────────
    # Separate file per command so monitor logs don't mix with detect logs.
    # Rotates at midnight, keeps 14 days of history.
    today        = datetime.now().strftime("%Y-%m-%d")
    command_file = LOG_DIR / f"{command}_{today}.log"

    file_handler = logging.handlers.TimedRotatingFileHandler(
        filename    = command_file,
        when        = "midnight",
        backupCount = 14,           # keep 14 days of daily logs
        encoding    = "utf-8",
    )
    file_handler.setLevel(LOG_LEVEL)
    file_handler.setFormatter(formatter)
    root.addHandler(file_handler)

    # ── 3. Catch-all pipeline.log ───────────────────────────────────
    # Always-on single file — useful for grepping across all commands.
    # Rotates at midnight, keeps 30 days so monthly scripts (e.g.
    # build_ats_slug_list.py, enrich_ats_companies.py) whose logs
    # fall through here are retained beyond the 14-day window.
    catchall = logging.handlers.TimedRotatingFileHandler(
        filename    = LOG_DIR / "pipeline.log",
        when        = "midnight",
        backupCount = 30,
        encoding    = "utf-8",
    )
    catchall.setLevel(LOG_LEVEL)
    catchall.setFormatter(formatter)
    root.addHandler(catchall)

    # ── 4. Silence noisy third-party loggers ───────────────────────
    # These would flood DEBUG output with HTTP wire traces.
    for noisy in ("urllib3", "requests", "urllib3.connectionpool",
                  "google.auth", "google.oauth2", "gspread"):
        logging.getLogger(noisy).setLevel(logging.WARNING)

    logging.getLogger(__name__).info(
        "Logging initialised — command=%s level=DEBUG file=%s",
        command, command_file.name
    )


def get_logger(name: str) -> logging.Logger:
    """
    Return a logger for the given module.

    Use as:
        logger = get_logger(__name__)

    This works correctly whether or not init_logging() has been
    called yet — the root logger will pick up handlers lazily.
    """
    return logging.getLogger(name)