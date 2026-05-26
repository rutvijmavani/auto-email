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
# Log files written to (all in logs/):
#
#   Daily commands  → {command}_YYYY-MM-DD.log   (14-day retention)
#     monitor, outreach, sync, nightly, monday, weekly,
#     detect, verify_filled, find, …
#
#   Monthly commands → {command}_YYYY-MM.log      (35-day retention)
#     monthly, enrich, build_ats_slug_list
#
#   Catch-all       → pipeline_YYYY-MM-DD.log     (14-day retention)
#     every command also writes here — useful for grepping
#     across all commands in a single day
#
# Retention is controlled by LOG_RETENTION_DAILY_DAYS /
# LOG_RETENTION_MONTHLY_DAYS in config.py and enforced by
# _cleanup_old_logs() which runs once per process at startup.
# See _cleanup_old_logs() docstring for the full retention table.
#
# Format: 2026-03-12 09:14:32.401 | DEBUG | jobs.serper | Slug rejected: ...

import logging
import logging.handlers
import os
import re
import sys
from datetime import datetime, timedelta
from pathlib import Path

# ─────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────

LOG_DIR   = Path(__file__).parent / "logs"
LOG_LEVEL = logging.DEBUG   # verbose during testing — change to INFO for prod

LOG_FORMAT  = "%(asctime)s.%(msecs)03d | %(levelname)-8s | %(name)s | %(message)s"
DATE_FORMAT = "%Y-%m-%d %H:%M:%S"

# Commands whose logs accumulate for a full month.
# These use YYYY-MM filenames and get LOG_RETENTION_MONTHLY_DAYS retention.
# Everything else uses YYYY-MM-DD and gets LOG_RETENTION_DAILY_DAYS retention.
_MONTHLY_COMMANDS = frozenset({"monthly", "enrich", "build_ats_slug_list"})

# Map CLI flag → log filename prefix
# Pipeline sets this via init_logging(command="monitor") at startup
_active_command: str = "pipeline"

# Module-level guard — init_logging() called only once per process
_initialized: bool = False


# ─────────────────────────────────────────
# INTERNAL HELPERS
# ─────────────────────────────────────────

# Filename patterns — used by _cleanup_old_logs().
#
# Monthly:  something_YYYY-MM.log
#             e.g. monthly_2026-04.log, enrich_2026-04.log
# Daily:    something_YYYY-MM-DD.log
#             e.g. monitor_2026-04-01.log, enrich_ats_companies_2026-04-01.log
#             NOTE: build_ats_slug_list_YYYY-MM-DD.log also matches this but
#             is a monthly-run command → gets monthly retention (35d) via
#             cmd_name lookup in _MONTHLY_COMMANDS inside _cleanup_old_logs().
# Rotation: something.log.YYYY-MM-DD
#             TimedRotatingFileHandler backups for long-running processes
#             e.g. pipeline.log.2026-05-01, scheduler_2026-05-06.log.2026-05-10
# Plain:    something.log (no date)
#             Long-running process logs: scheduler.log, fullscan.log, pipeline.log
#             Deleted once mtime ages past the daily retention window.
_MONTHLY_LOG_RE   = re.compile(r'^(.+)_\d{4}-\d{2}\.log$')
_DAILY_LOG_RE     = re.compile(r'^(.+)_\d{4}-\d{2}-\d{2}\.log$')
_ROTATION_LOG_RE  = re.compile(r'^.+\.log\.\d{4}-\d{2}-\d{2}$')


def _cleanup_old_logs() -> tuple[int, int]:
    """
    Delete log files that have exceeded their retention window.
    Returns (deleted_count, error_count).

    WHY THIS EXISTS
    ───────────────
    TimedRotatingFileHandler only prunes old backup files when the *same*
    running process crosses midnight and triggers a rotation.  Every pipeline
    command (monitor, detect, enrich …) runs as a short-lived cron process
    that terminates in minutes — the handler is never alive at midnight, so
    rotation and cleanup never fire.  Logs from months ago accumulate forever.

    This function is called once per process at the top of init_logging() and
    uses mtime (last-written time) to decide what to delete.

    Retention (mirrors LOG_RETENTION_* in config.py):

      *_YYYY-MM.log             monthly commands (monthly, enrich,         35d
                                build_ats_slug_list) — one file per month

      *_YYYY-MM-DD.log          daily commands (monitor, sync, outreach…)  14d
                                EXCEPT: if the command prefix is in
                                _MONTHLY_COMMANDS the file is treated as
                                a monthly-run in legacy YYYY-MM-DD format
                                (e.g. build_ats_slug_list_YYYY-MM-DD.log)
                                and gets 35d retention.

      *.log.YYYY-MM-DD          TimedRotatingFileHandler rotation backups   14d
                                for long-running processes:
                                  pipeline.log.2026-05-01
                                  scheduler_2026-05-06.log.2026-05-10

      *.log  (no date suffix)   Plain logs from long-running processes:     14d
                                  scheduler.log, fullscan.log, pipeline.log
                                Only deleted once the process stops writing
                                (mtime ages out naturally).
    """
    if not LOG_DIR.exists():
        return 0, 0

    # Import retention constants from config — fall back to safe defaults if
    # config is unavailable (e.g., during unit tests that import logger directly).
    try:
        from config import LOG_RETENTION_DAILY_DAYS, LOG_RETENTION_MONTHLY_DAYS
    except ImportError:
        LOG_RETENTION_DAILY_DAYS   = 14
        LOG_RETENTION_MONTHLY_DAYS = 35

    now            = datetime.now()
    daily_cutoff   = now - timedelta(days=LOG_RETENTION_DAILY_DAYS)
    monthly_cutoff = now - timedelta(days=LOG_RETENTION_MONTHLY_DAYS)

    deleted, errors = 0, 0
    for entry in LOG_DIR.iterdir():
        if not entry.is_file():
            continue
        try:
            mtime = datetime.fromtimestamp(entry.stat().st_mtime)
            name  = entry.name

            m = _MONTHLY_LOG_RE.match(name)
            if m:
                # e.g. monthly_2026-04.log, enrich_2026-04.log
                if mtime < monthly_cutoff:
                    entry.unlink(missing_ok=True)
                    deleted += 1
                continue

            m = _DAILY_LOG_RE.match(name)
            if m:
                # Extract the command name (everything before _YYYY-MM-DD).
                # build_ats_slug_list_YYYY-MM-DD.log is a monthly-run command
                # written in legacy YYYY-MM-DD format — give it monthly retention.
                cmd_name = m.group(1)
                cutoff   = (monthly_cutoff if cmd_name in _MONTHLY_COMMANDS
                            else daily_cutoff)
                if mtime < cutoff:
                    entry.unlink(missing_ok=True)
                    deleted += 1
                continue

            if _ROTATION_LOG_RE.match(name):
                # TimedRotatingFileHandler backups from long-running processes:
                #   pipeline.log.2026-05-01
                #   scheduler_2026-05-06.log.2026-05-10
                #   scheduler_2026-05-23.log.2026-05-23
                if mtime < daily_cutoff:
                    entry.unlink(missing_ok=True)
                    deleted += 1
                continue

            if name.endswith(".log"):
                # Plain undated logs: scheduler.log, fullscan.log, pipeline.log
                # These are written by long-running processes whose mtime is
                # always "now" while the process runs.  They are only deleted
                # once the process stops and the file stops being written to.
                if mtime < daily_cutoff:
                    entry.unlink(missing_ok=True)
                    deleted += 1

        except OSError:
            errors += 1

    return deleted, errors


# ─────────────────────────────────────────
# PUBLIC API
# ─────────────────────────────────────────

def init_logging(command: str = "pipeline") -> None:
    """
    Call once at the top of each pipeline entry point.

    Args:
        command: short name matching the CLI flag, e.g.
                 "monitor"            → logs/monitor_YYYY-MM-DD.log
                 "detect"             → logs/detect_YYYY-MM-DD.log
                 "sync"               → logs/sync_YYYY-MM-DD.log
                 "monthly"            → logs/monthly_YYYY-MM.log
                 "enrich"             → logs/enrich_YYYY-MM.log
                 "build_ats_slug_list"→ logs/build_ats_slug_list_YYYY-MM.log

    Monthly commands (in _MONTHLY_COMMANDS) use YYYY-MM filenames so all
    runs within the same month append to the same file.  Everything else
    uses YYYY-MM-DD (one file per day).

    A catch-all pipeline_YYYY-MM-DD.log is always written alongside the
    command-specific file — useful for grepping across all commands in a day.
    """
    global _active_command, _initialized
    if _initialized:
        return

    _active_command = command
    _initialized    = True

    LOG_DIR.mkdir(parents=True, exist_ok=True)

    # Run cleanup before configuring handlers so the stats can be logged below.
    _deleted, _errors = _cleanup_old_logs()

    root = logging.getLogger()
    root.setLevel(LOG_LEVEL)

    # Remove any handlers added by third-party libs before we configure
    root.handlers.clear()

    formatter = logging.Formatter(LOG_FORMAT, datefmt=DATE_FORMAT)

    # ── 1. Console handler ──────────────────────────────────────────
    # Only active when running interactively (TTY present).
    # Under cron, stdout is NOT a TTY, so we skip this handler.
    # Previously cron redirected stdout to the same file the
    # FileHandler already writes to — causing every log line to
    # appear twice.  Skipping the console handler under cron
    # eliminates the duplication while keeping the file handlers.
    if sys.stdout.isatty():
        console = logging.StreamHandler(sys.stdout)
        console.setLevel(LOG_LEVEL)
        console.setFormatter(formatter)
        root.addHandler(console)

    # ── 2. Command-specific log file ────────────────────────────────
    # Monthly commands accumulate into one YYYY-MM file per month.
    # Daily commands get a fresh YYYY-MM-DD file each day.
    # Plain FileHandler (append) — TimedRotatingFileHandler rotation
    # never fires for short-lived cron processes; _cleanup_old_logs()
    # handles deletion at startup instead.
    date_fmt     = "%Y-%m" if command in _MONTHLY_COMMANDS else "%Y-%m-%d"
    today        = datetime.now().strftime(date_fmt)
    command_file = LOG_DIR / f"{command}_{today}.log"

    file_handler = logging.FileHandler(command_file, mode="a", encoding="utf-8")
    file_handler.setLevel(LOG_LEVEL)
    file_handler.setFormatter(formatter)
    root.addHandler(file_handler)

    # ── 3. Catch-all pipeline_YYYY-MM-DD.log ────────────────────────
    # Always-on daily file — useful for grepping across all commands.
    # Dated format ensures it is subject to the same 14-day cleanup
    # as other daily logs.  One file per calendar day.
    today_daily   = datetime.now().strftime("%Y-%m-%d")
    pipeline_file = LOG_DIR / f"pipeline_{today_daily}.log"

    catchall = logging.FileHandler(pipeline_file, mode="a", encoding="utf-8")
    catchall.setLevel(LOG_LEVEL)
    catchall.setFormatter(formatter)
    root.addHandler(catchall)

    # ── 4. Silence noisy third-party loggers ───────────────────────
    # These would flood DEBUG output with HTTP wire traces.
    for noisy in ("urllib3", "requests", "urllib3.connectionpool",
                  "google.auth", "google.oauth2", "gspread"):
        logging.getLogger(noisy).setLevel(logging.WARNING)

    _log = logging.getLogger(__name__)
    _log.info(
        "Logging initialised — command=%s level=DEBUG file=%s",
        command, command_file.name
    )
    if _deleted or _errors:
        _log.info(
            "Log cleanup ran at startup — deleted=%d errors=%d",
            _deleted, _errors,
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
