#!/usr/bin/env python3
# logs/view_logs.py — Pipeline log viewer for debugging
#
# Usage:
#   python logs/view_logs.py                     # show today's logs (all commands)
#   python logs/view_logs.py --tail              # live tail pipeline.log
#   python logs/view_logs.py --cmd monitor       # today's monitor log
#   python logs/view_logs.py --cmd detect        # today's detect log
#   python logs/view_logs.py --errors            # errors + warnings only
#   python logs/view_logs.py --company "Stripe"  # filter by company name
#   python logs/view_logs.py --since 2h          # last 2 hours
#   python logs/view_logs.py --since 30m         # last 30 minutes
#   python logs/view_logs.py --summary           # run summary (counts per level)
#   python logs/view_logs.py --date 2026-03-11   # specific date

import argparse
import os
import re
import subprocess
import sys
from datetime import datetime, timedelta
from pathlib import Path

# ── Config ────────────────────────────────────────────────────
LOG_DIR = Path(__file__).parent.parent / "logs"

LEVEL_COLORS = {
    "DEBUG":    "\033[90m",    # grey
    "INFO":     "\033[0m",     # white
    "WARNING":  "\033[93m",    # yellow
    "ERROR":    "\033[91m",    # red
    "CRITICAL": "\033[41m",    # red background
}
RESET = "\033[0m"
BOLD  = "\033[1m"
DIM   = "\033[2m"


def colorize(line: str) -> str:
    for level, color in LEVEL_COLORS.items():
        if f"| {level}" in line or f"| {level:<8}" in line:
            return f"{color}{line}{RESET}"
    return line


def parse_log_time(line: str):
    """Parse timestamp from log line. Returns datetime or None."""
    m = re.match(r"(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})", line)
    if m:
        try:
            return datetime.strptime(m.group(1), "%Y-%m-%d %H:%M:%S")
        except ValueError:
            pass
    return None


def parse_since(since: str) -> datetime:
    """Parse '2h', '30m', '1d' into a cutoff datetime."""
    m = re.match(r"^(\d+)(m|h|d)$", since.strip())
    if not m:
        print(f"[ERROR] --since format must be like 30m, 2h, 1d — got: {since}")
        sys.exit(1)
    n, unit = int(m.group(1)), m.group(2)
    delta = {"m": timedelta(minutes=n), "h": timedelta(hours=n),
             "d": timedelta(days=n)}[unit]
    return datetime.now() - delta


def get_log_file(cmd: str, date_str: str) -> Path:
    return LOG_DIR / f"{cmd}_{date_str}.log"


def get_today() -> str:
    return datetime.now().strftime("%Y-%m-%d")


def print_header(title: str):
    width = 60
    print(f"\n{BOLD}{'═' * width}{RESET}")
    print(f"{BOLD}  {title}{RESET}")
    print(f"{BOLD}{'═' * width}{RESET}")


def stream_file(path: Path, filters: dict):
    """Print lines from a log file, applying filters."""
    if not path.exists():
        print(f"{DIM}[no log file: {path.name}]{RESET}")
        return 0

    errors_only = filters.get("errors_only", False)
    company     = filters.get("company", "").lower() if filters.get("company") else None
    since       = filters.get("since")      # datetime or None

    count = 0
    with open(path, "r", encoding="utf-8", errors="replace") as f:
        for line in f:
            line = line.rstrip()

            # ── since filter ──
            if since:
                ts = parse_log_time(line)
                if ts and ts < since:
                    continue

            # ── errors only ──
            if errors_only:
                if not any(lvl in line for lvl in ("WARNING", "ERROR", "CRITICAL")):
                    continue

            # ── company filter ──
            if company and company not in line.lower():
                continue

            print(colorize(line))
            count += 1

    return count


def do_summary(path: Path):
    """Print a count-per-level summary of a log file."""
    if not path.exists():
        print(f"{DIM}[no log: {path.name}]{RESET}")
        return

    counts   = {"DEBUG": 0, "INFO": 0, "WARNING": 0, "ERROR": 0, "CRITICAL": 0}
    runs     = 0
    duration = None
    companies_seen = set()
    new_jobs = 0

    with open(path, "r", encoding="utf-8", errors="replace") as f:
        for line in f:
            for level in counts:
                if f"| {level}" in line:
                    counts[level] += 1
                    break

            if "starting" in line and "═" not in line:
                runs += 1

            # Extract company names from log lines like: "── [1/20] 'Stripe'"
            m = re.search(r"'([^']+)'.*platform=", line)
            if m:
                companies_seen.add(m.group(1))

            # Count new jobs
            if "NEW JOB:" in line or "NEW JOB saved:" in line:
                new_jobs += 1

            # Duration
            m2 = re.search(r"Run complete in (\d+)s", line)
            if m2:
                duration = int(m2.group(1))

    print(f"\n  Log file:  {path.name}")
    print(f"  Runs:      {runs}")
    if duration is not None:
        print(f"  Duration:  {duration}s")
    print(f"  Companies: {len(companies_seen)}")
    print(f"  New jobs:  {new_jobs}")
    print()
    for level, color in LEVEL_COLORS.items():
        c = counts[level]
        if c > 0:
            bar = "█" * min(c, 40)
            print(f"  {color}{level:<9}{RESET}  {c:>5}  {DIM}{bar}{RESET}")


def do_tail(path: Path):
    """Live tail a log file."""
    if not path.exists():
        print(f"Waiting for {path} to be created... (start a pipeline run)")
        # Block until file appears
        import time
        while not path.exists():
            time.sleep(1)

    print(f"{BOLD}Tailing {path.name} — press Ctrl+C to stop{RESET}\n")
    try:
        subprocess.run(["tail", "-f", "-n", "50", str(path)])
    except KeyboardInterrupt:
        print("\n[stopped]")


def main():
    parser = argparse.ArgumentParser(
        description="Pipeline log viewer",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python logs/view_logs.py                       # today's full log
  python logs/view_logs.py --tail                # live tail
  python logs/view_logs.py --cmd monitor         # monitor log only
  python logs/view_logs.py --cmd detect          # detect log only
  python logs/view_logs.py --errors              # warnings + errors only
  python logs/view_logs.py --company "Stripe"    # filter by company
  python logs/view_logs.py --since 2h            # last 2 hours
  python logs/view_logs.py --summary             # counts per level
  python logs/view_logs.py --date 2026-03-11     # specific date
        """,
    )
    parser.add_argument("--cmd",     choices=["monitor", "detect", "sync", "find"],
                        help="Show only this command's log")
    parser.add_argument("--tail",    action="store_true",
                        help="Live tail (default: pipeline.log)")
    parser.add_argument("--errors",  action="store_true",
                        help="Show WARNING and ERROR lines only")
    parser.add_argument("--company", type=str,
                        help="Filter lines containing this company name")
    parser.add_argument("--since",   type=str,
                        help="Show lines from the last N minutes/hours/days (e.g. 2h, 30m, 1d)")
    parser.add_argument("--summary", action="store_true",
                        help="Show log summary (counts per level)")
    parser.add_argument("--date",    type=str,
                        help="Log date to view (YYYY-MM-DD, default: today)")
    args = parser.parse_args()

    LOG_DIR.mkdir(parents=True, exist_ok=True)
    date_str = args.date or get_today()

    filters = {
        "errors_only": args.errors,
        "company":     args.company,
        "since":       parse_since(args.since) if args.since else None,
    }

    # ── --tail ──
    if args.tail:
        path = (get_log_file(args.cmd, date_str)
                if args.cmd else LOG_DIR / "pipeline.log")
        do_tail(path)
        return

    # ── --summary ──
    if args.summary:
        print_header(f"Log Summary — {date_str}")
        if args.cmd:
            do_summary(get_log_file(args.cmd, date_str))
        else:
            do_summary(get_log_file("monitor", date_str))
            do_summary(get_log_file("detect", date_str))
        return

    # ── Normal view ──
    if args.cmd:
        path = get_log_file(args.cmd, date_str)
        print_header(f"{args.cmd.upper()} log — {date_str}")
        n = stream_file(path, filters)
        print(f"\n{DIM}── {n} lines shown ──{RESET}")
    else:
        # Show both monitor + detect for today
        for cmd in ["monitor", "detect"]:
            path = get_log_file(cmd, date_str)
            if path.exists():
                print_header(f"{cmd.upper()} log — {date_str}")
                n = stream_file(path, filters)
                print(f"\n{DIM}── {n} lines shown ──{RESET}")

        if not any(get_log_file(c, date_str).exists()
                   for c in ["monitor", "detect"]):
            print(f"\n{DIM}No logs found for {date_str} in {LOG_DIR}{RESET}")
            print(f"{DIM}Tip: run a pipeline command first, or check --date YYYY-MM-DD{RESET}")


if __name__ == "__main__":
    main()