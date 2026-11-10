"""
scripts/logs_merge.py
─────────────────────
Merge the per-command JSON log files in logs/ into ONE time-ordered view, so an incident that
touches several programs (scheduler -> watchdog -> manager -> workers) reads as a single story.
Read-only: it never writes to or rotates any log.

Examples (run from the project root):
    python scripts/logs_merge.py --since 2h
    python scripts/logs_merge.py --since 30m --level WARNING
    python scripts/logs_merge.py --since 1d --only scheduler,watchdog,manager --grep "LEVER 1|restart"
    python scripts/logs_merge.py --since "2026-11-08 23:50" --until "2026-11-09 00:10" --stack

Each output line:  <time> <tz> | LEVEL | <file> | pid | <logger> | <message>
<file> is the log file the line came from (rotated backups show as e.g. scheduler.log.2026-11-08).
"""

import argparse
import glob
import json
import os
import re
import sys
from datetime import datetime, timedelta

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

_LEVELS = {"DEBUG": 10, "INFO": 20, "WARNING": 30, "ERROR": 40, "CRITICAL": 50}
_REL_RE = re.compile(r"^\s*(\d+)\s*([smhd])\s*$", re.I)
_UNIT_S = {"s": 1, "m": 60, "h": 3600, "d": 86400}
_TIME_FMT = "%Y-%m-%d %H:%M:%S"


def parse_when(text: str, now: datetime | None = None) -> datetime:
    """'90m' / '2h' / '1d' = that long before now; otherwise 'YYYY-MM-DD[ HH:MM[:SS]]' (local time)."""
    now = now or datetime.now()
    m = _REL_RE.match(text)
    if m:
        return now - timedelta(seconds=int(m.group(1)) * _UNIT_S[m.group(2).lower()])
    for fmt in (_TIME_FMT, "%Y-%m-%d %H:%M", "%Y-%m-%d"):
        try:
            return datetime.strptime(text.strip(), fmt)
        except ValueError:
            continue
    raise ValueError(f"cannot parse time {text!r} (use e.g. 2h, 30m, 1d or 'YYYY-MM-DD HH:MM')")


def list_log_files(logs_dir: str, since: datetime, only: list[str] | None, include_catchall: bool) -> list[str]:
    """*.log and *.log.YYYY-MM-DD files in logs_dir last written at/after `since` (older ones cannot hold lines in range)."""
    paths = glob.glob(os.path.join(logs_dir, "*.log")) + glob.glob(os.path.join(logs_dir, "*.log.????-??-??"))
    out = []
    for p in sorted(set(paths)):
        name = os.path.basename(p)
        if not include_catchall and name.startswith("pipeline_"):
            continue    # catch-all of short-lived commands: duplicates their own command file
        if only and not any(name.startswith(o) for o in only):
            continue
        try:
            if datetime.fromtimestamp(os.path.getmtime(p)) < since:
                continue
        except OSError:
            continue
        out.append(p)
    return out


def read_entries(path: str, since: datetime, until: datetime | None, min_level: int,
                 pattern: "re.Pattern | None"):
    """Yield (sort_key, entry_dict) for matching JSON lines of one file; counts skipped non-JSON lines in `read_entries.skipped`."""
    src = os.path.basename(path)
    with open(path, "r", encoding="utf-8", errors="replace") as fh:
        for line in fh:
            line = line.strip()
            if not line.startswith("{"):
                read_entries.skipped += 1
                continue
            try:
                e = json.loads(line)
                ts = datetime.strptime(e["time"], _TIME_FMT)
            except (ValueError, KeyError, TypeError):
                read_entries.skipped += 1
                continue
            if ts < since or (until is not None and ts > until):
                continue
            if _LEVELS.get(e.get("level", ""), 0) < min_level:
                continue
            if pattern is not None and not pattern.search(str(e.get("msg", ""))) \
                    and not pattern.search(str(e.get("logger", ""))):
                continue
            e["_src"] = src
            yield (e["time"], float(e.get("ms", 0) or 0)), e


read_entries.skipped = 0


def format_entry(e: dict, show_stack: bool = False) -> str:
    head = "{time}{tz} | {level:<8} | {src} | {pid} | {logger} | {msg}".format(
        time=e.get("time", ""), tz=(" " + e["tz"]) if e.get("tz") else "",
        level=e.get("level", ""), src=e["_src"], pid=e.get("pid", "-"),
        logger=e.get("logger", ""), msg=e.get("msg", ""))
    extra = []
    if e.get("exc"):
        extra.append(str(e["exc"]).rstrip())
    if show_stack and e.get("stack"):
        extra.append(str(e["stack"]).rstrip())
    if extra:
        head += "\n" + "\n".join("    " + ln for blob in extra for ln in blob.splitlines())
    return head


def merge(logs_dir: str, since: datetime, until: datetime | None = None, min_level: int = 0,
          pattern: "re.Pattern | None" = None, only: list[str] | None = None,
          include_catchall: bool = False) -> list[dict]:
    """All matching entries from every log file, sorted by (time, ms)."""
    rows = []
    for p in list_log_files(logs_dir, since, only, include_catchall):
        rows.extend(read_entries(p, since, until, min_level, pattern))
    rows.sort(key=lambda r: r[0])
    return [e for _k, e in rows]


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="Merge per-command JSON logs into one time-ordered view.")
    ap.add_argument("--since", default="1h", help="2h / 30m / 1d, or 'YYYY-MM-DD HH:MM' local (default 1h)")
    ap.add_argument("--until", default=None, help="same formats; default = now")
    ap.add_argument("--level", default="DEBUG", choices=list(_LEVELS), help="minimum level (default DEBUG)")
    ap.add_argument("--grep", default=None, help="regex matched against message and logger name (case-insensitive)")
    ap.add_argument("--only", default=None, help="comma list of file-name prefixes, e.g. scheduler,watchdog,manager")
    ap.add_argument("--stack", action="store_true", help="also print the auto-injected call stacks")
    ap.add_argument("--include-catchall", action="store_true", help="include pipeline_<date>.log (duplicates short-lived commands)")
    ap.add_argument("--logs-dir", default=None, help="default: <project>/logs")
    ap.add_argument("--limit", type=int, default=0, help="print only the LAST N lines (0 = all)")
    args = ap.parse_args(argv)

    from logger import LOG_DIR
    logs_dir = args.logs_dir or str(LOG_DIR)
    try:
        since = parse_when(args.since)
        until = parse_when(args.until) if args.until else None
        pattern = re.compile(args.grep, re.I) if args.grep else None
    except (ValueError, re.error) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    only = [o.strip() for o in args.only.split(",") if o.strip()] if args.only else None

    read_entries.skipped = 0
    entries = merge(logs_dir, since, until, _LEVELS[args.level], pattern, only, args.include_catchall)
    if args.limit > 0:
        entries = entries[-args.limit:]
    for e in entries:
        print(format_entry(e, args.stack))
    print(f"-- {len(entries)} lines from {logs_dir}; {read_entries.skipped} non-JSON lines skipped", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
