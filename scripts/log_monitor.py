#!/usr/bin/env python3
r"""
scripts/log_monitor.py — Proactive pipeline log scanner with Redis-backed dedup.

┌─ Every 15 min (cron) ─────────────────────────────────────────────────────┐
│  1. Scan log files from last byte offset (nothing re-read, nothing missed) │
│  2. Fingerprint each flagged line (exception type + file + lineno)         │
│  3. NEW error  (not in Redis)   → immediate alert email + store in Redis   │
│  4. KNOWN error (in Redis)      → silently drop, just refresh "active" key │
│  5. Resolution sweep            → any error whose "active" key expired       │
│     (adaptive TTL: 5 min–24 h based on firing freq) → delete main key so   │
│     next occurrence = NEW again                                              │
│     → append to resolved log for next digest                                │
│  6. Digest due? (every 3 days)  → send NEW / ACTIVE / RESOLVED summary     │
└───────────────────────────────────────────────────────────────────────────┘

Redis key schema
───────────────
  log_monitor:err:{fp}      JSON error record   TTL = DEDUP_WINDOW   (7 days)
  log_monitor:act:{fp}      "1"                 TTL = dynamic (5 min – 24 h)
                            Refreshed on every occurrence with a TTL derived
                            from the error's own firing frequency:
                              TTL = clamp(N_CYCLES × avg_IAT, 5 min, 24 h)
                            When it expires → error is "resolved".
  log_monitor:ts:{fp}       list of timestamps  TTL = DEDUP_WINDOW   (7 days)
                            Last HISTORY_SIZE hit timestamps; drives act TTL.
  log_monitor:resolved      Redis list of JSON records for resolved errors
                            since the last digest.  Cleared after digest.
  log_monitor:digest_ts     Float str — Unix timestamp of last digest sent.

Why Redis instead of a local file?
  Byte offsets (what we've already read) live in a local JSON file so the
  scanner works even when Redis is briefly down.  Dedup / active / resolved
  state lives in Redis so TTL-based expiry is handled automatically without
  any cron cleanup job.  If Redis is unavailable and new errors were found,
  the scanner skips the dedup/alert step AND does NOT advance file offsets,
  so the same lines are re-evaluated on the next run once Redis recovers.

Cron entry (add via deploy/update_crontab.sh or setup_cron.sh):
  */15 * * * * /home/opc/mail/venv/bin/python /home/opc/mail/scripts/log_monitor.py \
               >> /home/opc/mail/logs/log_monitor_$(date +\%Y-\%m-\%d).log 2>&1
"""

from __future__ import annotations

import hashlib
import fcntl
import html as html_lib
import json
import os
import re
import smtplib
import time
from datetime import datetime, timedelta
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path
from typing import Any

# ── Paths ─────────────────────────────────────────────────────────────────────
PROJECT_DIR = Path(__file__).resolve().parent.parent
LOGS_DIR    = PROJECT_DIR / "logs"
STATE_FILE  = PROJECT_DIR / "data" / "log_monitor_state.json"   # byte offsets

# ── Tuning ────────────────────────────────────────────────────────────────────
DEDUP_WINDOW_S    = 7 * 24 * 3600   # suppress the same error for 7 days
DIGEST_INTERVAL_S = 3 * 24 * 3600   # send digest every 3 days
MAX_LOG_AGE_HOURS = 48               # only scan logs modified in last 48 h
CONTEXT_LINES     = 6                # lines of context captured after each hit

# Dynamic active-TTL: act TTL = N_CYCLES × avg inter-arrival time (IAT).
# Error is "resolved" when it misses N_CYCLES consecutive expected cycles.
HISTORY_SIZE      = 10    # timestamps to keep per fingerprint
N_CYCLES          = 3     # consecutive missed cycles → resolved
MIN_ACT_TTL_S     = 300   # 5 min floor  (very frequent errors)
MAX_ACT_TTL_S     = 86400 # 24 h ceiling (daily / infrequent errors)
DEFAULT_ACT_TTL_S = 3600  # 1 h — used until ≥ 2 occurrences are recorded

# ── Redis key prefixes ────────────────────────────────────────────────────────
_PFX_ERR      = "log_monitor:err:"
_PFX_ACT      = "log_monitor:act:"
_PFX_TS       = "log_monitor:ts:"
_KEY_RESOLVED = "log_monitor:resolved"
_KEY_DIGEST   = "log_monitor:digest_ts"

# ── Levels that trigger an alert when detected via field-split ────────────────
# WARNING is included; benign WARNINGs are filtered by SUPPRESS_PATTERNS below.
_ALERT_LEVELS: frozenset[str] = frozenset({"WARNING", "ERROR", "CRITICAL"})

# ── Patterns that flag a line as worth alerting on ────────────────────────────
# Level-based detection is handled in _is_flagged() via field-split, not here.
FLAG_PATTERNS: list[re.Pattern] = [
    re.compile(r'^Traceback \(most recent call last\):'),
    re.compile(r'^(TypeError|ValueError|AttributeError|KeyError|IndexError'
               r'|RuntimeError|OSError|IOError|PermissionError'
               r'|psycopg2\.\w+Error|redis\.exceptions\.\w+Error):'),
    # Cron wrapper non-zero exit — "[CRON] … | exit=1"
    re.compile(r'\[CRON\].*\|\s*exit=[^0\s]'),
]

# ── Patterns that SUPPRESS a line even if it matches FLAG_PATTERNS ────────────
SUPPRESS_PATTERNS: list[tuple[str, re.Pattern]] = [
    ("URL-extracted non-US location (expected filter behaviour)",
     re.compile(r'non-US location')),
    ("api_health default fallback (handled internally)",
     re.compile(r'returning default \d+ ms')),
    ("Band threshold recalibration failure (non-critical)",
     re.compile(r'band threshold recalibration failed')),
    ("Adaptive/fullscan backpressure (intended throttle)",
     re.compile(r'backpressure.*queue_depth')),
    ("fetch_job_detail guard fired / no new data (handled)",
     re.compile(r'fetch_job_detail returned NO new data')),
    ("ATS outage / canary state (watchdog monitors this)",
     re.compile(r'worker:outage:')),
    ("Scheduler auto-pause / auto-resume (expected)",
     re.compile(r'(auto.paused|auto.resumed)')),
]

# ── WARNING-level-only suppressions (never suppress ERROR/CRITICAL) ───────────
SUPPRESS_WARNING_PATTERNS: list[tuple[str, re.Pattern]] = [
    ("Redis BUSYGROUP on consumer group create (normal on restart)",
     re.compile(r'BUSYGROUP')),
    ("Brave Search API warning (external service, non-critical)",
     re.compile(r'brave.*api|api.*brave', re.I)),
    ("Log file cleanup warning (housekeeping, non-critical)",
     re.compile(r'log.*(cleanup|rotation|removed|deleted)', re.I)),
    ("Empty company name in form row (user data issue, not pipeline)",
     re.compile(r'missing company name')),
    ("Sentry not initialised (expected in dev/test environments)",
     re.compile(r'Sentry\b.*not\s+initiali[zs]ed|sentry_sdk.*not.*init', re.I)),
]


# ─────────────────────────────────────────────────────────────────────────────
# Redis connection
# ─────────────────────────────────────────────────────────────────────────────

def _get_redis():
    """
    Return a Redis client, or None if unavailable.

    Tries the project's get_redis() first (picks up REDIS_URL / sentinel
    config from the existing codebase).  Falls back to a plain redis-py
    client from the REDIS_URL env var.  Returns None rather than raising
    so the scanner degrades gracefully.
    """
    try:
        from workers.redis_client import get_redis as _project_get_redis
    except ImportError:
        pass
    else:
        # workers.redis_client is available — use its singleton client.
        # Do NOT swallow runtime errors here: if the import succeeds but
        # get_redis() fails, the direct fallback below may point to a
        # different Redis backend (e.g. different REDIS_URL resolution).
        try:
            return _project_get_redis()
        except Exception:
            return None   # Redis unavailable — degrade gracefully, no fallback client
    # workers.redis_client not importable (standalone / outside project tree)
    try:
        import redis as _redis
        from dotenv import load_dotenv
        load_dotenv(PROJECT_DIR / ".env")
        url = os.environ.get("REDIS_URL", "redis://localhost:6379/0")
        return _redis.from_url(
            url,
            decode_responses=True,
            socket_timeout=30,
            socket_connect_timeout=5,
        )
    except Exception:
        return None


# ─────────────────────────────────────────────────────────────────────────────
# Fingerprinting
# ─────────────────────────────────────────────────────────────────────────────

def _fingerprint(line: str, context: list[str]) -> str:
    """
    Produce a stable, variable-stripped fingerprint for a flagged log entry.

    Priority:
      1. Exception type + filename + lineno  (most stable — survives log rotation)
      2. Exception type alone
      3. Normalised flagged line (strip timestamps, IDs, company names)
    """
    all_lines = [line] + context

    exc_type = None
    exc_msg  = None   # normalized message portion of the exception line
    location = None

    # Look for bare exception line: "TypeError: fromisoformat…"
    for _check_line in all_lines:
        m = re.match(
            r'^([A-Za-z][A-Za-z0-9_.]*(?:Error|Exception|Warning))'
            r'(?:\s*:\s*(.*))?$',
            _check_line.strip(),
        )
        if m:
            exc_type = m.group(1).split(".")[-1]   # strip module prefix
            _raw_msg = (m.group(2) or "").strip()
            if _raw_msg:
                # Normalize variable parts so the same error class+message structure
                # hashes consistently regardless of run-time values.
                _raw_msg = re.sub(r'\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}[\d.,]*',
                                  'TS', _raw_msg)
                _raw_msg = re.sub(r'\b[0-9a-f]{8,}\b', 'HASH', _raw_msg)
                _raw_msg = re.sub(r'\b\d{4,}\b', 'N', _raw_msg)
                exc_msg = _raw_msg[:80]
            break

    # Look for traceback frame: '  File "/path/foo.py", line 123, in func'
    for _check_line in all_lines:
        m = re.search(r'File "([^"]+)", line (\d+)', _check_line)
        if m:
            _p     = Path(m.group(1))
            # Preserve parent dir so files with the same basename in different
            # modules (e.g. workers/utils.py vs jobs/utils.py) don't collide.
            fname  = f"{_p.parent.name}/{_p.name}"
            lineno = m.group(2)
            location = f"{fname}:{lineno}"
            break

    if exc_type and location:
        raw = f"{exc_type}:{location}"
    elif exc_type:
        # Include normalized message so ValueError: missing key and
        # ValueError: invalid timestamp produce distinct fingerprints.
        raw = f"{exc_type}:{exc_msg}" if exc_msg else exc_type
    elif location:
        # Traceback frame found without a recognisable exception type line —
        # use the file+lineno so the fingerprint is still stable.
        raw = location
    else:
        # Normalise: strip timestamps, PIDs, UUIDs, job/company identifiers
        raw = line
        raw = re.sub(
            r'\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}[\d.,]*', 'TS', raw
        )
        raw = re.sub(r"\bpid=\d+\b",               "pid=N",   raw)
        raw = re.sub(r"company=['\"]?[^'\",\s]+",  "company=X", raw)
        raw = re.sub(r"job_id=\S+",                "job_id=X", raw)
        raw = re.sub(r"\b[0-9a-f]{8,}\b",          "HASH",    raw)
        raw = re.sub(r"\b\d{4,}\b",                "N",       raw)

    return hashlib.sha256(raw.encode()).hexdigest()[:16]


# ─────────────────────────────────────────────────────────────────────────────
# State helpers (byte offsets — kept in a local file, not Redis)
# ─────────────────────────────────────────────────────────────────────────────

def _load_offsets() -> dict[str, int]:
    if STATE_FILE.exists():
        try:
            return json.loads(STATE_FILE.read_text()).get("offsets", {})
        except Exception as exc:
            print(f"[log_monitor] _load_offsets: failed to read state ({exc}) — starting from 0")
    return {}


def _load_inodes() -> dict[str, int]:
    if STATE_FILE.exists():
        try:
            return json.loads(STATE_FILE.read_text()).get("inodes", {})
        except Exception as exc:
            print(f"[log_monitor] _load_inodes: failed to read state ({exc}) — no inode tracking")
    return {}


def _save_offsets(offsets: dict[str, int],
                  inodes: dict[str, int] | None = None) -> None:
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    _tmp = STATE_FILE.with_suffix(".tmp")
    _tmp.write_text(json.dumps({"offsets": offsets, "inodes": inodes or {}}, indent=2))
    _tmp.replace(STATE_FILE)   # atomic rename — no partial-write corruption


# ─────────────────────────────────────────────────────────────────────────────
# Log scanning
# ─────────────────────────────────────────────────────────────────────────────

def _parse_log_level(line: str) -> str:
    """
    Extract log level from either JSON lines (new format) or pipe-delimited
    lines (old format / tracebacks).  Returns the level string or "".

    JSON:          {"level": "WARNING", ...}
    Pipe-delimited: "2026-07-15 10:45:17.281 | WARNING  | workers.foo | msg"
    Tracebacks/exceptions: no level field — returns "".
    """
    try:
        level = json.loads(line).get("level", "")
        return level if isinstance(level, str) else ""
    except (json.JSONDecodeError, ValueError, AttributeError):
        m = re.search(r'\b(DEBUG|INFO|WARNING|ERROR|CRITICAL)\b', line)
        return m.group(1) if m else ""


def _is_suppressed(line: str) -> bool:
    if any(pat.search(line) for _, pat in SUPPRESS_PATTERNS):
        return True
    # WARNING-only suppressions must not silence ERROR or CRITICAL lines
    if _parse_log_level(line) == "WARNING":
        return any(pat.search(line) for _, pat in SUPPRESS_WARNING_PATTERNS)
    return False


def _is_flagged(line: str) -> bool:
    # Fast path: check level — works for both JSON and pipe-delimited formats.
    if _parse_log_level(line) in _ALERT_LEVELS:
        return True
    # Slow path: tracebacks and exception type lines have no level field.
    return any(pat.search(line) for pat in FLAG_PATTERNS)


# Matches the traceback-component patterns (patterns 2 & 3 in FLAG_PATTERNS):
# "Traceback (most recent call last):" and exception-type lines like "ValueError: ...".
# These belong to an ongoing traceback and should stay attached as context to the
# preceding flagged line, not be split off as independent findings.
_TRACEBACK_COMPONENT_RE = re.compile(
    r'^(Traceback \(most recent call last\):'
    r'|(TypeError|ValueError|AttributeError|KeyError|IndexError'
    r'|RuntimeError|OSError|IOError|PermissionError'
    r'|psycopg2\.\w+Error|redis\.exceptions\.\w+Error):)'
)


def _is_traceback_component(line: str) -> bool:
    return bool(_TRACEBACK_COMPONENT_RE.match(line))


def scan_file(
    path: Path, offset: int
) -> tuple[int, list[tuple[str, list[str]]]]:
    """
    Read new content in *path* from *offset*.
    Returns (new_offset, [(flagged_line, context_lines), ...]).
    Handles log rotation: if file shrank, resets to 0.
    """
    try:
        size = path.stat().st_size
    except FileNotFoundError:
        return offset, []

    if size < offset:
        offset = 0           # rotated / truncated
    if size == offset:
        return offset, []    # nothing new

    # Read incrementally so large log deltas after downtime don't spike memory.
    # A small positional buffer of (raw_line, end_pos) tuples provides the
    # CONTEXT_LINES lookahead the algorithm needs without materialising the file.
    findings: list[tuple[str, list[str]]] = []
    new_offset = offset
    try:
        with open(path, errors="replace") as fh:
            fh.seek(offset)
            # Each entry: (raw_line, start_pos, end_pos)
            # start_pos is captured before readline() so we can re-position to
            # the line's start when it has no trailing newline (partial write).
            buf: list[tuple[str, int, int]] = []

            def _fill(n: int) -> None:
                while len(buf) <= n:
                    start = fh.tell()
                    raw = fh.readline()
                    if not raw:
                        break
                    buf.append((raw, start, fh.tell()))

            _fill(0)
            while buf:
                raw0, start0, end0 = buf[0]
                line = raw0.rstrip()
                if line and not _is_suppressed(raw0) and _is_flagged(line):
                    if not raw0.endswith('\n'):
                        # Flagged line is a partial write — stay at start so
                        # the completed line is re-read on the next scan cycle.
                        new_offset = start0
                        buf.pop(0)
                        _fill(0)
                        continue
                    context = []
                    last_consumed = 0
                    _fill(CONTEXT_LINES)
                    for j in range(1, min(1 + CONTEXT_LINES, len(buf))):
                        ctx_raw, _, _ = buf[j]
                        ctx = ctx_raw.rstrip()
                        if ctx and not _is_suppressed(ctx_raw) and _is_flagged(ctx_raw):
                            if not _is_traceback_component(ctx):
                                break
                        last_consumed = j
                        if ctx and not _is_suppressed(ctx_raw):
                            context.append(ctx)
                    findings.append((line, context))
                    new_offset = buf[last_consumed][2]  # end_pos
                    del buf[:last_consumed + 1]
                    _fill(0)
                else:
                    # For a complete line advance past it; for an unterminated
                    # final line stay at its start so it is re-read once the
                    # write completes (avoids silently skipping partial lines).
                    new_offset = end0 if raw0.endswith('\n') else start0
                    buf.pop(0)
                    _fill(0)
    except Exception:
        return offset, []

    return new_offset, findings


def collect_raw_findings(
    offsets: dict[str, int],
    inodes: dict[str, int] | None = None,
) -> tuple[dict[str, int], dict[str, int], dict[str, list[tuple[str, list[str]]]]]:
    """
    Scan all relevant log files.
    Returns (updated_offsets, updated_inodes, per-filename raw findings).
    Inode tracking detects log rotation where the replacement file grows larger
    than the old offset (the existing size<offset check handles file shrinkage).
    """
    cutoff = time.time() - MAX_LOG_AGE_HOURS * 3600

    priority = [
        LOGS_DIR / "scheduler.log",
        LOGS_DIR / "detail_worker.log",
        LOGS_DIR / "scan_worker.log",
        LOGS_DIR / "fullscan.log",
    ]
    def _mtime_safe(p) -> float:
        """Return mtime, or -inf when the file disappears between glob and stat."""
        try:
            return p.stat().st_mtime
        except FileNotFoundError:
            return float("-inf")

    recent_dated = [
        p for p in LOGS_DIR.glob("*.log")
        if p not in priority and _mtime_safe(p) >= cutoff
    ]

    # TimedRotatingFileHandler rotates scheduler.log → scheduler.log.YYYY-MM-DD
    # at midnight.  These files end in a date suffix, not ".log", so glob("*.log")
    # misses them entirely.  Include any such rotation backups within the cutoff
    # window so errors from yesterday evening are not silently skipped.
    rotation_backups = [
        p for p in LOGS_DIR.glob("*.log.[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]")
        if _mtime_safe(p) >= cutoff
    ]

    new_offsets = dict(offsets)
    new_inodes: dict[str, int] = dict(inodes or {})
    raw: dict[str, list] = {}

    # For rotated backups whose inode matches the prior active log inode,
    # inherit that log's offset so we don't re-scan already-processed content.
    if inodes is not None:
        for backup_path in rotation_backups:
            backup_key = str(backup_path)
            if backup_key in new_offsets:
                continue  # already tracked
            try:
                backup_inode = backup_path.stat().st_ino
            except OSError:
                continue
            if not backup_inode:
                continue
            for prior_key, prior_inode in inodes.items():
                if prior_inode == backup_inode and prior_key != backup_key:
                    new_offsets[backup_key] = offsets.get(prior_key, 0)
                    break

    for path in priority + recent_dated + rotation_backups:
        if not path.exists():
            continue
        key = str(path)
        saved_offset = new_offsets.get(key, 0)

        # Reset offset when the file was replaced (log rotation where the new
        # file has grown past the old offset — the size<offset check in
        # scan_file already covers truncation/shrinkage).
        if inodes is not None:
            try:
                st = path.stat()
                current_inode = st.st_ino
                if current_inode != 0:  # st_ino is 0 on some Windows filesystems
                    saved_inode = inodes.get(key)
                    if saved_inode is not None and saved_inode != 0 and current_inode != saved_inode:
                        saved_offset = 0  # file replaced — restart from beginning
                new_inodes[key] = current_inode
            except OSError:
                pass

        new_off, hits = scan_file(path, saved_offset)
        new_offsets[key] = new_off
        if hits:
            raw[path.name] = hits

    return new_offsets, new_inodes, raw


# ─────────────────────────────────────────────────────────────────────────────
# Dedup + resolution logic (Redis)
# ─────────────────────────────────────────────────────────────────────────────

def _err_key(fp: str) -> str:
    return f"{_PFX_ERR}{fp}"


def _act_key(fp: str) -> str:
    return f"{_PFX_ACT}{fp}"


def _ts_key(fp: str) -> str:
    return f"{_PFX_TS}{fp}"


# ─────────────────────────────────────────────────────────────────────────────
# Frequency tracking helpers
# ─────────────────────────────────────────────────────────────────────────────

def _update_frequency(r, fp: str, now: float) -> None:
    """Push current timestamp into the history ring-buffer (newest-first).

    Uses scan time (now), not the log line's own timestamp.  On first deploy
    or after downtime, a backlog of errors is processed in one pass so all
    timestamps collapse to ~now → avg_IAT≈0 → TTL=MIN_ACT_TTL_S (5 min).
    Those errors resolve before the next scan if they stopped firing.
    This is an accepted tradeoff: Sentry catches such errors in real-time;
    log_monitor's role is ongoing pattern detection, not historical replay.
    """
    key = _ts_key(fp)
    r.lpush(key, now)
    r.ltrim(key, 0, HISTORY_SIZE - 1)
    r.expire(key, DEDUP_WINDOW_S)   # auto-clean alongside err key


def _compute_act_ttl(r, fp: str) -> int:
    """
    Derive a dynamic act-key TTL from observed inter-arrival time (IAT).

    TTL = clamp(N_CYCLES × avg_IAT, MIN_ACT_TTL_S, MAX_ACT_TTL_S)

    Examples
    --------
    Error every 2 min   → avg_IAT=120 s  → TTL =   360 s  (6 min)
    Error every 2 h     → avg_IAT=7200 s → TTL = 21600 s  (6 h)
    Error every 8 h     → avg_IAT=28800s → TTL = 86400 s  (24 h, ceiling)

    Falls back to DEFAULT_ACT_TTL_S while fewer than 2 timestamps are recorded.
    """
    raw        = r.lrange(_ts_key(fp), 0, -1)
    timestamps = sorted(float(t) for t in raw)

    if len(timestamps) < 2:
        return DEFAULT_ACT_TTL_S

    iats    = [timestamps[i + 1] - timestamps[i] for i in range(len(timestamps) - 1)]
    avg_iat = sum(iats) / len(iats)
    ttl     = int(N_CYCLES * avg_iat)
    return max(MIN_ACT_TTL_S, min(MAX_ACT_TTL_S, ttl))


def process_findings(
    r,
    raw: dict[str, list[tuple[str, list[str]]]],
    now: float,
) -> dict[str, dict]:
    """
    Classify each raw finding as NEW or KNOWN using Redis.

    Returns {fp: record} for NEW errors only (the ones to alert on immediately).
    As a side-effect, refreshes the "active" TTL for all seen errors.
    """
    new_errors: dict[str, dict] = {}

    for fname, hits in raw.items():
        for line, context in hits:
            fp      = _fingerprint(line, context)
            err_key = _err_key(fp)
            act_key = _act_key(fp)

            known = r.exists(err_key)

            record: dict[str, Any] = {
                "fp":           fp,
                "first_seen":   now,
                "last_seen":    now,
                "count":        1,
                "log_file":     fname,
                "sample_line":  line,
                "sample_context": [c for c in context if c.strip()],
            }

            if not known:
                # Brand-new error — collect for immediate alert.
                # Do NOT write err_key to Redis here: writing before the email
                # is sent would suppress the alert for 7 days if the send fails.
                # err_key is written in main() after a successful send.
                if fp in new_errors:
                    # Same fingerprint appeared twice in this scan — accumulate.
                    new_errors[fp]["count"] += 1
                    new_errors[fp]["last_seen"] = now
                else:
                    new_errors[fp] = record
            else:
                # Known error — update last_seen + count, don't alert
                try:
                    stored = json.loads(r.get(err_key) or "{}")
                    stored["last_seen"] = now
                    stored["count"]     = stored.get("count", 1) + 1
                    # Preserve remaining TTL (don't reset the 7-day clock)
                    ttl = r.ttl(err_key)
                    if ttl > 0:
                        r.set(err_key, json.dumps(stored), ex=ttl)
                except Exception as _ttl_err:
                    print(f"[log_monitor] TTL update failed for {err_key}: {_ttl_err}")

            # Update frequency history + refresh active heartbeat with dynamic TTL
            _update_frequency(r, fp, now)
            act_ttl = _compute_act_ttl(r, fp)
            r.set(act_key, "1", ex=act_ttl)

    return new_errors


def sweep_resolved(r, now: float) -> list[dict]:
    """
    Find errors whose "active" key has expired (not seen for N_CYCLES × avg_IAT).

    These errors are considered resolved:
      - Their main err: key is deleted so the next occurrence is treated as NEW.
      - A summary record is pushed to log_monitor:resolved for the next digest.

    Returns the list of resolved records (for logging).
    """
    resolved = []

    try:
        for err_key in r.scan_iter(f"{_PFX_ERR}*"):
            fp      = err_key[len(_PFX_ERR):]
            act_key = _act_key(fp)

            if not r.exists(act_key):
                # Active key expired → error resolved.
                # Only delete err_key/ts_key after successfully writing the
                # resolved record; otherwise the digest loses the resolution entry.
                _resolution_written = False
                try:
                    raw = r.get(err_key)
                    if raw:
                        record              = json.loads(raw)
                        record["resolved_at"] = now
                        r.rpush(_KEY_RESOLVED, json.dumps(record))
                        r.ltrim(_KEY_RESOLVED, -200, -1)  # keep last 200
                        _resolution_written = True
                except Exception as _res_err:
                    print(f"[log_monitor] resolution sweep failed for {fp}: {_res_err}")
                if _resolution_written:
                    r.delete(err_key)
                    r.delete(_ts_key(fp))   # clear cadence history so next incident starts fresh
                    resolved.append(fp)
    except Exception as _sweep_err:
        print(f"[log_monitor] sweep_resolved failed: {_sweep_err}")

    return resolved


# ─────────────────────────────────────────────────────────────────────────────
# Email helpers
# ─────────────────────────────────────────────────────────────────────────────

def _esc(s: str) -> str:
    return html_lib.escape(str(s))


def _format_ts(ts: float) -> str:
    return datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M")


def _error_table_row(record: dict) -> str:
    sample = record.get("sample_line", "")
    meta_row = (
        f'<tr><td style="padding:2px 10px 6px;font-size:11px;color:#94a3b8">'
        f'📄 {_esc(record.get("log_file","?"))} &nbsp;·&nbsp; '
        f'first seen {_format_ts(record.get("first_seen", 0))} &nbsp;·&nbsp; '
        f'{record.get("count", 1):,} occurrence(s)</td></tr>'
    )

    # Try to parse the sample line as JSON (produced by JsonFormatter).
    parsed = None
    try:
        parsed = json.loads(sample) if sample else None
        if not isinstance(parsed, dict):
            parsed = None
    except (json.JSONDecodeError, ValueError):
        parsed = None

    if parsed:
        msg     = _esc(parsed.get("msg", sample))
        logger_name = _esc(parsed.get("logger", ""))
        ts      = _esc(parsed.get("time", ""))
        header  = (
            f'<tr><td style="padding:5px 10px 2px;font-family:monospace;font-size:12px;'
            f'color:#dc2626;word-break:break-all">{msg}</td></tr>'
        )
        label_parts = []
        if logger_name:
            label_parts.append(f'<b>{logger_name}</b>')
        if ts:
            label_parts.append(ts)
        label_row = (
            f'<tr><td style="padding:1px 10px 3px 28px;font-size:11px;color:#64748b">'
            + " &nbsp;·&nbsp; ".join(label_parts)
            + '</td></tr>'
        ) if label_parts else ""

        # Stack trace from Layer 1 auto-injection or Layer 2 exc_info
        trace_field = parsed.get("stack") or parsed.get("exc")
        trace_row = ""
        if trace_field:
            trace_row = (
                f'<tr><td style="padding:3px 10px 4px 28px;font-family:monospace;'
                f'font-size:10px;color:#7c3aed;white-space:pre-wrap;word-break:break-all">'
                f'Stack:\n{_esc(trace_field.rstrip())}</td></tr>'
            )

        ctx = "".join(
            f'<tr><td style="padding:1px 10px 1px 28px;font-family:monospace;'
            f'font-size:11px;color:#94a3b8;word-break:break-all">'
            f'{_esc(c)}</td></tr>'
            for c in record.get("sample_context", []) if c.strip()
        )
        return header + label_row + trace_row + ctx + meta_row
    else:
        # Non-JSON (pipe-delimited or raw text)
        line = _esc(sample)
        ctx  = "".join(
            f'<tr><td style="padding:2px 10px 2px 28px;font-family:monospace;'
            f'font-size:11px;color:#64748b;word-break:break-all">'
            f'{_esc(c)}</td></tr>'
            for c in record.get("sample_context", []) if c.strip()
        )
        return (
            f'<tr><td style="padding:5px 10px;font-family:monospace;font-size:12px;'
            f'color:#dc2626;word-break:break-all">{line}</td></tr>{ctx}'
            + meta_row
        )


def _send_email(subject: str, body_html: str):
    """Return True on success, False on transient SMTP failure, None if credentials are absent."""
    try:
        from dotenv import load_dotenv
        load_dotenv(PROJECT_DIR / ".env")
    except Exception:
        pass

    email    = os.environ.get("GMAIL_EMAIL", "")
    password = os.environ.get("GMAIL_APP_PASSWORD", "")
    if not email or not password:
        print("[log_monitor] GMAIL_EMAIL/GMAIL_APP_PASSWORD not set — skipping email")
        return None   # permanent misconfiguration, not a transient failure

    try:
        msg = MIMEMultipart("mixed")
        msg["From"]    = email
        msg["To"]      = email
        msg["Subject"] = subject
        msg.attach(MIMEText(body_html, "html", "utf-8"))

        with smtplib.SMTP("smtp.gmail.com", 587, timeout=30) as srv:
            srv.starttls()
            srv.login(email, password)
            srv.send_message(msg)

        print(f"[log_monitor] Email sent: {subject}")
        return True
    except Exception as exc:
        print(f"[log_monitor] Email failed: {exc}")
        return False


def send_immediate_alert(new_errors: dict[str, dict]):
    """Send a single batched email for all brand-new errors found in this scan."""
    total   = len(new_errors)
    files   = sorted({r["log_file"] for r in new_errors.values()})
    subject = (
        f"[Pipeline Alert] {total} new error(s) — "
        + ", ".join(files[:3])
        + (" …" if len(files) > 3 else "")
    )

    rows = "".join(_error_table_row(rec) for rec in new_errors.values())
    body = f"""<html><body style="font-family:sans-serif;padding:24px;color:#1e293b">
    <h2 style="color:#dc2626">⚠ New Pipeline Error(s) — {datetime.now().strftime('%Y-%m-%d %H:%M')}</h2>
    <p>
      {total} new unique error(s) detected.
      Repeat occurrences will be suppressed for <strong>7 days</strong>.
      Auto-resolved when the error goes quiet for 3 consecutive natural cycles
      (based on its own firing frequency — min 5 min, max 24 h).
    </p>
    <table style="width:100%;border-collapse:collapse;background:#fef2f2;
                  border:1px solid #fecaca;border-radius:4px">
      {rows}
    </table>
    <p style="margin-top:24px;color:#94a3b8;font-size:11px">
      Sent by scripts/log_monitor.py · duplicates suppressed for 7 days ·
      digest every 3 days
    </p>
    </body></html>"""

    return _send_email(subject, body)


def send_digest(r, now: float) -> bool:
    """
    Send a 3-day digest: NEW errors, STILL ACTIVE errors, RESOLVED errors.
    Clears the resolved log after sending.
    """
    last_digest = float(r.get(_KEY_DIGEST) or 0)

    # ── Collect active errors from Redis ──────────────────────────────────────
    new_since_digest: list[dict]  = []
    still_active:     list[dict]  = []

    try:
        for err_key in r.scan_iter(f"{_PFX_ERR}*"):
            fp  = err_key[len(_PFX_ERR):]
            raw = r.get(err_key)
            if not raw:
                continue
            rec = json.loads(raw)

            if rec.get("first_seen", 0) >= last_digest:
                new_since_digest.append(rec)
            else:
                still_active.append(rec)
    except Exception as _active_err:
        print(f"[log_monitor] send_digest: failed to collect active errors: {_active_err}")

    # ── Collect resolved errors ───────────────────────────────────────────────
    resolved: list[dict] = []
    try:
        raw_list = r.lrange(_KEY_RESOLVED, 0, -1)
        resolved = [json.loads(x) for x in raw_list]
    except Exception as _resolved_err:
        print(f"[log_monitor] send_digest: failed to collect resolved errors: {_resolved_err}")

    if not new_since_digest and not still_active and not resolved:
        print("[log_monitor] digest: nothing to report — skipping")
        r.set(_KEY_DIGEST, str(now))
        return True

    # ── Build email ───────────────────────────────────────────────────────────
    def _section(title: str, colour: str, icon: str, records: list[dict]) -> str:
        if not records:
            return (
                f'<h3 style="color:{colour};margin-top:24px">{icon} {title}</h3>'
                f'<p style="color:#64748b;font-size:13px">None.</p>'
            )
        rows = "".join(_error_table_row(r) for r in records)
        return (
            f'<h3 style="color:{colour};margin-top:24px">{icon} {title} ({len(records)})</h3>'
            f'<table style="width:100%;border-collapse:collapse;background:#f8fafc;'
            f'border:1px solid #e2e8f0;border-radius:4px">{rows}</table>'
        )

    last_str = _format_ts(last_digest) if last_digest else "—"
    body     = f"""<html><body style="font-family:sans-serif;padding:24px;color:#1e293b">
    <h2 style="color:#0f172a">📋 Pipeline Log Digest — {datetime.now().strftime('%Y-%m-%d')}</h2>
    <p style="color:#64748b">Period: {last_str} → now</p>

    {_section("New Errors",         "#dc2626", "🆕", new_since_digest)}
    {_section("Still Active",       "#b45309", "🔄", still_active)}
    {_section("Resolved",           "#16a34a", "✅", resolved)}

    <p style="margin-top:32px;color:#94a3b8;font-size:11px">
      Sent by scripts/log_monitor.py · next digest in ~3 days
    </p>
    </body></html>"""

    total_n = len(new_since_digest)
    total_a = len(still_active)
    total_r = len(resolved)
    subject = (
        f"[Pipeline Digest] "
        f"{total_n} new · {total_a} active · {total_r} resolved"
    )

    sent = _send_email(subject, body)
    if sent:
        r.set(_KEY_DIGEST, str(now))
        r.delete(_KEY_RESOLVED)   # clear resolved log after digest
    elif sent is None:
        # Credentials permanently absent — advance the digest timestamp so we
        # don't retry on every cron cycle (mirrors send_immediate_alert behavior).
        r.set(_KEY_DIGEST, str(now))
        # Don't clear _KEY_RESOLVED — resolved errors weren't communicated.

    return sent


# ─────────────────────────────────────────────────────────────────────────────
# Entry point
# ─────────────────────────────────────────────────────────────────────────────

def main() -> None:
    # Non-blocking lock: exit immediately if another instance is running.
    # Prevents duplicate alerts and state-file races on 15-min cron overlaps.
    _lock_path = PROJECT_DIR / "data" / "log_monitor.lock"
    _lock_path.parent.mkdir(parents=True, exist_ok=True)
    _lock_fh = open(_lock_path, "w")  # noqa: SIM115 — must stay open for flock lifetime
    try:
        fcntl.flock(_lock_fh, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        print("[log_monitor] another instance is running — exiting")
        _lock_fh.close()
        return

    now = time.time()
    print(f"[log_monitor] scan started at {datetime.fromtimestamp(now).strftime('%Y-%m-%d %H:%M:%S')}")

    # ── 1. Scan log files ────────────────────────────────────────────────────
    offsets                      = _load_offsets()
    inodes                       = _load_inodes()
    new_offsets, new_inodes, raw = collect_raw_findings(offsets, inodes)

    total_hits = sum(len(v) for v in raw.values())
    total_new  = sum(new_offsets.get(k, 0) - offsets.get(k, 0) for k in new_offsets)
    print(f"[log_monitor] scanned {total_new:,} new bytes — {total_hits} flagged line(s)")

    if not raw:
        print("[log_monitor] clean — no issues found")
        # Nothing to alert on — safe to advance offsets now.
        _save_offsets(new_offsets, new_inodes)
        # Still run the resolution sweep and digest check even on a clean run.
        # Without the sweep, expired action keys accumulate and errors that
        # recur after a quiet period are not treated as NEW.
        r = _get_redis()
        if r:
            try:
                resolved_fps = sweep_resolved(r, now)
                if resolved_fps:
                    print(f"[log_monitor] {len(resolved_fps)} error(s) resolved: {resolved_fps}")
            except Exception as _sw_err:
                print(f"[log_monitor] clean-run resolution sweep failed: {_sw_err}")
            try:
                last_digest = float(r.get(_KEY_DIGEST) or 0)
                if now - last_digest >= DIGEST_INTERVAL_S:
                    send_digest(r, now)
            except Exception as _dig_err:
                print(f"[log_monitor] clean-run digest check failed: {_dig_err}")
        return

    # ── 2. Dedup via Redis ───────────────────────────────────────────────────
    r = _get_redis()
    if r is None:
        # Redis unavailable — can't dedup or alert.  Don't advance offsets so
        # the next run can try again once Redis recovers.
        print("[log_monitor] Redis unavailable — skipping dedup/alert this run")
        return

    try:
        new_errors = process_findings(r, raw, now)
    except Exception as exc:
        print(f"[log_monitor] Redis error during dedup: {exc}")
        return

    # ── 3. Resolution sweep ──────────────────────────────────────────────────
    resolved_fps = sweep_resolved(r, now)
    if resolved_fps:
        print(f"[log_monitor] {len(resolved_fps)} error(s) resolved: {resolved_fps}")

    # ── 4. Immediate alert for brand-new errors ──────────────────────────────
    # Dedup keys are written BEFORE advancing offsets so a crash cannot advance
    # scan progress without recording the fingerprints.  If we wrote offsets
    # first and then crashed before writing dedup keys, the errors would be
    # silently skipped on the next run (offsets advanced past them, no dedup key
    # to suppress, but the log lines are never re-scanned).
    if new_errors:
        print(f"[log_monitor] {len(new_errors)} NEW error(s): {list(new_errors)}")
        sent = send_immediate_alert(new_errors)
        if sent:
            # Email delivered — persist dedup keys so next run suppresses these.
            for fp, record in new_errors.items():
                try:
                    r.set(_err_key(fp), json.dumps(record), ex=DEDUP_WINDOW_S)
                except Exception as _we:
                    print(f"[log_monitor] Redis write failed for {fp}: {_we}")
        elif sent is None:
            # Credentials permanently absent — mark as known so the same
            # fingerprints don't re-appear as new on every subsequent scan.
            for fp, record in new_errors.items():
                try:
                    r.set(_err_key(fp), json.dumps(record), ex=DEDUP_WINDOW_S)
                except Exception as _we:
                    print(f"[log_monitor] Redis write failed for {fp}: {_we}")
        else:
            # Transient SMTP failure — write dedup keys that outlive the next
            # digest run so send_digest can collect them.  TTL = DIGEST_INTERVAL_S
            # (3 days) so the key survives until the digest fires, but is shorter
            # than DEDUP_WINDOW_S (7 days) so the error can re-alert once the
            # SMTP outage clears.
            _TRANSIENT_SMTP_TTL_S = DIGEST_INTERVAL_S
            for fp, record in new_errors.items():
                try:
                    r.set(_err_key(fp), json.dumps(record), ex=_TRANSIENT_SMTP_TTL_S)
                except Exception as _we:
                    print(f"[log_monitor] Redis write failed for {fp}: {_we}")
            print("[log_monitor] Email send failed — errors recorded for next digest")
        # Advance offsets only after dedup keys are written.
        _save_offsets(new_offsets, new_inodes)
    else:
        known = total_hits - len(new_errors)
        print(f"[log_monitor] {known} known/duplicate occurrence(s) — suppressed")
        # No new errors — safe to advance offsets immediately.
        _save_offsets(new_offsets, new_inodes)

    # ── 5. Periodic digest ───────────────────────────────────────────────────
    try:
        last_digest = float(r.get(_KEY_DIGEST) or 0)
        if now - last_digest >= DIGEST_INTERVAL_S:
            send_digest(r, now)
    except Exception as exc:
        print(f"[log_monitor] digest check failed: {exc}")


if __name__ == "__main__":
    import sys
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    from logger import init_logging
    init_logging("log_monitor")
    main()
