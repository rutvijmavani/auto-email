"""
scripts/cron_watch.py — detects cron jobs that did not start, stalled, ran away or failed.

Called once per run from scripts/log_monitor.py (every 15 min).  Design: docs/cron-monitoring-design.md

Why it exists: on 2026-10-10 all run_*.sh wrappers had been missing for ~2 months; cron only wrote
"No such file or directory" to /var/log/cron and nothing alerted.

How it works (no per-job config; the crontab is the source of truth):
  1. `crontab -l` is parsed; each job line's schedule is expanded with croniter (CRON_TZ honoured).
  2. The job's log file is derived from the wrapper script's LOG_FILE= line (or a `>> file` redirect).
  3. Checks per job: wrapper missing, slot with no "[CRON] ... started at" line (did not start),
     in-progress run silent / too long versus the job's own history (stalled / runaway),
     finished run with exit != 0.
  4. Crontab unreadable/empty/shrunk, and cron lines that cannot be resolved ("unmonitored"), alert too.
All alerts of one run go out in one email.  State: data/cron_watch_state.json (atomic writes).
"""
from __future__ import annotations

import hashlib
import json
import os
import re
import subprocess
import sys
import time
import traceback
from datetime import datetime
from pathlib import Path
from typing import Callable

PROJECT_DIR = Path(__file__).resolve().parent.parent
if str(PROJECT_DIR) not in sys.path:
    sys.path.insert(0, str(PROJECT_DIR))

import config  # noqa: E402

STATE_FILE = PROJECT_DIR / "data" / "cron_watch_state.json"
ERROR_FLAG = PROJECT_DIR / "data" / "cron_watch_error.flag"

_STARTED_RE  = re.compile(r"\[CRON\].*? started at (\d{4}-\d\d-\d\d \d\d:\d\d:\d\d)")
_FINISHED_RE = re.compile(r"\[CRON\].*? finished at (\d{4}-\d\d-\d\d \d\d:\d\d:\d\d)\s*\|\s*exit=(-?\d+)")
_DATE_SUB_RE = re.compile(r"\$\(date \+([^)]*)\)")
_ENV_LINE_RE = re.compile(r'^\s*([A-Za-z_][A-Za-z0-9_]*)=("?)(.*?)\2\s*$')
_JOB_ENV_RE  = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*=")

_TS_FMT = "%Y-%m-%d %H:%M:%S"


# ── Crontab parsing ──────────────────────────────────────────────────────────

def parse_crontab(text: str) -> list[dict]:
    """Return one dict per job line: {line, expr, command, tz}.  NAME=value lines set the env;
    CRON_TZ applies to the job lines that follow it."""
    jobs: list[dict] = []
    tz = ""
    for raw in text.splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        if _JOB_ENV_RE.match(line):
            name, _, value = line.partition("=")
            if name.strip() == "CRON_TZ":
                tz = value.strip().strip("\"'")
            continue
        parts = line.split(None, 1)
        if line.startswith("@"):
            if len(parts) < 2:
                continue
            expr, command = parts[0], parts[1]
        else:
            fields = line.split(None, 5)
            if len(fields) < 6:
                continue
            expr, command = " ".join(fields[:5]), fields[5]
        jobs.append({"line": line, "expr": expr, "command": command, "tz": tz})
    return jobs


def is_ignored(line: str, patterns: list[str]) -> bool:
    return any(p and re.search(p, line) for p in patterns)


def ignore_patterns() -> list[str]:
    # ";;" delimiter: single commas are legal inside regexes ({1,3}, [a,b]).
    return [p.strip() for p in config.CRON_WATCH_IGNORE_PATTERNS.split(";;") if p.strip()]


# ── Log resolution ───────────────────────────────────────────────────────────

def expand_date(text: str, when_local: datetime) -> str:
    """Expand $(date +FMT) with the slot's local time and unescape cron's \\%."""
    text = text.replace("\\%", "%")
    return _DATE_SUB_RE.sub(lambda m: when_local.strftime(m.group(1)), text)


def _expand_vars(text: str, env: dict[str, str]) -> str:
    for _ in range(3):  # nested references (LOG_DIR="$PROJECT_DIR/logs")
        text = re.sub(r"\$\{(\w+)\}|\$(\w+)",
                      lambda m: env.get(m.group(1) or m.group(2), m.group(0)), text)
    return text


def wrapper_log_template(wrapper_text: str) -> str | None:
    """Return the wrapper's LOG_FILE value with $VARs expanded but $(date ...) left in place."""
    env: dict[str, str] = {}
    template = None
    for raw in wrapper_text.splitlines():
        m = _ENV_LINE_RE.match(raw)
        if not m:
            continue
        name, value = m.group(1), m.group(3)
        if name == "LOG_FILE":
            template = value
        else:
            env[name] = value
    if template is None:
        return None
    return _expand_vars(template, env)


def resolve_job(entry: dict) -> dict:
    """Describe how to find a job's log.  Result keys:
    kind  wrapper | direct | unresolved
    key   stable job id (wrapper basename / log basename)
    wrapper (path or None), template (log path with $(date) pending or None), missing (bool)
    """
    command = entry["command"]
    first = command.split()[0] if command.split() else ""
    if first.endswith(".sh"):
        key = os.path.basename(first)
        path = Path(first)
        if not path.is_file() or not os.access(path, os.X_OK):
            return {"kind": "wrapper", "key": key, "wrapper": first, "template": None, "missing": True}
        try:
            template = wrapper_log_template(path.read_text(encoding="utf-8", errors="replace"))
        except OSError:
            template = None
        if not template:
            return {"kind": "unresolved", "key": key, "wrapper": first, "template": None, "missing": False}
        return {"kind": "wrapper", "key": key, "wrapper": first, "template": template, "missing": False}
    m = re.search(r">>\s*(\S+)", command)
    if m and m.group(1) != "/dev/null":
        template = m.group(1)
        return {"kind": "direct", "key": "direct:" + os.path.basename(_DATE_SUB_RE.sub("", template)),
                "wrapper": None, "template": template, "missing": False}
    return {"kind": "unresolved", "key": "line:" + hashlib.sha1(entry["line"].encode()).hexdigest()[:8],
            "wrapper": None, "template": None, "missing": False}


def log_path_for(template: str, when_local: datetime) -> Path:
    return Path(expand_date(template, when_local))


# ── Log parsing ──────────────────────────────────────────────────────────────

def _to_epoch(ts: str) -> float:
    return time.mktime(time.strptime(ts, _TS_FMT))   # wrapper `date` writes system-local time


def parse_runs(log_path: Path) -> list[dict]:
    """Runs found in a log: {start, finish|None, exit|None}, in file order."""
    runs: list[dict] = []
    try:
        with open(log_path, "r", encoding="utf-8", errors="replace") as fh:
            for line in fh:
                if "[CRON]" not in line:
                    continue
                m = _STARTED_RE.search(line)
                if m:
                    runs.append({"start": _to_epoch(m.group(1)), "finish": None, "exit": None})
                    continue
                m = _FINISHED_RE.search(line)
                if m and runs and runs[-1]["finish"] is None:
                    runs[-1]["finish"] = _to_epoch(m.group(1))
                    runs[-1]["exit"] = int(m.group(2))
    except OSError:
        return []
    return runs


# ── Slots ────────────────────────────────────────────────────────────────────

def due_slots(expr: str, tz_name: str, since: float, until: float) -> list[float]:
    """Epoch times the schedule fires in (since, until].  Raises ValueError on a bad expression."""
    from croniter import croniter
    if tz_name:
        from zoneinfo import ZoneInfo
        tz = ZoneInfo(tz_name)
    else:
        tz = datetime.now().astimezone().tzinfo
    it = croniter(expr, datetime.fromtimestamp(since, tz))
    slots: list[float] = []
    while True:
        ts = it.get_next(datetime).timestamp()
        if ts > until:
            return slots
        slots.append(ts)


# ── State ────────────────────────────────────────────────────────────────────

def load_state(path: Path = STATE_FILE) -> dict:
    try:
        with open(path, "r", encoding="utf-8") as fh:
            state = json.load(fh)
        if isinstance(state, dict):
            state.setdefault("jobs", {})
            state.setdefault("sent", {})
            state.setdefault("known_job_keys", [])
            return state
    except FileNotFoundError:
        pass
    except (OSError, ValueError) as exc:
        print(f"[cron_watch] state unreadable ({exc}) — starting fresh")
    return {"jobs": {}, "sent": {}, "known_job_keys": []}


def save_state(state: dict, path: Path = STATE_FILE) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump(state, fh)
    os.replace(tmp, path)


# ── Thresholds ───────────────────────────────────────────────────────────────

def stall_window_s(history: list[dict]) -> tuple[float, bool]:
    """(window seconds, learned).  Floor until MIN_HISTORY_RUNS successful runs are recorded."""
    floor = config.CRON_WATCH_STALL_FLOOR_MIN * 60
    if len(history) < config.CRON_WATCH_MIN_HISTORY_RUNS:
        return floor, False
    longest = max(h.get("max_silence_s", 0) for h in history)
    return max(floor, config.CRON_WATCH_STALL_MULTIPLIER * longest), True


def runaway_limit_s(history: list[dict]) -> float | None:
    if len(history) < config.CRON_WATCH_MIN_HISTORY_RUNS:
        return None
    return config.CRON_WATCH_RUNTIME_MULTIPLIER * max(h.get("duration_s", 0) for h in history)


def _fmt_ts(ts: float) -> str:
    return datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M")


def _fmt_dur(seconds: float) -> str:
    return f"{int(seconds // 60)} min"


# ── Core ─────────────────────────────────────────────────────────────────────

def evaluate(entries: list[dict], state: dict, now: float) -> list[dict]:
    """Run every check, update `state` in place, return the alerts currently true.
    Each alert: {key, job, kind, text}."""
    alerts: list[dict] = []
    patterns = ignore_patterns()
    if "initialized_at" not in state:
        state["initialized_at"] = now
    init_at   = state["initialized_at"]
    lookback  = now - config.CRON_WATCH_LOOKBACK_HOURS * 3600
    grace     = config.CRON_WATCH_START_GRACE_MIN * 60
    hist_cut  = now - config.CRON_WATCH_HISTORY_DAYS * 86400
    now_local = datetime.fromtimestamp(now)
    current_keys: set[str] = set()

    for entry in entries:
        if is_ignored(entry["line"], patterns):
            continue
        info = resolve_job(entry)
        key = info["key"]

        if info["kind"] == "unresolved":
            alerts.append({"key": f"unmonitored:{key}", "job": key, "kind": "unmonitored",
                           "text": f"Unmonitored cron job (cannot find its log): {entry['line']}"})
            continue
        current_keys.add(key)

        if info["missing"]:
            alerts.append({"key": f"wrapper:{key}", "job": key, "kind": "wrapper",
                           "text": f"Cron wrapper missing or not executable: {info['wrapper']} "
                                   f"(run ./setup_cron.sh to regenerate)"})

        job = state["jobs"].setdefault(key, {"history": [], "current": None, "recorded_starts": []})
        job["history"] = [h for h in job["history"] if h["start"] >= hist_cut]
        job["recorded_starts"] = [s for s in job["recorded_starts"] if s >= hist_cut]

        # 1. Start check
        try:
            slots = due_slots(entry["expr"], entry["tz"], max(lookback, init_at), now - grace)
        except Exception as exc:  # bad expression / unknown timezone / @reboot
            alerts.append({"key": f"badexpr:{key}", "job": key, "kind": "unmonitored",
                           "text": f"Unmonitored cron job (schedule not understood: {exc}): {entry['line']}"})
            continue
        for slot in slots:
            if info["missing"]:
                alerts.append({"key": f"start:{key}:{int(slot)}", "job": key, "kind": "start",
                               "text": f"Cron job did not start: {key} (slot {_fmt_ts(slot)}, wrapper missing)"})
                continue
            log = log_path_for(info["template"], datetime.fromtimestamp(slot))
            if info["kind"] == "direct":
                ok = log.exists() and log.stat().st_mtime >= slot
            else:
                ok = any(slot <= r["start"] <= slot + grace for r in parse_runs(log))
            if not ok:
                alerts.append({"key": f"start:{key}:{int(slot)}", "job": key, "kind": "start",
                               "text": f"Cron job did not start: {key} (slot {_fmt_ts(slot)}, "
                                       f"log {log.name} has no start line)"})

        if info["missing"] or info["kind"] != "wrapper":
            continue

        # 2/3. Progress + finish checks over today's and yesterday's logs
        logs = {log_path_for(info["template"], datetime.fromtimestamp(t))
                for t in (now, now - 86400)}
        for log in sorted(logs):
            runs = parse_runs(log)
            for i, run in enumerate(runs):
                if run["finish"] is None:
                    if i != len(runs) - 1 or run["start"] < lookback:
                        continue
                    _check_in_progress(key, job, run, log, now, alerts)
                elif run["start"] not in job["recorded_starts"]:
                    _record_finished(key, job, run, init_at, alerts)

    # 4. Crontab shrink
    for gone in sorted(set(state["known_job_keys"]) - current_keys):
        alerts.append({"key": f"removed:{gone}", "job": gone, "kind": "crontab",
                       "text": f"Cron job no longer in the crontab: {gone}"})
    state["known_job_keys"] = sorted(current_keys)
    return alerts


def _check_in_progress(key: str, job: dict, run: dict, log: Path, now: float, alerts: list[dict]) -> None:
    try:
        mtime = log.stat().st_mtime
    except OSError:
        return
    silence = max(0.0, now - max(mtime, run["start"]))
    cur = job.get("current")
    if not cur or cur.get("start") != run["start"]:
        cur = {"start": run["start"], "max_silence_s": 0}
    cur["max_silence_s"] = max(cur["max_silence_s"], silence)
    job["current"] = cur

    window, learned = stall_window_s(job["history"])
    if silence > window:
        note = "" if learned else " (no history yet, default window used)"
        alerts.append({"key": f"stall:{key}:{int(run['start'])}", "job": key, "kind": "stall",
                       "text": f"Cron job stalled: {key} started {_fmt_ts(run['start'])}, "
                               f"no log output for {_fmt_dur(silence)} (limit {_fmt_dur(window)}){note}"})
    limit = runaway_limit_s(job["history"])
    elapsed = now - run["start"]
    if limit is not None and elapsed > limit:
        alerts.append({"key": f"runaway:{key}:{int(run['start'])}", "job": key, "kind": "runaway",
                       "text": f"Cron job running far longer than usual: {key} started "
                               f"{_fmt_ts(run['start'])}, {_fmt_dur(elapsed)} so far (limit {_fmt_dur(limit)})"})


def _record_finished(key: str, job: dict, run: dict, init_at: float, alerts: list[dict]) -> None:
    job["recorded_starts"].append(run["start"])
    cur = job.get("current")
    max_silence = cur["max_silence_s"] if cur and cur.get("start") == run["start"] else 0
    if cur and cur.get("start") == run["start"]:
        job["current"] = None
    if run["exit"] == 0:
        job["history"].append({"start": run["start"], "duration_s": run["finish"] - run["start"],
                               "max_silence_s": max_silence})
    elif run["start"] >= init_at:   # failures from before the watcher existed are not news
        alerts.append({"key": f"exit:{key}:{int(run['start'])}", "job": key, "kind": "exit",
                       "text": f"Cron job failed: {key} started {_fmt_ts(run['start'])} exit={run['exit']}"})


def crontab_text() -> tuple[str | None, str]:
    """(text or None on failure, error detail)."""
    try:
        res = subprocess.run(["crontab", "-l"], capture_output=True, text=True, timeout=30)
    except (OSError, subprocess.SubprocessError) as exc:
        return None, str(exc)
    if res.returncode != 0:
        return None, (res.stderr or res.stdout).strip()
    return res.stdout, ""


def select_new_alerts(alerts: list[dict], state: dict, now: float) -> list[dict]:
    realert = config.CRON_WATCH_REALERT_HOURS * 3600
    return [a for a in alerts if now - state["sent"].get(a["key"], 0) >= realert]


def _alert_email(alerts: list[dict]) -> tuple[str, str]:
    jobs = sorted({a["job"] for a in alerts})
    subject = f"[Cron Alert] {len(alerts)} problem(s): " + ", ".join(jobs[:4]) + (" …" if len(jobs) > 4 else "")
    items = "".join(f"<li style='margin:6px 0'>{_esc(a['text'])}</li>" for a in alerts)
    body = (f"<html><body style='font-family:sans-serif;padding:24px;color:#1e293b'>"
            f"<h2 style='color:#dc2626'>Cron problem(s) — {datetime.now().strftime('%Y-%m-%d %H:%M')}</h2>"
            f"<ul>{items}</ul>"
            f"<p style='color:#94a3b8;font-size:11px'>Sent by scripts/cron_watch.py · each problem re-alerts at most "
            f"every {config.CRON_WATCH_REALERT_HOURS} h while it persists</p></body></html>")
    return subject, body


def _esc(s: str) -> str:
    return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def run(send: Callable[[str, str], object], now: float | None = None,
        crontab: str | None = None, state_path: Path = STATE_FILE) -> list[dict]:
    """One watcher pass.  `send(subject, html)` follows log_monitor._send_email's return contract
    (True sent / None no credentials / False transient failure).  Returns the alerts that were due."""
    now = time.time() if now is None else now
    state = load_state(state_path)
    alerts: list[dict] = []

    err = ""
    if crontab is None:
        crontab, err = crontab_text()
    if crontab is None or not parse_crontab(crontab):
        detail = err or "no job lines"
        alerts.append({"key": "crontab:unreadable", "job": "crontab", "kind": "crontab",
                       "text": f"Crontab unreadable or empty — no scheduled jobs are active ({detail})"})
        entries: list[dict] = []
    else:
        entries = parse_crontab(crontab)

    if entries:
        alerts += evaluate(entries, state, now)
    else:
        state.setdefault("initialized_at", now)

    new = select_new_alerts(alerts, state, now)
    if new:
        subject, body = _alert_email(new)
        sent = send(subject, body)
        if sent is not False:
            for a in new:
                state["sent"][a["key"]] = now
        else:
            print("[cron_watch] email failed — will retry next run")
        print(f"[cron_watch] {len(new)} alert(s): " + " | ".join(a["text"] for a in new))
    else:
        print(f"[cron_watch] ok — {len(entries)} cron line(s) checked, no new problems")

    cutoff = now - 2 * config.CRON_WATCH_REALERT_HOURS * 3600
    state["sent"] = {k: v for k, v in state["sent"].items() if v >= cutoff}
    save_state(state, state_path)
    return new


def run_safe(send: Callable[[str, str], object]) -> None:
    """Never raises into log_monitor; a watcher crash is logged and emailed at most once per re-alert window."""
    if not config.CRON_WATCH_ENABLED:
        return
    try:
        run(send)
    except Exception:
        tb = traceback.format_exc()
        print(f"[cron_watch] FAILED:\n{tb}")
        try:
            window = config.CRON_WATCH_REALERT_HOURS * 3600
            if not ERROR_FLAG.exists() or time.time() - ERROR_FLAG.stat().st_mtime >= window:
                send("[Cron Alert] cron_watch itself crashed",
                     f"<html><body><pre>{_esc(tb)}</pre></body></html>")
                ERROR_FLAG.parent.mkdir(parents=True, exist_ok=True)
                ERROR_FLAG.write_text(tb[-2000:], encoding="utf-8")
        except Exception as exc:
            print(f"[cron_watch] could not send crash alert: {exc}")
