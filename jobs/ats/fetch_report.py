# jobs/ats/fetch_report.py — completeness record every ATS feeder attaches to its job list
#
# fetch_jobs() keeps returning a plain list of job dicts, so no caller or test changes. The list is a
# FetchResult — a list subclass carrying `.report` (a FetchReport) — and report_of() reads it back,
# building an "unknown" report for feeders that attach nothing (never a false "ok").
#
# What the report says:
#   fetched_distinct  distinct jobs actually received (by job id, else job url) — NOT raw rows, so a
#                     repeated page (Jibe once returned page 1 three times) cannot pass as complete
#   reported_total    what the platform (or the feed itself) says the board holds, when it says anything
#   stop_reason       why the loop ended; only STOP_ALL_PAGES / STOP_EMPTY_PAGE / STOP_SINGLE_RESPONSE
#                     prove the board was read to its end
#   complete          True  = read to the end and distinct >= reported_total less FETCH_TOLERANCE_PCT
#                     False = ended abnormally, or clearly short of the total
#                     None  = unknown (no total to compare, or the feeder reports nothing)
#   server_short      reported_total is advisory (Taleo serves fewer rows than it counts) and the board
#                     was read to the server's end: recorded once as a class, complete stays True
#
# Platforms without a total (Ashby, Lever, sitemap …) stay complete=None; a missing page is not
# detectable there, a repeated page still is.

import re
from dataclasses import asdict, dataclass
from typing import Optional

from config import FETCH_SERVER_SHORT_MIN_PCT, FETCH_TOLERANCE_PCT

# ── why a fetch loop ended ───────────────────────────────────────────────────
STOP_ALL_PAGES       = "all_pages"        # paged until offset reached the reported total
STOP_EMPTY_PAGE      = "empty_page"       # the server ran dry
STOP_SINGLE_RESPONSE = "single_response"  # the whole board arrives in one response
STOP_REPEAT_PAGE     = "repeat_page"      # a page added nothing new — the site ignored the paging param
STOP_REQUEST_FAILED  = "request_failed"   # an HTTP/parse failure cut the loop short
STOP_PAGE_CAP        = "page_cap"         # safety cap hit before the end
STOP_NO_CONFIG       = "no_config"        # slug unusable or target not discoverable — nothing was fetched
STOP_UNKNOWN         = "unknown"          # feeder attaches no report

CLEAN_STOPS  = frozenset({STOP_ALL_PAGES, STOP_EMPTY_PAGE, STOP_SINGLE_RESPONSE})
FAILED_STOPS = frozenset({STOP_REPEAT_PAGE, STOP_REQUEST_FAILED, STOP_PAGE_CAP, STOP_NO_CONFIG})

# ── where reported_total came from ───────────────────────────────────────────
TOTAL_ATS        = "ats"         # a total field in the platform's own response
TOTAL_FACET_SUM  = "facet_sum"   # Workday: sum of facet-value counts across the slices
TOTAL_FEED_COUNT = "feed_count"  # raw item count of the feed itself (SuccessFactors <Job> tags)


@dataclass
class FetchReport:
    stop_reason:      str = STOP_UNKNOWN
    pages:            int = 0
    fetched_distinct: int = 0
    unusable:         int = 0                 # rows received but unusable (no title / no id) — dropped by the feeder
    reported_total:   Optional[int] = None
    total_source:     Optional[str] = None
    repeated_pages:   int = 0
    advisory_total:   bool = False            # the platform may serve fewer rows than it reports
    complete:         Optional[bool] = None
    server_short:     bool = False
    note:             str = ""

    @property
    def shortfall(self) -> int:
        """Jobs the total claims that were not fetched (0 when there is no total)."""
        if self.reported_total is None:
            return 0
        return max(self.reported_total - self.fetched_distinct, 0)

    def _tolerance_pct(self) -> float:
        """A feed's own item count is exact (any parse loss is a defect); an ATS-claimed total gets slack."""
        return 0.0 if self.total_source == TOTAL_FEED_COUNT else FETCH_TOLERANCE_PCT

    def finalize(self) -> "FetchReport":
        """Derive `complete` / `server_short` from the counts and stop reason; returns self."""
        self.server_short = False
        if self.stop_reason in FAILED_STOPS:
            self.complete = False
        elif self.stop_reason not in CLEAN_STOPS or self.reported_total is None:
            self.complete = None
        elif self.fetched_distinct >= self.reported_total * (1 - self._tolerance_pct() / 100):
            self.complete = True
        elif (self.advisory_total
              and self.fetched_distinct >= self.reported_total * FETCH_SERVER_SHORT_MIN_PCT / 100):
            self.complete     = True
            self.server_short = True
        else:
            self.complete = False
        return self

    def to_dict(self) -> dict:
        return asdict(self)


class FetchResult(list):
    """A list of job dicts that also carries the FetchReport of the fetch that produced it."""
    report: Optional[FetchReport] = None


def count_distinct(jobs) -> int:
    """Distinct jobs by job_id, falling back to job_url; a job with neither is not countable."""
    seen = set()
    for j in jobs:
        key = j.get("job_id") or j.get("job_url")
        if key:
            seen.add(key)
    return len(seen)


def attach(jobs, report: FetchReport) -> FetchResult:
    """Finalize `report` and return `jobs` as a FetchResult carrying it."""
    out = FetchResult(jobs)
    out.report = report.finalize()
    return out


def failed(stop_reason: str, note: str = "") -> FetchResult:
    """An empty result with a report saying why nothing was fetched."""
    return attach([], FetchReport(stop_reason=stop_reason, note=note))


def new_sitemap_stats() -> dict:
    """
    Counters a sitemap walker fills in: files fetched, files that failed, branches cut by the depth limit,
    files that arrived but were not read to their end (truncated), and the bytes received.
    """
    return {"files": 0, "failed": 0, "too_deep": 0, "truncated": 0, "bytes": 0}


_ROOT_RE  = re.compile(r"<([A-Za-z_][\w:.-]*)")   # first element start; skips <?xml, <!-- and <!DOCTYPE
_TAG_RE   = re.compile(r"<([A-Za-z_][\w:.-]*)")
_SKIP_RE  = re.compile(r"<!\[CDATA\[.*?\]\]>|<!--.*?-->", re.DOTALL)   # text inside is content, not structure
_TAIL_LEN = 256


def _local(name: str) -> str:
    """Tag name without namespace prefix, lowercased (html.parser lowercases, the xml parser does not)."""
    return name.rsplit(":", 1)[-1].lower()


def check_sitemap_body(resp, stats, soup=None) -> bool:
    """
    Did this sitemap/feed file arrive, and get read by OUR code, in full? Records the answer in `stats`
    (stats["bytes"] += size, stats["truncated"] += 1 on a problem) and returns True when it looks whole.

    A sitemap lists non-job pages too, so its URL count can't be compared with a job count. What can be
    proved is that the file itself was consumed completely:
      * the root element is closed at the end of the text (a cut-off file ends mid-entry);
      * for EVERY tag name in the file, the raw text holds no more elements than the parser produced
        (`soup`, when the caller passes it) — a lenient parser silently drops what it can't read
        (Capgemini: 1 of ~1537 <Job>). Tag names come from the file itself, nothing is hardcoded;
        CDATA and comments are removed first so markup inside descriptions isn't counted.
    * when the server declares Content-Length, the bytes read off the connection (`resp.raw.tell()`, counted
      before decompression, so gzip is covered; else len(content) for an uncompressed body) must reach it.
    Chunked responses declare no length, so that check is skipped and the structural checks decide.
    """
    text = getattr(resp, "text", "") or ""
    content = getattr(resp, "content", None)
    size = len(content) if isinstance(content, (bytes, bytearray)) else len(text.encode("utf-8", "ignore"))
    stats["bytes"] = stats.get("bytes", 0) + size

    problem = ""
    headers = getattr(resp, "headers", None)
    declared = headers.get("Content-Length") if hasattr(headers, "get") else None
    encoded = headers.get("Content-Encoding") if hasattr(headers, "get") else None
    wire = None                       # bytes actually read off the connection (pre-decompression)
    tell = getattr(getattr(resp, "raw", None), "tell", None)
    if callable(tell):
        try:
            v = tell()
            wire = v if isinstance(v, int) and v > 0 else None
        except Exception:
            wire = None
    if wire is None and not isinstance(encoded, str) and isinstance(content, (bytes, bytearray)):
        wire = len(content)
    if isinstance(declared, (str, int)) and str(declared).isdigit() and wire is not None \
            and wire < int(declared):
        problem = f"received {wire} of {declared} bytes"
    body = _SKIP_RE.sub("", text.lstrip("﻿").strip())
    raw = {}
    for name in _TAG_RE.findall(body):
        key = _local(name)
        raw[key] = raw.get(key, 0) + 1
    if body and not problem:
        m = _ROOT_RE.search(body)
        if m:
            tail = body[-_TAIL_LEN:]
            closed = re.search(r"</\s*" + re.escape(m.group(1)) + r"\s*>\s*$", tail, re.IGNORECASE)
            empty_root = sum(raw.values()) == 1 and re.search(r"/>\s*$", tail)
            if not closed and not empty_root:
                problem = f"root <{m.group(1)}> never closed"
    if not problem and soup is not None:
        parsed = {}
        for tag in soup.find_all(True):
            key = _local(tag.name or "")
            parsed[key] = parsed.get(key, 0) + 1
        short = {k: (parsed.get(k, 0), n) for k, n in raw.items() if parsed.get(k, 0) < n}
        if short:
            worst = max(short, key=lambda k: short[k][1] - short[k][0])
            got, want = short[worst]
            problem = f"parsed {got} of {want} <{worst}> elements"

    if problem:
        stats["truncated"] = stats.get("truncated", 0) + 1
        stats.setdefault("problems", []).append(problem)
        return False
    return True


def sitemap_report(stats: dict, jobs, unusable: int = 0) -> FetchReport:
    """
    Report for a sitemap-derived fetch. The sitemap IS the board's listing, so there is no separate total:
    the thing to prove is that every sitemap file was read — fetched, and consumed to its end. A failed,
    truncated or depth-cut file means part of the listing was never seen (one dead child of a 4-file index
    silently drops a quarter of the jobs). complete therefore stays None on a clean read — whether the
    sitemap lags the live board is unknowable.
    """
    cut = stats.get("failed", 0) + stats.get("too_deep", 0) + stats.get("truncated", 0)
    if cut:
        stop = STOP_REQUEST_FAILED
        note = (f"{stats.get('failed', 0)} of {stats.get('files', 0)} sitemap files failed, "
                f"{stats.get('too_deep', 0)} cut by depth limit, {stats.get('truncated', 0)} truncated")
        if stats.get("problems"):
            note += " (" + "; ".join(stats["problems"][:3]) + ")"
    else:
        stop = STOP_ALL_PAGES if jobs else STOP_EMPTY_PAGE
        note = ""
    return FetchReport(
        stop_reason      = stop,
        pages            = stats.get("files", 0),
        fetched_distinct = count_distinct(jobs),
        unusable         = unusable,
        note             = note,
    )


def report_of(jobs) -> FetchReport:
    """
    The report attached to a fetch result, or — for feeders that attach none — an "unknown" report
    built from the jobs themselves (complete stays None: nothing says the board was read to its end).
    """
    rep = getattr(jobs, "report", None)
    if isinstance(rep, FetchReport):
        return rep
    return FetchReport(fetched_distinct=count_distinct(jobs)).finalize()
