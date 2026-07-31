"""
scripts/discover_h1b_ats.py — H-1B sponsor ATS discovery.

For each top H-1B sponsor (ranked by total USCIS approvals) this script:
  1. Queries Wikidata for canonical brand name + official website (P856).
  2. Falls back to Wikipedia article title for canonical name.
  3. Falls back to regex suffix-stripping for canonical name.
  4. Optionally uses Qwen3-8B for unusual names when --llm flag is passed.
  5. Tries common career URL paths on the discovered website.
  6. Fetches each career page and fingerprints ATS via HTML scanning
     (reuses jobs/ats/patterns.py match_ats_pattern).
  7. Stores results in h1b_ats_discovery table.

Usage:
    python scripts/discover_h1b_ats.py --top 20
    python scripts/discover_h1b_ats.py --top 20 --dry-run
    python scripts/discover_h1b_ats.py --fein 123456789
    python scripts/discover_h1b_ats.py --top 100 --llm
    python scripts/discover_h1b_ats.py --top 20 --force   # re-check even if recently checked
"""

import argparse
import ipaddress
import os
import re
import socket
import sys
import threading
import time
from collections import deque
from urllib.parse import urljoin, urlparse

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import requests

from db.connection import get_conn
from db.schema import init_db
from logger import get_logger, init_logging

log = get_logger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# Config
# ─────────────────────────────────────────────────────────────────────────────

# Wikidata REST API v1 (recommended over legacy Action API — versioned, cleaner structure)
_WIKIDATA_REST   = "https://www.wikidata.org/w/rest.php/wikibase/v1"
# SPARQL endpoint — separate host, separate rate-limit pool from the REST API
_WIKIDATA_SPARQL = "https://query.wikidata.org/sparql"
_WIKIPEDIA_API   = "https://en.wikipedia.org/w/api.php"

_SPARQL_CHUNK_SIZE = 100   # max QIDs per SPARQL VALUES block

_CAREER_PATHS = [
    "/careers",
    "/jobs",
    "/en/careers",
    "/en/jobs",
    "/en-us/careers",
    "/us/careers",
    "/about/careers",
    "/company/careers",
    "/about/jobs",
    "/work",
    "/join",
    "/join-us",
    "/working-here",
    "/work-with-us",
    "/open-positions",
    "/opportunities",
]

_CAREER_SUBDOMAINS = [
    "https://careers.{domain}",
    "https://jobs.{domain}",
    "https://work.{domain}",
]

_HTTP_TIMEOUT       = 12
_RECHECK_DAYS       = 7
_MAX_REDIRECTS      = 10
_RATE_LIMIT_BACKOFF = 10   # seconds to sleep on 429 before one retry

_LEGAL_SUFFIXES = re.compile(
    r"\s*[,.]?\s*\b(?:LLC|L\.L\.C\.|INC\.?|CORP\.?|CORPORATION|"
    r"LTD\.?|LIMITED|L\.P\.?|LP|LLP|L\.L\.P\.|PLLC|P\.L\.L\.C\.|"
    r"CO\.?|COMPANY|GROUP|HOLDINGS?|HOLDING|ENTERPRISES?|ASSOCIATES?|"
    r"SERVICES?|SOLUTIONS?|TECHNOLOGIES?|SYSTEMS?|PARTNERS?|"
    r"INTERNATIONAL|GLOBAL|AMERICA|AMERICAS|NA|N\.A\.|USA|US)\b\s*$",
    re.IGNORECASE,
)

_DBA_PATTERN = re.compile(
    r"\s+(?:D[/.]?B[/.]?A\.?|DOING BUSINESS AS)\s+.*$",
    re.IGNORECASE,
)

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/125.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
}

_URL_RE = re.compile(r'https?://[^\s"\'<>]+', re.IGNORECASE)

# ─────────────────────────────────────────────────────────────────────────────
# SSRF guard
# ─────────────────────────────────────────────────────────────────────────────

def _is_public_url(url: str) -> bool:
    """
    Return True only when url resolves to a public routable address.
    Rejects loopback, link-local, private RFC1918, and cloud-metadata ranges.
    Only http and https schemes are allowed.
    """
    try:
        parsed = urlparse(url)
        if parsed.scheme not in ("http", "https"):
            return False
        host   = parsed.hostname
        if not host:
            return False
        # Resolve all A/AAAA records; reject if ANY is non-public
        infos = socket.getaddrinfo(host, None)
        for info in infos:
            addr = ipaddress.ip_address(info[4][0])
            if (
                addr.is_loopback
                or addr.is_link_local
                or addr.is_private
                or addr.is_reserved
                or addr.is_unspecified
                or addr.is_multicast
                # cloud metadata: 169.254.169.254 (covered by is_link_local on IPv4)
                # explicitly block 100.64/10 (CGNAT shared) just in case
                or (isinstance(addr, ipaddress.IPv4Address)
                    and ipaddress.IPv4Address(addr) in ipaddress.IPv4Network("100.64.0.0/10"))
            ):
                return False
        return True
    except Exception:
        return False


# ─────────────────────────────────────────────────────────────────────────────
# Name normalisation
# ─────────────────────────────────────────────────────────────────────────────

def strip_legal_suffixes(name: str) -> str:
    """Remove DBA clause and common legal entity suffixes from a legal name."""
    name = _DBA_PATTERN.sub("", name).strip()
    prev = None
    while prev != name:
        prev = name
        name = _LEGAL_SUFFIXES.sub("", name).strip()
    return name.strip(" ,.")


# ─────────────────────────────────────────────────────────────────────────────
# Wikidata
# ─────────────────────────────────────────────────────────────────────────────

_API_HEADERS = {"User-Agent": "H1B-ATS-Discover/1.0 (research bot)"}


class _RateLimiter:
    """Thread-safe sliding-window rate limiter (same pattern as db/quota.py)."""

    def __init__(self, rpm: int) -> None:
        self._rpm  = rpm
        self._window: deque = deque()
        self._lock = threading.Lock()

    def acquire(self, api_name: str) -> None:
        while True:
            now = time.time()
            with self._lock:
                while self._window and self._window[0] < now - 60:
                    self._window.popleft()
                if len(self._window) < self._rpm:
                    self._window.append(now)
                    return
            log.debug("%s RPM limit (%d/min) — waiting 3s", api_name, self._rpm)
            time.sleep(3)


_wikidata_limiter  = _RateLimiter(rpm=100)  # anon REST limit is much higher; 20 was too conservative
_wikipedia_limiter = _RateLimiter(rpm=20)
_sparql_limiter    = _RateLimiter(rpm=30)   # query.wikidata.org — separate pool, 60s timeout per query


def _api_get(url: str, params: dict, api_name: str,
             limiter: _RateLimiter) -> "requests.Response | None":
    """GET with rate-limiter acquire and one Retry-After-aware retry on 429."""
    limiter.acquire(api_name)
    try:
        r = requests.get(url, params=params, headers=_API_HEADERS,
                         timeout=_HTTP_TIMEOUT)
        if r.status_code == 429:
            try:
                wait = int(r.headers.get("Retry-After", _RATE_LIMIT_BACKOFF))
            except (ValueError, TypeError):
                wait = _RATE_LIMIT_BACKOFF
            log.debug("%s rate-limited — waiting %ds", api_name, wait)
            time.sleep(wait)
            limiter.acquire(api_name)
            r = requests.get(url, params=params, headers=_API_HEADERS,
                             timeout=_HTTP_TIMEOUT)
        r.raise_for_status()
        return r
    except requests.exceptions.RequestException as e:
        log.debug("%s error: %s", api_name, e)
        return None


def _wikidata_search(query: str) -> list[dict]:
    """Return top Wikidata item search results using the REST API v1."""
    r = _api_get(
        f"{_WIKIDATA_REST}/search/items",
        {"q": query, "language": "en", "limit": 5},
        "Wikidata search",
        _wikidata_limiter,
    )
    if not r:
        return []
    try:
        return r.json().get("results", [])
    except ValueError:
        log.debug("Wikidata search: malformed JSON response")
        return []


def _wikidata_website(qid: str) -> str | None:
    """Fetch P856 (official website) statements for a Wikidata item via REST API v1.
    Skips deprecated statements. Prefers .com; falls back to shortest URL."""
    r = _api_get(
        f"{_WIKIDATA_REST}/entities/items/{qid}/statements",
        {"property": "P856"},
        "Wikidata statements",
        _wikidata_limiter,
    )
    if not r:
        return None
    try:
        p856 = r.json().get("P856", [])
    except ValueError:
        log.debug("Wikidata statements: malformed JSON response for %s", qid)
        return None
    urls: list[str] = []
    for stmt in p856:
        if stmt.get("rank") == "deprecated":
            continue
        val = stmt.get("value", {})
        if val.get("type") == "value":
            content = val.get("content", "")
            if isinstance(content, str) and content.startswith("http") and _is_public_url(content):
                urls.append(content.rstrip("/"))
    if not urls:
        return None
    com = [u for u in urls if (urlparse(u).hostname or "").endswith(".com")]
    return min(com or urls, key=len)


def _wikidata_search_for_qid(legal_name: str) -> tuple[str | None, str | None]:
    """
    Search Wikidata for legal_name; return (qid, canonical_name) from first hit.
    Does NOT fetch P856 — that is done later in a SPARQL batch.
    Tries both the original name and the suffix-stripped version.
    """
    queries = [legal_name]
    stripped = strip_legal_suffixes(legal_name)
    if stripped and stripped != legal_name:
        queries.append(stripped)
    for query in queries:
        results = _wikidata_search(query)
        if not results:
            continue
        hit = results[0]
        qid            = hit.get("id")
        canonical_name = hit.get("display-label", {}).get("value")
        if qid or canonical_name:
            return qid, canonical_name
    return None, None


def _sparql_batch_websites(qids: list[str]) -> dict[str, str | None]:
    """
    Fetch P856 (official website) for up to _SPARQL_CHUNK_SIZE Wikidata QIDs in
    one SPARQL query against query.wikidata.org (separate endpoint + rate-limit
    pool from the REST API).

    Returns {qid: best_url_or_None}. Applies the same .com-preference + shortest
    fallback as _wikidata_website.
    """
    if not qids:
        return {}

    values = " ".join(f"wd:{qid}" for qid in qids)
    sparql = (
        "SELECT ?item ?website WHERE { "
        f"VALUES ?item {{ {values} }} "
        "OPTIONAL { ?item wdt:P856 ?website. } "
        "}"
    )
    _sparql_limiter.acquire("SPARQL P856 batch")
    try:
        r = requests.get(
            _WIKIDATA_SPARQL,
            params={"query": sparql, "format": "json"},
            headers={**_API_HEADERS, "Accept": "application/sparql-results+json"},
            timeout=60,
        )
        r.raise_for_status()
        bindings = r.json()["results"]["bindings"]
    except (requests.exceptions.RequestException, ValueError, KeyError) as e:
        log.debug("SPARQL P856 batch error: %s", e)
        return {qid: None for qid in qids}

    url_map: dict[str, list[str]] = {qid: [] for qid in qids}
    for row in bindings:
        qid = row["item"]["value"].split("/")[-1]
        if "website" in row:
            url = row["website"]["value"].rstrip("/")
            if _is_public_url(url):
                url_map.setdefault(qid, []).append(url)

    out: dict[str, str | None] = {}
    for qid, urls in url_map.items():
        if not urls:
            out[qid] = None
        else:
            com = [u for u in urls if (urlparse(u).hostname or "").endswith(".com")]
            out[qid] = min(com or urls, key=len)
    return out


def _sparql_batch_websites_all(qids: list[str]) -> dict[str, str | None]:
    """Chunk qids into _SPARQL_CHUNK_SIZE batches and merge results."""
    out: dict[str, str | None] = {}
    for i in range(0, len(qids), _SPARQL_CHUNK_SIZE):
        chunk = qids[i : i + _SPARQL_CHUNK_SIZE]
        log.info("SPARQL P856 batch %d–%d of %d …",
                 i + 1, min(i + _SPARQL_CHUNK_SIZE, len(qids)), len(qids))
        out.update(_sparql_batch_websites(chunk))
    return out


def enrich_wikidata(legal_name: str) -> dict:
    """
    Search Wikidata for legal_name using the REST API.
    Returns dict with keys: canonical_name, website_url, qid (all may be None).
    Tries both the original name and the suffix-stripped version.
    """
    queries = [legal_name]
    stripped = strip_legal_suffixes(legal_name)
    if stripped and stripped != legal_name:
        queries.append(stripped)

    for query in queries:
        results = _wikidata_search(query)
        if not results:
            continue
        hit = results[0]
        qid            = hit.get("id")
        # REST API uses "display-label" key (hyphenated) for the label object
        canonical_name = hit.get("display-label", {}).get("value")
        website_url    = _wikidata_website(qid) if qid else None
        if canonical_name or website_url:
            return {
                "canonical_name": canonical_name,
                "website_url":    website_url,
                "qid":            qid,
                "source":         "wikidata",
            }

    return {"canonical_name": None, "website_url": None, "qid": None, "source": None}


# ─────────────────────────────────────────────────────────────────────────────
# Wikipedia fallback
# ─────────────────────────────────────────────────────────────────────────────

def enrich_wikipedia(legal_name: str) -> str | None:
    """
    Search Wikipedia for legal_name; return article title as canonical name.
    Only used when Wikidata returns no canonical name.
    Filters out legal case titles (contain " v. ") and disambiguation pages.
    """
    stripped = strip_legal_suffixes(legal_name)
    queries = [legal_name]
    if stripped and stripped != legal_name:
        queries.append(stripped)
    for query in queries:
        r = _api_get(_WIKIPEDIA_API, {
            "action": "query", "list": "search",
            "srsearch": query, "srlimit": 3,
            "srprop": "snippet", "format": "json",
        }, "Wikipedia search", _wikipedia_limiter)
        if not r:
            continue
        try:
            hits = r.json().get("query", {}).get("search", [])
        except ValueError:
            log.debug("Wikipedia search: malformed JSON response for query %r", query)
            continue
        for hit in hits:
            title = hit.get("title", "")
            # Skip legal cases and disambiguation pages
            if " v. " in title or title.endswith("(disambiguation)"):
                continue
            return title
    return None


# ─────────────────────────────────────────────────────────────────────────────
# Qwen3-8B fallback (optional, --llm flag)
# ─────────────────────────────────────────────────────────────────────────────

_llm = None

def _load_llm() -> bool:
    global _llm
    if _llm is not None:
        return True
    model_path = os.environ.get("EMAIL_PROCESSOR_MODEL_PATH", "")
    if not model_path:
        log.warning("EMAIL_PROCESSOR_MODEL_PATH not set — skipping LLM fallback")
        return False
    try:
        from llama_cpp import Llama
        log.info("Loading Qwen3-8B from %s …", model_path)
        _llm = Llama(model_path=model_path, n_ctx=512, n_threads=2, verbose=False)
        log.info("Model ready")
        return True
    except Exception as e:
        log.warning("Failed to load Qwen3-8B: %s", e)
        return False


_STRIP_THINK = re.compile(r"<think>.*?</think>", re.DOTALL)

def llm_extract_brand(legal_name: str) -> str | None:
    """Ask Qwen3 to extract the consumer brand name from a legal entity name."""
    if _llm is None:
        return None
    prompt = (
        "/no_think\n"
        "Extract the well-known consumer brand or company name from this legal entity name. "
        "Reply with ONLY the brand name, nothing else.\n"
        f"Legal name: {legal_name}\n"
        "Brand name:"
    )
    try:
        out = _llm(prompt, max_tokens=24, temperature=0.0, stop=["\n", ".", ","])
        text = out["choices"][0]["text"].strip()
        text = _STRIP_THINK.sub("", text).strip()
        return text if text else None
    except Exception as e:
        log.debug("LLM brand extraction error: %s", e)
        return None


# ─────────────────────────────────────────────────────────────────────────────
# Career page detection
# ─────────────────────────────────────────────────────────────────────────────

def _url_is_reachable(url: str) -> bool:
    """HEAD request; True if 2xx or 3xx."""
    try:
        r = requests.head(url, headers=_HEADERS, timeout=_HTTP_TIMEOUT,
                          allow_redirects=True)
        return r.status_code < 400
    except Exception:
        return False


def _fetch_html(url: str) -> tuple[str | None, str]:
    """
    GET url; return (html_text, final_url).
    Manually follows redirects so every intermediate hop is SSRF-validated.
    Returns (None, url) on failure or when any hop resolves to a non-public address.
    """
    if not _is_public_url(url):
        return None, url
    current = url
    try:
        for _ in range(_MAX_REDIRECTS):
            r = requests.get(current, headers=_HEADERS, timeout=_HTTP_TIMEOUT,
                             allow_redirects=False)
            if r.is_redirect:
                location = r.headers.get("Location", "")
                next_url  = urljoin(current, location)
                if not _is_public_url(next_url):
                    log.debug("Redirect to non-public URL blocked: %s", next_url)
                    return None, url
                current = next_url
                continue
            if r.status_code < 400:
                return r.text, current
            return None, current
        log.debug("Too many redirects for %s", url)
    except requests.exceptions.RequestException as e:
        log.debug("Fetch error %s: %s", url, e)
    return None, url


def _find_ats_in_html(html: str) -> tuple[str | None, str | None]:
    """
    Scan HTML for embedded ATS URLs.
    Returns (platform, slug) or (None, None).
    """
    from jobs.ats.patterns import match_ats_pattern

    urls = _URL_RE.findall(html)
    for u in urls:
        result = match_ats_pattern(u)
        if result:
            return result["platform"], result["slug"]
    return None, None


def discover_careers_url(website_url: str) -> tuple[str | None, str | None, str | None]:
    """
    Given a company website URL, find the careers page and detect ATS.
    Returns (careers_url, detected_platform, detected_slug).
    All may be None.
    """
    if not _is_public_url(website_url):
        log.warning("Skipping non-public URL: %s", website_url)
        return None, None, None

    parsed = urlparse(website_url)
    netloc = parsed.netloc
    domain = netloc.removeprefix("www.")
    base   = f"{parsed.scheme}://{netloc}"

    candidates: list[str] = []

    # Subdomain variants first (careers.company.com)
    for tmpl in _CAREER_SUBDOMAINS:
        candidates.append(tmpl.format(domain=domain))

    # Path variants on root domain
    for path in _CAREER_PATHS:
        candidates.append(base + path)

    for url in candidates:
        html, final_url = _fetch_html(url)
        if html is None:
            continue

        # Check if page is a meaningful careers page (not just redirect to homepage)
        final_parsed = urlparse(final_url)
        final_path   = final_parsed.path.rstrip("/")
        if not final_path or final_path in ("", "/"):
            log.debug("  %s → redirected to homepage, skipping", url)
            continue

        platform, slug = _find_ats_in_html(html)
        log.debug("  %s → careers page found; platform=%s slug=%s",
                  final_url, platform, slug)
        return final_url, platform, slug

    return None, None, None


# ─────────────────────────────────────────────────────────────────────────────
# DB helpers
# ─────────────────────────────────────────────────────────────────────────────

def load_top_sponsors(limit: int, conn) -> list[dict]:
    """
    Load top H-1B sponsors by total USCIS approvals.
    Joins dol_h1b_employers (for FEIN and employer_name) with
    uscis_h1b_petitions (for approval totals).
    Falls back to dol_h1b_employers.total_certified if no USCIS data.
    """
    cur = conn.cursor()
    cur.execute("""
        SELECT
            d.employer_fein,
            d.employer_name,
            COALESCE(
                SUM(
                    u.new_employment_approval +
                    u.continuation_approval +
                    u.change_same_employer_approval +
                    u.new_concurrent_approval +
                    u.change_of_employer_approval +
                    u.amended_approval
                ),
                d.total_certified
            ) AS total_approvals
        FROM dol_h1b_employers d
        LEFT JOIN uscis_h1b_petitions u
               ON u.tax_id = RIGHT(d.employer_fein, 4)
              AND (
                  u.employer_legal_norm = d.employer_name_norm
               OR u.employer_name_norm  = d.trade_name_dba_norm
              )
        GROUP BY d.employer_fein, d.employer_name, d.total_certified
        ORDER BY total_approvals DESC NULLS LAST
        LIMIT %s
    """, (limit,))
    rows = cur.fetchall()
    return [dict(r) for r in rows]


def load_by_fein(fein: str, conn) -> dict | None:
    cur = conn.cursor()
    cur.execute(
        "SELECT employer_fein, employer_name FROM dol_h1b_employers WHERE employer_fein = %s",
        (fein,),
    )
    row = cur.fetchone()
    return dict(row) if row else None


def get_discovery_row(fein: str, conn) -> dict | None:
    cur = conn.cursor()
    cur.execute(
        "SELECT * FROM h1b_ats_discovery WHERE employer_fein = %s", (fein,)
    )
    row = cur.fetchone()
    return dict(row) if row else None


def upsert_discovery(data: dict, conn, dry_run: bool = False) -> None:
    if dry_run:
        log.info(
            "[DRY-RUN] would upsert: fein=%s name=%r canonical=%r website=%r "
            "careers=%r platform=%s slug=%s",
            data["employer_fein"], data["employer_name"],
            data.get("canonical_name"), data.get("website_url"),
            data.get("careers_url"), data.get("detected_platform"),
            data.get("detected_slug"),
        )
        return
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO h1b_ats_discovery
            (employer_fein, employer_name, canonical_name, canonical_source,
             wikidata_qid, website_url, careers_url, detected_platform,
             detected_slug, last_checked)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
        ON CONFLICT (employer_fein) DO UPDATE SET
            employer_name    = EXCLUDED.employer_name,
            canonical_name   = EXCLUDED.canonical_name,
            canonical_source = EXCLUDED.canonical_source,
            wikidata_qid     = COALESCE(EXCLUDED.wikidata_qid, h1b_ats_discovery.wikidata_qid),
            website_url      = EXCLUDED.website_url,
            careers_url      = EXCLUDED.careers_url,
            detected_platform= EXCLUDED.detected_platform,
            detected_slug    = EXCLUDED.detected_slug,
            last_checked     = NOW()
    """, (
        data["employer_fein"],
        data["employer_name"],
        data.get("canonical_name"),
        data.get("canonical_source"),
        data.get("wikidata_qid"),
        data.get("website_url"),
        data.get("careers_url"),
        data.get("detected_platform"),
        data.get("detected_slug"),
    ))
    conn.commit()


# ─────────────────────────────────────────────────────────────────────────────
# Core processing
# ─────────────────────────────────────────────────────────────────────────────

def _is_recently_checked(fein: str, conn, force: bool) -> dict | None:
    """Return existing row if it was checked within _RECHECK_DAYS and force=False."""
    if force:
        return None
    existing = get_discovery_row(fein, conn)
    if not (existing and existing.get("last_checked")):
        return None
    from datetime import datetime, timezone
    lc = existing["last_checked"]
    if lc.tzinfo is None:
        lc = lc.replace(tzinfo=timezone.utc)
    else:
        lc = lc.astimezone(timezone.utc)
    if (datetime.now(timezone.utc) - lc).days < _RECHECK_DAYS:
        return existing
    return None


def process_employer(
    emp: dict,
    conn,
    dry_run: bool,
    use_llm: bool,
    force: bool,
    prefetched: dict | None = None,
) -> dict:
    """
    Enrich one employer and upsert into h1b_ats_discovery.

    prefetched (batch mode only): dict with keys canonical_name, website_url,
    canonical_source — skips Wikidata calls when provided. Wikipedia/LLM/regex
    fallbacks still run when canonical_name is missing.
    """
    fein  = emp["employer_fein"]
    name  = emp["employer_name"]

    log.info("── %s  %s", fein, name)

    # Skip if recently checked (unless forced)
    existing = _is_recently_checked(fein, conn, force)
    if existing:
        log.info("  Skipping — checked recently")
        return existing

    if prefetched is not None:
        # Batch mode: Wikidata search + P856 already resolved upstream
        wikidata_qid     = prefetched.get("qid")
        canonical_name   = prefetched.get("canonical_name")
        website_url      = prefetched.get("website_url")
        canonical_source = prefetched.get("canonical_source")
    else:
        # Single-employer mode (--fein): full inline Wikidata call
        log.info("  Wikidata …")
        wd = enrich_wikidata(name)
        wikidata_qid     = wd["qid"]
        canonical_name   = wd["canonical_name"]
        website_url      = wd["website_url"]
        canonical_source = wd["source"]

    # Wikipedia fallback for canonical name
    if not canonical_name:
        log.info("  Wikipedia fallback …")
        canonical_name   = enrich_wikipedia(name)
        canonical_source = "wikipedia" if canonical_name else None

    # Qwen3 fallback
    if not canonical_name and use_llm:
        log.info("  Qwen3 fallback …")
        canonical_name   = llm_extract_brand(name)
        canonical_source = "qwen" if canonical_name else None

    # Regex suffix strip as last resort
    if not canonical_name:
        canonical_name   = strip_legal_suffixes(name)
        canonical_source = "regex" if canonical_name != name else None

    log.info("  canonical=%r source=%s website=%s", canonical_name,
             canonical_source, website_url)

    # Step 5: Career page discovery (needs a website)
    careers_url       = None
    detected_platform = None
    detected_slug     = None

    if website_url:
        log.info("  Discovering careers page on %s …", website_url)
        try:
            careers_url, detected_platform, detected_slug = discover_careers_url(
                website_url
            )
        except Exception as e:
            log.warning("  Career discovery failed: %s", e)

    if careers_url:
        log.info("  careers=%s  platform=%s  slug=%s",
                 careers_url, detected_platform, detected_slug)
    else:
        log.info("  No careers page found")

    result = {
        "employer_fein":    fein,
        "employer_name":    name,
        "canonical_name":   canonical_name,
        "canonical_source": canonical_source,
        "wikidata_qid":     wikidata_qid,
        "website_url":      website_url,
        "careers_url":      careers_url,
        "detected_platform": detected_platform,
        "detected_slug":    detected_slug,
    }

    upsert_discovery(result, conn, dry_run=dry_run)
    return result


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

def _tally(stats: dict, result: dict, force: bool) -> None:
    if result.get("last_checked") and not force:
        stats["skipped"] += 1
        return
    stats["processed"] += 1
    if result.get("website_url"):
        stats["with_website"] += 1
    if result.get("detected_platform"):
        stats["with_ats"] += 1


def main():
    init_logging("discover_h1b_ats")

    parser = argparse.ArgumentParser(
        description="Discover ATS platforms for top H-1B sponsors"
    )
    parser.add_argument("--top",     type=int, default=20,
                        help="Process top N sponsors (default: 20)")
    parser.add_argument("--fein",    type=str, default=None,
                        help="Process a single employer by FEIN")
    parser.add_argument("--dry-run", action="store_true",
                        help="Print results without writing to DB")
    parser.add_argument("--llm",     action="store_true",
                        help="Use Qwen3-8B for brand name extraction fallback")
    parser.add_argument("--force",   action="store_true",
                        help="Re-check even if recently checked")
    args = parser.parse_args()

    if args.llm:
        _load_llm()

    init_db()
    conn = get_conn()

    if args.fein:
        employers = []
        row = load_by_fein(args.fein, conn)
        if row:
            employers = [row]
        else:
            log.error("FEIN %s not found in dol_h1b_employers", args.fein)
            sys.exit(1)
    else:
        log.info("Loading top %d H-1B sponsors …", args.top)
        employers = load_top_sponsors(args.top, conn)
        log.info("Loaded %d employers", len(employers))

    stats = {"processed": 0, "with_website": 0, "with_ats": 0, "skipped": 0}

    if args.fein:
        # ── Single-employer mode: inline Wikidata calls, no batching ─────────
        for i, emp in enumerate(employers, 1):
            log.info("[%d/%d]", i, len(employers))
            result = process_employer(
                emp, conn,
                dry_run=args.dry_run,
                use_llm=args.llm,
                force=args.force,
            )
            _tally(stats, result, args.force)
    else:
        # ── Batch mode: two-phase Wikidata → SPARQL P856 → career probe ──────
        #
        # Phase 1: search all employers (REST API, 100 RPM) → collect QIDs
        log.info("Phase 1: Wikidata search for %d employers …", len(employers))
        search_map: dict[str, dict] = {}   # fein → {qid, canonical_name, canonical_source}
        all_qids: list[str] = []

        for i, emp in enumerate(employers, 1):
            fein = emp["employer_fein"]
            name = emp["employer_name"]

            existing = get_discovery_row(fein, conn)

            # Skip entirely if checked recently (existing behaviour)
            if _is_recently_checked(fein, conn, args.force):
                log.info("[%d/%d] skip (recent): %s", i, len(employers), name)
                search_map[fein] = {"skip": True}
                stats["skipped"] += 1
                continue

            cached_qid = existing.get("wikidata_qid") if existing else None

            if cached_qid and not args.force:
                # QID already known — skip search entirely, go straight to SPARQL batch
                log.info("[%d/%d] qid cached (%s): %s", i, len(employers), cached_qid, name)
                entry = {
                    "qid":            cached_qid,
                    "canonical_name": existing.get("canonical_name"),
                    "canonical_source": existing.get("canonical_source"),
                }
            else:
                # New company or --force: run Wikidata search
                log.info("[%d/%d search] %s", i, len(employers), name)
                qid, canonical_name = _wikidata_search_for_qid(name)
                entry = {
                    "qid":            qid,
                    "canonical_name": canonical_name,
                    "canonical_source": "wikidata" if (qid or canonical_name) else None,
                }

            search_map[fein] = entry
            if entry.get("qid"):
                all_qids.append(entry["qid"])

        # Phase 2: batch-fetch P856 via SPARQL (one query per 100 QIDs)
        log.info("Phase 2: SPARQL P856 batch for %d QIDs …", len(all_qids))
        website_map = _sparql_batch_websites_all(all_qids)  # {qid: url_or_None}

        # Merge website URLs back into search_map
        for fein, entry in search_map.items():
            if entry.get("skip"):
                continue
            qid = entry.get("qid")
            entry["website_url"] = website_map.get(qid) if qid else None

        # Phase 3: career probe + upsert per employer
        log.info("Phase 3: career probe for %d employers …", len(employers))
        for i, emp in enumerate(employers, 1):
            fein = emp["employer_fein"]
            entry = search_map.get(fein, {})
            if entry.get("skip"):
                continue
            log.info("[%d/%d career] %s  %s", i, len(employers),
                     fein, emp["employer_name"])
            result = process_employer(
                emp, conn,
                dry_run=args.dry_run,
                use_llm=args.llm,
                force=args.force,
                prefetched=entry,
            )
            _tally(stats, result, args.force)
            time.sleep(0.2)  # light delay between career probes only

    conn.close()

    log.info(
        "Done. processed=%d skipped=%d with_website=%d with_ats=%d",
        stats["processed"], stats["skipped"],
        stats["with_website"], stats["with_ats"],
    )


if __name__ == "__main__":
    main()
