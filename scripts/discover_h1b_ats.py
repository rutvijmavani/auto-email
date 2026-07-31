"""
scripts/discover_h1b_ats.py — H-1B sponsor ATS discovery.

For each top H-1B sponsor this script:
  1. Queries Google Knowledge Graph API for canonical name + website + Freebase MID.
     Caches MID in DB — skips KG call on re-runs.
  2. Looks up Wikidata P10311 (official jobs URL) via Freebase MID → P646 SPARQL batch.
     If P10311 found → store as jobs_url and skip career probing.
  3. Probes 19 career URL patterns (follow redirects, validate final URL).
     Rejects: homepage redirects, SSO/auth redirects, unrelated-domain redirects.
     Bonus: if redirect lands on known ATS domain → captures ATS directly.
  4. Falls back to Brave search: "{company} careers" → top 10 results →
     filter to plausible career URLs (company token in domain / known ATS domain /
     careers|jobs keyword) → Qwen3-8B picks best when multiple survive.
  5. Fetches career page HTML → fingerprints embedded ATS.

Usage:
    python scripts/discover_h1b_ats.py --top 20
    python scripts/discover_h1b_ats.py --top 20 --dry-run
    python scripts/discover_h1b_ats.py --fein 123456789
    python scripts/discover_h1b_ats.py --top 20 --force
    python scripts/discover_h1b_ats.py --top 20 --llm   # load Qwen3 for Brave disambiguation
"""

import argparse
import ipaddress
import json
import os
import re
import socket
import sys
import threading
import time
from collections import deque
from datetime import datetime, timezone
from urllib.parse import urljoin, urlparse

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import requests

from db.connection import get_conn
from db.quota import can_call, increment_usage, within_rpm
from db.schema import init_db
from logger import get_logger, init_logging

log = get_logger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# Config
# ─────────────────────────────────────────────────────────────────────────────

_KG_ENDPOINT     = "https://kgsearch.googleapis.com/v1/entities:search"
_KG_API_KEY      = os.environ.get("KG_API_KEY", "")

_WIKIDATA_SPARQL = "https://query.wikidata.org/sparql"

_BRAVE_ENDPOINT  = "https://api.search.brave.com/res/v1/web/search"
_BRAVE_API_KEY   = os.environ.get("BRAVE_API_KEY", "")
_BRAVE_QUOTA_FILE = os.path.join("data", "brave_quota.json")
_BRAVE_QUOTA_LIMIT = 950   # conservative out of 1000 free/month

_SPARQL_CHUNK_SIZE = 50    # Freebase MIDs per SPARQL VALUES block
_HTTP_TIMEOUT      = 12
_RECHECK_DAYS      = 7
_MAX_REDIRECTS     = 10
_RATE_LIMIT_BACKOFF = 10

_API_HEADERS = {
    "User-Agent": "H1B-ATS-Discover/2.0 (research; server.unilog@gmail.com)",
    "Accept-Language": "en-US,en;q=0.9",
}

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/125.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
}

_URL_RE = re.compile(r'https?://[^\s"\'<>]+', re.IGNORECASE)

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

# Root domains of known ATS platforms — redirect to these is a valid career page
_KNOWN_ATS_DOMAINS = {
    "myworkdayjobs.com", "greenhouse.io", "lever.co", "ashbyhq.com",
    "icims.com", "smartrecruiters.com", "jobvite.com", "taleo.net",
    "successfactors.com", "oraclecloud.com", "brassring.com",
    "eightfold.ai", "phenompeople.com", "jobscore.com",
}

# Keywords in final redirect URL that indicate SSO / auth wall
_AUTH_KEYWORDS = ("login", "okta", "auth", "sso", "oauth", "saml", "signin")

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


# ─────────────────────────────────────────────────────────────────────────────
# SSRF guard
# ─────────────────────────────────────────────────────────────────────────────

def _is_public_url(url: str) -> bool:
    try:
        parsed = urlparse(url)
        if parsed.scheme not in ("http", "https"):
            return False
        host = parsed.hostname
        if not host:
            return False
        infos = socket.getaddrinfo(host, None)
        for info in infos:
            addr = ipaddress.ip_address(info[4][0])
            if (
                addr.is_loopback or addr.is_link_local or addr.is_private
                or addr.is_reserved or addr.is_unspecified or addr.is_multicast
                or (isinstance(addr, ipaddress.IPv4Address)
                    and addr in ipaddress.IPv4Network("100.64.0.0/10"))
            ):
                return False
        return True
    except Exception:
        return False


# ─────────────────────────────────────────────────────────────────────────────
# Name normalisation
# ─────────────────────────────────────────────────────────────────────────────

def strip_legal_suffixes(name: str) -> str:
    name = _DBA_PATTERN.sub("", name).strip()
    prev = None
    while prev != name:
        prev = name
        name = _LEGAL_SUFFIXES.sub("", name).strip()
    return name.strip(" ,.")


def _root_domain(url: str) -> str:
    """'careers.amazon.com' → 'amazon.com'"""
    host  = urlparse(url).hostname or ""
    parts = host.split(".")
    return ".".join(parts[-2:]) if len(parts) >= 2 else host


# ─────────────────────────────────────────────────────────────────────────────
# SPARQL rate limiter (shared, thread-safe sliding window)
# ─────────────────────────────────────────────────────────────────────────────

class _RateLimiter:
    def __init__(self, rpm: int) -> None:
        self._rpm    = rpm
        self._window: deque = deque()
        self._lock   = threading.Lock()

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


_sparql_limiter = _RateLimiter(rpm=30)


# ─────────────────────────────────────────────────────────────────────────────
# Google Knowledge Graph API
# ─────────────────────────────────────────────────────────────────────────────

def kg_search(legal_name: str) -> dict | None:
    """
    Search KG API for legal_name with types=Organization filter.
    Returns {name, url, kg_mid} or None.
    kg_mid is the Freebase MID stripped of 'kg:' prefix (e.g. '/m/0mgkg').
    Uses can_call / increment_usage for daily + RPM tracking.
    """
    if not _KG_API_KEY:
        log.warning("KG_API_KEY not set — skipping KG search for %r", legal_name)
        return None

    if not can_call("kg_api"):
        if not within_rpm("kg_api"):
            log.info("KG API RPM limit hit — waiting 60s")
            time.sleep(60)
            if not can_call("kg_api"):
                log.warning("KG API still unavailable after wait — skipping")
                return None
        else:
            log.warning("KG API daily limit (100k) reached")
            return None

    query = strip_legal_suffixes(legal_name) or legal_name
    try:
        resp = requests.get(
            _KG_ENDPOINT,
            params={
                "query":     query,
                "key":       _KG_API_KEY,
                "types":     "Organization",
                "limit":     1,
                "languages": "en",
                "indent":    "False",
            },
            headers=_API_HEADERS,
            timeout=_HTTP_TIMEOUT,
        )
        increment_usage("kg_api")

        if resp.status_code == 429:
            wait = int(resp.headers.get("Retry-After", _RATE_LIMIT_BACKOFF))
            log.debug("KG API rate-limited — waiting %ds", wait)
            time.sleep(wait)
            return None

        resp.raise_for_status()
        items = resp.json().get("itemListElement", [])
        if not items:
            log.debug("KG API: no results for %r", query)
            return None

        result = items[0].get("result", {})
        raw_id = result.get("@id", "")               # "kg:/m/0mgkg"
        kg_mid = raw_id.removeprefix("kg:") or None   # "/m/0mgkg"
        name   = result.get("name") or None
        url    = result.get("url") or None

        if url and not _is_public_url(url):
            url = None

        log.debug("KG hit: %r → name=%r url=%r mid=%r", query, name, url, kg_mid)
        return {"name": name, "url": url, "kg_mid": kg_mid}

    except requests.exceptions.RequestException as e:
        log.debug("KG API error for %r: %s", legal_name, e)
        return None


# ─────────────────────────────────────────────────────────────────────────────
# Wikidata SPARQL — P646 (Freebase MID) → QID + P10311 (jobs URL)
# ─────────────────────────────────────────────────────────────────────────────

def _sparql_batch_p10311(mids: list[str]) -> dict[str, dict]:
    """
    Batch-fetch Wikidata QID + P10311 (official jobs URL) for a list of
    Freebase MIDs using a single SPARQL VALUES query.

    Returns {mid: {"qid": str|None, "jobs_url": str|None}}.
    """
    if not mids:
        return {}

    values = " ".join(f'("{m}")' for m in mids)
    sparql = (
        "SELECT ?mid ?item ?jobs_url WHERE { "
        f"VALUES (?mid) {{ {values} }} "
        "?item wdt:P646 ?mid . "
        "OPTIONAL { ?item wdt:P10311 ?jobs_url } "
        "}"
    )
    headers = {**_API_HEADERS, "Accept": "application/sparql-results+json"}
    params  = {"query": sparql, "format": "json"}

    def _do_request():
        return requests.get(
            _WIKIDATA_SPARQL, params=params, headers=headers, timeout=60
        )

    _sparql_limiter.acquire("SPARQL P646+P10311 batch")
    try:
        r = _do_request()
        if r.status_code == 429:
            wait = int(r.headers.get("Retry-After", _RATE_LIMIT_BACKOFF))
            log.debug("SPARQL rate-limited — waiting %ds", wait)
            time.sleep(wait)
            _sparql_limiter.acquire("SPARQL P646+P10311 retry")
            r = _do_request()
        r.raise_for_status()
        bindings = r.json()["results"]["bindings"]
    except (requests.exceptions.RequestException, ValueError, KeyError) as e:
        log.debug("SPARQL P646+P10311 batch error: %s", e)
        return {m: {"qid": None, "jobs_url": None} for m in mids}

    out: dict[str, dict] = {m: {"qid": None, "jobs_url": None} for m in mids}
    for row in bindings:
        mid      = row.get("mid", {}).get("value")
        item_uri = row.get("item", {}).get("value", "")
        qid      = item_uri.split("/")[-1] if item_uri else None
        jobs_url = row.get("jobs_url", {}).get("value") or None
        if mid and mid in out:
            if qid:
                out[mid]["qid"] = qid
            if jobs_url and _is_public_url(jobs_url):
                out[mid]["jobs_url"] = jobs_url

    return out


def _sparql_batch_p10311_all(mids: list[str]) -> dict[str, dict]:
    """Chunk mids into _SPARQL_CHUNK_SIZE batches and merge results."""
    out: dict[str, dict] = {}
    for i in range(0, len(mids), _SPARQL_CHUNK_SIZE):
        chunk = mids[i: i + _SPARQL_CHUNK_SIZE]
        log.info(
            "SPARQL P646+P10311 batch %d–%d of %d …",
            i + 1, min(i + _SPARQL_CHUNK_SIZE, len(mids)), len(mids),
        )
        out.update(_sparql_batch_p10311(chunk))
    return out


# ─────────────────────────────────────────────────────────────────────────────
# Brave search — career page URL fallback
# ─────────────────────────────────────────────────────────────────────────────

# ─────────────────────────────────────────────────────────────────────────────
# Qwen3-8B — career URL disambiguation
# ─────────────────────────────────────────────────────────────────────────────

_llm = None
_STRIP_THINK = re.compile(r"<think>.*?</think>", re.DOTALL)


def _load_llm() -> bool:
    global _llm
    if _llm is not None:
        return True
    model_path = os.environ.get("EMAIL_PROCESSOR_MODEL_PATH", "")
    if not model_path:
        return False
    try:
        from llama_cpp import Llama
        log.info("Loading Qwen3-8B from %s …", model_path)
        _llm = Llama(model_path=model_path, n_ctx=512, n_threads=2, verbose=False)
        return True
    except Exception as e:
        log.warning("Failed to load Qwen3-8B: %s", e)
        return False


def _qwen_pick_career_url(
    candidates: list[str],
    company_name: str,
    website_url: str | None,
) -> str | None:
    """
    Ask Qwen3-8B to pick the official career page from a short candidate list.
    Returns the chosen URL or None if LLM unavailable / fails.
    """
    if _llm is None or not candidates:
        return None
    numbered = "\n".join(f"{i+1}. {u}" for i, u in enumerate(candidates))
    website_hint = f" (official website: {website_url})" if website_url else ""
    prompt = (
        "/no_think\n"
        f"Company: {company_name}{website_hint}\n\n"
        f"Candidate career page URLs:\n{numbered}\n\n"
        "Which number is the official career page for this company? "
        "Reply with ONLY the number."
    )
    try:
        out  = _llm(prompt, max_tokens=8, temperature=0.0, stop=["\n", ".", " "])
        text = _STRIP_THINK.sub("", out["choices"][0]["text"]).strip()
        idx  = int(text) - 1
        if 0 <= idx < len(candidates):
            log.debug("Qwen3 picked candidate %d: %s", idx + 1, candidates[idx])
            return candidates[idx]
    except Exception as e:
        log.debug("Qwen3 career URL pick failed: %s", e)
    return None


# ─────────────────────────────────────────────────────────────────────────────
# Brave search helpers
# ─────────────────────────────────────────────────────────────────────────────

def _company_tokens(name: str) -> set[str]:
    """Extract lowercase searchable tokens from company name for domain matching."""
    stripped = strip_legal_suffixes(name).lower()
    tokens   = {w for w in re.split(r"\W+", stripped) if len(w) >= 3}
    # also add concatenated form: "capital one" → "capitalone"
    joined = re.sub(r"\W+", "", stripped)
    if len(joined) >= 4:
        tokens.add(joined)
    return tokens


def _is_plausible_career_url(url: str, company_tokens: set[str]) -> bool:
    """
    True if the URL is plausibly the company's own career page:
      a) company name token appears in the domain
      b) URL is on a known ATS domain
      c) 'careers' or 'jobs' appears in domain or path
    """
    parsed = urlparse(url)
    domain = (parsed.hostname or "").lower()
    path   = parsed.path.lower()
    return (
        any(tok in domain for tok in company_tokens)
        or _root_domain(url) in _KNOWN_ATS_DOMAINS
        or any(kw in domain + path for kw in ("careers", "jobs", "career"))
    )


def _brave_load_quota() -> dict:
    try:
        with open(_BRAVE_QUOTA_FILE) as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {"calls": 0}


def _brave_save_quota(data: dict) -> None:
    os.makedirs("data", exist_ok=True)
    with open(_BRAVE_QUOTA_FILE, "w") as f:
        json.dump(data, f)


def brave_career_search(
    company_name: str,
    website_url: str | None = None,
) -> str | None:
    """
    Search Brave for "{company} careers", filter top 10 results to plausible
    career pages, then use Qwen3-8B to pick the best when multiple survive.

    Returns the chosen URL or None if quota exhausted / key missing / no match.
    """
    if not _BRAVE_API_KEY:
        log.debug("BRAVE_API_KEY not set — skipping Brave career search")
        return None

    quota = _brave_load_quota()
    if quota.get("calls", 0) >= _BRAVE_QUOTA_LIMIT:
        log.warning("Brave monthly quota exhausted — skipping search for %r", company_name)
        return None

    query  = f"{company_name} careers"
    tokens = _company_tokens(company_name)

    try:
        resp = requests.get(
            _BRAVE_ENDPOINT,
            headers={
                "X-Subscription-Token": _BRAVE_API_KEY,
                "Accept": "application/json",
            },
            params={"q": query, "count": 10},
            timeout=_HTTP_TIMEOUT,
        )

        if resp.status_code == 401:
            log.error("Brave API: invalid API key")
            return None
        if resp.status_code == 429:
            log.warning("Brave API: rate limited for %r", company_name)
            return None
        if resp.status_code != 200:
            log.debug("Brave API: HTTP %d for %r", resp.status_code, company_name)
            return None

        quota["calls"] = quota.get("calls", 0) + 1
        _brave_save_quota(quota)

        organics   = resp.json().get("web", {}).get("results", [])
        candidates = [
            item["url"] for item in organics
            if item.get("url")
            and _is_public_url(item["url"])
            and _is_plausible_career_url(item["url"], tokens)
        ]

        log.debug(
            "Brave: %d/%d results plausible for %r",
            len(candidates), len(organics), company_name,
        )

        if not candidates:
            return None
        if len(candidates) == 1:
            log.debug("Brave: single candidate → %s", candidates[0])
            return candidates[0]

        # Multiple candidates — ask Qwen3 to pick
        chosen = _qwen_pick_career_url(candidates[:3], company_name, website_url)
        if chosen:
            return chosen

        # Qwen3 unavailable — return first plausible result
        log.debug("Brave: Qwen3 unavailable, using first candidate: %s", candidates[0])
        return candidates[0]

    except requests.exceptions.RequestException as e:
        log.debug("Brave search error for %r: %s", company_name, e)
        return None


# ─────────────────────────────────────────────────────────────────────────────
# Career page detection
# ─────────────────────────────────────────────────────────────────────────────

def _fetch_html(url: str) -> tuple[str | None, str]:
    """
    GET url following redirects manually (SSRF-validates every hop).
    Returns (html_text, final_url) or (None, url) on failure.
    """
    if not _is_public_url(url):
        return None, url
    current = url
    try:
        for _ in range(_MAX_REDIRECTS):
            r = requests.get(
                current, headers=_HEADERS, timeout=_HTTP_TIMEOUT,
                allow_redirects=False,
            )
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
    """Scan HTML for embedded ATS URLs. Returns (platform, slug) or (None, None)."""
    from jobs.ats.patterns import match_ats_pattern
    for u in _URL_RE.findall(html):
        result = match_ats_pattern(u)
        if result:
            return result["platform"], result["slug"]
    return None, None


def _is_homepage(final_url: str) -> bool:
    path = urlparse(final_url).path.rstrip("/")
    return not path or path == ""


def _is_auth_redirect(final_url: str) -> bool:
    url_lower = final_url.lower()
    return any(kw in url_lower for kw in _AUTH_KEYWORDS)


def discover_careers_url(
    website_url: str,
) -> tuple[str | None, str | None, str | None]:
    """
    Probe 19 career URL patterns for company website.
    Returns (careers_url, detected_platform, detected_slug).

    Follows redirects but rejects:
      - Final URL is homepage (path == "/" or empty)
      - Final URL contains SSO/auth keywords
      - Final URL jumped to unrelated domain (not company domain or known ATS)

    Bonus: if redirect lands on known ATS domain, captures ATS from URL directly.
    """
    if not _is_public_url(website_url):
        log.warning("Skipping non-public URL: %s", website_url)
        return None, None, None

    parsed        = urlparse(website_url)
    netloc        = parsed.netloc
    domain        = netloc.removeprefix("www.")
    base          = f"{parsed.scheme}://{netloc}"
    company_root  = _root_domain(website_url)

    candidates: list[str] = []
    for tmpl in _CAREER_SUBDOMAINS:
        candidates.append(tmpl.format(domain=domain))
    for path in _CAREER_PATHS:
        candidates.append(base + path)

    for url in candidates:
        html, final_url = _fetch_html(url)
        if html is None:
            continue

        # Reject homepage redirects
        if _is_homepage(final_url):
            log.debug("  %s → homepage redirect, skipping", url)
            continue

        # Reject SSO/auth walls
        if _is_auth_redirect(final_url):
            log.debug("  %s → auth redirect (%s), skipping", url, final_url)
            continue

        final_root = _root_domain(final_url)

        # Reject unrelated domain (not company domain and not a known ATS)
        if final_root != company_root and final_root not in _KNOWN_ATS_DOMAINS:
            log.debug(
                "  %s → unrelated domain (%s), skipping", url, final_root
            )
            continue

        # If redirect landed directly on an ATS domain, try URL-level match first
        if final_root in _KNOWN_ATS_DOMAINS:
            from jobs.ats.patterns import match_ats_pattern
            result = match_ats_pattern(final_url)
            if result:
                log.debug(
                    "  %s → ATS redirect: %s slug=%s",
                    url, result["platform"], result["slug"],
                )
                return final_url, result["platform"], result["slug"]

        # Fingerprint HTML for embedded ATS
        platform, slug = _find_ats_in_html(html)
        log.debug(
            "  %s → career page found; platform=%s slug=%s",
            final_url, platform, slug,
        )
        return final_url, platform, slug

    return None, None, None


# ─────────────────────────────────────────────────────────────────────────────
# DB helpers
# ─────────────────────────────────────────────────────────────────────────────

def load_top_sponsors(limit: int, conn) -> list[dict]:
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
    return [dict(r) for r in cur.fetchall()]


def load_by_fein(fein: str, conn) -> dict | None:
    cur = conn.cursor()
    cur.execute(
        "SELECT employer_fein, employer_name FROM dol_h1b_employers "
        "WHERE employer_fein = %s",
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
            "[DRY-RUN] fein=%s name=%r canonical=%r website=%r kg_mid=%r "
            "jobs_url=%r careers=%r platform=%s slug=%s",
            data["employer_fein"], data["employer_name"],
            data.get("canonical_name"), data.get("website_url"),
            data.get("kg_mid"), data.get("jobs_url"),
            data.get("careers_url"), data.get("detected_platform"),
            data.get("detected_slug"),
        )
        return
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO h1b_ats_discovery
            (employer_fein, employer_name, canonical_name, canonical_source,
             wikidata_qid, kg_mid, website_url, jobs_url,
             careers_url, detected_platform, detected_slug, last_checked)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
        ON CONFLICT (employer_fein) DO UPDATE SET
            employer_name     = EXCLUDED.employer_name,
            canonical_name    = EXCLUDED.canonical_name,
            canonical_source  = EXCLUDED.canonical_source,
            wikidata_qid      = COALESCE(EXCLUDED.wikidata_qid,  h1b_ats_discovery.wikidata_qid),
            kg_mid            = COALESCE(EXCLUDED.kg_mid,         h1b_ats_discovery.kg_mid),
            website_url       = EXCLUDED.website_url,
            jobs_url          = COALESCE(EXCLUDED.jobs_url,       h1b_ats_discovery.jobs_url),
            careers_url       = EXCLUDED.careers_url,
            detected_platform = EXCLUDED.detected_platform,
            detected_slug     = EXCLUDED.detected_slug,
            last_checked      = NOW()
    """, (
        data["employer_fein"],
        data["employer_name"],
        data.get("canonical_name"),
        data.get("canonical_source"),
        data.get("wikidata_qid"),
        data.get("kg_mid"),
        data.get("website_url"),
        data.get("jobs_url"),
        data.get("careers_url"),
        data.get("detected_platform"),
        data.get("detected_slug"),
    ))
    conn.commit()


# ─────────────────────────────────────────────────────────────────────────────
# Core processing
# ─────────────────────────────────────────────────────────────────────────────

def _is_recently_checked(
    fein: str, conn, force: bool, existing: dict | None = None,
) -> dict | None:
    if force:
        return None
    if existing is None:
        existing = get_discovery_row(fein, conn)
    if not (existing and existing.get("last_checked")):
        return None
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
    force: bool,
    prefetched: dict | None = None,
) -> dict:
    """
    Enrich one employer through the full pipeline and upsert into h1b_ats_discovery.

    prefetched (batch mode): dict with keys canonical_name, website_url,
    canonical_source, kg_mid, jobs_url — skips KG + SPARQL calls when provided.
    """
    fein = emp["employer_fein"]
    name = emp["employer_name"]

    log.info("── %s  %s", fein, name)

    existing = _is_recently_checked(fein, conn, force)
    if existing:
        log.info("  Skipping — checked recently")
        return existing

    if prefetched is not None:
        canonical_name   = prefetched.get("canonical_name")
        canonical_source = prefetched.get("canonical_source")
        website_url      = prefetched.get("website_url")
        kg_mid           = prefetched.get("kg_mid")
        wikidata_qid     = prefetched.get("wikidata_qid")
        jobs_url         = prefetched.get("jobs_url")
    else:
        # Single-employer mode: inline KG + SPARQL calls
        kg_mid           = None
        wikidata_qid     = None
        jobs_url         = None

        existing_row = get_discovery_row(fein, conn)
        cached_mid   = existing_row.get("kg_mid") if existing_row else None

        if cached_mid and not force:
            log.info("  KG MID cached: %s", cached_mid)
            kg_mid         = cached_mid
            canonical_name = existing_row.get("canonical_name")
            website_url    = existing_row.get("website_url")
            canonical_source = existing_row.get("canonical_source")
        else:
            log.info("  KG API …")
            kg = kg_search(name)
            if kg:
                kg_mid           = kg.get("kg_mid")
                canonical_name   = kg.get("name")
                website_url      = kg.get("url")
                canonical_source = "kg" if (canonical_name or website_url) else None
            else:
                canonical_name   = strip_legal_suffixes(name) or None
                canonical_source = "regex" if canonical_name else None
                website_url      = None

        if kg_mid:
            log.info("  SPARQL P10311 for MID %s …", kg_mid)
            sparql_res   = _sparql_batch_p10311([kg_mid])
            entry        = sparql_res.get(kg_mid, {})
            wikidata_qid = entry.get("qid")
            jobs_url     = entry.get("jobs_url")

    log.info(
        "  canonical=%r source=%s website=%s jobs_url=%s",
        canonical_name, canonical_source, website_url, jobs_url,
    )

    careers_url       = None
    detected_platform = None
    detected_slug     = None

    if jobs_url:
        # P10311 found — use it as the careers URL, no further probing needed
        careers_url = jobs_url
        log.info("  P10311 jobs URL: %s", jobs_url)
    elif website_url:
        # Phase 3: 19-pattern probe
        log.info("  Probing 19 career URL patterns on %s …", website_url)
        try:
            careers_url, detected_platform, detected_slug = discover_careers_url(
                website_url
            )
        except Exception as e:
            log.warning("  Career probe failed: %s", e)

        # Phase 4: Brave search fallback
        if not careers_url:
            search_name = canonical_name or strip_legal_suffixes(name) or name
            log.info("  Brave search fallback for %r …", search_name)
            brave_url = brave_career_search(search_name, website_url=website_url)
            if brave_url:
                careers_url = brave_url
                log.info("  Brave found: %s", brave_url)
                # Phase 5: fingerprint the Brave result page
                try:
                    html, _ = _fetch_html(brave_url)
                    if html:
                        detected_platform, detected_slug = _find_ats_in_html(html)
                except Exception as e:
                    log.warning("  HTML fingerprint failed: %s", e)

    if careers_url:
        log.info(
            "  careers=%s  platform=%s  slug=%s",
            careers_url, detected_platform, detected_slug,
        )
    else:
        log.info("  No careers page found")

    result = {
        "employer_fein":    fein,
        "employer_name":    name,
        "canonical_name":   canonical_name,
        "canonical_source": canonical_source,
        "wikidata_qid":     wikidata_qid,
        "kg_mid":           kg_mid,
        "website_url":      website_url,
        "jobs_url":         jobs_url,
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
    if result.get("jobs_url"):
        stats["with_jobs_url"] += 1
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
    parser.add_argument("--force",   action="store_true",
                        help="Re-check even if recently checked")
    parser.add_argument("--llm",     action="store_true",
                        help="Load Qwen3-8B for Brave result disambiguation")
    args = parser.parse_args()

    if args.llm:
        _load_llm()

    init_db()
    conn = get_conn()

    if args.fein:
        row = load_by_fein(args.fein, conn)
        if not row:
            log.error("FEIN %s not found in dol_h1b_employers", args.fein)
            sys.exit(1)
        employers = [row]
    else:
        log.info("Loading top %d H-1B sponsors …", args.top)
        employers = load_top_sponsors(args.top, conn)
        log.info("Loaded %d employers", len(employers))

    stats = {
        "processed": 0, "skipped": 0,
        "with_website": 0, "with_jobs_url": 0, "with_ats": 0,
    }

    if args.fein:
        # Single-employer: inline KG + SPARQL, no batching
        for i, emp in enumerate(employers, 1):
            log.info("[%d/%d]", i, len(employers))
            result = process_employer(
                emp, conn, dry_run=args.dry_run, force=args.force,
            )
            _tally(stats, result, args.force)
    else:
        # ── Phase 1: KG API for all employers → collect kg_mids ──────────────
        log.info("Phase 1: KG API for %d employers …", len(employers))
        kg_map: dict[str, dict] = {}   # fein → {kg_mid, canonical_name, website_url, ...}
        all_mids: list[str]     = []
        seen_mids: set[str]     = set()

        for i, emp in enumerate(employers, 1):
            fein = emp["employer_fein"]
            name = emp["employer_name"]

            existing = get_discovery_row(fein, conn)

            if _is_recently_checked(fein, conn, args.force, existing=existing):
                log.info("[%d/%d] skip (recent): %s", i, len(employers), name)
                kg_map[fein] = {"skip": True}
                stats["skipped"] += 1
                continue

            cached_mid = existing.get("kg_mid") if existing else None

            if cached_mid and not args.force:
                log.info("[%d/%d] KG MID cached (%s): %s", i, len(employers), cached_mid, name)
                entry = {
                    "kg_mid":          cached_mid,
                    "canonical_name":  existing.get("canonical_name"),
                    "canonical_source": existing.get("canonical_source"),
                    "website_url":     existing.get("website_url"),
                    "wikidata_qid":    existing.get("wikidata_qid"),
                }
            else:
                log.info("[%d/%d KG] %s", i, len(employers), name)
                kg = kg_search(name)
                if kg:
                    entry = {
                        "kg_mid":          kg.get("kg_mid"),
                        "canonical_name":  kg.get("name"),
                        "canonical_source": "kg" if (kg.get("name") or kg.get("url")) else None,
                        "website_url":     kg.get("url"),
                        "wikidata_qid":    None,
                    }
                else:
                    stripped = strip_legal_suffixes(name)
                    entry = {
                        "kg_mid":          None,
                        "canonical_name":  stripped or None,
                        "canonical_source": "regex" if stripped else None,
                        "website_url":     None,
                        "wikidata_qid":    None,
                    }

            kg_map[fein] = entry
            mid = entry.get("kg_mid")
            if mid and mid not in seen_mids:
                seen_mids.add(mid)
                all_mids.append(mid)

        # ── Phase 2: SPARQL P646+P10311 batch for all MIDs ───────────────────
        log.info("Phase 2: SPARQL P10311 batch for %d MIDs …", len(all_mids))
        sparql_map = _sparql_batch_p10311_all(all_mids)   # {mid: {qid, jobs_url}}

        for entry in kg_map.values():
            if entry.get("skip"):
                continue
            mid = entry.get("kg_mid")
            if mid:
                sp = sparql_map.get(mid, {})
                entry["wikidata_qid"] = sp.get("qid")
                entry["jobs_url"]     = sp.get("jobs_url")
            else:
                entry["jobs_url"] = None

        # ── Phase 3: career probe + upsert ───────────────────────────────────
        log.info("Phase 3: career probe for %d employers …", len(employers))
        for i, emp in enumerate(employers, 1):
            fein  = emp["employer_fein"]
            entry = kg_map.get(fein, {})
            if entry.get("skip"):
                continue
            log.info("[%d/%d career] %s  %s", i, len(employers), fein, emp["employer_name"])
            result = process_employer(
                emp, conn,
                dry_run=args.dry_run,
                force=args.force,
                prefetched=entry,
            )
            _tally(stats, result, args.force)
            time.sleep(0.2)

    conn.close()

    log.info(
        "Done. processed=%d skipped=%d with_website=%d with_jobs_url=%d with_ats=%d",
        stats["processed"], stats["skipped"],
        stats["with_website"], stats["with_jobs_url"], stats["with_ats"],
    )


if __name__ == "__main__":
    main()
