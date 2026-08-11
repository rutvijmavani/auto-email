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

Two-pass architecture (Brave quota = 950/month):
  Pass 1 — KG + SPARQL + probe, no Brave (run freely, KG is 100k/day):
    python scripts/discover_h1b_ats.py --top 900
  Pass 2 — Brave only, for companies Pass 1 couldn't resolve:
    python scripts/discover_h1b_ats.py --brave-pass --top 950

Other usage:
    python scripts/discover_h1b_ats.py --top 20 --dry-run
    python scripts/discover_h1b_ats.py --fein 123456789   # single company by FEIN (includes Brave)
    python scripts/discover_h1b_ats.py --top 20 --force   # re-process already-checked companies
"""

import argparse
import html
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

from rapidfuzz import process as fuzz_process, utils as fuzz_utils
from rapidfuzz.fuzz import ratio as fuzz_ratio, WRatio

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import requests

from config import (
    CF_WORKER_SECRET, CF_WORKER_URL,
    DISCOVER_ATS_GEMINI_MODEL, DISCOVER_ATS_LLM_PROVIDER,
    REDIS_DB_MAINTENANCE, REDIS_GEMINI_LOCK,
)
from db.connection import get_conn
from db.quota import can_call, increment_usage, record_tpm, tpm_wait_seconds, within_rpm
from db.schema import init_db
from logger import get_logger, init_logging
from workers.redis_client import get_redis

log = get_logger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# Maintenance window
# ─────────────────────────────────────────────────────────────────────────────

def _is_maintenance(r) -> bool:
    if r is None:
        return False
    try:
        return bool(r.exists(REDIS_DB_MAINTENANCE))
    except Exception as exc:
        log.warning("Redis maintenance check failed (%s) — assuming not in maintenance", exc)
        return False


# ─────────────────────────────────────────────────────────────────────────────
# Config
# ─────────────────────────────────────────────────────────────────────────────

_KG_ENDPOINT     = "https://kgsearch.googleapis.com/v1/entities:search"
_KG_API_KEY      = os.environ.get("KG_API_KEY", "")
_KG_MAX_RETRIES  = 3   # max words to drop when a thin /g/ entity is returned

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

# Keywords in final redirect URL path+query that indicate SSO / auth wall
_AUTH_KEYWORDS = ("login", "okta", "auth", "sso", "oauth", "saml", "signin")
_AUTH_RE = re.compile(
    r"\b(?:" + "|".join(_AUTH_KEYWORDS) + r")\b",
    re.IGNORECASE,
)

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


def _kg_domain_gate(kg_url: str | None, sparql_p856: str | None, assigned_domain: str) -> bool:
    """
    Verify a KG entity against the LCA email-derived assigned_domain.

    Verification URL priority:
      1. KG entity URL (already fetched, primary)
      2. SPARQL P856 — fallback only when KG has no URL; same underlying
         data source so if KG has no URL, P856 probably won't either, but we try.

    Returns True only when a verification URL exists AND its root domain
    matches assigned_domain exactly.  No URL = False. Mismatch = False.
    """
    verification_url = kg_url or sparql_p856
    if not verification_url:
        return False
    host = (urlparse(verification_url).hostname or "").removeprefix("www.")
    return host == assigned_domain or host.endswith("." + assigned_domain)


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

_KG_FETCH_LIMIT      = 3   # candidates fetched per query
_KG_MIN_OVERLAP      = 30  # minimum score to accept at retry exhaustion
_KG_HIGH_CONFIDENCE  = 90  # return immediately — no point trying shorter queries
_KG_QUALITY_THRESHOLD = 60  # flag for human review when selected score < this

_KG_SIG_STOP = frozenset({
    # abbreviated entity type markers
    "inc", "llc", "ltd", "corp", "lp", "plc", "pvt", "co", "na", "llp",
    # full entity type markers
    "corporation", "incorporated", "limited", "company", "partnership",
    # connectors
    "the", "a", "an", "and", "of", "for", "in", "de", "los", "las",
})


def _entity_lead_in_query(legal_name: str, entity_name: str | None) -> bool:
    """Return True if EVERY significant token of the KG entity name has a
    plausible match somewhere in the legal name.

    No stripping — both names are tokenized raw and noise words (inc, llc,
    corporation, etc.) are removed via _KG_SIG_STOP.  Descriptive words like
    'Technologies', 'Enterprises', 'Solutions' are preserved so that
    'Fourth Technologies' ≠ 'Fourth Enterprises'.

    Match per token: prefix match OR fuzz.ratio ≥ 65.

    Rejects:
      'SQUAD SOFTWARE'    → 'San Diego Padres'       (san  ∉ squad/software)
      'Cruise LLC'        → 'Carnival Cruise Line'   (carnival ∉ cruise)
      'FOURTH ENTERPRISES'→ 'Fourth Technologies'    (technologies ∉ fourth/enterprises)

    Accepts:
      'WAL-MART ASSOCIATES' → 'Walmart'   (walmart.startswith('wal'))
      'HCL AMERICA'         → 'HCLTech'  (hcltech.startswith('hcl'))
      'ORACLE AMERICA'      → 'Oracle Corporation'  (oracle ✓, corporation filtered)
    """
    def _sig(s: str) -> list[str]:
        return [t for t in re.findall(r'\w+', (s or "").lower())
                if t not in _KG_SIG_STOP and len(t) > 1]

    q_toks = _sig(legal_name)
    e_toks = _sig(entity_name)

    if not q_toks or not e_toks:
        return True  # can't check — don't reject

    for et in e_toks:
        matched = any(
            et.startswith(qt) or qt.startswith(et) or fuzz_ratio(et, qt) >= 65
            for qt in q_toks
        )
        if not matched:
            return False

    return True


def _coverage_weighted_score(legal_name: str, entity_name: str | None) -> float:
    """Coverage-weighted match score between legal_name and entity_name.

    Score = best_raw x max(legal_coverage, entity_coverage), where:
      best_raw      = highest WRatio between any entity prefix and any legal prefix
      legal_cov     = tokens in winning legal prefix  / total legal sig tokens
      entity_cov    = tokens in winning entity prefix / total entity sig tokens

    This rewards matches where the entity explains a meaningful portion of the
    legal name, while protecting short distinctive brands (Amazon, Google) via
    entity_coverage — if the entire entity name is matched, coverage is 100%
    regardless of how long the legal name is.

    Examples:
      "AMAZON.COM SERVICES LLC" → "Amazon"                 100 (entity_cov=1/1)
      "COGNIZANT TECH SOLUTIONS US" → "Cognizant Tech"     100 (entity_cov=2/2)
      "SQUAD SOFTWARE INC" → "San Diego Padres"             ~20 → below threshold
    """
    def _sig(s: str) -> list[str]:
        return [t for t in re.findall(r'\w+', (s or "").lower())
                if t not in _KG_SIG_STOP and len(t) > 1]

    legal_toks  = _sig(legal_name)
    # Strip legal suffixes from KG entity name so "Oracle Corporation" → ["oracle"]
    entity_toks = _sig(strip_legal_suffixes(entity_name or "")) or _sig(entity_name)

    if not legal_toks or not entity_toks:
        return 0.0

    legal_pfx  = [" ".join(legal_toks[:i])  for i in range(1, len(legal_toks)  + 1)]
    entity_pfx = [" ".join(entity_toks[:i]) for i in range(1, len(entity_toks) + 1)]

    best = 0.0
    for ei, ep in enumerate(entity_pfx):
        m = fuzz_process.extractOne(ep, legal_pfx, scorer=WRatio,
                                    processor=fuzz_utils.default_process)
        if not m:
            continue
        raw, li = m[1], m[2]
        legal_cov  = (li + 1) / len(legal_toks)
        entity_cov = (ei + 1) / len(entity_toks)
        weighted   = raw * max(legal_cov, entity_cov)
        if weighted > best:
            best = weighted

    return best


def kg_search(legal_name: str) -> tuple[dict | None, list[dict]]:
    """Search KG API for legal_name with progressive word-stripping and best-match selection.

    Returns (selected, all_candidates) where:
      selected       — {name, url, kg_mid, _score} or None
      all_candidates — every /m/ entity seen across all attempts, deduplicated by
                       kg_mid (max score kept), sorted by score descending.
                       Used by the quality-events audit trail.

    Strategy:
    - Fetch _KG_FETCH_LIMIT candidates per query.  /g/ shells are always skipped.
    - Within each attempt, iterate candidates in KG relevance order.  Stop at the
      first /m/ candidate that scores ≥ _KG_MIN_OVERLAP (30) — KG's ranking is more
      reliable than cross-candidate score comparison.  Candidates below 30 are checked
      in order until one passes or the list is exhausted.
    - Score ≥ _KG_HIGH_CONFIDENCE (90) → return immediately, no need to retry.
    - 30 ≤ score < 90 → update global best if higher, then always continue to a
      shorter query (drop last word).  This lets a shorter query surface the correct
      brand entity — e.g. "Oracle America Inc" → OFS(68) at attempt 0, bare "Oracle"
      → Oracle Corporation(100) at attempt 2.
    - After _KG_MAX_RETRIES, return best_candidate if best_score ≥ _KG_MIN_OVERLAP,
      else None.
    """
    if not _KG_API_KEY:
        log.warning("KG_API_KEY not set — skipping KG search for %r", legal_name)
        return None, []

    if not can_call("kg_api"):
        if not within_rpm("kg_api"):
            log.info("KG API RPM limit hit — waiting 60s")
            time.sleep(60)
            if not can_call("kg_api"):
                log.warning("KG API still unavailable after wait — skipping")
                return None, []
        else:
            log.warning("KG API daily limit (100k) reached")
            return None, []

    base_query = strip_legal_suffixes(legal_name) or legal_name
    tokens     = base_query.split()
    best_candidate: dict | None = None
    best_score: int = -1
    all_seen:   dict[str, dict] = {}  # kg_mid → {name, kg_mid, score} — audit trail

    for attempt in range(min(_KG_MAX_RETRIES + 1, len(tokens))):
        query = " ".join(tokens[:len(tokens) - attempt])

        if attempt > 0 and not can_call("kg_api"):
            log.debug("KG API quota exhausted mid-retry for %r", legal_name)
            break

        try:
            resp = requests.get(
                _KG_ENDPOINT,
                params={
                    "query":     query,
                    "key":       _KG_API_KEY,
                    "types":     "Organization",
                    "limit":     _KG_FETCH_LIMIT,
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
                break

            resp.raise_for_status()
            items = resp.json().get("itemListElement", [])

            if not items:
                log.debug("KG API: no results for %r (attempt %d)", query, attempt)
                continue

            found_m = False
            for item in items:
                result  = item.get("result", {})
                raw_id  = result.get("@id", "")
                kg_mid  = raw_id.removeprefix("kg:") or None
                name    = html.unescape(result.get("name") or "") or None
                raw_url = result.get("url") or None
                url     = raw_url if raw_url and _is_public_url(raw_url) else None

                if kg_mid and kg_mid.startswith("/g/"):
                    log.debug("KG: /g/ thin shell %r (mid=%r) skipped", name, kg_mid)
                    continue

                # Gate 1: entity's leading token must plausibly match the legal name.
                # Rejects "San Diego Padres" for "SQUAD SOFTWARE", "Carnival Cruise Line"
                # for "Cruise LLC", etc.  Lead-gate failures are treated like /g/ shells —
                # found_m stays False so the retry loop tries a shorter query.
                if not _entity_lead_in_query(legal_name, name):
                    log.debug(
                        "KG: /m/ candidate %r (mid=%r) — lead mismatch vs %r, skipping",
                        name, kg_mid, legal_name,
                    )
                    continue

                found_m   = True
                # Gate 3: coverage-weighted score — rewards matches where the entity
                # explains a meaningful portion of the legal name, and protects
                # short distinctive brands via entity_coverage.
                score     = _coverage_weighted_score(legal_name, name)
                candidate = {"name": name, "url": url, "kg_mid": kg_mid, "_score": score}

                # Track every /m/ entity seen for the audit trail (dedup by kg_mid, max score)
                if kg_mid and (kg_mid not in all_seen or score > all_seen[kg_mid]["score"]):
                    all_seen[kg_mid] = {"name": name, "kg_mid": kg_mid, "score": score}

                if score >= _KG_HIGH_CONFIDENCE:
                    log.debug(
                        "KG hit: %r → name=%r url=%r mid=%r score=%d",
                        query, name, url, kg_mid, score,
                    )
                    all_candidates = sorted(all_seen.values(), key=lambda x: x["score"], reverse=True)
                    return candidate, all_candidates

                if score >= _KG_MIN_OVERLAP:
                    # First /m/ candidate above threshold — trust KG's relevance order,
                    # stop checking remaining candidates in this attempt.
                    log.debug(
                        "KG: /m/ candidate %r (mid=%r score=%d) — first above threshold%s",
                        name, kg_mid, score,
                        ", new best" if score > best_score else f", not better than best ({best_score})",
                    )
                    if score > best_score:
                        best_score     = score
                        best_candidate = candidate
                    break

                log.debug(
                    "KG: /m/ candidate %r (mid=%r score=%d) — below threshold, checking next",
                    name, kg_mid, score,
                )

            if not found_m:
                log.debug("KG: no usable /m/ results for %r (all /g/ or lead-rejected) — retrying without last word", query)
            else:
                log.debug(
                    "KG: best so far score=%d for %r — trying shorter query",
                    best_score, query,
                )

        except requests.exceptions.RequestException as e:
            log.debug("KG API error for %r: %s", legal_name, e)
            break

    all_candidates = sorted(all_seen.values(), key=lambda x: x["score"], reverse=True)

    if best_candidate and best_score >= _KG_MIN_OVERLAP:
        log.debug(
            "KG: retries exhausted for %r — best result: name=%r mid=%r score=%d",
            legal_name, best_candidate.get("name"), best_candidate.get("kg_mid"), best_score,
        )
        return best_candidate, all_candidates

    log.debug(
        "KG: no confident match for %r — best score=%d, returning None",
        legal_name, best_score,
    )
    return None, all_candidates


def upsert_quality_event(
    conn,
    fein:           str,
    legal_name:     str,
    event_type:     str,
    selected:       dict | None,
    all_candidates: list[dict],
    dry_run:        bool = False,
) -> None:
    """Write a KG quality event for human review.

    event_type: 'no_kg_match' | 'low_confidence'
    ON CONFLICT (fein): update only when the existing row is not yet resolved —
    resolved rows represent a human override and must not be clobbered.
    """
    if dry_run:
        log.info(
            "[DRY-RUN] quality event fein=%s type=%s selected=%r score=%s candidates=%d",
            fein, event_type,
            selected.get("name") if selected else None,
            selected.get("_score") if selected else None,
            len(all_candidates),
        )
        return
    conn.execute(
        """
        INSERT INTO h1b_ats_quality_events
            (fein, legal_name, event_type, kg_score, selected_name, selected_kg_mid, all_candidates)
        VALUES
            (%(fein)s, %(legal_name)s, %(event_type)s, %(kg_score)s,
             %(selected_name)s, %(selected_kg_mid)s, %(all_candidates)s::jsonb)
        ON CONFLICT (fein) DO UPDATE SET
            legal_name      = EXCLUDED.legal_name,
            event_type      = EXCLUDED.event_type,
            kg_score        = EXCLUDED.kg_score,
            selected_name   = EXCLUDED.selected_name,
            selected_kg_mid = EXCLUDED.selected_kg_mid,
            all_candidates  = EXCLUDED.all_candidates,
            created_at      = NOW()
        WHERE NOT h1b_ats_quality_events.resolved
        """,
        {
            "fein":            fein,
            "legal_name":      legal_name,
            "event_type":      event_type,
            "kg_score":        selected.get("_score") if selected else None,
            "selected_name":   selected.get("name")   if selected else None,
            "selected_kg_mid": selected.get("kg_mid") if selected else None,
            "all_candidates":  json.dumps(all_candidates),
        },
    )


# ─────────────────────────────────────────────────────────────────────────────
# Wikidata SPARQL — P646 (Freebase MID) → QID + P10311 (jobs URL)
# ─────────────────────────────────────────────────────────────────────────────

def _sparql_batch_p10311(mids: list[str]) -> dict[str, dict]:
    """
    Batch-fetch Wikidata QID + P10311 (official jobs URL) + P856 (official website)
    for a list of Freebase MIDs using a single SPARQL VALUES query.

    Returns {mid: {"qid": str|None, "jobs_url": str|None, "website": str|None}}.
    """
    if not mids:
        return {}

    _MID_RE = re.compile(r"^/m/[0-9a-z_]+$")
    mids = [m for m in mids if _MID_RE.match(m)]
    if not mids:
        return {}

    values = " ".join(f'("{m}")' for m in mids)
    sparql = (
        "SELECT ?mid ?item ?jobs_url ?website ?glassdoor_id ?crunchbase_id WHERE { "
        f"VALUES (?mid) {{ {values} }} "
        "?item wdt:P646 ?mid . "
        "OPTIONAL { ?item wdt:P10311 ?jobs_url } "
        "OPTIONAL { ?item wdt:P856 ?website } "
        "OPTIONAL { ?item wdt:P2267 ?glassdoor_id } "
        "OPTIONAL { ?item wdt:P2088 ?crunchbase_id } "
        "}"
    )
    headers = {**_API_HEADERS, "Accept": "application/sparql-results+json"}
    params  = {"query": sparql, "format": "json"}

    def _do_request():
        return requests.get(
            _WIKIDATA_SPARQL, params=params, headers=headers, timeout=60
        )

    _sparql_limiter.acquire("SPARQL P646+P10311+P856 batch")
    try:
        r = _do_request()
        if r.status_code == 429:
            wait = int(r.headers.get("Retry-After", _RATE_LIMIT_BACKOFF))
            log.debug("SPARQL rate-limited — waiting %ds", wait)
            time.sleep(wait)
            _sparql_limiter.acquire("SPARQL P646+P10311+P856 retry")
            r = _do_request()
        r.raise_for_status()
        bindings = r.json()["results"]["bindings"]
    except (requests.exceptions.RequestException, ValueError, KeyError) as e:
        log.debug("SPARQL P646+P10311+P856 batch error: %s", e)
        return {m: {"qid": None, "jobs_url": None, "website": None, "glassdoor_id": None, "crunchbase_id": None} for m in mids}

    out: dict[str, dict] = {m: {"qid": None, "jobs_url": None, "website": None, "glassdoor_id": None, "crunchbase_id": None} for m in mids}
    for row in bindings:
        mid           = row.get("mid", {}).get("value")
        item_uri      = row.get("item", {}).get("value", "")
        qid           = item_uri.split("/")[-1] if item_uri else None
        jobs_url      = row.get("jobs_url", {}).get("value") or None
        website       = row.get("website", {}).get("value") or None
        glassdoor_id  = row.get("glassdoor_id", {}).get("value") or None
        crunchbase_id = row.get("crunchbase_id", {}).get("value") or None
        if mid and mid in out:
            if qid:
                out[mid]["qid"] = qid
            if jobs_url and _is_public_url(jobs_url):
                out[mid]["jobs_url"] = jobs_url
            if website and _is_public_url(website):
                out[mid]["website"] = website
            if glassdoor_id:
                out[mid]["glassdoor_id"] = glassdoor_id
            if crunchbase_id:
                out[mid]["crunchbase_id"] = crunchbase_id

    return out


def _sparql_batch_p10311_all(mids: list[str]) -> dict[str, dict]:
    """Chunk mids into _SPARQL_CHUNK_SIZE batches and merge results."""
    out: dict[str, dict] = {}
    for i in range(0, len(mids), _SPARQL_CHUNK_SIZE):
        chunk = mids[i: i + _SPARQL_CHUNK_SIZE]
        log.info(
            "SPARQL P646+P10311+P856 batch %d–%d of %d …",
            i + 1, min(i + _SPARQL_CHUNK_SIZE, len(mids)), len(mids),
        )
        out.update(_sparql_batch_p10311(chunk))
    return out


# ─────────────────────────────────────────────────────────────────────────────
# Career URL disambiguation — Gemini (default) or local Qwen3-8B
# ─────────────────────────────────────────────────────────────────────────────

# ── Local (Qwen3-8B via llama_cpp) ────────────────────────────────────────────

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


# ── Gemini backend ─────────────────────────────────────────────────────────────

_gemini_client = None


def _get_gemini_client():
    global _gemini_client
    if _gemini_client is None:
        from google import genai
        api_key = os.environ.get("GEMINI_API_KEY_USER_1") or os.environ.get("GOOGLE_API_KEY")
        if not api_key:
            raise RuntimeError(
                "GEMINI_API_KEY_USER_1 (or GOOGLE_API_KEY) not set — required when DISCOVER_ATS_LLM_PROVIDER=gemini"
            )
        _gemini_client = genai.Client(api_key=api_key)
        log.info("Gemini provider initialised — model=%s", DISCOVER_ATS_GEMINI_MODEL)
    return _gemini_client


def _gemini_pick_career_url(
    candidates: list[str],
    company_name: str,
    website_url: str | None,
) -> str | None:
    if not candidates:
        return None
    numbered = "\n".join(f"{i+1}. {u}" for i, u in enumerate(candidates))
    website_hint = f" (official website: {website_url})" if website_url else ""
    prompt = (
        f"Company: {company_name}{website_hint}\n\n"
        f"Candidate career page URLs:\n{numbered}\n\n"
        "Which number is the official career page for this company? "
        "Reply with ONLY the number."
    )
    while not can_call(DISCOVER_ATS_GEMINI_MODEL, use_case="ats_disambig"):
        if within_rpm(DISCOVER_ATS_GEMINI_MODEL):
            # RPM is fine → daily quota exhausted; spinning won't help
            log.debug("ats_disambig: daily quota exhausted — skipping disambiguation")
            return None
        log.debug("ats_disambig: RPM limit reached — sleeping 5s")
        time.sleep(5)

    estimated = len(prompt) // 4 + 50
    wait_s = tpm_wait_seconds(DISCOVER_ATS_GEMINI_MODEL, estimated_tokens=estimated)
    if wait_s > 0:
        time.sleep(wait_s)

    try:
        from google.genai import types
        response = _get_gemini_client().models.generate_content(
            model=DISCOVER_ATS_GEMINI_MODEL,
            contents=prompt,
            config=types.GenerateContentConfig(temperature=0),
        )
        tokens = getattr(response.usage_metadata, "total_token_count", 0) or 0
        increment_usage(DISCOVER_ATS_GEMINI_MODEL, use_case="ats_disambig")
        record_tpm(DISCOVER_ATS_GEMINI_MODEL, tokens or estimated)
        text = (response.text or "").strip()
        m = re.search(r'\d+', text)
        if not m:
            log.debug("Gemini returned non-numeric response: %r", text)
            return None
        idx = int(m.group()) - 1
        if 0 <= idx < len(candidates):
            log.debug("Gemini picked candidate %d: %s", idx + 1, candidates[idx])
            return candidates[idx]
    except Exception as e:
        log.debug("Gemini career URL pick failed: %s", e)
    return None


# ── Public interface ───────────────────────────────────────────────────────────

def _pick_career_url(
    candidates: list[str],
    company_name: str,
    website_url: str | None,
) -> str | None:
    """Pick the best career URL from candidates using the configured LLM provider."""
    if DISCOVER_ATS_LLM_PROVIDER == "gemini":
        return _gemini_pick_career_url(candidates, company_name, website_url)
    # local path — lazy-load Qwen3 on first call
    _load_llm()
    return _qwen_pick_career_url(candidates, company_name, website_url)


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


_AGGREGATOR_DOMAINS = frozenset({
    "linkedin.com", "indeed.com", "glassdoor.com", "ziprecruiter.com",
    "monster.com", "careerbuilder.com", "simplyhired.com", "dice.com",
    "hired.com", "wellfound.com", "builtin.com",
})


def _is_plausible_career_url(url: str, company_tokens: set[str]) -> bool:
    """
    True if the URL is plausibly the company's own career page:
      a) company name token appears in the domain
      b) URL is on a known ATS domain
      c) 'career' or 'jobs' appears in domain or path
    Rejects job aggregators that would match (c) for any employer.
    """
    root = _root_domain(url)
    if root in _AGGREGATOR_DOMAINS:
        return False
    parsed = urlparse(url)
    domain = (parsed.hostname or "").lower()
    path   = parsed.path.lower()
    return (
        any(tok in domain for tok in company_tokens)
        or root in _KNOWN_ATS_DOMAINS
        or any(kw in domain + path for kw in ("career", "jobs"))
    )


def _brave_load_quota() -> dict:
    current_month = datetime.now().strftime("%Y-%m")
    try:
        with open(_BRAVE_QUOTA_FILE) as f:
            data = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {"calls": 0, "month": current_month}
    if data.get("month") != current_month:
        return {"calls": 0, "month": current_month}
    return data


def _brave_save_quota(data: dict) -> None:
    data.setdefault("month", datetime.now().strftime("%Y-%m"))
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

        # Multiple candidates — ask LLM to pick
        chosen = _pick_career_url(candidates[:3], company_name, website_url)
        if chosen:
            return chosen

        # LLM unavailable — return first plausible result
        log.debug("Brave: LLM unavailable, using first candidate: %s", candidates[0])
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
        result = _fetch_via_worker(url)
        if result:
            return result
    return None, url


def _fetch_via_worker(url: str) -> tuple[str, str] | None:
    """Proxy a URL fetch through the Cloudflare probe Worker.

    Used as fallback when the direct fetch times out or is connection-refused
    (OCI datacenter IP blocked). Returns (html_text, final_url) or None.
    """
    if not CF_WORKER_URL or not CF_WORKER_SECRET:
        return None
    try:
        resp = requests.post(
            CF_WORKER_URL,
            json={"url": url, "max_bytes": 65536},
            headers={"Authorization": f"Bearer {CF_WORKER_SECRET}"},
            timeout=30,
        )
        data = resp.json()
        if data.get("error") or (data.get("status") or 0) >= 400:
            log.debug("CF Worker: %s → error=%s status=%s", url, data.get("error"), data.get("status"))
            return None
        final_url = data.get("final_url") or url
        body      = data.get("body") or ""
        log.debug("CF Worker: %s → %s (status=%s)", url, final_url, data.get("status"))
        return body, final_url
    except Exception as exc:
        log.debug("CF Worker request failed for %s: %s", url, exc)
        return None


def _find_ats_in_html(html: str) -> tuple[str | None, str | None]:
    """Scan HTML for embedded ATS URLs. Returns (platform, slug) or (None, None)."""
    from jobs.ats.patterns import match_ats_pattern
    for u in _URL_RE.findall(html):
        result = match_ats_pattern(u)
        if result:
            return result["platform"], result["slug"]
    return None, None


def _is_homepage(final_url: str, company_host: str = "") -> bool:
    parsed = urlparse(final_url)
    path   = parsed.path.rstrip("/")
    if path:
        return False
    # Root path is only a homepage when the host matches the company website.
    # A careers subdomain (careers.amazon.com/) has a different host and is a
    # legitimate careers page, not a homepage redirect.
    final_host = (parsed.hostname or "").removeprefix("www.")
    return not company_host or final_host == company_host


def _is_auth_redirect(final_url: str) -> bool:
    parsed = urlparse(final_url)
    path_query = parsed.path + ("?" + parsed.query if parsed.query else "")
    return bool(_AUTH_RE.search(path_query))


_CDN_DOMAINS = frozenset({
    "cloudfront.net", "fastly.net", "akamai.net", "akamaized.net",
    "edgecastcdn.net", "stackpathcdn.com", "azureedge.net",
    "amazonaws.com", "azurewebsites.net", "firebaseapp.com",
})


def _resolve_website_redirect(url: str) -> str:
    """
    Fetch the company root URL and follow redirects to detect rebrands/domain changes.

    Cases:
      - Redirect fails / times out         → return original unchanged
      - Redirect → CDN or generic host     → return original (don't trust it)
      - Redirect → same root domain        → return resolved (http→https, www→naked are fine)
      - Redirect → different root domain   → return resolved (genuine rebrand)
    """
    if not _is_public_url(url):
        return url
    parsed   = urlparse(url)
    root_url = f"{parsed.scheme}://{parsed.netloc}/"
    final_url = None

    try:
        r = requests.get(root_url, timeout=8, allow_redirects=True,
                         headers=_API_HEADERS)
        final_url = r.url.rstrip("/")
    except Exception as exc:
        log.debug("_resolve_website_redirect: fetch failed for %s: %s", url, exc)
        result = _fetch_via_worker(root_url)
        if result:
            _, worker_final = result
            final_url = worker_final.rstrip("/")
            log.debug("_resolve_website_redirect: CF Worker resolved %s → %s", url, final_url)

    if final_url is None:
        return url

    final_root = _root_domain(final_url)
    orig_root  = _root_domain(url)

    if not final_root or final_root in _CDN_DOMAINS:
        log.debug("_resolve_website_redirect: CDN/generic redirect (%s) — keeping original", final_root)
        return url

    final_parsed = urlparse(final_url)
    # Strip redundant default ports (:443 on https, :80 on http)
    host = final_parsed.hostname or ""
    port = final_parsed.port
    if port and not (
        (final_parsed.scheme == "https" and port == 443) or
        (final_parsed.scheme == "http"  and port == 80)
    ):
        host = f"{host}:{port}"
    resolved_base = f"{final_parsed.scheme}://{host}"

    if final_root == orig_root:
        log.debug("_resolve_website_redirect: same root domain (%s→%s), using resolved base", url, resolved_base)
    else:
        log.info("_resolve_website_redirect: domain changed %s → %s", orig_root, final_root)

    return resolved_base


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
    company_host  = (parsed.hostname or "").removeprefix("www.")

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
        if _is_homepage(final_url, company_host):
            log.debug("  %s → homepage redirect, skipping", url)
            continue

        # Reject SSO/auth walls
        if _is_auth_redirect(final_url):
            log.debug("  %s → auth redirect (%s), skipping", url, final_url)
            continue

        final_root = _root_domain(final_url)

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
            d.poc_email_domain,
            fdm.assigned_domain,
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
        LEFT JOIN h1b_ats_discovery h ON h.employer_fein = d.employer_fein
        LEFT JOIN fein_domain_map fdm ON fdm.employer_fein = d.employer_fein
        WHERE (
            h.employer_fein IS NULL
            OR h.last_checked IS NULL
            OR h.last_checked < NOW() - INTERVAL '7 days'
        )
        AND (
            fdm.assigned_domain IS NULL
            OR (
                fdm.assigned_domain NOT IN (
                    SELECT domain FROM prospective_companies
                    WHERE domain IS NOT NULL
                      AND ats_platform IS NOT NULL
                      AND ats_platform NOT IN ('unknown', 'unsupported')
                )
                AND fdm.assigned_domain NOT IN (
                    SELECT domain FROM company_ats
                    WHERE is_monitored = TRUE
                )
            )
        )
        GROUP BY d.employer_fein, d.employer_name, d.poc_email_domain,
                 fdm.assigned_domain, d.total_certified
        ORDER BY total_approvals DESC NULLS LAST
        LIMIT %s
    """, (limit,))
    return [dict(r) for r in cur.fetchall()]


def load_by_fein(fein: str, conn) -> dict | None:
    cur = conn.cursor()
    cur.execute("""
        SELECT d.employer_fein, d.employer_name, d.poc_email_domain,
               fdm.assigned_domain
        FROM dol_h1b_employers d
        LEFT JOIN fein_domain_map fdm ON fdm.employer_fein = d.employer_fein
        WHERE d.employer_fein = %s
    """, (fein,))
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
             careers_url, detected_platform, detected_slug,
             glassdoor_id, crunchbase_id, last_checked)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
        ON CONFLICT (employer_fein) DO UPDATE SET
            employer_name     = EXCLUDED.employer_name,
            canonical_name    = EXCLUDED.canonical_name,
            canonical_source  = EXCLUDED.canonical_source,
            wikidata_qid      = COALESCE(EXCLUDED.wikidata_qid,    h1b_ats_discovery.wikidata_qid),
            kg_mid            = COALESCE(EXCLUDED.kg_mid,           h1b_ats_discovery.kg_mid),
            website_url       = COALESCE(EXCLUDED.website_url,      h1b_ats_discovery.website_url),
            jobs_url          = COALESCE(EXCLUDED.jobs_url,         h1b_ats_discovery.jobs_url),
            careers_url       = COALESCE(EXCLUDED.careers_url,      h1b_ats_discovery.careers_url),
            detected_platform = COALESCE(EXCLUDED.detected_platform, h1b_ats_discovery.detected_platform),
            detected_slug     = COALESCE(EXCLUDED.detected_slug,    h1b_ats_discovery.detected_slug),
            glassdoor_id      = COALESCE(EXCLUDED.glassdoor_id,     h1b_ats_discovery.glassdoor_id),
            crunchbase_id     = COALESCE(EXCLUDED.crunchbase_id,    h1b_ats_discovery.crunchbase_id),
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
        data.get("glassdoor_id"),
        data.get("crunchbase_id"),
    ))
    conn.commit()


def _upsert_company_ats(
    conn,
    fein: "str | None",
    domain: str,
    company_name: str,
    platform: str,
    slug: str,
    priority: int = 0,
) -> None:
    """
    Write a confirmed ATS detection to company_ats for manual review.

    is_monitored stays FALSE — a human must flip it before job monitor picks it up.
    ON CONFLICT (domain, platform): update slug + priority but never touch is_monitored
    or status, so a previously reviewed entry is not reset.

    Skips the write if this domain+platform is already actively monitored in
    prospective_companies (ats_platform not null/unknown/unsupported) or in
    company_ats (is_monitored=TRUE) — prevents duplicate monitoring.
    """
    cur = conn.cursor()

    cur.execute("""
        SELECT 1 FROM prospective_companies
        WHERE domain = %s
          AND ats_platform = %s
          AND ats_platform NOT IN ('unknown', 'unsupported')
        LIMIT 1
    """, (domain, platform))
    if cur.fetchone():
        log.debug(
            "_upsert_company_ats: %s/%s already in prospective_companies — skipping",
            domain, platform,
        )
        return

    cur.execute("""
        SELECT 1 FROM company_ats
        WHERE domain = %s AND platform = %s AND is_monitored = TRUE
        LIMIT 1
    """, (domain, platform))
    if cur.fetchone():
        log.debug(
            "_upsert_company_ats: %s/%s already monitored in company_ats — skipping",
            domain, platform,
        )
        return

    cur.execute("""
        INSERT INTO company_ats
            (employer_fein, domain, company_name, platform, slug, source, priority)
        VALUES (%s, %s, %s, %s, %s, 'career_detector', %s)
        ON CONFLICT (domain, platform) DO UPDATE SET
            employer_fein = COALESCE(company_ats.employer_fein, EXCLUDED.employer_fein),
            company_name  = COALESCE(company_ats.company_name,  EXCLUDED.company_name),
            slug          = CASE
                                WHEN company_ats.reviewed_at IS NOT NULL THEN company_ats.slug
                                ELSE EXCLUDED.slug
                            END,
            priority      = GREATEST(company_ats.priority, EXCLUDED.priority)
    """, (fein, domain, company_name, platform, slug, priority))
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
    skip_brave: bool = True,
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
        glassdoor_id     = prefetched.get("glassdoor_id")
        crunchbase_id    = prefetched.get("crunchbase_id")
    else:
        # Single-employer mode: inline KG + SPARQL calls
        kg_mid           = None
        wikidata_qid     = None
        jobs_url         = None
        glassdoor_id     = None
        crunchbase_id    = None

        existing_row = get_discovery_row(fein, conn)
        cached_mid   = existing_row.get("kg_mid") if existing_row else None

        assigned_domain = emp.get("assigned_domain")
        all_candidates  = []
        kg_url          = None

        if cached_mid and not force:
            log.info("  KG MID cached: %s", cached_mid)
            kg_mid           = cached_mid
            canonical_name   = existing_row.get("canonical_name")
            canonical_source = existing_row.get("canonical_source")
        else:
            log.info("  KG API …")
            kg, all_candidates = kg_search(name)
            if kg:
                kg_mid           = kg.get("kg_mid")
                canonical_name   = kg.get("name")
                kg_url           = kg.get("url")
                canonical_source = "kg_api" if canonical_name else None
                if (kg.get("_score") or 0) < _KG_QUALITY_THRESHOLD:
                    upsert_quality_event(conn, fein, name, "low_confidence", kg, all_candidates, dry_run)
            else:
                kg_mid         = None
                canonical_name = strip_legal_suffixes(name) or None
                canonical_source = "regex" if canonical_name else None
                kg_url         = None
                upsert_quality_event(conn, fein, name, "no_kg_match", None, all_candidates, dry_run)

        if kg_mid:
            log.info("  SPARQL P646+P10311+P856 for MID %s …", kg_mid)
            sparql_res    = _sparql_batch_p10311([kg_mid])
            entry         = sparql_res.get(kg_mid, {})
            sparql_p856   = entry.get("website") or None
            sparql_jobs   = entry.get("jobs_url") or None
            sparql_qid    = entry.get("qid")
            sparql_gd     = entry.get("glassdoor_id")
            sparql_cb     = entry.get("crunchbase_id")

            # Domain gate — verify KG entity against LCA email-derived domain.
            # kg_url is primary verification source; sparql_p856 is fallback.
            # No verifiable URL OR domain mismatch → discard entire KG entry.
            if assigned_domain and not _kg_domain_gate(kg_url, sparql_p856, assigned_domain):
                log.warning(
                    "  KG domain mismatch: kg_url=%r p856=%r assigned=%r — discarding KG entry",
                    kg_url, sparql_p856, assigned_domain,
                )
                upsert_quality_event(conn, fein, name, "kg_domain_mismatch",
                                     {"name": canonical_name, "kg_mid": kg_mid, "_score": 0},
                                     all_candidates if not (cached_mid and not force) else [],
                                     dry_run)
                kg_mid = wikidata_qid = jobs_url = glassdoor_id = crunchbase_id = None
                canonical_name   = strip_legal_suffixes(name) or None
                canonical_source = "regex" if canonical_name else None
            else:
                wikidata_qid  = sparql_qid
                jobs_url      = sparql_jobs
                glassdoor_id  = sparql_gd
                crunchbase_id = sparql_cb
        else:
            jobs_url = glassdoor_id = crunchbase_id = wikidata_qid = None

        # website_url: always from assigned_domain (LCA email-first).
        # Fall back to poc_email_domain only if fein_domain_map not yet populated.
        if assigned_domain:
            website_url = "https://" + assigned_domain
        elif emp.get("poc_email_domain"):
            website_url = "https://" + emp["poc_email_domain"]
            log.debug("  poc_email_domain fallback (fein_domain_map not yet populated): %s", website_url)
        else:
            website_url = None

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
        from jobs.ats.patterns import match_ats_pattern as _map
        _hit = _map(jobs_url)
        if _hit:
            detected_platform = _hit["platform"]
            detected_slug     = _hit.get("slug")
    elif website_url:
        # Phase 3: 19-pattern probe
        website_url = _resolve_website_redirect(website_url)
        log.info("  Probing 19 career URL patterns on %s …", website_url)
        try:
            careers_url, detected_platform, detected_slug = discover_careers_url(
                website_url
            )
        except Exception as e:
            log.warning("  Career probe failed: %s", e)

        # Phase 4: Brave search fallback (skipped in batch/KG-only mode)
        if not careers_url and not skip_brave:
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

    # Phase 6: career_page.py — 3-layer deep scan (redirect + full HTML + job links)
    # Runs when platform still unknown, whether jobs_url or website_url was found.
    if not detected_platform and website_url:
        _cp_domain = _root_domain(website_url)
        _cp_name   = canonical_name or name
        log.info("  Phase 6: career_page scan on domain=%s …", _cp_domain)
        try:
            from jobs.career_page import detect_via_career_page
            _cp_result = detect_via_career_page(_cp_name, _cp_domain)
            if _cp_result:
                detected_platform = _cp_result["platform"]
                detected_slug     = _cp_result.get("slug")
                log.info("  Phase 6 HIT: %s / %s", detected_platform, detected_slug)
        except Exception as e:
            log.warning("  Phase 6 (career_page) failed: %s", e)

    # Phase 7: career_detector.py — Chrome-impersonation BFS, last resort
    if not detected_platform and website_url:
        _cd_domain = _root_domain(website_url)
        log.info("  Phase 7: career_detector BFS on domain=%s …", _cd_domain)
        try:
            from jobs.ats.career_detector import detect_company
            _cd_results = detect_company(_cd_domain)
            if _cd_results:
                _best = _cd_results[0]
                detected_platform = _best["platform"]
                detected_slug     = _best.get("slug")
                if not careers_url:
                    careers_url = _best.get("source_url")
                log.info("  Phase 7 HIT: %s / %s", detected_platform, detected_slug)
        except Exception as e:
            log.warning("  Phase 7 (career_detector) failed: %s", e)

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
        "glassdoor_id":     glassdoor_id,
        "crunchbase_id":    crunchbase_id,
    }

    upsert_discovery(result, conn, dry_run=dry_run)

    if not dry_run and detected_platform and detected_slug and result.get("website_url"):
        domain = _root_domain(result["website_url"])
        if domain:
            _upsert_company_ats(
                conn,
                fein=fein,
                domain=domain,
                company_name=canonical_name or name,
                platform=detected_platform,
                slug=detected_slug,
                priority=int(emp.get("total_approvals") or 0),
            )
            log.info("  → company_ats upserted: %s / %s / %s", domain, detected_platform, detected_slug)

    return result


# ─────────────────────────────────────────────────────────────────────────────
# Brave pass — separate monthly sweep for companies with website but no careers
# ─────────────────────────────────────────────────────────────────────────────

def _load_brave_candidates(limit: int, conn) -> list[dict]:
    """Companies enriched by KG+probe but still missing a careers URL."""
    cur = conn.cursor()
    cur.execute("""
        SELECT h.employer_fein, h.employer_name, h.website_url, h.canonical_name,
               COALESCE(d.total_certified, 0) AS total_approvals
        FROM h1b_ats_discovery h
        LEFT JOIN dol_h1b_employers d ON d.employer_fein = h.employer_fein
        WHERE h.last_checked IS NOT NULL
          AND h.brave_checked_at IS NULL
          AND h.careers_url IS NULL
          AND h.website_url IS NOT NULL
        ORDER BY h.last_checked ASC
        LIMIT %s
    """, (limit,))
    return [dict(r) for r in cur.fetchall()]


def _brave_upsert(fein: str, careers_url: "str | None",
                  platform: "str | None", slug: "str | None", conn) -> None:
    """Mark brave_checked_at and persist any career URL found."""
    conn.cursor().execute("""
        UPDATE h1b_ats_discovery
        SET brave_checked_at  = NOW(),
            careers_url       = COALESCE(%s, careers_url),
            detected_platform = COALESCE(%s, detected_platform),
            detected_slug     = COALESCE(%s, detected_slug)
        WHERE employer_fein = %s
    """, (careers_url, platform, slug, fein))
    conn.commit()


def _run_brave_pass(conn, r, args) -> None:
    """--brave-pass: run Brave search on KG-enriched companies with no careers URL."""
    from jobs.ats.patterns import match_ats_pattern as _map

    candidates = _load_brave_candidates(args.top, conn)
    log.info("Brave pass: %d candidates (website known, careers missing)", len(candidates))

    for i, row in enumerate(candidates, 1):
        while _is_maintenance(r):
            log.info("Maintenance window active — pausing for 30s")
            time.sleep(30)

        fein         = row["employer_fein"]
        name         = row["employer_name"]
        website_url  = row["website_url"]
        search_name  = row.get("canonical_name") or strip_legal_suffixes(name) or name

        log.info("[%d/%d brave] %s  %s", i, len(candidates), fein, name)

        careers_url = platform = slug = None

        if not args.dry_run:
            # Check quota before calling — exhaustion must not stamp brave_checked_at
            if _brave_load_quota().get("calls", 0) >= _BRAVE_QUOTA_LIMIT:
                log.warning("Brave monthly quota exhausted — stopping brave pass at %d/%d",
                            i - 1, len(candidates))
                break

            brave_url = brave_career_search(search_name, website_url=website_url)
            if brave_url:
                careers_url = brave_url
                hit = _map(brave_url)
                if hit:
                    platform = hit["platform"]
                    slug     = hit.get("slug")
                else:
                    try:
                        html_content, _ = _fetch_html(brave_url)
                        if html_content:
                            platform, slug = _find_ats_in_html(html_content)
                    except Exception as e:
                        log.warning("  HTML fingerprint failed: %s", e)
                log.info("  Brave → %s  platform=%s", careers_url, platform)
            else:
                log.info("  Brave found nothing — marking as attempted")

            _brave_upsert(fein, careers_url, platform, slug, conn)
            if platform and slug and website_url:
                domain = _root_domain(website_url)
                if domain:
                    _upsert_company_ats(
                        conn,
                        fein=fein,
                        domain=domain,
                        company_name=row.get("canonical_name") or name,
                        platform=platform,
                        slug=slug,
                        priority=int(row.get("total_approvals") or 0),
                    )
                    log.info("  → company_ats upserted: %s / %s / %s", domain, platform, slug)
        else:
            log.info("  [DRY-RUN] would Brave-search %r on %s", search_name, website_url)

        time.sleep(0.2)

    log.info("Brave pass done. %d candidates processed.", len(candidates))


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
    parser.add_argument("--brave-pass", action="store_true",
                        help="Run Brave search only on companies already enriched by KG+probe")
    args = parser.parse_args()

    try:
        r = get_redis()
    except Exception:
        r = None

    if r and not args.dry_run:
        r.set(REDIS_GEMINI_LOCK, "1", ex=3600)
        log.info("Gemini lock set (TTL=1h, renews per employer) — email_processor will pause during this run")
    conn = None
    try:
        init_db()
        conn = get_conn()

        if args.brave_pass:
            _run_brave_pass(conn, r, args)
            return

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
                if r and not args.dry_run:
                    r.expire(REDIS_GEMINI_LOCK, 3600)
                while _is_maintenance(r):
                    log.info("Maintenance window active — pausing for 30s")
                    time.sleep(30)
                log.info("[%d/%d]", i, len(employers))
                result = process_employer(
                    emp, conn, dry_run=args.dry_run, force=args.force,
                    skip_brave=False,
                )
                _tally(stats, result, args.force)
        else:
            # ── Phase 1: KG API for all employers → collect kg_mids ──────────────
            log.info("Phase 1: KG API for %d employers …", len(employers))
            kg_map: dict[str, dict] = {}   # fein → {kg_mid, canonical_name, website_url, ...}
            all_mids: list[str]     = []
            seen_mids: set[str]     = set()

            for i, emp in enumerate(employers, 1):
                if r and not args.dry_run:
                    r.expire(REDIS_GEMINI_LOCK, 3600)
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
                        "kg_url":          existing.get("website_url"),
                        "wikidata_qid":    existing.get("wikidata_qid"),
                        "_all_candidates": [],
                    }
                else:
                    log.info("[%d/%d KG] %s", i, len(employers), name)
                    kg, all_candidates = kg_search(name)
                    if kg:
                        entry = {
                            "kg_mid":          kg.get("kg_mid"),
                            "canonical_name":  kg.get("name"),
                            "canonical_source": "kg_api" if kg.get("name") else None,
                            "kg_url":          kg.get("url"),
                            "wikidata_qid":    None,
                            "_all_candidates": all_candidates,
                        }
                        if (kg.get("_score") or 0) < _KG_QUALITY_THRESHOLD:
                            upsert_quality_event(conn, fein, name, "low_confidence", kg, all_candidates, args.dry_run)
                    else:
                        stripped = strip_legal_suffixes(name)
                        entry = {
                            "kg_mid":          None,
                            "canonical_name":  stripped or None,
                            "canonical_source": "regex" if stripped else None,
                            "kg_url":          None,
                            "wikidata_qid":    None,
                            "_all_candidates": all_candidates,
                        }
                        upsert_quality_event(conn, fein, name, "no_kg_match", None, all_candidates, args.dry_run)

                kg_map[fein] = entry
                mid = entry.get("kg_mid")
                if mid and mid not in seen_mids:
                    seen_mids.add(mid)
                    all_mids.append(mid)

            # ── Phase 2: SPARQL P646+P10311 batch for all MIDs ───────────────────
            log.info("Phase 2: SPARQL P10311 batch for %d MIDs …", len(all_mids))
            sparql_map = _sparql_batch_p10311_all(all_mids)   # {mid: {qid, jobs_url}}

            for fein, entry in kg_map.items():
                if entry.get("skip"):
                    continue
                mid = entry.get("kg_mid")
                emp_row = next((e for e in employers if e["employer_fein"] == fein), {})
                assigned_domain = emp_row.get("assigned_domain")

                if mid:
                    sp = sparql_map.get(mid, {})
                    sparql_p856   = sp.get("website") or None

                    # Domain gate — verify KG entity before accepting any of its data.
                    if assigned_domain and not _kg_domain_gate(entry.get("kg_url"), sparql_p856, assigned_domain):
                        log.warning(
                            "  [%s] KG domain mismatch: kg_url=%r p856=%r assigned=%r — discarding",
                            fein, entry.get("kg_url"), sparql_p856, assigned_domain,
                        )
                        upsert_quality_event(
                            conn, fein, emp_row.get("employer_name", ""),
                            "kg_domain_mismatch",
                            {"name": entry.get("canonical_name"), "kg_mid": mid, "_score": 0},
                            entry.get("_all_candidates", []),
                            args.dry_run,
                        )
                        stripped = strip_legal_suffixes(emp_row.get("employer_name", ""))
                        entry["kg_mid"]          = None
                        entry["canonical_name"]  = stripped or None
                        entry["canonical_source"] = "regex" if stripped else None
                        entry["wikidata_qid"]    = None
                        entry["jobs_url"]        = None
                        entry["glassdoor_id"]    = None
                        entry["crunchbase_id"]   = None
                    else:
                        entry["wikidata_qid"]  = sp.get("qid")
                        entry["jobs_url"]      = sp.get("jobs_url")
                        entry["glassdoor_id"]  = sp.get("glassdoor_id")
                        entry["crunchbase_id"] = sp.get("crunchbase_id")
                else:
                    entry["jobs_url"] = entry["glassdoor_id"] = entry["crunchbase_id"] = None

                # website_url: always from assigned_domain (LCA email-first).
                if assigned_domain:
                    entry["website_url"] = "https://" + assigned_domain
                elif emp_row.get("poc_email_domain"):
                    entry["website_url"] = "https://" + emp_row["poc_email_domain"]
                else:
                    entry["website_url"] = None

            # ── Phase 3: career probe + upsert ───────────────────────────────────
            log.info("Phase 3: career probe for %d employers …", len(employers))
            for i, emp in enumerate(employers, 1):
                if r and not args.dry_run:
                    r.expire(REDIS_GEMINI_LOCK, 3600)
                fein  = emp["employer_fein"]
                entry = kg_map.get(fein, {})
                if entry.get("skip"):
                    continue
                while _is_maintenance(r):
                    log.info("Maintenance window active — pausing for 30s")
                    time.sleep(30)
                log.info("[%d/%d career] %s  %s", i, len(employers), fein, emp["employer_name"])
                result = process_employer(
                    emp, conn,
                    dry_run=args.dry_run,
                    force=args.force,
                    prefetched=entry,
                )
                _tally(stats, result, args.force)
                time.sleep(0.2)

        log.info(
            "Done. processed=%d skipped=%d with_website=%d with_jobs_url=%d with_ats=%d",
            stats["processed"], stats["skipped"],
            stats["with_website"], stats["with_jobs_url"], stats["with_ats"],
        )
    finally:
        if conn:
            conn.close()
        if r and not args.dry_run:
            try:
                r.delete(REDIS_GEMINI_LOCK)
                log.info("Gemini lock cleared — email_processor resuming")
            except Exception as exc:
                log.warning("Failed to clear Gemini lock: %s", exc)


if __name__ == "__main__":
    main()
