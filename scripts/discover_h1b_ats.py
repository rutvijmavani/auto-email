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
import os
import re
import sys
import time
from urllib.parse import urlparse

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import requests

from db.connection import get_conn
from db.schema import init_db
from logger import get_logger, init_logging

log = get_logger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# Config
# ─────────────────────────────────────────────────────────────────────────────

_WIKIDATA_SEARCH = "https://www.wikidata.org/w/api.php"
_WIKIDATA_ENTITY = "https://www.wikidata.org/w/api.php"
_WIKIPEDIA_API   = "https://en.wikipedia.org/w/api.php"

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

_HTTP_TIMEOUT   = 12
_RECHECK_DAYS   = 7

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

def _wikidata_search(query: str) -> list[dict]:
    """Return top Wikidata entity search results for query."""
    try:
        r = requests.get(
            _WIKIDATA_SEARCH,
            params={
                "action":   "wbsearchentities",
                "search":   query,
                "language": "en",
                "type":     "item",
                "format":   "json",
                "limit":    5,
            },
            headers={"User-Agent": "H1B-ATS-Discover/1.0 (research bot)"},
            timeout=_HTTP_TIMEOUT,
        )
        r.raise_for_status()
        return r.json().get("search", [])
    except Exception as e:
        log.debug("Wikidata search error for %r: %s", query, e)
        return []


def _wikidata_website(qid: str) -> str | None:
    """Fetch official website (P856) for a Wikidata entity."""
    try:
        r = requests.get(
            _WIKIDATA_ENTITY,
            params={
                "action":    "wbgetentities",
                "ids":       qid,
                "props":     "claims",
                "languages": "en",
                "format":    "json",
            },
            headers={"User-Agent": "H1B-ATS-Discover/1.0 (research bot)"},
            timeout=_HTTP_TIMEOUT,
        )
        r.raise_for_status()
        entity = r.json().get("entities", {}).get(qid, {})
        claims = entity.get("claims", {})
        p856   = claims.get("P856", [])
        if p856:
            snak = p856[0].get("mainsnak", {})
            url  = snak.get("datavalue", {}).get("value")
            if url and url.startswith("http"):
                return url.rstrip("/")
    except Exception as e:
        log.debug("Wikidata entity fetch error for %s: %s", qid, e)
    return None


def enrich_wikidata(legal_name: str) -> dict:
    """
    Search Wikidata for legal_name.
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
        canonical_name = hit.get("label")
        website_url    = _wikidata_website(qid) if qid else None
        if canonical_name or website_url:
            return {
                "canonical_name": canonical_name,
                "website_url":    website_url,
                "qid":            qid,
                "source":         "wikidata",
            }
        time.sleep(0.2)

    return {"canonical_name": None, "website_url": None, "qid": None, "source": None}


# ─────────────────────────────────────────────────────────────────────────────
# Wikipedia fallback
# ─────────────────────────────────────────────────────────────────────────────

def enrich_wikipedia(legal_name: str) -> str | None:
    """
    Search Wikipedia for legal_name; return article title as canonical name.
    Only used when Wikidata returns no canonical name.
    """
    stripped = strip_legal_suffixes(legal_name)
    for query in [legal_name, stripped]:
        try:
            r = requests.get(
                _WIKIPEDIA_API,
                params={
                    "action":       "query",
                    "list":         "search",
                    "srsearch":     query,
                    "srlimit":      1,
                    "srprop":       "snippet",
                    "format":       "json",
                },
                headers={"User-Agent": "H1B-ATS-Discover/1.0 (research bot)"},
                timeout=_HTTP_TIMEOUT,
            )
            r.raise_for_status()
            hits = r.json().get("query", {}).get("search", [])
            if hits:
                return hits[0].get("title")
        except Exception as e:
            log.debug("Wikipedia search error for %r: %s", query, e)
        time.sleep(0.2)
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
    Returns (None, url) on failure.
    """
    try:
        r = requests.get(url, headers=_HEADERS, timeout=_HTTP_TIMEOUT,
                         allow_redirects=True)
        if r.status_code < 400:
            return r.text, r.url
    except Exception as e:
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
    parsed = urlparse(website_url)
    domain = parsed.netloc.lstrip("www.")
    base   = f"{parsed.scheme}://{parsed.netloc}"

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
             website_url, careers_url, detected_platform, detected_slug,
             last_checked)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW())
        ON CONFLICT (employer_fein) DO UPDATE SET
            employer_name    = EXCLUDED.employer_name,
            canonical_name   = EXCLUDED.canonical_name,
            canonical_source = EXCLUDED.canonical_source,
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
        data.get("website_url"),
        data.get("careers_url"),
        data.get("detected_platform"),
        data.get("detected_slug"),
    ))
    conn.commit()


# ─────────────────────────────────────────────────────────────────────────────
# Core processing
# ─────────────────────────────────────────────────────────────────────────────

def process_employer(emp: dict, conn, dry_run: bool, use_llm: bool,
                     force: bool) -> dict:
    fein  = emp["employer_fein"]
    name  = emp["employer_name"]

    log.info("── %s  %s", fein, name)

    # Skip if recently checked (unless forced)
    if not force:
        existing = get_discovery_row(fein, conn)
        if existing and existing.get("last_checked"):
            from datetime import datetime, timezone, timedelta
            age = datetime.now(timezone.utc) - existing["last_checked"].replace(
                tzinfo=timezone.utc
            )
            if age.days < _RECHECK_DAYS:
                log.info("  Skipping — checked %d days ago", age.days)
                return existing

    # Step 1: Wikidata
    log.info("  Wikidata …")
    wd = enrich_wikidata(name)
    time.sleep(0.5)

    canonical_name   = wd["canonical_name"]
    website_url      = wd["website_url"]
    canonical_source = wd["source"]

    # Step 2: Wikipedia fallback for canonical name
    if not canonical_name:
        log.info("  Wikipedia fallback …")
        canonical_name   = enrich_wikipedia(name)
        canonical_source = "wikipedia" if canonical_name else None
        time.sleep(0.3)

    # Step 3: Qwen3 fallback
    if not canonical_name and use_llm:
        log.info("  Qwen3 fallback …")
        canonical_name   = llm_extract_brand(name)
        canonical_source = "qwen" if canonical_name else None

    # Step 4: Regex suffix strip as last resort for canonical name
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

    for i, emp in enumerate(employers, 1):
        log.info("[%d/%d]", i, len(employers))
        result = process_employer(
            emp, conn,
            dry_run=args.dry_run,
            use_llm=args.llm,
            force=args.force,
        )
        if result.get("last_checked") and not args.force:
            stats["skipped"] += 1
            continue
        stats["processed"] += 1
        if result.get("website_url"):
            stats["with_website"] += 1
        if result.get("detected_platform"):
            stats["with_ats"] += 1

        # Polite delay between employers
        time.sleep(1.0)

    conn.close()

    log.info(
        "Done. processed=%d skipped=%d with_website=%d with_ats=%d",
        stats["processed"], stats["skipped"],
        stats["with_website"], stats["with_ats"],
    )


if __name__ == "__main__":
    main()
