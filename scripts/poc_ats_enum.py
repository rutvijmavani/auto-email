"""
scripts/poc_ats_enum.py — POC: enumerate ATS companies via two free sources.

  Source 1: hackertarget.com (subdomain lookup)
    → subdomain-based ATS: Workday, iCIMS, Taleo, Jobvite, SmartRecruiters
    → free, no auth, returns all known subdomains for a domain

  Source 2: Wayback Machine CDX API
    → path-based ATS: Greenhouse, Lever, Ashby
    → collapse=urlkey deduplicates — one result per unique URL ever crawled
    → much better than Common Crawl CDX for distinct company slugs

Usage:
    python scripts/poc_ats_enum.py
    python scripts/poc_ats_enum.py --source hackertarget
    python scripts/poc_ats_enum.py --source wayback
    python scripts/poc_ats_enum.py --platform workday
    python scripts/poc_ats_enum.py --out data/ats_enum_results.json
"""

import argparse
import json
import re
import time
from datetime import datetime

import requests

TIMEOUT = 60
HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; ATS-Enum-POC/1.0)"}

_EXCLUDED = {
    "www", "app", "api", "auth", "sso", "login", "secure", "portal",
    "careers", "jobs", "apply", "recruit", "talent", "hr", "sandbox",
    "demo", "test", "staging", "dev", "beta", "static", "cdn",
    "help", "support", "mail", "smtp", "ns1", "ns2", "ftp", "status",
    # Common HTTP path segments that leak into slug extraction
    ".well-known", "robots.txt", "sitemap.xml", "favicon.ico",
    "embed", "js", "css", "images", "assets", "fonts",
}

# ─────────────────────────────────────────────────────────────────────────────
# SOURCE 1: hackertarget.com subdomain enumeration
# Free, no auth — returns all subdomains of a domain it has indexed.
# URL: https://api.hackertarget.com/hostsearch/?q=<domain>
# Response: plain text, one "subdomain,ip" per line
# ─────────────────────────────────────────────────────────────────────────────

HACKERTARGET_PLATFORMS = {
    "workday": {
        "domain":  "myworkdayjobs.com",
        "pattern": re.compile(r"^([a-z0-9][a-z0-9\-]+)\.(wd\d+)\.myworkdayjobs\.com$", re.I),
        "slug_fn": lambda m: m.group(1).lower(),
        "note":    "tenant = subdomain before .wd1. / .wd3. etc.",
    },
    "icims": {
        "domain":  "icims.com",
        "pattern": re.compile(r"^((?:careers?-)?[a-z0-9][a-z0-9\-]+)\.icims\.com$", re.I),
        "slug_fn": lambda m: m.group(1).lower(),
        "note":    "slug includes careers- prefix (used verbatim in iCIMS API URLs)",
    },
    "taleo": {
        "domain":  "taleo.net",
        "pattern": re.compile(r"^([a-z0-9][a-z0-9\-]*)\.taleo\.net$", re.I),
        "slug_fn": lambda m: m.group(1).lower(),
        "note":    "slug = company subdomain",
    },
    "jobvite": {
        "domain":  "jobvite.com",
        "pattern": re.compile(r"^([a-z0-9][a-z0-9\-]*)\.jobvite\.com$", re.I),
        "slug_fn": lambda m: m.group(1).lower(),
        "note":    "slug = company subdomain",
    },
    "smartrecruiters": {
        "domain":  "smartrecruiters.com",
        "pattern": re.compile(r"^([a-z0-9][a-z0-9\-]*)\.smartrecruiters\.com$", re.I),
        "slug_fn": lambda m: m.group(1).lower(),
        "note":    "slug = company subdomain",
    },
}


def fetch_hackertarget(domain: str) -> list[str]:
    """Return all subdomains of domain from hackertarget.com."""
    url = f"https://api.hackertarget.com/hostsearch/?q={domain}"
    try:
        resp = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
        if resp.status_code != 200:
            print(f"    [hackertarget] HTTP {resp.status_code}")
            return []
        text = resp.text.strip()
        if "error" in text.lower() and len(text) < 200:
            print(f"    [hackertarget] API error: {text[:200]}")
            return []
        # Each line: "subdomain.domain.com,1.2.3.4"
        subdomains = []
        for line in text.splitlines():
            parts = line.split(",")
            if parts:
                subdomains.append(parts[0].strip().lower())
        return subdomains
    except Exception as exc:
        print(f"    [hackertarget] Error: {exc}")
        return []


def enumerate_hackertarget(platforms: list[str]) -> dict[str, set[str]]:
    results: dict[str, set[str]] = {}

    for platform in platforms:
        cfg = HACKERTARGET_PLATFORMS.get(platform)
        if not cfg:
            print(f"  [SKIP] Unknown platform: {platform}")
            continue

        print(f"\n  [{platform.upper()}] querying hackertarget for {cfg['domain']!r} ...")
        t0 = time.time()
        subdomains = fetch_hackertarget(cfg["domain"])
        elapsed = time.time() - t0
        print(f"    got {len(subdomains):,} subdomains in {elapsed:.1f}s")

        slugs: set[str] = set()
        for subdomain in subdomains:
            m = cfg["pattern"].match(subdomain)
            if not m:
                continue
            slug = cfg["slug_fn"](m)
            base = re.sub(r"^careers?-", "", slug)
            if base in _EXCLUDED or len(slug) < 2:
                continue
            slugs.add(slug)

        results[platform] = slugs
        print(f"    → {len(slugs):,} unique company slugs")
        if cfg.get("note"):
            print(f"    note: {cfg['note']}")
        print(f"    sample: {sorted(slugs)[:10]}")

        time.sleep(2)  # be polite to hackertarget free tier

    return results


# ─────────────────────────────────────────────────────────────────────────────
# SOURCE 2: Wayback Machine CDX API
# collapse=urlkey → one result per unique URL ever archived (deduplicates crawls)
# Free, no auth, handles large result sets reliably.
# ─────────────────────────────────────────────────────────────────────────────

WAYBACK_PLATFORMS = {
    # ── Path-based ATS (slug in URL path) ─────────────────────────────────────
    "greenhouse": {
        "url_patterns": [
            "boards.greenhouse.io/*",
            "job-boards.greenhouse.io/*",
        ],
        "slug_re": re.compile(
            r"(?:boards|job-boards)(?:\.eu)?\.greenhouse\.io/(?!embed/)([^/?&#\s/]+)",
            re.I,
        ),
        "subdomain_re": None,
        "note": "both boards. and job-boards. subdomains",
    },
    "lever": {
        "url_patterns": ["jobs.lever.co/*"],
        "slug_re": re.compile(r"jobs\.lever\.co/([^/?&#\s/]+)", re.I),
        "subdomain_re": None,
        "note": "lever stopped being indexed by CC after 2025-47 — wayback has historical data",
    },
    "ashby": {
        "url_patterns": ["jobs.ashbyhq.com/*"],
        "slug_re": re.compile(r"jobs\.ashbyhq\.com/([^/?&#\s/]+)", re.I),
        "subdomain_re": None,
        "note": "ashby sitemap exists but has malformed XML — wayback is cleaner",
    },
    # ── Subdomain-based ATS (slug is the subdomain) ───────────────────────────
    # Wayback CDX supports wildcard subdomain queries: *.domain.com/*
    # This captures real tenant traffic even though Workday/iCIMS/Taleo use
    # wildcard certs (which block crt.sh).
    "workday": {
        # Two URL patterns: myworkdayjobs.com (old) + myworkdaysite.com (new)
        # Chunked by year — full query times out, each year-chunk completes fine
        "url_patterns": ["myworkdayjobs.com", "myworkdaysite.com"],
        "match_type":   "domain",
        "chunk_years":  list(range(2015, 2027)),
        "slug_re": None,
        "subdomain_re": re.compile(
            # myworkdayjobs.com: accenture.wd103.myworkdayjobs.com → "accenture"
            r"https?://([a-z0-9][a-z0-9\-]+)\.(wd\d+)\.myworkdayjobs\.com"
            # myworkdaysite.com: wd1.myworkdaysite.com/recruiting/accenture/... → "accenture"
            r"|https?://wd\d+\.myworkdaysite\.com/(?:[a-z]{2}[-_][A-Z]{2}/)?recruiting/([a-z0-9][a-z0-9\-]+)",
            re.I,
        ),
        "slug_fn": lambda m: (m.group(1) or m.group(2)).lower(),
        "note": "myworkdayjobs.com + myworkdaysite.com, chunked year-by-year",
    },
    "icims": {
        "url_patterns": ["icims.com"],
        "match_type":   "domain",
        "chunk_years":  list(range(2015, 2027)),
        "slug_re": None,
        "subdomain_re": re.compile(
            r"https?://((?:careers?-)?[a-z0-9][a-z0-9\-]+)\.icims\.com",
            re.I,
        ),
        "slug_fn": lambda m: m.group(1).lower(),
        "note": "slug includes careers- prefix used verbatim in iCIMS API URLs",
    },
    "taleo": {
        "url_patterns": ["taleo.net"],
        "match_type":   "domain",
        "chunk_years":  list(range(2010, 2027)),  # Taleo is older
        "slug_re": None,
        "subdomain_re": re.compile(
            r"https?://([a-z0-9][a-z0-9\-]*)\.taleo\.net",
            re.I,
        ),
        "slug_fn": lambda m: m.group(1).lower(),
        "note": "slug = company subdomain",
    },
}

_CDX_API = "https://web.archive.org/cdx/search/cdx"


def fetch_wayback(
    url_pattern: str,
    limit: int = 500_000,
    match_type: str = None,
    from_date: str = None,
    to_date: str = None,
) -> list[str]:
    """
    Query Wayback CDX for all URLs matching pattern.
    collapse=urlkey returns one entry per unique URL (deduplicates across crawls).

    Uses output=text + streaming so large responses (10MB+) never truncate.

    match_type: "domain" for subdomain wildcards (myworkdayjobs.com covers all subdomains)
    from_date:  "YYYYMMDD" to limit to recent crawls — reduces response size dramatically
    """
    params = {
        "url":      url_pattern,
        "output":   "text",
        "fl":       "original",
        "collapse": "urlkey",
        "limit":    limit,
        "filter":   "statuscode:200",
    }
    if match_type:
        params["matchType"] = match_type
    if from_date:
        params["from"] = from_date
    if to_date:
        params["to"] = to_date

    try:
        resp = requests.get(
            _CDX_API,
            params=params,
            headers=HEADERS,
            timeout=120,
            stream=True,
        )
        if resp.status_code != 200:
            print(f"    [wayback] HTTP {resp.status_code} for {url_pattern!r}")
            return []

        urls = []
        for line in resp.iter_lines():
            if line:
                url = line.decode("utf-8", errors="replace").strip()
                if url:
                    urls.append(url)
        return urls

    except Exception as exc:
        print(f"    [wayback] Error for {url_pattern!r}: {exc}")
        return []


def enumerate_wayback(platforms: list[str]) -> dict[str, set[str]]:
    results: dict[str, set[str]] = {}

    for platform in platforms:
        cfg = WAYBACK_PLATFORMS.get(platform)
        if not cfg:
            print(f"  [SKIP] Unknown platform: {platform}")
            continue

        print(f"\n  [{platform.upper()}] querying Wayback CDX ...")
        slugs: set[str] = set()

        for url_pattern in cfg["url_patterns"]:
            chunk_years = cfg.get("chunk_years")
            chunks = (
                [(f"{y}0101", f"{y}1231") for y in chunk_years]
                if chunk_years
                else [(cfg.get("from_date"), None)]
            )

            for from_date, to_date in chunks:
                label = f"{url_pattern}  [{from_date[:4] if from_date else 'all'}]"
                print(f"    CDX: {label}")
                t0 = time.time()
                urls = fetch_wayback(
                    url_pattern,
                    match_type=cfg.get("match_type"),
                    from_date=from_date,
                    to_date=to_date,
                )
                elapsed = time.time() - t0
                print(f"    got {len(urls):,} unique URLs in {elapsed:.1f}s")

            new_slugs: set[str] = set()
            for url in urls:
                # Subdomain-based ATS: extract from hostname
                if cfg.get("subdomain_re"):
                    m = cfg["subdomain_re"].search(url)
                    if not m:
                        continue
                    slug = cfg["slug_fn"](m)
                else:
                    # Path-based ATS: extract from URL path
                    m = cfg["slug_re"].search(url)
                    if not m:
                        continue
                    slug = m.group(1).lower().rstrip("/")

                # Strip URL-encoding artifacts: "2fbloomberg" → "bloomberg" (2f = encoded /)
                slug = re.sub(r"^[0-9a-f]{2}(?=[a-z])", "", slug)
                # Strip numeric job-ID prefixes: "21941517-manulife" → "manulife"
                slug = re.sub(r"^\d+[-_]", "", slug)

                if (
                    slug
                    and slug not in _EXCLUDED
                    and len(slug) >= 2
                    and not slug.isdigit()
                    and not slug.startswith(".")
                    and re.match(r"^[a-z0-9][a-z0-9\-]*$", slug)
                ):
                    new_slugs.add(slug)

            added = new_slugs - slugs
            slugs.update(new_slugs)
            print(f"    → {len(new_slugs):,} slugs (+{len(added):,} new from this pattern)")

        results[platform] = slugs
        print(f"    TOTAL: {len(slugs):,} unique company slugs")
        if cfg.get("note"):
            print(f"    note: {cfg['note']}")
        print(f"    sample: {sorted(slugs)[:10]}")

    return results


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="POC: enumerate ATS companies via hackertarget + Wayback CDX"
    )
    parser.add_argument(
        "--source",
        choices=["hackertarget", "wayback", "all"],
        default="all",
        help="Which source to query (default: all)",
    )
    parser.add_argument(
        "--platform",
        default=None,
        help="Comma-separated platform(s) to query",
    )
    parser.add_argument(
        "--out",
        default=None,
        metavar="PATH",
        help="Save results as JSON to this path",
    )
    args = parser.parse_args()

    print("=" * 60)
    print("ATS Company Enumeration — POC")
    print(f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

    all_results: dict[str, dict] = {}

    # ── HACKERTARGET ─────────────────────────────────────────────────────────
    if args.source in ("hackertarget", "all"):
        platforms = (
            [p.strip() for p in args.platform.split(",")]
            if args.platform else list(HACKERTARGET_PLATFORMS)
        )
        platforms = [p for p in platforms if p in HACKERTARGET_PLATFORMS]
        if platforms:
            print(f"\nSOURCE 1: hackertarget.com subdomain enumeration")
            print(f"  Platforms: {platforms}")
            for platform, slugs in enumerate_hackertarget(platforms).items():
                all_results[platform] = {
                    "source": "hackertarget",
                    "count":  len(slugs),
                    "slugs":  sorted(slugs),
                }

    # ── WAYBACK CDX ──────────────────────────────────────────────────────────
    if args.source in ("wayback", "all"):
        platforms = (
            [p.strip() for p in args.platform.split(",")]
            if args.platform else list(WAYBACK_PLATFORMS)
        )
        platforms = [p for p in platforms if p in WAYBACK_PLATFORMS]
        if platforms:
            print(f"\nSOURCE 2: Wayback Machine CDX API")
            print(f"  Platforms: {platforms}")
            for platform, slugs in enumerate_wayback(platforms).items():
                if platform in all_results:
                    merged = set(all_results[platform]["slugs"]) | slugs
                    all_results[platform]["slugs"] = sorted(merged)
                    all_results[platform]["count"] = len(merged)
                    all_results[platform]["source"] += " + wayback"
                else:
                    all_results[platform] = {
                        "source": "wayback",
                        "count":  len(slugs),
                        "slugs":  sorted(slugs),
                    }

    # ── SUMMARY ──────────────────────────────────────────────────────────────
    print(f"\n{'=' * 60}")
    print("SUMMARY")
    print(f"{'=' * 60}")
    total = 0
    for platform, data in sorted(all_results.items()):
        print(f"  {platform:<20} {data['count']:>6,} companies  (source: {data['source']})")
        total += data["count"]
    print(f"  {'TOTAL':<20} {total:>6,} company slugs across all platforms")

    # ── SAVE ─────────────────────────────────────────────────────────────────
    if args.out:
        import os
        os.makedirs(os.path.dirname(args.out) if os.path.dirname(args.out) else ".", exist_ok=True)
        with open(args.out, "w") as f:
            json.dump(
                {"generated_at": datetime.now().isoformat(), "platforms": all_results},
                f, indent=2,
            )
        print(f"\nResults saved to: {args.out}")


if __name__ == "__main__":
    main()
