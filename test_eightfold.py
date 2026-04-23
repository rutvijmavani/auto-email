#!/usr/bin/env python3
"""
test_eightfold.py — Quick diagnostic for Eightfold.ai API health
Usage:  python test_eightfold.py [slug] [domain]
        python test_eightfold.py starbucks starbucks.com
        python test_eightfold.py lamresearch lamresearch.com
"""
import re
import sys
import time
import uuid
import json
import requests

SLUGS_DEFAULT = [
    ("starbucks",   "starbucks.com"),
    ("lamresearch", "lamresearch.com"),
]

HEADERS = {
    "accept":             "application/json, text/plain, */*",
    "accept-language":    "en-US,en;q=0.9",
    "sec-ch-ua":          '"Not:A-Brand";v="99","Google Chrome";v="145","Chromium";v="145"',
    "sec-ch-ua-mobile":   "?0",
    "sec-ch-ua-platform": '"Windows"',
    "sec-fetch-dest":     "empty",
    "sec-fetch-mode":     "cors",
    "sec-fetch-site":     "same-origin",
    "user-agent":         "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36",
}

# All known + candidate API paths — test each to find which one works
CANDIDATE_PATHS = [
    "/api/pcsx/search",          # current path in eightfold.py
    "/api/jobs",                 # simpler fallback seen on some tenants
    "/api/search",               # alternate
    "/api/v2/pcsx/search",       # versioned variant
    "/careers/api/jobs",         # path-prefixed
    "/careers/search",           # SPA search endpoint
]


def _fetch_csrf(session, slug):
    url = f"https://{slug}.eightfold.ai/careers?hl=en-US"
    print(f"  [CSRF] GET {url}")
    try:
        resp = session.get(url, timeout=15)
        print(f"  [CSRF] Status: {resp.status_code}  "
              f"cookies: {list(session.cookies.keys())}")

        for pattern in [
            r'<meta[^>]+name=["\']csrf-token["\'][^>]+content=["\']([^"\']+)["\']',
            r'csrf[_-]?[Tt]oken["\s:=]+["\']([A-Za-z0-9+/=._\-]{20,})["\']',
            r'"csrfToken"\s*:\s*"([^"]{20,})"',
        ]:
            m = re.search(pattern, resp.text)
            if m:
                token = m.group(1)
                print(f"  [CSRF] Token found ({len(token)} chars): {token[:20]}...")
                return token

        # Cookie fallback
        for name, val in session.cookies.items():
            if "csrf" in name.lower() or "xsrf" in name.lower():
                print(f"  [CSRF] Token from cookie '{name}': {val[:20]}...")
                return val

        print("  [CSRF] Token NOT found — will try without")
        return ""
    except Exception as e:
        print(f"  [CSRF] Error: {e}")
        return ""


def _probe_paths(session, slug, domain, csrf_token):
    """Try all candidate API paths and report which work."""
    base = f"https://{slug}.eightfold.ai"
    params = {
        "domain": domain,
        "query":  "",
        "start":  0,
        "num":    10,
        "sort_by": "distance",
        "filter_include_remote": "1",
        "hl": "en-US",
    }
    headers = {
        **HEADERS,
        "referer":                f"{base}/careers?hl=en-US",
        "x-browser-request-time": str(time.time()),
        "x-csrf-token":           csrf_token,
        "sentry-trace":           f"{uuid.uuid4().hex}-{uuid.uuid4().hex[:16]}-0",
    }

    working = None
    for path in CANDIDATE_PATHS:
        url = f"{base}{path}"
        try:
            resp = session.get(url, params=params, headers=headers, timeout=12)
            status = resp.status_code
            size   = len(resp.content)
            snippet = ""
            if status == 200:
                try:
                    data      = resp.json()
                    inner     = data.get("data", {})
                    positions = inner.get("positions", [])
                    total     = inner.get("count", "?")
                    snippet   = f"count={total}  positions_on_page={len(positions)}"
                    if positions:
                        sample_title = positions[0].get("name", "?")
                        snippet += f'  sample="{sample_title[:40]}"'
                    if working is None:
                        working = path
                except Exception:
                    snippet = f"non-JSON ({size} bytes): {resp.text[:80]!r}"
            else:
                snippet = resp.text[:80].strip().replace("\n", " ")

            marker = "✅" if status == 200 else "❌"
            print(f"    {marker} {path:40s}  HTTP {status}  {snippet}")

        except Exception as e:
            print(f"    ❌ {path:40s}  ERROR: {e}")

    return working


def test_slug(slug, domain):
    print(f"\n{'='*60}")
    print(f"  Slug: {slug}  Domain: {domain}")
    print(f"  Base: https://{slug}.eightfold.ai")
    print(f"{'='*60}")

    session = requests.Session()
    session.headers.update(HEADERS)

    csrf = _fetch_csrf(session, slug)
    print()
    print("  Probing API paths:")
    working = _probe_paths(session, slug, domain, csrf)

    if working:
        print(f"\n  ✅ Working path: {working}")
    else:
        print(f"\n  ❌ No working path found — endpoint may have changed or slug is wrong")

    # Also probe without domain param (some tenants ignore it)
    print("\n  Re-probing working path without domain= param:")
    if working:
        base = f"https://{slug}.eightfold.ai"
        params_no_domain = {
            "query": "", "start": 0, "num": 10,
            "sort_by": "distance", "filter_include_remote": "1", "hl": "en-US",
        }
        headers = {
            **HEADERS,
            "referer":      f"{base}/careers?hl=en-US",
            "x-csrf-token": csrf,
        }
        try:
            resp = requests.get(
                f"{base}{working}", params=params_no_domain,
                headers=headers, timeout=12
            )
            print(f"    Status: {resp.status_code}  "
                  f"bytes: {len(resp.content)}")
        except Exception as e:
            print(f"    Error: {e}")

    return working


def check_schwab():
    """
    Separately diagnose Charles Schwab TalentBrew 0-jobs issue.
    Fetch www.schwabjobs.com/sitemap.xml and show a sample of URL patterns.
    """
    print(f"\n{'='*60}")
    print("  Charles Schwab — TalentBrew sitemap check")
    print(f"{'='*60}")
    url = "https://www.schwabjobs.com/sitemap.xml"
    print(f"  GET {url}")
    try:
        resp = requests.get(
            url,
            headers={"User-Agent": HEADERS["user-agent"]},
            timeout=15,
        )
        print(f"  Status: {resp.status_code}  bytes: {len(resp.content)}")
        if resp.status_code == 200:
            text = resp.text
            # Show first 10 <loc> entries
            locs = re.findall(r"<loc>([^<]+)</loc>", text)
            print(f"  Total <loc> entries: {len(locs)}")
            print("  First 10 URLs:")
            for loc in locs[:10]:
                print(f"    {loc}")
            # Show how many match the expected /job/... pattern
            job_pattern = re.compile(r"/job/[^/]+/[^/]+/(\d+)/(\d+)/?$")
            matches = [l for l in locs if job_pattern.search(l)]
            print(f"\n  URLs matching /job/city/title/tenant/id: {len(matches)}")
            if not matches:
                print("  ⚠️  Pattern mismatch — schwabjobs.com uses a different URL structure")
                # Show a sample of non-matching job-like URLs
                job_like = [l for l in locs if "/job" in l.lower() or "/career" in l.lower()]
                print(f"  Job-like URLs (first 5):")
                for l in job_like[:5]:
                    print(f"    {l}")
            else:
                print(f"  Sample matching URLs:")
                for l in matches[:3]:
                    print(f"    {l}")
                    m = job_pattern.search(l)
                    print(f"      → tenant_id={m.group(1)}  job_id={m.group(2)}")
    except Exception as e:
        print(f"  Error: {e}")


if __name__ == "__main__":
    if len(sys.argv) == 3:
        slugs = [(sys.argv[1], sys.argv[2])]
    else:
        slugs = SLUGS_DEFAULT

    for slug, domain in slugs:
        test_slug(slug, domain)

    # Always run Schwab check
    check_schwab()

    print("\nDone.")
