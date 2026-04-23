#!/usr/bin/env python3
"""
test_domain_probe.py — Test Eightfold API with and without domain= param

Tests each slug with 3 combinations:
  1. domain=slug.com      (derived heuristic)
  2. domain=              (omitted entirely)
  3. domain=slug.eightfold.ai  (what's currently in DB — expected to fail)

Usage:
    python test_domain_probe.py
"""
import re
import time
import uuid
import requests

SLUGS = [
    "starbucks",
    "lamresearch",
    "qualcomm",
]

HEADERS = {
    "accept":             "application/json, text/plain, */*",
    "accept-language":    "en-US,en;q=0.9",
    "sec-ch-ua":          '"Not:A-Brand";v="99", "Google Chrome";v="145", "Chromium";v="145"',
    "sec-ch-ua-mobile":   "?0",
    "sec-ch-ua-platform": '"Windows"',
    "sec-fetch-dest":     "empty",
    "sec-fetch-mode":     "cors",
    "sec-fetch-site":     "same-origin",
    "user-agent":         "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36",
}


def fetch_csrf(session, slug):
    url = f"https://{slug}.eightfold.ai/careers?hl=en-US"
    try:
        resp = session.get(url, timeout=15)
        for pattern in [
            r'<meta[^>]+name=["\']csrf-token["\'][^>]+content=["\']([^"\']+)["\']',
            r'csrf[_-]?[Tt]oken["\s:=]+["\']([A-Za-z0-9+/=._\-]{20,})["\']',
            r'"csrfToken"\s*:\s*"([^"]{20,})"',
        ]:
            m = re.search(pattern, resp.text)
            if m:
                return m.group(1)
        for name, val in session.cookies.items():
            if "csrf" in name.lower() or "xsrf" in name.lower():
                return val
        return ""
    except Exception as e:
        print(f"  [CSRF] Error: {e}")
        return ""


def probe(session, slug, csrf_token, domain):
    """
    Single probe: GET /api/pcsx/search with or without domain= param.
    Returns (status_code, job_count, total, sample_title, error_snippet)
    """
    base   = f"https://{slug}.eightfold.ai"
    url    = f"{base}/api/pcsx/search"
    params = {
        "query":                 "",
        "start":                 0,
        "num":                   10,
        "sort_by":               "distance",
        "filter_include_remote": "1",
        "hl":                    "en-US",
    }
    if domain:
        params["domain"] = domain

    headers = {
        **HEADERS,
        "referer":                f"{base}/careers?hl=en-US",
        "x-browser-request-time": str(time.time()),
        "x-csrf-token":           csrf_token,
        "sentry-trace":           f"{uuid.uuid4().hex}-{uuid.uuid4().hex[:16]}-0",
    }

    try:
        resp = session.get(url, params=params, headers=headers, timeout=15)
        status = resp.status_code

        if status != 200:
            snippet = resp.text[:80].strip().replace("\n", " ")
            return status, 0, 0, "", snippet

        data      = resp.json()
        inner     = data.get("data", {})
        positions = inner.get("positions", [])
        total     = int(inner.get("count", 0))
        sample    = positions[0].get("name", "?")[:50] if positions else ""
        return status, len(positions), total, sample, ""

    except Exception as e:
        return 0, 0, 0, "", str(e)


def test_slug(slug):
    print(f"\n{'='*65}")
    print(f"  {slug.upper()}  →  https://{slug}.eightfold.ai")
    print(f"{'='*65}")

    session    = requests.Session()
    session.headers.update(HEADERS)
    csrf_token = fetch_csrf(session, slug)
    print(f"  CSRF: {'found' if csrf_token else 'not found'}\n")

    probes = [
        (f"{slug}.com",           "derived heuristic (slug.com)"),
        ("",                      "NO domain= param"),
        (f"{slug}.eightfold.ai",  "eightfold subdomain — current DB value"),
    ]

    results = []
    for domain, label in probes:
        status, on_page, total, sample, err = probe(session, slug, csrf_token, domain)

        if status == 200 and total > 0:
            marker = "✅"
            detail = f"total={total}  on_page={on_page}  sample=\"{sample}\""
        elif status == 200 and total == 0:
            marker = "⚠️ "
            detail = "HTTP 200 but 0 jobs returned"
        else:
            marker = "❌"
            detail = f"HTTP {status}  {err[:60]}" if err else f"HTTP {status}"

        param_str = f'domain="{domain}"' if domain else "domain= OMITTED"
        print(f"  {marker} [{param_str:<35}]  {detail}")
        results.append((domain, status, total, marker))

    # Summary
    working = [(d, t) for d, s, t, m in results if s == 200 and t > 0]
    print()
    if working:
        best_domain, best_total = working[0]
        print(f"  → Best param: {'domain=' + best_domain if best_domain else 'NO domain= param'}")
        print(f"  → Works without domain=: {'YES' if any(d == '' for d, _ in working) else 'NO'}")
    else:
        print(f"  → ⚠️  No probe returned jobs — tenant may require auth or have 0 openings")


if __name__ == "__main__":
    for slug in SLUGS:
        test_slug(slug)
    print("\nDone.")