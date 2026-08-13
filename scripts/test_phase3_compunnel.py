#!/usr/bin/env python3
"""
Reproduce Phase 3 and Phase 6 against Compunnel.

Run from the mail/ project root:
    python scripts/test_phase3_compunnel.py
"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

import logging
logging.basicConfig(level=logging.DEBUG, format="%(name)s  %(message)s")

import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

from urllib.parse import urlparse
import requests

# ── Phase 3 lists (copied from discover_h1b_ats.py) ──────────────────────────
_CAREER_PATHS = [
    "/careers", "/careers/", "/careers/jobs", "/jobs", "/jobs/",
    "/about/careers", "/company/careers", "/en/careers", "/us/careers",
    "/en/jobs", "/en-us/careers", "/join-us", "/work-with-us",
    "/work-here", "/opportunities", "/open-positions",
]

_CAREER_SUBDOMAINS = [
    "https://careers.{domain}",
    "https://jobs.{domain}",
    "https://work.{domain}",
]


def _root_domain(url_or_host: str) -> str:
    if "://" not in url_or_host:
        url_or_host = "https://" + url_or_host
    host = urlparse(url_or_host).hostname or ""
    parts = host.split(".")
    return ".".join(parts[-2:]) if len(parts) >= 2 else host


TIMEOUT = 10


def probe(url: str, company_root: str):
    try:
        r = requests.get(url, timeout=TIMEOUT, allow_redirects=True,
                         verify=False, headers={"User-Agent": "Mozilla/5.0"})
        final_url  = r.url
        final_root = _root_domain(final_url)
        redirected = final_url.rstrip("/") != url.rstrip("/")
        jumped     = final_root != company_root
        return {
            "status":        r.status_code,
            "final_url":     final_url,
            "html_len":      len(r.text),
            "redirected":    redirected,
            "jumped_domain": jumped,
            "final_root":    final_root,
        }
    except requests.exceptions.ConnectionError as e:
        return {"error": f"ConnectionError: {e}"}
    except requests.exceptions.Timeout:
        return {"error": "Timeout"}
    except Exception as e:
        return {"error": str(e)}


# ─────────────────────────────────────────────────────────────────────────────
# Phase 3
# ─────────────────────────────────────────────────────────────────────────────
def run_phase3(website_url: str):
    parsed       = urlparse(website_url)
    netloc       = parsed.netloc
    domain       = netloc.removeprefix("www.")
    base         = f"{parsed.scheme}://{netloc}"
    company_root = _root_domain(website_url)

    candidates = []
    for tmpl in _CAREER_SUBDOMAINS:
        candidates.append(tmpl.format(domain=domain))
    for path in _CAREER_PATHS:
        candidates.append(base + path)

    print(f"\n{'═'*90}")
    print(f"  PHASE 3 — company_root={company_root}")
    print(f"{'═'*90}")
    print(f"  {'CANDIDATE URL':<45} {'STATUS':>6}  {'FINAL URL / ERROR'}")
    print(f"{'─'*90}")

    for url in candidates:
        r = probe(url, company_root)
        if "error" in r:
            print(f"  {url:<45} {'ERR':>6}  {r['error']}")
        else:
            flag = ""
            if r["jumped_domain"]:
                flag = "  ← JUMPED TO EXTERNAL DOMAIN"
            elif r["status"] != 200:
                flag = f"  ← {r['status']}"
            elif r["html_len"] > 0:
                flag = f"  ← {r['html_len']} bytes"

            final = r["final_url"] if r["redirected"] else "(no redirect)"
            print(f"  {url:<45} {r['status']:>6}  {final}{flag}")

    print(f"{'─'*90}\n")


# ─────────────────────────────────────────────────────────────────────────────
# Phase 6
# ─────────────────────────────────────────────────────────────────────────────
def run_phase6(company: str, domain: str):
    from jobs.career_page import detect_via_career_page

    print(f"\n{'═'*90}")
    print(f"  PHASE 6 — detect_via_career_page(company={company!r}, domain={domain!r})")
    print(f"{'═'*90}")

    result = detect_via_career_page(company, domain)

    print(f"\n  RESULT: {result}\n")
    if result:
        print(f"  platform = {result.get('platform')}")
        print(f"  slug     = {result.get('slug')}")
        print(f"  careers_url = {result.get('careers_url')}")
    else:
        print("  → Phase 6 returned None (MISS)")
    print()


# ─────────────────────────────────────────────────────────────────────────────
def main():
    run_phase3("https://www.compunnel.com")
    run_phase6("COMPUNNEL SOFTWARE", "compunnel.com")


if __name__ == "__main__":
    main()
