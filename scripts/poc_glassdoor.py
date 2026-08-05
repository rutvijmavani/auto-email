"""
scripts/poc_glassdoor.py — POC: warm session + parse Glassdoor jobs page → apply URL.

Usage:
    python scripts/poc_glassdoor.py --curl "curl '...'" --employer-id 13461
    python scripts/poc_glassdoor.py --curl "curl '...'" --employer-id 13461 --dry-run

Paste the curl from Chrome DevTools (any Glassdoor page will do — cookies are
what matter, the URL gets replaced with the company jobs page).

What it does:
    1. Parses curl → extracts headers + career_page_url (glassdoor.com)
    2. Warms session by visiting glassdoor.com
    3. Hits glassdoor.com/Jobs/x-Jobs-E{employer_id}.htm
    4. Parses __next_f JSON chunks for first non-Easy-Apply applyUrl
    5. Follows /partner/jobListing.htm redirect → prints final ATS apply URL
"""

import argparse
import re
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    from curl_cffi.requests import Session as _Session
    _IMPERSONATE = "chrome146"
    _USE_CURL_CFFI = True
except ImportError:
    from requests import Session as _Session
    _IMPERSONATE = None
    _USE_CURL_CFFI = False

GLASSDOOR_BASE = "https://www.glassdoor.com"


def _make_session():
    if _USE_CURL_CFFI:
        return _Session(impersonate=_IMPERSONATE)
    return _Session()


def _jobs_page_url(employer_id: str) -> str:
    return f"{GLASSDOOR_BASE}/Jobs/x-Jobs-E{employer_id}.htm"


def _extract_apply_url(html_text: str) -> tuple[str | None, bool | None]:
    """
    Parse __next_f script chunks for first non-Easy-Apply job's applyUrl.

    Returns (apply_url, is_easy_apply) or (None, None) if not found.
    """
    # Collect all __next_f string payloads
    chunks = re.findall(
        r'self\.__next_f\.push\(\[1,\s*"(.*?)"\]\)',
        html_text,
        re.DOTALL,
    )
    combined = "".join(chunks)

    # Each job object in the JSON has both isEasyApply and applyUrl.
    # Try pattern: isEasyApply comes before applyUrl in the object.
    # We look for non-Easy-Apply jobs only.
    pattern = re.compile(
        r'"isEasyApply"\s*:\s*false'   # not easy apply
        r'(?:[^{}]{0,2000}?)'          # anything within same object (not crossing {})
        r'"applyUrl"\s*:\s*"([^"]+)"', # applyUrl field
        re.DOTALL,
    )
    m = pattern.search(combined)
    if m:
        return m.group(1), False

    # Try reverse order: applyUrl before isEasyApply
    pattern2 = re.compile(
        r'"applyUrl"\s*:\s*"([^"]+)"'
        r'(?:[^{}]{0,2000}?)'
        r'"isEasyApply"\s*:\s*false',
        re.DOTALL,
    )
    m2 = pattern2.search(combined)
    if m2:
        return m2.group(1), False

    return None, None


def run(curl_string: str, employer_id: str, dry_run: bool = False) -> None:
    from jobs.curl_parser import curl_to_slug_info

    # ── Step 1: Parse curl ────────────────────────────────────────────────────
    print(f"\n[1/4] Parsing curl...")
    slug_info = curl_to_slug_info(curl_string, career_page_url=GLASSDOOR_BASE)
    print(f"  Original URL : {slug_info['url']}")
    print(f"  curl_cffi    : {'yes (Chrome impersonation)' if _USE_CURL_CFFI else 'NO — pip install curl_cffi'}")
    print(f"  Headers      : {len(slug_info.get('headers', {}))} headers")

    jobs_url = _jobs_page_url(employer_id)
    print(f"  Target URL   : {jobs_url}")

    if dry_run:
        print("\n[DRY RUN] Stopping before network requests.")
        return

    # ── Step 2: Warm session on glassdoor.com ─────────────────────────────────
    print(f"\n[2/4] Warming session on {GLASSDOOR_BASE}...")
    session = _make_session()
    raw_headers = slug_info.get("headers", {})
    skip = {"cookie", "content-length", "host", "connection",
            "transfer-encoding", "accept-encoding"}
    extra = {k: v for k, v in raw_headers.items() if k.lower() not in skip}
    try:
        warm_resp = session.get(GLASSDOOR_BASE, headers=extra, timeout=20, allow_redirects=True)
        print(f"  Warm-up HTTP {warm_resp.status_code}  ({len(warm_resp.content):,} bytes)")
    except Exception as e:
        print(f"  Warm-up failed: {e} — continuing anyway")

    # ── Step 3: Fetch jobs page ───────────────────────────────────────────────
    print(f"\n[3/4] Fetching {jobs_url}...")
    try:
        resp = session.get(jobs_url, headers=extra, timeout=30, allow_redirects=True)
        print(f"  HTTP {resp.status_code}  ({len(resp.content):,} bytes)")
    except Exception as e:
        print(f"  [ERROR] Request failed: {e}")
        return

    if resp.status_code != 200:
        print(f"  [ERROR] Non-200 response (status={resp.status_code})")
        return

    # ── Step 4: Parse __next_f for applyUrl ──────────────────────────────────
    print(f"\n[4/4] Parsing __next_f for non-Easy-Apply applyUrl...")
    apply_path, is_easy = _extract_apply_url(resp.text)

    if not apply_path:
        print("  [ERROR] No non-Easy-Apply applyUrl found in page.")
        print("  This company may only have Easy Apply jobs on Glassdoor.")
        return

    print(f"  Found applyUrl: {apply_path}")

    # Resolve relative URL
    if apply_path.startswith("/"):
        apply_full = GLASSDOOR_BASE + apply_path
    else:
        apply_full = apply_path

    # ── Step 5: Follow redirect → final ATS URL ───────────────────────────────
    print(f"\n[5/4] Following redirect...")
    try:
        redir = session.get(apply_full, headers=extra, timeout=20, allow_redirects=True)
        final_url = redir.url
        print(f"\n{'='*60}")
        print(f"  FINAL ATS APPLY URL:")
        print(f"  {final_url}")
        print(f"{'='*60}\n")
    except Exception as e:
        print(f"  [ERROR] Redirect follow failed: {e}")
        print(f"  Partner URL: {apply_full}")


def main():
    ap = argparse.ArgumentParser(description="POC: Glassdoor → ATS apply URL")
    group = ap.add_mutually_exclusive_group(required=True)
    group.add_argument("--curl", help="Raw curl command from DevTools (inline)")
    group.add_argument("--curl-file", help="Path to file containing raw curl command")
    ap.add_argument("--employer-id", required=True, help="Glassdoor employer ID (e.g. 13461)")
    ap.add_argument("--dry-run", action="store_true", help="Parse only, no network requests")
    args = ap.parse_args()

    if args.curl_file:
        with open(args.curl_file, "r", encoding="utf-8") as f:
            curl_string = f.read()
    else:
        curl_string = args.curl

    run(
        curl_string=curl_string,
        employer_id=args.employer_id,
        dry_run=args.dry_run,
    )


if __name__ == "__main__":
    main()
