"""
scripts/poc_glassdoor.py — POC: Glassdoor jobs page → ATS apply URL.

Usage (local, no curl needed — home IP is not CF-blocked):
    python scripts/poc_glassdoor.py --employer-id 13461

Usage (with curl from Chrome DevTools — use when cookies/cf_clearance needed):
    python scripts/poc_glassdoor.py --curl-file curl.txt --employer-id 13461

Output:
    --output results.json   write {glassdoor_id, ats_url, employer_name} to file
    --dry-run               parse curl only, no network requests

What it does:
    1. (optional) Parses curl → extracts headers + cookies
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


def _normalize_win_curl(s: str) -> str:
    """Convert Windows cmd.exe curl (^ escapes) to bash-compatible form."""
    # Order matters: handle ^\^" (escaped quote) before ^" (delimiter)
    s = s.replace('^\\^"', '\\"')   # ^\^" → \"  (backslash-quote inside value)
    s = re.sub(r'\^ *\r?\n\s*', ' ', s)  # ^ + newline → space (line continuation)
    s = s.replace('^"', '"')         # ^" → "  (quote delimiter)
    s = s.replace('^%', '%')         # ^% → %
    s = s.replace('^&', '&')         # ^& → &
    s = s.replace('^{', '{').replace('^}', '}')
    return s


def _unescape(s: str) -> str:
    return re.sub(r'\\u([0-9a-fA-F]{4})', lambda m: chr(int(m.group(1), 16)), s)


def _make_session():
    if _USE_CURL_CFFI:
        return _Session(impersonate=_IMPERSONATE)
    return _Session()


def _jobs_page_url(employer_id: str) -> str:
    return f"{GLASSDOOR_BASE}/Jobs/x-Jobs-E{employer_id}.htm"


def _extract_apply_url(html_text: str, debug: bool = False) -> tuple[str | None, bool | None]:
    """
    Parse __next_f script chunks for first non-Easy-Apply job's apply URL.

    Handles two field name variants seen in the wild:
      - isEasyApply / applyUrl  (older Next.js SSR pages)
      - easyApply   / jobLink   (RSC / newer pages)

    Returns (apply_url, is_easy_apply) or (None, None) if not found.
    """
    chunks = re.findall(
        r'self\.__next_f\.push\(\[1,\s*"(.*?)"\]\)',
        html_text,
        re.DOTALL,
    )
    combined = "".join(chunks)

    if debug:
        # Show snippets around each easyApply occurrence to diagnose field names
        for m in re.finditer(r'.{0,120}[Ee]asyApply.{0,120}', combined):
            print(f"  [debug] ...{m.group()}...")
        for m in re.finditer(r'.{0,80}(?:applyUrl|jobLink).{0,80}', combined):
            print(f"  [debug] ...{m.group()}...")

    # URL capture: non-backslash-quote chars OR backslash + non-quote (e.g. &)
    _url_cap = r'((?:[^\\"]|\\[^"])+)'

    flag_patterns = [
        r'\\"easyApply\\":\s*false',
        r'\\"isEasyApply\\":\s*false',
        r'"easyApply"\s*:\s*false',
        r'"isEasyApply"\s*:\s*false',
    ]
    url_patterns = [
        (r'\\"jobLink\\":\s*\\"' + _url_cap, True),
        (r'\\"applyUrl\\":\s*\\"' + _url_cap, True),
        (r'"jobLink"\s*:\s*"([^"]+)"', False),
        (r'"applyUrl"\s*:\s*"([^"]+)"', False),
    ]

    for flag_pat in flag_patterns:
        for url_pat, needs_unescape in url_patterns:
            for m in [
                re.search(flag_pat + r'(?:.{0,500}?)' + url_pat, combined, re.DOTALL),
                re.search(url_pat + r'(?:.{0,500}?)' + flag_pat, combined, re.DOTALL),
            ]:
                if m:
                    url = m.group(1)
                    if needs_unescape:
                        url = _unescape(url)
                    return url, False

    return None, None


AUTH_FILE = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "glassdoor_auth.json")

_EDGE_UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/151.0.0.0 Safari/537.36 Edg/151.0.0.0")


def _extract_auth_from_curl(curl_string: str) -> tuple[dict, dict]:
    """
    Parse all cookies from a raw curl command.
    Returns (all_cookies, auth_cookies) where auth_cookies is just {at, gdId}.
    """
    import shlex
    tokens = shlex.split(curl_string)
    cookie_str = ""
    for i, t in enumerate(tokens):
        if t in ("-b", "--cookie") and i + 1 < len(tokens):
            cookie_str = tokens[i + 1]
            break
    all_cookies, auth = {}, {}
    for part in cookie_str.split(";"):
        part = part.strip()
        if "=" not in part:
            continue
        k, v = part.split("=", 1)
        k = k.strip()
        all_cookies[k] = v.strip()
        if k in ("at", "gdId"):
            auth[k] = v.strip()
    return all_cookies, auth


def _save_auth(auth: dict) -> None:
    import json as _json
    with open(AUTH_FILE, "w") as f:
        _json.dump(auth, f, indent=2)
    print(f"  Auth saved to {AUTH_FILE}  (valid ~1 year)")


def _load_auth() -> dict:
    import json as _json
    if os.path.exists(AUTH_FILE):
        try:
            return _json.loads(open(AUTH_FILE).read())
        except Exception:
            pass
    return {}


def _build_auth_session(cookies: dict):
    """Build a session pre-loaded with the given cookies."""
    session = _make_session()
    for k, v in cookies.items():
        session.cookies.set(k, v, domain=".glassdoor.com")
    return session


def run(
    curl_string: str | None,
    employer_id: str,
    dry_run: bool = False,
    warm_url: str = None,
    output_file: str = None,
    employer_name: str = None,
    auth_file: str = None,
) -> dict | None:
    import json as _json
    jobs_url = _jobs_page_url(employer_id)
    warm_url = warm_url or jobs_url

    extra = {
        "User-Agent": _EDGE_UA,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
    }

    # ── Step 1: Build authenticated session ───────────────────────────────────
    # Extract + persist auth tokens if curl provided (side-effect only — don't change session)
    if curl_string:
        print(f"\n[1/5] Extracting auth tokens from curl...")
        _, auth = _extract_auth_from_curl(curl_string)
        if "at" in auth and "gdId" in auth:
            print(f"  at    : {auth['at'][:40]}...")
            print(f"  gdId  : {auth['gdId']}")
            _save_auth(auth)
        else:
            print(f"  [WARN] Could not extract at/gdId from curl")
            auth = _load_auth()
    else:
        auth = _load_auth()
        if auth:
            print(f"\n[1/5] Using saved auth (at/gdId) for detail page requests")
        else:
            print(f"\n[1/5] No saved auth — detail page apply URL may be missing")

    if dry_run:
        print("[DRY RUN] Stopping before network requests.")
        return None

    # ── Original working session: plain curl_cffi, no cookies ─────────────────
    session = _make_session()
    print(f"\n[2/5] Warming session on {GLASSDOOR_BASE}/home ...")
    try:
        session.get(f"{GLASSDOOR_BASE}/home", headers=extra, timeout=20)
    except Exception:
        pass

    delay = float(os.environ.get("GD_DELAY", "2"))
    if delay > 0:
        import time
        time.sleep(delay)

    # ── Step 3: Try BFF JSON API first (clean JSON, no HTML parsing) ──────────
    bff_url = f"{GLASSDOOR_BASE}/bff/employer-profile-mono/get-employer-job-listings"
    bff_body = {"employerId": int(employer_id), "pageNumber": 1}
    bff_headers = {**extra, "Content-Type": "application/json",
                   "Origin": GLASSDOOR_BASE, "Referer": jobs_url}
    apply_path = None
    print(f"\n[3/5] Trying BFF API {bff_url}...")
    try:
        import json as _json
        bff_resp = session.post(bff_url, json=bff_body, headers=bff_headers, timeout=30)
        print(f"  HTTP {bff_resp.status_code}  ({len(bff_resp.content):,} bytes)")
        if bff_resp.status_code == 200:
            data = bff_resp.json()
            listings = data.get("jobListings", [])
            print(f"  {len(listings)} job listings returned")
            for listing in listings:
                hdr = listing.get("jobview", {}).get("header", {})
                if not hdr.get("easyApply", True):
                    apply_path = hdr.get("jobLink")
                    print(f"  Found non-EasyApply jobLink: {apply_path}")
                    break
            if apply_path is None and listings:
                print("  All listings are EasyApply — falling back to HTML scrape")
    except Exception as e:
        print(f"  BFF attempt failed: {e} — falling back to HTML scrape")

    # ── Step 3b: HTML fallback if BFF blocked or all EasyApply ───────────────
    if apply_path is None:
        print(f"\n[3b/5] Fetching HTML page {jobs_url}...")
        try:
            resp = session.get(jobs_url, headers=extra, timeout=30, allow_redirects=True)
            print(f"  HTTP {resp.status_code}  ({len(resp.content):,} bytes)")
        except Exception as e:
            print(f"  [ERROR] Request failed: {e}")
            return None

        if resp.status_code != 200:
            print(f"  [BLOCKED] {resp.status_code} — CF is blocking this IP/path")
            return None

        debug = os.environ.get("GD_DEBUG") == "1"
        print(f"\n[4/5] Parsing __next_f for non-Easy-Apply applyUrl...")
        apply_path, _ = _extract_apply_url(resp.text, debug=debug)

        if not apply_path:
            print("  [MISS] No non-Easy-Apply applyUrl found — company may only have Easy Apply jobs.")
            return None

        print(f"  Found applyUrl: {apply_path}")

    apply_full = GLASSDOOR_BASE + apply_path if apply_path.startswith("/") else apply_path

    # ── Step 5: Follow redirect → final ATS URL ───────────────────────────────
    print(f"\n[5/5] Following redirect...")
    try:
        redir = session.get(apply_full, headers=extra, timeout=20, allow_redirects=True)
        final_url = redir.url
    except Exception as e:
        print(f"  [ERROR] Redirect follow failed: {e}")
        print(f"  Partner URL: {apply_full}")
        return None

    from urllib.parse import urlparse
    gd_hosts = {"www.glassdoor.com", "glassdoor.com"}

    # If redirect stayed on Glassdoor, fetch the job detail page and extract external apply URL
    if urlparse(final_url).hostname in gd_hosts:
        print(f"  Redirect stayed on Glassdoor — fetching job detail page for external apply URL...")
        # Inject auth cookies only here — the jobs listing page works without them
        if auth:
            for k, v in auth.items():
                session.cookies.set(k, v, domain=".glassdoor.com")
            print(f"  Injected auth cookies (at+gdId) for authenticated detail page")
        try:
            detail = session.get(final_url, headers=extra, timeout=20, allow_redirects=True)
            print(f"  Detail page HTTP {detail.status_code}  ({len(detail.content):,} bytes)")
        except Exception as e:
            print(f"  [ERROR] Detail page fetch failed: {e}")
            return None

        # Look for external apply URL patterns in __next_f chunks
        detail_chunks = re.findall(
            r'self\.__next_f\.push\(\[1,\s*"(.*?)"\]\)',
            detail.text, re.DOTALL,
        )
        detail_combined = "".join(detail_chunks)

        print(f"  Detail page URL: {final_url}")
        if os.environ.get("GD_DEBUG_TCHUNKS") == "1":
            for tm in re.finditer(r'(\d+:T[0-9a-fA-F]+,)((?:.|\n){0,120})', detail_combined):
                print(f"  [T-chunk] {tm.group(1)} -> {tm.group(2)!r}")

        if os.environ.get("GD_DEBUG_DETAIL") == "1":
            for m in re.finditer(r'.{0,100}[Aa]pply.{0,100}', detail_combined):
                print(f"  [detail-debug] ...{m.group()}...")

        # Pattern 1: RSC raw text chunk — "N:T<hex>,https://external-url..."
        # This is how Glassdoor embeds the actual apply redirect URL (e.g. Indeed tracking URL)
        ext_url = None
        for m in re.finditer(r'\d+:T[0-9a-fA-F]+,(https?://[^\s"\\]+)', detail_combined):
            candidate = _unescape(m.group(1))
            parsed = urlparse(candidate)
            if parsed.hostname and parsed.hostname not in gd_hosts:
                ext_url = candidate
                print(f"  Found RSC text-chunk apply URL: {ext_url[:80]}...")
                break

        # Pattern 2: named JSON fields (fallback)
        if not ext_url:
            for pat in [
                r'\\"applyUrl\\":\s*\\"((?:[^\\"]|\\[^"])+)',
                r'\\"externalApplyUrl\\":\s*\\"((?:[^\\"]|\\[^"])+)',
                r'\\"applyButtonUrl\\":\s*\\"((?:[^\\"]|\\[^"])+)',
                r'"applyUrl"\s*:\s*"([^"]+)"',
            ]:
                m = re.search(pat, detail_combined, re.DOTALL)
                if m:
                    candidate = _unescape(m.group(1))
                    parsed = urlparse(candidate)
                    if parsed.hostname and parsed.hostname not in gd_hosts:
                        ext_url = candidate
                        break
                    if candidate.startswith("/partner/"):
                        partner_full = GLASSDOOR_BASE + candidate
                        print(f"  Following detail-page partner redirect: {partner_full[:80]}...")
                        try:
                            r2 = session.get(partner_full, headers=extra, timeout=20, allow_redirects=True)
                            r2_parsed = urlparse(r2.url)
                            if r2_parsed.hostname not in gd_hosts:
                                ext_url = r2.url
                        except Exception as e:
                            print(f"  [ERROR] Partner redirect failed: {e}")
                        break

        if not ext_url:
            print(f"  [MISS] No external apply URL found on job detail page")
            return None

        # Follow the redirect chain from the intermediate URL (e.g. Indeed tracking
        # URL → actual company ATS).  Allow_redirects=True gives us the final hop.
        print(f"  Following intermediate URL: {ext_url[:80]}...")
        try:
            r_ext = session.get(ext_url, headers=extra, timeout=20, allow_redirects=True)
            final_url = r_ext.url
        except Exception as e:
            print(f"  [WARN] Could not follow intermediate URL ({e}) — using as-is")
            final_url = ext_url
        print(f"  Final URL after redirect: {final_url}")

    print(f"\n{'='*60}")
    print(f"  FINAL ATS APPLY URL:")
    print(f"  {final_url}")
    print(f"{'='*60}\n")

    result = {
        "glassdoor_id": employer_id,
        "employer_name": employer_name or "",
        "ats_url": final_url,
    }

    if output_file:
        import json, pathlib
        existing = []
        p = pathlib.Path(output_file)
        if p.exists():
            try:
                existing = json.loads(p.read_text())
            except Exception:
                pass
        existing.append(result)
        p.write_text(json.dumps(existing, indent=2))
        print(f"  Saved to {output_file} ({len(existing)} total entries)")

    return result


def main():
    ap = argparse.ArgumentParser(description="Glassdoor → ATS apply URL")
    group = ap.add_mutually_exclusive_group(required=False)
    group.add_argument("--curl", help="Raw curl command from DevTools (inline)")
    group.add_argument("--curl-file", help="Path to file containing raw curl command")
    ap.add_argument("--employer-id", required=True, help="Glassdoor employer ID (e.g. 13461)")
    ap.add_argument("--employer-name", default="", help="Company name (for output file)")
    ap.add_argument("--dry-run", action="store_true", help="Parse only, no network requests")
    ap.add_argument("--warm-url", help="URL to warm session on (default: jobs listing page)")
    ap.add_argument("--output", help="JSON file to append results to (for Option B import)")
    ap.add_argument("--auth-file", default=AUTH_FILE, help="Path to persisted auth JSON (default: glassdoor_auth.json)")
    args = ap.parse_args()

    curl_string = None
    if args.curl_file:
        with open(args.curl_file, "r", encoding="utf-8") as f:
            curl_string = f.read()
    elif args.curl:
        curl_string = args.curl

    if curl_string and '^"' in curl_string:
        curl_string = _normalize_win_curl(curl_string)
        print("[info] Detected Windows cmd curl format — converted to bash format")

    run(
        curl_string=curl_string,
        employer_id=args.employer_id,
        dry_run=args.dry_run,
        warm_url=args.warm_url,
        output_file=args.output,
        employer_name=args.employer_name,
        auth_file=args.auth_file,
    )


if __name__ == "__main__":
    main()
