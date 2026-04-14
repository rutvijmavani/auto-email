"""
diagnose_career.py — Career site network response diagnostic tool with pagination

Usage:
    python diagnose_career.py https://explore.jobs.netflix.net/careers
    python diagnose_career.py https://explore.jobs.netflix.net/careers --pages 5
    python diagnose_career.py https://explore.jobs.netflix.net/careers --headful
"""

import re
import sys
import json
import time
import asyncio
import argparse
from urllib.parse import urlparse

# ─────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────

PAGE_LOAD_WAIT   = 6
MIN_ARRAY_LENGTH = 2
SAMPLE_SIZE      = 5
MAX_VAL_DISPLAY  = 80
DEFAULT_PAGES    = 3

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36",
]

SKIP_KEYWORDS = {
    "analytics", "tracking", "gtm", "google-tag", "facebook", "pixel",
    "segment", "mixpanel", "amplitude", "hotjar", "clarity", "sentry",
    "datadog", "newrelic", "fonts", "webpack", "chunk", "bundle",
    "polyfill", "runtime", "vendor", "cookielaw", "onetrust", "consent",
    "cookie", "fullstory", "privacy", "vslog", "ipapi", "intercom",
    "zendesk", "crisp", "drift", "hubspot", "marketo",
}

URL_PATTERN      = re.compile(r'^https?://', re.IGNORECASE)
ISO_DATE_PATTERN = re.compile(r'^\d{4}-\d{2}-\d{2}([T ]\d{2}:\d{2})?')
UNIX_TS_MIN      = 1_000_000_000
UNIX_TS_MS_MIN   = 1_000_000_000_000

TITLE_HINTS    = {"title", "jobtitle", "job_title", "position", "positionname",
                  "rolename", "role", "requisitiontitle", "name", "postingname"}
LOCATION_HINTS = {"location", "joblocation", "job_location", "city",
                  "citystate", "office", "site", "locationname", "cityinfo"}
ID_HINTS       = {"id", "jobid", "job_id", "requisitionid", "reqid",
                  "externalid", "postingid", "uniqueid", "slug", "atsjobid",
                  "displayjobid", "code", "refnum"}
URL_HINTS      = {"url", "joburl", "job_url", "applyurl", "detailurl",
                  "link", "href", "absoluteurl", "canonicalpositionurl"}
DATE_HINTS     = {"posteddate", "posted_date", "postedat", "posted_at",
                  "postingdate", "dateposted", "createdat", "publishedat",
                  "tcreate", "tupdate", "creationdate", "updateddate"}
DESC_HINTS     = {"description", "jobdescription", "job_description",
                  "summary", "overview", "content", "body", "detail",
                  "requirement", "responsibilities"}


# ─────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────

def should_skip(url):
    url_lower = url.lower()
    return any(kw in url_lower for kw in SKIP_KEYWORDS)


def try_parse_json(body_bytes):
    try:
        text = body_bytes.decode("utf-8", errors="ignore").strip()
        if not text or text[0] not in ("{", "["):
            return None
        return json.loads(text)
    except Exception:
        return None


def find_largest_array(data, depth=0, path=""):
    if depth > 6:
        return None, None

    best_arr  = None
    best_path = None
    best_len  = MIN_ARRAY_LENGTH - 1

    if isinstance(data, list):
        if len(data) > best_len and data and isinstance(data[0], dict):
            str_fields = sum(
                1 for v in data[0].values()
                if isinstance(v, str) and len(v) > 1
            )
            if str_fields >= 1:
                best_arr  = data
                best_path = path or "[root]"
                best_len  = len(data)

    elif isinstance(data, dict):
        for key, val in data.items():
            current_path = f"{path}.{key}" if path else key
            if isinstance(val, list) and len(val) > best_len:
                if val and isinstance(val[0], dict):
                    best_arr  = val
                    best_path = current_path
                    best_len  = len(val)
            if isinstance(val, (dict, list)):
                sub_arr, sub_path = find_largest_array(val, depth + 1, current_path)
                if sub_arr and len(sub_arr) > best_len:
                    best_arr  = sub_arr
                    best_path = sub_path
                    best_len  = len(sub_arr)

    return best_arr, best_path


def classify_value(field_name, value):
    fl = field_name.lower().replace("_", "").replace("-", "")

    if value is None or value == "" or value == []:
        return "empty"

    if isinstance(value, str):
        v = value.strip()
        if URL_PATTERN.match(v):           return "url"
        if ISO_DATE_PATTERN.match(v):      return "date"
        if re.match(r'^\d{4,}$', v):       return "id_numeric"
        if re.match(r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}'
                    r'-[0-9a-f]{4}-[0-9a-f]{12}$', v, re.I): return "id_uuid"
        if len(v) > 200:                   return "long_text"
        if fl in DESC_HINTS:               return "description"
        if fl in TITLE_HINTS:              return "title_hint"
        if fl in LOCATION_HINTS:           return "location_hint"
        if fl in ID_HINTS:                 return "id_hint"
        if fl in URL_HINTS:                return "url_hint"
        if fl in DATE_HINTS:               return "date_hint"
        return "short_string"

    if isinstance(value, int):
        if value >= UNIX_TS_MS_MIN: return "date_ms"
        if value >= UNIX_TS_MIN:    return "date_s"
        if 1 <= value <= 9_999_999: return "id_int"
        return "int"

    if isinstance(value, float):    return "float"
    if isinstance(value, list):     return f"list[{len(value)}]"
    if isinstance(value, dict):     return f"dict{{{len(value)} keys}}"
    return "other"


def job_relevance_score(fields_info):
    all_types = set()
    for classifications in fields_info.values():
        all_types.update(classifications)

    score = 0
    if any(t in all_types for t in ("title_hint", "short_string")):   score += 20
    if any(t in all_types for t in ("url", "url_hint")):               score += 15
    if any(t in all_types for t in ("date", "date_hint",
                                     "date_ms", "date_s")):            score += 10
    if any(t in all_types for t in ("id_numeric", "id_uuid",
                                     "id_int", "id_hint")):            score += 10
    if any(t in all_types for t in ("location_hint",)):                score += 10
    if any(t in all_types for t in ("description", "long_text")):      score += 15
    return score


def _score_response(resp):
    arr = resp.get("arr")
    if not arr:
        return 0
    sample = [j for j in arr[:SAMPLE_SIZE] if isinstance(j, dict)]
    if not sample:
        return 0
    all_keys = set()
    for job in sample:
        all_keys.update(job.keys())
    fields_info = {
        k: [classify_value(k, job.get(k)) for job in sample]
        for k in all_keys
    }
    return job_relevance_score(fields_info)


def fmt(val, max_len=MAX_VAL_DISPLAY):
    s = str(val)
    return s[:max_len] + "…" if len(s) > max_len else s


# ─────────────────────────────────────────
# PLAYWRIGHT — ASYNC CAPTURE
# ─────────────────────────────────────────

async def _dismiss_modals(page):
    """Try to close any modal/overlay that might block clicks."""
    dismiss_selectors = [
        '[aria-label*="close" i]',
        '[aria-label*="dismiss" i]',
        'button[class*="close" i]',
        'button[class*="modal-close" i]',
        '.modal .btn-close',
        '.modal [data-dismiss]',
        '.modal [data-bs-dismiss]',
    ]
    for sel in dismiss_selectors:
        try:
            el = page.locator(sel).first
            if await el.is_visible():
                await el.click(timeout=2000)
                await asyncio.sleep(0.5)
        except Exception:
            pass

    # Press Escape as fallback
    try:
        await page.keyboard.press("Escape")
        await asyncio.sleep(0.3)
    except Exception:
        pass


async def _get_next_button(page):
    """Multi-strategy next-page / load-more button finder."""
    strategies = [
        page.get_by_role("button", name=re.compile(
            r"show more|load more|next|more positions|more jobs", re.I)),
        page.get_by_role("link", name=re.compile(
            r"next|load more|show more", re.I)),
        page.locator('[aria-label*="next" i]'),
        page.locator('[title*="next" i]'),
        page.locator('[data-testid*="next" i]'),
        page.locator('.next, [class*="pagination-next" i], [class*="next-page" i]'),
        page.locator('[class*="show-more" i]'),
        page.locator('text=›'),
        page.locator('text=>'),
    ]
    for locator in strategies:
        try:
            candidate = locator.first
            if await candidate.is_visible() and await candidate.is_enabled():
                return candidate
        except Exception:
            continue
    return None


async def _try_click(page, btn):
    """
    Try multiple click strategies in order:
    1. scroll into view + normal click
    2. force click (bypasses interception)
    3. JS click
    Returns True if any succeeded.
    """
    # Strategy 1: scroll into view then click
    try:
        await btn.scroll_into_view_if_needed(timeout=3000)
        await asyncio.sleep(0.5)
        await btn.click(timeout=5000)
        return True
    except Exception:
        pass

    # Strategy 2: force click (ignores overlapping elements)
    try:
        await btn.click(force=True, timeout=5000)
        return True
    except Exception:
        pass

    # Strategy 3: JS click
    try:
        await btn.evaluate("el => el.click()")
        return True
    except Exception:
        pass

    return False


async def capture_responses(career_url, max_pages=DEFAULT_PAGES, headless=True):
    try:
        from playwright.async_api import async_playwright
    except ImportError:
        print("ERROR: playwright not installed.")
        print("  pip install playwright && playwright install chromium")
        sys.exit(1)

    try:
        from playwright_stealth import stealth_async
        use_stealth = True
    except ImportError:
        use_stealth = False

    import random
    captured  = []
    captured_lock = asyncio.Lock()
    pending_tasks = []

    print(f"\n{'='*70}")
    print(f"  Navigating to: {career_url}")
    print(f"  Pages to scan: {max_pages}")
    print(f"  Headless: {headless}")
    if use_stealth:
        print(f"  Stealth: enabled")
    print(f"{'='*70}\n")

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=headless)
        context = await browser.new_context(
            user_agent=random.choice(USER_AGENTS),
            viewport={"width": 1280, "height": 900},
            extra_http_headers={
                "Accept-Language": "en-US,en;q=0.9",
                "Accept": (
                    "text/html,application/xhtml+xml,application/xml;"
                    "q=0.9,application/json,*/*;q=0.8"
                ),
            }
        )
        page = await context.new_page()

        if use_stealth:
            await stealth_async(page)

        async def on_response(response):
            try:
                url = response.url
                if should_skip(url):
                    return
                status = response.status
                ct     = response.headers.get("content-type", "")
                body   = await response.body()
                if not body or len(body) < 50:
                    return
                data = try_parse_json(body)
                if data is None:
                    return

                arr, path = find_largest_array(data)

                async with captured_lock:
                    # Update existing entry if we have a larger array now
                    existing = next((r for r in captured if r["url"] == url), None)
                    if existing:
                        if arr:
                            arr_len = len(arr)
                            if arr_len > existing["arr_len"]:
                                existing.update({
                                    "arr": arr, "path": path,
                                    "arr_len": arr_len, "data": data,
                                })
                        return

                    entry = {
                        "url":     url,
                        "status":  status,
                        "ct":      ct,
                        "method":  response.request.method,
                        "data":    data,
                        "arr":     arr,
                        "path":    path,
                        "arr_len": len(arr) if arr else 0,
                        "raw_len": len(body),
                    }
                    captured.append(entry)

                # Live progress for interesting responses
                if arr and len(arr) >= 2:
                    score = _score_response(entry)
                    indicator = "🟢" if score >= 40 else "🟡" if score >= 20 else "🔴"
                    print(f"  {indicator} [{response.request.method}] "
                          f"{url[:75]}  →  {len(arr)} items  (score={score})")

            except Exception as e:
                # Log parse/processing failures with context
                import logging
                logging.getLogger("diagnose_career").error(
                    "Failed to process response from %s: %s",
                    getattr(response, 'url', 'unknown'),
                    e,
                    exc_info=True
                )

        def _on_response_sync(response):
            """Sync wrapper that schedules async handler as task."""
            task = asyncio.create_task(on_response(response))
            pending_tasks.append(task)

        page.on("response", _on_response_sync)

        # ── Initial load ──────────────────────────────────
        try:
            await page.goto(career_url, wait_until="networkidle", timeout=40000)
            print("  ✓ Page loaded (networkidle)")
        except Exception:
            try:
                await page.goto(career_url, wait_until="domcontentloaded",
                                timeout=25000)
                print("  ✓ Page loaded (domcontentloaded)")
                await asyncio.sleep(PAGE_LOAD_WAIT)
            except Exception as e:
                print(f"  ✗ Navigation failed: {e}")
                await browser.close()
                return []

        await asyncio.sleep(2)

        # ── Paginate ──────────────────────────────────────
        for page_num in range(1, max_pages):
            print(f"\n  ── Page {page_num + 1} / {max_pages} ──────────")

            # Dismiss any modals that might block clicks
            await _dismiss_modals(page)
            await asyncio.sleep(0.5)

            next_btn = await _get_next_button(page)

            if next_btn:
                print(f"  Found button — clicking...")
                prev_count = len(captured)
                success = await _try_click(page, next_btn)

                if success:
                    # Wait for new network activity
                    try:
                        await page.wait_for_load_state("networkidle", timeout=10000)
                    except Exception:
                        pass
                    await asyncio.sleep(2)

                    if len(captured) > prev_count:
                        print(f"  ✓ Click triggered {len(captured) - prev_count} new response(s)")
                    else:
                        print(f"  Click succeeded but no new responses — may be same data")
                else:
                    print(f"  All click strategies failed — trying scroll")
                    await page.mouse.wheel(0, 8000)
                    await asyncio.sleep(3)
            else:
                # Infinite scroll
                print(f"  No button found — scrolling...")
                prev_count = len(captured)
                await page.mouse.wheel(0, 8000)
                await asyncio.sleep(3)
                await page.mouse.wheel(0, 8000)
                await asyncio.sleep(2)

                if len(captured) == prev_count:
                    print(f"  No new responses after scroll — end of content")
                    break

        # Drain pending async tasks before closing browser
        if pending_tasks:
            await asyncio.gather(*pending_tasks, return_exceptions=True)

        await browser.close()

    return captured


# ─────────────────────────────────────────
# ANALYSIS + PRINT
# ─────────────────────────────────────────

def analyse_and_print(captured):
    if not captured:
        print("\n⚠  No JSON responses captured.\n")
        print("Possible reasons:")
        print("  - Page requires authentication")
        print("  - Job data is server-rendered HTML (no API calls)")
        print("  - Try --headful to watch what happens")
        return

    captured.sort(key=lambda r: r["arr_len"], reverse=True)

    print(f"\n{'='*70}")
    print(f"  CAPTURED {len(captured)} JSON RESPONSES")
    print(f"{'='*70}")

    for idx, resp in enumerate(captured):
        arr     = resp["arr"]
        arr_len = resp["arr_len"]
        url     = resp["url"]
        method  = resp["method"]
        status  = resp["status"]
        ct      = resp["ct"]
        path    = resp["path"] or ""

        print(f"\n{'─'*70}")
        print(f"  [{idx}] {method} {status}  —  {resp['raw_len']} bytes")
        print(f"  URL: {url}")
        print(f"  CT : {ct}")

        if not arr:
            print(f"  ARRAY: none")
            if isinstance(resp["data"], dict):
                print(f"  Top-level keys: {list(resp['data'].keys())[:12]}")
            continue

        print(f"  ARRAY: {arr_len} items at '{path}'")

        sample = [j for j in arr[:SAMPLE_SIZE] if isinstance(j, dict)]
        if not sample:
            print(f"  (array items are not dicts)")
            continue

        all_keys = set()
        for job in sample:
            all_keys.update(job.keys())

        fields_info = {}
        fields_vals = {}
        for key in sorted(all_keys):
            classifications = []
            vals = []
            for job in sample:
                val = job.get(key)
                classifications.append(classify_value(key, val))
                vals.append(val)
            fields_info[key] = classifications
            fields_vals[key] = vals

        rel_score = job_relevance_score(fields_info)
        indicator = ("🟢 LIKELY JOBS"   if rel_score >= 40 else
                     "🟡 POSSIBLE"      if rel_score >= 20 else
                     "🔴 UNLIKELY JOBS")
        print(f"  SCORE: {rel_score}  {indicator}")

        print(f"\n  {'FIELD':<35} {'TYPES':<30} SAMPLE VALUE")
        print(f"  {'─'*35} {'─'*30} {'─'*35}")

        def field_sort_key(k):
            types = set(fields_info[k])
            if any(t in types for t in ("title_hint", "url", "url_hint",
                                         "date_hint", "date_ms", "date_s",
                                         "id_hint", "location_hint",
                                         "description")):
                return 0
            if any(t in types for t in ("short_string", "id_numeric",
                                         "id_uuid", "id_int", "long_text")):
                return 1
            return 2

        for key in sorted(all_keys, key=field_sort_key):
            type_set  = set(fields_info[key])
            types_str = ", ".join(sorted(type_set))[:28]
            sample_val = next(
                (v for v in fields_vals[key]
                 if v is not None and v != "" and v != []),
                None
            )
            val_str = fmt(sample_val) if sample_val is not None else "(null)"
            print(f"  {key:<35} {types_str:<30} {val_str}")

    # ── Summary ───────────────────────────────────────────
    likely = [(i, r) for i, r in enumerate(captured) if _score_response(r) >= 40]

    print(f"\n{'='*70}")
    print(f"  SUMMARY")
    print(f"{'='*70}")

    if likely:
        print(f"\n  ✅ {len(likely)} response(s) look like job listings:\n")
        for i, r in likely:
            score = _score_response(r)
            print(f"     [{i}] score={score}  {r['arr_len']} items")
            print(f"          URL   : {r['url']}")
            print(f"          Path  : {r['path']}")
            print(f"          Method: {r['method']}")
            print()

        best_idx, best = max(likely, key=lambda x: _score_response(x[1]))
        print(f"  🏆 Best candidate: [{best_idx}]")
        print(f"     {best['url']}")
        print()
        parsed_path = urlparse(best["url"]).path
        filter_hint = parsed_path.split("/")[1] if "/" in parsed_path else "XHR"
        print(f"  Next step — capture the curl for this endpoint:")
        print(f"  1. Open DevTools → Network tab")
        print(f"  2. Filter by: {filter_hint}")
        print(f"  3. Reload the page or trigger the request again")
        print(f"  4. Right-click the request → Copy → Copy as cURL")
        print(f"  5. Paste into the Google Form (Listing Curl column)")
    else:
        print("\n  ⚠  No responses scored ≥ 40.")
        print("     Try:")
        print("     - --headful to watch what happens")
        print("     - --pages 5 to navigate more pages")
        print("     - A more specific URL (search/results page)")

    print()


# ─────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Career site network diagnostic with auto-pagination"
    )
    parser.add_argument("url", help="Career page URL to diagnose")
    parser.add_argument("--pages", type=int, default=DEFAULT_PAGES,
                        help=f"Pages to scan (default: {DEFAULT_PAGES})")
    parser.add_argument("--headful", action="store_true",
                        help="Run browser visibly (useful for debugging)")
    args = parser.parse_args()

    # Fix: properly await the coroutine
    captured = asyncio.run(
        capture_responses(
            args.url,
            max_pages=args.pages,
            headless=not args.headful,
        )
    )

    # captured is now a list, not a coroutine
    analyse_and_print(captured)


if __name__ == "__main__":
    main()