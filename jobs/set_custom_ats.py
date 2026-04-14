"""
jobs/set_custom_ats.py — Handler for --set-custom-ats pipeline command.

Usage:
    python pipeline.py --set-custom-ats "Siemens" --curl "curl '...'"
    python pipeline.py --set-custom-ats "Microsoft" \\
        --curl "curl '...listing...'" \\
        --detail-curl "curl '...detail/499961...'"

What it does:
    1. Parse listing curl → extract URL, method, params, headers
    2. Parse detail curl (optional) → build {job_id} template
    3. Store raw curl strings verbatim in DB (before parsing)
    4. Warm session from career_page_url (dynamic auth)
    5. Replay listing request to verify it works
    6. Auto-detect: response format, array path, field map, pagination
    7. Store full enriched config in prospective_companies.ats_slug
    8. Set ats_platform = 'custom'

Re-running --set-custom-ats updates the config in place.
Use this when:
  - Adding a new custom company
  - The stored config has expired (rare — session self-heals)
  - You've added a detail curl for the first time
  - You see a diagnostic flag for this company
"""

import json
import logging
from datetime import datetime

from jobs.utils import (
    REQUEST_TIMEOUT,
    SKIP_HEADERS,
    is_json as _is_json,
)

logger = logging.getLogger(__name__)


def run(company, curl_string, detail_curl=None , sample_job_url=None):
    """
    Main entry point for --set-custom-ats.

    Args:
        company     — company name (must match prospective_companies.company)
        curl_string — raw listing curl from DevTools
        detail_curl — raw detail curl from DevTools (optional)

    Returns True on success, False on failure.
    """
    logger.info("--set-custom-ats: company=%r detail_curl=%s",
                company, "yes" if detail_curl else "no")

    print(f"\n{'='*60}")
    print(f"  Setting custom ATS for: {company}")
    if detail_curl:
        print(f"  Detail curl: provided")
    print(f"{'='*60}\n")

    # ── Step 0: Store raw curls BEFORE parsing ────────────────────
    # Even if parsing fails, the original curl is preserved in DB
    # for debugging via --diagnostics.
    print("[0/5] Storing raw curl(s) in DB...")
    if not _store_raw_curls(company, curl_string, detail_curl):
        print("  [WARNING] Failed to store raw curl(s) — continuing anyway")
        logger.warning("Raw curl storage failed for %r, continuing", company)
    else:
        print("  [OK] Raw curl(s) stored")

    # ── Step 1: Parse listing curl ────────────────────────────────
    print("\n[1/5] Parsing listing curl...")
    try:
        from jobs.curl_parser import curl_to_slug_info
        # We don't know career_page_url here — user can add it
        # via the form. For --set-custom-ats, extract from Referer header.
        career_page_url = _extract_career_page_from_curl(curl_string)
        slug_info = curl_to_slug_info(
            curl_string, career_page_url=career_page_url
        )
    except ValueError as e:
        print(f"  [ERROR] Could not parse curl: {e}")
        logger.error("curl parse failed for %r: %s", company, e)
        _flag_parse_failure(company, "listing", str(e), curl_string)
        return False

    print(f"  URL    : {slug_info['url']}")
    print(f"  Method : {slug_info['method']}")
    print(f"  Params : {len(slug_info.get('params', {}))} params")
    print(f"  Headers: {len(slug_info.get('headers', {}))} headers")
    if career_page_url:
        print(f"  Career page: {career_page_url} (from Referer)")
    
    if sample_job_url and sample_job_url.strip():
        slug_info["sample_job_url"] = sample_job_url.strip()
        print(f"  Sample URL : {sample_job_url.strip()}")

    # ── Step 2: Parse detail curl (optional) ─────────────────────
    if detail_curl and detail_curl.strip():
        print("\n[2/5] Parsing detail curl...")
        try:
            from jobs.curl_parser import parse_detail_curl
            detail_config = parse_detail_curl(detail_curl, slug_info)
            slug_info["detail"] = detail_config
            print(f"  URL template : {detail_config.get('url_template')}")
            print(f"  job_id at   : {detail_config.get('id_location')} "
                  f"(pattern={detail_config.get('id_pattern')})")
            print(f"  Detected ID : {detail_config.get('detected_id')}")
        except Exception as e:
            print(f"  [WARNING] Detail curl parse failed: {e}")
            print("  Detail fetch will be disabled for this company.")
            logger.warning("detail curl parse failed for %r: %s", company, e)
            _flag_parse_failure(company, "detail", str(e), detail_curl)
    else:
        print("\n[2/5] No detail curl provided — skipping")
        print("  Jobs will be saved without descriptions.")
        print(f"  Add later: --set-custom-ats \"{company}\" --curl \"...\" "
              f"--detail-curl \"...\"")

    # ── Step 3: Warm session + replay ─────────────────────────────
    print("\n[3/5] Warming session and replaying listing request...")
    import requests as req_lib
    from jobs.ats.custom_career import _warm_session, _build_legacy_session

    session, strategy = _warm_session(slug_info, company)
    if session is None:
        print("  [WARNING] Career page unreachable — using plain session")
        session  = _build_legacy_session(slug_info)
        strategy = "none"
    else:
        print(f"  Session strategy: {strategy}")

    # Replay
    raw_bytes = _replay_listing(session, slug_info, company)
    if raw_bytes is None:
        print("  [ERROR] Listing request failed.")
        _flag_parse_failure(
            company, "replay",
            "Listing request returned no data",
            curl_string
        )
        return False

    print(f"  HTTP 200 — {len(raw_bytes)} bytes received")

    # ── Step 4: Auto-detect structure ─────────────────────────────
    print("\n[4/5] Detecting response structure...")
    from jobs.ats.custom_career import _detect_structure, _extract_jobs_array

    slug_info["_company"] = company
    detected = _detect_structure(raw_bytes, slug_info)
    if not detected:
        print("  [ERROR] Could not detect jobs in response.")
        print("  Ensure the curl is from the job LISTING endpoint,")
        print("  not a detail or search page.")
        _flag_parse_failure(
            company, "structure",
            "No jobs array detected in listing response",
            raw_bytes
        )
        return False

    slug_info = {**slug_info, **detected}
    fmt        = detected.get("format", "unknown")
    path       = detected.get("array_path", "?")
    field_map  = detected.get("field_map", {})
    pagination = detected.get("pagination", {})

    jobs_arr  = _extract_jobs_array(raw_bytes, slug_info)
    job_count = len(jobs_arr) if jobs_arr else 0

    print(f"  Format     : {fmt}")
    print(f"  Array path : {path} ({job_count} jobs on this page)")
    print(f"  Field map  :")
    print(f"    title    → {field_map.get('title', '?')}")
    print(f"    job_url  → {field_map.get('job_url', '?')}")
    print(f"    location → {field_map.get('location', '?')}")
    print(f"    posted_at→ {field_map.get('posted_at', '?')}")
    print(f"    job_id   → {field_map.get('job_id', '?')}")
    print(f"  Pagination : {pagination.get('type', 'none')}", end="")
    if pagination.get("type") != "none":
        print(f" (param={pagination.get('param')}, "
              f"page_size={pagination.get('page_size')})", end="")
    print()
    print(f"  Session    : {strategy}")

    if jobs_arr:
        sample = jobs_arr[0]
        print(f"\n  Sample job:")
        print(f"    Title   : "
              f"{sample.get(field_map.get('title', ''), '')[:60]}")
        print(f"    Location: "
              f"{sample.get(field_map.get('location', ''), '')[:60]}")
        print(f"    URL     : "
              f"{sample.get(field_map.get('job_url', ''), '')[:60]}")

    if slug_info.get("detail"):
        det = slug_info["detail"]
        print(f"\n  Detail config:")
        print(f"    Template : {det.get('url_template', '?')[:70]}")
        print(f"    job_id   : {det.get('id_location')} "
              f"({det.get('id_pattern')})")

    # ── Step 5: Save to DB ────────────────────────────────────────
    print("\n[5/5] Saving config to database...")
    success = _save_to_db(company, slug_info)
    if not success:
        return False

    # Resolve any open diagnostics for this company since config is fresh
    try:
        from db.custom_ats_diagnostics import resolve_all_for_company
        resolved = resolve_all_for_company(company)
        if resolved > 0:
            print(f"  [OK] Resolved {resolved} open diagnostic(s) "
                  f"for {company}")
    except Exception:
        pass

    print(f"\n{'='*60}")
    print(f"  [OK] Custom ATS configured for {company}")
    print(f"  --monitor-jobs will now fetch jobs automatically.")
    if not slug_info.get("detail"):
        print(f"  To add description fetch, re-run with --detail-curl:")
        print(f'  python pipeline.py --set-custom-ats "{company}" '
              f'--curl "..." --detail-curl "..."')
    print(f"  If this config ever stops working:")
    print(f"    python pipeline.py --diagnostics")
    print(f"    python pipeline.py --set-custom-ats \"{company}\" "
          f"--curl \"...\"")
    print(f"{'='*60}\n")

    logger.info(
        "--set-custom-ats: success for %r format=%s jobs_on_page=%d "
        "detail=%s strategy=%s",
        company, fmt, job_count,
        "yes" if slug_info.get("detail") else "no",
        strategy,
    )
    return True


# ─────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────

def _extract_career_page_from_curl(curl_string):
    """
    Try to extract career page URL from curl's Referer header.
    Returns URL string or None.
    """
    import re
    # Match both -H and --header forms, case-insensitive Referer/Referrer
    m = re.search(
        r'(?:-H|--header)\s+["\']?[Rr]efer(?:er|rer):\s*([^\s"\'\\^]+)',
        curl_string,
        re.IGNORECASE
    )
    if m:
        url = m.group(1).strip().rstrip("'\"^")
        if url.startswith("http"):
            return url
    return None


def _replay_listing(session, slug_info, company):
    """Replay listing request. Returns bytes or None."""
    import requests as req_lib
    import json as _json

    method = slug_info.get("method", "GET").upper()
    params = slug_info.get("params") or None
    body   = slug_info.get("body")

    # Rebuild GraphQL body if needed
    if slug_info.get("graphql_config") and slug_info.get("_lsd"):
        from jobs.curl_parser import build_graphql_body
        body = build_graphql_body(
            slug_info["graphql_config"],
            slug_info.get("_lsd", ""),
            slug_info.get("_rev", ""),
        )

    # _is_json imported from jobs.utils

    try:
        content_type = slug_info.get("headers", {}).get(
            "content-type", ""
        ).lower()

        if method == "POST":
            if "form" in content_type or (body and not _is_json(body)):
                resp = session.post(
                    slug_info["url"],
                    params=params,
                    data=body,
                    timeout=20,
                )
            else:
                resp = session.post(
                    slug_info["url"],
                    params=params,
                    json=json.loads(body) if body and _is_json(body) else None,
                    data=body if body and not _is_json(body) else None,
                    timeout=20,
                )
        else:
            resp = session.get(
                slug_info["url"],
                params=params,
                timeout=20,
            )

        if resp.status_code in (401, 403):
            print(f"  [ERROR] Auth error {resp.status_code} — "
                  f"career page session did not authenticate")
            logger.warning("replay auth error %d for %r",
                           resp.status_code, company)
            return None

        if not resp.ok:
            print(f"  [ERROR] HTTP {resp.status_code}")
            return None

        return resp.content

    except Exception as e:
        print(f"  [ERROR] Request failed: {e}")
        logger.error("replay failed for %r: %s", company, e)
        return None


def _store_raw_curls(company, curl_string, detail_curl=None):
    """
    Store raw curl strings in DB before any parsing.
    Creates the company row if it doesn't exist yet.
    """
    conn = None
    try:
        from db.connection import get_conn
        conn = get_conn()

        # Create row if needed
        existing = conn.execute(
            "SELECT id FROM prospective_companies WHERE company = ?",
            (company,)
        ).fetchone()

        if not existing:
            from urllib.parse import urlparse
            # Extract domain from the curl URL for new companies
            try:
                from jobs.curl_parser import curl_to_slug_info
                temp_slug = curl_to_slug_info(curl_string)
                parsed = urlparse(temp_slug.get("url", ""))
                domain = parsed.netloc or ""
            except Exception:
                domain = ""

            conn.execute(
                "INSERT OR IGNORE INTO prospective_companies "
                "(company, domain, priority, status, created_at) "
                "VALUES (?, ?, 2, 'active', ?)",
                (company, domain, datetime.utcnow())
            )

        # Store raw curls
        parts = []
        vals  = []
        if curl_string:
            parts.append("listing_curl_raw = ?")
            vals.append(curl_string)
        if detail_curl:
            parts.append("detail_curl_raw = ?")
            vals.append(detail_curl)

        if parts:
            conn.execute(
                f"UPDATE prospective_companies "
                f"SET {', '.join(parts)} WHERE company = ?",
                vals + [company]
            )

        conn.commit()
        return True
    except Exception as e:
        logger.error("Could not store raw curls for %r: %s", company, e, exc_info=True)
        return False
    finally:
        if conn:
            conn.close()


def _flag_parse_failure(company, step_name, error_msg, raw_data):
    """Write a diagnostic for parse/replay/detection failures."""
    try:
        from db.custom_ats_diagnostics import (
            flag_diagnostic_once, BLOCKED, UNKNOWN_PATTERN,
            STEP_STRUCTURE_DETECT, STEP_LISTING_FETCH, STEP_DETAIL_FETCH
        )

        step_map = {
            "listing":   STEP_LISTING_FETCH,
            "detail":    STEP_DETAIL_FETCH,
            "replay":    STEP_LISTING_FETCH,
            "structure": STEP_STRUCTURE_DETECT,
        }
        step = step_map.get(step_name, STEP_LISTING_FETCH)

        raw = raw_data
        if isinstance(raw, bytes):
            raw = raw[:5120]
        elif isinstance(raw, str) and len(raw) > 5120:
            raw = raw[:5120]

        flag_diagnostic_once(
            company      = company,
            step         = step,
            severity     = BLOCKED,
            pattern_hint = f"{step_name}_parse_failed",
            raw_response = raw,
            notes        = (
                f"--set-custom-ats failed at step: {step_name}. "
                f"Error: {error_msg}"
            ),
        )
    except Exception:
        pass


def _save_to_db(company, slug_info):
    """Upsert prospective_companies with custom ATS config."""
    conn = None
    try:
        from db.connection import get_conn
        conn = get_conn()

        existing = conn.execute(
            "SELECT id FROM prospective_companies WHERE company = ?",
            (company,)
        ).fetchone()

        slug_json = json.dumps(slug_info)
        now       = datetime.utcnow()

        from urllib.parse import urlparse
        domain = urlparse(slug_info.get("url", "")).netloc or ""

        if existing:
            conn.execute("""
                UPDATE prospective_companies
                SET ats_platform    = 'custom',
                    ats_slug        = ?,
                    ats_detected_at = ?,
                    domain          = ?,
                    status          = 'active'
                WHERE company = ?
            """, (slug_json, now, domain, company))
            print(f"  Updated existing company: {company}")
        else:
            conn.execute("""
                INSERT INTO prospective_companies
                  (company, domain, ats_platform, ats_slug,
                   ats_detected_at, priority, status, created_at)
                VALUES (?, ?, 'custom', ?, ?, 2, 'active', ?)
            """, (company, domain, slug_json, now, now))
            print(f"  Inserted new company: {company}")

        conn.commit()
        return True

    except Exception as e:
        print(f"  [ERROR] Database write failed: {e}")
        logger.error("DB write failed for %r: %s", company, e,
                     exc_info=True)
        return False
    finally:
        if conn is not None:
            conn.close()