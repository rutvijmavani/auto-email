"""
build_ats_slug_list.py — Build ATS company slug list via AWS Athena.

Queries Common Crawl columnar index (Parquet on S3) using Athena.
Cost: ~$0.00024 per crawl query. ~$0.003/year total.

Sources:
  1. Athena  — primary, all platforms except Lever
  2. Brave   — Lever + Oracle HCM + iCIMS fallback
  3. Backfill — one-time Lever import from CC-MAIN-2025-43

Smart refresh:
  - Tracks which crawls already scanned (scanned_crawls table)
  - Only queries Athena for NEW crawls not yet processed
  - Normal monthly run = 1 Athena query (newest crawl only)
  - CSV saved locally for recovery, deleted on next run
  - Stale slugs archived to ats_archive.csv.gz before deletion

Usage:
  python build_ats_slug_list.py              (normal monthly run)
  python build_ats_slug_list.py --test       (dry run, no Athena)
  python build_ats_slug_list.py --backfill   (one-time Lever import)
  python build_ats_slug_list.py --from-csv data/athena_2026-03-09.csv
  python build_ats_slug_list.py --skip-brave

Setup (.env):
  AWS_ACCESS_KEY_ID=...
  AWS_SECRET_ACCESS_KEY=...
  AWS_REGION=us-east-1
  ATHENA_DATABASE=ccindex
  ATHENA_TABLE=ccindex
  ATHENA_S3_OUTPUT=s3://your-bucket/athena-results/
  BRAVE_API_KEY=...  (optional, free tier: 1000/month)
                   Sign up: https://api.search.brave.com/
"""

import os
import re
import csv
import gzip
import json
import time
import glob
import boto3
import requests
import argparse
import pandas as pd
from datetime import datetime, timedelta
from pyathena import connect as athena_connect

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass


# ─────────────────────────────────────────
# CONFIG — AWS / ATHENA
# ─────────────────────────────────────────

AWS_REGION       = os.getenv("AWS_REGION", "us-east-1")
ATHENA_DATABASE  = os.getenv("ATHENA_DATABASE", "ccindex")
ATHENA_TABLE     = os.getenv("ATHENA_TABLE", "ccindex")
ATHENA_S3_OUTPUT = os.getenv("ATHENA_S3_OUTPUT", "")

# ─────────────────────────────────────────
# CONFIG — CRAWL WINDOW
# ─────────────────────────────────────────

CRAWLS_TO_USE    = 3
COLLINFO_URL     = "https://index.commoncrawl.org/collinfo.json"
COLLINFO_CACHE   = os.path.join("data", "collinfo_cache.json")

# Lever stopped being indexed by CC after 2025-47
# This crawl has the last good Lever data
LEVER_BACKFILL_CRAWL = "CC-MAIN-2025-43"

# ─────────────────────────────────────────
# CONFIG — BRAVE SEARCH API
# ─────────────────────────────────────────
# Bing Search API retired August 11, 2025.
# Replaced with Brave Search API (free tier: 1000/month).
# Sign up: https://api.search.brave.com/

BRAVE_API_KEY      = os.getenv("BRAVE_API_KEY", "")
BRAVE_ENDPOINT     = "https://api.search.brave.com/res/v1/web/search"
BRAVE_RESULTS_PAGE = 20    # Brave max = 20 per request
BRAVE_MAX_PAGES    = 50    # 50 pages × 20 = 1000 results max
BRAVE_RATE_LIMIT   = 1.0   # 1 req/sec (free tier limit)
BRAVE_QUOTA_FILE   = os.path.join("data", "brave_quota.json")
BRAVE_QUOTA_LIMIT  = 950   # hard stop at 950/1000 free tier

# Brave search queries for platforms CC misses
# Note: Brave free tier returns 422 for site: operator queries
# Use plain domain-based queries instead
BRAVE_QUERIES = {
    "lever": [
        "jobs.lever.co software engineer jobs",
        "jobs.lever.co engineering jobs",
        "jobs.lever.co product manager careers",
    ],
    "oracle_hcm": [
        "fa.oraclecloud.com hcmUI CandidateExperience jobs",
        "fa.oraclecloud.com careers apply jobs",
    ],
    "icims": [
        "careers icims.com jobs apply",
        "icims.com jobs software engineer careers",
    ],
}

# ─────────────────────────────────────────
# CONFIG — SLUG FILTERING
# ─────────────────────────────────────────

EXCLUDED_SLUGS = {
    "www", "app", "api", "secure", "portal",
    "candidate", "apply", "login", "login2",
    "sso", "hr", "talent", "auth", "developer",
    "jobs", "careers", "embed", "about", "contact",
    "privacy", "terms", "register", "search",
    "help", "support", "blog", "news", "press",
    "legal", "security", "static", "assets",
    # URL schemes — never valid slugs
    "http", "https", "ftp", "ftps", "mailto",
}

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; ATS-Slug-Builder/1.0)"
}


# ─────────────────────────────────────────
# CRAWL DISCOVERY (cached)
# ─────────────────────────────────────────

def get_recent_crawls(n=CRAWLS_TO_USE):
    """
    Fetch most recent N crawl IDs.
    Cached to disk — collinfo.json changes once/month.
    Never fetched more than once per day.
    """
    os.makedirs("data", exist_ok=True)

    # Use cache if fresh (< 24 hours old)
    if os.path.exists(COLLINFO_CACHE):
        mtime = os.path.getmtime(COLLINFO_CACHE)
        age   = time.time() - mtime
        if age < 86400:  # 24 hours
            with open(COLLINFO_CACHE) as f:
                data = json.load(f)
            crawls = data["crawls"][:n]
            print(f"[INFO] Crawl list from cache: {crawls}")
            return crawls

    # Fetch fresh
    try:
        resp = requests.get(
            COLLINFO_URL, headers=HEADERS, timeout=15
        )
        if resp.status_code == 200:
            data   = resp.json()
            ids    = [c["id"] for c in data if "id" in c]
            # Cache to disk
            with open(COLLINFO_CACHE, "w") as f:
                json.dump({"crawls": ids,
                           "fetched_at": datetime.now().isoformat()
                           }, f)
            crawls = ids[:n]
            print(f"[INFO] Sliding window: {crawls}")
            return crawls
    except Exception as e:
        print(f"[WARNING] Could not fetch collinfo.json: {e}")

    # Fallback
    print("[WARNING] Using hardcoded fallback crawls")
    return [
        "CC-MAIN-2026-08",
        "CC-MAIN-2026-04",
        "CC-MAIN-2025-51",
    ][:n]


# ─────────────────────────────────────────
# ATHENA QUERY
# ─────────────────────────────────────────

MAIN_QUERY = """
SELECT
    url,
    url_host_registered_domain,
    url_host_3rd_last_part,
    url_host_4th_last_part,
    url_host_5th_last_part,
    url_path
FROM "{database}"."{table}"
WHERE crawl  = '{crawl}'
  AND subset = 'warc'
  AND fetch_status = 200
  AND (

    -- Greenhouse: boards.greenhouse.io/{{slug}}
    --             job-boards.greenhouse.io/{{slug or numeric}}
    (
        url_host_registered_domain = 'greenhouse.io'
        AND url_host_3rd_last_part IN ('boards', 'job-boards')
    )

    -- Ashby: jobs.ashbyhq.com/{{slug}}
    OR (
        url_host_registered_domain = 'ashbyhq.com'
        AND url_host_3rd_last_part = 'jobs'
    )

    -- Workday: {{slug}}.{{wd}}.myworkdayjobs.com/{{path}}
    OR (
        url_host_registered_domain = 'myworkdayjobs.com'
        AND url_host_4th_last_part IS NOT NULL
        AND url_host_4th_last_part NOT IN (
            'jobs', 'apply', 'www', 'careers', 'secure'
        )
    )

    -- Oracle HCM: {{slug}}.fa.{{region}}.oraclecloud.com
    OR (
        url_host_registered_domain = 'oraclecloud.com'
        AND url_host_4th_last_part = 'fa'
        AND url_host_5th_last_part IS NOT NULL
        AND url_path LIKE '/hcmUI/CandidateExperience/%/sites/%'
    )

    -- iCIMS: {{slug}}.icims.com/jobs/...
    OR (
        url_host_registered_domain = 'icims.com'
        AND url_host_3rd_last_part IS NOT NULL
        AND url_host_3rd_last_part NOT IN (
            'www', 'app', 'secure', 'api',
            'developer', 'login', 'sso', 'portal'
        )
        AND url_path LIKE '/jobs/%'
    )

  )
"""

LEVER_BACKFILL_QUERY = """
SELECT
    url,
    url_host_registered_domain,
    url_host_3rd_last_part,
    url_host_4th_last_part,
    url_path
FROM "{database}"."{table}"
WHERE crawl  = '{crawl}'
  AND subset = 'warc'
  AND fetch_status = 200
  AND url_host_registered_domain = 'lever.co'
  AND url_host_3rd_last_part = 'jobs'
"""


def run_athena_query(sql, crawl_id, csv_path):
    """
    Execute Athena query and save results to local CSV.
    Deletes S3 result immediately after download.
    Logs data scanned + estimated cost.

    Returns:
        pandas DataFrame or None on failure
    """
    if not ATHENA_S3_OUTPUT:
        print("[ERROR] ATHENA_S3_OUTPUT not set in .env")
        return None

    print(f"  [ATHENA] Running query for {crawl_id}...")

    try:
        from pyathena.cursor import Cursor

        conn   = athena_connect(
            s3_staging_dir=ATHENA_S3_OUTPUT,
            region_name=AWS_REGION,
        )
        cursor = conn.cursor()
        cursor.execute(sql)

        # Fetch results into DataFrame
        rows    = cursor.fetchall()
        columns = [d[0] for d in cursor.description or []]
        df      = pd.DataFrame(rows, columns=columns)

        # ── Cost tracking ─────────────────────────────
        scanned_bytes = getattr(
            cursor, "data_scanned_in_bytes", None
        )
        if scanned_bytes:
            scanned_mb   = scanned_bytes / (1024 * 1024)
            scanned_tb   = scanned_bytes / (1024 ** 4)
            cost_usd     = scanned_tb * 5.0  # $5 per TB
            print(f"  [ATHENA] Data scanned: "
                  f"{scanned_mb:.2f} MB")
            print(f"  [ATHENA] Estimated cost: "
                  f"${cost_usd:.6f} USD")
            _log_athena_cost(crawl_id, scanned_mb, cost_usd)
        # ──────────────────────────────────────────────

        # Save locally for recovery
        os.makedirs("data", exist_ok=True)
        df.to_csv(csv_path, index=False)
        print(f"  [ATHENA] {len(df):,} rows → saved to {csv_path}")

        # Delete S3 result immediately
        _delete_s3_result(cursor)

        return df

    except Exception as e:
        print(f"  [ERROR] Athena query failed: {e}")
        return None


def _log_athena_cost(crawl_id, scanned_mb, cost_usd):
    """
    Append Athena query cost to data/athena_costs.json.
    Running total so you can track spend over time.
    """
    cost_file = os.path.join("data", "athena_costs.json")
    os.makedirs("data", exist_ok=True)

    # Load existing log
    try:
        with open(cost_file) as f:
            log = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        log = {"total_usd": 0.0, "queries": []}

    # Append this query
    log["queries"].append({
        "crawl_id":    crawl_id,
        "timestamp":   datetime.now().isoformat(),
        "scanned_mb":  round(scanned_mb, 2),
        "cost_usd":    round(cost_usd, 6),
    })
    log["total_usd"] = round(
        sum(q["cost_usd"] for q in log["queries"]), 6
    )

    with open(cost_file, "w") as f:
        json.dump(log, f, indent=2)

    print(f"  [ATHENA] Running total: "
          f"${log['total_usd']:.6f} USD "
          f"({len(log['queries'])} queries)")


def repair_athena_table():
    """
    Run MSCK REPAIR TABLE to make new crawl partitions visible.
    Called automatically when a new unscanned crawl is detected.
    Idempotent — safe to run multiple times.
    Takes ~30-60 seconds on first run, faster after.
    """
    # Athena v3 (Trino engine) uses REFRESH — not MSCK REPAIR TABLE
    sql = (f'REFRESH PARTITION METADATA '
           f'\"{ATHENA_DATABASE}\".\"{ATHENA_TABLE}\"')
    print(f"  [ATHENA] Running REFRESH PARTITION METADATA "
          f"(discovering new partitions)...")

    try:
        conn = athena_connect(
            s3_staging_dir=ATHENA_S3_OUTPUT,
            region_name=AWS_REGION,
        )
        cursor = conn.cursor()
        cursor.execute(sql)

        # Fetch result to confirm completion
        result = cursor.fetchall()
        new_partitions = [r for r in result if r] if result else []

        if new_partitions:
            print(f"  [ATHENA] {len(new_partitions)} new partition(s) "
                  f"discovered")
            for p in new_partitions[:5]:
                print(f"    {p}")
        else:
            print(f"  [ATHENA] Table partitions up to date")

        return True

    except Exception as e:
        print(f"  [WARNING] MSCK REPAIR TABLE failed: {e}")
        print(f"  [WARNING] Proceeding anyway — "
              f"new crawl may not be visible in Athena")
        return False


def _delete_s3_result(conn):
    """
    Delete specific Athena result files from S3.
    Uses query execution ID to target exact files only —
    never deletes other results in the bucket.
    """
    try:
        # Get query execution ID from pyathena connection
        query_id = getattr(conn, "query_id", None) or                    getattr(conn, "_query_id", None)

        if not query_id:
            print("  [WARNING] Could not get Athena query ID "
                  "— skipping S3 cleanup")
            return

        # Build exact S3 keys for this query only
        s3_path = ATHENA_S3_OUTPUT.rstrip("/")
        bucket  = s3_path.replace("s3://", "").split("/")[0]
        prefix  = "/".join(
            s3_path.replace("s3://", "").split("/")[1:]
        )
        prefix  = prefix.rstrip("/")

        s3   = boto3.client("s3", region_name=AWS_REGION)
        keys = [
            f"{prefix}/{query_id}.csv",
            f"{prefix}/{query_id}.csv.metadata",
        ]

        for key in keys:
            try:
                s3.delete_object(Bucket=bucket, Key=key)
            except Exception:
                pass  # file may not exist — that's fine

        print(f"  [S3] Result deleted: {query_id}.csv")

    except Exception as e:
        print(f"  [WARNING] Could not delete S3 result: {e}")


def _cleanup_old_csvs():
    """Delete local CSV files older than 2 days."""
    cutoff = datetime.now() - timedelta(days=2)
    for path in glob.glob("data/athena_*.csv"):
        mtime = datetime.fromtimestamp(os.path.getmtime(path))
        if mtime < cutoff:
            os.remove(path)
            print(f"  [CLEANUP] Deleted old CSV: {path}")


# ─────────────────────────────────────────
# SLUG EXTRACTION
# ─────────────────────────────────────────

def extract_slug(row):
    """
    Extract (platform, slug) from Athena result row.
    Uses column values directly — no regex needed.

    Returns (platform, slug) or None.
    """
    domain = str(row.get("url_host_registered_domain", "") or "")
    third  = str(row.get("url_host_3rd_last_part", "") or "")
    fourth = str(row.get("url_host_4th_last_part", "") or "")
    fifth  = str(row.get("url_host_5th_last_part", "") or "")
    path   = str(row.get("url_path", "") or "")

    # ── Greenhouse ──────────────────────────────────────
    if domain == "greenhouse.io":
        slug = path.strip("/").split("/")[0].lower()
        if not slug or slug.isdigit():
            return None  # skip numeric IDs from job-boards
        if slug in EXCLUDED_SLUGS or len(slug) < 2:
            return None
        return ("greenhouse", slug)

    # ── Ashby ────────────────────────────────────────────
    if domain == "ashbyhq.com":
        slug = path.strip("/").split("/")[0].lower()
        if not slug or slug in EXCLUDED_SLUGS or len(slug) < 2:
            return None
        return ("ashby", slug)

    # ── Lever (backfill only) ─────────────────────────────
    if domain == "lever.co":
        slug = path.strip("/").split("/")[0].lower()
        if not slug or slug in EXCLUDED_SLUGS or len(slug) < 2:
            return None
        return ("lever", slug)

    # ── Workday ──────────────────────────────────────────
    if domain == "myworkdayjobs.com":
        tenant = fourth.lower()
        wd     = third.lower()
        path_  = path.strip("/").split("/")[0]
        if not tenant or tenant in EXCLUDED_SLUGS:
            return None
        if not re.match(r'^wd[0-9]+$', wd, re.IGNORECASE):
            return None  # skip non-WD subdomains
        # Validate path_ — reject URL delimiters or scheme patterns
        if (path_ and (
            ":" in path_ or "//" in path_
            or len(path_) <= 3
            or not re.match(r'^[a-zA-Z0-9_-]+$', path_)
        )):
            path_ = ""  # store empty rather than invalid value
        return ("workday", json.dumps({
            "slug": tenant,
            "wd":   wd,
            "path": path_,
        }))

    # ── Oracle HCM ───────────────────────────────────────
    if domain == "oraclecloud.com":
        tenant = fifth.lower()
        region = third.lower()
        if not tenant or tenant in EXCLUDED_SLUGS:
            return None
        site_m  = re.search(r'/sites/([^/?#/]+)', path)
        site_id = site_m.group(1) if site_m else ""
        if not site_id:
            return None
        return ("oracle_hcm", json.dumps({
            "slug":   tenant,
            "region": region,
            "site":   site_id,
        }))

    # ── iCIMS ────────────────────────────────────────────
    if domain == "icims.com":
        slug = third.lower()
        slug = re.sub(r'^careers-', '', slug)  # strip prefix
        if not slug or slug in EXCLUDED_SLUGS or len(slug) < 2:
            return None
        return ("icims", slug)

    return None


def process_dataframe(df, crawl_id):
    """
    Extract all slugs from Athena result DataFrame.
    Returns dict: {platform: set(slugs)}
    """
    results = {}
    skipped = 0

    for _, row in df.iterrows():
        result = extract_slug(row)
        if result:
            platform, slug = result
            results.setdefault(platform, set()).add(slug)
        else:
            skipped += 1

    total = sum(len(s) for s in results.values())
    print(f"  [PARSE] {len(df):,} rows → "
          f"{total:,} valid slugs "
          f"({skipped:,} skipped)")

    for platform, slugs in sorted(results.items()):
        print(f"    {platform:<15} {len(slugs):>6,} slugs")

    return results


# ─────────────────────────────────────────
# BRAVE SEARCH FALLBACK
# ─────────────────────────────────────────

def _load_brave_quota():
    """Load Brave quota. Auto-resets on new month."""
    current_month = datetime.now().strftime("%Y-%m")
    try:
        with open(BRAVE_QUOTA_FILE) as f:
            data = json.load(f)
        if data.get("month") != current_month:
            data = {"month": current_month, "calls": 0}
            _save_brave_quota(data)
    except (FileNotFoundError, json.JSONDecodeError):
        data = {"month": current_month, "calls": 0}
    return data


def _save_brave_quota(data):
    os.makedirs("data", exist_ok=True)
    with open(BRAVE_QUOTA_FILE, "w") as f:
        json.dump(data, f)


def _brave_remaining():
    data = _load_brave_quota()
    return max(0, BRAVE_QUOTA_LIMIT - data["calls"])


def brave_search(query, offset=0):
    """
    Fetch one page of Brave Search results.
    Max 20 results per request (Brave free tier limit).
    Rate limit: 1 request/second.
    """
    if not BRAVE_API_KEY or _brave_remaining() <= 0:
        return []

    try:
        resp = requests.get(
            BRAVE_ENDPOINT,
            headers={
                "X-Subscription-Token": BRAVE_API_KEY,
                "Accept":               "application/json",
                "Accept-Encoding":      "gzip",
            },
            params={
                "q":      query,
                "count":  BRAVE_RESULTS_PAGE,
                "offset": offset,
            },
            timeout=30,
        )

        if resp.status_code == 401:
            print("  [BRAVE] Invalid API key — "
                  "check BRAVE_API_KEY in .env")
            return []
        if resp.status_code == 429:
            print("  [BRAVE] Rate limited — "
                  "waiting 60s")
            time.sleep(60)
            return []
        if resp.status_code == 422:
            print(f"  [BRAVE] Invalid query: {query}")
            return []
        if resp.status_code != 200:
            print(f"  [BRAVE] HTTP {resp.status_code}")
            return []

        # Increment quota on success only
        data = _load_brave_quota()
        data["calls"] += 1
        _save_brave_quota(data)

        # Brave response: results.web.results[].url
        web     = resp.json().get("web", {})
        results = web.get("results", [])
        return [r["url"] for r in results if "url" in r]

    except Exception as e:
        print(f"  [BRAVE] Error: {e}")
        return []


def fetch_from_brave(platforms, test_mode=False):
    """
    Fetch slugs via Brave Search for platforms CC misses.
    Lever, Oracle HCM, iCIMS — not well indexed by CC.
    Returns dict: {platform: set(slugs)}
    """
    print(f"\n{'─'*50}")
    print("SOURCE 2: Brave Search API")

    if not BRAVE_API_KEY:
        print("  [BRAVE] BRAVE_API_KEY not set — skipping")
        print("  [BRAVE] Sign up free: "
              "https://api.search.brave.com/")
        return {}

    remaining = _brave_remaining()
    quota     = _load_brave_quota()
    print(f"  [BRAVE] Quota: {quota['calls']}/{BRAVE_QUOTA_LIMIT} "
          f"used ({quota['month']}), {remaining} remaining")

    if remaining <= 0:
        print("  [BRAVE] Monthly quota exhausted — skipping")
        return {}

    results = {}

    for platform in platforms:
        queries = BRAVE_QUERIES.get(platform, [])
        if not queries:
            continue

        print(f"\n  Platform: {platform.upper()}")
        platform_slugs = set()

        for query in queries:
            if _brave_remaining() <= 0:
                print("  [BRAVE] Quota exhausted")
                break

            remaining = _brave_remaining()
            print(f"  [BRAVE] {query!r}  "
                  f"(quota: {remaining} remaining)")
            query_slugs = set()
            max_pages   = 1 if test_mode else min(
                BRAVE_MAX_PAGES, _brave_remaining()
            )

            for page in range(max_pages):
                if _brave_remaining() <= 0:
                    break

                urls = brave_search(
                    query,
                    offset=page * BRAVE_RESULTS_PAGE
                )
                if not urls:
                    break

                for url in urls:
                    result = _extract_slug_from_url(
                        url, platform
                    )
                    if result:
                        query_slugs.add(result)

                time.sleep(BRAVE_RATE_LIMIT)

            new = query_slugs - platform_slugs
            platform_slugs.update(query_slugs)
            print(f"    {len(query_slugs):,} slugs "
                  f"(+{len(new):,} new)")

        if platform_slugs:
            results[platform] = platform_slugs
            print(f"  [BRAVE] {platform}: "
                  f"{len(platform_slugs):,} total slugs")

    return results


def _extract_slug_from_url(url, platform):
    """Extract slug from a raw URL for a known platform."""
    url = url.lower()

    if platform == "lever":
        m = re.search(
            r'jobs\.lever\.co/([a-z0-9][a-z0-9\-]{1,50})',
            url
        )
        if m:
            slug = m.group(1)
            return slug if slug not in EXCLUDED_SLUGS else None

    if platform == "oracle_hcm":
        m = re.search(
            r'([a-z0-9\-]+)\.fa\.'
            r'([a-z0-9]+)\.oraclecloud\.com'
            r'/hcmui/candidateexperience'
            r'/[^/]+/sites/([^/?#/]+)',
            url
        )
        if m:
            return json.dumps({
                "slug":   m.group(1),
                "region": m.group(2),
                "site":   m.group(3),
            })

    if platform == "icims":
        m = re.search(
            r'(?:careers-)?([a-z0-9][a-z0-9\-]{1,50})'
            r'\.icims\.com/jobs',
            url
        )
        if m:
            slug = re.sub(r'^careers-', '', m.group(1))
            return slug if slug not in EXCLUDED_SLUGS else None

    return None


# ─────────────────────────────────────────
# DB SAVE + CLEANUP
# ─────────────────────────────────────────

def save_to_db(platform_slugs, crawl_id):
    """
    Save Athena-discovered slugs to ats_discovery.db.
    source='crawl' — subject to sliding window cleanup.
    Returns total new slugs inserted.
    """
    from db.schema_discovery import init_discovery_db
    from db.ats_companies import bulk_insert_slugs

    init_discovery_db()
    total_new = 0

    for platform, slugs in platform_slugs.items():
        if not slugs:
            continue
        added = bulk_insert_slugs(platform, slugs, crawl_id)
        total_new += added
        print(f"  [DB] {platform:<15} "
              f"{len(slugs):>6,} slugs  "
              f"{added:>5,} new")

    return total_new


def _save_brave_to_db(platform_slugs, crawl_id):
    """
    Save Brave Search-discovered slugs to ats_discovery.db.
    source='brave' — NEVER deleted by sliding window cleanup.
    Brave slugs are not crawl-sourced so they persist indefinitely.
    """
    from db.schema_discovery import init_discovery_db
    from db.ats_companies import get_discovery_conn

    init_discovery_db()
    total_new = 0

    conn = get_discovery_conn()
    try:
        for platform, slugs in platform_slugs.items():
            if not slugs:
                continue
            added = 0
            for slug in slugs:
                result = conn.execute("""
                    INSERT OR IGNORE INTO ats_companies
                        (platform, slug, crawl_source,
                         last_seen_crawl, source)
                    VALUES (?, ?, ?, ?, 'brave')
                """, (platform, str(slug), crawl_id, crawl_id))
                if result.rowcount > 0:
                    added += 1
            conn.commit()
            total_new += added
            print(f"  [DB] {platform:<15} "
                  f"{len(slugs):>6,} slugs  "
                  f"{added:>5,} new  (source=brave)")
    finally:
        conn.close()

    return total_new


def run_cleanup(window_crawls):
    """
    Archive and delete slugs not in sliding window.
    Only affects source='crawl' rows.
    """
    from db.ats_companies import remove_stale_crawls

    result = remove_stale_crawls(keep_crawls=window_crawls)
    if isinstance(result, tuple):
        deleted, archived = result
    else:
        deleted, archived = result, 0

    if deleted:
        print(f"\n[CLEANUP] Archived {archived:,} slugs → "
              f"data/ats_archive.csv.gz")
        print(f"[CLEANUP] Deleted {deleted:,} stale slugs "
              f"from DB (not in last {CRAWLS_TO_USE} crawls)")
    else:
        print(f"\n[CLEANUP] No stale slugs to remove")


# ─────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Build ATS slug list via Athena + Brave Search"
    )
    parser.add_argument(
        "--test",
        action="store_true",
        help="Dry run — skip Athena, use local CSV if available"
    )
    parser.add_argument(
        "--backfill",
        action="store_true",
        help="One-time Lever backfill from CC-MAIN-2025-43"
    )
    parser.add_argument(
        "--from-csv",
        default=None,
        metavar="PATH",
        help="Load from local CSV instead of querying Athena"
    )
    parser.add_argument(
        "--skip-brave",
        action="store_true",
        help="Skip Brave search"
    )
    args = parser.parse_args()

    print("ATS Slug List Builder")
    print("=" * 50)
    print(f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    from db.schema_discovery import init_discovery_db
    from db.ats_companies import (
        get_unscanned_crawls, mark_crawl_scanned,
        get_cache_hit_stats,
    )

    init_discovery_db()
    _cleanup_old_csvs()

    # ── BACKFILL MODE ────────────────────────────────────
    if args.backfill:
        _run_backfill(args)
        return

    # ── GET SLIDING WINDOW ───────────────────────────────
    window = get_recent_crawls(n=CRAWLS_TO_USE)
    print(f"Sliding window: {window}")

    # ── FIND UNSCANNED CRAWLS ────────────────────────────
    unscanned = get_unscanned_crawls(window)

    if not unscanned:
        print(f"\n[OK] All crawls already scanned: {window}")
        print("[OK] Nothing to do — DB is up to date")
        _print_stats()
        return

    print(f"\nNew crawls to scan: {unscanned}")
    print(f"Already scanned:    "
          f"{[c for c in window if c not in unscanned]}")

    # ── SOURCE 1: ATHENA ─────────────────────────────────
    print(f"\n{'─'*50}")
    print("SOURCE 1: AWS Athena")

    all_slugs = {}  # {platform: set(slugs)}

    # Register new crawl partitions in Glue catalog
    # Ensures new crawl partitions are visible to Athena
    if unscanned and not args.test and not args.from_csv:
        repair_athena_table(crawl_ids=unscanned)

    for crawl_id in unscanned:
        csv_path = (f"data/athena_"
                    f"{crawl_id}_"
                    f"{datetime.now().strftime('%Y-%m-%d')}"
                    f".csv")

        # Use local CSV if --from-csv or file exists
        if args.from_csv and os.path.exists(args.from_csv):
            print(f"  [CSV] Loading from {args.from_csv}")
            df = pd.read_csv(args.from_csv)
        elif os.path.exists(csv_path) and not args.test:
            print(f"  [CSV] Found existing: {csv_path}")
            df = pd.read_csv(csv_path)
        elif args.test:
            print(f"  [TEST] Skipping Athena for {crawl_id}")
            continue
        else:
            sql = MAIN_QUERY.format(
                database=ATHENA_DATABASE,
                table=ATHENA_TABLE,
                crawl=crawl_id,
            )
            df = run_athena_query(sql, crawl_id, csv_path)
            if df is None:
                print(f"  [ERROR] Query failed for {crawl_id}")
                continue

        # Extract slugs from results
        crawl_slugs = process_dataframe(df, crawl_id)

        # Merge into all_slugs
        for platform, slugs in crawl_slugs.items():
            all_slugs.setdefault(platform, set()).update(slugs)

        # Save to DB
        print(f"\n  Saving {crawl_id} to DB:")
        total_new = save_to_db(crawl_slugs, crawl_id)

        # Mark crawl as scanned
        total_found = sum(len(s) for s in crawl_slugs.values())
        mark_crawl_scanned(
            crawl_id,
            slugs_found=total_found,
            slugs_new=total_new,
        )
        print(f"  [OK] {crawl_id} marked as scanned "
              f"({total_found:,} slugs, {total_new:,} new)")

    # ── SOURCE 2: BRAVE ──────────────────────────────────
    if not args.skip_brave:
        # Brave fills gaps: Lever + Oracle + iCIMS
        brave_platforms = ["lever", "oracle_hcm", "icims"]
        brave_slugs    = fetch_from_brave(
            brave_platforms, test_mode=args.test
        )

        if brave_slugs:
            print("\n  Saving Brave results to DB:")
            # Use source='brave' so sliding window cleanup never deletes
            # these rows — they are not crawl-sourced
            latest_crawl = window[0] if window else "unknown"
            _save_brave_to_db(brave_slugs, latest_crawl)

    # ── CLEANUP: ARCHIVE + DELETE STALE ─────────────────
    if not args.test:
        run_cleanup(window)

    # ── FINAL STATS ──────────────────────────────────────
    _print_stats()


def _run_backfill(args):
    """
    One-time Lever backfill from CC-MAIN-2025-43.
    Lever stopped being indexed after 2025-47.
    Run once: python build_ats_slug_list.py --backfill
    """
    from db.schema_discovery import init_discovery_db
    from db.ats_companies import (
        bulk_insert_slugs, mark_crawl_scanned,
        get_scanned_crawls,
    )

    print(f"\nLever Backfill from {LEVER_BACKFILL_CRAWL}")
    print("─" * 50)

    # Skip if already done
    scanned = get_scanned_crawls()
    backfill_key = f"backfill-{LEVER_BACKFILL_CRAWL}"
    if backfill_key in scanned:
        print("[OK] Lever backfill already completed — skipping")
        return

    csv_path = f"data/athena_lever_backfill.csv"

    # Use existing CSV if available
    if os.path.exists(csv_path):
        print(f"[CSV] Loading from {csv_path}")
        df = pd.read_csv(csv_path)
    elif args.test:
        print("[TEST] Skipping Athena backfill")
        return
    else:
        sql = LEVER_BACKFILL_QUERY.format(
            database=ATHENA_DATABASE,
            table=ATHENA_TABLE,
            crawl=LEVER_BACKFILL_CRAWL,
        )
        df = run_athena_query(sql, LEVER_BACKFILL_CRAWL, csv_path)
        if df is None:
            print("[ERROR] Backfill query failed")
            return

    # Extract Lever slugs
    slugs = set()
    for _, row in df.iterrows():
        result = extract_slug(row)
        if result and result[0] == "lever":
            slugs.add(result[1])

    print(f"[BACKFILL] {len(slugs):,} Lever slugs found")

    # Insert with source='backfill'
    # backfill slugs are NEVER deleted by sliding window
    from db.ats_companies import get_discovery_conn
    conn  = get_discovery_conn()
    added = 0
    try:
        for slug in slugs:
            result = conn.execute("""
                INSERT OR IGNORE INTO ats_companies
                    (platform, slug, crawl_source,
                     last_seen_crawl, source)
                VALUES ('lever', ?, ?, ?, 'backfill')
            """, (slug, LEVER_BACKFILL_CRAWL,
                  LEVER_BACKFILL_CRAWL))
            if result.rowcount > 0:
                added += 1
        conn.commit()
    finally:
        conn.close()

    # Mark as done
    mark_crawl_scanned(
        backfill_key,
        slugs_found=len(slugs),
        slugs_new=added,
        query_type="backfill",
    )

    print(f"[BACKFILL] {added:,} new Lever slugs inserted "
          f"(source=backfill, never deleted)")
    print(f"[BACKFILL] Done — run once only")


def _print_stats():
    """Print DB stats after run."""
    from db.ats_companies import get_stats, get_cache_hit_stats

    print(f"\n{'='*50}")
    print("DB STATS")
    print(f"{'='*50}")

    stats = get_stats()
    for row in stats:
        print(f"  {row['platform']:<15} "
              f"{row['total']:>6,} total  "
              f"{row['enriched']:>5,} enriched  "
              f"{row['detected']:>4,} detected")

    hit_stats = get_cache_hit_stats()
    print(f"\n  Total active:    {hit_stats['total']:,}")
    print(f"  Enriched:        {hit_stats['enriched']:,} "
          f"({hit_stats['enriched_pct']}%)")
    print(f"  Crawls scanned:  {hit_stats['crawls_scanned']}")
    print(f"  Archive size:    "
          f"{hit_stats['archive_size_kb']:,} KB")

    print(f"\nNext steps:")
    print(f"  1. Enrich company names:")
    print(f"     python enrich_ats_companies.py")
    print(f"  2. Run detection:")
    print(f"     python pipeline.py --detect-ats --batch")


if __name__ == "__main__":
    main()