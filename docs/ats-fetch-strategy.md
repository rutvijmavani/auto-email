# ATS Fetch Strategy

## Overview

Each job board platform (ATS) has different API designs, pagination behaviors, date fields, and data availability. This document defines exactly how the adaptive polling engine should interact with each platform — what data to fetch at listing time, when to fetch full detail, and how to determine whether a job is genuinely new or updated.

---

## Table of Contents

1. [The Two Fetch Modes](#1-the-two-fetch-modes)
2. [Per-Platform Strategy Table](#2-per-platform-strategy-table)
3. [Platform Details](#3-platform-details)
4. [Smart Early Exit — Which Platforms Support It](#4-smart-early-exit--which-platforms-support-it)
5. [The ATS Config Reference](#5-the-ats-config-reference)

---

## 1. The Two Fetch Modes

Every ATS platform falls into one of two categories for Tier 1 (listing scan) polling:

### Mode A — Listing-Complete

The listing endpoint returns **all the data we need** in one call: job ID, title, location, description, and posting date. No separate detail page fetch is needed.

This is the ideal case: one HTTP request per poll, all data available.

**Platforms in Mode A:**
Greenhouse, Lever, Ashby, SmartRecruiters, ADP, SuccessFactors (XML), Jobvite

### Mode B — ID-Only (Detail Required)

The listing endpoint returns only job IDs and titles. Location, description, and posting date are only available on the individual job's detail page. We must make a separate HTTP request per job to get the full data.

In Mode B, Tier 1 polling fetches only IDs. The incremental filter identifies which IDs are new. Only new IDs trigger detail fetches. Already-seen IDs are skipped entirely.

**Platforms in Mode B:**
Workday, iCIMS, Taleo, Eightfold

**Why does this matter?** In Mode B, a company with 5,000 jobs does NOT require 5,000 HTTP requests per poll. It requires 1 listing call (to get all IDs) plus N detail calls (where N = number of new jobs since last poll). After day 1, N is typically 0–5.

---

## 2. Per-Platform Strategy Table

| Platform | Mode | Pagination | Sorted by Recency | Date Field | Detail Needed For | Updated_at Available | REFRESH_WINDOW |
|----------|------|-----------|-------------------|------------|-------------------|---------------------|----------------|
| Greenhouse | A | paginated (per_page=500) | ✓ Yes | `first_published` | Nothing (all data at listing) | ✓ `updated_at` | 72 hours |
| Lever | A | paginated | ✓ Yes | `createdAt` | Nothing | ✗ No | N/A |
| Ashby | A | single call | ✓ Yes | `publishedAt` | Nothing | ✓ `updatedAt` | 48 hours |
| SmartRecruiters | A | paginated (pageSize=100) | ✓ Yes | `releasedDate` | Nothing | ✓ `updatedAt` | 48 hours |
| ADP | A | paginated ($top/$skip) | ✗ No | `postingDate` (often None) | Nothing | ✗ No | N/A |
| SuccessFactors | A | single XML | ✗ N/A (one call) | `Posted-Date` | Nothing | ✗ No | N/A |
| Jobvite | A | single XML | ✗ No | `date-added` | Nothing | ✗ No | N/A |
| Workday | B | paginated | ✗ Unreliable | `postedOn` (listing) | Location, description | ✗ No | N/A |
| iCIMS | B | paginated (pr=0,1,2...) | ✗ No | Parsed from detail page | Everything (date, loc, desc) | ✗ No | N/A |
| Eightfold | B | paginated (GraphQL) | ✓ Yes | `updatedAt` | Location, description | ✓ `updatedAt` | 48 hours |
| Taleo | B | paginated (cap=50 pages) | ✗ No | `requisitionDate` | Description | ✗ No | N/A |
| Oracle HCM | A | paginated (limit=100) | ✓ Yes | `PostedDate` | Nothing | ✗ No | N/A |
| TalentBrew | A | paginated | ✗ No | `datePosted` | Nothing | ✗ No | N/A |
| Phenom | A | paginated | ✗ No | `publishDate` | Nothing | ✗ No | N/A |
| Avature | A | sitemap | ✗ No | parsed | Nothing | ✗ No | N/A |
| BambooHR | A | single call | ✗ No | `datePosted` | Nothing | ✗ No | N/A |

---

## 3. Platform Details

### Greenhouse

**What the listing returns:**
```json
{
  "id": 4567890,
  "title": "Senior Software Engineer",
  "first_published": "2026-04-10T14:23:00.000Z",
  "updated_at": "2026-04-22T09:15:00.000Z",
  "location": {"name": "New York, NY"},
  "absolute_url": "https://boards.greenhouse.io/stripe/jobs/4567890",
  "content": "<p>Full job description HTML...</p>"
}
```

**Fetch strategy:**
- Tier 1: Fetch all listings (single call, `?content=true` returns descriptions too)
- No detail fetch needed
- `first_published` = original posting date. **Never use `updated_at` for freshness.**
- `updated_at` = last edit date. Use for REFRESH_WINDOW check: if `updated_at > last_processed + 72hr`, re-process the job in case requirements changed meaningfully.

**Pagination:** `GET /v1/boards/{slug}/jobs?content=true`
Returns all jobs in one call (no pagination needed — Greenhouse returns everything at once).

**The updated_at problem:**
Greenhouse recruiters frequently edit job descriptions to update salary ranges, add new requirements, or fix typos. Each edit updates `updated_at`. A job posted 6 months ago with `updated_at` yesterday would appear as "new" if we used `updated_at` for freshness — which is wrong. Always use `first_published`.

---

### Lever

**What the listing returns:**
```json
{
  "id": "a1b2c3d4-...",
  "text": "Senior Backend Engineer",
  "createdAt": 1714000000000,
  "categories": {"location": "San Francisco, CA"},
  "descriptionPlain": "We are looking for...",
  "hostedUrl": "https://jobs.lever.co/stripe/a1b2c3d4-..."
}
```

**Fetch strategy:**
- Tier 1: Fetch all listings (`?mode=json`)
- No detail fetch needed
- `createdAt` = Unix timestamp in milliseconds. Convert to datetime. **This never changes.**
- No `updated_at` field in Lever's public API. Cannot detect job edits.

**Pagination:** `GET /v0/postings/{slug}?mode=json&limit=100&offset={offset}`
Paginate until empty page returned.

---

### Ashby

**What the listing returns:**
```json
{
  "id": "abc-123",
  "title": "Staff Engineer, Infrastructure",
  "publishedAt": "2026-04-15T10:00:00Z",
  "updatedAt": "2026-04-20T14:30:00Z",
  "location": {"name": "Remote"},
  "descriptionHtml": "<p>About this role...</p>",
  "jobUrl": "https://jobs.ashbyhq.com/linear/abc-123"
}
```

**Fetch strategy:**
- Tier 1: Fetch all listings (single GraphQL query)
- No detail fetch needed
- `publishedAt` = original publish date. Use for freshness.
- `updatedAt` available — use for REFRESH_WINDOW (48 hours).

---

### Workday

**What the listing returns:**
```json
{
  "title": "Senior Software Engineer",
  "postedOn": "Posted 3 Days Ago",
  "locationsText": "New York, New York, USA",
  "externalPath": "/job/New-York/Senior-Software-Engineer_R164560"
}
```

**Fetch strategy:**
- Tier 1: Fetch all listing pages (title + job_id extracted from URL suffix)
- `postedOn` is available at listing level but is a human-readable string ("Posted Today", "Posted 3 Days Ago") — parse it
- Description NOT available at listing level → Mode B (detail required for new jobs)
- No `updated_at` field → cannot detect edits, no REFRESH_WINDOW

**Pagination:** `POST {tenant_url}/wday/cxs/{company}/{path}/jobs`
Body: `{"limit": 20, "offset": 0, "searchText": ""}`
Continue until `len(page) < limit`.

**Critical note:** Full browser headers required. Using a plain User-Agent causes pagination to break after page 1 — pages 2+ return empty results. This appears to be Workday's bot detection. Always use the complete headers documented in `workday.py`.

**Job ID extraction:**
Job ID is embedded in the URL path, not returned as a separate field:
- `/job/New-York/Senior-SWE_R164560` → job_id = `R164560`
- `/job/Austin/PM-Lead_JR-0104946` → job_id = `JR-0104946`
Pattern: `_([A-Z]+-?[\d]+)$` or `_([A-Z][\d]+)$` at end of path.

---

### iCIMS

**What the listing returns:**
```html
<a class="iCIMS_Anchor" href="/jobs/12345/software-engineer/job?in_iframe=1">
  Job Title
  Software Engineer
</a>
```

**Fetch strategy:**
- Tier 1: Fetch all listing pages, extract job IDs from `a.iCIMS_Anchor` href
- Title is in the anchor text (after stripping "Job Title\n" label)
- ALL other data (date, location, description) requires a detail page fetch → Mode B
- `posted_at` extracted from detail page body: "Posted Date 3 days ago (05/03/2026 13:46)"

**URL format detection:**
iCIMS tenants use two subdomain formats — the stored slug does not tell us which:
- `careers-{slug}.icims.com` (most common)
- `{slug}.icims.com` (some tenants)

At fetch time, probe both variants. Use whichever returns 200. Cache the working variant in Redis for subsequent polls (avoid re-probing every time).

**Pagination:** `GET /jobs/search?pr={page}&in_iframe=1`
Continue until page returns no `a.iCIMS_Anchor` elements.

**Exit detection (iCIMS migration):**
Some companies that previously used iCIMS have migrated away. They redirect via JavaScript:
```javascript
window.top.location.href = 'https://careers.amd.com/jobs'
```
Detect this: if response body is <500 bytes AND contains an off-icims.com redirect → treat as dead, trigger ATS re-detection.

---

### Eightfold (Amazon, Starbucks)

**What the listing returns:**
```json
{
  "id": "5678901",
  "title": "Software Development Engineer",
  "location": "Seattle, Washington",
  "updatedAt": "2026-04-23T12:00:00Z",
  "department": "Engineering"
}
```

**Fetch strategy:**
- Tier 1: Fetch listing pages (GraphQL API, sorted newest first by default)
- `updatedAt` available at listing level → use for REFRESH_WINDOW (48 hours)
- Full description requires detail page → Mode B for new jobs only
- Smart early exit applicable (sorted by recency ✓)

**Why Eightfold matters:**
Amazon and Starbucks both use Eightfold with 10,000–20,000+ active job listings. Without smart early exit, Tier 1 would need to fetch hundreds of pages on every poll. With smart early exit (80% overlap threshold, 2-page confirm), it typically exits after 2–5 pages once the company's active listings are known.

---

### ADP WorkforceNow

**What the listing returns:**
```json
{
  "reqId": "R0012345",
  "publishedJobTitle": "Senior Software Engineer",
  "postingDate": "2026-04-23T16:09:11Z",
  "requisitionLocations": [...],
  "jobDescription": "<p>About the role...</p>",
  "jobQualifications": "<p>Requirements...</p>"
}
```

**Fetch strategy:**
- Tier 1: Fetch all pages (OData `$top=100`, `$skip` pagination)
- Full description available at listing level → Mode A (no detail needed)
- `postingDate` is in the schema but ADP often does not return it → `posted_at = None` is normal
- Requires curl_cffi with Chrome impersonation to bypass Akamai Bot Manager
- Session warm-up required: visit career page first to seed `ak_bmsc`/`bm_sv` cookies

**Critical — Akamai Bot Manager:**
ADP uses Akamai Bot Manager which silently returns `count: 0` for non-browser requests. Plain `requests` library returns empty results from server environments. Must use `curl_cffi` with Chrome TLS fingerprint. See `jobs/ats/adp.py` for the session warm-up pattern.

**Country codes:**
ADP returns ISO alpha-3 codes ("USA", "CAN"). Convert to alpha-2 ("US", "CA") using `alpha3_to_alpha2()` from `jobs/ats/base.py`.

---

### SAP SuccessFactors

**What the listing returns (XML):**
```xml
<Job>
  <JobTitle>Senior Software Engineer</JobTitle>
  <ReqId>782174</ReqId>
  <Posted-Date>03/29/2026</Posted-Date>
  <Job-Description><![CDATA[<p>About this role...</p>]]></Job-Description>
  <Location>San Jose, CA, USA Office (SANJOSE)</Location>
</Job>
```

**Fetch strategy:**
- Single XML feed returns ALL jobs at once (no pagination)
- Full description available in XML → Mode A (no detail needed)
- `Posted-Date` in MM/DD/YYYY format → parse directly
- Smart early exit N/A (single call returns everything)

**Path variants:**
SuccessFactors has two path variants that look identical but differ:
- `/career` — used by most tenants (Ericsson, NetApp)
- `/careers` — used by SAP itself and some others

**Self-healing:** `fetch_jobs()` tries the stored path first. If the response is HTML (not XML), it automatically tries the other path. The path that works is stored in the effective `slug_info` passed to `_normalize()`, ensuring job URLs are built correctly regardless of which path succeeded.

**Slug info format stored in DB:**
```json
{"slug": "Ericsson", "dc": "2", "region": "eu"}
{"slug": "netappinc", "dc": "4", "region": "com"}
{"slug": "SAP", "dc": "5", "region": "eu", "path": "/careers"}
```
`"path"` is only stored when non-default (`/careers`). Missing `"path"` key = use `/career`.

---

### Oracle HCM Cloud

**What the listing returns:**
```json
{
  "Id": 100189,
  "Title": "Software Engineer - Cloud Infrastructure",
  "PostedDate": "2026-04-18",
  "PrimaryLocation": "New York,New York,United States"
}
```

**Fetch strategy:**
- Fetch all pages (`limit=100`, `offset` pagination)
- Full location available → Mode A (description not at listing level, but acceptable)
- `PostedDate` is reliable original posting date
- `Id` (capital I) is the public-facing job ID. **NOT `ExternalJobId` or `RequisitionId`** — these are NULL on most Oracle tenants

**Pagination quirk:**
Oracle wraps jobs in `items[0].requisitionList`. The `hasMore` field is always `False` — **never use it for pagination**. Instead, stop when `len(page_jobs) < limit`.

---

## 4. Smart Early Exit — Which Platforms Support It

Smart early exit requires the listing to be sorted newest-first. Only use the overlap-ratio exit algorithm on these platforms:

| Platform | Sorted newest-first? | Smart exit applicable? |
|----------|---------------------|----------------------|
| Greenhouse | ✓ Default | ✓ Yes |
| Lever | ✓ Default | ✓ Yes |
| Ashby | ✓ Default | ✓ Yes |
| SmartRecruiters | ✓ Default | ✓ Yes |
| Eightfold | ✓ Default (`sort=date`) | ✓ Yes |
| Oracle HCM | ✓ (`sortBy=POSTING_DATES_DESC` in URL) | ✓ Yes |
| Workday | ✗ Unreliable | ✗ No |
| iCIMS | ✗ No guaranteed order | ✗ No |
| Taleo | ✗ Varies by tenant | ✗ No |
| ADP | ✗ No | ✗ No |
| SuccessFactors | ✗ N/A (single XML) | ✗ N/A |

**For platforms without recency sort:**
Use the simpler fallback: if an entire page has 0 new job IDs (100% overlap), stop paginating. This is less precise but still avoids fetching all 200+ pages when only 2 pages have changed.

**The algorithm in full:**

```python
def should_continue_paginating(page_jobs, seen_ids, overlap_pages,
                                sorted_by_recency, page_size):
    if not page_jobs:
        return False, overlap_pages  # empty page = genuine end of results

    seen_count    = sum(1 for j in page_jobs if j.job_id in seen_ids)
    overlap_ratio = seen_count / len(page_jobs)

    # Platform sorts newest first → safe to use overlap threshold
    if sorted_by_recency:
        THRESHOLD      = 0.80   # 80% of page is already-seen jobs
        CONFIRM_PAGES  = 2      # need 2 consecutive high-overlap pages to stop
        if overlap_ratio >= THRESHOLD:
            overlap_pages += 1
            if overlap_pages >= CONFIRM_PAGES:
                return False, overlap_pages   # clearly past new-job frontier
        else:
            overlap_pages = 0  # found new jobs → reset, keep going

    # Platform has no sort guarantee → simpler full-page check
    else:
        if overlap_ratio == 1.0:  # entire page is already known
            overlap_pages += 1
            if overlap_pages >= 1:  # only need 1 all-seen page to stop (less precise)
                return False, overlap_pages
        else:
            overlap_pages = 0

    return True, overlap_pages  # keep paginating
```

---

## 5. The ATS Config Reference

This is the canonical configuration used by `pipeline/ats_config.py`:

```python
# pipeline/ats_config.py

ATS_TIER1_CONFIG = {
    #
    # Each entry defines how the adaptive engine should interact with one platform.
    #
    # Keys:
    #   detail_needed      (bool)  — True if Tier 1 listing lacks full data (Mode B)
    #   sorted_by_recency  (bool)  — True if results are newest-first (smart exit ok)
    #   updated_at_field   (str)   — Field name for last-edit timestamp at listing level
    #                                None = not available
    #   refresh_window_hr  (int)   — Re-fetch detail if updated_at is N hours newer
    #                                than our last_updated. None = no refresh.
    #   date_field         (str)   — Field name for original posting date
    #   date_format        (str)   — strptime format or "iso" or "workday_human"
    #   page_size          (int)   — Items per page for paginated platforms
    #
    "greenhouse": {
        "detail_needed":     False,
        "sorted_by_recency": True,
        "updated_at_field":  "updated_at",
        "refresh_window_hr": 72,
        "date_field":        "first_published",
        "date_format":       "iso",
        "page_size":         500,
    },
    "lever": {
        "detail_needed":     False,
        "sorted_by_recency": True,
        "updated_at_field":  None,
        "refresh_window_hr": None,
        "date_field":        "createdAt",
        "date_format":       "unix_ms",
        "page_size":         100,
    },
    "ashby": {
        "detail_needed":     False,
        "sorted_by_recency": True,
        "updated_at_field":  "updatedAt",
        "refresh_window_hr": 48,
        "date_field":        "publishedAt",
        "date_format":       "iso",
        "page_size":         None,  # single call
    },
    "smartrecruiters": {
        "detail_needed":     False,
        "sorted_by_recency": True,
        "updated_at_field":  "updatedAt",
        "refresh_window_hr": 48,
        "date_field":        "releasedDate",
        "date_format":       "iso",
        "page_size":         100,
    },
    "adp": {
        "detail_needed":     False,
        "sorted_by_recency": False,
        "updated_at_field":  None,
        "refresh_window_hr": None,
        "date_field":        "postingDate",
        "date_format":       "iso",
        "page_size":         100,
    },
    "successfactors": {
        "detail_needed":     False,
        "sorted_by_recency": False,   # single XML dump, N/A
        "updated_at_field":  None,
        "refresh_window_hr": None,
        "date_field":        "Posted-Date",
        "date_format":       "%m/%d/%Y",
        "page_size":         None,    # single XML call
    },
    "workday": {
        "detail_needed":     True,
        "sorted_by_recency": False,
        "updated_at_field":  None,
        "refresh_window_hr": None,
        "date_field":        "postedOn",
        "date_format":       "workday_human",
        "page_size":         20,
    },
    "icims": {
        "detail_needed":     True,
        "sorted_by_recency": False,
        "updated_at_field":  None,
        "refresh_window_hr": None,
        "date_field":        None,    # only available on detail page
        "date_format":       None,
        "page_size":         None,    # iCIMS paginates by pr=0,1,2...
    },
    "oracle_hcm": {
        "detail_needed":     False,
        "sorted_by_recency": True,
        "updated_at_field":  None,
        "refresh_window_hr": None,
        "date_field":        "PostedDate",
        "date_format":       "%Y-%m-%d",
        "page_size":         100,
    },
    "eightfold": {
        "detail_needed":     True,
        "sorted_by_recency": True,
        "updated_at_field":  "updatedAt",
        "refresh_window_hr": 48,
        "date_field":        "updatedAt",
        "date_format":       "iso",
        "page_size":         20,
    },
    "taleo": {
        "detail_needed":     True,
        "sorted_by_recency": False,
        "updated_at_field":  None,
        "refresh_window_hr": None,
        "date_field":        "requisitionDate",
        "date_format":       "iso",
        "page_size":         25,
    },
    "jobvite": {
        "detail_needed":     False,
        "sorted_by_recency": False,
        "updated_at_field":  None,
        "refresh_window_hr": None,
        "date_field":        "date-added",
        "date_format":       "iso",
        "page_size":         None,    # single XML feed
    },
    "talentbrew": {
        "detail_needed":     False,
        "sorted_by_recency": False,
        "updated_at_field":  None,
        "refresh_window_hr": None,
        "date_field":        "datePosted",
        "date_format":       "iso",
        "page_size":         100,
    },
    "phenom": {
        "detail_needed":     False,
        "sorted_by_recency": False,
        "updated_at_field":  None,
        "refresh_window_hr": None,
        "date_field":        "publishDate",
        "date_format":       "iso",
        "page_size":         20,
    },
    "bamboohr": {
        "detail_needed":     False,
        "sorted_by_recency": False,
        "updated_at_field":  None,
        "refresh_window_hr": None,
        "date_field":        "datePosted",
        "date_format":       "iso",
        "page_size":         None,    # single JSON call
    },
}
```

### How the config is used

During each Tier 1 poll:

```python
config = ATS_TIER1_CONFIG[company.ats_platform]

# Fetch listing (pagination handled by smart_paginate)
jobs = smart_paginate(company, config)

# For each job in the listing:
for job in jobs:
    if job.job_id in seen_ids[company]:
        # Already seen this job
        if config["updated_at_field"]:
            job_updated_at = job[config["updated_at_field"]]
            last_known_updated = seen_ids[company][job.job_id].last_updated
            if (job_updated_at - last_known_updated).hours > config["refresh_window_hr"]:
                enqueue_detail_refresh(job)  # re-fetch — job meaningfully changed
        continue  # skip, already processed

    # Genuinely new job
    if config["detail_needed"]:
        enqueue_detail_fetch(job)    # Mode B: need detail page for full data
    else:
        save_job_immediately(job)    # Mode A: all data available now
        mark_as_seen(job.job_id)
```

### Per-company overrides

The `company_config` table can override any of these settings for a specific company. For example:

```sql
-- Force Workday for a specific tenant that sorts by recency (atypical)
INSERT INTO company_config (company, sorted_by_recency) 
VALUES ('Unusual Corp', TRUE);

-- Pin a critical company to minimum 15-minute polling
INSERT INTO company_config (company, max_interval) 
VALUES ('Dream Company Inc', 900);
```

Override lookup order: `company_config` → `ATS_TIER1_CONFIG` → defaults.
