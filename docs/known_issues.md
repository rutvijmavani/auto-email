# Known Issues & Future Improvements

Tracked bugs, limitations, and planned improvements.
Updated as issues are discovered or resolved.

---

## Status Legend
```
🔴 Bug        — incorrect behaviour, needs fixing
🟡 Limitation — works but not optimally
🟢 Improvement — enhancement, not blocking
⚪ Resolved   — fixed, kept for reference
```

---

## ATS Discovery (`build_ats_slug_list.py`)

### 🔴 MSCK REPAIR TABLE not supported in Athena v3
**Discovered:** 2026-03-11
**Symptom:**
```
[WARNING] MSCK REPAIR TABLE failed: An error occurred
(InvalidRequestException) ... mismatched input 'MSCK'
```
**Root cause:** Athena v3 uses Trino engine which doesn't
support `MSCK REPAIR TABLE`. `REFRESH PARTITION METADATA`
also rejected by SageMaker Unified Studio interface.
**Current workaround:** Non-fatal — Common Crawl registers
their own partitions in Glue automatically. New crawl
partitions are visible to Athena without manual repair.
Code falls through with warning and queries succeed.
**Proper fix:** Use boto3 Glue `create_partition` API to
register specific crawl partitions. Code updated to use
this approach but needs verification on next monthly run.
**Impact:** Low — queries work fine despite warning.
**Files:** `build_ats_slug_list.py` → `repair_athena_table()`

---

### 🟡 Brave Search `site:` operator returns 422
**Discovered:** 2026-03-11
**Symptom:**
```
[BRAVE] Invalid query: site:jobs.lever.co
```
**Root cause:** Brave free tier API does not support the
`site:` search operator — returns HTTP 422.
**Fix applied:** Replaced `site:` queries with plain domain
queries e.g. `"jobs.lever.co software engineer jobs"`.
**Remaining issue:** Plain queries return fewer and less
precise results than `site:` would. Lever slug coverage
may be lower than optimal.
**Better fix:** Upgrade to Brave paid tier ($3/1000 queries)
which supports `site:` operator, OR switch to SerpAPI
(100 free searches/month) which supports all operators.
**Impact:** Medium — Lever/Oracle/iCIMS slug coverage reduced.
**Files:** `build_ats_slug_list.py` → `BRAVE_QUERIES`

---

## Enrichment (`enrich_ats_companies.py`)

### 🔴 Phase A priority enrichment only finds ~6 companies
**Discovered:** 2026-03-11
**Symptom:**
```
[Phase A] Priority enrichment: 6 prospect-matching slugs
```
Expected: ~50-80 matches from 134 prospects.
**Root cause:** Phase A matches by `company_name` field
which is NULL for all unenriched slugs (`is_enriched=0`).
Slug-only matching is too strict — requires exact
`normalize(slug) == normalize(prospect_name)` match.
Large companies like Google, Apple, Microsoft ARE in the
DB as slugs but company_name is NULL → name match fails.
**Fix needed:**
```python
# Current: match by company_name (NULL for unenriched)
# Fix: match by slug directly against prospect names
# e.g. slug='stripe' should match prospect 'Stripe'
# regardless of company_name being populated
```
Query unenriched slugs and match slug values against
normalized prospect names + common slug variants
(e.g. "capitalone" → "Capital One").
**Impact:** Medium — Phase A benefit delayed, Phase B
covers it over time, Phase 2 detection unaffected.
**Files:** `enrich_ats_companies.py` → `run_priority_enrichment()`

---

### 🔴 Ashby enricher returns wrong company name
**Discovered:** 2026-03-11
**Symptom:**
```
greenhouse  purestorage  OK  Everpure
```
`purestorage` slug on Greenhouse returns "Everpure" —
a different company using the same slug.
**Root cause (Ashby):** Ashby public API (`/posting-api/job-board/{slug}`)
does not include `jobBoard.name` in response — only
returns `jobs` array. Previous code looked for
`jobBoard.name` which is never present.
**Fix applied:** 3-tier extraction:
1. `organizationName` field on job objects
2. HTML page title scrape from `jobs.ashbyhq.com/{slug}`
3. Fall back to `slug.title()`
**Remaining issue (Greenhouse slug collision):**
`purestorage` slug is used by "Everpure" not "Pure Storage"
on Greenhouse. Pure Storage uses a different slug.
This is a slug collision — same slug, different company.
**Fix needed for slug collision:**
```python
# After enrichment, validate company name matches
# expected prospect name using _name_matches()
# If mismatch → mark as wrong slug, try variants
# e.g. "purestorage" → try "pure-storage", "pstg" etc.
```
**Impact:** Low — wrong company name stored for one slug.
Detection phase (Phase 2) independently verifies correct
company via direct API probe.
**Files:** `enrich_ats_companies.py` → `enrich_ashby()`
`db/ats_companies.py` → post-enrichment validation

---

### 🟡 Ashby company name extraction falls back to slug.title()
**Discovered:** 2026-03-11
**Symptom:** Companies with no jobs posted return
slug-derived names e.g. "Snowflake" from "snowflake".
**Root cause:** No jobs in API response → can't extract
`organizationName`. HTML scrape works but adds latency.
**Fix:** Already improved with HTML fallback scrape.
Acceptable for now.
**Impact:** Low — slug.title() is usually correct for
well-known companies.

---

## Job Monitoring (`pipeline.py --monitor-jobs`)

### 🟢 Adaptive scheduling not yet implemented
**Planned:** When company count reaches 300+
**Description:** Currently all companies checked every day.
At 300+ companies, implement adaptive scheduling:
- Active companies (recent jobs): check daily
- Quiet companies (14+ empty days): check every 3 days
- Never-had-jobs companies: check weekly
**Benefit:** Cuts daily API calls by ~60% at scale.
**Files:** `db/job_monitor.py` → `get_monitorable_companies()`
`jobs/job_monitor.py` → main monitoring loop

---

### 🟢 Single Oracle VM — plan for 750-1000 companies
**Planned:** When company count reaches 750+
**Description:** At 750+ companies, consider splitting
across 2 Oracle Free VMs for IP distribution:
- VM1: companies A-M
- VM2: companies N-Z
Each VM has different public IP → half the per-IP request rate.
**Alternative:** AWS Lambda rotation (implement only if
api_health shows consistent 429s).
**Files:** `docs/deployment.md`

---

## ATS Detection

### 🟡 EU Lever instance not handled
**Discovered:** During architecture review
**Symptom:** Companies using Lever EU instance
(`api.eu.lever.co`) return empty results when queried
against global instance (`api.lever.co`).
**Fix needed:**
```python
# In jobs/ats/lever.py fetch_jobs():
# Try global first → if 0 results → try EU endpoint
# Store which instance works in ats_slug JSON
```
**Impact:** Low — affects only EU-based companies on Lever.
**Files:** `jobs/ats/lever.py` → `fetch_jobs()`
`prospective_companies` → `ats_slug` field

---

### 🟡 Oracle HCM Brave coverage is near zero
**Discovered:** 2026-03-11
**Symptom:**
```
[BRAVE] 'fa.oraclecloud.com hcmUI CandidateExperience jobs'
  0 slugs (+0 new)
```
**Root cause:** Oracle HCM URLs contain region-specific
subdomains (ap1, us2, eu1) that don't appear in plain
text searches.
**Fix needed:** Use Serper API for Oracle HCM discovery
instead of Brave (Serper supports `site:` operator).
Current Serper budget: 2500 credits → plenty for discovery.
**Impact:** Low — Oracle HCM slugs still discovered via
Athena CC index.
**Files:** `build_ats_slug_list.py` → `BRAVE_QUERIES`

---

## Database

### 🟢 serper_quota migration not implemented
**Discovered:** During CodeRabbit review
**Description:** `CHECK (id = 1)` constraint added to schema
but no migration for existing DBs that have the old schema
without the CHECK constraint.
**Fix needed:** Add migration in `init_db()` to:
1. Check if CHECK constraint exists
2. If not → rebuild table with constraint
3. Preserve existing data
**Impact:** None for fresh installs. Existing DBs work fine
without constraint — just not enforced at DB level.
**Files:** `db/schema.py` → `init_db()` migration block

---

## Tests

### 🟡 test_ats_discovery.py uses production init_discovery_db
**Status:** Partially fixed (CodeRabbit round 2)
**Remaining:** CRUD tests still use inline SQL instead of
calling real module functions (`bulk_insert_slugs`,
`delete_company`, cleanup helpers).
**Fix needed:** Replace inline SQL in TestATSCompaniesCRUD
with calls to production functions so regressions surface.
**Files:** `tests/test_ats_discovery.py`

---

### 🟡 Duplicate tests in test_job_monitor.py
**Discovered:** During CodeRabbit review
**Description:**
`test_unknown_platform_needs_redetection` and
`test_unknown_platform_triggers_redetection` test
identical conditions.
**Fix:** Merge into one test or differentiate edge cases.
**Files:** `tests/test_job_monitor.py` lines ~2260-2270

---

### 🟡 `_add` helper has unused `detected_at` parameter
**Discovered:** During CodeRabbit review
**Description:** `_add(self, company, platform=None, slug=None,
empty_days=0, detected_at=None)` — `detected_at` never used.
**Fix:** Remove from signature, update call sites.
**Files:** `tests/test_job_monitor.py` lines ~2661-2676

---

## Documentation

### 🟢 results/build_ats_list_test.txt has non-deterministic content
**Discovered:** During CodeRabbit review
**Description:** File contains shell prompt and timestamp
from local machine. Already added to `.gitignore` but
old version may still be tracked.
**Fix:** `git rm --cached results/build_ats_list_test.txt`
**Files:** `.gitignore`, `results/build_ats_list_test.txt`

---

## Resolved Issues ⚪

### ⚪ Bing Search API deprecated
**Resolved:** 2026-03-11
**Was:** Pipeline used Bing Search API which was retired
August 11, 2025.
**Fix:** Replaced with Brave Search API (free tier 1000/month).

### ⚪ Brave slugs deleted by sliding window cleanup
**Resolved:** 2026-03-11
**Was:** Brave-discovered slugs inserted with `source='crawl'`
were incorrectly deleted by the 3-crawl sliding window.
**Fix:** `_save_brave_to_db()` inserts with `source='brave'`
which is never deleted by cleanup.

### ⚪ S3 result deletion used wildcard instead of query ID
**Resolved:** 2026-03-11
**Was:** `_delete_s3_result()` listed all objects in S3 prefix
and deleted any `.csv` or `.metadata` files — dangerous.
**Fix:** Now uses `cursor.query_id` to target exact files.

### ⚪ serper.py charged credits on 429/401/403
**Resolved:** 2026-03-11
**Was:** `increment_serper_credits(1)` called before status
check — burned credits on failed requests.
**Fix:** Credits only incremented on HTTP 200.

### ⚪ iCIMS empty list ambiguous (error vs end-of-pages)
**Resolved:** 2026-03-11
**Was:** `_fetch_listing_page()` returned `[]` for both
HTTP errors and genuine end-of-results.
**Fix:** Returns `None` for errors, `[]` for end-of-pages.

### ⚪ Backslash in f-string expressions (Python 3.11)
**Resolved:** 2026-03-11
**Was:** Multiple files had `\n` or `\"` inside f-string
`{}` expressions — invalid in Python < 3.12.
**Fix:** Moved string building outside f-string expressions.

### ⚪ get_detection_queue_stats double-counting
**Resolved:** 2026-03-11
**Was:** Stats query used overlapping WHERE conditions
causing companies to appear in multiple priority buckets.
**Fix:** Uses same CASE expression as `get_detection_queue()`
with COALESCE for NULLs.

---

## Detection Results — Manual Review Required

*From first `--detect-ats` run on 2026-03-11*

### 🔴 Wrong detections — need `--override`

These companies were detected but with wrong slug/platform.
Run override commands on VM to fix.

| Company | Detected (wrong) | Correct | Override command |
|---|---|---|---|
| Capital One | lever/capital | workday | `--detect-ats "Capital One" --override workday '{"slug":"capitalone","wd":"wd12","path":"Capital_One"}'` |
| Applied Materials | ashby/applied | workday | `--detect-ats "Applied Materials" --override workday '{"slug":"appliedmaterials","wd":"wd1","path":"Applied_Materials"}'` |
| Best Buy | workday/bestbuycanada | workday | `--detect-ats "Best Buy" --override workday '{"slug":"bestbuy","wd":"wd5","path":"BestBuy"}'` |
| Charter Communications | workday/chartermfg | workday | `--detect-ats "Charter Communications" --override workday '{"slug":"spectrum","wd":"wd5","path":"Charter_Careers"}'` |
| FedEx | workday/FXE-LAC | workday | `--detect-ats "FedEx" --override workday '{"slug":"fedex","wd":"wd1","path":"FXE-US_External_Career_Site"}'` |
| Ford Motor Company | workday/fordfoundation | workday | `--detect-ats "Ford Motor Company" --override workday '{"slug":"ford","wd":"wd12","path":"Ford_Careers"}'` |
| US Bank | workday/db | workday | `--detect-ats "US Bank" --override workday '{"slug":"usbank","wd":"wd5","path":"USBankCareers"}'` |
| Western Digital | workday/westernalliancebank | workday | `--detect-ats "Western Digital" --override workday '{"slug":"westerndigital","wd":"wd1","path":"Western_Digital"}'` |
| Arm | icims/earlycareers-arm | workday | `--detect-ats "Arm" --override workday '{"slug":"arm","wd":"wd1","path":"Careers"}'` |
| Bloomberg | workday/Bloombergindustrygroup | workday | `--detect-ats "Bloomberg" --override workday '{"slug":"bloomberg","wd":"wd1","path":"Bloomberg_LP_Careers"}'` |

**Note:** Override slugs above are best guesses — verify each
URL manually before overriding:
```
https://{slug}.{wd}.myworkdayjobs.com/{path}
```

---

### 🟡 Unknown detections — need investigation

45 companies returned `unknown`. Some have known ATS that
detection phases missed. Verify and override manually:

| Company | Notes | Likely fix |
|---|---|---|
| Citibank | Uses Oracle HCM | `--override oracle_hcm` |
| Akamai Technologies | Uses Workday | `--override workday` |
| American Express | Custom ATS (jobs.amex.com) | `--override custom` |
| Bank of America | Custom/Taleo | `--override custom` |
| Goldman Sachs | Custom ATS | `--override custom` |
| Tesla | Custom ATS (tesla.com/careers) | `--override custom` |
| ServiceNow | Uses Workday | `--override workday` |
| Fidelity | Custom ATS (jobs.fidelity.com) | `--override custom` |
| Doordash | Uses Greenhouse | `--override greenhouse` |
| TikTok | Custom ATS (careers.tiktok.com) | `--override custom` |
| ByteDance | Custom ATS (jobs.bytedance.com) | `--override custom` |
| Starbucks | Taleo/Oracle | `--override custom` |
| Intuit | Uses Workday | `--override workday` |
| Wayfair | Uses Greenhouse | `--override greenhouse` |
| Bosch | SAP SuccessFactors | `--override custom` |
| Caterpillar | Oracle HCM | `--override oracle_hcm` |
| Charles Schwab | iCIMS | `--override icims` |
| Cruise | Greenhouse (acquired by GM) | `--override greenhouse` |
| Docusign | Workday | `--override workday` |
| Electronic Arts | Custom/Workday | investigate |
| Ericsson | Workday | `--override workday` |
| Fortinet | Greenhouse | `--override greenhouse` |
| Genentech | Roche/custom | `--override custom` |
| Honeywell | Workday | `--override workday` |
| Informatica | Workday | `--override workday` |
| Intuit | Workday | `--override workday` |
| Juniper Networks | Workday | `--override workday` |
| Lam Research | Workday | `--override workday` |
| Lucid Motors | Greenhouse/Lever | investigate |
| MathWorks | Custom | `--override custom` |
| NetApp | Workday | `--override workday` |
| Nokia | Workday | `--override workday` |
| Nutanix | Workday | `--override workday` |
| Optum | Workday (UnitedHealth) | `--override workday` |
| SAP America | SAP SuccessFactors | `--override custom` |
| Samsung Electronics America | Workday | `--override workday` |
| Siemens | Workday | `--override workday` |
| Sirius XM | Workday | `--override workday` |
| Splunk | Cisco/Workday | `--override workday` |
| Starbucks | Oracle Taleo | `--override custom` |
| Synopsys | Workday | `--override workday` |
| Texas Instruments | Workday | `--override workday` |
| VMware | Broadcom/Workday | `--override workday` |
| Visa | Workday | `--override workday` |
| Wells Fargo | Workday | `--override workday` |
| Xilinx | AMD/Workday | `--override workday` |

**Root causes for unknown detections:**
```
1. HTML redirect scan missed career page redirect
   → Company uses non-standard career page URL
   → Fix: improve P3a redirect scan patterns

2. Serper returned wrong company match
   → Company name too generic for search
   → Fix: more specific search queries

3. Company uses unsupported ATS
   → SAP SuccessFactors, Taleo, custom
   → Fix: add to KNOWN_CUSTOM_ATS or implement new ATS module
```

---

### 🟢 Correctly detected (82/134 = 61%)

Good detection rate for first run. Overrides will bring
coverage to ~90%+.

```
workday:     ~55 companies  ✓
greenhouse:  ~15 companies  ✓
lever:        ~5 companies  ✓ (some wrong)
ashby:        ~2 companies  ✓ (some wrong)
icims:        ~3 companies  ✓ (some wrong)
oracle_hcm:   ~2 companies  ✓
custom:        7 companies  ✓
```


---

## Session 2 Fixes (2026-03-11)

### ⚪ Workday en-US locale prefix extracted as path
**Resolved:** 2026-03-11
**Was:** Serper returns URLs like `nvidia.wd5.myworkdayjobs.com/en-US/NVIDIAExternalCareerSite`
Our regex captured `en-US` as path → 404 on every API call
54 Workday companies returning 0 jobs (66% of monitored companies)
**Fix:** Skip locale segments in path extraction in both
`patterns.py` and `build_ats_slug_list.py`
Also removed `en-US` from `WD_PATH_VARIANTS` in `workday.py`

### ⚪ Workday self-population stored plain slug — SUPERSEDED
**Status:** Superseded by Session 4 fix (see below).
**Was:** `mark_from_detection()` called with plain slug
`"nvidia"` instead of full JSON
`{"slug":"nvidia","wd":"wd5","path":"NVIDIAExternalCareerSite"}`
→ P1 lookup never matched (plain slug can't build URL)
→ Serper used on every re-detection instead of DB
**Session 2 fix applied:** Extract plain slug before saving
in `ats_detector.py`. Backfill script:
`backfill_workday_discovery.py`
**Session 4 correction:** The Session 2 fix was reversed.
`ats_detector.py` `_store_and_return()` now saves the FULL
JSON slug to `ats_discovery.db` (not plain slug), because
`fetch_jobs()` requires `wd` and `path` fields to build
the correct Workday URL. A plain `"nvidia"` slug causes
0 jobs — the full JSON `{"slug":"nvidia","wd":"wd5",
"path":"NVIDIAExternalCareerSite"}` is required.

### ⚪ myworkdaysite.com not supported (Fidelity, Wells Fargo)
**Resolved:** 2026-03-11
**Was:** Pattern only matched `myworkdayjobs.com`
`myworkdaysite.com` has different URL structure:
`wd1.myworkdaysite.com/recruiting/{tenant}/{career_site}`
**Fix:** New pattern added in `patterns.py`
`_build_url()` in `workday.py` handles both domains

### ⚪ Oracle HCM wrong API finder format
**Resolved:** 2026-03-11
**Was:** `finder=CandidateExperience&CandidateExperienceId=CX_1001`
→ 400 Bad Request on every call
**Fix:** Correct format discovered via browser XHR inspection:
`finder=findReqs%3BsiteNumber%3DCX_1001%2Climit%3D25%2Coffset%3D0`
Semicolon-separated params inside finder value

### ⚪ Oracle HCM regional URLs not supported
**Resolved:** 2026-03-11
**Was:** Only `{slug}.fa.oraclecloud.com` supported
Goldman Sachs uses `hdpc.fa.us2.oraclecloud.com` (us2 region)
**Fix:** `patterns.py` captures optional region segment
`_build_oracle_url()` handles both standard and regional URLs
Goldman Sachs: slug=hdpc, region=us2, site=LateralHiring

### ⚪ AMD/Arm/Rivian wrong iCIMS detection
**Resolved:** 2026-03-11
**Was:** All three detected as iCIMS but had migrated away
AMD   → careers.amd.com (custom)
Arm   → careers.arm.com (custom)
Rivian → careers.rivian.com (custom)
**Fix:** JS redirect detection in `icims.py` — page <500 chars
with `window.top.location.href` → returns None → stops pagination
Override script marks all three as custom

### ⚪ iCIMS fetcher verified working
**Verified:** 2026-03-11
Tested against `careers-nyit.icims.com`:
  200 status, 57KB page, 20 jobs found
  `a.iCIMS_Anchor` selector works ✓
  `?pr={page}&in_iframe=1` pagination works ✓
  `_clean_title()` strips "Job Title\n" prefix ✓

### ⚪ Coverage alert not emailed (only printed to CLI)
**Resolved:** 2026-03-11
**Was:** `_send_no_jobs_email()` had no alerts param
→ Coverage warnings only appeared in CLI output
**Fix:** `_send_no_jobs_email(alerts=alerts)` — renders
alert table in email body, adds ⚠️ to subject line

### ⚪ fetch_jobs NameError: slug/wd undefined
**Resolved:** 2026-03-11
**Was:** After `_build_url` refactor, `slug` and `wd`
no longer defined before list comprehension in `fetch_jobs`
→ 7 test failures, blocked deployment
**Fix:** Extract `slug = slug_info.get("slug", "")` and
`wd = slug_info.get("wd", "")` at top of `fetch_jobs`

---

## Session 3 Fixes & Decisions (2026-03-11)

### ⚪ Workday API uses POST not GET (critical)
**Resolved:** 2026-03-11
**Was:** `fetch_json()` uses `requests.get()` with params
Workday requires `requests.post()` with JSON body
→ ALL Workday companies returning 0 jobs since day one
→ GET returns 400, POST returns 200 with jobs
**Verified:** salesforce.wd12: GET=0 jobs, POST=1,381 jobs
**Fix:** Added `fetch_json_post()` to `base.py`
Workday `fetch_jobs()` and `detect()` now use POST

### ⚪ Oracle HCM pagination hardcoded at 25 jobs
**Resolved:** 2026-03-11
**Was:** limit/offset were URL params but Oracle requires
them INSIDE the finder string value:
`finder=findReqs;siteNumber=CX_1001,limit=25,offset=0`
URL params ignored → always returned first 25 only
JPMorgan has 1100+ jobs but only 25 fetched
**Fix:** `_build_oracle_url()` now embeds limit+offset
inside finder value using `urllib.parse.quote()`

### ⚪ Plain slugs stored without wd/path (23 companies)
**Status:** Pending fix on VM
**Was:** `backfill_workday_discovery.py` inserted plain
slugs e.g. "nvidia" instead of JSON
`{"slug":"nvidia","wd":"wd5","path":"NVIDIAExternalCareerSite"}`
→ P1 returns plain slug → `_build_url()` can't build URL
→ 23 companies returning 0 jobs
**Fix:** Clear those 23 from main DB → Serper re-detects
with correct JSON slug (POST fix ensures jobs returned)

---

## Pending — Detection Phase 2 Manual Review

### 🟡 45 unknown companies need manual ATS detection
**Decision:** Use Google Sheet approach
**Plan:**
  1. Create Google Sheet with columns:
     Company | Domain | Job URL | ATS Platform | ATS Slug | Status
  2. Manually find one job URL per company (~2 min each)
  3. Code reads sheet → runs match_ats_pattern(url)
     → extracts platform + slug automatically
  4. Stores in DB via --override
**Timeline:** 45 companies × 2 min = ~90 min total
**Priority companies to investigate first:**
  - Citibank      (Oracle HCM likely)
  - Goldman Sachs (Oracle HCM confirmed: hdpc/us2/LateralHiring)
  - Wells Fargo   (myworkdaysite.com: wf/WellsFargoJobs)
  - Fidelity      (myworkdaysite.com: fmr/FidelityCareers)
  - Tesla         (custom ATS — Greenhouse or Lever)
  - ServiceNow    (Workday likely)
  - Intuit        (Workday likely)
  - Doordash      (Greenhouse likely)
  - Wayfair       (Greenhouse likely)
  - Lam Research  (Workday likely)
  - Texas Instruments (Workday likely)

---

## Job Monitor Pipeline — Known Bugs (Lower Priority)

### 🔴 Workday fetch_jobs uses GET not POST
**Now fixed** — see above

### 🟡 redetect_workday.sh check script too strict
The bad_paths check flags valid short paths like:
  "Ext", "ANT", "Search", "jobs", "nke2"
These may actually work — need to verify via POST test
before blindly treating as bad

### 🟡 FedEx detected as FXE-LAC (Latin America path)
Correct US path: FXE-US_External_Career_Site
Needs manual override after Workday POST fix deployed

### 🟡 Best Buy detected as bestbuycanada (Canada)
Correct US slug: bestbuy
Needs manual override

### 🟡 US Bank detected as Deutsche Bank slug (db)
Both share same Workday tenant — wrong company match
Needs manual override with correct usbank slug

### 🟡 Bloomberg detected as Bloombergindustrygroup
Bloomberg Industry Group is subsidiary not main Bloomberg LP
May return different jobs than expected

### 🟡 Charter Communications detected as chartermfg
Charter Manufacturing ≠ Charter Communications (Spectrum)
Needs manual override with spectrum slug

### 🟡 Ford Motor Company
Detected via Oracle HCM — verify correct site ID
Oracle pagination fixed in Session 3 — now returns full job count

---

## Session 4 Decisions & Status (2026-03-12)

### ✅ Workday GET→POST fix deployed
All Workday companies now return full job counts.
Pagination fix: total cached from first page (page 2+ returns total=0).

### ✅ Serper Workday verification — 3-layer approach
Layer 1: urlWID from career site HTML (e.g. "ASMLExternalCareerSite")
  → Definitive only when source is live HTML (urlWID regex match).
  → If HTML fetch fails or urlWID is generic, falls through to Layer 2.
  → Never uses path/slug fallbacks as Layer 1 — those feed Layer 2/3
    to avoid premature rejection on non-HTML values.
Layer 2: path from Serper URL slug_info["path"] directly
  (e.g. "qualcomm_careers" → contains "qualcomm" ✓)
Layer 3: title_verified fallback (when urlWID+path both generic,
  e.g. ms.wd5/External — only trusted if Serper also returned text)
Result: ASML, Qualcomm, Morgan Stanley now correctly detected.

### ✅ Oracle HCM removed from Serper permanently
site:fa.oraclecloud.com returns JPMorgan for every company query.
Oracle detection via P3a exclusively: `career_page.py` scans HTML
for oraclecloud URLs during the company.com/careers redirect scan.
`_verify_slug_via_api()` always returns False for oracle_hcm —
no separate oracle_hcm.detect() pass is called anywhere.

### ✅ Full JSON slug stored in ats_discovery.db
`_store_and_return()` in `ats_detector.py` saves the complete JSON
slug (e.g. `{"slug":"nvidia","wd":"wd5","path":"NVIDIAExternalCareerSite"}`)
to `ats_discovery.db` via `mark_from_detection()`. Plain slugs cause
0 jobs because `_build_url()` needs `wd` and `path` to construct the
correct Workday URL. The Session 2 note about extracting a plain slug
before saving is superseded — full JSON is the correct behaviour.

### ✅ _slug_valid_for_company validation scope
`_slug_valid_for_company` (in `jobs/ats/patterns.py` or equivalent)
validates company name against slug for: Greenhouse, Lever, iCIMS.
It is deliberately skipped for Workday and Oracle HCM because:
  - Workday slugs are tenant identifiers (e.g. "wd5"), not company names
  - Oracle HCM slugs are internal site codes, not company names
  - Validation for these platforms uses urlWID/path (Workday) or
    career_page HTML scan (Oracle HCM) instead
Future debugging: if a Greenhouse/Lever/iCIMS slug passes API probe
but fails company validation, check `_slug_valid_for_company`. For
Workday/Oracle mismatches, check Layer 1 (urlWID HTML) and Layer 2
(path) in `_verify_slug_via_api()` in `jobs/serper.py`.

### ✅ Bridge fixes
- Redundant oracle_hcm.detect() removed from P3a (career_page handles it)
- career_page.py is the single source of truth for Oracle HCM detection
- job_monitor.py run() now passes domain=company_row.get("domain") to
  detect_ats() during re-detection, enabling P3a to run with correct
  domain for Oracle HCM and other HTML-redirect-based platforms

### ✅ form_sync.py integration
Applied job URLs auto-extract ATS + add company to prospective pool
status='applied' — not monitored until explicitly activated.

### 🔴 Unknown companies — UNRESOLVED (by design)
Decision: Use 2 Google Forms approach
  Form 1 (existing): Job applications → extracts ATS from URL automatically
  Form 2 (new):      Prospective companies → manual entry of company + URL
                     when you find a company worth tracking

Root causes of unknowns:
  - JS-rendered career pages (Playwright needed or manual URL)
  - Non-standard career page paths (/our-firm/careers etc.)
  - Unsupported ATS (Taleo, SAP SuccessFactors, Brassring, Workday HCM)
  - Custom ATS (Tesla, Netflix, Uber, Google, Apple, Meta, Amazon, Microsoft)
  - Serper finds wrong company (Lam Research → silfex subsidiary slug)

### 🔴 ats_discovery.db enrichment — needs more data
Current P1 hit rate is limited because ats_discovery.db is sparse.
More monitoring runs → more self-population → better P1 coverage.
Decision: retain more than 3 crawls data in ats_companies DB
to build a richer reference dataset over time.

### 🟡 Detection failure classification — future work
All UNKNOWN companies look the same in DB.
Need detection_failure_reason column to categorise:
  → JS-rendered → needs Google Sheet
  → Unsupported ATS → mark permanently
  → Wrong domain → fixable automatically
  → Custom ATS → already handled separately