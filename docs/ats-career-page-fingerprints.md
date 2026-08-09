# ATS Career Page Fingerprints

How many examples per platform? The goal is to find the **stable invariant** — the part of the
fingerprint that never changes across companies on that platform. Once the invariant is confirmed
across 3–5 independent company career pages, the pattern is reliable. Platforms with known
variants (different URL structures per region or product line) need more examples to cover all
variants.

---

## Build Order

1. **Collect examples first** — for each platform, paste real HTML snippets from actual company career pages. Lock in the keyword identifier and extraction regex once 3–5 examples confirm the invariant.
2. **Build HTML fetch + keyword detection chunk** — single script, single HTML fetch per company, scans for all platform keywords. Test against known companies where ATS is already confirmed.
3. **Fix anomalies** before expanding further.
4. **Add URL extraction + slug/path parsing** — extract structured data from matched URLs.
5. **Wire into the pipeline** — only after chunks 2–4 are validated.

Do NOT touch `patterns.py` or any existing working pipeline code until fingerprinting is fully validated end-to-end. Existing pipeline takes priority — fingerprinting is additive, not a replacement yet.

---

## Detection Approach

**Generalise broadly — use platform keyword identifiers, not exact domains.**

Each platform has a stable keyword that appears in ALL its URL variants. Scan for the keyword,
not the full domain. This catches subdomains, instance variants, and future URL changes automatically.

| platform | keyword identifier | known URL variants |
|---|---|---|
| Workday | `workday` | `myworkdayjobs.com`, `myworkday.com`, `wd{N}.myworkday.com` |
| Greenhouse | `greenhouse` | `boards.greenhouse.io`, `greenhouse.io` |
| Lever | `lever` | `jobs.lever.co`, `lever.co` |
| Ashby | `ashby` | `jobs.ashbyhq.com`, `ashbyhq.com` |
| iCIMS | `icims` | `careers.icims.com`, `*.icims.com` |
| Taleo | `taleo` | `*.taleo.net` |
| SuccessFactors | `successfactors` | `*.successfactors.com`, `*.successfactors.eu` |
| SmartRecruiters | `smartrecruiters` | `careers.smartrecruiters.com` |
| Jobvite | `jobvite` | `jobs.jobvite.com` |
| Eightfold | `eightfold` | `*.eightfold.ai` |

**Scan the entire raw page text** — not just specific attributes. The keyword can be anywhere: inline `<script>` blocks, `<script type="application/json">` blobs, `data-*` attributes, GTM configs, `__NEXT_DATA__`, `j2w.init()`, href/src values. One `keyword in page_text` check catches all of them.

**Algorithm — same logic repeated at every page level:**

```
for each page level (career → listing → JD → apply):
    page_text = fetch(url)
    match = scan(page_text, ATS_KEYWORDS)
    if match:
        slug = extract(page_text, platform)
        return result

    # also scan each <script src> file
    for script_url in parse_script_srcs(page_text):
        bundle_text = fetch(script_url)
        match = scan(bundle_text, ATS_KEYWORDS)
        if match:
            slug = extract(bundle_text, platform)
            return result

    next_url = find_next_page_link(page_text)
```

- `scan()` = `keyword in text` — pure string search, no HTML parsing, no DOM
- `extract()` = regex on the same raw text — only runs after a keyword match confirms the platform
- `find_next_page_link()` = scoring-based — extract all hrefs from raw text, score each candidate, return highest scorer (see scoring design below)
- `parse_script_srcs()` = regex over raw text for `<script src="...">` — only fetched when HTML scan fails
- No BeautifulSoup, no Playwright, no browser. Pure `requests` + `re`.

**Key insight:** ATS fingerprint location is company-specific, not ATS-specific. Same platform can appear at career page, listing page, JD page, or apply page. Always go deeper before declaring a miss. Spotify/Lever proves JS bundle scanning is mandatory — HTML scan fails completely on some companies.

---

## Implementation Design

The whole detector collapses to three pieces. Adding a new ATS = one line in the keyword table + one small `extract_*` function. Everything else is already handled.

### 1. Keyword table — detection

```python
ATS_KEYWORDS = {
    "workday":                   extract_workday,
    "greenhouse":                extract_greenhouse,
    "successfactors":            extract_successfactors,
    "lever":                     extract_lever,
    "smartrecruiters":           extract_smartrecruiters,
    "eightfold":                 extract_eightfold,
    "ashbyhq":                   extract_ashby,
    "taleo":                     extract_taleo,
    "phenompeople":              extract_phenom,
    "talentbrew":                extract_talentbrew,
    "oraclecloud":               extract_oracle_hcm,
    "avatureReferrerQueryParam": extract_avature,
    "icims":                     extract_icims,
    "jobvite":                   extract_jobvite,
}
```

### 2. `scan(text)` — runs on any string, HTML or JS bundle

```python
def scan(text):
    for keyword, extractor in ATS_KEYWORDS.items():
        if keyword in text:
            return extractor(text)
    return None
```

### 3. `detect(url)` — the loop

```python
def detect(url, depth=0):
    html = fetch(url)
    result = scan(html)                          # try HTML first
    if result: return result

    for src in find_script_srcs(html):           # try JS bundles
        result = scan(fetch(src))
        if result: return result

    if depth < MAX_DEPTH:                        # navigate deeper
        next_url = find_next_page_link(html, url)
        if next_url:
            return detect(next_url, depth + 1)

    return None
```

### 4. `find_next_page_link(html, current_url)` — scoring-based navigation

Extract every `href` from raw text, score each candidate, return the highest scorer.

```python
# positive signals — URL path keywords
URL_KEYWORDS = {
    "job": 3, "jobs": 3, "career": 3, "careers": 3,
    "position": 2, "positions": 2, "opening": 2, "openings": 2,
    "role": 1, "roles": 1, "work": 1, "hiring": 2,
}

# positive signals — anchor text (text between <a> and </a>)
ANCHOR_KEYWORDS = {
    "view jobs": 5, "see jobs": 5, "explore jobs": 5,
    "open positions": 5, "job openings": 5, "apply now": 4,
    "view openings": 4, "search jobs": 4, "browse jobs": 4,
    "all jobs": 3, "careers": 3, "opportunities": 2,
}

# negative signals — discard immediately
SKIP_PATTERNS = [
    r'^#', r'^mailto:', r'^tel:', r'javascript:',
    r'facebook\.com', r'twitter\.com', r'linkedin\.com',
    r'instagram\.com', r'youtube\.com',
]
```

**Scoring rules:**
- Extract all `(href, anchor_text)` pairs via regex from raw HTML
- Discard any href matching a `SKIP_PATTERNS` entry
- Discard hrefs pointing to a different domain (stay on same site)
- Score = sum of URL_KEYWORDS hits in href path + sum of ANCHOR_KEYWORDS hits in anchor text (case-insensitive)
- Prefer same-domain relative or absolute URLs over external redirects
- Pick highest score; if tie, prefer the one appearing earlier in the page

**Target file:** `jobs/ats/career_detector.py`

---

## Platform Fingerprints

---

### Workday

**Keyword identifier:** `workday`

**Known URL variants:**
```
https://{company}.wd{N}.myworkdayjobs.com/{path}   ← job board (most common)
https://{company}.myworkday.com/...                 ← platform variant
https://wd{N}.myworkday.com/...                     ← instance variant
```
- `{company}` — company-specific slug (e.g. `accenture`, `amazon`)
- `{N}` — Workday instance number (varies: `1`, `3`, `5`, `103`, etc.) — NOT predictable, must be read from page
- `{path}` — career portal name (e.g. `AccentureCareers`, `amazon_dkf9LB`)

**Extraction regex (job board variant):**
```
([a-z0-9-]+)\.wd(\d+)\.myworkdayjobs\.com/([a-zA-Z0-9_-]+)
```
Group 1 = company slug, Group 2 = WD instance, Group 3 = path

**Fingerprint locations on career page:**

| type | what to look for |
|---|---|
| `<a href>` | any URL containing `workday` |
| `data-*` attribute | JSON-encoded value containing `workday` URL |
| `<script src>` | any src containing `workday` |

**Confirmed examples:**

| company | career page | fingerprint URL found |
|---|---|---|
| Accenture | `accenture.com/us-en/careers` | `accenture.wd103.myworkdayjobs.com/AccentureCareers/userHome` (in `data-cmp-data-layer` JSON + `href`) |
| Adobe | confirmed | `myworkdayjobs.com` variant |
| Agilent | confirmed | `myworkdayjobs.com` variant (multi-portal: student + experienced) |
| AIG | confirmed | `myworkdayjobs.com` variant |

**Notes:**
- `data-cmp-data-layer` is an Adobe Analytics attribute — JSON-encoded, contains `xdm:linkURL` with the full Workday URL. Check both `href` and this attribute.
- WD instance number (`wd103`) is not predictable — must be read from the page, not guessed.
- Path is case-sensitive.
- Multi-portal companies (Agilent) have multiple Workday URLs — first match wins.

**Status: ✅ LOCKED IN**

---

---

### Greenhouse

**Keyword identifier:** `greenhouse`

**Known URL variants:**
```
https://boards.greenhouse.io/embed/job_board/js?for={slug}   ← embed script
https://boards.greenhouse.io/{slug}                           ← hosted board
https://job-boards.greenhouse.io/{slug}                       ← newer variant
```

**Extraction regex:**
```
greenhouse\.io/(?:embed/job_board/js\?for=|)([a-z0-9_-]+)
```
Group 1 = company slug

**Fingerprint locations — Greenhouse appears at different levels per company:**

| type | what to look for | page level |
|---|---|---|
| `__NEXT_DATA__` embedded JSON | `"greenhouseId"` field in job listings JSON | career page (Next.js sites) |
| `<script src>` | src containing `greenhouse` | job detail page (sometimes only here) |
| `<a href>` | href containing `greenhouse.io` | career or listing page |
| `<iframe src>` | src containing `greenhouse.io` | career page (embedded board) |

**Detection note:** Greenhouse fingerprint location varies by company. Stripe exposes it on the career page itself via `__NEXT_DATA__`. Airbnb only exposes it on individual job detail pages. Always run all layers — do NOT assume which level will have the fingerprint.

**Extraction — two paths:**
- If found via `greenhouse.io` URL → apply regex above, Group 1 = slug
- If found via `__NEXT_DATA__` `greenhouseId` field → slug is the `slug` field in same JSON object

**Confirmed examples:**

| company | career page | fingerprint found at | fingerprint |
|---|---|---|---|
| Airbnb | `careers.airbnb.com` | job detail page (`careers.airbnb.com/positions/7732569/`) | `<script src="https://boards.greenhouse.io/embed/job_board/js?for=airbnb">` |
| Stripe | `stripe.com/careers/search` | career page `__NEXT_DATA__` | `"listings":[{"greenhouseId":7844214,"slug":"aeo-and-geo-marketing-manager",...}]` |
| Adaptive Biotech | `adaptivebiotech.com` | career page `<iframe>` | `<iframe src="https://job-boards.greenhouse.io/embed/job_board?for=adaptivebiotechnologies&validityToken=...">` |

**Notes:**
- `greenhouseId` in `__NEXT_DATA__` is definitive — field name makes ATS explicit, no URL matching needed.
- Greenhouse slug: use `for=` param from script URL or iframe src, or `slug` field from `__NEXT_DATA__` JSON.
- Adaptive Biotech uses an embedded `<iframe>` directly on the career page — the `src` contains `?for={slug}`, same extraction regex as the script embed variant.
- Airbnb listing page (`/positions/`) is server-rendered so curl returns real HTML with job URLs — but this is Airbnb-specific, not a general rule.

**Examples needed to lock in:** ✅ 3/3 confirmed — pattern locked in

---

---

### Oracle HCM

**Keyword identifiers:** `oraclecloud` (standard domain) + `oj-hcm-ce` (custom domain via script tag)

**Why two keywords:** Companies can proxy Oracle HCM behind their own domain (Dell uses `enterpriseplatform.dell.com`). In that case `oraclecloud` never appears in page HTML — only the Oracle HCM script tag reveals the platform.

**Known URL variants:**
```
https://{tenant}.fa.oraclecloud.com/hcmUI/CandidateExperience/en/sites/{site}/jobs   ← standard
https://{custom-domain}/hcmUI/CandidateExperience/en/sites/{site}/jobs               ← custom domain (Dell)
```
- `{tenant}` — Oracle-assigned tenant ID (e.g. `iawmqy` for Dell)
- `{site}` — career site name (e.g. `careers`)
- `/hcmUI/CandidateExperience/` path is invariant across all Oracle HCM deployments

**Script fingerprint (custom domain detection):**
```
https://static.oracle.com/cdn/fa/oj-hcm-ce/{version}/js/main-minimal.js
```
Keyword `oj-hcm-ce` is specific to Oracle HCM Candidate Experience — safe to use as sole identifier.

**Extraction:**
- Standard domain: tenant = subdomain of `*.fa.oraclecloud.com`, site = path segment after `/sites/`
- Custom domain (e.g. Dell): tenant IS in static HTML — the page also embeds a second Oracle script tag pointing to `{tenant}.fa.{region}.oraclecloud.com/hcmUI/CandExpStatic/js/ce-custom.js`. `career_page.py` scans all `<script src>` values and the pattern at `patterns.py:189–203` extracts the slug from that URL. The `oj-hcm-ce` tag triggers platform detection; the `*.fa.oraclecloud.com/hcmUI/` tag provides the tenant. Example: AmEx embeds `egug.fa.us2.oraclecloud.com:443/hcmUI/CandExpStatic/js/ce-custom.js`.

**Slug structure (our pipeline):**
```json
{"slug": "{tenant}", "ocs": true, "region": "", "site": "{site}"}
```
Example (Dell): `{"slug": "iawmqy", "ocs": true, "region": "", "site": "careers"}`

**Fingerprint locations on career page:**

| type | what to look for | page level |
|---|---|---|
| `<script src>` | src containing `oj-hcm-ce` | career page |
| `<a href>` or redirect | URL containing `oraclecloud` | career page or redirect |
| URL path | `/hcmUI/CandidateExperience/` in any URL | career page itself (if custom domain) |

**Confirmed examples:**

| company | career page | fingerprint found at | fingerprint |
|---|---|---|---|
| Dell | `dell.com` → `enterpriseplatform.dell.com/hcmUI/CandidateExperience/en/sites/careers/jobs` | career page | `<script src="https://static.oracle.com/cdn/fa/oj-hcm-ce/.../js/main-minimal.js">` |

**Notes:**
- Career page URL itself contains `/hcmUI/CandidateExperience/` — detectable even before scanning HTML.
- Custom domain is the tricky case; `oj-hcm-ce` script covers it reliably.

**Examples needed to lock in:** 2 more (1/3 confirmed)

---

---

### TalentBrew (Radancy)

**Keyword identifier:** `talentbrew`

**Known URL variants:**
```
https://tbcdn.talentbrew.com/...                          ← CDN script (always present)
https://jobs.{company}.com/job/{city}/{slug}/{tenant}/{id} ← job listing URL pattern
```
- `{tenant}` — numeric tenant ID (e.g. `27595` for Intuit, `391` for Disney)
- Tenant ID is per-domain and must be hardcoded in `patterns.py` — it does NOT appear in any parseable position in the career page HTML itself

**Script fingerprint:**
```
<script src="https://tbcdn.talentbrew.com/...">
```
Keyword `talentbrew` in `tbcdn.talentbrew.com` is the stable identifier. Platform is in `_RICH_SLUG_PLATFORMS` — `career_page.py` only detects presence, slug/tenant come from `patterns.py`.

**Tenant extraction mechanism:**
Auto-detected at runtime from the sitemap — NOT hardcoded. `talentbrew.py:_detect_tenant_id()` fetches `{base}/sitemap.xml`, matches all `<loc>` URLs against `/job/{city}/{slug}/{tenant_id}/{job_id}`, and returns the most-frequent numeric tenant segment. `fetch_jobs()` always re-derives the live value and updates `slug_info["tenant_id"]` in-place if it drifted (observed: Schwab drifted `27326 → 33727`).

`KNOWN_DOMAINS` in `talentbrew.py` (line 58–64) only maps base domains — Schwab's tenant is stored as `""` deliberately. `patterns.py` values are initial hints for URL pattern matching only, not what the fetcher uses.

For new TalentBrew domains (e.g. 7-Eleven): add an entry to `KNOWN_DOMAINS` in `talentbrew.py` with `{"tenant_id": ""}` — detection will auto-discover the live tenant from the sitemap.

**Fingerprint locations on career page:**

| type | what to look for | page level |
|---|---|---|
| `<script src>` | src containing `talentbrew` | career page |
| job listing URL | numeric segment is tenant ID | job detail page |

**Confirmed examples:**

| company | career page | fingerprint found at | fingerprint |
|---|---|---|---|
| 7-Eleven | (career page URL TBD) | career page | `<script src="https://tbcdn.talentbrew.com/...">` |

**Notes:**
- Tenant ID is NOT derivable from the career page HTML alone — it requires inspecting a job detail URL or being pre-populated in `patterns.py`.
- `patterns.py` is the authoritative source; each new TalentBrew company needs an entry added there.

**Examples needed to lock in:** 2 more with known tenant IDs confirmed

---

---

### Phenom (Phenom People)

**Keyword identifier:** `phenompeople`

**Known URL variants:**
```
https://cdn.phenompeople.com/CareerConnectResources/...   ← CDN (script or stylesheet)
https://{custom-domain}/us/en/job/{id}/{slug}              ← job detail URL (custom domain per company)
```
- No shared hosted domain — every company uses a custom domain (eBay → `jobs.ebayinc.com`, Chewy → `careers.chewy.com`)
- CDN domain `cdn.phenompeople.com` is the invariant identifier across ALL Phenom deployments

**Script / stylesheet fingerprint:**
```html
<script src="https://cdn.phenompeople.com/CareerConnectResources/...">
<link href="https://cdn.phenompeople.com/CareerConnectResources/...">
```
Both `<script src>` and `<link href>` can carry the CDN URL — scan both.

**Fingerprint locations on career page:**

| type | what to look for | page level |
|---|---|---|
| `<script src>` | src containing `phenompeople` | career page |
| `<link href>` | href containing `phenompeople` | career page |

**Slug structure (our pipeline):**
```json
{"base": "https://jobs.ebayinc.com", "path": "us/en", "sitemap": "us/en/sitemap.xml"}
```
Each company entry is hardcoded in `patterns.py` lines 288–301 — slug extraction is domain-specific, not URL-parseable from the CDN tag alone.

**Confirmed examples:**

| company | career page | fingerprint found at | fingerprint |
|---|---|---|---|
| eBay | `ebay.com/careers` | career page | `<script src="https://cdn.phenompeople.com/CareerConnectResources/common/js/vendor/dayjs/locale/en.js">` |
| Genentech | `gene.com/careers` | career page | `<link href="https://cdn.phenompeople.com/CareerConnectResources/common/js/globalplatform/.../globalstyles.css">` |

**Notes:**
- `phenompeople` keyword is cleaner than `phenom` (too generic) — use it as the identifier.
- Detection confirms Phenom; slug/base domain must be pre-registered in `patterns.py` per company.
- `<link href>` (stylesheet) is a valid detection surface — not just `<script src>`.

**Examples needed to lock in:** 1 more (2/3 confirmed)

---

---

### Eightfold

**Keyword identifier:** `eightfold`

**Known URL variants:**
```
https://{slug}.eightfold.ai/careers                               ← standard hosted
https://{slug}.eightfold.ai/careers?domain={company-domain}...   ← with domain param
```
- `{slug}` = company identifier (e.g. `lamresearch`, `starbucks`)
- `domain=` query param provides the company domain — used by `_enrich_eightfold_domain()` in `career_page.py` to fill the `domain` field in slug_info

**Extraction regex:**
```
([a-z0-9][a-z0-9\-]*)\.eightfold\.ai/
```
Group 1 = slug

**Slug structure (our pipeline):**
```json
{"slug": "lamresearch", "domain": "lamresearch.com"}
```
`domain` filled from `?domain=` query param or hostname prefix strip.

**Fingerprint locations on career page:**

| type | what to look for | page level |
|---|---|---|
| `<a href>` | href containing `eightfold.ai` | career page |
| `<script src>` | src containing `eightfold` | career page |

**Confirmed examples:**

| company | career page | fingerprint found at | fingerprint |
|---|---|---|---|
| Lam Research | `lamresearch.com/careers` | career page `<a href>` | `<a href="https://lamresearch.eightfold.ai/careers?domain=lamresearch.com&start=0&...">` |

**Notes:**
- `domain=` query param is the company domain — use it to populate `slug_info["domain"]`.
- `career_page.py:_enrich_eightfold_domain()` handles this enrichment automatically.

**Examples needed to lock in:** 2 more (1/3 confirmed)

---

---

### ADP

**Keyword identifier:** `myjobs.adp.com`

**Known URL variants:**
```
https://myjobs.adp.com/{slug}/cx/job-listing        ← listing page
https://myjobs.adp.com/{slug}/cx/job-details?reqId= ← job detail
https://myjobs.adp.com/apply/auth?lang=us-US        ← generic auth (no slug)
```
- `{slug}` = company identifier (e.g. `apply`, `scacareers`)
- Generic auth URL (`/apply/auth`) confirms ADP but does NOT contain the company slug — slug extraction requires the `/cx/` URL

**Extraction regex (`patterns.py`):**
```
myjobs.adp.com/([a-z0-9][a-z0-9\-]*)/cx/
```
Group 1 = slug

**Fingerprint locations on career page:**

| type | what to look for | page level |
|---|---|---|
| `<a href>` | href containing `myjobs.adp.com` | career page |

**Confirmed examples:**

| company | career page | fingerprint found at | fingerprint |
|---|---|---|---|
| ADP | `adp.com` | career page `<a href>` | `<a href="https://myjobs.adp.com/apply/auth?lang=us-US">` (generic auth — confirms ADP, slug TBD) |

**Notes:**
- Generic `/apply/auth` link detects ADP but slug is not extractable from it — need to follow a job link to get `/cx/` URL with the company slug.
- Slug extraction requires Layer 3 (follow a job detail link).

**Examples needed to lock in:** 2 more with slug-bearing `/cx/` URLs confirmed

---

---

### SuccessFactors (SAP)

**Keyword identifier:** `successfactors`

**Known URL variants:**
```
performancemanager{N}.successfactors.com   ← asset CDN (jQuery etc.) — platform detection only, NO slug
career{N}.successfactors.com/careers?company={slug}   ← US career portal — slug here
career{N}.successfactors.eu/careers?company={slug}    ← EU career portal — slug here
{slug}.jobs2web.com                                    ← jobs2web variant
```
- `{N}` — datacenter number (not predictable)
- `{slug}` — company identifier (e.g. `EYHRISPRD1`)
- `/careers` or `/career` (singular exists — regex handles both)

**Canonical fingerprint — `j2w.init()` inline script:**
```js
j2w.init({
    "ssoCompanyId": "{slug}",
    "ssoUrl":       "https://career{N}.successfactors.{com|eu}",
    ...
});
```
`ssoCompanyId` = slug, `ssoUrl` = datacenter URL. Both fields always present together. This is the definitive SuccessFactors fingerprint — slug AND datacenter in one inline `<script>` block.

**Secondary URL variants (for pattern matching fallback):**
```
performancemanager{N}.successfactors.com   ← asset CDN, career page script src — platform detection only, NO slug
career{N}.successfactors.com/careers?company={slug}   ← apply button / GTM — slug present
career{N}.successfactors.eu/careers?company={slug}    ← EU variant
{slug}.jobs2web.com                                    ← jobs2web variant
```

**Extraction from `j2w.init()`:**
Regex on raw inline script text:
```
"ssoCompanyId"\s*:\s*['"]([^'"]+)['"]
"ssoUrl"\s*:\s*['"]([^'"]+)['"]
```
`career_page.py` already has this detector in `_scan_html()`.

**Fingerprint locations:**

| type | what to look for | page level |
|---|---|---|
| Inline `<script>` | `j2w.init({...ssoCompanyId...ssoUrl...})` | career page OR listing page |
| `<script src>` | `performancemanager{N}.successfactors.com` | career page (platform only, no slug) |
| `<a href>` Apply button / GTM | `career{N}.successfactors.com/careers?company=` | job detail / listing |

**Confirmed examples:**

| company | fingerprint found at | `ssoCompanyId` | `ssoUrl` |
|---|---|---|---|
| AGCO | career page inline `<script>` | `Agco` | `https://career4.successfactors.com` |
| EY | listing page inline `<script>` | `EYHRISPRD1` | `https://career5.successfactors.eu` |

**Notes:**
- `j2w.init()` is the canonical fingerprint — always has both slug and datacenter.
- Location varies (career page for AGCO, listing page for EY) — scan both career page and listing page.
- `performancemanager*` script confirms platform but never contains slug — don't rely on it for slug extraction.
- EU companies: `ssoUrl` ends in `.eu`; US: `.com`.
- `jobs2web.com` is a SF variant handled separately in `patterns.py`.

**Status: ✅ LOCKED IN**

---

---

### Ashby

**Keyword identifier:** `ashbyhq`

**Known URL variants:**
```
https://jobs.ashbyhq.com/{slug}                              ← job board listing
https://jobs.ashbyhq.com/{slug}/{job-id}/application         ← apply page
```
- `{slug}` = company identifier (e.g. `depthfirst`, `snowflake`)
- Job ID is a UUID — ignore it, slug is always the first path segment

**Extraction regex (`patterns.py`):**
```
jobs\.ashbyhq\.com/([^/?&#\s]+)
```
Group 1 = slug

**Fingerprint locations:**

| type | what to look for | page level |
|---|---|---|
| `<a href>` Apply button | `jobs.ashbyhq.com/{slug}/...` | JD page |
| `<a href>` / `<iframe src>` | `jobs.ashbyhq.com/{slug}` | career page (embedded board) |

**Confirmed examples:**

| company | fingerprint found at | fingerprint |
|---|---|---|
| Depth First | JD page Apply button `<a href>` | `https://jobs.ashbyhq.com/depthfirst/{job-id}/application` |

**Notes:**
- Fingerprint appears on JD page Apply button at minimum — Layer 3 required.
- Some companies embed the Ashby board directly on career page via iframe — Layer 1 may catch it.

**Status: 1/3 confirmed — need 2 more examples**

---

---

### SmartRecruiters

**Keyword identifier:** `smartrecruiters`

**Known URL variants:**
```
https://jobs.smartrecruiters.com/{slug}/{job-id}                              ← job listing link (most common)
https://jobs.smartrecruiters.com/oneclick-ui/company/{slug}/publication/{id}  ← oneclick-ui variant
https://jobs.smartrecruiters.com/{slug}                                        ← hosted board
```
- `{slug}` = company identifier (e.g. `Versant3`, `ServiceNow`)
- Job ID is numeric — ignore it, slug is always the first path segment

**Extraction regex (`patterns.py`):**
```
jobs\.smartrecruiters\.com/oneclick-ui/company/([^/?&#\s]+)   ← must run first
jobs\.smartrecruiters\.com/([^/?&#\s]+)                        ← standard
```
Group 1 = slug (oneclick-ui pattern takes priority — same domain, longer path)

**Fingerprint locations:**

| type | what to look for | page level |
|---|---|---|
| `<a href>` | job listing links directly to `jobs.smartrecruiters.com/{slug}/...` | career page |
| `<iframe src>` | embedded SmartRecruiters board | career page |

**Confirmed examples:**

| company | fingerprint found at | fingerprint |
|---|---|---|
| Versant | career page `<a href>` (job listing link) | `https://jobs.smartrecruiters.com/Versant3/744000142211249` |

**Notes:**
- Versant exposes SmartRecruiters URLs directly on the career page — company-specific configuration, not a SmartRecruiters guarantee.
- Other companies may only expose the fingerprint on the listing page or JD page depending on how they built their career site.
- Slug is case-sensitive (`Versant3` not `versant3`).

**Status: 1/3 confirmed — need 2 more examples**

---

---

### Taleo (Oracle Taleo)

**Keyword identifier:** `taleo`

**Known URL variants:**
```
https://{company}.taleo.net/careersection/{section}/jobdetail.ftl?job={id}   ← job detail
https://{company}.taleo.net/careersection/{section}/jobapply.ftl?job={id}    ← apply page
https://{company}.taleo.net/careersection/{section}/jobsearch.ftl            ← listing
```
- `{company}` = subdomain slug (e.g. `cognizant`)
- `{section}` = career section name (e.g. `Lateral`, `External`, `Campus`)
- Section auto-discovered by `taleo.py` on first fetch — not needed for detection

**Extraction regex (`patterns.py`):**
```
([a-z0-9][a-z0-9\-]*?)\.taleo\.net/careersection/([^/?&#\s]+)/
```
Group 1 = company slug, Group 2 = section

**Slug structure (our pipeline):**
```json
{"company": "cognizant", "portal_id": "", "section": "lateral"}
```
`portal_id` auto-discovered on first `fetch_jobs()` call.

**Fingerprint locations:**

| type | what to look for | page level |
|---|---|---|
| `<a href>` Apply button | `{company}.taleo.net/careersection/...` | JD page |
| `<a href>` job listing link | same pattern | career or listing page |

**Confirmed examples:**

| company | fingerprint found at | fingerprint |
|---|---|---|
| Cognizant | JD page Apply button `<a href>` | `https://cognizant.taleo.net/careersection/Lateral/jobapply.ftl?job=00069858501` |

**Notes:**
- Subdomain IS the slug — `cognizant.taleo.net` → slug `cognizant`.
- Section varies (`Lateral`, `External`, `Campus`) — stored in slug_info, used by fetcher.

**Status: 1/3 confirmed — need 2 more examples**

---

---

### Lever

**Keyword identifier:** `lever`

**Known URL variants:**
```
https://jobs.lever.co/{slug}            ← job board listing
https://jobs.lever.co/{slug}/{job-id}   ← job detail
https://hire.lever.co/{slug}            ← alternate domain
```
- `{slug}` = company identifier (e.g. `spotify`, `netflix`)

**Extraction regex (`patterns.py`):**
```
(?:jobs|hire)\.lever\.co/([^/?&#\s]+)
```
Group 1 = slug

**Fingerprint locations:**

| type | what to look for | page level |
|---|---|---|
| JS bundle text | `lever.co/{slug}` string literal | career page JS files |
| `<a href>` | `jobs.lever.co/{slug}` link | listing or JD page |

**Confirmed examples:**

| company | fingerprint found at | notes |
|---|---|---|
| Spotify | JS bundle (career page) — NOT in HTML | HTML scan fails completely — JS file scan required |

**Notes:**
- Spotify career page has zero Lever fingerprint in static HTML — URL is buried in a JS bundle.
- This is the canonical example proving JS file scanning is mandatory, not optional.
- Lever slug expected: `spotify` — unconfirmed until JS scan implemented and tested.

**Status: 0/3 confirmed — JS scan needed to extract**

---

### Avature

**Keyword identifier:** `avature`

**Known URL variants:**
```
https://{company}.avature.net/{slug}                      ← custom subdomain job board
https://careers.{company}.com/...                         ← custom career page backed by Avature
```
- Avature is often configured with a fully custom career page UI — the Avature tenant URL only appears on the job detail or apply page.

**Extraction regex:**
```
([a-z0-9-]+)\.avature\.net/([^/?&#\s"'<>]+)
```
Group 1 = company subdomain, Group 2 = path/slug

**Fingerprint locations:**

| type | what to look for | page level |
|---|---|---|
| JSON blob (`<script type="application/json">`) | `"avatureReferrerQueryParam"` key | career page (custom UI) |
| `<a href>` | `*.avature.net/...` link | listing or JD page |
| `<script src>` | `*.avature.net/...` JS file | listing or JD page |

**Confirmed examples:**

| company | fingerprint found at | notes |
|---|---|---|
| Wayfair (`wayfair.com/careers/jobs`) | Career page — `<script type="application/json" id="wfAppData">` contains `"avatureReferrerQueryParam":"&source="` | Fully custom career UI on top of Avature; no Avature URL on career/listing page — only the referrer param key reveals the ATS. Avature tenant URL appears on JD or apply page. |

**Notes:**
- Wayfair built a fully custom job search UI (`careers_job_search_results` React component). The JSON data blob embedded server-side in `<script type="application/json" id="wfAppData">` contains `"avatureReferrerQueryParam"` alongside `"greenhouseReferrerQueryParam"` — Wayfair previously used Greenhouse and now uses Avature.
- Scan `<script type="application/json">` text content, not just href/src attributes.
- The `avature` keyword alone may not appear in this JSON — the key is the full string `avatureReferrerQueryParam`. Add `avatureReferrerQueryParam` as a secondary keyword to scan.
- Actual Avature tenant URL (with slug) only appears at the job detail or apply page level.

**Status: 1/3 confirmed — need Avature tenant URL from JD/apply page**

---

<!-- Add platforms below in this format:

### {Platform Name}

**Invariant domain:** `{domain}`

**URL pattern:**
\```
https://{pattern}
\```

**Extraction regex:**
\```
{regex}
\```

**Fingerprint locations on career page:**

| type | what to look for |
|---|---|

**Confirmed examples:**

| company | career page | fingerprint URL found |
|---|---|---|

**Notes:**

**Examples needed to lock in:**

-->

## Platforms To Document

Ordered by number of companies in prospective_companies (highest first = highest priority).
Knock these down one by one — visit a real company's career page, grab the HTML fingerprint.

**Note: `jibe` and `talentbrew` are both iCIMS products** — they share `icims.com` infrastructure.
Single keyword `icims` covers both. Document together.

| platform | companies | keyword (known/guessed) | example company to check | status |
|---|---|---|---|---|
| workday | 5+ | `workday` | Accenture, Adobe, Agilent, AIG | ✅ LOCKED IN |
| greenhouse | 5 | `greenhouse` | Airbnb, Stripe, Adaptive Biotech | ✅ LOCKED IN |
| oracle_hcm | 5 | `oraclecloud` + `oj-hcm-ce` | Dell (`dell.com`) | ✅ documented (1 example, need 2 more) |
| phenom | 5 | `phenompeople` | eBay, Genentech | ✅ documented (2 examples, need 1 more) |
| smartrecruiters | 5 | `smartrecruiters` | Versant | ✅ documented (1 example, need 2 more) |
| successfactors | 5 | `j2w.init()` + `successfactors` | AGCO, EY | ✅ LOCKED IN |
| talentbrew + jibe | 5+4 | `talentbrew` / `icims` | Charles Schwab (`schwabjobs.com`) | ✅ talentbrew documented (1 example, needs tenant IDs) |
| eightfold | 3 | `eightfold` | Lam Research | ✅ documented (1 example, need 2 more) |
| lever | 3 | `lever` | Spotify | ⚠️ HTML scan fails — JS bundle scan required |
| avature | 2 | `avature` / `avatureReferrerQueryParam` | Wayfair (career page JSON blob), EA (`jobs.ea.com`) | ✅ documented (1 example, need Avature tenant URL from JD/apply page) |
| ashby | 2 | `ashbyhq` | Depth First, Snowflake | ✅ documented (1 example, need 2 more) |
| taleo | 1 | `taleo` | Cognizant | ✅ documented (1 example, need 2 more) |
| jobvite | 1 | `jobvite` | Nutanix (`nutanix.com`) | ⬜ |
| adp | 1 | `myjobs.adp.com` | ADP (`adp.com`) | ✅ documented (1 example, need slug-bearing URL) |
