# Email Pattern Inference — System Design

## Problem

CareerShift is the current source for recruiter contact info but is limited to 50 new contacts/day and costs money. We need a scalable, free alternative for finding recruiter email addresses without paying per-contact.

## Core Insight

Every H-1B sponsor files an LCA (ETA-9035E) with the DOL. Section D of the form includes:
- `EMPLOYER_POC_FIRST_NAME`
- `EMPLOYER_POC_LAST_NAME`
- `EMPLOYER_POC_EMAIL`
- `EMPLOYER_POC_JOB_TITLE`

When the POC email is a personal corporate email (not a generic `hr@` or `immigration@`), we have:
**name + email → derivable pattern**.

Since email format is org-wide policy, once we know the pattern for a company domain, we can generate email addresses for any employee at that company given their name.

---

## What We Get From LCA Data

| Field | Example |
|---|---|
| First name | Ravi |
| Last name | Shankar |
| Email | ravi.shankar@fourththechnologies.com |
| Job title | CEO/PRESIDENT |

From this single row: pattern = `{first}.{last}@domain` ✓

Multiple LCA filings per company (different quarters, different POCs) give multiple name+email pairs — each one either confirms the pattern or reveals an additional pattern variant.

---

## Pipeline Architecture — How Domain Resolution Now Works

### Old flow (KG-first)
```
Company name → KG search → website URL → career probe → ATS detection
```
KG was the only source of truth for website/domain. Complex name-matching logic built around it.

### New flow (LCA email-first)
```
DOL LCA + USCIS join → rank by H1B approval count → priority score
                                ↓
LCA emails → domain counting per FEIN → probe domain → follow redirect → website URL
                                                                        ↓
                                                              career probe (19 patterns, CF Worker)
                                                              → fein_domain_map.careers_url
                                                                        ↓
                                              (processed in priority order — top H1B sponsors first)
                                                                        ↓
                                                          career_page.py  ← gun: fast, BeautifulSoup
                                                              HIT → company_ats (source="career_page", is_monitored=FALSE)
                                                              MISS ↓
                                                          career_detector.py  ← rifle: BFS, JS bundles
                                                              HIT → company_ats (source="career_detector", is_monitored=FALSE)
                                                              MISS → "undetected" queue (frontend)
                                                                        ↓
                                                          manual review → set is_monitored=TRUE → job monitor picks up
                                                                        ↓
                                                          recruiter scraping (LCA email patterns + CareerShift)
                                                              → same priority order as ATS detection
```
KG/Wikidata is now a secondary enrichment layer, not the domain source of truth.

### Priority order for `website_url`
1. **LCA email-derived domain** → probe → follow redirect → confirmed website ✓ (primary)
2. **KG/Wikidata P856** → fallback if no email domain data exists for this FEIN
3. **DDG Instant Answers** → essentially deprecated (same Wikidata source as KG, low hit rate)
4. **Domain inference (name slug)** → essentially deprecated (too many false positives)

### Priority order for `careers_url`
1. **Career probe** (19 URL patterns, via CF Worker) → primary
2. **Wikidata P10311** (jobs URL property) → fallback if probe fails or gets blocked

### KG/Wikidata's remaining role (narrowed significantly)
- **Canonical name** — useful for display
- **Glassdoor ID** (Wikidata P2267) — for profile links on Discover page
- **Crunchbase ID** (Wikidata P2088) — for profile links on Discover page
- **Wikidata P10311** — career URL fallback only
- **Domain resolution** — no longer KG's job at all

---

## Domain Counting Per FEIN

### How it works
Across all LCA filings for a FEIN, count occurrences of each email domain (everything after `@`, whitespace stripped). Build a frequency map:
```
FEIN 123456789: {xyz.com: 80, xy.com: 20}  → assign xyz.com (80% confidence)
FEIN 987654321: {gmail.com: 5, amazon.com: 95} → exclude gmail.com → assign amazon.com
```

### Generic domain exclusion (decided)
Exclude these from counting entirely — never let them accumulate:
`gmail.com`, `yahoo.com`, `hotmail.com`, `outlook.com`, `aol.com`, `icloud.com`, `protonmail.com`, `live.com`

### Assignment threshold (decided)
- Always assign the winner (highest count after excluding generic domains)
- Set `low_confidence = TRUE` when either:
  - Total filings with non-generic email data < 3, OR
  - Winning domain share < 70%

### Domain probing (decided)
Once domain is assigned:
1. Probe `https://{domain}` → follow redirects (same as `_resolve_website_redirect`)
2. Final URL after redirect = `website_url`
3. Handles rebrands automatically (e.g. `lntinfotech.com` → `ltimindtree.com`)
4. All probing via CF Worker to avoid OCI datacenter IP blocks

### KG validation using email-derived domain (decided)
- If email-derived domain exists AND KG returns an entity → check if KG entity URL root domain matches email-derived domain
- Mismatch → hard reject that KG entity (not the right company)
- No email-derived domain → skip domain gate, fall back to existing name-matching gates
- Domain inference (name slug) is NOT used as a KG gate — too unreliable

### Parent/child resolution (decided)
Multiple FEINs sharing the same email domain all get assigned that domain:
- `Amazon.com Services LLC` → `@amazon.com` → website: `amazon.com`
- `Amazon Development Center U.S.` → `@amazon.com` → website: `amazon.com`
- Both profiles show `amazon.com` — correct, they ARE the same organisation

---

## Pattern Detection — Dynamic Template Approach

No hardcoded pattern list. Instead, patterns are reverse-derived from the data for each LCA row.

**Name component tokens** (mapped directly to LCA columns — no parsing needed):

| token | meaning | LCA source |
|---|---|---|
| `{fn}`, `{fi}`, `{fn[:N]}` | full first / initial / first N chars | `EMPLOYER_POC_FIRST_NAME` |
| `{mn}`, `{mi}`, `{mn[:N]}` | full middle / initial / first N chars | `EMPLOYER_POC_MIDDLE_NAME` |
| `{ln}`, `{li}`, `{ln[:N]}` | full last / initial / first N chars | `EMPLOYER_POC_LAST_NAME` |
| `{d}` | trailing digit suffix (collision resolver) | derived — strip from end of local part |

Middle name is a dedicated field in the LCA — no multi-word parsing required. This allows reconstruction of patterns like `{mn[:4]}{fn[:3]}` → `chanhpoo` for Pooja Chandahalli Shreenivasan.

**Do NOT use for pattern detection:**
- `PREPARER_EMAIL` — law firm / immigration attorney who filed the form, not the company
- `AGENT_ATTORNEY_EMAIL_ADDRESS` — attorney email, not a company email

**Separators:** `.`, `_`, `-`, `` (none)

**Algorithm:** try all combinations of 1–3 tokens + separator that reconstruct the local part exactly. If found → pattern stored as a template string. If none match → unrecognized (logged, skipped for Table 3).

```
local: "ravi.shankar"  →  {fn}.{ln}  ✓
local: "rshankar"      →  {fi}{ln}   ✓
local: "r_shankar"     →  {fi}_{ln}  ✓
local: "shankar.r"     →  {ln}.{fi}  ✓
local: "chanhpoo"      →  no match   (Amazon-style compressed — unrecognized)
```

**Normalization before matching:** lowercase, strip accents (`María` → `maria`), strip non-alpha characters (`O'Brien` → `obrien`).

### Digit Handling

Large companies append digits to resolve name collisions. Digits can appear **anywhere** in the local part — not just at the end:

```
jsmith2    →  trailing digit   (most common)
j2smith    →  digit in middle
john2.smith →  digit after first component
2jsmith    →  leading digit    (rare)
```

**Detection:** if the local part doesn't match any template as-is, locate all digit sequences within it, replace each with a `{d}` placeholder, and try matching the remaining parts against name tokens.

```
local: "jsmith2"   →  "jsmith{d}"    →  {fi}{ln}{d}   ✓
local: "j2smith"   →  "j{d}smith"    →  {fi}{d}{ln}   ✓
local: "john2.smith" →  "john{d}.smith" →  {fn}{d}.{ln} ✓
local: "jsmith"    →  direct match   →  {fi}{ln}      ✓
```

Template captures the digit position: `{fi}{ln}{d}`, `{fi}{d}{ln}`, etc. `has_digit = true` flagged on the pattern entry regardless of position.

**When generating:** produce the base email without digit (`jsmith@amazon.com`) and note "digit possible for this domain — try `jsmith2`, `jsmith3` if base bounces." Exact digit is unpredictable without internal directory access.

---

## The Multi-Pattern Problem (Amazon etc.)

Large companies with hundreds of thousands of employees sometimes develop multiple email patterns over time — typically when:
- Pattern 1 fills up (too many John Smiths → `jsmith` collides)
- Acquisitions bring in different format conventions
- Different orgs within the company use different formats

**Amazon examples from LCA data:**
```
Chandahalli Shreenivasan, Pooja  →  chanhpoo@amazon.com
Kannaiah, Shilpa                 →  kanshilp@amazon.jobs
Eedara, Lakshmi                  →  eedarana@amazon.jobs
```

These are non-standard compressed patterns — not easily derivable from a single example.

**How to handle:**
- Track ALL patterns observed per domain, not just one
- Score each pattern by number of confirmed observations across all LCA filings
- When generating a candidate email for a new person, generate one per known pattern, ranked by score
- High-scoring pattern (many observations) → high confidence
- Low-scoring / one-off patterns (Amazon-style) → lower confidence, flag as uncertain

---

## Generic / Non-Personal Emails

Some POC emails are not personal:
- `hr@company.com`, `immigration@company.com`, `hrlegal@company.com`, `info@company.com`

**Decision:**
- Cannot derive a name-based pattern from these → skip pattern derivation
- Still useful as direct HR contact emails → store separately
- Job title (`EMPLOYER_POC_JOB_TITLE`) helps classify intent

---

## Data Freshness / Staleness

- **Domain itself**: Low staleness risk — companies rarely change their email domain
- **Pattern**: Low staleness risk — org-wide email format policy rarely changes
- **POC as direct contact**: Higher staleness risk — person may have left the company. Flag with LCA filing date.

---

## Relationship to CareerShift

| | CareerShift | LCA Pattern Inference |
|---|---|---|
| Contacts/day | 50 | Unlimited |
| Cost | Paid per-contact | Free |
| Verified | Yes | No (guessed from pattern) |
| Scale | Bottleneck | Scales with LCA data |

**Decision:** CareerShift stays as primary verified source. Pattern inference is the scale path — bypasses per-contact cost entirely. High-confidence pattern (confirmed 10+ times) → nearly as reliable as CareerShift for that domain.

---

## Storage — Decided: 3-Table Design

### Table 1 — `fein_domain_map` (PK: `employer_fein`)

| column | type | purpose |
|---|---|---|
| `employer_fein` | TEXT PK | FK → `dol_h1b_employers` |
| `domain_counts` | JSONB | `{"infosys.com": 80, "infosysbpm.com": 20}` — raw counts per domain |
| `total_emails` | INT | total non-generic emails seen across all filings |
| `assigned_domain` | TEXT | winner — highest count after excluding generic domains |
| `confidence` | REAL | winning domain count / total_emails |
| `low_confidence` | BOOLEAN | `true` if `confidence < 0.70` — ratio alone handles all cases |
| `website_url` | TEXT | probed + redirect-resolved final URL |
| `careers_url` | TEXT | career page found via probe |
| `updated_at` | TIMESTAMPTZ | |

**Note on `low_confidence`**: the ratio alone captures everything — if 2 filings both point to `infosys.com`, confidence = 1.0 → not low confidence. If 2 filings split across two domains, confidence = 0.5 → low confidence. No need for a `total_emails < N` condition.

Feeds directly into the discovery pipeline: `assigned_domain` → probe → `website_url` → career probe → `careers_url`. Also used as KG hard gate (see above).

---

### Table 2 — `lca_contacts` (PK: `email`)

| column | type | purpose |
|---|---|---|
| `email` | TEXT PK | deduplicates naturally — same HR email across 50 filings = 1 row |
| `domain` | TEXT | extracted from email; FK → `email_patterns` |
| `first_name` | TEXT | from LCA |
| `last_name` | TEXT | from LCA |
| `job_title` | TEXT | from LCA |
| `employer_fein` | TEXT | FK → `dol_h1b_employers` |
| `employer_name` | TEXT | denormalised for display |
| `lca_case_number` | TEXT | unique LCA case identifier |
| `lca_quarter` | TEXT | which file (`FY2026_Q2`) |
| `decision_date` | DATE | freshness signal — how old is this contact? |
| `middle_name` | TEXT | from LCA `EMPLOYER_POC_MIDDLE_NAME` — dedicated field, required for `{mn}`/`{mi}`/`{mn[:N]}` pattern tokens |
| `is_generic` | BOOLEAN | `true` if `hr@`, `immigration@` etc. — no pattern derivable |

**Note on email as PK**: if the same person files under two subsidiary FEINs, PK keeps the most recent filing. Fine — for pattern building we only need the email+name pair once. If the same HR alias (`hr@company.com`) appears across 100 filings, stored once, last filing wins.

---

### Table 3 — `email_patterns` (PK: `domain`)

| column | type | purpose |
|---|---|---|
| `domain` | TEXT PK | e.g. `infosys.com` |
| `patterns` | JSONB | `[{pattern_id, count, probability, example_local, last_seen}]` |
| `total_unique_personal` | INT | unique non-generic emails used to build patterns |
| `updated_at` | TIMESTAMPTZ | last time patterns were recomputed |

**Probability formula**: `count / total_unique_personal` per pattern. Example — Amazon, 100 unique emails: `fn.ln → 0.50`, `f.ln → 0.20`, `fn.li → 0.20`.

**No `low_sample` flag** — `total_unique_personal` is exposed as a raw count. Context determines reliability: 1 email at Fourth Technologies (10 employees) = high confidence. 5 emails at Amazon (1.5M employees) = low confidence. User sees the count and decides.

### Pattern Storage Threshold — Probability Floor

**Decision: store a pattern only if `probability >= 5%`** (pattern count / total_unique_personal).

No hard count threshold — probability captures both ends of the spectrum naturally:

| scenario | total | count | probability | stored? |
|---|---|---|---|---|
| Fourth Technologies — only email seen | 1 | 1 | 100% | ✓ |
| Infosys `{fn}.{ln}` dominant pattern | 1000 | 847 | 84.7% | ✓ |
| Amazon `{fn}.{ln}` minority pattern | 8000 | 400 | 5.0% | ✓ |
| Amazon noise (one-off) | 8000 | 1 | 0.01% | ✗ |

Amazon-style compressed emails (`chanhpoo`, `kanshilp`) fail template matching entirely and never reach Table 3 at all — so "thousands of unique patterns" from unrecognizable emails is not a real concern.

**Generation output** for a new person — all patterns ≥ 5%, sorted descending by probability:
```
infosys.com — Deepa Nair:
  deepa.nair@infosys.com   (80%, 847 records on file)
  d.nair@infosys.com       (15%, 160 records on file)
  deepan@infosys.com        (5%,  53 records on file)
```
No arbitrary cap in the data layer. Return everything above the threshold. UI can collapse low-probability ones if needed — that's a frontend concern. Always surface info, let user decide.

---

---

## Columns Added to Existing Tables

### `dol_h1b_soc_breakdown` — wage aggregates

Wages are normalized to **annual equivalent** at ingest time:
`Hour × 2080 | Week × 52 | Bi-Weekly × 26 | Month × 12 | Year × 1`

NULL wages are excluded from aggregation. `wage_count` tracks how many filings contributed data.

| column | type | purpose |
|---|---|---|
| `wage_from_min` | REAL | minimum annual `WAGE_RATE_OF_PAY_FROM` across filings for this employer+SOC |
| `wage_from_max` | REAL | maximum annual `WAGE_RATE_OF_PAY_FROM` |
| `wage_from_sum` | REAL | sum for avg: `wage_from_sum / wage_count` |
| `wage_to_min` | REAL | minimum annual `WAGE_RATE_OF_PAY_TO` |
| `wage_to_max` | REAL | maximum annual `WAGE_RATE_OF_PAY_TO` |
| `wage_to_sum` | REAL | sum for avg: `wage_to_sum / wage_count` |
| `wage_count` | INT | filings with non-NULL wage data for this employer+SOC |

Enables all three query patterns without extra tables:
- **Per company avg wage**: `SUM(wage_from_sum) / SUM(wage_count)` across all SOC rows for a FEIN
- **Per SOC per company**: direct row read
- **Per SOC across all companies**: aggregate on `soc_code` across entire table

---

### `dol_h1b_employers` — wage rollup

Computed from `dol_h1b_soc_breakdown` at upsert time. Fast single-company lookup without aggregation.

| column | type | purpose |
|---|---|---|
| `wage_from_min` | REAL | company-wide minimum annual offered wage (from side) |
| `wage_from_max` | REAL | company-wide maximum annual offered wage (from side) |
| `wage_from_avg` | REAL | company-wide average annual offered wage (from side) |
| `wage_to_min` | REAL | company-wide minimum annual offered wage (to side) |
| `wage_to_max` | REAL | company-wide maximum annual offered wage (to side) |
| `wage_to_avg` | REAL | company-wide average annual offered wage (to side) |

---

## Table 4 — `generated_contacts` (Future Scope)

Not built yet. Generation is currently stateless — apply patterns from Table 3, return a ranked list, store nothing.

**When to build this**: once users can send emails through the platform and we have real send outcomes (bounces, replies) to track.

**Why it becomes necessary at that point**:
- Verification pipeline — need to persist an email to attach a verification status to it
- Reliability measurement — track outcome per generated email, feed back into Table 3 pattern quality scores over time

### Schema (when built)

| column | type | purpose |
|---|---|---|
| `email` | TEXT PK | the generated email address |
| `first_name` | TEXT | who it was generated for |
| `last_name` | TEXT | |
| `domain` | TEXT | FK → `email_patterns` |
| `pattern_id` | TEXT | which pattern generated it (`fn.ln`) |
| `pattern_confidence` | REAL | probability score at time of generation |
| `records_on_file` | INT | how many LCA samples the pattern was built from |
| `verification_status` | TEXT | `unverified` / `bounced` / `replied` |
| `generated_at` | TIMESTAMPTZ | |
| `verified_at` | TIMESTAMPTZ | |

**On SMTP verification**: not viable at scale — datacenter IPs flagged as harvesters, Google Workspace / O365 return `250 OK` for any address (anti-enumeration). Reliable signals only come from actual send outcomes (bounces, replies). Defer until users are sending emails through the platform.

---

## Pre-Deployment Checklist

Tasks to complete just before implementing and deploying this feature:

- [ ] **Fresh DB start** — wipe corrupted data (Squad Software → San Diego Padres incident and any similar KG mismatches); don't patch, start clean
- [ ] **Deploy CF Worker** — `wrangler deploy` + `wrangler secret put PROBE_SECRET`; required for domain probing and career page probing via non-datacenter IPs
- [ ] **Re-ingest all LCA files** — backfills `poc_email_domain` into `dol_h1b_employers` for all existing FEINs
- [ ] **Implement new pipeline design first** — current `discover_h1b_ats.py` uses old KG-first approach; new LCA email-first design, KG hard gate, and 3 new tables must be built before any discovery run
- [ ] **Run full 900 companies discovery** — only after above steps; validates new gates end-to-end

---

## Open Questions

None — all design decisions locked in. See "What IS Decided" below.

---

## What IS Decided

- Source: `EMPLOYER_POC_FIRST_NAME`, `EMPLOYER_POC_LAST_NAME`, `EMPLOYER_POC_EMAIL` from DOL LCA Excel (already downloaded)
- Generic email domains excluded from counting from the start (blocklist above)
- Always assign winner domain; flag `low_confidence` when < 3 filings or < 70% share
- Domain probed via redirect (same as `_resolve_website_redirect`), all via CF Worker
- Email-derived domain is a hard KG gate — mismatch → reject KG entity
- Domain inference (name slug) is NOT used as a KG gate
- Parent/child FEINs sharing same email domain → all get same website — correct and intentional
- Pattern is domain-scoped, not FEIN-scoped
- Multi-pattern companies → track all patterns with frequency scores
- Pattern staleness risk is low; direct POC contact staleness risk is higher
- CareerShift stays as primary verified source; pattern inference is the scale/free path
- LCA email domain already added to `dol_h1b_employers.poc_email_domain` as website fallback (existing change) — this feature builds on top of that
- KG/Wikidata role narrowed: canonical name + Glassdoor/Crunchbase IDs + P10311 fallback only
- **Storage**: 3-table design — `fein_domain_map`, `lca_contacts`, `email_patterns` (see above); plus wage aggregates added to `dol_h1b_soc_breakdown` and rollup columns to `dol_h1b_employers` (see "Columns Added to Existing Tables")
- `low_confidence` uses confidence ratio only (`< 0.70`); no `total_emails < N` condition
- `total_unique_personal` exposed as raw count; no `low_sample` boolean flag — user decides reliability
- Generated emails: stateless for now, not stored; Table 4 `generated_contacts` designed and deferred (see above)
- Middle name: `EMPLOYER_POC_MIDDLE_NAME` is a dedicated LCA column — stored as `middle_name` in `lca_contacts`; no parsing needed; closes `cameron.t.villa`-style open question
- Digit handling: `{d}` token can appear anywhere in the local part (not just trailing); detect by replacing digit sequences with `{d}` placeholder and retrying match
- Pattern storage threshold: probability floor of 5% (count / total_unique_personal) — no hard count threshold; naturally handles both small companies (1/1 = 100% → store) and large noisy ones (1/8000 = 0.01% → drop)
- Multi-pattern output: return ALL patterns ≥ 5% sorted by probability descending, no cap — UI handles collapse if needed
- **HR contacts**: surface separately from pattern-derived contacts — "Direct HR contacts on file" section with filing count as signal of how active the inbox is
- **Generation trigger + name source**: no name input from user — company profile shows the pattern itself with a dummy name (e.g. `firstname.lastname@infosys.com`). User derives specific emails themselves.
- **Feedback**: thumbs up/down per pattern after user tries it → feeds `verified_hits` counter back into Table 3. Stronger signal than per-email bounce tracking.
- **Scope**: email pattern inference is a future value-add feature. Core goal remains ATS detection and job monitor pipeline. Do not build this until the core pipeline is solid.
- SMTP verification not viable at scale from datacenter IPs; deferred until send pipeline exists
