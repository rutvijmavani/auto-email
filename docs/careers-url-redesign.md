# careers_url Canonicalization — Design & Implementation Plan

> Covers every decision made in the 2026-08-21/24 design review.
> Read this before touching any of the files listed in §9.
> §12 has the full before/after system flowchart — start there for orientation.

---

## 1. Problem Statement

Two separate fields store `careers_url` with diverging values:

| Table | Field | Written by | Semantics |
|---|---|---|---|
| `fein_domain_map` | `careers_url` | `domain_enrichment_worker` (Phase 3 + Phase 6) | HTTP probe result — real-time, most reliable |
| `h1b_ats_discovery` | `careers_url` | `discover_h1b_ats.py` + `discover_h1b_ats_worker` | Redundant copy, sometimes stale, sometimes different |

This causes:
- `api.py _background_verify` keys its Redis HEAD-check on `fein_domain_map.careers_url`
- Discover page badge keys its HEAD-check on `h1b_ats_discovery.careers_url`
- The two can differ → inconsistent badge vs API state
- `discover_h1b_ats_worker` re-runs Phase 3 + Phase 6 that enrichment worker already ran → pure wasted quota
- KG (Wikidata P10311) is probed **first**, overriding a real-time HTTP result → stale data wins

---

## 2. Canonical Data Model

**Single source of truth: `fein_domain_map.careers_url`**

Remove `careers_url` and `careers_source` from `h1b_ats_discovery`.
Add `careers_source` to `fein_domain_map` (already has `careers_url`).

### Schema changes

```sql
-- fein_domain_map: add careers_source column
ALTER TABLE fein_domain_map
    ADD COLUMN IF NOT EXISTS careers_source TEXT;
-- 'phase3' | 'phase6' | 'phase1_kg' | 'phase4_brave' | 'phase7' | 'head_check'

-- h1b_ats_discovery: drop the duplicate columns
ALTER TABLE h1b_ats_discovery DROP COLUMN IF EXISTS careers_url;
ALTER TABLE h1b_ats_discovery DROP COLUMN IF EXISTS careers_source;
```

Update `db/schema.py`:
- `fein_domain_map` CREATE TABLE → add `careers_source TEXT`
- `h1b_ats_discovery` CREATE TABLE → remove `careers_url`, `careers_source`
- Add migration block in `_run_migrations()` so existing installs get the column

---

## 3. Write Rules

### Write function — unconditional

`_write_careers()` always overwrites. No trigger param. No `WHERE careers_url IS NULL` guard.
The caller decides **whether** to call it — only call when a probe actively found a URL.
If nothing found → don't call → existing value is preserved automatically.

```python
def _write_careers(conn, fein: str, careers_url: str, source: str) -> None:
    conn.execute("""
        UPDATE fein_domain_map
        SET careers_url    = %s,
            careers_source = %s,
            updated_at     = NOW()
        WHERE employer_fein = %s
    """, (careers_url, source, fein))
```

### Why no guard is needed in the enrichment worker

The enrichment worker only receives companies in two situations:
1. `careers_url IS NULL` — pushed directly from producer (bypasses head_check)
2. `careers_url IS SET but dead` — head_check confirmed non-200 and routed here

In both cases Phase 3 should always run. The routing upstream is the guard — not a condition inside the worker.

### What trigger still controls

Trigger is not used inside write functions. It controls two routing decisions:

| Trigger | After enrichment worker — push to discovery? | head_check 200 response |
|---|---|---|
| `"enrichment"` | Yes, if petition_count ≥ gate | n/a (bypasses head_check) |
| `"staleness"` | Yes, if petition_count ≥ gate | n/a (bypasses head_check) |
| `"on_demand"` | **No** — stop at enrichment | **Stop** — URL is alive, nothing more to do |
| `"redetect"` | Yes, always (gate bypassed) | → discovery (ATS re-detection needed) |
| `"manual"` | Yes, always (gate bypassed) | → discovery |

**Note:** api.py pushes with trigger=`"enrichment"` when `careers_url IS NULL` (never enriched) — goes directly to `enrichment:on_demand`, bypassing head_check. The `"on_demand"` trigger only reaches the enrichment worker after head_check confirmed the URL is dead (Cases 3, 4, 6).

---

## 4. Priority Chain for careers_url Discovery

Ordered from most reliable to last resort. Each step only runs if the previous found nothing.

```
Step 1: Read fein_domain_map.careers_url
        If already set (and trigger != redetect/manual) → skip all below, go straight to Phase 7 ATS detection

Step 2: Phase 3 — HTTP path probe (discover_careers_url())
        Probes career subdomains + 16 paths from www.{public_domain}
        External domain jump guard applies (skip if redirect leaves company root)
        → write to fein_domain_map.careers_url if NULL (or overwrite if redetect)
        → set careers_source = 'phase3'

Step 3: KG / Wikidata P10311 fallback
        ONLY runs if Step 2 found nothing
        → write to fein_domain_map.careers_url if NULL
        → set careers_source = 'phase1_kg'

Step 4: Brave search (950/month quota — LAST RESORT before Phase 7)
        ONLY runs if Step 2 AND Step 3 both found nothing
        → write to fein_domain_map.careers_url if NULL
        → set careers_source = 'phase4_brave'

Step 5: Phase 7 — full ATS detector (career_detector.py detect_company())
        Runs with seed_url = fein_domain_map.careers_url (if set from any step above)
        If careers_url still NULL: probes all paths independently
        Phase 7 writes ats_platform + ats_slug → company_ats
        Phase 7 MAY discover a careers URL as a side effect → write to fein_domain_map.careers_url
        → set careers_source = 'phase7'
```

**Why KG is NOT first:** Phase 3 does real-time HTTP probing. KG data can be months stale. Phase 3 is free (no quota). KG is fallback only.

**Why Brave is last resort:** 950 request/month free-tier cap.

---

## 5. Queue Architecture

### Three workers, three queue pairs

Every queue has two lanes. Workers always check the `:on_demand` lane first, drain `:batch` when it is empty.

```
head_check:on_demand  (LIST — LPUSH/RPOP, FIFO)  ─┐
head_check:batch      (LIST — LPUSH/RPOP, FIFO)  ─┘→ head_check_worker

enrichment:on_demand  (LIST — LPUSH/RPOP, FIFO)  ─┐
enrichment:batch      (ZSET — petition_count)     ─┘→ domain_enrichment_worker

discovery:on_demand   (LIST — LPUSH/RPOP, FIFO)  ─┐
discovery:batch       (ZSET — petition_count)     ─┘→ discover_h1b_ats_worker
```

**REDETECT_QUEUE is eliminated.** It was a separate queue for the discover worker, but `discovery:batch` / `discovery:on_demand` with `trigger="redetect"` carries the same semantics. The `trigger` field drives worker behaviour; a separate queue name added no value.

### Why LIST for on_demand, ZSET for batch — and why head_check is all LISTs

- **on_demand (LIST):** all on_demand items are equally urgent — FIFO is correct. `BLPOP` blocks efficiently, no polling.
- **enrichment:batch / discovery:batch (ZSET):** petition_count ordering is meaningful here — spend expensive KG/Brave/CF quota on high-value companies first. `ZPOPMAX` gives highest score. `ZADD GT` deduplicates on the member string.
- **head_check:batch (LIST, not ZSET):** HEAD check is a cheap 2-3s HTTP call (or an instant Redis cache hit). No meaningful reason to prioritise high-petition companies over low-petition ones at this stage — FIFO is fine. Deduplication is handled by the Redis cache (see §6) — same FEIN popped twice within 6h hits the cache on the second pop, routes instantly at zero HTTP cost.

### No magic priority scores

Previous design used `ON_DEMAND_SCORE = 999_999` as a ceiling in a shared ZSET. That encodes two different dimensions (urgency + importance) into one number — fragile, arbitrary, collidable.

The two-lane design makes urgency structural, not numeric. on_demand items are always processed first because they are in a separate queue that workers check first. No ceiling numbers needed.

### Worker loop pattern

head_check_worker — both lanes are LISTs:
```python
while True:
    flush_delayed()  # not applicable for head_check but kept for consistency
    item = r.blpop("head_check:on_demand", "head_check:batch", timeout=1)
    if not item:
        break
    process(item)
```

enrichment_worker and discovery_worker — on_demand is LIST, batch is ZSET:
```python
while True:
    flush_delayed()  # move ready delayed items into :batch
    item = r.blpop(f"{queue}:on_demand", timeout=1)
    if not item:
        item = r.zpopmax(f"{queue}:batch")
    if not item:
        break  # both empty — worker exits cleanly
    process(item)
```

### Universal queue member schema

Every producer, every queue, same three keys. Missing values are `null`, never omitted.

```json
{"fein": "123456789", "trigger": "enrichment|staleness|on_demand|redetect|manual", "source": "company_ats|prospective|job_monitor|staleness_checker|api|null"}
```

### Tier propagation through hops

The `tier` of an item (on_demand vs batch) travels with it through every worker hop:
- on_demand item routed by head_check_worker → pushed to `enrichment:on_demand` or `discovery:on_demand`
- batch item routed by head_check_worker → pushed to `enrichment:batch` or `discovery:batch`

An on_demand item never gets demoted to batch mid-chain.

### Who pushes where

| Producer | Trigger | Lane |
|---|---|---|
| `api.py` / Discover page (user visit, careers_url NULL, never enriched) | `"enrichment"` | `enrichment:on_demand` |
| `api.py` / Discover page (user visit, any other case) | `"on_demand"` | `head_check:on_demand` |
| `staleness_checker` enrichment pass | `"staleness"` | `enrichment:batch` |
| `staleness_checker` redetect pass | `"redetect"` | `head_check:batch` |
| `job_monitor` (zero-jobs streak) | `"redetect"` | `head_check:batch` |
| `fuzzy_match_uscis_dol` (bulk after USCIS load) | `"enrichment"` | `enrichment:batch` |
| `domain_enrichment_worker` (after enrichment, petition_count ≥ threshold) | `"enrichment"` | `discovery:batch` |
| head_check_worker (HEAD=200 or valid redirect, source was on_demand) | `"redetect"` | `discovery:on_demand` |
| head_check_worker (HEAD=200 or valid redirect, source was batch) | `"redetect"` | `discovery:batch` |
| head_check_worker (non-200, source was on_demand) | original trigger | `enrichment:on_demand` |
| head_check_worker (non-200, source was batch) | original trigger | `enrichment:batch` |

### Delayed queues — no starvation

`enrichment:delayed` and `discovery:delayed` are ZSETs scored by `not_before` Unix timestamp. They are **not** a separate priority tier that competes at the bottom of the queue indefinitely.

Each worker loop calls `flush_delayed()` at the **top** of every cycle — before touching on_demand or batch:

```
top of every loop cycle:
  1. flush_delayed() → ZRANGEBYSCORE delayed -inf now() → ZADD batch (score preserved)
  2. BLPOP :on_demand (or 1s timeout)
  3. ZPOPMAX :batch
  4. break if both empty
```

When a delayed item's timestamp passes, it is promoted into the batch ZSET with its original petition_count score and competes normally from that point. A Certspotter-delayed item waits exactly as long as the Retry-After header says, then resumes with the same petition_count priority it had before. A KG-exhausted item in `discovery:delayed` waits until midnight quota reset (score = now()+86400), then joins discovery:batch at normal priority.

### Petition gate

The gate (only companies above threshold go to discovery) lives at push time, not inside the discover worker. `domain_enrichment_worker` checks petition_count before pushing to `discovery:batch`. Items with `trigger="redetect"` bypass the gate — the company is already monitored, it already earned its place.

### Manager.py autoscaling

Manager.py must watch all six lanes:
- `LLEN head_check:on_demand` + `LLEN head_check:batch` → start/stop head_check_worker instances
- `LLEN enrichment:on_demand` + `ZCARD enrichment:batch` → start/stop domain_enrichment_worker instances
- `LLEN discovery:on_demand` + `ZCARD discovery:batch` → start/stop discover_h1b_ats_worker instances

---

## 6. HEAD Check Worker — 6 Cases

`workers/head_check_worker.py` — lightweight, no quota, just HTTP HEAD + Redis cache + routing.

### Redis cache

```
Key:   head_check:{fein}
Value: {"url": "...", "status": 200, "final_url": "...", "checked_at": <epoch>}
TTL:   HEAD_CHECK_CACHE_TTL (config.py, default 6h)
```

On cache hit: verify stored `url` matches current `fein_domain_map.careers_url`. If mismatch → treat as miss (URL changed in DB since last check).

**Cache invalidation:** `domain_enrichment_worker` must `r.delete(f"head_check:{fein}")` after writing a new `careers_url`. Otherwise head_check_worker would see a stale cache entry pointing to the old dead URL.

**Deduplication:** head_check:batch is a LIST — no ZSET member deduplication. The Redis cache handles this naturally: if the same FEIN is pushed twice and popped twice within the TTL window, the second pop is a cache hit and routes instantly at zero HTTP cost. No extra dedup mechanism needed.

### The 6 cases

```
HEAD careers_url → follow redirect chain (up to HEAD_CHECK_MAX_REDIRECTS)
                          ↓
Inspect final_url:

CASE 1: Redirect → same root domain, careers-like path
  careers.stripe.com → stripe.com/careers
  → URL just moved within same domain
  → UPDATE fein_domain_map SET careers_url = final_url, careers_source = 'head_check'
  → r.delete(head_check:{fein})   ← invalidate cache so next check uses new URL
  → on_demand trigger: STOP (URL updated, user is served)
  → redetect/other: push discovery  trigger="redetect"

CASE 2: Redirect → known ATS domain (greenhouse.io, lever.co, workday, etc.)
  careers.stripe.com → boards.greenhouse.io/stripe
  → Company switched to hosted ATS — new careers_url found for free
  → UPDATE fein_domain_map SET careers_url = final_url, careers_source = 'head_check'
  → r.delete(head_check:{fein})
  → on_demand trigger: STOP
  → redetect/other: push discovery  trigger="redetect"

CASE 3: Redirect → same domain, homepage / non-careers path
  careers.stripe.com → stripe.com   (no career path)
  → Careers page removed, not just moved
  → push enrichment  trigger=original

CASE 4: Redirect → unrelated 3rd party / unknown domain
  careers.stripe.com → somecdn.com/404
  → Dead link, no useful signal
  → push enrichment  trigger=original

CASE 5: No redirect, clean 200
  → URL is healthy
  → on_demand trigger: STOP (URL is alive, nothing to do)
  → redetect/other: push discovery  trigger="redetect"

CASE 6: Timeout / connection error
  → Treat conservatively as dead
  → push enrichment  trigger=original
```

Cases 1 and 2 self-heal `careers_url` in-place — no enrichment worker run needed.
Estimated 40–60% reduction in enrichment:batch traffic vs naive non-200 routing.

### classify_redirect helper

```python
KNOWN_ATS_DOMAINS = {
    "greenhouse.io", "lever.co", "ashbyhq.com", "myworkdayjobs.com",
    "myworkdaysite.com", "icims.com", "successfactors.com", "taleo.net",
    "eightfold.ai", "avature.net", "phenompeople.com", "talentbrew.com",
    "smartrecruiters.com", "jobvite.com",
}
CAREER_PATH_TERMS = {"career", "careers", "job", "jobs", "work", "join", "hiring", "talent"}

def classify_redirect(original_url, final_url):
    orig_root = extract_root_domain(original_url)
    final_root = extract_root_domain(final_url)

    if final_root in KNOWN_ATS_DOMAINS:
        return "ats_redirect"          # Case 2

    if final_root == orig_root:
        path = urlparse(final_url).path.lower()
        if any(t in path for t in CAREER_PATH_TERMS):
            return "same_domain_careers"   # Case 1
        return "same_domain_homepage"      # Case 3

    return "unrelated"                     # Case 4
```

### head_check_worker needs a DB connection

Cases 1 and 2 require a single `UPDATE fein_domain_map SET careers_url = %s, careers_source = 'head_check' WHERE employer_fein = %s`. Lightweight write, worth it to avoid a full enrichment worker run.

### api.py on-visit flow

User visits company X:
1. Return cached DB data immediately — never block the user
2. Push to `head_check:on_demand` (fast Redis write, no HTTP in api.py)
3. head_check_worker picks it up asynchronously — routes to discovery or enrichment

Special case — `careers_url IS NULL` and `last_enriched_at IS NULL` (never enriched):
- Skip HEAD_CHECK entirely (nothing to check)
- Push directly to `enrichment:on_demand` with trigger=`"enrichment"`

---

## 7. discover_h1b_ats_worker Redesign

### What to REMOVE
- Phase 3 re-probe (`discover_careers_url()` call inside the worker) — enrichment worker already ran this
- Phase 6 re-run (`detect_via_career_page()` call inside the worker) — enrichment worker already ran this
- Writing `careers_url` / `careers_source` to `h1b_ats_discovery` (table no longer has these columns)

### New decision tree

```python
# Step 1: check company_ats — is ATS already known?
existing_ats = query company_ats WHERE employer_fein = fein AND is_monitored = TRUE

if existing_ats and trigger not in ("redetect", "manual"):
    update last_discovered_at = NOW()
    return  # already detected, not a re-detection run

# Step 2: read careers_url set by enrichment worker (Phase 3/6 already ran there)
careers_url = SELECT careers_url FROM fein_domain_map WHERE employer_fein = fein

# Step 3: KG / Wikidata P10311
#   Only runs if careers_url is NULL or trigger forces re-detection
if not careers_url or trigger in ("redetect", "manual"):
    if not kg_checked:
        kg_result = lookup_wikidata_p10311(fein)
        mark kg_checked = True
        if kg_result and kg_result is direct ATS URL:
            write ats_platform, ats_slug → company_ats
            run_phase7(seed_url=kg_result)
            return
        if kg_result:
            careers_url = kg_result
            _write_careers(conn, fein, careers_url, source="phase1_kg")

# Step 4: Brave search (only if KG also found nothing)
    if not careers_url:
        careers_url = brave_search_careers(company_name)
        if careers_url:
            _write_careers(conn, fein, careers_url, source="phase4_brave")

# Step 5: Phase 7 ATS detection — always runs
run_phase7(seed_url=careers_url)  # seed_url may be None
```

### write_careers() — unconditional, same in both workers

```python
def _write_careers(conn, fein: str, careers_url: str, source: str) -> None:
    conn.execute("""
        UPDATE fein_domain_map
        SET careers_url    = %s,
            careers_source = %s,
            updated_at     = NOW()
        WHERE employer_fein = %s
    """, (careers_url, source, fein))
```

Called only when a probe actively found a URL. No trigger param. No WHERE NULL guard.
Same function shape in both `domain_enrichment_worker` and `discover_h1b_ats_worker`.

---

## 8. systemd [Install] Fix

### Problem
Non-template unit files `domain-enrichment-worker.service` and `discover-h1b-ats-worker.service`
both have `[Install] WantedBy=multi-user.target`.
`install-systemd.sh` enables all non-`@` units at boot.
Result: boot starts `domain-enrichment-worker.service` AND manager.py starts `@1`/`@2` → 3 workers compete.

### Fix
Remove `[Install]` sections from BOTH non-template `.service` files.
Workers start exclusively via manager.py autoscaler — not at boot.

Update `install-systemd.sh`:
```bash
[[ "$_unit" == "domain-enrichment-worker.service" ]] && continue
[[ "$_unit" == "discover-h1b-ats-worker.service" ]] && continue
```

---

## 9. File-by-File Implementation Order

### Must follow this order — each step depends on the previous

**1. `db/schema.py`**
   - Add `careers_source TEXT` to `fein_domain_map`
   - Remove `careers_url`, `careers_source` from `h1b_ats_discovery`
   - Add migration in `_run_migrations()`

**2. `config.py`**
   - Add queue name constants: `HEAD_CHECK_ON_DEMAND`, `HEAD_CHECK_BATCH`, `ENRICHMENT_ON_DEMAND`, `ENRICHMENT_BATCH`, `DISCOVERY_ON_DEMAND`, `DISCOVERY_BATCH`
   - Remove `REDETECT_QUEUE` constant
   - Add `HEAD_CHECK_CACHE_TTL`, `HEAD_CHECK_TIMEOUT_S`, `HEAD_CHECK_MAX_REDIRECTS`

**3. `workers/head_check_worker.py`** (new file)
   - Redis cache check → HEAD request → classify_redirect → route
   - DB write for Cases 1 + 2 (self-heal careers_url)
   - Cache invalidation after DB write
   - Tier propagation: source tier → correct on_demand or batch lane
   - Systemd unit file: `head-check-worker@.service`

**4. `workers/domain_enrichment_worker.py`**
   - `_write_careers()`: add `trigger` param; conditional overwrite vs fill-NULL
   - Write `careers_source` alongside `careers_url`
   - After write: `r.delete(f"head_check:{fein}")` to invalidate cache
   - Pass trigger through from queue payload
   - After enrichment: push to `discovery:batch` (not old DISCOVERY_QUEUE) if threshold met

**5. `scripts/discover_h1b_ats.py`** (nightly batch script)
   - Remove `careers_url`, `careers_source` from `upsert_discovery()`
   - Fix KG priority: Phase 3 first, KG fallback only
   - Phase 7 writes careers_url to `fein_domain_map` if currently NULL

**6. `workers/discover_h1b_ats_worker.py`**
   - Consume from `discovery:on_demand` + `discovery:batch` (new lanes)
   - Remove Phase 3 re-probe, Phase 6 re-run
   - Implement new decision tree (§7 above)
   - `write_careers()` with trigger-aware logic
   - No writes to `h1b_ats_discovery.careers_url`

**7. `scripts/staleness_checker.py`**
   - Redetect pass (3a/3b): push to `head_check:batch` instead of `REDETECT_QUEUE`
   - All pushes use universal member schema (trigger + source always present)

**8. `jobs/job_monitor.py`**
   - Zero-jobs streak: push to `head_check:batch` instead of `REDETECT_QUEUE`
   - Universal member schema

**9. `api.py`**
   - On-visit: push to `head_check:on_demand` (not inline HEAD check, not daemon thread)
   - Special case NULL + never enriched: push to `enrichment:on_demand` trigger="enrichment"
   - `_background_verify()`: HEAD-check Redis key uses `fein_domain_map.careers_url`
   - `verify_company()` DB query: read from `fein_domain_map` not `h1b_ats_discovery`
   - `finally` block: guard `conn.rollback()` so `conn.close()` always runs

**10. `frontend/pages/3_Discover.py`**
   - Badge query: JOIN `fein_domain_map` for `careers_url` display
   - On-visit: push to `head_check:on_demand`

**11. `workers/manager.py`**
   - Watch all six lanes (LLEN on_demand + ZCARD batch for each worker type)
   - Add head_check_worker to autoscaling logic

**12. `workers/worker_control.py`**
   - Add `HEAD_CHECK_WORKERS` tuple
   - Remove any `REDETECT_WORKERS` reference

**13. `deploy/systemd/`**
   - Add `head-check-worker@.service` (template unit, no [Install])
   - Remove `[Install]` from `domain-enrichment-worker.service`
   - Remove `[Install]` from `discover-h1b-ats-worker.service`

**14. `deploy/install-systemd.sh`**
   - Register `head-check-worker@.service` in all three places
   - Add skip entries for the two non-template units

---

## 10. Pending Pass 53 Confirmed Fixes (independent of redesign)

Apply these separately — they don't depend on the redesign being complete.

| # | File | Fix |
|---|---|---|
| 1 | `api.py` ~533-537 | Guard `conn.rollback()` in finally block so `conn.close()` always runs |
| 2 | `scripts/staleness_checker.py` ~255-256 | Fix LOWER ordering in prospective domain join to match index expressions exactly |
| 3 | `scripts/health_check.py` ~656-677 | Move NULL filters inside DISTINCT ON subquery |
| 4 | `scripts/pipeline_metrics.py` ~183-228 | Same NULL filter fix |

### staleness_checker.py index expression fix

```sql
-- idx_fdm_assigned_domain_norm: LOWER first, then scheme strip, then www strip
-- idx_pc_domain_norm:           scheme strip first, then LOWER, then www strip

-- Query join must match EACH index verbatim:
ON regexp_replace(regexp_replace(LOWER(f.assigned_domain), '^https?://', ''), '^www\.', '') =
   regexp_replace(LOWER(regexp_replace(pc.domain, '^https?://', '')), '^www\.', '')
```

---

## 11. What NOT to Do

- **Do NOT use `REDETECT_QUEUE`.** It is eliminated. Use `discovery:on_demand` or `discovery:batch` with `trigger="redetect"`.
- **Do NOT use a single ZSET with a magic ceiling score for on_demand priority.** Use the two-lane design (LIST for on_demand, ZSET for batch).
- **Do NOT push to `head_check:*` when careers_url is NULL and last_enriched_at is NULL.** Nothing to HEAD check — push directly to `enrichment:on_demand`.
- **Do NOT do the HEAD check inline in api.py** (daemon thread, no retry, no observability). Push to `head_check:on_demand` instead.
- **Do NOT clear `careers_url = NULL` at the start of a redetect run.** Only overwrite when a probe actively finds a new URL.
- **Do NOT run Phase 3 or Phase 6 inside the discovery worker.** Read `fein_domain_map.careers_url` instead.
- **Do NOT put KG lookup before Phase 3.** KG is a fallback, not first-choice.
- **Do NOT check `careers_url_verified_at` to skip HEAD checks.** Always run — it's background and costs nothing to the user.
- **Do NOT write `careers_url` to `h1b_ats_discovery` after the schema migration.** That column no longer exists.
- **Do NOT forget to `r.delete(f"head_check:{fein}")` after writing a new careers_url.** Stale cache causes wrong routing on the next visit.

---

## 12. Full System Flowchart — TODAY vs AFTER REDESIGN

Use this section to orient before touching any file. The flowchart shows the complete data path end-to-end, all queues, all quota systems, and what happens at exhaustion.

---

### TODAY (current production state)

```
┌───────────────────────────────────────────────────────────────────────────┐
│  DATA INGESTION                                                           │
│  DOL LCA upload → sync_dol_lca → process_dol_lca → fein_domain_map      │
│  USCIS upload   → process_uscis_h1b → fuzzy_match_uscis_dol             │
│                                  └→ pushes to domain_enrichment_queue    │
└───────────────────────────────────────────────────────────────────────────┘
          │
          ▼
┌──────────────────────────────┐   ┌──────────────────────────────────────┐
│  domain_enrichment_queue     │   │  redetect_queue                      │
│  (ZSET, score=petition_count)│   │  (ZSET — inconsistent member schema) │
│  ← staleness_checker Pass 1  │   │  ← staleness_checker Pass 3          │
│  ← fuzzy_match_uscis_dol     │   │    (payload missing "trigger" key)   │
│  ← api.py on-demand          │   │  ← job_monitor zero-jobs streak      │
│    (score=ENRICHMENT_ON_      │   │    (payload missing "trigger" key)   │
│     DEMAND_SCORE=500)         │   │  consumer: ambiguous                 │
└──────────────────────────────┘   └──────────────────────────────────────┘
          │                                       │
          └──────────────┬────────────────────────┘
                         ▼
          ┌────────────────────────────────────────────┐
          │  domain_enrichment_worker (@1, @2)          │
          │                                             │
          │  Step 1: public domain resolution           │
          │    CF Worker probe  ← cf_quota.json         │
          │    Certspotter CT   ← 10/hr, react 429     │
          │    → enrichment:delayed on 429              │
          │  Step 2: Phase 3 — discover_careers_url()   │
          │    CF Worker        ← cf_quota.json         │
          │  Step 3: Phase 6 — detect_via_career_page() │
          │    CF Worker        ← cf_quota.json         │
          │                                             │
          │  Write: fein_domain_map.careers_url         │
          │  Write: fein_domain_map.public_domain       │
          │  Write: company_ats (if ATS found)          │
          └────────────────────────────────────────────┘
                         │
                         ▼ (petition_count ≥ threshold)
          ┌──────────────────────────────┐
          │  discovery_queue             │
          │  (ZSET, score=petition_count)│
          │  ← enrichment_worker         │
          │  ← staleness_checker Pass 2  │
          └──────────────────────────────┘
                         │
                         ▼
          ┌─────────────────────────────────────────────┐
          │  discover_h1b_ats_worker (@1, @2)            │
          │                                             │
          │  BUG: Step 1 = KG lookup (FIRST!)           │
          │    ← kg_quota.json 85K/day                  │
          │    ← KG data can be months stale            │
          │  Step 2: Wikidata SPARQL (P10311)            │
          │  BUG: Step 3 = Phase 3 re-run               │
          │    ← CF Worker (DOUBLE-RUN — wasted quota)  │
          │  Step 4: Brave search                        │
          │    ← brave_quota.json 950/month             │
          │  BUG: Step 5 = Phase 6 re-run               │
          │    ← CF Worker (DOUBLE-RUN — wasted quota)  │
          │  Step 6: Phase 7 — career_detector           │
          │                                             │
          │  BUG: writes careers_url to BOTH:           │
          │    fein_domain_map.careers_url              │
          │    h1b_ats_discovery.careers_url ← STALE    │
          └─────────────────────────────────────────────┘

┌──────────────────────────────────────────────────────────┐
│  NIGHTLY CRON (discover_h1b_ats.py --top N)              │
│  Pass 1: KG + HTTP probe for ~100 companies              │
│  Pass 2: --brave-pass (Brave only)                       │
│  PROBLEM: blows KG 85K/day + Brave 950/month overnight   │
│           leaves zero quota for event-driven workers     │
└──────────────────────────────────────────────────────────┘

┌──────────────────────────────────────────────────────────┐
│  DAILY CRON (staleness_checker.py)                       │
│  Pass 1: enriched > 90d → domain_enrichment_queue        │
│  Pass 2: discovered > 90d → discovery_queue              │
│  Pass 3: consecutive_empty_days ≥ 14 → redetect_queue    │
│    BUG: member schema missing "trigger" key              │
└──────────────────────────────────────────────────────────┘

┌──────────────────────────────────────────────────────────┐
│  ALWAYS-ON job_monitor daemon                            │
│  10 scan_workers + 2 detail_workers + 2 fullscan_workers │
│  + 1 watchdog → job listings DB                          │
│  on consecutive_empty_days ≥ threshold:                  │
│    → redetect_queue (BUG: no "trigger" in payload)       │
└──────────────────────────────────────────────────────────┘

QUOTA STATE TODAY:
  KG 85K/day      → nightly batch consumes nearly all overnight
  Brave 950/month → nightly batch consumes nearly all overnight
  CF Worker daily → drained further by double-runs in discover worker
  Certspotter 10/hr → react-to-429 works; enrichment:delayed exists
```

---

### AFTER REDESIGN

```
┌───────────────────────────────────────────────────────────────────────────┐
│  DATA INGESTION (unchanged)                                               │
│  DOL LCA → fein_domain_map  (careers_url lives HERE ONLY)                │
│  USCIS → fuzzy_match_uscis_dol                                           │
│  h1b_ats_discovery.careers_url column: DROPPED                           │
└───────────────────────────────────────────────────────────────────────────┘

ROUTING RULE — producers check fein_domain_map.careers_url before pushing:
  careers_url IS NULL → skip head_check entirely → enrichment:batch/on_demand
  careers_url NOT NULL → head_check:batch/on_demand

QUEUE LANES (universal member schema on all: {fein, trigger, source}):

  head_check:on_demand  (LIST)   ← api.py on-demand visit (careers_url known)
  head_check:batch      (LIST)   ← staleness_checker Pass 1 (careers_url known),
                                    staleness_checker Pass 3 (redetect),
                                    job_monitor (redetect)

  enrichment:on_demand  (LIST)   ← api.py (careers_url NULL),
                                    head_check_worker (Cases 3,4,6 — on_demand)
  enrichment:batch      (ZSET)   ← fuzzy_match_uscis_dol (careers_url NULL),
                                    staleness_checker Pass 1 (careers_url NULL),
                                    head_check_worker (Cases 3,4,6 — batch)
  enrichment:delayed    (ZSET scored by not_before) ← Certspotter 429

  discovery:redetect    (ZSET scored by petition_count)
                                  ← head_check_worker (Cases 1,2,5 — trigger="redetect"),
                                    staleness_checker Pass 3 (via head_check),
                                    job_monitor (via head_check)
  discovery:batch       (ZSET scored by petition_count)
                                  ← staleness_checker Pass 2 (trigger="staleness"),
                                    domain_enrichment_worker (after enrichment,
                                      trigger != on_demand, petition_count ≥ gate)
  discovery:delayed     (ZSET scored by not_before) ← KG quota exhaustion

  discovery:redetect always drained before discovery:batch — worker ZPOPMAX
  redetect first, then batch. Priority is structural (pop order), not score-based.

  DLQs: head_check:dlq, enrichment:dlq, discovery:dlq

┌─────────────────────────────────────────────────────────────────┐
│  ENTRY POINT — producer checks careers_url state before pushing │
└─────────────────────────────────────────────────────────────────┘
                          │
          ┌───────────────┴───────────────┐
          │                               │
  careers_url IS NULL              careers_url IS SET
          │                               │
          ▼                               ▼
  enrichment:on_demand (LIST)    head_check:on_demand (LIST)
  enrichment:batch    (ZSET)     head_check:batch    (LIST)
  (skip head_check)              (verify first — FIFO, no score)
          │                               │
          │                               ▼
          │               ┌──────────────────────────────────┐
          │               │  head_check_worker               │
          │               │                                  │
          │               │  BLPOP on_demand then batch      │
          │               │                                  │
          │               │  Check Redis: head_check:{fein}  │
          │               │    cache HIT + url matches DB    │
          │               │      → use cached result (0 HTTP)│
          │               │    cache MISS or url mismatch    │
          │               │      → HTTP HEAD → follow chain  │
          │               │      → cache result TTL=6h       │
          │               │                                  │
          │               │  Dedup: same FEIN popped twice   │
          │               │  within TTL → cache hit on 2nd   │
          │               │  pop, routes instantly, 0 HTTP   │
          │               └──────────────────────────────────┘
          │                               │
          │         ┌─────────────────────┼──────────────────┐
          │         │                     │                  │
          │       200 OK             Redirect           Non-200/dead
          │         │                     │                  │
          │         │           _write_careers(url)          │
          │         │           (always, irrespective of     │
          │         │            trigger — URL changed)      │
          │         │           r.delete(head_check:{fein})  │
          │         │                     │                  │
          │    ┌────┴────┐           ┌────┴────┐             │
          │ on_demand redetect    on_demand  redetect         │
          │    │        │             │         │             │
          │  STOP   discovery       STOP    discovery         │
          │         :redetect               :redetect         │
          │                                                   │
          └───────────────────┬───────────────────────────────┘
                              │  (non-200/dead from head_check,
                              │   OR careers_url NULL from start)
                              ▼
          ┌──────────────────────────────────────────────────┐
          │  enrichment_worker (domain_enrichment_worker.py) │
          │                                                  │
          │  BLPOP on_demand then batch                      │
          │  top of loop: flush enrichment:delayed           │
          │    always → enrichment:batch (one target only)   │
          │                                                  │
          │  Step 1: Phase 1 — public domain resolution      │
          │    → CF Worker probe  ← cf_quota.json            │
          │    → certspotter      ← 10/hr, react 429         │
          │    → if found: write public_domain to DB now     │
          │    → certspotter 429: push enrichment:delayed    │
          │      (score = now + Retry-After), return         │
          │                                                  │
          │  Step 2: Phase 3 — HTTP career path probe        │
          │    → always run — no careers_url guard           │
          │      (routing upstream already ensures we are    │
          │       here only when URL is missing or dead)     │
          │    → if found:                                   │
          │        _write_careers(url, source='phase3')      │
          │        skip Phase 6                              │
          │    → if not found: continue to Phase 6           │
          │                                                  │
          │  Step 3: Phase 6 — career page ATS scan          │
          │    → only if Phase 3 found nothing               │
          │    → if finds careers_url:                       │
          │        _write_careers(url, source='phase6')      │
          │    → if finds ATS platform/slug:                 │
          │        write to company_ats                      │
          │                                                  │
          │  Step 4: push to discovery:batch                 │
          │    → only if petition_count ≥ gate               │
          │    → AND trigger is NOT "on_demand"              │
          │    → even if Phase 3 + Phase 6 found nothing     │
          │      (pass down naturally)                       │
          └──────────────────────────────────────────────────┘
                              │
                              ▼
          ┌──────────────────────────────────────────────────┐
          │  discovery_worker (discover_h1b_ats_worker.py)   │
          │                                                  │
          │  ZPOPMAX redetect first, then batch              │
          │  (no on_demand lane — priority is structural)    │
          │  top of loop: flush discovery:delayed            │
          │    trigger="redetect" → discovery:redetect       │
          │    else               → discovery:batch          │
          │                                                  │
          │  Step 1: read careers_url from fein_domain_map   │
          │    (written by enrichment worker — no Phase 3)   │
          │                                                  │
          │  Step 2: KG / Wikidata P10311                    │
          │    → kg_quota.json 85K/day                       │
          │    → exhausted: push discovery:delayed           │
          │      (score = now + 86400), return               │
          │    → if found: _write_careers(url, source='kg')  │
          │                                                  │
          │  Step 3: Brave (only if KG found nothing)        │
          │    → brave_quota.json 950/month                  │
          │    → exhausted: skip, go Phase 7, log WARNING    │
          │    → if found: _write_careers(url, source='brave'│
          │                                                  │
          │  Step 4: Phase 7 — career_detector (always runs) │
          │  write company_ats                               │
          └──────────────────────────────────────────────────┘

_write_careers() — unconditional, no trigger param, same in both workers:

  UPDATE fein_domain_map
  SET careers_url    = %s,
      careers_source = %s,
      updated_at     = NOW()
  WHERE employer_fein = %s

  Called only when probe found a URL. Existing value always overwritten.
  Routing upstream (entry check + head_check) ensures this is only called
  when appropriate.

┌──────────────────────────────────────────────────────────┐
│  NIGHTLY CRON (discover_h1b_ats.py)                      │
│  STATUS: DISABLED (cron timer off after worker confirmed)│
│  Functions stay — both workers import from this script   │
└──────────────────────────────────────────────────────────┘

┌──────────────────────────────────────────────────────────┐
│  DAILY CRON (staleness_checker.py) — updated             │
│  Pass 1: enriched > 90d, careers_url NOT NULL            │
│           → head_check:batch  (trigger="staleness")      │
│          enriched > 90d, careers_url NULL                │
│           → enrichment:batch  (trigger="staleness")      │
│  Pass 2: discovered > 90d → discovery:batch (trigger=   │
│           "staleness")                                   │
│  Pass 3: consecutive_empty_days ≥ 14                     │
│           → head_check:batch  (trigger="redetect")       │
│  Universal schema: all payloads always have              │
│    {fein, trigger, source}                               │
└──────────────────────────────────────────────────────────┘

┌──────────────────────────────────────────────────────────┐
│  ALWAYS-ON job_monitor daemon — updated                  │
│  on consecutive_empty_days ≥ threshold:                  │
│    → head_check:batch  (trigger="redetect",              │
│                          source="company_ats")           │
└──────────────────────────────────────────────────────────┘

┌──────────────────────────────────────────────────────────┐
│  manager.py — updated                                    │
│  head_check:  LLEN on_demand + LLEN batch  (both LISTs) │
│  enrichment:  LLEN on_demand + ZCARD batch               │
│  discovery:   ZCARD redetect + ZCARD batch               │
│    (no on_demand lane — dropped; delayed excluded)       │
│  Scales each worker pool independently                   │
└──────────────────────────────────────────────────────────┘

QUOTA BUDGET (now spread across the day):
┌───────────────┬────────────────┬─────────────────────────────────────────┐
│ Quota         │ Limit          │ When exhausted                          │
├───────────────┼────────────────┼─────────────────────────────────────────┤
│ KG            │ 85K/day        │ Skip KG. Push to discovery:delayed      │
│ kg_quota.json │                │ score=now()+86400. flush_delayed picks  │
│               │                │ up after midnight quota reset.          │
├───────────────┼────────────────┼─────────────────────────────────────────┤
│ Brave         │ 950/month      │ Skip Brave. Jump to Phase 7.            │
│brave_quota.json│               │ Log WARNING once per exhaustion event.  │
│               │                │ Phase 7 runs in its place — no re-queue.│
├───────────────┼────────────────┼─────────────────────────────────────────┤
│ CF Worker     │ daily (config) │ Fall back to direct requests. If direct │
│ cf_quota.json │                │ also fails → enrichment:delayed with    │
│               │                │ fixed backoff. Never silently drop.     │
├───────────────┼────────────────┼─────────────────────────────────────────┤
│ Certspotter   │ 10 req/hr      │ React to 429 + Retry-After. Push to     │
│ (react to 429)│                │ enrichment:delayed score=now()+Retry-   │
│               │                │ After. Already implemented — no change. │
└───────────────┴────────────────┴─────────────────────────────────────────┘
```

---

## 13. Pain Point 6 — manager.py Watching All Six Lanes (Locked Design)

**Problem:** manager.py currently watches two pools (domain_enrichment, discovery) using
ZCARD on legacy ZSETs (DOMAIN_ENRICHMENT_QUEUE, REDETECT_QUEUE, DISCOVERY_QUEUE).
After redesign there are three worker pools across seven queue lanes with mixed types
(LIST and ZSET), and redetect priority must be structural — no magic score constants.

**Locked decisions:**

1. `discovery:on_demand` — DROPPED entirely. on_demand stops at enrichment; no producer
   ever pushes to it. Not tracked, not created, not added to config.py.

2. Redetect priority in discovery — structural, via a dedicated `discovery:redetect` ZSET
   (score = petition_count). Worker pops redetect first (ZPOPMAX), then batch (ZPOPMAX).
   No arbitrary boost constant. Within each tier, higher petition_count wins naturally.

3. Staleness vs redetect distinction:
   - trigger="staleness" (discovered > 90d, any company) → discovery:batch
   - trigger="redetect"  (consecutive_empty_days ≥ N, monitored companies) → discovery:redetect

4. Delayed ZSETs (enrichment:delayed, discovery:delayed) excluded from manager depth.
   They are time-parked items. flush_delayed() promotes them at the right time; counting
   them now would over-scale workers for work they cannot yet process.

**Final lane table:**

| Lane                  | Type | Score          | Producer(s)                                           | Manager cmd  |
|-----------------------|------|----------------|-------------------------------------------------------|--------------|
| head_check:on_demand  | LIST | —              | api.py (careers_url SET)                              | LLEN         |
| head_check:batch      | LIST | —              | staleness Pass 1+3, job_monitor                       | LLEN         |
| enrichment:on_demand  | LIST | —              | api.py (careers_url NULL), head_check Cases 3,4,6 (on_demand) | LLEN |
| enrichment:batch      | ZSET | petition_count | fuzzy_match, staleness Pass 1, head_check Cases 3,4,6 (batch) | ZCARD |
| discovery:redetect    | ZSET | petition_count | head_check Cases 1,2,5 (redetect), staleness Pass 3, job_monitor | ZCARD |
| discovery:batch       | ZSET | petition_count | staleness Pass 2, enrichment worker Step 4            | ZCARD        |

**Discovery worker pop order:**
```python
item = r.zpopmax("discovery:redetect") or r.zpopmax("discovery:batch")
```

**manager.py depth formulas (delayed excluded):**
```
head_check_depth  = LLEN(head_check:on_demand)  + LLEN(head_check:batch)
enrich_depth      = LLEN(enrichment:on_demand)  + ZCARD(enrichment:batch) + inflight
discovery_depth   = ZCARD(discovery:redetect)   + ZCARD(discovery:batch)  + inflight
```

**flush_delayed() promotion routing:**

`enrichment:delayed` → always promotes to `enrichment:batch`. Single target, no routing needed.

`discovery:delayed` → two possible targets. `flush_delayed()` reads `trigger` from the payload:
```python
if payload["trigger"] == "redetect":
    r.zadd("discovery:redetect", {member: score})
else:
    r.zadd("discovery:batch", {member: score})
```
No new fields needed — the universal `{fein, trigger, source}` schema carries the routing
information already. Items promoted to the correct lane retain their original petition_count
score, so priority order within each lane is preserved after promotion.

**config.py constants to add:**
```python
HEAD_CHECK_ON_DEMAND   = "head_check:on_demand"
HEAD_CHECK_BATCH       = "head_check:batch"
HEAD_CHECK_DLQ         = "head_check:dlq"
ENRICHMENT_ON_DEMAND   = "enrichment:on_demand"
ENRICHMENT_BATCH       = "enrichment:batch"
ENRICHMENT_DELAYED     = "enrichment:delayed"
ENRICHMENT_DLQ         = "enrichment:dlq"
DISCOVERY_REDETECT     = "discovery:redetect"
DISCOVERY_BATCH        = "discovery:batch"
DISCOVERY_DELAYED      = "discovery:delayed"
DISCOVERY_DLQ          = "discovery:dlq"
# Remove: REDETECT_QUEUE, DOMAIN_ENRICHMENT_QUEUE, DISCOVERY_QUEUE,
#         DOMAIN_ENRICHMENT_DELAYED (renamed to ENRICHMENT_DELAYED above),
#         ENRICHMENT_ON_DEMAND_SCORE, ENRICHMENT_HIGH_PRIORITY_SCORE (no longer needed)
# Keep:   DOMAIN_ENRICHMENT_INFLIGHT → rename to ENRICHMENT_INFLIGHT
#          DISCOVERY_INFLIGHT stays (already correctly named)
```

---

## 14. Blast Radius & Implementation Order

### Blast Radius by File

**NEW FILES** (create from scratch):
- `workers/head_check_worker.py` — full new worker (HEAD check, cache, 6 cases, routing)
- `deploy/systemd/head-check-worker@.service` — systemd template unit

**HIGH — core pop/write/routing logic changes:**

| File | What changes |
|---|---|
| `workers/domain_enrichment_worker.py` | Pop logic: ZPOPMAX ZSET → BLPOP(on_demand) + ZPOPMAX(batch); Lua inflight script updated. `_write_careers()`: remove WHERE NULL guard → unconditional. `_flush_delayed()`: remove origin_queue routing → always ENRICHMENT_BATCH. `_requeue_delayed()`: remove origin_queue param. `_process_company()`: remove `if not careers_url:` Phase 3 guard; remove `origin_queue == REDETECT_QUEUE` from Step 4; add `trigger != "on_demand"` for Step 4. `_push_to_discovery()`: route to DISCOVERY_REDETECT vs DISCOVERY_BATCH on trigger. All queue constants updated. |
| `workers/discover_h1b_ats_worker.py` | Pop logic: ZPOPMAX(DISCOVERY_QUEUE) → ZPOPMAX(DISCOVERY_REDETECT) first, then ZPOPMAX(DISCOVERY_BATCH); Lua inflight script updated. `_flush_delayed()`: add trigger routing (redetect→DISCOVERY_REDETECT, else→DISCOVERY_BATCH). `careers_src` write guard line 382: remove `not company.get("careers_url")` condition → unconditional. Queue constants updated. |
| `workers/manager.py` | Add head_check as third pool (LLEN on_demand + LLEN batch). enrich_depth: drop REDETECT_QUEUE, LLEN(ENRICHMENT_ON_DEMAND) + ZCARD(ENRICHMENT_BATCH). discovery_depth: ZCARD(DISCOVERY_REDETECT) + ZCARD(DISCOVERY_BATCH). Drop delayed ZSETs from all depth formulas. |

**MEDIUM — routing + constant swaps:**

| File | What changes |
|---|---|
| `api.py` | `_trigger_enrichment()`: ZADD DOMAIN_ENRICHMENT_QUEUE with score → LPUSH ENRICHMENT_ON_DEMAND. Inline HEAD check block (lines 607–639): replace with LPUSH HEAD_CHECK_ON_DEMAND. NULL path (line 599): LPUSH ENRICHMENT_ON_DEMAND. Remove ENRICHMENT_ON_DEMAND_SCORE usage. |
| `scripts/staleness_checker.py` | Pass 1: split by careers_url NULL → ENRICHMENT_BATCH / NOT NULL → HEAD_CHECK_BATCH. Pass 2: DISCOVERY_QUEUE → DISCOVERY_BATCH. Pass 3: REDETECT_QUEUE → HEAD_CHECK_BATCH (trigger="redetect"). |
| `config.py` | Add new constants (HEAD_CHECK_*, ENRICHMENT_*, DISCOVERY_*). Rename DOMAIN_ENRICHMENT_INFLIGHT → ENRICHMENT_INFLIGHT. Remove REDETECT_QUEUE, DOMAIN_ENRICHMENT_QUEUE, DISCOVERY_QUEUE, DOMAIN_ENRICHMENT_DELAYED, ENRICHMENT_ON_DEMAND_SCORE, ENRICHMENT_HIGH_PRIORITY_SCORE. |

**LOW — one or two lines:**

| File | What changes |
|---|---|
| `jobs/job_monitor.py` | Line 870: REDETECT_QUEUE → HEAD_CHECK_BATCH |
| `workers/worker_control.py` | Add HEAD_CHECK_WORKERS tuple; register in _KNOWN_UNITS |
| `db/schema.py` | Add `careers_source TEXT` to fein_domain_map. DROP `h1b_ats_discovery.careers_url`. |
| `deploy/install-systemd.sh` | Register head-check-worker@.service in the 3 required places |

**VERIFY ONLY** (read; change only if they reference h1b_ats_discovery.careers_url):
- `scripts/discover_h1b_ats.py` — check if upsert writes to h1b_ats_discovery.careers_url
- `frontend/pages/3_Discover.py` — check if it reads from h1b_ats_discovery.careers_url

---

### Implementation Order

Ordered to minimise broken-state windows: schema and constants first, new worker next,
then consumers (workers that read queues), then producers (scripts that push to queues),
then infrastructure (manager, deploy).

```
Step 1  config.py
        — foundation; every other file depends on the new constants
        — rename/remove old constants; add all new ones

Step 2  db/schema.py
        — add careers_source to fein_domain_map
        — DROP h1b_ats_discovery.careers_url

Step 3  workers/head_check_worker.py  [NEW]
        — full implementation: BLPOP on_demand→batch, Redis cache TTL=6h,
          HTTP HEAD, 6 cases, trigger-aware routing to enrichment/discovery

Step 4  workers/domain_enrichment_worker.py
        — update Lua pop script (two-source: LIST + ZSET)
        — _write_careers() unconditional
        — _flush_delayed() → always ENRICHMENT_BATCH
        — _requeue_delayed() drop origin_queue param
        — _process_company() Phase 3 guard removed, Step 4 trigger check
        — _push_to_discovery() DISCOVERY_REDETECT vs DISCOVERY_BATCH routing

Step 5  workers/discover_h1b_ats_worker.py
        — update Lua pop script (two-source: DISCOVERY_REDETECT + DISCOVERY_BATCH)
        — _flush_delayed() trigger routing
        — careers_src write guard removed

Step 6  scripts/discover_h1b_ats.py  [VERIFY]
        — confirm no writes to h1b_ats_discovery.careers_url; remove if found

Step 7  scripts/staleness_checker.py
        — Pass 1 split (NULL → ENRICHMENT_BATCH, NOT NULL → HEAD_CHECK_BATCH)
        — Pass 2 → DISCOVERY_BATCH
        — Pass 3 → HEAD_CHECK_BATCH

Step 8  jobs/job_monitor.py
        — single push target change: REDETECT_QUEUE → HEAD_CHECK_BATCH

Step 9  api.py
        — _trigger_enrichment() → LPUSH ENRICHMENT_ON_DEMAND
        — inline HEAD check block → LPUSH HEAD_CHECK_ON_DEMAND
        — NULL routing → LPUSH ENRICHMENT_ON_DEMAND

Step 10 frontend/pages/3_Discover.py  [VERIFY]
        — confirm careers_url source; update JOIN if reading h1b_ats_discovery.careers_url

Step 11 workers/manager.py
        — add head_check pool; update depth formulas for enrichment + discovery

Step 12 workers/worker_control.py
        — add HEAD_CHECK_WORKERS; register in _KNOWN_UNITS

Step 13 deploy/systemd/head-check-worker@.service  [NEW]
        — systemd template unit (mirror domain-enrichment-worker@.service pattern)

Step 14 deploy/install-systemd.sh
        — register head-check-worker@.service in the 3 required places
```
