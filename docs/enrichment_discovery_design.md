# Domain Enrichment & ATS Discovery — Design Document

---

## 1. Full Pipeline

```text
LCA upload                              USCIS H1B upload
    ↓                                       ↓
sync_dol_lca.py                        process_uscis_h1b.py
process_dol_lca.py                     fuzzy_match_uscis_dol.py
    ↓                                       ↓
fein_domain_map                        petition_count per employer
(assigned_domain, root-grouped)                ↓
    └──────────────────────────────────────→
                        ↓
            populate enrichment:batch / enrichment:on_demand
            (ZSET scored by petition_count / LIST for on-demand priority)
                        ↓
            domain_enrichment_worker        ← EVENT-DRIVEN BATCH
            (on-demand, terminates when queue empty)
                        ↓
            writes: public_domain, careers_url, ats_platform/slug (bonus)
            pushes: petition_count >= threshold → discovery:batch / discovery:redetect
                        ↓
            discover_h1b_ats_worker         ← EVENT-DRIVEN BATCH
            (on-demand, terminates when queue empty)
                        ↓
            writes: ats_platform, ats_slug, careers_url (if better)
```

---

## 2. Sync Pipeline (unchanged, triggers async work)

| Script | Type | Output |
|---|---|---|
| `sync_dol_lca.py` | sync | raw LCA rows in DB |
| `process_dol_lca.py` | sync | `fein_domain_map` (assigned_domain, root-grouped) |
| `process_uscis_h1b.py` | sync | USCIS H1B petition data |
| `fuzzy_match_uscis_dol.py` | sync | petition_count joined to employers |

After `fuzzy_match_uscis_dol.py` completes:
- Populate `enrichment:batch` (Redis ZSET, score = petition_count) for new/stale companies
- Populate `enrichment:on_demand` (Redis LIST, LIFO: producers LPUSH, worker LPOPs) for API-triggered single-company refreshes
- Only rows WHERE `(public_domain IS NULL OR last_enriched_at IS NULL OR last_enriched_at < NOW() - INTERVAL '90 days')`

**Why USCIS must complete before enrichment queue is populated:**
petition_count is the priority score. Without it, all companies get score=0
and we process in arbitrary order — potentially enriching low-value companies
before high-value ones.

---

## 3. Domain Enrichment Worker

### Purpose
Find the correct public domain and careers URL for all 25k target companies.
Light ATS detection as a bonus (Phase 6 may return platform+slug for free).

### Trigger / Lifecycle
- **Queue population**: `staleness_checker.py` and `api.py` push to `enrichment:batch` / `enrichment:on_demand` — they never start workers directly
- **Start/Stop**: the autoscaler (`manager.py`) starts and stops worker instances based on queue depth; workers exit cleanly when both queues drain
- **Not always-on**: terminates between batches, unlike job monitor

### Fixed Worker Count
- 2 workers (domain-enrichment-worker@1, domain-enrichment-worker@2)
- Quota is the real throttle, not worker count
- Extra workers = redundancy only, not throughput gain

### Processing Steps (per company)

```text
STEP 1 — Public domain resolution
    a. HTTP redirect check (no quota):
       GET assigned_domain → follow redirects → extract root domain
       e.g. jpmchase.com → redirects to → jpmorgan.com
       e.g. bofa.com     → redirects to → bankofamerica.com

    b. Root-domain fallback (no quota):
       Strip subdomain from assigned_domain, check if root resolves
       e.g. jobs.compunnel.com → compunnel.com
       e.g. ny.email.gs.com   → gs.com → goldmansachs.com

    c. CT log via certspotter (API key required, 10 full-domain/hr):
       ONLY for domains where a+b both fail (DNS-fail / no web server)
       e.g. fmr.com → finds fidelity.com in cert SANs
       e.g. aexp.com → finds americanexpress.com in cert SANs
       - First page only (100 certs) — sufficient in most cases
       - GENERIC_ROOTS denylist filters cloud/email/CDN providers
       - _has_web() confirms candidate has a real web server
       - On 429: read Retry-After header → re-queue with exact delay
       - crt.sh as fallback if certspotter fails
    → writes: public_domain, public_domain_method

STEP 2 — Phase 3: path probe (CF Worker quota if direct blocked)
    Probes career subdomains + 16 paths from www.{public_domain}
    External domain jump guard: skip if redirect leaves company root domain
    → writes: careers_url (if found)

STEP 3 — Phase 6: career_page (CF Worker quota)
    Runs ONLY when Phase 3 found no ATS platform (if not p3_platform):
    - Phase 3 found careers_url but no platform → use as seed (fast path, ATS scan only)
    - Phase 3 missed entirely    → probe www.{domain} across CAREER_PATHS independently
    → writes: careers_url (if found or improved)
             ats_platform + ats_slug (if Phase 6 detects — bonus)

STEP 4 — Push to discovery:redetect or discovery:batch (Section 5 queue keys)
    IF petition_count >= threshold:
        trigger == "redetect" → ZADD discovery:redetect petition_count {...}
        else                  → ZADD discovery:batch    petition_count {...}
        {"fein": company_fein, "trigger": "fuzzy_match"|"staleness"|"redetect"|...}
```

### Failure Handling
- Transient errors (network, timeout): retry up to 3× with exponential backoff
- Permanent failures (bad domain, no web presence after all steps): push to DLQ
- CT log 429: read `Retry-After` header → store item in the `enrichment:delayed` ZSET
  (`ENRICHMENT_DELAYED`) scored by `time.time() + retry_after`; `_flush_delayed()` moves
  ready items into `enrichment:batch` (or `enrichment:on_demand` for `tier="on_demand"`)
  before each pop. Future timestamps are never written directly to `enrichment:batch`
  (which uses petition_count as score).

### Quota Tracking
- `cf_quota.json` — shared with existing workers (CF Worker calls)
- Certspotter: NO quota file — react to 429 + Retry-After header only
  (no rate-limit headers on success responses, can't proactively track)

### Maintenance Window
- Stops before daily maintenance window (same schedule as all other workers)
- Resumes after maintenance

---

## 4. Discover H1B ATS Worker

### Purpose
Full ATS detection (platform + slug) for top 2k companies by petition count.
Uses quota-heavy phases (KG, Brave) that are too expensive to run for all 25k.

### Trigger / Lifecycle
- **Start**: manager.py autoscaler polls `discovery:redetect` + `discovery:batch` depth every cycle → starts worker(s) (see §12 Decision 3)
- **Stop**: worker exits cleanly when queue drained; manager stops the pool after N idle cycles
- **Not always-on**: same event-driven batch model as enrichment worker

### Fixed Worker Count
- 2 workers (discover-h1b-ats-worker@1, discover-h1b-ats-worker@2)
- Same quota-throttled model

### Discovery Queue Sources (3)
1. `domain_enrichment_worker` — after every completed enrichment run (first-time AND the 90-day re-enrichment), when `petition_count >= STALENESS_DISCOVERY_MIN_PETITIONS` (or trigger is `redetect`). This is the ONLY entry for `discovery:batch`; there is no discovery staleness cron (removed 2026-09-21 — it queued every FEIN a second time under a different trigger, doubling the work). Discovery therefore re-runs every `ENRICH_STALENESS_DAYS` via enrichment.
2. `job_fetcher_worker` — `consecutive_zero_jobs > threshold` (re-detection trigger)
3. Admin script — new ATS platform added → push all monitored companies

### Processing Steps (per company)

**⚠️ Design/code drift, identified 2026-09-22:** this section documents intended behavior
that `process_employer()` / `discover_h1b_ats_worker.py` never actually implemented — the
code runs the full Phase 3→4→5→6→7 chain unconditionally on every company reaching this
worker, even though `domain_enrichment_worker` (§3) already ran Phase 3 and Phase 6 against
the exact same domain moments earlier and is guaranteed to have failed at both (see the
"Guaranteed redundancy" note below). The flow below is the corrected version, refined from
the original flat "careers_url set → skip 3+4+5+6" rule to account for KG priority and the
domain-gate check. **Not yet implemented in code as of 2026-09-22** — this section is the
target for that fix.

**Guaranteed redundancy this fixes:** `domain_enrichment_worker` (§3, Steps 2-3) always runs
Phase 3, and always runs Phase 6 unless Phase 3 already found the ATS. A FEIN only reaches
`discover_h1b_ats_worker` with `already_has_ats = False`, which (by that same logic) means
Phase 3 AND Phase 6 already ran against this domain and found no ATS. Redoing either phase
here is guaranteed to reproduce "not found" — it cannot discover something a moment-earlier,
identical probe against the same domain missed.

```text
already_has_ats (existing_row.detected_platform + detected_slug both set)?
    YES → skip entirely UNLESS trigger ∈ {re_detection, manual, redetect}
          or source ∈ {company_ats, prospective}  (re-detection paths — see below)

    NO → trigger ∈ {enrichment, staleness}?   (normal first-pass path)
        │
        NO (re_detection / manual / redetect / company_ats / prospective)
            → FULL re-probe, unchanged: Phase 1 KG → Phase 3 → Phase 4 Brave →
              Phase 5 → Phase 6 → Phase 7. Nothing skipped — the site may have
              changed since the last detection, which is the entire point of a
              re-detection trigger.
        │
        YES (normal path)
            → Phase 1: KG lookup — ALWAYS runs regardless of the branches below
                        (wikidata_qid, glassdoor_id, crunchbase_id + jobs_url
                        as a *candidate* careers_url only)
              → KG domain (kg_url / P856) matches our public_domain?
                    NO  → discard the entire KG entry (no ids, no jobs_url candidate)
                    YES → keep wikidata_qid / glassdoor_id / crunchbase_id
                          unconditionally (pure metadata, no conflict)

              → careers_url already found by enrichment (Phase 3/6, from fein_domain_map)?
                    YES → careers_url = enrichment's value.
                          KG's jobs_url is NEVER used to overwrite it — a direct
                          probe of the company's own site outranks a Wikidata
                          fact, which can be stale.
                          SKIP Phase 4 Brave (careers_url already known)

                    NO  → KG gave a domain-matched jobs_url?
                              YES → careers_url = KG's jobs_url; SKIP Phase 4 Brave
                              NO  → Phase 4: Brave search fallback
                                    careers_url = Brave result (or still none)

              → SKIP Phase 6 unconditionally (enrichment already ran it for every
                normal-trigger FEIN — it always runs unless Phase 3 found the ATS,
                and we're only in this branch because it didn't)

              → Phase 7: career_detector BFS — only genuinely new work left;
                runs regardless of whether careers_url came from enrichment,
                KG, or Brave

Priority order for careers_url, strictly enforced on the normal path:
    enrichment's probed value (Phase 3/6)  >  KG's jobs_url  >  Brave  >  Phase 7
```

### Re-detection Trigger Behaviour
When triggered by job_fetcher or admin script:
- Skip the `ats_platform already set` guard
- Re-run from Phase 7 with `seed_url=careers_url` (if set)
- KG skipped on re-detection (kg_checked already True)
- Used when: company switches ATS, new ATS platform added, jobs dropping to zero

### Failure Handling
- Same as enrichment worker: 3 retries → DLQ
- DLQ tagged with trigger source and error reason

### Quota Tracking
- `kg_quota.json` — shared (85K/day)
- `cf_quota.json` — shared (85K/day)
- `brave_quota.json` — shared

### Maintenance Window
- Same as all other workers

---

## 5. Queue Design

### Six-lane queue architecture (implemented 2026-08-24)

```text
head_check:on_demand   LIST  — on-demand HEAD checks (API-triggered)
head_check:batch       LIST  — batch HEAD checks (staleness cron + redetect)

enrichment:on_demand   LIST  — high-priority domain enrichment
enrichment:batch       ZSET  — batch domain enrichment, score=petition_count
enrichment:delayed     ZSET  — CT log 429 backoff, score=retry_at timestamp

discovery:redetect     ZSET  — redetect path, score=petition_count (priority lane)
discovery:batch        ZSET  — normal discovery, score=petition_count
discovery:delayed      ZSET  — discovery backoff, score=retry_at timestamp

Payload (JSON):
    {"fein": "12-3456789", "trigger": "staleness"|"on_demand"|"redetect"|
     "fuzzy_match"|"delayed_retry"|"reclaimed", "source": "company_ats"|"prospective"|null,
     "petition_count": 1234}

`petition_count` is omitted for on_demand-triggered payloads (api.py's `/verify-company`
producers push to enrichment:on_demand and head_check:on_demand without it — there is no
petition count context for a single user-initiated lookup). It is required wherever the
target queue is a ZSET scored by petition_count (discovery:redetect, discovery:batch,
enrichment:batch) or wherever a downstream consumer reads it for tier propagation
(head_check:batch redetect items).
```

### DLQ (Dead Letter Queue)
```text
enrichment:dlq
discovery:dlq
head_check:dlq

Per entry — max-retry failures (_move_to_dlq):
    fein              company FEIN that exhausted retries
    error_reason      "max_retries_exceeded" | "processing_error"
    retry_count       attempt count at time of DLQ push
    failed_at         Unix timestamp

Per entry — malformed delayed-queue payloads:
    fein              "MALFORMED" literal
    error_reason      "malformed_payload"
    last_error        exception text
    retry_count       0
    raw               repr() of the offending Redis member
    failed_at         Unix timestamp

Admin script: review + manual retry
```

### Queue Depth Monitoring
- Manager reads `ZCARD` for enrichment and discovery ZSET queues; `LLEN` for detail/fullscan LIST queues
- head_check depth = `LLEN(head_check:on_demand) + LLEN(head_check:batch)` plus the summed
  `LLEN` of every `head_check:inflight:instance:*` key (so in-flight items from a crashed
  worker still count toward pool sizing until reclaimed)
- `_get_queue_metrics` (`workers/manager.py`) returns `domain_enrichment`, `discovery`, and
  `head_check` keys alongside `detail`, `scan`, `fullscan`
- `HEAD_CHECK_WORKERS` is the autoscaled pool size for head_check, driven by `queue_data["head_check"]`
  in the same `_run_ats_pool_cycle` autoscale loop used for domain_enrichment and discovery

---

## 6. Worker Lifecycle (On-Demand Batch)

```text
Event occurs (LCA upload / USCIS upload / staleness cron / re-detection trigger)
    ↓
Queue populated
    ↓
manager.py autoscaler polls queue depth on its next cycle (see §12 Decision 3)
    ↓
Starts N workers (systemd or direct invocation)
    ↓
Workers process items (ZPOPMAX → work → write → repeat)
    ↓
ZPOPMAX returns empty → worker exits cleanly
    ↓
Queue drained — no workers running until next event
```

NOT like job_monitor (perpetual daemon).
Queue draining may take multiple days for initial 25k load — that is expected and fine.

---

## 7. Operational Config (per worker)

Each worker needs:

```text
1. Worker script
   - init_logging + get_logger  (logs/ directory)
   - Redis heartbeat every N seconds
   - Graceful SIGTERM shutdown  (finish current item, then exit)
   - Quota check before each external call
   - Backpressure signal to Redis when quota exhausted
   - Retry logic: 3× exponential backoff → DLQ

2. Systemd unit file
   - Registered in 3 places in install-systemd.sh
   - One-time manual install after adding unit file

3. Certspotter (enrichment worker only)
   - API key required (free tier, 10 full-domain queries/hr)
   - No quota file — react to 429 + Retry-After header
   - Re-queue with score = now() + retry_after on 429
   - crt.sh as fallback if certspotter unavailable

4. DLQ writer
   - On 3rd retry failure: push to {worker}:dlq with error metadata
```

---

## 8. DB Changes Needed

### fein_domain_map (additions)
```sql
public_domain_method      TEXT        -- 'http_redirect' | 'root_fallback' | 'certspotter' | 'crtsh'
last_enriched_at          TIMESTAMPTZ -- when enrichment worker last processed this row
kg_checked                BOOLEAN     -- whether KG has been queried for this company
last_discovered_at        TIMESTAMPTZ -- when discovery worker last processed this row
careers_url_verified_at   TIMESTAMPTZ -- when the careers URL was last HEAD-checked as reachable
```

### company_ats (existing, verify)
```text
employer_fein, domain, company_name, platform, slug, source, priority, detected_at — exist
is_monitored, reviewed_at, first_scanned_at, last_checked_at, consecutive_empty_days — exist
trigger_source — add: 'enrichment' | 'discovery' | 'redetection'
```

---

## 9. Refresh / Staleness

### Enrichment triggers (populating scripts enqueue work; manager.py autoscaler starts workers)
```text
1. fuzzy_match_uscis_dol.py completes (daily via h1b-pipeline 03:00)
       WHERE last_enriched_at IS NULL            -- never enriched, ONLY
       → ZADD enrichment:batch petition_count {"fein": ..., "trigger": "enrichment", "tier": "batch"}

2. staleness_checker (daily cron 05:00)
       WHERE last_enriched_at < NOW() - INTERVAL '<ENRICH_STALENESS_DAYS> days'   -- enriched before, now stale
         careers_url IS NULL     → ZADD enrichment:batch petition_count {"fein": ..., "trigger": "staleness", "tier": "batch"}
         careers_url IS NOT NULL → guarded RPUSH head_check:batch {"fein": ..., "trigger": "staleness"}

   The two selection sets are disjoint (NULL vs. older-than-90-days), so a FEIN is never queued twice.
   The trigger is part of the ZSET member JSON; when both producers selected the same FEIN, ZADD
   saw two different members and every company was enriched twice (fixed 2026-09-21).

3. User visits company page (on-demand verification — see below)
       → LPUSH enrichment:on_demand {"fein": ..., "trigger": "on_demand"}
```

### Discovery triggers
```text
domain_enrichment_worker (after each completed run, incl. the 90-day re-enrichment):
    petition_count >= STALENESS_DISCOVERY_MIN_PETITIONS (or trigger="redetect")
        → ZADD discovery:batch petition_count {"fein": ..., "trigger": <enrichment trigger>, "source": ...}
    (No discovery staleness cron — removed 2026-09-21; enrichment forwarding is the only entry.)

head_check_worker (trigger="redetect" path):
        → ZADD discovery:redetect petition_count fein
```

### On-demand verification (stale-while-revalidate)
```text
User visits company X in UI
    ↓
api.py /verify-company always returns cached data immediately (never blocks the user),
then routes in the background (per-FEIN cooldown key gates duplicate enqueues):
    ↓
careers_url known?
    YES → LPUSH head_check:on_demand {"fein": ..., "trigger": "on_demand", "source": null}
          head_check_worker applies the shared HEAD-check cache, redirect
          classification and routing (Cases 1–6):
            200 / clean → careers_url_verified_at = NOW()
            dead / redirect to wrong domain → re-enrichment or discovery routing
    NO  → LPUSH enrichment:on_demand
          {"fein": ..., "trigger": "on_demand", "source": null, "tier": "on_demand"}
    ↓
manager.py autoscaler detects queue depth and starts worker(s)
worker re-detects careers_url in background
UI updates when fresh result written back
```

Key principle: user always sees something immediately (cached).
Verification and re-detection happen silently in background.
Penalty = one visit with stale data, self-correcting.

---

## 10. Migration — 3 AM Batch Job Retirement

The existing `discover_h1b_ats.py` currently runs as a nightly 3 AM systemd service.
Once `discover_h1b_ats_worker` is implemented and tested in production, retire it:

```text
systemctl disable <3am-discover-service>
systemctl stop    <3am-discover-service>
remove unit file from install-systemd.sh
```

Do NOT retire until new worker is confirmed working end-to-end.

---

## 11. Pipeline Performance Metrics

### Goal
Understand which phase is responsible for each discovery outcome — without digging through logs.
When something breaks or degrades, the metrics table tells you exactly which script to fix.

### DB Table — `h1b_enrichment_metrics`

```sql
CREATE TABLE IF NOT EXISTS h1b_enrichment_metrics (
    id                  BIGSERIAL PRIMARY KEY,
    employer_fein       TEXT        NOT NULL,
    run_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    worker              TEXT        NOT NULL,   -- 'domain_enrichment' | 'discovery'
    trigger             TEXT,                   -- 'staleness' | 'on_demand' | 'redetect' | 'fuzzy_match' | 'delayed_retry' | 'reclaimed'

    -- public domain (enrichment worker only)
    public_domain_method TEXT,   -- 'http_redirect' | 'root_fallback' | 'certspotter' |
                                 --  'crtsh' | 'same_domain' | 'no_signal' | NULL (not run)
    public_domain        TEXT,   -- resolved value, or NULL

    -- career URL (whichever worker found it first)
    careers_source       TEXT,   -- 'phase1_kg' | 'phase3' | 'phase4' | 'phase6' | 'phase7' | NULL
    careers_url          TEXT,   -- resolved value, or NULL

    -- ATS detection
    ats_source           TEXT,   -- 'phase3' | 'phase6' | 'phase7' | 'phase1_kg' | NULL
    ats_platform         TEXT,
    ats_slug             TEXT,

    -- performance
    duration_ms          INT     -- wall-clock ms for this company's full processing
);

CREATE INDEX IF NOT EXISTS idx_h1b_enrichment_metrics_fein   ON h1b_enrichment_metrics (employer_fein);
CREATE INDEX IF NOT EXISTS idx_h1b_enrichment_metrics_run_at ON h1b_enrichment_metrics (run_at DESC);
CREATE INDEX IF NOT EXISTS idx_h1b_enrichment_metrics_worker ON h1b_enrichment_metrics (worker, run_at DESC);
```

One row appended per company per worker run. Allows trend analysis over time
(e.g. "after improving Phase 6, how many companies moved from phase7 → phase6?").

### Scripts Responsible (source of truth per field)

| Field | Responsible script |
|---|---|
| `public_domain_method` / `public_domain` | `jobs/public_domain.py` |
| `careers_source = 'phase3'` / `careers_url` | `scripts/discover_h1b_ats.py` → `discover_careers_url()` |
| `careers_source = 'phase6'` / `careers_url` | `jobs/career_page.py` → `detect_via_career_page()` |
| `careers_source = 'phase1_kg'` | `scripts/discover_h1b_ats.py` → Wikidata P10311 |
| `ats_source = 'phase3'` | `scripts/discover_h1b_ats.py` → `discover_careers_url()` ATS redirect |
| `ats_source = 'phase6'` | `jobs/career_page.py` → `detect_via_career_page()` |
| `ats_source = 'phase7'` | `jobs/ats/career_detector.py` → `detect_company()` |
| `ats_source = 'phase1_kg'` | `scripts/discover_h1b_ats.py` → KG P10311 direct ATS URL |

### Where Metrics Are Written

- **`domain_enrichment_worker`**: writes `public_domain_method`, `public_domain`, `careers_source`, `careers_url`, `ats_source`, `ats_platform`, `ats_slug` after each company completes
- **`discover_h1b_ats_worker`**: writes `ats_source`, `ats_platform`, `ats_slug`, `careers_source`, `careers_url` after each company completes
- Both write `worker`, `trigger`, `duration_ms`, `run_at`

### Viewing

**`scripts/health_check.py`** — summary block added to existing health check:
```text
  H1B PIPELINE METRICS  (last 7 days)
  ──────────────────────────────────────────────────────────
  Public domain    18,421 processed  http_redirect 68%  root_fallback 18%
                                     certspotter 9%     no_signal 5%
  Career URL       14,302 found      phase3 52%  phase6 41%  phase1_kg 7%
  ATS detected      9,841 found      phase3 28%  phase6 45%  phase7 27%
  No ATS found      4,461            (top companies without ATS logged separately)
```

**`scripts/pipeline_metrics.py`** — standalone on-demand report:
- Full phase breakdown table per metric
- Top N companies with `no_signal` public domain (high petition_count, no resolution)
- Top N companies with `careers_url` but no ATS (discovery missed)
- Phase regression table: last 30 days vs prior 30 days (did a fix help?)
- Per-platform ATS breakdown (how many companies on Workday vs Greenhouse vs etc.)

### Attempted / Found / Missed — denominator model (agreed 2026-09-22)

**Problem:** the `health_check.py` summary block above shows `public domain` as
`"X/Y resolved"` but `career URL`/`ATS detected` as a bare `"N found"` with no denominator —
so a raw found-count alone invites guessing at the missing ratio. Fixing this needs a real
"attempted" population for all three metrics, not just public domain.

**Key insight — count attempts per company-cycle, not per phase/worker.** A FEIN that gets a
`domain_enrichment` run in the window is committed to the *entire* pipeline: if enrichment's
Phase 3/6 doesn't resolve career URL or ATS, that FEIN is guaranteed to eventually get
discovery's Phase 1/Phase 7 pass too (that's the whole point of the enrichment→discovery
forward, §9). So "attempted" is not "did phase X run this window" (which would need separate,
harder-to-define denominators per phase, and breaks down for ATS since it's genuinely
attempted in both Phase 6 *and* Phase 7) — it's "did this company enter the pipeline this
window." That number is identical for all three metrics:

```sql
attempted (public domain) = attempted (career URL) = attempted (ATS)
    = COUNT(DISTINCT employer_fein) FROM h1b_enrichment_metrics
      WHERE worker = 'domain_enrichment' AND run_at > NOW() - INTERVAL '<days>'
      -- i.e. exactly pd_total's existing population
```

**Found is current live state, read once — not an incrementally-tracked counter:**
```sql
-- public domain: fein_domain_map.public_domain IS NOT NULL, for the attempted population
-- career URL:    fein_domain_map.careers_url   IS NOT NULL, for the attempted population
-- ATS:           company_ats has a row with platform + slug set, for the attempted population
```
`missed = attempted - found`. This automatically reflects whichever phase ultimately resolved
it — enrichment's Phase 3/6 immediately, or discovery's Phase 1 (KG jobs_url/direct ATS) or
Phase 7 (career_detector BFS) later — without needing to increment/decrement a counter
per phase or care which worker's row is newest. It is also what makes the ATS case tractable:
ATS being attempted in both Phase 6 (enrichment) and Phase 7 (discovery) stops being a
counting problem, because "was phase 6 attempted" / "was phase 7 attempted" are never counted
separately — only "is this company, cycle-wide, resolved yet."

The phase-mix breakdown (`phase6 57%  phase3 32%  phase4 10%  phase7 1%  phase1_kg 0%`) is a
separate question — "of the ones found, which phase gets credit" — answered from the current
`careers_source`/`ats_source` value on the same attempted population; it is unaffected by this
model and needs no change.

Display becomes, matching public domain's existing format:
```
[✓] public domain    6648/6958 resolved  same_domain 86%  http_redirect 6%  ...
[✓] career URL       5566/6958 found     phase6 57%  phase3 32%  phase4 10%  ...
[✓] ATS detected     2093/6958 found     phase3 65%  phase7 19%  phase6 10%  ...
```
Applies to both `scripts/health_check.py` (live dashboard) and `scripts/pipeline_metrics.py`
(on-demand report) — both have the same found-only-count gap today. **Not yet implemented in
code as of 2026-09-22.**

### Retention

`RETENTION_ENRICHMENT_METRICS_DAYS = 90` (config.py) — rows older than 90 days are deleted
automatically. `db/schema.py`'s `_cleanup_h1b_enrichment_metrics` runs this DELETE and is
invoked by `init_db()` on every startup. Kept longer than `RETENTION_MONITOR_STATS` (60 days)
because pipeline debugging benefits from a full quarter of phase-by-phase history.

### External API Health Tracking — `external_api_health` table (agreed 2026-09-22)

**Goal:** track the 4 third-party APIs the enrichment/discovery pipeline depends on
(certspotter, crt.sh, Brave, KG/Wikidata) so degradation (elevated error rates, slow
responses, rate-limit exhaustion) shows up here *before* it shows up as a queue-depth
problem in `health_check.py`. Modeled on the existing `api_health` table (job-scan polling's
adaptive/fullscan health tracker) — reuses its write mechanics and column shapes where they
apply, deliberately diverges where they don't.

```sql
CREATE TABLE external_api_health (
    id                  BIGSERIAL PRIMARY KEY,
    date                DATE NOT NULL,
    service             TEXT NOT NULL,   -- 'certspotter' | 'crtsh' | 'brave' | 'kg'

    requests_made       INTEGER DEFAULT 0,
    requests_ok         INTEGER DEFAULT 0,
    requests_429        INTEGER DEFAULT 0,
    requests_403        INTEGER DEFAULT 0,
    requests_404        INTEGER DEFAULT 0,
    requests_5xx        INTEGER DEFAULT 0,
    requests_other_err  INTEGER DEFAULT 0,

    avg_response_ms     INTEGER DEFAULT 0,
    max_response_ms     INTEGER DEFAULT 0,
    total_ms            BIGINT  DEFAULT 0,

    first_429_at        TIMESTAMP,
    backoff_total_s     INTEGER DEFAULT 0,

    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(date, service)
);
```

**Column rationale:**
- `avg_response_ms` / `max_response_ms` / `total_ms` — directly relevant to the throughput
  investigation's slow-tail premise. `api_health`'s write pattern is reused exactly: `avg` is
  recomputed as `total_ms / requests_made` on every write (cheap, exact — not sampled/decayed),
  `max` via `GREATEST()`.
- `first_429_at` — certspotter, Brave, and KG all have documented rate limits with 429
  semantics. Knowing *when* the cap was first hit in the day (e.g. 14:32 vs 09:15) distinguishes
  early-exhaustion (needs harder throttling) from end-of-day noise (fine as-is) — info the
  aggregate 429 count alone can't give.
- `backoff_total_s` — quantifies actual worker time lost waiting on a third party, ties
  straight into the report's utilization/slow-tail sections. certspotter's existing
  `_certspotter_retry_after` backoff logic just needs this column to make that cost visible.
- `created_at`, `UNIQUE(date, service)` — kept for consistency with `api_health`
  (`platform` renamed to `service` here).
- **`requests_403`** — added beyond `api_health`'s column set. certspotter/crt.sh treat
  401/403 as an alert-worthy "key revoked" condition (per the `log_monitor` suppression
  rules), so it gets its own bucket instead of falling into `requests_other_err`.
- **Deliberately NOT copied from `api_health`:** the `context` column
  (`'normal' | 'backoff' | 'canary'`) — that concept exists there for job-scan polling's
  adaptive-vs-fullscan machinery, which doesn't apply to these 4 one-shot enrichment API calls.

**Retention:** follows the exact same pattern as `api_health` and `h1b_enrichment_metrics` —
a `RETENTION_EXTERNAL_API_HEALTH` constant in `config.py`, a `_cleanup_external_api_health(c)`
function in `db/schema.py` (`DELETE FROM external_api_health WHERE date < cutoff`, cutoff =
`datetime.now() - timedelta(days=RETENTION_EXTERNAL_API_HEALTH)`), and a call to it added to
the `# ── Cleanup pass ──` block inside `init_db()` (runs on every process startup, not a cron —
same as every other `_cleanup_*` function in that block). **Value: 60 days, matching
`RETENTION_API_HEALTH` (config.py, `= 60`)** — this table is explicitly modeled on `api_health`,
and daily third-party-API-health aggregates don't need `h1b_enrichment_metrics`'s 90-day window
(that's per-company phase history used for regression detection over a full quarter; this is a
much smaller, coarser per-service-per-day rollup where 60 days of trend is plenty).

**Status as of 2026-09-22: IMPLEMENTED** — `config.py` has `RETENTION_EXTERNAL_API_HEALTH = 60`;
`db/schema.py` has the `CREATE TABLE`/index and `_cleanup_external_api_health(c)`, wired into
`init_db()`'s cleanup pass; the new `db/external_api_health.py` module (synchronous write,
modeled on `api_health.py::record_scaling_event`'s retry-once pattern — call volume here never
approaches the 20-thread job-scan volume that justifies `api_health`'s own background writer
queue) provides `record_external_request()` + query functions; all 4 call sites are
instrumented (`_ct_certspotter`/`_ct_crtsh` in `jobs/public_domain.py`, `kg_search`/
`brave_career_search` in `scripts/discover_h1b_ats.py`); `pipeline_metrics.py` has a new
"EXTERNAL API HEALTH" report section. 16 new unit tests in `tests/test_external_api_health.py`,
all passing. **Not yet verified against a live database** (no Postgres in this dev environment —
same caveat as the denominator-model work above; user owns live verification on next VM deploy).
Full implementation detail in `project_codebase_map.md` under each touched file's entry and
`project_enrichment_throughput_analysis.md`'s "Performance report" section.

---

## 12. Decisions

1. **N workers** — 2 enrichment + 2 discovery (redundancy, not throughput)
2. **petition_count threshold** — top 2000 by rank (not a fixed count value)
3. **Queue-watcher** — no separate watcher; populating scripts (staleness_checker,
   fuzzy_match_uscis_dol, api.py) only enqueue work. manager.py autoscaler polls
   queue depths every cycle and starts/stops worker instances accordingly.
   `systemctl start` on an already-running worker is a no-op — safe from multiple triggers.
4. **Certspotter** — API key required (free tier, $0/month)
   - Unauthenticated: 429 after 1-2 requests, Retry-After ~352s — unusable
   - With API key: 10 full-domain queries/hr, predictable
   - No quota file needed — react to 429 + Retry-After
   - Reference script: /tmp/test_pd4.py (8/8 passing, ready to integrate)
5. **Re-detection threshold** — `JOB_MONITOR_REDETECT_DAYS = 14` (already in config.py)
   (`DISCOVER_REDETECT_EMPTY_DAYS = 30` is now used only by h1b_pipeline; there is no discovery staleness cron)
6. **Staleness interval** — `ENRICH_STALENESS_DAYS = 90` (in config.py); enrichment refresh after 90 days
