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
            populate domain_enrichment_queue
            (Redis ZSET, score = petition_count)
                        ↓
            domain_enrichment_worker        ← EVENT-DRIVEN BATCH
            (on-demand, terminates when queue empty)
                        ↓
            writes: public_domain, careers_url, ats_platform/slug (bonus)
            pushes: petition_count >= threshold → discovery_queue
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
- Populate `domain_enrichment_queue` (Redis ZSET)
- Score = petition_count for each company
- Only rows WHERE `public_domain IS NULL` OR `last_enriched_at < NOW() - INTERVAL '90 days'`

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
- **Start**: staleness_checker or api.py calls `systemctl start domain-enrichment-worker@{1,2}` directly
- **Stop**: worker exits cleanly when `ZPOPMAX` returns empty (queue drained)
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
    ALWAYS runs regardless of Phase 3 result:
    - Phase 3 found careers_url → use as seed (fast path, ATS scan only)
    - Phase 3 missed            → probe www.{domain} across CAREER_PATHS independently
    → writes: careers_url (if found or improved)
             ats_platform + ats_slug (if Phase 6 detects — bonus)

STEP 4 — Push to discovery_queue
    IF petition_count >= threshold:
        ZADD discovery_queue petition_count {"fein": company_fein, "trigger": "fuzzy_match"|"staleness"|"on_demand"|...}
```

### Failure Handling
- Transient errors (network, timeout): retry up to 3× with exponential backoff
- Permanent failures (bad domain, no web presence after all steps): push to DLQ
- CT log 429: read `Retry-After` header → store item in `DOMAIN_ENRICHMENT_DELAYED` ZSET
  scored by `time.time() + retry_after`; `_flush_delayed()` moves ready items into
  `domain_enrichment_queue` before each ZPOPMAX. Future timestamps are never written
  directly to `domain_enrichment_queue` (which uses petition_count as score).

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
- **Start**: queue-watcher detects `ZCARD discovery_queue > 0` → starts worker(s)
- **Stop**: worker exits cleanly when queue drained
- **Not always-on**: same event-driven batch model as enrichment worker

### Fixed Worker Count
- 2 workers (discover-h1b-ats-worker@1, discover-h1b-ats-worker@2)
- Same quota-throttled model

### Discovery Queue Sources (4)
1. `domain_enrichment_worker` — after enrichment completes for a company above threshold
2. `staleness_checker` cron — `last_discovered_at > DISCOVER_REDETECT_EMPTY_DAYS (30d) AND petition_count >= threshold`
3. `job_fetcher_worker` — `consecutive_zero_jobs > threshold` (re-detection trigger)
4. Admin script — new ATS platform added → push all monitored companies

### Processing Steps (per company)

```text
ats_platform already set (from enrichment)?
    YES → skip entirely UNLESS re-detection trigger (source = job_fetcher or admin)

    NO →
        kg_checked = False (never run KG for this company)?
            → Phase 1: KG lookup (85K/day quota)
              ALWAYS runs on first pass, even if careers_url already set
              (KG may return a direct ATS URL more authoritative than HTTP probing)
            → Phase 2: domain gate (compare KG domain vs public_domain)
            → mark kg_checked = True

        KG returned ATS directly?
            YES → write ats_platform, ats_slug → done

        careers_url set (from enrichment Phase 3/6)?
            YES → Phase 7 (seed_url=careers_url)
                  skip Phase 3+4+5+6 — enrichment already did this work

            NO →
                Phase 3: path probe
                Phase 4: Brave search (quota)
                Phase 5: redirect
                Phase 6: career_page
                Phase 7: full ATS detector (seed_url=None, probes all paths)
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

### domain_enrichment_queue
```text
Type:  Redis ZSET
Key:   domain_enrichment_queue
Score: petition_count  (higher = processed first)
Member: company_fein

Population:
    After fuzzy_match_uscis_dol.py completes:
    ZADD domain_enrichment_queue petition_count fein
    WHERE public_domain IS NULL
       OR last_enriched_at < NOW() - INTERVAL '90 days'

Consumption:
    ZPOPMAX domain_enrichment_queue  (highest petition_count first)
```

### discovery_queue
```text
Type:  Redis ZSET
Key:   discovery_queue
Score: petition_count
Member: company_fein + trigger_source (json blob)

Consumption:
    ZPOPMAX discovery_queue
```

### DLQ (Dead Letter Queue)
```text
domain_enrichment:dlq
discovery:dlq

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
- `_get_queue_metrics` returns `domain_enrichment` and `discovery` keys alongside `detail`, `scan`, `fullscan`

---

## 6. Worker Lifecycle (On-Demand Batch)

```text
Event occurs (LCA upload / USCIS upload / staleness cron / re-detection trigger)
    ↓
Queue populated
    ↓
Queue-watcher detects ZCARD > 0
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

### Enrichment triggers (3 ways workers start)
```text
1. fuzzy_match_uscis_dol.py completes
       → bulk populate domain_enrichment_queue (all null/stale public_domain rows)
       → systemctl start domain-enrichment-worker@1 domain-enrichment-worker@2

2. staleness_checker (daily cron)
       WHERE (
         -- Never resolved but past staleness window (prevents re-queue every day for persistent fails)
         (public_domain IS NULL
           AND (last_enriched_at IS NULL OR last_enriched_at < NOW() - INTERVAL '<ENRICH_STALENESS_DAYS> days'))
         OR
         -- Stale or never processed, only for actively monitored companies
         (last_enriched_at IS NULL OR last_enriched_at < NOW() - INTERVAL '<ENRICH_STALENESS_DAYS> days')
         AND employer_fein IN (monitored_feins from get_monitorable_companies())
       )
       → ZADD domain_enrichment_queue petition_count {"fein": ..., "trigger": "staleness"}
       → systemctl start domain-enrichment-worker@1 domain-enrichment-worker@2

3. User visits company page (on-demand verification — see below)
       → may push to domain_enrichment_queue with HIGH priority
       → systemctl start domain-enrichment-worker@1 domain-enrichment-worker@2
```

### Discovery triggers
```text
job_fetcher_worker:
    consecutive_zero_jobs > JOB_MONITOR_REDETECT_DAYS (14)
        → ZADD discovery_queue petition_count fein
        → trigger_source = 'job_fetcher'
        → systemctl start discover-h1b-ats-worker@1 discover-h1b-ats-worker@2

staleness_checker (daily cron):
    WHERE last_discovered_at < NOW() - INTERVAL '<DISCOVER_REDETECT_EMPTY_DAYS> days'
      AND petition_count >= threshold
        → ZADD discovery_queue petition_count fein
        → systemctl start discover-h1b-ats-worker@1 discover-h1b-ats-worker@2

Admin script (new ATS platform added):
    → ZADD discovery_queue petition_count fein FOR ALL monitored companies
    → trigger_source = 'admin'
    → systemctl start discover-h1b-ats-worker@1 discover-h1b-ats-worker@2
```

### On-demand verification (stale-while-revalidate)
```text
User visits company X in UI
    ↓
Background: HEAD/GET careers_url (cheap, no quota cost)
    ↓
200 returned?
    YES → mark careers_url_verified_at = NOW(), show cached data (fast path)
    NO  (non-200 / redirects to wrong domain) →
            show cached data immediately  (never block the user)
            ZADD domain_enrichment_queue ENRICHMENT_HIGH_PRIORITY_SCORE {"fein": ..., "trigger": "on_demand"}
            systemctl start domain-enrichment-worker@1 domain-enrichment-worker@2
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
    trigger             TEXT,                   -- 'enrichment' | 're_detection' | 'staleness' | 'manual'

    -- public domain (enrichment worker only)
    public_domain_method TEXT,   -- 'http_redirect' | 'root_fallback' | 'certspotter' |
                                 --  'crtsh' | 'same_domain' | 'no_signal' | NULL (not run)
    public_domain        TEXT,   -- resolved value, or NULL

    -- career URL (whichever worker found it first)
    careers_source       TEXT,   -- 'phase3' | 'phase6' | 'p10311' | NULL
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
| `careers_source = 'p10311'` | `scripts/discover_h1b_ats.py` → Wikidata P10311 |
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
  Career URL       14,302 found      phase3 52%  phase6 41%  p10311 7%
  ATS detected      9,841 found      phase3 28%  phase6 45%  phase7 27%
  No ATS found      4,461            (top companies without ATS logged separately)
```

**`scripts/pipeline_metrics.py`** — standalone on-demand report:
- Full phase breakdown table per metric
- Top N companies with `no_signal` public domain (high petition_count, no resolution)
- Top N companies with `careers_url` but no ATS (discovery missed)
- Phase regression table: last 30 days vs prior 30 days (did a fix help?)
- Per-platform ATS breakdown (how many companies on Workday vs Greenhouse vs etc.)

### Retention

Metrics rows are historical — keep indefinitely (table is small, ~25k rows per full run).
No automated cleanup. If it grows, add a `RETENTION_ENRICHMENT_METRICS_DAYS` config var.

---

## 12. Decisions

1. **N workers** — 2 enrichment + 2 discovery (redundancy, not throughput)
2. **petition_count threshold** — top 2000 by rank (not a fixed count value)
3. **Queue-watcher** — no separate watcher; the script that populates the queue
   directly calls `systemctl start {worker}@1 {worker}@2`.
   `systemctl start` on an already-running worker is a no-op — safe from multiple triggers.
4. **Certspotter** — API key required (free tier, $0/month)
   - Unauthenticated: 429 after 1-2 requests, Retry-After ~352s — unusable
   - With API key: 10 full-domain queries/hr, predictable
   - No quota file needed — react to 429 + Retry-After
   - Reference script: /tmp/test_pd4.py (8/8 passing, ready to integrate)
5. **Re-detection threshold** — `JOB_MONITOR_REDETECT_DAYS = 14` (already in config.py)
   and `DISCOVER_REDETECT_EMPTY_DAYS = 30` for discovery staleness
6. **Staleness interval** — 90 days for enrichment refresh (to be added to config.py)
