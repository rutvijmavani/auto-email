# H-1B Employer Classification & LLM Infrastructure — Design Document

## Goal

The job-scanning pipeline monitors companies for open roles. A key question is:
**which companies are worth monitoring?** H-1B sponsorship data is one of the
strongest signals — a company that sponsors H-1B visas is actively hiring
technical talent, files public records with the federal government, and is often
a good target for the pipeline.

Two separate government datasets contain this information:

| Dataset | Source | What it tracks |
|---|---|---|
| DOL LCA data | Department of Labor | Every H-1B labor condition application filed — one record per position, keyed by employer FEIN |
| USCIS H-1B petitions | U.S. Citizenship and Immigration Services | Aggregate approvals/denials per employer per fiscal year |

Combining these two datasets gives a complete picture of which employers sponsor
H-1B workers, how many, and what kind of roles. The problem is that **the two
datasets cannot be joined directly** — DOL records use FEIN (a tax ID), while
USCIS records use employer names only, which drift and vary across filings.

This document covers:
- How the DOL–USCIS join is solved
- The three-tier fuzzy matching pipeline
- How an AI model resolves cases that fuzzy matching cannot
- The LLM infrastructure that powers both H-1B disambiguation and email status tracking
- All design decisions made along the way

---

## The Core Problem — Two Datasets, No Common Key

### DOL LCA data

The Department of Labor publishes quarterly Excel files containing every H-1B
Labor Condition Application (LCA). Each row is one application:

```text
EMPLOYER_FEIN: 912345678
EMPLOYER_NAME: Amazon Web Services, Inc.
CASE_STATUS:   Certified
VISA_CLASS:    H-1B
...
```

This dataset is FEIN-keyed and clean. `scripts/process_dol_lca.py` ingests these
files into `dol_h1b_employers`, aggregating filing counts per employer.

### USCIS H-1B petition data

USCIS publishes a separate dataset of H-1B petition decisions — how many
petitions each employer had approved, denied, or withdrawn per fiscal year. This
data does not include FEINs. It only has employer names:

```text
EMPLOYER_NAME: AMAZON WEB SERVICES INC
STATE:         WA
INITIAL_APPROVAL: 5,231
```

`scripts/process_uscis_h1b.py` ingests this into `uscis_h1b_petitions`.

### Why joining them is hard

The same company appears under different names in different filings:

```text
DOL:   "Amazon Web Services, Inc."
USCIS: "AMAZON WEB SERVICES INC"
USCIS: "Amazon.com Services LLC"      ← a different USCIS filing for the same org
DOL:   "AWS Inc."                     ← DBA variant
```

A naive string match fails constantly. The datasets cannot be joined with a
simple `WHERE dol.name = uscis.name`.

---

## The Join Strategy — Two-Part Key

### Employer legal name normalization

Both datasets run their employer names through the same normalization function:

1. Uppercase
2. Remove DBA qualifiers — "Foo Inc. DBA Bar Corp" → "Foo Inc."
3. Remove legal suffixes — "Inc.", "LLC", "Corp.", "Ltd.", etc.
4. Collapse whitespace, strip punctuation

The result is stored as `employer_legal_norm`. "Amazon Web Services, Inc." and
"AMAZON WEB SERVICES INC" both normalize to "AMAZON WEB SERVICES".

### Last-4 FEIN as a bridge key

USCIS data has no FEIN, but the DOL data does. The join uses both:

```text
Step 1: Normalize the employer name from both datasets
Step 2: Find DOL employers whose normalized name is similar to the USCIS entry
Step 3: Require that the last 4 digits of the DOL FEIN match the last 4 digits
        of the USCIS tax_id field (a partial identifier present in USCIS data)
```

This two-part key (normalized name similarity + last-4 FEIN match) eliminates
the vast majority of false matches. Two different companies that happen to have
similar names will almost never share the same last-4 FEIN.

### The unmatched table

When no DOL employer can be found for a USCIS entry, the record goes into
`uscis_dol_unmatched`. This table is the input queue for the fuzzy matching
pipeline described below.

---

## The Fuzzy Matching Pipeline

**Script:** `scripts/fuzzy_match_uscis_dol.py`

For each unmatched USCIS employer, the script finds all DOL employers with the
same last-4 FEIN and scores them using **rapidfuzz** (a fast text similarity
library). It then makes a three-tier decision:

```text
USCIS employer with no DOL match
    ↓
Find all DOL employers where last-4 FEIN matches
    ↓
Score each DOL candidate using rapidfuzz token_set_ratio
    ↓
    ├── Score ≥ 95  (extremely confident match)
    │       → Tier 1: auto-match immediately
    │         INSERT into uscis_dol_fuzzy_map, DELETE from uscis_dol_unmatched
    │         match_stage = 'fuzzy'
    │
    ├── Score ≥ 80 AND gap from #2 candidate ≥ 25  (one clear winner)
    │       → Tier 2: dominant-winner auto-match
    │         INSERT into uscis_dol_fuzzy_map, DELETE from uscis_dol_unmatched
    │         match_stage = 'fuzzy_dominant'
    │
    └── Score 40–94, gap < 25  (genuinely ambiguous)
            → Tier 3: push to Redis stream for async LLM resolution
              Script finishes; LLM worker resolves in background
```

### Why three tiers and not just one threshold?

A single threshold would either:
- Be too aggressive (≥ 80 catches many false positives when two similar-sounding
  companies share a last-4 FEIN by coincidence), or
- Be too conservative (≥ 95 misses obvious matches where the name has minor
  formatting differences)

The gap rule for Tier 2 is the key insight: if the top candidate scores 85 and
the next-best scores 60, the 25-point gap is strong evidence the top candidate
is correct. If the top candidate scores 85 and the next-best scores 82, the
near-tie means the script cannot confidently distinguish them — that goes to
the LLM.

### The Redis stream

Tier 3 cases are pushed to a Redis stream (`llm:h1b:disambiguate`) rather than
processed inline. The script finishes immediately. The LLM worker reads from
this stream asynchronously and resolves each case independently.

This means:
- The fuzzy matching script can process thousands of employers in seconds, without
  waiting for LLM inference (which takes ~15–30 seconds per case)
- If the LLM is unavailable (e.g. model is loading), unresolved cases stay in
  the stream safely — nothing is lost
- The script can be re-run at any time without duplicating work; the stream and
  database have idempotency guards throughout

---

## LLM Disambiguation — The H-1B Worker

**File:** `workers/h1b_llm_worker.py`

This worker runs as a persistent background process (`h1b-llm-worker.service`).
It continuously reads from the `llm:h1b:disambiguate` Redis stream and resolves
ambiguous employer matches one at a time.

### What the model does

For each job in the stream, the worker sends a prompt to Qwen3-8B that looks like:

```text
I need to identify which DOL Labor Condition Application employer record
corresponds to a USCIS H-1B petition employer.

USCIS employer name: "Amazon.com Services LLC"
USCIS normalized:    "AMAZON COM SERVICES"

DOL candidates (same last-4 FEIN, ranked by name similarity):
  [1] FEIN=912345678 | Amazon Web Services, Inc. (similarity score: 87/100)
  [2] FEIN=912345001 | Amazon.com Services LLC   (similarity score: 92/100)
  [3] FEIN=912340099 | Amazon Logistics, Inc.    (similarity score: 71/100)

Instructions:
- Select the candidate that is the SAME legal entity as the USCIS employer.
- A subsidiary is NOT the same as its parent (e.g. "Amazon Web Services" ≠ "Amazon.com").
- If NO candidate is the same legal entity, set "match" to null.

Respond with ONLY a JSON object:
{"match": "<FEIN from candidates above, or null>"}
```

The model uses `/think` mode — it reasons step by step internally before
committing to an answer, which significantly improves accuracy on close cases.

### What happens after the model responds

```text
Model responds: {"match": "912345001"}
    ↓
Validate: is "912345001" one of the candidates we showed? (prevents hallucinations)
    ↓  yes
Write to uscis_dol_fuzzy_map:
    employer_legal_norm = "AMAZON COM SERVICES"
    tax_id              = "5001"  (last-4)
    dol_fein            = "912345001"
    match_stage         = "llm"
    candidates_json     = [...]   (full audit trail)
    ↓
DELETE from uscis_dol_unmatched (same transaction — atomic)
    ↓
XACK the Redis stream message ✓
```

If the model says `{"match": null}` — no candidate is the same legal entity —
the message is acknowledged and the USCIS employer remains in `uscis_dol_unmatched`
permanently. It cannot be matched; this is logged.

If the model returns something that is not one of the candidate FEINs, the
response is treated as a hallucination and discarded. The message is acknowledged
and the employer stays unmatched.

### Reliability — Dead Letter Queue

If the LLM cannot be reached (e.g. the server crashed or is loading), the worker
does not acknowledge the message. It stays in the Redis Pending Entry List (PEL)
and is automatically re-delivered the next time the worker starts.

If a message has been delivered and failed more than 5 times (configurable via
`H1B_DISAMBIG_MAX_DELIVERIES`), it is moved to a Dead Letter Queue stream
(`llm:h1b:dlq`) and acknowledged — removed from the active processing loop.
The watchdog alerts when DLQ depth grows, so nothing fails silently.

If the DLQ stream itself cannot be written to, the message is **not** acknowledged
and remains in the PEL for manual recovery. This is the safe failure: a message
in the PEL can always be retrieved; a silently dropped message cannot.

### Reliability — PEL drain on startup

When the worker restarts after a crash, it first drains its Pending Entry List —
re-processing any messages it had claimed but not yet acknowledged. Rather than
blocking on a failed message (one that the LLM cannot process this session), it
uses **XAUTOCLAIM** with a cursor to advance past failed entries. This means:

```text
PEL at startup: [A (failed — LLM timeout), B (new), C (new)]

Old behavior (XREADGROUP "0"):
    → Always re-delivers A (the oldest)
    → A fails again → spin forever, B and C never processed

New behavior (XAUTOCLAIM with cursor):
    → Processes A, fails → cursor advances past A
    → Processes B, succeeds ✓
    → Processes C, succeeds ✓
    → A stays in PEL — retried on next startup
```

---

## The LLM Infrastructure — llama-server

Both the H-1B disambiguation worker and the email status tracking processor use
the same AI model: **Qwen3-8B** running under **llama-server** (from llama.cpp).

### What is llama-server?

llama-server is a lightweight HTTP server that loads an AI model into memory once
and then handles inference requests via an API. It is the open-source standard for
self-hosted model serving — fast, low overhead, and supports the same API format
as OpenAI's API.

Instead of each worker loading the model into its own process (which would use
5GB of RAM per worker, quickly exhausting the 12GB VM), a single llama-server
instance holds the model in memory and all workers send HTTP requests to it.

```text
workers/email_processor.py  ──┐
                               ├──→  llama-server (port 8080)  ──→  Qwen3-8B in memory
workers/h1b_llm_worker.py  ──┘           (one model, shared)
```

### Why not in-process inference?

| Option | What it means | Why rejected |
|---|---|---|
| Each worker loads Qwen itself | 5GB RAM × 2 workers = 10GB — exceeds 12GB VM before pipeline overhead | Out of memory |
| Email processor only loads Qwen | H-1B disambiguation cannot use the same model without loading a second copy | Same problem |
| **Single llama-server, shared by all** | One copy of the model, multiple workers send HTTP requests | **Used** — ~5GB total regardless of how many workers connect |

### The shared client — `workers/llm_client.py`

All workers use the same thin client module:

```python
from workers.llm_client import call_llm

result = call_llm(prompt)   # returns text with <think> blocks stripped
```

`call_llm` opens one HTTP connection per process at startup and reuses it. If the
model returns a `<think>...</think>` block (Qwen3's internal reasoning trace), it
is stripped before the result is returned. Unterminated `<think>` blocks — which
can occur when the model reaches the token limit mid-thought — are also stripped.

---

## llama-server as a systemd Service

**Unit file:** `deploy/systemd/llama-server.service`  
**Process manager:** `scripts/llama_server_wrapper.py`

llama-server cannot be run directly as a systemd unit because model loading takes
1–2 minutes. If systemd were told "the service is ready" at process start, every
other service that depends on it would begin sending requests before the model is
loaded — and get errors back.

The solution is a **wrapper process** that:

1. Starts llama-server as a child process
2. Polls the `/health` endpoint every few seconds
3. When the health endpoint returns `{"status": "ok"}`, signals systemd that the
   service is now ready (`sd_notify READY=1`)
4. Continues polling during normal operation — if llama-server dies or stops
   responding, the wrapper exits and systemd restarts both

```text
systemd starts llama_server_wrapper.py
    ↓
wrapper spawns llama-server subprocess
    ↓
Phase 1 — polling loop (model loading, ~2 minutes)
    Every LLM_POLL_S seconds:
        GET /health
        ├── "loading"  → wait, keep polling
        ├── "ok"       → send sd_notify("READY=1") to systemd → break
        └── process died → exit (systemd restarts)
    ↓
Phase 2 — liveness keepalive
    Every LLM_POLL_S seconds:
        GET /health
        ├── "ok"       → continue
        ├── not "ok"   → kill child, exit (systemd restarts)
        └── process died → exit (systemd restarts)
```

### SIGTERM forwarding

When systemd stops the service (e.g. during deploy or reboot), it sends SIGTERM
to the wrapper process. Without forwarding, the child llama-server process would
become an orphan — still running, holding the GPU/CPU and 5GB of RAM, while
systemd thinks the service is down.

The wrapper installs a SIGTERM handler that:
1. Forwards SIGTERM to the llama-server child
2. Waits up to 5 seconds for it to exit cleanly
3. If it does not respond, sends SIGKILL and waits
4. Only then exits itself

This ensures llama-server always releases its resources when the service stops.

### Hardening

The systemd unit uses Linux sandboxing to limit what the service can access:

```text
ProtectHome=tmpfs            — home directory is hidden (empty filesystem mounted)
BindPaths=/home/opc/mail     — only the app directory is re-exposed
           /home/opc/models  — only the models directory is re-exposed
           /home/opc/llama.cpp
PrivateTmp=true              — isolated /tmp
NoNewPrivileges=true         — process cannot gain elevated permissions
ProtectSystem=full           — system directories are read-only
OOMScoreAdj=-500             — kernel is told: prefer killing other processes over this one
```

The OOM score setting matters: the VM only has 12GB RAM. If memory pressure grows,
the kernel would normally prefer to kill the process using the most memory — which
would be llama-server (5GB). The adjusted score tells the kernel to kill other
processes first, keeping the model loaded.

---

## The H-1B LLM Worker as a systemd Service

**Unit file:** `deploy/systemd/h1b-llm-worker.service`

The H-1B worker is linked to llama-server at the systemd level:

```ini
[Unit]
Requires=llama-server.service
After=network-online.target llama-server.service
```

`Requires=` means: if llama-server stops, h1b-llm-worker stops automatically.
`After=` means: h1b-llm-worker only starts after llama-server reports ready.

This relationship mirrors what happens at the application level: the worker sends
HTTP requests to llama-server. If llama-server is gone, those requests fail. The
systemd dependency makes the failure explicit rather than silent.

The worker is not in the normal deploy restart loop. Unlike the scheduler, API,
or email processor — which are always running — the H-1B worker only has work
during quarterly batch runs when USCIS data is ingested. Most of the time it runs
idle (blocked on an empty Redis stream) or not at all. Systemd starts it when
llama-server becomes active and stops it when llama-server stops.

---

## Email Status Tracking — The Other LLM Consumer

**File:** `workers/email_processor.py`

The email status tracking pipeline uses the same llama-server for a completely
different purpose: reading incoming Gmail notifications and determining whether
they represent a status update on a job application (interview invite, rejection,
offer).

This is covered in depth in [email-status-tracking.md](email-status-tracking.md).
From an infrastructure standpoint, the key point is:

**Both pipelines are async and share one model.**

```text
Gmail push notification → email_processor.py
                              └── call_llm(gate prompt)      → llama-server:8080
                              └── call_llm(extraction prompt) → llama-server:8080

fuzzy_match_uscis_dol.py → llm:h1b:disambiguate (Redis stream)
                              ↓
h1b_llm_worker.py          └── call_llm(disambiguation prompt) → llama-server:8080
```

llama-server is configured with `--parallel 1` — it processes one inference
request at a time. If both pipelines have work at the same moment, one waits.
This is intentional: the VM has 2 OCPUs. Running two inferences in parallel
does not make them faster; it makes both slower.

H-1B disambiguation and email processing are both low-volume, non-time-critical
tasks. A few seconds of queuing is not meaningful.

---

## Watchdog Monitoring

**File:** `workers/watchdog.py`

The watchdog monitors both llama-server and the H-1B worker as first-class
services — not as afterthoughts.

### llama-server monitoring

`check_systemd_services()` includes llama-server in its regular sweep. If the
systemd unit is in a `failed` state:

```text
Watchdog detects llama-server in failed state
    ↓
sudo systemctl reset-failed llama-server
sudo systemctl restart llama-server
    ↓
Sends alert email to operator
```

The restart is meaningful even when the H-1B worker is not actively processing:
email_processor.py sends inference requests to llama-server at any time an email
arrives. If llama-server is down and nobody notices, email status tracking
silently stops working.

### H-1B worker heartbeat monitoring

The H-1B worker writes a **heartbeat** to Redis every 30 seconds while it is
running:

```text
key: worker:alive:h1b_llm_worker:{pid}
value: {"pid": 12345, "ts": 1722614400.0, "processed": 47}
TTL: 90 seconds (3× the heartbeat interval)
```

The watchdog's `check_h1b_llm_worker_heartbeat()` function:

1. Scans all matching heartbeat keys
2. Parses each record, validating that it is a proper dictionary with a numeric,
   finite timestamp (rejects corrupted data, `inf`, `NaN`)
3. Picks the record with the most recent timestamp
4. Evaluates staleness: if the latest heartbeat is more than 60 seconds old,
   the worker is likely hung or dead

```text
Dead-after threshold: 60 seconds
Heartbeat TTL:        90 seconds

If dead-after < TTL:
    Key still exists but timestamp is stale → STALE warning
    (Worker is registered but not updating — hung process)

If dead-after > TTL:
    Key has expired → key missing warning
    (Worker stopped sending heartbeats entirely)
```

The 60-second dead-after is set deliberately below the 90-second TTL so the
"STALE — process may be hung" branch is reachable. If dead-after were above the
TTL, the key would expire before the staleness check could ever trigger, and the
worker would only appear as "missing" — never as "hung."

This check runs from `_run_all_checks()` directly, independent of whether the
scheduler is up. Even a complete scheduler outage does not prevent the H-1B
heartbeat from being evaluated.

### What the watchdog does when issues are found

| Condition | Action |
|---|---|
| llama-server in `failed` state | reset-failed + restart via sudoers-granted systemctl |
| h1b-llm-worker in `failed` state | reset-failed + restart via sudoers-granted systemctl |
| h1b_llm_worker heartbeat STALE | restart + operator alert email |
| h1b_llm_worker heartbeat missing | start + operator alert email |
| DLQ depth > 50 | operator alert email (no auto-heal — manual investigation required) |

---

## Database Schema — Classification Tables

### `uscis_dol_fuzzy_map` — confirmed matches

One row per matched USCIS employer. Written by the fuzzy script (Tiers 1–2)
and the LLM worker (Tier 3).

```sql
CREATE TABLE uscis_dol_fuzzy_map (
    employer_legal_norm  TEXT NOT NULL,  -- normalized USCIS employer name
    tax_id               TEXT NOT NULL,  -- last-4 digits of FEIN from USCIS data
    dol_fein             TEXT NOT NULL,  -- full FEIN from dol_h1b_employers
    match_score          REAL,           -- rapidfuzz score (null for LLM-only matches)
    match_stage          TEXT NOT NULL,  -- 'fuzzy' | 'fuzzy_dominant' | 'llm'
    candidates_json      JSONB,          -- all candidates shown to the LLM (audit trail)
    created_at           TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (employer_legal_norm, tax_id)
);
```

### `uscis_dol_unmatched` — pending or unresolvable

Employers that could not be matched via name normalization or fuzzy matching.
The fuzzy script's input queue. Cleaned up when a match is found.

```sql
CREATE TABLE uscis_dol_unmatched (
    employer_legal_norm  TEXT NOT NULL,
    employer_name        TEXT,
    tax_id               TEXT NOT NULL,
    total_approvals      INTEGER,
    PRIMARY KEY (employer_legal_norm, tax_id)
);
```

---

## Reliability Summary

```text
Risk                              Mitigation
─────────────────────────────────────────────────────────────────────────────
LLM temporarily unavailable       Message stays in PEL, re-delivered on restart
LLM repeatedly fails one message  DLQ after 5 deliveries; watchdog alerts at depth > 50
LLM publish to DLQ fails          XACK withheld; message stays in PEL for manual recovery
PEL entry blocks later entries    XAUTOCLAIM cursor advances past failed entries
Worker crashes mid-processing      Message stays in PEL, claimed on restart
Worker stuck / hung               Heartbeat goes stale; watchdog detects and restarts
llama-server crashes              Wrapper exits; systemd restarts both; h1b-worker depends on it
llama-server OOM-killed           OOMScoreAdj=-500 in unit file deprioritises this
Corrupted heartbeat key           Per-record validation; invalid key counted, skipped
Auto-match creates false positive  LLM tier only sees cases with score 40–94 and close gap
LLM hallucinates a FEIN           Validation: only candidate FEINs are accepted; rest discarded
```

---

## What Was Considered and Rejected

| Approach | Why rejected |
|---|---|
| Join DOL and USCIS on full normalized name alone | Too many false positives — many companies have similar names; last-4 FEIN narrows the candidate set dramatically |
| Join on exact name match | Even normalized names drift across filings ("AWS" vs "Amazon Web Services") |
| Use Gemini API for LLM disambiguation | Employer names (legal entities, FEINs, government records) are not sensitive, but sending them to an external API adds latency, cost, and an external dependency — self-hosted inference is already running |
| Single confidence threshold for auto-match | ≥ 80 catches false positives; ≥ 95 misses obvious matches. Gap rule (Tier 2) captures the middle ground without LLM overhead |
| In-process LLM inference (llama-cpp-python) per worker | Each worker loading Qwen3-8B would use 5GB RAM; two workers = 10GB before pipeline overhead — exceeds VM |
| Separate GPU inference server | No GPU on the Oracle Cloud A1 Flex ARM VM; llama.cpp CPU inference is the only option |
| XREADGROUP "0" for PEL drain | Always re-delivers the oldest pending entry; a single failed message blocks all subsequent healthy ones; XAUTOCLAIM with cursor solves this |
| XACK the main stream even when DLQ publish fails | Silently loses the message; withholding XACK keeps it in PEL for recovery |
| Single dead-after threshold above heartbeat TTL | Key expires before staleness check triggers; "hung worker" branch is unreachable — threshold must be below TTL |
| Run H-1B worker always (not dependent on llama-server) | Worker has nothing to do when llama-server is down; systemd Requires= makes the dependency explicit and prevents silent inference failures |
| Include H-1B worker in deploy restart loop | Worker runs idle most of the time; restarting it on every deploy unnecessarily interrupts any in-progress disambiguation batch |

---

## Files Involved

| File | Purpose |
|---|---|
| `scripts/process_uscis_h1b.py` | Ingests USCIS H-1B petition Excel data into `uscis_h1b_petitions` and `uscis_dol_unmatched` |
| `scripts/process_dol_lca.py` | Ingests DOL LCA quarterly Excel data into `dol_h1b_employers` |
| `scripts/fuzzy_match_uscis_dol.py` | Three-tier matching: auto-match (Tier 1–2) or push to Redis stream (Tier 3) |
| `workers/h1b_llm_worker.py` | Async LLM disambiguation consumer — reads Redis stream, sends prompts to Qwen3, writes confirmed matches |
| `workers/llm_client.py` | Shared OpenAI-compatible client — all workers use this to call llama-server; strips `<think>` blocks |
| `workers/email_processor.py` | Email status tracking — also uses `llm_client.py` for Qwen3 gate and extraction calls |
| `scripts/llama_server_wrapper.py` | Process manager for llama-server: Phase 1 load polling, sd_notify, Phase 2 liveness, SIGTERM forwarding |
| `deploy/systemd/llama-server.service` | systemd unit for llama-server (Type=notify, TimeoutStartSec=infinity) |
| `deploy/systemd/h1b-llm-worker.service` | systemd unit for H-1B worker (Requires=llama-server.service) |
| `workers/watchdog.py` | Monitors llama-server and h1b-llm-worker heartbeats; auto-heals via systemctl |
| `deploy/install-systemd.sh` | Provisions sudoers rules for watchdog to restart llama-server and h1b-llm-worker |
| `db/schema.py` | Defines `uscis_dol_fuzzy_map`, `uscis_dol_unmatched`, and related indexes |
| `config.py` | All LLM and H-1B stream constants: `LLM_BASE_URL`, `H1B_DISAMBIG_STREAM`, `H1B_DISAMBIG_MAX_DELIVERIES`, `H1B_DISAMBIG_DLQ_STREAM`, `H1B_DISAMBIG_MAXLEN` |
