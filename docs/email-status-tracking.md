# Automatic Job Application Status Tracking

## Overview

When you apply to a job, the application is added to the platform with a status
of **Applied**. From that point on, the company's ATS (Applicant Tracking System)
sends you emails when your status changes — a rejection, an invitation to
interview, a phone screen, or an offer.

Without this feature, you would need to manually open each email, understand
what it means, and update the status on the platform yourself. This feature
eliminates that entirely. Emails are monitored in real time. When a
job-related email arrives, the platform automatically identifies which
application it belongs to, determines what the update means, and updates the
status — with no manual intervention required.

---

## What It Does

```text
You receive a rejection email from Stripe
    ↓
Platform detects it within seconds
    ↓
Identifies: company = Stripe, status = Rejection
    ↓
Finds your Stripe application in the database
    ↓
Updates status from "Applied" → "Rejected"
    ↓
You see the updated status next time you open the dashboard
```

This works for all status changes:

| Email type | Status updated to |
|---|---|
| Rejection notice | Rejected |
| Interview invitation | Interview Scheduled |
| Phone screen request | Phone Screen |
| Online assessment invitation | Assessment |
| Offer letter | Offer Received |

---

## How Emails Are Monitored

The platform uses **Gmail Push Notifications** via **Google Cloud Pub/Sub**.

Instead of checking your inbox every few minutes (polling), Gmail notifies our
server the instant a new email arrives. This is push-based — the server does
nothing until Gmail calls it.

```text
New email arrives in your Gmail inbox
    ↓
Gmail immediately notifies Google Cloud Pub/Sub
    ↓
Pub/Sub pushes a tiny notification to our server (contains your email address
and a history ID — not the email content itself)
    ↓
Our server fetches the actual email using the Gmail API
    ↓
Email is processed
```

### Why not poll instead?

Polling (checking every N minutes) wastes resources, adds latency, and
eventually hits Gmail API rate limits. Push notifications are instant, free
within quota, and require no background polling process.

---

## Multi-User Design

The platform supports multiple users (currently 2, designed for up to 10).
Each user has their own Gmail inbox that needs to be monitored independently.

### One-time setup per user

When a new user joins the platform, they authorize Gmail access once:

```text
User visits authorization URL
    ↓
Google shows permissions screen: "Allow this app to read your Gmail"
    ↓
User clicks Allow
    ↓
A refresh token is stored securely in the database against their user ID
    ↓
Gmail watch is started on their account
    ↓
Done — no further action ever required from the user
```

### How users are differentiated

Every Pub/Sub notification contains the email address of the inbox that
triggered it:

```json
{
  "emailAddress": "rutvij@gmail.com",
  "historyId": "12345"
}
```

The server reads `emailAddress`, looks up which user it belongs to, fetches
that user's stored refresh token, and retrieves their email. This means each
user's emails are always processed against their own applications — never
mixed with another user's data.

### Gmail watch renewal

Gmail Push Notifications require a **watch** to be active on each inbox. A
watch expires after 7 days and must be renewed, or push notifications stop.

Each user's watch is renewed independently based on when their watch expires,
not on a shared schedule. A daily cron job runs at 2 AM and renews any watch
expiring within the next 48 hours:

```text
Daily at 2 AM — renew_gmail_watch.py
    ↓
For each active user:
    └─ Is their watch_expires_at within 48 hours?
        → YES: call gmail.watch() silently using stored refresh token
               update watch_expires_at in DB
        → NO:  skip
```

This means:
- User 1 added on July 17 → watch expires July 24 → renewed July 22
- User 2 added on July 22 → watch expires July 29 → renewed July 27
- Each user renews independently based on their own cycle

The user is never involved in renewal. The server handles everything using
the refresh token stored at setup time.

> **OAuth tokens vs Gmail watch:** These are two separate things. The OAuth
> refresh token (stored at first-time setup) does not expire on a fixed
> schedule, but Google may invalidate it for inactivity (no use for 6+ months),
> a user password change, security policy limits, explicit revocation, or other
> account-level events. The renewal flow must handle `invalid_grant` / `RefreshError`
> by sending a reauthorization alert — a dead refresh token cannot be recovered
> automatically. The Gmail watch (a subscription telling Gmail where to send
> notifications) is what expires every 7 days. Renewing the watch does not
> require the user to re-authorize anything — it is a server-to-server API call.

---

## Email Classification

Not every email that arrives in your inbox is job-related. The platform must
decide, for each email, whether it is worth processing.

### Why not a keyword or domain filter?

An obvious approach is to only process emails from known ATS domains
(`@myworkday.com`, `@ashbyhq.com`, etc.) or emails whose subject contains
words like "application" or "interview." This was considered and rejected
for a specific reason: **it requires constant maintenance**.

New ATS platforms emerge. Existing platforms change their sender domains.
Rejection emails sometimes come from addresses that don't match any known
pattern. Every exception requires a code change — and any email that doesn't
match the filter is silently dropped, meaning a real status update is missed.

### What is used instead

A machine learning model classifies every email as either job-related or
irrelevant. This requires no list of patterns to maintain. The model
understands the meaning of the email, not just its surface features, so it
handles new ATS platforms and unusual formats automatically.

---

## The Processing Model — Qwen3-8B

A single model handles the entire pipeline in two sequential calls per email.
The model loads once at service startup (~2 minutes) and stays in memory.

```text
Call 1 — subject + sender only  →  Qwen3-8B  (~5–6 seconds)
              ↓ yes / not_sure
Call 2 — full email body        →  Qwen3-8B  (~25–30 seconds)
```

**Why two calls instead of one?** The first call reads only the email
subject and sender — a metadata-only Gmail API call that does not fetch the
body. If the model determines the email is not job-related, the body is never
fetched and the second call never runs. This keeps irrelevant emails (shopping
receipts, newsletters, job alerts) cheap at ~5 seconds instead of ~30 seconds.

### Call 1 — Gate

Reads only the **subject line and sender address**. Three possible outputs:

| Output | Meaning | Action |
|---|---|---|
| `yes` | Clearly about a submitted application | Fetch full body → Call 2 |
| `not_sure` | Ambiguous — cannot tell without reading the body | Fetch full body → Call 2 |
| `no` | Clearly not about a submitted application | Discard immediately |

**When uncertain, the gate outputs `not_sure`.** A false `no` means a missed
rejection or interview invite — real data loss. A false `yes`/`not_sure` costs
one extra Qwen3-8B call (~25 seconds). The prompt instructs the model
explicitly: *"Never output no unless certain."*

Tested on 12 real emails: **12/12 exact match, 12/12 safe** (no dangerous
drops), ~5–6s per classification.

### Call 2 — Extraction

Only runs when Call 1 outputs `yes` or `not_sure`. Receives the full email
body and handles:

1. **What company is it from?** — Extracted from the email content
2. **What job title does it refer to?** — Extracted from the email content
3. **What is the status update?** — Classified as rejection, interview invite,
   phone screen, assessment, or offer
4. **Which application does this belong to?** — When 2–3 candidates remain
   after fuzzy matching, the model reasons through them and picks one

Reasoning is the primary requirement for Step 4 — the model must handle
conflict resolution reliably, not just text generation.

### Thinking mode — two speeds, one model

Qwen3 has a built-in thinking toggle controlled via the prompt:

```text
Call 1 + Call 2 Steps 1–3 (fast)   →  /no_think in prompt
                                       outputs directly, no reasoning chain
                                       (empty <think></think> block is stripped
                                       in post-processing — does not affect output)

Call 2 Step 4 — disambiguation      →  /think in prompt
                                       model reasons step by step before
                                       committing to one application
                                       ~30–60 seconds, more reliable decision
```

### Why this model

| Model considered | Reason rejected |
|---|---|
| Keyword/domain patterns | Brittle — breaks when patterns change |
| Flan-T5-base | Unreliable JSON output, cannot reason over candidates |
| Gemini API | Email content would leave the server — privacy concern |
| Llama 3.1 8B Instruct | Good reasoning but no built-in thinking mode |
| Gemma 4 12B Q3_K_M | Fits VM but no thinking mode; generalised not reasoning-focused |
| GLM-5.2 | 753B parameters — needs ~235GB RAM, impossible locally |
| Bonsai-27B 1-bit (PrismML) | Requires custom PrismML llama.cpp fork with Q1_0_g128 hybrid-attention kernels; no CPU throughput benchmarks; standard llama-cpp-python produced 544s per inference — impractical |
| Ternary Bonsai 27B (PrismML) | Same custom-kernel requirement (Q2_0_g128); 8.4GB at 4K context + 2GB pipeline = 10.4GB, exceeds 9.2GB available; no CPU benchmarks |
| Qwen3-0.6B (gate candidate) | Too small for reliable gate decisions; subject lines like "An update on your application" are genuinely ambiguous — 0.6B makes edge-case errors that silently drop real rejections |
| Qwen3-1.7B (gate candidate) | Tested as a separate gate model — 12/12 safe on subject-only but produced dangerous false negatives on full body classification; running alongside 8B adds 1.1GB RAM for no benefit since 8B achieves the same gate accuracy at ~5–6s |
| Qwen3-32B | Q2_K (lowest usable quantization) requires ~11GB — exceeds 9.2GB available; overkill for the task |
| **Qwen3-8B Q4_K_M** | **Built-in thinking mode for reasoning, strong structured JSON output, 5/5 on real email test, 12/12 on gate test — handles both calls reliably** |

### Technical details

| | Value |
|---|---|
| Model | `Qwen3-8B-Q4_K_M.gguf` |
| Source | `lmstudio-community/Qwen3-8B-GGUF` on HuggingFace |
| Format | GGUF 4-bit quantized |
| Runtime | `llama-cpp-python` |
| RAM | ~5.0GB |
| Load time | ~2 minutes (once at service startup, stays in memory) |
| Call 1 time | ~5–6 seconds (subject+sender, 1 output token) |
| Call 2 time | ~25–30 seconds (full body, JSON); ~30–60s (disambiguation with `/think`) |

```python
llm = Llama(model_path=".../Qwen3-8B-Q4_K_M.gguf", chat_format="chatml", ...)
```

### Infrastructure

| Resource | Specification |
|---|---|
| Server | Oracle Cloud A1 Flex (ARM Ampere) |
| OCPUs | 2 |
| RAM | 12GB total (~9.2GB available) |
| Model (Qwen3-8B) | ~5.0GB |
| Pipeline footprint | ~2.0GB at peak |
| Headroom | ~2.2GB |

---

## Application Matching

After the email processor receives a notification, it must identify exactly
which pending application the email belongs to. This is the hardest part of
the pipeline — and the approach is carefully designed to be reliable at scale
(500+ pending applications) while handling real-world messiness like typos,
abbreviations, and informal company names.

### Why user-typed company names cannot be the primary anchor

Company names stored in the database are entered by users and are inherently
unreliable as an exact match key:

```text
User typed "strip"       → email says "Stripe"
User typed "JPM"         → email says "JPMorgan Chase"
User typed "google"      → email says "Alphabet Inc."
User typed "Amazon AWS"  → email says "Amazon.com"
```

Human error, abbreviations, and informal names will always occur across
multiple users. The matching strategy is designed to handle all of these
without any manual correction.

### Why passing all candidates to Qwen does not scale

With 500 pending applications, passing the full list to Qwen would exceed
the model's context window, slow inference significantly, and produce
unreliable results. Qwen never sees more than 2–3 candidates at once — the
funnel below ensures this.

### The matching funnel — 4 layers

```text
Email arrives  (user may have 500+ pending applications)
    ↓
Layer 1 — ATS sender domain filter
    Derived from the ATS platform stored at apply time (not user-typed).
    Email from @greenhouse.io → only check Greenhouse applications.
    Email from @myworkday.com → only check Workday applications.
    500 applications → ~80 candidates
    ↓
Layer 2 — Qwen extracts clean company name + job title
    Qwen reads the email and outputs structured data:
      { company: "Stripe", title: "Software Engineer" }
    Output is clean and normalised — not affected by what the user typed.
    Title may be null if the email does not mention the role clearly.
    ↓
Layer 3 — Fuzzy match  (clean Qwen output → messy DB entries)
    Library: rapidfuzz
    Company: fuzz.WRatio(qwen_company, db_company) ≥ 80  (required)
    Title:   fuzz.token_sort_ratio(qwen_title, db_title) ≥ 70
             (only applied when Qwen extracted a title — null skips this)

    The direction of comparison matters:
      Qwen output is CLEAN  ("Stripe")
      DB entries are MESSY  ("strip", "Stripe Inc.")
      Clean vs messy fuzzy matching works reliably.
      Messy vs messy does not.

    Example with 500 applications:
      ATS domain filter         500 → ~80  (Greenhouse only)
      Fuzzy company "Stripe"    ~80 → ~4   (all Stripe applications)
      Fuzzy title "Software Eng"  ~4 → 1–2 (narrowed by role)
    ↓
    ├── 0 candidates → write to unmatched_emails, skip
    ├── 1 candidate  → update status ✓
    └── 2–3 candidates → Layer 4
    ↓
Layer 4 — Qwen disambiguation  (2–3 candidates only, never 500)
    "This email is about Stripe — Software Engineer.
     Here are 2 matching applications:
       1. strip — Software Engineer   (applied 2026-07-18)
       2. Stripe Inc — Product Manager (applied 2026-07-15)
     Which application does this email refer to?
     Note: application names may contain typos."
    → Qwen picks one → update status ✓
```

### Why company + title together is the right combination

Company name alone can still return multiple matches when a user has applied
to more than one role at the same company. Title narrows the match to a
specific position, making the combination a very strong indicator:

```text
4 Stripe applications in DB:
  strip — Software Engineer   ← company ✓  title ✓  → match
  Stripe — Product Manager    ← company ✓  title ✗
  Stripe — Data Engineer      ← company ✓  title ✗
  Stripe — DevOps Engineer    ← company ✓  title ✗
Result: 1 candidate → direct update, no disambiguation needed
```

### Fuzzy matching library

**`rapidfuzz`** — fast C++ backend, BSD licensed, industry standard.

| Algorithm | Used for | Why |
|---|---|---|
| `fuzz.WRatio` | Company name | Picks the best algorithm automatically; handles abbreviations and suffixes |
| `fuzz.token_sort_ratio` | Job title | Sorts words before comparing — "Senior Software Engineer" and "Software Engineer Senior" score 100 |

---

## Application Status Design

### Two separate status columns

The `applications` table currently has a `status` column used by the entire
pipeline — recruiter outreach, job scanning, cache lookups — all filtering
`WHERE status = 'active'`. This column must not be changed.

Email tracking adds a **second column** on the same table:

```text
status        TEXT   — pipeline dimension (existing, unchanged)
                       active / closed / prospective / exhausted

email_status  TEXT   — hiring process dimension (new)
                       NULL / phone_screen / assessment / interview / offer / rejected
```

`email_status` is set exclusively by `email_processor.py`. No other part of
the pipeline reads or writes it. `status` remains untouched by email
processing except in one case: when `email_status` becomes `rejected` or
`offer`, `status` is also flipped to `closed` — the pipeline has no further
work to do on a finalized application.

### Status values and transitions

| `email_status` | Meaning | Also sets `status` |
|---|---|---|
| `NULL` | No email received yet | — |
| `phone_screen` | Phone screen scheduled or confirmed | — |
| `interview` | Interview invitation received | — |
| `assessment` | Online coding test or work simulation invitation | — |
| `offer` | Offer letter received | `closed` |
| `rejected` | Rejection received | `closed` |

### New columns on applications

Two nullable columns are added to the `applications` table by this feature:

```sql
ats_company  TEXT   -- canonical company name as written by the ATS in their email
                    -- e.g. user typed "stripe", ATS email says "Stripe Inc." → stored here
ats_title    TEXT   -- canonical job title as written by the ATS
```

These are set by `email_processor.py` when an email is successfully matched and
extracted. The matching funnel uses `ats_company` when available (more reliable
than the user-typed `company`), falling back to `company` for fuzzy matching
when `ats_company` is NULL. The user's original entry in `company` is never
modified.

**Transition rules:**

`offer` and `rejected` are terminal states reachable from any current
`email_status` — this is the normal hiring process:

```text
NULL → phone_screen → interview → rejected   ✓
NULL → rejected                              ✓  (immediate rejection)
NULL → interview → offer                     ✓
NULL → phone_screen → offer                  ✓
```

The only blocked transitions are backward intermediate moves
(`interview → phone_screen`) which have no real-world meaning.

### Dynamic tunnel URL — fully automatic

The Cloudflare tunnel URL changes every time `cloudflared` restarts
(trycloudflare.com quick tunnels are ephemeral). Two mechanisms keep the
system working automatically:

**Pub/Sub push endpoint:** `scripts/tunnel_manager.py` patches the GitHub
Gist when a new tunnel URL is detected. It is extended to also update the
Pub/Sub push subscription in the same step:

```text
Tunnel restarts → new trycloudflare.com URL detected
    ↓
tunnel_manager.py patches GitHub Gist  →  Chrome extension picks up new URL
tunnel_manager.py calls Pub/Sub API    →  push endpoint updated instantly
    subscription.modify_push_config(new_url + '/email-push')  # OIDC JWT auth; no token in URL
```

**OAuth redirect URI — Cloudflare Worker shim:** Google does not allow
wildcard redirect URIs for Web Application OAuth clients. The solution is
a permanent Cloudflare Worker (`mail-oauth-redirect.rutvijmavani.workers.dev`)
that acts as a stable redirect shim — registered once in Google Console,
never changes, and always follows the current tunnel:

```text
User visits /oauth/start
    ↓
api.py fetches GIST_CONFIG_URL → reads current api_base
    ↓
redirect_uri = 'https://mail-oauth-redirect.rutvijmavani.workers.dev/oauth/callback'
(stored in nonce alongside user_id)
    ↓
Google consent screen → redirects to Worker URL
    ↓
Worker fetches Gist → gets current tunnel URL → 302 to tunnel/oauth/callback
    ↓
api.py /oauth/callback handles token exchange using Worker URL as redirect_uri ✓
```

The Worker code (~10 lines):
```javascript
export default {
  async fetch(request) {
    const url = new URL(request.url);
    const gist = await fetch('https://gist.githubusercontent.com/rutvijmavani/4f400d820fd0b390c15dd7d6592d8053/raw/api-config.json');
    const { api_base } = await gist.json();
    const target = api_base.replace(/\/$/, '') + url.pathname + url.search;
    // Validate before redirecting: target must use https:// and a known tunnel hostname
    // (e.g. *.trycloudflare.com or *.ngrok-free.app). Reject anything else to prevent
    // open-redirect abuse if the Gist is tampered with.
    return Response.redirect(target, 302);
  }
};
```

Free tier: 100,000 requests/day. Actual usage: ~2 requests total
(one per user authorization, which happens once per user ever).

If the Gist fetch fails (network blip), the Worker should return a 503 or
redirect to the `FALLBACK_REDIRECT_URI` Worker environment variable (set to the
current tunnel URL at deploy time). `api.py` also has a server-side fallback:
if the Worker URL cannot be reached, it falls back to the `GMAIL_OAUTH_REDIRECT_URI`
env var for the redirect_uri parameter.

---

## Full Pipeline Flow

```text
New email arrives in user's Gmail inbox
    ↓
Gmail → Pub/Sub → POST /email-push  (webhook in api.py)
    ↓
Write raw notification to Redis stream  (stream:email-push)
    ↓ (< 100ms)
Return HTTP 200 to Pub/Sub  ← Pub/Sub's job is done
    ↓
Email processor worker reads from Redis stream
    ↓
Fetch email METADATA only via Gmail API  (subject + sender, no body)
    ↓
Call 1 — Qwen3-8B gate  (~5–6 seconds, subject+sender only):
    ├── no       → not job-related → discard ✓
    └── yes / not_sure → actionable or ambiguous
                                    ↓
                          Fetch full email body via Gmail API
                                    ↓
                          Call 2 — Qwen3-8B extraction  (~25–30 seconds):
                              { company: "Stripe", title: "SWE", status: "rejected" }
                                    ↓
                          4-layer matching funnel:
                              1. ATS domain filter (500 → ~80)
                              2. Qwen3-8B extracts clean company + title
                              3. rapidfuzz: clean output vs messy DB entries
                              4. Qwen3-8B disambiguation (2–3 candidates only)
                              ├── 0 matches → write to unmatched_emails, skip
                              ├── 1 match   → update status in DB ✓
                              └── 2–3 matches → Qwen3-8B disambiguates → update ✓
```

---

## Reliability Architecture

### Webhook endpoint placement

The `/email-push` webhook is added to the existing `api.py` Flask server. No
separate service is needed. `api.py` is already:

- Running as a systemd service (`pipeline-api.service`)
- Publicly reachable via Cloudflare tunnel
- Protected by API key authentication
- Monitored and auto-restarted by systemd

Adding a new route to an existing service is simpler and avoids a second
port, second systemd unit, and second tunnel entry.

### Watchdog monitoring for pipeline-api

`pipeline-api.service` already has `Restart=always` in its systemd unit,
which means systemd restarts it automatically on a normal crash within
seconds. However, if it crashes repeatedly (5 times in 5 minutes), systemd
enters a `failed` state and stops retrying. At that point, only a
`reset-failed + restart` sequence recovers it.

`pipeline-api` is added to the watchdog's `check_systemd_services()` list
with the same heal action used for the scheduler:

```text
Watchdog detects pipeline-api in failed state
    ↓
sudo systemctl reset-failed pipeline-api
sudo systemctl restart pipeline-api
    ↓
If restart fails 3 times → escalation email sent → auto-heal paused 24h
```

This makes `pipeline-api` a first-class monitored service, not a blind spot.

### Two-layer durability — at-least-once delivery

Two independent systems provide at-least-once delivery while Pub/Sub and
Gmail history remain available. Expired history IDs or watch gaps (e.g. watch
not renewed within 7 days) can permanently lose messages that arrived while
the watch was inactive. Within normal operation:

### Implementation notes — history.list

**One Pub/Sub notification can cover multiple emails.** If several emails
arrive in quick succession, Gmail may bundle them under a single `historyId`.
`history.list` returns all of them. Each must be queued as a separate Redis
entry:

```python
page_token = None
all_history_id = None
while True:
    resp = gmail.users.history().list(
        userId="me", startHistoryId=last_id, pageToken=page_token
    ).execute()
    for record in resp.get("history", []):
        for msg in record.get("messagesAdded", []):
            redis.lpush("stream:email-push", msg["message"]["id"])
    # historyId is only on the first page; capture from any page that has it
    if "historyId" in resp:
        all_history_id = resp["historyId"]
    page_token = resp.get("nextPageToken")
    if not page_token:
        break
if all_history_id:
    update_last_history_id(user_id, all_history_id)
```

**Filter for `messagesAdded` only.** `history.list` also returns label
changes, reads, and deletions. Processing those would attempt to fetch and
classify emails that haven't changed — filter them out explicitly as shown
above.

---

**Layer 1 — Pub/Sub (external)**

Pub/Sub retains unacknowledged notifications for up to 7 days and retries
delivery with exponential backoff. If `pipeline-api` is down, Pub/Sub keeps
retrying. When the server comes back up, all queued notifications are
delivered automatically. This handles the server-down scenario completely.

**Layer 2 — Redis List (internal)**

When a notification is received, it is written to a Redis List
(`stream:email-push`, accessed via LPUSH/LMOVE/LREM) before Pub/Sub is
acknowledged. The acknowledgment (HTTP 200) is only sent after the Redis
write succeeds — a fast operation taking under 100ms.

```text
Pub/Sub delivers notification
    ↓
LPUSH stream:email-push  (Redis List, AOF-persistent)
    ↓  (< 100ms)
Return HTTP 200 to Pub/Sub  ← notification acknowledged
    ↓
Background processor reads and processes from Redis List
```

Redis already has AOF persistence enabled (writes approximately every 1 second
with `appendfsync everysec`). Jobs in the list survive a planned restart. However,
the approximately one-second write interval does not guarantee durability
immediately after LPUSH: returning HTTP 200 to Pub/Sub after a successful LPUSH
does not guarantee the write survived a crash if it occurred before the next AOF
flush. In practice, at most ~1 second of writes can be lost on an unclean shutdown.
For crash-after-acknowledgment scenarios involving that window, Pub/Sub will not
retry (it already received 200). This handles the common crash-after-acknowledgment
case but is not a zero-loss guarantee under all failure modes.

Together, these two layers mean:

| Failure scenario | Handled by |
|---|---|
| Server down when notification arrives | Pub/Sub retries for up to 7 days |
| Server crashes before writing to Redis | Pub/Sub retries (not yet acknowledged) |
| Server crashes after writing to Redis but before processing | Redis stream retains the job |
| Redis crashes | AOF persistence restores stream on restart |

### Email processor service

The email processor runs as a dedicated systemd service
(`email-processor.service`), independent from all other pipeline services.

**Why a separate systemd service and not part of an existing one:**

| Option | Why rejected |
|---|---|
| Spawn from scheduler | Email processing is unrelated to job scanning; a scheduler restart would kill the email processor and force Qwen to reload unnecessarily |
| Run inside api.py (Gunicorn) | Gunicorn runs multiple worker processes — each would load Qwen separately (~4.5GB × 2 = 9GB, exceeding VM RAM) |
| **New systemd service** | Clean isolation, independent lifecycle, Qwen loads exactly once |

**`email_processor.py` is a single monolithic process.** It reads from the
Redis stream AND runs Qwen inference AND updates the database — all inside
one process. There is no separate inference server or sub-component that
receives jobs. Qwen is loaded directly into the process via
`llama-cpp-python`.

This matters for failure handling: if Qwen throws an exception, it is caught
within the same process and the retry logic fires immediately. There is no
scenario where the queue reader is unaware that inference has failed — they
share the same execution context.

```text
email_processor.py — one process, everything inside
    ├── Reads from stream:email-push       (Redis)
    ├── Calls Gmail API — metadata only    (subject + sender)
    ├── Runs Qwen3-8B gate call            (llama-cpp-python, loaded at startup)
    ├── Calls Gmail API — full body        (only if gate outputs yes/not_sure)
    ├── Runs Qwen3-8B extraction call      (llama-cpp-python, same loaded model)
    ├── Runs 4-layer matching funnel       (rapidfuzz + Qwen3-8B disambiguation)
    └── Writes status update               (PostgreSQL)
```

The single worker follows the same at-least-once delivery pattern used by
the detail worker:

```text
Read job from stream:email-push
    ↓
LMOVE → stream:email-push:inflight:{pid}  (atomic — job safe even if crash)
    ↓
Process email (fetch → Qwen → DB update)
    ↓
Success → LREM from inflight list ✓
Failure → retry (see below)
```

If the process crashes mid-processing, the job remains in its inflight list.
On the next startup, `_recover_stuck_jobs()` scans inflight lists from dead
processes and requeues them automatically — the same recovery mechanism the
detail worker uses.

**Multiple workers were considered and rejected:**
- Email volume is low: ~5–10 actionable emails per day after the gate filters
- Both models are CPU-intensive — two simultaneous inferences on a 2-core VM
  create contention and make both slower than sequential processing

### Retry mechanism

Not all failures are equal. The system distinguishes two types and handles
them differently:

**Transient failures** — temporary conditions that will resolve on their own:
Gmail API timeout, DB connection blip, Qwen inference crash. Retrying will
help.

**Permanent failures** — logical outcomes that will never change by retrying:
the email is irrelevant (not job-related), or the matching funnel returns 0
candidates (the application simply is not in the DB). Retrying will never
help.

```text
Failure type          →  Action

Transient (API/DB/Qwen error)
  Attempt 1  →  wait 30 seconds  →  retry
  Attempt 2  →  wait 5 minutes   →  retry
  Attempt 3  →  move to queue:email:dlq

Permanent — irrelevant email
  →  acknowledge immediately, log and discard

Permanent — 0 candidates found
  →  acknowledge immediately, write to unmatched_emails table
     (see Unmatched Emails section below)
     does NOT enter the retry queue or DLQ
```

The retry attempt count is stored alongside the job payload in the stream.
Sending a 0-match through the retry queue would be wasteful — the missing
application will not appear in the DB between retry attempts.

### Dead Letter Queue (DLQ)

Jobs that fail all 3 retry attempts are moved to `queue:email:dlq`. This
follows the exact same design as the existing `queue:detail:dlq`:

| Setting | Value |
|---|---|
| Max entries | 200 (hard cap via `LPUSH + LTRIM 0 199`) |
| Entry TTL | 7 days |
| Entry format | `{"_dlq_added_at": "<ISO timestamp>", "payload": <original job>}` |
| Watchdog alert | Depth > 50 triggers WARNING email |
| Age cleanup | Lua-atomic script in `watchdog._check_dlq_health()` — same function extended to cover `queue:email:dlq` alongside `queue:detail:dlq` |

The 200-entry cap and 7-day TTL prevent unbounded growth. In practice, the
DLQ should rarely receive entries — it exists as a safety net for genuinely
unexpected failures, not as a normal operating condition.

---

## Edge Cases

### HTML-only email body

Many ATS systems (Greenhouse, Workday, Ashby) send HTML-only emails with no
plain text alternative. The Gmail API returns the body in `payload.parts` for
multipart emails. Grabbing `payload.body.data` directly yields nothing or raw
HTML tags passed to the model, which produces garbage output.

**Fix:** extract the `text/plain` part first; if absent, strip HTML tags from
the `text/html` part:

```python
def extract_body(payload):
    """Recursively traverse MIME parts; return text/plain or stripped text/html."""
    def _walk(part):
        mime = part.get("mimeType", "")
        body = part.get("body", {})
        # Skip attachments (have attachmentId, no inline data)
        if body.get("attachmentId"):
            return None
        if mime == "text/plain" and body.get("data"):
            return base64.urlsafe_b64decode(body["data"]).decode()
        if mime == "text/html" and body.get("data"):
            html = base64.urlsafe_b64decode(body["data"]).decode()
            return re.sub(r"<[^>]+>", " ", html)
        # Recurse into nested multipart/*
        for sub in part.get("parts", []):
            result = _walk(sub)
            if result:
                return result
        return None

    text = _walk(payload)
    if text:
        return text
    # Fallback: single-part email with data directly on payload body
    data = payload.get("body", {}).get("data")
    return base64.urlsafe_b64decode(data).decode() if data else ""
```

### Email body too long

Some rejection emails arrive with multi-page legal footers, unsubscribe
notices, and company boilerplate. With `n_ctx=2048`, anything beyond ~1500
words is silently truncated by llama-cpp. If the actual rejection sentence is
buried at the end, the model never sees it.

**Fix:** truncate the body before passing to the model. The budget must
account for the model token budget: `n_ctx=2048` must accommodate the system
prompt, the JSON output schema, and the email body together. At ~1 token per
word, roughly 1,200–1,400 words of body text leaves adequate headroom for
prompt overhead and the required JSON output. The actionable content
(rejection, invite, offer language) is always near the top of job-related
emails — the tail is always boilerplate:

```python
body = " ".join(body.split()[:1400])   # adjust based on measured prompt overhead
```

### Model inference hangs indefinitely

`llm.create_chat_completion()` has no built-in timeout. If the model hangs,
the worker blocks forever and no further emails are processed. A thread-based
timeout cannot actually kill a hung C++ inference call — the thread keeps
running after the timeout fires.

**Fix:** run inference in a forked child process with a 120-second timeout.
`process.terminate()` sends SIGTERM to the child, actually killing the C++
call. The parent continues immediately:

```python
import multiprocessing

def _run_with_timeout(fn, timeout=120):
    def _worker(q, fn):
        try:
            q.put(("ok", fn()))
        except Exception as exc:
            try:
                q.put(("err", exc))
            except Exception:
                q.put(("err", RuntimeError(str(exc))))

    q = multiprocessing.Queue()
    p = multiprocessing.Process(target=_worker, args=(q, fn))
    p.start()
    try:
        p.join(timeout)
        if p.is_alive():
            p.terminate()
            p.join()
            raise RuntimeError(f"inference timed out after {timeout}s")
        try:
            kind, val = q.get(timeout=1)   # q.empty() is unreliable across processes
            if kind == "ok":
                return val
            raise val
        except queue.Empty:
            raise RuntimeError("inference process exited without result")
    finally:
        q.close()
        q.join_thread()  # drain feeder thread
        p.close()        # release process handle
```

On Linux, `fork()` gives the child process a copy-on-write view of the
parent's memory — the loaded `_llm` object is available in the child
without reloading the model. Terminating the child does not affect the
parent's model state (model weights are mmap'd read-only).

### Gmail historyId expiry (404)

Gmail retains history records for approximately 7 days. If
`last_history_id` stored in the DB is older than that, `history.list`
returns HTTP 404. The generic retry path would loop forever on a stale
cursor that will never become valid.

**Fix:** detect `HttpError` with status 404 on `history.list`, reset the
cursor to the current notification's `history_id` (which is always fresh),
and return `"discard"`:

```python
except HttpError as exc:
    if exc.resp.status == 404:
        logger.warning("startHistoryId expired — resetting cursor to %s", history_id)
        update_history_id(user_id, history_id)
        return "discard"   # missed messages unrecoverable; cursor now valid
    return "retry"
```

Missed messages during the gap are unrecoverable, but the cursor is
immediately reset so all future notifications work correctly.

### Unmatched email duplicates on retry

When `any_failed=True` after processing a batch, the notification returns
`"retry"` without advancing the cursor. On the next attempt, all messages
in the batch are reprocessed — including any that already wrote a row to
`unmatched_emails`. A plain `INSERT` would create duplicate rows.

**Fix:** `unmatched_emails` has a `gmail_message_id` column with a partial
unique index (`WHERE gmail_message_id IS NOT NULL`). `_write_unmatched`
uses `ON CONFLICT (gmail_message_id) WHERE gmail_message_id IS NOT NULL DO NOTHING`
so retry attempts are silent no-ops for already-written rows.

### OAuth token revoked

If the user visits Google security settings and removes app access, all Gmail
API calls fail with `401 invalid_grant`. The retry mechanism would exhaust
all attempts and push the job into the DLQ — but every subsequent email for
that user would do the same, filling the DLQ silently.

**Fix:** detect `invalid_grant` specifically and treat it as a permanent
failure (not transient) — skip the retry queue entirely and send the user an
alert email asking them to re-authorize. Do not retry: the token will not
become valid again on its own.

```python
from google.auth.exceptions import RefreshError

except RefreshError:
    # Token refresh failed at the credentials layer (e.g. token expired/revoked
    # before the API call was even made). Treat as permanent.
    send_reauth_alert(user_id)
    return  # permanent — do not retry
except HttpError as e:
    if "invalid_grant" in str(e):
        send_reauth_alert(user_id)
        return  # permanent — do not retry
    raise  # transient — let retry mechanism handle it
```

### Emails missed during watch gap

If `email-processor.service` is down when a Gmail watch expires and the
renewal cron also fails, push notifications stop. When the watch is eventually
renewed, any emails that arrived during the gap are permanently lost — Pub/Sub
only delivers notifications it observed while the watch was active.

**Fix:** on every watch renewal, immediately call `history.list` from
`last_history_id` to catch up on any missed messages before the new watch
takes over:

```python
def renew_watch(user_id):
    new_watch = gmail.watch(...)
    catch_up_missed_emails(user_id)   # history.list from last_history_id
    update_watch_expiry(user_id, new_watch["expiration"])
```

This runs inside `renew_gmail_watch.py` and requires no separate mechanism —
the same `last_history_id` used for normal processing is the starting point
for the catch-up.

---

## Unmatched Emails

When the matching funnel returns 0 candidates — meaning the email was
job-related but no pending application could be identified — the email is
not silently discarded. It is written to the `unmatched_emails` table and
surfaced to the user in the dashboard.

### Why this matters

A 0-match can happen in legitimate situations that the user should know about:

```text
Applied on mobile  → Chrome extension never ran → no DB entry
Company name typed so differently that fuzzy match cannot bridge the gap
Email arrived before user added the application to the platform
```

In all of these cases, a real status update (rejection, interview invite)
would be silently lost without this mechanism. The unmatched panel gives the
user full visibility and a way to act on it.

### Current notification — daily digest email

There is no web frontend yet. Unmatched emails are surfaced in the existing
daily digest email, which the user already receives every morning:

```text
─── Unmatched Emails (2) ────────────────────────────
Could not match these job-related emails to an application.
Check your inbox and manually update the status if needed.

  Stripe · Software Engineer · Rejection
  From: no-reply@greenhouse.io · July 17
  "Thank you for applying. After careful consideration..."

  Waymo · ML Engineer · Interview Invite
  From: recruiting@waymo.com · July 17
  "We'd love to schedule a call to discuss..."
─────────────────────────────────────────────────────
```

> **Deferred — dashboard panel:** When a frontend is built, unmatched emails
> will be surfaced as interactive cards where the user can link an email
> directly to an application (triggering an immediate status update) or
> dismiss it. The `unmatched_emails` table is built now so no data is lost
> in the meantime.

### Retention

Unmatched entries older than 30 days are cleaned up automatically by the
watchdog's daily maintenance cycle — the same pattern used for DLQ TTL
cleanup.

---

## Database Schema — unmatched_emails

| Column | Type | Notes |
|---|---|---|
| `id` | SERIAL PK | Auto-increment |
| `user_id` | INT (FK → users) | Which user this email belongs to |
| `gmail_message_id` | TEXT (UNIQUE partial) | Gmail message ID — deduplication key; partial unique index `WHERE NOT NULL` so retry attempts are silent no-ops |
| `email_from` | TEXT | Sender address |
| `email_subject` | TEXT | Subject line |
| `extracted_company` | TEXT | Company name as extracted by Qwen |
| `extracted_title` | TEXT | Job title as extracted by Qwen (nullable) |
| `extracted_status` | TEXT | rejection / interview / phone_screen / assessment / offer |
| `email_snippet` | TEXT | First ~300 characters of email body for display |
| `received_at` | TIMESTAMPTZ | When the email arrived in the inbox |
| `dismissed` | BOOLEAN | True if user dismissed without linking |
| `created_at` | TIMESTAMPTZ | When this record was written |

**Indexes:**
- `idx_unmatched_emails_user_created` on `(user_id, created_at)` — user-scoped queries
- `idx_unmatched_emails_message_id` unique partial on `(gmail_message_id) WHERE NOT NULL` — retry dedup
- `idx_unmatched_emails_created_at` on `(created_at)` — global retention delete in `_cleanup_unmatched_emails`

---

| Component | Usage | Cost |
|---|---|---|
| Gmail API | ~200 emails/day × 5 quota units = 1,000 units/day (limit: 1B/day) | Free |
| Google Cloud Pub/Sub | ~200 notifications/day × 100 bytes = ~600KB/month (limit: 10GB/month) | Free |
| Oracle Cloud VM | 2 OCPUs + 12GB RAM (within Always Free A1 quota) | Free |
| Qwen3-8B model | Self-hosted, open source (Apache 2.0) | Free |
| **Total** | | **$0/month** |

---

## What Was Considered and Rejected

| Approach | Why rejected |
|---|---|
| Call ATS APIs to check status | ATS APIs are employer-facing only — no candidate-facing status API exists |
| Scrape ATS candidate portals | Requires user to visit portal manually; defeats the purpose of automation |
| Email alias tagging (e.g. rutvij+stripe@gmail.com) | Fragile — React forms and Workday intercept and strip the alias |
| Keyword/domain whitelist filter | Requires constant maintenance as new ATS platforms and patterns emerge |
| Separate 1.7B gate model + 8B extractor | Tested — 1.7B achieved 12/12 safe on subject-only but produced dangerous false negatives on body classification; 8B achieves the same gate accuracy at ~5–6s with no extra RAM or model to manage |
| Gemini API for email processing | Email content (company names, job titles, salary info) would leave the server |
| Fixed 6-day renewal cron (shared across all users) | Does not handle users added at different times with different expiry dates |
| Separate service for /email-push webhook | Adds complexity — new port, new systemd unit, new tunnel entry; api.py already covers all requirements |
| Multiple email processor workers | Email volume (~5–10/day) does not justify it; Qwen is CPU-intensive and parallel inference on 2 cores is slower than sequential |
| Acknowledge Pub/Sub immediately before writing to Redis | If server crashes after ack but before Redis write, notification is permanently lost |
| Build a custom retry queue from scratch | Unnecessary — same inflight + DLQ pattern already proven in detail_worker |
| Spawn email processor from the scheduler | Email processing is unrelated to job scanning; scheduler restart would kill email processor and force Qwen to reload |
| Run email processor inside api.py / Gunicorn | Gunicorn spawns multiple worker processes — each would load Qwen separately, exhausting VM RAM |
| Qwen as a separate inference server | Would decouple the queue reader from the inference engine — reader could dequeue jobs while inference server is down, wasting retries silently. Single-process design eliminates this entirely |
| Bonsai-27B 1-bit (PrismML) | Requires PrismML's custom llama.cpp fork with Q1_0_g128 hybrid-attention kernels — standard llama-cpp-python produced 544s per inference; no CPU throughput benchmarks exist; all published benchmarks are Apple Silicon (Metal) or NVIDIA (CUDA) |
| Ternary Bonsai 27B (PrismML) | Same custom Q2_0_g128 kernel requirement; memory at 4K context (8.4GB) + pipeline (2GB) = 10.4GB, exceeds 9.2GB available; no CPU benchmarks |
| Qwen3-0.6B as gate model | Too small for reliable 3-class gate decision; subject lines like "An update on your application" are genuinely ambiguous — edge-case errors silently drop real rejections |
| Qwen3-32B | Q2_K (lowest usable quantization) requires ~11GB — exceeds 9.2GB available at any quantization level; overkill for classifying short email subject lines |
| Pure fuzzy match against raw DB company names | User-typed names are too noisy — "strip" vs "Stripe" would fail exact match; the fix is having Qwen produce a clean company name first, then fuzzy-matching that clean output against the messy DB entries |
| Silently discard 0-match emails | A missed status update (rejection, interview invite) is a real loss; unmatched emails are stored in DB and surfaced via daily digest so nothing is silently lost |
| Build unmatched email dashboard panel now | No web frontend exists yet; the `unmatched_emails` table is built now to preserve data, and the panel is deferred until a frontend is available |
| Chrome extension for Gmail reading | Polling-based, only works while browser is open, SW can be killed by Chrome — server-side OAuth + Gmail Watch is significantly more reliable |
| Gmail forwarding to a shared inbox | Requires users to maintain ATS domain filter lists; misses emails from new ATS platforms; breaks email metadata |
| confirmed_sender column on applications | Premature optimization — at 5–10 emails/day, Qwen running every time is perfectly fine; adds edge cases (sender address changes, same sender for multiple applications) for no real benefit at this scale |
| Extend existing `status` column with email states | `status` is used by dozens of pipeline queries filtering `WHERE status = 'active'`; changing it would silently break recruiter outreach, cache lookups, and job scanning — separate `email_status` column is the right split |
| Pull subscription instead of push | Equally reliable but introduces Pub/Sub redelivery and Qwen non-determinism concerns; push with Redis stream is simpler and already fully designed |
| `processed_emails` table + `gmail_message_id` check | Only needed for pull (Pub/Sub redelivery); push returns HTTP 200 once and Pub/Sub never redelivers — no deduplication table needed |
| Custom domain for stable Pub/Sub URL | Costs money; not needed — `tunnel_manager.py` already handles URL changes and is extended to update the Pub/Sub push endpoint automatically |
| Retry 0-match emails like transient failures | A missing application will not appear in the DB between retry attempts; 0-match is a permanent outcome and goes directly to unmatched_emails, not the retry queue |
| Ack email normalization + pending_acks table | Use acknowledgement emails to retroactively set ats_company/ats_title on applications; store unmatched acks in a pending_acks table and resolve them when the application is later added via Chrome extension or Google Form. Rejected — over-engineering at current volume (5–10 emails/day, 2 users). Fuzzy matching already handles most company name mismatches; unmatched_emails + daily digest is the safety net for the rest. ats_company and ats_title columns are added to the schema as nullable for future use but not populated from acks |
| Pass hundreds of candidates to Qwen for disambiguation | Exceeds Qwen's context window; inference is slow and unreliable at that scale. The 4-layer funnel (rapidfuzz exact → fuzzy → title → Qwen) ensures Qwen never sees more than 2–3 candidates |
| Company name alone as the match key | Multiple open applications at the same company would return multiple candidates with no way to distinguish them; title is needed as a second signal |

---

## Files Involved

| File | Status | Purpose |
|---|---|---|
| `workers/email_processor.py` | Implemented | Long-running daemon — reads Redis List, runs Qwen inference, 4-layer matching funnel, DB status update, retry/DLQ for transient failures, unmatched_emails write for 0-match |
| `scripts/renew_gmail_watch.py` | Implemented | Daily watch renewal — checks all users' watch_expires_at, renews those within 48 hours |
| `api.py` | Modified | Added `POST /email-push` webhook endpoint (writes to Redis List, returns 200); added `GET /oauth/start` and `GET /oauth/callback` endpoints for one-time user authorization |
| `db/schema.py` | Modified | Added `gmail_tokens` table and `unmatched_emails` table |
| `workers/watchdog.py` | Modified | Added `pipeline-api` and `email-processor` to `check_systemd_services()`; extended `_check_dlq_health()` to cover `queue:email:dlq`; added 30-day cleanup for `unmatched_emails` |
| `deploy/systemd/email-processor.service` | Implemented | systemd unit for the email processor daemon |
| `deploy/install-systemd.sh` | Modified | Installs `email-processor.service` alongside existing units |
| `setup_cron.sh` | Modified | Daily 2 AM renewal job for `renew_gmail_watch.py` added |
| `requirements.txt` | To be modified | Add `rapidfuzz` (fuzzy matching), `llama-cpp-python` (Qwen inference), `cryptography` (token encryption) |

---

## Database Schema — gmail_tokens

One row per user. Stores OAuth credentials and Gmail watch state.

| Column | Type | Notes |
|---|---|---|
| `user_id` | INT (FK → users) | Primary key — one row per user |
| `gmail_email` | TEXT | User's Gmail address — matched against incoming Pub/Sub `emailAddress` field to identify which user an email belongs to |
| `refresh_token_enc` | TEXT | Fernet-encrypted (AES-128-CBC + HMAC-SHA256), base64-encoded OAuth refresh token. Named `_enc` to make encryption explicit — never use the raw value |
| `watch_id` | TEXT | ID returned by `gmail.watch()` — required to stop or renew the watch |
| `watch_expires_at` | TIMESTAMPTZ | Expiry timestamp of the active Gmail watch — renewal cron checks this daily |
| `last_history_id` | TEXT | Last Gmail historyId successfully processed for this user. Pub/Sub delivers a historyId (not the email itself) — the processor calls `gmail.users.history.list(startHistoryId=last_history_id)` to fetch new messages, then updates this value. Must be persisted: if the service restarts without it, the processor cannot know where it left off and will either miss emails or reprocess old ones. |
| `created_at` | TIMESTAMPTZ | When the user first authorized Gmail access |
| `updated_at` | TIMESTAMPTZ | Last time the watch was renewed or token was updated |

### Refresh token encryption

The refresh token grants permanent read access to a user's Gmail inbox. It
is encrypted at rest using **AES-256 application-level encryption** before
being written to the database.

A single encryption key is stored in `.env` as `GMAIL_TOKEN_ENCRYPTION_KEY`.
The application encrypts before every DB write and decrypts after every DB
read. The ciphertext stored in the DB is useless without the key.

```text
Security model:

Attacker obtains DB dump only    → encrypted tokens — useless without key
Attacker obtains .env only       → encryption key — no tokens to decrypt
Attacker obtains both            → full compromise (same as any credential theft)
```

This is standard defence-in-depth. No single leaked artifact is sufficient
to read a user's Gmail.

**Why application-level encryption over PostgreSQL pgcrypto:**
- No DB extension required
- Simpler to implement and audit
- Key rotation is straightforward: update `GMAIL_TOKEN_ENCRYPTION_KEY` in
  `.env` and re-encrypt all rows in one migration script
- Library: `cryptography` (Fernet — AES-128-CBC with HMAC-SHA256 and a
  128-bit AES key derived from a 32-byte Fernet key; standard Python choice
  for symmetric authenticated encryption)

**Column naming convention:** `refresh_token_enc` (not `refresh_token`) makes
the encryption contract explicit in the schema itself. Any developer reading
the code immediately knows this value must be decrypted before use — it
cannot be accidentally passed somewhere raw.

---

## OAuth Verification Roadmap

The OAuth consent screen shows an "unverified app" warning today because
Google verification is not worth pursuing for a private 2-person tool.
As the product grows, verification is achievable in phases:

| Stage | Users | Action | Cost | Outcome |
|---|---|---|---|---|
| Now | 2 (private) | None — trusted users click "Continue anyway" | Free | Works fine |
| Growing | 50–100 | Submit app to Google for review: privacy policy page + domain verification | Free, 3–6 weeks | Warning disappears, normal consent screen |
| Serious scale | 100+ | CASA Tier 1 security audit | ~$1,500–$3,000 | Fully trusted, no friction for any user |

Verification is a phased investment that matches where the product actually
is — not a blocker for launch.

---

## One-Time Setup Required

Before this feature can go live, the following must be done once per
deployment environment:

1. **Google Cloud project** — Enable Gmail API and Pub/Sub API
2. **Pub/Sub topic and subscription** — Create a push subscription pointing
   to `<tunnel-url>/email-push` with authentication type **OIDC token** and a
   Google-managed service account email; `tunnel_manager.py` keeps the URL
   current automatically after the first setup. Do not use a query-string token
   (`?token=...`) — bearer tokens in URLs appear in server logs. Verify the
   incoming OIDC JWT in `api.py` on every `/email-push` request before processing.
3. **OAuth2 credentials** — Create a new Web Application OAuth 2.0 Client ID
   in Google Console (separate from the Chrome Extension client). Add exactly
   one authorized redirect URI:
   `https://mail-oauth-redirect.rutvijmavani.workers.dev/oauth/callback`
   This never changes — the Cloudflare Worker handles all future tunnel URL
   changes automatically. Each user authorizes once via `/oauth/start?user_id=N`.
4. **Cloudflare Worker** — Deploy `mail-oauth-redirect` to Cloudflare Workers
   (free tier). Already live at
   `https://mail-oauth-redirect.rutvijmavani.workers.dev`. Reads `api_base`
   from the Gist and 302-redirects all OAuth callbacks to the current tunnel.
5. **`GIST_CONFIG_URL` env var** — Set to the raw Gist URL (same value as in
   `chrome-extension/config.js`). `api.py` reads it to build the dynamic
   redirect URI at OAuth start time.
6. **`GMAIL_OAUTH_REDIRECT_URI` env var** — Set to the Worker URL
   (`https://mail-oauth-redirect.rutvijmavani.workers.dev/oauth/callback`).
   Used as fallback when the Gist is unreachable.
6. **Model download** — Download the model to the server (one-time):
   ```bash
   wget https://huggingface.co/Qwen/Qwen3-8B-GGUF/resolve/main/Qwen3-8B-Q4_K_M.gguf \
     -O /home/opc/mail/models/Qwen3-8B-Q4_K_M.gguf
   ```
