# Product Roadmap — Job Intelligence Platform

> Living document. Update as decisions are made, features ship, or scope changes.
> Last updated: 2026-07-10

---

## Vision

A **collaborative, source-of-truth job intelligence platform** where job hunters collectively monitor company career pages directly — delivering near-real-time freshness that LinkedIn/Indeed cannot match — and share a growing database of companies, job postings, and recruiter contacts, while keeping application tracking personal.

**Core differentiator:** We monitor the ATS directly (Workday, Greenhouse, Lever, eightfold, etc.), not an aggregator. Jobs typically surface here within 1–6 hours of posting — hours or days before LinkedIn/Indeed syndicates them. (Actual latency depends on ATS polling interval and platform delays; "instant" is not guaranteed.)

**Business model:** Not decided. Validate invite-only first. Do not think about subscriptions or billing until there are active users who find the product indispensable.

---

## What Exists Today (Single-User, Production)

| Component | Status | Notes |
|-----------|--------|-------|
| Adaptive scheduler | ✅ Production | Redis ZSET gap-filling, dynamic 1h–6h intervals per company |
| scan_worker | ✅ Production | Redis Streams consumer groups |
| detail_worker | ✅ Production | Job detail enrichment, country filtering |
| fullscan_worker | ✅ Production | Periodic full sweep |
| watchdog | ✅ Production | Auto-restarts dead workers via systemctl |
| PostgreSQL | ✅ Production | 29,312 jobs, 139 companies |
| Redis | ✅ Production | AOF persistence |
| Sentry | ✅ Production | Real-time exception capture, ERROR+ events |
| log_monitor | ✅ Production | 15-min batch file scanner, email alerts |
| health_check | ✅ Production | System status script |
| ATS platforms | ✅ Production | Workday, eightfold, SuccessFactors, Greenhouse, Lever, Ashby |
| Email digest | ✅ Production | New job notifications to one email address |
| Oracle Cloud deploy | ✅ Production | systemd, single server |

**Current scale:** 139 companies monitored · ~2,000 new US-based jobs/week · 29,312 total postings tracked

---

## What We Are NOT Building (Until Explicitly Decided)

- Authentication system (login, passwords, sessions)
- Billing or subscription tiers
- Mobile app
- Browser extension
- Public signup / self-serve
- API for third parties
- Multi-region deployment

These stay off the table until invite-only is validated and the next problem is clear.

**Phase 1 data ownership note:** All per-user data (watchlists, job statuses, recruiter contacts) is operator-managed — created and modified via admin scripts only. Users have no direct access to the system in Phase 1. There is no authentication, no user-facing UI, and no self-service data deletion.

Minimum privacy controls before inviting users:
- Operator access must be authorized (i.e., only you run the admin scripts; no shared credentials)
- Users must consent to their email and job-tracking data being stored
- Retention policy: define how long data is kept; delete user data on request (manual, via admin script)
- User data export: operator can dump a user's rows on request (watchlist, statuses, outreach)
- Incident response: if data is exposed, notify affected users promptly

Full privacy and authorization infrastructure (role-based access, self-service deletion, audit logs) is deferred to Phase 3 (Web UI) when users interact with the system directly.

---

## Phased Plan

### Phase 0 — Immediate (This Week)

| Task | Effort | Why |
|------|--------|-----|
| log_monitor: field-split level parsing | 30 min | Current regex misses WARNING-level errors (e.g. Barclays missing keys). `line.split('\|')[1].strip()` is reliable and level-aware. Fixes the current known bug. |
| Commit + push all pending changes | 1–2 hrs | 15+ modified files uncommitted. Block everything else until this is done. |

---

### Phase 1 — Invite-Only (1–3 months)

**Goal:** 3–5 job-hunting friends using the platform. Each adds companies they're targeting. Everyone benefits. This is the first real validation that the collaborative model works.

**Rule:** Do not build Phase 2 until Phase 1 has active users who find the digest genuinely useful.

#### 1a — Multi-User Foundation

**New DB tables:**
```sql
CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    email TEXT UNIQUE NOT NULL,
    name TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE user_watchlist (
    user_id INT REFERENCES users(id),
    company_id INT REFERENCES companies(id),
    PRIMARY KEY (user_id, company_id)
);

CREATE TABLE recruiters (
    id SERIAL PRIMARY KEY,
    company_id INT REFERENCES companies(id),
    name TEXT,
    email TEXT,
    title TEXT,
    linkedin_url TEXT,
    added_by INT REFERENCES users(id)
);

CREATE TABLE user_job_status (
    user_id INT REFERENCES users(id),
    job_id INT REFERENCES jobs(id),
    status TEXT CHECK (status IN ('seen','saved','applied','rejected','offer')),
    notes TEXT,
    applied_at TIMESTAMPTZ,
    PRIMARY KEY (user_id, job_id)
);

CREATE TABLE user_outreach (
    id SERIAL PRIMARY KEY,
    user_id INT REFERENCES users(id),
    recruiter_id INT REFERENCES recruiters(id),
    contacted_at TIMESTAMPTZ,
    response_at TIMESTAMPTZ,
    notes TEXT
);
```

Add `verified BOOLEAN DEFAULT FALSE` and `added_by INT REFERENCES users(id)` to the existing `companies` table. Crowd-sourced submissions start unverified.

**Key design principle:** Jobs are non-rivalrous. If 3 users watch Google, scan Google once → all 3 see the results. Scheduler computes `UNION of all users' watchlists` = companies to scan.

**Data split:**
```text
SHARED (all users benefit)          PER-USER (personal layer)
├── companies                        ├── user_watchlist
├── jobs                             ├── user_job_status
└── recruiters                       └── user_outreach
```

**Effort:** 2 days

#### 1b — Per-User Email Digest

Query each user's watchlist → new jobs since last digest → send to their email. Currently hardcoded to one address.

Also: namespace Redis dedup keys per-user → `digest:dedup:{user_id}:{job_id}`

**Effort:** 1 day

#### 1c — Transactional Email (Required Before Sending to Multiple Users)

Gmail will get flagged as spam with multiple recipients. Switch to AWS SES, SendGrid, or Postmark before inviting anyone. SES is cheapest ($0 for first 62K emails/month from EC2).

**Effort:** half day

#### 1d — Admin Script

```bash
python add_user.py --email friend@example.com --companies "Google,Apple,Stripe"
python add_company.py --company "Stripe" --url "https://stripe.com/jobs"
```

No web UI needed. You (the operator) add users and companies manually via these scripts. Friends tell you which companies to add; you run the script. There is no self-service mechanism in Phase 1.

**Effort:** half day

#### 1e — Company Coverage Bootstrap

**Biggest leverage item.** Simplify.jobs maintains an open-source company + ATS mapping on GitHub. One import script jumps coverage from 139 → thousands overnight without manual ATS detection work.

Write a one-time migration script that:
1. Pulls from Simplify's public data
2. Maps their ATS platform names to yours
3. Inserts companies as `verified = FALSE`
4. You manually verify a subset, mark them `verified = TRUE`

**Effort:** 1–2 days

#### Phase 1 — What You Do NOT Need

- No web UI
- No auth system
- No API
- One server is enough for 3–10 users

**Total Phase 1 effort:** ~1.5–2 weeks of actual work (spread over 1–3 months part-time)

**Phase 1 exit criteria (move to Phase 2 only when):**
- At least 3 active users receiving digests
- Users are requesting companies to add (operator adds them via admin script)
- The digest is surfacing jobs people wouldn't have found otherwise
- At least one person has found and applied to a job through it

---

### Phase 2 — Scale (3–6 months, after Phase 1 validated)

Only build this when Phase 1 is working and you feel load or coverage limits.

#### 2a — Multi-Server + Scheduler Leader Election

Workers already scale horizontally via Redis Streams consumer groups — zero code change. Only the scheduler needs coordination:

```python
import uuid, redis

LEADER_KEY = "scheduler:leader"
LEADER_TTL = 30  # seconds

token = str(uuid.uuid4())  # unique ownership token per instance
acquired = r.set(LEADER_KEY, token, nx=True, ex=LEADER_TTL)
if not acquired:
    stand_by_until_leader_dies()

# Renew only if we still own the lock (Lua: compare token, then expire)
_RENEW_SCRIPT = redis.client.Script(r, """
    if redis.call("get", KEYS[1]) == ARGV[1] then
        return redis.call("expire", KEYS[1], ARGV[2])
    else
        return 0
    end
""")

while True:
    renewed = _RENEW_SCRIPT(keys=[LEADER_KEY], args=[token, LEADER_TTL])
    if not renewed:
        break  # another instance took over — stop and exit
    run_scheduling_tick()
    sleep(10)
```

If leader dies → lock expires → standby takes over. A paused former leader cannot renew leadership because the Lua script verifies the token before extending TTL.

**Effort:** ~30 lines of code, 2–3 days including testing

#### 2b — JSON Structured Logging

Becomes essential when logs need to be aggregated across multiple servers. Not worth doing on a single server — human-readable logs are better there.

**Changes:**
- All `logging.Formatter` → emit JSON `{"time":…, "level":…, "logger":…, "msg":…}`
- `log_monitor.py` → `json.loads(line)["level"]` replaces regex/field-split
- TTY detection → pretty-print for local dev
- Central log shipper (Loki, CloudWatch) → ingest JSON directly

**Effort:** 3–4 days

#### 2c — Distributed Worker Fleet

Each user optionally deploys worker-only instances on their own free cloud tier. Central server handles coordination + data only. Workers use each user's IP for scraping — distributes rate limiting risk.

```
Central Server (your Oracle instance)
├── PostgreSQL  — all shared data
├── Redis       — task queue + coordination
└── Scheduler   — leader (Phase 2a)

User 1's Oracle Free Tier     User 2's AWS Free Tier
└── scan_worker               └── scan_worker
└── detail_worker             └── detail_worker
    their IP                      their IP
```

Deployment for each user (two files, one command):
```yaml
# docker-compose.yml
# Use an immutable digest instead of :latest to prevent unexpected upgrades.
# Example: ghcr.io/yourhandle/pipeline-worker@sha256:<digest>
services:
  scan_worker:
    image: ghcr.io/yourhandle/pipeline-worker@sha256:<digest>
    env_file: .env    # REDIS_URL + DATABASE_URL only
    restart: always

  detail_worker:
    image: ghcr.io/yourhandle/pipeline-worker@sha256:<digest>
    env_file: .env
    restart: always
```

**Why this works without code changes:** Consumer groups handle work distribution. `XAUTOCLAIM` handles dead workers. Workers don't know or care where they run.

**Security before enabling this:**
- TLS on Redis + PostgreSQL
- Redis ACLs: each worker gets a dedicated credential with access restricted to the streams and keys it actually uses (no KEYS *, no CONFIG, no admin commands); rotate credentials per deployment
- Worker DB credential = append job results only (no read of user data, no admin)
- Firewall: allowlist known worker IPs only
- Workers are authenticated to Redis by credential only — they are not trusted to self-identify their user or tenant; all task scoping is enforced server-side via stream namespacing
- Define per-worker egress quotas (max requests/minute per IP) to contain abuse if a worker is compromised

**Threat model for distributed workers:**
- **Compromised worker:** A worker that is taken over by an attacker can read jobs from its assigned streams and write fabricated job results to the DB. Mitigate: append-only DB credential (cannot read user data), stream namespacing (limits blast radius to specific queues), Redis ACL (cannot inspect other tenants' keys).
- **Result integrity:** A compromised worker could submit fabricated job records (wrong titles, locations, or URLs). Mitigate: the central server issues a signed task token per work item; submitted results must include the token and are bound to the assigned company + source URL + platform; the server validates company, source URL, and platform before writing to the DB. Only results that pass server-side validation can appear in user digests.
- **Malicious user-deployed worker:** A user could deploy a worker and point it at a target company's ATS repeatedly to DoS it. Mitigate: per-worker egress quotas enforced via Redis rate-limit keys; scheduler controls polling intervals and cannot be overridden by workers.
- **Credential leak:** If a user's `.env` is exposed, an attacker gets Redis + DB credentials. Mitigate: per-user Redis credentials with minimal ACL scope; DB credential is append-only and scoped to the job-results tables.
- **User consent:** Users who deploy workers must explicitly consent to their IP being used for scraping. Document this clearly in the deployment instructions.

**Note:** Only offer this to technical users who can set up a server. Non-technical users wait for the web UI.

**Effort:** 2–3 days packaging + 1 day security hardening

---

### Phase 3 — Web UI (6–12 months)

Needed when non-technical friends can't use a CLI or config file. This unlocks the full collaborative model — anyone can add companies, track applications, and contribute without technical setup.

**Minimum viable pages:**

| Page | Purpose |
|------|---------|
| Dashboard | New jobs since last visit, by company |
| Companies | My watchlist — add company by pasting careers URL (ATS auto-detect) |
| Jobs | Browse + filter — mark seen/saved/applied |
| Recruiters | Shared pool — log outreach, track responses |
| Application tracker | My pipeline: applied → interviewing → offer/rejected |

**Tech:** Next.js (already on resume) + FastAPI thin layer over existing PostgreSQL.

**Auth:** Email magic-link (no passwords). next-auth or lucia-auth handles it.

**AI relevance filter (add here):** Before sending the digest, score jobs against user's resume. One LLM API call per user per cycle. Prevents digest overload as company count grows.

```python
# rough shape — per user, per digest
top_jobs = llm.rank_by_relevance(user_resume, new_jobs, top_n=10)
```

**LLM data-handling requirements before building this:**
- Choose an LLM provider that does not train on API inputs by default, or get explicit opt-out confirmation
- Redact PII from resumes before sending (phone numbers, home address, passport/visa details) — job titles and skills are sufficient for ranking
- User must explicitly consent to their resume being sent to a third-party LLM service
- Confirm provider's data retention policy (how long prompts are stored, if at all)
- Treat LLM failure as non-fatal: if the ranking call fails, fall back to sending all new jobs unranked rather than skipping the digest
- Each user's resume is sent only in their own API call — never batch multiple users' data in one prompt

**Effort:** 4–6 weeks

---

## ATS Management Strategy

### Detecting ATS When Adding a Company

Paste careers URL → auto-detect platform:

```python
import ipaddress, socket, requests
from urllib.parse import urlparse, urljoin

_MAX_DETECT_REDIRECTS = 5
_MAX_BODY_BYTES       = 1_000_000
_METADATA_NETS = [
    ipaddress.ip_network("169.254.169.254/32"),  # AWS/GCP/Azure IMDS
    ipaddress.ip_network("fd00:ec2::254/128"),    # AWS IMDSv6
]

def _ssrf_check(url: str) -> None:
    """
    Raise ValueError if url targets a private/loopback/link-local/metadata address.
    Resolves ALL addresses returned by getaddrinfo (not just the first) so a host
    with a mix of public and private records is rejected. Does not prevent
    DNS-rebinding between this check and the actual TCP connection — pin the
    resolved address for the request if that threat is in scope.
    """
    p = urlparse(url)
    if p.scheme not in ("http", "https"):
        raise ValueError(f"detect_ats: unsupported scheme {p.scheme!r}")
    hostname = p.hostname or ""
    try:
        # Try raw IP first (no DNS lookup needed)
        addrs = [ipaddress.ip_address(hostname)]
    except ValueError:
        # Hostname — resolve every A/AAAA record and check all of them
        try:
            results = socket.getaddrinfo(hostname, None, socket.AF_UNSPEC)
            addrs   = [ipaddress.ip_address(r[4][0]) for r in results]
        except (socket.gaierror, ValueError):
            return  # unresolvable; the request will fail naturally
    for ip in addrs:
        if ip.is_private or ip.is_loopback or ip.is_link_local:
            raise ValueError(f"detect_ats: blocked address {ip} for {hostname!r}")
        if any(ip in net for net in _METADATA_NETS):
            raise ValueError(f"detect_ats: metadata IP blocked: {ip}")

def _read_body(resp, limit: int = _MAX_BODY_BYTES) -> str:
    """Read up to `limit` bytes from a response, closing it when done."""
    cl = resp.headers.get("Content-Length", "")
    if cl.isdigit() and int(cl) > limit:
        raise ValueError(f"detect_ats: Content-Length {cl} exceeds {limit}-byte limit")
    chunks = []
    total  = 0
    try:
        for chunk in resp.iter_content(chunk_size=65_536):
            total += len(chunk)
            if total > limit:
                raise ValueError(f"detect_ats: response body exceeds {limit}-byte limit")
            chunks.append(chunk)
    finally:
        resp.close()
    return b"".join(chunks).decode("utf-8", errors="replace")

def detect_ats(careers_url: str) -> str:
    _ssrf_check(careers_url)

    session = requests.Session()
    resp = session.get(careers_url, allow_redirects=False, timeout=10, stream=True)

    # Follow redirects manually, validating each hop before following
    hops = 0
    while resp.is_redirect and hops < _MAX_DETECT_REDIRECTS:
        resp.close()
        next_url = resp.headers.get("Location", "")
        if not next_url.startswith(("http://", "https://")):
            next_url = urljoin(resp.url, next_url)
        _ssrf_check(next_url)          # validate scheme + all resolved IPs at every hop
        resp = session.get(next_url, allow_redirects=False, timeout=10, stream=True)
        hops += 1

    html = _read_body(resp)            # enforces 1 MB cap; closes resp
    url  = resp.url

    fingerprints = [
        ('workday',    'myworkdayjobs.com'),
        ('greenhouse', 'boards.greenhouse.io'),
        ('lever',      'jobs.lever.co'),
        ('ashby',      'jobs.ashbyhq.com'),
        ('eightfold',  'eightfold.ai'),
        ('greenhouse', 'greenhouse-job-board'),  # embedded iframe
        ('lever',      'jobs.lever.co/embed'),
    ]
    for platform, signal in fingerprints:
        if signal in url or signal in html:
            return platform

    return 'unknown'  # flag for manual review
```

Covers ~70–80% of companies. Only `unknown` needs manual attention.

### Prefer Official / Public Endpoints

Using official APIs is lower-risk than scraping — but ToS still applies to all endpoints; review each platform's developer terms before production use. Migrate these platforms to their official APIs as a priority:

| Platform | Public API | Base URL |
|----------|-----------|----------|
| Greenhouse | Yes | `boards-api.greenhouse.io/v1/boards/{slug}/jobs` |
| Lever | Yes | `api.lever.co/v0/postings/{slug}` |
| Ashby | Yes | `ashbyhq.com/api/non-user-facing/posting-api/{slug}` |
| Workday | No | Scraping required |
| eightfold | No | Scraping required |
| SuccessFactors | No | Scraping required |

Greenhouse + Lever + Ashby together cover ~40% of tech companies.

### ATS Change Detection

Companies switch ATS ~once every 2–5 years. Already largely handled:

- Scan returns 0 jobs for N consecutive runs → auto-flag
- On flag: re-run `detect_ats()` automatically
- Alert admin: company name, current config, detected ATS, careers URL

No significant new work needed.

### The Scraping Risk

Personal use and invite-only: practically low-profile, but legal and terms-of-service status depends on each ATS platform's ToS and applicable law — do not assume it is legally fine without review before scaling beyond personal use.
Commercial scale (1,000+ users): Workday and eightfold actively fight scrapers. Distributed fleet model helps (each user's IP, not yours). Long-term mitigation: official API partnerships or lean on public-API ATS platforms. ToS review and explicit scraping constraints are required before this stage.

---

## Decisions Already Made

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Log parsing fix | C (field-split), skip A | A is throwaway when D lands; C takes 30 min |
| Logging architecture | D (JSON), skip B | B redundant with Sentry; D pays off at multi-server |
| When to do D | Alongside Phase 2 | Multi-server is when JSON logs become essential |
| Multi-user data model | Shared jobs + recruiters, per-user watchlist/status/outreach | Jobs are non-rivalrous; application history is personal |
| Worker deployment | Distributed fleet on users' free tiers | Solves rate limiting, zero infra cost |
| Phase ordering | Invite-only before multi-server | Get users before scaling infra |
| Business model | Not decided | Validate invite-only first |

---

## Network Effect

```text
More users
  → more companies added collaboratively
  → better ATS + recruiter coverage
  → more jobs discovered, fresher
  → more value for all users
  → attracts more users
```

The company + ATS database built collaboratively is the hard-to-replicate moat. Each user contributes companies they're already researching for themselves — the work is distributed across motivated people with aligned incentives.

---

## Realistic Timeline (Part-Time, While Job Hunting)

| Phase | What ships | Realistic timeframe |
|-------|-----------|-------------------|
| 0 | log_monitor fix, pending PR | This week |
| 1 | Invite-only: users, digest, email service, company bootstrap | 1–3 months |
| 2 | Multi-server, JSON logging, distributed fleet | 3–6 months |
| 3 | Web UI, AI relevance filter | 6–12 months |
| Beyond | Decided based on what Phase 1–3 teaches us | Unknown |
