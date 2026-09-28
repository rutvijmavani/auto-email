# Discovery Pipeline Hardening — Design (Gate Fix → Fetch Mechanism → Relay Queue)

**Status:** Designed 2026-09-23, Part 4 added 2026-09-24, Parts 5-6 added 2026-09-27, reviewed for gaps 2026-09-27 (6.2 added; Part 5 problem statement + 5.1 wording corrected), Part 1 sample-recheck + Part 2 CF-Worker-limit decisions locked 2026-09-27, blast-radius + implementation-steps runbook added for all six parts 2026-09-27 — implementation-ready, pending go-ahead
**Implementation order:** Part 1 (confirmation gate) → Part 2 (curl_cffi default) → Part 3 (mobile relay queue). Each part changes what the next part measures, so land and re-measure in this order — see the "Why this order" note at the end of each part. Part 4 (per-origin request metrics) is not a sequential step — its instrumentation is threaded into Parts 2 and 3 as they're built, so the comparison data exists from each tier's first request rather than being bolted on afterward. Part 5 (ATS platform/slug duplication) is unrelated to Parts 1-4's fetch/relay chain — an independent problem in the same doc because it was designed in the same session, not because it depends on or is depended on by the others. It can land in any order relative to them.

This single doc replaces three earlier separate drafts (`public-domain-confirmation-gate.md`, `curl-cffi-impersonation-default.md`, `mobile-relay-queue.md`) — kept as one file because the three changes are tightly coupled (each resizes the population the next one acts on) and splitting them made implementation harder to follow, not easier.

---

## Part 0 — Shared Problem Statement

Production stats that motivate all three parts:

```
public domain     13487/14226 resolved   same_domain 85%  http_redirect 7%  no_signal 5%  certspotter 1%  root_fallback 1%  crtsh 1%
career URL        10685/14226 found      phase6 60%  phase3 33%  phase4 5%  phase7 2%  phase1_kg 0%
```

`same_domain` is 85% of all resolutions (~12,100 companies), but the current logic never checks whether the response that "confirmed" a domain was actually a success — a 403/429/503 is silently accepted as confirmation today. The 3,541 companies with no career URL at all (14,226 − 10,685) are a plausible downstream symptom.

Confirmed this session for BNP Paribas Securities Corp (FEIN `13-3235334`):

| Origin | Result |
|---|---|
| OCI VM, plain HTTP | 403, `Server: AkamaiGHost`, `Set-Cookie: ak_bot=...` |
| OCI VM, `curl_cffi` Chrome-impersonated (TLS/JA3 match) | 403, identical — rules out TLS fingerprint as the signal for this case |
| CF Worker (Cloudflare's own egress) | Worker call succeeds (200), but relayed target status is 403 — Cloudflare's ranges are also flagged |
| Home PC, residential ISP IP | **200**, correct final URL (`https://group.bnpparibas/`) |
| Home PC tethered to phone hotspot (T-Mobile CGNAT, confirmed two different `172.56.x.x` addresses across an airplane-mode toggle) | **200**, identical correct result |

Three independent, separable problems fall out of this evidence:

1. **The gate logic itself is wrong** — non-2xx is being treated as confirmation. Fix regardless of anything else. → Part 1.
2. **Some blocks are TLS-fingerprint-based** (plain `requests`' non-browser fingerprint) — a different, independently fixable mechanism. → Part 2.
3. **Some blocks are IP-reputation-based** (datacenter/cloud ASN blocking, unaffected by fingerprint) — only a non-datacenter egress IP clears these. → Part 3.

---

## Part 1 — Public Domain Confirmation Gate

### Design decision

**Only an actual 2xx response, after following redirects to their final hop, confirms a domain.** Any non-2xx (4xx, 5xx, or a non-HTTP error) is inconclusive — never a confirmation and never a rejection — and must cascade to the next resolution step (root-domain fallback → CT-log lookup), exactly as a genuine redirect-away does today.

This deliberately does **not** try to keep growing a list of known bot-vendor fingerprints to distinguish "blocked" from "confirmed." Checking that list against tens of thousands of companies to find every false positive isn't feasible, and a new vendor not yet in the list would silently reintroduce the same bug. Status-code-only is a structural fix, not a maintenance treadmill.

**Applies to the final response, not each hop** — `_redirect_domain()` already follows redirects before deciding (163-240); the 2xx check evaluates the final response after that following completes. A domain that correctly 301s `bnpparibas.com` → `www.bnpparibas.com` → 200 still confirms normally.

| Function | Current | Fixed |
|---|---|---|
| `_has_web()` (243-264) | `status < 500` → "has a website" | `status < 400` → "has a website" |
| `_redirect_domain()` (163-240) | no redirect + no vendor match → return `""` (confirmed) | final status in 2xx → return `""` (confirmed); otherwise return `None` (inconclusive, cascade) |

### Status-code bucketing

A blind "non-2xx → always fall through to CT-log" treats a 429 (rate-limited) identically to a 403 with no redirect (likely IP-reputation blocked) — wasteful, since a 429 just needs a plain retry later, not CT-log lookup or a relay hop.

**New columns**:
- `fein_domain_map.public_domain_last_status` (nullable `INT`) — records the numeric status code seen on the last inconclusive confirmation attempt, independent of `public_domain_method`. `NULL` when the last attempt was a clean 2xx or a non-HTTP error (no status code).
- `fein_domain_map.public_domain_retry_count` (`INT NOT NULL DEFAULT 0`) — counts consecutive transient (429/503) plain-retries since the last success. Reset to `0` on any 2xx confirmation. Checked by the staleness pass below to decide when to stop plain-retrying and escalate to the relay instead.

- `public_domain_last_status IN (429, 503)` **and** `public_domain_retry_count < PD_RETRY_CAP` → transient, safe to plain-retry on the next staleness pass (see below).
- `public_domain_last_status IN (429, 503)` **and** `public_domain_retry_count >= PD_RETRY_CAP` → plain-retrying isn't clearing it (possibly a WAF returning 503 instead of 403 — see Part 3's trigger note) — escalate to Part 3's relay queue like any other non-transient status.
- `public_domain_last_status = 403` (or any 4xx) with `public_domain_method = 'no_signal'` → strongest candidate for Part 3's relay queue immediately, no plain-retry needed first.

### Staleness pass — plain-retry for transient statuses

Today `scripts/staleness_checker.py` has no pass for `public_domain`; `no_signal` rows sitting on a 429/503 are never revisited unless something unrelated re-triggers enrichment for that `fein` (new petition, LCA re-ingest). That's silent and unbounded — the opposite of what a capped retry should look like.

Add a new pass, same shape as pass 3's ATS redetect (`run_redetect_staleness`, `scripts/staleness_checker.py:247-325`): a periodic SQL scan over `fein_domain_map` for `public_domain_last_status IN (429, 503) AND public_domain_retry_count < PD_RETRY_CAP AND updated_at < NOW() - INTERVAL 'PD_RETRY_INTERVAL_DAYS days'`, pushing matches to the existing enrichment queue with `trigger="pd_retry"` — this reuses `domain_enrichment_worker.py`'s existing pd-resolution step (Step 1, lines 373-393), no new worker needed. That call site (not the staleness pass itself) increments `public_domain_retry_count` on another transient result; once it hits `PD_RETRY_CAP`, the row naturally falls out of this pass's `WHERE` clause and becomes relay-eligible per the bucketing above.

```python
# config.py
PD_RETRY_CAP            = 4     # plain-retries before escalating to relay
PD_RETRY_INTERVAL_DAYS  = 2     # cadence between plain-retries
```

### Expected consequence — read before implementing

This will very likely **shrink** the automatically-confirmed `same_domain` bucket and **grow** the `no_signal`/pending bucket, at least initially. That's the fix working correctly, not a regression — expect `no_signal` to rise after deploy, and expect Part 3's relay queue to have a bigger day-one backlog than "occasional companies."

**Existing rows are not automatically reclassified** — `fein_domain_map` rows already resolved under the old logic keep their stored values; this fix only changes behavior for *new* resolution attempts.

**Locked 2026-09-27**: the sample re-check (200-500 existing `same_domain` rows, re-evaluated against the new gate logic to measure the actual false-confirmation rate) ships **as part of this rollout**, not deferred — it's cheap, needs no new infra, and is the only way to answer whether a full re-scan of the ~12,100 existing `same_domain` rows is warranted. The **full re-scan itself stays a separate, explicit decision**, gated on that sample's result — not bundled into this fix, and not committed to upfront.

### Out of scope (flagged, not solved here)

- **CT-log bare-root-token gap**: `_ct_certspotter`/`_ct_crtsh` (267-388) scope their query to the input domain's own subdomain hierarchy — neither would find a sibling custom-gTLD domain like `group.bnpparibas` even after this fix correctly cascades execution there. Likely a manual-override case (same pattern as the existing shared-FEIN university handling), not general automated custom-gTLD detection.

### Files to change

1. `db/schema.py` — add `public_domain_last_status INT` and `public_domain_retry_count INT NOT NULL DEFAULT 0` to `fein_domain_map`.
2. `jobs/public_domain.py` — `_has_web()`: `status < 500` → `status < 400`. `_redirect_domain()`: final-response 2xx → confirmed (`""`); otherwise `None` + record `public_domain_last_status`.
3. Domain-enrichment-worker call site — thread `public_domain_last_status` through to the write, defaulting to `NULL` on a clean 2xx or non-HTTP failure. This is the single call site every pd attempt passes through regardless of trigger (`enrichment`/`redetect`/`pd_retry`), so it also owns the counter: reset `public_domain_retry_count` to `0` on a 2xx success, increment it on a repeat transient (429/503) result.
4. `scripts/staleness_checker.py` — new pass mirroring `run_redetect_staleness`, pushes transient (429/503) `no_signal` rows under `PD_RETRY_CAP` to enrichment with `trigger="pd_retry"`. Only responsible for the periodic scan/push — does not touch `public_domain_retry_count` itself (see item 3).
5. `config.py` — add `PD_RETRY_CAP`, `PD_RETRY_INTERVAL_DAYS`.
6. Sample re-check script (ships with this rollout — see "Expected consequence" above, locked 2026-09-27) — re-evaluates 200-500 existing `same_domain` rows against the new gate logic to measure the false-confirmation rate; its result gates whether a full re-scan of the ~12,100 rows is separately commissioned.

### Blast radius

- **Touches**: `fein_domain_map` schema (2 new columns, both additive/nullable-or-defaulted), `jobs/public_domain.py`, the domain-enrichment-worker pd call site, `scripts/staleness_checker.py`, `config.py`.
- **Schema change is safe** — purely additive (`NULL`-default and `DEFAULT 0`), no backfill needed, no risk of the ALTER-lock deadlock class already hit once on `fein_domain_map`/`dol_h1b_employers` (see [[project_deploy_pel_incident]]) as long as it goes through the same `_SkipNoopColumnDDL`-guarded `init_db` path already used for prior additive columns on this table.
- **Behavior change, not just data change**: `_has_web()`/`_redirect_domain()`'s stricter 2xx-only gate is live for every pd resolution the moment this code deploys — no feature flag, no gradual rollout. This is intentional (see "Expected consequence") but means `no_signal` growth and Part 3's relay backlog start accumulating immediately, before Part 3 exists to drain them — that backlog just sits harmlessly in `fein_domain_map` until Part 3 lands, nothing depends on it draining right away.
- **No in-flight queue risk** — pd resolution is a synchronous per-company call, not a persistent queue with a payload shape that could go stale across this deploy.
- **Rollback**: reverting the code is safe and lossless (columns stay populated but unused); dropping the columns is only needed for a full undo and is likewise safe since nothing else depends on them yet.
- **Ordering constraint**: schema migration must land before the code that reads/writes the two new columns, or those writes fail outright.

### Implementation steps

1. `db/schema.py` — add `public_domain_last_status INT` and `public_domain_retry_count INT NOT NULL DEFAULT 0` to `fein_domain_map`; run the migration; verify the columns exist on the target DB before deploying code.
2. `config.py` — add `PD_RETRY_CAP = 4`, `PD_RETRY_INTERVAL_DAYS = 2`.
3. `jobs/public_domain.py` — fix `_has_web()`'s threshold (`< 500` → `< 400`) and `_redirect_domain()`'s final-response logic (2xx → confirmed, otherwise `None` + return the status for the caller to record).
4. Domain-enrichment-worker pd call site — thread the returned status into the `fein_domain_map` write; reset `public_domain_retry_count` to `0` on a 2xx, increment it on a repeat 429/503.
5. `scripts/staleness_checker.py` — add the new pd-retry pass (mirroring `run_redetect_staleness`), pushing eligible rows to enrichment with `trigger="pd_retry"`. Confirm it uses `get_logger`/`init_logging` from day one, not `logging.basicConfig()` (per [[feedback_logging_standard]]), and is registered wherever periodic passes are scheduled.
6. Build and run the sample re-check script against 200-500 existing `same_domain` rows; record the false-confirmation rate.
7. **Deploy order**: schema migration → `config.py` → `public_domain.py` → enrichment-worker call site → `staleness_checker.py`, as one deploy (restart the enrichment worker and the staleness-checker process/service afterward) → run the sample re-check separately, any time after.
8. **Verify**: re-run pd resolution for BNP Paribas (`13-3235334`) and confirm it now lands in `no_signal` with `public_domain_last_status = 403`, not `same_domain`; confirm `public_domain_retry_count` increments across two consecutive 429/503 runs and resets on a 2xx; confirm the staleness pass's first run logs a pass (per [[feedback_logging_standard]] — the log monitor only catches what's actually in `logs/`); review the sample re-check's output before deciding anything about the full re-scan.

### Why this order

Must land first: Parts 2 and 3 both act on whatever population `no_signal` contains, and that population is currently undercounted because non-2xx responses are being miscounted as `same_domain`. Measuring Part 2/3's impact before this lands would need to be redone once this changes the denominator.

---

## Part 2 — Chrome-Impersonated Fetching as the Default

### Proposal

`jobs/public_domain.py` and `scripts/discover_h1b_ats.py` currently fetch every candidate through `jobs/http_safe.py::make_safe_session()` — a plain `requests.Session`, with a distinctive non-browser TLS/JA3 fingerprint some bot-management vendors detect independent of IP reputation. This codebase already has a confirmed production case: `docs/ats-fetch-strategy.md` (256-260) — ADP WorkforceNow's Akamai Bot Manager silently returns `count: 0` for plain-`requests` calls, worked around in `jobs/ats/adp.py` via `curl_cffi` Chrome impersonation. `career_detector.py` (37-40, 140) already carries `curl_cffi` as a dependency for the same reason.

Make Chrome-impersonated `curl_cffi` the **default**, not a fallback, for every direct-OCI fetch in the pd/career-URL discovery path — used on the first attempt for every candidate, not only after a failure is observed. Generalizes the existing ADP pattern from one platform to the whole discovery path.

**Session must be injectable, not hardcoded** — this is a hard requirement, not an implementation detail, because Part 3's mobile relay depends on it: the relay worker needs to call these exact same functions with a proxied session, without duplicating any of their step logic. Concretely: `def discover_public_domain(assigned_domain, session=None)`, and internally `session = session or curl_cffi.requests.Session(impersonate="chrome124")` — called normally, behavior is unchanged (builds its own direct session); called from the relay worker with `session=curl_cffi.requests.Session(proxies={"https": f"socks5://{MOBILE_RELAY_PROXY_HOST}:{MOBILE_RELAY_PROXY_PORT}"})`, the identical algorithm runs but every fetch exits through the home-PC tunnel instead of OCI. Checked against current signatures — `discover_careers_url()` (Phase 3, [discover_h1b_ats.py:1270](../scripts/discover_h1b_ats.py:1270)) and `detect_company()` (Phase 7, [career_detector.py:1014](../jobs/ats/career_detector.py:1014)) already take `session=None`; `discover_public_domain()`/`_redirect_domain()`/`_has_web()` ([public_domain.py:391](../jobs/public_domain.py:391)/[:163](../jobs/public_domain.py:163)/[:243](../jobs/public_domain.py:243)) and `detect_via_career_page()` (Phase 6, [career_page.py:131](../jobs/career_page.py:131)) do not yet — those four are the ones this part must add the parameter to.

**Also adds a CF-Worker fallback tier for pd** — today only career-url discovery falls back to `_fetch_via_worker()` when direct-OCI fails; `public_domain.py` has no equivalent (confirmed by grep — no `_fetch_via_worker`/`cf_worker` references in that file). This is a near-free addition: `_fetch_via_worker()` already exists, is already quota-tracked (`CF_WORKER_DAILY_LIMIT`/`get_day_request_count("cf_worker")`), and is already proven in production for career-url — pd just needs to call it. It catches a distinct block class (a site blocking OCI's specific ASN but not Cloudflare's edge ranges), and, like Part 2's curl_cffi change, shrinks the population that ever needs to reach Part 3's relay. It does **not** help against general IP-reputation blocking the way BNP is blocked — Part 0's evidence already shows Cloudflare's own ranges got the same 403 there — so this is a genuine additional chance, not a substitute for the relay. **Cost**: `CF_WORKER_DAILY_LIMIT` is one shared daily budget across all Worker calls; adding pd traffic to it means less headroom left for career-url on the same day unless the limit itself is raised — Part 4's new `pd_cf_worker`/`career_cf_worker` per-phase metrics make this contention visible so the limit can be tuned with real data instead of guessed.

**Locked 2026-09-27**: don't raise `CF_WORKER_DAILY_LIMIT` preemptively. IP-reputation/WAF blocking is almost always evaluated per-domain at the CDN/WAF layer, not per-path — so a company that blocks OCI's IP at the pd-confirmation fetch very likely also blocks it at career-URL probing on the same root domain (confirmed for BNP — the 403 was identical at both). That means pd's new CF-Worker calls mostly land on companies that were already going to need CF-Worker/relay for career-url anyway, not a large new independent population — expected volume is low (~1-3 extra calls/company, skewed toward the low end). The one case this doesn't cover is a company whose `careers_url` sits on different hosting/subdomain than its root domain (independent block posture) — narrower and not assumed away, just not the common case. Ship as designed; only revisit the limit if Part 4's `pd_cf_worker` data shows pd pulling meaningfully more than this estimate.

**Scoping note, confirmed this session**: this does **not** fix the BNP/Akamai IP-reputation case — curl_cffi Chrome impersonation from the OCI VM got the identical 403 plain `requests` did. Fingerprint and IP-reputation blocking are independent mechanisms; this fixes the fingerprint one only. Some companies are blocked by fingerprint only (resolved directly by this fix, no relay needed), by IP reputation only (unaffected, still needs Part 3), by both (partially helped, still needs Part 3), or by neither (unaffected either way).

### Career-URL block-vs-miss tracking + deferring Phase 4 (Brave) past the relay

**Motivation, confirmed this session with BNP Paribas**: today's career-URL cascade runs Phase 3 → Phase 4 (Brave, costs one of the ~950/month quota) → Phase 5 → Phase 6 → Phase 7, all during the direct-OCI/CF-Worker discovery pass, before Part 3's relay is ever considered. For a company that's purely IP-blocked (Phase 3's 19 probed patterns all 403, not 404), Phase 4 burns quota that the relay would very likely have made unnecessary — BNP's correct career page resolved for free once fetched from a non-datacenter IP. Deferring Phase 4 past a relay attempt only helps for the block case, though — for a company that's a genuine miss (Phase 3 patterns all 404, career page just isn't at a guessed path), no IP would make Phase 3 find it, so delaying Phase 4 there would only add latency with no quota saved. The cascade needs to tell these two cases apart before it can defer safely.

**New column**: `fein_domain_map.careers_url_last_status` (nullable `INT`) — Phase 3 probes ~19 URL patterns per company, each can return a different status, so this records one summary signal, not 19: the **most block-like status seen across all probed patterns** on the last attempt, not just the last one tried.
- If **any** pattern returns 403/429/503 → record that status (block-like).
- If **all** patterns return 404 or a connection-level miss with no block signal anywhere → record `404` (genuine miss).
- `NULL` when `careers_url` is already resolved, or Phase 3 hasn't run yet.

**Bucketing / deferral trigger**:
- `careers_url_last_status IN (403, 429, 503)` → block-like → **skip Phase 4 in this pass**; still run Phase 5/6/7 (direct-OCI + CF-Worker, no quota cost) as today, and if the cascade still comes up short, push to Part 3's relay queue per the existing gating (§"What pushes to this queue" below). Phase 4 is deferred, not cancelled — see the relay give-up hook below for where it eventually runs if the relay never resolves it.
- `careers_url_last_status = 404` (or `NULL`, treated the same as a conservative default so an unanticipated case doesn't silently skip Phase 4 forever) → genuine miss, or status not yet known → run Phase 4 immediately, same order as today. No relay is going to fix a path that doesn't exist.

**Closing the loop — where the deferred Phase 4 actually runs**: relay only covers Phase 3/6/7 (Part 3's drain worker explicitly skips Phase 4 — it's a search-API call, not IP-blocked, so relaying it changes nothing). So a company that was deferred here and then exhausts `MOBILE_RELAY_MAX_ATTEMPTS` in the relay queue without resolving `careers_url` would otherwise never get its Phase 4 attempt at all. Fix: the relay drain worker's give-up step (Part 3, step 5) calls Phase 4 once, directly, before the final drop — same worker is already touching that `fein` at that moment, no new queue or scheduling needed. Net effect: Phase 4 always eventually runs for a block-like company, just after the free relay attempt instead of before it; quota is only spent when the relay couldn't resolve it for free.

**Deferred, not built this pass — per-pattern granular tracking**: a natural follow-up is recording which *specific* patterns failed (e.g. "404 on `careers.{domain}`, `{domain}/careers`, ...") rather than just the summary status, to answer "should we add/drop URL patterns" with real cross-company data. That's a materially bigger build — `discover_careers_url()` today returns only the first hit or `None`, so surfacing all ~19 individual outcomes needs a shape change plus a new sink (a side table like `career_url_probe_results(fein, pattern, status_code, checked_at)` is the right shape for a `GROUP BY pattern` analysis, not a JSON blob on the hot-path row). `careers_url_last_status` alone is sufficient for the deferral gate above and is what this pass builds; the per-pattern table is flagged as an explicit future addition, worth building only once someone actually wants to sit down and do the pattern-tuning analysis — same treatment as Part 1's sample re-check follow-up.

### Where this sits in the fetch chain

Replaces the plain-`requests` direct-OCI step, and (for pd) adds the CF-Worker tier that only career-url had before — after this part, both phases share the same shape of chain:

```
career-url today:  plain-requests (direct-OCI) → CF Worker → give up
pd today:          plain-requests (direct-OCI) → give up                    (no CF-Worker tier)

career-url after:  curl_cffi Chrome-impersonated (direct-OCI) → CF Worker → mobile relay queue (Part 3) → give up
pd after:          curl_cffi Chrome-impersonated (direct-OCI) → CF Worker → mobile relay queue (Part 3) → give up
```

### The SSRF guard problem — not a drop-in swap

`make_safe_session()`'s `SSRFAdapter` (`jobs/http_safe.py` 56-131) closes a DNS-rebinding TOCTOU gap by hooking `requests.adapters.HTTPAdapter.send()` — resolve once, validate every returned IP, then rewrite the outbound request to the pre-validated IP (HTTP) or keep the hostname for correct SNI while still pre-validated (HTTPS). This is `requests`/`urllib3`-adapter-specific; `curl_cffi.requests.Session` wraps libcurl directly and has no equivalent mounting hook. Cannot be ported as-is.

**This codebase already made this exact trade-off once**, in `career_detector.py` (562-590): curl_cffi fetches there are guarded by a pre-flight `is_private_host()` check before each request and before following each redirect hop — resolve-then-check, not resolve-and-pin. The existing code comment (line 572) acknowledges the residual gap explicitly as accepted: a DNS server with TTL=0 could return a different IP between the check and the connect.

**Decision**: follow the same precedent for `public_domain.py`/`discover_h1b_ats.py`'s curl_cffi calls — pre-flight `is_private_host()` (already imported/used via `_is_public_url`) before every request and redirect hop, accepting the same documented TOCTOU gap already accepted elsewhere. Not a new or regressed security posture, just extended to two more files.

If curl_cffi/libcurl's `CURLOPT_RESOLVE` (hostname→IP pinning, exposed as a `resolve=` param — needs confirming against the installed version) is usable, that would close the gap the same way `SSRFAdapter` does. Worth a quick spike before implementation, but not a blocker — the pre-flight pattern is already accepted precedent and good enough to proceed with if resolve-pinning doesn't pan out.

### Files to change

1. `jobs/public_domain.py` — add `session=None` to `discover_public_domain()`, `_redirect_domain()`, `_has_web()`; internally default to a new `curl_cffi.requests.Session(impersonate="chrome124")` when `session` isn't passed in, gated by the pre-flight `is_private_host()` pattern from `career_detector.py`. Thread the passed-in/defaulted session through every fetch call inside these three functions (including each redirect hop), replacing the current `make_safe_session()` calls. Also add a CF-Worker fallback step (call `_fetch_via_worker()`, same as `scripts/discover_h1b_ats.py` already does for career-url) when the direct-OCI curl_cffi attempt is inconclusive, before falling through to Part 1's cascade/Part 3's relay queue.
2. `scripts/discover_h1b_ats.py` — same swap in `_fetch_html`/`_probe_career_urls`'s direct-fetch path; `discover_careers_url()` (Phase 3) already accepts `session=None`, just needs its internal default changed from `requests`/`make_safe_session()` to curl_cffi. The existing manual redirect-following loop's per-hop validation stays, only the underlying HTTP client changes. Also: record `careers_url_last_status` (most block-like status across the ~19 probed patterns) at the end of Phase 3, and gate the Phase 4 call on it per the bucketing above (skip when block-like, run immediately when a genuine 404/`NULL`).
3. `jobs/career_page.py` — add `session=None` to `detect_via_career_page()` (Phase 6), same default-curl_cffi-session pattern; not previously in scope for this part, but required so Part 3's relay worker can call it directly.
4. `jobs/ats/career_detector.py` — no signature change needed, `detect_company()` (Phase 7) already accepts `session=None`; just confirm its default-session construction uses the same `impersonate="chrome124")`/pre-flight pattern as the rest (likely already does, since curl_cffi Chrome impersonation originated here).
5. `jobs/http_safe.py` — no change to `make_safe_session()` itself (still used elsewhere for `requests`-based paths); add a new sibling helper (e.g. `make_safe_curl_session()`) instead of modifying it, so other callers are unaffected.
6. Quick spike (not a blocker) — check whether the installed `curl_cffi` version exposes `CURLOPT_RESOLVE` cleanly; if yes, use it instead of the pre-flight-only pattern for a strictly stronger guarantee.
7. `db/schema.py` — add `careers_url_last_status INT` (nullable) to `fein_domain_map`.

### Blast radius

- **Touches**: `jobs/public_domain.py`, `scripts/discover_h1b_ats.py`, `jobs/career_page.py`, `jobs/http_safe.py` (additive sibling helper only), `db/schema.py` (1 new nullable column). `jobs/ats/career_detector.py` is verify-only, no expected change.
- **Highest-risk item: the SSRF guard is not a drop-in.** Every new curl_cffi call site in `public_domain.py`/`discover_h1b_ats.py`/`career_page.py` must carry the pre-flight `is_private_host()` check (before the initial request **and** before following each redirect hop) before it ships — shipping the curl_cffi swap without this reintroduces an SSRF gap `make_safe_session()`'s `SSRFAdapter` currently closes for these two files. This is the one thing in Part 2 that must not be skipped or deferred.
- **Live traffic pattern change, no flag**: every pd/career-url fetch after this deploy goes out Chrome-impersonated instead of plain `requests`, for every company, immediately — not a gradual rollout. Expected to only help or be neutral (curl_cffi impersonation is a superset of legitimate browser behavior), but it's a real change to what OCI's outbound traffic looks like to every target site.
- **Shared quota exposure**: the new pd → CF-Worker fallback tier draws from the same global `CF_WORKER_DAILY_LIMIT` career-url already uses. Per the locked decision above, expected impact is low, but this is the first place that assumption gets tested against real traffic — watch `cf_worker` daily consumption right after deploy.
- **No data/queue risk** — no persistent queue payload shape changes; `careers_url_last_status` is additive and nullable.
- **Rollback**: reverting the curl_cffi swap is a pure code revert (back to `make_safe_session()`), no persisted state depends on which client made a given past request. The new column is safe to leave in place either way.
- **Hard dependency**: Part 3 cannot be implemented before this part lands — the relay drain worker calls these exact functions with an injected session, so the `session=None` parameter must exist on all four functions first.

### Implementation steps

1. `db/schema.py` — add `careers_url_last_status INT` (nullable) to `fein_domain_map`; migrate first.
2. Quick spike — check whether the installed `curl_cffi` version exposes `CURLOPT_RESOLVE` (`resolve=`) cleanly; decide pin-based vs. pre-flight-only validation before writing the session helper in step 3, since that decision shapes its implementation.
3. `jobs/http_safe.py` — add the new sibling helper (e.g. `make_safe_curl_session()`), building a `curl_cffi.requests.Session(impersonate="chrome124")` with whichever guard step 2 settled on. Leave `make_safe_session()` untouched.
4. `jobs/public_domain.py` — add `session=None` to `discover_public_domain()`/`_redirect_domain()`/`_has_web()`, default via the new helper, thread the session through every fetch including redirect hops, add the CF-Worker fallback tier.
5. `scripts/discover_h1b_ats.py` — swap `_fetch_html`/`_probe_career_urls`'s direct-fetch path to the new curl_cffi default; add `careers_url_last_status` recording at the end of Phase 3; gate the Phase 4 call on the bucketing rule.
6. `jobs/career_page.py` — add `session=None` to `detect_via_career_page()`, same default pattern (lands now even though only Part 3 calls it with a non-default session — keeps the four functions consistent in one deploy).
7. `jobs/ats/career_detector.py` — confirm (no code change expected) `detect_company()`'s existing default-session construction already matches the same impersonate/pre-flight pattern.
8. **Deploy order**: schema migration → `http_safe.py` helper → `public_domain.py` → `discover_h1b_ats.py` → `career_page.py`, as one deploy (these four are interdependent on the new session parameter existing) → restart `domain_enrichment_worker` and `discover_h1b_ats_worker`.
9. **Verify**: re-run BNP Paribas through pd and career-url resolution; confirm the curl_cffi path is hit (log line) and the result matches Part 0's evidence (still 403 from OCI — that's expected, Part 2 doesn't fix IP-reputation blocking); confirm the pd CF-Worker fallback fires for a known-blocked case; confirm `careers_url_last_status` populates correctly on the next Phase-3 run for a company whose probes are all 403 vs. all 404.

### Why this order

Lands after Part 1 (so it's measured against the corrected `no_signal` population, not the inflated `same_domain` one) and before Part 3 (it shrinks the population Part 3's relay queue has to carry — fingerprint-blocked companies resolve directly here and never need the relay).

---

## Part 3 — Mobile Relay Queue

### Problem

After Parts 1-2, whatever remains in `no_signal` with a 403-class `public_domain_last_status` is very likely IP-reputation-blocked — confirmed for BNP: only a non-datacenter, non-cloud-Worker IP (residential ISP or mobile-carrier hotspot) clears the block; the existing direct-OCI → CF-Worker fallback has no way around this because both are exactly the class of IP that gets blocked.

This adds a third tier: relay the request through the user's home PC over a private tunnel, **whenever that machine happens to be reachable**, without making the automated pipeline depend on it being reachable at any given moment.

**Scale note**: the real size of this queue is unknown until Part 1 lands (see Part 0/Part 1) and is expected to be **significantly larger than a handful** — the design below (dedicated pool-scaled worker, capped retries) is built assuming meaningful volume, not occasional manual lookups.

### Design goals

- No dependency on the home PC being online — degrades to today's behavior (direct-OCI → CF-Worker → give up) whenever the relay is unavailable, same as CF Worker quota exhaustion today.
- No new always-on infrastructure purchase — reuses hardware already owned.
- Secure by construction — no open proxy exposed to the internet; only a mutually key-authenticated tunnel peer can reach it.
- Reuse existing queue/manager/quota patterns (inflight-ZSET + atomic Lua pop + reclaim-on-startup, same as `discover_h1b_ats_worker.py`; manager-scaled pool via `_run_ats_pool_cycle`, same as `domain_enrichment`/`discovery`/`head_check`; `external_api_health` quota/health tracking) rather than inventing new machinery.
- Bounded retries — a company blocked for a reason unrelated to IP reputation (e.g. genuinely dead site) must not loop in this queue forever.

### Topology

```
                         (public IP, one UDP port open)
   ┌─────────────┐   WireGuard tunnel   ┌──────────────────┐
   │   OCI VM     │◄────────────────────►│   Home PC        │
   │ (WG server)  │   10.10.0.1 ⇄ .2      │  (WG client)     │
   └─────────────┘                       │  local SOCKS5     │
                                          │  proxy :1080      │
                                          └─────────┬────────┘
                                                     │ tethered (optional)
                                                     ▼
                                          ┌──────────────────┐
                                          │  Phone (hotspot)  │
                                          │  mobile-carrier IP│
                                          └──────────────────┘
```

- **OCI VM** is the WireGuard server (already has a public IP; adding an open inbound port is trivial — no NAT-traversal problem on this side).
- **Home PC** is a WireGuard client/peer. WireGuard's keepalive keeps the tunnel usable bidirectionally even though the PC is behind home-router NAT — the PC only needs to have dialed out once; after that the OCI VM can reach `10.10.0.2` any time the tunnel is up.
- The PC runs a small local SOCKS5 proxy (e.g. `microsocks`, or a ~50-line Python `asyncio` proxy) bound to the WireGuard interface only, never `0.0.0.0` — unreachable from anywhere except across the tunnel.
- **Phone is not a WireGuard peer.** A persistent proxy server inside Termux on Android is fragile (battery optimization / background-process killing). The phone just tethers to the PC as today; whichever network the PC is actually using (home WiFi or phone hotspot) is what the relay's outbound requests use.

### Queue

```python
# config.py
MOBILE_RELAY_QUEUE    = "mobile_relay:queue"     # Redis ZSET — companies awaiting a reachable relay
MOBILE_RELAY_INFLIGHT = "mobile_relay:inflight"  # Redis ZSET — popped, currently being processed
MOBILE_RELAY_MAX_ATTEMPTS = 5               # capped retries before giving up permanently
MOBILE_RELAY_PROXY_HOST   = "10.10.0.2"     # WireGuard tunnel IP of the home PC
MOBILE_RELAY_PROXY_PORT   = 1080
MOBILE_RELAY_PROBE_TIMEOUT_S = 2            # cheap reachability check, not a real fetch
```

```python
# workers/worker_control.py
MOBILE_RELAY_WORKERS = [...]   # scaled 0↔1 by manager's _run_ats_pool_cycle, same list-shape as ENRICHMENT_WORKERS/DISCOVERY_WORKERS/HEAD_CHECK_WORKERS
```

Score = enqueue timestamp (FIFO, same convention as `discovery:redetect`). Payload is intentionally minimal — **`{"fein": "13-3235334", "attempts": 0}`**. No snapshot of `assigned_domain`/`website_url`/last-seen status code: the queue item can sit for hours or days before the home PC is online, and other pipeline paths (redetection, manual fixes) may touch the row in the meantime, so the drain step always re-reads current state from `fein_domain_map` by `fein` rather than trusting a stale payload.

**What pushes to this queue**: discovery reads the company's *current* `fein_domain_map` row — `public_domain`/`public_domain_last_status` were already set upstream by enrichment's Step 1 (direct-OCI curl_cffi + CF-Worker, Part 2), discovery does not re-invoke `discover_public_domain()` itself. When that row still shows `public_domain` unresolved (`no_signal`/`ct_quota`, see Part 1) **and/or** `careers_url` unresolved (discovery's own Phase 3/4/6/7 cascade, direct-OCI + CF-Worker, all exhausted with no hit) — push `{"fein", "attempts": 0}` here instead of giving up, **but only if `public_domain_last_status` is not a transient code under retry (429/503 with `public_domain_retry_count < PD_RETRY_CAP`, per Part 1's bucketing)**. 429/503 mean "try again later, same IP is fine" — those stay in Part 1's new pd-staleness pass and never touch the relay *while still under the retry cap*. Once `public_domain_retry_count >= PD_RETRY_CAP`, a persistent 429/503 is no longer trusted as purely transient (it may be a WAF returning 503 instead of 403) and becomes relay-eligible like any other non-transient status. Everything else (403, other 4xx, or no status at all / `no_signal`) is relay-worthy immediately — this is a denylist, not an allowlist, so a status this doc hasn't anticipated still defaults to "send it to the relay" rather than silently never getting retried. Dedup on `fein` (don't re-push if already queued). Strictly additive — a company is never held up waiting on this queue before other pipeline stages proceed.

**Who decides this — enrichment vs. discovery, not the same thing.** `domain_enrichment_worker.py` does a cheap pd + Phase3/Phase6 pass of its own (Steps 1-3, writing directly whatever it finds), but its push to the discovery queue (`_push_to_discovery`, lines 307-316, called unconditionally at line 463-465 whenever `trigger != "on_demand"` and either `trigger == "redetect"` or the petition-count floor is met) does **not** gate on whether pd/`careers_url` are still missing — it fires regardless of what Phase 3/6 found. Enrichment is upstream of and blind to the mobile-relay decision entirely; it never pushes to `mobile_relay` itself.

The relay push belongs to the **discovery** pipeline (`scripts/discover_h1b_ats.py`/`discover_h1b_ats_worker.py` — see Files to change, item 4 below), which is the thing that actually runs the full phase cascade (1/3/4/5/6/7) via direct-OCI + CF-Worker. Only after that full cascade comes up short — and the 429/503 retry-cap gating above says it's not just transient — does discovery push the company to `mobile_relay`. So the chain is: **enrichment** (cheap early pd/Phase3/Phase6 pass, unconditional handoff) → **discovery** (full phase cascade, direct-OCI + CF-Worker) → **mobile_relay** (last resort, pushed by discovery once the cascade is exhausted).

### Drain loop — `mobile_relay` is a pool, not an inline call

`workers/manager.py` runs one always-on `while True` loop on `MANAGER_CYCLE_S = 60`s ([manager.py:1506](../workers/manager.py:1506)/[:74](../workers/manager.py:74)) that makes scaling decisions for every pool — it never does the actual scraping/enrichment/discovery work itself. `domain_enrichment`, `discovery` (h1b_ats), and `head_check` already follow this exact split: manager just computes `combined_depth` from their queue and calls `_run_ats_pool_cycle(...)` ([manager.py:1584-1617](../workers/manager.py:1584)) to scale that pool's worker count via `worker_control.py`'s `*_WORKERS` list; the real work runs in a dedicated long-running worker script (`domain_enrichment_worker.py`, `discover_h1b_ats_worker.py`) with its own loop, its own inflight ZSET, its own pace. `mobile_relay` follows the same split — **not** an inline call inside manager's own cycle, which would block every other pool's scaling decisions for however long a slow relay fetch takes (a single company via WireGuard/SOCKS5 + Phase 7's BFS crawl can easily run well past 60s).

**Manager's cycle** (cheap, same cost class as its other per-pool depth checks):
1. Probe reachability — plain TCP connect to `(MOBILE_RELAY_PROXY_HOST, MOBILE_RELAY_PROXY_PORT)` with `MOBILE_RELAY_PROBE_TIMEOUT_S` timeout.
2. Compute `combined_depth = ZCARD(MOBILE_RELAY_QUEUE) + ZCARD(MOBILE_RELAY_INFLIGHT)`.
3. Call `_run_ats_pool_cycle(r, pool_label="mobile_relay", combined_depth=..., worker_units=MOBILE_RELAY_WORKERS, hb_prefix="mobile_relay_drain_worker")` — scales the pool to 1 worker when reachable *and* `combined_depth > 0`, to 0 otherwise (unreachable, or nothing to do). No alert on scale-to-0 — offline home PC is expected/normal, not a failure.

**`scripts/mobile_relay_drain_worker.py`** (new, structured exactly like `discover_h1b_ats_worker.py`): its own `while True` loop, started/stopped by manager via `worker_control.py`, never called into directly.
1. On startup: `_reclaim_inflight(r, MOBILE_RELAY_INFLIGHT)` — same pattern as [discover_h1b_ats_worker.py:464-484](../workers/discover_h1b_ats_worker.py:464), requeues anything left inflight by a prior crash/SIGKILL (e.g. the PC being shut off mid-company) back onto `MOBILE_RELAY_QUEUE`.
2. Loop: atomic Lua pop from `MOBILE_RELAY_QUEUE` into `MOBILE_RELAY_INFLIGHT` (same `ZPOPMAX`+`ZADD` script as [discover_h1b_ats_worker.py:62-67](../workers/discover_h1b_ats_worker.py:62)), one `fein` at a time (no `MOBILE_RELAY_BATCH_SIZE` — a dedicated worker loop processes serially at whatever pace the relay allows, it doesn't need artificial batching).
3. For that `fein`: read the current row from `fein_domain_map` (`assigned_domain`, `public_domain`, `careers_url`) — always fresh, never trusts stale queue payload.
   - **No separate "quick check" step and no separate "from scratch" step** — reuses the exact same functions the direct-OCI/CF path already calls, just handed a relay-backed session (curl_cffi with `proxies={"https": f"socks5://{MOBILE_RELAY_PROXY_HOST}:{MOBILE_RELAY_PROXY_PORT}"}`) instead of the direct/CF-Worker session. Those functions already implement "try the known/cheap candidate first, escalate to a fuller search" internally, so running them via relay reproduces exactly that behavior from the home IP:
     - **If `public_domain` still missing**: call `discover_public_domain(assigned_domain, session=relay_session)` (Steps 1 direct → 2 root-fallback → 3 CT-log, unchanged — see Part 1/2, which already require this function to take a pluggable session).
     - **If `careers_url` still missing**: call the career discovery chain via relay, but **only Phase 3, Phase 6, and Phase 7** — the direct-probe (`discover_careers_url`), the 3-layer scan (`detect_via_career_page`), and the Chrome-impersonation BFS (`career_detector.detect_company`). **Phase 4 (Brave search) is skipped** — it's a search-API call, not a fetch of the target site itself, so it isn't IP-blocked and re-running it via relay changes nothing.
4. On success for either: write `public_domain`/`careers_url` the same way the direct-OCI path does today; if both are now resolved, `zrem` from `MOBILE_RELAY_INFLIGHT` and stop.
5. On failure (still unresolved after relay attempt): increment `attempts`; drop permanently at `MOBILE_RELAY_MAX_ATTEMPTS` (genuinely dead site or non-IP block, e.g. CAPTCHA/JS challenge) — otherwise re-`zadd` onto `MOBILE_RELAY_QUEUE` with a fresh timestamp. Either way, `zrem` from `MOBILE_RELAY_INFLIGHT`. **Before the permanent drop specifically** (not on each intermediate retry): if `careers_url` is still unresolved and Part 2's Phase 4 deferral skipped it earlier (`careers_url_last_status` was block-like when discovery last ran), call Phase 4 (Brave) once, directly, right here — relay never attempts Phase 4 itself (step 3 above), so this is the only place a deferred company still gets its Brave attempt before being written off. Quota is spent only at this last-resort point, not upfront.
6. If the worker (or the PC) dies mid-`fein`: nothing removes it from `MOBILE_RELAY_INFLIGHT`, so the next time the worker starts (manager scales it back to 1 once reachable again) step 1's reclaim picks it up and requeues it — full re-run from scratch, no partial-progress checkpointing, same as today's discovery worker crash recovery.

This is why Part 3 now explicitly depends on Part 2's session-plumbing refactor: `discover_public_domain()` and the Phase 3/6/7 functions need to accept an injectable session/proxy rather than hardcoding `make_safe_session()`/direct curl_cffi internally, so the relay drain worker can call the *identical* functions instead of re-implementing their step logic.

### Security

- WireGuard peers authenticated by public key only — no open proxy ever exposed to the public internet.
- Local SOCKS5 proxy binds only to the WireGuard interface, never `0.0.0.0` — unreachable except across the authenticated tunnel, even from the home LAN.
- No credentials/secrets travel this path — plain HTTP(S) GETs to public career pages and public-domain candidates, same requests the direct-OCI path already makes.

### Health / observability

Reuses `external_api_health` (service=`"mobile_relay"`), same pattern as `certspotter`/`crtsh`/`brave`/`cf_worker`:

- `record_external_request("mobile_relay", status_code, response_ms)` on every attempted fetch.
- Queue depth (`ZCARD MOBILE_RELAY_QUEUE`) surfaced the same way `discovery:redetect` depth already is.
- No alert purely from "relay unreachable" (expected steady state). An alert only if queue depth grows unboundedly over a long window — left as a future addition, not built this pass.

### IP rotation — deferred

Investigated and explicitly deferred, not forgotten:

- **Phone-hotspot manual toggle** (airplane mode → new T-Mobile CGNAT IP, confirmed working this session) — rejected as a pipeline mechanism; requires a human, not automatable for an unattended drain loop.
- **Programmatic toggle via ADB** — Android-only; user has an iPhone, and iOS exposes no API (not even Shortcuts) to toggle cellular/airplane-mode state. Jailbreaking rejected as disproportionate.
- **Dedicated LTE/5G USB modem or MiFi with a scriptable reconnect API** — the actually-automatable option, structurally identical to commercial mobile-proxy services (bank of modems, force-reconnect between uses). **Not built in this first pass** — user chose to ship the single-IP home-PC relay first and accept that some companies (Akamai-blocked from every available IP, or blocked via cross-customer reputation scoring) simply won't resolve for now, rather than delay on rotation infrastructure.

**Revisit once Part 1 gives a real population count.** If the true `no_signal`/403 population is in the thousands, a single unrotated home IP will itself accumulate request volume against Akamai's cross-customer Client Reputation scoring and may see increased friction on other unrelated Akamai-protected sites over time. Accepted for now per the user's explicit call to keep this simple; re-evaluate once real queue depth is known.

### What this does not solve

- Genuinely dead sites, redesigned career-page paths, or CAPTCHA/JS-sensor challenges that require actually executing a challenge (not just presenting a "better" IP) — `MOBILE_RELAY_MAX_ATTEMPTS` exists specifically to stop retrying those forever.
- Guaranteed availability — explicitly best-effort, gated on the user's own PC/phone being on and connected. Pipeline correctness never depends on it.
- Running a proxy service directly on the phone — deliberately avoided due to Android background-process reliability.

### Files to change

1. `config.py` — add `MOBILE_RELAY_QUEUE`, `MOBILE_RELAY_INFLIGHT`, `MOBILE_RELAY_MAX_ATTEMPTS`, `MOBILE_RELAY_PROXY_HOST`, `MOBILE_RELAY_PROXY_PORT`, `MOBILE_RELAY_PROBE_TIMEOUT_S`.
2. WireGuard setup (infra, not application code) — server config on OCI VM, client config on home PC, key pairs exchanged out of band (not committed to the repo).
3. Home PC — local SOCKS5 proxy service (new small script, e.g. `scripts/local/mobile_relay_proxy.py`, or `microsocks` + a tiny launcher), bound to the WireGuard interface only.
4. `scripts/discover_h1b_ats.py` — after its own Phase 3/4/6/7 cascade is exhausted, check the current `fein_domain_map` row (not a fresh `discover_public_domain()` call — that's enrichment's job, see "Who decides" above) and push `{"fein", "attempts": 0}` to `MOBILE_RELAY_QUEUE` (dedup on `fein`) when `public_domain` and/or `careers_url` are still unresolved, instead of silently giving up.
5. `db/external_api_health.py` — no schema change needed (`service` is already free-text); just start calling `record_external_request("mobile_relay", ...)` from the new drain worker.
6. `workers/worker_control.py` — add `MOBILE_RELAY_WORKERS` list, same shape as `ENRICHMENT_WORKERS`/`DISCOVERY_WORKERS`/`HEAD_CHECK_WORKERS`.
7. `workers/manager.py` — add the reachability probe + `combined_depth` computation, then one more `_run_ats_pool_cycle(pool_label="mobile_relay", ...)` call alongside the existing `head_check`/`domain_enrichment`/`discovery` calls ([manager.py:1584-1617](../workers/manager.py:1584)). Manager itself does no relay fetching.
8. New `scripts/mobile_relay_drain_worker.py` — dedicated long-running worker, structured like `discover_h1b_ats_worker.py` (own loop, own `_reclaim_inflight`, atomic Lua pop-to-inflight, one `fein` at a time): implements the fetch-via-proxy + write-result + requeue/drop/reclaim logic above, including the deferred-Phase-4 call on final give-up (see Part 2's deferral design and step 5 above).
9. `install-systemd.sh` — register the new `mobile_relay_drain_worker` unit (see [[reference_systemd_deploy]] for the 3-place pattern).

### Blast radius

- **Touches**: `config.py`, real infra (OCI VM firewall/security-list — one inbound UDP port opened; home PC — new always-running local proxy process), `scripts/discover_h1b_ats.py` (push logic), `workers/worker_control.py`, **`workers/manager.py`** (the single always-on scaling loop shared by every pool), new `scripts/mobile_relay_drain_worker.py`, `install-systemd.sh`.
- **Highest blast-radius item: the `manager.py` change.** Manager runs one `while True` loop that scales `domain_enrichment`/`discovery`/`head_check` in the same cycle. Adding the `mobile_relay` reachability probe + `_run_ats_pool_cycle` call into that same cycle means an unhandled exception in the new code (e.g. a bad TCP-connect call, a Redis hiccup on the new ZSETs) could, if not isolated, abort the cycle before it reaches the other pools' scaling decisions — a mobile_relay bug taking down `domain_enrichment`/`discovery`/`head_check` scaling would be a serious regression for something explicitly designed to be best-effort. **Hardening addition, not yet in the doc's design**: wrap the new probe+scale block in its own try/except so a failure there logs and no-ops rather than propagating into the shared cycle.
- **Real infra/security change**: opening one inbound UDP port on the OCI VM's firewall/security list is a genuine (if narrow) attack-surface change — mitigated by WireGuard's public-key auth (no anonymous access), but worth confirming the security-list rule is scoped to that port only, not broadened.
- **New always-on process on the user's personal machine** — the local SOCKS5 proxy. Bound to the WireGuard interface only, per the design; worth a manual check post-setup that it's genuinely unreachable from the home LAN and the public internet, not just "should be" by construction.
- **No data/credential risk** — plain HTTP(S) GETs to public pages only, per the doc's own Security section.
- **Rollback**: trivial and safe — scale `MOBILE_RELAY_WORKERS` to 0 and/or stop the systemd unit; nothing else in the pipeline depends on this pool existing (strictly additive, per the design goals). Tearing down the WireGuard tunnel is likewise safe; the reachability probe already degrades cleanly to "unreachable."
- **Hard dependency**: Part 2 must be fully deployed and verified first — every function this part calls (`discover_public_domain()`, `discover_careers_url()`, `detect_via_career_page()`, `detect_company()`) must already accept `session=`, or there's nothing for the drain worker to call.

### Implementation steps

1. Confirm Part 2 is deployed and verified (hard dependency — do not start this part otherwise).
2. WireGuard setup (infra) — OCI VM as server (open the one UDP port in its security list), home PC as client, keys exchanged out of band (never committed to the repo). Verify the tunnel is up (e.g. `ping 10.10.0.2` from the OCI VM) before writing any application code against it.
3. Home PC — stand up the local SOCKS5 proxy bound to the WireGuard interface only; verify unreachable from both the public internet and the home LAN, not just from across the tunnel.
4. `config.py` — add the six `MOBILE_RELAY_*` constants.
5. `workers/worker_control.py` — add `MOBILE_RELAY_WORKERS`.
6. New `scripts/mobile_relay_drain_worker.py` — implement reclaim-on-startup, atomic pop, relay-session fetch (pd and/or careers_url per the current `fein_domain_map` row), write-result, requeue/drop/reclaim, and the deferred-Phase-4 give-up call.
7. `workers/manager.py` — add the reachability probe, `combined_depth` computation, and `_run_ats_pool_cycle` call, **wrapped in its own try/except** (see Blast radius above — this isolation is an addition on top of the original design, add it here). Deploy this only once step 6's worker script already exists, since manager will immediately start trying to scale it.
8. `scripts/discover_h1b_ats.py` — add the post-cascade push to `MOBILE_RELAY_QUEUE`, including the 429/503 retry-cap gating.
9. `install-systemd.sh` — register the `mobile_relay_drain_worker` unit (3-place pattern, [[reference_systemd_deploy]]); manually install the unit file on the VM since it's being added after initial setup.
10. **Deploy order**: WireGuard + local proxy (steps 2-3, verifiable independently of any app code) → `config.py`/`worker_control.py` (steps 4-5) → drain-worker script (step 6) → `manager.py` (step 7) → `discover_h1b_ats.py` push logic (step 8) → systemd registration (step 9).
11. **Verify**: force a known IP-blocked reproducer (BNP Paribas) through the full cascade and confirm it lands in `MOBILE_RELAY_QUEUE`; confirm manager scales the worker to 1 once the tunnel is reachable and back to 0 when it isn't; confirm the drain worker resolves it via relay, writes the result, and removes it from the queue; confirm an `external_api_health` row appears for `"mobile_relay"`; watch manager's logs across several cycles to confirm the other pools (`domain_enrichment`/`discovery`/`head_check`) keep scaling normally throughout, with no exception surfacing from the new block.

### Why this order

Lands last because Parts 1 and 2 both shrink the population this queue has to carry — building this first would mean sizing and testing it against an inflated, partly-wrong `no_signal` count.

---

## Part 4 — Per-Phase, Per-Origin Request Metrics

### Problem

Parts 2 and 3 add two new fetch origins (curl_cffi direct-OCI, home-PC relay) alongside the existing CF Worker tier, for two separate phases (public-domain confirmation, career-URL discovery). Once all three origins exist, there's no way to answer "is the relay actually worth running?" or "is curl_cffi resolving pd better than career-url, or about the same?" without breaking down request outcomes by **both** origin and phase — a single aggregate `no_signal` count can't show that.

### Design decision

Reuse `external_api_health` rather than build a new table — it already has the date+service grain, the UNIQUE/upsert mechanics, and the write/query functions (`record_external_request`, `get_service_stats`, `get_external_health_summary`, `get_day_request_count`) this needs. Two additions:

**1. New `service` values** — one per (phase × origin) combination, so existing per-service queries and the alphabetical grouping in `get_external_health_summary()` split the report into comparable rows with no query changes:

- `pd_oci`, `pd_cf_worker`, `pd_relay`
- `career_oci`, `career_cf_worker`, `career_relay`

**2. Two new columns**, matching `api_health`'s fuller error-subtype breakdown (the pattern you pointed at) rather than `external_api_health`'s current folded-into-`other_err` approach — the whole point here is comparing failure *modes* across origins, so timeout vs. connection-refused vs. "some other exception" needs to stay distinguishable per origin:

```sql
ALTER TABLE external_api_health ADD COLUMN IF NOT EXISTS requests_timeout  INTEGER DEFAULT 0;
ALTER TABLE external_api_health ADD COLUMN IF NOT EXISTS requests_conn_err INTEGER DEFAULT 0;
```

Existing services (`certspotter`/`crtsh`/`brave`/`kg`/`cf_worker`) keep writing 0 into both — no behavior change for them, this only activates for the six new service values.

### Status-code buckets — starting point, expected to be tuned

Reuses `external_api_health`'s existing bucket set (`ok`/`429`/`403`/`404`/`5xx`/`other_err`) plus the two new sub-type columns above. For pd/career-url probing specifically:

- `403` is the single most load-bearing bucket — it's the Akamai/bot-block signal Parts 1-3 are all built around.
- `404` is expected to be **routinely high and not an error** for `career_oci`/`career_relay` — `_CAREER_PATHS` deliberately probes many guessed paths per company, most of which don't exist. Worth remembering when reading the report so a high 404 rate on career-phase rows isn't mistaken for a problem the way it would be for `pd_*` rows (a domain either resolves or it doesn't, no guessing fan-out).
- `429`/`5xx`/`timeout`/`conn_err` matter most for judging whether the relay (`*_relay`) is worth its complexity — if `pd_relay`/`career_relay` show mostly `ok` where `*_oci` showed mostly `403`, that's the queue earning its keep; if `*_relay` shows a lot of `timeout`/`conn_err` instead, that's the home connection itself being the bottleneck, not IP reputation.
- As you said, add/drop buckets as real traffic shows what's actually being seen — this starting set is not meant to be final.

### The CF-Worker double-write

`_fetch_via_worker()`'s two existing call sites (`scripts/discover_h1b_ats.py`, `jobs/ats/career_detector.py`) already call `record_external_request("cf_worker", ...)`, and `get_day_request_count("cf_worker")` gates `config.CF_WORKER_DAILY_LIMIT` — a **global** daily quota across all Worker usage, not per-phase. That existing call/label must not change or be renamed, or the quota gate breaks.

Instead, each CF-Worker call site adds a **second**, additional `record_external_request()` call under the new phase-specific label (`pd_cf_worker` or `career_cf_worker`) — purely for this report, never read by any quota gate. Two writes per Worker call, same pattern `external_api_health` already tolerates (nothing in the schema or write path assumes one row per logical request).

**No longer a gap**: earlier drafts of this doc flagged that `public_domain.py` had no CF-Worker fallback (so `pd_cf_worker` would sit at 0) — Part 2 now adds that tier explicitly (see Part 2's "Also adds a CF-Worker fallback tier for pd"), so `pd_cf_worker` gets real data from Part 2's first deploy onward, same as `career_cf_worker` does today.

### Where each counter gets written (threaded into Parts 2/3, not a separate step)

- `pd_oci` / `career_oci` — inside Part 2's new curl_cffi call sites (`jobs/public_domain.py`, `scripts/discover_h1b_ats.py`) — greenfield, direct-OCI fetches aren't recorded in `external_api_health` at all today.
- `pd_cf_worker` / `career_cf_worker` — the double-write addition above, at the existing `_fetch_via_worker()` call sites.
- `pd_relay` / `career_relay` — inside Part 3's drain-loop fetch, alongside the `"mobile_relay"` recording Part 3 already specifies (also two writes: keep `"mobile_relay"` for the existing relay-wide health/queue-depth view, add the phase-specific label for this comparison).

### Files to change

1. `db/schema.py` — add `requests_timeout`/`requests_conn_err` columns to `external_api_health` (via the `ALTER TABLE ... ADD COLUMN IF NOT EXISTS` pattern already used elsewhere in this file, e.g. the existing `api_health` error-subtype migration at line ~1125).
2. `db/external_api_health.py` — extend `record_external_request()` with an optional `error_kind` param (`"timeout"` | `"conn_err"` | `None`), consulted only when `status_code == 0`, routing to the new columns instead of always falling into `requests_other_err`. Backward compatible — existing callers that don't pass it keep today's behavior exactly.
3. `jobs/public_domain.py` / `scripts/discover_h1b_ats.py` — call `record_external_request(...)` with the new `pd_oci`/`career_oci` labels at Part 2's curl_cffi call sites, catching `Timeout`/`ConnectionError` distinctly to populate `error_kind`.
4. Both `_fetch_via_worker()` call sites — add the second, phase-specific `record_external_request()` write described above.
5. Part 3's relay drain function — add the `pd_relay`/`career_relay` write alongside the existing `"mobile_relay"` write already specified in Part 3.

### Blast radius

- **Touches**: `db/schema.py` (additive columns only), `db/external_api_health.py` (`record_external_request()` signature grows an optional param), and the call sites threaded into Parts 2/3's own new code — no independent call sites of its own outside those two.
- **Schema change is additive and safe** — two `ADD COLUMN IF NOT EXISTS ... DEFAULT 0` statements, same pattern already used for `api_health`'s error-subtype columns. No backfill needed (defaults to 0, matches "no data yet" semantics exactly).
- **`record_external_request()` signature change is backward compatible** — new `error_kind` param is optional and defaults to not touching the new columns; every existing caller (`certspotter`/`crtsh`/`brave`/`kg`/`cf_worker` call sites) is untouched and keeps writing 0 into both new columns, exactly as today.
- **No behavior change for existing services** — this part only ever *adds* rows/columns; it never changes what a `403`/`429`/`5xx` bucket means for `certspotter`/`crtsh`/`brave`/`kg`/`cf_worker`, and the `cf_worker` quota gate (`get_day_request_count("cf_worker")`) keeps reading the exact same label it does today (the double-write adds a second, separately-labeled row — see "Files to change" item 4 — it doesn't touch the existing one).
- **No standalone rollback needed** — since this part has no independent deploy step (its writes are threaded into Part 2's and Part 3's call sites), rolling it back means reverting the schema migration (safe, columns just go unused) and removing the extra `record_external_request()` calls from Parts 2/3's code, not a separate action.
- **Reporting-only risk surface**: worst case here is a wrong bucket count or a missing row — never a crash of the calling code, since `record_external_request()` failures should stay swallowed the same way the existing calls already are (confirm this defensive pattern holds for the new call sites too, not just the old ones).

### Implementation steps

Note: this part has no standalone deploy — its pieces land embedded inside Part 2's and Part 3's own implementation sequences. Listed here as one coherent checklist for tracking, in the order they actually get touched:

1. `db/schema.py` — add the `requests_timeout`/`requests_conn_err` columns (do this first, before any code that would write to them lands).
2. `db/external_api_health.py` — extend `record_external_request()` with the optional `error_kind` param; unit-test that omitting it reproduces today's exact behavior (regression guard for the five existing services).
3. During Part 2's implementation: add the `pd_oci`/`career_oci` writes at the new curl_cffi call sites, and the second phase-specific write at both existing `_fetch_via_worker()` call sites (`pd_cf_worker`/`career_cf_worker`) — do not touch or rename the existing unlabeled `"cf_worker"` write the quota gate depends on.
4. During Part 3's implementation: add the `pd_relay`/`career_relay` write alongside the drain worker's existing `"mobile_relay"` write.
5. **Verify**: after Part 2 deploys, confirm `pd_oci`/`career_oci`/`pd_cf_worker`/`career_cf_worker` rows start appearing in `external_api_health` with sensible bucket counts; after Part 3 deploys, confirm `pd_relay`/`career_relay` rows appear too; spot-check that `cf_worker`'s own row/count is unaffected (same numbers it would have shown pre-Part-4, modulo real traffic growth) — this is the regression check that the double-write didn't fork the quota gate's input.

---

## Part 5 — ATS Platform/Slug Duplication Hardening

### Status

Designed 2026-09-27 — pending implementation. Unrelated to Parts 1-4 (see note at top of doc); grouped here because it came out of the same design session, not because it's coupled to the fetch/relay chain.

### Problem statement

`company_ats` and `h1b_ats_discovery` both store `platform`/`slug` (as `platform`/`slug` and `detected_platform`/`detected_slug` respectively) for the same employer, written independently by two different workers:

- `workers/domain_enrichment_worker.py` — `_write_ats()` ([domain_enrichment_worker.py:290](../workers/domain_enrichment_worker.py:290)) writes only `company_ats`, from Phase 3/Phase 6 results (call sites at [:437](../workers/domain_enrichment_worker.py:437) and [:444](../workers/domain_enrichment_worker.py:444)).
- `scripts/discover_h1b_ats.py` — `process_employer()` writes **both**: `upsert_discovery()` ([:1982](../scripts/discover_h1b_ats.py:1982)) into `h1b_ats_discovery.detected_platform`/`detected_slug`, and `_upsert_company_ats()` ([:2004](../scripts/discover_h1b_ats.py:2004)) into `company_ats`.
- `scripts/discover_h1b_ats.py` — a **third**, independent path: `_run_brave_pass()` ([:2097-2156](../scripts/discover_h1b_ats.py:2097)), the separate `--brave-pass` monthly sweep, also writes both tables (`_brave_upsert()` into `h1b_ats_discovery`, then `_upsert_company_ats()` into `company_ats`) on its own candidate list, entirely outside `process_employer()`. Noted for completeness — it doesn't re-run Phase 6/7 so it isn't part of the redundant-work problem below, but any future full consolidation needs to account for it as a third writer, not two.

Concrete consequences, confirmed this session:

1. **Redundant work** — when `discover_h1b_ats_worker` runs the normal first-pass path (`trigger ∈ {enrichment, staleness}`), it's reached specifically *because* `already_has_ats` (a `h1b_ats_discovery`-only read) was false — but enrichment may have already found and written the platform/slug into `company_ats`, which discovery never checks. `process_employer()`'s `known_careers_url` branch ([:1819-1833](../scripts/discover_h1b_ats.py:1819)) only recovers a platform via `match_ats_pattern(url)` against the known careers URL string itself; it does not look at `company_ats`. If that pattern-match misses (e.g. enrichment found it via Phase 6's embedded-HTML fingerprint, not a URL-pattern match), Phase 6 ([:1887](../scripts/discover_h1b_ats.py:1887)) and Phase 7 ([:1909](../scripts/discover_h1b_ats.py:1909)) both re-run from scratch even though the answer already exists in `company_ats`.
2. **Three independent `is_monitored` flags for what should be one concept** — `prospective_companies.is_monitored` (default `TRUE`, read by the job monitor), `company_ats.is_monitored` (default `FALSE`, read by the job monitor), and `h1b_ats_discovery.is_monitored` (cosmetic — the job monitor never reads it; confirmed against `db/job_monitor.py:278-417`, which is a `UNION ALL` of only `prospective_companies` and `company_ats`, nothing else). The frontend's "Add to monitoring" button ([frontend/pages/3_Discover.py:1152](../frontend/pages/3_Discover.py:1152)) only ever sets the first and third of these; it never touches `company_ats.is_monitored`, even when a matching `company_ats` row exists for the same domain+platform.

Not harmful today — the job monitor's actual scan-eligibility query (`get_monitorable_companies()`) and its de-dup `NOT EXISTS` clause already prevent double-monitoring the same company through both tables at once. This is a "concerning, worth fixing" duplication (extra debugging surface, wasted Phase 6/7 work), not a correctness bug in production right now.

### Decision: not a full consolidation (yet)

Collapsing `company_ats`/`h1b_ats_discovery` (or the three monitoring flags) into one source of truth is the real fix, but it touches the Discover/Companies frontend pages, `already_has_ats`, both upsert functions, and the job monitor's UNION query — too large to bundle with the smaller items below. **Explicitly deferred**, tracked here as future work; the three items below are cheap interim mitigations that reduce the pain without that larger migration.

### 5.1 — Discovery checks `company_ats` before Phase 6/7

**Design decision**: before reaching Phase 6 ([:1887](../scripts/discover_h1b_ats.py:1887)), if `detected_platform` is still unset and `website_url` is known, look up `company_ats` by root domain. If a row exists with a non-`unknown`/`unsupported` platform and a non-empty slug, use it directly (`ats_source = "company_ats_cache"`) and skip Phase 6/7 entirely for this pass — the answer is already on record, re-probing is guaranteed to reproduce it (or, worse, a stale/partial re-detection could disagree with the reviewed row).

Ordering: this check must run *after* the `known_careers_url`/`jobs_url`/`website_url` branches (they're free/cheap and may already have found it via URL-pattern match) and *before* the Phase 6 gate, so it only fires when everything cheaper has already failed to find a platform.

Row selection when a domain has multiple `company_ats` rows (different platforms tried over time): prefer `is_monitored = TRUE`, then most-recently-reviewed, then highest `priority` — consistent with how `_upsert_company_ats()` already treats `is_monitored`/`reviewed_at`/`priority` individually elsewhere in that function (an `is_monitored=TRUE` skip-check, a `reviewed_at IS NOT NULL` slug-overwrite guard, `GREATEST()` on priority at conflict — not a single combined ordering anywhere today, but the same signals).

### 5.2 — Enrichment mirrors ATS writes into `h1b_ats_discovery`

**Design decision**: `_write_ats()` ([domain_enrichment_worker.py:290](../workers/domain_enrichment_worker.py:290)) gets a sibling mirror-write into `h1b_ats_discovery.detected_platform`/`detected_slug`, called right alongside each existing `_write_ats()` call (Phase 6 at [:437](../workers/domain_enrichment_worker.py:437), Phase 3 at [:444](../workers/domain_enrichment_worker.py:444)) — same shape as `upsert_discovery()`'s own platform-change guard in `scripts/discover_h1b_ats.py` ([:1487-1492](../scripts/discover_h1b_ats.py:1487)): a slug never survives under a different platform than the one it was detected with.

`h1b_ats_discovery.employer_name` is `NOT NULL`, everything else the mirror needs (`employer_fein`, `employer_name`) is already in scope in `_process_company()`. `INSERT ... ON CONFLICT (employer_fein) DO UPDATE` — creates the row if `discover_h1b_ats_worker` hasn't reached this `fein` yet, updates it if it has. This closes the gap 5.1 depends on: once both workers write both tables, discovery's own future runs (and any other reader) see the same platform/slug regardless of which worker found it first.

### 5.3 — "Add to monitoring" toggle syncs `company_ats.is_monitored` too

**Design decision**: the Discover-page "Add to monitoring" button ([frontend/pages/3_Discover.py:1152](../frontend/pages/3_Discover.py:1152)), alongside its existing `UPDATE h1b_ats_discovery SET is_monitored = TRUE` ([:1178](../frontend/pages/3_Discover.py:1178)), adds:

```sql
UPDATE company_ats
SET is_monitored = TRUE, reviewed_at = NOW()
WHERE domain = %s AND platform = %s
```

using the same `domain`/`platform` already in scope on that button (`_norm_domain(website)`, `platform` from the loaded `disc` row) — the same `(domain, platform)` pair `company_ats`'s own `UNIQUE` constraint and `_upsert_company_ats()` key off, not `employer_fein` (not reliably populated on every `company_ats` row).

`reviewed_at = NOW()` is intentional, not incidental: clicking this button is the user looking at a detected platform/slug and explicitly confirming it — the exact same confirmation the standalone `company_ats` review-panel toggle ([:1384](../frontend/pages/3_Discover.py:1384)) records. Leaving `reviewed_at` untouched would make the row look like it still needs manual review when a human just reviewed it right here.

**No matching row is expected and fine** — a company_ats row only exists if enrichment (Part 5.2) or discovery's own `_upsert_company_ats()` already wrote one for this domain+platform. If discovery found the platform via Phase 4/Brave before Part 5.2 lands, or before enrichment ever ran, there's nothing to update; the `UPDATE` affects 0 rows and `h1b_ats_discovery` stays the authoritative record for that path, same as today. Confirmed as an acceptable no-op, not an error to guard against.

**Not changed**: the other two "Add to monitoring" variants on the same page (the no-ATS paste-URL flow, [:1209](../frontend/pages/3_Discover.py:1209); the wrong-ATS override, [:1269](../frontend/pages/3_Discover.py:1269)) don't have a resolved `platform`/`slug` at click time in the same way — no `company_ats` row identity to target yet. Out of scope for this item; revisit if those flows ever gain a resolved platform before the pipeline runs.

### Files to change

1. `scripts/discover_h1b_ats.py` — new lookup helper (e.g. `_lookup_company_ats(conn, domain) -> dict | None`), called in `process_employer()` right before the Phase 6 gate ([:1887](../scripts/discover_h1b_ats.py:1887)); sets `detected_platform`/`detected_slug`/`ats_source="company_ats_cache"` on a hit, which naturally short-circuits both the Phase 6 and Phase 7 `if not detected_platform` gates.
2. `workers/domain_enrichment_worker.py` — new `_write_discovery_ats(conn, fein, employer_name, platform, slug, source)` mirroring into `h1b_ats_discovery`; called alongside both existing `_write_ats()` call sites ([:437](../workers/domain_enrichment_worker.py:437), [:444](../workers/domain_enrichment_worker.py:444)).
3. `frontend/pages/3_Discover.py` — add the `company_ats` update next to the existing `h1b_ats_discovery` update in the "Add to monitoring" button handler ([:1174-1184](../frontend/pages/3_Discover.py:1174)).

### Blast radius

- **Touches**: `scripts/discover_h1b_ats.py` (`process_employer()` — a new read before the Phase 6 gate), `workers/domain_enrichment_worker.py` (`_write_ats()` call sites — a new write alongside each), `frontend/pages/3_Discover.py` (one button handler — a new write).
- **5.1 is a pure read-and-short-circuit** — it can only ever cause `process_employer()` to skip Phase 6/7 *more* than it does today, never run something new. Worst case if the lookup logic has a bug: it either fails to find a real `company_ats` hit (falls through to Phase 6/7 exactly as today — a missed optimization, not a correctness regression) or wrongly short-circuits on a bad match (sets a wrong platform/slug from a stale/incorrect `company_ats` row). The second failure mode is the one worth guarding in testing — verify the row-selection tie-break (`is_monitored` → `reviewed_at` → `priority`) picks the *reviewed* row when a domain has multiple conflicting `company_ats` entries, not just the most recent one.
- **5.2 writes into `h1b_ats_discovery` from a worker that has never written there before** — `domain_enrichment_worker.py` currently only touches `company_ats`. This is the highest-risk item in Part 5: an `INSERT ... ON CONFLICT (employer_fein) DO UPDATE` from a second, independently-scheduled worker means two processes can now write the same `h1b_ats_discovery` row. Confirm the platform-change guard (mirroring `upsert_discovery()`'s own `[:1487-1492]` logic — never let a slug survive under a different platform than the one it was detected with) is applied identically in the new mirror-write, or discovery's and enrichment's writes could leapfrog each other into an inconsistent state (platform from one write, stale slug from the other).
- **5.3 is additive and cleanly no-ops** — per the design's own note, an `UPDATE ... WHERE domain = %s AND platform = %s` matching 0 rows (no `company_ats` row yet) is expected and harmless. No new failure mode beyond "the sync doesn't happen yet," which is the pre-Part-5 status quo anyway.
- **No schema changes, no migrations, nothing infra-facing** — this part is pure application logic across three files.
- **Rollback**: each of 5.1/5.2/5.3 is independently revertable (per the "Why this order" note below, they don't have to land or roll back together) — reverting 5.1 just means Phase 6/7 goes back to always re-running; reverting 5.2 stops the new mirror-writes (existing `company_ats`-only writes keep working exactly as before); reverting 5.3 just drops the new `company_ats` UPDATE from the button handler.

### Implementation steps

1. `workers/domain_enrichment_worker.py` — implement `_write_discovery_ats()` (5.2) and wire it into both existing `_write_ats()` call sites; port `upsert_discovery()`'s platform-change guard into it explicitly (see Blast radius above — this is the one correctness-critical detail).
2. Deploy and verify 5.2 alone first (per "Why this order relative to itself" below) — confirm `h1b_ats_discovery` rows start getting populated/updated from enrichment's Phase 3/6 detections, including for feins `discover_h1b_ats_worker` hasn't reached yet.
3. `scripts/discover_h1b_ats.py` — implement `_lookup_company_ats()` (5.1) and call it in `process_employer()` right before the Phase 6 gate; implement the tie-break ordering (`is_monitored` → `reviewed_at` → `priority`).
4. Deploy and verify 5.1 — force a case where `company_ats` already has the answer (a domain enrichment already resolved) and confirm discovery's next pass short-circuits before Phase 6/7, sets `ats_source = "company_ats_cache"`, and picks the correct row when multiple `company_ats` rows exist for the same domain.
5. `frontend/pages/3_Discover.py` — add 5.3's `company_ats` UPDATE next to the existing `h1b_ats_discovery` UPDATE in the "Add to monitoring" handler.
6. Deploy and verify 5.3 — click "Add to monitoring" on a company with an existing matching `company_ats` row, confirm both tables now show `is_monitored = TRUE` and `reviewed_at` is refreshed; click it on a company with no matching `company_ats` row and confirm the UPDATE no-ops cleanly (no error surfaced to the UI).

### Why this order relative to itself

5.2 should land before or alongside 5.1 — 5.1's cache-skip is only as good as how consistently `company_ats` is populated, and 5.2 is what makes discovery's own writes visible to future enrichment-side lookups too (not just the other direction). 5.3 is independent of the other two and can land separately.

---

## Part 6 — `add_prospective_company()` Re-Enable Gap

### Status

Designed 2026-09-27 — pending implementation. Surfaced while designing Part 5, then set aside to finish Part 5 first; unrelated to Part 5's platform/slug duplication problem — this one is specifically about the "Add to monitoring" click path failing to actually re-enable monitoring in one case.

### Problem statement

`add_prospective_company()` ([db/prospective.py:92](../db/prospective.py:92)) does `INSERT ... ON CONFLICT(company) DO NOTHING`, then — only when the row already existed (`inserted == False`) — runs a follow-up UPDATE that refreshes `domain`/`ats_platform`/`ats_slug` ([:113-129](../db/prospective.py:113)). That UPDATE never touches `is_monitored`.

Consequence: if a `prospective_companies` row already exists with `is_monitored = FALSE`, clicking "Add to monitoring" on the Discover page ([frontend/pages/3_Discover.py:1152](../frontend/pages/3_Discover.py:1152)) reports success (or "Already in pipeline") and unconditionally sets the cosmetic `h1b_ats_discovery.is_monitored = TRUE` ([:1178](../frontend/pages/3_Discover.py:1178)) — but the real flag the job monitor reads, `prospective_companies.is_monitored`, silently stays `FALSE`. The company is never actually scanned, and nothing in the UI indicates that.

### Design decision

Give `add_prospective_company()` an explicit opt-in param rather than always raising `is_monitored` on conflict — the function is also called from flows that must *not* silently start monitoring a company (the no-ATS paste-URL flow, [:1212](../frontend/pages/3_Discover.py:1212); the wrong-ATS override, [:1272](../frontend/pages/3_Discover.py:1272) — both intentionally omit platform/slug and rely on a later pipeline step to confirm before monitoring starts).

```python
def add_prospective_company(company, priority=0, domain=None, platform=None,
                             slug=None, enable_monitoring=False):
    ...
    # conflict-path UPDATE, only when enable_monitoring:
    #   is_monitored = TRUE (never lowered — only ever raises it)
```

`enable_monitoring=True` only from the one call site that means "the user just confirmed this platform/slug and wants it monitored" ([:1155](../frontend/pages/3_Discover.py:1155), the platform-known "Add to monitoring" button — the same click Part 5.3 already extends to `company_ats`). Every other existing call site keeps the current default (`False`) and current behavior unchanged.

### Files to change

1. `db/prospective.py` — add `enable_monitoring: bool = False` param to `add_prospective_company()`; when `True`, the conflict-path UPDATE also sets `is_monitored = TRUE` (an unconditional raise, not a toggle — this path never turns monitoring off).
2. `frontend/pages/3_Discover.py` — pass `enable_monitoring=True` at the one "Add to monitoring" call site with a known platform ([:1155](../frontend/pages/3_Discover.py:1155)). No other call site changes.

### 6.2 — Wrong-ATS override's `company_ats` UPDATE keys on the wrong column

**Problem, surfaced during this doc's own review**: the wrong-ATS override's `company_ats` write ([frontend/pages/3_Discover.py:1300-1306](../frontend/pages/3_Discover.py:1300)) is:

```sql
UPDATE company_ats SET is_monitored = FALSE
WHERE employer_fein = %s AND platform = %s
```

Part 5.3's own rationale for its sibling write in the same file explicitly says `employer_fein` is "not reliably populated on every `company_ats` row," which is why 5.3 matches on `(domain, platform)` instead. This existing override query never got that treatment: on any `company_ats` row with a `NULL`/unpopulated `employer_fein`, the `UPDATE` silently affects 0 rows, the UI reports "Correction submitted" regardless, and the wrong platform keeps `is_monitored = TRUE` — actively wrong monitoring continues, not just a missed opportunity to enable it (the failure direction Part 6's Fix 1 was careful to avoid).

**Design decision**: fix it, not just note it — matches Fix 1's severity, not a deferred item. Change the match to `(domain, platform)`, using the same `_norm_domain(website)` already in scope at that call site (same pair 5.3 and `_upsert_company_ats()`'s own `UNIQUE` constraint key off):

```sql
UPDATE company_ats SET is_monitored = FALSE
WHERE domain = %s AND platform = %s
```

### Files to change (6.2)

1. `frontend/pages/3_Discover.py` — change the wrong-ATS override's `company_ats` UPDATE ([:1300-1306](../frontend/pages/3_Discover.py:1300)) from `employer_fein = %s` to `domain = %s`, passing `_norm_domain(website)` (already computed earlier in this branch) instead of `fein`.

### Considered and rejected — mirroring `company_ats.is_monitored` → `h1b_ats_discovery.is_monitored`

Originally proposed as a second fix here (mirror the review-panel toggle, [:1384](../frontend/pages/3_Discover.py:1384), into `h1b_ats_discovery.is_monitored` so the Discover page's badge doesn't go stale). Rejected after re-reading the page's own button-visibility check ([:1146](../frontend/pages/3_Discover.py:1146)):

```python
if monitored or pipeline_st or ca_any_monitored:
```

`ca_any_monitored` ([:1137](../frontend/pages/3_Discover.py:1137)) already reads `company_ats.is_monitored` live on every page load — toggling the review panel already correctly suppresses the "Add to monitoring" button with no write into `h1b_ats_discovery` required. There's no actual staleness to fix here; nothing to build.

Also worth recording why this isn't a "make the two flags consistent" problem in the first place: `company_ats.is_monitored` and `h1b_ats_discovery.is_monitored` answer different questions. The former is real — it's what `get_monitorable_companies()` reads to decide what to actually scan. The latter is never read by the job monitor at all (`db/job_monitor.py:278-417` unions only `prospective_companies` and `company_ats`); it exists purely to gate the Discover page's own UI (show/hide the "Add to monitoring" prompt). The wrong-ATS override flow ([:1292-1307](../frontend/pages/3_Discover.py:1292)) deliberately sets them to *opposite* values in the same transaction — `company_ats.is_monitored = FALSE` (stop scanning the wrong platform for real) alongside `h1b_ats_discovery.is_monitored = TRUE` (suppress the normal prompt while re-detection is pending) — so a blanket bidirectional sync between the two would fight that flow. Part 5.3's write (Discover-page "Add to monitoring" button sets both tables `TRUE`) isn't an instance of this same mistake: it's a single explicit user action where both flags' independent questions happen to have the same answer, not a general sync rule.

### Blast radius

- **Touches**: `db/prospective.py` (`add_prospective_company()` — new optional param), `frontend/pages/3_Discover.py` (one call site gets the new param; a separate, unrelated `company_ats` UPDATE gets its match column changed).
- **Fix 1's signature change is backward compatible by construction** — `enable_monitoring: bool = False` defaults to exactly today's behavior; every call site except the one platform-known "Add to monitoring" button ([:1155](../frontend/pages/3_Discover.py:1155)) is untouched. The only behavior change is at that one call site, and it's a **fix**, not a new capability — it makes "Add to monitoring" actually enable monitoring in the one case (existing `prospective_companies` row with `is_monitored=FALSE`) where it silently didn't before.
- **6.2 is the higher-severity item of the two** — it fixes an *active wrong-monitoring* bug (wrong platform stays monitored because the UPDATE silently matches 0 rows), not a missed-opportunity gap. Per [[feedback_fix_all_valid_bugs]], both ship together regardless of relative complexity, but 6.2 is the one to verify most carefully in a diagnostic before trusting the fix.
- **6.2's risk if the fix itself is wrong**: switching the match from `employer_fein` to `domain` reuses the exact `_norm_domain(website)` value already computed earlier in the same branch (per the design) — no new normalization logic to get wrong, just wiring an existing local variable into a different WHERE clause. Low risk of introducing a *new* bug; the main verification need is confirming this UPDATE now actually affects a row where it previously affected 0.
- **No schema changes, no migrations, no infra.** Pure application-logic fixes in two files.
- **Rollback**: both fixes are independently revertable one-liners (revert the param default's usage at the one call site; revert the WHERE clause's column back to `employer_fein`) — reverting either just restores its respective pre-fix bug, no new risk introduced by rolling back.

### Implementation steps

1. `db/prospective.py` — add `enable_monitoring: bool = False` to `add_prospective_company()`; conflict-path UPDATE also sets `is_monitored = TRUE` when `True` (unconditional raise, never a toggle-down).
2. `frontend/pages/3_Discover.py` — pass `enable_monitoring=True` at the one platform-known "Add to monitoring" call site ([:1155](../frontend/pages/3_Discover.py:1155)); leave every other call site unchanged.
3. **Verify Fix 1**: find or create a `prospective_companies` row with `is_monitored = FALSE` that also has a resolved platform/slug in `h1b_ats_discovery`; click "Add to monitoring"; confirm `prospective_companies.is_monitored` flips to `TRUE` (not just the cosmetic `h1b_ats_discovery` flag as before the fix).
4. `frontend/pages/3_Discover.py` — change the wrong-ATS override's `company_ats` UPDATE ([:1300-1306](../frontend/pages/3_Discover.py:1300)) from `employer_fein = %s` to `domain = %s`, passing the branch's existing `_norm_domain(website)`.
5. **Verify 6.2**: reproduce the failure case directly — find (or set up) a `company_ats` row with `employer_fein` NULL/unpopulated for a company about to go through the wrong-ATS override flow; confirm that *before* the fix the UPDATE affects 0 rows (reproduces the bug) and *after* the fix it correctly flips `is_monitored = FALSE` on the right `(domain, platform)` row. Also re-run the flow on a row that *does* have `employer_fein` populated, to confirm the fix doesn't regress the case that happened to work before.
6. Both fixes can deploy together in one pass — they touch overlapping files but not overlapping code paths, and neither has a dependency on the other or on any other Part in this doc.

---

## Cross-cutting notes

- **Out-of-scope everywhere**: CT-log bare-root-token gap (Part 1) applies identically here — a custom-gTLD domain won't be found by any of these three parts; still a manual-override case.
- **Related existing docs**: `ats-redetection-design.md` (ZSET queue + manager-autoscale pattern Part 3 reuses), `enrichment_discovery_design.md` §11 (`external_api_health` pattern Part 3 mirrors), `ats-fetch-strategy.md` (existing ADP/Akamai curl_cffi precedent Part 2 generalizes).
