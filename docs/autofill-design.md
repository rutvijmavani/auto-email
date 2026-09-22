# Application Autofill — Design

Status: **DESIGN DRAFT — nothing built yet.** Extends the Chrome extension
(`chrome-extension/`, see `chrome-extension.md`) from *capturing* jobs to *filling*
applications, in the spirit of Simplify / Tsenta.

---

## 1. Goals and non-goals

**Goals**
- Fill job application forms from a per-user profile + resume, including questions
  the tool has never seen, using an LLM for mapping and drafting.
- Multi-user from day one.
- Support the hardest ATS (Workday) in the first release, not as an afterthought.
- Learn from user corrections, silently, per user; learn question mappings globally.

**Non-goals (phase 1)**
- Auto-submit. The extension fills; the user reviews and clicks Submit.
- Server-side headless / direct-request submission (Tsenta-style). Possible later.
- Solving CAPTCHAs, handling email verification codes, or storing ATS passwords.

---

## 2. Principles

1. **Blank + highlight beats a guess.** Anything not confidently mapped is left empty
   and flagged, never filled with a guess.
2. **Never infer sensitive attributes** (gender, race, disability, veteran status,
   criminal record, work authorization, sponsorship). Profile answer or blank.
3. **The shared dictionary holds mappings, never personal data.** Only
   `question → field` mappings are cross-user. Answers and profiles are per-user.
4. **Store facts, not clicks.** Learned answers are stored as plain-language profile
   facts, not option strings, because option wording differs per company
   ("Woman" vs "Female").
5. **No hardcoded values.** Every threshold, timeout, model name and vote count below
   is a `config.py` env var.
6. **The LLM never sees an ATS password** and the extension never stores one.

---

## 3. Architecture

```text
content script (per ATS page)          Flask API (api.py)                 Postgres / Gemini|Claude
  ├─ extract form → FieldSpec[]   ──►   POST /autofill/plan       ──►    question_mappings (shared)
  ├─ fill fields + mark source    ◄──   {field → value, source,          user_profile_facts
  ├─ overlay: review UI                  confidence}                     user_answers
  └─ on Submit click: diff        ──►   POST /autofill/learn      ──►    LLM (mapping / drafting)
```

- The extension **never calls an LLM directly** (no API keys in the extension).
- Auth reuses the extension→API mechanism (`EXTENSION_API_KEY`, `X-Extension-Id`),
  extended with a per-user identity (the existing `detectUser` / `users` table).
- Autofill LLM traffic uses its **own API key and quota**, separate from the batch
  pipeline workers, so interactive fills never queue behind discovery jobs.

---

## 4. Data model (sketch)

```sql
-- per-user, plain-language + structured facts
user_profile_facts (
  id, user_id, fact_key TEXT,        -- e.g. gender, needs_sponsorship_now, work_auth_status
  fact_text TEXT,                    -- "Requires H-1B sponsorship: yes"
  value JSONB,                       -- structured value
  source TEXT,                       -- profile_form | resume | user_confirmed | learned
  updated_at, superseded_by
)

-- shared across users: NO personal data
-- One row per MEANING (canonical mapping); many phrasings point at it via question_aliases
question_mappings (
  id,
  options_shape TEXT,                -- coarse type: yesno | single | multi | text | date
  category TEXT,                     -- profile_field | derived | free_text | user_specific
  field_expr JSONB,                  -- {"fields":["work_authorized","needs_sponsorship_now"],
                                     --  "transform":"a AND NOT b"}
  confidence REAL, votes_for INT, votes_against INT,   -- shared by all aliases
  status TEXT,                       -- provisional | trusted | demoted
  sensitive BOOLEAN,                 -- legal / EEO class → stricter promotion rules
  created_at, updated_at
)

-- shared: one row per distinct PHRASING of a question
question_aliases (
  id, mapping_id REFERENCES question_mappings,
  question_norm TEXT UNIQUE,         -- normalized text (exact-match key)
  question_embedding VECTOR,
  polarity_cues TEXT[],              -- extracted negation/time cues, see §5 step 3 gate
  accepted_by TEXT,                  -- exact | embedding | llm_confirm | llm_classify
  llm_confirmed BOOLEAN,             -- required before a sensitive alias is used
  seen_count INT, created_at, last_seen_at
)

-- per-user answers to non-mappable questions + history for few-shot
user_answers (
  id, user_id, question_norm, question_embedding,
  answer_text, company, job_url, kind,   -- fact | company_specific | free_text
  created_at
)

-- what was filled and how, for the diff at submit time
autofill_events (
  id, user_id, job_url, ats, field_ref, question_norm,
  filled_value, source,              -- profile | llm | blank
  final_value, changed BOOLEAN, created_at
)
```

Sensitive facts (EEO, work authorization) are stored encrypted at rest and are never
copied into `question_mappings`.

---

## 5. Fill pipeline

1. **Extract** the form into `FieldSpec[]`: `{ref, label, type, options[], required,
   section, autocomplete, name/id, platform_id}`. Source: DOM + accessibility tree;
   screenshot fallback only for widgets the DOM cannot describe (Workday, §11).
2. **Layer 1 — attributes.** `autocomplete`, `type=email|tel`, known platform ids
   (`data-automation-id` on Workday, stable names on Greenhouse/Lever/Ashby) map
   the obvious fields with no LLM call.
3. **Layer 2 — shared dictionary (alias cascade).** Every distinct phrasing is stored
   as an alias of one canonical mapping. A new phrasing is resolved by the cheapest
   step that can decide, in this order; the LLM is a last resort, not the default:
   1. **Normalize + exact match** on `question_aliases.question_norm`. No cost.
   2. **Embedding nearest neighbour** (small local model, CPU-cheap). If similarity
      ≥ `AUTOFILL_ALIAS_ACCEPT_SIM` **and** the negation gate passes **and** the
      mapping is not `sensitive` (or the nearest alias is already `llm_confirmed`),
      accept: insert a new alias pointing at that mapping. No LLM call.
   3. **Gray zone.** Similarity between `AUTOFILL_ALIAS_REVIEW_SIM` and the accept
      threshold, **or** the negation gate fails, **or** the mapping is `sensitive`
      → one small "same question?" LLM call against the top candidate. Confirmed →
      new alias (`llm_confirmed = true`); rejected → falls to Layer 3.
   4. **Below `AUTOFILL_ALIAS_REVIEW_SIM`** → Layer 3.

   **Negation gate (deterministic).** Embeddings place "Do you require sponsorship?"
   and "Do you *not* require sponsorship?" very close together, so similarity alone is
   never trusted. Extract polarity cues from both texts (`not`, `no`, `without`,
   `unless`, `never`, `no longer`, and time cues such as `now`, `currently`, `future`,
   `in the future`, `ever`; list lives in config, per language). If the cue sets
   differ, the pair goes to the LLM regardless of similarity.

   A resolved alias → evaluate the mapping's `field_expr` against the user's facts.
   Cost note: each new phrasing is paid for once, globally, and never again; phrasings
   repeat heavily, so LLM calls fall off as coverage grows.
4. **Layer 3 — unseen question.** One batched LLM call per page classifies each
   unseen question into a category (§6) and, for `profile_field`, proposes
   `field_expr`. Result stored as a `provisional` canonical mapping plus its first
   alias (`accepted_by = llm_classify`). The call may also point the phrasing at an
   existing mapping the embedding step missed; that adds an alias, not a new mapping.
5. **Answer.** `profile_field` → deterministic evaluation, then option translation
   (alias lists + LLM picks among the *actual* on-page options, or `unknown`).
   `derived` / `free_text` → see §6. Anything unresolved → blank + highlight.
6. **Fill** with framework-safe writes (native value setter + `input`/`change`/`blur`
   events; React/Workday controlled inputs ignore plain `.value =`). Record every
   filled value + source in `autofill_events`.
7. **Review overlay.** Low-confidence and blank required fields listed; nothing submits.

---

## 6. Question routing categories

| Category | Example | Answered how | In shared dictionary? |
|---|---|---|---|
| `profile_field` | gender, phone, sponsorship | Deterministic from facts via `field_expr` | **Yes** |
| `derived` | "Worked at X or an affiliate?", "years of Python?" | Phase 1: **blank + highlight**. Later: resume-employer check vs company + `fein_domain_map` parent/subsidiary, LLM fallback | No (depends on company) |
| `free_text` | "Why us?", "hardest problem?" | LLM draft from resume + JD + user's past answers as few-shot; always flagged draft | No |
| `user_specific` | one-off custom question | Blank; learned per user from the answer | No |

Only stable personal-fact questions enter the shared dictionary. Company-parameterized
questions never do (avoids a combinatorial flood).

---

## 7. Mapping record and confidence

- `field_expr` carries **field(s) + transform**, so polarity is decided once per unique
  question and is deterministic at runtime:
  - "Require sponsorship?" → `needs_sponsorship_now`, identity
  - "Authorized without sponsorship?" → `work_authorized AND NOT needs_sponsorship_now`
- Options are **not** stored; they are translated per form at runtime.
- New mapping = `provisional`: field is filled but highlighted. Promotion to `trusted`
  needs `AUTOFILL_PROMOTE_VOTES` agreeing users (config). `sensitive` mappings use a
  higher bar (`AUTOFILL_PROMOTE_VOTES_SENSITIVE`) and require the quote-justification check.
- Votes and status live on the **canonical mapping** and are shared by all its aliases.
  An alias inherits the mapping's `field_expr`, so an alias is only valid if it means
  the same thing *with the same polarity* — this is exactly what the negation gate in
  §5 step 3 protects.
- Demotion: if the correction rate on a mapping exceeds `AUTOFILL_DEMOTE_RATE` over a
  minimum sample, set `demoted` (falls back to Layer 3 / blank). If corrections
  concentrate on **one alias** while the mapping is otherwise healthy, detach that alias
  (delete it, or re-point it after an LLM re-classification) instead of demoting the
  whole mapping — a wrongly accepted alias must not poison its siblings.
- Threshold calibration: `AUTOFILL_ALIAS_ACCEPT_SIM` / `AUTOFILL_ALIAS_REVIEW_SIM` are
  set from the phase 0 benchmark (§14), which must record how many real questions land
  in each cascade step and the false-accept rate at candidate thresholds.
- Known trap: **near-neighbour fields with the same value** (now vs future sponsorship;
  relocate vs remote). The classifier must quote the question wording that justifies the
  chosen field; a mapping with no supporting wording is rejected.

---

## 8. Learning loop (silent by default)

At Submit click, diff `filled_value` vs current value per field. Only learn on an
actual submit (abandoned forms are ignored).

- **Blank filled by user / value changed** → one LLM call with
  `{question, options, user_answer, profile facts}` returning:
  `maps_to_field | none`, `transform`, `profile_value_agrees`, `new_fact`,
  `justifying_quote`, `confidence`.
  - mapping right, profile agrees → **vote for** mapping
  - mapping right, profile disagrees → **profile fact was stale** → update fact
    (`source = user_confirmed`, highest priority)
  - mapping wrong / no field → `against` vote or category `user_specific`
  - answer varies by job (relocation etc.) → mark **context-dependent**, flag, never
    silently overwrite
- New facts are stored as text, e.g. "Gender: woman", and outrank LLM/profile-form values.
- History kept (`superseded_by`) so the user can review/undo learned facts on the
  profile page.
- Reverse inference from the answer alone is used **only** when no mapping was proposed,
  and only with the question in the prompt (Yes/No is ambiguous without it).

---

## 9. Sensitive-field policy

Never inferred or guessed: gender, race/ethnicity, disability, veteran status,
criminal history, work authorization, sponsorship, salary history. Rule: profile
answer (or `user_confirmed` fact) → fill; otherwise blank + highlight. The first time
the user answers, that answer becomes the fact. The name is never used to infer gender.

---

## 10. LLM call types and model choice

| Call | When | Shape |
|---|---|---|
| Category + mapping | unseen question | batched per page, small/fast model |
| "Same question?" confirm | embedding near-hit | tiny, per candidate |
| Option pick | option text can't be alias-matched | constrained to real options or `unknown` |
| Free-text draft | `free_text` fields | stronger model, resume + JD + few-shot |
| Learning / distillation | after submit | one per changed field |

Models are config (`AUTOFILL_MODEL_FAST`, `AUTOFILL_MODEL_STRONG`). Candidate split:
Haiku-class or Gemini Flash-class for mapping/option picks, Sonnet-class or larger Gemini
for drafts. **Decide by benchmark, not opinion** (§13 step 0). Estimated budget per
page: deterministic fill < 1 s, one batched call ~2–5 s (small model), most time is
user review. Fire the mapping call as soon as questions are visible so it overlaps with
deterministic filling.

---

## 11. Workday (hardest ATS — in scope for phase 1)

Why it is hard: multi-step SPA (no page loads), custom widgets, per-tenant account,
repeatable sections, tenant-customised questions.

**Flow to support:** Apply → (Autofill with Resume / Apply Manually) → Sign in / Create
account → My Information → My Experience → Application Questions → Voluntary
Disclosures → Self Identify → Review → Submit.

| Problem | Approach |
|---|---|
| SPA, steps swap without navigation | `MutationObserver` on the app root; a step detector keyed on the stepper/section headings; run extract→plan→fill per step |
| Stable hooks | Workday exposes `data-automation-id` on most fields — primary Layer 1 key for a Workday adapter (name, email, phone, address, country/state, etc.) |
| Custom dropdowns / listboxes | Adapter routine: click the trigger → wait for popup → type-ahead → select the option by text; fallback to screenshot-assisted pick if the popup is unlabeled |
| Date widgets | Separate month/day/year spinbuttons; fill each part, dispatch events |
| Controlled React inputs | Native setter + `input`/`change`/`blur` |
| Repeatable sections (jobs, education, languages) | Read the count from structured resume/profile; click "Add" N times, fill each instance by index; ordered work history in profile |
| Resume upload | Fetch the PDF from the API as a `Blob`, attach via `DataTransfer` to the file input; verify the parsed-resume prefill Workday performs and reconcile (Workday may pre-populate from parsing — diff before overwriting) |
| Login / account creation | **The extension stops and hands control to the user.** No passwords stored or filled by us; the browser password manager / user does it. Email verification is manual in phase 1. Extension resumes when the stepper shows the first application step |
| Tenant-custom questions | Fall through to Layers 2–3 (shared dictionary + LLM) exactly like any other ATS |
| Manifest | `*.myworkdayjobs.com`, `*.myworkdaysite.com` already matched; verify the application-step URLs are covered |

Workday adapter = a thin layer that provides `extractFields()`, `fillField()` and
`onStepChange()` for the core pipeline; question mapping/answering stays platform-agnostic.

**Risk:** Workday markup changes break selectors. Mitigation: `data-automation-id` first,
label-based fallback, an adapter self-check that reports "0 fields detected" to the
overlay instead of failing silently, and a fixture-based test set (saved DOM snapshots).

---

## 12. Other platforms

Same adapter interface (`extractFields`, `fillField`, `onStepChange`), phased after the
core: Greenhouse, Lever, Ashby, SmartRecruiters (standard forms, mostly Layer 1) →
iCIMS, Taleo, SuccessFactors (wizards, iframes) → Oracle CandidateExperience, others.
The platform list is driven by the extension's existing content-script match list.

---

## 13. Multi-user and privacy

- Every table with personal data is keyed by `user_id`; `question_mappings` is the only
  shared table and holds no answers or profile values.
- Encrypt sensitive facts at rest; never log answers, resume text or facts (log ids
  and categories only — use `init_logging`).
- The resume/profile sent to the LLM is a privacy decision to state to users; only
  the relevant slices are sent per call.
- Shared mappings are seeded/verified by the `sensitive` promotion bar so one user's
  mistake cannot become everyone's rule.
- Per-user rate limits on LLM-backed endpoints.

---

## 14. Phased plan

0. **Benchmark (≈1 h):** 30–50 real questions × 2–3 candidate models; measure latency,
   grounding to resume, valid-option rate, polarity accuracy. Pick models from data.
   Also measure the alias cascade (§5 step 3): share of questions resolved by exact /
   embedding / gray-zone LLM / full classify, and the false-accept rate (incl. negated
   pairs) at candidate similarity thresholds, to set `AUTOFILL_ALIAS_*_SIM`.
1. **Profile + storage:** tables (§4), profile editor (Streamlit), resume ingestion.
2. **Core pipeline + one simple ATS** (Greenhouse): Layers 1–3, overlay, blank+highlight.
3. **Learning loop + shared dictionary** with confidence/promotion rules.
4. **Workday adapter** (§11) on saved-DOM fixtures first, then live.
5. Lever, Ashby, SmartRecruiters; then wizard ATSs.
6. `derived` questions (resume-employer + affiliate lookup); free-text drafting polish.

---

## 15. Open questions

- Encryption approach and key management for sensitive facts.
- Embedding model + vector store (pgvector on the existing Postgres vs external).
- Where the profile editor lives (Streamlit page vs extension options page).
- Whether Workday "Autofill with Resume" should be used or bypassed (parsing quality
  varies; reconciliation logic needed either way).
- Later: fully automatic server-side submission (browser or direct-request), email
  verification handling via the existing email-tracking pipeline.
