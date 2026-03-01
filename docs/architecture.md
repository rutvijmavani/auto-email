# Architecture

## Overview

The Recruiter Outreach Pipeline is an automated system that finds recruiter contacts for companies you apply to and sends personalized cold outreach emails. It consists of three independently runnable stages that work together as a daily workflow.

## Components

### Pipeline Stages

**`--sync-forms`** — Job Ingestion
Pulls new job applications from a Google Form, inserts them into the database, and immediately attempts to scrape the job description from the provided URL. Fast portals (Greenhouse, Lever) succeed here. JS-heavy portals (Ashby, Workday) are retried during `--find-only`.

**`--find-only`** — Contact Discovery
The core intelligence stage. Runs overnight and does three things:
1. Verifies existing recruiters using the tiered verification system
2. Scrapes CareerShift for new recruiter contacts using quota-aware distribution
3. Generates personalized AI email content via Gemini for all applications

**`--outreach-only`** — Email Sending
Sends scheduled outreach emails within the configured morning send window (9 AM - 11 AM). Handles the full email sequence (initial, follow-up 1, follow-up 2) and detects hard bounces automatically.

---

## Daily Workflow

```
Evening
  └── Fill Google Form for each job applied
  └── python pipeline.py --sync-forms
        → inserts applications into DB
        → scrapes job descriptions (fast portals)

Night
  └── python pipeline.py --find-only
        → tiered verification of existing recruiters (free)
        → scrapes CareerShift for new recruiters (quota-aware)
        → uses leftover CareerShift quota for under-stocked companies
        → generates AI email content via Gemini
        → uses leftover Gemini quota for applications missing cache
        → runs quota health check → sends alert email if needed

Morning (9 AM - 11 AM)
  └── python pipeline.py --outreach-only
        → schedules initial emails for new recruiter+application pairs
        → sends due emails within send window
        → schedules next follow-up after each send
        → detects hard bounces → marks recruiter inactive
```

---

## Data Flow

```
Google Form
    ↓
applications table
    ↓
jobs table (job descriptions)
    ↓
recruiters table (CareerShift scraping)
    ↓
application_recruiters table (many-to-many links)
    ↓
ai_cache table (Gemini generated content)
    ↓
outreach table (email sequence)
    ↓
Email sent to recruiter
```

---

## Component Responsibilities

| Component | Responsibility |
|---|---|
| `careershift/auth.py` | Login to CareerShift via Symplicity portal, save session |
| `careershift/find_emails.py` | Scrape recruiters, tiered verification, quota management |
| `outreach/outreach_engine.py` | Schedule and send emails, bounce detection |
| `outreach/template_engine.py` | Build email body and subject from AI cache |
| `outreach/ai_full_personalizer.py` | Generate email content via Gemini AI |
| `outreach/email_sender.py` | SMTP sending with resume attachment |
| `jobs/job_fetcher.py` | Fetch and cache job descriptions |
| `jobs/job_scraper.py` | Scrape JD from various ATS portals |
| `jobs/form_sync.py` | Pull Google Form responses into DB |
| `db/db.py` | Single source of truth — all database operations |
| `db/quota_manager.py` | Thin wrapper for Gemini quota functions |
| `db/job_cache.py` | Thin wrapper for job cache functions |
| `pipeline.py` | Orchestrator with CLI flags |
| `config.py` | All configuration in one place |

---

## Error Handling and Fallbacks

**Job description scraping fails:**
```
JD scraped successfully    → generate_all_content(company, title, job_text)
JD scraping fails          → generate_all_content_without_jd(company, title)
AI quota exhausted         → skip, retry tomorrow
```

**CareerShift search fails:**
```
Pass 1: HR title + RequireEmail + exclude senior titles  → ideal
Pass 2: HR title + RequireEmail + include senior titles  → fallback
Pass 3: No filters + exclude senior titles               → last resort
```

**Email sending fails:**
```
Hard bounce (SMTPRecipientsRefused) → mark recruiter inactive, cancel outreach
Other failure                       → mark outreach failed, retry not automatic
Outside send window                 → reschedule for tomorrow
AI cache missing                    → warn and skip, run --find-only first
```

---

## Key Design Decisions

**Single SQLite database** — All data lives in `data/recruiter_pipeline.db` with WAL mode enabled for concurrent reads/writes. Three separate databases (`job_cache.db`, `quota.db`, `recruiter_pipeline.db`) were consolidated into one.

**Company-level recruiters** — Recruiter contacts are stored at the company level, not per application. The `application_recruiters` join table handles the many-to-many relationship. This prevents duplicate recruiter rows when applying to multiple roles at the same company.

**Option B outreach scheduling** — Follow-up emails are scheduled only after the previous stage is sent with no reply. This prevents scheduling follow-ups for recruiters who already responded.

**Cached profiles are free** — CareerShift's daily limit (50 new contacts) only applies to first-time profile views. Re-visiting cached profiles is free, which is why tiered recruiter verification costs zero quota.

**AI content pre-generated** — Email content is generated during `--find-only` (night) not during `--outreach-only` (morning). This ensures the morning send window is purely a sending step with no external API dependencies.