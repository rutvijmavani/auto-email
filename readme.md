# Recruiter Outreach Pipeline

An automated pipeline for finding recruiter contacts and sending personalized cold outreach emails after job applications.

---

## Overview

This pipeline automates three things:

1. **Contact discovery** — finds recruiter emails from CareerShift for companies you applied to
2. **Email personalization** — generates unique outreach emails using the job description and Gemini AI
3. **Scheduled sending** — sends emails within a configured morning window and follows up automatically

---

## Project Structure

```
recruiter/
|
|-- main.py                      # Entry point
|-- pipeline.py                  # Orchestrator with CLI flags
|-- config.py                    # All configuration settings
|-- credentials.json             # Google service account (not committed)
|-- Resume.pdf                   # Attached to every outreach email (not committed)
|-- .env                         # API keys and credentials (not committed)
|
|-- careershift/                 # CareerShift scraping
|   |-- auth.py                  # Login and session management
|   |-- find_emails.py           # Recruiter contact scraper
|
|-- outreach/                    # Email generation and sending
|   |-- outreach_engine.py       # Scheduling and send logic
|   |-- template_engine.py       # Email body and subject builder
|   |-- email_sender.py          # SMTP sender with resume attachment
|   |-- ai_full_personalizer.py  # Gemini AI content generation
|
|-- jobs/                        # Job data ingestion
|   |-- job_fetcher.py           # Fetches and caches job descriptions
|   |-- job_scraper.py           # Scrapes JD from various ATS portals
|   |-- form_sync.py             # Syncs Google Form responses to DB
|
|-- db/                          # Database layer
|   |-- db.py                    # Single SQLite DB, all tables and helpers
|   |-- job_cache.py             # Thin wrapper for job cache functions
|   |-- quota_manager.py         # Thin wrapper for AI quota functions
|
|-- data/                        # Runtime data (not committed)
|   |-- recruiter_pipeline.db    # Main SQLite database
|   |-- careershift_session.json # CareerShift login session
|
|-- tests/                       # Unit and integration tests
|   |-- test_add.py
|   |-- test_find_only.py
|   |-- test_outreach_only.py
|   |-- test_integration.py
|   |-- run_tests.py
```

---

## Database Schema

All data lives in a single SQLite database (`data/recruiter_pipeline.db`) with WAL mode enabled.

| Table | Description |
|---|---|
| `applications` | Jobs you applied to |
| `recruiters` | Company-level recruiter contacts |
| `application_recruiters` | Many-to-many join between recruiters and applications |
| `outreach` | Email sequences per recruiter and application |
| `careershift_quota` | Daily CareerShift profile view limit tracking |
| `ai_cache` | Generated email content per company and job (TTL: 21 days) |
| `jobs` | Cached job descriptions scraped from URLs (TTL: 21 days) |
| `model_usage` | Gemini AI daily quota usage per model |

---

## Setup

### 1. Install dependencies

```bash
pip install playwright beautifulsoup4 python-dotenv gspread google-auth google-generativeai
playwright install chromium
```

### 2. Configure environment variables

Create a `.env` file in the project root:

```env
# Email credentials
EMAIL=your.email@gmail.com
APP_PASSWORD=your_gmail_app_password

# Gemini AI
GEMINI_API_KEY=your_gemini_api_key

# Google Sheets integration
GOOGLE_SHEET_ID=your_google_sheet_id
```

### 3. Configure settings

Edit `config.py`:

```python
SEND_INTERVAL_DAYS = 7       # Days between follow-up emails
SEND_WINDOW_START  = 9       # Send window start (9 AM)
SEND_WINDOW_END    = 11      # Send window preferred end (11 AM)
GRACE_PERIOD_HOURS = 1       # Hard cutoff at 12 PM
SEND_TIMEZONE      = "America/New_York"
```

### 4. Set up CareerShift session

```bash
python careershift/auth.py
```

Logs in to CareerShift via your university Symplicity portal and saves the session to `data/careershift_session.json`.

### 5. Set up Google Sheets integration (optional)

Required only if using Google Form for job submissions.

- Create a Google Cloud project and enable the Sheets and Drive APIs
- Create a service account with Editor role and download `credentials.json`
- Share your Google Sheet with the service account email
- Add `GOOGLE_SHEET_ID` to `.env`

---

## Daily Workflow

### Evening — Add jobs after applying

**Option A: Google Form (recommended)**
Fill out the Google Form after each application. Then run:
```bash
python pipeline.py --sync-forms
```

**Option B: Manual entry**
```bash
python pipeline.py --add
```

### Night — Find recruiters and generate email content

```bash
python pipeline.py --find-only
```

This step:
- Checks DB for existing recruiters per company
- Scrapes CareerShift for new contacts (quota-aware)
- Links recruiters to applications
- Generates personalized email content via Gemini AI

### Morning — Send outreach emails (9 AM - 11 AM)

```bash
python pipeline.py --outreach-only
```

This step:
- Waits until 9 AM if run too early
- Sends emails within the configured window
- Schedules the next follow-up after each send
- Reschedules unsent emails to tomorrow if past 12 PM cutoff

---

## Outreach Sequence

Each recruiter receives up to 3 emails per application:

```
Day 0:  Initial email        (sent immediately)
Day 7:  Follow-up 1          (sent if no reply)
Day 14: Follow-up 2          (sent if no reply)
        Sequence ends
```

If the recruiter replies at any point, set `replied = 1` in the `outreach` table to stop further emails.

---

## CareerShift Quota

CareerShift allows viewing 50 new contact profiles per day. The pipeline manages this automatically:

- Distributes daily quota fairly across new companies
- Re-verifying existing recruiters is free (cached profiles)
- Quota usage is tracked in `careershift_quota` table
- Re-visiting previously viewed profiles does not count against the limit

**Quota distribution formula:**

```
base = remaining_quota // new_companies
extra = remaining_quota % new_companies

First `extra` companies get base + 1 contacts
Remaining companies get base contacts
Total credits used = remaining_quota (fully utilized)
```

---

## AI Content Generation

Email content is generated using Google Gemini and cached for 21 days.

| Model | Daily limit |
|---|---|
| gemini-2.5-flash-lite | 20 calls |
| gemini-2.5-flash | 20 calls (fallback) |

Each job application requires one AI call, which generates all three email stages at once (initial, follow-up 1, follow-up 2).

---

## Testing

```bash
# Run all tests
python tests/run_tests.py

# Unit tests only
python tests/run_tests.py --unit

# Integration tests only
python tests/run_tests.py --integration
```

All tests use an isolated `data/test_pipeline.db` that is created and deleted automatically. Your real database is never touched.

---

## Limitations

- CareerShift session expires periodically and must be renewed by running `auth.py`
- CareerShift daily limit: 50 new profile views
- Gemini AI daily limit: 40 total calls (20 per model)
- Job description scraping may fail on heavily authenticated portals (Workday, Taleo, iCIMS may require Playwright)
- Google Form date field exports in M/D/YYYY format which is handled automatically

---

## Notes

- Never commit `.env`, `credentials.json`, `Resume.pdf`, or the `data/` folder
- All three databases (`recruiter_pipeline.db`, old `job_cache.db`, old `quota.db`) are now consolidated into `recruiter_pipeline.db`
- TTL for all caches is 21 days to cover the full 3-email outreach cycle (7 days x 3 emails)
- Expired cache entries are cleaned up automatically on every pipeline run