# DOL H-1B LCA Discovery Pipeline — Design Document

## Goal

Build a discovery tool that processes DOL LCA quarterly Excel files to find
H-1B sponsoring companies, aggregate employer profiles, and let the user
browse/filter them to make informed decisions about which companies to add
to the job pipeline.

**This is not a classifier gate.** It is a human-in-the-loop discovery tool.

---

## Data Source

**DOL LCA Public Disclosure Files** — Form ETA-9035  
Published quarterly at: https://www.dol.gov/agencies/eta/foreign-labor/performance  
Cadence: Q1 (Jan–Mar), Q2 (Apr–Jun), Q3 (Jul–Sep), Q4 (Oct–Dec)  
Format: Excel (.xlsx), one row per LCA application  
Scale: ~500K+ records per year, growing every quarter

### Key columns used

| Column | Use |
|---|---|
| `EMPLOYER_FEIN` | Primary key — stable IRS identifier across quarters |
| `EMPLOYER_NAME` | Legal business name of the actual H1B filer |
| `CASE_STATUS` | Certified / Certified-Withdrawn / Denied / Withdrawn |
| `VISA_CLASS` | Filter to `H-1B` only |
| `TOTAL_WORKER_POSITIONS` | Workers requested per case |
| `SOC_CODE` | Occupation code — used for breakdown, NOT for pipeline filtering |
| `SOC_TITLE` | Occupation title |
| `JOB_TITLE` | Actual job title filed |
| `DECISION_DATE` | Used to derive fiscal year for trends |
| `NAICS_CODE` | Industry classification |
| `H-1B_DEPENDENT` | Y/N — employer heavily reliant on H1B workers |
| `WILLFUL_VIOLATOR` | Y/N — prior DOL violation |
| `EMPLOYER_STATE`, `EMPLOYER_CITY` | Location |

### Columns explicitly excluded

- `SECONDARY_ENTITY_BUSINESS_NAME` — this is the **client/worksite** (e.g. Citibank
  hosting a consultant), NOT the H1B filer. Never used as employer identity.
- All attorney, agent, preparer, POC fields — not relevant
- Wage fields — out of scope for now

---

## Design Decisions (Locked)

### 1. FEIN as primary key
FEIN is the stable cross-quarter identifier. Employer names drift across filings
("Infosys BPO Ltd" vs "Infosys Limited"). FEIN from IRS does not.  
**NULL FEIN rows are skipped** — they cannot be reliably matched across quarters.

### 2. What counts as "certified"
- `Certified` + `Certified-Withdrawn` → **count as certified** (petition was
  approved; withdrawal happened after the fact)
- `Denied` + `Withdrawn` → count as filed but not certified
- **Approval rate** = `total_certified / total_filed` — a key signal of employer
  reliability. Low approval rate (< 50%) is a red flag.

### 3. No SOC filtering in the pipeline
The pipeline stores ALL H-1B filings regardless of SOC code. A civil engineering
firm that files mostly civil engineer H1Bs may still open software roles — we
want to discover those employers too. The user filters by SOC, role type, or
any other dimension entirely from the frontend.  
**The pipeline's job is to aggregate and enrich data, not to filter it out.**

### 4. All filtering happens in SQL, not in application code
Streamlit is the current frontend but is considered temporary. Every filter
(SOC code, state, NAICS, approval rate, year) must be expressible as an indexed
SQL query so that any future frontend (API, React, etc.) gets the same
performance without rewriting logic.

### 5. Raw records are never stored
Only aggregated per-employer stats are persisted. This keeps the database lean
regardless of how many quarterly files are processed.

### 6. Quarter deduplication built in from day one
Each employer row tracks `quarters_processed` (array of quarter identifiers like
`['FY2026_Q2', 'FY2025_Q4']`). Re-running the ingestion script on the same file
is a no-op. New quarters add incrementally to existing totals.

---

## Schema — Three Tables

### `dol_h1b_employers` — one row per employer (FEIN)

```sql
CREATE TABLE dol_h1b_employers (
    employer_fein       TEXT PRIMARY KEY,
    employer_name       TEXT NOT NULL,          -- most recent canonical name
    employer_city       TEXT,
    employer_state      TEXT,
    naics_code          TEXT,
    h1b_dependent       BOOLEAN,
    willful_violator    BOOLEAN,

    -- aggregate counts (all quarters combined)
    total_filed         INTEGER NOT NULL DEFAULT 0,
    total_certified     INTEGER NOT NULL DEFAULT 0,
    total_denied        INTEGER NOT NULL DEFAULT 0,
    total_withdrawn     INTEGER NOT NULL DEFAULT 0,
    total_positions     INTEGER NOT NULL DEFAULT 0,   -- all filed
    certified_positions INTEGER NOT NULL DEFAULT 0,   -- certified only
    approval_rate       REAL,                         -- recomputed on every upsert

    -- display only (not used for SQL filtering)
    top_job_titles      JSONB,   -- [{"title": "...", "count": N}, ...]

    quarters_processed  TEXT[]  NOT NULL DEFAULT '{}',
    last_updated        TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_dol_employers_state        ON dol_h1b_employers (employer_state);
CREATE INDEX idx_dol_employers_naics        ON dol_h1b_employers (naics_code);
CREATE INDEX idx_dol_employers_approval     ON dol_h1b_employers (approval_rate DESC);
CREATE INDEX idx_dol_employers_certified    ON dol_h1b_employers (total_certified DESC);
CREATE INDEX idx_dol_employers_h1b_dep      ON dol_h1b_employers (h1b_dependent);
```

### `dol_h1b_soc_breakdown` — certified counts per employer per SOC code

```sql
CREATE TABLE dol_h1b_soc_breakdown (
    employer_fein   TEXT NOT NULL REFERENCES dol_h1b_employers(employer_fein),
    soc_code        TEXT NOT NULL,
    soc_title       TEXT,
    total_filed     INTEGER NOT NULL DEFAULT 0,
    total_certified INTEGER NOT NULL DEFAULT 0,
    total_positions INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (employer_fein, soc_code)
);

CREATE INDEX idx_dol_soc_code      ON dol_h1b_soc_breakdown (soc_code);
CREATE INDEX idx_dol_soc_certified ON dol_h1b_soc_breakdown (soc_code, total_certified DESC);
```

### `dol_h1b_yearly` — year-over-year breakdown per employer

```sql
CREATE TABLE dol_h1b_yearly (
    employer_fein   TEXT NOT NULL REFERENCES dol_h1b_employers(employer_fein),
    year            INTEGER NOT NULL,
    filed           INTEGER NOT NULL DEFAULT 0,
    certified       INTEGER NOT NULL DEFAULT 0,
    denied          INTEGER NOT NULL DEFAULT 0,
    withdrawn       INTEGER NOT NULL DEFAULT 0,
    positions       INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (employer_fein, year)
);

CREATE INDEX idx_dol_yearly_fein ON dol_h1b_yearly (employer_fein, year);
```

---

## Ingestion Script — `scripts/process_dol_lca.py`

### Invocation
```bash
python scripts/process_dol_lca.py --file LCA_FY2026_Q2.xlsx --quarter FY2026_Q2
```

### Processing steps
1. Load Excel file with pandas
2. Filter to `VISA_CLASS == 'H-1B'` (excludes E-3, H-1B1 Chile/Singapore)
3. Skip rows where `EMPLOYER_FEIN` is NULL or empty
4. Check `quarters_processed` in DB — skip entire quarter if already loaded
5. Derive `year` from `DECISION_DATE`
6. Classify each row: certified = `CASE_STATUS IN ('Certified', 'Certified-Withdrawn')`
7. Aggregate by `EMPLOYER_FEIN`:
   - Employer-level totals (filed, certified, denied, withdrawn, positions)
   - SOC breakdown (per FEIN+SOC_CODE)
   - Yearly breakdown (per FEIN+year)
   - Top 15 job titles by count (for JSONB display column)
8. Upsert all three tables (add to existing totals, don't replace)
9. Recompute `approval_rate` = `total_certified / total_filed`
10. Append quarter to `quarters_processed`, update `last_updated`

---

## Discover Page — Frontend Filters

All filters translate directly to SQL WHERE clauses:

| Filter | SQL |
|---|---|
| Employer name search | `employer_name ILIKE '%query%'` |
| State | `employer_state = ?` |
| NAICS industry | `naics_code LIKE '?%'` |
| H1B dependent | `h1b_dependent = TRUE` |
| Min approval rate | `approval_rate >= ?` |
| Min certified count | `total_certified >= ?` |
| SOC code (via join) | `EXISTS (SELECT 1 FROM dol_h1b_soc_breakdown WHERE employer_fein = e.employer_fein AND soc_code LIKE '?%' AND total_certified > 0)` |

Sort options: total certified DESC, approval rate DESC, total positions DESC

**Employer profile view** (on row click):
- Year-over-year chart (from `dol_h1b_yearly`)
- SOC breakdown table (from `dol_h1b_soc_breakdown`)
- Top job titles (from `top_job_titles` JSONB)
- "Add to pipeline" button → inserts into `prospective_companies`

---

## Automation

Run on the Oracle server as a cron job or manual trigger each quarter:

```bash
# Example: new Q3 file drops, run ingestion
python scripts/process_dol_lca.py --file LCA_FY2026_Q3.xlsx --quarter FY2026_Q3
```

The `quarters_processed` guard makes it safe to re-run. DOL typically publishes
new quarters within 3-4 months of the period end.

---

## Out of Scope (for now)

- Storing raw LCA records (too large, not needed)
- Wage/salary analysis
- Attorney/law firm analysis
- E-3, H-1B1 visa classes
- Automatic DOL file download (manual download + script run is fine)
- Linking FEIN to ATS automatically (user does this manually via "Add to pipeline")
