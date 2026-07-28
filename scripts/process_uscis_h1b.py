"""
scripts/process_uscis_h1b.py — USCIS H-1B Employer Data Hub ingestion

Loads a locally downloaded USCIS H-1B petition CSV (or Excel) and upserts
each row into uscis_h1b_petitions.  One DB row per unique combination of
(employer_name, tax_id, fiscal_year, naics_code, city, zip) — exactly the
granularity published by USCIS.

Source: USCIS H-1B Employer Data Hub (FY2024–FY2026).
  Data for these years is only available via the Tableau embed on:
  https://www.uscis.gov/tools/reports-and-studies/h-1b-employer-data-hub
  Download manually: "Crosstab View" → filter year → "Download to Excel" → CSV.

Usage:
    python scripts/process_uscis_h1b.py --file ~/Downloads/H1B_FY2026.csv
    python scripts/process_uscis_h1b.py --file ~/Downloads/H1B_FY2025.xlsx

Design decisions:
  - employer_name_norm stored alongside raw name for DOL join:
      re.sub(r"[^A-Z0-9 ]", " ", name.upper()).strip() — collapses punctuation
      to spaces then deduplicates whitespace.  "Amazon.com Services LLC" and
      "AMAZON COM SERVICES LLC" both become "AMAZON COM SERVICES LLC".
  - ON CONFLICT DO UPDATE — re-running is safe; values are replaced (not added)
      because the USCIS file is the authoritative count, not an accumulation.
  - Rows with blank employer name or tax_id are skipped.
  - Integer columns that come in as NaN (blank cell) default to 0.
"""

import argparse
import os
import re
import sys

import pandas as pd

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from db.connection import get_conn
from logger import get_logger, init_logging

log = get_logger(__name__)

# Map CSV column headers → DB column names.
# USCIS uses these exact headers in the Tableau CSV export.
_COL_MAP = {
    "Fiscal Year":                          "fiscal_year",
    "Employer (Petitioner) Name":           "employer_name",
    "Tax ID":                               "tax_id",
    "Industry (NAICS) Code":               "naics_code",
    "Petitioner City":                      "city",
    "Petitioner State":                     "state",
    "Petitioner Zip Code":                  "zip",
    "New Employment Approval":              "new_employment_approval",
    "New Employment Denial":               "new_employment_denial",
    "Continuation Approval":               "continuation_approval",
    "Continuation Denial":                 "continuation_denial",
    "Change with Same Employer Approval":  "change_same_employer_approval",
    "Change with Same Employer Denial":    "change_same_employer_denial",
    "New Concurrent Approval":             "new_concurrent_approval",
    "New Concurrent Denial":              "new_concurrent_denial",
    "Change of Employer Approval":         "change_of_employer_approval",
    "Change of Employer Denial":          "change_of_employer_denial",
    "Amended Approval":                    "amended_approval",
    "Amended Denial":                      "amended_denial",
}

_INT_COLS = [
    "fiscal_year",
    "new_employment_approval", "new_employment_denial",
    "continuation_approval",   "continuation_denial",
    "change_same_employer_approval", "change_same_employer_denial",
    "new_concurrent_approval", "new_concurrent_denial",
    "change_of_employer_approval",  "change_of_employer_denial",
    "amended_approval",        "amended_denial",
]


def _norm(name: str) -> str:
    """Normalize employer name for DOL join: uppercase, punctuation → space, collapse whitespace."""
    return re.sub(r"\s+", " ", re.sub(r"[^A-Z0-9 ]", " ", name.upper())).strip()


# ─────────────────────────────────────────────────────────────────────────────
# Load
# ─────────────────────────────────────────────────────────────────────────────

def load_file(path: str) -> pd.DataFrame:
    log.info("Loading %s …", path)
    ext = os.path.splitext(path)[1].lower()
    if ext in (".xlsx", ".xls"):
        df = pd.read_excel(path, dtype=str)
    else:
        df = pd.read_csv(path, dtype=str)

    log.info("Raw shape: %d rows × %d columns", len(df), len(df.columns))

    # Strip leading/trailing whitespace from column headers
    df.columns = [c.strip() for c in df.columns]

    # Validate required columns are present
    missing = [c for c in _COL_MAP if c not in df.columns]
    if missing:
        log.error("Missing expected columns: %s", missing)
        log.error("Columns found: %s", list(df.columns))
        sys.exit(1)

    # Keep only the columns we care about and rename to DB names
    df = df[list(_COL_MAP.keys())].rename(columns=_COL_MAP)

    # Strip whitespace from all string values
    for col in df.columns:
        df[col] = df[col].str.strip()

    # Drop rows with blank employer name or tax_id (unusable for join)
    before = len(df)
    df = df[df["employer_name"].notna() & (df["employer_name"] != "")]
    df = df[df["tax_id"].notna() & (df["tax_id"] != "")]
    dropped = before - len(df)
    if dropped:
        log.warning("Dropped %d rows with blank employer_name or tax_id", dropped)

    # Coerce integer columns (blank → 0)
    for col in _INT_COLS:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)

    # Derive normalized name
    df["employer_name_norm"] = df["employer_name"].apply(_norm)

    log.info("Loaded %d rows after filtering", len(df))
    years = sorted(df["fiscal_year"].unique())
    log.info("Fiscal years in file: %s", years)
    return df


# ─────────────────────────────────────────────────────────────────────────────
# Upsert
# ─────────────────────────────────────────────────────────────────────────────

def upsert(df: pd.DataFrame) -> None:
    conn = get_conn()
    try:
        inserted = updated = 0
        for row in df.itertuples(index=False):
            result = conn.execute("""
                INSERT INTO uscis_h1b_petitions (
                    employer_name, employer_name_norm, tax_id, fiscal_year,
                    naics_code, city, state, zip,
                    new_employment_approval,        new_employment_denial,
                    continuation_approval,          continuation_denial,
                    change_same_employer_approval,  change_same_employer_denial,
                    new_concurrent_approval,        new_concurrent_denial,
                    change_of_employer_approval,    change_of_employer_denial,
                    amended_approval,               amended_denial,
                    ingested_at
                ) VALUES (
                    %s, %s, %s, %s,
                    %s, %s, %s, %s,
                    %s, %s, %s, %s,
                    %s, %s, %s, %s,
                    %s, %s, %s, %s,
                    NOW()
                )
                ON CONFLICT (employer_name, tax_id, fiscal_year, naics_code, city, zip)
                DO UPDATE SET
                    employer_name_norm              = EXCLUDED.employer_name_norm,
                    new_employment_approval         = EXCLUDED.new_employment_approval,
                    new_employment_denial           = EXCLUDED.new_employment_denial,
                    continuation_approval           = EXCLUDED.continuation_approval,
                    continuation_denial             = EXCLUDED.continuation_denial,
                    change_same_employer_approval   = EXCLUDED.change_same_employer_approval,
                    change_same_employer_denial     = EXCLUDED.change_same_employer_denial,
                    new_concurrent_approval         = EXCLUDED.new_concurrent_approval,
                    new_concurrent_denial           = EXCLUDED.new_concurrent_denial,
                    change_of_employer_approval     = EXCLUDED.change_of_employer_approval,
                    change_of_employer_denial       = EXCLUDED.change_of_employer_denial,
                    amended_approval                = EXCLUDED.amended_approval,
                    amended_denial                  = EXCLUDED.amended_denial,
                    ingested_at                     = NOW()
            """, (
                row.employer_name, row.employer_name_norm, row.tax_id, row.fiscal_year,
                _str_or_none(row.naics_code), _str_or_none(row.city),
                _str_or_none(row.state),      _str_or_none(row.zip),
                row.new_employment_approval,       row.new_employment_denial,
                row.continuation_approval,         row.continuation_denial,
                row.change_same_employer_approval, row.change_same_employer_denial,
                row.new_concurrent_approval,       row.new_concurrent_denial,
                row.change_of_employer_approval,   row.change_of_employer_denial,
                row.amended_approval,              row.amended_denial,
            ))
            if result.rowcount == 1:
                inserted += 1
            else:
                updated += 1

        conn.commit()
        log.info("Done: %d inserted, %d updated", inserted, updated)
    finally:
        conn.close()


def _str_or_none(val) -> str | None:
    if val is None:
        return None
    s = str(val).strip()
    return s if s and s.lower() not in ("nan", "none") else None


# ─────────────────────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Ingest a USCIS H-1B Employer Data Hub CSV/Excel file")
    parser.add_argument("--file", required=True, help="Path to the downloaded CSV or Excel file")
    args = parser.parse_args()

    if not os.path.exists(args.file):
        log.error("File not found: %s", args.file)
        sys.exit(1)

    df = load_file(args.file)
    upsert(df)


if __name__ == "__main__":
    init_logging("process_uscis_h1b")
    main()
