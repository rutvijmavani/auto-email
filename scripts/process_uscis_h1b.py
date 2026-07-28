"""
scripts/process_uscis_h1b.py — USCIS H-1B Employer Data Hub ingestion

Loads a locally downloaded USCIS H-1B petition CSV (or Excel) and upserts
each row into uscis_h1b_petitions.  One DB row per unique combination of
(employer_name, tax_id, fiscal_year, naics_code, city, zip) — exactly the
granularity published by USCIS.

Source: USCIS H-1B Employer Data Hub (FY2024-FY2026).
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
    """Normalize employer name: uppercase, strip & and AND, punctuation → space, collapse whitespace."""
    name = name.upper()
    name = re.sub(r"&", " ", name)
    name = re.sub(r"\bAND\b", " ", name)
    return re.sub(r"\s+", " ", re.sub(r"[^A-Z0-9 ]", " ", name)).strip()


_SPACED_LETTER_RE = re.compile(r"\b([A-Z])(?: ([A-Z]))+\b")


def _collapse_spaced_letters(name: str) -> str:
    """Collapse sequences of space-separated single uppercase letters: 'U S' → 'US', 'L L C' → 'LLC'."""
    return _SPACED_LETTER_RE.sub(lambda m: m.group(0).replace(" ", ""), name)


_ABBREV_MAP = [
    (re.compile(r"\bSVCS\b"),     "SERVICES"),
    (re.compile(r"\bSRVCS\b"),    "SERVICES"),
    (re.compile(r"\bTECHNOL\b"),  "TECHNOLOGIES"),
    (re.compile(r"\bINTL\b"),     "INTERNATIONAL"),
    (re.compile(r"\bMGMT\b"),     "MANAGEMENT"),
    (re.compile(r"\bMGT\b"),      "MANAGEMENT"),
    (re.compile(r"\bNATL\b"),     "NATIONAL"),
    (re.compile(r"\bMFG\b"),      "MANUFACTURING"),
    (re.compile(r"\bGRP\b"),      "GROUP"),
    (re.compile(r"\bLTD\b"),      "LIMITED"),
    (re.compile(r"\bPWC\b"),      "PRICEWATERHOUSECOOPERS"),
]


def _expand_abbrevs(name: str) -> str:
    """Expand common H-1B abbreviations to canonical full-word forms."""
    for pattern, replacement in _ABBREV_MAP:
        name = pattern.sub(replacement, name)
    return re.sub(r"\s+", " ", name).strip()


def _legal_norm(name: str) -> str:
    """Normalize for DOL join: strip DBA suffix, collapse spaced letters, expand abbreviations.

    "FIDELITY TECHNOLOGY GROUP LLC D B A FIDELITY INVESTMENTS"
        → "FIDELITY TECHNOLOGY GROUP LLC"
    "TATA CONSULTANCY SVCS LTD" → "TATA CONSULTANCY SERVICES LIMITED"
    "ERNST AND YOUNG U S LLP" → "ERNST YOUNG US LLP"
    """
    normed = _norm(name)
    # Detect DBA before collapsing — _collapse_spaced_letters would turn 'D B A' → 'DBA'
    dba_pos = normed.find(" D B A ")
    if dba_pos != -1:
        normed = normed[:dba_pos]
    normed = _collapse_spaced_letters(normed)
    normed = _expand_abbrevs(normed)
    return normed.strip()


# ─────────────────────────────────────────────────────────────────────────────
# Load
# ─────────────────────────────────────────────────────────────────────────────

def _detect_encoding(path: str) -> str:
    """Sniff BOM to handle Tableau CSV exports (UTF-16 LE) and UTF-8-BOM files."""
    with open(path, "rb") as f:
        bom = f.read(4)
    if bom[:2] in (b"\xff\xfe", b"\xfe\xff"):
        return "utf-16"
    if bom[:3] == b"\xef\xbb\xbf":
        return "utf-8-sig"
    return "utf-8"


def _detect_sep(path: str, encoding: str) -> str:
    """Sniff delimiter from the first line — Tableau TSVs are often saved as .csv."""
    with open(path, encoding=encoding, errors="replace") as f:
        first_line = f.readline()
    tabs   = first_line.count("\t")
    commas = first_line.count(",")
    return "\t" if tabs > commas else ","


def load_file(path: str) -> pd.DataFrame:
    log.info("Loading %s …", path)
    ext = os.path.splitext(path)[1].lower()
    if ext in (".xlsx", ".xls"):
        df = pd.read_excel(path, dtype=str)
    else:
        encoding = _detect_encoding(path)
        sep      = _detect_sep(path, encoding)
        log.info("Detected encoding=%s sep=%r", encoding, sep)
        df = pd.read_csv(path, dtype=str, encoding=encoding, sep=sep)

    log.info("Raw shape: %d rows x %d columns", len(df), len(df.columns))

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

    # Coerce integer columns; blank petition counts default to 0.
    # Strip thousands-separator commas first ("1,447" → "1447") — USCIS CSV uses
    # comma formatting for large numbers which pd.to_numeric cannot parse as-is.
    for col in _INT_COLS:
        df[col] = df[col].str.replace(",", "", regex=False)
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)

    # Reject rows where fiscal_year resolved to 0 (was blank or non-numeric)
    before = len(df)
    df = df[df["fiscal_year"] > 0]
    invalid_fy = before - len(df)
    if invalid_fy:
        log.warning("Dropped %d rows with invalid fiscal_year (blank or non-numeric)", invalid_fy)

    # Reject rows with any negative petition count (data corruption indicator)
    _count_cols = [c for c in _INT_COLS if c != "fiscal_year"]
    neg_mask = (df[_count_cols] < 0).any(axis=1)
    if neg_mask.any():
        log.warning("Dropped %d rows with negative petition counts", int(neg_mask.sum()))
        df = df[~neg_mask]

    # Derive normalized names.
    # employer_name_norm:  full name after norm + collapse + expand (no DBA strip) — used for DBA brand match.
    # employer_legal_norm: legal name only (DBA stripped) + collapse + expand — used for DOL legal name match.
    df["employer_name_norm"]  = df["employer_name"].apply(
        lambda n: _expand_abbrevs(_collapse_spaced_letters(_norm(n)))
    )
    df["employer_legal_norm"] = df["employer_name"].apply(_legal_norm)

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
        processed = 0
        for row in df.itertuples(index=False):
            # naics_code, city, zip are part of the unique key — use empty string
            # instead of NULL so PostgreSQL equality comparisons work on re-runs.
            # (NULL != NULL in PG; two rows with NULL naics_code would not conflict.)
            conn.execute("""
                INSERT INTO uscis_h1b_petitions (
                    employer_name, employer_name_norm, employer_legal_norm, tax_id, fiscal_year,
                    naics_code, city, state, zip,
                    new_employment_approval,        new_employment_denial,
                    continuation_approval,          continuation_denial,
                    change_same_employer_approval,  change_same_employer_denial,
                    new_concurrent_approval,        new_concurrent_denial,
                    change_of_employer_approval,    change_of_employer_denial,
                    amended_approval,               amended_denial,
                    ingested_at
                ) VALUES (
                    %s, %s, %s, %s, %s,
                    %s, %s, %s, %s,
                    %s, %s, %s, %s,
                    %s, %s, %s, %s,
                    %s, %s, %s, %s,
                    NOW()
                )
                ON CONFLICT (employer_name, tax_id, fiscal_year, naics_code, city, zip)
                DO UPDATE SET
                    employer_name_norm              = EXCLUDED.employer_name_norm,
                    employer_legal_norm             = EXCLUDED.employer_legal_norm,
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
                row.employer_name, row.employer_name_norm, row.employer_legal_norm, row.tax_id, row.fiscal_year,
                _str_or_empty(row.naics_code), _str_or_empty(row.city),
                _str_or_none(row.state),       _str_or_empty(row.zip),
                row.new_employment_approval,       row.new_employment_denial,
                row.continuation_approval,         row.continuation_denial,
                row.change_same_employer_approval, row.change_same_employer_denial,
                row.new_concurrent_approval,       row.new_concurrent_denial,
                row.change_of_employer_approval,   row.change_of_employer_denial,
                row.amended_approval,              row.amended_denial,
            ))
            processed += 1

        conn.commit()
        log.info("Done: %d rows processed", processed)
    finally:
        conn.close()


# ─────────────────────────────────────────────────────────────────────────────
# Unmatched diagnostics
# ─────────────────────────────────────────────────────────────────────────────

def populate_unmatched() -> None:
    """
    Refresh uscis_dol_unmatched with every USCIS petition row that has no
    matching row in dol_h1b_employers (normalized name + last 4 FEIN join).
    TRUNCATE + INSERT so the table always reflects the current DB state.
    Ideally 0 rows — any row is a join key anomaly to investigate.
    """
    conn = get_conn()
    try:
        conn.execute("TRUNCATE TABLE uscis_dol_unmatched")
        result = conn.execute("""
            INSERT INTO uscis_dol_unmatched
                (employer_name, employer_name_norm, tax_id, fiscal_year, state, total_approvals)
            SELECT
                u.employer_name,
                u.employer_name_norm,
                u.tax_id,
                u.fiscal_year,
                u.state,
                (u.new_employment_approval + u.continuation_approval +
                 u.change_same_employer_approval + u.new_concurrent_approval +
                 u.change_of_employer_approval  + u.amended_approval) AS total_approvals
            FROM uscis_h1b_petitions u
            LEFT JOIN dol_h1b_employers d
              ON (
                  u.employer_legal_norm = d.employer_name_norm
               OR u.employer_name_norm  = d.trade_name_dba_norm
              )
             AND right(d.employer_fein, 4) = u.tax_id
            WHERE d.employer_fein IS NULL
            ORDER BY total_approvals DESC
        """)
        count = result.rowcount
        conn.commit()
        if count == 0:
            log.info("Unmatched check: 0 rows — composite key matched all USCIS employers")
        else:
            log.warning("Unmatched check: %d rows written to uscis_dol_unmatched — investigate join key", count)
    finally:
        conn.close()


def _str_or_none(val) -> str | None:
    if val is None:
        return None
    s = str(val).strip()
    return s if s and s.lower() not in ("nan", "none") else None


def _str_or_empty(val) -> str:
    """Like _str_or_none but returns '' for missing/null values.
    Used for unique-key columns (naics_code, city, zip) so NULL != NULL
    in PostgreSQL does not prevent ON CONFLICT from matching on re-runs."""
    if val is None:
        return ""
    s = str(val).strip()
    return s if s and s.lower() not in ("nan", "none") else ""


# ─────────────────────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Ingest a USCIS H-1B Employer Data Hub CSV/Excel file")
    parser.add_argument("--file", help="Path to the downloaded CSV or Excel file")
    parser.add_argument(
        "--refresh-unmatched", action="store_true",
        help="Skip ingest — only rebuild uscis_dol_unmatched from current DB state",
    )
    args = parser.parse_args()

    if args.refresh_unmatched:
        populate_unmatched()
        return

    if not args.file:
        parser.error("--file is required unless --refresh-unmatched is set")

    if not os.path.exists(args.file):
        log.error("File not found: %s", args.file)
        sys.exit(1)

    df = load_file(args.file)
    upsert(df)
    populate_unmatched()


if __name__ == "__main__":
    init_logging("process_uscis_h1b")
    main()
