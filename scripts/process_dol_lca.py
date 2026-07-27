"""
scripts/process_dol_lca.py — DOL LCA quarterly file ingestion

Loads a DOL H-1B LCA Excel disclosure file, aggregates per employer FEIN,
and upserts into three tables:
  - dol_h1b_employers      (employer-level totals + metadata)
  - dol_h1b_soc_breakdown  (per employer × SOC code)
  - dol_h1b_yearly         (per employer × year)

Usage:
    python scripts/process_dol_lca.py --file LCA_FY2026_Q2.xlsx --quarter FY2026_Q2

Design decisions (see docs/dol_h1b_pipeline.md):
  - Only H-1B visa class processed (not E-3, H-1B1 Chile/Singapore)
  - Rows with NULL/empty EMPLOYER_FEIN are skipped
  - Certified = CASE_STATUS in ('Certified', 'Certified-Withdrawn')
  - No SOC filtering — all roles stored, frontend filters
  - Quarter deduplication via quarters_processed array
  - Upserts are additive — re-running different quarters accumulates correctly
"""

import argparse
import json
import os
import sys
from collections import defaultdict

import pandas as pd

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from db.connection import get_conn
from logger import get_logger, init_logging

log = get_logger(__name__)

CERTIFIED_STATUSES = {"Certified", "Certified-Withdrawn"}


# ─────────────────────────────────────────────────────────────────────────────
# Load + filter
# ─────────────────────────────────────────────────────────────────────────────

def load_file(path: str) -> pd.DataFrame:
    log.info("Loading %s …", path)
    df = pd.read_excel(path, dtype=str)
    log.info("Loaded %d rows, %d columns", len(df), len(df.columns))

    # Normalise column names (strip whitespace, upper)
    df.columns = [c.strip().upper().replace(" ", "_") for c in df.columns]

    # Filter to H-1B only
    before = len(df)
    df = df[df["VISA_CLASS"].str.strip().str.upper() == "H-1B"]
    log.info("After H-1B filter: %d rows (dropped %d)", len(df), before - len(df))

    # Drop rows with missing FEIN
    before = len(df)
    df = df[df["EMPLOYER_FEIN"].notna() & (df["EMPLOYER_FEIN"].str.strip() != "")]
    log.info("After FEIN filter: %d rows (dropped %d)", len(df), before - len(df))

    return df


# ─────────────────────────────────────────────────────────────────────────────
# Aggregate
# ─────────────────────────────────────────────────────────────────────────────

def aggregate(df: pd.DataFrame) -> dict:
    """
    Returns a dict keyed by FEIN with three sub-dicts:
      employer   — employer-level totals and metadata
      soc        — {soc_code: {filed, certified, positions, soc_title}}
      yearly     — {year: {filed, certified, denied, withdrawn, positions}}
    """
    results = {}

    # Precompute derived columns once
    df = df.copy()
    df["_fein"]       = df["EMPLOYER_FEIN"].str.strip()
    df["_certified"]  = df["CASE_STATUS"].str.strip().isin(CERTIFIED_STATUSES)
    df["_denied"]     = df["CASE_STATUS"].str.strip() == "Denied"
    df["_withdrawn"]  = df["CASE_STATUS"].str.strip() == "Withdrawn"
    df["_positions"]  = pd.to_numeric(df.get("TOTAL_WORKER_POSITIONS", 1), errors="coerce").fillna(1).astype(int)
    df["_soc_code"]   = df.get("SOC_CODE", pd.Series("", index=df.index)).str.strip().fillna("")
    df["_soc_title"]  = df.get("SOC_TITLE", pd.Series("", index=df.index)).str.strip().fillna("")
    df["_job_title"]  = df.get("JOB_TITLE", pd.Series("", index=df.index)).str.strip().fillna("")
    df["_year"]       = pd.to_datetime(df.get("DECISION_DATE"), errors="coerce").dt.year.fillna(0).astype(int)

    for fein, group in df.groupby("_fein"):
        # Most recent canonical name (last row in file for this FEIN)
        employer_name  = group["EMPLOYER_NAME"].iloc[-1].strip() if "EMPLOYER_NAME" in group else ""
        employer_state = group.get("EMPLOYER_STATE", pd.Series()).iloc[-1] if "EMPLOYER_STATE" in group else None
        employer_city  = group.get("EMPLOYER_CITY",  pd.Series()).iloc[-1] if "EMPLOYER_CITY"  in group else None
        naics_code     = group.get("NAICS_CODE",     pd.Series()).iloc[-1] if "NAICS_CODE"     in group else None
        h1b_dependent  = _parse_bool(group.get("H-1B_DEPENDENT",  pd.Series()).iloc[-1] if "H-1B_DEPENDENT"  in group else None)
        willful_viol   = _parse_bool(group.get("WILLFUL_VIOLATOR", pd.Series()).iloc[-1] if "WILLFUL_VIOLATOR" in group else None)

        total_filed     = len(group)
        total_certified = int(group["_certified"].sum())
        total_denied    = int(group["_denied"].sum())
        total_withdrawn = int(group["_withdrawn"].sum())
        total_positions = int(group["_positions"].sum())
        cert_positions  = int(group.loc[group["_certified"], "_positions"].sum())

        # Top 15 job titles (for display JSONB)
        title_counts = (
            group["_job_title"]
            .value_counts()
            .head(15)
        )
        top_job_titles = [
            {"title": t, "count": int(c)}
            for t, c in title_counts.items()
            if t
        ]

        # SOC breakdown
        soc_data = {}
        for soc_code, soc_grp in group.groupby("_soc_code"):
            if not soc_code:
                continue
            soc_data[soc_code] = {
                "soc_title":       soc_grp["_soc_title"].iloc[0],
                "total_filed":     len(soc_grp),
                "total_certified": int(soc_grp["_certified"].sum()),
                "total_positions": int(soc_grp["_positions"].sum()),
            }

        # Yearly breakdown
        yearly_data = {}
        for year, yr_grp in group.groupby("_year"):
            if year == 0:
                continue
            yearly_data[int(year)] = {
                "filed":     len(yr_grp),
                "certified": int(yr_grp["_certified"].sum()),
                "denied":    int(yr_grp["_denied"].sum()),
                "withdrawn": int(yr_grp["_withdrawn"].sum()),
                "positions": int(yr_grp["_positions"].sum()),
            }

        results[fein] = {
            "employer": {
                "employer_fein":       fein,
                "employer_name":       employer_name,
                "employer_state":      _str_or_none(employer_state),
                "employer_city":       _str_or_none(employer_city),
                "naics_code":          _str_or_none(naics_code),
                "h1b_dependent":       h1b_dependent,
                "willful_violator":    willful_viol,
                "total_filed":         total_filed,
                "total_certified":     total_certified,
                "total_denied":        total_denied,
                "total_withdrawn":     total_withdrawn,
                "total_positions":     total_positions,
                "certified_positions": cert_positions,
                "top_job_titles":      top_job_titles,
            },
            "soc":    soc_data,
            "yearly": yearly_data,
        }

    log.info("Aggregated %d unique employers (FEINs)", len(results))
    return results


def _parse_bool(val) -> bool | None:
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    return str(val).strip().upper() == "Y"


def _str_or_none(val) -> str | None:
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    s = str(val).strip()
    return s if s else None


# ─────────────────────────────────────────────────────────────────────────────
# Upsert
# ─────────────────────────────────────────────────────────────────────────────

def upsert(aggregated: dict, quarter: str) -> None:
    conn = get_conn()
    try:
        # Check which FEINs already have this quarter processed
        feins = list(aggregated.keys())
        existing = {}
        if feins:
            rows = conn.execute("""
                SELECT employer_fein, quarters_processed
                FROM dol_h1b_employers
                WHERE employer_fein = ANY(%s)
            """, (feins,)).fetchall()
            for r in rows:
                existing[r["employer_fein"]] = r["quarters_processed"] or []

        skipped = sum(1 for fein in feins if quarter in existing.get(fein, []))
        if skipped:
            log.warning("%d employers already have quarter %s — will skip their rows", skipped, quarter)

        emp_count = soc_count = year_count = 0

        for fein, data in aggregated.items():
            # Skip if this quarter was already processed for this FEIN
            if quarter in existing.get(fein, []):
                continue

            e = data["employer"]
            approval_rate = (e["total_certified"] / e["total_filed"]) if e["total_filed"] > 0 else None

            conn.execute("""
                INSERT INTO dol_h1b_employers (
                    employer_fein, employer_name, employer_city, employer_state,
                    naics_code, h1b_dependent, willful_violator,
                    total_filed, total_certified, total_denied, total_withdrawn,
                    total_positions, certified_positions, approval_rate,
                    top_job_titles, quarters_processed, last_updated
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s,
                    %s, ARRAY[%s]::TEXT[], NOW()
                )
                ON CONFLICT (employer_fein) DO UPDATE SET
                    employer_name       = EXCLUDED.employer_name,
                    employer_city       = EXCLUDED.employer_city,
                    employer_state      = EXCLUDED.employer_state,
                    naics_code          = EXCLUDED.naics_code,
                    h1b_dependent       = EXCLUDED.h1b_dependent,
                    willful_violator    = EXCLUDED.willful_violator,
                    total_filed         = dol_h1b_employers.total_filed         + EXCLUDED.total_filed,
                    total_certified     = dol_h1b_employers.total_certified     + EXCLUDED.total_certified,
                    total_denied        = dol_h1b_employers.total_denied        + EXCLUDED.total_denied,
                    total_withdrawn     = dol_h1b_employers.total_withdrawn     + EXCLUDED.total_withdrawn,
                    total_positions     = dol_h1b_employers.total_positions     + EXCLUDED.total_positions,
                    certified_positions = dol_h1b_employers.certified_positions + EXCLUDED.certified_positions,
                    approval_rate       = CASE
                        WHEN (dol_h1b_employers.total_filed + EXCLUDED.total_filed) > 0
                        THEN (dol_h1b_employers.total_certified + EXCLUDED.total_certified)::REAL
                             / (dol_h1b_employers.total_filed + EXCLUDED.total_filed)
                        ELSE NULL
                    END,
                    top_job_titles      = EXCLUDED.top_job_titles,
                    quarters_processed  = dol_h1b_employers.quarters_processed || EXCLUDED.quarters_processed,
                    last_updated        = NOW()
            """, (
                fein, e["employer_name"], e["employer_city"], e["employer_state"],
                e["naics_code"], e["h1b_dependent"], e["willful_violator"],
                e["total_filed"], e["total_certified"], e["total_denied"], e["total_withdrawn"],
                e["total_positions"], e["certified_positions"], approval_rate,
                json.dumps(e["top_job_titles"]), quarter,
            ))
            emp_count += 1

            # SOC breakdown — additive upsert
            for soc_code, s in data["soc"].items():
                conn.execute("""
                    INSERT INTO dol_h1b_soc_breakdown
                        (employer_fein, soc_code, soc_title, total_filed, total_certified, total_positions)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (employer_fein, soc_code) DO UPDATE SET
                        soc_title       = EXCLUDED.soc_title,
                        total_filed     = dol_h1b_soc_breakdown.total_filed     + EXCLUDED.total_filed,
                        total_certified = dol_h1b_soc_breakdown.total_certified + EXCLUDED.total_certified,
                        total_positions = dol_h1b_soc_breakdown.total_positions + EXCLUDED.total_positions
                """, (fein, soc_code, s["soc_title"], s["total_filed"], s["total_certified"], s["total_positions"]))
                soc_count += 1

            # Yearly breakdown — additive upsert
            for year, y in data["yearly"].items():
                conn.execute("""
                    INSERT INTO dol_h1b_yearly
                        (employer_fein, year, filed, certified, denied, withdrawn, positions)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (employer_fein, year) DO UPDATE SET
                        filed     = dol_h1b_yearly.filed     + EXCLUDED.filed,
                        certified = dol_h1b_yearly.certified + EXCLUDED.certified,
                        denied    = dol_h1b_yearly.denied    + EXCLUDED.denied,
                        withdrawn = dol_h1b_yearly.withdrawn + EXCLUDED.withdrawn,
                        positions = dol_h1b_yearly.positions + EXCLUDED.positions
                """, (fein, year, y["filed"], y["certified"], y["denied"], y["withdrawn"], y["positions"]))
                year_count += 1

        conn.commit()
        log.info(
            "Upserted: %d employers, %d SOC rows, %d yearly rows (skipped %d already-processed)",
            emp_count, soc_count, year_count, skipped,
        )
    finally:
        conn.close()


# ─────────────────────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Ingest a DOL LCA quarterly Excel file")
    parser.add_argument("--file",    required=True, help="Path to the .xlsx file")
    parser.add_argument("--quarter", required=True, help="Quarter identifier, e.g. FY2026_Q2")
    args = parser.parse_args()

    if not os.path.exists(args.file):
        log.error("File not found: %s", args.file)
        sys.exit(1)

    df          = load_file(args.file)
    aggregated  = aggregate(df)
    upsert(aggregated, args.quarter)
    log.info("Done — quarter %s ingested successfully", args.quarter)


if __name__ == "__main__":
    init_logging("process_dol_lca")
    main()
