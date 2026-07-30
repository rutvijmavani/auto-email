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
import re
import sys
from collections import defaultdict

import pandas as pd

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from db.connection import get_conn
from logger import get_logger, init_logging

log = get_logger(__name__)

CERTIFIED_STATUSES = {"Certified", "Certified-Withdrawn"}

_SPACED_LETTER_RE = re.compile(r"\b([A-Z])(?: ([A-Z]))+\b")
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
    (re.compile(r"\bUNIV\b"),     "UNIVERSITY"),
]


def _norm_name(name: str, strip_dba: bool = False) -> str:
    """Normalize a DOL employer name to the same form as USCIS employer_legal_norm.

    Transform chain (mirrors process_uscis_h1b._legal_norm() when strip_dba=True):
      strip apostrophes → strip & and AND → strip punctuation → collapse whitespace →
      strip leading THE → optionally strip DBA/AKA suffix →
      collapse spaced letters (U S → US) → expand abbreviations (SVCS → SERVICES).

    strip_dba=True for employer_name (legal name, DBA suffix must be removed to match
    USCIS employer_legal_norm). strip_dba=False for trade_name_dba (the DBA IS the brand name).
    """
    if not name or not name.strip():
        return ""
    name = name.upper()
    name = re.sub(r"'", "", name)           # strip apostrophes: "Moody's" → "MOODYS"
    name = re.sub(r"&", " ", name)
    name = re.sub(r"\bAND\b", " ", name)
    name = re.sub(r"[^A-Z0-9 ]", " ", name)
    name = re.sub(r"\s+", " ", name).strip()
    # Strip leading "THE " (DOL may include it; USCIS often drops it)
    if name.startswith("THE "):
        name = name[4:]
    if strip_dba:
        # Must detect DBA before collapsing — collapse turns ' D B A ' → 'DBA'
        for marker in (" D B A ", " DBA ", " AKA "):
            pos = name.find(marker)
            if pos != -1:
                name = name[:pos]
                break
    name = _SPACED_LETTER_RE.sub(lambda m: m.group(0).replace(" ", ""), name)
    for pat, repl in _ABBREV_MAP:
        name = pat.sub(repl, name)
    return re.sub(r"\s+", " ", name).strip()


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
        trade_name_dba = _str_or_none(group["TRADE_NAME_DBA"].iloc[-1] if "TRADE_NAME_DBA" in group else None)
        h1b_dependent  = _parse_bool(group.get("H-1B_DEPENDENT",  pd.Series()).iloc[-1] if "H-1B_DEPENDENT"  in group else None)
        willful_viol   = _parse_bool(group.get("WILLFUL_VIOLATOR", pd.Series()).iloc[-1] if "WILLFUL_VIOLATOR" in group else None)

        employer_name_norm  = _norm_name(employer_name, strip_dba=True)
        trade_name_dba_norm = _norm_name(trade_name_dba) if trade_name_dba else None

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
                "employer_name_norm":  employer_name_norm,
                "employer_state":      _str_or_none(employer_state),
                "employer_city":       _str_or_none(employer_city),
                "naics_code":          _str_or_none(naics_code),
                "trade_name_dba":      trade_name_dba,
                "trade_name_dba_norm": trade_name_dba_norm,
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


def _merge_job_titles(new_titles: list, existing_json) -> list:
    """Merge new quarter's title counts with existing DB counts; return top 15 sorted desc."""
    combined: dict[str, int] = {}
    if isinstance(existing_json, list):
        for item in existing_json:
            t = item.get("title", "") if isinstance(item, dict) else ""
            if t:
                combined[t] = combined.get(t, 0) + (item.get("count", 0) if isinstance(item, dict) else 0)
    for item in new_titles:
        t = item.get("title", "") if isinstance(item, dict) else ""
        if t:
            combined[t] = combined.get(t, 0) + (item.get("count", 0) if isinstance(item, dict) else 0)
    return [{"title": t, "count": c} for t, c in sorted(combined.items(), key=lambda x: -x[1])[:15]]


# ─────────────────────────────────────────────────────────────────────────────
# Upsert
# ─────────────────────────────────────────────────────────────────────────────

def upsert(aggregated: dict, quarter: str) -> None:
    conn = get_conn()
    try:
        # Check which FEINs already have this quarter processed
        feins = list(aggregated.keys())
        existing        = {}
        existing_titles = {}
        if feins:
            rows = conn.execute("""
                SELECT employer_fein, quarters_processed, top_job_titles
                FROM dol_h1b_employers
                WHERE employer_fein = ANY(%s)
            """, (feins,)).fetchall()
            for r in rows:
                existing[r["employer_fein"]]        = r["quarters_processed"] or []
                existing_titles[r["employer_fein"]] = r["top_job_titles"]

        skipped = sum(1 for fein in feins if quarter in existing.get(fein, []))
        if skipped:
            log.warning("%d employers already have quarter %s — will skip their rows", skipped, quarter)

        emp_count = soc_count = year_count = 0

        for fein, data in aggregated.items():
            # Skip if this quarter was already processed for this FEIN
            if quarter in existing.get(fein, []):
                continue

            e = data["employer"]
            approval_rate  = (e["total_certified"] / e["total_filed"]) if e["total_filed"] > 0 else None
            merged_titles  = _merge_job_titles(e["top_job_titles"], existing_titles.get(fein))

            conn.execute("""
                INSERT INTO dol_h1b_employers (
                    employer_fein, employer_name, employer_name_norm,
                    employer_city, employer_state,
                    naics_code, trade_name_dba, trade_name_dba_norm,
                    h1b_dependent, willful_violator,
                    total_filed, total_certified, total_denied, total_withdrawn,
                    total_positions, certified_positions, approval_rate,
                    top_job_titles, quarters_processed, last_updated
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s,
                    %s, ARRAY[%s]::TEXT[], NOW()
                )
                ON CONFLICT (employer_fein) DO UPDATE SET
                    employer_name       = EXCLUDED.employer_name,
                    employer_name_norm  = EXCLUDED.employer_name_norm,
                    employer_city       = EXCLUDED.employer_city,
                    employer_state      = EXCLUDED.employer_state,
                    naics_code          = EXCLUDED.naics_code,
                    trade_name_dba      = EXCLUDED.trade_name_dba,
                    trade_name_dba_norm = EXCLUDED.trade_name_dba_norm,
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
                fein, e["employer_name"], e["employer_name_norm"],
                e["employer_city"], e["employer_state"],
                e["naics_code"], e["trade_name_dba"], e["trade_name_dba_norm"],
                e["h1b_dependent"], e["willful_violator"],
                e["total_filed"], e["total_certified"], e["total_denied"], e["total_withdrawn"],
                e["total_positions"], e["certified_positions"], approval_rate,
                json.dumps(merged_titles), quarter,
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
