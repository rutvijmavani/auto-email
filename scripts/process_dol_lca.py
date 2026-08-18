"""
scripts/process_dol_lca.py — DOL LCA quarterly file ingestion

Loads a DOL H-1B LCA Excel disclosure file, aggregates per employer FEIN,
and upserts into five tables:
  - dol_h1b_employers      (employer-level totals + metadata + wage rollup)
  - dol_h1b_soc_breakdown  (per employer × SOC code + wage aggregates)
  - dol_h1b_yearly         (per employer × year)
  - fein_domain_map        (domain frequency map per FEIN → assigned_domain)
  - lca_contacts           (one row per unique POC email — for pattern inference)

Usage:
    python scripts/process_dol_lca.py --file LCA_FY2026_Q2.xlsx --quarter FY2026_Q2

Design decisions (see docs/dol_h1b_pipeline.md, docs/email-pattern-inference.md):
  - Only H-1B visa class processed (not E-3, H-1B1 Chile/Singapore)
  - Rows with NULL/empty EMPLOYER_FEIN are skipped
  - Certified = CASE_STATUS in ('Certified', 'Certified-Withdrawn')
  - No SOC filtering — all roles stored, frontend filters
  - Quarter deduplication via quarters_processed array
  - Upserts are additive — re-running different quarters accumulates correctly
  - Wages normalized to annual equivalent (Hour×2080, Week×52, Bi-Weekly×26, Month×12, Year×1)
  - Generic email domains excluded from domain counting (gmail, yahoo, etc.)
  - fein_domain_map.low_confidence = TRUE when winning domain share < 70%
  - PREPARER_EMAIL and AGENT_ATTORNEY_EMAIL are NOT used (law firm contacts, not company)
"""

import argparse
import json
import tldextract
_tldextract = tldextract.TLDExtract(suffix_list_urls=())
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

# Stable lock ID for pg_advisory_xact_lock — prevents two concurrent upsert()
# calls from interleaving their read-merge-write cycles on the same FEINs.
_UPSERT_LOCK_ID = 0x70726F63_6573734C  # hex for "processL"

_GENERIC_DOMAINS = {
    "gmail.com", "yahoo.com", "hotmail.com", "outlook.com",
    "aol.com", "icloud.com", "protonmail.com", "live.com",
}

_GENERIC_LOCAL_PREFIXES = {
    "hr", "info", "immigration", "hrlegal", "legal", "recruiting",
    "recruitment", "talent", "jobs", "careers", "contact", "admin",
    "hiring", "staffing", "noreply", "no-reply",
}

_WAGE_ANNUAL_MULTIPLIER = {
    "hour":      2080,
    "week":      52,
    "bi-weekly": 26,
    "month":     12,
    "year":      1,
}

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

def _is_generic_email(local: str) -> bool:
    """Return True if the local part looks like a role address (hr@, info@, etc.)."""
    return local.lower().split("+")[0] in _GENERIC_LOCAL_PREFIXES


def _normalize_wage(value, unit: str) -> float | None:
    """Convert a wage value to annual equivalent. Returns None if unparseable."""
    import math
    try:
        amount = float(str(value).replace(",", "").strip())
    except (ValueError, TypeError):
        return None
    if not math.isfinite(amount):
        return None
    multiplier = _WAGE_ANNUAL_MULTIPLIER.get(unit.lower().strip() if unit else "", None)
    if multiplier is None:
        return None
    return round(amount * multiplier, 2)


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
    df["_fein"]        = df["EMPLOYER_FEIN"].str.strip()
    df["_certified"]   = df["CASE_STATUS"].str.strip().isin(CERTIFIED_STATUSES)
    df["_denied"]      = df["CASE_STATUS"].str.strip() == "Denied"
    df["_withdrawn"]   = df["CASE_STATUS"].str.strip() == "Withdrawn"
    df["_positions"]   = pd.to_numeric(df.get("TOTAL_WORKER_POSITIONS", 1), errors="coerce").fillna(1).astype(int)
    df["_soc_code"]    = df.get("SOC_CODE", pd.Series("", index=df.index)).str.strip().fillna("")
    df["_soc_title"]   = df.get("SOC_TITLE", pd.Series("", index=df.index)).str.strip().fillna("")
    df["_job_title"]   = df.get("JOB_TITLE", pd.Series("", index=df.index)).str.strip().fillna("")
    df["_year"]        = pd.to_datetime(df.get("DECISION_DATE"), errors="coerce").dt.year.fillna(0).astype(int)
    df["_wage_from"]   = df.get("WAGE_RATE_OF_PAY_FROM", pd.Series(dtype=str))
    df["_wage_to"]     = df.get("WAGE_RATE_OF_PAY_TO",   pd.Series(dtype=str))
    df["_wage_unit"]   = df.get("WAGE_UNIT_OF_PAY",      pd.Series(dtype=str)).fillna("")
    df["_poc_email"]   = df.get("EMPLOYER_POC_EMAIL",     pd.Series(dtype=str)).str.strip().fillna("")
    df["_poc_first"]   = df.get("EMPLOYER_POC_FIRST_NAME",  pd.Series(dtype=str)).str.strip().fillna("")
    df["_poc_middle"]  = df.get("EMPLOYER_POC_MIDDLE_NAME", pd.Series(dtype=str)).str.strip().fillna("")
    df["_poc_last"]    = df.get("EMPLOYER_POC_LAST_NAME",   pd.Series(dtype=str)).str.strip().fillna("")
    df["_poc_title"]   = df.get("EMPLOYER_POC_JOB_TITLE",   pd.Series(dtype=str)).str.strip().fillna("")
    df["_case_num"]    = df.get("CASE_NUMBER",   pd.Series(dtype=str)).str.strip().fillna("")
    df["_dec_date"]    = pd.to_datetime(df.get("DECISION_DATE"), errors="coerce")

    for fein, group in df.groupby("_fein"):
        # Most recent canonical name (last row in file for this FEIN)
        employer_name  = group["EMPLOYER_NAME"].iloc[-1].strip() if "EMPLOYER_NAME" in group else ""
        employer_state = group.get("EMPLOYER_STATE", pd.Series()).iloc[-1] if "EMPLOYER_STATE" in group else None
        employer_city  = group.get("EMPLOYER_CITY",  pd.Series()).iloc[-1] if "EMPLOYER_CITY"  in group else None
        naics_code     = group.get("NAICS_CODE",     pd.Series()).iloc[-1] if "NAICS_CODE"     in group else None
        trade_name_dba = _str_or_none(group["TRADE_NAME_DBA"].iloc[-1] if "TRADE_NAME_DBA" in group else None)
        h1b_dependent  = _parse_bool(group.get("H-1B_DEPENDENT",  pd.Series()).iloc[-1] if "H-1B_DEPENDENT"  in group else None)
        willful_viol   = _parse_bool(group.get("WILLFUL_VIOLATOR", pd.Series()).iloc[-1] if "WILLFUL_VIOLATOR" in group else None)
        if "EMPLOYER_POC_EMAIL" in group.columns and group["EMPLOYER_POC_EMAIL"].notna().any():
            _non_blank = group[group["EMPLOYER_POC_EMAIL"].notna() & (group["EMPLOYER_POC_EMAIL"].str.strip() != "")]
            if not _non_blank.empty:
                _poc_by_date = _non_blank.sort_values("_dec_date", ascending=False, na_position="last")
                poc_email_domain = _extract_email_domain(_poc_by_date["EMPLOYER_POC_EMAIL"].iloc[0])
            else:
                poc_email_domain = None
        else:
            poc_email_domain = None

        employer_name_norm  = _norm_name(employer_name, strip_dba=True)
        trade_name_dba_norm = _norm_name(trade_name_dba) if trade_name_dba else None
        # NOTE: if _norm_name behavior changed (DBA strip, THE prefix, AKA, UNIV),
        # existing dol_h1b_employers.employer_name_norm values are inconsistent.
        # Re-run a full ingest (delete + re-process all quarterly files) to backfill.

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

        # SOC breakdown + wage aggregates per SOC
        soc_data = {}
        for soc_code, soc_grp in group.groupby("_soc_code"):
            if not soc_code:
                continue
            wage_from_vals = []
            wage_to_vals   = []
            for _, row in soc_grp.iterrows():
                unit = row["_wage_unit"]
                wf = _normalize_wage(row["_wage_from"], unit)
                wt = _normalize_wage(row["_wage_to"],   unit)
                if wf is not None:
                    wage_from_vals.append(wf)
                if wt is not None:
                    wage_to_vals.append(wt)
            soc_data[soc_code] = {
                "soc_title":       soc_grp["_soc_title"].iloc[0],
                "total_filed":     len(soc_grp),
                "total_certified": int(soc_grp["_certified"].sum()),
                "total_positions": int(soc_grp["_positions"].sum()),
                "wage_from_min":   min(wage_from_vals)  if wage_from_vals else None,
                "wage_from_max":   max(wage_from_vals)  if wage_from_vals else None,
                "wage_from_sum":   sum(wage_from_vals)  if wage_from_vals else None,
                "wage_to_min":     min(wage_to_vals)    if wage_to_vals   else None,
                "wage_to_max":     max(wage_to_vals)    if wage_to_vals   else None,
                "wage_to_sum":     sum(wage_to_vals)    if wage_to_vals   else None,
                "wage_count":      len(wage_from_vals),
                "wage_to_count":   len(wage_to_vals),
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

        # Domain frequency map — count non-generic corporate email domains per FEIN
        domain_counts: dict[str, int] = {}
        for email_raw in group["_poc_email"]:
            email_raw = str(email_raw).strip()
            if not email_raw or "@" not in email_raw:
                continue
            domain_part = email_raw.split("@")[-1].strip().lower()
            if not domain_part or domain_part in _GENERIC_DOMAINS:
                continue
            domain_counts[domain_part] = domain_counts.get(domain_part, 0) + 1
        total_emails = sum(domain_counts.values())
        root_totals: dict[str, int] = {}
        if domain_counts:
            # Group subdomains by PSL-aware registrable domain and sum counts.
            # ny.email.gs.com(3027) + gs.com(3) → gs.com(3030).
            # tldextract handles multi-label TLDs: acme.co.uk → acme.co.uk, not co.uk.
            for _d, _cnt in domain_counts.items():
                _ext  = _tldextract.extract(_d)
                _root = _ext.registered_domain or _d
                root_totals[_root] = root_totals.get(_root, 0) + _cnt
            assigned_domain = min(root_totals, key=lambda k: (-root_totals[k], k))
            confidence      = root_totals[assigned_domain] / total_emails
            low_confidence  = confidence < 0.70
        else:
            assigned_domain = None
            confidence      = None
            low_confidence  = False

        # Per-row POC contacts — one entry per unique email seen in this quarter
        poc_rows: dict[str, dict] = {}
        for _, row in group.iterrows():
            email_raw = str(row["_poc_email"]).strip().lower()
            if not email_raw or "@" not in email_raw:
                continue
            local, domain_part = email_raw.rsplit("@", 1)
            domain_part = domain_part.strip()
            if domain_part in _GENERIC_DOMAINS:
                continue
            is_generic = _is_generic_email(local)
            dec_date   = row["_dec_date"]
            poc_rows[email_raw] = {
                "email":           email_raw,
                "domain":          domain_part,
                "first_name":      row["_poc_first"]  or None,
                "middle_name":     row["_poc_middle"] or None,
                "last_name":       row["_poc_last"]   or None,
                "job_title":       row["_poc_title"]  or None,
                "employer_fein":   fein,
                "employer_name":   employer_name,
                "lca_case_number": row["_case_num"]   or None,
                "lca_quarter":     None,  # filled in upsert()
                "decision_date":   dec_date.date() if pd.notna(dec_date) else None,
                "is_generic":      is_generic,
            }

        # Employer-level wage rollup (across all SOC codes for this FEIN)
        all_from = [v for s in soc_data.values() if s["wage_from_min"] is not None
                    for v in ([s["wage_from_min"]] if s["wage_count"] > 0 else [])]
        all_to   = [s["wage_to_min"] for s in soc_data.values()
                    if s["wage_to_min"] is not None and s["wage_to_count"] > 0]
        wf_sum   = sum(s["wage_from_sum"] for s in soc_data.values() if s["wage_from_sum"] is not None)
        wt_sum   = sum(s["wage_to_sum"]   for s in soc_data.values() if s["wage_to_sum"]   is not None)
        wf_max   = max((s["wage_from_max"] for s in soc_data.values() if s["wage_from_max"] is not None), default=None)
        wt_max   = max((s["wage_to_max"]   for s in soc_data.values() if s["wage_to_max"]   is not None), default=None)
        w_count  = sum(s["wage_count"]    for s in soc_data.values())
        wt_count = sum(s["wage_to_count"] for s in soc_data.values())

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
                "poc_email_domain":    poc_email_domain,
                "total_filed":         total_filed,
                "total_certified":     total_certified,
                "total_denied":        total_denied,
                "total_withdrawn":     total_withdrawn,
                "total_positions":     total_positions,
                "certified_positions": cert_positions,
                "top_job_titles":      top_job_titles,
                "wage_from_min":       min(all_from) if all_from else None,
                "wage_from_max":       wf_max,
                "wage_from_avg":       round(wf_sum / w_count,  2) if w_count  > 0 else None,
                "wage_from_count":     w_count,
                "wage_to_min":         min(all_to)   if all_to   else None,
                "wage_to_max":         wt_max,
                "wage_to_avg":         round(wt_sum  / wt_count, 2) if wt_count > 0 else None,
                "wage_to_count":       wt_count,
            },
            "soc":          soc_data,
            "yearly":       yearly_data,
            "domain_map":   {
                "domain_counts":  root_totals,   # PSL-aware root → count (not raw emails)
                "total_emails":   total_emails,
                "assigned_domain": assigned_domain,
                "confidence":      confidence,
                "low_confidence":  low_confidence,
            },
            "poc_rows": poc_rows,
        }

    log.info("Aggregated %d unique employers (FEINs)", len(results))
    return results


def _extract_email_domain(raw) -> str | None:
    if not raw or (isinstance(raw, float) and pd.isna(raw)):
        return None
    email = str(raw).strip()
    if "@" not in email:
        return None
    domain = email.split("@")[-1].strip()
    return domain if domain else None


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
        # Serialize concurrent upsert calls — two parallel runs processing
        # different LCA files can overlap on the same FEIN; without this lock
        # the read-merge-write cycle is not atomic and one run's counts can
        # silently overwrite the other's.
        conn.execute("SELECT pg_advisory_xact_lock(%s)", (_UPSERT_LOCK_ID,))

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

        # Pre-fetch existing domain counts so the merge can be done in Python (once per fein)
        existing_domain_counts: dict = {}
        existing_email_totals:  dict = {}
        if feins:
            fdm_rows = conn.execute("""
                SELECT employer_fein, domain_counts, total_emails
                FROM fein_domain_map
                WHERE employer_fein = ANY(%s)
            """, (feins,)).fetchall()
            for _r in fdm_rows:
                existing_domain_counts[_r["employer_fein"]] = _r["domain_counts"] or {}
                existing_email_totals[_r["employer_fein"]]  = _r["total_emails"]  or 0

        emp_count = soc_count = year_count = poc_count = 0

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
                    top_job_titles, quarters_processed, poc_email_domain, last_updated,
                    wage_from_min, wage_from_max, wage_from_avg, wage_from_count,
                    wage_to_min,   wage_to_max,   wage_to_avg,   wage_to_count
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s,
                    %s, ARRAY[%s]::TEXT[], %s, NOW(),
                    %s, %s, %s, %s, %s, %s, %s, %s
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
                    poc_email_domain    = COALESCE(EXCLUDED.poc_email_domain, dol_h1b_employers.poc_email_domain),
                    last_updated        = NOW(),
                    wage_from_min       = LEAST(dol_h1b_employers.wage_from_min, EXCLUDED.wage_from_min),
                    wage_from_max       = GREATEST(dol_h1b_employers.wage_from_max, EXCLUDED.wage_from_max),
                    wage_from_avg       = CASE
                        WHEN EXCLUDED.wage_from_avg IS NOT NULL AND dol_h1b_employers.wage_from_avg IS NOT NULL
                        THEN (dol_h1b_employers.wage_from_avg * dol_h1b_employers.wage_from_count
                              + EXCLUDED.wage_from_avg * EXCLUDED.wage_from_count)
                             / NULLIF(dol_h1b_employers.wage_from_count + EXCLUDED.wage_from_count, 0)
                        WHEN EXCLUDED.wage_from_avg IS NOT NULL
                        THEN EXCLUDED.wage_from_avg
                        ELSE dol_h1b_employers.wage_from_avg
                    END,
                    wage_from_count     = COALESCE(dol_h1b_employers.wage_from_count, 0) + EXCLUDED.wage_from_count,
                    wage_to_min         = LEAST(dol_h1b_employers.wage_to_min, EXCLUDED.wage_to_min),
                    wage_to_max         = GREATEST(dol_h1b_employers.wage_to_max, EXCLUDED.wage_to_max),
                    wage_to_avg         = CASE
                        WHEN EXCLUDED.wage_to_avg IS NOT NULL AND dol_h1b_employers.wage_to_avg IS NOT NULL
                        THEN (dol_h1b_employers.wage_to_avg * dol_h1b_employers.wage_to_count
                              + EXCLUDED.wage_to_avg * EXCLUDED.wage_to_count)
                             / NULLIF(dol_h1b_employers.wage_to_count + EXCLUDED.wage_to_count, 0)
                        WHEN EXCLUDED.wage_to_avg IS NOT NULL
                        THEN EXCLUDED.wage_to_avg
                        ELSE dol_h1b_employers.wage_to_avg
                    END,
                    wage_to_count       = COALESCE(dol_h1b_employers.wage_to_count, 0) + EXCLUDED.wage_to_count
            """, (
                fein, e["employer_name"], e["employer_name_norm"],
                e["employer_city"], e["employer_state"],
                e["naics_code"], e["trade_name_dba"], e["trade_name_dba_norm"],
                e["h1b_dependent"], e["willful_violator"],
                e["total_filed"], e["total_certified"], e["total_denied"], e["total_withdrawn"],
                e["total_positions"], e["certified_positions"], approval_rate,
                json.dumps(merged_titles), quarter, e["poc_email_domain"],
                e["wage_from_min"], e["wage_from_max"], e["wage_from_avg"], e["wage_from_count"],
                e["wage_to_min"],   e["wage_to_max"],   e["wage_to_avg"],   e["wage_to_count"],
            ))
            emp_count += 1

            # SOC breakdown — additive upsert with wage aggregates
            for soc_code, s in data["soc"].items():
                conn.execute("""
                    INSERT INTO dol_h1b_soc_breakdown (
                        employer_fein, soc_code, soc_title,
                        total_filed, total_certified, total_positions,
                        wage_from_min, wage_from_max, wage_from_sum,
                        wage_to_min,   wage_to_max,   wage_to_sum,
                        wage_count, wage_to_count
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (employer_fein, soc_code) DO UPDATE SET
                        soc_title       = EXCLUDED.soc_title,
                        total_filed     = dol_h1b_soc_breakdown.total_filed     + EXCLUDED.total_filed,
                        total_certified = dol_h1b_soc_breakdown.total_certified + EXCLUDED.total_certified,
                        total_positions = dol_h1b_soc_breakdown.total_positions + EXCLUDED.total_positions,
                        wage_from_min   = LEAST(dol_h1b_soc_breakdown.wage_from_min, EXCLUDED.wage_from_min),
                        wage_from_max   = GREATEST(dol_h1b_soc_breakdown.wage_from_max, EXCLUDED.wage_from_max),
                        wage_from_sum   = COALESCE(dol_h1b_soc_breakdown.wage_from_sum, 0) + COALESCE(EXCLUDED.wage_from_sum, 0),
                        wage_to_min     = LEAST(dol_h1b_soc_breakdown.wage_to_min, EXCLUDED.wage_to_min),
                        wage_to_max     = GREATEST(dol_h1b_soc_breakdown.wage_to_max, EXCLUDED.wage_to_max),
                        wage_to_sum     = COALESCE(dol_h1b_soc_breakdown.wage_to_sum, 0) + COALESCE(EXCLUDED.wage_to_sum, 0),
                        wage_count      = COALESCE(dol_h1b_soc_breakdown.wage_count, 0)    + COALESCE(EXCLUDED.wage_count, 0),
                        wage_to_count   = COALESCE(dol_h1b_soc_breakdown.wage_to_count, 0) + COALESCE(EXCLUDED.wage_to_count, 0)
                """, (
                    fein, soc_code, s["soc_title"],
                    s["total_filed"], s["total_certified"], s["total_positions"],
                    s["wage_from_min"], s["wage_from_max"], s["wage_from_sum"],
                    s["wage_to_min"],   s["wage_to_max"],   s["wage_to_sum"],
                    s["wage_count"],    s["wage_to_count"],
                ))
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

            # fein_domain_map — merge domain count JSON with existing row.
            # Legacy rows with raw subdomain keys (e.g. email.gs.com) are re-rooted
            # to their registrable domain only when a later quarter re-processes that
            # FEIN; FEINs that never reappear retain raw subdomain keys indefinitely.
            dm = data["domain_map"]
            if dm["total_emails"] > 0:
                # Merge new domain counts with existing DB row in Python, then plain-upsert.
                # domain_counts stores PSL-aware registrable_domain → count (not raw emails).
                # No regex needed — keys are already roots (tldextract applied in Python).
                # Normalize keys from DB — rows written before PSL migration may have
                # raw subdomain keys (e.g. email.gs.com); re-root them so merging is correct.
                _raw_prev    = existing_domain_counts.get(fein, {})
                _prev_counts: dict = {}
                for _pk, _pv in _raw_prev.items():
                    _pext = _tldextract.extract(_pk)
                    _proot = _pext.registered_domain or _pk
                    _prev_counts[_proot] = _prev_counts.get(_proot, 0) + _pv
                _prev_total  = existing_email_totals.get(fein, 0)
                _merged: dict = dict(_prev_counts)
                for _dom, _cnt in dm["domain_counts"].items():
                    _merged[_dom] = _merged.get(_dom, 0) + _cnt
                _merged_total = _prev_total + dm["total_emails"]
                if _merged:
                    _assigned = min(_merged, key=lambda k: (-_merged[k], k))
                    _conf     = _merged[_assigned] / _merged_total
                    _low_conf = _conf < 0.70
                else:
                    _assigned = None
                    _conf     = None
                    _low_conf = False
                conn.execute("""
                    INSERT INTO fein_domain_map
                        (employer_fein, domain_counts, total_emails,
                         assigned_domain, confidence, low_confidence, updated_at)
                    VALUES (%s, %s::jsonb, %s, %s, %s, %s, NOW())
                    ON CONFLICT (employer_fein) DO UPDATE SET
                        domain_counts   = EXCLUDED.domain_counts,
                        total_emails    = EXCLUDED.total_emails,
                        assigned_domain = EXCLUDED.assigned_domain,
                        confidence      = EXCLUDED.confidence,
                        low_confidence  = EXCLUDED.low_confidence,
                        updated_at      = NOW()
                """, (
                    fein,
                    json.dumps(_merged),
                    _merged_total,
                    _assigned,
                    _conf,
                    _low_conf,
                ))

            # lca_contacts — one row per unique email, last filing wins on conflict
            for poc in data["poc_rows"].values():
                conn.execute("""
                    INSERT INTO lca_contacts
                        (email, domain, first_name, middle_name, last_name,
                         job_title, employer_fein, employer_name,
                         lca_case_number, lca_quarter, decision_date, is_generic)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (email) DO UPDATE SET
                        first_name      = EXCLUDED.first_name,
                        middle_name     = EXCLUDED.middle_name,
                        last_name       = EXCLUDED.last_name,
                        job_title       = EXCLUDED.job_title,
                        employer_fein   = EXCLUDED.employer_fein,
                        employer_name   = EXCLUDED.employer_name,
                        lca_case_number = EXCLUDED.lca_case_number,
                        lca_quarter     = EXCLUDED.lca_quarter,
                        decision_date   = EXCLUDED.decision_date,
                        is_generic      = EXCLUDED.is_generic
                    WHERE EXCLUDED.decision_date IS NOT NULL
                      AND (lca_contacts.decision_date IS NULL
                           OR EXCLUDED.decision_date > lca_contacts.decision_date)
                """, (
                    poc["email"], poc["domain"],
                    poc["first_name"], poc["middle_name"], poc["last_name"],
                    poc["job_title"], poc["employer_fein"], poc["employer_name"],
                    poc["lca_case_number"], quarter,
                    poc["decision_date"], poc["is_generic"],
                ))
                poc_count += 1

        conn.commit()
        log.info(
            "Upserted: %d employers, %d SOC rows, %d yearly rows, %d POC contacts (skipped %d already-processed)",
            emp_count, soc_count, year_count, poc_count, skipped,
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
