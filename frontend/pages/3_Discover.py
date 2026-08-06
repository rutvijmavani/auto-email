"""
frontend/pages/3_Discover.py — DOL H-1B employer browser.

Browse aggregated LCA disclosures to find companies worth adding to the pipeline.
All filtering is SQL-based with indexed columns; no pandas post-filtering.
"""

import concurrent.futures
import json
import logging
import re
import sys
import os
from urllib.parse import quote_plus

import pandas as pd
import streamlit as st

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))
from db.connection import get_conn
from db.prospective import add_prospective_company, submit_to_prospective_sheet
from frontend.db_utils import query as _query, SOURCE_LABELS as _SOURCE_LABELS

_URL_RE = re.compile(r'https?://[^\s"\'<>]+', re.IGNORECASE)
_INLINE_PROBE_TIMEOUT = 30  # seconds; caps career-page probe in inline discovery

log = logging.getLogger(__name__)

st.set_page_config(page_title="Discover", page_icon="🔎", layout="wide")

# ─────────────────────────────────────────────────────────────────────────────
# Data helpers
# ─────────────────────────────────────────────────────────────────────────────


@st.cache_data(ttl=3600)
def load_filter_options() -> dict:
    states = _query(
        "SELECT DISTINCT employer_state FROM dol_h1b_employers "
        "WHERE employer_state IS NOT NULL ORDER BY employer_state"
    )
    return {
        "states": states["employer_state"].tolist() if not states.empty else [],
    }


@st.cache_data(ttl=300)
def load_employers(
    search: str,
    state: str,
    naics_prefix: str,
    soc_prefix: str,
    h1b_dependent: bool,
    min_approval: float,
    min_certified: int,
    sort_by: str,
    limit: int,
) -> pd.DataFrame:
    clauses: list[str] = []
    params: list = []

    if search:
        clauses.append("e.employer_name ILIKE %s")
        params.append(f"%{search}%")

    if state:
        clauses.append("e.employer_state = %s")
        params.append(state)

    if naics_prefix:
        clauses.append("e.naics_code LIKE %s")
        params.append(f"{naics_prefix}%")

    if h1b_dependent:
        clauses.append("e.h1b_dependent = TRUE")

    if min_approval > 0:
        clauses.append("e.approval_rate >= %s")
        params.append(min_approval / 100.0)

    if min_certified > 0:
        clauses.append("e.total_certified >= %s")
        params.append(min_certified)

    if soc_prefix:
        clauses.append("""
            EXISTS (
                SELECT 1 FROM dol_h1b_soc_breakdown s
                WHERE s.employer_fein = e.employer_fein
                  AND s.soc_code LIKE %s
                  AND s.total_certified > 0
            )
        """)
        params.append(f"{soc_prefix}%")

    where = ("WHERE " + " AND ".join(clauses)) if clauses else ""

    order = {
        "Total certified ↓":  "e.total_certified DESC",
        "Approval rate ↓":    "e.approval_rate DESC NULLS LAST",
        "Total filed ↓":      "e.total_filed DESC",
        "Name A→Z":           "e.employer_name ASC",
    }.get(sort_by, "e.total_certified DESC")

    sql = f"""
        SELECT
            e.employer_fein,
            e.employer_name,
            e.employer_city,
            e.employer_state,
            e.naics_code,
            e.h1b_dependent,
            e.willful_violator,
            e.total_filed,
            e.total_certified,
            e.total_denied,
            e.total_positions,
            e.approval_rate,
            e.top_job_titles,
            array_length(e.quarters_processed, 1) AS quarters_count
        FROM dol_h1b_employers e
        {where}
        ORDER BY {order}
        LIMIT %s
    """
    params.append(limit)
    return _query(sql, params)


@st.cache_data(ttl=120)
def load_employer_yearly(fein: str) -> pd.DataFrame:
    return _query(
        "SELECT year, filed, certified, denied, withdrawn, positions "
        "FROM dol_h1b_yearly WHERE employer_fein = %s ORDER BY year",
        (fein,),
    )


@st.cache_data(ttl=120)
def load_employer_soc(fein: str) -> pd.DataFrame:
    return _query(
        "SELECT soc_code, soc_title, total_filed, total_certified, total_positions "
        "FROM dol_h1b_soc_breakdown WHERE employer_fein = %s "
        "ORDER BY total_certified DESC LIMIT 20",
        (fein,),
    )


@st.cache_data(ttl=300)
def load_uscis_petitions(fein: str, employer_name: str) -> pd.DataFrame:
    """
    Load USCIS H-1B petition rows for this employer, aggregated by fiscal year.
    Join key: (employer_legal_norm = dol.employer_name_norm) OR (employer_name_norm = dol.trade_name_dba_norm),
    constrained by last-4 FEIN. Handles both legal-name and brand-name USCIS entries.
    Returns one row per fiscal year; empty DataFrame if no match.
    """
    return _query("""
        SELECT
            u.fiscal_year,
            SUM(u.new_employment_approval)        AS new_employment_approval,
            SUM(u.new_employment_denial)          AS new_employment_denial,
            SUM(u.continuation_approval)          AS continuation_approval,
            SUM(u.continuation_denial)            AS continuation_denial,
            SUM(u.change_same_employer_approval)  AS change_same_employer_approval,
            SUM(u.change_same_employer_denial)    AS change_same_employer_denial,
            SUM(u.new_concurrent_approval)        AS new_concurrent_approval,
            SUM(u.new_concurrent_denial)          AS new_concurrent_denial,
            SUM(u.change_of_employer_approval)    AS change_of_employer_approval,
            SUM(u.change_of_employer_denial)      AS change_of_employer_denial,
            SUM(u.amended_approval)               AS amended_approval,
            SUM(u.amended_denial)                 AS amended_denial
        FROM uscis_h1b_petitions u
        JOIN dol_h1b_employers d ON d.employer_fein = %s
        LEFT JOIN uscis_dol_fuzzy_map f
          ON f.employer_legal_norm = COALESCE(NULLIF(u.employer_legal_norm, ''), u.employer_name_norm)
         AND f.tax_id              = u.tax_id
         AND f.dol_fein            = d.employer_fein
        WHERE u.tax_id = RIGHT(%s, 4)
          AND (
              u.employer_legal_norm = d.employer_name_norm
           OR u.employer_name_norm  = d.trade_name_dba_norm
           OR f.dol_fein IS NOT NULL
          )
        GROUP BY u.fiscal_year
        ORDER BY u.fiscal_year
    """, (fein, fein))


def _pipeline_status(employer_name: str, canonical_name: str | None = None) -> str | None:
    """Return existing pipeline status string, or None if not in pipeline.
    Checks both the raw DOL name and canonical_name (added via ATS panel).
    """
    names = [employer_name.strip()]
    if canonical_name and canonical_name not in ("—", employer_name.strip()):
        names.append(canonical_name.strip())
    conn = get_conn()
    try:
        cur = conn.cursor()
        placeholders = ", ".join(["%s"] * len(names))
        cur.execute(
            f"SELECT status FROM prospective_companies WHERE company IN ({placeholders})",
            names,
        )
        row = cur.fetchone()
        return dict(row)["status"] if row else None
    finally:
        conn.close()


@st.cache_data(ttl=120)
def load_ats_discovery(fein: str) -> dict | None:
    """Load ATS discovery row for a given employer FEIN."""
    df = _query(
        "SELECT * FROM h1b_ats_discovery WHERE employer_fein = %s", (fein,)
    )
    return df.to_dict("records")[0] if not df.empty else None


def _run_inline_discovery(fein: str, emp_name: str) -> dict | None:
    """
    Run a quick inline ATS discovery from the UI.
    KG API → Wikidata P10311 → career probe. No Qwen3/Brave.
    Writes result to h1b_ats_discovery and returns the row.
    """
    import time
    from scripts.discover_h1b_ats import (
        kg_search, _sparql_batch_p10311, strip_legal_suffixes,
        discover_careers_url, upsert_discovery,
    )

    # Phase 1: KG API → canonical name + website + Freebase MID
    kg             = kg_search(emp_name)
    kg_mid         = None
    canonical_name = None
    website_url    = None
    canonical_source = None
    if kg:
        kg_mid         = kg.get("kg_mid")
        canonical_name = kg.get("name") or None
        website_url    = kg.get("url") or None
        canonical_source = "kg_api" if (canonical_name or website_url) else None
    if not canonical_name:
        canonical_name   = strip_legal_suffixes(emp_name) or None
        canonical_source = canonical_source or ("regex" if canonical_name != emp_name else None)

    # Phase 2: Wikidata P646 → P10311 (official jobs URL)
    wikidata_qid = jobs_url = None
    if kg_mid:
        sparql_result = _sparql_batch_p10311([kg_mid])
        wd = sparql_result.get(kg_mid, {})
        wikidata_qid = wd.get("qid")
        jobs_url     = wd.get("jobs_url")

    # Phase 3: career probe (skip if P10311 found)
    careers_url = jobs_url  # use Wikidata jobs URL if available
    detected_platform = detected_slug = None
    probe_error = None
    if jobs_url:
        _hit = _match_ats_from_url(jobs_url)
        if _hit:
            detected_platform = _hit["platform"]
            detected_slug     = _hit.get("slug")
    if not careers_url and website_url:
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as ex:
            future = ex.submit(discover_careers_url, website_url)
            try:
                careers_url, detected_platform, detected_slug = future.result(
                    timeout=_INLINE_PROBE_TIMEOUT
                )
            except concurrent.futures.TimeoutError:
                probe_error = f"Career probe timed out after {_INLINE_PROBE_TIMEOUT}s"
                log.warning("Career-page probe timed out for %r", emp_name)
            except Exception as e:
                probe_error = str(e)
                log.warning("Career-page probe failed for %r: %s", emp_name, e)

    data = {
        "employer_fein":     fein,
        "employer_name":     emp_name,
        "canonical_name":    canonical_name,
        "canonical_source":  canonical_source,
        "wikidata_qid":      wikidata_qid,
        "kg_mid":            kg_mid,
        "website_url":       website_url,
        "jobs_url":          jobs_url,
        "careers_url":       careers_url,
        "detected_platform": detected_platform,
        "detected_slug":     detected_slug,
    }
    if probe_error:
        import streamlit as _st
        _st.warning(f"Careers probe failed: {probe_error}. Result saved as ATS unknown.")
    conn = get_conn()
    try:
        upsert_discovery(data, conn, dry_run=False)
    finally:
        conn.close()
    return data


def _match_ats_from_url(url: str) -> dict | None:
    """Run patterns.py on a user-pasted URL. Returns {platform, slug} or None."""
    try:
        from jobs.ats.patterns import match_ats_pattern
        return match_ats_pattern(url.strip())
    except Exception:
        return None


# ─────────────────────────────────────────────────────────────────────────────
# Sidebar
# ─────────────────────────────────────────────────────────────────────────────

with st.sidebar:
    st.title("🔎 Discover")
    st.divider()

    search       = st.text_input("Employer name", placeholder="e.g. Google")
    opts         = load_filter_options()
    state        = st.selectbox(
        "State",
        [""] + opts["states"],
        format_func=lambda x: "All states" if x == "" else x,
    )
    naics_prefix = st.text_input("NAICS prefix", placeholder="e.g. 54")
    soc_prefix   = st.text_input("SOC code prefix", placeholder="e.g. 15-1")

    st.divider()

    h1b_dependent = st.checkbox("H1B dependent only")
    min_approval  = st.slider("Min approval rate (%)", 0, 100, 0, step=5)
    min_certified = st.number_input("Min certified (all-time)", min_value=0, value=0, step=10)

    st.divider()

    sort_by = st.selectbox(
        "Sort by",
        ["Total certified ↓", "Approval rate ↓", "Total filed ↓", "Name A→Z"],
    )
    limit = st.selectbox("Max results", [100, 500, 1000, 2000], index=1)

    st.divider()
    if st.button("🔄 Refresh", use_container_width=True):
        st.cache_data.clear()
        st.rerun()


# ─────────────────────────────────────────────────────────────────────────────
# Load data
# ─────────────────────────────────────────────────────────────────────────────

df = load_employers(
    search=search,
    state=state,
    naics_prefix=naics_prefix,
    soc_prefix=soc_prefix,
    h1b_dependent=h1b_dependent,
    min_approval=float(min_approval),
    min_certified=int(min_certified),
    sort_by=sort_by,
    limit=int(limit),
)


# ─────────────────────────────────────────────────────────────────────────────
# Metrics
# ─────────────────────────────────────────────────────────────────────────────

m1, m2, m3, m4 = st.columns(4)
m1.metric("Employers shown", f"{len(df):,}")
m2.metric("States", df["employer_state"].nunique() if not df.empty else 0)
m3.metric(
    "Avg approval rate",
    f"{df['approval_rate'].mean() * 100:.1f}%"
    if not df.empty and df["approval_rate"].notna().any()
    else "—",
)
m4.metric(
    "Total certified",
    f"{int(df['total_certified'].sum()):,}" if not df.empty else "0",
)

st.divider()


# ─────────────────────────────────────────────────────────────────────────────
# Employer table
# ─────────────────────────────────────────────────────────────────────────────

if df.empty:
    st.info("No employers match the current filters.")
    st.stop()

display = df[[
    "employer_name", "employer_city", "employer_state", "naics_code",
    "total_filed", "total_certified", "approval_rate", "h1b_dependent", "quarters_count",
]].copy()

display["approval_rate"] = (display["approval_rate"] * 100).round(1)
display["h1b_dependent"] = display["h1b_dependent"].map({True: "✓", False: "", None: ""}).fillna("")

display.rename(columns={
    "employer_name":   "Employer",
    "employer_city":   "City",
    "employer_state":  "State",
    "naics_code":      "NAICS",
    "total_filed":     "Filed",
    "total_certified": "Certified",
    "approval_rate":   "Approval %",
    "h1b_dependent":   "H1B Dep.",
    "quarters_count":  "Qtrs",
}, inplace=True)

selected = st.dataframe(
    display,
    use_container_width=True,
    hide_index=True,
    on_select="rerun",
    selection_mode="single-row",
    column_config={
        "Approval %": st.column_config.NumberColumn("Approval %", format="%.1f%%"),
        "Filed":      st.column_config.NumberColumn("Filed",      format="%d"),
        "Certified":  st.column_config.NumberColumn("Certified",  format="%d"),
        "Qtrs":       st.column_config.NumberColumn("Qtrs",       format="%d"),
    },
)


# ─────────────────────────────────────────────────────────────────────────────
# Employer detail panel — shown when a row is selected
# ─────────────────────────────────────────────────────────────────────────────

sel_rows = selected.selection.rows if selected.selection else []
if not sel_rows:
    st.stop()

emp  = df.iloc[sel_rows[0]]
fein = emp["employer_fein"]
name = emp["employer_name"]

st.divider()
st.subheader(name)
st.caption(
    f"{emp['employer_city'] or '—'}, {emp['employer_state'] or '—'}"
    f"  ·  FEIN: `{fein}`"
    f"  ·  NAICS: `{emp['naics_code'] or '—'}`"
)

disc = load_ats_discovery(fein)
_canonical = (disc or {}).get("canonical_name") or None

col_chart, col_meta = st.columns([3, 1])

with col_chart:
    yearly = load_employer_yearly(fein)
    if not yearly.empty:
        chart_df = yearly.set_index("year")[["certified", "denied", "withdrawn"]]
        st.bar_chart(chart_df, color=["#2196F3", "#F44336", "#9E9E9E"])
        st.caption("Year-over-year LCA filings: certified / denied / withdrawn")
    else:
        st.info("No yearly breakdown available.")

with col_meta:
    rate     = emp["approval_rate"]
    h1b      = emp["h1b_dependent"]
    wv       = emp["willful_violator"]
    quarters = emp["quarters_count"]

    rate_str     = f"{rate * 100:.1f}%" if (rate is not None and not pd.isna(rate)) else "—"
    quarters_str = "0" if (quarters is None or pd.isna(quarters)) else str(int(quarters))

    st.markdown("**Summary**")
    st.markdown(f"Filed: **{emp['total_filed']:,}**")
    st.markdown(f"Certified: **{emp['total_certified']:,}**")
    st.markdown(f"Denied: **{emp['total_denied']:,}**")
    st.markdown(f"Positions: **{emp['total_positions']:,}**")
    st.markdown(f"Approval: **{rate_str}**")
    st.markdown(f"H1B dependent: **{'Yes' if h1b is True else ('No' if h1b is False else '—')}**")
    st.markdown(f"Willful violator: **{'⚠️ Yes' if wv else 'No'}**")
    st.markdown(f"Quarters: **{quarters_str}**")

    st.divider()

    existing_status = _pipeline_status(name, _canonical)
    if existing_status:
        st.success(f"In pipeline — {existing_status}")
    else:
        pipeline_name = _canonical if _canonical and _canonical != "—" else name
        if st.button("➕ Add to pipeline", use_container_width=True, type="primary"):
            try:
                inserted = add_prospective_company(pipeline_name, priority=0)
                if inserted:
                    st.success("Added to pipeline!")
                    st.rerun()
                else:
                    st.info("Already in pipeline.")
            except Exception as exc:
                log.exception("Failed to add %r to pipeline", name)
                st.error(f"Error: {exc}")

# SOC breakdown + top job titles
st.divider()
col_soc, col_titles = st.columns([3, 2])

with col_soc:
    st.markdown("**SOC breakdown** (top 20 by certified)")
    soc = load_employer_soc(fein)
    if not soc.empty:
        soc.rename(columns={
            "soc_code":        "SOC code",
            "soc_title":       "Title",
            "total_filed":     "Filed",
            "total_certified": "Certified",
            "total_positions": "Positions",
        }, inplace=True)
        st.dataframe(soc, use_container_width=True, hide_index=True)
    else:
        st.info("No SOC breakdown available.")

with col_titles:
    st.markdown("**Top job titles**")
    titles_raw = emp.get("top_job_titles")
    if titles_raw:
        if isinstance(titles_raw, str):
            try:
                titles_raw = json.loads(titles_raw)
            except json.JSONDecodeError:
                titles_raw = None

        if isinstance(titles_raw, dict):
            for title, count in sorted(titles_raw.items(), key=lambda x: -(int(x[1]) if x[1] is not None else 0))[:10]:
                try:
                    c_int = int(count)
                except (TypeError, ValueError):
                    c_int = None
                st.markdown(f"- {title} *({c_int:,})*" if c_int is not None else f"- {title}")
        elif isinstance(titles_raw, list):
            for item in titles_raw[:10]:
                if isinstance(item, dict):
                    t = item.get("title", "—")
                    c = item.get("count")
                    try:
                        c_int = int(c) if c is not None else None
                    except (TypeError, ValueError):
                        c_int = None
                    st.markdown(f"- {t} *({c_int:,})*" if c_int is not None else f"- {t}")
                else:
                    st.markdown(f"- {item}")
        else:
            st.info("No job title data available.")
    else:
        st.info("No job title data available.")


# ─────────────────────────────────────────────────────────────────────────────
# USCIS petition panel
# ─────────────────────────────────────────────────────────────────────────────

st.divider()
st.markdown("#### USCIS H-1B Petitions (FY2024-FY2026)")

uscis = load_uscis_petitions(fein, name)

if uscis.empty:
    st.info("No USCIS petition data found for this employer. Ingest FY2024-FY2026 files via `process_uscis_h1b.py` to populate.")
else:
    _approval_cols = [
        "new_employment_approval", "continuation_approval",
        "change_same_employer_approval", "new_concurrent_approval",
        "change_of_employer_approval", "amended_approval",
    ]
    _denial_cols = [
        "new_employment_denial", "continuation_denial",
        "change_same_employer_denial", "new_concurrent_denial",
        "change_of_employer_denial", "amended_denial",
    ]

    new_sponsorships = int((uscis["new_employment_approval"] + uscis["change_of_employer_approval"]).sum())
    total_approvals  = int(uscis[_approval_cols].sum().sum())
    total_denials    = int(uscis[_denial_cols].sum().sum())
    total_petitions  = total_approvals + total_denials
    uscis_rate       = f"{total_approvals / total_petitions * 100:.1f}%" if total_petitions > 0 else "—"

    u1, u2, u3, u4 = st.columns(4)
    u1.metric(
        "New sponsorships",
        f"{new_sponsorships:,}",
        help="New Employment + Change of Employer approvals — workers actively brought in on H1B",
    )
    u2.metric("Total approvals", f"{total_approvals:,}")
    u3.metric("Total petitions", f"{total_petitions:,}")
    u4.metric("USCIS approval rate", uscis_rate)

    # Stacked bar: group into 4 meaningful buckets per fiscal year
    chart_data = uscis.set_index("fiscal_year").assign(
        **{
            "Active sponsorship":  lambda d: d["new_employment_approval"] + d["change_of_employer_approval"],
            "Continuations":       lambda d: d["continuation_approval"],
            "Other approvals":     lambda d: d["new_concurrent_approval"] + d["change_same_employer_approval"] + d["amended_approval"],
            "Denials":             lambda d: d[_denial_cols].sum(axis=1),
        }
    )[["Active sponsorship", "Continuations", "Other approvals", "Denials"]]

    st.bar_chart(chart_data, color=["#2196F3", "#4CAF50", "#9E9E9E", "#F44336"])
    st.caption(
        "Active sponsorship = New Employment + Change of Employer approvals  ·  "
        "Continuations = renewals of existing H1B workers"
    )

    with st.expander("Full petition breakdown by year"):
        breakdown = uscis.set_index("fiscal_year").rename(columns={
            "new_employment_approval":       "New Emp ✓",
            "new_employment_denial":         "New Emp ✗",
            "continuation_approval":         "Continuation ✓",
            "continuation_denial":           "Continuation ✗",
            "change_same_employer_approval": "Same Emp Chg ✓",
            "change_same_employer_denial":   "Same Emp Chg ✗",
            "new_concurrent_approval":       "Concurrent ✓",
            "new_concurrent_denial":         "Concurrent ✗",
            "change_of_employer_approval":   "Chg Employer ✓",
            "change_of_employer_denial":     "Chg Employer ✗",
            "amended_approval":              "Amended ✓",
            "amended_denial":                "Amended ✗",
        })
        st.dataframe(breakdown, use_container_width=True)


# ─────────────────────────────────────────────────────────────────────────────
# ATS Discovery panel
# ─────────────────────────────────────────────────────────────────────────────

st.divider()
st.markdown("#### ATS Discovery")

if disc is None:
    st.info("No discovery data yet for this employer.")
    if st.button("Run ATS discovery", key="run_disc"):
        with st.spinner("Querying KG API + Wikidata + probing careers page …"):
            try:
                disc = _run_inline_discovery(fein, name)
                st.cache_data.clear()
                st.rerun()
            except Exception as exc:
                log.exception("Inline discovery failed for %r", name)
                st.error(f"Discovery failed: {exc}")
else:
    # ── Info row ─────────────────────────────────────────────────────────────
    d1, d2, d3, d4 = st.columns(4)

    canonical = disc.get("canonical_name") or "—"
    source    = disc.get("canonical_source") or ""
    source_label = _SOURCE_LABELS.get(source, source)
    d1.markdown(f"**Brand name**\n\n{canonical}")
    if source_label:
        d1.caption(f"Source: {source_label}")

    website = disc.get("website_url")
    if website:
        d2.markdown(f"**Website**\n\n[{website}]({website})")
    else:
        d2.markdown("**Website**\n\n—")

    careers  = disc.get("careers_url")
    jobs_url = disc.get("jobs_url")

    def _url_editor(
        container, title: str, db_col: str, key_prefix: str,
        current_val: str | None,
        display_md: str | None = None,
        caption: str | None = None,
    ) -> None:
        """Inline URL editor: show value + Edit button, or text input + Save/Cancel.

        current_val  — the actual URL stored in the DB (used in the text input).
        display_md   — optional markdown override for the read-only view; falls
                       back to a link on current_val, then "—".
        """
        _ALLOWED_COLS = frozenset({"careers_url", "jobs_url", "sample_apply_url"})
        if db_col not in _ALLOWED_COLS:
            raise ValueError(f"_url_editor: forbidden column {db_col!r}")
        _sk = f"{key_prefix}_{fein}"
        container.markdown(f"**{title}**")
        if st.session_state.get(_sk):
            new_val = container.text_input(
                title, value=current_val or "",
                key=f"input_{key_prefix}_{fein}",
                label_visibility="collapsed",
            )
            c1, c2 = container.columns(2)
            if c1.button("Save", key=f"save_{key_prefix}_{fein}", type="primary"):
                _to_save = new_val.strip() or None
                if _to_save and not _to_save.startswith(("http://", "https://")):
                    container.error("URL must start with http:// or https://")
                else:
                    conn = get_conn()
                    try:
                        conn.execute(
                            f"UPDATE h1b_ats_discovery SET {db_col} = %s WHERE employer_fein = %s",
                            (_to_save, fein),
                        )
                        conn.commit()
                    finally:
                        conn.close()
                    st.session_state.pop(_sk, None)
                    load_ats_discovery.clear()
                    st.rerun()
            if c2.button("Cancel", key=f"cancel_{key_prefix}_{fein}"):
                st.session_state.pop(_sk, None)
                st.rerun()
        else:
            shown = display_md or (f"[{current_val}]({current_val})" if current_val else "—")
            container.markdown(shown)
            if caption:
                container.caption(caption)
            if container.button("Edit", key=f"edit_{key_prefix}_btn_{fein}"):
                st.session_state[_sk] = True
                st.rerun()

    # ── Careers page (editable) ───────────────────────────────────────────────
    _url_editor(d3, "Careers page", "careers_url", "edit_careers", careers)

    # ── Official jobs URL (editable) ──────────────────────────────────────────
    if jobs_url and jobs_url != careers:
        _jobs_display_md = f"[{jobs_url}]({jobs_url})"
        _jobs_caption    = "Source: Wikidata P10311"
    elif jobs_url:
        _jobs_display_md = "*(same as careers page)*"
        _jobs_caption    = None
    else:
        _jobs_display_md = None
        _jobs_caption    = None
    _url_editor(d4, "Official jobs URL", "jobs_url", "edit_jobs",
                jobs_url, display_md=_jobs_display_md, caption=_jobs_caption)

    # ── Sample apply URL (one real listing URL for manual review) ────────────
    apply_col, _ = st.columns([2, 3])
    _url_editor(
        apply_col,
        "Sample apply URL",
        "sample_apply_url",
        "edit_apply",
        disc.get("sample_apply_url"),
        caption="Paste a real job listing URL from this ATS to confirm before adding to monitoring",
    )

    # ── Wikidata detail row ───────────────────────────────────────────────────
    if source == "wikidata":
        with st.expander("🌐 Wikidata details", expanded=False):
            wd_cols = st.columns(3)
            wd_cols[0].markdown(f"**Entity label**\n\n{canonical}")
            if website:
                wd_cols[1].markdown(f"**Official site (P856)**\n\n[{website}]({website})")
            else:
                wd_cols[1].markdown("**Official site (P856)**\n\n—")
            # Link to Wikidata search so user can inspect the entity directly
            wd_search = f"https://www.wikidata.org/w/index.php?search={quote_plus(canonical)}"
            wd_cols[2].markdown(f"**Wikidata**\n\n[Search on Wikidata ↗]({wd_search})")

    # ── Glassdoor + Crunchbase links ─────────────────────────────────────────
    glassdoor_id  = disc.get("glassdoor_id")
    crunchbase_id = disc.get("crunchbase_id")
    if glassdoor_id or crunchbase_id:
        ext_cols = st.columns(2)
        if glassdoor_id:
            gd_url = f"https://www.glassdoor.com/Overview/Working-at-EI_IE{glassdoor_id}.htm"
            ext_cols[0].markdown(f"**Glassdoor**\n\n[View on Glassdoor ↗]({gd_url})")
        if crunchbase_id:
            cb_url = f"https://www.crunchbase.com/organization/{crunchbase_id}"
            ext_cols[1].markdown(f"**Crunchbase**\n\n[View on Crunchbase ↗]({cb_url})")

    # ── ATS badge ────────────────────────────────────────────────────────────
    platform = disc.get("detected_platform")
    slug     = disc.get("detected_slug")
    monitored = disc.get("is_monitored", False)

    st.markdown("")  # spacing

    if platform:
        badge_col, action_col = st.columns([2, 3])
        badge_col.success(f"ATS detected: **{platform}**" + (f"  ·  slug: `{slug}`" if slug else ""))

        with action_col:
            pipeline_st = _pipeline_status(name, canonical if canonical != "—" else None)
            if monitored or pipeline_st:
                st.info(f"Already in pipeline — {pipeline_st or 'monitoring'}")
            else:
                if st.button("Add to monitoring", key="add_mon", type="primary"):
                    try:
                        pipeline_name = canonical if canonical != "—" else name
                        inserted = add_prospective_company(
                            pipeline_name,
                            priority=1,
                            domain=website,
                        )
                        # Feed URLs to the sheet only for new inserts.
                        if inserted:
                            try:
                                submit_to_prospective_sheet(
                                    company        = pipeline_name,
                                    career_page_url= disc.get("careers_url"),
                                    job_url        = disc.get("jobs_url"),
                                    domain         = website,
                                )
                            except Exception as _se:
                                log.warning("Sheet queue failed for %r: %s", pipeline_name, _se)
                                st.warning("ATS detection queuing failed — company was added to pipeline.")
                        # Mark is_monitored only after pipeline insert succeeds
                        conn = get_conn()
                        try:
                            cur = conn.cursor()
                            cur.execute(
                                "UPDATE h1b_ats_discovery SET is_monitored = TRUE "
                                "WHERE employer_fein = %s",
                                (fein,),
                            )
                            conn.commit()
                        finally:
                            conn.close()
                        load_ats_discovery.clear()
                        if inserted:
                            st.success("Added to pipeline! Queued for ATS detection.")
                        else:
                            st.info("Already in pipeline.")
                        st.rerun()
                    except Exception as exc:
                        log.exception("Failed to add %r to pipeline", name)
                        st.error(f"Error: {exc}")
    else:
        # No ATS detected — let user paste apply URL
        st.warning("ATS not detected from careers page. If you find the apply URL, paste it below.")

        paste_url = st.text_input(
            "Apply / job listing URL",
            placeholder="https://boards.greenhouse.io/stripe  or  https://stripe.wd1.myworkdayjobs.com/…",
            key="paste_ats_url",
        )

        if paste_url:
            matched = _match_ats_from_url(paste_url)
            if matched:
                p2 = matched["platform"]
                s2 = matched.get("slug")
                st.success(f"Detected: **{p2}**" + (f"  ·  slug: `{s2}`" if s2 else ""))

                if st.button("Confirm and add to monitoring", key="confirm_ats", type="primary"):
                    try:
                        pipeline_name = canonical if canonical != "—" else name
                        # Insert into pipeline first; only mark is_monitored on success
                        inserted = add_prospective_company(
                            pipeline_name,
                            priority=1,
                            domain=website,
                        )
                        if inserted:
                            try:
                                submit_to_prospective_sheet(
                                    company        = pipeline_name,
                                    career_page_url= disc.get("careers_url"),
                                    job_url        = paste_url,
                                    domain         = website,
                                )
                            except Exception as _se:
                                log.warning("Sheet queue failed for %r: %s", pipeline_name, _se)
                                st.warning("ATS detection queuing failed — company was added to pipeline.")
                        conn = get_conn()
                        try:
                            cur = conn.cursor()
                            cur.execute("""
                                UPDATE h1b_ats_discovery
                                SET detected_platform = %s,
                                    detected_slug     = %s,
                                    careers_url       = COALESCE(careers_url, %s),
                                    is_monitored      = TRUE
                                WHERE employer_fein = %s
                            """, (p2, s2, paste_url, fein))
                            conn.commit()
                        finally:
                            conn.close()
                        load_ats_discovery.clear()
                        if inserted:
                            st.success("Added to pipeline! Queued for ATS detection.")
                        else:
                            st.info("Already in pipeline.")
                        st.rerun()
                    except Exception as exc:
                        log.exception("Failed to confirm ATS for %r", name)
                        st.error(f"Error: {exc}")
            else:
                st.error("Could not detect ATS from that URL. Check the URL and try again.")

    # ── Re-run discovery ──────────────────────────────────────────────────────
    checked = disc.get("last_checked")
    if checked:
        checked_str = str(checked)[:16].replace("T", " ")
        st.caption(f"Last checked: {checked_str}")

    if st.button("Re-run discovery", key="rerun_disc"):
        with st.spinner("Re-checking …"):
            try:
                _run_inline_discovery(fein, name)
                load_ats_discovery.clear()
                st.rerun()
            except Exception as exc:
                log.exception("Re-run discovery failed for %r", name)
                st.error(f"Error: {exc}")
