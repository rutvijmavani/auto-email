"""
frontend/pages/3_Discover.py — DOL H-1B employer browser.

Browse aggregated LCA disclosures to find companies worth adding to the pipeline.
All filtering is SQL-based with indexed columns; no pandas post-filtering.
"""

import json
import logging
import sys
import os

import pandas as pd
import streamlit as st

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))
from db.connection import get_conn
from db.prospective import add_prospective_company
from frontend.db_utils import query as _query

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
    Join key: last 4 FEIN digits + normalized employer name (same transform as _norm()).
    Returns one row per fiscal year; empty DataFrame if no match.
    """
    return _query("""
        SELECT
            fiscal_year,
            SUM(new_employment_approval)        AS new_employment_approval,
            SUM(new_employment_denial)          AS new_employment_denial,
            SUM(continuation_approval)          AS continuation_approval,
            SUM(continuation_denial)            AS continuation_denial,
            SUM(change_same_employer_approval)  AS change_same_employer_approval,
            SUM(change_same_employer_denial)    AS change_same_employer_denial,
            SUM(new_concurrent_approval)        AS new_concurrent_approval,
            SUM(new_concurrent_denial)          AS new_concurrent_denial,
            SUM(change_of_employer_approval)    AS change_of_employer_approval,
            SUM(change_of_employer_denial)      AS change_of_employer_denial,
            SUM(amended_approval)               AS amended_approval,
            SUM(amended_denial)                 AS amended_denial
        FROM uscis_h1b_petitions
        WHERE tax_id = RIGHT(%s, 4)
          AND employer_name_norm = TRIM(regexp_replace(
                regexp_replace(upper(%s), '[^A-Z0-9 ]', ' ', 'g'),
                '\\s+', ' ', 'g'))
        GROUP BY fiscal_year
        ORDER BY fiscal_year
    """, (fein, employer_name))


def _pipeline_status(employer_name: str) -> str | None:
    """Return existing pipeline status string, or None if not in pipeline."""
    conn = get_conn()
    try:
        cur = conn.execute(
            "SELECT status FROM prospective_companies WHERE company = %s",
            (employer_name.strip(),),
        )
        row = cur.fetchone()
        return dict(row)["status"] if row else None
    finally:
        conn.close()


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

    existing_status = _pipeline_status(name)
    if existing_status:
        st.success(f"In pipeline — {existing_status}")
    else:
        if st.button("➕ Add to pipeline", use_container_width=True, type="primary"):
            try:
                inserted = add_prospective_company(name, priority=0)
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
