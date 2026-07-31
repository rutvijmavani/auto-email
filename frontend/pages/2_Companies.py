"""
frontend/pages/2_Companies.py — Monitored company list with ATS status,
job activity metrics, and pipeline controls.
"""

import sys
import os

import pandas as pd
import streamlit as st

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))
from db.connection import get_conn
from frontend.db_utils import query as _query, SOURCE_LABELS as _SOURCE_LABELS

st.set_page_config(page_title="Companies", page_icon="🏢", layout="wide")

# ─────────────────────────────────────────────────────────────────────────────
# Constants
# ─────────────────────────────────────────────────────────────────────────────

_STATUS_ICON = {
    "pending":   "🟡",
    "scraped":   "🟢",
    "converted": "🔵",
    "exhausted": "⚫",
}

_ALL_STATUSES = ["pending", "scraped", "converted", "exhausted"]

# ─────────────────────────────────────────────────────────────────────────────
# Data helpers
# ─────────────────────────────────────────────────────────────────────────────


@st.cache_data(ttl=120)
def load_companies(
    search: str,
    statuses: tuple,
    platforms: tuple,
    h1b_only: bool,
    sort_by: str,
) -> pd.DataFrame:
    clauses: list[str] = []
    params: list = []

    if search:
        clauses.append("p.company ILIKE %s")
        params.append(f"%{search}%")

    if statuses:
        clauses.append("p.status = ANY(%s)")
        params.append(list(statuses))

    if platforms:
        clauses.append("p.ats_platform = ANY(%s)")
        params.append(list(platforms))

    if h1b_only:
        clauses.append("p.h1b_sponsor = TRUE")

    where = ("WHERE " + " AND ".join(clauses)) if clauses else ""

    order = {
        "Priority ↓":      "p.priority DESC, p.created_at DESC",
        "Most jobs (30d)": "jobs_30d DESC NULLS LAST",
        "Latest job ↓":    "last_job_seen DESC NULLS LAST",
        "Added ↓":         "p.created_at DESC",
        "Name A→Z":        "p.company ASC",
    }.get(sort_by, "p.priority DESC, p.created_at DESC")

    sql = f"""
        SELECT
            p.company,
            p.status,
            p.priority,
            p.ats_platform,
            p.ats_slug,
            p.domain,
            p.consecutive_empty_days,
            p.last_checked_at,
            p.created_at,
            p.h1b_sponsor,
            COUNT(j.id) FILTER (WHERE j.first_seen >= NOW() - INTERVAL '30 days') AS jobs_30d,
            COUNT(j.id) FILTER (WHERE j.first_seen >= NOW() - INTERVAL '7 days')  AS jobs_7d,
            COUNT(j.id)                                                             AS jobs_total,
            MAX(j.first_seen)                                                       AS last_job_seen
        FROM prospective_companies p
        LEFT JOIN job_postings j ON j.company = p.company
        {where}
        GROUP BY
            p.id, p.company, p.status, p.priority, p.ats_platform, p.ats_slug,
            p.domain, p.consecutive_empty_days, p.last_checked_at,
            p.created_at, p.h1b_sponsor
        ORDER BY {order}
    """
    return _query(sql, params)


@st.cache_data(ttl=300)
def load_filter_options() -> dict:
    platforms = _query(
        "SELECT DISTINCT ats_platform FROM prospective_companies "
        "WHERE ats_platform IS NOT NULL AND ats_platform != 'unknown' "
        "ORDER BY ats_platform"
    )
    return {
        "platforms": platforms["ats_platform"].tolist() if not platforms.empty else [],
    }


@st.cache_data(ttl=60)
def load_job_trend(company: str) -> pd.DataFrame:
    return _query("""
        SELECT DATE(first_seen) AS day, COUNT(*) AS jobs
        FROM job_postings
        WHERE company = %s AND first_seen >= NOW() - INTERVAL '90 days'
        GROUP BY day ORDER BY day
    """, (company,))


@st.cache_data(ttl=60)
def load_recent_jobs(company: str) -> pd.DataFrame:
    return _query("""
        SELECT title, location, status, skill_score, first_seen, job_url
        FROM job_postings
        WHERE company = %s
        ORDER BY first_seen DESC
        LIMIT 15
    """, (company,))


@st.cache_data(ttl=120)
def load_discovery(company: str) -> dict | None:
    """Load h1b_ats_discovery row for this company by name (case-insensitive)."""
    df = _query(
        "SELECT canonical_name, canonical_source, website_url, careers_url, "
        "detected_platform, detected_slug, last_checked "
        "FROM h1b_ats_discovery WHERE employer_name ILIKE %s LIMIT 1",
        (company,),
    )
    return df.to_dict("records")[0] if not df.empty else None


def _update_status(company: str, new_status: str) -> None:
    conn = get_conn()
    try:
        conn.execute(
            "UPDATE prospective_companies SET status = %s WHERE company = %s",
            (new_status, company),
        )
        conn.commit()
    finally:
        conn.close()


def _update_priority(company: str, priority: int) -> None:
    conn = get_conn()
    try:
        conn.execute(
            "UPDATE prospective_companies SET priority = %s WHERE company = %s",
            (priority, company),
        )
        conn.commit()
    finally:
        conn.close()


def _remove_company(company: str) -> None:
    conn = get_conn()
    try:
        conn.execute(
            "DELETE FROM prospective_companies WHERE company = %s",
            (company,),
        )
        conn.commit()
    finally:
        conn.close()


# ─────────────────────────────────────────────────────────────────────────────
# Sidebar
# ─────────────────────────────────────────────────────────────────────────────

with st.sidebar:
    st.title("🏢 Companies")
    st.divider()

    search = st.text_input("Search company", placeholder="e.g. Google")

    opts = load_filter_options()

    selected_statuses = st.multiselect(
        "Status",
        _ALL_STATUSES,
        default=["pending", "scraped"],
        format_func=lambda s: f"{_STATUS_ICON.get(s, '')} {s}",
    )

    selected_platforms = st.multiselect("ATS platform", opts["platforms"])

    st.divider()

    h1b_only = st.checkbox("H-1B sponsors only")
    sort_by  = st.selectbox(
        "Sort by",
        ["Priority ↓", "Most jobs (30d)", "Latest job ↓", "Added ↓", "Name A→Z"],
    )

    st.divider()
    if st.button("🔄 Refresh", use_container_width=True):
        st.cache_data.clear()
        st.rerun()


# ─────────────────────────────────────────────────────────────────────────────
# Load
# ─────────────────────────────────────────────────────────────────────────────

df = load_companies(
    search=search,
    statuses=tuple(selected_statuses),
    platforms=tuple(selected_platforms),
    h1b_only=h1b_only,
    sort_by=sort_by,
)


# ─────────────────────────────────────────────────────────────────────────────
# Metrics
# ─────────────────────────────────────────────────────────────────────────────

total   = len(df)
active  = int((df["status"].isin(["pending", "scraped"])).sum()) if not df.empty else 0
ats_det = int((df["ats_platform"].notna() & (df["ats_platform"] != "unknown")).sum()) if not df.empty else 0
jobs_7d = int(df["jobs_7d"].sum()) if not df.empty else 0

m1, m2, m3, m4 = st.columns(4)
m1.metric("Total companies",  f"{total:,}")
m2.metric("Active (pipeline)", f"{active:,}")
m3.metric("ATS detected",     f"{ats_det:,}")
m4.metric("Jobs this week",   f"{jobs_7d:,}")

st.divider()


# ─────────────────────────────────────────────────────────────────────────────
# Company table
# ─────────────────────────────────────────────────────────────────────────────

if df.empty:
    st.info("No companies match the current filters.")
    st.stop()

display = df[[
    "company", "status", "ats_platform", "ats_slug",
    "jobs_7d", "jobs_30d", "last_job_seen", "consecutive_empty_days",
    "priority", "h1b_sponsor",
]].copy()

display["status"] = display["status"].map(
    lambda s: f"{_STATUS_ICON.get(s, '')} {s}" if s else "—"
)
display["ats_platform"] = display["ats_platform"].fillna("—").replace("unknown", "—")
display["h1b_sponsor"]  = display["h1b_sponsor"].map(
    {True: "✓", False: "", None: ""}
).fillna("")

display.rename(columns={
    "company":               "Company",
    "status":                "Status",
    "ats_platform":          "ATS",
    "ats_slug":              "Slug",
    "jobs_7d":               "Jobs 7d",
    "jobs_30d":              "Jobs 30d",
    "last_job_seen":         "Last job",
    "consecutive_empty_days": "Empty days",
    "priority":              "Priority",
    "h1b_sponsor":           "H-1B",
}, inplace=True)

selected = st.dataframe(
    display,
    use_container_width=True,
    hide_index=True,
    on_select="rerun",
    selection_mode="single-row",
    column_config={
        "Jobs 7d":    st.column_config.NumberColumn("Jobs 7d",    format="%d"),
        "Jobs 30d":   st.column_config.NumberColumn("Jobs 30d",   format="%d"),
        "Priority":   st.column_config.NumberColumn("Priority",   format="%d"),
        "Empty days": st.column_config.NumberColumn("Empty days", format="%d"),
        "Last job":   st.column_config.DateColumn("Last job",     format="MMM D, YYYY"),
    },
)


# ─────────────────────────────────────────────────────────────────────────────
# Detail panel
# ─────────────────────────────────────────────────────────────────────────────

sel_rows = selected.selection.rows if selected.selection else []
if not sel_rows:
    st.stop()

row  = df.iloc[sel_rows[0]]
name = row["company"]

st.divider()
st.subheader(name)

col_left, col_right = st.columns([3, 1])

# ── Job trend chart ───────────────────────────────────────────────────────────
with col_left:
    trend = load_job_trend(name)
    if not trend.empty:
        trend = trend.set_index("day")
        st.bar_chart(trend["jobs"], color="#2196F3")
        st.caption("New job postings per day (last 90 days)")
    else:
        st.info("No job postings found for this company yet.")

    st.markdown("**Recent jobs**")
    recent = load_recent_jobs(name)
    if not recent.empty:
        recent_disp = recent.copy()
        recent_disp.rename(columns={
            "title":       "Title",
            "location":    "Location",
            "status":      "Status",
            "skill_score": "Score",
            "first_seen":  "First seen",
            "job_url":     "URL",
        }, inplace=True)
        st.dataframe(
            recent_disp,
            use_container_width=True,
            hide_index=True,
            column_config={
                "URL":        st.column_config.LinkColumn("URL", display_text="Open ↗"),
                "First seen": st.column_config.DateColumn("First seen", format="MMM D, YYYY"),
                "Score":      st.column_config.NumberColumn("Score", format="%d"),
            },
        )
    else:
        st.info("No jobs yet.")

# ── Meta + actions ────────────────────────────────────────────────────────────
with col_right:
    st.markdown("**Pipeline**")

    ats      = row["ats_platform"] or "—"
    slug     = row["ats_slug"]     or "—"
    domain   = row["domain"]       or "—"
    h1b      = row["h1b_sponsor"]
    empty_d  = row["consecutive_empty_days"]
    checked  = row["last_checked_at"]
    added    = row["created_at"]
    priority = int(row["priority"]) if row["priority"] is not None else 0

    if domain != "—":
        st.markdown(f"Domain: [{domain}]({domain})")
    else:
        st.markdown("Domain: —")

    st.markdown(f"ATS: **{ats}**" + (f"  ·  `{slug}`" if slug != "—" else ""))
    st.markdown(f"H-1B sponsor: **{'✓ Yes' if h1b is True else ('✗ No' if h1b is False else '—')}**")
    st.markdown(f"Consecutive empty days: **{empty_d or 0}**")

    if added:
        st.caption(f"Added: {str(added)[:10]}")
    if checked:
        st.caption(f"Last checked: {str(checked)[:16].replace('T', ' ')}")

    st.divider()

    # ── Status control ────────────────────────────────────────────────────────
    st.markdown("**Change status**")
    current_status = row["status"] or "pending"
    new_status = st.selectbox(
        "Status",
        _ALL_STATUSES,
        index=_ALL_STATUSES.index(current_status) if current_status in _ALL_STATUSES else 0,
        format_func=lambda s: f"{_STATUS_ICON.get(s, '')} {s}",
        key=f"status_{name}",
        label_visibility="collapsed",
    )
    if new_status != current_status:
        if st.button("Save status", key=f"save_status_{name}", use_container_width=True):
            _update_status(name, new_status)
            load_companies.clear()
            st.success(f"Status → {new_status}")
            st.rerun()

    # ── Priority control ──────────────────────────────────────────────────────
    st.markdown("**Priority**")
    new_priority = st.number_input(
        "Priority",
        min_value=0, max_value=10,
        value=priority,
        step=1,
        key=f"priority_{name}",
        label_visibility="collapsed",
    )
    if new_priority != priority:
        if st.button("Save priority", key=f"save_prio_{name}", use_container_width=True):
            _update_priority(name, int(new_priority))
            load_companies.clear()
            st.success(f"Priority → {new_priority}")
            st.rerun()

    st.divider()

    # ── Discovery data (Wikidata) ─────────────────────────────────────────────
    disc = load_discovery(name)
    if disc:
        src = disc.get("canonical_source") or ""
        with st.expander("Discovery data", expanded=False):
            if disc.get("canonical_name"):
                st.markdown(f"Brand name: **{disc['canonical_name']}**")
            if src:
                st.caption(f"Source: {_SOURCE_LABELS.get(src, src)}")
            if disc.get("website_url"):
                url = disc["website_url"]
                st.markdown(f"Website: [{url}]({url})")
            if disc.get("careers_url"):
                curl = disc["careers_url"]
                st.markdown(f"Careers: [{curl}]({curl})")
            if disc.get("detected_platform"):
                p = disc["detected_platform"]
                s = disc.get("detected_slug") or ""
                st.markdown(f"ATS detected: **{p}**" + (f"  ·  `{s}`" if s else ""))

    # ── Remove button ─────────────────────────────────────────────────────────
    st.divider()
    if st.button("🗑 Remove from pipeline", key=f"remove_{name}",
                 use_container_width=True, type="secondary"):
        st.session_state[f"confirm_remove_{name}"] = True

    if st.session_state.get(f"confirm_remove_{name}"):
        st.warning(f"Remove **{name}** from pipeline?")
        c1, c2 = st.columns(2)
        if c1.button("Yes, remove", key=f"yes_remove_{name}", type="primary"):
            _remove_company(name)
            load_companies.clear()
            st.session_state.pop(f"confirm_remove_{name}", None)
            st.rerun()
        if c2.button("Cancel", key=f"cancel_remove_{name}"):
            st.session_state.pop(f"confirm_remove_{name}", None)
            st.rerun()
