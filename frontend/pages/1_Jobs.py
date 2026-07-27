"""
frontend/pages/1_Jobs.py — Live job listings from job_postings table.

Replaces the weekly email digest with a filterable, searchable view.
"""

import sys
import os
from datetime import date, timedelta

import pandas as pd
import streamlit as st

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))
from db.connection import get_conn

st.set_page_config(page_title="Jobs", page_icon="📋", layout="wide")

# ─────────────────────────────────────────────────────────────────────────────
# Data loading
# ─────────────────────────────────────────────────────────────────────────────

def _query(sql: str, params=None) -> pd.DataFrame:
    """Execute SQL and return a DataFrame using our custom _Connection."""
    conn = get_conn()
    try:
        cur = conn.execute(sql, params or ())
        rows = cur.fetchall()
        return pd.DataFrame([dict(r) for r in rows]) if rows else pd.DataFrame()
    finally:
        conn.close()


@st.cache_data(ttl=120)
def load_jobs(
    search: str,
    companies: tuple,
    platforms: tuple,
    statuses: tuple,
    date_from: date,
    date_to: date,
) -> pd.DataFrame:
    clauses = [
        "j.status = ANY(%s)",
        "j.first_seen >= %s",
        "j.first_seen <= %s",
    ]
    params: list = [list(statuses), date_from, date_to]

    if search:
        clauses.append("(j.title ILIKE %s OR j.company ILIKE %s)")
        params += [f"%{search}%", f"%{search}%"]

    if companies:
        clauses.append("j.company = ANY(%s)")
        params.append(list(companies))

    if platforms:
        clauses.append("j.ats_platform = ANY(%s)")
        params.append(list(platforms))

    where = " AND ".join(clauses)
    sql = f"""
        SELECT
            j.id,
            j.company,
            j.title,
            j.location,
            j.ats_platform  AS platform,
            j.status,
            j.skill_score,
            j.first_seen,
            j.posted_at,
            j.job_url,
            j.description,
            j.found_by
        FROM job_postings j
        WHERE {where}
        ORDER BY j.first_seen DESC, j.id DESC
        LIMIT 2000
    """
    return _query(sql, params)


@st.cache_data(ttl=300)
def load_filter_options() -> dict:
    companies = _query(
        "SELECT DISTINCT company FROM job_postings WHERE status IN ('new','digested') ORDER BY company"
    )
    platforms = _query(
        "SELECT DISTINCT ats_platform FROM job_postings WHERE ats_platform IS NOT NULL ORDER BY ats_platform"
    )
    return {
        "companies": companies["company"].tolist() if not companies.empty else [],
        "platforms": platforms["ats_platform"].tolist() if not platforms.empty else [],
    }


# ─────────────────────────────────────────────────────────────────────────────
# Sidebar filters
# ─────────────────────────────────────────────────────────────────────────────

with st.sidebar:
    st.title("📋 Jobs")
    st.divider()

    search = st.text_input("🔍 Search title or company", placeholder="e.g. backend engineer")

    opts = load_filter_options()

    selected_companies = st.multiselect("Company", opts["companies"])
    selected_platforms = st.multiselect("ATS Platform", opts["platforms"])

    st.divider()

    selected_statuses = st.multiselect(
        "Status",
        ["new", "digested", "expired"],
        default=["new", "digested"],
    )

    st.divider()

    col_a, col_b = st.columns(2)
    date_from = col_a.date_input("From", value=date.today() - timedelta(days=30))
    date_to   = col_b.date_input("To",   value=date.today())

    st.divider()
    if st.button("🔄 Refresh", use_container_width=True):
        st.cache_data.clear()
        st.rerun()

# ─────────────────────────────────────────────────────────────────────────────
# Load data
# ─────────────────────────────────────────────────────────────────────────────

if not selected_statuses:
    selected_statuses = ["new", "digested"]

df = load_jobs(
    search=search,
    companies=tuple(selected_companies),
    platforms=tuple(selected_platforms),
    statuses=tuple(selected_statuses),
    date_from=date_from,
    date_to=date_to,
)

# ─────────────────────────────────────────────────────────────────────────────
# Top metrics
# ─────────────────────────────────────────────────────────────────────────────

week_ago  = date.today() - timedelta(days=7)
this_week = df[pd.to_datetime(df["first_seen"], errors="coerce") >= pd.Timestamp(week_ago)] if not df.empty else df

m1, m2, m3, m4 = st.columns(4)
m1.metric("Total jobs",      len(df))
m2.metric("Companies",       df["company"].nunique() if not df.empty else 0)
m3.metric("Added this week", len(this_week))
m4.metric("Avg skill score", f"{df['skill_score'].mean():.0f}" if not df.empty else "—")

st.divider()

# ─────────────────────────────────────────────────────────────────────────────
# Jobs table
# ─────────────────────────────────────────────────────────────────────────────

if df.empty:
    st.info("No jobs match the current filters.")
    st.stop()

STATUS_ICON = {"new": "🟢", "digested": "🔵", "expired": "⚫"}

display = df[[
    "company", "title", "location", "platform",
    "status", "skill_score", "first_seen", "job_url",
]].copy()

display["status"] = display["status"].map(lambda s: f"{STATUS_ICON.get(s, '')} {s}")
display.rename(columns={
    "company":     "Company",
    "title":       "Title",
    "location":    "Location",
    "platform":    "Platform",
    "status":      "Status",
    "skill_score": "Score",
    "first_seen":  "First seen",
    "job_url":     "URL",
}, inplace=True)

selected = st.dataframe(
    display,
    use_container_width=True,
    hide_index=True,
    on_select="rerun",
    selection_mode="single-row",
    column_config={
        "URL": st.column_config.LinkColumn("URL", display_text="Open ↗"),
        "First seen": st.column_config.DateColumn("First seen", format="MMM D, YYYY"),
        "Score": st.column_config.NumberColumn("Score", format="%d"),
    },
)

# ─────────────────────────────────────────────────────────────────────────────
# Job detail panel — shown when a row is selected
# ─────────────────────────────────────────────────────────────────────────────

rows = selected.selection.rows if selected.selection else []
if rows:
    job = df.iloc[rows[0]]

    st.divider()
    col_info, col_meta = st.columns([3, 1])

    with col_info:
        st.subheader(f"{job['title']}")
        st.caption(f"**{job['company']}**  ·  {job['location'] or 'Location not specified'}")

        desc = job.get("description")
        if isinstance(desc, str) and desc.strip():
            with st.expander("Job description", expanded=True):
                st.markdown(desc[:4000] + ("…" if len(desc) > 4000 else ""))
        else:
            st.info("Description not available (job may have expired).")

    with col_meta:
        st.markdown("**Details**")
        st.markdown(f"Platform: `{job['platform'] or '—'}`")
        st.markdown(f"Found by: `{job['found_by'] or '—'}`")
        st.markdown(f"First seen: `{job['first_seen']}`")
        if job.get("posted_at"):
            st.markdown(f"Posted at: `{str(job['posted_at'])[:10]}`")
        st.markdown(f"Skill score: `{job['skill_score']}`")
        st.markdown(f"Status: `{job['status']}`")
        st.link_button("Open job posting ↗", job["job_url"], use_container_width=True)
