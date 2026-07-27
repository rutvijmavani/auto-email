import streamlit as st

st.set_page_config(page_title="Job Pipeline", page_icon="🔍", layout="wide")

st.title("🔍 Job Pipeline")
st.markdown("Use the sidebar to navigate between pages.")

col1, col2, col3 = st.columns(3)
col1.page_link("pages/1_Jobs.py",      label="📋 Jobs",      icon="📋")
col2.page_link("pages/2_Companies.py", label="🏢 Companies", icon="🏢")
col3.page_link("pages/3_Discover.py",  label="🔎 Discover",  icon="🔎")
