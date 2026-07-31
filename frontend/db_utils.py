"""
frontend/db_utils.py — shared database helpers for Streamlit pages.
"""

import sys
import os

import pandas as pd

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from db.connection import get_conn


SOURCE_LABELS: dict[str, str] = {
    "wikidata":  "🌐 Wikidata",
    "wikipedia": "📖 Wikipedia",
    "qwen":      "🤖 Qwen3",
    "regex":     "🔤 Regex strip",
    "kg_api":    "🔍 Google KG",
}


def query(sql: str, params=None) -> pd.DataFrame:
    """Execute SQL and return a DataFrame. Returns an empty DataFrame on no results."""
    conn = get_conn()
    try:
        cur = conn.execute(sql, params or ())
        rows = cur.fetchall()
        return pd.DataFrame([dict(r) for r in rows]) if rows else pd.DataFrame()
    finally:
        conn.close()
