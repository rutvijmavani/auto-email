"""
reset_quota.py — Temporary script to reset today's polluted quota row.
Delete this file after running.

Usage: python reset_quota.py
"""

from db.db import get_conn
from datetime import datetime

conn = get_conn()
today = datetime.now().strftime("%Y-%m-%d")

# Check current state before reset
c = conn.cursor()
c.execute("SELECT * FROM careershift_quota WHERE date = ?", (today,))
row = c.fetchone()

if row:
    print(f"Current quota row for {today}:")
    print(f"  used={row['used']} | remaining={row['remaining']} | total={row['total_limit']}")
    conn.execute("DELETE FROM careershift_quota WHERE date = ?", (today,))
    conn.commit()
    print(f"[OK] Quota reset for {today} — will be re-fetched from CareerShift on next --find-only run")
else:
    print(f"[INFO] No quota row found for {today} — nothing to reset")

conn.close()