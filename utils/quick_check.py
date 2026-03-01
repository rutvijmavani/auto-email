import sqlite3
conn = sqlite3.connect("data/recruiter_pipeline.db")
for table in ["applications", "recruiters", "application_recruiters", "outreach", "ai_cache", "jobs"]:
    count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    print(f"{table}: {count} rows")