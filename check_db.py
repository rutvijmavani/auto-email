import sqlite3
import os

DB_PATH = os.path.join(os.path.dirname(__file__), "data", "recruiter_pipeline.db")

conn = sqlite3.connect(DB_PATH)
conn.row_factory = sqlite3.Row

print("\n" + "="*60)
print("UNMONITORED COMPANIES BREAKDOWN")
print("="*60)

# Summary by platform
rows = conn.execute("""
    SELECT 
        COALESCE(ats_platform, 'NULL') as platform,
        COUNT(*) as count
    FROM prospective_companies
    WHERE ats_platform IN ('custom', 'unknown', 'unsupported')
       OR ats_platform IS NULL
    GROUP BY ats_platform
    ORDER BY count DESC
""").fetchall()

print("\n[SUMMARY BY PLATFORM]")
total = 0
for row in rows:
    print(f"  {row['platform']:<20} {row['count']} companies")
    total += row['count']
print(f"  {'─'*30}")
print(f"  {'TOTAL':<20} {total} companies")

# Full list per platform
platforms = conn.execute("""
    SELECT DISTINCT COALESCE(ats_platform, 'NULL') as platform
    FROM prospective_companies
    WHERE ats_platform IN ('custom', 'unknown', 'unsupported')
       OR ats_platform IS NULL
    ORDER BY platform
""").fetchall()

for p in platforms:
    platform = p['platform']
    companies = conn.execute("""
        SELECT company, ats_slug, domain
        FROM prospective_companies
        WHERE COALESCE(ats_platform, 'NULL') = ?
        ORDER BY company
    """, (platform,)).fetchall()

    print(f"\n[{platform.upper()}] — {len(companies)} companies")
    print(f"  {'Company':<35} {'Slug':<30} {'Domain'}")
    print(f"  {'─'*80}")
    for c in companies:
        slug   = str(c['ats_slug'] or '')[:28]
        domain = str(c['domain'] or '')
        print(f"  {c['company']:<35} {slug:<30} {domain}")

conn.close()
print("\n" + "="*60)