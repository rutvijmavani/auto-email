"""
scripts/seed_test_companies.py — Insert 6 real companies for local testing.

Run once after init_db() to populate prospective_companies with known-good
ATS configs so scan_worker tests use real data.

Usage:
    python scripts/seed_test_companies.py
"""

from db.db import init_db, get_conn

COMPANIES = [
    {
        "company":               "Netflix",
        "priority":              0,
        "status":                "active",
        "ats_platform":          "workday",
        "ats_slug":              '{"slug": "netflix", "wd": "wd108", "path": "Netflix"}',
        "domain":                "jobs.netflix.com",
        "consecutive_empty_days": 0,
    },
    {
        "company":               "Nvidia",
        "priority":              0,
        "status":                "active",
        "ats_platform":          "workday",
        "ats_slug":              '{"slug": "nvidia", "wd": "wd5", "path": "NVIDIAExternalCareerSite"}',
        "domain":                "nvidia.com",
        "consecutive_empty_days": 0,
    },
    {
        "company":               "Salesforce",
        "priority":              0,
        "status":                "exhausted",
        "ats_platform":          "workday",
        "ats_slug":              '{"slug": "salesforce", "wd": "wd12", "path": "External_Career_Site"}',
        "domain":                "salesforce.com",
        "consecutive_empty_days": 0,
    },
    {
        "company":               "Adobe",
        "priority":              0,
        "status":                "scraped",
        "ats_platform":          "workday",
        "ats_slug":              '{"slug": "adobe", "wd": "wd5", "path": "external_experienced"}',
        "domain":                "adobe.com",
        "consecutive_empty_days": 0,
    },
    {
        "company":               "Airbnb",
        "priority":              0,
        "status":                "scraped",
        "ats_platform":          "greenhouse",
        "ats_slug":              "airbnb",
        "domain":                "airbnb.com",
        "consecutive_empty_days": 0,
    },
    {
        "company":               "Stripe",
        "priority":              0,
        "status":                "scraped",
        "ats_platform":          "greenhouse",
        "ats_slug":              "stripe",
        "domain":                "stripe.com",
        "consecutive_empty_days": 0,
    },
]


def seed():
    init_db()
    conn = get_conn()
    inserted = 0
    skipped  = 0
    try:
        for c in COMPANIES:
            cursor = conn.execute("""
                INSERT INTO prospective_companies
                    (company, priority, status, ats_platform, ats_slug,
                     domain, consecutive_empty_days,
                     ats_detected_at, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                ON CONFLICT(company) DO NOTHING
            """, (
                c["company"], c["priority"], c["status"],
                c["ats_platform"], c["ats_slug"],
                c["domain"], c["consecutive_empty_days"],
            ))
            if cursor.rowcount > 0:
                inserted += 1
                print(f"  [OK]   Inserted: {c['company']} ({c['ats_platform']})")
            else:
                skipped += 1
                print(f"  [SKIP] Skipped:  {c['company']} (already exists)")
        conn.commit()
    finally:
        conn.close()

    print(f"\nDone — {inserted} inserted, {skipped} skipped.")


if __name__ == "__main__":
    seed()
