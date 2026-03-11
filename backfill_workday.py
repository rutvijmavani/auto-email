#!/usr/bin/env python3
# backfill_workday_discovery.py
# One-time script to backfill Workday detection results
# into ats_discovery.db so future P1 lookups work

from db.ats_companies import get_discovery_conn, mark_from_detection
from db.schema_discovery import init_discovery_db

init_discovery_db()

workday_slugs = [
    ("workday", "adobe", "Adobe"),
    ("workday", "analogdevices", "Analog Devices"),
    ("workday", "aptiv", "Aptiv"),
    ("workday", "asml", "ASML"),
    ("workday", "att", "AT&T"),
    ("workday", "autodesk", "Autodesk"),
    ("workday", "barclays", "Barclays"),
    ("workday", "bestbuycanada", "Best Buy"),
    ("workday", "blackrock", "BlackRock"),
    ("workday", "bloomberg", "Bloomberg"),
    ("workday", "broadcom", "Broadcom"),
    ("workday", "cadence", "Cadence Design Systems"),
    ("workday", "chartermfg", "Charter Communications"),
    ("workday", "chewy", "Chewy"),
    ("workday", "cigna", "Cigna"),
    ("workday", "comcast", "Comcast"),
    ("workday", "cox", "Cox Automotive"),
    ("workday", "db", "Deutsche Bank"),
    ("workday", "ebay", "eBay"),
    ("workday", "elevancehealth", "Elevance Health"),
    ("workday", "expedia", "Expedia"),
    ("workday", "fedex", "FedEx"),
    ("workday", "fordfoundation", "Ford Motor Company"),
    ("workday", "generalmotors", "General Motors"),
    ("workday", "gilead", "Gilead Sciences"),
    ("workday", "homedepot", "Home Depot"),
    ("workday", "humana", "Humana"),
    ("workday", "illumina", "Illumina"),
    ("workday", "intel", "Intel"),
    ("workday", "kla", "KLA Corporation"),
    ("workday", "marvell", "Marvell Semiconductor"),
    ("workday", "mastercard", "Mastercard"),
    ("workday", "micron", "Micron Technology"),
    ("workday", "motorolasolutions", "Motorola Solutions"),
    ("workday", "ms", "Morgan Stanley"),
    ("workday", "nike", "Nike"),
    ("workday", "nordstrom", "Nordstrom"),
    ("workday", "nvidia", "Nvidia"),
    ("workday", "nxp", "NXP USA"),
    ("workday", "paloaltonetworks", "Palo Alto Networks"),
    ("workday", "paypal", "PayPal"),
    ("workday", "qualcomm", "Qualcomm"),
    ("workday", "redhat", "Red Hat"),
    ("workday", "salesforce", "Salesforce"),
    ("workday", "snapchat", "Snap"),
    ("workday", "statestreet", "State Street"),
    ("workday", "target", "Target"),
    ("workday", "tmobile", "T-Mobile"),
    ("workday", "walmart", "Walmart"),
    ("workday", "workday", "Workday"),
    ("workday", "zillow", "Zillow"),
    ("workday", "zoom", "Zoom"),
]

conn = get_discovery_conn()
added = 0
updated = 0

for platform, slug, company_name in workday_slugs:
    # Check if exists
    existing = conn.execute("""
        SELECT company_name, source FROM ats_companies
        WHERE platform = ? AND slug = ?
    """, (platform, slug)).fetchone()

    if existing:
        if not existing["company_name"]:
            # Update company_name if missing
            conn.execute("""
                UPDATE ats_companies
                SET company_name = ?, is_enriched = 1,
                    source = CASE WHEN source = 'crawl'
                             THEN 'detection' ELSE source END
                WHERE platform = ? AND slug = ?
            """, (company_name, platform, slug))
            updated += 1
    else:
        # Insert new
        conn.execute("""
            INSERT OR IGNORE INTO ats_companies
                (platform, slug, company_name,
                 is_enriched, source)
            VALUES (?, ?, ?, 1, 'detection')
        """, (platform, slug, company_name))
        added += 1

conn.commit()
conn.close()
print(f"[OK] Backfill complete: {added} added, {updated} updated")
print(f"[OK] Future P1 detections will use DB instead of Serper")