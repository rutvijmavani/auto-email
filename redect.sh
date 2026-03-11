#!/bin/bash
# redetect_workday.sh
# Clears ALL Workday detections and re-detects from scratch
# Run AFTER deploying the en-US locale path fix
#
# Usage: bash redetect_workday.sh

set -euo pipefail

cd /home/opc/mail || { echo "[ERROR] Could not cd to /home/opc/mail"; exit 1; }
source venv/bin/activate || { echo "[ERROR] Could not activate venv"; exit 1; }

echo "Step 1 — Clearing all Workday detections..."
python3 -c "
from db.connection import get_conn
conn = get_conn()
result = conn.execute('''
    UPDATE prospective_companies
    SET ats_platform    = NULL,
        ats_slug        = NULL,
        ats_detected_at = NULL
    WHERE ats_platform = 'workday'
''')
conn.commit()
conn.close()
print(f'[OK] Cleared {result.rowcount} Workday detections — ready for re-detection')
"

echo ""
echo "Step 2 — Re-detecting all cleared companies..."
echo "(Uses fixed path extractor — should get correct career site names)"
python pipeline.py --detect-ats

echo ""
echo "Step 3 — Check results..."
python3 -c "
import json
from db.connection import get_conn
conn = get_conn()

rows = conn.execute('''
    SELECT company, ats_slug
    FROM prospective_companies
    WHERE ats_platform = 'workday'
    ORDER BY company
''').fetchall()

bad = []
good = []
for r in rows:
    try:
        path = json.loads(r['ats_slug']).get('path','')
        if path.lower() in ('en-us','en-gb','search','jobs','') or len(path) <= 3:
            bad.append((r['company'], path))
        else:
            good.append((r['company'], path))
    except:
        bad.append((r['company'], str(r['ats_slug'])))

print(f'Good paths: {len(good)}')
print(f'Bad paths:  {len(bad)}')
if bad:
    print()
    print('Still bad (need manual override):')
    for c, p in bad:
        print(f'  {c:<35} path={p}')
"

echo ""
echo "Step 4 — Run monitoring to verify jobs are returned..."
python pipeline.py --monitor-jobs