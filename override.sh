#!/bin/bash
# override_detections.sh
# Run on VM to fix wrong ATS detections
# Verified URLs as of 2026-03-11
#
# Usage: bash override_detections.sh
# Or run individual commands one at a time

cd /home/opc/mail
source venv/bin/activate

echo "Fixing wrong ATS detections..."
echo ""

# ─────────────────────────────────────────
# WRONG DETECTIONS — VERIFIED FIXES
# ─────────────────────────────────────────

# 1. Capital One
# Was:     lever / capital  (wrong — "capital" is a generic slug)
# Correct: workday — capitalone.wd12.myworkdayjobs.com/Capital_One
echo "[1/9] Capital One..."
python pipeline.py --detect-ats "Capital One" \
  --override workday '{"slug":"capitalone","wd":"wd12","path":"Capital_One"}'

# 2. Applied Materials
# Was:     ashby / applied  (wrong — "applied" is different company on Ashby)
# Correct: workday — amat.wd1.myworkdayjobs.com/External
echo "[2/9] Applied Materials..."
python pipeline.py --detect-ats "Applied Materials" \
  --override workday '{"slug":"amat","wd":"wd1","path":"External"}'

# 3. US Bank
# Was:     workday / db / DBWebsite  (wrong — that's Deutsche Bank!)
# Correct: workday — usbank.wd1.myworkdayjobs.com/US_Bank_Careers
echo "[3/9] US Bank..."
python pipeline.py --detect-ats "US Bank" \
  --override workday '{"slug":"usbank","wd":"wd1","path":"US_Bank_Careers"}'

# 4. Western Digital
# Was:     workday / westernalliancebank  (wrong — Western Alliance Bank)
# Correct: smartrecruiters — careers.smartrecruiters.com/westerndigital
echo "[4/9] Western Digital..."
python pipeline.py --detect-ats "Western Digital" \
  --override smartrecruiters westerndigital

# 5. Ford Motor Company
# Was:     workday / fordfoundation  (wrong — Ford Foundation is a charity)
# Correct: oracle_hcm — efds.fa.em5.oraclecloud.com/hcmUI/CandidateExperience
echo "[5/9] Ford Motor Company..."
python pipeline.py --detect-ats "Ford Motor Company" \
  --override oracle_hcm '{"slug":"efds","region":"em5","site":"CX_1"}'

# 6. Best Buy
# Was:     workday / bestbuycanada (wrong — Canada subsidiary)
# Correct: workday — bestbuy.wd5.myworkdayjobs.com/en-US/BestBuy
echo "[6/9] Best Buy..."
python pipeline.py --detect-ats "Best Buy" \
  --override workday '{"slug":"bestbuy","wd":"wd5","path":"BestBuy"}'

# 7. Charter Communications
# Was:     workday / chartermfg (wrong — Charter Manufacturing, different company)
# Correct: workday — spectrum.wd5.myworkdayjobs.com/Spectrum_Careers
echo "[7/9] Charter Communications..."
python pipeline.py --detect-ats "Charter Communications" \
  --override workday '{"slug":"spectrum","wd":"wd5","path":"Spectrum_Careers"}'

# 8. Arm
# Was:     icims / earlycareers-arm (early careers board only)
# Correct: workday — arm.wd1.myworkdayjobs.com/Careers
echo "[8/9] Arm..."
python pipeline.py --detect-ats "Arm" \
  --override workday '{"slug":"arm","wd":"wd1","path":"Careers"}'

# 9. Bloomberg
# Was:     workday / Bloombergindustrygroup (subsidiary, not main entity)
# Correct: workday — bloomberg.wd1.myworkdayjobs.com/Bloomberglp
echo "[9/9] Bloomberg..."
python pipeline.py --detect-ats "Bloomberg" \
  --override workday '{"slug":"bloomberg","wd":"wd1","path":"Bloomberglp"}'

# ─────────────────────────────────────────
# FedEx — needs verification first
# Was:     workday / FXE-LAC (Latin America path)
# Verify:  https://fedex.wd1.myworkdayjobs.com/en-US/FXE-US_External_Career_Site
# ─────────────────────────────────────────
# echo "[?] FedEx..."
# python pipeline.py --detect-ats "FedEx" \
#   --override workday '{"slug":"fedex","wd":"wd1","path":"FXE-US_External_Career_Site"}'

echo ""
echo "Done! Run python pipeline.py --monitor-status to verify."

# ─────────────────────────────────────────
# iCIMS SLUG FIXES
# ─────────────────────────────────────────

# AMD — migrated away from iCIMS → careers.amd.com (custom ATS)
echo "[iCIMS] AMD — marking as custom..."
python pipeline.py --detect-ats "AMD" --override custom amd

# Arm — migrated away from iCIMS → careers.arm.com (custom ATS)
echo "[iCIMS] Arm — marking as custom..."
python pipeline.py --detect-ats "Arm" --override custom arm

# Rivian — migrated away from iCIMS → careers.rivian.com (custom ATS)
echo "[iCIMS] Rivian — marking as custom..."
python pipeline.py --detect-ats "Rivian" --override custom rivian

# ─────────────────────────────────────────
# ORACLE HCM — Goldman Sachs
# ─────────────────────────────────────────
echo "[Oracle] Goldman Sachs..."
python pipeline.py --detect-ats "Goldman Sachs" \
  --override oracle_hcm '{"slug":"hdpc","region":"us2","site":"LateralHiring"}'