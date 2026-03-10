# jobs/google_detector.py — DEPRECATED
#
# This file is kept as a stub for backward compatibility only.
# ATS detection now uses the 4-phase approach:
#
#   Phase 1: jobs/ats_sitemap.py   (sitemap lookup)
#   Phase 2: jobs/ats_verifier.py  (API name probe)
#   Phase 3a: jobs/career_page.py  (HTML redirect scan)
#   Phase 3b: jobs/serper.py       (Serper API)
#
# This file is NOT imported by any active code.

# Kept as sentinel for any legacy test references
QUOTA_EXHAUSTED = object()
GOOGLE_BLOCKED  = object()