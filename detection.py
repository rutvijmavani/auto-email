import logging
logging.basicConfig(level=logging.DEBUG, format="%(message)s")

from jobs.ats.career_detector import detect_company

TESTS = [
    ("accenture.com",   "workday"),
    ("stripe.com",      "greenhouse"),
    ("spotify.com",     "lever"),
    ("nomura.com",      "successfactors"),
    ("ashbyhq.com",     "ashby"),
    ("notion.so",       "greenhouse"),
    ("figma.com",       "greenhouse"),
    ("ramp.com",        "lever"),
]

for domain, expected in TESTS:
    result = detect_company(domain)
    if result:
        platform = result.get("platform", "?")
        slug     = result.get("slug", "")
        status   = "✓" if platform == expected else "WRONG"
        print(f"[{status}] {domain}: {platform} slug={slug!r}  (expected {expected})")
    else:
        print(f"[MISS] {domain}: None  (expected {expected})")
    print()
