if __name__ == "__main__":
    import sys, io, logging
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")
    logging.basicConfig(level=logging.DEBUG, format="%(message)s")

    from jobs.ats.career_detector import detect_company

    TESTS = [
        ("accenture.com",   "workday"),
        ("stripe.com",      "greenhouse"),
        ("spotify.com",     "lever"),
        ("nomura.com",      "successfactors"),
        ("ashbyhq.com",     "ashby"),
        ("notion.so",       "ashby"),
        ("figma.com",       "greenhouse"),
        ("ramp.com",        "ashby"),
    ]

    for domain, expected in TESTS:
        results = detect_company(domain)
        if not results:
            print(f"[MISS] {domain}: None  (expected {expected})")
        elif len(results) == 1:
            r      = results[0]
            status = "✓" if r.get("platform") == expected else "WRONG"
            print(f"[{status}] {domain}: {r['platform']} slug={r['slug']!r}  (expected {expected})")
        else:
            platforms = [r["platform"] for r in results]
            status    = "✓" if expected in platforms else "WRONG"
            print(f"[{status}] {domain}: MULTI-ATS (expected {expected})")
            for r in results:
                print(f"       {r['platform']} slug={r['slug']!r}  @ {r.get('source_url','')}")
        print()
