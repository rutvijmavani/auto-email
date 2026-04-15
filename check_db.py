# test_custom_ats.py
import json
from db.connection import get_conn
from jobs.ats.custom_career import fetch_jobs

def test_company(company_name):
    conn = get_conn()
    row = conn.execute(
        "SELECT ats_platform, ats_slug FROM prospective_companies "
        "WHERE company = ?", (company_name,)
    ).fetchone()
    conn.close()

    if not row:
        print(f"[ERROR] {company_name} not found in DB")
        return

    platform = row["ats_platform"]
    slug     = row["ats_slug"]

    print(f"\n{'='*55}")
    print(f"  Testing: {company_name}")
    print(f"  Platform: {platform}")
    print(f"{'='*55}")

    if platform != "custom":
        print(f"[SKIP] Not a custom ATS — platform={platform}")
        return

    if not slug:
        print("[ERROR] No slug stored — run --sync-prospective first")
        return

    try:
        slug_info = json.loads(slug)
    except (json.JSONDecodeError, TypeError) as e:
        print(f"  [ERROR] Malformed ats_slug for {company_name}: {e}")
        return

    print(f"  URL: {slug_info.get('url')}")
    print(f"  Strategy: {slug_info.get('session_strategy', 'not detected yet')}")
    print(f"  Detail: {'yes' if slug_info.get('detail') else 'no'}")
    print()

    try:
        jobs = fetch_jobs(slug_info, company_name)

        from jobs.ats.custom_career import _fetch_page, _build_legacy_session, _warm_session, _extract_jobs_array
        session, _ = _warm_session(slug_info, company_name)
        raw = _fetch_page(session, slug_info, page=1, offset=0)
        jobs_raw = _extract_jobs_array(raw, slug_info)
        if jobs_raw:
            print("\nRAW FIRST JOB KEYS:", list(jobs_raw[0].keys()))
            print("RAW FIRST JOB:", json.dumps(jobs_raw[0], indent=2))

        print(f"\n  Result: {len(jobs)} jobs fetched")
        if jobs:
            print(f"\n  Sample (first 3):")
            for job in jobs[:3]:
                print(f"    - {job.get('title', '?')[:50]}")
                print(f"      Location: {job.get('location', '?')}")
                print(f"      URL: {job.get('job_url', '?')}")
                print(f"      Description: "
                      f"{'yes' if job.get('description') else 'no'}")
        else:
            print("  [WARNING] No jobs returned — check diagnostics:")
            print("  python pipeline.py --diagnostics")
    except Exception as e:
        print(f"  [ERROR] {company_name} / {slug_info.get('url', '?')}: {e}")

if __name__ == "__main__":
    import sys
    from db.db import init_db
    init_db()

    # Test specific companies or pass as args
    companies = sys.argv[1:] if len(sys.argv) > 1 else ["Amazon"]

    for company in companies:
        test_company(company)