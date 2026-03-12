"""
test_serper_unknowns.py — Diagnose Serper results for unknown companies

Shows exactly what Serper returns for each company so we can tell:
  1. Is Serper returning relevant URLs at all?
  2. Is our pattern matcher missing valid URLs?
  3. Is the company using an unsupported ATS?

Run: python test_serper_unknowns.py
     python test_serper_unknowns.py --company "Goldman Sachs"
     python test_serper_unknowns.py --credits   (check remaining credits)
"""

import os
import sys
import argparse
import requests
from dotenv import load_dotenv

load_dotenv()

SERPER_API_KEY = os.getenv("SERPER_API_KEY")
SERPER_URL     = "https://google.serper.dev/search"

SERPER_SEARCHES = [
    ("workday",    "site:myworkdayjobs.com"),
    ("oracle_hcm", "site:oraclecloud.com/hcmUI"),
]

UNKNOWN_COMPANIES = [
    "Akamai Technologies",
    "American Express",
    "Bank of America",
    "Bosch",
    "ByteDance",
    "Caterpillar",
    "Charles Schwab",
    "Citibank",
    "Citrix",
    "Cruise",
    "Docusign",
    "Doordash",
    "Electronic Arts",
    "Ericsson",
    "Fidelity",
    "Fortinet",
    "Genentech",
    "Goldman Sachs",
    "Honeywell",
    "Informatica",
    "Intuit",
    "Juniper Networks",
    "Lam Research",
    "Lucid",
    "MathWorks",
    "NetApp",
    "Nokia",
    "Nutanix",
    "Optum",
    "SAP America",
    "Samsung Electronics America",
    "ServiceNow",
    "Siemens",
    "Sirius XM",
    "Splunk",
    "Starbucks",
    "Synopsys",
    "Tesla",
    "Texas Instruments",
    "TikTok",
    "VMware",
    "Visa",
    "Wayfair",
    "Wells Fargo",
    "Xilinx",
]


def check_credits():
    """Check remaining Serper credits."""
    from db.quota import get_serper_quota
    used, limit = get_serper_quota()
    print(f"Serper credits: {used}/{limit} used, {limit - used} remaining")


def search_company(company, verbose=True):
    """Run Serper searches for one company and print all results."""
    from jobs.ats.patterns import match_ats_pattern

    print(f"\n{'='*60}")
    print(f"  {company}")
    print(f"{'='*60}")

    found_any = False

    for platform, site_filter in SERPER_SEARCHES:
        query = f"{company} {site_filter}"

        try:
            resp = requests.post(
                SERPER_URL,
                headers={
                    "X-API-KEY":    SERPER_API_KEY,
                    "Content-Type": "application/json",
                },
                json={"q": query, "num": 10},
                timeout=10
            )

            if resp.status_code != 200:
                print(f"  [{platform}] HTTP {resp.status_code} — skipping")
                continue

            items = resp.json().get("organic", [])

            print(f"\n  Query: {query}")
            print(f"  Results: {len(items)}")

            if not items:
                print("  → No results from Serper")
                continue

            for i, item in enumerate(items, 1):
                url     = item.get("link", "")
                title   = item.get("title", "")
                snippet = item.get("snippet", "")[:80]

                # Try to match ATS pattern
                result  = match_ats_pattern(url)
                matched = f"✓ {result['platform']} / {result['slug'][:40]}" \
                          if result else "✗ no pattern match"

                print(f"\n  [{i}] {matched}")
                print(f"       Title:   {title[:70]}")
                print(f"       URL:     {url[:90]}")
                print(f"       Snippet: {snippet}")

                if result:
                    found_any = True

        except Exception as e:
            print(f"  [{platform}] ERROR: {e}")

    if not found_any:
        print(f"\n  → CONCLUSION: No ATS detected for {company}")
        print(f"     Either uses custom ATS, SAP/Taleo, or Serper can't find it")

    return found_any


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--company", help="Test single company")
    parser.add_argument("--credits", action="store_true")
    parser.add_argument("--all",     action="store_true",
                        help="Test all 45 unknown companies (uses ~90 credits)")
    args = parser.parse_args()

    if not SERPER_API_KEY:
        print("[ERROR] SERPER_API_KEY not set in .env")
        sys.exit(1)

    if args.credits:
        check_credits()
        return

    if args.company:
        search_company(args.company)
        return

    if args.all:
        print(f"Testing {len(UNKNOWN_COMPANIES)} companies")
        print("This uses ~{len(UNKNOWN_COMPANIES)*2} Serper credits\n")
        detected = []
        not_detected = []
        for company in UNKNOWN_COMPANIES:
            found = search_company(company)
            if found:
                detected.append(company)
            else:
                not_detected.append(company)

        print(f"\n{'='*60}")
        print(f"SUMMARY")
        print(f"{'='*60}")
        print(f"Detected:     {len(detected)}")
        print(f"Not detected: {len(not_detected)}")
        print(f"\nNot detected (need Google Sheet):")
        for c in not_detected:
            print(f"  {c}")
        return

    # Default: interactive mode — pick company
    print("Unknown companies:")
    for i, c in enumerate(UNKNOWN_COMPANIES, 1):
        print(f"  {i:>2}. {c}")
    print()
    choice = input("Enter company number or name (or 'all'): ").strip()

    if choice.lower() == 'all':
        for company in UNKNOWN_COMPANIES:
            search_company(company)
    elif choice.isdigit():
        idx = int(choice) - 1
        if 0 <= idx < len(UNKNOWN_COMPANIES):
            search_company(UNKNOWN_COMPANIES[idx])
        else:
            print("Invalid number")
    else:
        search_company(choice)


if __name__ == "__main__":
    main()