"""
tests/run_tests.py — Run all tests with summary.

Usage:
    python tests/run_tests.py                  # all tests
    python tests/run_tests.py --unit           # unit tests only
    python tests/run_tests.py --integration    # integration tests only
    python tests/run_tests.py --scraper        # job scraper tests only
    python tests/run_tests.py --scraper --live # job scraper with live HTTP
"""

import sys
import os
import unittest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

os.makedirs("data", exist_ok=True)


def run_suite(test_modules):
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()
    for module in test_modules:
        suite.addTests(loader.loadTestsFromModule(module))
    runner = unittest.TextTestRunner(verbosity=2)
    return runner.run(suite)


def main():
    args = sys.argv[1:]

    from tests import (
        test_database,
        test_outreach,
        test_pipeline,
        test_integration,
        test_job_scraper,
    )

    unit_tests        = [test_database, test_outreach, test_pipeline]
    integration_tests = [test_integration]
    scraper_tests     = [test_job_scraper]
    all_tests         = unit_tests + integration_tests + scraper_tests

    if "--unit" in args:
        print("\n" + "=" * 60)
        print("Running Unit Tests")
        print("=" * 60)
        result = run_suite(unit_tests)

    elif "--integration" in args:
        print("\n" + "=" * 60)
        print("Running Integration Tests")
        print("=" * 60)
        result = run_suite(integration_tests)

    elif "--scraper" in args:
        print("\n" + "=" * 60)
        print("Running Job Scraper Tests")
        print("=" * 60)
        result = run_suite(scraper_tests)

    else:
        print("\n" + "=" * 60)
        print("Running All Tests")
        print("=" * 60)
        result = run_suite(all_tests)

    print("\n" + "=" * 60)
    if result.wasSuccessful():
        print(f"[OK] All {result.testsRun} tests passed!")
    else:
        print(f"[FAILED] {len(result.failures)} failure(s), "
              f"{len(result.errors)} error(s) out of {result.testsRun} tests")
    print("=" * 60)

    sys.exit(0 if result.wasSuccessful() else 1)


if __name__ == "__main__":
    main()