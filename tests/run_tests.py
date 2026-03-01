"""
tests/run_tests.py — Run all unit and integration tests with summary.

Usage:
    python tests/run_tests.py              # run all tests
    python tests/run_tests.py --unit       # run unit tests only
    python tests/run_tests.py --integration # run integration tests only
"""

import sys
import os
import unittest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

# Ensure data directory exists
os.makedirs("data", exist_ok=True)


def run_suite(test_modules):
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()

    for module in test_modules:
        suite.addTests(loader.loadTestsFromModule(module))

    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    return result


def main():
    args = sys.argv[1:]

    from tests import test_add, test_find_only, test_outreach_only, test_integration

    unit_tests = [test_add, test_find_only, test_outreach_only]
    integration_tests = [test_integration]

    if "--unit" in args:
        print("\n" + "=" * 60)
        print("[TEST] Running Unit Tests")
        print("=" * 60)
        result = run_suite(unit_tests)

    elif "--integration" in args:
        print("\n" + "=" * 60)
        print("[INFO] Running Integration Tests")
        print("=" * 60)
        result = run_suite(integration_tests)

    else:
        print("\n" + "=" * 60)
        print("[TEST] Running All Tests (Unit + Integration)")
        print("=" * 60)
        result = run_suite(unit_tests + integration_tests)

    print("\n" + "=" * 60)
    if result.wasSuccessful():
        print(f"[OK] All {result.testsRun} tests passed!")
    else:
        print(f"[ERROR] {len(result.failures)} failure(s), {len(result.errors)} error(s) out of {result.testsRun} tests")
    print("=" * 60)

    sys.exit(0 if result.wasSuccessful() else 1)


if __name__ == "__main__":
    main()