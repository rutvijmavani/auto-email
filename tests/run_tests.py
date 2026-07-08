"""
tests/run_tests.py — Run all tests with summary.

Usage:
    python tests/run_tests.py                  # all unit + integration tests
    python tests/run_tests.py --unit           # unit tests only
    python tests/run_tests.py --integration    # integration tests only
    python tests/run_tests.py --scraper        # job scraper tests only
    python tests/run_tests.py --scraper --live # job scraper with live HTTP
    python tests/run_tests.py --e2e            # unit tests + full end-to-end test
                                               # (requires Redis + DB + live HTTP)

--e2e runs the complete dual-tier pipeline test (scripts/test_e2e.py) after
the unit suite passes.  Covers Mode A (Greenhouse) and Mode B (Workday) detail
worker paths, fullscan, watchdog, and Redis signal handling.  Needs network
access to Greenhouse and Workday APIs.
"""

import subprocess
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


def _print_summary(result):
    print("\n" + "=" * 60)
    if result.wasSuccessful():
        print(f"[OK] All {result.testsRun} tests passed!")
    else:
        print(f"[FAILED] {len(result.failures)} failure(s), "
              f"{len(result.errors)} error(s) out of {result.testsRun} tests")
    print("=" * 60)


def main():
    args = sys.argv[1:]

    import importlib
    _import_failures = []

    def _try_import(name):
        try:
            return importlib.import_module(f"tests.{name}")
        except Exception as exc:
            _import_failures.append((name, exc))
            print(f"[ERROR] Could not import tests.{name}: {exc}", file=sys.stderr)
            return None

    # Single source of truth for all module names grouped by suite.
    # _loaded is the canonical mapping; suite lists are built from it below.
    _unit_names = [
        "test_database", "test_outreach", "test_pipeline",
        "test_validation", "test_verify_only", "test_prospective",
        "test_reports", "test_job_monitor", "test_location_filter",
        # Phase 10/11 comprehensive suites
        "test_adaptive_engine", "test_adaptive_health",
        "test_fullscan_helpers", "test_paginator",
        "test_watchdog", "test_pipeline_health", "test_phase11_alerts",
        # ATS + scheduler contract suites
        "test_ats_payload_contract", "test_detail_worker_contracts",
        "test_scheduler_contracts",
        # Phase 3 new-feature suites
        "test_detail_worker_inflight", "test_heartbeat",
        "test_monitor_report_phase1", "test_startup_validation",
        # Pipeline CLI suites
        "test_add", "test_find_only", "test_outreach_only",
        # DB + worker suites
        "test_api_health", "test_api_health_p95", "test_fullscan_bootstrap",
        "test_job_monitor_slot", "test_scan_worker_streams",
        "test_scheduler_streams", "test_slot",
        # Redis client
        "test_redis_client",
    ]
    _integration_names = ["test_integration"]
    _scraper_names     = ["test_job_scraper"]
    _all_names         = _unit_names + _integration_names + _scraper_names

    # Only import modules needed for the selected suite so a broken unrelated
    # test module does not block a targeted --unit, --integration, or --scraper run.
    if "--unit" in args or "--e2e" in args:
        _names_to_import = _unit_names
    elif "--integration" in args:
        _names_to_import = _integration_names
    elif "--scraper" in args:
        _names_to_import = _scraper_names
    else:
        _names_to_import = _all_names  # default: load everything

    _loaded = {n: _try_import(n) for n in _names_to_import}

    if _import_failures:
        print(f"\n[ERROR] {len(_import_failures)} module(s) failed to import:",
              file=sys.stderr)
        for _name, _exc in _import_failures:
            print(f"       tests.{_name}: {_exc}", file=sys.stderr)
        print("[ERROR] Fix import errors before running the suite.", file=sys.stderr)
        sys.exit(1)

    unit_tests        = [_loaded[n] for n in _unit_names        if _loaded.get(n)]
    integration_tests = [_loaded[n] for n in _integration_names if _loaded.get(n)]
    scraper_tests     = [_loaded[n] for n in _scraper_names     if _loaded.get(n)]
    all_tests         = unit_tests + integration_tests + scraper_tests

    # Only enforce the non-empty unit-suite guard when we're actually running units.
    if not args or "--unit" in args or "--e2e" in args:
        if not unit_tests:
            print("[ERROR] No unit test modules loaded — aborting.", file=sys.stderr)
            sys.exit(1)

    if "--unit" in args:
        print("\n" + "=" * 60)
        print("Running Unit Tests")
        print("=" * 60)
        result = run_suite(unit_tests)
        _print_summary(result)
        sys.exit(0 if result.wasSuccessful() else 1)

    elif "--integration" in args:
        print("\n" + "=" * 60)
        print("Running Integration Tests")
        print("=" * 60)
        result = run_suite(integration_tests)
        _print_summary(result)
        sys.exit(0 if result.wasSuccessful() else 1)

    elif "--scraper" in args:
        print("\n" + "=" * 60)
        print("Running Job Scraper Tests")
        print("=" * 60)
        result = run_suite(scraper_tests)
        _print_summary(result)
        sys.exit(0 if result.wasSuccessful() else 1)

    elif "--e2e" in args:
        # ── Phase 1: unit tests ───────────────────────────────────────────────
        print("\n" + "=" * 60)
        print("Phase 1 — Unit Tests")
        print("=" * 60)
        result = run_suite(unit_tests)
        _print_summary(result)
        if not result.wasSuccessful():
            print("\n[ABORTED] Unit tests failed — skipping E2E test")
            sys.exit(1)

        # ── Phase 2: end-to-end test (scripts/test_e2e.py) ───────────────────
        print("\n" + "=" * 60)
        print("Phase 2 — End-to-End Test  (scripts/test_e2e.py)")
        print("Requires: Redis running, DB populated, network access")
        print("=" * 60 + "\n")

        project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
        e2e_script   = os.path.join(project_root, "scripts", "test_e2e.py")

        ret = subprocess.run(
            [sys.executable, e2e_script],
            cwd=project_root,
        )

        print("\n" + "=" * 60)
        if ret.returncode == 0:
            print(f"[OK] All {result.testsRun} unit tests + E2E test passed!")
        else:
            print(f"[FAILED] Unit tests passed but E2E test failed "
                  f"(exit code {ret.returncode})")
        print("=" * 60)
        sys.exit(ret.returncode)

    else:
        print("\n" + "=" * 60)
        print("Running All Tests")
        print("=" * 60)
        result = run_suite(all_tests)
        _print_summary(result)
        sys.exit(0 if result.wasSuccessful() else 1)


if __name__ == "__main__":
    main()