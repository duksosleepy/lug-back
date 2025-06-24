#!/usr/bin/env python3
"""
Test Runner for POS Machine Logic Tests

This script provides an easy way to run all POS machine logic tests
and get a quick overview of the business logic implementation.
"""

import subprocess
import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(project_root))


def run_test(test_file, description):
    """Run a single test file and report results"""
    print(f"\n{'=' * 80}")
    print(f"RUNNING: {description}")
    print(f"File: {test_file}")
    print(f"{'=' * 80}")

    try:
        # Run the test file
        result = subprocess.run(
            [sys.executable, str(Path(__file__).parent / test_file)],
            capture_output=True,
            text=True,
            timeout=120,
        )

        if result.returncode == 0:
            print("✅ TEST PASSED")
            print(result.stdout)
        else:
            print("❌ TEST FAILED")
            print("STDOUT:", result.stdout)
            print("STDERR:", result.stderr)

        return result.returncode == 0

    except subprocess.TimeoutExpired:
        print("⏰ TEST TIMED OUT (>120 seconds)")
        return False
    except Exception as e:
        print(f"💥 ERROR RUNNING TEST: {e}")
        return False


def run_quick_validation():
    """Run a quick validation to check if the system is set up correctly"""
    print("🔍 QUICK SYSTEM VALIDATION")
    print("-" * 40)

    try:
        # Test basic imports
        from src.accounting.counterparty_extractor import CounterpartyExtractor
        from src.accounting.fast_search import (
            search_counterparties,
            search_pos_machines,
        )

        print("✅ All required modules import successfully")

        # Test basic functionality
        extractor = CounterpartyExtractor()
        test_desc = "TT POS 14100333 test transaction"
        entities = extractor.extract_entity_info(test_desc)
        print("✅ CounterpartyExtractor working")

        # Test database access
        pos_results = search_pos_machines("14100", field_name="code", limit=1)
        print(
            f"✅ POS machine search working (found {len(pos_results)} results)"
        )

        cp_results = search_counterparties("test", limit=1)
        print(
            f"✅ Counterparty search working (found {len(cp_results)} results)"
        )

        return True

    except Exception as e:
        print(f"❌ System validation failed: {e}")
        return False


def main():
    """Main test runner function"""
    print("🧪 POS MACHINE LOGIC TEST RUNNER")
    print("=" * 80)
    print("This will test your UPDATED POS machine business logic:")
    print("1. Extract POS code from description")
    print("2. Search POS machine in index (get department_code)")
    print("3. Clean department_code (split by '-'/'_', take last, remove spaces)")
    print("4. Search counterparties by cleaned code (NOT address)")
    print("5. Return best counterparty match")
    print("=" * 80)

    # Run system validation first
    if not run_quick_validation():
        print("\n❌ System validation failed. Please check your setup.")
        return

    print("\n✅ System validation passed. Running tests...")

    # Test configurations
    tests = [
        {
            "file": "test_new_pos_logic.py",
            "description": "NEW POS Logic with Department Code Cleaning",
            "priority": "HIGH",
        },
        {
            "file": "test_minimum_records.py",
            "description": "Minimum Records Requirement Test (Quick)",
            "priority": "HIGH",
        },
        {
            "file": "test_pos_focused.py",
            "description": "OLD POS Logic (Step-by-Step Validation)",
            "priority": "NORMAL",
        },
        {
            "file": "test_pos_machine_logic.py",
            "description": "Comprehensive POS Machine Logic Test Suite",
            "priority": "NORMAL",
        },
        {
            "file": "test_two_condition_logic.py",
            "description": "Two-Condition Counterparty Logic Test",
            "priority": "NORMAL",
        },
    ]

    results = {}

    # Ask user which tests to run
    print("\nAvailable tests:")
    for i, test in enumerate(tests, 1):
        priority_marker = "🔥" if test["priority"] == "HIGH" else "📝"
        print(f"  {i}. {priority_marker} {test['description']}")

    print("\nOptions:")
    print(f"  1-{len(tests)}: Run specific test")
    print("  a: Run all tests")
    print("  f: Run focused test only (recommended)")
    print("  q: Quit")

    choice = input("\nEnter your choice: ").strip().lower()

    if choice == "q":
        print("👋 Exiting...")
        return

    elif choice == "f":
        # Run only the focused test
        print("\n🎯 Running focused business logic test...")
        success = run_test(
            "test_pos_focused.py", "Focused Step-by-Step Business Logic Test"
        )
        if success:
            print("\n🎉 Focused test completed successfully!")
            print(
                "Your POS machine business logic appears to be working correctly."
            )
        else:
            print("\n❌ Focused test failed. Please check the implementation.")

    elif choice == "a":
        # Run all tests
        print("\n🚀 Running all tests...")
        for test in tests:
            success = run_test(test["file"], test["description"])
            results[test["file"]] = success

        # Summary
        print(f"\n{'=' * 80}")
        print("📊 TEST SUMMARY")
        print(f"{'=' * 80}")
        for test_file, success in results.items():
            status = "✅ PASSED" if success else "❌ FAILED"
            print(f"  {test_file}: {status}")

        total_tests = len(results)
        passed_tests = sum(results.values())
        print(f"\nOverall: {passed_tests}/{total_tests} tests passed")

        if passed_tests == total_tests:
            print(
                "🎉 All tests passed! Your POS machine logic is working correctly."
            )
        else:
            print("⚠️ Some tests failed. Please review the implementation.")

    elif choice.isdigit() and 1 <= int(choice) <= len(tests):
        # Run specific test
        test_index = int(choice) - 1
        test = tests[test_index]
        print(f"\n🎯 Running: {test['description']}")
        success = run_test(test["file"], test["description"])
        if success:
            print(f"\n✅ Test '{test['description']}' completed successfully!")
        else:
            print(f"\n❌ Test '{test['description']}' failed.")

    else:
        print("❌ Invalid choice. Please try again.")


if __name__ == "__main__":
    main()
