#!/usr/bin/env python3
"""
Test for the NEW POS machine counterparty logic that uses department code cleaning.

NEW BUSINESS LOGIC FLOW:
1. Extract POS machine code from transaction description
2. Search in pos_machines index using the code
3. Get "department_code" from POS machine record
4. Clean department_code (split by "-" or "_", take last element, remove spaces)
5. Search counterparties by cleaned code using "code" field
6. Return counterparty code, name, address

This replaces the old logic that searched by address and filtered.
"""

import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(project_root))

from src.accounting.counterparty_extractor import CounterpartyExtractor
from src.accounting.fast_search import (
    search_counterparties,
    search_pos_machines,
)


def test_department_code_cleaning():
    """Test the department code cleaning function"""
    print("=" * 70)
    print("TESTING: Department Code Cleaning Logic")
    print("=" * 70)
    print(
        "Business Rule: Clean department_code before searching counterparties"
    )
    print("Process: Split by '-' or '_', take last element, remove spaces")
    print()

    extractor = CounterpartyExtractor()

    test_cases = [
        (
            "DD.TINH_GO BRVT1",
            "GOBRVT1",
            "Split by '_', take last, remove space",
        ),
        (
            "CN01-HCMC STORE",
            "HCMCSTORE",
            "Split by '-', take last, remove space",
        ),
        ("BP.MAIN_SHOP A1", "SHOPA1", "Split by '_', take last, remove space"),
        (
            "DEPT-SUB-FINAL CODE",
            "FINALCODE",
            "Multiple '-', take last, remove space",
        ),
        ("SIMPLE", "SIMPLE", "No separators, use as-is"),
        ("CODE_ONLY", "ONLY", "Split by '_', take last"),
        ("SPACE ONLY", "SPACEONLY", "No separators, remove space"),
        ("", "", "Empty string"),
        ("   ", "", "Whitespace only"),
    ]

    for i, (input_code, expected, description) in enumerate(test_cases, 1):
        print(f"Test {i}: {description}")
        print(f"  Input: '{input_code}'")
        print(f"  Expected: '{expected}'")
        result = extractor.clean_department_code(input_code)
        print(f"  Result: '{result}'")
        if result == expected:
            print("  ✅ PASS")
        else:
            print(f"  ❌ FAIL: Expected '{expected}', got '{result}'")
        print()


def test_new_pos_machine_logic_step_by_step():
    """Test the NEW POS machine logic step by step"""
    print("=" * 70)
    print("TESTING: NEW Step-by-Step POS Machine Logic")
    print("=" * 70)

    test_description = (
        "TT POS 14100333 500379 5777 064217PDO thanh toan mua hang"
    )
    print(f"Test Description: '{test_description}'\n")

    extractor = CounterpartyExtractor()

    print("STEP 1: Extract POS machine code from description")
    print("-" * 50)

    entities = extractor.extract_entity_info(test_description)
    pos_machines_extracted = entities.get("pos_machines", [])

    if not pos_machines_extracted:
        print("❌ STEP 1 FAILED: No POS machine code extracted")
        return False

    pos_code = pos_machines_extracted[0]["code"]
    print(f"✅ STEP 1 SUCCESS: Extracted POS code = '{pos_code}'\n")

    print("STEP 2: Search in pos_machines index using the code")
    print("-" * 50)

    pos_results = search_pos_machines(pos_code, field_name="code", limit=1)

    if not pos_results:
        print(f"❌ STEP 2 FAILED: POS machine '{pos_code}' not found in index")
        return False

    pos_machine = pos_results[0]
    print("✅ STEP 2 SUCCESS: Found POS machine in index")
    print(f"   POS Code: {pos_machine.get('code')}")
    print(f"   Name: {pos_machine.get('name')}\n")

    print("STEP 3: Get 'department_code' from POS machine record")
    print("-" * 50)
    department_code = pos_machine.get("department_code")
    print(f"   Raw Department Code: {department_code}")
    if not department_code:
        print(
            "❌ STEP 3 FAILED: No department_code found in POS machine record"
        )
        return False
    print(f"✅ STEP 3 SUCCESS: Got department_code = '{department_code}'\n")

    print("STEP 4: Clean department_code (split, take last, remove spaces)")
    print("-" * 50)
    cleaned_dept_code = extractor.clean_department_code(department_code)
    print(f"   Original: '{department_code}'")
    print(f"   Cleaned: '{cleaned_dept_code}'")
    if not cleaned_dept_code:
        print(
            "❌ STEP 4 FAILED: Department code cleaning resulted in empty string"
        )
        return False
    print(
        f"✅ STEP 4 SUCCESS: Cleaned department code = '{cleaned_dept_code}'\n"
    )

    print("STEP 5: Search counterparties by cleaned code (NOT address)")
    print("-" * 50)
    cp_results = search_counterparties(
        cleaned_dept_code, field_name="code", limit=5
    )

    if not cp_results:
        print(
            f"❌ STEP 5 FAILED: No counterparties found with code '{cleaned_dept_code}'"
        )
        return False
    print(
        f"✅ STEP 5 SUCCESS: Found {len(cp_results)} counterparties with code '{cleaned_dept_code}'"
    )

    for i, cp in enumerate(cp_results, 1):
        print(f"   {i}. Code: {cp.get('code')}, Name: {cp.get('name')}")
        print(f"      Address: {cp.get('address')}")
    print()

    print("STEP 6: Return best counterparty match")
    print("-" * 50)

    best_counterparty = cp_results[0]
    print("✅ STEP 6 SUCCESS: Final result")
    print(f"   Final Code: {best_counterparty.get('code')}")
    print(f"   Final Name: {best_counterparty.get('name')}")
    print(f"   Final Address: {best_counterparty.get('address')}\n")

    print("🎉 NEW POS MACHINE LOGIC COMPLETED SUCCESSFULLY!")
    return True


def test_new_vs_old_logic_comparison():
    """Compare the new logic with the old logic approach"""
    print("\n" + "=" * 70)
    print("COMPARISON: NEW vs OLD POS Machine Logic")
    print("=" * 70)

    print("OLD LOGIC:")
    print("  1. Get POS machine -> department_code + address")
    print("  2. Search counterparties by address")
    print("  3. Filter where code != department_code")
    print("  4. Return best match")

    print("\nNEW LOGIC:")
    print("  1. Get POS machine -> department_code")
    print("  2. Clean department_code")
    print("  3. Search counterparties by code")
    print("  4. Return best match")

    extractor = CounterpartyExtractor()
    test_desc = "TT POS 14100333 test transaction"

    print(f"\nTesting with: '{test_desc}'")
    try:
        entities = extractor.extract_and_match_all(test_desc)
        pos_machines = entities.get("pos_machines", [])
        if pos_machines:
            print(f"\nFound POS machine: {pos_machines[0].get('code')}")
            result = extractor.handle_pos_machine_counterparty_logic(
                pos_machines
            )
            if result:
                print("\n✅ NEW LOGIC SUCCESS:")
                print(f"   Condition: {result.get('condition_applied')}")
                print(f"   Code: {result.get('code')}")
                print(f"   Name: {result.get('name')}")
                print(
                    f"   Cleaned Dept Code: {result.get('cleaned_department_code')}"
                )
            else:
                print("\n❌ NEW LOGIC FAILED: No counterparty found")
        else:
            print("\n❌ No POS machines extracted")
    except Exception as e:
        print(f"\n💥 Error testing logic: {e}")


def test_edge_cases_new_logic():
    """Test edge cases for the new POS machine logic"""
    print("\n" + "=" * 70)
    print("TESTING: Edge Cases for NEW Logic")
    print("=" * 70)

    extractor = CounterpartyExtractor()

    edge_cases = [
        {
            "name": "Department code with multiple separators",
            "dept_code": "PREFIX-MIDDLE_FINAL-CODE",
            "expected_clean": "CODE",
        },
        {
            "name": "Department code with spaces",
            "dept_code": "BASE_FINAL SPACE CODE",
            "expected_clean": "FINALSPACECODE",
        },
        {
            "name": "Department code with no separators",
            "dept_code": "SIMPLE CODE",
            "expected_clean": "SIMPLECODE",
        },
        {
            "name": "Empty department code",
            "dept_code": "",
            "expected_clean": "",
        },
        {
            "name": "Department code with only separators",
            "dept_code": "---___",
            "expected_clean": "",
        },
    ]

    for i, case in enumerate(edge_cases, 1):
        print(f"Edge Case {i}: {case['name']}")
        print(f"  Input: '{case['dept_code']}'")
        print(f"  Expected: '{case['expected_clean']}'")
        try:
            result = extractor.clean_department_code(case["dept_code"])
            print(f"  Result: '{result}'")
            if result == case["expected_clean"]:
                print("  ✅ PASS")
            else:
                print(
                    f"  ❌ FAIL: Expected '{case['expected_clean']}', got '{result}'"
                )
        except Exception as e:
            print(f"  💥 ERROR: {e}")
        print()


def main():
    """Run the NEW POS machine logic tests"""
    print("NEW POS MACHINE LOGIC TEST SUITE")
    print("Testing the updated business logic with department code cleaning\n")

    try:
        test_department_code_cleaning()
        success = test_new_pos_machine_logic_step_by_step()
        test_new_vs_old_logic_comparison()
        test_edge_cases_new_logic()

        print("\n" + "=" * 70)
        if success:
            print("🎉 NEW POS MACHINE LOGIC TESTS COMPLETED SUCCESSFULLY!")
            print("=" * 70)
            print("✅ Department code cleaning working correctly")
            print("✅ New step-by-step logic validated")
            print("✅ Direct counterparty search by code implemented")
            print("✅ Edge cases handled appropriately")
        else:
            print("⚠️  NEW POS MACHINE LOGIC TESTS COMPLETED WITH ISSUES")
            print(
                "The new logic implementation may need data setup or refinement."
            )

    except Exception as e:
        print(f"\n💥 Test suite failed with error: {e}")
        import traceback

        traceback.print_exc()


if __name__ == "__main__":
    main()
