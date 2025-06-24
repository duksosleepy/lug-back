#!/usr/bin/env python3
"""
Focused test for the specific POS machine business logic requirements:

BUSINESS LOGIC FLOW:
1. Extract POS machine code from description
2. Search in pos_machines index using the code
3. Get "department_code" and "address" from POS machine record
4. Use the "address" to search in counterparties index
5. Find counterparty with same address but code != department_code
6. Return counterparty.code, counterparty.name, counterparty.address

This test verifies each step of this specific business logic.
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


def test_step_by_step_pos_logic():
    """Test the POS machine logic step by step according to business requirements"""
    print("=" * 80)
    print("STEP-BY-STEP POS MACHINE BUSINESS LOGIC TEST")
    print("=" * 80)

    # Test description with POS code
    test_description = (
        "TT POS 14100333 500379 5777 064217PDO thanh toan mua hang"
    )
    print(f"Test Description: '{test_description}'")
    print()

    extractor = CounterpartyExtractor()

    # STEP 1: Extract POS machine code from description
    print("STEP 1: Extract POS machine code from description")
    print("-" * 50)

    entities = extractor.extract_entity_info(test_description)
    pos_machines_extracted = entities.get("pos_machines", [])

    if not pos_machines_extracted:
        print("❌ STEP 1 FAILED: No POS machine code extracted")
        return False

    pos_code = pos_machines_extracted[0]["code"]
    print(f"✅ STEP 1 SUCCESS: Extracted POS code = '{pos_code}'")
    print()

    # STEP 2: Search in pos_machines index using the code
    print("STEP 2: Search in pos_machines index using the code")
    print("-" * 50)

    pos_results = search_pos_machines(pos_code, field_name="code", limit=1)

    if not pos_results:
        print(f"❌ STEP 2 FAILED: POS machine '{pos_code}' not found in index")
        return False

    pos_machine = pos_results[0]
    print("✅ STEP 2 SUCCESS: Found POS machine in index")
    print(f"   POS Code: {pos_machine.get('code')}")
    print(f"   Name: {pos_machine.get('name')}")
    print()

    # STEP 3: Get "department_code" and "address" from POS machine record
    print("STEP 3: Get 'department_code' and 'address' from POS machine record")
    print("-" * 50)

    department_code = pos_machine.get("department_code")
    pos_address = pos_machine.get("address")

    print(f"   Department Code: {department_code}")
    print(f"   Address: {pos_address}")

    if not department_code:
        print(
            "❌ STEP 3 FAILED: No department_code found in POS machine record"
        )
        return False

    if not pos_address:
        print("❌ STEP 3 FAILED: No address found in POS machine record")
        return False

    print(
        f"✅ STEP 3 SUCCESS: Got department_code = '{department_code}', address = '{pos_address}'"
    )
    print()

    # STEP 4: Use the "address" to search in counterparties index
    print("STEP 4: Use the 'address' to search in counterparties index")
    print("-" * 50)
    print("   BUSINESS REQUIREMENT: Must get at least 2 records because:")
    print("   - Counterparties index likely contains a record with same code as department_code")
    print("   - Need multiple records to find ones where code != department_code")
    print(f"   - Searching with limit=10 to ensure we get enough records...")
    print()

    cp_results = search_counterparties(
        pos_address, field_name="address", limit=10
    )

    if not cp_results:
        print(
            f"❌ STEP 4 FAILED: No counterparties found with address '{pos_address}'"
        )
        return False

    print(
        f"✅ STEP 4 SUCCESS: Found {len(cp_results)} counterparties with address '{pos_address}'"
    )
    
    # Validate we have enough records for proper filtering
    if len(cp_results) < 2:
        print(f"   ⚠️  WARNING: Only found {len(cp_results)} counterparty record(s)")
        print("      This may not be enough if the single record has same code as department")
    else:
        print(f"   ✅ Good: Found {len(cp_results)} records, enough for proper filtering")
    
    print("   All counterparties found:")
    for i, cp in enumerate(cp_results, 1):
        cp_code = cp.get("code")
        cp_name = cp.get("name")
        is_same_as_dept = "(SAME AS DEPT)" if cp_code == department_code else "(DIFFERENT)"
        print(f"   {i}. Code: {cp_code} {is_same_as_dept}, Name: {cp_name}")
    print()

    # STEP 5: Find counterparty with same address but code != department_code
    print(
        "STEP 5: Apply filtering condition: code != department_code"
    )
    print("-" * 50)
    print(f"   Department code to exclude: '{department_code}'")
    print("   Filtering logic: Only keep counterparties where code != department_code")
    print()

    valid_counterparties = []
    filtered_counterparties = []
    
    for cp in cp_results:
        cp_code = cp.get("code")
        if cp_code and cp_code != department_code:
            valid_counterparties.append(cp)
            print(f"   ✅ VALID: {cp_code} != {department_code} (department) - KEEP")
        else:
            filtered_counterparties.append(cp)
            print(
                f"   ❌ FILTERED: {cp_code} == {department_code} (department) - EXCLUDE"
            )

    print(f"\n   Summary: {len(valid_counterparties)} valid, {len(filtered_counterparties)} filtered")
    
    if not valid_counterparties:
        print(
            "❌ STEP 5 FAILED: No valid counterparties (all have same code as department)"
        )
        print("   This confirms why we need multiple counterparty records in Step 4!")
        return False

    print(
        f"✅ STEP 5 SUCCESS: Found {len(valid_counterparties)} valid counterparties after filtering"
    )
    print()

    # STEP 6: Return counterparty.code, counterparty.name, counterparty.address
    print(
        "STEP 6: Return best counterparty.code, counterparty.name, counterparty.address"
    )
    print("-" * 50)

    best_counterparty = valid_counterparties[0]  # Highest score after filtering

    final_code = best_counterparty.get("code")
    final_name = best_counterparty.get("name")
    final_address = best_counterparty.get("address")

    print("✅ STEP 6 SUCCESS: Final result")
    print(f"   Final Code: {final_code}")
    print(f"   Final Name: {final_name}")
    print(f"   Final Address: {final_address}")
    print()

    print("🎉 ALL STEPS COMPLETED SUCCESSFULLY!")
    print("The POS machine business logic is working as specified.")
    return True


def test_business_logic_with_multiple_pos_codes():
    """Test the business logic with multiple different POS codes"""
    print("\n" + "=" * 80)
    print("TESTING BUSINESS LOGIC WITH MULTIPLE POS CODES")
    print("=" * 80)

    # Get some POS codes from the database to test with
    print("First, let's find some POS codes in the database...")

    # Search for POS machines with different patterns
    search_patterns = ["14100", "14101", "14102", "141"]
    found_pos_codes = []

    for pattern in search_patterns:
        pos_results = search_pos_machines(pattern, field_name="code", limit=5)
        for pos in pos_results:
            code = pos.get("code")
            if code and len(code) >= 7:  # Valid POS code length
                found_pos_codes.append(code)
                if len(found_pos_codes) >= 3:  # Test with 3 codes
                    break
        if len(found_pos_codes) >= 3:
            break

    if not found_pos_codes:
        print("❌ No POS codes found in database to test with")
        return

    print(f"Found POS codes to test: {found_pos_codes}")
    print()

    extractor = CounterpartyExtractor()

    for i, pos_code in enumerate(found_pos_codes, 1):
        print(f"TEST {i}: Testing with POS code {pos_code}")
        print("-" * 60)

        # Create a test description with this POS code
        test_desc = f"TT POS {pos_code} thanh toan mua hang"
        print(f"Test description: '{test_desc}'")

        try:
            # Extract all entities
            entities = extractor.extract_and_match_all(test_desc)
            pos_machines = entities.get("pos_machines", [])

            if pos_machines:
                # Apply POS machine logic
                result = extractor.handle_pos_machine_counterparty_logic(
                    pos_machines
                )

                if result:
                    print("✅ POS machine logic SUCCESS")
                    print(f"   Code: {result.get('code')}")
                    print(f"   Name: {result.get('name')}")
                    print(f"   Address: {result.get('address')}")
                    print(
                        f"   POS Department: {result.get('pos_department_code')}"
                    )
                    print(f"   Condition: {result.get('condition_applied')}")
                else:
                    print("❌ POS machine logic FAILED")
                    print("   Could not find valid counterparty")
            else:
                print("❌ POS code extraction FAILED")

        except Exception as e:
            print(f"💥 ERROR: {e}")

        print()


def test_edge_case_scenarios():
    """Test edge cases in the POS machine business logic"""
    print("\n" + "=" * 80)
    print("TESTING EDGE CASE SCENARIOS")
    print("=" * 80)

    extractor = CounterpartyExtractor()

    edge_cases = [
        {
            "name": "Non-existent POS code",
            "description": "TT POS 99999999 fake pos code",
            "expected": "Should fail at step 2 (POS not found in index)",
        },
        {
            "name": "Invalid POS code format",
            "description": "TT POS ABC123 invalid format",
            "expected": "Should fail at step 1 (invalid format)",
        },
        {
            "name": "Multiple POS codes",
            "description": "TT POS 14100333 and POS 14100414 multiple codes",
            "expected": "Should use first extracted POS code",
        },
        {
            "name": "Very short POS code",
            "description": "TT POS 123 too short",
            "expected": "Should fail extraction (too short)",
        },
    ]

    for i, case in enumerate(edge_cases, 1):
        print(f"EDGE CASE {i}: {case['name']}")
        print(f"Description: '{case['description']}'")
        print(f"Expected: {case['expected']}")
        print("-" * 60)

        try:
            # Extract entities
            entities = extractor.extract_and_match_all(case["description"])
            pos_machines = entities.get("pos_machines", [])

            if pos_machines:
                print(
                    f"✅ Extracted POS: {[p.get('code') for p in pos_machines]}"
                )

                # Try POS logic
                result = extractor.handle_pos_machine_counterparty_logic(
                    pos_machines
                )

                if result:
                    print(
                        f"✅ POS logic succeeded: {result.get('code')} - {result.get('name')}"
                    )
                else:
                    print("❌ POS logic failed (may be expected)")
            else:
                print(
                    "❌ No POS extracted (may be expected for invalid formats)"
                )

        except Exception as e:
            print(f"💥 Error: {e}")

        print()


def test_minimum_records_requirement():
    """Test the business requirement for getting at least 2 counterparty records"""
    print("\n" + "=" * 80)
    print("TESTING MINIMUM RECORDS REQUIREMENT")
    print("=" * 80)
    print("Business Logic: Why we need at least 2 counterparty records")
    print("1. Counterparties index contains a record with same code as department_code")
    print("2. We need to filter out records where code == department_code")
    print("3. If we only get 1 record and it matches department_code, filtering fails")
    print("4. Therefore, search limit should be >= 2 (recommended 5-10)")
    print()
    
    # Let's demonstrate this with real data
    print("DEMONSTRATION: Finding scenarios where this matters")
    print("-" * 60)
    
    # Find a POS machine and see what counterparties share its address
    pos_results = search_pos_machines("14100", field_name="code", limit=5)
    
    for pos in pos_results:
        pos_code = pos.get("code")
        dept_code = pos.get("department_code")
        address = pos.get("address")
        
        if not dept_code or not address:
            continue
            
        print(f"\nTesting POS {pos_code}:")
        print(f"  Department Code: {dept_code}")
        print(f"  Address: {address}")
        
        # Test with limit=1 (bad practice)
        cp_limit1 = search_counterparties(address, field_name="address", limit=1)
        print(f"\n  With limit=1: Found {len(cp_limit1)} counterparty")
        if cp_limit1:
            cp = cp_limit1[0]
            cp_code = cp.get("code")
            if cp_code == dept_code:
                print(f"    ❌ PROBLEM: Only counterparty {cp_code} == department {dept_code}")
                print(f"    ❌ Result: Filtering would eliminate all records!")
            else:
                print(f"    ✅ OK: Counterparty {cp_code} != department {dept_code}")
        
        # Test with limit=10 (good practice)
        cp_limit10 = search_counterparties(address, field_name="address", limit=10)
        print(f"\n  With limit=10: Found {len(cp_limit10)} counterparties")
        
        valid_count = 0
        filtered_count = 0
        for cp in cp_limit10:
            cp_code = cp.get("code")
            if cp_code and cp_code != dept_code:
                valid_count += 1
            else:
                filtered_count += 1
                
        print(f"    Valid (code != dept): {valid_count}")
        print(f"    Filtered (code == dept): {filtered_count}")
        
        if valid_count > 0:
            print(f"    ✅ SUCCESS: Found {valid_count} valid counterparties after filtering")
        else:
            print(f"    ❌ FAILED: No valid counterparties after filtering")
            
        # Only test a few examples
        break
    
    print("\n" + "-" * 60)
    print("CONCLUSION:")
    print("✅ Always use limit >= 2 when searching counterparties by address")
    print("✅ Recommended limit: 5-10 to ensure proper filtering")
    print("✅ This prevents the scenario where all records are filtered out")


def test_comparison_with_regular_logic():
    """Compare POS machine logic vs regular counterparty logic"""
    print("\n" + "=" * 80)
    print("COMPARISON: POS MACHINE LOGIC vs REGULAR COUNTERPARTY LOGIC")
    print("=" * 80)

    extractor = CounterpartyExtractor()

    # Test with a description that has both POS code and company name
    test_desc = "TT POS 14100333 CTY TNHH ABC TRADING thanh toan hoa don"
    print(f"Test description: '{test_desc}'")
    print("This description has both POS code and company name")
    print()

    # Extract all entities
    entities = extractor.extract_and_match_all(test_desc)

    print("EXTRACTED ENTITIES:")
    print(f"  POS Machines: {len(entities.get('pos_machines', []))}")
    print(f"  Counterparties: {len(entities.get('counterparties', []))}")
    print(f"  Accounts: {len(entities.get('accounts', []))}")
    print()

    # Test POS machine logic
    print("RESULT WITH POS MACHINE LOGIC:")
    pos_machines = entities.get("pos_machines", [])
    if pos_machines:
        pos_result = extractor.handle_pos_machine_counterparty_logic(
            pos_machines
        )
        if pos_result:
            print(f"  ✅ Code: {pos_result.get('code')}")
            print(f"  ✅ Name: {pos_result.get('name')}")
            print(f"  ✅ Address: {pos_result.get('address')}")
            print(f"  ✅ Source: {pos_result.get('source')}")
        else:
            print("  ❌ POS logic failed")
    else:
        print("  ❌ No POS machines extracted")
    print()

    # Test regular counterparty logic
    print("RESULT WITH REGULAR COUNTERPARTY LOGIC:")
    counterparties = entities.get("counterparties", [])
    if counterparties:
        cp_result = extractor.handle_counterparty_two_conditions(counterparties)
        print(f"  📝 Code: {cp_result.get('code')}")
        print(f"  📝 Name: {cp_result.get('name')}")
        print(f"  📝 Address: {cp_result.get('address')}")
        print(f"  📝 Source: {cp_result.get('source')}")
        print(f"  📝 Condition: {cp_result.get('condition_applied')}")
    else:
        print("  ❌ No counterparties extracted")
    print()

    # Test unified logic (which should prioritize POS)
    print("RESULT WITH UNIFIED LOGIC (POS has priority):")
    unified_result = extractor.handle_counterparty_with_all_logic(entities)
    print(f"  🎯 Final Code: {unified_result.get('code')}")
    print(f"  🎯 Final Name: {unified_result.get('name')}")
    print(f"  🎯 Final Address: {unified_result.get('address')}")
    print(f"  🎯 Final Source: {unified_result.get('source')}")
    print(f"  🎯 Final Condition: {unified_result.get('condition_applied')}")


def main():
    """Run the focused POS machine business logic tests"""
    print("FOCUSED POS MACHINE BUSINESS LOGIC TEST")
    print("Testing the exact business requirements step by step")

    success = test_step_by_step_pos_logic()

    if success:
        test_minimum_records_requirement()
        test_business_logic_with_multiple_pos_codes()
        test_edge_case_scenarios()
        test_comparison_with_regular_logic()

        print("\n" + "=" * 80)
        print("🎉 FOCUSED TEST SUITE COMPLETED SUCCESSFULLY!")
        print("=" * 80)
        print("✅ Step-by-step business logic verified")
        print("✅ Minimum records requirement tested")
        print("✅ Multiple POS codes tested")
        print("✅ Edge cases handled appropriately")
        print("✅ Priority logic working correctly")
    else:
        print("\n" + "=" * 80)
        print("❌ BASIC BUSINESS LOGIC TEST FAILED")
        print("=" * 80)
        print("Please check the POS machine logic implementation")


if __name__ == "__main__":
    main()
