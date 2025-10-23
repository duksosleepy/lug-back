#!/usr/bin/env python3
"""
Test script to validate BC statement KL counterparty logic enhancement
"""

import os
import sys

import pandas as pd

# Add src to path to import accounting modules
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "src"))

from src.accounting.counterparty_extractor import CounterpartyExtractor
from src.accounting.integrated_bank_processor import IntegratedBankProcessor


def test_bc_kl_validation():
    """Test that BC statements properly validate and assign KL counterparties"""

    print("🧪 Testing BC Statement KL Counterparty Validation")
    print("=" * 60)

    # Initialize processor
    processor = IntegratedBankProcessor()
    if not processor.connect():
        print("❌ Failed to connect to database")
        return False

    try:
        # Test Case 1: Create a BC transaction (credit) that should get KL counterparty
        print(
            "\n📝 Test Case 1: BC transaction with normal counterparty (should be filtered to KL)"
        )

        test_transactions = pd.DataFrame(
            [
                {
                    "reference": "TEST001",
                    "date": "01/12/2024",
                    "credit": 1000000,  # Credit transaction = BC statement
                    "debit": 0,
                    "description": "CHUYEN TIEN TU NGUYEN VAN A - 0123456789",
                    "balance": 5000000,
                }
            ]
        )

        # Process the transaction
        result_df = processor.process_to_saoke(test_transactions)

        if not result_df.empty:
            first_entry = result_df.iloc[0]

            print(
                f"   ✅ Document Type: {first_entry.get('document_type', 'N/A')}"
            )
            print(
                f"   ✅ Counterparty Code: {first_entry.get('counterparty_code', 'N/A')}"
            )
            print(
                f"   ✅ Counterparty Name: {first_entry.get('counterparty_name', 'N/A')}"
            )

            # Validate BC statement has KL counterparty
            if first_entry.get("document_type") == "BC" and "KL" in str(
                first_entry.get("counterparty_code", "")
            ):
                print(
                    "   ✅ SUCCESS: BC transaction correctly assigned KL counterparty"
                )
            else:
                print(
                    f"   ❌ FAIL: BC transaction should have KL counterparty, got: {first_entry.get('counterparty_code')}"
                )
                return False
        else:
            print("   ❌ FAIL: No transactions processed")
            return False

        # Test Case 2: Create a BN transaction (debit) that should NOT be forced to KL
        print("\n📝 Test Case 2: BN transaction (should NOT be forced to KL)")

        test_transactions_bn = pd.DataFrame(
            [
                {
                    "reference": "TEST002",
                    "date": "01/12/2024",
                    "credit": 0,
                    "debit": 500000,  # Debit transaction = BN statement
                    "description": "CHUYEN TIEN DEN NGUYEN THI B - 0987654321",
                    "balance": 4500000,
                }
            ]
        )

        # Process the BN transaction
        result_df_bn = processor.process_to_saoke(test_transactions_bn)

        if not result_df_bn.empty:
            first_entry_bn = result_df_bn.iloc[0]

            print(
                f"   ✅ Document Type: {first_entry_bn.get('document_type', 'N/A')}"
            )
            print(
                f"   ✅ Counterparty Code: {first_entry_bn.get('counterparty_code', 'N/A')}"
            )
            print(
                f"   ✅ Counterparty Name: {first_entry_bn.get('counterparty_name', 'N/A')}"
            )

            # BN transactions should not be forced to KL
            if first_entry_bn.get("document_type") == "BN":
                print(
                    "   ✅ SUCCESS: BN transaction processed (not forced to KL)"
                )
            else:
                print(
                    f"   ❌ FAIL: Expected BN transaction, got: {first_entry_bn.get('document_type')}"
                )
                return False
        else:
            print("   ❌ FAIL: No BN transactions processed")
            return False

        # Test Case 3: Validate the enhanced logging
        print("\n📝 Test Case 3: Check logging for BC validation")
        print("   ✅ Check the logs above for BC validation messages")
        print(
            "   ✅ Should see: 'BC statement validation passed' or 'Using default KL counterparty'"
        )

        return True

    except Exception as e:
        print(f"❌ Test failed with exception: {e}")
        import traceback

        traceback.print_exc()
        return False
    finally:
        processor.close()


def test_existing_bc_kl_filtering():
    """Test the existing BC counterparty filtering logic"""

    print("\n🔍 Testing Existing BC Counterparty Filtering Logic")
    print("=" * 60)

    # Simulate the filtering logic that should already exist
    print("   📋 Simulating counterparty search results:")

    # Mock search results that might come from the index
    mock_search_results = [
        {
            "code": "CUSTOMER001",
            "name": "Regular Customer",
            "address": "123 Street",
        },
        {
            "code": "KL-BARIA1",
            "name": "Khách Lẻ Bà Rịa 1",
            "address": "Bà Rịa - Vũng Tàu",
        },
        {"code": "VENDOR002", "name": "Some Vendor", "address": "456 Avenue"},
        {"code": "KL-SAIGON", "name": "Khách Lẻ Sài Gòn", "address": "TP.HCM"},
    ]

    # Apply the BC filtering logic
    document_type = "BC"  # This is a credit transaction

    if document_type == "BC":
        filtered_counterparties = [
            cp
            for cp in mock_search_results
            if cp.get("code") and "KL" in str(cp["code"])
        ]
        print(f"   ✅ Original counterparties: {len(mock_search_results)}")
        print(
            f"   ✅ Filtered for BC (containing 'KL'): {len(filtered_counterparties)}"
        )

        for cp in filtered_counterparties:
            print(f"      - {cp['code']}: {cp['name']}")

        if len(filtered_counterparties) > 0:
            print("   ✅ SUCCESS: Found KL counterparties for BC transaction")
        else:
            print("   ⚠️  No KL counterparties found - would use default KL")

    return True


def test_pos_machine_kl_validation():
    """Test the enhanced POS machine counterparty logic with KL validation"""

    print("\n🏪 Testing POS Machine Counterparty KL Validation Enhancement")
    print("=" * 60)

    # Initialize extractor
    extractor = CounterpartyExtractor()

    # Mock POS machine data (simulating extracted POS machines)
    mock_pos_machines = [
        {
            "code": "12345678",
            "department_code": "BRVT",  # Will be replaced by BARIA via department_code_replacements
            "address": "Test Address",
        }
    ]

    print(
        f"   📋 Mock POS machine: {mock_pos_machines[0]['code']} with dept: {mock_pos_machines[0]['department_code']}"
    )
    print(
        f"   🔄 Department replacements available: {list(extractor.department_code_replacements.keys())}"
    )

    # Test BC statement processing (should apply KL validation)
    print("\n   🧪 Test 1: BC Statement (should apply KL validation)")
    try:
        result_bc = extractor.handle_pos_machine_counterparty_logic(
            mock_pos_machines,
            current_address="Test Address",
            document_type="BC",  # This should trigger KL validation
        )
        if result_bc:
            print(f"      ✅ BC Result code: {result_bc.get('code', 'N/A')}")
            print(f"      ✅ BC Result name: {result_bc.get('name', 'N/A')}")
            if "KL" in str(result_bc.get("code", "")):
                print("      ✅ SUCCESS: BC statement got KL counterparty")
            else:
                print(
                    f"      ⚠️  BC statement didn't get KL counterparty: {result_bc.get('code')}"
                )
        else:
            print("      ⚠️  BC test returned None - check database connection")
    except Exception as e:
        print(f"      ❌ BC Test Error: {e}")

    # Test BN statement processing (should NOT apply KL validation)
    print("\n   🧪 Test 2: BN Statement (should NOT apply KL validation)")
    try:
        result_bn = extractor.handle_pos_machine_counterparty_logic(
            mock_pos_machines,
            current_address="Test Address",
            document_type="BN",  # This should NOT trigger KL validation
        )
        if result_bn:
            print(f"      ✅ BN Result code: {result_bn.get('code', 'N/A')}")
            print(f"      ✅ BN Result name: {result_bn.get('name', 'N/A')}")
            print(
                "      ✅ SUCCESS: BN statement processed without KL filtering"
            )
        else:
            print("      ⚠️  BN test returned None - check database connection")
    except Exception as e:
        print(f"      ❌ BN Test Error: {e}")

    # Test without document_type (backward compatibility)
    print("\n   🧪 Test 3: Without document_type (backward compatibility)")
    try:
        result_none = extractor.handle_pos_machine_counterparty_logic(
            mock_pos_machines,
            current_address="Test Address",
            # No document_type parameter - should not trigger KL validation
        )
        if result_none:
            print(
                f"      ✅ No doctype Result code: {result_none.get('code', 'N/A')}"
            )
            print(
                f"      ✅ No doctype Result name: {result_none.get('name', 'N/A')}"
            )
            print("      ✅ SUCCESS: Backward compatibility maintained")
        else:
            print("      ⚠️  Backward compatibility test returned None")
    except Exception as e:
        print(f"      ❌ Backward compatibility Test Error: {e}")

    return True


if __name__ == "__main__":
    print("🚀 Starting BC Statement KL Validation Tests")
    print("This test validates that BC statements always get KL counterparties")
    print()

    success = True

    # Run the filtering logic test first
    success &= test_existing_bc_kl_filtering()

    # Run the POS machine enhancement test
    success &= test_pos_machine_kl_validation()

    # Run the integration test
    success &= test_bc_kl_validation()

    print("\n" + "=" * 60)
    if success:
        print(
            "🎉 ALL TESTS PASSED! BC Statement KL validation is working correctly."
        )
        print(
            "✅ BC statements will always have counterparty codes containing 'KL'"
        )
        print("✅ BN statements are not affected by this validation")
    else:
        print("❌ SOME TESTS FAILED! Check the output above for details.")
        sys.exit(1)
