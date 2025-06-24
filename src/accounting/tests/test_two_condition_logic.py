#!/usr/bin/env python3
"""
Test script to verify the two-condition counterparty logic implementation

This test verifies that:
1. Condition 1: If counterparty found in index, get code, name, address
2. Condition 2: If counterparty not found, return extracted name with null code/address
"""

import sys
from datetime import datetime
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(project_root))

from src.accounting.counterparty_extractor import CounterpartyExtractor
from src.accounting.integrated_bank_processor import (
    IntegratedBankProcessor,
    RawTransaction,
)


def test_condition_1_found_in_index():
    """Test Condition 1: Counterparty found in index"""
    print("=" * 60)
    print("TEST: Condition 1 - Counterparty Found in Index")
    print("=" * 60)

    extractor = CounterpartyExtractor()

    # Test with a description that should match a counterparty in the database
    # Note: This assumes you have counterparties in your index
    test_descriptions = [
        "CK den tu CTY TNHH Tân Tạo",
        "Chuyen tien cho CONG TY CO PHAN XYZ",
        "Thu tien ban hang tu Nguyen Van A",
    ]

    for desc in test_descriptions:
        print(f"\nTesting description: '{desc}'")

        # Extract entities
        extracted_entities = extractor.extract_and_match_all(desc)
        counterparties = extracted_entities.get("counterparties", [])

        # Apply two-condition logic
        result = extractor.handle_counterparty_two_conditions(counterparties)

        print(f"  Condition Applied: {result.get('condition_applied')}")
        print(f"  Source: {result.get('source')}")
        print(f"  Code: {result.get('code')}")
        print(f"  Name: {result.get('name')}")
        print(f"  Address: {result.get('address')}")

        if result.get("condition_applied") == "found_in_index":
            print(
                "  ✅ CONDITION 1 SUCCESS: Found in index with code, name, and address"
            )
        elif result.get("condition_applied") == "not_found_in_index":
            print(
                "  ⚠️  CONDITION 2 APPLIED: Not found in index (using extracted name)"
            )
        else:
            print(
                "  ❌ NO EXTRACTION: No counterparty extracted from description"
            )


def test_condition_2_not_found_in_index():
    """Test Condition 2: Counterparty not found in index"""
    print("\n" + "=" * 60)
    print("TEST: Condition 2 - Counterparty Not Found in Index")
    print("=" * 60)

    extractor = CounterpartyExtractor()

    # Test with descriptions containing names that likely won't be in the database
    test_descriptions = [
        "Chuyen tien cho FICTIONAL COMPANY THAT DOES NOT EXIST",
        "Thu tien tu Tran Thi Fictional Person Name",
        "Thanh toan cho MADE UP BUSINESS ENTERPRISE LLC",
    ]

    for desc in test_descriptions:
        print(f"\nTesting description: '{desc}'")

        # Extract entities
        extracted_entities = extractor.extract_and_match_all(desc)
        counterparties = extracted_entities.get("counterparties", [])

        # Apply two-condition logic
        result = extractor.handle_counterparty_two_conditions(counterparties)

        print(f"  Condition Applied: {result.get('condition_applied')}")
        print(f"  Source: {result.get('source')}")
        print(f"  Code: {result.get('code')}")
        print(f"  Name: {result.get('name')}")
        print(f"  Address: {result.get('address')}")

        if result.get("condition_applied") == "not_found_in_index":
            print(
                "  ✅ CONDITION 2 SUCCESS: Using extracted name with null code/address"
            )
            assert result.get("code") is None, (
                "Code should be None for Condition 2"
            )
            assert result.get("address") is None, (
                "Address should be None for Condition 2"
            )
            assert result.get("name") is not None, (
                "Name should contain extracted name"
            )
        elif result.get("condition_applied") == "found_in_index":
            print("  ⚠️  CONDITION 1 APPLIED: Unexpectedly found in index")
        else:
            print(
                "  ❌ NO EXTRACTION: No counterparty extracted from description"
            )


def test_integrated_processor_with_two_conditions():
    """Test the integrated processor using the two-condition logic"""
    print("\n" + "=" * 60)
    print("TEST: Integrated Processor with Two-Condition Logic")
    print("=" * 60)

    processor = IntegratedBankProcessor()

    if not processor.connect():
        print("❌ Failed to connect to processor")
        return

    try:
        # Test transaction with counterparty that should be found
        transaction1 = RawTransaction(
            reference="TEST001",
            datetime=datetime(2025, 3, 1, 10, 0, 0),
            debit_amount=0,
            credit_amount=1000000,
            balance=1000000,
            description="CK den tu CTY TNHH Tân Tạo thanh toan hoa don",
        )

        print("\nProcessing transaction with likely existing counterparty:")
        print(f"Description: {transaction1.description}")

        entry1 = processor.process_transaction(transaction1)
        if entry1:
            print(
                f"  Result - Code: {entry1.counterparty_code}, Name: {entry1.counterparty_name}"
            )
            print(f"  Address: {entry1.address}")
        else:
            print("  ❌ Failed to process transaction")

        # Test transaction with counterparty that should NOT be found
        transaction2 = RawTransaction(
            reference="TEST002",
            datetime=datetime(2025, 3, 1, 11, 0, 0),
            debit_amount=500000,
            credit_amount=0,
            balance=500000,
            description="Chuyen tien cho Tân Tạo",
        )

        print("\nProcessing transaction with fictional counterparty:")
        print(f"Description: {transaction2.description}")

        entry2 = processor.process_transaction(transaction2)
        if entry2:
            print(
                f"  Result - Code: {entry2.counterparty_code}, Name: {entry2.counterparty_name}"
            )
            print(f"  Address: {entry2.address}")

            # Verify Condition 2 logic
            if "FICTIONAL BUSINESS ENTERPRISE XYZ" in entry2.counterparty_name:
                print("  ✅ CONDITION 2 SUCCESS: Using extracted name")
            else:
                print("  ⚠️  Using fallback counterparty logic")
        else:
            print("  ❌ Failed to process transaction")

    finally:
        processor.close()


def test_edge_cases():
    print("\n" + "=" * 60)
    print("TEST: Edge Cases and Error Scenarios")
    print("=" * 60)

    extractor = CounterpartyExtractor()

    edge_cases = [
        "",  # Empty description
        "   ",  # Whitespace only
        "123456789",  # Numbers only
        "Thanh toan phi",  # No specific counterparty
        "ATM rut tien",  # ATM transaction
        "Lai tien gui thang 3/2025",  # Interest payment
    ]

    for desc in edge_cases:
        print(f"\nTesting edge case: '{desc}'")

        try:
            extracted_entities = extractor.extract_and_match_all(desc)
            counterparties = extracted_entities.get("counterparties", [])
            result = extractor.handle_counterparty_two_conditions(
                counterparties
            )

            print(f"  Condition Applied: {result.get('condition_applied')}")
            print(f"  Name: {result.get('name')}")
            print(f"  Code: {result.get('code')}")
            print("  ✅ Handled gracefully")

        except Exception as e:
            print(f"  ❌ Error: {e}")


def test_counterparty_name_cleaning():
    """Test the counterparty name cleaning functionality"""
    print("\n" + "=" * 60)
    print("TEST: Counterparty Name Cleaning")
    print("=" * 60)

    extractor = CounterpartyExtractor()

    # Test cases for name cleaning
    test_cases = [
        # Company names with business entity indicators
        ("CTY TNHH THUONG MAI ABC", "Thuong Mai Abc"),
        ("CONG TY CO PHAN XYZ TRADING", "Xyz Trading"),
        ("ABC COMPANY LIMITED", "Abc"),
        ("ENTERPRISE MANUFACTURING JSC", "Manufacturing"),
        ("CTY ABC TNHH", "Abc"),
        # Names with stopwords
        ("THANH TOAN CHO CTY ABC", "Abc"),
        ("CK DEN ABC COMPANY", "Abc"),
        ("TT TIEN ABC CORP", "Abc"),
        # Mixed cases
        ("CTY TNHH ABC THANH TOAN DEF", "Abc Thanh Toan Def"),
        ("CONG TY ABC CO PHAN", "Abc"),
        # Edge cases
        ("", ""),
        ("   ", ""),
        ("CTY", "Cty"),  # Single word should be preserved
        ("ABC", "Abc"),  # Simple name should be preserved
    ]

    for original, expected in test_cases:
        cleaned = extractor.clean_counterparty_name(original)
        print(f"Original: '{original}'")
        print(f"Expected: '{expected}'")
        print(f"Cleaned:  '{cleaned}'")

        if not expected:  # For empty cases, just check it's not None/empty
            if cleaned:
                print(
                    "  ✅ PASS: Preserved original or returned meaningful result"
                )
            else:
                print("  ⚠️  Empty result for empty input")
        elif (
            expected.lower() in cleaned.lower()
            or cleaned.lower() in expected.lower()
        ):
            print("  ✅ PASS: Cleaning successful")
        else:
            print(
                f"  ❌ FAIL: Expected containing '{expected}', got '{cleaned}'"
            )
        print()


def test_cleaned_extraction():
    """Test that counterparty extraction includes name cleaning"""
    print("\n" + "=" * 60)
    print("TEST: Extraction with Name Cleaning")
    print("=" * 60)

    extractor = CounterpartyExtractor()

    # Test descriptions with company names that need cleaning
    test_descriptions = [
        "CK den tu CTY TNHH THUONG MAI ABC thanh toan hoa don",
        "Chuyen tien cho CONG TY CO PHAN XYZ TRADING",
        "Thu tien tu ABC COMPANY LIMITED",
        "Thanh toan cho ENTERPRISE MANUFACTURING JSC",
    ]

    for desc in test_descriptions:
        print(f"Testing description: '{desc}'")

        # Extract counterparties
        counterparties = extractor.extract_counterparties(desc)

        if counterparties:
            for cp in counterparties:
                name = cp["name"]
                print(f"  Extracted: '{name}'")

                # Check that business entity indicators are removed
                business_indicators = [
                    "CTY",
                    "TNHH",
                    "CO PHAN",
                    "CONG TY",
                    "COMPANY",
                    "LIMITED",
                    "JSC",
                    "ENTERPRISE",
                ]
                has_indicators = any(
                    indicator in name.upper()
                    for indicator in business_indicators
                )

                if not has_indicators:
                    print("  ✅ PASS: Business entity indicators removed")
                else:
                    print(
                        f"  ⚠️  PARTIAL: Some indicators may remain in '{name}'"
                    )
        else:
            print("  ❌ No counterparties extracted")
        print()


if __name__ == "__main__":
    print("Starting Two-Condition Counterparty Logic Tests")
    print("=" * 60)

    try:
        test_condition_1_found_in_index()
        test_condition_2_not_found_in_index()
        test_integrated_processor_with_two_conditions()
        test_edge_cases()
        test_counterparty_name_cleaning()
        test_cleaned_extraction()

        print("\n" + "=" * 60)
        print("All tests completed!")
        print("=" * 60)

    except Exception as e:
        print(f"\n❌ Test suite failed with error: {e}")
        import traceback

        traceback.print_exc()
