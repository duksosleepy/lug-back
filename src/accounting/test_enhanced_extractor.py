#!/usr/bin/env python3
"""
Test script for enhanced CounterpartyExtractor with unified entity extraction

This script tests the enhanced CounterpartyExtractor module's ability to
extract multiple entity types in a single pass (counterparties, accounts,
POS machines, departments) and perform searches in the appropriate indexes.
"""

import sys
from pathlib import Path

# Add the project root to the path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from src.accounting.counterparty_extractor import CounterpartyExtractor
from src.util.logging import get_logger

logger = get_logger(__name__)


def test_entity_extraction():
    """Test the enhanced entity extraction functionality"""
    print("\n===== Testing Enhanced Entity Extraction =====")

    extractor = CounterpartyExtractor()

    # Test descriptions with a mix of entity types
    test_descriptions = [
        # Description with counterparty, account numbers, and POS
        "REF141A2530705CBCJM B/O 1410393840 CTY TNHH SANG TAM F/O 963880036508 CONG TY TNHH PHAT TRIEN PHU MY HUNG NHH79651001 CTY TNHH SANG TAM TT POS 14100414 TIEN LAI CHAM THANH TOAN T01 2025 TAI CRM CHO CTY PHU MY HUNG",
        # Description with account transfers
        "Chuyen tien tu TK BIDV 3840 Sang Tam qua TK BIDV 7655 Sang Tam",
        # Description with person name and department code
        "CK den tu TK 0501000123456 CTY TNHH THUONG MAI XYZ BP CN01 THANH TOAN CHO Nguyen Van Anh",
        # Description with POS transaction
        "TT POS 14100333 500379 5777 064217PDO CTY TNHH MTV DICH VU VAN TAI",
    ]

    for i, desc in enumerate(test_descriptions, 1):
        print(f"\nExample {i}:")
        print(f"Description: {desc}")

        # Extract all entity types
        entity_info = extractor.extract_entity_info(desc)

        # Report what was found
        print("\nExtracted entities:")

        if entity_info.get("counterparties"):
            print(f"\nCounterparties ({len(entity_info['counterparties'])}):")
            for j, cp in enumerate(entity_info["counterparties"], 1):
                print(
                    f"  {j}. {cp['name']} (Type: {cp['type']}, Confidence: {cp['confidence']:.2f})"
                )

        if entity_info.get("accounts"):
            print(f"\nAccounts ({len(entity_info['accounts'])}):")
            for j, acc in enumerate(entity_info["accounts"], 1):
                print(
                    f"  {j}. {acc['code']} (Type: {acc['type']}, Confidence: {acc['confidence']:.2f})"
                )

        if entity_info.get("pos_machines"):
            print(f"\nPOS Machines ({len(entity_info['pos_machines'])}):")
            for j, pos in enumerate(entity_info["pos_machines"], 1):
                print(
                    f"  {j}. {pos['code']} (Type: {pos['type']}, Confidence: {pos['confidence']:.2f})"
                )

        if entity_info.get("departments"):
            print(f"\nDepartments ({len(entity_info['departments'])}):")
            for j, dept in enumerate(entity_info["departments"], 1):
                print(
                    f"  {j}. {dept['code']} (Type: {dept['type']}, Confidence: {dept['confidence']:.2f})"
                )


def test_unified_search():
    """Test the unified search functionality"""
    print("\n===== Testing Unified Entity Search =====")

    extractor = CounterpartyExtractor()

    # Use a description with multiple entity types
    test_description = "REF141A2530705CBCJM B/O 1410393840 CTY TNHH SANG TAM F/O 963880036508 CONG TY TNHH PHAT TRIEN PHU MY HUNG NHH79651001 CTY TNHH SANG TAM TT POS 14100414 TIEN LAI CHAM THANH TOAN T01 2025 TAI CRM CHO CTY PHU MY HUNG"

    print(f"Description: {test_description}")

    try:
        # First extract all entity types
        entity_info = extractor.extract_entity_info(test_description)

        print("\nExtracted entities:")
        for entity_type, entities in entity_info.items():
            if entities:
                print(f"\n{entity_type.capitalize()} ({len(entities)}):")
                for i, entity in enumerate(entities, 1):
                    if entity_type == "counterparties":
                        print(
                            f"  {i}. {entity['name']} (Type: {entity['type']}, Confidence: {entity['confidence']:.2f})"
                        )
                    else:
                        print(
                            f"  {i}. {entity['code']} (Type: {entity['type']}, Confidence: {entity['confidence']:.2f})"
                        )

        # Search for matches in appropriate indexes
        print("\nSearching for matches in database indexes:")
        matched_entities = extractor.search_entities(entity_info)

        for entity_type, matches in matched_entities.items():
            if matches:
                print(f"\n{entity_type.capitalize()} matches ({len(matches)}):")
                for i, match in enumerate(matches, 1):
                    if entity_type == "counterparties":
                        print(
                            f"  {i}. {match.get('name', 'N/A')} (Code: {match.get('code', 'N/A')}, Score: {match.get('score', 0):.4f})"
                        )
                        print(
                            f"     Extracted: {match.get('extracted_name', 'N/A')}"
                        )
                    else:
                        code_field = (
                            "code" if "code" in match else "extracted_code"
                        )
                        print(
                            f"  {i}. {match.get('name', 'N/A')} (Code: {match.get(code_field, 'N/A')}, Score: {match.get('score', 0):.4f})"
                        )
            else:
                print(f"\nNo {entity_type} matches found in database")

    except Exception as e:
        print(f"Error in unified search: {e}")
        import traceback

        traceback.print_exc()


def test_extract_and_match_all():
    """Test the main entry point for entity extraction and matching"""
    print("\n===== Testing extract_and_match_all =====")

    extractor = CounterpartyExtractor()

    # Test descriptions
    test_descriptions = [
        "Chuyen tien tu TK BIDV 3840 Sang Tam qua TK BIDV 7655 Sang Tam",
        "TT POS 14100333 500379 5777 064217PDO CTY TNHH MTV DICH VU VAN TAI",
    ]

    for i, desc in enumerate(test_descriptions, 1):
        print(f"\nExample {i}:")
        print(f"Description: {desc}")

        try:
            # Use the combined extract and match all function
            matched_entities = extractor.extract_and_match_all(desc)

            # Show results
            for entity_type, matches in matched_entities.items():
                if matches:
                    print(
                        f"\n{entity_type.capitalize()} matches ({len(matches)}):"
                    )
                    for j, match in enumerate(matches, 1):
                        if entity_type == "counterparties":
                            print(
                                f"  {j}. {match.get('name', 'N/A')} (Code: {match.get('code', 'N/A')}, Score: {match.get('score', 0):.4f})"
                            )
                            print(
                                f"     Extracted: {match.get('extracted_name', 'N/A')}"
                            )
                        else:
                            code_field = (
                                "code" if "code" in match else "extracted_code"
                            )
                            print(
                                f"  {j}. {match.get('name', 'N/A')} (Code: {match.get(code_field, 'N/A')}, Score: {match.get('score', 0):.4f})"
                            )

        except Exception as e:
            print(f"Error in extract_and_match_all: {e}")
            import traceback

            traceback.print_exc()


def performance_comparison():
    """Compare performance between old and new approach"""
    print("\n===== Performance Comparison =====")

    import time

    extractor = CounterpartyExtractor()

    # Test with a complex description containing multiple entity types
    test_description = "REF141A2530705CBCJM B/O 1410393840 CTY TNHH SANG TAM F/O 963880036508 CONG TY TNHH PHAT TRIEN PHU MY HUNG NHH79651001 CTY TNHH SANG TAM TT POS 14100414 TIEN LAI CHAM THANH TOAN T01 2025 TAI CRM CHO CTY PHU MY HUNG"

    iterations = 10

    # Test old approach - separate extraction and searches
    start_time = time.time()
    for _ in range(iterations):
        # 1. Extract counterparties
        counterparties = extractor.extract_counterparties(test_description)
        # 2. Match counterparties
        for cp in counterparties:
            matches = extractor.match_counterparty_in_db(cp["name"])
        # 3. Extract account codes
        accounts = extractor._extract_accounts(test_description.upper())
        # 4. Extract POS machines
        pos_machines = extractor._extract_pos_machines(test_description.upper())
    old_time = time.time() - start_time

    # Test new approach - unified extraction and search
    start_time = time.time()
    for _ in range(iterations):
        # Single method call does everything
        matched_entities = extractor.extract_and_match_all(test_description)
    new_time = time.time() - start_time

    print(f"Old approach: {old_time:.4f} seconds for {iterations} iterations")
    print(f"New approach: {new_time:.4f} seconds for {iterations} iterations")

    if new_time < old_time:
        improvement = ((old_time - new_time) / old_time) * 100
        print(f"Performance improvement: {improvement:.1f}%")
    else:
        print("No performance improvement observed")


def main():
    """Main function to run all tests"""
    print("\n========================================")
    print("ENHANCED COUNTERPARTY EXTRACTOR TEST SUITE")
    print("========================================")

    # Run tests
    test_entity_extraction()
    test_unified_search()
    test_extract_and_match_all()
    performance_comparison()

    print("\n========================================")
    print("TEST SUITE COMPLETE")
    print("========================================")


if __name__ == "__main__":
    # Run the main test suite
    main()
