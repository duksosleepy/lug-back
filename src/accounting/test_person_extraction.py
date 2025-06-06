#!/usr/bin/env python3
"""
Test script specifically for person name extraction from bank transaction descriptions

This script focuses on testing the CounterpartyExtractor's ability to extract person names
from bank statement transaction descriptions.
"""

import sys
from pathlib import Path

# Add the project root to the path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from src.accounting.counterparty_extractor import CounterpartyExtractor
from src.util.logging import get_logger

logger = get_logger(__name__)


def test_person_name_extraction():
    """Test the extraction of person names from descriptions"""
    print("\n===== Testing Person Name Extraction =====")

    # Create the extractor
    extractor = CounterpartyExtractor()

    # Examples with person names - including the specific example
    examples = [
        "HBK-TKThe :9906700964, tai Vietcombank. ND Cty Sang Tam hoan tien don hang LUGTMDT2HSP260200126.02.25 cho Pham Nguyen Phuong Thao -CTLNHIDO000011263466536-1/2-PMT-002",
        "CK den tu tai khoan 1234567890 chuyen tien cho Nguyen Van Anh",
        "TK 9876543210 gui cho Tran Thi Minh Hang -Hoan tien hang thieu-02072025",
        "Phi giao dich chi tien cho ba Le Thi Hoa",
        "Cty Sang Tam hoan tien Pham Quang Tuan -Khong du hang-04052025",
        "Thanh toan cho Hoang Van Hai",
        "CK den tu TK 0501000123456 CTY TNHH THUONG MAI XYZ TT hang",
        "CK den tu TK 1234567890 hoan tien cho Nguyen Van Thai (1,500,000 VND)",
        "CK di TK 11122233344 Sang Tam tra luong cho Truong Thi Thuy Linh T5/2025",
        "Chuyen tien cho Vo Thi Mai -Tien ung luong-05-2025",
    ]

    print("Testing with individual examples:")
    for i, example in enumerate(examples, 1):
        print(f"\nExample {i}: {example}")

        # Extract counterparties
        counterparties = extractor.extract_counterparties(example)

        # Filter to only show person type counterparties
        person_counterparties = [
            p for p in counterparties if p.get("type") == "person"
        ]

        if person_counterparties:
            print(f"Found {len(person_counterparties)} person counterparties:")
            for j, party in enumerate(person_counterparties, 1):
                print(
                    f"  {j}. {party['name']} (Confidence: {party['confidence']:.2f})"
                )
        else:
            print("No person counterparties found")

    # Test the integrated extract_and_match functionality
    print("\n\n===== Testing Person Name Database Matching =====")
    print(
        "(This will only show results if the names match entries in the database)"
    )

    for i, example in enumerate(examples[:3], 1):  # Test first 3 examples only
        print(f"\nExample {i}: {example}")

        # Use the combined extract and match function
        try:
            results = extractor.extract_and_match(example)

            # Filter to only show person matches
            person_results = [
                r for r in results if r.get("match_type") == "person"
            ]

            if person_results:
                print(
                    f"Found and matched {len(person_results)} person counterparties:"
                )
                for j, result in enumerate(person_results, 1):
                    print(
                        f"  {j}. Extracted: {result.get('extracted_name', 'N/A')}"
                    )
                    print(
                        f"     Matched: {result.get('name', 'N/A')} (Code: {result.get('code', 'N/A')})"
                    )
                    print(
                        f"     Score: {result.get('score', 0):.4f}, Confidence: {result.get('extraction_confidence', 0):.2f}"
                    )
            else:
                print("No person counterparties matched in database")
        except Exception as e:
            print(f"Error in extract_and_match: {e}")


if __name__ == "__main__":
    # Run the person name extraction test
    test_person_name_extraction()
