#!/usr/bin/env python3
"""
Test script for counterparty extraction from bank transaction descriptions

This script tests the CounterpartyExtractor module independently from the
rest of the banking transaction processor.
"""

import sys
from pathlib import Path

# Add the project root to the path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from src.accounting.counterparty_extractor import CounterpartyExtractor
from src.util.logging import get_logger

logger = get_logger(__name__)


def test_basic_extraction():
    """Test basic pattern-based extraction without database matching"""
    print("\n===== Testing Basic Counterparty Extraction =====")

    extractor = CounterpartyExtractor()

    # Test descriptions with varying patterns
    test_descriptions = [
        # Example with B/O and F/O patterns
        "REF141A2530705CBCJM B/O 1410393840 CTY TNHH SANG TAM F/O 963880036508 CONG TY TNHH PHAT TRIEN PHU MY HUNG NHH79651001 CTY TNHH SANG TAM TT TIEN LAI CHAM THANH TOAN T01 2025 TAI CRM CHO CTY PHU MY HUNG",
        # Example with CTY pattern
        "CK DEN TU TK 0501000123456 CTY TNHH THUONG MAI XYZ THANH TOAN TIEN HANG THANG 10/2024",
        # Example with CONG TY pattern
        "CHUYEN KHOAN CHI TRA CO TUC CHO CO DONG CONG TY CO PHAN ABC HD SO 123456",
        # Example with company after POS code
        "TT POS 14100333 500379 5777 064217PDO CTY TNHH MTV DICH VU VAN TAI",
        # Example with NHH pattern
        "CK TU TK 012345678 NHH79651001 CTY TNHH XAY DUNG VA DICH VU TONG HOP THANH TOAN HOA DON MUA HANG",
        "Cty Sang Tam TT tien coc thi cong gian hang tai TTTM Vincom Mega Mall Times City Quay TN-28 theo TDNT so: TN-28OTLTTTM-VMMTC cho Cty Van Hanh Vincom Retail",
        "Chi nhanh Cty TNHH Sang Tam tai Ha Noi thanh toan tien hang cho Cty TNHH Sang Tam",
    ]

    for i, desc in enumerate(test_descriptions, 1):
        print(f"\nExample {i}:")
        print(f"Description: {desc}")

        # Extract counterparties
        counterparties = extractor.extract_counterparties(desc)

        if counterparties:
            print(f"Found {len(counterparties)} counterparties:")
            for j, party in enumerate(counterparties, 1):
                print(
                    f"  {j}. {party['name']} (Type: {party['type']}, Confidence: {party['confidence']:.2f})"
                )
        else:
            print("No counterparties found")


def test_database_matching():
    """Test database matching of extracted counterparties"""
    print("\n===== Testing Database Matching =====")

    try:
        extractor = CounterpartyExtractor()

        # Only proceed if fast_search module is available
        test_description = "Cty Sang Tam TT tien coc thi cong gian hang tai TTTM Vincom Mega Mall Times City Quay TN-28 theo TDNT so: TN-28OTLTTTM-VMMTC cho Cty Van Hanh Vincom Retail"

        print(f"Description: {test_description}")

        # First extract counterparties
        counterparties = extractor.extract_counterparties(test_description)

        if not counterparties:
            print("No counterparties extracted to match")
            return

        print(f"Extracted {len(counterparties)} counterparties:")
        for i, party in enumerate(counterparties, 1):
            print(
                f"  {i}. {party['name']} (Type: {party['type']}, Confidence: {party['confidence']:.2f})"
            )

        print("\nAttempting database matching:")
        for party in counterparties:
            print(f"\nLooking for matches for: {party['name']}")

            try:
                matches = extractor.match_counterparty_in_db(party["name"])

                if matches:
                    print(f"Found {len(matches)} matches:")
                    for j, match in enumerate(matches, 1):
                        code = match.get("code", "N/A")
                        name = match.get("name", "N/A")
                        score = match.get("score", 0)
                        print(
                            f"  {j}. {name} (Code: {code}, Score: {score:.4f})"
                        )
                else:
                    print("No matches found in database")
            except Exception as e:
                print(f"Error matching in database: {e}")

    except Exception as e:
        print(f"Database matching test failed: {e}")


def test_extract_and_match():
    """Test the combined extract and match functionality"""
    print("\n===== Testing Extract and Match Combined =====")

    try:
        extractor = CounterpartyExtractor()

        test_descriptions = [
            "REF141A2530705CBCJM B/O 1410393840 CTY TNHH SANG TAM F/O 963880036508 CONG TY TNHH PHAT TRIEN PHU MY HUNG",
            "CK DEN TU TK 0501000123456 CTY TNHH THUONG MAI XYZ THANH TOAN TIEN HANG THANG 10/2024",
            "CHUYEN KHOAN CHI TRA CO TUC CHO CO DONG CONG TY CO PHAN ABC HD SO 123456",
            "Cty Sang Tam TT tien coc thi cong gian hang tai TTTM Vincom Mega Mall Times City Quay TN-28 theo TDNT so: TN-28OTLTTTM-VMMTC cho Cty Van Hanh Vincom Retail",
            "Chi nhanh Cty TNHH Sang Tam tai Ha Noi thanh toan tien hang cho Cty TNHH Sang Tam",
        ]

        for i, desc in enumerate(test_descriptions, 1):
            print(f"\nExample {i}:")
            print(f"Description: {desc}")

            try:
                # Use the combined extract and match function
                results = extractor.extract_and_match(desc)

                if results:
                    print(f"Found and matched {len(results)} counterparties:")
                    for j, result in enumerate(results, 1):
                        extracted = result.get("extracted_name", "N/A")
                        matched = result.get("name", "N/A")
                        code = result.get("code", "N/A")
                        score = result.get("score", 0)
                        confidence = result.get("extraction_confidence", 0)

                        print(f"  {j}. Extracted: {extracted}")
                        print(f"     Matched: {matched} (Code: {code})")
                        print(
                            f"     Score: {score:.4f}, Confidence: {confidence:.2f}"
                        )
                else:
                    print("No counterparties found or matched")
            except Exception as e:
                print(f"Error in extract_and_match: {e}")

    except Exception as e:
        print(f"Extract and match test failed: {e}")


def test_with_real_examples():
    """Test with real-world examples provided by the user"""
    print("\n===== Testing with Real Examples =====")

    # Use the specific examples from the user's question
    examples = [
        "REF141A2530705CBCJM B/O 1410393840 CTY TNHH SANG TAM F/O 963880036508 CONG TY TNHH PHAT TRIEN PHU MY HUNG NHH79651001 CTY TNHH SANG TAM TT TIEN LAI CHAM THANH TOAN T01 2025 TAI CRM CHO CTY PHU MY HUNG",
        "REF141A2530705CBCJM B/O 1410393840 CTY TNHH SANG TAM F/O 963880036508 CONG TY TNHH PHAT TRIEN PHU MY HUNG",
        "HBK-TKThe :9906700964, tai Vietcombank. ND Cty Sang Tam hoan tien don hang LUGTMDT2HSP260200126.02.25 cho Pham Nguyen Phuong Thao -CTLNHIDO000011263466536-1/2-PMT-002",
        "CK den tu tai khoan 1234567890 chuyen tien cho Nguyen Van Anh",
        "TK 9876543210 gui cho Tran Thi Minh Hang -Hoan tien hang thieu-02072025",
        "Phi giao dich chi tien cho ba Le Thi Hoa",
    ]

    extractor = CounterpartyExtractor()

    for i, example in enumerate(examples, 1):
        print(f"\nExample {i}: {example}")

        # Extract counterparties
        counterparties = extractor.extract_counterparties(example)

        if counterparties:
            print(f"\nExtracted {len(counterparties)} counterparties:")
            for j, party in enumerate(counterparties, 1):
                print(
                    f"  {j}. {party['name']} (Type: {party['type']}, Confidence: {party['confidence']:.2f})"
                )

            # Try to match with database
            print("\nMatching with database:")
            try:
                # Use the combined extract and match function
                results = extractor.extract_and_match(example)

                if results:
                    print(f"Found and matched {len(results)} counterparties:")
                    for j, result in enumerate(results, 1):
                        print(
                            f"  {j}. Extracted: {result.get('extracted_name', 'N/A')}"
                        )
                        print(
                            f"     Matched: {result.get('name', 'N/A')} (Code: {result.get('code', 'N/A')})"
                        )
                else:
                    print("No matches found in database")
            except Exception as e:
                print(f"Error matching with database: {e}")
        else:
            print("No counterparties extracted")


def benchmark_performance(num_iterations: int = 100):
    """Benchmark the performance of the counterparty extraction"""
    print(
        f"\n===== Benchmarking Performance ({num_iterations} iterations) ====="
    )

    import time

    extractor = CounterpartyExtractor()

    # Test descriptions
    test_descriptions = [
        "REF141A2530705CBCJM B/O 1410393840 CTY TNHH SANG TAM F/O 963880036508 CONG TY TNHH PHAT TRIEN PHU MY HUNG",
        "CK DEN TU TK 0501000123456 CTY TNHH THUONG MAI XYZ THANH TOAN TIEN HANG THANG 10/2024",
        "CHUYEN KHOAN CHI TRA CO TUC CHO CO DONG CONG TY CO PHAN ABC HD SO 123456",
        "TT POS 14100333 500379 5777 064217PDO CTY TNHH MTV DICH VU VAN TAI",
    ]

    # Benchmark extraction only
    start_time = time.time()
    extraction_count = 0

    for _ in range(num_iterations):
        for desc in test_descriptions:
            counterparties = extractor.extract_counterparties(desc)
            extraction_count += len(counterparties)

    extraction_time = time.time() - start_time

    print("Extraction only:")
    print(f"  Processed {len(test_descriptions) * num_iterations} descriptions")
    print(f"  Found {extraction_count} counterparties")
    print(f"  Total time: {extraction_time:.4f} seconds")
    print(
        f"  Average time per description: {extraction_time / (len(test_descriptions) * num_iterations) * 1000:.2f} ms"
    )

    # Skip database matching benchmark as it depends on database access


def test_person_name_extraction():
    """Test the extraction of person names from descriptions"""
    print("\n===== Testing Person Name Extraction =====")

    # Examples with person names
    examples = [
        "HBK-TKThe :9906700964, tai Vietcombank. ND Cty Sang Tam hoan tien don hang LUGTMDT2HSP260200126.02.25 cho Pham Nguyen Phuong Thao -CTLNHIDO000011263466536-1/2-PMT-002",
        "CK den tu tai khoan 1234567890 chuyen tien cho Nguyen Van Anh",
        "TK 9876543210 gui cho Tran Thi Minh Hang -Hoan tien hang thieu-02072025",
        "Phi giao dich chi tien cho ba Le Thi Hoa",
        "Cty Sang Tam hoan tien Pham Quang Tuan -Khong du hang-04052025",
        "Thanh toan cho Hoang Van Hai",
    ]

    extractor = CounterpartyExtractor()

    for i, example in enumerate(examples, 1):
        print(f"\nExample {i}: {example}")

        # Extract counterparties
        counterparties = extractor.extract_counterparties(example)

        if counterparties:
            print(f"Found {len(counterparties)} counterparties:")
            for j, party in enumerate(counterparties, 1):
                print(
                    f"  {j}. {party['name']} (Type: {party['type']}, Confidence: {party['confidence']:.2f})"
                )
        else:
            print("No counterparties found")


def main():
    """Main function to run all tests"""
    print("\n========================================")
    print("COUNTERPARTY EXTRACTOR TEST SUITE")
    print("========================================")

    # Run tests
    test_basic_extraction()
    test_database_matching()
    test_extract_and_match()
    test_with_real_examples()
    test_person_name_extraction()  # Test our new person name extraction
    benchmark_performance(10)  # Use a smaller number for quicker testing

    print("\n========================================")
    print("TEST SUITE COMPLETE")
    print("========================================")


if __name__ == "__main__":
    # Run the main test suite
    main()
