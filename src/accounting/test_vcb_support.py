#!/usr/bin/env python3
"""
Test script for VCB bank statement processing
"""

import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from src.accounting.bank_configs import get_bank_config
from src.accounting.integrated_bank_processor import IntegratedBankProcessor


def test_bank_detection():
    """Test bank detection from filename"""
    processor = IntegratedBankProcessor()

    # Test VCB detection
    vcb_filenames = [
        "VCB_statement_example.xlsx",
        "vcb_statement_example.xlsx",
        "VCB 501000094615.xlsx",
    ]

    for filename in vcb_filenames:
        bank_code = processor.extract_bank_from_filename(filename)
        print(f"Filename: {filename} -> Bank: {bank_code}")

        if bank_code:
            bank_info = processor.get_bank_info_by_name(bank_code)
            if bank_info:
                print(f"  Found bank: {bank_info.get('shortName', 'Unknown')}")
            else:
                print(f"  Bank info not found for: {bank_code}")

    # Test BIDV detection (existing)
    bidv_filenames = ["BIDV 3840.xlsx", "bidv_statement.xlsx"]

    for filename in bidv_filenames:
        bank_code = processor.extract_bank_from_filename(filename)
        print(f"Filename: {filename} -> Bank: {bank_code}")


def test_bank_configs():
    """Test bank configuration loading"""

    # Test VCB config
    vcb_config = get_bank_config("VCB")
    print(f"VCB Config: {vcb_config.name}")
    print(
        f"VCB Header patterns: {list(vcb_config.statement_config.header_patterns.keys())}"
    )
    print(
        f"VCB Reference patterns: {vcb_config.statement_config.header_patterns['reference']}"
    )
    print(
        f"VCB Date patterns: {vcb_config.statement_config.header_patterns['date']}"
    )

    # Test BIDV config
    bidv_config = get_bank_config("BIDV")
    print(f"\\nBIDV Config: {bidv_config.name}")
    print(
        f"BIDV Header patterns: {list(bidv_config.statement_config.header_patterns.keys())}"
    )


def test_vcb_header_patterns():
    """Test VCB-specific header patterns"""
    vcb_config = get_bank_config("VCB")

    # Test header patterns that should match VCB file
    vcb_headers = [
        "Ngày giao dịch",
        "Số tham chiếu",
        "Số tiền ghi nợ",
        "Số tiền ghi có",
        "Mô tả",
    ]

    print("\\nTesting VCB header matching:")
    for header in vcb_headers:
        header_normalized = (
            header.lower()
            .replace("ò", "o")
            .replace("ố", "o")
            .replace("ệ", "e")
            .replace("ã", "a")
            .replace("ự", "u")
            .replace("ộ", "o")
            .replace("ả", "a")
        )

        # Check which field this header matches
        for (
            field_name,
            patterns,
        ) in vcb_config.statement_config.header_patterns.items():
            for pattern in patterns:
                pattern_normalized = pattern.lower()
                if pattern_normalized in header_normalized:
                    print(
                        f"  '{header}' matches field '{field_name}' with pattern '{pattern}'"
                    )
                    break


if __name__ == "__main__":
    print("=== Testing VCB Bank Statement Processing ===\\n")

    print("1. Testing bank detection from filename:")
    test_bank_detection()

    print("\\n2. Testing bank configurations:")
    test_bank_configs()

    print("\\n3. Testing VCB header patterns:")
    test_vcb_header_patterns()

    print("\\n=== Test completed ===")
