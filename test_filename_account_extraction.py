#!/usr/bin/env python3
"""Test script to verify filename-based account extraction functionality"""

import sys
from pathlib import Path

# Add the project root to the path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from src.accounting.integrated_bank_processor import IntegratedBankProcessor

def test_filename_account_extraction():
    """Test the extract_account_from_filename method"""
    processor = IntegratedBankProcessor()
    
    # Test cases
    test_cases = [
        # (filename, expected_account_code_or_none)
        ("BIDV 3840.xlsx", "1121114"),  # Should find account 3840
        ("ACB 8368 statement.ods", "1121124"),  # Should find account 8368
        ("VCB 7803 transaction.xlsx", "1121117"),  # Should find account 7803
        ("MBB 4567 transfer.ods", None),  # Unknown account
        ("NoAccountStatement.xlsx", None),  # No account number in filename
        ("BIDV_statement.xlsx", None),  # Bank name but no account number
    ]
    
    print("Testing filename-based account extraction:")
    print("=" * 50)
    
    all_passed = True
    for filename, expected in test_cases:
        result = processor.extract_account_from_filename(filename)
        status = "PASS" if result == expected else "FAIL"
        if result != expected:
            all_passed = False
        print(f"{status}: '{filename}' -> {result} (expected: {expected})")
    
    print("=" * 50)
    if all_passed:
        print("All tests PASSED!")
    else:
        print("Some tests FAILED!")
    
    return all_passed

if __name__ == "__main__":
    test_filename_account_extraction()