#!/usr/bin/env python3
"""Test script to verify the fix for BIDV 4664 account mapping"""

import sys
from pathlib import Path

# Add the project root to the path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from src.accounting.integrated_bank_processor import IntegratedBankProcessor

def test_4664_account_extraction():
    """Test that BIDV 4664 files correctly map to account 1121203"""
    processor = IntegratedBankProcessor()
    
    # Test the specific case mentioned in the issue
    test_filename = "BIDV 4664 HN T09.xlsx"
    result = processor.extract_account_from_filename(test_filename)
    
    print(f"Testing filename: {test_filename}")
    print(f"Extracted account: {result}")
    print(f"Expected account: 1121203")
    print(f"Test {'PASSED' if result == '1121203' else 'FAILED'}")
    
    # Additional test cases
    test_cases = [
        ("BIDV 4664.xlsx", "1121203"),
        ("ACB 8368 statement.ods", "1121124"),  # Should still work for other accounts
        ("VCB 7803 transaction.xlsx", "1121117"),  # Should still work for other accounts
    ]
    
    print("\nAdditional test cases:")
    all_passed = True
    for filename, expected in test_cases:
        result = processor.extract_account_from_filename(filename)
        status = "PASS" if result == expected else "FAIL"
        if result != expected:
            all_passed = False
        print(f"{status}: '{filename}' -> {result} (expected: {expected})")
    
    if all_passed:
        print("\nAll tests PASSED!")
    else:
        print("\nSome tests FAILED!")
    
    return result == "1121203" and all_passed

if __name__ == "__main__":
    try:
        success = test_4664_account_extraction()
        sys.exit(0 if success else 1)
    except Exception as e:
        print(f"Error running test: {e}")
        sys.exit(1)