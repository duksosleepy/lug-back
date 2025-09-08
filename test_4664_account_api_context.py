#!/usr/bin/env python3
"""Test script to verify the fix works in the API context for BIDV 4664 account mapping"""

import sys
from pathlib import Path
import io
import pandas as pd
from datetime import datetime

# Add the project root to the path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from src.accounting.integrated_bank_processor import IntegratedBankProcessor

def test_4664_account_in_api_context():
    """Test that BIDV 4664 files correctly set the default bank account in API context"""
    processor = IntegratedBankProcessor()
    
    # Simulate what happens in the API when processing "BIDV 4664 HN T09.xlsx"
    test_filename = "BIDV 4664 HN T09.xlsx"
    
    print(f"Testing filename: {test_filename}")
    
    # This is what happens in the API code:
    # 1. Try to extract account from header (will fail for this test)
    # 2. Try to infer account from filename
    account_from_filename = processor.extract_account_from_filename(test_filename)
    
    if account_from_filename:
        processor.default_bank_account = account_from_filename
        print(f"Set default bank account to {processor.default_bank_account} based on filename")
    else:
        print("No account found in filename")
    
    print(f"Final default_bank_account: {processor.default_bank_account}")
    print(f"Expected: 1121203")
    print(f"Test {'PASSED' if processor.default_bank_account == '1121203' else 'FAILED'}")
    
    # Test with a sample DataFrame to simulate process_to_saoke
    print("\n--- Testing with sample transaction data ---")
    
    # Create a sample transactions DataFrame like what would be parsed from an Excel file
    sample_data = {
        "reference": ["TEST001"],
        "date": [datetime(2025, 9, 8)],
        "debit": [0],
        "credit": [1000000],
        "balance": [1000000],
        "description": ["Test transaction for BIDV 4664"]
    }
    
    transactions_df = pd.DataFrame(sample_data)
    print(f"Sample transactions:\n{transactions_df}")
    
    # Process using process_to_saoke (this is what the API does)
    try:
        # Set the bank name and info to ensure proper processing
        processor.current_bank_name = "BIDV"
        processor.current_bank_info = {
            "code": "BIDV",
            "name": "NGÂN HÀNG THƯƠNG MẠI CỔ PHẦN ĐẦU TƯ VÀ PHÁT TRIỂN VIỆT NAM",
            "short_name": "BIDV",
            "address": "Tháp BIDV, số 35 Hàng Vôi, Quận Hoàn Kiếm, Hà Nội"
        }
        
        # Process the transactions
        processed_df = processor.process_to_saoke(transactions_df)
        
        print(f"\nProcessed DataFrame columns: {list(processed_df.columns)}")
        if len(processed_df) > 0:
            print(f"First processed entry:")
            print(f"  Debit account: {processed_df.iloc[0]['debit_account']}")
            print(f"  Credit account: {processed_df.iloc[0]['credit_account']}")
            print(f"  Bank account (from filename): {account_from_filename}")
            
            # For a credit transaction (money coming in), the bank account should be the debit account
            expected_debit = account_from_filename  # The bank account should be debited
            actual_debit = processed_df.iloc[0]['debit_account']
            
            print(f"Expected debit account: {expected_debit}")
            print(f"Actual debit account: {actual_debit}")
            
            if actual_debit == expected_debit:
                print("Debit account test PASSED")
                return True
            else:
                print("Debit account test FAILED")
                return False
        else:
            print("No transactions processed")
            return False
            
    except Exception as e:
        print(f"Error processing transactions: {e}")
        return False

if __name__ == "__main__":
    try:
        success = test_4664_account_in_api_context()
        print(f"\nOverall test {'PASSED' if success else 'FAILED'}")
        sys.exit(0 if success else 1)
    except Exception as e:
        print(f"Error running test: {e}")
        sys.exit(1)