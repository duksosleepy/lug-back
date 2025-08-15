#!/usr/bin/env python3
"""
Test script for DUY TRI SMS transaction handling in IntegratedBankProcessor

This script tests the ability of the IntegratedBankProcessor to correctly handle
transactions with "DUY TRI SMS" in the description.
"""

import sys
import os
from datetime import datetime
from pathlib import Path

# Add the project root to the path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from src.accounting.integrated_bank_processor import IntegratedBankProcessor, RawTransaction

def test_duy_tri_sms_handling():
    """Test DUY TRI SMS transaction processing"""
    processor = IntegratedBankProcessor()
    
    # Initialize the processor
    if not processor.connect():
        print("Error: Failed to initialize processor")
        return False
    
    print("Testing DUY TRI SMS transaction handling...")
    
    # Set up a test environment
    processor.current_bank_name = "BIDV"
    processor.current_bank_info = {
        "code": 50619,
        "name": "NGÂN HÀNG THƯƠNG MẠI CỔ PHẦN ĐẦU TƯ VÀ PHÁT TRIỂN VIỆT NAM",
        "short_name": "BIDV",
        "address": "Tháp BIDV, số 35 Hàng Vôi, Quận Hoàn Kiếm, Hà Nội"
    }
    processor.default_bank_account = "1121114"
    
    # Create a test transaction with DUY TRI SMS in the description
    test_transaction = RawTransaction(
        reference="TEST-SMS-001",
        datetime=datetime.now(),
        debit_amount=10000,  # Example amount (10,000 VND)
        credit_amount=0,
        balance=1000000,
        description="PHI DUY TRI SMS BIDV THANG 06/2025",
    )
    
    # Process the transaction
    result = processor.process_transaction(test_transaction)
    
    if not result:
        print("Error: Failed to process DUY TRI SMS transaction")
        return False
    
    # Check the result fields
    print("\nTransaction processing result:")
    print(f"Description: {result.description}")
    print(f"Counterparty code: {result.counterparty_code}")
    print(f"Counterparty name: {result.counterparty_name}")
    print(f"Address: {result.address}")
    print(f"Debit account: {result.debit_account}")
    print(f"Credit account: {result.credit_account}")
    
    # Validate results
    success = True
    expected_results = {
        "description": "Phí Duy trì BSMS",
        "counterparty_code": "50619",
        "debit_account": "6427",
        "credit_account": "1121114"
    }
    
    for field, expected_value in expected_results.items():
        actual_value = getattr(result, field)
        if actual_value != expected_value:
            print(f"Error: {field} mismatch. Expected '{expected_value}', got '{actual_value}'")
            success = False
    
    if success:
        print("\nSUCCESS: DUY TRI SMS transaction processed correctly!")
        return True
    else:
        print("\nFAILED: DUY TRI SMS transaction processing has issues.")
        return False

if __name__ == "__main__":
    test_duy_tri_sms_handling()
