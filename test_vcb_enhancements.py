#!/usr/bin/env python3
"""
Test script to verify VCB enhancement implementation

This script tests the new business logic for VCB file processing:
1. INTEREST PAYMENT transactions
2. THU PHI QLTK TO CHUC-VND transactions
3. IBVCB transfer transactions
"""

import sys
from pathlib import Path
from datetime import datetime

# Add project root to path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from src.accounting.vcb_processor import (
    process_vcb_interest_transaction,
    process_vcb_fee_transaction,
    process_vcb_transfer_transaction
)


def test_vcb_enhancements():
    """Test VCB enhancement implementations"""
    
    print("🧪 Testing VCB Enhancement Implementation")
    print("=" * 50)
    
    # Test Case 1: Interest Payment Transaction
    print("\n💰 TEST CASE 1: VCB Interest Payment Transaction")
    interest_transaction = {
        "reference": "INT-001",
        "datetime": datetime(2025, 6, 15, 10, 30, 0),
        "debit_amount": 0,
        "credit_amount": 500000,
        "balance": 10500000,
        "description": "INTEREST PAYMENT",
        "bank_account": "1121120"  # Current file bank account
    }
    
    print(f"\n  🔍 Test: {interest_transaction['description']}")
    result = process_vcb_interest_transaction(interest_transaction["description"], interest_transaction)
    
    if result and len(result) == 1:
        record = result[0]
        print(f"    ✅ Interest Payment Processing Successful")
        print(f"    📝 Description: {record['description']}")
        print(f"    🏢 Counterparty: {record['counterparty_code']} - {record['counterparty_name']}")
        print(f"    📍 Address: {record['address']}")
        print(f"    💳 Debit Account: {record.get('debit_account', 'N/A')}")
        print(f"    💳 Credit Account: {record.get('credit_account', 'N/A')}")
        
        # Verify business requirements
        expected_debit = "1121120"   # Bank account
        expected_credit = "5154"     # Interest income account
        
        if record.get('debit_account') == expected_debit and record.get('credit_account') == expected_credit:
            print(f"    ✅ Business Logic CORRECT: Debit={expected_debit}, Credit={expected_credit}")
        else:
            print(f"    ❌ Business Logic INCORRECT: Expected Debit={expected_debit}, Credit={expected_credit}")
            print(f"        Actual Debit={record.get('debit_account')}, Credit={record.get('credit_account')}")
    else:
        print(f"    ❌ Failed to process interest payment")
    
    # Test Case 2: Account Management Fee Transaction
    print("\n\n💳 TEST CASE 2: VCB Account Management Fee Transaction")
    fee_transaction = {
        "reference": "FEE-001",
        "datetime": datetime(2025, 6, 15, 11, 0, 0),
        "debit_amount": 150000,
        "credit_amount": 0,
        "balance": 10350000,
        "description": "THU PHI QLTK TO CHUC-VND",
        "bank_account": "1121120"  # Current file bank account
    }
    
    print(f"\n  🔍 Test: {fee_transaction['description']}")
    result = process_vcb_fee_transaction(fee_transaction["description"], fee_transaction)
    
    if result and len(result) == 1:
        record = result[0]
        print(f"    ✅ Account Fee Processing Successful")
        print(f"    📝 Description: {record['description']}")
        print(f"    🏢 Counterparty: {record['counterparty_code']} - {record['counterparty_name']}")
        print(f"    📍 Address: {record['address']}")
        print(f"    💳 Debit Account: {record.get('debit_account', 'N/A')}")
        print(f"    💳 Credit Account: {record.get('credit_account', 'N/A')}")
        
        # Verify business requirements
        expected_debit = "6427"      # Fee expense account
        expected_credit = "1121120"  # Bank account
        
        if record.get('debit_account') == expected_debit and record.get('credit_account') == expected_credit:
            print(f"    ✅ Business Logic CORRECT: Debit={expected_debit}, Credit={expected_credit}")
        else:
            print(f"    ❌ Business Logic INCORRECT: Expected Debit={expected_debit}, Credit={expected_credit}")
            print(f"        Actual Debit={record.get('debit_account')}, Credit={record.get('credit_account')}")
    else:
        print(f"    ❌ Failed to process account fee")
    
    # Test Case 3: Transfer Transaction
    print("\n\n🔁 TEST CASE 3: VCB Transfer Transaction")
    transfer_transaction = {
        "reference": "TRF-001",
        "datetime": datetime(2025, 6, 15, 11, 30, 0),
        "debit_amount": 10000000,
        "credit_amount": 0,
        "balance": 350000,
        "description": "IBVCB.1706250930138002.034244.IBTC.Chuyen tien tu TK VCB (6868) Sang Tam qua TK BIDV (7655) Sang Tam",
        "bank_account": "1121120"  # Current file bank account
    }
    
    print(f"\n  🔍 Test: {transfer_transaction['description'][:50]}...")
    result = process_vcb_transfer_transaction(transfer_transaction["description"], transfer_transaction)
    
    if result and len(result) == 2:
        main_record = result[0]
        fee_record = result[1]
        
        print(f"    ✅ Transfer Transaction Processing Successful")
        print(f"    📝 Main Record Description: {main_record['description']}")
        print(f"    💳 Main Record Debit Account: {main_record.get('debit_account', 'N/A')}")
        print(f"    💳 Main Record Credit Account: {main_record.get('credit_account', 'N/A')}")
        
        # Verify main record business requirements
        expected_main_debit = "1131"     # Transfer account
        expected_main_credit = "1121120" # Bank account
        
        if main_record.get('debit_account') == expected_main_debit and main_record.get('credit_account') == expected_main_credit:
            print(f"    ✅ Main Record Business Logic CORRECT: Debit={expected_main_debit}, Credit={expected_main_credit}")
        else:
            print(f"    ❌ Main Record Business Logic INCORRECT: Expected Debit={expected_main_debit}, Credit={expected_main_credit}")
            print(f"        Actual Debit={main_record.get('debit_account')}, Credit={main_record.get('credit_account')}")
        
        print(f"    📝 Fee Record Description: {fee_record['description']}")
        print(f"    🏢 Fee Record Counterparty: {fee_record['counterparty_code']} - {fee_record['counterparty_name']}")
        print(f"    📍 Fee Record Address: {fee_record['address']}")
        print(f"    💳 Fee Record Debit Account: {fee_record.get('debit_account', 'N/A')}")
        print(f"    💳 Fee Record Credit Account: {fee_record.get('credit_account', 'N/A')}")
        
        # Verify fee record business requirements
        expected_fee_debit = "6427"      # Fee expense account
        expected_fee_credit = "1121120"  # Bank account
        expected_counterparty_code = "31754"  # Sáng Tâm company
        
        if (fee_record.get('debit_account') == expected_fee_debit and 
            fee_record.get('credit_account') == expected_fee_credit and
            fee_record.get('counterparty_code') == expected_counterparty_code):
            print(f"    ✅ Fee Record Business Logic CORRECT: Debit={expected_fee_debit}, Credit={expected_fee_credit}, Counterparty={expected_counterparty_code}")
        else:
            print(f"    ❌ Fee Record Business Logic INCORRECT:")
            print(f"        Expected Debit={expected_fee_debit}, Credit={expected_fee_credit}, Counterparty={expected_counterparty_code}")
            print(f"        Actual Debit={fee_record.get('debit_account')}, Credit={fee_record.get('credit_account')}, Counterparty={fee_record.get('counterparty_code')}")
    else:
        print(f"    ❌ Failed to process transfer transaction")
    
    print("\n" + "=" * 50)
    print("✅ VCB Enhancement Testing Complete!")


if __name__ == "__main__":
    test_vcb_enhancements()