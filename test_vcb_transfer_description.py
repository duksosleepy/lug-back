#!/usr/bin/env python3
"""
Test script to verify VCB transfer description formatting

This script tests that transfer transactions correctly format the description as
"Chuyển tiền từ TK VCB (6868) Sáng Tâm qua TK ACB (9139) Sáng Tâm".
"""

import sys
from pathlib import Path
from datetime import datetime

# Add project root to path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from src.accounting.vcb_processor import process_vcb_transfer_transaction


def test_vcb_transfer_description_formatting():
    """Test VCB transfer description formatting"""
    
    print("🧪 Testing VCB Transfer Description Formatting")
    print("=" * 50)
    
    # Test Case 1: VCB to ACB Transfer
    print("\n🔁 TEST CASE 1: VCB to ACB Transfer")
    transfer_transaction = {
        "reference": "TRF-001",
        "datetime": datetime(2025, 6, 18, 11, 30, 0),
        "debit_amount": 10000000,
        "credit_amount": 0,
        "balance": 350000,
        "description": "IBVCB.1806250063454006.043183.IBTC.Chuyen tien tu TK VCB (6868) Sang Tam qua TK ACB (9139) Sang Tam",
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
        
        # Verify description formatting
        expected_description = "Chuyển tiền từ TK VCB (6868) Sáng Tâm qua TK ACB (9139) Sáng Tâm"
        if main_record['description'] == expected_description:
            print(f"    ✅ Description Formatting CORRECT: {expected_description}")
        else:
            print(f"    ❌ Description Formatting INCORRECT:")
            print(f"        Expected: {expected_description}")
            print(f"        Actual: {main_record['description']}")
            
        # Verify account assignments
        expected_main_debit = "1131"
        expected_main_credit = "1121120"
        
        if (main_record.get('debit_account') == expected_main_debit and 
            main_record.get('credit_account') == expected_main_credit):
            print(f"    ✅ Main Record Accounts CORRECT: Debit={expected_main_debit}, Credit={expected_main_credit}")
        else:
            print(f"    ❌ Main Record Accounts INCORRECT:")
            print(f"        Expected Debit={expected_main_debit}, Credit={expected_main_credit}")
            print(f"        Actual Debit={main_record.get('debit_account')}, Credit={main_record.get('credit_account')}")
        
        print(f"    📝 Fee Record Description: {fee_record['description']}")
        print(f"    🏢 Fee Record Counterparty: {fee_record['counterparty_code']} - {fee_record['counterparty_name']}")
        print(f"    💳 Fee Record Debit Account: {fee_record.get('debit_account', 'N/A')}")
        print(f"    💳 Fee Record Credit Account: {fee_record.get('credit_account', 'N/A')}")
        
        # Verify fee record accounts
        expected_fee_debit = "6427"
        expected_fee_credit = "1121120"
        
        if (fee_record.get('debit_account') == expected_fee_debit and 
            fee_record.get('credit_account') == expected_fee_credit):
            print(f"    ✅ Fee Record Accounts CORRECT: Debit={expected_fee_debit}, Credit={expected_fee_credit}")
        else:
            print(f"    ❌ Fee Record Accounts INCORRECT:")
            print(f"        Expected Debit={expected_fee_debit}, Credit={expected_fee_credit}")
            print(f"        Actual Debit={fee_record.get('debit_account')}, Credit={fee_record.get('credit_account')}")
    else:
        print(f"    ❌ Failed to process transfer transaction")
    
    # Test Case 2: VCB to BIDV Transfer
    print("\n\n🔁 TEST CASE 2: VCB to BIDV Transfer")
    transfer_transaction2 = {
        "reference": "TRF-002",
        "datetime": datetime(2025, 6, 18, 12, 0, 0),
        "debit_amount": 5000000,
        "credit_amount": 0,
        "balance": 350000,
        "description": "IBVCB.1806250063454007.043184.IBTC.Chuyen tien tu TK VCB (6868) Sang Tam qua TK BIDV (7655) Sang Tam",
        "bank_account": "1121120"  # Current file bank account
    }
    
    print(f"\n  🔍 Test: {transfer_transaction2['description'][:50]}...")
    result = process_vcb_transfer_transaction(transfer_transaction2["description"], transfer_transaction2)
    
    if result and len(result) == 2:
        main_record = result[0]
        fee_record = result[1]
        
        print(f"    ✅ Transfer Transaction Processing Successful")
        print(f"    📝 Main Record Description: {main_record['description']}")
        
        # Verify description formatting
        expected_description = "Chuyển tiền từ TK VCB (6868) Sáng Tâm qua TK BIDV (7655) Sáng Tâm"
        if main_record['description'] == expected_description:
            print(f"    ✅ Description Formatting CORRECT: {expected_description}")
        else:
            print(f"    ❌ Description Formatting INCORRECT:")
            print(f"        Expected: {expected_description}")
            print(f"        Actual: {main_record['description']}")
    else:
        print(f"    ❌ Failed to process transfer transaction")
    
    print("\n" + "=" * 50)
    print("✅ VCB Transfer Description Formatting Testing Complete!")


if __name__ == "__main__":
    test_vcb_transfer_description_formatting()
