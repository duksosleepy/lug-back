#!/usr/bin/env python3
"""
Test script to verify VCB VISA/MASTER fee record account correction

This script tests that fee records correctly place 1311 in the credit account (Tk_Co),
not the debit account (Tk_No).
"""

import sys
from pathlib import Path
from datetime import datetime

# Add project root to path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from src.accounting.vcb_processor import process_vcb_pos_transaction


def test_vcb_fee_account_correction():
    """Test VCB fee account correction"""
    
    print("🧪 Testing VCB Fee Account Correction")
    print("=" * 50)
    
    # Test Case 1: VISA Transaction
    print("\n💳 TEST CASE 1: VCB VISA Transaction")
    visa_transaction = {
        "reference": "VISA-001",
        "datetime": datetime(2025, 6, 15, 10, 30, 0),
        "debit_amount": 0,
        "credit_amount": 399000,
        "balance": 67000000,
        "description": "T/t T/ung the VISA:CT TNHH SANG TAM; MerchNo: 3700109907 Gross Amt: Not On-Us=399,000.00 VND; VAT Amt:13,079.00/11 = 1,189.00 VND(VAT code:0306131754); Code:1005; SLGD: Not On-Us=1; Ngay 15/06/2025.",
        "bank_account": "1121120"  # Current file bank account
    }
    
    print(f"\n  🔍 Test: {visa_transaction['description'][:50]}...")
    result = process_vcb_pos_transaction(visa_transaction["description"], visa_transaction)
    
    if result and len(result) == 2:
        main_record = result[0]
        fee_record = result[1]
        
        print(f"    ✅ VISA Transaction Processing Successful")
        print(f"    📝 Main Record Description: {main_record['description']}")
        print(f"    💳 Main Record Debit Account: {main_record.get('debit_account', 'N/A')}")
        print(f"    💳 Main Record Credit Account: {main_record.get('credit_account', 'N/A')}")
        
        print(f"    📝 Fee Record Description: {fee_record['description']}")
        print(f"    💳 Fee Record Debit Account: {fee_record.get('debit_account', 'N/A')}")
        print(f"    💳 Fee Record Credit Account: {fee_record.get('credit_account', 'N/A')}")
        
        # Verify fee record business requirements
        expected_fee_debit = "6417"   # Fee account for VISA
        expected_fee_credit = "1311"  # 1311 should be in credit account
        
        if (fee_record.get('debit_account') == expected_fee_debit and 
            fee_record.get('credit_account') == expected_fee_credit):
            print(f"    ✅ Fee Record Business Logic CORRECT: Debit={expected_fee_debit}, Credit={expected_fee_credit}")
        else:
            print(f"    ❌ Fee Record Business Logic INCORRECT:")
            print(f"        Expected Debit={expected_fee_debit}, Credit={expected_fee_credit}")
            print(f"        Actual Debit={fee_record.get('debit_account')}, Credit={fee_record.get('credit_account')}")
    else:
        print(f"    ❌ Failed to process VISA transaction")
    
    # Test Case 2: MASTER Transaction
    print("\n\n💳 TEST CASE 2: VCB MASTER Transaction")
    master_transaction = {
        "reference": "MASTER-001",
        "datetime": datetime(2025, 6, 15, 11, 0, 0),
        "debit_amount": 0,
        "credit_amount": 2500000,
        "balance": 69500000,
        "description": "T/t T/ung the MASTER:CT TNHH SANG TAM; MerchNo: 3700109908 Gross Amt: Not On-Us=2,500,000.00 VND; VAT Amt:89,925.00/11 = 8,175.00 VND(VAT code:0306131755); Code:1006; SLGD: Not On-Us=1; Ngay 15/06/2025.",
        "bank_account": "1121120"  # Current file bank account
    }
    
    print(f"\n  🔍 Test: {master_transaction['description'][:50]}...")
    result = process_vcb_pos_transaction(master_transaction["description"], master_transaction)
    
    if result and len(result) == 2:
        main_record = result[0]
        fee_record = result[1]
        
        print(f"    ✅ MASTER Transaction Processing Successful")
        print(f"    📝 Main Record Description: {main_record['description']}")
        print(f"    💳 Main Record Debit Account: {main_record.get('debit_account', 'N/A')}")
        print(f"    💳 Main Record Credit Account: {main_record.get('credit_account', 'N/A')}")
        
        print(f"    📝 Fee Record Description: {fee_record['description']}")
        print(f"    💳 Fee Record Debit Account: {fee_record.get('debit_account', 'N/A')}")
        print(f"    💳 Fee Record Credit Account: {fee_record.get('credit_account', 'N/A')}")
        
        # Verify fee record business requirements
        expected_fee_debit = "6427"   # Fee account for MASTER
        expected_fee_credit = "1311"  # 1311 should be in credit account
        
        if (fee_record.get('debit_account') == expected_fee_debit and 
            fee_record.get('credit_account') == expected_fee_credit):
            print(f"    ✅ Fee Record Business Logic CORRECT: Debit={expected_fee_debit}, Credit={expected_fee_credit}")
        else:
            print(f"    ❌ Fee Record Business Logic INCORRECT:")
            print(f"        Expected Debit={expected_fee_debit}, Credit={expected_fee_credit}")
            print(f"        Actual Debit={fee_record.get('debit_account')}, Credit={fee_record.get('credit_account')}")
    else:
        print(f"    ❌ Failed to process MASTER transaction")
    
    print("\n" + "=" * 50)
    print("✅ VCB Fee Account Correction Testing Complete!")


if __name__ == "__main__":
    test_vcb_fee_account_correction()