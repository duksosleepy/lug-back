#!/usr/bin/env python3
"""
Test script to verify NEW MBB Trace/ACSP keyword detection functionality

This script tests the updated Trace/ACSP keyword detection functionality
where only Trace/ACSP keywords are required (no phone number or person name needed)
for MBB bank transactions.
"""

import sys
from pathlib import Path
from datetime import datetime

# Add project root to path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from src.accounting.integrated_bank_processor import IntegratedBankProcessor, RawTransaction


def test_mbb_trace_acsp_new_rules():
    """Test NEW MBB Trace/ACSP rules - only Trace/ACSP keywords required"""
    
    processor = IntegratedBankProcessor()
    
    if not processor.connect():
        print("❌ Failed to connect to processor")
        return
        
    # Set current bank to MBB
    processor.current_bank_name = "MBB"
    processor.current_bank_info = {
        "code": "MB",
        "name": "Ngân hàng TMCP Quân đội",
        "short_name": "MBB",
        "address": "Hà Nội, Việt Nam"
    }
    
    print("🧪 Testing NEW MBB Trace/ACSP Rule Implementation")
    print("=" * 55)
    
    # Test Case 1: ONLY Trace Keyword Detection (No phone, no person name)
    print("\n🎯 TEST CASE 1: ONLY Trace Keyword Detection (NEW BEHAVIOR)")
    test_cases_trace_only = [
        "Ma giao dich  Trace165646",
        "Ma giao dich Trace 156445",
        "Ma GD Trace/abc123456",
        "Giao dich Trace987654321",
        "TRACE/def789012",
    ]
    
    for i, description in enumerate(test_cases_trace_only, 1):
        transaction = RawTransaction(
            reference=f"TRACE-ONLY-{i:03d}",
            datetime=datetime(2025, 3, 5, 10, i),
            debit_amount=0,
            credit_amount=1500000,
            balance=68500000,
            description=description,
        )
        
        print(f"\n  🔍 Test {i}: {description}")
        entry = processor.process_transaction(transaction)
        
        if entry:
            print(f"    ✅ Transaction Processed")
            print(f"    📝 Description: {entry.description}")
            print(f"    🏢 Counterparty: {entry.counterparty_code} - {entry.counterparty_name}")
            print(f"    📍 Address: {entry.address}")
            
            # Check if it's using KLONLINE counterparty
            if entry.counterparty_code == "KLONLINE":
                print(f"    🎯 KLONLINE Rule Applied Correctly (NEW BEHAVIOR)")
            else:
                print(f"    ❌ KLONLINE Rule NOT Applied")
        else:
            print(f"    ❌ Failed to process")
    
    # Test Case 2: ONLY ACSP Keyword Detection (No phone, no person name)
    print("\n\n🎯 TEST CASE 2: ONLY ACSP Keyword Detection (NEW BEHAVIOR)")
    test_cases_acsp_only = [
        "Ma giao dich ACSP/abc123456",
        "Ma GD A CSP/def789012",  # Note the space in ACSP
        "Ma Giao Dich ACSP/ghi345678",
        "ACSP/xyz987654",
        "A CSP/jkl321098",  # Note the space in ACSP
    ]
    
    for i, description in enumerate(test_cases_acsp_only, 1):
        transaction = RawTransaction(
            reference=f"ACSP-ONLY-{i:03d}",
            datetime=datetime(2025, 3, 5, 11, i),
            debit_amount=0,
            credit_amount=2000000,
            balance=70500000,
            description=description,
        )
        
        print(f"\n  🔍 Test {i}: {description}")
        entry = processor.process_transaction(transaction)
        
        if entry:
            print(f"    ✅ Transaction Processed")
            print(f"    📝 Description: {entry.description}")
            print(f"    🏢 Counterparty: {entry.counterparty_code} - {entry.counterparty_name}")
            print(f"    📍 Address: {entry.address}")
            
            # Check if it's using KLONLINE counterparty
            if entry.counterparty_code == "KLONLINE":
                print(f"    🎯 KLONLINE Rule Applied Correctly (NEW BEHAVIOR)")
            else:
                print(f"    ❌ KLONLINE Rule NOT Applied")
        else:
            print(f"    ❌ Failed to process")
    
    # Test Case 3: Description Cleanup Test
    print("\n\n🧹 TEST CASE 3: Description Cleanup Test")
    test_cases_cleanup = [
        "0975430142 FT25196964084701 Ma gi ao dich Trace165646",
        "Nguyen Van An chuyen tien Ma giao dich ACSP/abc123456",
        "0903101927 Tran Thi Kim Thuong Ma GD Trace050110",
    ]
    
    for i, description in enumerate(test_cases_cleanup, 1):
        transaction = RawTransaction(
            reference=f"CLEANUP-{i:03d}",
            datetime=datetime(2025, 3, 5, 12, i),
            debit_amount=0,
            credit_amount=2500000,
            balance=72500000,
            description=description,
        )
        
        print(f"\n  🔍 Test {i}: {description}")
        entry = processor.process_transaction(transaction)
        
        if entry:
            print(f"    ✅ Transaction Processed")
            print(f"    📝 Original: {description}")
            print(f"    📝 Final Description: {entry.description}")
            print(f"    🏢 Counterparty: {entry.counterparty_code} - {entry.counterparty_name}")
            
            # Check if phone numbers and person names were removed
            if entry.counterparty_code == "KLONLINE":
                print(f"    🎯 KLONLINE Rule Applied with Description Cleanup")
                # Check that the description is in the expected format
                if "Thu tiền KH online thanh toán cho PO:" in entry.description:
                    print(f"    ✅ Description correctly formatted")
                else:
                    print(f"    ❌ Description not correctly formatted")
            else:
                print(f"    ❌ KLONLINE Rule NOT Applied")
        else:
            print(f"    ❌ Failed to process")
    
    # Test Case 4: Non-MBB Bank (Should NOT trigger KLONLINE even with Trace/ACSP)
    print("\n\n🏦 TEST CASE 4: Non-MBB Bank (Should NOT trigger KLONLINE)")
    processor.current_bank_name = "VCB"
    processor.current_bank_info = {
        "code": "VCB",
        "name": "Ngân hàng TMCP Ngoại thương Việt Nam",
        "short_name": "VCB",
        "address": "Hà Nội, Việt Nam"
    }
    
    transaction = RawTransaction(
        reference="VCB-TRACE-001",
        datetime=datetime(2025, 3, 5, 13, 0),
        debit_amount=0,
        credit_amount=1500000,
        balance=68500000,
        description="Ma giao dich Trace123456",  # Same pattern as MBB test
    )
    
    print(f"\n  🔍 VCB Test: {transaction.description}")
    entry = processor.process_transaction(transaction)
    
    if entry:
        if entry.counterparty_code == "KLONLINE":
            print(f"    ❌ KLONLINE Rule Applied to non-MBB bank (Should NOT happen)")
            print(f"    📝 Description: {entry.description}")
            print(f"    🏢 Counterparty: {entry.counterparty_code} - {entry.counterparty_name}")
        else:
            print(f"    ✅ KLONLINE Rule NOT Applied to VCB (Correct)")
            print(f"    🏢 Counterparty: {entry.counterparty_code} - {entry.counterparty_name}")
    else:
        print(f"    ❌ Failed to process")
    
    processor.close()
    
    print("\n" + "=" * 55)
    print("✅ NEW MBB Trace/ACSP Rule Testing Complete!")
    print("\nExpected Results:")
    print("- Trace keyword alone should trigger KLONLINE rule (NEW)")
    print("- ACSP keyword alone should trigger KLONLINE rule (NEW)") 
    print("- Both should have same counterparty info:")
    print("  * Code: KLONLINE")
    print("  * Name: KHÁCH LẺ KHÔNG LẤY HÓA ĐƠN (ONLINE)")
    print("  * Address: 4 Grand Canal Square, Grand Canal Harbour, Dublin 2, Ireland")
    print("- Phone numbers and person names should be removed from descriptions")
    print("- Non-MBB banks should NOT trigger KLONLINE rule even with Trace/ACSP")


if __name__ == "__main__":
    test_mbb_trace_acsp_new_rules()