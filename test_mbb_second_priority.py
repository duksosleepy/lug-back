#!/usr/bin/env python3
"""
Test script to verify MBB Second Priority Rule Implementation

This script tests the new second priority logic for MBB statements 
that have phone numbers or Vietnamese person names but don't have 
trace/ACSP keywords.
"""

import sys
from pathlib import Path
from datetime import datetime

# Add project root to path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from src.accounting.integrated_bank_processor import IntegratedBankProcessor, RawTransaction


def test_mbb_second_priority_rules():
    """Test MBB second priority rules for phone numbers and Vietnamese person names without trace/ACSP"""
    
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
    
    print("🧪 Testing MBB Second Priority Rule Implementation")
    print("=" * 55)
    
    # Test Case 1: MBB with Phone Numbers (Second Priority)
    print("\n📱 TEST CASE 1: MBB Phone Number Detection (Second Priority)")
    test_cases_phone = [
        "thao 0937976698",
        "TRAN DAT PHU QUOC 0901951867",
        "0984459116",
    ]
    
    for i, description in enumerate(test_cases_phone, 1):
        transaction = RawTransaction(
            reference=f"PHONE-2ND-{i:03d}",
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
                print(f"    🎯 KLONLINE Rule Applied Correctly (Second Priority)")
                
                # Check if description is formatted correctly for second priority
                if entry.description == "Thu tiền KH online thanh toán cho PO:":
                    print(f"    ✅ Description correctly formatted for second priority")
                else:
                    print(f"    ❌ Description NOT correctly formatted for second priority")
            else:
                print(f"    ❌ KLONLINE Rule NOT Applied")
        else:
            print(f"    ❌ Failed to process")
    
    # Test Case 2: MBB with Vietnamese Person Names (Second Priority)
    print("\n\n👤 TEST CASE 2: Vietnamese Person Name Detection (Second Priority)")
    test_cases_names = [
        "MA THI BICH NGOC  chuyen tien",
        "TANG THI THU THAO chuyen tien",
        "hong ly nguyen",
    ]
    
    for i, description in enumerate(test_cases_names, 1):
        transaction = RawTransaction(
            reference=f"NAME-2ND-{i:03d}",
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
                print(f"    🎯 KLONLINE Rule Applied Correctly (Second Priority)")
                
                # Check if description is formatted correctly for second priority
                if entry.description == "Thu tiền KH online thanh toán cho PO:":
                    print(f"    ✅ Description correctly formatted for second priority")
                else:
                    print(f"    ❌ Description NOT correctly formatted for second priority")
            else:
                print(f"    ❌ KLONLINE Rule NOT Applied")
        else:
            print(f"    ❌ Failed to process")
    
    # Test Case 3: Negative Cases (Should NOT trigger KLONLINE)
    print("\n\n❌ TEST CASE 3: Negative Cases (Should NOT trigger KLONLINE)")
    test_cases_negative = [
        "Transfer to company account 123456789",  # No phone, no Vietnamese name
        "Payment for services rendered",  # Generic description
        "Chuyen khoan den tai khoan 987654321",  # Account transfer, no person name
    ]
    
    for i, description in enumerate(test_cases_negative, 1):
        transaction = RawTransaction(
            reference=f"NEG-{i:03d}",
            datetime=datetime(2025, 3, 5, 12, i),
            debit_amount=0,
            credit_amount=1000000,
            balance=69500000,
            description=description,
        )
        
        print(f"\n  🔍 Test {i}: {description}")
        entry = processor.process_transaction(transaction)
        
        if entry:
            if entry.counterparty_code == "KLONLINE":
                print(f"    ❌ KLONLINE Rule Applied (Should NOT have triggered)")
                print(f"    📝 Description: {entry.description}")
                print(f"    🏢 Counterparty: {entry.counterparty_code} - {entry.counterparty_name}")
            else:
                print(f"    ✅ KLONLINE Rule NOT Applied (Correct)")
                print(f"    🏢 Counterparty: {entry.counterparty_code} - {entry.counterparty_name}")
        else:
            print(f"    ❌ Failed to process")
    
    # Test Case 4: Non-MBB Bank (Should NOT trigger KLONLINE)
    print("\n\n🏦 TEST CASE 4: Non-MBB Bank (Should NOT trigger KLONLINE)")
    processor.current_bank_name = "VCB"
    processor.current_bank_info = {
        "code": "VCB",
        "name": "Ngân hàng TMCP Ngoại thương Việt Nam",
        "short_name": "VCB",
        "address": "Hà Nội, Việt Nam"
    }
    
    transaction = RawTransaction(
        reference="VCB-001",
        datetime=datetime(2025, 3, 5, 13, 0),
        debit_amount=0,
        credit_amount=1500000,
        balance=68500000,
        description="thao 0937976698",  # Same pattern as MBB test
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
    print("✅ MBB Second Priority Rule Testing Complete!")
    print("\nExpected Results:")
    print("- Phone number cases (without trace/ACSP) should trigger KLONLINE rule (Second Priority)")
    print("- Vietnamese person name cases (without trace/ACSP) should trigger KLONLINE rule (Second Priority)") 
    print("- Both should have same counterparty info:")
    print("  * Code: KLONLINE")
    print("  * Name: KHÁCH LẺ KHÔNG LẤY HÓA ĐƠN (ONLINE)")
    print("  * Address: 4 Grand Canal Square, Grand Canal Harbour, Dublin 2, Ireland")
    print("- Description should be formatted as: \"Thu tiền KH online thanh toán cho PO:\" (without number)")
    print("- Non-MBB banks should NOT trigger KLONLINE rule")
    print("- Generic descriptions should NOT trigger KLONLINE rule")


if __name__ == "__main__":
    test_mbb_second_priority_rules()
