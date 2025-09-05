#!/usr/bin/env python3
"""
Test script to verify MBB Trace/ACSP Enhancement Implementation

This script tests the enhanced MBB Trace/ACSP keyword detection functionality
with the new business logic requirement:

"BUT now i want to more accuracy, change this logic to if current bank is MBB 
and have trace or ACSP in description, remove the phone number and person name for me."
"""

import sys
from pathlib import Path
from datetime import datetime

# Add project root to path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from src.accounting.integrated_bank_processor import IntegratedBankProcessor, RawTransaction


def test_mbb_trace_acsp_enhancement():
    """Test enhanced MBB Trace/ACSP logic with phone number and person name removal"""
    
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
    
    print("🧪 Testing MBB Trace/ACSP Enhancement Implementation")
    print("=" * 55)
    
    # Test Case 1: MBB with Trace keyword + phone number (should remove phone number)
    print("\n📱 TEST CASE 1: MBB with Trace keyword + phone number")
    test_cases_trace_phone = [
        "Chuyen tien tu TK MB 3944 Sang Tam   qua TK ACB 8368 Sang Tam - Ma giao   dich  Trace292923 0937976698",
        "MA THI BICH NGOC  chuyen tien Ma giao dich  Trace165646 0903101927",
        "0984459116 Nguyen Van An chuyen tien Ma giao dich Trace050110",
    ]
    
    for i, description in enumerate(test_cases_trace_phone, 1):
        transaction = RawTransaction(
            reference=f"TRACE-PHONE-{i:03d}",
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
                print(f"    🎯 KLONLINE Rule Applied Correctly")
                
                # Check if description is formatted correctly and phone numbers removed
                if "Trace" in description or "ACSP" in description:
                    if "Thu tiền KH online thanh toán cho PO:" in entry.description:
                        print(f"    ✅ Description correctly formatted")
                        
                        # Check if phone numbers are removed
                        phone_patterns = [
                            r"\b0[35789]\d{8,9}\b",
                            r"\b[35789]\d{8,9}\b",
                            r"\b\+84[35789]\d{8}\b",
                            r"\b84[35789]\d{8}\b",
                        ]
                        
                        phone_found = False
                        for pattern in phone_patterns:
                            import re
                            if re.search(pattern, entry.description):
                                phone_found = True
                                break
                        
                        if not phone_found:
                            print(f"    ✅ Phone numbers removed from description")
                        else:
                            print(f"    ❌ Phone numbers NOT removed from description")
                    else:
                        print(f"    ❌ Description NOT correctly formatted")
                else:
                    print(f"    ❌ No Trace/ACSP keywords found")
            else:
                print(f"    ❌ KLONLINE Rule NOT Applied")
        else:
            print(f"    ❌ Failed to process")
    
    # Test Case 2: MBB with ACSP keyword + Vietnamese person name (should remove person name)
    print("\n\n👤 TEST CASE 2: MBB with ACSP keyword + Vietnamese person name")
    test_cases_acsp_name = [
        "MA THI BICH NGOC  chuyen tien Ma giao dich ACSP/abc123456",
        "TANG THI THU THAO chuyen tien Ma GD A CSP/def789012",
        "hong ly nguyen Ma GD ACSP/ghi345678",
    ]
    
    for i, description in enumerate(test_cases_acsp_name, 1):
        transaction = RawTransaction(
            reference=f"ACSP-NAME-{i:03d}",
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
                print(f"    🎯 KLONLINE Rule Applied Correctly")
                
                # Check if description is formatted correctly and person names removed
                if "Trace" in description or "ACSP" in description:
                    if "Thu tiền KH online thanh toán cho PO:" in entry.description:
                        print(f"    ✅ Description correctly formatted")
                        
                        # Check if person names are removed (this is harder to verify automatically)
                        # We'll just check that the description doesn't contain the full original name
                        person_names = ["MA THI BICH NGOC", "TANG THI THU THAO", "hong ly nguyen"]
                        person_found = False
                        for name in person_names:
                            if name.lower() in entry.description.lower():
                                person_found = True
                                break
                        
                        if not person_found:
                            print(f"    ✅ Person names removed from description")
                        else:
                            print(f"    ❌ Person names NOT removed from description")
                    else:
                        print(f"    ❌ Description NOT correctly formatted")
                else:
                    print(f"    ❌ No Trace/ACSP keywords found")
            else:
                print(f"    ❌ KLONLINE Rule NOT Applied")
        else:
            print(f"    ❌ Failed to process")
    
    # Test Case 3: MBB with ONLY Trace/ACSP keywords (no phone or person name)
    print("\n\n🎯 TEST CASE 3: MBB with ONLY Trace/ACSP keywords")
    test_cases_trace_acsp_only = [
        "Ma giao dich Trace165646",
        "Chuyen tien Ma GD A CSP/def789012",
        "Thu tien Ma GD ACSP/ghi345678",
    ]
    
    for i, description in enumerate(test_cases_trace_acsp_only, 1):
        transaction = RawTransaction(
            reference=f"TRACE-ONLY-{i:03d}",
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
            print(f"    📝 Description: {entry.description}")
            print(f"    🏢 Counterparty: {entry.counterparty_code} - {entry.counterparty_name}")
            print(f"    📍 Address: {entry.address}")
            
            # Check if it's using KLONLINE counterparty
            if entry.counterparty_code == "KLONLINE":
                print(f"    🎯 KLONLINE Rule Applied Correctly")
                
                # Check if description is formatted correctly
                if "Trace" in description or "ACSP" in description:
                    if "Thu tiền KH online thanh toán cho PO:" in entry.description:
                        print(f"    ✅ Description correctly formatted")
                    else:
                        print(f"    ❌ Description NOT correctly formatted")
                else:
                    print(f"    ❌ No Trace/ACSP keywords found")
            else:
                print(f"    ❌ KLONLINE Rule NOT Applied")
        else:
            print(f"    ❌ Failed to process")
    
    # Test Case 4: Negative Cases (Should NOT trigger KLONLINE)
    print("\n\n❌ TEST CASE 4: Negative Cases (Should NOT trigger KLONLINE)")
    test_cases_negative = [
        "Transfer to company account 123456789",  # No Trace/ACSP
        "Payment for services rendered",  # Generic description
        "Chuyen khoan den tai khoan 987654321",  # Account transfer, no Trace/ACSP
    ]
    
    for i, description in enumerate(test_cases_negative, 1):
        transaction = RawTransaction(
            reference=f"NEG-{i:03d}",
            datetime=datetime(2025, 3, 5, 13, i),
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
    
    # Test Case 5: Non-MBB Bank (Should NOT trigger KLONLINE even with Trace/ACSP)
    print("\n\n🏦 TEST CASE 5: Non-MBB Bank (Should NOT trigger KLONLINE)")
    processor.current_bank_name = "VCB"  # Change to non-MBB bank
    processor.current_bank_info = {
        "code": "VCB",
        "name": "Ngân hàng TMCP Ngoại thương Việt Nam",
        "short_name": "VCB",
        "address": "Hà Nội, Việt Nam"
    }
    
    test_cases_non_mbb = [
        "Ma giao dich Trace165646",  # Same pattern as MBB test
        "Chuyen tien Ma GD A CSP/def789012",  # Same pattern as MBB test
    ]
    
    for i, description in enumerate(test_cases_non_mbb, 1):
        transaction = RawTransaction(
            reference=f"NON-MBB-{i:03d}",
            datetime=datetime(2025, 3, 5, 14, i),
            debit_amount=0,
            credit_amount=1500000,
            balance=68500000,
            description=description,
        )
        
        print(f"\n  🔍 Test {i}: {description}")
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
    print("✅ MBB Trace/ACSP Enhancement Testing Complete!")
    print("\nExpected Results:")
    print("- MBB with Trace/ACSP keywords should ALWAYS trigger KLONLINE rule")
    print("- Phone numbers should be removed from descriptions when Trace/ACSP detected")
    print("- Person names should be removed from descriptions when Trace/ACSP detected")
    print("- Non-MBB banks should NOT trigger KLONLINE rule even with Trace/ACSP")
    print("- Descriptions without Trace/ACSP should NOT trigger KLONLINE rule")
    print("\nCounterparty Info:")
    print("  * Code: KLONLINE")
    print("  * Name: KHÁCH LẺ KHÔNG LẤY HÓA ĐƠN (ONLINE)")
    print("  * Address: 4 Grand Canal Square, Grand Canal Harbour, Dublin 2, Ireland")


if __name__ == "__main__":
    test_mbb_trace_acsp_enhancement()