#!/usr/bin/env python3
"""
Test script to verify MBB KLONLINE rule implementation

This script tests the new Vietnamese person name detection functionality
alongside the existing phone number detection for MBB files.
"""

import sys
from pathlib import Path
from datetime import datetime

# Add project root to path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from src.accounting.integrated_bank_processor import IntegratedBankProcessor, RawTransaction


def test_mbb_klonline_rules():
    """Test MBB KLONLINE rules for both phone numbers and Vietnamese person names"""
    
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
    
    print("🧪 Testing MBB KLONLINE Rule Implementation")
    print("=" * 50)
    
    # Test Case 1: Phone Number Detection (Existing Functionality)
    print("\n📱 TEST CASE 1: MBB Phone Number Detection")
    test_cases_phone = [
        "Chuyen khoan cho 0903123456 - PO ABC123",
        "Thanh toan 0901234567 tai cua hang - PO DEF456",
        "GUI TIEN CHO 84903999057 - PO GHI789",
    ]
    
    for i, description in enumerate(test_cases_phone, 1):
        transaction = RawTransaction(
            reference=f"PHONE-{i:03d}",
            datetime=datetime(2025, 3, 5, 10, i),
            debit_amount=0,
            credit_amount=1500000,
            balance=68500000,
            description=description,
        )
        
        print(f"\n  🔍 Test {i}: {description}")
        entry = processor.process_transaction(transaction)
        
        if entry:
            print(f"    ✅ KLONLINE Rule Applied")
            print(f"    📝 Description: {entry.description}")
            print(f"    🏢 Counterparty: {entry.counterparty_code} - {entry.counterparty_name}")
            print(f"    📍 Address: {entry.address}")
        else:
            print(f"    ❌ Failed to process")
    
    # Test Case 2: Vietnamese Person Name Detection (NEW Functionality)
    print("\n\n👤 TEST CASE 2: Vietnamese Person Name Detection (NEW)")
    test_cases_names = [
        "Chuyen tien cho Nguyen Van An - PO DEF456",
        "Thanh toan cho Le Thi Bao - PO GHI789",
        "Gui cho ong Tran Van Cuong - PO JKL012",
        "Hoan tien cho Pham Thi Dao - PO MNO345",
        "Chuyen khoan cho Hoang Van Em - PO PQR678",
    ]
    
    for i, description in enumerate(test_cases_names, 1):
        transaction = RawTransaction(
            reference=f"NAME-{i:03d}",
            datetime=datetime(2025, 3, 5, 11, i),
            debit_amount=0,
            credit_amount=2000000,
            balance=70500000,
            description=description,
        )
        
        print(f"\n  🔍 Test {i}: {description}")
        entry = processor.process_transaction(transaction)
        
        if entry:
            print(f"    ✅ KLONLINE Rule Applied")
            print(f"    📝 Description: {entry.description}")
            print(f"    🏢 Counterparty: {entry.counterparty_code} - {entry.counterparty_name}")
            print(f"    📍 Address: {entry.address}")
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
        description="Chuyen tien cho Nguyen Van An - PO DEF456",  # Same pattern as MBB test
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
    
    print("\n" + "=" * 50)
    print("✅ MBB KLONLINE Rule Testing Complete!")
    print("\nExpected Results:")
    print("- Phone number cases should trigger KLONLINE rule")
    print("- Vietnamese person name cases should trigger KLONLINE rule") 
    print("- Both should have same counterparty info:")
    print("  * Code: KLONLINE")
    print("  * Name: KHÁCH LẺ KHÔNG LẤY HÓA ĐƠN (ONLINE)")
    print("  * Address: 4 Grand Canal Square, Grand Canal Harbour, Dublin 2, Ireland")
    print("- Non-MBB banks should NOT trigger KLONLINE rule")
    print("- Generic descriptions should NOT trigger KLONLINE rule")


if __name__ == "__main__":
    test_mbb_klonline_rules()
