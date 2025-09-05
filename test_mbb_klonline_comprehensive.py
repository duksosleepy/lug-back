#!/usr/bin/env python3
"""
Comprehensive test script to verify MBB KLONLINE rule implementation
with trace/ACSP keywords
"""

import sys
from pathlib import Path
from datetime import datetime

# Add project root to path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from src.accounting.integrated_bank_processor import IntegratedBankProcessor, RawTransaction


def test_mbb_klonline_comprehensive():
    """Test MBB KLONLINE rules with trace/ACSP keywords"""
    
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
    
    print("🧪 Testing MBB KLONLINE Rule Implementation with trace/ACSP")
    print("=" * 60)
    
    # Test Case 1: Phone Number + Trace keyword
    print("\n📱 TEST CASE 1: Phone Number + Trace keyword")
    test_cases_phone_trace = [
        "0975430142 FT25196964084701   Ma gi  ao dich  Trace165646 Trace 165646",
        "Vali Lusetti MP010   0903101927   T  ran Thi Kim Thuong   Ma giao dich    Trace050110 Trace 050110",
        "nguyen thi bao ngoc   sdt 091479688  0   Ma giao dich  Trace155262 Trace   155262",
        "PHAN HIEN BAO THU chuyen tien   Ma   giao dich  Trace621407 Trace 621407",
        "Tran Huu Phuoc 0365397739   Ma giao   dich  Trace703698 Trace 703698",
        "0795858128   Ma giao dich  Trace156  445 Trace 156445",
    ]
    
    for i, description in enumerate(test_cases_phone_trace, 1):
        transaction = RawTransaction(
            reference=f"PHONE-TRACE-{i:03d}",
            datetime=datetime(2025, 3, 5, 10, i),
            debit_amount=0,
            credit_amount=1500000,
            balance=68500000,
            description=description,
        )
        
        print(f"\n  🔍 Test {i}: {description}")
        entry = processor.process_transaction(transaction)
        
        if entry:
            if entry.counterparty_code == "KLONLINE":
                print(f"    ✅ KLONLINE Rule Applied")
                print(f"    📝 Description: {entry.description}")
                print(f"    🏢 Counterparty: {entry.counterparty_code} - {entry.counterparty_name}")
                print(f"    📍 Address: {entry.address}")
            else:
                print(f"    ❌ KLONLINE Rule NOT Applied")
                print(f"    📝 Description: {entry.description}")
                print(f"    🏢 Counterparty: {entry.counterparty_code} - {entry.counterparty_name}")
        else:
            print(f"    ❌ Failed to process")
    
    # Test Case 2: Vietnamese Person Name + ACSP keyword
    print("\n\n👤 TEST CASE 2: Vietnamese Person Name + ACSP keyword")
    test_cases_name_acsp = [
        "MBVCB.9934446701.602687.LE VIET THU   chuyen tien LUG.CT tu 011100035983  3 LE VIET THU toi 7432085703944 CON  G TY TRACH NHIEM HUU HAN SANG TAM t ai MB- Ma GD ACSP/ ky602687",
        "NGUYEN TAN PHAT 0916085810- Ma GD A  CSP/ iA471233",
    ]
    
    for i, description in enumerate(test_cases_name_acsp, 1):
        transaction = RawTransaction(
            reference=f"NAME-ACSP-{i:03d}",
            datetime=datetime(2025, 3, 5, 11, i),
            debit_amount=0,
            credit_amount=2000000,
            balance=70500000,
            description=description,
        )
        
        print(f"\n  🔍 Test {i}: {description}")
        entry = processor.process_transaction(transaction)
        
        if entry:
            if entry.counterparty_code == "KLONLINE":
                print(f"    ✅ KLONLINE Rule Applied")
                print(f"    📝 Description: {entry.description}")
                print(f"    🏢 Counterparty: {entry.counterparty_code} - {entry.counterparty_name}")
                print(f"    📍 Address: {entry.address}")
            else:
                print(f"    ❌ KLONLINE Rule NOT Applied")
                print(f"    📝 Description: {entry.description}")
                print(f"    🏢 Counterparty: {entry.counterparty_code} - {entry.counterparty_name}")
        else:
            print(f"    ❌ Failed to process")
    
    # Test Case 3: Negative Cases (Should NOT trigger KLONLINE)
    print("\n\n❌ TEST CASE 3: Negative Cases (Should NOT trigger KLONLINE)")
    test_cases_negative = [
        "Chuyen khoan cho 0903123456 - PO ABC123",  # No trace/ACSP
        "Chuyen tien cho Nguyen Van An - PO DEF456",  # No trace/ACSP
        "Transfer to company account 123456789",  # Generic description
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
        description="0975430142 FT25196964084701   Ma gi  ao dich  Trace165646 Trace 165646",  # MBB pattern
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
    
    print("\n" + "=" * 60)
    print("✅ MBB KLONLINE Rule Testing Complete!")
    print("\nExpected Results:")
    print("- Phone number + trace cases should trigger KLONLINE rule")
    print("- Vietnamese person name + ACSP cases should trigger KLONLINE rule") 
    print("- Both should have same counterparty info:")
    print("  * Code: KLONLINE")
    print("  * Name: KHÁCH LẺ KHÔNG LẤY HÓA ĐƠN (ONLINE)")
    print("  * Address: 4 Grand Canal Square, Grand Canal Harbour, Dublin 2, Ireland")
    print("- Cases without trace/ACSP should NOT trigger KLONLINE rule")
    print("- Non-MBB banks should NOT trigger KLONLINE rule")


if __name__ == "__main__":
    test_mbb_klonline_comprehensive()