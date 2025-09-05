#!/usr/bin/env python3
"""
Comprehensive test script to verify MBB First and Second Priority Rules

This script tests both:
1. First Priority: MBB with trace/ACSP keywords (includes number in description)
2. Second Priority: MBB with phone numbers or Vietnamese person names (excludes number from description)
"""

import sys
from pathlib import Path
from datetime import datetime

# Add project root to path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from src.accounting.integrated_bank_processor import IntegratedBankProcessor, RawTransaction


def test_mbb_priority_rules():
    """Test MBB first and second priority rules"""
    
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
    
    print("🧪 Testing MBB First and Second Priority Rule Implementation")
    print("=" * 60)
    
    # FIRST PRIORITY TESTS: MBB with trace/ACSP keywords (INCLUDE number in description)
    print("\n🏆 TEST CASE 1: First Priority - MBB with Trace/ACSP Keywords")
    print("   (Should INCLUDE number in description)")
    
    first_priority_tests = [
        # With phone number + trace/ACSP
        ("0975430142 FT25196964084701   Ma gi  ao dich  Trace165646 Trace 165646", "Trace165646"),
        ("Vali Lusetti MP010   0903101927   T  ran Thi Kim Thuong   Ma giao dich    Trace050110 Trace 050110", "Trace050110"),
        ("nguyen thi bao ngoc   sdt 091479688  0   Ma giao dich  Trace155262 Trace   155262", "Trace155262"),
        ("PHAN HIEN BAO THU chuyen tien   Ma   giao dich  Trace621407 Trace 621407", "Trace621407"),
        ("Tran Huu Phuoc 0365397739   Ma giao   dich  Trace703698 Trace 703698", "Trace703698"),
        ("0795858128   Ma giao dich  Trace156  445 Trace 156445", "Trace156445"),
        
        # With ACSP keywords + phone number
        ("MBVCB.9934446701.602687.LE VIET THU   chuyen tien LUG.CT tu 011100035983  3 LE VIET THU toi 7432085703944 CON  G TY TRACH NHIEM HUU HAN SANG TAM t ai MB- Ma GD ACSP/ ky602687", "ky602687"),
        ("NGUYEN TAN PHAT 0916085810- Ma GD A  CSP/ iA471233", "iA471233"),
    ]
    
    for i, (description, expected_number) in enumerate(first_priority_tests, 1):
        transaction = RawTransaction(
            reference=f"FIRST-{i:03d}",
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
                print(f"    🎯 KLONLINE Rule Applied Correctly (First Priority)")
                
                # Check if description is formatted correctly for first priority (includes number)
                expected_desc = f"Thu tiền KH online thanh toán cho PO: {expected_number}"
                if entry.description == expected_desc:
                    print(f"    ✅ Description correctly formatted for first priority WITH number")
                else:
                    print(f"    ❌ Description NOT correctly formatted for first priority")
                    print(f"      Expected: {expected_desc}")
                    print(f"      Actual:   {entry.description}")
            else:
                print(f"    ❌ KLONLINE Rule NOT Applied")
        else:
            print(f"    ❌ Failed to process")
    
    # SECOND PRIORITY TESTS: MBB with phone numbers or Vietnamese person names (EXCLUDE number from description)
    print("\n\n🥈 TEST CASE 2: Second Priority - MBB with Phone Numbers or Vietnamese Person Names")
    print("   (Should EXCLUDE number from description)")
    
    second_priority_tests = [
        # Phone numbers only (no trace/ACSP)
        ("thao 0937976698", "phone"),
        ("TRAN DAT PHU QUOC 0901951867", "phone"),
        ("0984459116", "phone"),
        
        # Vietnamese person names only (no trace/ACSP)
        ("MA THI BICH NGOC  chuyen tien", "vietnamese_name"),
        ("TANG THI THU THAO chuyen tien", "vietnamese_name"),
        ("hong ly nguyen", "vietnamese_name"),
    ]
    
    for i, (description, reason) in enumerate(second_priority_tests, 1):
        transaction = RawTransaction(
            reference=f"SECOND-{i:03d}",
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
                
                # Check if description is formatted correctly for second priority (excludes number)
                expected_desc = "Thu tiền KH online thanh toán cho PO:"
                if entry.description == expected_desc:
                    print(f"    ✅ Description correctly formatted for second priority WITHOUT number")
                else:
                    print(f"    ❌ Description NOT correctly formatted for second priority")
                    print(f"      Expected: {expected_desc}")
                    print(f"      Actual:   {entry.description}")
            else:
                print(f"    ❌ KLONLINE Rule NOT Applied")
        else:
            print(f"    ❌ Failed to process")
    
    # NEGATIVE TESTS: Non-MBB banks should NOT trigger KLONLINE
    print("\n\n❌ TEST CASE 3: Negative Cases (Non-MBB Banks)")
    print("   (Should NOT trigger KLONLINE rule)")
    
    processor.current_bank_name = "VCB"  # Change to non-MBB bank
    processor.current_bank_info = {
        "code": "VCB",
        "name": "Ngân hàng TMCP Ngoại thương Việt Nam",
        "short_name": "VCB",
        "address": "Hà Nội, Việt Nam"
    }
    
    negative_tests = [
        ("thao 0937976698", "VCB phone number"),
        ("MA THI BICH NGOC  chuyen tien", "VCB Vietnamese person name"),
        ("0975430142 FT25196964084701   Ma gi  ao dich  Trace165646 Trace 165646", "VCB with trace"),
    ]
    
    for i, (description, test_type) in enumerate(negative_tests, 1):
        transaction = RawTransaction(
            reference=f"NEG-{i:03d}",
            datetime=datetime(2025, 3, 5, 12, i),
            debit_amount=0,
            credit_amount=1000000,
            balance=69500000,
            description=description,
        )
        
        print(f"\n  🔍 Test {i} ({test_type}): {description}")
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
    
    processor.close()
    
    print("\n" + "=" * 60)
    print("✅ MBB First and Second Priority Rule Testing Complete!")
    print("\nExpected Results:")
    print("🏆 First Priority (with trace/ACSP):")
    print("  * Counterparty: KLONLINE - KHÁCH LẺ KHÔNG LẤY HÓA ĐƠN (ONLINE)")
    print("  * Address: 4 Grand Canal Square, Grand Canal Harbour, Dublin 2, Ireland")
    print("  * Description: 'Thu tiền KH online thanh toán cho PO: {number}' (WITH number)")
    print("\n🥈 Second Priority (without trace/ACSP):")
    print("  * Counterparty: KLONLINE - KHÁCH LẺ KHÔNG LẤY HÓA ĐƠN (ONLINE)")
    print("  * Address: 4 Grand Canal Square, Grand Canal Harbour, Dublin 2, Ireland")
    print("  * Description: 'Thu tiền KH online thanh toán cho PO:' (WITHOUT number)")
    print("\n❌ Non-MBB Banks:")
    print("  * Should NOT trigger KLONLINE rule")


if __name__ == "__main__":
    test_mbb_priority_rules()