#!/usr/bin/env python3
"""
Test script to verify Vietnamese person name detection
"""

import sys
from pathlib import Path
from datetime import datetime

# Add project root to path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from src.accounting.integrated_bank_processor import IntegratedBankProcessor


def test_vietnamese_person_name_detection():
    """Test Vietnamese person name detection"""
    
    processor = IntegratedBankProcessor()
    
    if not processor.connect():
        print("❌ Failed to connect to processor")
        return
    
    # Test cases from the requirements
    test_cases = [
        "MA THI BICH NGOC  chuyen tien",
        "TANG THI THU THAO chuyen tien", 
        "hong ly nguyen",
        "thao 0937976698",  # Mixed case - should detect phone number
        "TRAN DAT PHU QUOC 0901951867",  # Mixed case - should detect phone number
        "0984459116",  # Pure phone number
    ]
    
    print("🔍 Testing Vietnamese Person Name Detection")
    print("=" * 50)
    
    for i, description in enumerate(test_cases, 1):
        print(f"\n📝 Test {i}: {description}")
        
        # Test phone number extraction
        phone_number = processor._extract_phone_number_from_description(description)
        print(f"   📱 Phone Number: '{phone_number}'")
        
        # Test person name extraction (only if no phone number)
        person_name = ""
        if not phone_number:
            person_name = processor._extract_vietnamese_person_name_from_description(description)
            print(f"   👤 Person Name: '{person_name}'")
        else:
            print(f"   👤 Person Name: SKIPPED (Phone number found)")
        
        # Test combined MBB detection
        mbb_detected = (phone_number or person_name)
        print(f"   🎯 MBB Detection: {'YES' if mbb_detected else 'NO'}")
        
        if mbb_detected:
            print(f"      ✅ Will trigger MBB second priority rule")
        else:
            print(f"      ❌ Will NOT trigger MBB second priority rule")
    
    processor.close()


if __name__ == "__main__":
    test_vietnamese_person_name_detection()