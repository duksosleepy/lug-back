#!/usr/bin/env python3
"""
Debug script to check why Vietnamese person name detection is not working
"""

import sys
from pathlib import Path
import re

# Add project root to path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from src.accounting.integrated_bank_processor import IntegratedBankProcessor


def debug_vietnamese_person_name_detection():
    """Debug Vietnamese person name detection"""
    
    processor = IntegratedBankProcessor()
    
    if not processor.connect():
        print("❌ Failed to connect to processor")
        return
    
    # Test cases from the requirements
    test_cases = [
        "MA THI BICH NGOC  chuyen tien",
        "TANG THI THU THAO chuyen tien", 
        "hong ly nguyen",
        "thao 0937976698",
        "TRAN DAT PHU QUOC 0901951867",
        "0984459116",
    ]
    
    print("🔍 Debugging Vietnamese Person Name Detection")
    print("=" * 50)
    
    for i, description in enumerate(test_cases, 1):
        print(f"\n📝 Test {i}: {description}")
        
        # Test phone number extraction
        phone_number = processor._extract_phone_number_from_description(description)
        print(f"   📱 Phone Number: '{phone_number}'")
        
        # Test person name extraction
        person_name = processor._extract_vietnamese_person_name_from_description(description)
        print(f"   👤 Person Name: '{person_name}'")
        
        # Test if Vietnamese person name is detected
        is_vietnamese_name = processor._detect_vietnamese_person_name_in_description(description)
        print(f"   🎯 Vietnamese Name Detected: {'YES' if is_vietnamese_name else 'NO'}")
        
        # Test MBB phone or person detection
        mbb_detected = (
            "MBB" == "MBB"  # Simulating current bank is MBB
            and (phone_number or person_name)
        )
        print(f"   🏦 MBB Detection: {'YES' if mbb_detected else 'NO'}")
    
    processor.close()


if __name__ == "__main__":
    debug_vietnamese_person_name_detection()
