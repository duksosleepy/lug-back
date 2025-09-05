#!/usr/bin/env python3
"""
Debug script to analyze company name extraction patterns
"""

import re
from src.accounting.counterparty_extractor import CounterpartyExtractor

# Sample transaction descriptions and expected company names
test_cases = [
    # (description, expected_company_name)
    ("CONG TY TNHH TRUNG TAM THUONG MAI RIVERSIDE TOWER-CONG TY TNHH TRUNG TAM THUONG MAI RIVERSIDE TOWER CHUYEN TRA DOANH THU KHU MUA SAM TANG 1 TU NGAY 01-15/06/2025 GIAN HANG KNOMO DA KHAU TRU PHI DV T6.2025", "Riverside Tower"),
    ("CTY SANG TAM TT TIEN THUE MB, PHI DV T06.2025 TAI GO CAN THO THEO HD 948 CHO CTY CP BDS VIET - NHAT", "VIET - NHAT"),
    ("CTY SANG TAM TT TIEN MB, PHI DV T06.2025 TAI GO HAI PHONG VA GO DA NANG THEO HD 8363; HD 8512 CHO CTY AN LAC", "AN LAC"),
    ("CTY TNHH MTV DT VA TM THE GARDEN-GARDEN TRA TIEN", "THE GARDEN"),
    ("AEON VIETNAM CO LTD-AEON VIETNAM THANH TOAN TIEN HANG", "AEON"),
    ("GHTKJSC-GIAOHANGTIETKIEM CHUYEN TIEN COD 20.06.2025 S22890267 BC2150369392.S22890267.200625", "GHTK"),
    ("VINHOMES1-GD 991412-2025-06-27T16:23:51-970407-/CTR/VINHOMES PAYS FOR VENDOR 0020026536/MAC/EAA4BC0000004C12", "VINHOMES"),
]

def analyze_patterns():
    """Analyze patterns in the test cases"""
    extractor = CounterpartyExtractor()
    
    print("ANALYZING COMPANY NAME EXTRACTION PATTERNS")
    print("=" * 50)
    
    for i, (description, expected) in enumerate(test_cases, 1):
        print(f"\nTest Case {i}:")
        print(f"Description: {description}")
        print(f"Expected: {expected}")
        
        # Extract counterparties using the current implementation
        extracted = extractor.extract_counterparties(description)
        print(f"Extracted: {extracted}")
        
        # Show cleaned names
        if extracted:
            for party in extracted:
                raw_name = party['name']
                cleaned_name = extractor.clean_counterparty_name(raw_name)
                print(f"  Raw: '{raw_name}' -> Cleaned: '{cleaned_name}'")
        
        print("-" * 30)

if __name__ == "__main__":
    analyze_patterns()