# Analysis of Counterparty Extraction Enhancement Requirements

## Current Issues Identified

Based on the examples provided and code analysis, here are the main issues with the current counterparty extraction logic:

### 1. Missing Pattern Recognition for Company Names

The current implementation fails to extract company names from several key patterns in the examples:

1. `"CONG TY TNHH TRUNG TAM THUONG MAI RIVERSIDE TOWER-CONG TY TNHH TRUNG TAM THUONG MAI RIVERSIDE TOWER CHUYEN TRA DOANH THU KHU MUA SAM TANG 1 TU NGAY 01-15/06/2025 GIAN HANG KNOMO DA KHAU TRU PHI DV T6.2025"` → `"Riverside Tower"`
   - Issue: The current patterns don't handle the hyphenated format where the company name appears twice

2. `"CTY SANG TAM TT TIEN THUE MB, PHI DV T06.2025 TAI GO CAN THO THEO HD 948 CHO CTY CP BDS VIET - NHAT"` → `"VIET - NHAT"`
   - Issue: The pattern "CHO CTY CP BDS VIET - NHAT" is not recognized as a company name

3. `"CTY SANG TAM TT TIEN MB, PHI DV T06.2025 TAI GO HAI PHONG VA GO DA NANG THEO HD 8363; HD 8512 CHO CTY AN LAC"` → `"AN LAC"`
   - Issue: The pattern "CHO CTY AN LAC" is partially matched but not correctly extracted

4. `"CTY TNHH MTV DT VA TM THE GARDEN-GARDEN TRA TIEN"` → `"THE GARDEN"`
   - Issue: The pattern with hyphen is not handled properly

5. `"AEON VIETNAM CO LTD-AEON VIETNAM THANH TOAN TIEN HANG"` → `"AEON"`
   - Issue: International company names with "CO LTD" not properly extracted

6. `"GHTKJSC-GIAOHANGTIETKIEM CHUYEN TIEN COD 20.06.2025 S22890267 BC2150369392.S22890267.200625"` → `"GHTK"`
   - Issue: Abbreviated company names from concatenated formats not extracted

7. `"VINHOMES1-GD 991412-2025-06-27T16:23:51-970407-/CTR/VINHOMES PAYS FOR VENDOR 0020026536/MAC/EAA4BC0000004C12"` → `"VINHOMES"`
   - Issue: Brand names from concatenated formats not extracted

### 2. Inadequate Company Name Cleaning

The current cleaning logic has issues with:
- Removing duplicate company names when they appear twice in a description
- Properly extracting the core company name from concatenated or hyphenated formats
- Handling international company naming conventions

### 3. Missing Pattern Recognition Rules

The current counterparty_patterns in counterparty_extractor.py lacks rules for:
- Company names after "CHO CTY" or "CHO CONG TY"
- Company names in hyphenated formats
- Abbreviated company names from concatenated strings
- International company names with "CO LTD" or "JSC"

## Recommended Enhancements

### 1. Add New Counterparty Patterns

Add these patterns to the counterparty_patterns list in counterparty_extractor.py:

```python
# Enhanced patterns for company names after "CHO CTY" or "CHO CONG TY"
(r"CHO\\s+(?:CTY|CONG TY)\\s+(?:TNHH|CO PHAN|CP|JSC)?\\s*([A-Z][A-Z\\s]+?)(?=\\s+(?:HD|THANH TOAN|CHUYEN KHOAN|CK|CHI|F/O|B/O|\\d{6}|T\\d{1,2}|THANG))", "beneficiary"),
(r"CHO\\s+(?:CTY|CONG TY)\\s+(?:TNHH|CO PHAN|CP|JSC)?\\s*([A-Z][A-Z\\s]+)", "beneficiary"),

# Enhanced patterns for hyphenated company names
(r"([A-Z][A-Z\\s]+)-\\1", "hyphenated_duplicate"),  # Handles RIVERSIDE TOWER-RIVERSIDE TOWER
(r"([A-Z][A-Z\\s]+)-[A-Z0-9]+", "hyphenated_prefix"),  # Handles VINHOMES1-GD...

# Enhanced patterns for international company names
(r"([A-Z]+)\\s+VIETNAM\\s+(?:CO\\s+LTD|COMPANY|CORP)", "international_vietnam"),
(r"([A-Z]+)\\s+(?:CO\\s+LTD|COMPANY|CORP|JSC)", "international"),

# Enhanced patterns for abbreviated company names from concatenated strings
(r"([A-Z]{3,6})[A-Z]+(?:JSC)?-", "abbreviation_prefix"),  # Handles GHTKJSC-
```

### 2. Enhance the Company Name Cleaning Logic

Add special handling in the clean_counterparty_name method to:
1. Handle duplicate company names in hyphenated formats
2. Extract core company names from international naming conventions
3. Properly abbreviate company names when needed

### 3. Add Special Business Logic for Common Cases

Add specific handling for:
- GHTK → Giao hàng tiết kiệm abbreviation
- AEON → AEON company name extraction
- VINHOMES → Vinhomes brand name extraction

### 4. Improve Pattern Matching Order

Ensure the most specific patterns are matched first to avoid over-matching with generic patterns.

## Implementation Plan

1. Update counterparty_patterns in counterparty_extractor.py with new patterns
2. Enhance clean_counterparty_name method with special handling for the business cases
3. Add specific business logic for known company name patterns
4. Test with the provided examples to ensure correct extraction
5. Verify that existing functionality is not broken by the changes