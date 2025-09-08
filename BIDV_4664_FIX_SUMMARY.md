# Fix for BIDV 4664 Account Mapping Issue

## Problem
When processing files with names like "BIDV 4664 HN T09.xlsx", the system was incorrectly setting the debit/credit account to `1121206` instead of the expected `1121203`. 

The issue was in the `extract_account_from_filename` method in `integrated_bank_processor.py`. Although there was a special account mapping defined for account "4664" -> "1121203", the method was not properly applying this mapping because it was checking for special mappings AFTER attempting regular account matching.

## Solution
Modified the `extract_account_from_filename` method to check for special account mappings BEFORE attempting regular account matching. This ensures that special cases like "4664" are correctly mapped to "1121203" before the system tries to find a regular account match.

## Changes Made
1. In `src/accounting/integrated_bank_processor.py`, modified the `extract_account_from_filename` method to prioritize special account mappings over regular account matching.

## Testing
- Created comprehensive tests to verify the fix works correctly
- Verified that existing functionality remains intact
- Confirmed that the debit account for BIDV 4664 files is now correctly set to `1121203`

## Impact
This fix ensures that when processing BIDV files with "4664" in the filename, the system correctly identifies and uses account `1121203` (Tiền Việt Nam - BIDV CN Chợ Lớn - 14110000184664 - CN Hà Nội) as both the default bank account and the debit account for credit transactions.