# Transaction Processing Rules Update

This document provides instructions for implementing additional transaction processing rules for bank statements.

## Overview

Seven new rules need to be implemented to properly process specific transaction descriptions and assign the correct accounts:

1. **GIAI NGAN TKV SO pattern:**
   - Input: `"GIAI NGAN TKV SO 492175109 CUA CONG TY TNHH SANG TAM THANH TOAN TIEN"`
   - Output: `"Nhận giải ngân HĐGN 492175109"`
   - Account: `34111`

2. **TRICH TAI KHOAN with DE THU THANH LY pattern:**
   - Input: `"TRICH TAI KHOAN 33388368 DE THU THANH LY TK VAY 462669209"`
   - Output: `"Trả hết nợ gốc TK vay KU 462669209"`
   - Account: `34111`

3. **THU PHI TT pattern:**
   - Input: `"THU PHI TT SO BTD253238700 PHI CHUYEN TIEN 183.28 USD TI GIA 26243, DIEN PHI 8 USD TI GIA 26243, PHI NHNN (OUR) 25 USD TI GIA 26243,"`
   - Output: `"Phí TTQT"`
   - Account: `6427`

4. **GHTK pattern:**
   - Input: `"GHTKJSC-GIAOHANGTIETKIEM CHUYEN TIEN COD 20.06.2025 S22890267 BC2150369392.S22890267.200625"`
   - Output: `"GHTK thanh toán tiền thu hộ theo bảng kê"`

5. **LAI NHAP VON pattern:**
   - Input: `"##LAI NHAP VON#"`
   - Output: `"Lãi nhập vốn NH [bank code]"` (e.g., "Lãi nhập vốn NH ACB")

6. **TRICH THU TIEN VAY - LAI pattern:**
   - Input: `"TRICH THU TIEN VAY - LAI : 3579090 - ACCT VAY 488972159"`
   - Output: `"Trả lãi vay KU 488972159"`
   - Account: `6354`

7. **GIAI NGAN TKV SO with long description pattern:**
   - Input: `"GIAI NGAN TKV SO 494113749 CUA CONG TY TNHH SANG TAM BO SUNG VON LUU DONG..."`
   - Output: `"Nhận giải ngân HĐGN 494113749"`
   - Account: `34111`

## Implementation Files

This package contains the following files:

1. `transaction_processor_update.py`: Contains the complete implementation code
2. `transaction_processor_patch.txt`: Shows the exact changes needed to the existing code
3. `test_transaction_rules.py`: Test script to verify the implementation works correctly

## Implementation Steps

1. **Update the Special Account Mappings**:
   - Open `integrated_bank_processor.py`
   - Locate the `special_account_mappings` dictionary in the `__init__` method
   - Verify all the required mappings exist (see `transaction_processor_patch.txt`)
   - Add any missing mappings

2. **Enhance the Pattern Matching Logic**:
   - Find the pattern-specific logic in the `process_transaction` method
   - Update the existing pattern handlers or add new ones as needed
   - Use the implementations provided in `transaction_processor_update.py`

3. **Test the Implementation**:
   - Run the test script: `python src/accounting/test_transaction_rules.py`
   - Verify that all test cases pass
   - Fix any issues with the implementation

## Additional Notes

- The implementation takes care to maintain backwards compatibility with existing functionality
- The new patterns are integrated within the existing pattern matching structure
- Account determination logic is carefully preserved with enhancements for specific patterns
- The implementation uses the same logging approach as the existing code

## Support

If you have any questions or encounter issues with the implementation, please contact the tech lead for assistance.
