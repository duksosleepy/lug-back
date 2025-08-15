# DUY TRI SMS Transaction Handling Implementation

This patch adds support for handling bank statement entries with "DUY TRI SMS" in the description, following the business requirements to:

1. Set Ma_Dt (counterparty_code) to the bank code from _banks.json
2. Set Ong_Ba (counterparty_name) to the bank name from _banks.json 
3. Set Dia_Chi (address) to the bank address from _banks.json
4. Set Dien_Giai (description) to "Phí Duy trì BSMS"

## Implementation Files

This package contains the following files:

1. `duy_tri_sms_patch.txt` - The actual code changes needed for the implementation
2. `apply_duy_tri_sms_patch.py` - Automated script to apply the changes
3. `test_duy_tri_sms.py` - Test script to verify the implementation works correctly

## Implementation Options

### Option 1: Automated Implementation

Run the automated patch script:

```bash
cd /home/khoi/code/lug-back/src/accounting
python patch/apply_duy_tri_sms_patch.py
```

### Option 2: Manual Implementation

1. **Add DUY TRI SMS to special_account_mappings**:
   - Open `integrated_bank_processor.py`
   - Locate the `special_account_mappings` dictionary in the `__init__` method
   - Add the following entries at the end of the dictionary:
   
   ```python
   "DUY TRI SMS": "6427",  # SMS maintenance fee
   "PHI DUY TRI SMS": "6427",  # Alternative for SMS maintenance fee
   "BSMS": "6427",  # BIDV SMS service fee
   ```

2. **Add the special handling logic**:
   - In the `process_transaction` method, find the section where other special pattern handling occurs
   - Add the following code block after other patterns like "Thanh toan lai", "ONL KDV PO", etc.:
   
   ```python
   # NEW BUSINESS LOGIC: Special handling for "DUY TRI SMS" transactions
   elif "DUY TRI SMS" in transaction.description.upper():
       bank_name = self.current_bank_name
       if bank_name:
           # Get full bank info from current_bank_info
           bank_address = self.current_bank_info.get("address", "")
           bank_code = self.current_bank_info.get("code", "")

           # Set counterparty code to bank code from _banks.json
           if bank_code:
               counterparty_code = str(bank_code)  # Make sure it's a string
               self.logger.info(f"Setting counterparty code to bank code: {counterparty_code}")
           else:
               counterparty_code = "BANK-" + bank_name if bank_name else counterparty_code

           description = "Phí Duy trì BSMS"
           
           # Override counterparty with bank info
           counterparty_name = self.current_bank_info.get("name", bank_name) or counterparty_name
           address = bank_address or address

           self.logger.info(
               f"Applied enhanced 'DUY TRI SMS' logic with bank info: {description}\n"
               f"Counterparty Code: {counterparty_code}, Counterparty Name: {counterparty_name}"
           )
       else:
           # Fallback to standard description if no bank info
           description = "Phí Duy trì BSMS"
           self.logger.info(f"Applied standard 'DUY TRI SMS' logic (no bank info): {description}")

       # Also set the account determination as if this was a special pattern with fixed accounts
       if rule.document_type == "BN":  # Payment/Debit transaction - this is the typical case for SMS fees
           debit_account = self.special_account_mappings.get("DUY TRI SMS", "6427")
           credit_account = self.default_bank_account
       else:  # BC - Receipt/Credit transaction - unlikely for SMS fees, but included for completeness
           debit_account = self.default_bank_account
           credit_account = self.special_account_mappings.get("DUY TRI SMS", "6427")
   ```

## Testing the Implementation

After applying the changes, run the test script to verify the implementation:

```bash
cd /home/khoi/code/lug-back/src/accounting
python patch/test_duy_tri_sms.py
```

The test script will create a sample DUY TRI SMS transaction and verify that it's processed correctly according to the business requirements.

## Additional Notes

- The implementation adds the appropriate account mappings (6427 - Bank service fees)
- The pattern matching is case-insensitive for better reliability
- Bank information is extracted from the current bank context (from file name or header)
- The code provides robust fallbacks if bank information is not available
- Detailed logs are generated to track the processing of these transactions
