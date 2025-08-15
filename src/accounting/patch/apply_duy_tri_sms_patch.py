#!/usr/bin/env python3
"""
Patch for handling DUY TRI SMS transactions in IntegratedBankProcessor

This patch adds support for processing bank statement entries with "DUY TRI SMS" 
in the description, setting the counterparty information to the bank details from
_banks.json and using a standardized description.
"""

import re
from pathlib import Path

def apply_patch():
    """Apply the patch to integrated_bank_processor.py"""
    processor_path = Path(__file__).parent.parent / "integrated_bank_processor.py"
    
    if not processor_path.exists():
        print(f"Error: {processor_path} not found")
        return False
    
    # Read the original file content
    with open(processor_path, "r", encoding="utf-8") as f:
        content = f.read()
    
    # Step 1: Add DUY TRI SMS to special_account_mappings
    # Find the special_account_mappings dictionary
    mappings_pattern = r"self\.special_account_mappings\s*=\s*\{[^}]*\}"
    mappings_match = re.search(mappings_pattern, content, re.DOTALL)
    
    if not mappings_match:
        print("Error: special_account_mappings dictionary not found")
        return False
    
    mappings_text = mappings_match.group(0)
    
    # Check if DUY TRI SMS is already in the mappings
    if "DUY TRI SMS" in mappings_text:
        print("DUY TRI SMS mapping already exists")
    else:
        # Add the new mapping at the end of the dictionary
        new_mappings_text = mappings_text.rstrip("}") + ',\n    "DUY TRI SMS": "6427",  # SMS maintenance fee\n    "PHI DUY TRI SMS": "6427",  # Alternative for SMS maintenance fee\n    "BSMS": "6427",  # BIDV SMS service fee\n}'
        content = content.replace(mappings_text, new_mappings_text)
        print("Added DUY TRI SMS to special_account_mappings")
    
    # Step 2: Add the special handling for DUY TRI SMS in process_transaction
    # Look for a good insertion point after other special patterns
    insertion_patterns = [
        r"# ENHANCED LOGIC: Special handling for \"Phí cà thẻ\"[^\n]*\n",
        r"# NEW BUSINESS LOGIC: Special handling for \"ONL KDV PO\" pattern[^\n]*\n",
        r"# Apply NEW business logic for POS machine statements[^\n]*\n",
        r"# RULE 1 & 2: Special handling for \"TRICH TAI KHOAN\" patterns[^\n]*\n"
    ]
    
    insertion_point = None
    for pattern in insertion_patterns:
        match = re.search(pattern, content)
        if match:
            insertion_point = match.end()
    
    if insertion_point is None:
        print("Error: Could not find a suitable insertion point for DUY TRI SMS logic")
        return False
    
    # Check if DUY TRI SMS handling is already implemented
    if "Special handling for \"DUY TRI SMS\"" in content:
        print("DUY TRI SMS handling already implemented")
    else:
        # Prepare the code block to insert
        duy_tri_sms_code = """            # NEW BUSINESS LOGIC: Special handling for "DUY TRI SMS" transactions
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
                        f"Applied enhanced 'DUY TRI SMS' logic with bank info: {description}\\n"
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

"""
        # Find the last elif in the chain before our insertion point
        # Look for a chunk of content with an elif to modify
        content_chunk = content[:insertion_point]
        last_elif_pos = content_chunk.rfind("elif ")
        
        if last_elif_pos == -1:
            print("Error: Could not find 'elif' pattern to insert after")
            return False
        
        # Insert our code after the last elif block
        # Find the end of the last elif block
        last_block_end = content_chunk.find("\n", last_elif_pos)
        
        # Insert our code after the last elif block
        new_content = content[:last_block_end] + "\n" + duy_tri_sms_code + content[last_block_end:]
        content = new_content
        
        print("Added DUY TRI SMS handling code")
    
    # Write the modified content back to the file
    with open(processor_path, "w", encoding="utf-8") as f:
        f.write(content)
    
    print(f"Successfully patched {processor_path}")
    return True

if __name__ == "__main__":
    apply_patch()
