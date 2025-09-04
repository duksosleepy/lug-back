#!/usr/bin/env python3
"""
VCB Transaction Processor

This module contains specialized functions for processing VCB (Vietcombank)
bank statements with specific patterns and requirements.
"""

import re
from typing import List

from src.util.logging import get_logger

logger = get_logger(__name__)


def process_vcb_pos_transaction(
    description: str, transaction_data: dict
) -> List[dict]:
    """
    Process VCB POS transactions with MerchNo and VAT information.

    For transactions like:
    "T/t T/ung the MASTER:CT TNHH SANG TAM; MerchNo: 3700109907 Gross Amt: Not On-Us=2,500,000.00 VND;
    VAT Amt:89,925.00/11 = 8,175.00 VND(VAT code:0306131754); Code:1005; SLGD: Not On-Us=1; Ngay 6/07/2025."

    Creates:
    - Main record: "Thu tiền bán hàng của khách lẻ POS {tid}"
    - Fee record: "Phí thu tiền bán hàng của khách lẻ POS {tid}"

    Args:
        description (str): The transaction description text
        transaction_data (dict): The original transaction data

    Returns:
        list: List of processed transaction records (main and fee)
    """
    logger.info(f"Processing VCB POS transaction: {description}")

    # Detect card type (VISA or MASTER)
    card_type = None
    if "T/t T/ung the VISA" in description:
        card_type = "VISA"
    elif "T/t T/ung the MASTER" in description:
        card_type = "MASTER"
    else:
        logger.warning(f"Unknown card type in transaction: {description}")
        return [transaction_data]  # Return original if card type not recognized

    logger.info(f"Detected card type: {card_type}")

    # 1. Extract MID from description
    mid_match = re.search(r"MerchNo:\s*(\d+)", description)
    if not mid_match:
        logger.warning(
            f"Failed to extract MID from VCB POS transaction: {description}"
        )
        return [transaction_data]  # Return original if pattern not found

    mid = mid_match.group(1)

    # 2. Extract VAT amount for fee record
    # UPDATED LOGIC: For VISA/MASTER transactions, extract the VAT amount that appears before the '/' symbol
    # Example: "VAT Amt:89,925.00/11 = 8,175.00" should extract 89,925.00
    vat_amount = 0
    vat_match = re.search(r"VAT Amt:([0-9,]+\.?[0-9]*)/", description)
    if vat_match:
        # Parse VAT amount (value before the '/' symbol)
        vat_amount_str = vat_match.group(1)
        vat_amount = float(vat_amount_str.replace(",", ""))
        logger.info(
            f"Extracted VAT amount (before '/'): {vat_amount} from description"
        )
    else:
        # Fallback to older patterns - try without the division symbol
        vat_match = re.search(r"VAT Amt:([0-9,]+\.?[0-9]*)", description)
        if vat_match:
            # Parse VAT amount
            vat_amount_str = vat_match.group(1)
            vat_amount = float(vat_amount_str.replace(",", ""))
            logger.info(
                f"Extracted VAT amount: {vat_amount} from description using fallback pattern"
            )
        else:
            logger.warning(
                f"Failed to extract VAT amount from VCB POS transaction: {description}"
            )
            vat_amount = 0

    # 3. Search for MID in vcb_mids index
    from src.accounting.fast_search import search_vcb_mids

    # Log that we're searching for this MID
    logger.info(f"Searching for MID: {mid} in vcb_mids index")

    # First try exact search
    mid_records = search_vcb_mids(mid, field_name="mid", limit=1)

    # If not found, try searching as a prefix
    if not mid_records:
        logger.warning(
            f"MID not found with exact match: {mid}, trying partial match"
        )
        # Try partial match with regex search
        mid_records = search_vcb_mids(mid[:4], field_name="mid", limit=5)
        if mid_records:
            # Find the closest match if any
            logger.info(
                f"Found {len(mid_records)} potential matches with prefix: {mid[:4]}"
            )
            for rec in mid_records:
                logger.info(
                    f"Potential match: MID={rec.get('mid', 'N/A')}, Code={rec.get('code', 'N/A')}"
                )
        else:
            logger.warning(
                f"No MID matches found even with partial search: {mid}"
            )
            return [transaction_data]  # Return original if MID not found

    mid_record = mid_records[0]
    logger.info(f"Selected MID record: {mid_record}")

    # 4. Extract code and tid
    code = mid_record.get("code", "")
    tid = mid_record.get("tid", "")

    # Log the extracted values
    logger.info(f"Extracted code: '{code}', tid: '{tid}' from MID record")

    # Check if code is empty and try additional fields
    if not code:
        logger.warning(
            f"Empty code for MID {mid}, trying department_code field"
        )
        code = mid_record.get("department_code", "")
        if code:
            logger.info(f"Found code in department_code field: {code}")

    # 5. Process code (get part after "*", "_" or "-")
    if not code:
        logger.error(f"Empty code value received for MID: {mid}")
        processed_code = "KL"  # Default to KL if code is empty
    elif "-" in code:
        processed_code = code.split("-")[1]
        logger.info(
            f"Processed code with '-' delimiter: {code} -> {processed_code}"
        )
    elif "_" in code:
        processed_code = code.split("_")[1]
        logger.info(
            f"Processed code with '_' delimiter: {code} -> {processed_code}"
        )
    elif "*" in code:
        processed_code = code.split("*")[1]
        logger.info(
            f"Processed code with '*' delimiter: {code} -> {processed_code}"
        )
    else:
        processed_code = code
        logger.warning(f"Unexpected code format (missing delimiter): {code}")

    # 6. Create the main record with custom description
    main_record = transaction_data.copy()
    main_record["description"] = f"Thu tiền bán hàng của khách lẻ POS {tid}"
    main_record["original_description"] = description
    main_record["sequence"] = 1

    # 7. Create fee record with appropriate description and amount
    fee_record = transaction_data.copy()
    fee_record["description"] = f"Phí thu tiền bán hàng của khách lẻ POS {tid}"
    fee_record["original_description"] = description
    fee_record["sequence"] = 2

    # Set the fee amount using the extracted VAT amount
    if vat_amount > 0:
        if transaction_data.get("debit_amount", 0) > 0:
            fee_record["debit_amount"] = vat_amount
            fee_record["amount1"] = vat_amount
            fee_record["amount2"] = vat_amount
        else:
            fee_record["credit_amount"] = vat_amount
            fee_record["amount1"] = vat_amount
            fee_record["amount2"] = vat_amount
    else:
        logger.warning(
            "Using default fee calculation as VAT amount extraction failed"
        )
        # Default fee calculation as fallback
        if transaction_data.get("debit_amount", 0) > 0:
            fee_amount = (
                transaction_data["debit_amount"] * 0.001
            )  # 0.1% of transaction amount
            fee_record["debit_amount"] = fee_amount
            fee_record["amount1"] = fee_amount
            fee_record["amount2"] = fee_amount
        else:
            fee_amount = (
                transaction_data["credit_amount"] * 0.001
            )  # 0.1% of transaction amount
            fee_record["credit_amount"] = fee_amount
            fee_record["amount1"] = fee_amount
            fee_record["amount2"] = fee_amount

    # UPDATED: Set fee accounts according to new requirements
    # debit/credit account is 6417 for VISA or 6427 for other types, remaining account is 1311
    if card_type == "VISA":
        fee_account = "6417"  # Use 6417 for VISA transactions
    else:
        fee_account = "6427"  # Use 6427 for other card types (MASTER, etc.)

    # Assign accounts based on transaction direction
    # For fee records, 1311 should be in the credit account (Tk_Co), not debit account (Tk_No)
    if transaction_data.get("debit_amount", 0) > 0:
        # For debit transactions (money going out)
        fee_record["debit_account"] = fee_account  # Debit the fee account
        fee_record["credit_account"] = "1311"  # Credit account 1311
    else:
        # For credit transactions (money coming in)
        fee_record["debit_account"] = fee_account  # Debit the fee account
        fee_record["credit_account"] = "1311"  # Credit account 1311

    logger.info(
        f"Set fee record accounts: Dr={fee_record.get('debit_account')}, Cr={fee_record.get('credit_account')}"
    )

    # 8. Use the processed code to find counterparty
    from src.accounting.fast_search import search_exact_counterparties

    logger.info(
        f"Searching for counterparty with processed code: {processed_code}"
    )
    counterparty_results = search_exact_counterparties(
        processed_code, field_name="code", limit=1
    )

    if not counterparty_results:
        logger.warning(
            f"No counterparty found for processed code: {processed_code}"
        )
        # Try searching with a more flexible approach
        logger.info(
            f"Trying partial search for counterparty code: {processed_code}"
        )
        counterparty_results = search_exact_counterparties(
            processed_code, field_name="code", limit=5
        )

        if counterparty_results:
            logger.info(
                f"Found {len(counterparty_results)} potential counterparties with partial search"
            )
            for cp in counterparty_results:
                logger.info(
                    f"Potential counterparty: Code={cp.get('code', 'N/A')}, Name={cp.get('name', 'N/A')}"
                )
        else:
            logger.warning(
                f"No counterparties found even with partial search for: {processed_code}"
            )

    if counterparty_results:
        counterparty = counterparty_results[0]
        # Set counterparty information to both records
        for record in [main_record, fee_record]:
            record["counterparty_code"] = counterparty.get("code", "")
            record["counterparty_name"] = counterparty.get("name", "")
            record["address"] = counterparty.get("address", "")

    logger.info(
        f"Processed {card_type} transaction successfully with MID {mid}, TID {tid}"
    )
    return [main_record, fee_record]


def process_vcb_transfer_transaction(
    description: str, transaction_data: dict
) -> List[dict]:
    """
    Process VCB transfer transactions (IBVCB).

    For transactions like:
    "IBVCB.1706250930138002.034244.IBTC.Chuyen tien tu TK VCB (6868) Sang Tam qua TK BIDV (7655) Sang Tam"

    Creates:
    - Main record: "Chuyển tiền từ TK VCB (6868) Sáng Tâm qua TK BIDV (7655) Sáng Tâm"
    - Fee record: "Phí chuyển tiền ST: {amount of main record}"

    Args:
        description (str): The transaction description text
        transaction_data (dict): The original transaction data

    Returns:
        list: List of processed transaction records (main and fee)
    """
    logger.info(f"Processing VCB transfer transaction: {description}")

    # Check if we have a bank account from the current file
    bank_account = transaction_data.get("bank_account", "1121114")  # Default if not provided

    # Create main record
    main_record = transaction_data.copy()
    main_record["sequence"] = 1
    main_record["original_description"] = description

    # Format the description for Sang Tam transfers
    if "SANG TAM" in description.upper():
        # Pattern for transfers between accounts
        # More flexible pattern that works with both parentheses and space formats
        pattern = r"tu\s+tk\s+([A-Z]+).*?(\d+).*?sang\s+tam.*?qua\s+tk\s+([A-Z]+).*?(\d+).*?sang\s+tam"
        match = re.search(pattern, description, re.IGNORECASE)
        
        if match:
            from_bank = match.group(1)
            from_account = match.group(2)
            to_bank = match.group(3)
            to_account = match.group(4)
            
            formatted_desc = f"Chuyển tiền từ TK {from_bank} ({from_account}) Sáng Tâm qua TK {to_bank} ({to_account}) Sáng Tâm"
            main_record["description"] = formatted_desc
        else:
            main_record["description"] = description
    else:
        main_record["description"] = description

    # According to business requirements:
    # For VCB file, if statement like "IBVCB.1706250930138002.034244.IBTC.Chuyen tien tu TK VCB (6868) Sang Tam qua TK BIDV (7655) Sang Tam", 
    # please the debit/credit of main record is 1131 and the bank of current file
    # Example: 1131 and 1121120
    
    # Set appropriate accounts for main record based on business requirements
    if main_record.get("debit_amount", 0) > 0:
        # Debit transaction: money going out
        # Debit account should be 1131, credit account should be the bank account
        main_record["debit_account"] = "1131"
        main_record["credit_account"] = bank_account
    else:
        # Credit transaction: money coming in
        # Debit account should be the bank account, credit account should be 1131
        main_record["debit_account"] = bank_account
        main_record["credit_account"] = "1131"

    # Create fee record
    fee_record = transaction_data.copy()
    fee_record["sequence"] = 2
    fee_record["original_description"] = description

    # Set fee description based on main transaction amount
    amount = main_record.get("debit_amount", 0)
    if amount == 0:
        amount = main_record.get("credit_amount", 0)

    fee_record["description"] = f"Phí chuyển tiền ST: {amount:,.0f}"

    # Set fee amount (try to extract from description or use default)
    vat_match = re.search(r"VAT Amt:([0-9,]+\\.[0-9]+)", description)
    if vat_match:
        vat_amount_str = vat_match.group(1)
        vat_amount = float(vat_amount_str.replace(",", ""))
        logger.info(f"Extracted VAT amount: {vat_amount} from description")

        if transaction_data.get("debit_amount", 0) > 0:
            fee_record["debit_amount"] = vat_amount
            fee_record["amount1"] = vat_amount
            fee_record["amount2"] = vat_amount
        else:
            fee_record["credit_amount"] = vat_amount
            fee_record["amount1"] = vat_amount
            fee_record["amount2"] = vat_amount
    else:
        # Default fee calculation if VAT amount not found
        if transaction_data.get("debit_amount", 0) > 0:
            fee_amount = (
                transaction_data["debit_amount"] * 0.001
            )  # 0.1% of transaction amount
            fee_record["debit_amount"] = fee_amount
            fee_record["amount1"] = fee_amount
            fee_record["amount2"] = fee_amount
        else:
            fee_amount = (
                transaction_data["credit_amount"] * 0.001
            )  # 0.1% of transaction amount
            fee_record["credit_amount"] = fee_amount
            fee_record["amount1"] = fee_amount
            fee_record["amount2"] = fee_amount

    # According to business requirements:
    # For VCB file, if statement like "IBVCB...", 
    # please the debit/credit of fee record is 6427 and the bank of current file
    # Example: 6427 and 1121120
    
    # Set appropriate accounts for fee record based on business requirements
    if transaction_data.get("debit_amount", 0) > 0:
        # Debit transaction: money going out (fee payment)
        # Debit account should be 6427, credit account should be the bank account
        fee_record["debit_account"] = "6427"
        fee_record["credit_account"] = bank_account
    else:
        # Credit transaction: money coming in (fee received - less common)
        # Debit account should be the bank account, credit account should be 6427
        fee_record["debit_account"] = bank_account
        fee_record["credit_account"] = "6427"

    logger.info(
        f"Set transfer main accounts: Dr={main_record.get('debit_account')}, Cr={main_record.get('credit_account')}"
    )
    logger.info(
        f"Set transfer fee accounts: Dr={fee_record.get('debit_account')}, Cr={fee_record.get('credit_account')}"
    )

    # According to business requirements:
    # For VCB file, if statement like "IBVCB...", 
    # with statement the code, name and address is {"31754", "Công Ty TNHH Sáng Tâm","32-34 Đường 74, Phường 10, Quận 6, Tp. Hồ Chí Minh"}
    # (Sáng Tâm company info)
    
    # Set fee counterparty to Sáng Tâm company
    fee_record["counterparty_code"] = "31754"
    fee_record["counterparty_name"] = "Công Ty TNHH Sáng Tâm"
    fee_record["address"] = "32-34 Đường 74, Phường 10, Quận 6, Tp. Hồ Chí Minh"

    return [main_record, fee_record]


def process_vcb_interest_transaction(
    description: str, transaction_data: dict
) -> List[dict]:
    """
    Process VCB interest payment transactions (INTEREST PAYMENT).

    For transactions like: "INTEREST PAYMENT"

    Creates a single record with custom description:
    "Lãi nhập vốn NH Vietcombank"

    Args:
        description (str): The transaction description text
        transaction_data (dict): The original transaction data

    Returns:
        list: List containing one transaction record
    """
    logger.info(f"Processing VCB interest transaction: {description}")

    # Create a single record with custom description
    record = transaction_data.copy()
    record["description"] = "Lãi nhập vốn NH Vietcombank"
    record["original_description"] = description
    record["sequence"] = 1

    # Set counterparty to bank
    record["counterparty_code"] = "VCB"
    record["counterparty_name"] = "NGÂN HÀNG TMCP NGOẠI THƯƠNG VIỆT NAM"
    record["address"] = "198 Trần Quang Khải, Hoàn Kiếm, Hà Nội"

    # According to business requirements:
    # For VCB file, if statement is "INTEREST PAYMENT", 
    # please the debit/credit account is the bank of current file and 5154
    # Example: 1121120 and 5154
    
    # Check if we have a bank account from the current file
    bank_account = transaction_data.get("bank_account", "1121114")  # Default if not provided
    
    # Set appropriate accounts based on business requirements
    if record.get("credit_amount", 0) > 0:
        # Credit transaction: money coming in (interest payment received)
        # Debit account should be the bank account, credit account should be 5154
        record["debit_account"] = bank_account
        record["credit_account"] = "5154"
    else:
        # Debit transaction: money going out (interest payment made)
        # Debit account should be 5154, credit account should be the bank account
        record["debit_account"] = "5154"
        record["credit_account"] = bank_account

    return [record]


def process_vcb_fee_transaction(
    description: str, transaction_data: dict
) -> List[dict]:
    """
    Process VCB account management fee transactions (THU PHI QLTK).

    For transactions like: "THU PHI QLTK TO CHUC-VND"

    Creates a single record with custom description:
    "Phí quản lý tài khoản NH Vietcombank (VND)"

    Args:
        description (str): The transaction description text
        transaction_data (dict): The original transaction data

    Returns:
        list: List containing one transaction record
    """
    logger.info(f"Processing VCB account fee transaction: {description}")

    # Create a single record with custom description
    record = transaction_data.copy()
    record["description"] = "Phí quản lý tài khoản NH Vietcombank (VND)"
    record["original_description"] = description
    record["sequence"] = 1

    # Set counterparty to bank
    record["counterparty_code"] = "VCB"
    record["counterparty_name"] = "NGÂN HÀNG TMCP NGOẠI THƯƠNG VIỆT NAM"
    record["address"] = "198 Trần Quang Khải, Hoàn Kiếm, Hà Nội"

    # According to business requirements:
    # For VCB file, if statement is "THU PHI QLTK TO CHUC-VND", 
    # please the debit/credit account is 6427 and the bank of current file
    # Example: 6427 and 1121120
    
    # Check if we have a bank account from the current file
    bank_account = transaction_data.get("bank_account", "1121114")  # Default if not provided
    
    # Set appropriate accounts based on business requirements
    if record.get("debit_amount", 0) > 0:
        # Debit transaction: money going out (fee payment)
        # Debit account should be 6427, credit account should be the bank account
        record["debit_account"] = "6427"
        record["credit_account"] = bank_account
    else:
        # Credit transaction: money coming in (fee received - less common)
        # Debit account should be the bank account, credit account should be 6427
        record["debit_account"] = bank_account
        record["credit_account"] = "6427"

    return [record]


def process_generic_vcb_transaction(
    description: str, transaction_data: dict
) -> List[dict]:
    """
    Process generic VCB transactions that don't match specific patterns.
    All VCB transactions should create both main and fee records (except interest and account fee).

    Args:
        description (str): The transaction description text
        transaction_data (dict): The original transaction data

    Returns:
        list: List of processed transaction records (main and fee)
    """
    logger.info(f"Processing generic VCB transaction: {description}")

    # Check for special cases first
    if (
        "T/t T/ung the MASTER" in description
        or "T/t T/ung the VISA" in description
    ):
        return process_vcb_pos_transaction(description, transaction_data)
    elif "IBVCB" in description:
        return process_vcb_transfer_transaction(description, transaction_data)
    elif "INTEREST PAYMENT" in description:
        return process_vcb_interest_transaction(description, transaction_data)
    elif "THU PHI QLTK TO CHUC-VND" in description:
        return process_vcb_fee_transaction(description, transaction_data)

    # Create a copy of the original transaction data for the main record
    main_record = transaction_data.copy()
    main_record["sequence"] = 1
    main_record["original_description"] = description

    # Create a fee record
    fee_record = transaction_data.copy()
    fee_record["sequence"] = 2
    fee_record["original_description"] = description

    # Set fee description
    fee_record["description"] = "Phí giao dịch ngân hàng"

    # Try to extract VAT amount for fee record
    # UPDATED: Try to extract VAT amount before the division symbol first
    vat_match = re.search(r"VAT Amt:([0-9,]+\.?[0-9]*)/", description)
    if vat_match:
        # Parse VAT amount (value before the '/' symbol)
        vat_amount_str = vat_match.group(1)
        vat_amount = float(vat_amount_str.replace(",", ""))
        logger.info(
            f"Extracted VAT amount (before '/'): {vat_amount} from description"
        )
    else:
        # If not found, try the simpler format (VAT Amt:89,925.00)
        vat_match = re.search(r"VAT Amt:([0-9,]+\.?[0-9]*)", description)

        if vat_match:
            # Parse VAT amount
            vat_amount_str = vat_match.group(1)
            vat_amount = float(vat_amount_str.replace(",", ""))
            logger.info(
                f"Extracted VAT amount: {vat_amount} from description using fallback pattern"
            )
        else:
            # If no VAT amount found, use a small percentage of the transaction amount
            logger.warning(
                "No VAT amount found in description, using calculated fee"
            )
            if transaction_data.get("debit_amount", 0) > 0:
                vat_amount = (
                    transaction_data["debit_amount"] * 0.001
                )  # 0.1% of transaction amount
            else:
                vat_amount = (
                    transaction_data["credit_amount"] * 0.001
                )  # 0.1% of transaction amount

    # Set fee amount from VAT amount
    if transaction_data.get("debit_amount", 0) > 0:
        fee_record["debit_amount"] = vat_amount
        fee_record["amount1"] = vat_amount
        fee_record["amount2"] = vat_amount
    else:
        fee_record["credit_amount"] = vat_amount
        fee_record["amount1"] = vat_amount
        fee_record["amount2"] = vat_amount

    # UPDATED: Set fee accounts according to new requirements
    # For fee records, use 6417 for VISA or 6427 for other types, remaining account is 1311
    fee_account = "6427"  # Default for generic transactions

    # Check for VISA-related transactions
    if "VISA" in description:
        fee_account = "6417"  # Use 6417 for VISA transactions

    # Assign accounts based on transaction direction
    # For fee records, 1311 should be in the credit account (Tk_Co), not debit account (Tk_No)
    if transaction_data.get("debit_amount", 0) > 0:
        # For debit transactions (money going out)
        fee_record["debit_account"] = fee_account  # Debit the fee account
        fee_record["credit_account"] = "1311"  # Credit account 1311
    else:
        # For credit transactions (money coming in)
        fee_record["debit_account"] = fee_account  # Debit the fee account
        fee_record["credit_account"] = "1311"  # Credit account 1311

    logger.info(
        f"Set generic transaction fee accounts: Dr={fee_record.get('debit_account')}, Cr={fee_record.get('credit_account')}"
    )

    # Set fee counterparty to bank
    fee_record["counterparty_code"] = "VCB"
    fee_record["counterparty_name"] = "NGÂN HÀNG TMCP NGOẠI THƯƠNG VIỆT NAM"
    fee_record["address"] = "198 Trần Quang Khải, Hoàn Kiếm, Hà Nội"

    return [main_record, fee_record]
