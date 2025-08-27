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
    # First try to extract VAT amount after the equals sign (format: VAT Amt:89,925.00/11 = 8,175.00)
    vat_match = re.search(r"VAT Amt:.*?/11\s*=\s*([0-9,]+\.[0-9]+)", description)
    if not vat_match:
        # If not found, try the simpler format (VAT Amt:89,925.00)
        vat_match = re.search(r"VAT Amt:([0-9,]+\.[0-9]+)", description)
        if not vat_match:
            logger.warning(
                f"Failed to extract VAT amount from VCB POS transaction: {description}"
            )
            vat_amount = 0
        else:
            # Parse VAT amount
            vat_amount_str = vat_match.group(1)
            vat_amount = float(vat_amount_str.replace(",", ""))
    else:
        # Parse VAT amount (value after "=" sign)
        vat_amount_str = vat_match.group(1)
        vat_amount = float(vat_amount_str.replace(",", ""))

    # 3. Search for MID in vcb_mids index
    from src.accounting.fast_search import search_vcb_mids

    mid_records = search_vcb_mids(mid, field_name="mid", limit=1)
    if not mid_records:
        logger.warning(f"MID not found in vcb_mids index: {mid}")
        return [transaction_data]  # Return original if MID not found

    mid_record = mid_records[0]

    # 4. Extract code and tid
    code = mid_record.get("code", "")
    tid = mid_record.get("tid", "")

    # 5. Process code (get part after "_" or "-")
    if "-" in code:
        processed_code = code.split("-")[1]
    elif "_" in code:
        processed_code = code.split("_")[1]
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

    # Set fee amount from VAT amount
    if vat_amount > 0:
        if transaction_data.get("debit_amount", 0) > 0:
            fee_record["debit_amount"] = vat_amount
            fee_record["amount1"] = vat_amount
            fee_record["amount2"] = vat_amount
        else:
            fee_record["credit_amount"] = vat_amount
            fee_record["amount1"] = vat_amount
            fee_record["amount2"] = vat_amount

    # Set fee account to 6417
    if main_record.get("debit_amount", 0) > 0:
        fee_record["credit_account"] = "6417"
    else:
        fee_record["debit_account"] = "6417"

    # 8. Use the processed code to find counterparty
    from src.accounting.fast_search import search_counterparties

    counterparty_results = search_counterparties(
        processed_code, field_name="code", limit=1
    )

    if counterparty_results:
        counterparty = counterparty_results[0]
        # Set counterparty information to both records
        for record in [main_record, fee_record]:
            record["counterparty_code"] = counterparty.get("code", "")
            record["counterparty_name"] = counterparty.get("name", "")
            record["address"] = counterparty.get("address", "")

    logger.info(f"Processed {card_type} transaction successfully with MID {mid}, TID {tid}")
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

    # Create main record
    main_record = transaction_data.copy()
    main_record["sequence"] = 1
    main_record["original_description"] = description

    # Format the description for Sang Tam transfers
    if "SANG TAM" in description.upper():
        # Pattern for transfers between accounts
        pattern = r"tu\s+tk\s+(\w+)\s*\((\d+)\)\s+sang\s+tam.*?qua\s+tk\s+(\w+)\s*\((\d+)\)\s+sang\s+tam"
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
    vat_match = re.search(r"VAT Amt:([0-9,]+\.[0-9]+)", description)
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

    # Set fee account to 6427 for transfers
    if main_record.get("debit_amount", 0) > 0:
        fee_record["credit_account"] = "6427"
    else:
        fee_record["debit_account"] = "6427"

    # Set fee counterparty to bank
    fee_record["counterparty_code"] = "VCB"
    fee_record["counterparty_name"] = "NGÂN HÀNG TMCP NGOẠI THƯƠNG VIỆT NAM"
    fee_record["address"] = "198 Trần Quang Khải, Hoàn Kiếm, Hà Nội"

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

    # Set appropriate accounts (assume interest income is credited)
    if record.get("credit_amount", 0) > 0:
        record["credit_account"] = "811"  # Interest income account
        record["debit_account"] = "1121114"  # Default bank account
    else:
        record["debit_account"] = "811"  # Interest income account
        record["credit_account"] = "1121114"  # Default bank account

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

    # Set appropriate accounts (assume fee is debited)
    if record.get("debit_amount", 0) > 0:
        record["debit_account"] = "6417"  # Fee expense account
        record["credit_account"] = "1121114"  # Default bank account
    else:
        record["credit_account"] = "6417"  # Fee expense account
        record["debit_account"] = "1121114"  # Default bank account

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
    if "T/t T/ung the MASTER" in description or "T/t T/ung the VISA" in description:
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
    # First try to extract VAT amount after the equals sign (format: VAT Amt:89,925.00/11 = 8,175.00)
    vat_match = re.search(r"VAT Amt:.*?/11\s*=\s*([0-9,]+\.[0-9]+)", description)
    if not vat_match:
        # If not found, try the simpler format (VAT Amt:89,925.00)
        vat_match = re.search(r"VAT Amt:([0-9,]+\.[0-9]+)", description)
    
    if vat_match:
        # Parse VAT amount
        vat_amount_str = vat_match.group(1)
        vat_amount = float(vat_amount_str.replace(",", ""))
        logger.info(f"Extracted VAT amount: {vat_amount} from description")

        # Set fee amount from VAT amount
        if transaction_data.get("debit_amount", 0) > 0:
            fee_record["debit_amount"] = vat_amount
            fee_record["amount1"] = vat_amount
            fee_record["amount2"] = vat_amount
        else:
            fee_record["credit_amount"] = vat_amount
            fee_record["amount1"] = vat_amount
            fee_record["amount2"] = vat_amount
    else:
        # If no VAT amount found, use a small percentage of the transaction amount
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

    # Set fee account based on transaction type
    if (
        "CHUYEN TIEN" in description.upper()
        or "TRANSFER" in description.upper()
        or "IBVCB" in description
    ):
        # Use 6427 for transfers
        if main_record.get("debit_amount", 0) > 0:
            fee_record["credit_account"] = "6427"
        else:
            fee_record["debit_account"] = "6427"
    else:
        # Use 6417 for other transactions
        if main_record.get("debit_amount", 0) > 0:
            fee_record["credit_account"] = "6417"
        else:
            fee_record["debit_account"] = "6417"

    # Set fee counterparty to bank
    fee_record["counterparty_code"] = "VCB"
    fee_record["counterparty_name"] = "NGÂN HÀNG TMCP NGOẠI THƯƠNG VIỆT NAM"
    fee_record["address"] = "198 Trần Quang Khải, Hoàn Kiếm, Hà Nội"

    return [main_record, fee_record]
