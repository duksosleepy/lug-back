#!/usr/bin/env python3
"""
Advanced Features for BIDV to Saoke Converter

This module provides advanced features for the BIDV to Saoke Converter:
1. extract_pos_codes - Extract multiple POS codes from a description
2. validate_saoke_entry - Validate saoke entries for accounting compliance
3. export_to_accounting_system - Export saoke entries to various accounting systems
4. generate_reconciliation_report - Reconcile bank statements with accounting records
"""

import json
import re
from datetime import datetime
from typing import Dict, List, Optional, Tuple, Union

import limbo  # Using Limbo for database access
import pandas as pd

from src.accounting.bank_statement_processor import (
    BankStatementProcessor,
    SaokeEntry,
)
from src.util.logging import get_logger

logger = get_logger(__name__)


def extract_pos_codes(
    description: str, pos_patterns: Optional[List[str]] = None
) -> List[str]:
    """
    Extract multiple POS terminal codes from description

    Args:
        description: Transaction description to analyze
        pos_patterns: Optional list of regex patterns for POS codes
                      (defaults to common patterns if None)

    Returns:
        List of extracted POS codes
    """
    if pos_patterns is None:
        pos_patterns = [
            r"POS\s+(\d{7,8})",  # POS followed by 7-8 digits
            r"POS(\d{7,8})",  # POS directly followed by digits
            r"POS[\s\-:_]+(\d{7,8})",  # POS with various separators
            r"(\d{7,8})(?=.*POS)",  # 7-8 digits followed by POS elsewhere
            r"TIP\s+(\d{7,8})",  # TIP followed by 7-8 digits (alternative to POS)
        ]

    pos_codes = []

    # Apply each pattern to find all matches
    for pattern in pos_patterns:
        matches = re.finditer(pattern, description, re.IGNORECASE)
        for match in matches:
            pos_code = match.group(1)
            if pos_code not in pos_codes:
                pos_codes.append(pos_code)

    # Sort and return unique codes
    return sorted(pos_codes)


def validate_saoke_entry(
    entry: SaokeEntry, db_path: str = "banking_enterprise.db"
) -> Tuple[bool, List[str]]:
    """
    Validate a saoke entry for accounting compliance

    Args:
        entry: SaokeEntry to validate
        db_path: Path to database for validation against reference data

    Returns:
        Tuple of (is_valid, list_of_errors)
    """
    errors = []

    # Check required fields
    required_fields = {
        "document_type": "Document type",
        "date": "Date",
        "document_number": "Document number",
        "counterparty_code": "Counterparty code",
        "description": "Description",
        "amount1": "Amount",
        "debit_account": "Debit account",
        "credit_account": "Credit account",
    }

    for field, field_name in required_fields.items():
        value = getattr(entry, field)
        if not value:
            errors.append(f"Missing required field: {field_name}")

    # Skip further validation if essential fields are missing
    if errors:
        return False, errors

    # Check amount is non-zero
    if entry.amount1 <= 0:
        errors.append(f"Transaction amount must be positive: {entry.amount1}")

    # Validate document number format (e.g., BC03/001)
    doc_pattern = r"^(BC|BN)\d{2}/\d{3}$"
    if not re.match(doc_pattern, entry.document_number):
        errors.append(
            f"Invalid document number format: {entry.document_number}"
        )

    # Validate date format (DD/M/YYYY)
    date_pattern = r"^\d{1,2}/\d{1,2}/\d{4}$"
    if not re.match(date_pattern, entry.date):
        errors.append(f"Invalid date format: {entry.date}")

    # Connect to database for reference data validation
    try:
        conn = limbo.connect(db_path)
        cursor = conn.cursor()

        # Validate debit account exists
        cursor.execute(
            "SELECT code FROM accounts WHERE code = ?", (entry.debit_account,)
        )
        if not cursor.fetchone():
            errors.append(f"Debit account not found: {entry.debit_account}")

        # Validate credit account exists
        cursor.execute(
            "SELECT code FROM accounts WHERE code = ?", (entry.credit_account,)
        )
        if not cursor.fetchone():
            errors.append(f"Credit account not found: {entry.credit_account}")

        # Validate counterparty exists
        cursor.execute(
            "SELECT code FROM counterparties WHERE code = ?",
            (entry.counterparty_code,),
        )
        if not cursor.fetchone():
            errors.append(f"Counterparty not found: {entry.counterparty_code}")

        # Validate department if provided
        if entry.department:
            cursor.execute(
                "SELECT code FROM departments WHERE code = ?",
                (entry.department,),
            )
            if not cursor.fetchone():
                errors.append(f"Department not found: {entry.department}")

        # Validate cost code if provided
        if entry.cost_code:
            cursor.execute(
                "SELECT code FROM cost_categories WHERE code = ?",
                (entry.cost_code,),
            )
            if not cursor.fetchone():
                errors.append(f"Cost code not found: {entry.cost_code}")

        conn.close()
    except Exception as e:
        errors.append(f"Database validation error: {e}")

    # Return validation result
    return len(errors) == 0, errors


def export_to_accounting_system(
    entries: List[Union[SaokeEntry, Dict]],
    system_type: str = "excel",
    output_file: Optional[str] = None,
    include_metadata: bool = False,
) -> str:
    """
    Export saoke entries to various accounting systems

    Args:
        entries: List of SaokeEntry objects or dictionaries
        system_type: Type of export format ("excel", "csv", "json", "xml")
        output_file: Output file path (generated if None)
        include_metadata: Whether to include metadata fields

    Returns:
        Path to the exported file
    """
    # Convert entries to dictionaries if they are SaokeEntry objects
    entry_dicts = []
    for entry in entries:
        if isinstance(entry, SaokeEntry):
            entry_dict = entry.as_dict()
        else:
            entry_dict = entry.copy()

        # Remove raw_transaction field for export
        if "raw_transaction" in entry_dict:
            del entry_dict["raw_transaction"]

        # Optionally remove other metadata fields
        if not include_metadata:
            metadata_fields = ["original_description", "transaction_type"]
            for field in metadata_fields:
                if field in entry_dict:
                    del entry_dict[field]

        entry_dicts.append(entry_dict)

    # Generate default output filename if not specified
    if output_file is None:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = f"saoke_export_{timestamp}.{system_type}"

    # Export based on system type
    if system_type == "excel":
        df = pd.DataFrame(entry_dicts)
        df.to_excel(output_file, index=False, engine="openpyxl")
        return output_file

    elif system_type == "csv":
        df = pd.DataFrame(entry_dicts)
        df.to_csv(output_file, index=False)
        return output_file

    elif system_type == "json":
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(entry_dicts, f, ensure_ascii=False, indent=2)
        return output_file

    elif system_type == "xml":
        # Create XML structure
        xml_lines = ['<?xml version="1.0" encoding="UTF-8"?>', "<SaokeEntries>"]

        for entry in entry_dicts:
            xml_lines.append("  <Entry>")
            for key, value in entry.items():
                if value is not None:  # Skip None values
                    # Ensure XML-safe content
                    if isinstance(value, str):
                        value = (
                            value.replace("&", "&amp;")
                            .replace("<", "&lt;")
                            .replace(">", "&gt;")
                        )
                    xml_lines.append(f"    <{key}>{value}</{key}>")
            xml_lines.append("  </Entry>")

        xml_lines.append("</SaokeEntries>")

        with open(output_file, "w", encoding="utf-8") as f:
            f.write("\n".join(xml_lines))

        return output_file

    else:
        raise ValueError(f"Unsupported export format: {system_type}")


def generate_reconciliation_report(
    bank_statement_file: str,
    accounting_file: str,
    output_file: Optional[str] = None,
    tolerance: float = 0.01,
) -> Dict:
    """
    Generate reconciliation report between bank statement and accounting records

    Args:
        bank_statement_file: Path to bank statement file
        accounting_file: Path to accounting records file
        output_file: Optional path to save report (Excel format)
        tolerance: Tolerance for amount differences

    Returns:
        Dictionary with reconciliation report details
    """
    # Process bank statement
    processor = BankStatementProcessor()
    processor.connect()
    bank_df = processor.process_to_saoke(bank_statement_file)
    processor.close()

    # Load accounting records
    if accounting_file.endswith(".xlsx") or accounting_file.endswith(".xls"):
        accounting_df = pd.read_excel(accounting_file)
    elif accounting_file.endswith(".csv"):
        accounting_df = pd.read_csv(accounting_file)
    else:
        raise ValueError(
            f"Unsupported accounting file format: {accounting_file}"
        )

    # Normalize column names
    bank_df.columns = [col.lower() for col in bank_df.columns]
    accounting_df.columns = [col.lower() for col in accounting_df.columns]

    # Ensure required columns exist
    required_bank_cols = ["date", "amount1", "description"]
    required_acc_cols = ["date", "amount", "description"]

    for col in required_bank_cols:
        if col not in bank_df.columns:
            raise ValueError(f"Bank statement missing required column: {col}")

    for col in required_acc_cols:
        if col not in accounting_df.columns:
            raise ValueError(f"Accounting file missing required column: {col}")

    # Convert amount columns to numeric
    bank_df["amount1"] = pd.to_numeric(bank_df["amount1"], errors="coerce")
    accounting_df["amount"] = pd.to_numeric(
        accounting_df["amount"], errors="coerce"
    )

    # Ensure date columns are datetime
    bank_df["date"] = pd.to_datetime(
        bank_df["date"], format="%d/%m/%Y", errors="coerce"
    )
    accounting_df["date"] = pd.to_datetime(
        accounting_df["date"], errors="coerce"
    )

    # Create matching key columns for comparison
    bank_df["date_key"] = bank_df["date"].dt.strftime("%Y-%m-%d")
    accounting_df["date_key"] = accounting_df["date"].dt.strftime("%Y-%m-%d")

    # Initialize report data
    matched = []
    unmatched_bank = []
    unmatched_accounting = []

    # Match transactions
    bank_matched_indices = set()
    accounting_matched_indices = set()

    # First pass: exact amount and date matches
    for bank_idx, bank_row in bank_df.iterrows():
        for acc_idx, acc_row in accounting_df.iterrows():
            if (
                acc_idx not in accounting_matched_indices
                and bank_idx not in bank_matched_indices
                and abs(bank_row["amount1"] - acc_row["amount"]) < tolerance
                and bank_row["date_key"] == acc_row["date_key"]
            ):
                matched.append(
                    {
                        "bank": bank_row.to_dict(),
                        "accounting": acc_row.to_dict(),
                        "match_type": "exact",
                    }
                )

                bank_matched_indices.add(bank_idx)
                accounting_matched_indices.add(acc_idx)
                break

    # Second pass: fuzzy matching (same amount, date within 3 days)
    for bank_idx, bank_row in bank_df.iterrows():
        if bank_idx in bank_matched_indices:
            continue

        for acc_idx, acc_row in accounting_df.iterrows():
            if acc_idx in accounting_matched_indices:
                continue

            bank_date = pd.to_datetime(bank_row["date_key"])
            acc_date = pd.to_datetime(acc_row["date_key"])
            date_diff = abs((bank_date - acc_date).days)

            if (
                abs(bank_row["amount1"] - acc_row["amount"]) < tolerance
                and date_diff <= 3
            ):
                matched.append(
                    {
                        "bank": bank_row.to_dict(),
                        "accounting": acc_row.to_dict(),
                        "match_type": "fuzzy",
                        "date_diff": date_diff,
                    }
                )

                bank_matched_indices.add(bank_idx)
                accounting_matched_indices.add(acc_idx)
                break

    # Collect unmatched transactions
    for bank_idx, bank_row in bank_df.iterrows():
        if bank_idx not in bank_matched_indices:
            unmatched_bank.append(bank_row.to_dict())

    for acc_idx, acc_row in accounting_df.iterrows():
        if acc_idx not in accounting_matched_indices:
            unmatched_accounting.append(acc_row.to_dict())

    # Generate summary
    bank_total = bank_df["amount1"].sum()
    accounting_total = accounting_df["amount"].sum()

    report = {
        "summary": {
            "bank_transactions": len(bank_df),
            "accounting_transactions": len(accounting_df),
            "matched_count": len(matched),
            "unmatched_bank_count": len(unmatched_bank),
            "unmatched_accounting_count": len(unmatched_accounting),
            "bank_total": bank_total,
            "accounting_total": accounting_total,
            "difference": bank_total - accounting_total,
            "match_percentage": round(len(matched) / len(bank_df) * 100, 2)
            if len(bank_df) > 0
            else 0,
        },
        "matched": matched,
        "unmatched_bank": unmatched_bank,
        "unmatched_accounting": unmatched_accounting,
    }

    # Export to Excel if output file specified
    if output_file:
        with pd.ExcelWriter(output_file) as writer:
            # Summary sheet
            summary_df = pd.DataFrame([report["summary"]])
            summary_df.to_excel(writer, sheet_name="Summary", index=False)

            # Matched transactions
            if matched:
                matched_bank = [m["bank"] for m in matched]
                matched_acc = [m["accounting"] for m in matched]

                # Create a combined DataFrame
                combined_matched = []
                for i, match in enumerate(matched):
                    row = {
                        "match_type": match.get("match_type", "exact"),
                        "bank_date": match["bank"].get("date"),
                        "bank_amount": match["bank"].get("amount1"),
                        "bank_description": match["bank"].get("description"),
                        "acc_date": match["accounting"].get("date"),
                        "acc_amount": match["accounting"].get("amount"),
                        "acc_description": match["accounting"].get(
                            "description"
                        ),
                    }
                    combined_matched.append(row)

                pd.DataFrame(combined_matched).to_excel(
                    writer, sheet_name="Matched", index=False
                )

            # Unmatched bank transactions
            if unmatched_bank:
                pd.DataFrame(unmatched_bank).to_excel(
                    writer, sheet_name="Unmatched_Bank", index=False
                )

            # Unmatched accounting transactions
            if unmatched_accounting:
                pd.DataFrame(unmatched_accounting).to_excel(
                    writer, sheet_name="Unmatched_Accounting", index=False
                )

    return report


if __name__ == "__main__":
    # Example usage

    # Test extract_pos_codes
    test_description = (
        "Payment TT POS 14100333 and POS14100334 for sales on 01/03/2025"
    )
    pos_codes = extract_pos_codes(test_description)
    print(f"Extracted POS codes: {pos_codes}")

    # Test validation
    # (Requires SaokeEntry object and database connection)

    # Test export
    sample_entries = [
        {
            "document_type": "BC",
            "date": "01/3/2025",
            "document_number": "BC03/001",
            "currency": "VND",
            "exchange_rate": 1.0,
            "counterparty_code": "KL-GOBARIA1",
            "counterparty_name": "Khách Lẻ Không Lấy Hóa Đơn",
            "description": "Thu tiền bán hàng khách lẻ (POS 14100333)",
            "amount1": 888000,
            "amount2": 888000,
            "debit_account": "1121114",
            "credit_account": "1311",
        }
    ]

    # Export to different formats
    for format_type in ["excel", "csv", "json", "xml"]:
        output_file = export_to_accounting_system(sample_entries, format_type)
        print(f"Exported to {format_type}: {output_file}")

    # Test reconciliation
    # (Requires actual bank statement and accounting files)
