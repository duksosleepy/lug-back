#!/usr/bin/env python3
"""
Accounting API Module

This module provides FastAPI endpoints for processing bank statements and
converting them to accounting-friendly formats.
"""

import base64
import io
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional, Tuple

import pandas as pd
from bs4 import BeautifulSoup
from fastapi import APIRouter, HTTPException, UploadFile
from loguru import logger
from pydantic import BaseModel

from src.accounting.bank_statement_reader import BankStatementReader
from src.accounting.integrated_bank_processor import IntegratedBankProcessor
from src.util import validate_excel_file

# Removed pending_fee_entries - now handled by process_to_saoke method

# Create router for accounting endpoints
router = APIRouter(prefix="/accounting", tags=["accounting"])

# Initialize reader and processor
reader = BankStatementReader()
processor = IntegratedBankProcessor()


# Removed create_basic_saoke_entry function - no longer needed with process_to_saoke method


class AccountingResponse(BaseModel):
    """Response model for accounting endpoints"""

    success: bool
    message: str
    result_file: Optional[str] = None  # Base64 encoded file
    filename: Optional[str] = None


def read_excel_with_fallback(input_buffer):
    """Read Excel file with multiple engine fallback for compatibility, including HTML format"""
    engines = ["calamine", "openpyxl", "xlrd"]

    # First try standard Excel engines
    for engine in engines:
        try:
            input_buffer.seek(0)  # Reset buffer position
            logger.info(f"Attempting to read Excel with engine: {engine}")
            raw_df = pd.read_excel(input_buffer, header=None, engine=engine)
            logger.info(f"Successfully read Excel file with engine: {engine}")
            logger.info(f"DataFrame shape: {raw_df.shape} (rows x columns)")
            if (
                raw_df.shape[1] > 15
            ):  # If we have many columns, log this for MBBank
                logger.info(
                    f"Wide format detected with {raw_df.shape[1]} columns - suitable for MBBank"
                )
            return raw_df
        except Exception as e:
            logger.warning(f"Engine {engine} failed: {str(e)}")
            continue

    # If all Excel engines fail, try reading as HTML (common with bank exports)
    try:
        input_buffer.seek(0)
        logger.info("Attempting to read as HTML format (bank export)")

        # Read as HTML - this handles Excel HTML format from banks
        html_tables = pd.read_html(input_buffer, encoding="utf-8")

        if html_tables and len(html_tables) > 0:
            # Use the first (and usually only) table
            raw_df = html_tables[0]
            logger.info(
                f"Successfully read HTML format with {len(raw_df)} rows"
            )
            return raw_df
        else:
            raise Exception("No tables found in HTML content")

    except Exception as e:
        logger.warning(f"HTML reading failed: {str(e)}")

    # If HTML fails, try parsing as Excel XML format (Microsoft Excel XML)
    try:
        input_buffer.seek(0)
        logger.info("Attempting to read as Excel XML format (Microsoft XML)")

        # Read the content and parse with BeautifulSoup
        content = input_buffer.read().decode("utf-8")
        soup = BeautifulSoup(content, "xml")

        # Look for Microsoft Excel Web Archive format first
        excel_workbook = soup.find("ExcelWorkbook")
        if excel_workbook:
            logger.info("Detected Microsoft Excel Web Archive format")
            worksheets = excel_workbook.find("ExcelWorksheets")
            if worksheets:
                worksheet_sources = worksheets.findAll("ExcelWorksheet")
                if worksheet_sources:
                    # This is a web archive that references external files
                    # We can't process it without the external files
                    raise Exception(
                        f"Excel Web Archive format detected - requires external files. Found {len(worksheet_sources)} worksheet(s) but data is in external files like 'sheet001.htm'"
                    )

        # Look for standard Excel XML worksheets
        worksheets = soup.findAll("Worksheet")
        if not worksheets:
            # Try alternative worksheet tags
            worksheets = soup.findAll("worksheet")
        if not worksheets:
            # Try looking for any table-like structures
            worksheets = soup.findAll("Table") or soup.findAll("table")

        if not worksheets:
            raise Exception("No worksheets found in XML content")

        # Process the first worksheet
        sheet = worksheets[0]
        sheet_as_list = []

        # Look for rows in different possible formats
        rows = (
            sheet.findAll("Row") or sheet.findAll("row") or sheet.findAll("tr")
        )

        for row in rows:
            row_data = []
            # Look for cells in different possible formats
            cells = (
                row.findAll("Cell")
                or row.findAll("cell")
                or row.findAll("td")
                or row.findAll("th")
            )

            for cell in cells:
                # Extract cell data, handling different formats
                cell_text = ""
                if cell.Data:
                    cell_text = cell.Data.text if cell.Data.text else ""
                elif cell.text:
                    cell_text = cell.text.strip()
                elif cell.string:
                    cell_text = cell.string.strip()

                row_data.append(cell_text)

            if row_data:  # Only add non-empty rows
                sheet_as_list.append(row_data)

        if sheet_as_list:
            raw_df = pd.DataFrame(sheet_as_list)
            logger.info(
                f"Successfully read Excel XML format with {len(raw_df)} rows"
            )
            return raw_df
        else:
            raise Exception("No data found in XML worksheet")

    except Exception as e:
        logger.warning(f"Excel XML reading failed: {str(e)}")

    # If all methods fail, raise error
    raise Exception(
        "Cannot detect file format - all engines failed (tried Excel, HTML, and XML formats)"
    )


def find_header_row(
    df: pd.DataFrame, bank_config=None
) -> Tuple[int, Dict[str, int]]:
    """
    Find the header row containing the required column headers

    Args:
        df: DataFrame with the raw Excel data
        bank_config: Bank-specific configuration for header patterns

    Returns:
        Tuple of (header_row_index, column_mapping)
    """
    # Debug logging for MBB
    bank_code = "UNKNOWN"
    if bank_config and hasattr(bank_config, "code"):
        bank_code = bank_config.code
        logger.info(
            f"Processing {bank_code} file with bank-specific configuration"
        )

    # Get header patterns from bank configuration if provided
    if bank_config and hasattr(bank_config, "statement_config"):
        # Use bank-specific header patterns
        config_patterns = bank_config.statement_config.header_patterns
        logger.info(f"{bank_code} header patterns: {config_patterns}")
        header_patterns = []
        for field_patterns in config_patterns.values():
            header_patterns.extend(field_patterns)
    else:
        # Default patterns for backward compatibility
        logger.info("Using default header patterns")
        header_patterns = [
            "so tham chieu",
            "so ct",
            "ma gd",
            "reference",  # Reference number
            "ngay hieu luc",
            "ngay hl",
            "ngay giao dich",  # VCB: Ngày giao dịch
            "ngay",
            "date",  # Date
            "ghi no",
            "so tien ghi no",  # VCB: Số tiền ghi nợ
            "debit",
            "tien ra",
            "chi",  # Debit
            "ghi co",
            "so tien ghi co",  # VCB: Số tiền ghi có
            "credit",
            "tien vao",
            "thu",  # Credit
            "so du",
            "balance",  # Balance
            "mo ta",
            "dien giai",
            "noi dung",
            "description",  # Description
        ]

    # Normalize text for comparison
    def normalize(text):
        if pd.isna(text) or not isinstance(text, str):
            return ""
        # Convert to lowercase but preserve spaces
        text = text.lower().strip()
        # Replace common Vietnamese diacritics but keep spaces
        replacements = {
            "á": "a",
            "à": "a",
            "ả": "a",
            "ã": "a",
            "ạ": "a",
            "ă": "a",
            "ắ": "a",
            "ằ": "a",
            "ẳ": "a",
            "ẵ": "a",
            "ặ": "a",
            "â": "a",
            "ấ": "a",
            "ầ": "a",
            "ẩ": "a",
            "ẫ": "a",
            "ậ": "a",
            "é": "e",
            "è": "e",
            "ẻ": "e",
            "ẽ": "e",
            "ẹ": "e",
            "ê": "e",
            "ế": "e",
            "ề": "e",
            "ể": "e",
            "ễ": "e",
            "ệ": "e",
            "í": "i",
            "ì": "i",
            "ỉ": "i",
            "ĩ": "i",
            "ị": "i",
            "ó": "o",
            "ò": "o",
            "ỏ": "o",
            "õ": "o",
            "ọ": "o",
            "ô": "o",
            "ố": "o",
            "ồ": "o",
            "ổ": "o",
            "ỗ": "o",
            "ộ": "o",
            "ơ": "o",
            "ớ": "o",
            "ờ": "o",
            "ở": "o",
            "ỡ": "o",
            "ợ": "o",
            "ú": "u",
            "ù": "u",
            "ủ": "u",
            "ũ": "u",
            "ụ": "u",
            "ư": "u",
            "ứ": "u",
            "ừ": "u",
            "ử": "u",
            "ữ": "u",
            "ự": "u",
            "ý": "y",
            "ỳ": "y",
            "ỷ": "y",
            "ỹ": "y",
            "ỵ": "y",
            "đ": "d",
        }
        for vietnamese, english in replacements.items():
            text = text.replace(vietnamese, english)
        return text

    # Search each row for headers
    for row_idx in range(min(20, len(df))):  # Look at first 20 rows
        row_values = [normalize(val) for val in df.iloc[row_idx].values]

        # Debug: Show raw row values for the first few rows
        if row_idx <= 10:
            logger.debug(
                f"Row {row_idx}: {[str(val) for val in df.iloc[row_idx].values]}"
            )
            logger.debug(f"Normalized Row {row_idx}: {row_values}")

            # Special debug for MBBank - check if BÚT TOÁN is in this row
            if bank_config and bank_config.code == "MBB":
                for col_idx, val in enumerate(df.iloc[row_idx].values):
                    if pd.notna(val) and "BÚT TOÁN" in str(val).upper():
                        logger.info(
                            f"MBB: Found 'BÚT TOÁN' in Row {row_idx}, Column {col_idx}: '{val}'"
                        )

        # Count how many headers we found in this row
        header_count = 0
        column_mapping = {}

        # Enhanced debug for MBBank - show all columns in header row
        if row_idx == 7 and bank_config and bank_config.code == "MBB":
            logger.info(f"MBB Header Row {row_idx} - All columns:")
            for col_idx, val in enumerate(df.iloc[row_idx].values):
                if pd.notna(val) and str(val).strip():
                    logger.info(f"  Column {col_idx}: '{val}'")

        for col_idx, cell_value in enumerate(row_values):
            # Skip empty values
            if not cell_value:
                continue

            # Check if this cell contains any header pattern
            matched_patterns = [
                pattern for pattern in header_patterns if pattern in cell_value
            ]
            if matched_patterns:
                logger.debug(
                    f"Row {row_idx}, Col {col_idx}: '{cell_value}' matched patterns: {matched_patterns}"
                )
                header_count += 1

                # Map column to standard name using bank-specific patterns
                if bank_config and hasattr(bank_config, "statement_config"):
                    patterns = bank_config.statement_config.header_patterns
                    # Use bank-specific mapping - check each field
                    for field_name, field_patterns in patterns.items():
                        # Normalize the configured patterns for comparison
                        normalized_patterns = [
                            normalize(pattern) for pattern in field_patterns
                        ]
                        if any(
                            normalized_pattern in cell_value
                            for normalized_pattern in normalized_patterns
                        ):
                            if (
                                field_name not in column_mapping
                            ):  # Avoid overwriting if already found
                                column_mapping[field_name] = col_idx
                                logger.info(
                                    f"{bank_code}: Mapped '{field_name}' to column {col_idx} ('{df.iloc[row_idx].values[col_idx]}')"
                                )
                            break
                else:
                    # Default mapping for backward compatibility
                    if any(
                        pattern in cell_value
                        for pattern in [
                            "so tham chieu",
                            "so ct",
                            "ma gd",
                            "reference",
                        ]
                    ):
                        column_mapping["reference"] = col_idx
                    elif any(
                        pattern in cell_value
                        for pattern in [
                            "ngay hieu luc",
                            "ngay hl",
                            "ngay giao dich",
                            "ngay",
                            "date",
                        ]
                    ):
                        column_mapping["date"] = col_idx
                    elif any(
                        pattern in cell_value
                        for pattern in [
                            "ghi no",
                            "so tien ghi no",
                            "debit",
                            "tien ra",
                            "chi",
                        ]
                    ):
                        column_mapping["debit"] = col_idx
                    elif any(
                        pattern in cell_value
                        for pattern in [
                            "ghi co",
                            "so tien ghi co",
                            "credit",
                            "tien vao",
                            "thu",
                        ]
                    ):
                        column_mapping["credit"] = col_idx
                    elif any(
                        pattern in cell_value
                        for pattern in ["so du", "balance"]
                    ):
                        column_mapping["balance"] = col_idx
                    elif any(
                        pattern in cell_value
                        for pattern in [
                            "mo ta",
                            "dien giai",
                            "noi dung",
                            "description",
                        ]
                    ):
                        column_mapping["description"] = col_idx

        # If we found at least 4 headers and have all critical fields, consider this the header row
        # For ACB, we need "Số GD" (reference), "Ngày giao dịch" (date), and "Nội dung giao dịch" (description)
        critical_fields_found = 0
        required_critical_fields = ["reference", "date", "description"]

        for critical_field in required_critical_fields:
            if critical_field in column_mapping:
                critical_fields_found += 1

        # Log analysis for this row
        logger.debug(
            f"Row {row_idx}: Found {header_count} headers, {critical_fields_found} critical fields"
        )
        logger.debug(f"Row {row_idx}: Column mapping so far: {column_mapping}")

        # Require at least 4 total headers AND all 3 critical fields for ACB and MBB
        min_headers_needed = 4
        min_critical_needed = 3

        # For bank-specific requirements
        if (
            bank_config
            and hasattr(bank_config, "code")
            and bank_config.code in ["ACB", "MBB"]
        ):
            # ACB and MBB require all critical fields to avoid matching summary rows
            if (
                header_count >= min_headers_needed
                and critical_fields_found >= min_critical_needed
            ):
                logger.info(
                    f"Found valid {bank_config.code} header row at index {row_idx} with {header_count} headers and {critical_fields_found} critical fields"
                )
                logger.info(
                    f"{bank_config.code} Column mapping: {column_mapping}"
                )

                # For MBB, verify that 'reference' points to 'BÚT TOÁN' column
                if bank_config.code == "MBB" and "reference" in column_mapping:
                    ref_col_idx = column_mapping["reference"]
                    ref_header = df.iloc[row_idx].values[ref_col_idx]
                    logger.info(
                        f"MBB: Reference field mapped to column {ref_col_idx} with header '{ref_header}'"
                    )
                elif bank_config.code == "MBB":
                    logger.warning(
                        f"MBB: Reference column not found in row {row_idx}. Available mappings: {column_mapping}"
                    )
                    # Try to find BÚT TOÁN manually
                    for col_idx, val in enumerate(df.iloc[row_idx].values):
                        if pd.notna(val) and "BÚT TOÁN" in str(val).upper():
                            logger.info(
                                f"MBB: Found 'BÚT TOÁN' manually in column {col_idx}: '{val}'"
                            )
                            column_mapping["reference"] = col_idx
                            break

                return row_idx, column_mapping
        else:
            # For other banks, use the original logic
            if header_count >= 3:
                logger.info(
                    f"Found header row at index {row_idx} with {header_count} headers"
                )
                logger.info(f"Column mapping: {column_mapping}")
                return row_idx, column_mapping

    # If no header row found, return -1 and empty mapping
    return -1, {}


def extract_transactions(
    df: pd.DataFrame,
    header_row: int,
    column_mapping: Dict[str, int],
    bank_config=None,
) -> pd.DataFrame:
    """
    Extract transactions from the DataFrame starting from the row after the header

    Args:
        df: DataFrame with the raw data
        header_row: Index of the header row
        column_mapping: Mapping of standard column names to column indices
        bank_config: Bank-specific configuration for processing rules

    Returns:
        DataFrame with the transactions
    """
    # Extract data rows (after header row)
    data_rows = []

    # Start from the row after the header
    start_row = header_row + 1

    # Determine if this is VCB, ACB, or MBB bank requiring null reference check
    is_vcb_bank = (
        bank_config
        and hasattr(bank_config, "code")
        and bank_config.code.upper() == "VCB"
    )

    is_acb_bank = (
        bank_config
        and hasattr(bank_config, "code")
        and bank_config.code.upper() == "ACB"
    )

    is_mbb_bank = (
        bank_config
        and hasattr(bank_config, "code")
        and bank_config.code.upper() == "MBB"
    )

    # Process each row until we find a termination indicator or end of data
    for row_idx in range(start_row, len(df)):
        row = df.iloc[row_idx]

        # VCB-specific: Stop reading when reference number is null
        if is_vcb_bank and "reference" in column_mapping:
            ref_col_idx = column_mapping["reference"]
            if ref_col_idx < len(row):
                ref_value = row[ref_col_idx]
                if (
                    pd.isna(ref_value)
                    or ref_value == ""
                    or str(ref_value).strip() == ""
                ):
                    logger.info(
                        f"VCB: Found null reference at row {row_idx}, stopping extraction"
                    )
                    break

        # ACB-specific: Stop reading when 'Số GD' (reference number) is null
        if is_acb_bank and "reference" in column_mapping:
            ref_col_idx = column_mapping["reference"]
            if ref_col_idx < len(row):
                ref_value = row[ref_col_idx]
                if (
                    pd.isna(ref_value)
                    or ref_value == ""
                    or str(ref_value).strip() == ""
                ):
                    logger.info(
                        f"ACB: Found null 'Số GD' at row {row_idx}, stopping extraction"
                    )
                    break

        # MBB-specific: Stop reading when 'BÚT TOÁN' (reference number) is null
        if is_mbb_bank and "reference" in column_mapping:
            ref_col_idx = column_mapping["reference"]
            if ref_col_idx < len(row):
                ref_value = row[ref_col_idx]
                logger.debug(
                    f"MBB Row {row_idx}: BÚT TOÁN column {ref_col_idx} value: '{ref_value}'"
                )
                if (
                    pd.isna(ref_value)
                    or ref_value == ""
                    or str(ref_value).strip() == ""
                ):
                    logger.info(
                        f"MBB: Found null 'BÚT TOÁN' at row {row_idx}, stopping extraction"
                    )
                    break

        # Check if this is a termination row (e.g., totals row)
        row_text = " ".join(
            [str(val).lower() for val in row.values if pd.notna(val)]
        )

        # Use bank-specific termination patterns if available
        termination_patterns = [
            "tong cong",
            "total",
            "so du cuoi ky",
            "cong phat sinh",
        ]
        if bank_config and hasattr(bank_config, "statement_config"):
            termination_patterns = (
                bank_config.statement_config.data_end_patterns
            )

        if any(term.lower() in row_text for term in termination_patterns):
            logger.info(f"Found termination row at index {row_idx}: {row_text}")
            break

        # Create a record for this row
        record = {}

        # Extract values using the column mapping
        for col_name, col_idx in column_mapping.items():
            if col_idx < len(row):
                value = row[col_idx]

                # Debug logging for MBB to show what's being extracted
                if (
                    is_mbb_bank and row_idx <= start_row + 3
                ):  # Show first few data rows
                    logger.debug(
                        f"MBB Row {row_idx}: Extracting {col_name} from column {col_idx}, value: '{value}'"
                    )

                # Handle different column types
                if col_name == "date":
                    # Try to parse as date
                    if pd.notna(value):
                        try:
                            if isinstance(value, str) and (
                                "/" in value or "-" in value
                            ):
                                value = pd.to_datetime(value)
                            # Keep datetime objects as is
                        except Exception as e:
                            logger.warning(f"Failed to parse date {value}: {e}")
                elif col_name in ["debit", "credit", "balance"]:
                    # Convert to numeric
                    if pd.notna(value):
                        try:
                            if isinstance(value, str):
                                value = value.replace(",", "").replace(" ", "")
                            value = pd.to_numeric(value)
                        except Exception as e:
                            logger.error(
                                f"Row {row_idx}, Column {col_idx} ({col_name}): Failed to parse numeric value '{value}': {e}"
                            )
                            # Skip this row if we can't parse critical numeric data
                            continue
                elif col_name in ["reference", "description"]:
                    # Keep as string
                    if pd.notna(value):
                        value = str(value).strip()
                    else:
                        value = ""

                record[col_name] = value

        # For MBB: Include rows with valid 'BÚT TOÁN' (reference) value, but be more flexible
        if is_mbb_bank:
            # Check if we have a reference value (even if column mapping wasn't perfect)
            has_reference = (
                "reference" in record
                and record["reference"]
                and str(record["reference"]).strip()
            )

            # For MBB, also check if we have transaction data (amount or description)
            has_transaction_data = (
                ("debit" in record and record["debit"] and record["debit"] != 0)
                or (
                    "credit" in record
                    and record["credit"]
                    and record["credit"] != 0
                )
                or (
                    "description" in record
                    and record["description"]
                    and str(record["description"]).strip()
                )
            )

            if has_reference or has_transaction_data:
                data_rows.append(record)
                logger.debug(
                    f"MBB: Added row {row_idx} - reference: '{record.get('reference', 'N/A')}', has_data: {has_transaction_data}"
                )
            else:
                logger.debug(
                    f"MBB: Skipped row {row_idx} - no reference and no transaction data"
                )
        else:
            # Include ALL rows from the data area for other banks
            data_rows.append(record)

    # Create DataFrame from records
    transactions_df = pd.DataFrame(data_rows)

    # Fill missing values
    if "debit" not in transactions_df:
        transactions_df["debit"] = 0
    if "credit" not in transactions_df:
        transactions_df["credit"] = 0
    if "reference" not in transactions_df:
        transactions_df["reference"] = [
            f"AUTO-{i + 1}" for i in range(len(transactions_df))
        ]
    if "description" not in transactions_df:
        transactions_df["description"] = ""

    transactions_df["debit"] = transactions_df["debit"].fillna(0)
    transactions_df["credit"] = transactions_df["credit"].fillna(0)

    # Auto-generate references for rows where it's missing
    if "reference" in transactions_df.columns:
        for i, ref in enumerate(transactions_df["reference"]):
            if pd.isna(ref) or ref == "":
                transactions_df.at[i, "reference"] = f"AUTO-{i + 1}"

    return transactions_df


@router.post("", response_model=AccountingResponse)
async def process_bank_statement(file: UploadFile):
    """
    Process BIDV bank statement file to Saoke format using IntegratedBankProcessor

    Args:
        file: Uploaded BIDV bank statement file

    Returns:
        JSON with base64 encoded result file
    """
    logger.info(f"Processing bank statement: {file.filename}")

    try:
        # Validate file format
        validate_excel_file(file.filename)

        # Read file content
        content = await file.read()

        # Create input buffer
        input_buffer = io.BytesIO(content)

        # Create output buffer for the result
        output_buffer = io.BytesIO()

        try:
            # Connect to the database
            if not processor.connect():
                logger.error("Failed to connect to database")
                raise HTTPException(
                    status_code=500,
                    detail="Failed to connect to database",
                )

            try:
                # First identify bank from filename if available
                if file and file.filename:
                    # Extract bank name from filename (e.g., BIDV from 'BIDV 3840.ods')
                    bank_name = processor.extract_bank_from_filename(
                        file.filename
                    )
                    if bank_name:
                        logger.info(
                            f"Extracted bank name: '{bank_name}' from filename"
                        )
                        bank_info = processor.get_bank_info_by_name(bank_name)

                        # DEBUG: Check what get_bank_info_by_name returned
                        logger.info(
                            f"Bank info result: {type(bank_info)} - {bank_info}"
                        )

                        if bank_info:
                            # Ensure bank_info is a dictionary before accessing
                            if isinstance(bank_info, dict):
                                processor.current_bank_name = bank_info.get(
                                    "short_name", ""
                                )
                                processor.current_bank_info = bank_info
                                logger.info(
                                    f"Identified bank {processor.current_bank_name} from filename"
                                )
                            else:
                                logger.error(
                                    f"Bank info is not a dictionary: {type(bank_info)} - {bank_info}"
                                )
                                # Set default values to prevent further errors
                                processor.current_bank_name = bank_name
                                processor.current_bank_info = {
                                    "code": bank_name,
                                    "short_name": bank_name,
                                    "name": f"Bank {bank_name}",
                                }
                        else:
                            logger.warning(
                                f"Could not find bank info for '{bank_name}' from filename"
                            )
                    else:
                        logger.warning(
                            f"Could not extract bank name from filename: {file.filename}"
                        )
                else:
                    logger.warning("No filename available to identify bank")

                # Log the bank information that will be used throughout the process
                if processor.current_bank_name:
                    logger.info(f"Using bank: {processor.current_bank_name}")
                    if processor.current_bank_info:
                        logger.info(
                            f"Bank full name: {processor.current_bank_info.get('name')}"
                        )
                        logger.info(
                            f"Bank address: {processor.current_bank_info.get('address')}"
                        )
                else:
                    logger.warning(
                        "No bank information available for this file"
                    )

                # Step 1: Read the raw Excel file - KEEP ORIGINAL EXTRACTION LOGIC
                logger.info(f"Reading Excel file: {file.filename}")

                # Read raw Excel data without headers with engine fallback
                raw_df = read_excel_with_fallback(input_buffer)

                # Get bank configuration for processing
                bank_config = None
                if processor.current_bank_name:
                    bank_config = processor.get_bank_config_for_bank(
                        processor.current_bank_name
                    )
                    logger.info(
                        f"Using bank config for: {processor.current_bank_name}"
                    )

                # Find the header row and column mapping with bank-specific patterns
                header_row, column_mapping = find_header_row(
                    raw_df, bank_config
                )

                if header_row == -1 or not column_mapping:
                    logger.error(
                        "Could not find header row with required columns"
                    )
                    raise HTTPException(
                        status_code=400,
                        detail="Could not find header row with required columns",
                    )

                # Extract transactions with bank-specific rules
                transactions_df = extract_transactions(
                    raw_df, header_row, column_mapping, bank_config
                )

                # Debug logging for MBB to show what data was extracted
                if bank_config and bank_config.code == "MBB":
                    logger.info(
                        f"MBB: Extracted {len(transactions_df)} transactions from Excel file"
                    )
                    logger.info(
                        f"MBB: Transactions DataFrame columns: {list(transactions_df.columns)}"
                    )
                    if len(transactions_df) > 0:
                        for i, (_, row) in enumerate(
                            transactions_df.head(3).iterrows()
                        ):
                            logger.info(
                                f"MBB Row {i + 1}: debit={row.get('debit', 'N/A')}, credit={row.get('credit', 'N/A')}, ref={row.get('reference', 'N/A')}, desc={str(row.get('description', 'N/A'))[:50]}..."
                            )

                # Step 2: Extract bank account from header using IntegratedBankProcessor
                input_buffer.seek(0)
                bank_account = processor.extract_account_from_header(
                    input_buffer
                )
                if bank_account:
                    processor.default_bank_account = bank_account
                    logger.info(f"Detected bank account: {bank_account}")
                else:
                    # Try to infer account from filename if available
                    if file and file.filename:
                        if "3840" in file.filename:
                            logger.info(
                                f"Inferring account 3840 from filename: {file.filename}"
                            )
                            account_match = processor.find_account_by_code(
                                "3840"
                            )
                            if account_match:
                                processor.default_bank_account = (
                                    account_match.code
                                )
                                logger.info(
                                    f"Set default bank account to {processor.default_bank_account} based on filename"
                                )
                            else:
                                # If no match in database, use a direct mapping for 3840
                                logger.warning(
                                    "Account code '3840' not found in database. Available account codes logged for debugging."
                                )
                                # Log some available account codes for debugging
                                try:
                                    from src.accounting.fast_search import (
                                        search_accounts,
                                    )

                                    sample_accounts = search_accounts(
                                        "3", field_name="code", limit=10
                                    )
                                    logger.info(
                                        f"Sample accounts starting with '3': {[acc.get('code', 'N/A') for acc in sample_accounts]}"
                                    )
                                except Exception as e:
                                    logger.warning(
                                        f"Could not query sample accounts: {e}"
                                    )

                                logger.info(
                                    "Using hardcoded mapping for account 3840"
                                )
                                processor.default_bank_account = "1121115"  # Use a different account than default

                # Step 3: Now that we have the transactions_df, use the processor to determine
                # accounts and create SaokeEntry objects

                # If transactions_df is empty, handle gracefully
                if transactions_df.empty:
                    logger.warning("No transactions found in the file")
                    return AccountingResponse(
                        success=False,
                        message="No transactions found in the file",
                    )

                # Process the transactions using the IntegratedBankProcessor
                saoke_entries = []

                # Convert transactions_df to RawTransaction objects
                raw_transactions = []
                for _, row in transactions_df.iterrows():
                    try:
                        if "date" not in row or pd.isna(row["date"]):
                            continue

                        # Create a RawTransaction object
                        from src.accounting.integrated_bank_processor import (
                            RawTransaction,
                        )

                        if bank_config and bank_config.code == "MBB":
                            logger.info(
                                f"MBB: transactions_df columns: {list(transactions_df.columns)}"
                            )
                            logger.info("MBB: Sample row data:")
                            if len(transactions_df) > 0:
                                sample_row = transactions_df.iloc[0]
                                for col, val in sample_row.items():
                                    logger.info(
                                        f"  {col}: '{val}' (type: {type(val)})"
                                    )
                        transaction = RawTransaction(
                            reference=str(row.get("reference", "")),
                            datetime=row["date"]
                            if isinstance(row["date"], datetime)
                            else pd.to_datetime(row["date"]),
                            debit_amount=float(row.get("debit", 0)),
                            credit_amount=float(row.get("credit", 0)),
                            balance=float(row.get("balance", 0))
                            if "balance" in row
                            else 0,
                            description=str(row.get("description", "")),
                        )
                        raw_transactions.append(transaction)

                        # Debug logging for MBB transactions
                        if bank_config and bank_config.code == "MBB":
                            logger.info(
                                f"MBB Raw Transaction: ref={transaction.reference}, debit={transaction.debit_amount}, credit={transaction.credit_amount}, desc={transaction.description[:50]}..."
                            )
                    except Exception as e:
                        logger.error(
                            f"Error converting row to RawTransaction: {e}"
                        )
                        continue

                # Use the existing process_to_saoke method which already handles VISA fee entries correctly
                logger.info(
                    "Using process_to_saoke method for comprehensive transaction processing"
                )

                # Special handling for LAI NHAP VON in ACB statements
                lai_nhap_von_acb_present = False
                for _, row in transactions_df.iterrows():
                    description = str(row.get("description", ""))
                    if (
                        "LAI NHAP VON" in description.upper()
                        and processor.current_bank_name == "ACB"
                    ):
                        logger.info(
                            f"Found 'LAI NHAP VON' in ACB statement: {description}"
                        )
                        lai_nhap_von_acb_present = True
                        # The processor will handle special logic for this case

                # Special handling for HUYNH THI THANH TAM transactions
                thanh_tam_present = False
                for idx, row in transactions_df.iterrows():
                    description = str(row.get("description", ""))
                    if "HUYNH THI THANH TAM" in description.upper():
                        logger.info(
                            f"Special handling for HUYNH THI THANH TAM transaction at row {idx}"
                        )
                        thanh_tam_present = True
                        # The amount is not changed - only the account number will be set in processed_df

                # Call process_to_saoke with the transactions DataFrame
                processed_df = processor.process_to_saoke(transactions_df)

                if processed_df.empty:
                    logger.error("No transactions were successfully processed")
                    return AccountingResponse(
                        success=False,
                        message="No transactions were successfully processed.",
                    )

                # Override counterparty information for special cases
                if lai_nhap_von_acb_present:
                    for idx, row in processed_df.iterrows():
                        description = str(row.get("description", ""))
                        if "LAI NHAP VON" in description.upper():
                            logger.info(
                                f"Setting ACB bank info for 'LAI NHAP VON' transaction at row {idx}"
                            )
                            processed_df.at[idx, "counterparty_code"] = "301452948"
                            processed_df.at[idx, "counterparty_name"] = "NGÂN HÀNG TMCP Á CHÂU"
                            processed_df.at[idx, "address"] = "442 Nguyễn Thị Minh Khai, Phường 05, Quận 3, Thành phố Hồ Chí Minh, Việt Nam"
                            
                            # Ensure special account handling too
                            is_credit = (row.get("credit_amount", 0) > 0 or row.get("credit", 0) > 0)
                            if is_credit:
                                processed_df.at[idx, "credit_account"] = "811"
                                logger.info("Setting credit account to 811 for LAI NHAP VON transaction")
                            else:
                                processed_df.at[idx, "debit_account"] = "811"
                                logger.info("Setting debit account to 811 for LAI NHAP VON transaction")

                # Override counterparty information for HUYNH THI THANH TAM
                if thanh_tam_present:
                    for idx, row in processed_df.iterrows():
                        description = str(row.get("description", ""))
                        if "HUYNH THI THANH TAM" in description.upper():
                            logger.info(
                                f"Setting HUYNH THI THANH TAM info for transaction at row {idx}"
                            )
                            processed_df.at[idx, "counterparty_code"] = "HTTT"
                            processed_df.at[idx, "counterparty_name"] = (
                                "HUỲNH THỊ THANH TÂM"
                            )
                            processed_df.at[idx, "address"] = (
                                "D05.3 Tầng 6, C/c An Phú, 959-961-965 Hậu Giang, P.11, Q.6, TP.HCM"
                            )
                            # Set debit/credit account to 3388
                            is_credit = (
                                row.get("credit_amount", 0) > 0
                                or row.get("credit", 0) > 0
                            )
                            if is_credit:
                                processed_df.at[idx, "credit_account"] = "3388"
                                logger.info(
                                    "Setting credit account to 3388 for HUYNH THI THANH TAM transaction"
                                )
                            else:
                                processed_df.at[idx, "debit_account"] = "3388"
                                logger.info(
                                    "Setting debit account to 3388 for HUYNH THI THANH TAM transaction"
                                )

                # Convert processed DataFrame to saoke entries list
                saoke_entries = processed_df.to_dict("records")
                processed_count = len(saoke_entries)
                failed_count = (
                    0  # process_to_saoke handles its own error counting
                )

                logger.info(
                    f"Transaction processing complete: {processed_count} entries created (including VISA fee entries)"
                )

                # Process dates from the original saoke_entries to ensure DD/MM/YYYY format
                # Do this before creating the formatted DataFrame
                for entry in saoke_entries:
                    try:
                        # Handle 'date' field
                        if "date" in entry and entry["date"]:
                            # If it's a string with a time component (contains space)
                            if (
                                isinstance(entry["date"], str)
                                and " " in entry["date"]
                            ):
                                # Extract just the date part
                                date_part = entry["date"].split(" ")[0]
                                # Ensure it's in DD/MM/YYYY format
                                if "/" in date_part:
                                    parts = date_part.split("/")
                                    if len(parts) == 3:
                                        day = parts[0].zfill(2)
                                        month = parts[1].zfill(2)
                                        year = parts[2]
                                        entry["date"] = f"{day}/{month}/{year}"
                            else:
                                # Try to parse and format
                                try:
                                    dt = pd.to_datetime(
                                        entry["date"], errors="coerce"
                                    )
                                    if not pd.isna(dt):
                                        entry["date"] = dt.strftime("%d/%m/%Y")
                                except:
                                    pass

                        # Handle 'date2' field with the same logic
                        if "date2" in entry and entry["date2"]:
                            # If it's a string with a time component
                            if (
                                isinstance(entry["date2"], str)
                                and " " in entry["date2"]
                            ):
                                date_part = entry["date2"].split(" ")[0]
                                if "/" in date_part:
                                    parts = date_part.split("/")
                                    if len(parts) == 3:
                                        day = parts[0].zfill(2)
                                        month = parts[1].zfill(2)
                                        year = parts[2]
                                        entry["date2"] = f"{day}/{month}/{year}"
                            else:
                                try:
                                    dt = pd.to_datetime(
                                        entry["date2"], errors="coerce"
                                    )
                                    if not pd.isna(dt):
                                        entry["date2"] = dt.strftime("%d/%m/%Y")
                                except:
                                    pass
                    except Exception as e:
                        logger.warning(f"Error formatting dates in entry: {e}")

                # The processed_df already contains the complete data from process_to_saoke
                # saoke_entries is now the list of dictionaries from the DataFrame

                logger.info(
                    f"Processed {len(processed_df)} transactions with IntegratedBankProcessor ({failed_count} failed)"
                )

                # Create Excel file with specific format for Saoke
                # Format columns according to the requirements and EXACT ORDER specified
                formatted_df = pd.DataFrame()

                # Add columns in the EXACT ORDER as specified in the requirements
                formatted_df["Ma_Ct"] = processed_df.get("document_type", "BC")
                formatted_df["Ngay_Ct"] = processed_df.get("date", "")
                formatted_df["So_Ct"] = processed_df.get("document_number", "")
                formatted_df["Ma_Tte"] = processed_df.get("currency", "VND")
                formatted_df["Ty_Gia"] = processed_df.get("exchange_rate", 1)
                formatted_df["Ma_Dt"] = processed_df.get(
                    "counterparty_code", ""
                )
                formatted_df["Ong_Ba"] = processed_df.get(
                    "counterparty_name", ""
                )
                formatted_df["Dia_Chi"] = processed_df.get("address", "")
                formatted_df["Dien_Giai"] = processed_df.get("description", "")
                formatted_df["Tien"] = processed_df.get("amount1", 0)
                formatted_df["Tien_Nt"] = processed_df.get("amount2", 0)
                formatted_df["Tk_No"] = processed_df.get("debit_account", "")
                formatted_df["Tk_Co"] = processed_df.get("credit_account", "")
                formatted_df["Ma_Thue"] = ""  # Empty
                formatted_df["Thue_GtGt"] = 0  # 0
                formatted_df["Tien3"] = 0  # 0
                formatted_df["Tien_Nt3"] = 0  # 0
                formatted_df["Tk_No3"] = ""  # Empty
                formatted_df["Tk_Co3"] = ""  # Empty
                formatted_df["Han_Tt"] = 0  # 0
                formatted_df["Ngay_Ct0"] = processed_df.get(
                    "date2", processed_df.get("date", "")
                )
                formatted_df["So_Ct0"] = ""  # Empty
                formatted_df["So_Seri0"] = ""  # Empty
                formatted_df["Ten_Vt"] = ""  # Empty
                formatted_df["Ma_So_Thue"] = ""  # Empty
                formatted_df["Ten_DtGtGt"] = ""  # Empty
                formatted_df["Ma_Tc"] = ""  # Empty
                formatted_df["Ma_Bp"] = ""  # Empty - Department code
                formatted_df["Ma_Km"] = ""  # Empty - Cost code
                formatted_df["Ma_Hd"] = ""  # Empty
                formatted_df["Ma_Sp"] = ""  # Empty
                formatted_df["Ma_Job"] = ""  # Empty
                formatted_df["DUYET"] = ""  # Empty

                # Additional safety measure: process the formatted dataframe directly
                # to ensure all dates are in DD/MM/YYYY format
                for date_col in ["Ngay_Ct", "Ngay_Ct0"]:
                    if date_col in formatted_df.columns:
                        # Handle each cell individually with careful error handling
                        for idx in formatted_df.index:
                            try:
                                value = formatted_df.at[idx, date_col]

                                # Skip empty values
                                if pd.isna(value) or value == "":
                                    continue

                                # If it's already a string, check if it has time component
                                if isinstance(value, str):
                                    # Remove any leading apostrophes or other formatting characters
                                    value = value.lstrip("'\"")

                                    # Check if there's a time part (contains space)
                                    if " " in value:
                                        # Just take the date part before the space
                                        date_part = value.split(" ")[0]
                                    else:
                                        date_part = value

                                    # Check if it looks like a date
                                    if "/" in date_part:
                                        # Split by / to extract components
                                        parts = date_part.split("/")
                                        if len(parts) == 3:
                                            # Format as DD/MM/YYYY
                                            day = parts[0].zfill(
                                                2
                                            )  # Ensure 2 digits
                                            month = parts[1].zfill(
                                                2
                                            )  # Ensure 2 digits
                                            year = parts[2]
                                            formatted_df.at[idx, date_col] = (
                                                f"{day}/{month}/{year}"
                                            )
                                            continue

                                # Try to parse as datetime and format
                                dt_value = pd.to_datetime(
                                    value, errors="coerce"
                                )
                                if not pd.isna(dt_value):
                                    formatted_df.at[idx, date_col] = (
                                        dt_value.strftime("%d/%m/%Y")
                                    )
                            except Exception as e:
                                logger.warning(
                                    f"Error processing date at index {idx}: {e}"
                                )

                        logger.info(
                            f"Reformatted {date_col} column to ensure proper date formatting"
                        )

                # Save the formatted DataFrame to Excel
                with pd.ExcelWriter(output_buffer, engine="openpyxl") as writer:
                    formatted_df.to_excel(writer, index=False)

                # Get the Excel content
                output_buffer.seek(0)
                excel_content = output_buffer.getvalue()

                # Encode to base64
                encoded_content = base64.b64encode(excel_content).decode(
                    "utf-8"
                )

                # Create output filename
                output_filename = f"{Path(file.filename).stem}_processed.xls"

                # Return success response
                return AccountingResponse(
                    success=True,
                    message=f"Successfully processed {len(formatted_df)} transactions",
                    result_file=encoded_content,
                    filename=output_filename,
                )
            finally:
                # Make sure to close the database connection
                processor.close()

        except Exception as e:
            logger.error(
                f"Error processing bank statement: {str(e)}", exc_info=True
            )
            raise HTTPException(
                status_code=400,
                detail=f"Error processing bank statement: {str(e)}",
            )

    except Exception as e:
        logger.error(
            f"Error processing bank statement: {str(e)}", exc_info=True
        )
        raise HTTPException(
            status_code=400, detail=f"Error processing bank statement: {str(e)}"
        )
