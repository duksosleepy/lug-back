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

# Create router for accounting endpoints
router = APIRouter(prefix="/accounting", tags=["accounting"])

# Initialize reader and processor
reader = BankStatementReader()
processor = IntegratedBankProcessor()


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
    # Get header patterns from bank configuration if provided
    if bank_config and hasattr(bank_config, "statement_config"):
        # Use bank-specific header patterns
        config_patterns = bank_config.statement_config.header_patterns
        header_patterns = []
        for field_patterns in config_patterns.values():
            header_patterns.extend(field_patterns)
    else:
        # Default patterns for backward compatibility
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
        # Convert to lowercase and remove diacritics
        text = text.lower().strip()
        # Replace common Vietnamese diacritics
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

        # Count how many headers we found in this row
        header_count = 0
        column_mapping = {}

        for col_idx, cell_value in enumerate(row_values):
            if any(pattern in cell_value for pattern in header_patterns):
                header_count += 1

                # Map column to standard name using bank-specific patterns
                if bank_config and hasattr(bank_config, "statement_config"):
                    patterns = bank_config.statement_config.header_patterns
                    # Use bank-specific mapping
                    for field_name, field_patterns in patterns.items():
                        if any(
                            normalize(pattern) in cell_value
                            for pattern in field_patterns
                        ):
                            column_mapping[field_name] = col_idx
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

        # If we found at least 3 headers, consider this the header row
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

    # Determine if this is VCB bank requiring null reference check
    is_vcb_bank = (
        bank_config
        and hasattr(bank_config, "code")
        and bank_config.code.upper() == "VCB"
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
                            logger.warning(
                                f"Failed to parse numeric value {value}: {e}"
                            )

                record[col_name] = value

        # Include ALL rows from the data area, not just those with references
        # This ensures we capture all transactions in the statement
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
                        bank_info = processor.get_bank_info_by_name(bank_name)
                        if bank_info:
                            processor.current_bank_name = bank_info.get(
                                "short_name", ""
                            )
                            processor.current_bank_info = bank_info
                            logger.info(
                                f"Identified bank {processor.current_bank_name} from filename"
                            )
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

                logger.info(
                    f"Extracted {len(transactions_df)} transactions from raw data"
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
                    except Exception as e:
                        logger.error(
                            f"Error converting row to RawTransaction: {e}"
                        )
                        continue

                # Process each transaction using the processor's account determination logic
                processed_count = 0
                failed_count = 0

                for i, transaction in enumerate(raw_transactions):
                    try:
                        entry = processor.process_transaction(transaction)
                        if entry:
                            saoke_entries.append(entry.as_dict())
                            processed_count += 1
                        else:
                            failed_count += 1
                            logger.warning(
                                f"Failed to process transaction {i + 1}: {transaction.reference}"
                            )
                    except Exception as e:
                        failed_count += 1
                        logger.error(
                            f"Error processing transaction {i + 1} ({transaction.reference}): {e}"
                        )
                        continue

                logger.info(
                    f"Transaction processing complete: {processed_count} successful, {failed_count} failed"
                )

                # Create a DataFrame from the saoke entries
                if not saoke_entries:
                    logger.error("No transactions were successfully processed")
                    return AccountingResponse(
                        success=False,
                        message=f"No transactions were successfully processed. {failed_count} transactions failed.",
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

                # Create a DataFrame from the updated saoke entries
                processed_df = pd.DataFrame(saoke_entries)

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
