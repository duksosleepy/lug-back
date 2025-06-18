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


def find_header_row(df: pd.DataFrame) -> Tuple[int, Dict[str, int]]:
    """
    Find the header row containing the required column headers

    Args:
        df: DataFrame with the raw Excel data

    Returns:
        Tuple of (header_row_index, column_mapping)
    """
    # Headers to look for (case-insensitive, without accents)
    header_patterns = [
        "so tham chieu",
        "so ct",
        "ma gd",
        "reference",  # Reference number
        "ngay hieu luc",
        "ngay hl",
        "ngay",
        "date",  # Date
        "ghi no",
        "debit",
        "tien ra",
        "chi",  # Debit
        "ghi co",
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

                # Map column to standard name
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
                    for pattern in ["ngay hieu luc", "ngay hl", "ngay", "date"]
                ):
                    column_mapping["date"] = col_idx
                elif any(
                    pattern in cell_value
                    for pattern in ["ghi no", "debit", "tien ra", "chi"]
                ):
                    column_mapping["debit"] = col_idx
                elif any(
                    pattern in cell_value
                    for pattern in ["ghi co", "credit", "tien vao", "thu"]
                ):
                    column_mapping["credit"] = col_idx
                elif any(
                    pattern in cell_value for pattern in ["so du", "balance"]
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
    df: pd.DataFrame, header_row: int, column_mapping: Dict[str, int]
) -> pd.DataFrame:
    """
    Extract transactions from the DataFrame starting from the row after the header

    Args:
        df: DataFrame with the raw data
        header_row: Index of the header row
        column_mapping: Mapping of standard column names to column indices

    Returns:
        DataFrame with the transactions
    """
    # Extract data rows (after header row)
    data_rows = []

    # Start from the row after the header
    start_row = header_row + 1

    # Process each row until we find a termination indicator or end of data
    for row_idx in range(start_row, len(df)):
        row = df.iloc[row_idx]

        # Check if this is a termination row (e.g., totals row)
        row_text = " ".join(
            [str(val).lower() for val in row.values if pd.notna(val)]
        )
        if any(
            term in row_text
            for term in [
                "tong cong",
                "total",
                "so du cuoi ky",
                "cong phat sinh",
            ]
        ):
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
                # Step 1: Read the raw Excel file - KEEP ORIGINAL EXTRACTION LOGIC
                logger.info(f"Reading Excel file: {file.filename}")

                # Read raw Excel data without headers
                input_buffer.seek(0)
                raw_df = pd.read_excel(
                    input_buffer, header=None, engine="calamine"
                )

                # Find the header row and column mapping
                header_row, column_mapping = find_header_row(raw_df)

                if header_row == -1 or not column_mapping:
                    logger.error(
                        "Could not find header row with required columns"
                    )
                    raise HTTPException(
                        status_code=400,
                        detail="Could not find header row with required columns",
                    )

                # Extract transactions (only rows with reference numbers)
                transactions_df = extract_transactions(
                    raw_df, header_row, column_mapping
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
                for transaction in raw_transactions:
                    entry = processor.process_transaction(transaction)
                    if entry:
                        saoke_entries.append(entry.as_dict())

                # Create a DataFrame from the saoke entries
                processed_df = pd.DataFrame(saoke_entries)

                logger.info(
                    f"Processed {len(processed_df)} transactions with IntegratedBankProcessor"
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
                formatted_df["Ma_Bp"] = processed_df.get("department", "")
                formatted_df["Ma_Km"] = processed_df.get("cost_code", "")
                formatted_df["Ma_Hd"] = ""  # Empty
                formatted_df["Ma_Sp"] = ""  # Empty
                formatted_df["Ma_Job"] = ""  # Empty
                formatted_df["DUYET"] = ""  # Empty

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
