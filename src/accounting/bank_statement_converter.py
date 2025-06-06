#!/usr/bin/env python3
"""
Bank Statement Converter Module

This module handles the conversion of BIDV bank statements to the required saoke format.
It uses the integrated processor to parse and process bank statements, then converts
the output to the specified format with all required headers.
"""

import io
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Union

import pandas as pd
from loguru import logger

from src.accounting.integrated_processor import IntegratedBankProcessor
from src.util.logging import get_logger

logger = get_logger(__name__)


class BankStatementConverter:
    """
    Converts BIDV bank statements to saoke format with specific headers.
    """

    def __init__(self, db_path: str = "banking_enterprise.db"):
        """Initialize the converter"""
        self.db_path = db_path
        self.processor = IntegratedBankProcessor(db_path=db_path)
        self.logger = logger

    def connect(self) -> bool:
        """Connect to the database"""
        return self.processor.connect()

    def close(self):
        """Close database connection"""
        self.processor.close()

    def convert_to_saoke(
        self,
        input_file: Union[str, io.BytesIO],
        output_file: Optional[str] = None,
    ) -> pd.DataFrame:
        """
        Convert BIDV bank statement to saoke format

        Args:
            input_file: Path to Excel/ODS file or BytesIO object
            output_file: Path to save the output file (optional)

        Returns:
            DataFrame with saoke format
        """
        try:
            # Process the bank statement using the integrated processor
            self.logger.info(f"Processing bank statement from {input_file}")
            if not self.processor.connect():
                raise RuntimeError("Failed to connect to database")

            # Process the file using the integrated processor
            processed_df = self.processor.process_to_saoke(input_file)

            # Format the output dataframe with the required saoke format
            saoke_df = self.format_to_saoke(processed_df)

            # Save to file if specified
            if output_file and not saoke_df.empty:
                self.logger.info(f"Saving output to {output_file}")
                if output_file.endswith(".ods"):
                    saoke_df.to_excel(output_file, index=False, engine="odf")
                else:
                    saoke_df.to_excel(output_file, index=False)

            return saoke_df

        except Exception as e:
            self.logger.error(f"Error converting bank statement: {e}")
            import traceback
            traceback.print_exc()
            raise
        finally:
            self.processor.close()

    def format_to_saoke(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Format the processed dataframe to the required saoke format

        Args:
            df: Processed DataFrame from integrated_processor

        Returns:
            DataFrame with saoke format
        """
        try:
            # Create an empty DataFrame for the saoke format
            saoke_df = pd.DataFrame()

            # Map columns from the processor output to the required saoke format
            # Required headers from the task description
            saoke_df["Ma_Ct"] = df["document_type"]  # BC, BN, etc.
            saoke_df["Ngay_Ct"] = df["date"]  # Date from processing
            saoke_df["So_Ct"] = df["document_number"]  # Document number
            saoke_df["Ma_Tte"] = "VND"  # Always VND
            saoke_df["Ty_Gia"] = 1  # Always 1
            saoke_df["Ma_Dt"] = df["counterparty_code"]  # Counterparty code
            saoke_df["Ong_Ba"] = df["counterparty_name"]  # Counterparty name
            saoke_df["Dia_Chi"] = df["address"]  # Counterparty address
            saoke_df["Dien_Giai"] = df["description"]  # Description from processing
            saoke_df["Tien"] = df["amount1"]  # Amount
            saoke_df["Tien_Nt"] = df["amount1"]  # Same as Tien
            saoke_df["Tk_No"] = df["debit_account"]  # Debit account
            saoke_df["Tk_Co"] = df["credit_account"]  # Credit account
            
            # Add all other required fields with default values
            saoke_df["Ma_Thue"] = ""  # Empty
            saoke_df["Thue_GtGt"] = 0  # 0
            saoke_df["Tien3"] = 0  # 0
            saoke_df["Tien_Nt3"] = 0  # 0
            saoke_df["Tk_No3"] = ""  # Empty
            saoke_df["Tk_Co3"] = ""  # Empty
            saoke_df["Han_Tt"] = 0  # 0
            saoke_df["Ngay_Ct0"] = df["date"]  # Same as Ngay_Ct
            saoke_df["So_Ct0"] = ""  # Empty
            saoke_df["So_Seri0"] = ""  # Empty
            saoke_df["Ten_Vt"] = ""  # Empty
            saoke_df["Ma_So_Thue"] = ""  # Empty
            saoke_df["Ten_DtGtGt"] = ""  # Empty
            saoke_df["Ma_Tc"] = ""  # Empty
            saoke_df["Ma_Bp"] = df.get("department", "")  # Department if available, otherwise empty
            saoke_df["Ma_Km"] = df.get("cost_code", "")  # Cost code if available, otherwise empty
            saoke_df["Ma_Hd"] = ""  # Empty
            saoke_df["Ma_Sp"] = ""  # Empty
            saoke_df["Ma_Job"] = ""  # Empty
            saoke_df["DUYET"] = ""  # Empty
            
            self.logger.info(f"Formatted {len(saoke_df)} rows to saoke format")
            return saoke_df

        except Exception as e:
            self.logger.error(f"Error formatting to saoke: {e}")
            import traceback
            traceback.print_exc()
            raise


def test_converter():
    """Test the bank statement converter"""
    import sys
    
    if len(sys.argv) > 1:
        input_file = sys.argv[1]
        output_file = sys.argv[2] if len(sys.argv) > 2 else "saoke_output.xlsx"
        
        converter = BankStatementConverter()
        try:
            df = converter.convert_to_saoke(input_file, output_file)
            print(f"Converted {len(df)} transactions to {output_file}")
        except Exception as e:
            print(f"Error: {e}")
    else:
        print("Usage: python bank_statement_converter.py <input_file> [output_file]")


if __name__ == "__main__":
    test_converter()
