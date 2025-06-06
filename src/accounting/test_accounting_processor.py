#!/usr/bin/env python3
"""
Test script for accounting processing endpoints

This script tests the bank statement converter and processor
to ensure they correctly convert BIDV bank statements to saoke format.
"""

import io
import sys
from pathlib import Path

import pandas as pd
from loguru import logger

from src.accounting.bank_statement_converter import BankStatementConverter
from src.util.logging import get_logger, setup_logging

# Set up logging
setup_logging(log_level="INFO", json_logs=False, log_to_file=True)
logger = get_logger(__name__)


def test_accounting_processor():
    """Test the bank statement converter and processor"""
    # Path to test files
    current_dir = Path(__file__).parent
    input_file = current_dir / "BIDV 3840.xlsx"
    output_file = current_dir / "saoke_test_output.xlsx"

    # Check if input file exists
    if not input_file.exists():
        logger.error(f"Input file {input_file} does not exist")
        print(f"ERROR: Input file {input_file} does not exist")
        return False

    # Initialize converter
    converter = BankStatementConverter()

    try:
        # Process the file
        logger.info(f"Processing {input_file}...")
        df = converter.convert_to_saoke(str(input_file), str(output_file))
        
        # Check if processing was successful
        if df.empty:
            logger.error("Processing resulted in empty DataFrame")
            print("ERROR: Processing resulted in empty DataFrame")
            return False
        
        # Print some info about the processed data
        logger.info(f"Successfully processed {len(df)} transactions")
        print(f"Successfully processed {len(df)} transactions")
        
        # Check if all required columns are present
        required_columns = [
            "Ma_Ct", "Ngay_Ct", "So_Ct", "Ma_Tte", "Ty_Gia", "Ma_Dt", "Ong_Ba", 
            "Dia_Chi", "Dien_Giai", "Tien", "Tien_Nt", "Tk_No", "Tk_Co", "Ma_Thue",
            "Thue_GtGt", "Tien3", "Tien_Nt3", "Tk_No3", "Tk_Co3", "Han_Tt",
            "Ngay_Ct0", "So_Ct0", "So_Seri0", "Ten_Vt", "Ma_So_Thue", "Ten_DtGtGt",
            "Ma_Tc", "Ma_Bp", "Ma_Km", "Ma_Hd", "Ma_Sp", "Ma_Job", "DUYET"
        ]
        
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            logger.error(f"Missing required columns: {missing_columns}")
            print(f"ERROR: Missing required columns: {missing_columns}")
            return False
        
        # Print first few rows
        print("\nSample output data:")
        print(df.head().to_string())
        
        # Print output file location
        if output_file.exists():
            logger.info(f"Output saved to {output_file}")
            print(f"\nOutput saved to {output_file}")
        
        return True
    
    except Exception as e:
        logger.error(f"Error testing accounting processor: {e}")
        print(f"ERROR: {e}")
        import traceback
        traceback.print_exc()
        return False
    finally:
        # Close database connection
        converter.close()


if __name__ == "__main__":
    # If file path is provided as argument, use it
    if len(sys.argv) > 1:
        input_file = sys.argv[1]
        output_file = sys.argv[2] if len(sys.argv) > 2 else "saoke_output.xlsx"
        
        converter = BankStatementConverter()
        if converter.connect():
            try:
                df = converter.convert_to_saoke(input_file, output_file)
                print(f"Processed {len(df)} transactions to {output_file}")
            except Exception as e:
                print(f"Error: {e}")
            finally:
                converter.close()
    else:
        # Run the test
        success = test_accounting_processor()
        if success:
            print("\nTest completed successfully!")
        else:
            print("\nTest failed!")
