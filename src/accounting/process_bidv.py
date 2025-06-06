#!/usr/bin/env python3
"""
BIDV Bank Statement Processing Script

This script provides a command-line interface to process BIDV bank statements
using the integrated processor with counterparty extraction.
"""

import argparse
import sys
from pathlib import Path

# Add the project root to the path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from src.accounting.integrated_processor import IntegratedBankProcessor
from src.util.logging import get_logger

logger = get_logger(__name__)


def main():
    """Main function to process BIDV bank statements"""
    parser = argparse.ArgumentParser(description="Process BIDV bank statements")
    parser.add_argument(
        "input_file", help="Path to input BIDV bank statement (ODS or XLSX)"
    )
    parser.add_argument(
        "--output",
        "-o",
        help="Path to output file (default: saoke_output.ods)",
        default="saoke_output.ods",
    )
    parser.add_argument(
        "--debug", "-d", help="Enable debug output", action="store_true"
    )

    args = parser.parse_args()

    # Check if input file exists
    input_path = Path(args.input_file)
    if not input_path.exists():
        logger.error(f"Input file not found: {input_path}")
        return 1

    # Initialize processor
    processor = IntegratedBankProcessor()

    if args.debug:
        logger.setLevel("DEBUG")
    
    # Connect to database
    if processor.connect():
        try:
            # Process the file
            logger.info(f"Processing {input_path}...")
            df = processor.process_to_saoke(str(input_path), args.output)
            
            # Print summary
            logger.info(f"Processed {len(df)} transactions")
            
            # More detailed summary in debug mode
            if args.debug and not df.empty:
                # Count by document type
                doc_counts = df["document_type"].value_counts()
                logger.info("Transactions by document type:")
                for doc_type, count in doc_counts.items():
                    logger.info(f"  {doc_type}: {count}")
                
                # Count transactions with extracted counterparties
                default_cp = "Khách Lẻ Không Lấy Hóa Đơn"
                extracted_count = len(df[df["counterparty_name"] != default_cp])
                logger.info(f"Transactions with extracted counterparties: {extracted_count}")
                
            return 0
        except Exception as e:
            logger.error(f"Error processing file: {e}")
            import traceback
            traceback.print_exc()
            return 1
        finally:
            processor.close()
    else:
        logger.error("Failed to connect to database")
        return 1


if __name__ == "__main__":
    sys.exit(main())
