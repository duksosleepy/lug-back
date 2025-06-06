#!/usr/bin/env python3
"""
Test Script for Integrated Bank Processor with Counterparty Extraction

This script demonstrates and tests the integrated processor with real data.
It processes a BIDV bank statement and saves the results to a saoke format file.
"""

import sys
from pathlib import Path

# Add the project root to the path if running as main script
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from src.accounting.integrated_processor import IntegratedBankProcessor


def main():
    """Main test function"""
    # Path to input and output files
    input_file = Path(__file__).parent / "BIDV 3840.ods"
    output_file = Path(__file__).parent / "saoke_output.ods"
    
    if not input_file.exists():
        print(f"Input file not found: {input_file}")
        return
    
    print(f"Processing bank statement: {input_file}")
    print(f"Output will be saved to: {output_file}")
    
    # Initialize the processor
    processor = IntegratedBankProcessor()
    
    if processor.connect():
        try:
            # Process the file
            df = processor.process_to_saoke(str(input_file), str(output_file))
            
            # Print summary
            print(f"\nProcessed {len(df)} transactions")
            
            # Show sample of processed data
            if not df.empty:
                print("\nSample of processed data:")
                print(df[["document_type", "date", "counterparty_name", "debit_account", "credit_account", "amount1"]].head())
                
                # Counts by document type
                doc_counts = df["document_type"].value_counts()
                print("\nTransactions by document type:")
                for doc_type, count in doc_counts.items():
                    print(f"{doc_type}: {count}")
                
                # Count transactions with extracted counterparties
                default_cp = "Khách Lẻ Không Lấy Hóa Đơn"
                extracted_count = len(df[df["counterparty_name"] != default_cp])
                print(f"\nTransactions with extracted counterparties: {extracted_count} ({extracted_count/len(df)*100:.1f}%)")
                
        except Exception as e:
            print(f"Error processing file: {e}")
            import traceback
            traceback.print_exc()
        finally:
            processor.close()
    else:
        print("Failed to connect to database")


if __name__ == "__main__":
    main()
