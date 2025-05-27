#!/usr/bin/env python3
"""
Test script to verify bank statement to saoke conversion
"""

from datetime import datetime

from src.accounting.bank_statement_processor import (
    BankStatementProcessor,
    RawTransaction,
)


def test_saoke_conversion():
    """Test conversion of bank statement to saoke format"""

    # Sample transaction from your example
    sample_transaction = RawTransaction(
        reference="0001NFS4-7YJO3BV08",
        datetime=datetime(2025, 3, 1, 11, 17, 53),
        debit_amount=0,
        credit_amount=888000,
        balance=112028712,
        description="TT POS 14100333 500379 5777 064217PDO",
    )

    # Initialize processor
    processor = BankStatementProcessor()

    if not processor.connect():
        print("❌ Failed to connect to database")
        print("Run initialize_database.py first!")
        return

    try:
        # Process the transaction
        entry = processor.process_transaction(sample_transaction)

        if entry:
            print("✅ Transaction processed successfully!")
            print("\n📋 Saoke Entry:")
            print(f"Document Type: {entry.document_type}")
            print(f"Date: {entry.date}")
            print(f"Document Number: {entry.document_number}")
            print(f"Currency: {entry.currency}")
            print(f"Sequence: {entry.sequence}")
            print(f"Counterparty Code: {entry.counterparty_code}")
            print(f"Counterparty Name: {entry.counterparty_name}")
            print(f"Address: {entry.address}")
            print(f"Description: {entry.description}")
            print(f"Original Description: {entry.original_description}")
            print(f"Amount1: {entry.amount1:,.0f}")
            print(f"Amount2: {entry.amount2:,.0f}")
            print(f"Debit Account: {entry.debit_account}")
            print(f"Credit Account: {entry.credit_account}")
            print(f"Date2: {entry.date2}")
            print(f"Department: {entry.department}")

            # Expected output comparison
            print("\n📊 Expected vs Actual:")
            expected = {
                "Document Type": "BC",
                "Date": "01/3/2025",
                "Document Number": "BC03/001",
                "Currency": "VND",
                "Sequence": "1",
                "Counterparty Code": "KL-GOBARIA1",
                "Counterparty Name": "Khách Lẻ Không Lấy Hóa Đơn",
                "Address": "QH 1S9+10 TTTM Go BR-VT, Số 2A, Nguyễn Đình Chiểu, KP 1, P.Phước Hiệp, TP.Bà Rịa, T.Bà Rịa-Vũng Tàu",
                "Description": "Thu tiền bán hàng khách lẻ (POS 14100333 - GO BRVT)_5777_1",
                "Amount": "888,000",
                "Debit Account": "1121114",
                "Credit Account": "1311",
                "Department": "GO BRVT",
            }

            actual = {
                "Document Type": entry.document_type,
                "Date": entry.date,
                "Document Number": entry.document_number,
                "Currency": entry.currency,
                "Sequence": str(entry.sequence),
                "Counterparty Code": entry.counterparty_code,
                "Counterparty Name": entry.counterparty_name,
                "Address": entry.address,
                "Description": entry.description,
                "Amount": f"{entry.amount1:,.0f}",
                "Debit Account": entry.debit_account,
                "Credit Account": entry.credit_account,
                "Department": entry.department,
            }

            all_match = True
            for key in expected:
                if expected[key] != actual[key]:
                    print(
                        f"❌ {key}: Expected '{expected[key]}' but got '{actual[key]}'"
                    )
                    all_match = False
                else:
                    print(f"✅ {key}: {actual[key]}")

            if all_match:
                print("\n🎉 All fields match perfectly!")
            else:
                print("\n⚠️ Some fields don't match. Check the configuration.")

        else:
            print("❌ Failed to process transaction")

    finally:
        processor.close()


def test_full_file_processing():
    """Test processing of actual BIDV file"""
    print("\n" + "=" * 50)
    print("Testing full file processing...")

    processor = BankStatementProcessor()

    try:
        # Process the actual file
        result_df = processor.process_to_saoke(
            input_file="BIDV 3840.ods", output_file="saoke_output.xlsx"
        )

        if not result_df.empty:
            print(f"✅ Processed {len(result_df)} transactions")
            print("\nFirst 5 entries:")
            print(result_df.head())

            # Save sample to CSV for inspection
            result_df.head(10).to_csv("saoke_sample.csv", index=False)
            print("\n📄 Sample saved to saoke_sample.csv")
        else:
            print("❌ No transactions processed")

    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback

        traceback.print_exc()


if __name__ == "__main__":
    print("🧪 Testing Bank Statement to Saoke Conversion")
    print("=" * 50)

    # Test single transaction
    test_saoke_conversion()

    # Test full file if available
    import os

    if os.path.exists("BIDV 3840.ods"):
        test_full_file_processing()
    else:
        print("\n⚠️ BIDV 3840.ods not found. Skipping full file test.")
