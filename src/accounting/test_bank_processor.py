#!/usr/bin/env python3
"""
Test script for bank statement processor
"""

import io

import pandas as pd

from src.accounting.bank_statement_processor import (
    BankStatementProcessor,
)


def create_test_data():
    """Create test bank statement data"""
    test_data = [
        {
            "reference": "0001NFS4-7YJO3BV08",
            "datetime": "01/03/2025 11:17:53",
            "debit": 0,
            "credit": 888000,
            "balance": 112028712,
            "description": "TT POS 14100333 500379 5777 064217PDO",
        },
        {
            "reference": "0002ABC1-XYZ123",
            "datetime": "02/03/2025 14:30:22",
            "debit": 50000,
            "credit": 0,
            "balance": 111978712,
            "description": "PHI DICH VU INTERNET BANKING",
        },
        {
            "reference": "0003DEF2-UVW456",
            "datetime": "03/03/2025 09:15:10",
            "debit": 0,
            "credit": 25000,
            "balance": 112003712,
            "description": "LAI TIEN GUI THANG 02/2025",
        },
    ]

    # Create test Excel file in memory
    df = pd.DataFrame(test_data)
    buffer = io.BytesIO()
    with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
        df.to_excel(writer, index=False)
    buffer.seek(0)

    return buffer


def test_transaction_matching():
    """Test transaction rule matching"""
    processor = BankStatementProcessor()
    if not processor.connect():
        print("❌ Database connection failed")
        return False

    test_cases = [
        ("TT POS 14100333 500379 5777 064217PDO", True, "BC"),
        ("PHI DICH VU INTERNET BANKING", False, "BN"),
        ("LAI TIEN GUI THANG 02/2025", True, "BC"),
        ("CHUYEN KHOAN DEN TK 123456", True, "BC"),
        ("CHUYEN KHOAN DI TK 789012", False, "BN"),
    ]

    print("🔍 Testing transaction rule matching...")
    for description, is_credit, expected_doc_type in test_cases:
        match = processor.match_transaction_rule(description, is_credit)
        if match and match.document_type == expected_doc_type:
            print(f"✅ '{description}' -> {match.document_type}")
        else:
            print(
                f"❌ '{description}' -> {match.document_type if match else 'No match'} (expected {expected_doc_type})"
            )

    processor.close()
    return True


def test_pos_extraction():
    """Test POS code extraction"""
    processor = BankStatementProcessor()

    test_cases = [
        ("TT POS 14100333 500379 5777", "14100333"),
        ("TT POS 1234567 ABCDEF", "1234567"),
        ("THANH TOAN POS12345678", "12345678"),
        ("No POS here", None),
    ]

    print("\n🏪 Testing POS code extraction...")
    for description, expected in test_cases:
        result = processor.extract_pos_code(description)
        if result == expected:
            print(f"✅ '{description}' -> {result}")
        else:
            print(f"❌ '{description}' -> {result} (expected {expected})")


def test_full_processing():
    """Test full processing pipeline"""
    print("\n📊 Testing full processing pipeline...")

    # Create test data
    test_buffer = create_test_data()

    # Process
    processor = BankStatementProcessor()
    try:
        result_df = processor.process_to_saoke(test_buffer)

        if not result_df.empty:
            print(f"✅ Processed {len(result_df)} transactions successfully")
            print("\nSample output:")
            print(
                result_df[
                    [
                        "document_type",
                        "date",
                        "document_number",
                        "description",
                        "amount1",
                    ]
                ].head()
            )

            # Save test output
            result_df.to_excel("test_saoke_output.xlsx", index=False)
            print("📄 Test output saved to 'test_saoke_output.xlsx'")

        else:
            print("❌ No transactions processed")

    except Exception as e:
        print(f"❌ Processing failed: {e}")
        import traceback

        traceback.print_exc()


def validate_database_setup():
    """Validate database has required tables and data"""
    print("🗄️  Validating database setup...")

    processor = BankStatementProcessor()
    if not processor.connect():
        print("❌ Database connection failed")
        return False

    try:
        # Check required tables exist
        required_tables = [
            "transaction_rules",
            "pos_machines",
            "counterparties",
            "departments",
            "accounts",
        ]

        for table in required_tables:
            processor.cursor.execute(f"SELECT COUNT(*) FROM {table}")
            count = processor.cursor.fetchone()[0]
            print(f"📋 {table}: {count} records")

        # Check if we have transaction rules
        processor.cursor.execute(
            "SELECT COUNT(*) FROM transaction_rules WHERE is_active = 1"
        )
        active_rules = processor.cursor.fetchone()[0]

        if active_rules == 0:
            print(
                "⚠️  No active transaction rules found. Run setup_default_transaction_rules()"
            )
            return False

        print("✅ Database validation passed")
        return True

    except Exception as e:
        print(f"❌ Database validation failed: {e}")
        return False
    finally:
        processor.close()


if __name__ == "__main__":
    print("🚀 Starting bank statement processor tests...\n")

    # Run tests
    if validate_database_setup():
        test_pos_extraction()
        test_transaction_matching()
        test_full_processing()
        print("\n✨ All tests completed!")
    else:
        print(
            "\n❌ Database setup validation failed. Please check your database."
        )
