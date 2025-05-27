#!/usr/bin/env python3
"""
Quick test script to analyze your BIDV 3840.ods file

This script will:
1. Show you the file structure
2. Detect where the transaction data is located
3. Extract and display sample transactions
4. Show you exactly what data areas were found

Just run: python quick_test.py
"""

import sys
from pathlib import Path

import pandas as pd


def normalize_text(text: str) -> str:
    """Normalize Vietnamese text for comparison"""
    if pd.isna(text) or not isinstance(text, str):
        return ""

    text = str(text).lower().strip()

    # Remove Vietnamese diacritics
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


def analyze_bidv_file(file_path):
    """Analyze BIDV bank statement file"""

    print("🏦 BIDV Bank Statement File Analyzer")
    print("=" * 50)

    # Check if file exists
    if not Path(file_path).exists():
        print(f"❌ File not found: {file_path}")
        return

    try:
        # Get sheet names
        excel_file = pd.ExcelFile(file_path, engine="calamine")
        sheets = excel_file.sheet_names
        print(f"📋 Found {len(sheets)} sheet(s): {sheets}")

        # Analyze first sheet (or specify which one)
        sheet_name = sheets[0]
        print(f"\n🔍 Analyzing sheet: '{sheet_name}'")

        # Read entire sheet without headers
        df = pd.read_excel(
            file_path, sheet_name=sheet_name, header=None, engine="calamine"
        )
        print(
            f"📊 Sheet dimensions: {len(df)} rows × {len(df.columns)} columns"
        )

        # Show raw structure (first 15 rows)
        print("\n📄 Raw content (first 15 rows):")
        print("-" * 80)
        for idx in range(min(15, len(df))):
            row_data = []
            for col_idx in range(
                min(8, len(df.columns))
            ):  # Show first 8 columns
                cell_value = df.iloc[idx, col_idx]
                if pd.isna(cell_value):
                    cell_str = "[empty]"
                else:
                    cell_str = str(cell_value)[:20]  # Truncate long values
                row_data.append(cell_str.ljust(20))
            print(f"Row {idx:2d}: " + " | ".join(row_data))

        # Look for header patterns
        print("\n🎯 Looking for transaction headers...")
        header_patterns = {
            "date": ["ngày gd", "ngay gd", "ngày", "ngay", "date"],
            "reference": [
                "số tham chiếu",
                "so tham chieu",
                "mã gd",
                "ma gd",
                "reference",
            ],
            "description": [
                "mô tả",
                "mo ta",
                "diễn giải",
                "dien giai",
                "nội dung",
                "noi dung",
            ],
            "debit": ["ghi nợ", "ghi no", "tiền ra", "tien ra", "debit"],
            "credit": ["ghi có", "ghi co", "tiền vào", "tien vao", "credit"],
            "balance": ["số dư", "so du", "balance"],
        }

        header_row = -1
        column_mapping = {}

        # Search for headers in first 20 rows
        for row_idx in range(min(20, len(df))):
            found_columns = {}

            for col_idx in range(len(df.columns)):
                cell_value = df.iloc[row_idx, col_idx]
                normalized_cell = normalize_text(str(cell_value))

                for col_type, patterns in header_patterns.items():
                    for pattern in patterns:
                        normalized_pattern = normalize_text(pattern)
                        if (
                            normalized_pattern in normalized_cell
                            or normalized_cell in normalized_pattern
                        ):
                            found_columns[col_type] = col_idx
                            break

            if len(found_columns) >= 3:  # Need at least 3 column matches
                header_row = row_idx
                column_mapping = found_columns
                break

        if header_row >= 0:
            print(f"✅ Found header row at: Row {header_row}")
            print("📍 Column mapping:")
            for col_type, col_idx in column_mapping.items():
                header_value = df.iloc[header_row, col_idx]
                print(
                    f"   {col_type.ljust(12)}: Column {col_idx} ('{header_value}')"
                )

            # Find data boundaries
            start_row = header_row + 1
            end_row = len(df)

            # Look for end patterns
            end_patterns = [
                "tổng cộng",
                "tong cong",
                "total",
                "số dư cuối",
                "so du cuoi",
            ]
            for row_idx in range(start_row, len(df)):
                for col_idx in range(len(df.columns)):
                    cell_value = df.iloc[row_idx, col_idx]
                    normalized_cell = normalize_text(str(cell_value))

                    for pattern in end_patterns:
                        if normalize_text(pattern) in normalized_cell:
                            end_row = row_idx
                            print(
                                f"🔚 Found data end at Row {row_idx}: '{cell_value}'"
                            )
                            break
                if end_row != len(df):
                    break

            data_rows = end_row - start_row
            print(
                f"📊 Data area: Rows {start_row}-{end_row} ({data_rows} transaction rows)"
            )

            # Extract sample transactions
            if data_rows > 0:
                print("\n💰 Sample transactions (first 5):")
                print("-" * 80)

                sample_rows = min(5, data_rows)
                for i in range(sample_rows):
                    row_idx = start_row + i
                    print(f"\nTransaction {i + 1} (Row {row_idx}):")

                    for col_type, col_idx in column_mapping.items():
                        value = df.iloc[row_idx, col_idx]
                        print(f"  {col_type.ljust(12)}: {value}")

            # Show extraction summary
            print("\n📈 EXTRACTION SUMMARY")
            print("-" * 30)
            print("✅ Data successfully detected")
            print(
                f"📍 Location: Sheet '{sheet_name}', Rows {start_row}-{end_row}"
            )
            print(f"📊 Total transactions: {data_rows}")
            print(f"🗂️ Columns identified: {len(column_mapping)}")
            print("✅ Ready for processing!")

        else:
            print("❌ Could not detect header row")
            print("💡 The file might have a different format than expected")
            print("💡 Manual inspection may be needed")

            # Show some hints
            print("\n🔍 Look for rows containing these Vietnamese terms:")
            for col_type, patterns in header_patterns.items():
                print(f"   {col_type}: {', '.join(patterns)}")

    except Exception as e:
        print(f"❌ Error analyzing file: {e}")
        import traceback

        traceback.print_exc()


def main():
    # Default file name
    default_file = "BIDV 3840.ods"

    # Check for command line argument
    if len(sys.argv) > 1:
        file_path = sys.argv[1]
    else:
        file_path = default_file

    print(f"🚀 Starting analysis of: {file_path}")
    analyze_bidv_file(file_path)


if __name__ == "__main__":
    main()
