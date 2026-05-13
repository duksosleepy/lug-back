"""
Excel comparison processor for comparing two Excel files.

Compares two Excel files based on composite key [Mã đơn hàng + Doanh Thu]
and returns records that exist only in the first file (Gmail file).
"""

from io import BytesIO
from typing import BinaryIO

import pandas as pd
from loguru import logger

from src.util.date_utils import format_ngay_ct


class ExcelComparisonProcessor:
    """
    Processor for comparing two Excel files and identifying differences.

    Compares files based on composite key: Mã đơn hàng + Doanh Thu
    Returns records that exist only in the first file (Gmail file).
    """

    def __init__(self, gmail_file: BinaryIO, system_file: BinaryIO):
        """
        Initialize the comparison processor.

        Args:
            gmail_file: Binary IO stream of the Gmail Excel file
            system_file: Binary IO stream of the System Excel file
        """
        self.gmail_file: BinaryIO = gmail_file
        self.system_file: BinaryIO = system_file

    def process_to_buffer(self, output_buffer: BinaryIO) -> dict[str, int]:
        """
        Process the comparison and write results to output buffer.

        Args:
            output_buffer: Binary IO stream to write the result Excel file

        Returns:
            dict containing:
                - only_gmail_count: Number of records only in Gmail file
                - total_gmail: Total records in Gmail file
                - total_system: Total records in System file
        """
        try:
            # Read both Excel files
            df_gmail = pd.read_excel(self.gmail_file)
            df_system = pd.read_excel(self.system_file)

            logger.info(f"Gmail file: {len(df_gmail)} records")
            logger.info(f"System file: {len(df_system)} records")
            logger.info(f"Gmail columns: {df_gmail.columns.tolist()}")
            logger.info(f"System columns: {df_system.columns.tolist()}")

            # Store original Gmail column names to restore later
            original_gmail_columns = df_gmail.columns.tolist()

            # Rename columns for consistency (handle different column name variations)
            # This is needed for comparison logic but we'll restore original names later
            column_mapping = {"Doanh thu": "Doanh Thu"}
            df_gmail_renamed = df_gmail.rename(columns=column_mapping)

            # Convert columns to string and handle NaN
            df_gmail_renamed["Mã đơn hàng"] = (
                df_gmail_renamed["Mã đơn hàng"].fillna("").astype(str)
            )
            df_gmail_renamed["Doanh Thu"] = (
                df_gmail_renamed["Doanh Thu"].fillna(0).astype(float)
            )

            df_system["Mã đơn hàng"] = (
                df_system["Mã đơn hàng"].fillna("").astype(str)
            )
            df_system["Doanh Thu"] = df_system["Doanh Thu"].fillna(0).astype(float)

            # Create composite key: Mã đơn hàng + Doanh Thu
            df_gmail_renamed["key"] = (
                df_gmail_renamed["Mã đơn hàng"]
                + "|"
                + df_gmail_renamed["Doanh Thu"].astype(str)
            )
            df_system["key"] = (
                df_system["Mã đơn hàng"] + "|" + df_system["Doanh Thu"].astype(str)
            )

            gmail_keys = set(df_gmail_renamed["key"])
            system_keys = set(df_system["key"])

            only_gmail_keys = gmail_keys - system_keys

            logger.info(f"Unique keys in Gmail: {len(gmail_keys)}")
            logger.info(f"Unique keys in System: {len(system_keys)}")
            logger.info(f"Keys in both: {len(gmail_keys & system_keys)}")
            logger.info(f"Keys only in Gmail: {len(only_gmail_keys)}")

            # Get actual records that exist only in Gmail file
            only_in_gmail = df_gmail_renamed[
                df_gmail_renamed["key"].isin(only_gmail_keys)
            ].copy()

            # Remove the temporary 'key' column before exporting
            only_in_gmail = only_in_gmail.drop(columns=["key"])

            # Restore original Gmail column names (reverse the mapping)
            reverse_mapping = {v: k for k, v in column_mapping.items()}
            only_in_gmail = only_in_gmail.rename(columns=reverse_mapping)

            # Sort by Mã đơn hàng for better readability
            only_in_gmail_sorted = only_in_gmail.sort_values(by="Mã đơn hàng")

            logger.info(
                f"Records only in Gmail file: {len(only_in_gmail_sorted)}"
            )

            # Create a display dataframe for proper date formatting
            display_df = only_in_gmail_sorted.copy()

            # Process "Ngày Ct" column to ensure DD/MM/YYYY format
            if "Ngày Ct" in display_df.columns:
                display_df["Ngày Ct"] = format_ngay_ct(display_df["Ngày Ct"])
                logger.info("Formatted 'Ngày Ct' column to DD/MM/YYYY format")

            # Write to Excel buffer - cast to BytesIO to satisfy type checker
            output_bytes = BytesIO() if not isinstance(output_buffer, BytesIO) else output_buffer
            with pd.ExcelWriter(output_bytes, engine="openpyxl") as writer:
                display_df.to_excel(
                    writer, index=False, sheet_name="Only in Gmail"
                )

            # Copy content back to original buffer if needed
            if output_bytes is not output_buffer:
                output_buffer.write(output_bytes.getvalue())

            # Return statistics
            return {
                "only_gmail_count": len(only_in_gmail_sorted),
                "total_gmail": len(df_gmail),
                "total_system": len(df_system),
            }

        except Exception as e:
            logger.error(f"Error during Excel comparison: {str(e)}", exc_info=True)
            raise
