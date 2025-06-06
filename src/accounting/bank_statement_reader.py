import io
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Union

import pandas as pd

from src.util.logging import get_logger

logger = get_logger(__name__)


@dataclass
class DataArea:
    """Represents the detected data area in a bank statement"""

    sheet_name: str
    start_row: int
    end_row: int
    start_col: int
    end_col: int
    column_mapping: Dict[str, int]  # Column name -> column index
    total_rows: int


@dataclass
class BankStatementConfig:
    """Configuration for different bank statement formats"""

    bank_name: str = "BIDV"

    # Vietnamese column headers to look for
    header_patterns: Dict[str, List[str]] = None

    # Patterns that indicate start/end of data
    data_start_patterns: List[str] = None
    data_end_patterns: List[str] = None

    # Required columns
    required_columns: List[str] = None

    def __post_init__(self):
        if self.header_patterns is None:
            self.header_patterns = {
                "reference": [
                    "số tham chiếu",
                    "so tham chieu",
                    "mã gd",
                    "ma gd",
                    "reference",
                    "ref",
                    "số ct",
                    "so ct",
                ],
                "date": [
                    "ngày hl",
                    "ngay hl",
                    "ngày",
                    "ngay",
                    "date",
                    "ngày hiệu lực",
                    "ngay hieu luc",
                ],
                "debit": [
                    "ghi nợ",
                    "ghi no",
                    "tiền ra",
                    "tien ra",
                    "debit",
                    "rút",
                    "rut",
                    "chi",
                ],
                "credit": [
                    "ghi có",
                    "ghi co",
                    "tiền vào",
                    "tien vao",
                    "credit",
                    "nạp",
                    "nap",
                    "thu",
                ],
                "balance": ["số dư", "so du", "balance", "tồn quỹ", "ton quy"],
                "description": [
                    "mô tả",
                    "mo ta",
                    "diễn giải",
                    "dien giai",
                    "nội dung",
                    "noi dung",
                    "description",
                    "detail",
                ],
            }

        if self.data_start_patterns is None:
            self.data_start_patterns = [
                "STT",
                "stt",
                "Ngày HL",
                "ngày hl",
                "Số tham chiếu",
                "so tham chieu",
            ]

        if self.data_end_patterns is None:
            self.data_end_patterns = [
                "tổng cộng",
                "tong cong",
                "total",
                "số dư cuối kỳ",
                "so du cuoi ky",
                "phát sinh trong kỳ",
                "phat sinh trong ky",
                "cộng phát sinh",
                "cong phat sinh",
            ]

        if self.required_columns is None:
            self.required_columns = ["date", "description"]


class BankStatementReader:
    """
    Intelligent reader for bank statement Excel/ODS files that can detect
    the actual data area and extract transaction information.
    """

    def __init__(self, config: Optional[BankStatementConfig] = None):
        self.config = config or BankStatementConfig()
        self.logger = get_logger(__name__)

    def normalize_text(self, text: str) -> str:
        """Normalize Vietnamese text for comparison"""
        if pd.isna(text) or not isinstance(text, str):
            return ""

        # Convert to lowercase and remove diacritics for comparison
        text = str(text).lower().strip()

        # Remove common Vietnamese diacritics for matching
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

    def detect_sheets(self, file_path: Union[str, io.BytesIO]) -> List[str]:
        """Detect and return list of sheet names"""
        try:
            if isinstance(file_path, (str, Path)):
                excel_file = pd.ExcelFile(file_path, engine="calamine")
            else:
                excel_file = pd.ExcelFile(file_path, engine="calamine")

            sheets = excel_file.sheet_names
            self.logger.info(f"Found {len(sheets)} sheets: {sheets}")
            return sheets

        except Exception as e:
            self.logger.error(f"Error detecting sheets: {e}")
            return []

    def find_header_row(self, df: pd.DataFrame) -> Tuple[int, Dict[str, int]]:
        """
        Find the header row and map columns to their positions

        Returns:
            Tuple of (header_row_index, column_mapping)
        """
        column_mapping = {}
        best_row = -1
        best_score = 0

        # Search through first 20 rows for headers
        search_rows = min(20, len(df))

        for row_idx in range(search_rows):
            # Skip rows that don't have a reference column indicator
            has_reference_indicator = False
            for col_idx in range(len(df.columns)):
                cell_value = df.iloc[row_idx, col_idx]
                normalized_cell = self.normalize_text(str(cell_value))
                reference_patterns = [
                    "so tham chieu",
                    "ma gd",
                    "reference",
                    "ref",
                    "so ct",
                ]
                if any(
                    pattern in normalized_cell for pattern in reference_patterns
                ):
                    has_reference_indicator = True
                    break

            if not has_reference_indicator:
                continue

            row_score = 0
            current_mapping = {}

            # Check each column in this row
            for col_idx in range(len(df.columns)):
                cell_value = df.iloc[row_idx, col_idx]
                normalized_cell = self.normalize_text(str(cell_value))

                # Check against each header pattern
                for (
                    column_type,
                    patterns,
                ) in self.config.header_patterns.items():
                    for pattern in patterns:
                        normalized_pattern = self.normalize_text(pattern)
                        if (
                            normalized_pattern in normalized_cell
                            or normalized_cell in normalized_pattern
                        ):
                            current_mapping[column_type] = col_idx
                            row_score += 1
                            self.logger.debug(
                                f"Found {column_type} at row {row_idx}, col {col_idx}: '{cell_value}'"
                            )
                            break

            # This row is better if it has more matching columns
            if row_score > best_score:
                best_score = row_score
                best_row = row_idx
                column_mapping = current_mapping.copy()

        self.logger.info(f"Best header row: {best_row} with score {best_score}")
        self.logger.info(f"Column mapping: {column_mapping}")

        return best_row, column_mapping

    def find_data_boundaries(
        self, df: pd.DataFrame, header_row: int
    ) -> Tuple[int, int]:
        """
        Find the start and end of actual transaction data

        Returns:
            Tuple of (start_row, end_row)
        """
        start_row = header_row + 1
        end_row = len(df)

        # Skip rows that contain "**Số tham chiếu" or similar patterns
        skip_patterns = [
            "**số tham chiếu",
            "**so tham chieu",
            "**số ct",
            "**số chứng từ",
        ]

        # Find the actual start of data by skipping invalid rows
        for row_idx in range(
            start_row, min(start_row + 10, len(df))
        ):  # Check next 10 rows
            should_skip = False
            for col_idx in range(len(df.columns)):
                cell_value = df.iloc[row_idx, col_idx]
                if pd.notna(cell_value) and isinstance(cell_value, str):
                    cell_lower = self.normalize_text(str(cell_value))
                    if any(pattern in cell_lower for pattern in skip_patterns):
                        should_skip = True
                        self.logger.info(
                            f"Skipping row {row_idx} with '**Số tham chiếu' pattern"
                        )
                        start_row = row_idx + 1  # Skip this row
                        break
            if not should_skip:
                # If row has a date, it's likely a valid data row
                has_date = False
                for col_idx in range(len(df.columns)):
                    cell_value = df.iloc[row_idx, col_idx]
                    if pd.notna(cell_value):
                        try:
                            # Try to parse as date
                            if isinstance(cell_value, str) and (
                                "/" in cell_value or "-" in cell_value
                            ):
                                pd.to_datetime(cell_value, errors="raise")
                                has_date = True
                                break
                        except:
                            pass
                if has_date:
                    break  # Found a valid row with date

        # Look for data end patterns
        for row_idx in range(start_row, len(df)):
            for col_idx in range(len(df.columns)):
                cell_value = df.iloc[row_idx, col_idx]
                normalized_cell = self.normalize_text(str(cell_value))

                # Check if this row contains end patterns
                for pattern in self.config.data_end_patterns:
                    normalized_pattern = self.normalize_text(pattern)
                    if normalized_pattern in normalized_cell:
                        end_row = row_idx
                        self.logger.info(
                            f"Found data end at row {row_idx}: '{cell_value}'"
                        )
                        return start_row, end_row

        # If no explicit end found, look for empty rows or rows with mostly empty cells
        for row_idx in range(start_row, len(df)):
            non_empty_cells = 0
            for col_idx in range(len(df.columns)):
                if (
                    pd.notna(df.iloc[row_idx, col_idx])
                    and str(df.iloc[row_idx, col_idx]).strip()
                ):
                    non_empty_cells += 1

            # If less than 2 cells have data, consider this the end
            if non_empty_cells < 2:
                end_row = row_idx
                self.logger.info(f"Found data end at empty row {row_idx}")
                break

        self.logger.info(f"Data boundaries: rows {start_row} to {end_row}")
        return start_row, end_row

    def detect_data_area(
        self,
        file_path: Union[str, io.BytesIO],
        sheet_name: Optional[str] = None,
    ) -> Optional[DataArea]:
        """
        Detect the data area containing transaction information

        Returns:
            DataArea object with detected boundaries and column mapping
        """
        try:
            # Detect sheets if not specified
            if sheet_name is None:
                sheets = self.detect_sheets(file_path)
                if not sheets:
                    raise ValueError("No sheets found in file")
                sheet_name = sheets[0]  # Use first sheet by default
                self.logger.info(f"Using sheet: {sheet_name}")

            # Read the entire sheet first
            df = pd.read_excel(
                file_path, sheet_name=sheet_name, header=None, engine="calamine"
            )

            self.logger.info(
                f"Sheet '{sheet_name}' has {len(df)} rows and {len(df.columns)} columns"
            )

            # Find header row and column mapping
            header_row, column_mapping = self.find_header_row(df)

            if header_row == -1:
                self.logger.warning("No header row found")
                return None

            # Check if we have required columns
            missing_columns = []
            for required_col in self.config.required_columns:
                if required_col not in column_mapping:
                    missing_columns.append(required_col)

            if missing_columns:
                self.logger.warning(
                    f"Missing required columns: {missing_columns}"
                )
                # Continue anyway, might still be able to process

            # Find data boundaries
            start_row, end_row = self.find_data_boundaries(df, header_row)

            # Determine column range
            used_columns = list(column_mapping.values())
            start_col = min(used_columns) if used_columns else 0
            end_col = max(used_columns) + 1 if used_columns else len(df.columns)

            data_area = DataArea(
                sheet_name=sheet_name,
                start_row=start_row,
                end_row=end_row,
                start_col=start_col,
                end_col=end_col,
                column_mapping=column_mapping,
                total_rows=end_row - start_row,
            )

            self.logger.info(
                f"Detected data area: {data_area.total_rows} rows, "
                f"columns {start_col}-{end_col}"
            )

            return data_area

        except Exception as e:
            self.logger.error(f"Error detecting data area: {e}")
            return None

    def extract_transactions(
        self,
        file_path: Union[str, io.BytesIO],
        data_area: Optional[DataArea] = None,
    ) -> pd.DataFrame:
        """
        Extract transaction data from the detected area

        Returns:
            DataFrame with transaction data and standardized column names
        """
        try:
            if data_area is None:
                data_area = self.detect_data_area(file_path)
                if data_area is None:
                    raise ValueError("Could not detect data area")

            # Read only the data area
            df = pd.read_excel(
                file_path,
                sheet_name=data_area.sheet_name,
                skiprows=data_area.start_row,
                nrows=data_area.total_rows,
                usecols=range(data_area.start_col, data_area.end_col),
                header=None,
                engine="calamine",
            )

            # Create standardized column names based on mapping
            standard_columns = {}
            for col_type, col_idx in data_area.column_mapping.items():
                # Adjust column index relative to start_col
                adjusted_idx = col_idx - data_area.start_col
                if 0 <= adjusted_idx < len(df.columns):
                    standard_columns[adjusted_idx] = col_type

            # Rename columns
            df = df.rename(columns=standard_columns)

            # Clean up the data
            df = self.clean_transaction_data(df)

            self.logger.info(f"Extracted {len(df)} transaction rows")
            return df

        except Exception as e:
            self.logger.error(f"Error extracting transactions: {e}")
            raise

    def clean_transaction_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean and standardize transaction data"""
        try:
            # Remove completely empty rows
            df = df.dropna(how="all")

            # Parse dates if date column exists
            if "date" in df.columns:
                df["date"] = pd.to_datetime(df["date"], errors="coerce")
                # Remove rows with invalid dates
                df = df.dropna(subset=["date"])

            # Clean amount columns
            for col in ["debit", "credit", "balance"]:
                if col in df.columns:
                    # Remove commas and convert to numeric
                    df[col] = (
                        df[col]
                        .astype(str)
                        .str.replace(",", "")
                        .str.replace(" ", "")
                    )
                    df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

            # Clean text columns
            text_columns = ["description", "reference"]
            for col in text_columns:
                if col in df.columns:
                    df[col] = df[col].astype(str).str.strip()
                    df[col] = df[col].replace("nan", "")

            # Filter out rows that don't look like transactions
            if "description" in df.columns:
                # Remove rows with empty descriptions
                df = df[df["description"].str.len() > 0]

            self.logger.info(f"Cleaned data: {len(df)} valid transaction rows")
            return df

        except Exception as e:
            self.logger.error(f"Error cleaning data: {e}")
            return df

    def read_bank_statement(
        self,
        file_path: Union[str, io.BytesIO],
        sheet_name: Optional[str] = None,
        debug: bool = False,
    ) -> pd.DataFrame:
        """
        Main method to read and parse bank statement

        Args:
            file_path: Path to Excel/ODS file or BytesIO object
            sheet_name: Specific sheet to read (auto-detect if None)
            debug: Whether to save debug information

        Returns:
            DataFrame with standardized transaction data
        """
        try:
            self.logger.info(f"Reading bank statement from {file_path}")

            # Detect data area
            data_area = self.detect_data_area(file_path, sheet_name)
            if data_area is None:
                raise ValueError("Could not detect data area in bank statement")

            if debug:
                self.logger.info(f"DEBUG - Data area detected: {data_area}")

            # Extract transaction data
            df = self.extract_transactions(file_path, data_area)

            if debug:
                # Save debug information
                debug_info = {
                    "data_area": data_area.__dict__,
                    "sample_data": df.head().to_dict(),
                    "column_info": {
                        col: str(df[col].dtype) for col in df.columns
                    },
                    "total_rows": len(df),
                }
                self.logger.info(f"DEBUG - Extraction info: {debug_info}")

            return df

        except Exception as e:
            self.logger.error(f"Error reading bank statement: {e}")
            raise


# Test function to validate the reader
def test_bank_statement_reader():
    """Test the bank statement reader with sample data"""
    import tempfile

    # Create sample data that mimics BIDV format
    sample_data = [
        ["BIDV Bank Statement", "", "", "", "", ""],
        ["Account: 38400001234567", "", "", "", "", ""],
        ["Period: 01/03/2025 - 31/03/2025", "", "", "", "", ""],
        ["", "", "", "", "", ""],
        [
            "STT",
            "Ngày HL",
            "Số tham chiếu",
            "Mô tả",
            "Ghi nợ",
            "Ghi có",
            "Số dư",
        ],
        [
            "1",
            "01/03/2025",
            "0001NFS4-7YJO3BV08",
            "TT POS 14100333 500379 5777",
            "",
            "888,000",
            "112,028,712",
        ],
        [
            "2",
            "02/03/2025",
            "0002ABC1-XYZ123",
            "PHI DICH VU INTERNET BANKING",
            "50,000",
            "",
            "111,978,712",
        ],
        [
            "3",
            "03/03/2025",
            "0003DEF2-UVW456",
            "LAI TIEN GUI THANG 02/2025",
            "",
            "25,000",
            "112,003,712",
        ],
        ["", "", "", "", "", "", ""],
        ["Tổng cộng", "", "", "", "50,000", "913,000", ""],
        ["Số dư cuối kỳ: 112,003,712", "", "", "", "", "", ""],
    ]

    # Create temporary Excel file
    df_sample = pd.DataFrame(sample_data)

    with tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False) as tmp:
        df_sample.to_excel(tmp.name, index=False, header=False)

        # Test the reader
        reader = BankStatementReader()
        result_df = reader.read_bank_statement(tmp.name, debug=True)

        print("Test Results:")
        print(f"Extracted {len(result_df)} transactions")
        print(result_df)

        return result_df


if __name__ == "__main__":
    test_bank_statement_reader()
