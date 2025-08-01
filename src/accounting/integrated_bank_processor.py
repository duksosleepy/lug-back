#!/usr/bin/env python3
"""
Integrated Bank Statement Processor with Enhanced Account Determination and Counterparty Extraction

This module processes bank statements (BIDV) to convert them to 'saoke' format
for accounting purposes, using dynamic account determination and counterparty extraction.

Key features:
- Intelligent account determination based on transaction metadata
- Counterparty extraction from transaction descriptions
- Bank account detection from statement headers
- Decoupled design with fast_search for database operations
- Enhanced pattern matching for POS terminals, departments, and transaction types
"""

import io
import json
import re
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Union

import pandas as pd

# Add the project root to the path if running as main script
if __name__ == "__main__":
    project_root = Path(__file__).parent.parent.parent
    sys.path.insert(0, str(project_root))

from src.accounting.bank_statement_reader import BankStatementReader
from src.accounting.counterparty_extractor import CounterpartyExtractor
from src.util.logging import get_logger

logger = get_logger(__name__)


@dataclass
class RawTransaction:
    """Represents a raw bank statement transaction"""

    reference: str
    datetime: datetime
    debit_amount: float
    credit_amount: float
    balance: float
    description: str


@dataclass
class TransactionRule:
    """Represents a rule for matching and categorizing transactions"""

    id: int
    pattern: str
    document_type: str
    transaction_type: Optional[str]
    counterparty_code: Optional[str]
    department_code: Optional[str]
    cost_code: Optional[str]
    description_template: Optional[str]
    is_credit: bool
    priority: int


@dataclass
class AccountMatch:
    """Represents a matched account from the database"""

    code: str
    name: str
    numeric_code: str
    position: int  # Position in the description where the code was found


@dataclass
class TransferInfo:
    """Information extracted from transfer transactions"""

    from_account: Optional[str] = None
    to_account: Optional[str] = None
    from_code: Optional[str] = None
    to_code: Optional[str] = None


@dataclass
class SaokeEntry:
    """Represents a processed saoke accounting entry"""

    # Required fields
    document_type: str  # BC (credit) or BN (debit)
    date: str  # Format: DD/MM/YYYY
    document_number: str  # Format: BC03/001
    currency: str  # VND
    exchange_rate: float  # 1.0
    counterparty_code: str  # KL-GOBARIA1
    counterparty_name: str  # Khách Lẻ Không Lấy Hóa Đơn
    address: str  # Full address
    description: str  # Thu tiền bán hàng khách lẻ (POS code - location)
    original_description: str  # Original bank statement description
    amount1: float  # Transaction amount
    amount2: float  # Same as amount1 for standard transactions
    debit_account: str  # 1121114
    credit_account: str  # 1311
    sequence: int = 1  # Default sequence number

    # Optional fields
    cost_code: Optional[str] = None
    department: Optional[str] = None  # GO BRVT
    invoice_number: Optional[str] = None
    invoice_date: Optional[str] = None
    date2: Optional[str] = None  # Alternative date format MM/DD/YYYY
    transaction_type: Optional[str] = None  # Transaction type (SALE, FEE, etc.)

    # Metadata fields
    raw_transaction: Optional[RawTransaction] = None

    def as_dict(self) -> Dict:
        """Convert entry to dictionary for DataFrame creation"""
        return {
            "document_type": self.document_type,
            "date": self.date,
            "document_number": self.document_number,
            "currency": self.currency,
            "exchange_rate": self.exchange_rate,
            "sequence": self.sequence,
            "counterparty_code": self.counterparty_code,
            "counterparty_name": self.counterparty_name,
            "address": self.address,
            "description": self.description,
            "original_description": self.original_description,
            "amount1": self.amount1,
            "amount2": self.amount2,
            "debit_account": self.debit_account,
            "credit_account": self.credit_account,
            "cost_code": self.cost_code,
            "department": self.department,
            "invoice_number": self.invoice_number,
            "invoice_date": self.invoice_date,
            "date2": self.date2,
            "transaction_type": self.transaction_type,
        }


class IntegratedBankProcessor:
    """
    Integrated processor with enhanced account determination and counterparty extraction
    """

    def __init__(self, db_path: str = "banking_enterprise.db"):
        """Initialize the processor"""
        self.db_path = Path(__file__).parent / db_path
        self.reader = BankStatementReader()
        self.logger = logger

        # Initialize the counterparty extractor
        self.counterparty_extractor = CounterpartyExtractor(
            db_path=str(self.db_path)
        )

        # Bank information tracking
        self.current_bank_name = ""
        self.current_bank_info = {}

        # Flag for tracking special interest payment transactions
        self._current_transaction_is_interest_payment = False

        # Load bank information from banks.json
        self.banks_data = []
        self._load_bank_info()

        # Document number counters
        self.doc_counters = {"BC": 1, "BN": 1}

        # Caches for lookups
        self.account_cache = {}  # Cache for account lookups
        self.rule_cache = {}

        # Default bank account (will be overridden if extracted from header)
        self.default_bank_account = "1121114"

        # Patterns for extracting numeric codes
        self.numeric_patterns = [
            # Account numbers (10+ digits)
            (r"\b(\d{10,})\b", "account_number"),
            # Medium codes (6-9 digits)
            (r"\b(\d{6,9})\b", "medium_code"),
            # Short codes (4-5 digits)
            (r"\b(\d{4,5})\b", "short_code"),
            # POS codes specifically
            (r"POS\s*(\d{7,8})", "pos_code"),
            # Codes after specific keywords
            (r"TK\s+(\d+)", "account_ref"),
            (r"STK\s+(\d+)", "account_ref"),
            (r"tai\s+khoan\s+(\d+)", "account_ref"),
            (r"so\s+TK\s+(\d+)", "account_ref"),
        ]

        # Transfer patterns to identify source and destination
        self.transfer_patterns = [
            # "from account X to account Y" patterns
            (
                r"tu\s+(?:TK\s+)?(?:BIDV\s+)?(\d+).*?(?:qua|sang|den)\s+(?:TK\s+)?(?:BIDV\s+)?(\d+)",
                "from_to",
            ),
            (
                r"chuyen\s+tien\s+tu\s+(?:TK\s+)?(\d+).*?(?:den|sang)\s+(?:TK\s+)?(\d+)",
                "from_to",
            ),
            (
                r"from\s+(?:account\s+)?(\d+).*?to\s+(?:account\s+)?(\d+)",
                "from_to",
            ),
            # "CK from X" or "CK to Y" patterns
            (r"CK\s+tu\s+(?:TK\s+)?(\d+)", "from"),
            (r"CK\s+den\s+(?:TK\s+)?(\d+)", "to"),
            (r"nhan\s+tu\s+(?:TK\s+)?(\d+)", "from"),
            (r"chuyen\s+den\s+(?:TK\s+)?(\d+)", "to"),
        ]

        # Transaction type patterns
        self.transaction_type_patterns = {
            "SALE": [r"ban\s*hang", r"thanh\s*toan", r"mua", r"pos"],
            "FEE": [r"phi", r"fee", r"service\s*charge"],
            "INTEREST": [r"lai", r"interest", r"lãi\s*suất"],
            "TRANSFER": [r"chuyen\s*khoan", r"transfer", r"ck"],
            "WITHDRAWAL": [r"rut\s*tien", r"withdrawal", r"atm"],
            "DEPOSIT": [r"nap\s*tien", r"deposit", r"gui\s*tien"],
        }

        # POS patterns
        self.pos_patterns = [
            r"POS\s+(\d{7,8})",  # POS followed by 7-8 digits
            r"POS(\d{7,8})",  # POS directly followed by digits
            r"POS[\s\-:_]+(\d{7,8})",  # POS with various separators
        ]

        # Location patterns
        self.location_patterns = [
            r"GO\s+([A-Z]+)",  # GO followed by uppercase letters
            r"GO([A-Z]+)",  # GO directly followed by uppercase letters
            r"CH\s+([A-Z0-9]+)",  # CH (cửa hàng) followed by code
        ]

        # Special account mappings for specific Vietnamese keywords
        self.special_account_mappings = {
            "tiền cọc": "244",  # Deposit money
            "tiền lãi": "811",  # Interest income
            "PHI QUAN LY TAI KHOAN": "6427",  # Account management fee
            "NOPTHUE": "333823",  # Tax payment
            "Thanh toan lai": "5154",  # Interest payment
            "thanh toan tien hang": "13682",  # Goods payment
            "hoan tien": "1311",
        }

    def _load_bank_info(self):
        """Load bank information from _banks.json"""
        try:
            banks_json_path = Path(__file__).parent / "_banks.json"
            if banks_json_path.exists():
                with open(banks_json_path, "r", encoding="utf-8") as f:
                    self.banks_data = json.load(f)
                self.logger.info(
                    f"Loaded {len(self.banks_data)} banks from _banks.json"
                )
            else:
                self.logger.warning(
                    f"_banks.json not found at {banks_json_path}"
                )
                self.banks_data = []
        except Exception as e:
            self.logger.error(f"Error loading bank info: {e}")
            self.banks_data = []

    def extract_bank_from_filename(self, filename: str) -> Optional[str]:
        """Extract bank name from filename"""
        if not filename:
            return None

        filename_upper = filename.upper()

        # Common bank name patterns in filenames
        bank_patterns = [
            "BIDV",
            "VCB",
            "VIETCOMBANK",
            "ACB",
            "TECHCOMBANK",
            "TPB",
            "TPBANK",
            "SACOMBANK",
            "SCB",
            "MBB",
            "MBBANK",
            "VIB",
            "SHB",
            "EXIMBANK",
            "HDBANK",
            "OCB",
            "VPBANK",
            "AGRIBANK",
            "VIETINBANK",
        ]

        for pattern in bank_patterns:
            if pattern in filename_upper:
                # Map some common variations to standard names
                if pattern in ["VIETCOMBANK"]:
                    return "VCB"
                elif pattern in ["TECHCOMBANK"]:
                    return "TCB"
                elif pattern in ["TPBANK"]:
                    return "TPB"
                elif pattern in ["MBBANK"]:
                    return "MBB"
                elif pattern in ["EXIMBANK"]:
                    return "EIB"
                elif pattern in ["VIETINBANK"]:
                    return "ICB"
                else:
                    return pattern

        return None

    def get_bank_config_for_bank(self, bank_short_name: str):
        """Get bank configuration for processing"""
        try:
            from src.accounting.bank_configs import get_bank_config

            return get_bank_config(bank_short_name)
        except Exception as e:
            self.logger.error(
                f"Error getting bank config for {bank_short_name}: {e}"
            )
            return None

    def get_bank_info_by_name(self, bank_name: str) -> Dict:
        """Get bank information by short name

        Args:
            bank_name: Bank name or code to search for

        Returns:
            Dictionary with bank information if found, None otherwise
        """
        if not bank_name:
            return None

        bank_name_upper = bank_name.upper()

        # First try exact match on short_name
        for bank in self.banks_data:
            if bank_name_upper == bank.get("short_name", "").upper():
                self.logger.info(
                    f"Found exact bank match: {bank.get('short_name')}"
                )
                return bank

        # If not found by short_name, try more flexible matching
        for bank in self.banks_data:
            bank_full_name = bank.get("name", "").upper()
            short_name = bank.get("short_name", "").upper()

            if (
                bank_name_upper in bank_full_name
                or bank_name_upper in short_name
            ):
                self.logger.info(
                    f"Found partial bank match: {bank.get('short_name')}"
                )
                return bank

        self.logger.warning(
            f"Could not find bank information for '{bank_name}'"
        )
        return None

    def set_bank_from_filename(self, filename: str) -> bool:
        """Set current bank based on filename pattern (e.g., 'BIDV 3840.ods')"""
        if not filename:
            return False

        # Extract bank name from filename
        potential_bank_name = self.extract_bank_from_filename(filename)
        if not potential_bank_name:
            return False

        # Get bank info
        bank_info = self.get_bank_info_by_name(potential_bank_name)
        if bank_info:
            self.current_bank_name = bank_info.get("short_name", "")
            self.current_bank_info = bank_info
            self.logger.info(
                f"Set current bank to {self.current_bank_name} from filename"
            )
            return True

        self.logger.warning(
            f"Could not match bank name '{potential_bank_name}' from filename"
        )
        return False

    def connect(self) -> bool:
        """Initialize the processor without database connection"""
        try:
            # No need to connect to the database anymore
            self.logger.info(
                "Initializing processor with fast_search instead of database"
            )

            # Pre-load all accounts into cache for faster lookups
            self._load_accounts_cache()

            return True
        except Exception as e:
            self.logger.error(f"Initialization error: {e}")
            return False

    def close(self):
        """No connection to close"""
        # Nothing to do as we're not using a database connection
        pass

    def _load_accounts_cache(self):
        """Pre-load all accounts into memory for faster searching using fast_search instead of SQL"""
        try:
            from src.accounting.fast_search import search_accounts

            # Get all accounts using multiple search strategies to ensure we get all accounts
            self.logger.info(
                "Loading accounts cache using multiple search strategies..."
            )

            all_accounts = []

            # Strategy 1: Try common account prefixes to get comprehensive results
            account_prefixes = [
                "1",
                "2",
                "3",
                "4",
                "5",
                "6",
                "7",
                "8",
                "9",
                "0",
            ]
            for prefix in account_prefixes:
                try:
                    prefix_accounts = search_accounts(
                        prefix, field_name="code", limit=500
                    )
                    all_accounts.extend(prefix_accounts)
                    self.logger.debug(
                        f"Found {len(prefix_accounts)} accounts with prefix '{prefix}'"
                    )
                except Exception as e:
                    self.logger.warning(
                        f"Error searching accounts with prefix '{prefix}': {e}"
                    )
                    continue

            # Strategy 2: Search by name patterns to catch any missed accounts
            name_patterns = [
                "tai khoan",
                "tien",
                "vay",
                "no",
                "thu",
                "chi",
                "lai",
                "von",
            ]
            for pattern in name_patterns:
                try:
                    name_accounts = search_accounts(
                        pattern, field_name="name", limit=200
                    )
                    all_accounts.extend(name_accounts)
                    self.logger.debug(
                        f"Found {len(name_accounts)} accounts with name pattern '{pattern}'"
                    )
                except Exception as e:
                    self.logger.warning(
                        f"Error searching accounts with name pattern '{pattern}': {e}"
                    )
                    continue

            # Remove duplicates based on account code
            unique_accounts = {}
            for account in all_accounts:
                account_code = account.get("code")
                if account_code and account_code not in unique_accounts:
                    unique_accounts[account_code] = account

            all_accounts = list(unique_accounts.values())
            self.logger.info(
                f"Found {len(all_accounts)} unique accounts after deduplication"
            )

            self.account_cache = {}
            count = 0

            for account in all_accounts:
                account_code = account["code"]
                account_name = account["name"]

                # Extract all numeric codes from the account name
                for pattern, _ in self.numeric_patterns:
                    matches = re.findall(pattern, account_name, re.IGNORECASE)
                    for match in matches:
                        if match not in self.account_cache:
                            self.account_cache[match] = []
                        self.account_cache[match].append(
                            {"code": account_code, "name": account_name}
                        )
                        count += 1

                # Also index by account code itself for direct lookups
                if account_code not in self.account_cache:
                    self.account_cache[account_code] = []
                self.account_cache[account_code].append(
                    {"code": account_code, "name": account_name}
                )
                count += 1

            self.logger.info(
                f"Loaded {len(self.account_cache)} numeric codes from {len(all_accounts)} accounts using fast_search"
            )

        except Exception as e:
            self.logger.error(f"Error loading accounts cache: {e}")
            import traceback

            traceback.print_exc()

            # Fallback: Initialize empty cache to prevent errors
            self.account_cache = {}

    def extract_account_from_header(
        self, file_path: Union[str, io.BytesIO]
    ) -> Optional[str]:
        """
        Extract bank account number from the header of the bank statement

        Args:
            file_path: Path to Excel/ODS file or BytesIO object

        Returns:
            Account code if found, None otherwise
        """
        try:
            # Read the file with pandas to access header information
            if isinstance(file_path, (str, Path)):
                excel_file = pd.ExcelFile(file_path, engine="calamine")
            else:
                excel_file = pd.ExcelFile(file_path, engine="calamine")

            # Get the first sheet
            sheet_name = excel_file.sheet_names[0]

            # Read the first 15 rows to find account number
            header_df = pd.read_excel(
                file_path,
                sheet_name=sheet_name,
                header=None,
                nrows=15,
                engine="calamine",
            )
            # Look for account number pattern in the header rows
            # More specific pattern to match "Số tài khoản: 1410177655"
            # And ACB pattern: "BẢNG SAO KÊ GIAO DỊCH - Số tài khoản (VND): 33388368"
            # And MBBank pattern: "Tài khoản/Account No: 7432085703944"
            account_patterns = [
                r"(?:số tài khoản|so tai khoan|account number|account|tk)[\s:]+([0-9]+)",
                r"(?:stk|account number|account|tk)[\s:]+([0-9]+)",
                r"(?:[0-9]{6,10})[\s\-\._:]+([0-9]{4})",  # Match last 4 digits specifically
                r"\b(3840)\b",  # Match the specific account 3840 directly
                # ACB-specific patterns
                r"bảng sao kê giao dịch.*?số tài khoản.*?\(vnd\):\s*([0-9]+)",
                r"bang sao ke giao dich.*?so tai khoan.*?\(vnd\):\s*([0-9]+)",
                r"bảng sao kê.*?số tài khoản.*?([0-9]+)",
                r"bang sao ke.*?so tai khoan.*?([0-9]+)",
                # MBBank-specific patterns for single-cell format
                r"tài khoản/account no:[\s]*([0-9]+)",
                r"tai khoan/account no:[\s]*([0-9]+)",  # Non-diacritic version
                r"account no:[\s]*([0-9]+)",  # English only version
            ]

            for _, row in header_df.iterrows():
                for col_idx, cell in enumerate(row):
                    if pd.notna(cell) and isinstance(cell, str):
                        # Check if this cell or its neighbors contain account info
                        cell_text = str(cell).lower()

                        # Try exact match first
                        for pattern in account_patterns:
                            match = re.search(pattern, cell_text, re.IGNORECASE)
                            if match:
                                account_number = match.group(1)
                                self.logger.info(
                                    f"Found account number in header: {account_number}"
                                )

                                # First try to use the last 4 digits to match an account
                                if len(account_number) > 4:
                                    last_4_digits = account_number[-4:]
                                    self.logger.info(
                                        f"Using last 4 digits as identifier: {last_4_digits}"
                                    )

                                    # Find account code in database
                                    account_match = self.find_account_by_code(
                                        last_4_digits
                                    )
                                    if account_match:
                                        self.logger.info(
                                            f"Matched account number {last_4_digits} to account {account_match.code}"
                                        )
                                        return account_match.code

                                # Try full account number
                                account_match = self.find_account_by_code(
                                    account_number
                                )
                                if account_match:
                                    self.logger.info(
                                        f"Matched full account number {account_number} to account {account_match.code}"
                                    )
                                    return account_match.code

                        # Check for adjacent cells containing account number (row 9 especially)
                        # Also check for MBBank single-cell format
                        if (
                            "tai khoan" in cell_text
                            or "tài khoản" in cell_text
                            or "stk" in cell_text
                            or "account" in cell_text
                        ):
                            # First check if account number is in the same cell (MBBank format)
                            for pattern in account_patterns:
                                same_cell_match = re.search(
                                    pattern, cell_text, re.IGNORECASE
                                )
                                if same_cell_match:
                                    account_number = same_cell_match.group(1)
                                    self.logger.info(
                                        f"Found account number in same cell: {account_number}"
                                    )

                                    # Use the last 4 digits
                                    last_4_digits = account_number[-4:]
                                    account_match = self.find_account_by_code(
                                        last_4_digits
                                    )
                                    if account_match:
                                        self.logger.info(
                                            f"Matched account number {last_4_digits} to account {account_match.code}"
                                        )
                                        return account_match.code

                            # Then check the next cell for the account number (VCB format)
                            if col_idx + 1 < len(row) and pd.notna(
                                row[col_idx + 1]
                            ):
                                next_cell = str(row[col_idx + 1])
                                # If it's a numeric value, it's likely the account number
                                account_number = re.sub(
                                    r"[^0-9]", "", next_cell
                                )
                                if account_number and len(account_number) >= 4:
                                    self.logger.info(
                                        f"Found account number in adjacent cell: {account_number}"
                                    )

                                    # Use the last 4 digits
                                    last_4_digits = account_number[-4:]
                                    account_match = self.find_account_by_code(
                                        last_4_digits
                                    )
                                    if account_match:
                                        self.logger.info(
                                            f"Matched account number {last_4_digits} to account {account_match.code}"
                                        )
                                        return account_match.code

            # Special case for row 9 which often contains account info
            if len(header_df) > 9:
                row_9 = header_df.iloc[
                    8
                ]  # 0-based index, so row 9 is at index 8
                for col_idx, cell in enumerate(row_9):
                    if pd.notna(cell) and isinstance(cell, str):
                        cell_text = str(cell)
                        # Extract any number sequence with at least 4 digits
                        matches = re.findall(r"\b\d{4,}\b", cell_text)
                        for match in matches:
                            if len(match) >= 4:
                                # Try the last 4 digits
                                last_4_digits = match[-4:]
                                self.logger.info(
                                    f"Found potential account number in row 9: {match}, using last 4 digits: {last_4_digits}"
                                )
                                account_match = self.find_account_by_code(
                                    last_4_digits
                                )
                                if account_match:
                                    self.logger.info(
                                        f"Matched account number {last_4_digits} to account {account_match.code}"
                                    )
                                    return account_match.code

            self.logger.warning("No account number found in header")
            return None

        except Exception as e:
            self.logger.error(
                f"Error extracting account number from header: {e}"
            )
            return None

    def extract_numeric_codes(
        self, description: str
    ) -> List[Tuple[str, str, int]]:
        """
        Extract all numeric codes from a description

        Args:
            description: Transaction description

        Returns:
            List of tuples (code, type, position)
        """
        codes = []
        seen = set()  # Avoid duplicates

        for pattern, code_type in self.numeric_patterns:
            for match in re.finditer(pattern, description, re.IGNORECASE):
                code = match.group(1)
                position = match.start()

                # Skip if we've seen this code
                if code not in seen:
                    seen.add(code)
                    codes.append((code, code_type, position))

        # Sort by position in description
        codes.sort(key=lambda x: x[2])

        return codes

    def find_account_by_code(self, numeric_code: str) -> Optional[AccountMatch]:
        """
        Find account by numeric code in the name column using fast_search instead of SQL

        Args:
            numeric_code: The numeric code to search for

        Returns:
            AccountMatch object or None
        """
        self.logger.debug(f"Looking up account code: '{numeric_code}'")

        # Check cache first
        if numeric_code in self.account_cache:
            accounts = self.account_cache[numeric_code]
            if accounts:
                # Return the first match
                account = accounts[0]
                self.logger.info(
                    f"Found account '{numeric_code}' in cache: {account['code']} - {account['name']}"
                )
                return AccountMatch(
                    code=account["code"],
                    name=account["name"],
                    numeric_code=numeric_code,
                    position=0,
                )
        else:
            self.logger.debug(
                f"Account code '{numeric_code}' not found in cache"
            )

        # If not found in cache, use fast_search
        from src.accounting.fast_search import search_accounts

        # Try multiple search strategies
        # 1. Search by name (original logic)
        results = search_accounts(numeric_code, field_name="name", limit=1)
        if results:
            result = results[0]
            self.logger.info(
                f"Found account '{numeric_code}' via name search: {result['code']} - {result['name']}"
            )
            return AccountMatch(
                code=result["code"],
                name=result["name"],
                numeric_code=numeric_code,
                position=0,
            )

        # 2. Search by code directly (new strategy for direct account code lookups)
        try:
            code_results = search_accounts(
                numeric_code, field_name="code", limit=1
            )
            if code_results:
                result = code_results[0]
                self.logger.info(
                    f"Found account '{numeric_code}' via code search: {result['code']} - {result['name']}"
                )
                return AccountMatch(
                    code=result["code"],
                    name=result["name"],
                    numeric_code=numeric_code,
                    position=0,
                )
        except Exception as e:
            self.logger.debug(f"Code search failed for '{numeric_code}': {e}")
        else:
            self.logger.warning(
                f"Account code '{numeric_code}' not found in database"
            )
            # Log some similar accounts for debugging
            try:
                if len(numeric_code) >= 2:
                    partial_results = search_accounts(
                        numeric_code[:2], field_name="name", limit=5
                    )
                    self.logger.debug(
                        f"Similar accounts starting with '{numeric_code[:2]}': {[acc.get('code', 'N/A') for acc in partial_results]}"
                    )
            except Exception as e:
                self.logger.debug(f"Could not search for similar accounts: {e}")

        return None

    def extract_transfer_info(self, description: str) -> TransferInfo:
        """
        Extract transfer information from description

        Args:
            description: Transaction description

        Returns:
            TransferInfo object with source and destination accounts
        """
        info = TransferInfo()

        # Try each transfer pattern
        for pattern, pattern_type in self.transfer_patterns:
            match = re.search(pattern, description, re.IGNORECASE)
            if match:
                if pattern_type == "from_to":
                    # Pattern captures both from and to
                    info.from_code = match.group(1)
                    info.to_code = match.group(2)

                    # Find corresponding accounts
                    from_match = self.find_account_by_code(info.from_code)
                    to_match = self.find_account_by_code(info.to_code)

                    if from_match:
                        info.from_account = from_match.code
                    if to_match:
                        info.to_account = to_match.code

                    break

                elif pattern_type == "from":
                    info.from_code = match.group(1)
                    from_match = self.find_account_by_code(info.from_code)
                    if from_match:
                        info.from_account = from_match.code

                elif pattern_type == "to":
                    info.to_code = match.group(1)
                    to_match = self.find_account_by_code(info.to_code)
                    if to_match:
                        info.to_account = to_match.code

        return info

    def identify_transaction_type(self, description: str) -> Optional[str]:
        """Identify transaction type from description"""
        normalized_desc = description.lower()

        for trans_type, patterns in self.transaction_type_patterns.items():
            for pattern in patterns:
                if re.search(pattern, normalized_desc, re.IGNORECASE):
                    self.logger.debug(
                        f"Identified transaction type {trans_type} from '{description}'"
                    )
                    return trans_type

        # If POS is in the description, it's likely a sale
        if re.search(r"pos", normalized_desc, re.IGNORECASE):
            return "SALE"

        return None

    def extract_pos_code(self, description: str) -> Optional[str]:
        """Extract POS terminal code from description"""
        for pattern in self.pos_patterns:
            match = re.search(pattern, description, re.IGNORECASE)
            if match:
                pos_code = match.group(1)
                self.logger.debug(
                    f"Extracted POS code: {pos_code} from '{description}'"
                )
                return pos_code

        return None

    def extract_location_code(self, description: str) -> Optional[str]:
        """Extract location code from description (like GO BRVT)"""
        for pattern in self.location_patterns:
            match = re.search(pattern, description, re.IGNORECASE)
            if match:
                location_code = match.group(1)
                self.logger.debug(
                    f"Extracted location code: {location_code} from '{description}'"
                )
                return location_code
        return None

    def extract_4_digit_code(self, description: str) -> Optional[str]:
        """Extract 4-digit code from transaction description for POS formatting"""
        # Look for 4-digit codes in the description
        pattern = r"\b(\d{4})\b"
        matches = re.findall(pattern, description)
        if matches:
            # Return the last found 4-digit code (most likely to be relevant)
            four_digit_code = matches[-1]
            self.logger.debug(
                f"Extracted 4-digit code: {four_digit_code} from '{description}'"
            )
            return four_digit_code
        return None

    def get_description_department_code(self, department_code: str) -> str:
        """Get department code for description formatting (different from counterparty search)

        For POS description formatting, we want a cleaner format:
        - Input: "DD.TINH_GO BRVT1"
        - Output: "GO BRVT" (removes trailing digits)

        This is different from clean_department_code which is used for counterparty search.
        """
        if not department_code or not isinstance(department_code, str):
            return ""

        original_code = department_code.strip()
        self.logger.debug(
            f"Processing department code for description: '{original_code}'"
        )

        # Step 1: Try splitting by "_" first, then "-"
        if "_" in original_code:
            parts = original_code.split("_")
        elif "-" in original_code:
            parts = original_code.split("-")
        else:
            parts = [original_code]

        # Step 2: Take the last element
        last_element = parts[-1].strip() if parts else original_code

        # Step 3: For description formatting, remove trailing digits but keep the base location
        # "GO BRVT1" -> "GO BRVT"
        cleaned_code = re.sub(r"\d+$", "", last_element).strip()

        self.logger.debug(
            f"Department code for description: '{original_code}' -> '{cleaned_code}'"
        )

        return cleaned_code

    def format_sang_tam_transfer_description(
        self, description: str
    ) -> Optional[str]:
        """
        Format transfer descriptions that contain 'Sang Tam' between two bank accounts.

        This handles business logic for inter-bank transfers where both accounts
        belong to "Sáng Tâm" entity, standardizing the description format.

        Examples:
        Input:  "CHUYEN TIEN TU TK VCB (7803) SANG TAM QUA TK ACB (8368) SANG TAM GD 023763-061425 18:11:43"
        Output: "Chuyển tiền từ TK VCB (7803) Sáng Tâm qua TK ACB (8368) Sáng Tâm"

        Input:  "Chuyen tien tu TK BIDV 3840 Sang Tam qua TK BIDV 7655 Sang Tam"
        Output: "Chuyển tiền từ TK BIDV (3840) Sáng Tâm qua TK BIDV (7655) Sáng Tâm"

        Args:
            description: Original transaction description

        Returns:
            Formatted description or None if not a Sang Tam transfer
        """
        if not description:
            return None

        # Normalize for pattern matching (case insensitive)
        normalized_desc = description.upper().strip()

        # Check if this is a Sang Tam transfer (must contain transfer keywords AND Sang Tam)
        transfer_keywords = ["CHUYEN TIEN", "CHUYEN KHOAN", "TRANSFER"]
        has_transfer_keyword = any(
            keyword in normalized_desc for keyword in transfer_keywords
        )
        has_sang_tam = "SANG TAM" in normalized_desc

        if not (has_transfer_keyword and has_sang_tam):
            return None

        self.logger.info(f"Detected Sang Tam transfer: {description}")

        # Pattern 1: Bank name with account in parentheses (VCB (7803))
        # Handles: "TU TK VCB (7803) SANG TAM QUA TK ACB (8368) SANG TAM"
        pattern1 = r"tu\s+tk\s+([A-Z]+)\s*\((\d+)\)\s+sang\s+tam.*?qua\s+tk\s+([A-Z]+)\s*\((\d+)\)\s+sang\s+tam"
        match1 = re.search(pattern1, normalized_desc, re.IGNORECASE)

        if match1:
            source_bank = match1.group(1)
            source_account = match1.group(2)
            dest_bank = match1.group(3)
            dest_account = match1.group(4)

            formatted_desc = f"Chuyển tiền từ TK {source_bank} ({source_account}) Sáng Tâm qua TK {dest_bank} ({dest_account}) Sáng Tâm"
            self.logger.info(
                f"Formatted Sang Tam transfer (Pattern 1): {formatted_desc}"
            )
            return formatted_desc

        # Pattern 2: Bank name with separate account number (BIDV 3840)
        # Handles: "TU TK BIDV 3840 SANG TAM QUA TK BIDV 7655 SANG TAM"
        pattern2 = r"tu\s+tk\s+([A-Z]+)\s+(\d+)\s+sang\s+tam.*?qua\s+tk\s+([A-Z]+)\s+(\d+)\s+sang\s+tam"
        match2 = re.search(pattern2, normalized_desc, re.IGNORECASE)

        if match2:
            source_bank = match2.group(1)
            source_account = match2.group(2)
            dest_bank = match2.group(3)
            dest_account = match2.group(4)

            formatted_desc = f"Chuyển tiền từ TK {source_bank} ({source_account}) Sáng Tâm qua TK {dest_bank} ({dest_account}) Sáng Tâm"
            self.logger.info(
                f"Formatted Sang Tam transfer (Pattern 2): {formatted_desc}"
            )
            return formatted_desc

        # Pattern 3: Mixed format - handle cases where one account has parentheses and one doesn't
        # This is a fallback pattern for edge cases
        pattern3 = r"tu\s+tk\s+([A-Z]+)\s*(?:\((\d+)\)|(\d+))\s+sang\s+tam.*?qua\s+tk\s+([A-Z]+)\s*(?:\((\d+)\)|(\d+))\s+sang\s+tam"
        match3 = re.search(pattern3, normalized_desc, re.IGNORECASE)

        if match3:
            source_bank = match3.group(1)
            source_account = match3.group(2) or match3.group(
                3
            )  # One of these will be None
            dest_bank = match3.group(4)
            dest_account = match3.group(5) or match3.group(
                6
            )  # One of these will be None

            formatted_desc = f"Chuyển tiền từ TK {source_bank} ({source_account}) Sáng Tâm qua TK {dest_bank} ({dest_account}) Sáng Tâm"
            self.logger.info(
                f"Formatted Sang Tam transfer (Pattern 3): {formatted_desc}"
            )
            return formatted_desc

        # If we get here, it contains Sang Tam and transfer keywords but doesn't match our patterns
        self.logger.warning(
            f"Sang Tam transfer detected but couldn't parse account details: {description}"
        )
        return None

    def _detect_onl_kdv_po_pattern(self, description: str) -> bool:
        """Detect ONL KDV PO pattern in transaction description"""
        # Pattern supports both single and multiple PO numbers
        pattern = r"ONL\s+KDV\s+PO\s+\w+"
        return bool(re.search(pattern, description, re.IGNORECASE))

    def _format_onl_kdv_po_description(self, description: str) -> Optional[str]:
        """Format ONL KDV PO transaction description - handles single or multiple PO numbers"""
        # More precise pattern to capture only PO numbers (alphanumeric codes starting with letters)
        # This pattern looks for PO codes that start with letters and may contain numbers
        pattern = r"ONL\s+KDV\s+PO\s+([A-Z][A-Z0-9]*(?:\s+[A-Z][A-Z0-9]*)*)"
        match = re.search(pattern, description, re.IGNORECASE)

        if match:
            # Extract all PO numbers and take the last one
            po_numbers_text = match.group(1).strip()
            po_numbers = po_numbers_text.split()

            if po_numbers:
                # Always use the last PO number (for both single and multiple cases)
                last_po_number = po_numbers[-1]
                return f"Thu tiền KH ONLINE thanh toán cho PO: {last_po_number}"

        return None

    def _normalize_vietnamese_text(self, text: str) -> str:
        """
        Normalize Vietnamese text for pattern matching by removing diacritics
        and converting to uppercase for consistent comparison.

        Args:
            text: Input Vietnamese text

        Returns:
            Normalized text with diacritics removed
        """
        if not text:
            return text

        # Convert to uppercase first
        normalized = text.upper()

        # Vietnamese diacritic removal mapping
        vietnamese_chars = {
            "Á": "A",
            "À": "A",
            "Ả": "A",
            "Ã": "A",
            "Ạ": "A",
            "Ă": "A",
            "Ắ": "A",
            "Ằ": "A",
            "Ẳ": "A",
            "Ẵ": "A",
            "Ặ": "A",
            "Â": "A",
            "Ấ": "A",
            "Ầ": "A",
            "Ẩ": "A",
            "Ẫ": "A",
            "Ậ": "A",
            "É": "E",
            "È": "E",
            "Ẻ": "E",
            "Ẽ": "E",
            "Ẹ": "E",
            "Ê": "E",
            "Ế": "E",
            "Ề": "E",
            "Ể": "E",
            "Ễ": "E",
            "Ệ": "E",
            "Í": "I",
            "Ì": "I",
            "Ỉ": "I",
            "Ĩ": "I",
            "Ị": "I",
            "Ó": "O",
            "Ò": "O",
            "Ỏ": "O",
            "Õ": "O",
            "Ọ": "O",
            "Ô": "O",
            "Ố": "O",
            "Ồ": "O",
            "Ổ": "O",
            "Ỗ": "O",
            "Ộ": "O",
            "Ơ": "O",
            "Ớ": "O",
            "Ờ": "O",
            "Ở": "O",
            "Ỡ": "O",
            "Ợ": "O",
            "Ú": "U",
            "Ù": "U",
            "Ủ": "U",
            "Ũ": "U",
            "Ụ": "U",
            "Ư": "U",
            "Ứ": "U",
            "Ừ": "U",
            "Ử": "U",
            "Ữ": "U",
            "Ự": "U",
            "Ý": "Y",
            "Ỳ": "Y",
            "Ỷ": "Y",
            "Ỹ": "Y",
            "Ỵ": "Y",
            "Đ": "D",
        }

        # Replace Vietnamese characters
        for vn_char, en_char in vietnamese_chars.items():
            normalized = normalized.replace(vn_char, en_char)

        return normalized

    def determine_special_accounts(
        self, description: str, document_type: str
    ) -> Tuple[Optional[str], Optional[str]]:
        """
        Check for special Vietnamese keywords and return appropriate account mappings.
        This has the highest priority and should be called before other account determination logic.

        Args:
            description: Transaction description
            document_type: Document type (BC for receipts, BN for payments)

        Returns:
            Tuple of (debit_account, credit_account) or (None, None) if no special keywords found
        """
        if not description:
            return None, None

        # Normalize the description for comparison
        normalized_desc = self._normalize_vietnamese_text(description)

        self.logger.debug(
            f"Checking special accounts for: '{description}' -> '{normalized_desc}'"
        )

        # ENHANCED LOGIC: Special handling for "Phí cà thẻ" (Card fee)
        normalized_phi_ca_the = self._normalize_vietnamese_text("Phí cà thẻ")
        if normalized_phi_ca_the in normalized_desc:
            self.logger.info(
                f"Found 'Phí cà thẻ' in description: '{description}'"
            )

            # Use the same special account as interest payment (Thanh toan lai)
            special_account = self.special_account_mappings.get(
                "Thanh toan lai", "5154"
            )

            # Check that the account exists in our system
            account_match = self.find_account_by_code(special_account)
            if not account_match:
                self.logger.warning(
                    f"Special account {special_account} for 'Phí cà thẻ' not found in database. Falling back to normal logic."
                )
            else:
                # Apply the same logic as interest payment based on document type
                if document_type == "BC":  # Receipt/Credit transaction
                    # For receipts: money comes into bank, special account is credited
                    debit_account = self.default_bank_account
                    credit_account = special_account

                    # Store information about this being a special payment (same as interest payment)
                    # This will cause process_transaction to use the bank info from filename
                    self._current_transaction_is_interest_payment = True
                    self.logger.info(
                        "Set card fee transaction to use bank info from filename"
                    )
                    return debit_account, credit_account
                else:  # BN - Payment/Debit transaction
                    # For payments: money goes out of bank, special account is debited
                    debit_account = special_account
                    credit_account = self.default_bank_account

                    # Store information about this being a special payment (same as interest payment)
                    # This will cause process_transaction to use the bank info from filename
                    self._current_transaction_is_interest_payment = True
                    self.logger.info(
                        "Set card fee transaction to use bank info from filename"
                    )
                    return debit_account, credit_account

        # ENHANCED LOGIC: Special handling for "Thanh toan lai" (Interest payment)
        normalized_thanh_toan_lai = self._normalize_vietnamese_text(
            "Thanh toan lai"
        )
        if normalized_thanh_toan_lai in normalized_desc:
            self.logger.info(
                f"Found 'Thanh toan lai' in description: '{description}'"
            )

            # Apply enhanced logic using bank information if available
            special_account = self.special_account_mappings.get(
                "Thanh toan lai", "5154"
            )

            # Check that the account exists in our system
            account_match = self.find_account_by_code(special_account)
            if not account_match:
                self.logger.warning(
                    f"Special account {special_account} for 'Thanh toan lai' not found in database. Falling back to normal logic."
                )
            else:
                # Apply the special logic based on document type
                if document_type == "BC":  # Receipt/Credit transaction
                    # For receipts: money comes into bank, special account is credited
                    debit_account = self.default_bank_account
                    credit_account = special_account

                    # Check if we have bank information to enhance the description
                    bank_info_str = ""
                    if self.current_bank_name:
                        bank_info_str = f" - {self.current_bank_name}"
                        if (
                            self.current_bank_info
                            and self.current_bank_info.get("address")
                        ):
                            bank_info_str += (
                                f" ({self.current_bank_info.get('address')})"
                            )

                    self.logger.info(
                        f"Enhanced 'Thanh toan lai' mapping (Receipt): Dr={debit_account} (bank), Cr={credit_account} (interest){bank_info_str}"
                    )
                    # Store information about this being a special interest payment
                    # This will be used later in process_transaction to enhance the entry
                    self._current_transaction_is_interest_payment = True
                    return debit_account, credit_account
                else:  # BN - Payment/Debit transaction
                    # For payments: money goes out of bank, special account is debited
                    debit_account = special_account
                    credit_account = self.default_bank_account

                    # Check if we have bank information to enhance the description
                    bank_info_str = ""
                    if self.current_bank_name:
                        bank_info_str = f" - {self.current_bank_name}"
                        if (
                            self.current_bank_info
                            and self.current_bank_info.get("address")
                        ):
                            bank_info_str += (
                                f" ({self.current_bank_info.get('address')})"
                            )

                    self.logger.info(
                        f"Enhanced 'Thanh toan lai' mapping (Payment): Dr={debit_account} (interest), Cr={credit_account} (bank){bank_info_str}"
                    )
                    # Store information about this being a special interest payment
                    # This will be used later in process_transaction to enhance the entry
                    self._current_transaction_is_interest_payment = True
                    return debit_account, credit_account

        # Check each special keyword mapping (standard logic for other keywords)
        for keyword, special_account in self.special_account_mappings.items():
            # Skip Thanh toan lai as we already handled it above
            if keyword == "Thanh toan lai":
                continue

            # Normalize the keyword for comparison
            normalized_keyword = self._normalize_vietnamese_text(keyword)

            if normalized_keyword in normalized_desc:
                self.logger.info(
                    f"Found special keyword '{keyword}' -> account {special_account} in description: '{description}'"
                )

                # Validate that the special account exists in our account cache/database
                account_match = self.find_account_by_code(special_account)
                if not account_match:
                    self.logger.warning(
                        f"Special account {special_account} for keyword '{keyword}' not found in database. Falling back to normal logic."
                    )
                    continue

                # Determine debit/credit based on document type and account nature
                if document_type == "BC":  # Receipt/Credit transaction
                    # For receipts: money comes into bank, special account is credited
                    debit_account = self.default_bank_account
                    credit_account = special_account

                    self.logger.info(
                        f"Special account mapping (Receipt): Dr={debit_account} (bank), Cr={credit_account} (special)"
                    )
                    return debit_account, credit_account

                else:  # BN - Payment/Debit transaction
                    # For payments: money goes out of bank, special account is debited
                    debit_account = special_account
                    credit_account = self.default_bank_account

                    self.logger.info(
                        f"Special account mapping (Payment): Dr={debit_account} (special), Cr={credit_account} (bank)"
                    )
                    return debit_account, credit_account

        # No special keywords found
        self.logger.debug("No special keywords found in description")
        # Reset the interest payment flag
        self._current_transaction_is_interest_payment = False
        return None, None

    def determine_accounts_by_codes(
        self,
        description: str,
        is_credit: bool,
        document_type: str,
        transaction_type: Optional[str] = None,
    ) -> Tuple[Optional[str], Optional[str]]:
        """
        Determine accounts by extracting and matching numeric codes

        Args:
            description: Transaction description
            is_credit: Whether this is a credit transaction
            document_type: Document type (BC or BN)
            transaction_type: Transaction type if known

        Returns:
            Tuple of (debit_account, credit_account) or (None, None) if not found
        """
        # Extract all numeric codes
        codes = self.extract_numeric_codes(description)

        if not codes:
            return None, None

        # Check if this is a transfer transaction
        if (
            "chuyen" in description.lower()
            or "transfer" in description.lower()
            or "CK" in description
        ):
            transfer_info = self.extract_transfer_info(description)

            if transfer_info.from_account and transfer_info.to_account:
                # For transfers: money moves FROM one account TO another
                # Debit the destination, Credit the source
                return transfer_info.to_account, transfer_info.from_account

            # If we only found one account in a transfer
            if transfer_info.from_account:
                # Money leaving from this account
                if document_type == "BN":  # Payment
                    return None, transfer_info.from_account  # Credit the source

            if transfer_info.to_account:
                # Money going to this account
                if document_type == "BC":  # Receipt
                    return (
                        transfer_info.to_account,
                        None,
                    )  # Debit the destination

        # For non-transfer transactions, find accounts by codes
        matched_accounts = []
        for code, code_type, position in codes:
            account_match = self.find_account_by_code(code)
            if account_match:
                account_match.position = position
                matched_accounts.append(account_match)

        if not matched_accounts:
            return None, None

        # Use the first matched account
        primary_account = matched_accounts[0].code

        # Determine account placement based on transaction type
        if document_type == "BC":  # Receipt/Credit
            # For receipts: debit bank account, credit income/receivable
            return primary_account, None
        else:  # BN - Payment/Debit
            # For payments: debit expense, credit bank account
            return None, primary_account

    def get_transaction_rules(self, is_credit: bool) -> List[TransactionRule]:
        """Get transaction rules using predefined patterns instead of database"""
        if is_credit in self.rule_cache:
            return self.rule_cache[is_credit]

        # Define hardcoded rules instead of fetching from database
        # These rules would have previously been in the transaction_rules table
        rules = []

        if is_credit:  # Credit/Receipt rules
            # POS transactions
            rules.append(
                TransactionRule(
                    id=1,
                    pattern=r"TT\s*POS\s*(\d{7,8})",
                    document_type="BC",
                    transaction_type="SALE",
                    counterparty_code=None,
                    department_code=None,
                    cost_code=None,
                    description_template="Thu tiền bán hàng khách lẻ (POS {pos_code})",
                    is_credit=True,
                    priority=90,
                )
            )

            # Bank transfers - incoming
            rules.append(
                TransactionRule(
                    id=2,
                    pattern=r"CK\s*den|CHUYEN\s*KHOAN\s*DEN|Chuyen\s*tien.*tu\s*TK",
                    document_type="BC",
                    transaction_type="TRANSFER",
                    counterparty_code=None,
                    department_code=None,
                    cost_code=None,
                    description_template="Thu tiền chuyển khoản",
                    is_credit=True,
                    priority=70,
                )
            )

            # Interest income
            rules.append(
                TransactionRule(
                    id=3,
                    pattern=r"L[AÃ]I.*SU[ẤẬ]T|LAI.*TIEN.*GUI|INTEREST",
                    document_type="BC",
                    transaction_type="INTEREST",
                    counterparty_code="BIDV",
                    department_code=None,
                    cost_code=None,
                    description_template="Lãi tiền gửi ngân hàng",
                    is_credit=True,
                    priority=60,
                )
            )

            # Default rule for receipts
            rules.append(
                TransactionRule(
                    id=4,
                    pattern=r".*",
                    document_type="BC",
                    transaction_type=None,
                    counterparty_code="KL",
                    department_code=None,
                    cost_code=None,
                    description_template="Thu tiền khác",
                    is_credit=True,
                    priority=1,
                )
            )
        else:  # Debit/Payment rules
            # ATM withdrawals
            rules.append(
                TransactionRule(
                    id=5,
                    pattern=r"ATM.*R[uú]t\s*ti[eề]n|RUT\s*TIEN.*ATM",
                    document_type="BN",
                    transaction_type="WITHDRAWAL",
                    counterparty_code=None,
                    department_code=None,
                    cost_code="RUTIEN",
                    description_template="Rút tiền mặt tại ATM",
                    is_credit=False,
                    priority=80,
                )
            )

            # Bank transfers - outgoing
            rules.append(
                TransactionRule(
                    id=6,
                    pattern=r"CK\s*di|CHUYEN\s*KHOAN\s*DI|Chuyen\s*tien.*qua\s*TK",
                    document_type="BN",
                    transaction_type="TRANSFER",
                    counterparty_code=None,
                    department_code=None,
                    cost_code=None,
                    description_template="Chuyển khoản thanh toán",
                    is_credit=False,
                    priority=70,
                )
            )

            # Internet banking transactions
            rules.append(
                TransactionRule(
                    id=7,
                    pattern=r"IB|INTERNET\s*BANKING|BIDV\s*ONLINE",
                    document_type="BN",
                    transaction_type="FEE",
                    counterparty_code=None,
                    department_code=None,
                    cost_code="DICHVU",
                    description_template="Giao dịch Internet Banking",
                    is_credit=False,
                    priority=60,
                )
            )

            # Bank fees
            rules.append(
                TransactionRule(
                    id=8,
                    pattern=r"PH[IÍ].*[NG]H.*|PHI.*DICH.*VU|FEE",
                    document_type="BN",
                    transaction_type="FEE",
                    counterparty_code="BIDV",
                    department_code=None,
                    cost_code="PHIDV",
                    description_template="Phí dịch vụ ngân hàng",
                    is_credit=False,
                    priority=50,
                )
            )

            # Default rule for payments
            rules.append(
                TransactionRule(
                    id=9,
                    pattern=r".*",
                    document_type="BN",
                    transaction_type=None,
                    counterparty_code=None,
                    department_code=None,
                    cost_code="KHAC",
                    description_template="Chi tiền khác",
                    is_credit=False,
                    priority=1,
                )
            )

        self.rule_cache[is_credit] = rules
        return rules

    def match_transaction_rule(
        self, description: str, is_credit: bool
    ) -> Optional[TransactionRule]:
        """Match a transaction description to a rule"""
        rules = self.get_transaction_rules(is_credit)

        for rule in rules:
            if re.search(rule.pattern, description, re.IGNORECASE):
                return rule

        # Return default rule (last in list)
        return rules[-1] if rules else None

    def determine_accounts_by_counterparty(
        self,
        counterparty_code: str,
        document_type: str,
        transaction_type: Optional[str] = None,
    ) -> Tuple[Optional[str], Optional[str]]:
        """
        Determine accounts based on counterparty information using fast_search

        Args:
            counterparty_code: Counterparty code
            document_type: Document type (BC or BN)
            transaction_type: Transaction type if known

        Returns:
            Tuple of (debit_account, credit_account) or (None, None) if not found
        """
        from src.accounting.fast_search import search_counterparties

        if not counterparty_code:
            return None, None

        # Search for the counterparty to get more information
        counterparty_results = search_counterparties(
            counterparty_code, field_name="code", limit=1
        )

        if not counterparty_results:
            return None, None

        # Use the found counterparty information to determine appropriate accounts
        counterparty = counterparty_results[0]

        # Default account mappings based on document type and counterparty type
        # These would previously have been in the account_mapping_rules table
        if document_type == "BC":  # Receipt/Credit
            # For receipts: debit bank account, credit income/receivable
            return self.default_bank_account, "1311"
        else:  # BN - Payment/Debit
            # For payments: debit expense, credit bank account
            return "3311", self.default_bank_account

    def determine_accounts(
        self,
        description: str,
        document_type: str,
        is_credit: bool,
        transaction_type: Optional[str] = None,
        pos_code: Optional[str] = None,
        department_code: Optional[str] = None,
        counterparty_code: Optional[str] = None,
    ) -> Tuple[str, str]:
        """
        Enhanced account determination with counterparty extraction

        Args:
            description: Transaction description
            document_type: Document type (BC or BN)
            is_credit: Whether this is a credit transaction
            transaction_type: Transaction type if known
            pos_code: POS machine code if applicable
            department_code: Department code if known
            counterparty_code: Counterparty code if extracted

        Returns:
            Tuple of (debit_account, credit_account)
        """
        # PRIORITY 1: Check for special Vietnamese keywords first
        special_debit, special_credit = self.determine_special_accounts(
            description, document_type
        )
        if special_debit and special_credit:
            self.logger.info(
                f"Using special account mapping: Dr={special_debit}, Cr={special_credit}"
            )
            return special_debit, special_credit

        # PRIORITY 2: Try to determine accounts by extracting numeric codes
        debit_by_code, credit_by_code = self.determine_accounts_by_codes(
            description, is_credit, document_type, transaction_type
        )

        # PRIORITY 3: If we found accounts by code extraction, use them
        if debit_by_code and credit_by_code:
            self.logger.info(
                f"Determined complete accounts by code extraction: Dr={debit_by_code}, Cr={credit_by_code}"
            )
            return debit_by_code, credit_by_code

        # PRIORITY 4: If we have a counterparty code, try to determine accounts based on that
        if counterparty_code:
            debit_by_cp, credit_by_cp = self.determine_accounts_by_counterparty(
                counterparty_code, document_type, transaction_type
            )

            # If we found accounts by counterparty, use them, potentially combining with code extraction
            if debit_by_cp or credit_by_cp:
                final_debit = debit_by_code or debit_by_cp
                final_credit = credit_by_code or credit_by_cp

                if final_debit and final_credit:
                    self.logger.info(
                        f"Determined accounts by counterparty and code: Dr={final_debit}, Cr={final_credit}"
                    )
                    return final_debit, final_credit

        # PRIORITY 5: If we have partial accounts from code extraction, fill in the missing ones
        if debit_by_code or credit_by_code:
            if document_type == "BC":  # Receipt
                if not credit_by_code:
                    # Default credit account for receipts
                    credit_by_code = "1311"  # Accounts receivable
                if not debit_by_code:
                    # This shouldn't happen for receipts, but use default bank
                    debit_by_code = self.default_bank_account
            else:  # BN - Payment
                if not debit_by_code:
                    # Default debit account for payments
                    debit_by_code = "3311"  # Other expenses
                if not credit_by_code:
                    # This shouldn't happen for payments, but use default bank
                    credit_by_code = self.default_bank_account

            self.logger.info(
                f"Determined partial accounts by code extraction: Dr={debit_by_code}, Cr={credit_by_code}"
            )
            return debit_by_code, credit_by_code

        # PRIORITY 6: Fallback to using fast_search for POS and department-based determination
        try:
            # Use fast_search.py to look up information instead of SQL queries
            from src.accounting.fast_search import (
                search_departments,
                search_pos_machines,
            )

            # Log the default bank account before determining accounts
            self.logger.info(
                f"Current default bank account: {self.default_bank_account}"
            )

            # Try POS-specific rules using fast_search
            if pos_code:
                pos_results = search_pos_machines(
                    pos_code, field_name="code", limit=1
                )
                if pos_results:
                    pos_info = pos_results[0]
                    # Use standard mappings for POS transactions
                    if document_type == "BC":
                        # For POS sales receipts
                        return self.default_bank_account, "1311"
                    else:
                        # For POS-related payments (card fees)
                        return "3311", self.default_bank_account

            # Try department-specific rules using fast_search
            if department_code:
                dept_results = search_departments(
                    department_code, field_name="code", limit=1
                )
                if dept_results:
                    dept_info = dept_results[0]
                    # Use standard mappings for department
                    if document_type == "BC":
                        # For department receipts
                        return self.default_bank_account, "1311"
                    else:
                        # For department payments
                        return "3311", self.default_bank_account

            # Transaction type based mappings
            if transaction_type:
                if document_type == "BC":
                    if transaction_type == "INTEREST":
                        return self.default_bank_account, "5154"
                    elif transaction_type == "TRANSFER":
                        return self.default_bank_account, "1311"
                    else:
                        return self.default_bank_account, "1311"
                else:  # BN - Payment
                    if transaction_type == "FEE":
                        return "3311", self.default_bank_account
                    elif transaction_type == "SALARY":
                        return "6411", self.default_bank_account
                    elif transaction_type == "TAX":
                        return "3331", self.default_bank_account
                    elif transaction_type == "UTILITY":
                        return "3311", self.default_bank_account
                    elif transaction_type == "WITHDRAWAL":
                        return "1111", self.default_bank_account
                    elif transaction_type == "TRANSFER":
                        return "3311", self.default_bank_account
                    else:
                        return "3311", self.default_bank_account

            # Default mappings - Updated to use 3311 and 1311
            if document_type == "BC":
                return self.default_bank_account, "1311"
            else:
                return "3311", self.default_bank_account

        except Exception as e:
            self.logger.error(f"Error in account determination: {e}")

        # Final fallback - Updated to use 3311 and 1311
        if document_type == "BC":
            return self.default_bank_account, "1311"
        else:
            return "3311", self.default_bank_account

    def generate_document_number(
        self, doc_type: str, transaction_date: datetime
    ) -> str:
        """Generate a document number for the transaction"""
        month = transaction_date.month
        counter = self.doc_counters.get(doc_type, 1)

        # Format: BC03/001 (for March, first transaction)
        doc_number = f"{doc_type}{month:02d}/{counter:03d}"

        # Increment counter for next document
        self.doc_counters[doc_type] = counter + 1

        return doc_number

    def format_date(self, dt: datetime, format_type: str = "vn") -> str:
        """Format date in required format"""
        # All formats now use standard DD/MM/YYYY
        return f"{dt.day:02d}/{dt.month:02d}/{dt.year}"

    def get_counterparty_info(self, counterparty_code: str) -> Tuple[str, str]:
        """
        Get counterparty name and address using fast_search instead of database

        Args:
            counterparty_code: Counterparty code

        Returns:
            Tuple of (name, address)
        """
        name = "Khách Lẻ Không Lấy Hóa Đơn"
        address = ""

        if not counterparty_code:
            return name, address

        # Use fast_search to find counterparty
        from src.accounting.fast_search import search_counterparties

        results = search_counterparties(
            counterparty_code, field_name="code", limit=1
        )

        if results:
            result = results[0]
            name = result["name"] or name  # Use default if None
            address = result["address"] or ""  # Empty string if None

        return name, address

    def process_transaction(
        self, transaction: RawTransaction
    ) -> Optional[SaokeEntry]:
        """Process a single transaction into a saoke entry"""
        try:
            # Special case for BIDV 3840 accounts - ensure we're using the correct account
            if (
                "3840" in transaction.description
                and self.default_bank_account == "1121114"
            ):
                self.logger.warning(
                    "Detected reference to account 3840 in transaction description but using default account 1121114"
                )
                account_match = self.find_account_by_code("3840")
                if account_match:
                    self.default_bank_account = account_match.code
                    self.logger.info(
                        f"Updated default bank account to {self.default_bank_account} based on transaction description"
                    )
            # Determine if transaction is credit or debit
            is_credit = transaction.credit_amount > 0

            # Match transaction rule
            rule = self.match_transaction_rule(
                transaction.description, is_credit
            )
            if not rule:
                self.logger.warning(
                    f"No matching rule for transaction: {transaction.reference}"
                )
                return None

            # Generate document number
            doc_number = self.generate_document_number(
                rule.document_type, transaction.datetime
            )

            # Extract all entities (counterparty, account, POS machine, etc.) from description in one pass
            # This is the key enhancement - using extract_and_match_all instead of separate extractions
            extracted_entities = (
                self.counterparty_extractor.extract_and_match_all(
                    transaction.description
                )
            )

            # Apply NEW business logic based on object type from CounterpartyExtractor
            extracted_accounts = extracted_entities.get("accounts", [])
            extracted_pos_machines = extracted_entities.get("pos_machines", [])
            extracted_counterparties = extracted_entities.get(
                "counterparties", []
            )

            # Priority 1: If extracted object is an account
            if extracted_accounts:
                self.logger.info(
                    "Detected account object, using Sáng Tâm company info"
                )
                counterparty_info = {
                    "code": "31754",
                    "name": "Công Ty TNHH Sáng Tâm",
                    "address": "32-34 Đường 74, Phường 10, Quận 6, Tp. Hồ Chí Minh",
                    "source": "hardcoded_account_rule",
                    "condition_applied": "account_detected",
                    "phone": "",
                    "tax_id": "",
                }
            # Priority 2: If extracted object is POS machine or counterparty
            elif extracted_pos_machines or extracted_counterparties:
                if extracted_pos_machines:
                    # Use POS machine counterparty logic (gets counterparty from department_code)
                    self.logger.info(
                        "Detected POS machine object, applying POS machine counterparty logic"
                    )

                    # Determine current address for POS machine logic
                    current_address = None

                    # Option 1: Use bank address from current_bank_info
                    if self.current_bank_info and self.current_bank_info.get(
                        "address"
                    ):
                        current_address = self.current_bank_info["address"]
                        self.logger.info(
                            f"Using bank address as current_address: {current_address}"
                        )

                    # Call the enhanced POS machine logic with address parameter
                    counterparty_info = self.counterparty_extractor.handle_pos_machine_counterparty_logic(
                        extracted_pos_machines, current_address=current_address
                    )

                    # If POS machine logic failed, fall back to default
                    if not counterparty_info:
                        self.logger.warning(
                            "POS machine counterparty logic failed, using default counterparty"
                        )
                        counterparty_info = {
                            "code": "KL",
                            "name": "Khách Lẻ Không Lấy Hóa Đơn",
                            "address": "",
                            "source": "default_after_pos_failure",
                            "condition_applied": "pos_machine_failed",
                            "phone": "",
                            "tax_id": "",
                        }
                else:
                    # Use counterparty info directly
                    cp_info = extracted_counterparties[
                        0
                    ]  # Take first/best match
                    self.logger.info(
                        f"Detected counterparty object, using extracted info: {cp_info.get('name', cp_info.get('code', ''))}"
                    )
                    counterparty_info = {
                        "code": cp_info.get("code"),
                        "name": cp_info.get(
                            "name", cp_info.get("extracted_name", "")
                        ),
                        "address": cp_info.get("address", ""),
                        "source": "counterparty_extracted",
                        "condition_applied": "counterparty_detected",
                        "phone": cp_info.get("phone", ""),
                        "tax_id": cp_info.get("tax_id", ""),
                    }
            # Priority 3: Fall back to existing two-condition logic
            else:
                self.logger.info(
                    "No specific object detected, using existing two-condition logic"
                )
                counterparty_info = self.counterparty_extractor.handle_counterparty_two_conditions(
                    extracted_counterparties
                )

            # Extract counterparty details from the result
            counterparty_code = (
                counterparty_info.get("code") or rule.counterparty_code or "KL"
            )
            counterparty_name = counterparty_info.get("name")
            counterparty_address = counterparty_info.get("address") or ""
            condition_applied = counterparty_info.get(
                "condition_applied", "unknown"
            )

            self.logger.info(
                f"Counterparty processing result - Condition: {condition_applied}, "
                f"Code: {counterparty_code}, Name: {counterparty_name}, "
                f"Source: {counterparty_info.get('source', 'unknown')}"
            )

            # Process extracted accounts
            extracted_accounts = extracted_entities.get("accounts", [])

            # Get POS machine if available
            extracted_pos = extracted_entities.get("pos_machines", [])
            pos_code = None
            pos_name = None
            if extracted_pos:
                pos_code = extracted_pos[0].get("code")
                pos_name = extracted_pos[0].get("name")
                self.logger.info(
                    f"Extracted POS machine: {pos_code} - {pos_name}"
                )
            # If no POS code found from extractor, try the pattern matcher
            elif not pos_code:
                pos_code = self.extract_pos_code(transaction.description)

            # Get department if available
            extracted_departments = extracted_entities.get("departments", [])
            department_code = (
                extracted_departments[0].get("code")
                if extracted_departments and not rule.department_code
                else rule.department_code
            )

            # If no department code, try to extract from description
            if not department_code:
                location_code = self.extract_location_code(
                    transaction.description
                )
                department_code = location_code

            # Determine accounts with enhanced code and counterparty extraction
            debit_account, credit_account = self.determine_accounts(
                description=transaction.description,
                document_type=rule.document_type,
                is_credit=is_credit,
                transaction_type=rule.transaction_type,
                pos_code=pos_code,
                department_code=department_code,
                counterparty_code=counterparty_code,
            )

            # Get counterparty info from database only if not already obtained from two-condition logic
            if not counterparty_name:
                counterparty_name, counterparty_address = (
                    self.get_counterparty_info(counterparty_code)
                )
            elif not counterparty_address and counterparty_code:
                # Already have the name, just look up the address if we don't have it
                _, counterparty_address = self.get_counterparty_info(
                    counterparty_code
                )

            # Ensure we have a final address (even if empty)
            address = counterparty_address or ""

            # Format description - Apply specialized business logic based on transaction type

            # ENHANCED LOGIC: Special handling for "Thanh toan lai" (Interest payment) transactions
            if self._current_transaction_is_interest_payment:
                # Apply enhanced description with bank information if available
                bank_name = self.current_bank_name
                if bank_name:
                    # Get full bank info from current_bank_info
                    bank_address = self.current_bank_info.get("address", "")
                    bank_code = self.current_bank_info.get("code", "")

                    # Set counterparty code to bank code from _banks.json
                    if bank_code:
                        counterparty_code = str(
                            bank_code
                        )  # Make sure it's a string
                        self.logger.info(
                            f"Setting counterparty code to bank code: {counterparty_code}"
                        )
                    else:
                        counterparty_code = (
                            "BANK-" + bank_name
                            if bank_name
                            else counterparty_code
                        )

                    if rule.document_type == "BN":  # Payment
                        description = f"Thanh toán lãi tiền vay {bank_name}"
                        if bank_address:
                            description += f" - {bank_address}"
                    else:  # Receipt
                        description = f"Lãi tiền gửi ngân hàng {bank_name}"
                        if bank_address:
                            description += f" - {bank_address}"

                    # Override counterparty with bank info
                    counterparty_name = (
                        self.current_bank_info.get("name", bank_name)
                        or counterparty_name
                    )
                    address = bank_address or address

                    self.logger.info(
                        f"Applied enhanced 'Thanh toan lai' logic with bank info: {description}\n"
                        f"Counterparty Code: {counterparty_code}, Counterparty Name: {counterparty_name}"
                    )
                else:
                    # Fallback to standard description if no bank info
                    if rule.document_type == "BN":  # Payment
                        description = "Thanh toán lãi tiền vay ngân hàng"
                    else:  # Receipt
                        description = "Lãi tiền gửi ngân hàng"
                    self.logger.info(
                        f"Applied standard 'Thanh toan lai' logic (no bank info): {description}"
                    )

                # Reset the flag for next transaction
                self._current_transaction_is_interest_payment = False

            # NEW BUSINESS LOGIC: Special handling for "Sang Tam" transfer transactions
            elif "sang tam" in transaction.description.lower() and any(
                keyword in transaction.description.lower()
                for keyword in ["chuyen tien", "chuyen khoan", "transfer"]
            ):
                formatted_transfer_desc = (
                    self.format_sang_tam_transfer_description(
                        transaction.description
                    )
                )
                if formatted_transfer_desc:
                    description = formatted_transfer_desc
                    self.logger.info(
                        f"Applied Sang Tam transfer formatting: {description}"
                    )
                else:
                    # Fallback to original description if formatting failed
                    description = transaction.description
                    self.logger.warning(
                        f"Sang Tam transfer detected but formatting failed, using original: {description}"
                    )

            # NEW BUSINESS LOGIC: Special handling for "ONL KDV PO" pattern
            elif self._detect_onl_kdv_po_pattern(transaction.description):
                formatted_onl_kdv_po_desc = self._format_onl_kdv_po_description(
                    transaction.description
                )
                if formatted_onl_kdv_po_desc:
                    description = formatted_onl_kdv_po_desc
                    self.logger.info(
                        f"Applied ONL KDV PO formatting: {description}"
                    )
                else:
                    # Fallback to original description if formatting failed
                    description = transaction.description
                    self.logger.warning(
                        f"ONL KDV PO pattern detected but formatting failed, using original: {description}"
                    )

            # Apply NEW business logic for POS machine statements
            elif pos_code:
                # Extract 4-digit code from description
                four_digit_code = self.extract_4_digit_code(
                    transaction.description
                )

                # Get POS machine info to find department code for description formatting
                pos_department_code = ""
                if extracted_pos:
                    pos_department_code = extracted_pos[0].get(
                        "department_code", ""
                    )

                # Get cleaned department code for description
                description_dept_code = self.get_description_department_code(
                    pos_department_code
                )

                # Determine base description based on transaction type
                if rule.document_type == "BN":
                    # BN: POS machine payments = Card fee transaction
                    base_description = "Phí cà thẻ"
                else:
                    # BC: POS machine receipts = Sales transaction
                    base_description = "Thu tiền bán hàng khách lẻ"

                # Build the complete description in new format
                if description_dept_code:
                    description = f"{base_description} (POS {pos_code} - {description_dept_code})"
                else:
                    description = f"{base_description} (POS {pos_code})"

                # Add 4-digit code if found
                if four_digit_code:
                    description += f"_{four_digit_code}"

                self.logger.info(
                    f"Applied NEW POS machine logic: {description}"
                )

                # For BN POS transactions ("Phí cà thẻ..."), use bank info from filename as counterparty
                # Similar to "Thanh toan lai" statements
                if rule.document_type == "BN":
                    bank_name = self.current_bank_name
                    if bank_name:
                        # Get full bank info from current_bank_info
                        bank_address = self.current_bank_info.get("address", "")
                        bank_code = self.current_bank_info.get("code", "")

                        # Set counterparty code to bank code from _banks.json
                        if bank_code:
                            counterparty_code = str(
                                bank_code
                            )  # Make sure it's a string
                            self.logger.info(
                                f"Setting POS card fee counterparty code to bank code: {counterparty_code}"
                            )
                        else:
                            counterparty_code = (
                                "BANK-" + bank_name
                                if bank_name
                                else counterparty_code
                            )

                        # Override counterparty with bank info
                        counterparty_name = (
                            self.current_bank_info.get("name", bank_name)
                            or counterparty_name
                        )
                        address = bank_address or address

                        self.logger.info(
                            f"Applied enhanced 'Phí cà thẻ' logic with bank info from filename: {description}\n"
                            f"Counterparty Code: {counterparty_code}, Counterparty Name: {counterparty_name}"
                        )
            else:
                # For all other cases, use the raw/original description
                description = transaction.description
                self.logger.info(f"Using original description: {description}")

            # Extract transaction sequence number
            if pos_code and "_" in transaction.description:
                seq_match = re.search(r"_(\d+)_(\d+)", transaction.description)
                if seq_match:
                    sequence = int(seq_match.group(2))
                else:
                    sequence = 1
            else:
                sequence = 1

            # Calculate amount
            amount = (
                transaction.credit_amount
                if is_credit
                else transaction.debit_amount
            )

            # Create saoke entry
            entry = SaokeEntry(
                document_type=rule.document_type,
                date=self.format_date(transaction.datetime, "vn"),
                document_number=doc_number,
                currency="VND",
                exchange_rate=1.0,
                counterparty_code=counterparty_code,
                counterparty_name=counterparty_name,
                address=address,
                description=description,
                original_description=transaction.description,
                amount1=amount,
                amount2=amount,
                debit_account=debit_account,
                credit_account=credit_account,
                cost_code=rule.cost_code,
                department=department_code,
                sequence=sequence,
                date2=self.format_date(transaction.datetime, "alt"),
                transaction_type=rule.transaction_type,
                raw_transaction=transaction,
            )

            return entry

        except Exception as e:
            self.logger.error(
                f"Error processing transaction {transaction.reference}: {e}"
            )
            import traceback

            traceback.print_exc()
            return None

    def process_to_saoke(
        self,
        input_file: Union[str, pd.DataFrame, io.BytesIO],
        output_file: Optional[str] = None,
    ) -> pd.DataFrame:
        """
        Process a bank statement file to saoke format

        Args:
            input_file: Path to file, DataFrame, or BytesIO object
            output_file: Optional path to save output Excel file

        Returns:
            DataFrame with processed saoke entries
        """
        # Initialize the processor
        if not self.connect():
            raise RuntimeError("Failed to initialize processor")

        try:
            # Reset document counters
            self.doc_counters = {"BC": 1, "BN": 1}

            # Extract account from header if available
            if not isinstance(input_file, pd.DataFrame):
                bank_account = self.extract_account_from_header(input_file)
                if bank_account:
                    self.default_bank_account = bank_account
                    self.logger.info(
                        f"Using default bank account: {self.default_bank_account}"
                    )
                else:
                    # If we couldn't extract the account, check filename for hints
                    if isinstance(input_file, str):
                        filename = Path(input_file).name
                        if "3840" in filename:
                            self.logger.info(
                                f"Found account number 3840 in filename: {filename}"
                            )
                            # Try to find the 3840 account in the database
                            account_match = self.find_account_by_code("3840")
                            if account_match:
                                self.default_bank_account = account_match.code
                                self.logger.info(
                                    f"Set default bank account to {self.default_bank_account} based on filename"
                                )

            # Read transactions
            if isinstance(input_file, pd.DataFrame):
                transactions_df = input_file
            else:
                self.logger.info(f"Reading bank statement from {input_file}")
                transactions_df = self.reader.read_bank_statement(
                    input_file, debug=True
                )

            self.logger.info(f"Processing {len(transactions_df)} transactions")

            # Convert to RawTransaction objects
            raw_transactions = []
            for _, row in transactions_df.iterrows():
                try:
                    if "date" not in row or pd.isna(row["date"]):
                        continue

                    transaction = RawTransaction(
                        reference=str(row.get("reference", "")),
                        datetime=row["date"]
                        if isinstance(row["date"], datetime)
                        else pd.to_datetime(row["date"]),
                        debit_amount=float(row.get("debit", 0)),
                        credit_amount=float(row.get("credit", 0)),
                        balance=float(row.get("balance", 0)),
                        description=str(row.get("description", "")),
                    )
                    raw_transactions.append(transaction)
                except Exception as e:
                    self.logger.error(f"Error converting row: {e}")
                    continue

            # Process each transaction
            saoke_entries = []
            for transaction in raw_transactions:
                entry = self.process_transaction(transaction)
                if entry:
                    saoke_entries.append(entry.as_dict())

            # Create output DataFrame
            output_df = pd.DataFrame(saoke_entries)

            # Save to file if specified
            if output_file and not output_df.empty:
                # Ensure proper column order for saoke format
                required_columns = [
                    "document_type",
                    "date",
                    "document_number",
                    "currency",
                    "exchange_rate",
                    "counterparty_code",
                    "counterparty_name",
                    "address",
                    "description",
                    "amount1",
                    "amount2",
                    "debit_account",
                    "credit_account",
                ]

                for col in required_columns:
                    if col not in output_df.columns:
                        output_df[col] = ""

                output_df = output_df[
                    required_columns
                    + [
                        col
                        for col in output_df.columns
                        if col not in required_columns
                    ]
                ]

                # Save the file
                if output_file.endswith(".ods"):
                    output_df.to_excel(output_file, index=False, engine="odf")
                else:
                    output_df.to_excel(output_file, index=False)

                self.logger.info(f"Saved output to {output_file}")

            return output_df

        except Exception as e:
            self.logger.error(f"Error processing bank statement: {e}")
            import traceback

            traceback.print_exc()
            return pd.DataFrame()
        finally:
            self.close()


def test_integrated_processor():
    """Test the integrated processor with counterparty extraction"""
    processor = IntegratedBankProcessor()

    if processor.connect():
        print("Connected to database successfully")

        # Test with example transaction for Sang Tam transfer
        example1 = RawTransaction(
            reference="0771NE9A-7YKBRMYAB",
            datetime=datetime(2025, 3, 1, 11, 17, 53),
            debit_amount=94000000,
            credit_amount=0,
            balance=55065443,
            description="Chuyen tien tu TK BIDV 3840 Sang Tam qua TK BIDV 7655 Sang Tam",
        )

        # Test with VCB-ACB transfer example
        example1b = RawTransaction(
            reference="0771NE9A-7YKBRMYBC",
            datetime=datetime(2025, 3, 1, 11, 17, 53),
            debit_amount=50000000,
            credit_amount=0,
            balance=55065443,
            description="CHUYEN TIEN TU TK VCB (7803) SANG TAM QUA TK ACB (8368) SANG TAM GD 023763-061425 18:11:43",
        )

        # Test with ONL KDV PO example (single PO number)
        example1c = RawTransaction(
            reference="0771NE9A-9YKBRMYCD",
            datetime=datetime(2025, 3, 3, 14, 30, 45),
            debit_amount=0,
            credit_amount=2500000,
            balance=62565443,
            description="ONL KDV PO DEF456",
        )

        # Test with ONL KDV PO example (multiple PO numbers)
        example1d = RawTransaction(
            reference="0771NE9A-0YKBRMYDE",
            datetime=datetime(2025, 3, 4, 16, 45, 12),
            debit_amount=0,
            credit_amount=3750000,
            balance=66315443,
            description="ONL KDV PO SON19448 SON19538   Ma g iao dich  Trace372024 Trace 372024",
        )

        example2 = RawTransaction(
            reference="0771NE9A-8YKBRMYAC",
            datetime=datetime(2025, 3, 2, 10, 15, 30),
            debit_amount=0,
            credit_amount=5000000,
            balance=60065443,
            description="CK den tu CTY TNHH THUONG MAI DICH VU ABC thanh toan hoa don",
        )

        print("\nProcessing BIDV Sang Tam transfer (3840 -> 7655):")
        entry1 = processor.process_transaction(example1)
        if entry1:
            print(f"Original: {example1.description}")
            print(f"Formatted: {entry1.description}")
            print(f"Document Type: {entry1.document_type}")
            print(f"Amount: {entry1.amount1:,.0f}")
            print(f"Debit Account: {entry1.debit_account}")
            print(f"Credit Account: {entry1.credit_account}")

        print("\nProcessing VCB-ACB Sang Tam transfer (7803 -> 8368):")
        entry1b = processor.process_transaction(example1b)
        if entry1b:
            print(f"Original: {example1b.description}")
            print(f"Formatted: {entry1b.description}")
            print(f"Document Type: {entry1b.document_type}")
            print(f"Amount: {entry1b.amount1:,.0f}")
            print(f"Debit Account: {entry1b.debit_account}")
            print(f"Credit Account: {entry1b.credit_account}")

        print("\nProcessing ONL KDV PO transaction (single PO number):")
        entry1c = processor.process_transaction(example1c)
        if entry1c:
            print(f"Original: {example1c.description}")
            print(f"Formatted: {entry1c.description}")
            print(f"Document Type: {entry1c.document_type}")
            print(f"Amount: {entry1c.amount1:,.0f}")
            print(f"Debit Account: {entry1c.debit_account}")
            print(f"Credit Account: {entry1c.credit_account}")

        print("\nProcessing ONL KDV PO transaction (multiple PO numbers):")
        entry1d = processor.process_transaction(example1d)
        if entry1d:
            print(f"Original: {example1d.description}")
            print(f"Formatted: {entry1d.description}")
            print(f"Document Type: {entry1d.document_type}")
            print(f"Amount: {entry1d.amount1:,.0f}")
            print(f"Debit Account: {entry1d.debit_account}")
            print(f"Credit Account: {entry1d.credit_account}")

        print("\nProcessing transaction with counterparty extraction:")
        entry2 = processor.process_transaction(example2)
        if entry2:
            print(f"Document Type: {entry2.document_type}")
            print(f"Date: {entry2.date}")
            print(f"Document Number: {entry2.document_number}")
            print(
                f"Counterparty: {entry2.counterparty_code} - {entry2.counterparty_name}"
            )
            print(f"Description: {entry2.description}")
            print(f"Amount: {entry2.amount1:,.0f}")
            print(f"Debit Account: {entry2.debit_account}")
            print(f"Credit Account: {entry2.credit_account}")

        processor.close()
    else:
        print("Failed to connect to database")


if __name__ == "__main__":
    # Test the integrated processor
    test_integrated_processor()

    # Process BIDV file if arguments provided
    if len(sys.argv) > 1:
        input_file = sys.argv[1]
        output_file = sys.argv[2] if len(sys.argv) > 2 else "saoke_output.xlsx"

        processor = IntegratedBankProcessor()
        if processor.connect():
            try:
                df = processor.process_to_saoke(input_file, output_file)
                print(f"\nProcessed {len(df)} transactions")
            except Exception as e:
                print(f"Error processing file: {e}")
            finally:
                processor.close()
        else:
            print("Failed to connect to database")
