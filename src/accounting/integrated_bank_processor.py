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

            # Get all accounts using fast_search
            # We need to search for a pattern that will match all accounts
            # Using an empty string should return all accounts with a high limit
            all_accounts = search_accounts("", field_name="code", limit=1000)

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

            self.logger.info(
                f"Loaded {len(self.account_cache)} numeric codes from {len(all_accounts)} accounts using fast_search"
            )

        except Exception as e:
            self.logger.error(f"Error loading accounts cache: {e}")
            import traceback

            traceback.print_exc()

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
            account_patterns = [
                r"(?:số tài khoản|so tai khoan|account number|account|tk)[\s:]+([0-9]+)",
                r"(?:stk|account number|account|tk)[\s:]+([0-9]+)",
                r"(?:[0-9]{6,10})[\s\-\._:]+([0-9]{4})",  # Match last 4 digits specifically
                r"\b(3840)\b",  # Match the specific account 3840 directly
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
                        if (
                            "tai khoan" in cell_text
                            or "tài khoản" in cell_text
                            or "stk" in cell_text
                            or "account" in cell_text
                        ):
                            # Check the next cell for the account number
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
        # Check cache first
        if numeric_code in self.account_cache:
            accounts = self.account_cache[numeric_code]
            if accounts:
                # Return the first match
                account = accounts[0]
                return AccountMatch(
                    code=account["code"],
                    name=account["name"],
                    numeric_code=numeric_code,
                    position=0,
                )

        # If not found in cache, use fast_search
        from src.accounting.fast_search import search_accounts

        # Try to find accounts that contain this numeric code in their name
        results = search_accounts(numeric_code, field_name="name", limit=1)

        if results:
            result = results[0]
            return AccountMatch(
                code=result["code"],
                name=result["name"],
                numeric_code=numeric_code,
                position=0,
            )

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
                location = match.group(0)  # Get the whole match (GO BRVT)
                self.logger.debug(
                    f"Extracted location: {location} from '{description}'"
                )
                return location

        return None

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
            return "6278", self.default_bank_account

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
        # First try to determine accounts by extracting numeric codes
        debit_by_code, credit_by_code = self.determine_accounts_by_codes(
            description, is_credit, document_type, transaction_type
        )

        # If we found accounts by code extraction, use them
        if debit_by_code and credit_by_code:
            self.logger.info(
                f"Determined complete accounts by code extraction: Dr={debit_by_code}, Cr={credit_by_code}"
            )
            return debit_by_code, credit_by_code

        # If we have a counterparty code, try to determine accounts based on that
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

        # If we have partial accounts from code extraction, fill in the missing ones
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
                    debit_by_code = "6278"  # Other expenses
                if not credit_by_code:
                    # This shouldn't happen for payments, but use default bank
                    credit_by_code = self.default_bank_account

            self.logger.info(
                f"Determined partial accounts by code extraction: Dr={debit_by_code}, Cr={credit_by_code}"
            )
            return debit_by_code, credit_by_code

        # Fallback to using fast_search for POS and department-based determination
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
                        # For POS-related payments
                        return "6278", self.default_bank_account

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
                        return "6278", self.default_bank_account

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
                        return "6278", self.default_bank_account
                    elif transaction_type == "SALARY":
                        return "6411", self.default_bank_account
                    elif transaction_type == "TAX":
                        return "3331", self.default_bank_account
                    elif transaction_type == "UTILITY":
                        return "6278", self.default_bank_account
                    elif transaction_type == "WITHDRAWAL":
                        return "1111", self.default_bank_account
                    elif transaction_type == "TRANSFER":
                        return "6278", self.default_bank_account
                    else:
                        return "6278", self.default_bank_account

            # Default mappings
            if document_type == "BC":
                return self.default_bank_account, "1311"
            else:
                return "6278", self.default_bank_account

        except Exception as e:
            self.logger.error(f"Error in account determination: {e}")

        # Final fallback
        if document_type == "BC":
            return self.default_bank_account, "1311"
        else:
            return "6278", self.default_bank_account

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
        if format_type == "vn":
            # Vietnamese format: DD/M/YYYY (e.g., 01/3/2025)
            return f"{dt.day:02d}/{dt.month}/{dt.year}"
        elif format_type == "alt":
            # Alternative format: M/DD/YYYY (e.g., 3/01/2025)
            return f"{dt.month}/{dt.day:02d}/{dt.year}"
        else:
            # Standard format: DD/MM/YYYY
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

            # Process extracted counterparties
            extracted_counterparties = extracted_entities.get(
                "counterparties", []
            )
            # Use the extracted counterparty if found, otherwise use the one from rule
            counterparty_code = rule.counterparty_code or "KL"
            counterparty_name = None

            if extracted_counterparties:
                best_match = extracted_counterparties[0]
                # Extract both code and name, prioritizing name for the final output
                counterparty_code = best_match.get("code") or counterparty_code
                counterparty_name = best_match.get("name")
                self.logger.info(
                    f"Extracted counterparty: {counterparty_code} - {counterparty_name}"
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

            # Get counterparty info from database
            if not counterparty_name:
                counterparty_name, address = self.get_counterparty_info(
                    counterparty_code
                )
            else:
                # Already have the name, just look up the address
                _, address = self.get_counterparty_info(counterparty_code)

            # Format description
            description = rule.description_template or transaction.description

            # Replace placeholders in description
            if "{pos_code}" in description:
                if pos_name:  # Prioritize POS name when available
                    if department_code:
                        description = description.replace(
                            "{pos_code}", f"{pos_name} - {department_code}"
                        )
                    else:
                        description = description.replace(
                            "{pos_code}", pos_name
                        )
                elif pos_code:  # Fall back to code if name not available
                    if department_code:
                        description = description.replace(
                            "{pos_code}", f"{pos_code} - {department_code}"
                        )
                    else:
                        description = description.replace(
                            "{pos_code}", pos_code
                        )

            # If we have an extracted counterparty, update the description
            if counterparty_name and "{counterparty}" in description:
                description = description.replace(
                    "{counterparty}", counterparty_name
                )

            # Handle {suffix} placeholder
            if "{suffix}" in description:
                # Extract transaction sequence number if possible
                if pos_code and "_" in transaction.description:
                    seq_match = re.search(
                        r"_(\d+)_(\d+)", transaction.description
                    )
                    if seq_match:
                        suffix = f"{seq_match.group(1)}_{seq_match.group(2)}"
                    else:
                        suffix = "1"
                else:
                    suffix = "1"
                description = description.replace("{suffix}", suffix)

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

        # Test with example transaction
        example1 = RawTransaction(
            reference="0771NE9A-7YKBRMYAB",
            datetime=datetime(2025, 3, 1, 11, 17, 53),
            debit_amount=94000000,
            credit_amount=0,
            balance=55065443,
            description="Chuyen tien tu TK BIDV 3840 Sang Tam qua TK BIDV 7655 Sang Tam",
        )

        example2 = RawTransaction(
            reference="0771NE9A-8YKBRMYAC",
            datetime=datetime(2025, 3, 2, 10, 15, 30),
            debit_amount=0,
            credit_amount=5000000,
            balance=60065443,
            description="CK den tu CTY TNHH THUONG MAI DICH VU ABC thanh toan hoa don",
        )

        print("\nProcessing transaction with account codes 3840 and 7655:")
        entry1 = processor.process_transaction(example1)
        if entry1:
            print(f"Document Type: {entry1.document_type}")
            print(f"Date: {entry1.date}")
            print(f"Document Number: {entry1.document_number}")
            print(f"Description: {entry1.description}")
            print(f"Amount: {entry1.amount1:,.0f}")
            print(f"Debit Account: {entry1.debit_account}")
            print(f"Credit Account: {entry1.credit_account}")

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
