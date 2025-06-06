#!/usr/bin/env python3
"""
Fixed Bank Statement Processor with Account Code Extraction

This version fixes the issue with accessing database results from Limbo,
ensuring account codes are correctly extracted from transaction descriptions.

Key fixes:
- Properly access Limbo database results using index instead of key-value
- Modified _load_accounts_cache to correctly extract and store account codes
- Reduced logging for cleaner terminal output
"""

import io
import re
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import List, Optional, Tuple, Union

import limbo
import pandas as pd

# Add the project root to the path if running as main script
if __name__ == "__main__":
    project_root = Path(__file__).parent.parent.parent
    sys.path.insert(0, str(project_root))

from src.accounting.bank_statement_processor import (
    RawTransaction,
    SaokeEntry,
    TransactionRule,
)
from src.accounting.bank_statement_reader import BankStatementReader
from src.util.logging import get_logger

logger = get_logger(__name__)


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


class FixedBankProcessor:
    """
    Fixed processor with proper Limbo database result handling
    """

    def __init__(self, db_path: str = "banking_enterprise.db"):
        self.db_path = Path(__file__).parent / db_path
        self.conn = None
        self.cursor = None
        self.reader = BankStatementReader()
        self.logger = logger

        # Document number counters
        self.doc_counters = {"BC": 1, "BN": 1}

        # Caches for database lookups
        self.account_cache = {}  # Cache for account lookups
        self.rule_cache = {}

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

    def connect(self) -> bool:
        """Connect to Limbo/SQLite database"""
        try:
            if not self.db_path.exists():
                self.logger.error(
                    f"Database file {self.db_path} does not exist"
                )
                return False

            self.conn = limbo.connect(str(self.db_path))
            self.cursor = self.conn.cursor()
            self.logger.info(f"Connected to database: {self.db_path}")

            # Pre-load all accounts into cache for faster lookups
            self._load_accounts_cache()

            return True
        except Exception as e:
            self.logger.error(f"Database connection error: {e}")
            return False

    def close(self):
        """Close database connection"""
        if self.conn:
            self.conn.close()
            self.conn = None
            self.cursor = None

    def _load_accounts_cache(self):
        """Pre-load all accounts into memory for faster searching"""
        try:
            query = "SELECT code, name FROM accounts WHERE is_detail = 1"
            self.cursor.execute(query)
            rows = self.cursor.fetchall()

            self.account_cache = {}
            count = 0

            for row in rows:
                # Limbo returns tuples, not dict-like objects
                account_code = row[0]  # Index 0 for the 'code' column
                account_name = row[1]  # Index 1 for the 'name' column

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
                f"Loaded {len(self.account_cache)} numeric codes from {len(rows)} accounts table entries"
            )

        except Exception as e:
            self.logger.error(f"Error loading accounts cache: {e}")
            import traceback

            traceback.print_exc()

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
        Find account by numeric code in the name column

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

        # If not found in cache, try direct database lookup
        try:
            query = "SELECT code, name FROM accounts WHERE name LIKE ? LIMIT 1"
            like_pattern = f"%{numeric_code}%"

            self.cursor.execute(query, (like_pattern,))
            result = self.cursor.fetchone()

            if result:
                return AccountMatch(
                    code=result[0],  # Index 0 for the 'code' column
                    name=result[1],  # Index 1 for the 'name' column
                    numeric_code=numeric_code,
                    position=0,
                )
        except Exception as e:
            self.logger.error(f"Error in direct account lookup: {e}")

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
        """Get transaction rules from database"""
        if is_credit in self.rule_cache:
            return self.rule_cache[is_credit]

        try:
            query = """
                SELECT * FROM transaction_rules
                WHERE is_credit = ? AND is_active = 1
                ORDER BY priority DESC
            """
            self.cursor.execute(query, (1 if is_credit else 0,))
            rows = self.cursor.fetchall()

            # Get column names
            column_names = [desc[0] for desc in self.cursor.description]

            if not rows:
                # Return default rule
                default_rule = TransactionRule(
                    id=0,
                    pattern=".*",
                    document_type="BC" if is_credit else "BN",
                    transaction_type=None,
                    counterparty_code="KL" if is_credit else None,
                    department_code=None,
                    cost_code=None if is_credit else "KHAC",
                    description_template="Thu tiền khác"
                    if is_credit
                    else "Chi tiền khác",
                    is_credit=is_credit,
                    priority=1,
                )
                return [default_rule]

            rules = []
            for row in rows:
                # Convert tuple to dict with column names
                row_dict = {
                    column_names[i]: row[i] for i in range(len(column_names))
                }

                rule = TransactionRule(
                    id=row_dict["id"],
                    pattern=row_dict["pattern"],
                    document_type=row_dict["document_type"],
                    transaction_type=row_dict.get("transaction_type"),
                    counterparty_code=row_dict.get("counterparty_code"),
                    department_code=row_dict.get("department_code"),
                    cost_code=row_dict.get("cost_code"),
                    description_template=row_dict.get("description_template"),
                    is_credit=bool(row_dict["is_credit"]),
                    priority=row_dict["priority"],
                )
                rules.append(rule)

            self.rule_cache[is_credit] = rules
            return rules

        except Exception as e:
            self.logger.error(f"Error getting transaction rules: {e}")
            # Return default rule
            default_rule = TransactionRule(
                id=0,
                pattern=".*",
                document_type="BC" if is_credit else "BN",
                transaction_type=None,
                counterparty_code="KL" if is_credit else None,
                department_code=None,
                cost_code=None if is_credit else "KHAC",
                description_template="Thu tiền khác"
                if is_credit
                else "Chi tiền khác",
                is_credit=is_credit,
                priority=1,
            )
            return [default_rule]

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

    def determine_accounts(
        self,
        description: str,
        document_type: str,
        is_credit: bool,
        transaction_type: Optional[str] = None,
        pos_code: Optional[str] = None,
        department_code: Optional[str] = None,
    ) -> Tuple[str, str]:
        """
        Enhanced account determination with code extraction

        Args:
            description: Transaction description
            document_type: Document type (BC or BN)
            is_credit: Whether this is a credit transaction
            transaction_type: Transaction type if known
            pos_code: POS machine code if applicable
            department_code: Department code if known

        Returns:
            Tuple of (debit_account, credit_account)
        """
        # First try to determine accounts by extracting numeric codes
        debit_by_code, credit_by_code = self.determine_accounts_by_codes(
            description, is_credit, document_type, transaction_type
        )

        # If we found accounts by code extraction, use them
        if debit_by_code or credit_by_code:
            # Fill in missing account with defaults
            if document_type == "BC":  # Receipt
                if not credit_by_code:
                    # Default credit account for receipts
                    credit_by_code = "1311"  # Accounts receivable
                if not debit_by_code:
                    # This shouldn't happen for receipts, but use default bank
                    debit_by_code = "1121114"
            else:  # BN - Payment
                if not debit_by_code:
                    # Default debit account for payments
                    debit_by_code = "6278"  # Other expenses
                if not credit_by_code:
                    # This shouldn't happen for payments, but use default bank
                    credit_by_code = "1121114"

            self.logger.info(
                f"Determined accounts by code extraction: Dr={debit_by_code}, Cr={credit_by_code}"
            )
            return debit_by_code, credit_by_code

        # Fallback to existing logic for POS and department-based determination
        try:
            # Check account_mapping_rules table
            self.cursor.execute(
                "SELECT name FROM sqlite_schema WHERE type='table' AND name='account_mapping_rules'"
            )
            if self.cursor.fetchone() is None:
                # Default accounts
                if document_type == "BC":
                    return "1121114", "1311"
                else:
                    return "6278", "1121114"

            # Try POS-specific rules
            if pos_code:
                query = """
                    SELECT * FROM account_mapping_rules
                    WHERE rule_type = 'POS' AND entity_code = ?
                    AND document_type = ? AND is_active = 1
                    ORDER BY priority DESC
                """
                self.cursor.execute(query, (pos_code, document_type))
                row = self.cursor.fetchone()
                if row:
                    # Get column names
                    column_names = [desc[0] for desc in self.cursor.description]
                    # Convert tuple to dict with column names
                    row_dict = {
                        column_names[i]: row[i]
                        for i in range(len(column_names))
                    }
                    return row_dict["debit_account"], row_dict["credit_account"]

            # Try department-specific rules
            if department_code:
                query = """
                    SELECT * FROM account_mapping_rules
                    WHERE rule_type = 'DEPT' AND entity_code = ?
                    AND document_type = ? AND is_active = 1
                    ORDER BY priority DESC
                """
                self.cursor.execute(query, (department_code, document_type))
                row = self.cursor.fetchone()
                if row:
                    # Get column names
                    column_names = [desc[0] for desc in self.cursor.description]
                    # Convert tuple to dict with column names
                    row_dict = {
                        column_names[i]: row[i]
                        for i in range(len(column_names))
                    }
                    return row_dict["debit_account"], row_dict["credit_account"]

            # Try transaction type rules
            if transaction_type:
                query = """
                    SELECT * FROM account_mapping_rules
                    WHERE rule_type = 'TYPE' AND entity_code = '*'
                    AND document_type = ? AND transaction_type = ? AND is_active = 1
                    ORDER BY priority DESC
                """
                self.cursor.execute(query, (document_type, transaction_type))
                row = self.cursor.fetchone()
                if row:
                    # Get column names
                    column_names = [desc[0] for desc in self.cursor.description]
                    # Convert tuple to dict with column names
                    row_dict = {
                        column_names[i]: row[i]
                        for i in range(len(column_names))
                    }
                    return row_dict["debit_account"], row_dict["credit_account"]

            # Default rules
            query = """
                SELECT * FROM account_mapping_rules
                WHERE rule_type = 'DEFAULT' AND entity_code = '*'
                AND document_type = ? AND is_active = 1
                ORDER BY priority DESC
            """
            self.cursor.execute(query, (document_type,))
            row = self.cursor.fetchone()
            if row:
                # Get column names
                column_names = [desc[0] for desc in self.cursor.description]
                # Convert tuple to dict with column names
                row_dict = {
                    column_names[i]: row[i] for i in range(len(column_names))
                }
                return row_dict["debit_account"], row_dict["credit_account"]

        except Exception as e:
            self.logger.error(f"Error in account determination: {e}")

        # Final fallback
        if document_type == "BC":
            return "1121114", "1311"
        else:
            return "6278", "1121114"

    def generate_document_number(self, doc_type: str, date: datetime) -> str:
        """Generate document number for saoke format"""
        month = date.month
        counter = self.doc_counters.get(doc_type, 1)
        doc_number = f"{doc_type}{month:02d}/{counter:03d}"
        self.doc_counters[doc_type] = counter + 1
        return doc_number

    def format_date(self, dt: datetime, format_type: str = "vn") -> str:
        """Format date for saoke format"""
        if format_type == "vn":
            return f"{dt.day:02d}/{dt.month}/{dt.year}"
        elif format_type == "alt":
            return f"{dt.month}/{dt.day:02d}/{dt.year}"
        else:
            return f"{dt.day:02d}/{dt.month:02d}/{dt.year}"

    def process_transaction(
        self, transaction: RawTransaction
    ) -> Optional[SaokeEntry]:
        """Process a single transaction into a saoke entry"""
        try:
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

            # Extract all numeric codes for processing
            codes = self.extract_numeric_codes(transaction.description)

            # Determine accounts with enhanced code extraction
            debit_account, credit_account = self.determine_accounts(
                description=transaction.description,
                document_type=rule.document_type,
                is_credit=is_credit,
                transaction_type=rule.transaction_type,
                pos_code=None,  # Will be extracted if POS transaction
                department_code=rule.department_code,
            )

            # Get counterparty info
            counterparty_code = rule.counterparty_code or "KL"
            counterparty_name = "Khách Lẻ Không Lấy Hóa Đơn"
            address = ""

            if counterparty_code:
                try:
                    query = "SELECT name, address FROM counterparties WHERE code = ?"
                    self.cursor.execute(query, (counterparty_code,))
                    row = self.cursor.fetchone()
                    if row:
                        # Limbo returns tuples, access by index
                        counterparty_name = row[0]  # name
                        address = row[1] or ""  # address
                except Exception as e:
                    self.logger.error(f"Error getting counterparty: {e}")

            # Format description
            description = rule.description_template or transaction.description

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
                department=None,
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
        """Process a bank statement file to saoke format"""
        if not self.conn and not self.connect():
            raise RuntimeError("Database connection failed")

        try:
            # Reset document counters
            self.doc_counters = {"BC": 1, "BN": 1}

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


def test_fixed_processor():
    """Test the fixed processor with account extraction"""
    processor = FixedBankProcessor()

    if processor.connect():
        print("Connected to database successfully")

        # Test with example transaction
        example = RawTransaction(
            reference="0771NE9A-7YKBRMYAB",
            datetime=datetime(2025, 3, 1, 11, 17, 53),
            debit_amount=94000000,
            credit_amount=0,
            balance=55065443,
            description="Chuyen tien tu TK BIDV 3840 Sang Tam qua TK BIDV 7655 Sang Tam",
        )

        print("\nProcessing transaction with account codes 3840 and 7655:")
        entry = processor.process_transaction(example)
        if entry:
            print(f"Document Type: {entry.document_type}")
            print(f"Date: {entry.date}")
            print(f"Document Number: {entry.document_number}")
            print(f"Description: {entry.description}")
            print(f"Amount: {entry.amount1:,.0f}")
            print(f"Debit Account: {entry.debit_account}")
            print(f"Credit Account: {entry.credit_account}")

        processor.close()
    else:
        print("Failed to connect to database")


if __name__ == "__main__":
    # Test the fixed processor
    test_fixed_processor()

    # Process BIDV file if arguments provided
    if len(sys.argv) > 1:
        input_file = sys.argv[1]
        output_file = sys.argv[2] if len(sys.argv) > 2 else "saoke_output.xlsx"

        processor = FixedBankProcessor()
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
