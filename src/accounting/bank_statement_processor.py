#!/usr/bin/env python3
"""
Enhanced Bank Statement Processor with Dynamic Account Determination

This module processes bank statements (BIDV) to convert them to 'saoke' format
for accounting purposes, using dynamic account determination based on transaction
metadata rather than hardcoded accounts in transaction rules.
"""

import io
import re
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Union

import limbo  # Using Limbo for database access
import pandas as pd

from src.accounting.bank_statement_reader import BankStatementReader
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
class AccountMappingRule:
    """Represents a rule for mapping transaction metadata to accounts"""

    id: int
    rule_type: str
    entity_code: str
    document_type: str
    transaction_type: Optional[str]
    debit_account: str
    credit_account: str
    priority: int


@dataclass
class POSMachine:
    """Represents a POS machine in the system"""

    code: str
    department_code: str
    name: str
    address: Optional[str]
    account_holder: Optional[str]
    account_number: Optional[str]
    bank_name: Optional[str]


@dataclass
class Department:
    """Represents a department in the system"""

    code: str
    name: str
    parent_code: Optional[str]
    is_detail: bool
    data_source: Optional[str]


@dataclass
class Counterparty:
    """Represents a counterparty (supplier, customer, etc.)"""

    code: str
    name: str
    address: Optional[str]


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


class BankStatementProcessor:
    """
    Processes bank statements and generates saoke entries
    for accounting systems, using dynamic account determination
    """

    def __init__(self, db_path: str = "banking_enterprise.db"):
        """Initialize with path to SQLite database"""
        self.db_path = Path(__file__).parent / db_path
        self.conn = None
        self.cursor = None
        self.reader = BankStatementReader()
        self.logger = logger

        # Document number counters
        self.doc_counters = {
            "BC": 1,  # Credit counter
            "BN": 1,  # Debit counter
        }

        # Cache for database lookups
        self.pos_cache = {}
        self.dept_cache = {}
        self.counterparty_cache = {}
        self.rule_cache = {}
        self.account_mapping_cache = {}

        # Common regex patterns
        self.pos_patterns = [
            r"POS\s+(\d{7,8})",  # POS followed by 7-8 digits
            r"POS(\d{7,8})",  # POS directly followed by digits
            r"POS[\s\-:_]+(\d{7,8})",  # POS with various separators
        ]

        self.location_patterns = [
            r"GO\s+([A-Z]+)",  # GO followed by uppercase letters
            r"GO([A-Z]+)",  # GO directly followed by uppercase letters
            r"CH\s+([A-Z0-9]+)",  # CH (cửa hàng) followed by code
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

    def connect(self) -> bool:
        """Connect to the database using Limbo"""
        try:
            if not self.db_path.exists():
                self.logger.error(
                    f"Database file {self.db_path} does not exist"
                )
                return False

            # Using Limbo for database connection
            self.conn = limbo.connect(str(self.db_path))
            self.cursor = self.conn.cursor()
            self.logger.info(f"Connected to Limbo database: {self.db_path}")
            return True
        except Exception as e:
            self.logger.error(f"Error connecting to Limbo database: {e}")
            return False

    def close(self):
        """Close the database connection"""
        if self.conn:
            self.conn.close()
            self.conn = None
            self.cursor = None

    def get_transaction_rules(self, is_credit: bool) -> List[TransactionRule]:
        """Get transaction rules for matching"""
        if is_credit in self.rule_cache:
            return self.rule_cache[is_credit]

        try:
            # First check if the table exists
            self.cursor.execute(
                "SELECT name FROM sqlite_schema WHERE type='table' AND name='transaction_rules'"
            )
            if self.cursor.fetchone() is None:
                self.logger.error("transaction_rules table does not exist")
                return []

            query = """
                SELECT * FROM transaction_rules
                WHERE is_credit = ? AND is_active = 1
                ORDER BY priority DESC
            """
            self.cursor.execute(query, (1 if is_credit else 0,))
            rows = self.cursor.fetchall()

            # Handle None result from fetchall()
            if rows is None or len(rows) == 0:
                self.logger.warning(
                    f"No transaction rules found for is_credit={is_credit}"
                )
                # Return a default rule to prevent further errors
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
                # Convert row to dict for easier access
                row_dict = dict(
                    zip([column[0] for column in self.cursor.description], row)
                )

                rule = TransactionRule(
                    id=row_dict["id"],
                    pattern=row_dict["pattern"],
                    document_type=row_dict["document_type"],
                    transaction_type=row_dict.get(
                        "transaction_type"
                    ),  # May not exist in older databases
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
            self.logger.error(
                f"Error getting transaction rules: {e}\nReturning default rule"
            )
            import traceback

            traceback.print_exc()
            # Return a default rule to prevent cascading errors
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

    def get_account_mapping_rules(self) -> List[AccountMappingRule]:
        """Get account mapping rules"""
        if "all" in self.account_mapping_cache:
            return self.account_mapping_cache["all"]

        try:
            # Check if account_mapping_rules table exists
            self.cursor.execute(
                "SELECT name FROM sqlite_schema WHERE type='table' AND name='account_mapping_rules'"
            )
            if self.cursor.fetchone() is None:
                self.logger.warning(
                    "account_mapping_rules table does not exist"
                )
                return []

            query = """
                SELECT * FROM account_mapping_rules
                WHERE is_active = 1
                ORDER BY priority DESC
            """
            self.cursor.execute(query)
            rows = self.cursor.fetchall()

            # Handle None result from fetchall()
            if rows is None:
                rows = []

            rules = []
            for row in rows:
                # Convert row to dict for easier access
                row_dict = dict(
                    zip([column[0] for column in self.cursor.description], row)
                )

                rule = AccountMappingRule(
                    id=row_dict["id"],
                    rule_type=row_dict["rule_type"],
                    entity_code=row_dict["entity_code"],
                    document_type=row_dict["document_type"],
                    transaction_type=row_dict["transaction_type"],
                    debit_account=row_dict["debit_account"],
                    credit_account=row_dict["credit_account"],
                    priority=row_dict["priority"],
                )
                rules.append(rule)

            self.account_mapping_cache["all"] = rules
            return rules
        except Exception as e:
            self.logger.error(f"Error getting account mapping rules: {e}")
            return []

    def match_transaction_rule(
        self, description: str, is_credit: bool
    ) -> Optional[TransactionRule]:
        """Match a transaction description to a rule"""
        rules = self.get_transaction_rules(is_credit)
        normalized_desc = description.lower()

        for rule in rules:
            pattern = rule.pattern
            if re.search(pattern, normalized_desc, re.IGNORECASE):
                self.logger.debug(
                    f"Matched rule: {pattern} for '{description}'"
                )
                return rule

        # Return default rule if no match (should be last in priority order)
        if rules:
            self.logger.debug(f"Using default rule for '{description}'")
            return rules[-1]

        return None

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

    def get_pos_machine(self, pos_code: str) -> Optional[POSMachine]:
        """Get POS machine details from database"""
        if pos_code in self.pos_cache:
            return self.pos_cache[pos_code]

        try:
            query = "SELECT * FROM pos_machines WHERE code = ?"
            self.cursor.execute(query, (pos_code,))
            row = self.cursor.fetchone()

            if row is not None:
                # Convert row to dict if it's a tuple
                if isinstance(row, tuple):
                    columns = [col[0] for col in self.cursor.description]
                    row_dict = dict(zip(columns, row))
                else:
                    row_dict = row

                    pos = POSMachine(
                        code=row_dict.get("code", ""),
                        department_code=row_dict.get("department_code", ""),
                        name=row_dict.get("name", ""),
                        address=row_dict.get("address"),
                        account_holder=row_dict.get("account_holder"),
                        account_number=row_dict.get("account_number"),
                        bank_name=row_dict.get("bank_name"),
                    )
                    self.pos_cache[pos_code] = pos
                    return pos

        except Exception as e:
            self.logger.error(f"Error getting POS machine {pos_code}: {e}")

        return None

    def get_department(self, dept_code: str) -> Optional[Department]:
        """Get department details from database"""
        if dept_code in self.dept_cache:
            return self.dept_cache[dept_code]

        if not dept_code:
            return None

        try:
            query = "SELECT * FROM departments WHERE code = ?"
            self.cursor.execute(query, (dept_code,))
            row = self.cursor.fetchone()

            if row is not None:
                # Convert row to dict for easier access
                row_dict = dict(
                    zip([column[0] for column in self.cursor.description], row)
                )

                dept = Department(
                    code=row_dict["code"],
                    name=row_dict["name"],
                    parent_code=row_dict["parent_code"],
                    is_detail=bool(row_dict["is_detail"]),
                    data_source=row_dict["data_source"],
                )
                self.dept_cache[dept_code] = dept
                return dept

        except Exception as e:
            self.logger.error(f"Error getting department {dept_code}: {e}")

        return None

    def get_counterparty(self, party_code: str) -> Optional[Counterparty]:
        """Get counterparty details from database"""
        if not party_code:
            return None

        if party_code in self.counterparty_cache:
            return self.counterparty_cache[party_code]

        try:
            query = (
                "SELECT code, name, address FROM counterparties WHERE code = ?"
            )
            self.cursor.execute(query, (party_code,))
            row = self.cursor.fetchone()

            if row is not None:
                # Convert row to dict for easier access
                row_dict = dict(
                    zip([column[0] for column in self.cursor.description], row)
                )

                party = Counterparty(
                    code=row_dict["code"],
                    name=row_dict["name"],
                    address=row_dict["address"],
                )
                self.counterparty_cache[party_code] = party
                return party

            # Special case for retail customers
            if party_code in ["KL", "KL-GOBARIA1"]:
                party = Counterparty(
                    code=party_code,
                    name="Khách Lẻ Không Lấy Hóa Đơn",
                    address="QH 1S9+10 TTTM Go BR-VT, Số 2A, Nguyễn Đình Chiểu, KP 1, P.Phước Hiệp, TP.Bà Rịa, T.Bà Rịa-Vũng Tàu",
                )
                self.counterparty_cache[party_code] = party
                return party

        except Exception as e:
            self.logger.error(f"Error getting counterparty {party_code}: {e}")

        return None

    def determine_accounts(
        self,
        document_type: str,
        pos_code: Optional[str] = None,
        department_code: Optional[str] = None,
        transaction_type: Optional[str] = None,
    ) -> Tuple[str, str]:
        """
        Determine debit and credit accounts based on transaction metadata
        using the account mapping rules
        """
        # Check if account_mapping_rules table exists
        self.cursor.execute(
            "SELECT name FROM sqlite_schema WHERE type='table' AND name='account_mapping_rules'"
        )
        if self.cursor.fetchone() is None:
            # Fallback to using transaction rules if account_mapping_rules doesn't exist
            self.logger.warning(
                "account_mapping_rules table does not exist, falling back to transaction rules"
            )
            return self._fallback_account_determination(document_type)

        # Get all account mapping rules
        all_rules = self.get_account_mapping_rules()
        matching_rules = []

        # Try POS-specific rules first (highest priority)
        if pos_code:
            for rule in all_rules:
                if (
                    rule.rule_type == "POS"
                    and rule.entity_code == pos_code
                    and rule.document_type == document_type
                    and (
                        rule.transaction_type == transaction_type
                        or rule.transaction_type == "*"
                        or not transaction_type
                    )
                ):
                    matching_rules.append(rule)

        # Try department-specific rules next
        if not matching_rules and department_code:
            for rule in all_rules:
                if (
                    rule.rule_type == "DEPT"
                    and rule.entity_code == department_code
                    and rule.document_type == document_type
                    and (
                        rule.transaction_type == transaction_type
                        or rule.transaction_type == "*"
                        or not transaction_type
                    )
                ):
                    matching_rules.append(rule)

        # Try transaction type rules next
        if not matching_rules and transaction_type:
            for rule in all_rules:
                if (
                    rule.rule_type == "TYPE"
                    and rule.entity_code == "*"
                    and rule.document_type == document_type
                    and (
                        rule.transaction_type == transaction_type
                        or rule.transaction_type == "*"
                    )
                ):
                    matching_rules.append(rule)

        # Try default rules as last resort
        if not matching_rules:
            for rule in all_rules:
                if (
                    rule.rule_type == "DEFAULT"
                    and rule.entity_code == "*"
                    and rule.document_type == document_type
                ):
                    matching_rules.append(rule)

        # Sort by priority and take the highest
        if matching_rules:
            matching_rules.sort(key=lambda r: r.priority, reverse=True)
            best_rule = matching_rules[0]
            self.logger.debug(
                f"Using account mapping rule: {best_rule.rule_type}-{best_rule.entity_code}"
            )
            return best_rule.debit_account, best_rule.credit_account

        # Fallback default accounts if no rules match
        return self._fallback_account_determination(document_type)

    def _fallback_account_determination(
        self, document_type: str
    ) -> Tuple[str, str]:
        """Fallback account determination if no mapping rules exist"""
        if document_type == "BC":  # Credit/Receipt
            return "1121114", "1311"  # Bank account, Accounts receivable
        else:  # Debit/Payment
            return "6278", "1121114"  # Other expenses, Bank account

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

            # Extract POS code if present
            pos_code = self.extract_pos_code(transaction.description)
            pos_machine = None
            department_code = rule.department_code
            department_name = None

            if pos_code:
                pos_machine = self.get_pos_machine(pos_code)
                if pos_machine and pos_machine.department_code:
                    department_code = pos_machine.department_code

            # Get department details
            department = None
            if department_code:
                department = self.get_department(department_code)
                if department:
                    department_name = department.name

            # If no department found from POS or rule, try to extract from description
            if not department_name:
                location_code = self.extract_location_code(
                    transaction.description
                )
                if location_code:
                    department_name = location_code

            # Get counterparty details
            counterparty_code = (
                rule.counterparty_code or "KL-GOBARIA1"
            )  # Default to retail customer
            counterparty = self.get_counterparty(counterparty_code)

            # Identify transaction type
            transaction_type = rule.transaction_type
            if not transaction_type:
                transaction_type = self.identify_transaction_type(
                    transaction.description
                )

            # Determine accounts using dynamic account determination
            debit_account, credit_account = self.determine_accounts(
                document_type=rule.document_type,
                pos_code=pos_code,
                department_code=department_code,
                transaction_type=transaction_type,
            )

            # Prepare description
            description = rule.description_template or "Giao dịch ngân hàng"

            # Replace placeholders in description
            if "{pos_code}" in description and pos_code:
                if department_name:
                    description = description.replace(
                        "{pos_code}", f"{pos_code} - {department_name}"
                    )
                else:
                    description = description.replace("{pos_code}", pos_code)

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

            # Calculate amounts
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
                counterparty_name=counterparty.name
                if counterparty
                else "Khách Lẻ Không Lấy Hóa Đơn",
                address=counterparty.address
                if counterparty and counterparty.address
                else "",
                description=description,
                original_description=transaction.description,
                amount1=amount,
                amount2=amount,
                debit_account=debit_account,
                credit_account=credit_account,
                cost_code=rule.cost_code,
                department=department_name,
                sequence=sequence,
                date2=self.format_date(transaction.datetime, "alt"),
                transaction_type=transaction_type,
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
        if not self.conn and not self.connect():
            raise RuntimeError("Database connection failed")

        try:
            # Reset document counters
            self.doc_counters = {"BC": 1, "BN": 1}

            # Read transactions from input
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
                    # Check for required fields
                    if "date" not in row or pd.isna(row["date"]):
                        self.logger.warning(
                            f"Skipping row with missing date: {row}"
                        )
                        continue

                    # Create transaction object
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
                    self.logger.error(
                        f"Error converting row to transaction: {e}"
                    )
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
            if output_file and saoke_entries:
                output_df.to_excel(output_file, index=False)
                self.logger.info(f"Saved output to {output_file}")

            return output_df

        except Exception as e:
            self.logger.error(f"Error processing bank statement: {e}")
            import traceback

            traceback.print_exc()
            return pd.DataFrame()


if __name__ == "__main__":
    # Simple test
    processor = BankStatementProcessor()
    if processor.connect():
        print("Connected to database successfully")

        # Test with example transaction
        example = RawTransaction(
            reference="0001NFS4-7YJO3BV08",
            datetime=datetime(2025, 3, 1, 11, 17, 53),
            debit_amount=0,
            credit_amount=888000,
            balance=112028712,
            description="TT POS 14100333 500379 5777 064217PDO",
        )

        entry = processor.process_transaction(example)
        if entry:
            print("\nProcessed Transaction:")
            print(f"Document Type: {entry.document_type}")
            print(f"Date: {entry.date}")
            print(f"Document Number: {entry.document_number}")
            print(f"Description: {entry.description}")
            print(f"Amount: {entry.amount1:,.0f}")
            print(f"Debit Account: {entry.debit_account}")
            print(f"Credit Account: {entry.credit_account}")
            print(f"Department: {entry.department}")
            print(f"Transaction Type: {entry.transaction_type}")

        processor.close()
    else:
        print("Failed to connect to database")
