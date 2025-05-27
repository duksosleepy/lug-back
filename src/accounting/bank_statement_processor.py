import io
import re
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List, Optional, Union

import limbo
import pandas as pd

from src.util.logging import get_logger

logger = get_logger(__name__)


@dataclass
class TransactionMatch:
    """Represents a matched transaction rule result"""

    document_type: str  # BC or BN
    debit_account: str
    credit_account: str
    counterparty_code: Optional[str] = None
    department_code: Optional[str] = None
    cost_code: Optional[str] = None
    description_template: Optional[str] = None
    priority: int = 0


@dataclass
class RawTransaction:
    """Raw bank transaction data"""

    reference: str
    datetime: datetime
    debit_amount: float
    credit_amount: float
    balance: float
    description: str


@dataclass
class SaokeEntry:
    """Structured accounting entry (saoke format)"""

    document_type: str  # BC/BN
    date: str  # DD/MM/YYYY format
    document_number: str  # e.g., BC03/001
    currency: str  # VND
    sequence: int  # 1
    counterparty_code: str  # e.g., KL-GOBARIA1
    counterparty_name: str  # e.g., Khách Lẻ Không Lấy Hóa Đơn
    address: str  # Full address
    description: str  # Processed description
    original_description: str  # Original bank statement description
    amount1: float  # Transaction amount
    amount2: float  # Same as amount1
    debit_account: str  # e.g., 1121114
    credit_account: str  # e.g., 1311
    field1: float = 0  # Reserved fields
    field2: float = 0
    field3: float = 0
    field4: float = 0
    date2: str = ""  # MM/DD/YYYY format
    department: str = ""  # e.g., GO BRVT


class BankStatementProcessor:
    """
    Processes bank statements and transforms them to saoke format
    using the accounting database for enrichment and categorization.
    """

    def __init__(self, db_path: str = "banking_enterprise.db"):
        self.db_path = db_path
        self.conn = None
        self.cursor = None
        self.transaction_rules = []
        self.pos_machines = {}
        self.counterparties = {}
        self.departments = {}
        self.accounts = {}

        # Document number counters
        self.doc_counters = {"BC": 0, "BN": 0}

    def connect(self):
        """Connect to database and load reference data"""
        try:
            self.conn = limbo.connect(self.db_path)
            self.cursor = self.conn.cursor()
            logger.info(f"Connected to database: {self.db_path}")

            self._load_reference_data()
            return True
        except Exception as e:
            logger.error(f"Database connection error: {e}")
            return False

    def close(self):
        """Close database connection"""
        if self.conn:
            self.conn.close()

    def _load_reference_data(self):
        """Load reference data from database tables"""
        try:
            # Load transaction rules (ordered by priority)
            self.cursor.execute("""
                SELECT pattern, document_type, debit_account, credit_account,
                       counterparty_code, department_code, cost_code,
                       description_template, is_credit, priority
                FROM transaction_rules
                WHERE is_active = 1
                ORDER BY priority DESC, id
            """)
            self.transaction_rules = self.cursor.fetchall()

            # Load POS machines
            self.cursor.execute("""
                SELECT code, department_code, name, address,
                       account_holder, account_number, bank_name
                FROM pos_machines
            """)
            for row in self.cursor.fetchall():
                self.pos_machines[row[0]] = {
                    "department_code": row[1],
                    "name": row[2],
                    "address": row[3],
                    "account_holder": row[4],
                    "account_number": row[5],
                    "bank_name": row[6],
                }

            # Load counterparties
            self.cursor.execute(
                "SELECT code, name, address FROM counterparties"
            )
            for row in self.cursor.fetchall():
                self.counterparties[row[0]] = {
                    "name": row[1],
                    "address": row[2],
                }

            # Load departments
            self.cursor.execute("SELECT code, name FROM departments")
            for row in self.cursor.fetchall():
                self.departments[row[0]] = row[1]

            # Load accounts
            self.cursor.execute("SELECT code, name FROM accounts")
            for row in self.cursor.fetchall():
                self.accounts[row[0]] = row[1]

            logger.info(
                f"Loaded {len(self.transaction_rules)} rules, "
                f"{len(self.pos_machines)} POS machines, "
                f"{len(self.counterparties)} counterparties"
            )

        except Exception as e:
            logger.error(f"Error loading reference data: {e}")
            raise

    def read_bank_statement(
        self, file_path: Union[str, io.BytesIO]
    ) -> List[RawTransaction]:
        """Read and parse bank statement file using intelligent data area detection"""
        try:
            # Use the enhanced reader to detect and extract data
            from src.accounting.bank_statement_reader import BankStatementReader

            reader = BankStatementReader()
            df = reader.read_bank_statement(file_path, debug=True)

            if df.empty:
                raise ValueError("No transaction data found in bank statement")

            transactions = []
            for _, row in df.iterrows():
                try:
                    # Map standardized column names to transaction fields
                    reference = (
                        str(row.get("reference", ""))
                        if pd.notna(row.get("reference", ""))
                        else ""
                    )

                    # Handle date parsing
                    date_value = row.get("date", datetime.now())
                    if pd.isna(date_value):
                        date_value = datetime.now()
                    elif isinstance(date_value, str):
                        date_value = pd.to_datetime(date_value)

                    # Handle amounts
                    debit_amount = (
                        float(row.get("debit", 0))
                        if pd.notna(row.get("debit", 0))
                        else 0
                    )
                    credit_amount = (
                        float(row.get("credit", 0))
                        if pd.notna(row.get("credit", 0))
                        else 0
                    )
                    balance = (
                        float(row.get("balance", 0))
                        if pd.notna(row.get("balance", 0))
                        else 0
                    )

                    # Description
                    description = (
                        str(row.get("description", ""))
                        if pd.notna(row.get("description", ""))
                        else ""
                    )

                    transaction = RawTransaction(
                        reference=reference,
                        datetime=date_value,
                        debit_amount=debit_amount,
                        credit_amount=credit_amount,
                        balance=balance,
                        description=description,
                    )
                    transactions.append(transaction)

                except Exception as e:
                    logger.warning(f"Error parsing transaction row: {e}")
                    continue

            logger.info(
                f"Parsed {len(transactions)} transactions from bank statement"
            )
            return transactions

        except Exception as e:
            logger.error(f"Error reading bank statement: {e}")
            raise

    def match_transaction_rule(
        self, description: str, is_credit: bool
    ) -> Optional[TransactionMatch]:
        """Match transaction description against rules"""
        for rule in self.transaction_rules:
            (
                pattern,
                doc_type,
                debit_acc,
                credit_acc,
                counterparty,
                dept,
                cost,
                desc_template,
                rule_is_credit,
                priority,
            ) = rule

            # Check if rule applies to credit/debit
            if rule_is_credit != (1 if is_credit else 0):
                continue

            # Try to match pattern
            try:
                if re.search(pattern, description, re.IGNORECASE):
                    return TransactionMatch(
                        document_type=doc_type,
                        debit_account=debit_acc,
                        credit_account=credit_acc,
                        counterparty_code=counterparty,
                        department_code=dept,
                        cost_code=cost,
                        description_template=desc_template,
                        priority=priority,
                    )
            except re.error as e:
                logger.warning(f"Invalid regex pattern '{pattern}': {e}")
                continue

        return None

    def extract_pos_code(self, description: str) -> Optional[str]:
        """Extract POS machine code from description"""
        # Look for POS pattern: "POS XXXXXXXX" or "POS XXXXXXX"
        pos_match = re.search(r"POS\s*(\d{7,8})", description, re.IGNORECASE)
        if pos_match:
            return pos_match.group(1)
        return None

    def enrich_transaction_data(
        self, transaction: RawTransaction, match: TransactionMatch
    ) -> Dict:
        """Enrich transaction with additional data from database"""
        enriched = {
            "counterparty_name": "Khách Lẻ",
            "address": "",
            "department_name": "",
            "description": match.description_template
            or transaction.description,
        }

        # Extract POS code if present
        pos_code = self.extract_pos_code(transaction.description)

        # Extract suffix from description (e.g., "5777_1" from the transaction)
        suffix_match = re.search(r"(\d{4}_\d+)", transaction.description)
        suffix = suffix_match.group(1) if suffix_match else ""

        if pos_code and pos_code in self.pos_machines:
            pos_info = self.pos_machines[pos_code]

            # Get department info
            dept_code = pos_info["department_code"]
            if dept_code and dept_code in self.departments:
                enriched["department_name"] = self.departments[dept_code]

            # Get POS address
            enriched["address"] = pos_info.get("address", "")

            # Update description with POS info and suffix
            if match.description_template:
                desc = match.description_template
                desc = desc.replace("{pos_code}", pos_code)
                desc = desc.replace("{suffix}", suffix)
                enriched["description"] = desc

        # Get counterparty info
        if (
            match.counterparty_code
            and match.counterparty_code in self.counterparties
        ):
            cp_info = self.counterparties[match.counterparty_code]
            enriched["counterparty_name"] = cp_info["name"]
            # Only override address if not already set from POS
            if not enriched["address"]:
                enriched["address"] = cp_info["address"]

        # Get department name if not already set
        if not enriched["department_name"] and match.department_code:
            if match.department_code in self.departments:
                enriched["department_name"] = self.departments[
                    match.department_code
                ]

        return enriched

    def generate_document_number(self, doc_type: str, date: datetime) -> str:
        """Generate sequential document number"""
        self.doc_counters[doc_type] += 1
        # Format: BC03/001 (TypeMonth/Counter)
        month_str = f"{date.month:02d}"
        counter_str = f"{self.doc_counters[doc_type]:03d}"
        return f"{doc_type}{month_str}/{counter_str}"

    def process_transaction(
        self, transaction: RawTransaction
    ) -> Optional[SaokeEntry]:
        """Process a single transaction into saoke format"""
        try:
            # Determine if this is a credit or debit
            is_credit = transaction.credit_amount > 0
            amount = (
                transaction.credit_amount
                if is_credit
                else transaction.debit_amount
            )

            # Match against transaction rules
            match = self.match_transaction_rule(
                transaction.description, is_credit
            )
            if not match:
                logger.warning(
                    f"No rule matched for transaction: {transaction.description}"
                )
                return None

            # Enrich with additional data
            enriched = self.enrich_transaction_data(transaction, match)

            # Generate document number
            doc_number = self.generate_document_number(
                match.document_type, transaction.datetime
            )

            # Create saoke entry
            entry = SaokeEntry(
                document_type=match.document_type,
                date=transaction.datetime.strftime("%d/%m/%Y"),
                document_number=doc_number,
                currency="VND",
                sequence=1,
                counterparty_code=match.counterparty_code or "KL",
                counterparty_name=enriched["counterparty_name"],
                address=enriched["address"],
                description=enriched["description"],
                original_description=transaction.description,
                amount1=amount,
                amount2=amount,
                debit_account=match.debit_account,
                credit_account=match.credit_account,
                date2=transaction.datetime.strftime("%m/%d/%Y"),
                department=enriched["department_name"],
            )

            return entry

        except Exception as e:
            logger.error(f"Error processing transaction: {e}")
            return None

    def process_to_saoke(
        self,
        input_file: Union[str, io.BytesIO],
        output_file: Optional[str] = None,
    ) -> pd.DataFrame:
        """Process bank statement to saoke format"""
        if not self.connect():
            raise ConnectionError("Could not connect to database")

        try:
            # Read bank statement
            transactions = self.read_bank_statement(input_file)

            # Process each transaction
            saoke_entries = []
            unmatched_count = 0

            for transaction in transactions:
                entry = self.process_transaction(transaction)
                if entry:
                    saoke_entries.append(entry)
                else:
                    unmatched_count += 1

            logger.info(
                f"Processed {len(saoke_entries)} transactions, "
                f"{unmatched_count} unmatched"
            )

            # Convert to DataFrame
            if saoke_entries:
                df = pd.DataFrame([entry.__dict__ for entry in saoke_entries])

                # Save to file if specified
                if output_file:
                    df.to_excel(output_file, index=False)
                    logger.info(f"Saoke file saved to: {output_file}")

                return df
            else:
                logger.warning("No transactions were successfully processed")
                return pd.DataFrame()

        finally:
            self.close()

    def process_to_buffer(
        self, input_file: Union[str, io.BytesIO]
    ) -> io.BytesIO:
        """Process bank statement and return as BytesIO buffer"""
        df = self.process_to_saoke(input_file)

        buffer = io.BytesIO()
        with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
            df.to_excel(writer, index=False)
        buffer.seek(0)

        return buffer


# Example usage
if __name__ == "__main__":
    processor = BankStatementProcessor()

    # Process file
    result_df = processor.process_to_saoke(
        input_file="BIDV 3840.ods", output_file="saoke_output.ods"
    )

    print(f"Processed {len(result_df)} entries")
