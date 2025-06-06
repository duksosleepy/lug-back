import logging
import os

import limbo  # Limbo's Python binding for a Rust-written SQLite engine
import pandas as pd

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("db_import.log"), logging.StreamHandler()],
)
logger = logging.getLogger("BankingDBImport")


class ExcelDataImporter:
    """
    Import Excel data into an existing Banking Transaction Database using Limbo.
    """

    def __init__(self, db_path="banking_enterprise.db"):
        """Initialize the importer with path to existing database"""
        self.db_path = db_path
        self.conn = None
        self.cursor = None

        # Track statistics
        self.stats = {
            "accounts": 0,
            "departments": 0,
            "counterparties": 0,
            "cost_categories": 0,
            "pos_machines": 0,
            "rules": 0,
            "errors": 0,
        }

    def connect(self):
        """Connect to the existing database via Limbo"""
        if not os.path.exists(self.db_path):
            logger.error(
                f"Database file {self.db_path} does not exist! Run the SQL schema file first."
            )
            return False

        try:
            # Connect using Limbo
            self.conn = limbo.connect(self.db_path)
            self.cursor = self.conn.cursor()
            logger.info(f"Connected to Limbo database: {self.db_path}")
            return True
        except Exception as e:
            logger.error(f"Error connecting to Limbo database: {e}")
            return False

    def close(self):
        """Close the database connection"""
        if self.conn:
            self.conn.close()
            self.conn = None
            self.cursor = None

    def begin_transaction(self):
        """Start a transaction"""
        if not self.conn and not self.connect():
            return False
        self.cursor.execute("BEGIN;")
        return True

    def commit_transaction(self):
        """Commit the current transaction"""
        if self.conn:
            self.conn.commit()

    def rollback_transaction(self):
        """Roll back the current transaction"""
        if self.conn:
            self.conn.rollback()

    def clear_existing_data(self, table_name):
        """Clear existing data from a table"""
        try:
            self.cursor.execute(
                "SELECT name FROM sqlite_schema WHERE type='table' AND name=?",
                (table_name,),
            )
            if not self.cursor.fetchone():
                logger.warning(f"Table {table_name} does not exist in database")
                return False

            self.cursor.execute(f"DELETE FROM {table_name}")
            logger.info(f"Cleared all data from {table_name}")
            return True
        except Exception as e:
            logger.error(f"Error clearing data from {table_name}: {e}")
            self.stats["errors"] += 1
            return False

    def get_sheet_info(self, file_path):
        """Helper method to get sheet information from Excel file"""
        try:
            xl_file = pd.ExcelFile(file_path)
            sheet_names = xl_file.sheet_names
            logger.info(f"Available sheets in {file_path}: {sheet_names}")
            return sheet_names
        except Exception as e:
            logger.error(f"Error reading sheet names from {file_path}: {e}")
            return []

    def import_accounts(
        self,
        file_path="danhmuc_taikhoan_copy.xlsx",
        sheet_name=None,
        clear_existing=True,
    ):
        """Import chart of accounts data"""
        logger.info(f"Importing accounts from: {file_path}")

        try:
            # Check available sheets if sheet_name not specified
            if sheet_name is None:
                sheet_names = self.get_sheet_info(file_path)
                if len(sheet_names) > 1:
                    logger.warning(
                        f"Multiple sheets found: {sheet_names}. Using first sheet."
                    )
                sheet_name = 0  # Use first sheet by default

            logger.info(f"Reading from sheet: {sheet_name}")

            if not self.begin_transaction():
                return False

            if clear_existing:
                self.clear_existing_data("accounts")

            df = pd.read_excel(file_path, sheet_name=sheet_name)

            # Log DataFrame info for debugging
            logger.info(f"DataFrame shape: {df.shape}")
            logger.info(f"DataFrame columns: {list(df.columns)}")

            df.fillna(
                {
                    col: "" if df[col].dtype == "object" else 0
                    for col in df.columns
                },
                inplace=True,
            )

            accounts_data = []
            for _, row in df.iterrows():
                is_detail = (
                    1 if str(row.get("Tk_Cuoi", "")).lower() == "true" else 0
                )
                accounts_data.append(
                    (
                        row.get("Tk", ""),
                        row.get("Ten_Tk", ""),
                        row.get("Ten_TkE", ""),
                        row.get("Tk_Cha", "") or None,
                        is_detail,
                    )
                )

            for account in accounts_data:
                try:
                    self.cursor.execute(
                        """
                        INSERT INTO accounts
                            (code, name, name_english, parent_code, is_detail)
                        VALUES (?, ?, ?, ?, ?)
                        """,
                        account,
                    )
                    self.stats["accounts"] += 1
                except Exception as e:
                    logger.error(f"Error inserting account {account[0]}: {e}")
                    self.stats["errors"] += 1

            self.commit_transaction()
            logger.info(f"Imported {self.stats['accounts']} accounts.")
            return True

        except Exception as e:
            self.rollback_transaction()
            logger.error(f"Error importing accounts: {e}")
            self.stats["errors"] += 1
            return False

    def import_departments(
        self,
        file_path="danhmuc_bophan_copy.xlsx",
        sheet_name=None,
        clear_existing=True,
    ):
        """Import departments data"""
        logger.info(f"Importing departments from: {file_path}")

        try:
            # Check available sheets if sheet_name not specified
            if sheet_name is None:
                sheet_names = self.get_sheet_info(file_path)
                if len(sheet_names) > 1:
                    logger.warning(
                        f"Multiple sheets found: {sheet_names}. Using first sheet."
                    )
                sheet_name = 0  # Use first sheet by default

            logger.info(f"Reading from sheet: {sheet_name}")

            if not self.begin_transaction():
                return False

            if clear_existing:
                self.clear_existing_data("departments")

            df = pd.read_excel(file_path, sheet_name=sheet_name)

            # Log DataFrame info for debugging
            logger.info(f"DataFrame shape: {df.shape}")
            logger.info(f"DataFrame columns: {list(df.columns)}")

            df.fillna(
                {
                    col: "" if df[col].dtype == "object" else 0
                    for col in df.columns
                },
                inplace=True,
            )

            departments_data = []
            for _, row in df.iterrows():
                is_detail = (
                    1 if str(row.get("Nh_Cuoi", "")).lower() == "true" else 0
                )
                departments_data.append(
                    (
                        row.get("Ma_Bp", ""),
                        row.get("Ten_Bp", ""),
                        row.get("Ma_Bp_Cha", "") or None,
                        is_detail,
                        row.get("Ma_Data", ""),
                    )
                )

            for department in departments_data:
                try:
                    self.cursor.execute(
                        """
                        INSERT INTO departments
                            (code, name, parent_code, is_detail, data_source)
                        VALUES (?, ?, ?, ?, ?)
                        """,
                        department,
                    )
                    self.stats["departments"] += 1
                except Exception as e:
                    logger.error(
                        f"Error inserting department {department[0]}: {e}"
                    )
                    self.stats["errors"] += 1

            self.commit_transaction()
            logger.info(f"Imported {self.stats['departments']} departments.")
            return True

        except Exception as e:
            self.rollback_transaction()
            logger.error(f"Error importing departments: {e}")
            self.stats["errors"] += 1
            return False

    def import_counterparties(
        self,
        file_path="danhmuc_doituong_copy.xlsx",
        sheet_name=None,
        clear_existing=True,
    ):
        """Import counterparties data"""
        logger.info(f"Importing counterparties from: {file_path}")

        try:
            # Check available sheets if sheet_name not specified
            if sheet_name is None:
                sheet_names = self.get_sheet_info(file_path)
                if len(sheet_names) > 1:
                    logger.warning(
                        f"Multiple sheets found: {sheet_names}. Using first sheet."
                    )
                sheet_name = 0  # Use first sheet by default

            logger.info(f"Reading from sheet: {sheet_name}")

            if not self.begin_transaction():
                return False

            if clear_existing:
                self.clear_existing_data("counterparties")

            df = pd.read_excel(file_path, sheet_name=sheet_name)

            # Log DataFrame info for debugging
            logger.info(f"DataFrame shape: {df.shape}")
            logger.info(f"DataFrame columns: {list(df.columns)}")

            df.fillna(
                {
                    col: "" if df[col].dtype == "object" else 0
                    for col in df.columns
                },
                inplace=True,
            )

            counterparties_data = []
            for _, row in df.iterrows():
                try:
                    party_type = int(row.get("Loai_Dt", 0))
                except (ValueError, TypeError):
                    party_type = 0

                counterparties_data.append(
                    (
                        row.get("Ma_Dt", ""),
                        row.get("Ten_Dt", ""),
                        row.get("Ong_Ba", ""),
                        row.get("Chuc_Vu", ""),
                        row.get("Ma_Nh_Dt", ""),
                        party_type,
                        row.get("Ma_Kv", ""),
                        row.get("Dia_Chi", ""),
                        row.get("So_Phone", ""),
                        row.get("So_Fax", ""),
                        row.get("Ma_So_Thue", ""),
                        row.get("So_Tk_NH", ""),
                        row.get("Ten_NH", ""),
                        row.get("Ten_Tp", ""),
                    )
                )

            for party in counterparties_data:
                try:
                    self.cursor.execute(
                        """
                        INSERT INTO counterparties (
                            code, name, contact_person, position, group_code, type,
                            region_code, address, phone, fax, tax_id,
                            bank_account, bank_name, city
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        party,
                    )
                    self.stats["counterparties"] += 1
                except Exception as e:
                    logger.error(
                        f"Error inserting counterparty {party[0]}: {e}"
                    )
                    self.stats["errors"] += 1

            self.commit_transaction()
            logger.info(
                f"Imported {self.stats['counterparties']} counterparties."
            )
            return True

        except Exception as e:
            self.rollback_transaction()
            logger.error(f"Error importing counterparties: {e}")
            self.stats["errors"] += 1
            return False

    def import_cost_categories(
        self,
        file_path="danhmuc_khoanmuc_copy.xlsx",
        sheet_name=None,
        clear_existing=True,
    ):
        """Import cost categories data"""
        logger.info(f"Importing cost categories from: {file_path}")

        try:
            # Check available sheets if sheet_name not specified
            if sheet_name is None:
                sheet_names = self.get_sheet_info(file_path)
                if len(sheet_names) > 1:
                    logger.warning(
                        f"Multiple sheets found: {sheet_names}. Using first sheet."
                    )
                sheet_name = 0  # Use first sheet by default

            logger.info(f"Reading from sheet: {sheet_name}")

            if not self.begin_transaction():
                return False

            if clear_existing:
                self.clear_existing_data("cost_categories")

            df = pd.read_excel(file_path, sheet_name=sheet_name)

            # Log DataFrame info for debugging
            logger.info(f"DataFrame shape: {df.shape}")
            logger.info(f"DataFrame columns: {list(df.columns)}")

            df.fillna(
                {
                    col: "" if df[col].dtype == "object" else 0
                    for col in df.columns
                },
                inplace=True,
            )

            categories_data = [
                (
                    row.get("Ma_Km", ""),
                    row.get("Ten_Km", ""),
                    row.get("Ma_Data", ""),
                )
                for _, row in df.iterrows()
            ]

            for category in categories_data:
                try:
                    self.cursor.execute(
                        """
                        INSERT INTO cost_categories
                            (code, name, data_source)
                        VALUES (?, ?, ?)
                        """,
                        category,
                    )
                    self.stats["cost_categories"] += 1
                except Exception as e:
                    logger.error(
                        f"Error inserting cost category {category[0]}: {e}"
                    )
                    self.stats["errors"] += 1

            self.commit_transaction()
            logger.info(
                f"Imported {self.stats['cost_categories']} cost categories."
            )
            return True

        except Exception as e:
            self.rollback_transaction()
            logger.error(f"Error importing cost categories: {e}")
            self.stats["errors"] += 1
            return False

    def import_pos_machines(
        self,
        file_path="danhmuc_maypos_copy.xlsx",
        sheet_name=None,
        clear_existing=True,
    ):
        """Import POS machines data"""
        logger.info(f"Importing POS machines from: {file_path}")

        try:
            # Check available sheets if sheet_name not specified
            if sheet_name is None:
                sheet_names = self.get_sheet_info(file_path)
                if len(sheet_names) > 1:
                    logger.warning(
                        f"Multiple sheets found: {sheet_names}. Using first sheet."
                    )
                sheet_name = 0  # Use first sheet by default

            logger.info(f"Reading from sheet: {sheet_name}")

            if not self.begin_transaction():
                return False

            if clear_existing:
                self.clear_existing_data("pos_machines")

            df = pd.read_excel(file_path, sheet_name=sheet_name)

            # Log DataFrame info for debugging
            logger.info(f"DataFrame shape: {df.shape}")
            logger.info(f"DataFrame columns: {list(df.columns)}")
            df.fillna(
                {
                    col: "" if df[col].dtype == "object" else 0
                    for col in df.columns
                },
                inplace=True,
            )

            pos_data = []
            skipped_empty = 0
            processed = 0

            for _, row in df.iterrows():
                # Try multiple possible column name variations
                code_col = next(
                    (
                        col
                        for col in [
                            "Mã TIP",
                            "Ma_TIP",
                            "MÃ TIP",
                            "MA_TIP",
                            "Mã POS",
                            "Ma_POS",
                        ]
                        if col in row.index
                    ),
                    None,
                )
                dept_col = next(
                    (
                        col
                        for col in [
                            "Mã đối tượng",
                            "Ma_Dt",
                            "MÃ ĐỐI TƯỢNG",
                            "MA_DT",
                            "Mã BP",
                            "Ma_BP",
                        ]
                        if col in row.index
                    ),
                    None,
                )
                name_col = next(
                    (
                        col
                        for col in [
                            "Tên",
                            "Ten",
                            "TÊN",
                            "TEN",
                            "Tên POS",
                            "Ten_POS",
                        ]
                        if col in row.index
                    ),
                    None,
                )
                addr_col = next(
                    (
                        col
                        for col in ["Địa chỉ", "Dia_Chi", "ĐỊA CHỈ", "DIA_CHI"]
                        if col in row.index
                    ),
                    None,
                )
                holder_col = next(
                    (
                        col
                        for col in [
                            "CHỦ TÀI KHOẢN",
                            "Chu_TK",
                            "CHU TAI KHOAN",
                            "CHU_TK",
                            "Chủ TK",
                        ]
                        if col in row.index
                    ),
                    None,
                )
                account_col = next(
                    (
                        col
                        for col in [
                            "TK THỤ HƯỞNG",
                            "TK_TH",
                            "TK THU HUONG",
                            "Số TK",
                            "So_TK",
                        ]
                        if col in row.index
                    ),
                    None,
                )
                bank_col = next(
                    (
                        col
                        for col in [
                            "NGÂN HÀNG",
                            "Ngan_Hang",
                            "NGAN HANG",
                            "NH",
                            "Ngân hàng",
                        ]
                        if col in row.index
                    ),
                    None,
                )

                if code_col is None:
                    logger.warning("Could not find POS code column in row")
                    continue

                pos_code = str(row.get(code_col, "")).strip()

                if not pos_code:
                    skipped_empty += 1
                    continue

                account_number = (
                    str(row.get(account_col, "")) if account_col else ""
                )

                pos_data.append(
                    (
                        pos_code,
                        row.get(dept_col, "") if dept_col else "",
                        row.get(name_col, "") if name_col else "",
                        row.get(addr_col, "") if addr_col else "",
                        row.get(holder_col, "") if holder_col else "",
                        account_number,
                        row.get(bank_col, "") if bank_col else "",
                    )
                )
                processed += 1

            logger.info(
                f"Processed {processed} rows, skipped {skipped_empty} empty rows"
            )
            logger.info(f"Total POS data entries to insert: {len(pos_data)}")

            for pos in pos_data:
                try:
                    if pos[1]:  # If department code exists
                        self.cursor.execute(
                            "SELECT code FROM departments WHERE code = ?",
                            (pos[1],),
                        )
                        if not self.cursor.fetchone():
                            logger.warning(
                                f"Department {pos[1]} not found for POS {pos[0]}, keeping the code anyway"
                            )
                    self.cursor.execute(
                        """
                        INSERT INTO pos_machines (
                            code, department_code, name, address,
                            account_holder, account_number, bank_name
                        ) VALUES (?, ?, ?, ?, ?, ?, ?)
                        """,
                        pos,
                    )
                    self.stats["pos_machines"] += 1
                except Exception as e:
                    logger.error(f"Error inserting POS machine {pos[0]}: {e}")
                    logger.error(f"Full POS data: {pos}")
                    self.stats["errors"] += 1

            self.commit_transaction()
            logger.info(f"Imported {self.stats['pos_machines']} POS machines.")
            return True

        except Exception as e:
            self.rollback_transaction()
            logger.error(f"Error importing POS machines: {e}")
            self.stats["errors"] += 1
            return False

    def setup_default_transaction_rules(self, clear_existing=True):
        """Set up default transaction rules for categorization"""
        logger.info("Setting up default transaction rules")

        try:
            if not self.begin_transaction():
                return False

            if clear_existing:
                self.clear_existing_data("transaction_rules")

            self.cursor.execute(
                "SELECT code, department_code FROM pos_machines"
            )
            pos_machines = self.cursor.fetchall()

            rules_data = []
            for pos_code, dept_code in pos_machines:
                rules_data.append(
                    (
                        f"pos.*{pos_code}",
                        "BC",
                        "1121",
                        "1311",
                        None,
                        dept_code,
                        None,
                        f"Thu tiền bán hàng khách lẻ (POS {pos_code})",
                        1,
                        10,
                    )
                )

            general_rules = [
                # Credit rules
                (
                    r"l[aã]i|thanh\s*to[aá]n\s*l[aã]i|interest",
                    "BC",
                    "1121",
                    "5154",
                    "50619",
                    None,
                    None,
                    "Thu lãi tiền gửi",
                    1,
                    10,
                ),
                (
                    r"h[aà]\s*n[oộ]i|hanoi",
                    "BC",
                    "1121",
                    "13682",
                    "31754-007M",
                    None,
                    None,
                    "CN Hà Nội thanh toán tiền hàng",
                    1,
                    10,
                ),
                # Debit rules
                (
                    r"ph[ií].*pos",
                    "BN",
                    "3311",
                    "1121",
                    "50619",
                    None,
                    "PHINH_CATHE",
                    "Phí giao dịch POS",
                    0,
                    10,
                ),
                (
                    r"chuy[eể]n\s*ti[eề]n.*3840.*7655",
                    "BN",
                    "1121108",
                    "1121",
                    "31754",
                    None,
                    None,
                    "Chuyển tiền giữa tài khoản",
                    0,
                    10,
                ),
                (
                    r"ho[aà]n\s*ti[eề]n",
                    "BN",
                    "1311",
                    "1121",
                    None,
                    None,
                    None,
                    "Hoàn tiền khách hàng",
                    0,
                    10,
                ),
                (
                    r"c[oọ]c.*vincom|vincom.*c[oọ]c",
                    "BN",
                    "244",
                    "1121",
                    "50673-VC",
                    None,
                    None,
                    "Đặt cọc thuê mặt bằng",
                    0,
                    10,
                ),
                (
                    r"[dđ]i[eệ]n",
                    "BN",
                    "6278",
                    "1121",
                    "CTY CNDV",
                    None,
                    "DIEN",
                    "Thanh toán tiền điện",
                    0,
                    10,
                ),
                # Default
                (r".*", "BC", "1121", "1311", None, None, None, None, 1, 1),
                (r".*", "BN", "6278", "1121", None, None, None, None, 0, 1),
            ]
            rules_data.extend(general_rules)

            for rule in rules_data:
                try:
                    self.cursor.execute(
                        """
                        INSERT INTO transaction_rules (
                            pattern, document_type, debit_account, credit_account,
                            counterparty_code, department_code, cost_code,
                            description_template, is_credit, priority
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        rule,
                    )
                    self.stats["rules"] += 1
                except Exception as e:
                    logger.error(f"Error inserting rule '{rule[0]}': {e}")
                    self.stats["errors"] += 1

            self.commit_transaction()
            logger.info(f"Set up {self.stats['rules']} transaction rules")
            return True

        except Exception as e:
            self.rollback_transaction()
            logger.error(f"Error setting up transaction rules: {e}")
            self.stats["errors"] += 1
            return False

    def import_all_data(
        self,
        accounts_file="danhmuctaikhoan.xls",
        accounts_sheet="Sheet1",
        departments_file="danhmucbophan.xls",
        departments_sheet="Sheet1",
        counterparties_file="danhmucdoituong.xls",
        counterparties_sheet="Sheet1",
        cost_categories_file="danhmuckhoanmuc.xls",
        cost_categories_sheet="Sheet1",
        pos_machines_file="danhmucmaypos.xlsx",
        pos_machines_sheet="POS_BIDV",
        clear_existing=True,
    ):
        """Import all data from Excel files into the existing Limbo database"""
        logger.info("Starting data import into Limbo database")

        if not self.connect():
            return False

        success = True
        success &= self.import_accounts(
            accounts_file, accounts_sheet, clear_existing
        )
        success &= self.import_departments(
            departments_file, departments_sheet, clear_existing
        )
        success &= self.import_counterparties(
            counterparties_file, counterparties_sheet, clear_existing
        )
        success &= self.import_cost_categories(
            cost_categories_file, cost_categories_sheet, clear_existing
        )
        success &= self.import_pos_machines(
            pos_machines_file, pos_machines_sheet, clear_existing
        )
        # success &= self.setup_default_transaction_rules(clear_existing)

        self.close()
        logger.info("=== Import Summary ===")
        logger.info(f"Accounts: {self.stats['accounts']}")
        logger.info(f"Departments: {self.stats['departments']}")
        logger.info(f"Counterparties: {self.stats['counterparties']}")
        logger.info(f"Cost Categories: {self.stats['cost_categories']}")
        logger.info(f"POS Machines: {self.stats['pos_machines']}")
        logger.info(f"Transaction Rules: {self.stats['rules']}")
        logger.info(f"Errors: {self.stats['errors']}")

        if success:
            logger.info("Data import completed successfully!")
        else:
            logger.warning("Data import completed with errors!")

        return success


if __name__ == "__main__":
    importer = ExcelDataImporter(db_path="test.db")

    # Option 1: Import with automatic sheet detection (will use first sheet if multiple)
    importer.import_all_data(clear_existing=True)

    # Option 2: Import with specific sheet names
    # importer.import_all_data(
    #     accounts_sheet="Data",  # or sheet index like 1
    #     departments_sheet="Sheet1",
    #     counterparties_sheet="Data",
    #     cost_categories_sheet="Sheet1",
    #     pos_machines_sheet="MAYPOS",  # Specify the correct sheet name here
    #     clear_existing=True
    # )

    # Option 3: Import individual files with specific sheets
    # importer.import_pos_machines(
    #     file_path="danhmucmaypos.xlsx",
    #     sheet_name="MAYPOS",  # or use index like sheet_name=1 for second sheet
    #     clear_existing=True
    # )
