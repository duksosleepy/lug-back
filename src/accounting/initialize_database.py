#!/usr/bin/env python3
"""
Script to initialize the banking database with required reference data
"""

import logging

import limbo

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def initialize_database(db_path="banking_enterprise.db"):
    """Initialize database with reference data for bank statement processing"""

    conn = limbo.connect(db_path)
    cursor = conn.cursor()

    try:
        # First, create all tables from schema
        logger.info("Creating database schema...")
        with open("src/accounting/schema.sql", "r") as f:
            schema_sql = f.read()
            cursor.executescript(schema_sql)

        # Insert sample reference data
        logger.info("Inserting reference data...")

        # Accounts - Add more specific account codes
        cursor.executemany(
            """
            INSERT OR IGNORE INTO accounts (code, name, name_english, is_detail)
            VALUES (?, ?, ?, ?)
        """,
            [
                ("1121", "Tiền gửi ngân hàng", "Bank deposits", 0),
                ("1121114", "TK 114 - BIDV 3840", "BIDV Account 3840", 1),
                ("1311", "Phải thu khách hàng", "Accounts receivable", 0),
                ("3311", "Phải trả người bán", "Accounts payable", 0),
                ("5154", "Thu nhập lãi", "Interest income", 0),
                ("6278", "Chi phí khác", "Other expenses", 0),
            ],
        )

        # Departments
        cursor.executemany(
            """
            INSERT OR IGNORE INTO departments (code, name, is_detail)
            VALUES (?, ?, ?)
        """,
            [
                ("GOBRVT", "GO Bà Rịa Vũng Tàu", 1),
                ("GOHANOI", "GO Hà Nội", 1),
                ("GOHCM", "GO Hồ Chí Minh", 1),
            ],
        )

        # Counterparties
        cursor.executemany(
            """
            INSERT OR IGNORE INTO counterparties (code, name, address, type)
            VALUES (?, ?, ?, ?)
        """,
            [
                ("KL", "Khách Lẻ", "Không Lấy Hóa Đơn", 1),
                (
                    "KL-GOBARIA1",
                    "Khách Lẻ Không Lấy Hóa Đơn",
                    "QH 1S9+10 TTTM Go BR-VT, Số 2A, Nguyễn Đình Chiểu, KP 1, P.Phước Hiệp, TP.Bà Rịa, T.Bà Rịa-Vũng Tàu",
                    1,
                ),
                (
                    "50619",
                    "BIDV",
                    "Ngân hàng TMCP Đầu tư và Phát triển Việt Nam",
                    2,
                ),
                ("31754", "Chi nhánh Hà Nội", "Hà Nội", 1),
                ("31754-007M", "CN Hà Nội - Mã 007M", "Hà Nội", 1),
            ],
        )

        # POS Machines with full details
        cursor.executemany(
            """
            INSERT OR IGNORE INTO pos_machines (code, department_code, name, address)
            VALUES (?, ?, ?, ?)
        """,
            [
                (
                    "14100333",
                    "GOBRVT",
                    "POS GO BRVT",
                    "TTTM Go BR-VT, Số 2A, Nguyễn Đình Chiểu, KP 1, P.Phước Hiệp, TP.Bà Rịa, T.Bà Rịa-Vũng Tàu",
                ),
                ("14100334", "GOHANOI", "POS GO HÀ NỘI", "Hà Nội"),
                ("14100335", "GOHCM", "POS GO HCM", "Hồ Chí Minh"),
            ],
        )

        # Enhanced transaction rules
        cursor.execute("DELETE FROM transaction_rules")  # Clear existing rules

        # POS transaction rules - specific for each POS machine
        cursor.executemany(
            """
            INSERT INTO transaction_rules (
                pattern, document_type, debit_account, credit_account,
                counterparty_code, department_code, cost_code,
                description_template, is_credit, priority
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
            [
                # Specific POS rules with counterparty codes
                (
                    r"POS\s*14100333",
                    "BC",
                    "1121114",
                    "1311",
                    "KL-GOBARIA1",
                    "GOBRVT",
                    None,
                    "Thu tiền bán hàng khách lẻ (POS 14100333 - GO BRVT)_{suffix}",
                    1,
                    100,
                ),
                (
                    r"POS\s*14100334",
                    "BC",
                    "1121114",
                    "1311",
                    "KL",
                    "GOHANOI",
                    None,
                    "Thu tiền bán hàng khách lẻ (POS 14100334 - GO HÀ NỘI)_{suffix}",
                    1,
                    100,
                ),
                (
                    r"POS\s*14100335",
                    "BC",
                    "1121114",
                    "1311",
                    "KL",
                    "GOHCM",
                    None,
                    "Thu tiền bán hàng khách lẻ (POS 14100335 - GO HCM)_{suffix}",
                    1,
                    100,
                ),
                # General rules
                (
                    r"l[aã]i|interest",
                    "BC",
                    "1121114",
                    "5154",
                    "50619",
                    None,
                    None,
                    "Thu lãi tiền gửi",
                    1,
                    80,
                ),
                (
                    r"ph[ií].*pos",
                    "BN",
                    "3311",
                    "1121114",
                    "50619",
                    None,
                    "PHINH_CATHE",
                    "Phí giao dịch POS",
                    0,
                    70,
                ),
                (
                    r"h[aà]\s*n[oộ]i",
                    "BC",
                    "1121114",
                    "13682",
                    "31754-007M",
                    None,
                    None,
                    "CN Hà Nội thanh toán tiền hàng",
                    1,
                    60,
                ),
                # Default rules
                (
                    r".*",
                    "BC",
                    "1121114",
                    "1311",
                    "KL",
                    None,
                    None,
                    "Thu tiền khác",
                    1,
                    1,
                ),
                (
                    r".*",
                    "BN",
                    "6278",
                    "1121114",
                    None,
                    None,
                    "KHAC",
                    "Chi tiền khác",
                    0,
                    1,
                ),
            ],
        )

        conn.commit()
        logger.info("Database initialized successfully!")

        # Display summary
        cursor.execute("SELECT COUNT(*) FROM accounts")
        logger.info(f"Accounts: {cursor.fetchone()[0]}")

        cursor.execute("SELECT COUNT(*) FROM departments")
        logger.info(f"Departments: {cursor.fetchone()[0]}")

        cursor.execute("SELECT COUNT(*) FROM counterparties")
        logger.info(f"Counterparties: {cursor.fetchone()[0]}")

        cursor.execute("SELECT COUNT(*) FROM pos_machines")
        logger.info(f"POS Machines: {cursor.fetchone()[0]}")

        cursor.execute("SELECT COUNT(*) FROM transaction_rules")
        logger.info(f"Transaction Rules: {cursor.fetchone()[0]}")

    except Exception as e:
        logger.error(f"Error initializing database: {e}")
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    initialize_database()
