-- Updated Schema.sql with Enhanced Account Structure for Dynamic Account Determination
-- This schema includes sample BIDV accounts for testing account code extraction functionality
-- Key enhancement: accounts table populated with bank account examples for code matching
-- Removed bank_accounts table as it's no longer needed

-- Keep all existing tables from original schema
CREATE TABLE IF NOT EXISTS accounts (
    code TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    name_english TEXT,
    parent_code TEXT REFERENCES accounts(code) ON UPDATE CASCADE,
    is_detail INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP
);

-- Departments (danhmuc_bophan_copy.xlsx)
CREATE TABLE IF NOT EXISTS departments (
    code TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    parent_code TEXT REFERENCES departments(code) ON UPDATE CASCADE,
    is_detail INTEGER NOT NULL DEFAULT 0,
    data_source TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP
);

-- Counterparties (danhmuc_doituong_copy.xlsx)
CREATE TABLE IF NOT EXISTS counterparties (
    code TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    contact_person TEXT,
    position TEXT,
    group_code TEXT,
    type INTEGER,
    region_code TEXT,
    address TEXT,
    phone TEXT,
    fax TEXT,
    tax_id TEXT,
    bank_account TEXT,
    bank_name TEXT,
    city TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP
);

-- Cost Categories (danhmuc_khoanmuc_copy.xlsx)
CREATE TABLE IF NOT EXISTS cost_categories (
    code TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    data_source TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP
);

-- POS Machines (danhmuc_maypos_copy.xlsx)
CREATE TABLE IF NOT EXISTS pos_machines (
    code TEXT PRIMARY KEY,
    department_code TEXT REFERENCES departments(code) ON UPDATE CASCADE,
    name TEXT NOT NULL,
    address TEXT,
    account_holder TEXT,
    account_number TEXT,
    bank_name TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP
);

-- Transaction processing tables (keep existing structure)
CREATE TABLE IF NOT EXISTS banks (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    code TEXT NOT NULL UNIQUE,
    name TEXT NOT NULL,
    description TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS bank_statements (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    account_number TEXT NOT NULL,
    start_date DATE NOT NULL,
    end_date DATE NOT NULL,
    opening_balance NUMERIC(20,2) NOT NULL,
    closing_balance NUMERIC(20,2) NOT NULL,
    total_debit NUMERIC(20,2) NOT NULL,
    total_credit NUMERIC(20,2) NOT NULL,
    import_date TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    imported_by TEXT,
    status TEXT DEFAULT 'active',
    UNIQUE(account_number, start_date, end_date)
);

CREATE TABLE IF NOT EXISTS transaction_status (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    code TEXT NOT NULL UNIQUE,
    name TEXT NOT NULL,
    description TEXT
);

CREATE TABLE IF NOT EXISTS transactions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    statement_id INTEGER NOT NULL REFERENCES bank_statements(id) ON DELETE CASCADE,
    reference_number TEXT,
    transaction_date DATETIME NOT NULL,
    description TEXT,
    debit_amount NUMERIC(20,2) DEFAULT 0,
    credit_amount NUMERIC(20,2) DEFAULT 0,
    balance NUMERIC(20,2),
    pos_code TEXT REFERENCES pos_machines(code) ON UPDATE CASCADE,
    status_code TEXT NOT NULL DEFAULT 'NEW' REFERENCES transaction_status(code) ON UPDATE CASCADE,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP,
    UNIQUE(statement_id, reference_number, transaction_date, debit_amount, credit_amount)
);

CREATE TABLE IF NOT EXISTS document_types (
    code TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    description TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS transaction_mappings (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    transaction_id INTEGER NOT NULL REFERENCES transactions(id) ON DELETE CASCADE,
    document_type TEXT NOT NULL REFERENCES document_types(code) ON UPDATE CASCADE,
    document_number TEXT NOT NULL,
    document_date DATE NOT NULL,
    counterparty_code TEXT REFERENCES counterparties(code) ON UPDATE CASCADE,
    debit_account TEXT NOT NULL REFERENCES accounts(code) ON UPDATE CASCADE,
    credit_account TEXT NOT NULL REFERENCES accounts(code) ON UPDATE CASCADE,
    amount NUMERIC(20,2) NOT NULL,
    department_code TEXT REFERENCES departments(code) ON UPDATE CASCADE,
    cost_code TEXT REFERENCES cost_categories(code) ON UPDATE CASCADE,
    description TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP,
    UNIQUE(document_type, document_number)
);

-- Tables removed as we now use fast_search.py instead of SQL queries
-- CREATE TABLE IF NOT EXISTS transaction_rules
-- CREATE TABLE IF NOT EXISTS account_mapping_rules

CREATE TABLE IF NOT EXISTS transaction_history (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    transaction_id INTEGER NOT NULL REFERENCES transactions(id) ON DELETE CASCADE,
    field_name TEXT NOT NULL,
    old_value TEXT,
    new_value TEXT,
    changed_by TEXT,
    changed_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- ============================================================================
-- INSERT ENHANCED SAMPLE DATA FOR ACCOUNT EXTRACTION TESTING
-- ============================================================================

-- Banks
INSERT OR REPLACE INTO banks (code, name, description) VALUES
('BIDV', 'Bank for Investment and Development of Vietnam', 'BIDV Bank');

-- Transaction status
INSERT OR REPLACE INTO transaction_status (code, name, description) VALUES
('NEW',       'New',       'Newly imported transaction'),
('MAPPED',    'Mapped',    'Transaction mapped to accounting entry'),
('PROCESSED', 'Processed', 'Transaction fully processed'),
('RECONCILED','Reconciled','Transaction reconciled with accounting');

-- Document types
INSERT OR REPLACE INTO document_types (code, name, description) VALUES
('BC', 'Cash Receipt', 'Receipt of cash or bank deposit'),
('BN', 'Cash Payment', 'Payment of cash or bank withdrawal');

-- ============================================================================
-- ENHANCED ACCOUNTS DATA FOR ACCOUNT CODE EXTRACTION
-- ============================================================================

-- Sample data for transaction_rules and account_mapping_rules has been removed
-- These rules are now hardcoded in integrated_processor.py


-- ============================================================================
-- INDEXES FOR PERFORMANCE (keep existing)
-- ============================================================================

DROP INDEX IF EXISTS idx_transactions_date;
DROP INDEX IF EXISTS idx_transactions_statement;
DROP INDEX IF EXISTS idx_transactions_status;
DROP INDEX IF EXISTS idx_mapping_transaction;
DROP INDEX IF EXISTS idx_mapping_accounts;
DROP INDEX IF EXISTS idx_mapping_document;
-- Indexes removed for tables that no longer exist
-- DROP INDEX IF EXISTS idx_rules_pattern;
-- DROP INDEX IF EXISTS idx_rules_priority;
-- DROP INDEX IF EXISTS idx_rules_credit;
DROP INDEX IF EXISTS idx_accounts_parent;
DROP INDEX IF EXISTS idx_departments_parent;
DROP INDEX IF EXISTS idx_pos_department;
DROP INDEX IF EXISTS idx_statements_dates;
-- Indexes removed for tables that no longer exist
-- DROP INDEX IF EXISTS idx_acc_mapping_lookup;
-- DROP INDEX IF EXISTS idx_acc_mapping_priority;
DROP INDEX IF EXISTS idx_accounts_name_search;

CREATE INDEX IF NOT EXISTS idx_transactions_date       ON transactions(transaction_date);
CREATE INDEX IF NOT EXISTS idx_transactions_statement  ON transactions(statement_id);
CREATE INDEX IF NOT EXISTS idx_transactions_status     ON transactions(status_code);
CREATE INDEX IF NOT EXISTS idx_mapping_transaction     ON transaction_mappings(transaction_id);
CREATE INDEX IF NOT EXISTS idx_mapping_accounts        ON transaction_mappings(debit_account, credit_account);
CREATE INDEX IF NOT EXISTS idx_mapping_document        ON transaction_mappings(document_type, document_number);
-- Indexes removed for tables that no longer exist
-- CREATE INDEX IF NOT EXISTS idx_rules_pattern           ON transaction_rules(pattern);
-- CREATE INDEX IF NOT EXISTS idx_rules_priority          ON transaction_rules(priority);
-- CREATE INDEX IF NOT EXISTS idx_rules_credit            ON transaction_rules(is_credit, is_active);
CREATE INDEX IF NOT EXISTS idx_accounts_parent         ON accounts(parent_code);
CREATE INDEX IF NOT EXISTS idx_departments_parent      ON departments(parent_code);
CREATE INDEX IF NOT EXISTS idx_pos_department          ON pos_machines(department_code);
CREATE INDEX IF NOT EXISTS idx_statements_dates        ON bank_statements(account_number, start_date, end_date);
-- Indexes removed for tables that no longer exist
-- CREATE INDEX IF NOT EXISTS idx_acc_mapping_lookup      ON account_mapping_rules(rule_type, entity_code, document_type);
-- CREATE INDEX IF NOT EXISTS idx_acc_mapping_priority    ON account_mapping_rules(priority);

-- CRITICAL: Index for account name searching (enables fast account code extraction)
CREATE INDEX IF NOT EXISTS idx_accounts_name_search    ON accounts(name);
