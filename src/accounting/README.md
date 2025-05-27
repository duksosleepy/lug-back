# Bank Statement to Saoke Converter

This module converts BIDV bank statements to the standardized Saoke accounting format.

## Overview

The system processes bank transactions by:
1. **Extracting information from descriptions** - POS codes, departments, counterparties
2. **Mapping to database entries** - Using reference data to enrich transactions
3. **Generating proper accounting entries** - In the required Saoke format

## Key Features

- **Dynamic account assignment** - Accounts are determined based on transaction patterns
- **POS machine mapping** - Each POS machine maps to specific departments and counterparties
- **Flexible description parsing** - Extracts suffixes and formats descriptions correctly
- **Database-driven enrichment** - All reference data stored in SQLite database

## Setup Instructions

### 1. Initialize the Database

```bash
cd /home/khoi/code/lug-back/src/accounting
python initialize_enhanced_db.py
```

This creates and populates:
- Account codes (including specific bank accounts like 1121114)
- Departments (GO BRVT, GO HÀ NỘI, etc.)
- Counterparties (KL-GOBARIA1, etc.)
- POS machine mappings
- Account mapping rules

### 2. Test the Setup

```bash
python run_setup.py
```

This will:
- Initialize the database
- Test with a sample transaction
- Process your BIDV file if present

### 3. Process Bank Statements

```bash
python bank_statement_processor.py
```

## How It Works

### Transaction Processing Flow

1. **Read Bank Statement** → Detects BIDV format automatically
2. **Extract Information** → POS code, amounts, dates from description
3. **Match Rules** → Find appropriate processing rule
4. **Enrich Data** → Look up department, counterparty, accounts
5. **Generate Entry** → Create Saoke-formatted record

### Example Transformation

**Input (BIDV Statement):**
```
Reference: 0001NFS4-7YJO3BV08
Date: 01/03/2025 11:17:53
Debit: 0
Credit: 888,000
Balance: 112,028,712
Description: TT POS 14100333 500379 5777 064217PDO
```

**Output (Saoke Format):**
```
Document Type: BC
Date: 01/3/2025
Document Number: BC03/001
Currency: VND
Sequence: 1
Counterparty Code: KL-GOBARIA1
Counterparty Name: Khách Lẻ Không Lấy Hóa Đơn
Address: QH 1S9+10 TTTM Go BR-VT, Số 2A, Nguyễn Đình Chiểu, KP 1, P.Phước Hiệp, TP.Bà Rịa, T.Bà Rịa-Vũng Tàu
Description: Thu tiền bán hàng khách lẻ (POS 14100333 - GO BRVT)_5777_1
Original Description: TT POS 14100333 500379 5777 064217PDO
Amount1: 888,000
Amount2: 888,000
Debit Account: 1121114
Credit Account: 1311
Date2: 3/1/2025
Department: GO BRVT
```

## Key Mappings

### POS Machines → Counterparties
- POS 14100333 → KL-GOBARIA1 (GO Bà Rịa Vũng Tàu)
- POS 14100334 → KL-GOHANOI1 (GO Hà Nội)
- POS 14100335 → KL-GOHCM1 (GO Hồ Chí Minh)

### Transaction Types → Accounts
- Credit (BC): 1121114 (Bank) → 1311 (Receivable)
- Debit (BN): Various expense accounts → 1121114 (Bank)

### Description Patterns
- Extracts POS code: "POS 14100333" → 14100333
- Extracts suffix: "5777 064217PDO" → "_5777_1"
- Formats: "Thu tiền bán hàng khách lẻ (POS {code} - {dept}){suffix}"

## Adding New Mappings

To add new POS machines or counterparties:

1. Edit `initialize_enhanced_db.py`
2. Add entries to the appropriate sections
3. Re-run the initialization script

## Troubleshooting

- **No matches found**: Check transaction_rules table
- **Wrong accounts**: Check account_mappings table
- **Missing counterparty**: Add to counterparties table
- **Wrong department**: Check pos_machines mapping

## File Structure

```
src/accounting/
├── bank_statement_processor.py   # Main processor
├── bank_statement_reader.py      # BIDV format reader
├── initialize_enhanced_db.py     # Database setup
├── run_setup.py                  # Complete setup script
├── schema.sql                    # Database schema
└── banking_enterprise.db         # SQLite database
```
