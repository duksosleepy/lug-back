# Bank Statement Converter Module

This module provides functionality to convert BIDV bank statements to the saoke format required for accounting systems.

## Features

- Processes BIDV bank statements (Excel/ODS files) and converts them to the standard saoke format
- Extracts transaction details including dates, amounts, descriptions, and counterparties
- Determines appropriate account codes (debit/credit) based on transaction patterns
- Formats output with all required fields according to accounting specifications
- Provides both API endpoints and command-line utilities for processing

## API Endpoints

- `/accounting/process/online` - Process bank statements with online mode
- `/accounting/process/offline` - Process bank statements with offline mode

Both endpoints accept an uploaded Excel/ODS file and return a JSON response with:
- `success`: Whether the processing was successful
- `message`: Status message
- `result_file`: Base64-encoded Excel file with the processed data
- `filename`: Suggested filename for the processed file

## Usage

### API Usage

Send a POST request with the bank statement file to either endpoint:

```
POST /accounting/process/online
Content-Type: multipart/form-data
file: [BIDV bank statement file]
```

### Command Line Usage

You can also use the module from the command line:

```
python bank_statement_converter.py input_file.xlsx [output_file.xlsx]
```

## Required Output Format

The output file follows a specific format with these fields:

- Ma_Ct: Transaction type (BN, BC, etc.)
- Ngay_Ct: Transaction date (equal to "ngày hiệu lực" in input file)
- So_Ct: Document number
- Ma_Tte: "VND"
- Ty_Gia: 1
- Ma_Dt: Counterparty code
- Ong_Ba: Counterparty name
- Dia_Chi: Counterparty address
- Dien_Giai: Transaction description
- Tien: Amount
- Tien_Nt: Amount (same as Tien)
- Tk_No: Debit account
- Tk_Co: Credit account

Plus additional required fields (may be empty or have default values):
Ma_Thue, Thue_GtGt, Tien3, Tien_Nt3, Tk_No3, Tk_Co3, Han_Tt, Ngay_Ct0,
So_Ct0, So_Seri0, Ten_Vt, Ma_So_Thue, Ten_DtGtGt, Ma_Tc, Ma_Bp, Ma_Km,
Ma_Hd, Ma_Sp, Ma_Job, DUYET

## Testing

You can test the module by running:

```
python test_accounting_processor.py
```

This will process a sample file and display the results.
