import os
from pathlib import Path

import pandas as pd
from tantivy import (
    Document,
    Filter,
    Index,
    SchemaBuilder,
    TextAnalyzerBuilder,
    Tokenizer,
)

# Configure logging
from src.util.logging import get_logger

logger = get_logger(__name__)


def index_counterparties(file_path="danhmucdoituong.xls", sheet_name=None):
    """Import documents from Excel file into a Tantivy index with schema matching database field names"""
    logger.info(f"Importing counterparties from: {file_path}")

    try:
        # Create index directory if it doesn't exist
        index_path = Path("index/counterparties")
        os.makedirs(index_path, exist_ok=True)
        logger.info(f"Using index directory: {index_path}")

        # Check available sheets if sheet_name not specified
        if sheet_name is None:
            try:
                xl_file = pd.ExcelFile(file_path)
                sheet_names = xl_file.sheet_names
                logger.info(f"Available sheets in {file_path}: {sheet_names}")
                if len(sheet_names) > 1:
                    logger.warning(
                        f"Multiple sheets found: {sheet_names}. Using first sheet."
                    )
                sheet_name = 0  # Use first sheet by default
            except Exception as e:
                logger.error(f"Error reading sheet names from {file_path}: {e}")
                return False

        logger.info(f"Reading from sheet: {sheet_name}")

        # Create schema with field names matching the database schema (in English)
        schema_builder = SchemaBuilder()
        # Define fields based on the database schema
        code_field = schema_builder.add_text_field("code", stored=True)  # Ma_Dt
        name_field = schema_builder.add_text_field(
            "name", stored=True, tokenizer_name="vietnamese_normalized"
        )  # Ten_Dt
        contact_person_field = schema_builder.add_text_field(
            "contact_person",
            stored=True,
            tokenizer_name="vietnamese_normalized",
        )  # Ong_Ba
        position_field = schema_builder.add_text_field(
            "position", stored=True
        )  # Chuc_Vu
        group_code_field = schema_builder.add_text_field(
            "group_code", stored=True
        )  # Ma_Nh_Dt
        type_field = schema_builder.add_text_field(
            "type", stored=True
        )  # Loai_Dt
        region_code_field = schema_builder.add_text_field(
            "region_code", stored=True
        )  # Ma_Kv
        address_field = schema_builder.add_text_field(
            "address", stored=True, tokenizer_name="vietnamese_normalized"
        )  # Dia_Chi
        phone_field = schema_builder.add_text_field(
            "phone", stored=True
        )  # So_Phone
        fax_field = schema_builder.add_text_field("fax", stored=True)  # So_Fax
        tax_id_field = schema_builder.add_text_field(
            "tax_id", stored=True
        )  # Ma_So_Thue
        bank_account_field = schema_builder.add_text_field(
            "bank_account", stored=True
        )  # So_Tk_NH
        bank_name_field = schema_builder.add_text_field(
            "bank_name", stored=True, tokenizer_name="vietnamese_normalized"
        )  # Ten_NH
        city_field = schema_builder.add_text_field(
            "city", stored=True, tokenizer_name="vietnamese_normalized"
        )  # Ten_Tp

        # Build the schema
        schema = schema_builder.build()

        # Create an index with the schema
        index = Index(schema, path=str(index_path))

        # Create custom analyzer with ASCII folding for Vietnamese text
        vietnamese_analyzer = (
            TextAnalyzerBuilder(Tokenizer.simple())
            .filter(Filter.ascii_fold())
            .filter(Filter.lowercase())
            .build()
        )

        # Register the analyzer
        index.register_tokenizer("vietnamese_normalized", vietnamese_analyzer)

        # Read Excel file
        df = pd.read_excel(file_path, sheet_name=sheet_name)

        # Log DataFrame info for debugging
        logger.info(f"DataFrame shape: {df.shape}")
        logger.info(f"DataFrame columns: {list(df.columns)}")

        # Fill NA values
        df.fillna(
            {col: "" if df[col].dtype == "object" else 0 for col in df.columns},
            inplace=True,
        )

        # Create a writer for the index
        writer = index.writer()

        # Document count
        doc_count = 0

        # Process each row in the DataFrame
        for _, row in df.iterrows():
            try:
                party_type = int(row.get("Loai_Dt", 0))
            except (ValueError, TypeError):
                party_type = 0

            doc_dict = {
                "code": str(row.get("Ma_Dt", "")),
                "name": str(row.get("Ten_Dt", "")),
                "contact_person": str(row.get("Ong_Ba", "")),
                "position": str(row.get("Chuc_Vu", "")),
                "group_code": str(row.get("Ma_Nh_Dt", "")),
                "type": str(party_type),
                "region_code": str(row.get("Ma_Kv", "")),
                "address": str(row.get("Dia_Chi", "")),
                "phone": str(row.get("So_Phone", "")),
                "fax": str(row.get("So_Fax", "")),
                "tax_id": str(row.get("Ma_So_Thue", "")),
                "bank_account": str(row.get("So_Tk_NH", "")),
                "bank_name": str(row.get("Ten_NH", "")),
                "city": str(row.get("Ten_Tp", "")),
            }

            # Add document to the index
            writer.add_document(Document.from_dict(doc_dict))
            doc_count += 1

        # Commit changes to the index
        writer.commit()
        writer.wait_merging_threads()

        logger.info(f"Added {doc_count} counterparties to the index")
        return True

    except Exception as e:
        logger.error(f"Error importing counterparties: {e}")
        return False


def index_accounts(file_path="danhmuctaikhoan.xls", sheet_name=None):
    """Import account chart data from Excel file into a Tantivy index"""
    logger.info(f"Importing accounts from: {file_path}")

    try:
        # Create index directory if it doesn't exist
        index_path = Path("index/accounts")
        os.makedirs(index_path, exist_ok=True)
        logger.info(f"Using index directory: {index_path}")

        # Check available sheets if sheet_name not specified
        if sheet_name is None:
            try:
                xl_file = pd.ExcelFile(file_path)
                sheet_names = xl_file.sheet_names
                logger.info(f"Available sheets in {file_path}: {sheet_names}")
                if len(sheet_names) > 1:
                    logger.warning(
                        f"Multiple sheets found: {sheet_names}. Using first sheet."
                    )
                sheet_name = 0  # Use first sheet by default
            except Exception as e:
                logger.error(f"Error reading sheet names from {file_path}: {e}")
                return False

        logger.info(f"Reading from sheet: {sheet_name}")

        # Create schema based on accounts table structure
        schema_builder = SchemaBuilder()
        code_field = schema_builder.add_text_field("code", stored=True)
        name_field = schema_builder.add_text_field(
            "name", stored=True, tokenizer_name="vietnamese_normalized"
        )
        name_english_field = schema_builder.add_text_field(
            "name_english", stored=True, tokenizer_name="vietnamese_normalized"
        )
        parent_code_field = schema_builder.add_text_field(
            "parent_code", stored=True
        )
        is_detail_field = schema_builder.add_text_field(
            "is_detail", stored=True
        )

        # Build the schema
        schema = schema_builder.build()

        # Create an index with the schema
        index = Index(schema, path=str(index_path))

        # Create custom analyzer with ASCII folding for Vietnamese text
        vietnamese_analyzer = (
            TextAnalyzerBuilder(Tokenizer.simple())
            .filter(Filter.ascii_fold())
            .filter(Filter.lowercase())
            .build()
        )

        # Register the analyzer
        index.register_tokenizer("vietnamese_normalized", vietnamese_analyzer)

        # Read Excel file
        df = pd.read_excel(file_path, sheet_name=sheet_name)

        # Log DataFrame info for debugging
        logger.info(f"DataFrame shape: {df.shape}")
        logger.info(f"DataFrame columns: {list(df.columns)}")

        # Fill NA values
        df.fillna(
            {col: "" if df[col].dtype == "object" else 0 for col in df.columns},
            inplace=True,
        )

        # Create a writer for the index
        writer = index.writer()

        # Document count
        doc_count = 0

        # Process each row in the DataFrame
        for _, row in df.iterrows():
            # Determine if the account is a detail account
            is_detail = (
                1 if str(row.get("Tk_Cuoi", "")).lower() == "true" else 0
            )

            # Create document with fields matching database schema
            doc_dict = {
                "code": str(row.get("Tk", "")),
                "name": str(row.get("Ten_Tk", "")),
                "name_english": str(row.get("Ten_TkE", "")),
                "parent_code": str(row.get("Tk_Cha", "") or ""),
                "is_detail": str(is_detail),
            }

            # Only add document if it has a code
            if doc_dict["code"]:
                writer.add_document(Document.from_dict(doc_dict))
                doc_count += 1

        # Commit changes to the index
        writer.commit()
        writer.wait_merging_threads()

        logger.info(f"Added {doc_count} accounts to the index")
        return True

    except Exception as e:
        logger.error(f"Error importing accounts: {e}")
        return False


def index_departments(file_path="danhmucbophan.xls", sheet_name=None):
    """Import department data from Excel file into a Tantivy index"""
    logger.info(f"Importing departments from: {file_path}")

    try:
        # Create index directory if it doesn't exist
        index_path = Path("index/departments")
        os.makedirs(index_path, exist_ok=True)
        logger.info(f"Using index directory: {index_path}")

        # Check available sheets if sheet_name not specified
        if sheet_name is None:
            try:
                xl_file = pd.ExcelFile(file_path)
                sheet_names = xl_file.sheet_names
                logger.info(f"Available sheets in {file_path}: {sheet_names}")
                if len(sheet_names) > 1:
                    logger.warning(
                        f"Multiple sheets found: {sheet_names}. Using first sheet."
                    )
                sheet_name = 0  # Use first sheet by default
            except Exception as e:
                logger.error(f"Error reading sheet names from {file_path}: {e}")
                return False

        logger.info(f"Reading from sheet: {sheet_name}")

        # Create schema based on departments table structure
        schema_builder = SchemaBuilder()
        code_field = schema_builder.add_text_field("code", stored=True)  # Ma_Bp
        name_field = schema_builder.add_text_field(
            "name", stored=True, tokenizer_name="vietnamese_normalized"
        )  # Ten_Bp
        parent_code_field = schema_builder.add_text_field(
            "parent_code", stored=True
        )  # Ma_Bp_Cha
        is_detail_field = schema_builder.add_text_field(
            "is_detail", stored=True
        )  # Nh_Cuoi
        data_source_field = schema_builder.add_text_field(
            "data_source", stored=True
        )  # Ma_Data

        # Build the schema
        schema = schema_builder.build()

        # Create an index with the schema
        index = Index(schema, path=str(index_path))

        # Create custom analyzer with ASCII folding for Vietnamese text
        vietnamese_analyzer = (
            TextAnalyzerBuilder(Tokenizer.simple())
            .filter(Filter.ascii_fold())
            .filter(Filter.lowercase())
            .build()
        )

        # Register the analyzer
        index.register_tokenizer("vietnamese_normalized", vietnamese_analyzer)

        # Read Excel file
        df = pd.read_excel(file_path, sheet_name=sheet_name)

        # Log DataFrame info for debugging
        logger.info(f"DataFrame shape: {df.shape}")
        logger.info(f"DataFrame columns: {list(df.columns)}")

        # Fill NA values
        df.fillna(
            {col: "" if df[col].dtype == "object" else 0 for col in df.columns},
            inplace=True,
        )

        # Create a writer for the index
        writer = index.writer()

        # Document count
        doc_count = 0

        # Process each row in the DataFrame
        for _, row in df.iterrows():
            is_detail = (
                1 if str(row.get("Nh_Cuoi", "")).lower() == "true" else 0
            )

            doc_dict = {
                "code": str(row.get("Ma_Bp", "")),
                "name": str(row.get("Ten_Bp", "")),
                "parent_code": str(row.get("Ma_Bp_Cha", "") or ""),
                "is_detail": str(is_detail),
                "data_source": str(row.get("Ma_Data", "")),
            }

            # Only add document if it has a code
            if doc_dict["code"]:
                writer.add_document(Document.from_dict(doc_dict))
                doc_count += 1

        # Commit changes to the index
        writer.commit()
        writer.wait_merging_threads()

        logger.info(f"Added {doc_count} departments to the index")
        return True

    except Exception as e:
        logger.error(f"Error importing departments: {e}")
        return False


def index_cost_categories(file_path="danhmuckhoanmuc.xls", sheet_name=None):
    """Import cost category data from Excel file into a Tantivy index"""
    logger.info(f"Importing cost categories from: {file_path}")

    try:
        # Create index directory if it doesn't exist
        index_path = Path("index/cost_categories")
        os.makedirs(index_path, exist_ok=True)
        logger.info(f"Using index directory: {index_path}")

        # Check available sheets if sheet_name not specified
        if sheet_name is None:
            try:
                xl_file = pd.ExcelFile(file_path)
                sheet_names = xl_file.sheet_names
                logger.info(f"Available sheets in {file_path}: {sheet_names}")
                if len(sheet_names) > 1:
                    logger.warning(
                        f"Multiple sheets found: {sheet_names}. Using first sheet."
                    )
                sheet_name = 0  # Use first sheet by default
            except Exception as e:
                logger.error(f"Error reading sheet names from {file_path}: {e}")
                return False

        logger.info(f"Reading from sheet: {sheet_name}")

        # Create schema based on cost_categories table structure
        schema_builder = SchemaBuilder()
        code_field = schema_builder.add_text_field("code", stored=True)  # Ma_Km
        name_field = schema_builder.add_text_field(
            "name", stored=True, tokenizer_name="vietnamese_normalized"
        )  # Ten_Km
        data_source_field = schema_builder.add_text_field(
            "data_source", stored=True
        )  # Ma_Data

        # Build the schema
        schema = schema_builder.build()

        # Create an index with the schema
        index = Index(schema, path=str(index_path))

        # Create custom analyzer with ASCII folding for Vietnamese text
        vietnamese_analyzer = (
            TextAnalyzerBuilder(Tokenizer.simple())
            .filter(Filter.ascii_fold())
            .filter(Filter.lowercase())
            .build()
        )

        # Register the analyzer
        index.register_tokenizer("vietnamese_normalized", vietnamese_analyzer)

        # Read Excel file
        df = pd.read_excel(file_path, sheet_name=sheet_name)

        # Log DataFrame info for debugging
        logger.info(f"DataFrame shape: {df.shape}")
        logger.info(f"DataFrame columns: {list(df.columns)}")

        # Fill NA values
        df.fillna(
            {col: "" if df[col].dtype == "object" else 0 for col in df.columns},
            inplace=True,
        )

        # Create a writer for the index
        writer = index.writer()

        # Document count
        doc_count = 0

        # Process each row in the DataFrame
        for _, row in df.iterrows():
            doc_dict = {
                "code": str(row.get("Ma_Km", "")),
                "name": str(row.get("Ten_Km", "")),
                "data_source": str(row.get("Ma_Data", "")),
            }

            # Only add document if it has a code
            if doc_dict["code"]:
                writer.add_document(Document.from_dict(doc_dict))
                doc_count += 1

        # Commit changes to the index
        writer.commit()
        writer.wait_merging_threads()

        logger.info(f"Added {doc_count} cost categories to the index")
        return True

    except Exception as e:
        logger.error(f"Error importing cost categories: {e}")
        return False


def index_pos_machines(file_path="danhmucmaypos.xlsx", sheet_name=None):
    """Import POS machines data from Excel file into a Tantivy index"""
    logger.info(f"Importing POS machines from: {file_path}")

    try:
        # Create index directory if it doesn't exist
        index_path = Path("index/pos_machines")
        os.makedirs(index_path, exist_ok=True)
        logger.info(f"Using index directory: {index_path}")

        # Check available sheets if sheet_name not specified
        if sheet_name is None:
            try:
                xl_file = pd.ExcelFile(file_path)
                sheet_names = xl_file.sheet_names
                logger.info(f"Available sheets in {file_path}: {sheet_names}")
                if len(sheet_names) > 1:
                    logger.warning(
                        f"Multiple sheets found: {sheet_names}. Using first sheet."
                    )
                sheet_name = 0  # Use first sheet by default
            except Exception as e:
                logger.error(f"Error reading sheet names from {file_path}: {e}")
                return False

        logger.info(f"Reading from sheet: {sheet_name}")

        # Create schema based on pos_machines table structure
        schema_builder = SchemaBuilder()
        code_field = schema_builder.add_text_field(
            "code", stored=True
        )  # MA_TIP/Mã TIP/etc
        department_code_field = schema_builder.add_text_field(
            "department_code", stored=True
        )  # Ma_Dt/Mã đối tượng/etc
        name_field = schema_builder.add_text_field(
            "name", stored=True, tokenizer_name="vietnamese_normalized"
        )  # Tên/Ten/etc
        address_field = schema_builder.add_text_field(
            "address", stored=True, tokenizer_name="vietnamese_normalized"
        )  # Địa chỉ/Dia_Chi/etc
        account_holder_field = schema_builder.add_text_field(
            "account_holder",
            stored=True,
            tokenizer_name="vietnamese_normalized",
        )  # CHỦ TÀI KHOẢN/Chu_TK/etc
        account_number_field = schema_builder.add_text_field(
            "account_number", stored=True
        )  # TK THỤ HƯỞNG/TK_TH/etc
        bank_name_field = schema_builder.add_text_field(
            "bank_name", stored=True, tokenizer_name="vietnamese_normalized"
        )  # NGÂN HÀNG/Ngan_Hang/etc

        # Build the schema
        schema = schema_builder.build()

        # Create an index with the schema
        index = Index(schema, path=str(index_path))

        # Create custom analyzer with ASCII folding for Vietnamese text
        vietnamese_analyzer = (
            TextAnalyzerBuilder(Tokenizer.simple())
            .filter(Filter.ascii_fold())
            .filter(Filter.lowercase())
            .build()
        )

        # Register the analyzer
        index.register_tokenizer("vietnamese_normalized", vietnamese_analyzer)

        # Read Excel file
        df = pd.read_excel(file_path, sheet_name=sheet_name)

        # Log DataFrame info for debugging
        logger.info(f"DataFrame shape: {df.shape}")
        logger.info(f"DataFrame columns: {list(df.columns)}")

        # Fill NA values
        df.fillna(
            {col: "" if df[col].dtype == "object" else 0 for col in df.columns},
            inplace=True,
        )

        # Create a writer for the index
        writer = index.writer()

        # Document count
        doc_count = 0
        skipped_empty = 0

        # Process each row in the DataFrame
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

            doc_dict = {
                "code": pos_code,
                "department_code": str(row.get(dept_col, ""))
                if dept_col
                else "",
                "name": str(row.get(name_col, "")) if name_col else "",
                "address": str(row.get(addr_col, "")) if addr_col else "",
                "account_holder": str(row.get(holder_col, ""))
                if holder_col
                else "",
                "account_number": str(row.get(account_col, ""))
                if account_col
                else "",
                "bank_name": str(row.get(bank_col, "")) if bank_col else "",
            }

            # Add document to the index
            writer.add_document(Document.from_dict(doc_dict))
            doc_count += 1

        # Commit changes to the index
        writer.commit()
        writer.wait_merging_threads()

        logger.info(
            f"Added {doc_count} POS machines to the index (skipped {skipped_empty} empty entries)"
        )
        return True

    except Exception as e:
        logger.error(f"Error importing POS machines: {e}")
        return False


# Import documents from Excel files
if __name__ == "__main__":
    # Import counterparties
    index_counterparties(file_path="danhmucdoituong.xls")

    # Import accounts
    index_accounts(file_path="danhmuctaikhoan.xls")

    # Import departments
    index_departments(file_path="danhmucbophan.xls")

    # Import cost categories
    index_cost_categories(file_path="danhmuckhoanmuc.xls")

    # Import POS machines
    index_pos_machines(file_path="danhmucmaypos.xlsx")
