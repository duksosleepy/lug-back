from pathlib import Path

from tantivy import Filter, Index, Query, TextAnalyzerBuilder, Tokenizer

# Configure logging
from src.util.logging import get_logger

logger = get_logger(__name__)

# Get the absolute path to the index directory
INDEX_DIR = Path(__file__).parent / "index"


def search_counterparties(query_text, field_name="name", limit=10):
    """Search counterparties by name or other fields"""
    logger.info(
        f"Searching counterparties for '{query_text}' in field '{field_name}'"
    )

    try:
        # Load the counterparties index using absolute path
        counterparties_index = INDEX_DIR / "counterparties"
        logger.info(f"Opening index at {counterparties_index}")
        index = Index.open(str(counterparties_index))

        # Create custom analyzer with ASCII folding for Vietnamese text
        vietnamese_analyzer = (
            TextAnalyzerBuilder(Tokenizer.raw())
            .filter(Filter.ascii_fold())
            .filter(Filter.lowercase())
            .build()
        )

        # Register the analyzer
        index.register_tokenizer("vietnamese_normalized", vietnamese_analyzer)

        # Get searcher
        searcher = index.searcher()

        processed_terms = vietnamese_analyzer.analyze(query_text)
        processed_term = processed_terms[0] if processed_terms else query_text
        regex_pattern = f".*{processed_term}.*"
        query = Query.regex_query(index.schema, field_name, regex_pattern)

        # Execute search
        search_result = searcher.search(query, limit)

        # Format results
        results = []
        for score, doc_address in search_result.hits:
            doc = searcher.doc(doc_address)
            results.append(
                {
                    "score": score,
                    "code": doc.get_first("code") or "",
                    "name": doc.get_first("name") or "",
                    "address": doc.get_first("address") or "",
                    "phone": doc.get_first("phone") or "",
                    "tax_id": doc.get_first("tax_id") or "",
                }
            )

        return results

    except Exception as e:
        logger.error(f"Error searching counterparties: {e}")
        return []


def search_accounts(query_text, field_name="name", limit=10):
    """Search accounts by name or code"""
    logger.info(
        f"Searching accounts for '{query_text}' in field '{field_name}'"
    )

    try:
        # Load the accounts index using absolute path
        accounts_index = INDEX_DIR / "accounts"
        logger.info(f"Opening accounts index at {accounts_index}")
        index = Index.open(str(accounts_index))

        # Create custom analyzer with ASCII folding for Vietnamese text
        vietnamese_analyzer = (
            TextAnalyzerBuilder(Tokenizer.simple())
            .filter(Filter.ascii_fold())
            .filter(Filter.lowercase())
            .build()
        )

        # Register the analyzer
        index.register_tokenizer("vietnamese_normalized", vietnamese_analyzer)

        # Get searcher
        searcher = index.searcher()

        if query_text.isdigit():
            # For numeric input, construct regex pattern and use regex_query
            regex_pattern = f".*{query_text}.*"
            query = Query.regex_query(index.schema, field_name, regex_pattern)

        else:
            # For normal string input, process with Vietnamese analyzer
            processed_terms = vietnamese_analyzer.analyze(query_text)
            processed_term = (
                processed_terms[0] if processed_terms else query_text
            )
            regex_pattern = f".*{processed_term}.*"
            query = Query.regex_query(index.schema, field_name, regex_pattern)

        # Execute search
        search_result = searcher.search(query, limit)

        # Format results
        results = []
        for score, doc_address in search_result.hits:
            doc = searcher.doc(doc_address)
            results.append(
                {
                    "score": score,
                    "code": doc.get_first("code") or "",
                    "name": doc.get_first("name") or "",
                    "name_english": doc.get_first("name_english") or "",
                    "parent_code": doc.get_first("parent_code") or "",
                    "is_detail": doc.get_first("is_detail") or "",
                }
            )

        return results

    except Exception as e:
        logger.error(f"Error searching accounts: {e}")
        return []


def prefix_search_accounts(prefix, limit=10):
    """Search accounts by code prefix"""
    logger.info(f"Prefix searching accounts for '{prefix}'")

    try:
        # Load the accounts index using absolute path
        accounts_index = INDEX_DIR / "accounts"
        logger.info(
            f"Opening accounts index at {accounts_index} for prefix search"
        )
        index = Index.open(str(accounts_index))

        # Create custom analyzer with ASCII folding for Vietnamese text
        vietnamese_analyzer = (
            TextAnalyzerBuilder(Tokenizer.simple())
            .filter(Filter.ascii_fold())
            .filter(Filter.lowercase())
            .build()
        )

        # Register the analyzer
        index.register_tokenizer("vietnamese_normalized", vietnamese_analyzer)

        # Get searcher
        searcher = index.searcher()

        processed_terms = vietnamese_analyzer.analyze(prefix)

        processed_term = processed_terms[0] if processed_terms else prefix
        regex_pattern = f"{processed_term}.*"
        query = Query.regex_query(index.schema, "code", regex_pattern)

        # Execute search
        search_result = searcher.search(query, limit)

        # Format results
        results = []
        for score, doc_address in search_result.hits:
            doc = searcher.doc(doc_address)
            results.append(
                {
                    "score": score,
                    "code": doc.get_first("code") or "",
                    "name": doc.get_first("name") or "",
                    "name_english": doc.get_first("name_english") or "",
                    "parent_code": doc.get_first("parent_code") or "",
                    "is_detail": doc.get_first("is_detail") or "",
                }
            )

        return results

    except Exception as e:
        logger.error(f"Error prefix searching accounts: {e}")
        return []


def search_departments(query_text, field_name="name", limit=10):
    """Search departments by name or code"""
    logger.info(
        f"Searching departments for '{query_text}' in field '{field_name}'"
    )

    try:
        # Load the departments index using absolute path
        departments_index = INDEX_DIR / "departments"
        logger.info(f"Opening departments index at {departments_index}")
        index = Index.open(str(departments_index))

        # Create custom analyzer with ASCII folding for Vietnamese text
        vietnamese_analyzer = (
            TextAnalyzerBuilder(Tokenizer.raw())
            .filter(Filter.ascii_fold())
            .filter(Filter.lowercase())
            .build()
        )

        # Register the analyzer
        index.register_tokenizer("vietnamese_normalized", vietnamese_analyzer)

        # Get searcher
        searcher = index.searcher()

        processed_terms = vietnamese_analyzer.analyze(query_text)
        processed_term = processed_terms[0] if processed_terms else query_text
        regex_pattern = f".*{processed_term}.*"
        query = Query.regex_query(index.schema, field_name, regex_pattern)

        # Execute search
        search_result = searcher.search(query, limit)

        # Format results
        results = []
        for score, doc_address in search_result.hits:
            doc = searcher.doc(doc_address)
            results.append(
                {
                    "score": score,
                    "code": doc.get_first("code") or "",
                    "name": doc.get_first("name") or "",
                    "parent_code": doc.get_first("parent_code") or "",
                    "is_detail": doc.get_first("is_detail") or "",
                    "data_source": doc.get_first("data_source") or "",
                }
            )

        return results

    except Exception as e:
        logger.error(f"Error searching departments: {e}")
        return []


def search_cost_categories(query_text, field_name="code", limit=10):
    """Search cost categories by name or code"""
    logger.info(
        f"Searching cost categories for '{query_text}' in field '{field_name}'"
    )

    try:
        # Load the cost_categories index using absolute path
        cost_categories_index = INDEX_DIR / "cost_categories"
        logger.info(f"Opening cost categories index at {cost_categories_index}")
        index = Index.open(str(cost_categories_index))

        # Create custom analyzer with ASCII folding for Vietnamese text
        vietnamese_analyzer = (
            TextAnalyzerBuilder(Tokenizer.raw())
            .filter(Filter.ascii_fold())
            .filter(Filter.lowercase())
            .build()
        )

        # Register the analyzer
        index.register_tokenizer("vietnamese_normalized", vietnamese_analyzer)

        # Get searcher
        searcher = index.searcher()

        processed_terms = vietnamese_analyzer.analyze(query_text)
        processed_term = processed_terms[0] if processed_terms else query_text
        regex_pattern = f".*{processed_term}.*"
        query = Query.regex_query(index.schema, field_name, regex_pattern)

        # Execute search
        search_result = searcher.search(query, limit)

        # Format results
        results = []
        for score, doc_address in search_result.hits:
            doc = searcher.doc(doc_address)
            results.append(
                {
                    "score": score,
                    "code": doc.get_first("code") or "",
                    "name": doc.get_first("name") or "",
                    "data_source": doc.get_first("data_source") or "",
                }
            )

        return results

    except Exception as e:
        logger.error(f"Error searching cost categories: {e}")
        return []


def search_pos_machines(query_text, field_name="code", limit=10):
    """Search POS machines by name, code, or other fields"""
    logger.info(
        f"Searching POS machines for '{query_text}' in field '{field_name}'"
    )

    try:
        # Load the pos_machines index using absolute path
        pos_machines_index = INDEX_DIR / "pos_machines"
        logger.info(f"Opening POS machines index at {pos_machines_index}")
        index = Index.open(str(pos_machines_index))

        # Create custom analyzer with ASCII folding for Vietnamese text
        vietnamese_analyzer = (
            TextAnalyzerBuilder(Tokenizer.simple())
            .filter(Filter.ascii_fold())
            .filter(Filter.lowercase())
            .build()
        )

        # Register the analyzer
        index.register_tokenizer("vietnamese_normalized", vietnamese_analyzer)

        # Get searcher
        searcher = index.searcher()

        # For all inputs, use regex search
        regex_pattern = f".*{query_text}.*"
        query = Query.regex_query(index.schema, field_name, regex_pattern)

        # Execute search
        search_result = searcher.search(query, limit)

        # Format results
        results = []
        for score, doc_address in search_result.hits:
            doc = searcher.doc(doc_address)
            results.append(
                {
                    "score": score,
                    "code": doc.get_first("code") or "",
                    "department_code": doc.get_first("department_code") or "",
                    "name": doc.get_first("name") or "",
                    "address": doc.get_first("address") or "",
                    "account_holder": doc.get_first("account_holder") or "",
                    "account_number": doc.get_first("account_number") or "",
                    "bank_name": doc.get_first("bank_name") or "",
                }
            )

        return results

    except Exception as e:
        logger.error(f"Error searching POS machines: {e}")
        return []


def search_pos_by_department(department_code, limit=50):
    """Search POS machines by department code"""
    logger.info(f"Searching POS machines for department '{department_code}'")
    return search_pos_machines(
        department_code, field_name="department_code", limit=limit
    )


# Example usage
if __name__ == "__main__":
    # Search for counterparties
    print("=== Counterparty Search Examples ===")

    # Basic search for company name
    results = search_counterparties("THÀNH VIÊN SHINHAN")
    print("\nCounterparties with 'THÀNH VIÊN SHINHAN' in name:")
    for result in results:
        print(
            f"Code: {result['code']} | Name: {result['name']} | Score: {result['score']:.4f}"
        )

    # Search for accounts by name
    print("\n=== Account Search Examples ===")
    results = search_accounts("tien")
    print("\nAccounts with 'tien' in name:")
    for result in results:
        print(
            f"Code: {result['code']} | Name: {result['name']} | Is Detail: {result['is_detail']} | Score: {result['score']:.4f}"
        )

    # Search for accounts by code prefix
    results = prefix_search_accounts("112")
    print("\nAccounts with code starting with '112':")
    for result in results:
        print(
            f"Code: {result['code']} | Name: {result['name']} | Is Detail: {result['is_detail']} | Score: {result['score']:.4f}"
        )

    # Search for departments
    print("\n=== Department Search Examples ===")
    results = search_departments("AEONMALL HẢI PHÒNG")
    print("\nDepartments with 'ĐĐKD BIGC in name:")
    for result in results:
        print(
            f"Code: {result['code']} | Name: {result['name']} | Is Detail: {result['is_detail']} | Score: {result['score']:.4f}"
        )

    # Search for cost categories
    print("\n=== Cost Category Search Examples ===")
    results = search_cost_categories("BHXH")
    print("\nCost categories with 'BHXH' in name:")
    for result in results:
        print(
            f"Code: {result['code']} | Name: {result['name']} | Score: {result['score']:.4f}"
        )

    # Search for POS machines
    print("\n=== POS Machine Search Examples ===")
    results = search_pos_machines("14100414")
    print("\nPOS machines with '14100414' in name or bank:")
    for result in results:
        print(
            f"Code: {result['code']} | Name: {result['name']} | Bank: {result['bank_name']} | Score: {result['score']:.4f}"
        )

    # Search for POS machines by department
    results = search_pos_by_department("CN01")
    print("\nPOS machines for department 'CN01':")
    for result in results:
        print(
            f"Code: {result['code']} | Name: {result['name']} | Department: {result['department_code']} | Score: {result['score']:.4f}"
        )
