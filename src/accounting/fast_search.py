from tantivy import Filter, Index, Query, TextAnalyzerBuilder, Tokenizer

# Configure logging
from src.util.logging import get_logger

logger = get_logger(__name__)


def search_counterparties(query_text, field_name="name", limit=10):
    """Search counterparties by name or other fields"""
    logger.info(
        f"Searching counterparties for '{query_text}' in field '{field_name}'"
    )

    try:
        # Load the counterparties index
        index = Index.open("index/counterparties")

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

        # Process the search terms with Vietnamese analyzer
        # processed_terms = vietnamese_analyzer.analyze(query_text)
        # processed_query = (
        #     " ".join(processed_terms) if processed_terms else query_text
        # )

        # # Create query
        # query = index.parse_query(processed_query, [field_name])

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
        # Load the accounts index
        index = Index.open("index/accounts")

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
        # Load the accounts index
        index = Index.open("index/accounts")

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

        # Create prefix query

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


# Example usage
if __name__ == "__main__":
    # Search for counterparties
    print("=== Counterparty Search Examples ===")

    # Basic search for company name
    results = search_counterparties("SHINHAN")
    print("\nCounterparties with 'công ty' in name:")
    for result in results:
        print(
            f"Code: {result['code']} | Name: {result['name']} | Score: {result['score']:.4f}"
        )

    # Search for accounts by name
    print("\n=== Account Search Examples ===")
    results = search_accounts("3840")
    print("\nAccounts with 'Lon - 63' in name:")
    print(len(results))
    for result in results:
        print(
            f"Code: {result['code']} | Name: {result['name']} | Is Detail: {result['is_detail']} | Score: {result['score']:.4f}"
        )

    # Search for accounts by code prefix
    results = prefix_search_accounts("11211")
    print("\nAccounts with code starting with '11211':")
    for result in results:
        print(
            f"Code: {result['code']} | Name: {result['name']} | Is Detail: {result['is_detail']} | Score: {result['score']:.4f}"
        )
