import libsql_experimental as libsql

from src.accounting.create_embedding import generate_embeddings
from src.util.logging import get_logger

logger = get_logger(__name__)


def find_similar_accounts(query_text, limit=1):
    """Find accounts similar to query text"""
    con = None
    try:
        con = libsql.connect("test.db")
        cur = con.cursor()

        query_embedding = generate_embeddings(query_text)
        embedding_sql = f"[{','.join(map(str, query_embedding))}]"
        cur.execute(
            f"""
            SELECT code, name, vector_distance_cos(embedding, vector64(?)) as similarity
            FROM departments
            ORDER BY similarity ASC
            LIMIT {limit}
        """,
            (embedding_sql,),
        )

        return cur.fetchall()

    except Exception as e:
        logger.error(f"Error finding similar accounts: {e}")
        raise
    finally:
        if con:
            con.close()


if __name__ == "__main__":
    try:
        results = find_similar_accounts("Le MINH XUaN")
        print(f"Found {len(results)} similar accounts")
        for code, name, similarity in results:
            print(f"{code}: {name} (similarity: {similarity:.4f})")
    except Exception as e:
        logger.error(f"Failed to find similar accounts: {e}")
