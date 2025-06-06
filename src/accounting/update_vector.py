import asyncio

import libsql_experimental as libsql

from src.accounting.create_embedding import generate_batch_embeddings
from src.util.logging import get_logger

logger = get_logger(__name__)

# Increase batch size to improve throughput
BATCH_SIZE = 50  # Increased from 30 to 50


async def update_product_embeddings(
    db: str, batch_size: int = BATCH_SIZE
) -> None:
    """Update embeddings for products by combining text fields and generating embeddings."""
    con = None
    try:
        con = libsql.connect(db)
        cur = con.cursor()

        # Get products that need embeddings
        cur.execute("SELECT code, name FROM departments;")
        rows = cur.fetchall()

        if not rows:
            logger.info("No products found that need embeddings.")
            return

        total_products = len(rows)
        logger.info(f"Found {total_products} products that need embeddings.")

        # Process in batches
        for i in range(0, total_products, batch_size):
            try:
                batch = rows[i : i + batch_size]

                # Generate embeddings for batch
                batch_texts = [row[1] for row in batch]  # Extract names
                embedding_lists = await generate_batch_embeddings(batch_texts)

                # Prepare update statements
                for j, (code, _) in enumerate(batch):
                    if j < len(embedding_lists):
                        embedding_array = embedding_lists[j]
                        embedding_sql = (
                            f"[{','.join(map(str, embedding_array))}]"
                        )

                        # Use parameterized query to prevent SQL injection
                        query = f"UPDATE departments SET embedding = vector64('{embedding_sql}') WHERE code = ?"
                        cur.execute(query, (code,))

                # Commit after each batch
                con.commit()

                logger.info(
                    f"Updated embeddings for batch of {len(batch)} products ({i + len(batch)}/{total_products})"
                )

            except Exception as e:
                logger.error(f"Error in batch {i // batch_size + 1}: {e}")
                con.rollback()
                raise

        logger.info("Successfully updated all embeddings")

    except Exception as e:
        logger.error(f"Error updating product embeddings: {e}")
        if con:
            con.rollback()
        raise
    finally:
        if con:
            con.close()


if __name__ == "__main__":
    asyncio.run(update_product_embeddings("test.db"))
