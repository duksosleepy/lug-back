from functools import lru_cache
from typing import List

import numpy as np
from fastembed import TextEmbedding

from src.util.logging import get_logger

logger = get_logger(__name__)

# Initialize model on-demand rather than immediately
_embedding_model = None


def get_embedding_model() -> TextEmbedding:
    """Initialize the embedding model only when needed."""
    global _embedding_model
    if _embedding_model is None:
        # Use the smallest available model (384 dimensions)
        _embedding_model = TextEmbedding(
            model_name="sentence-transformers/all-MiniLM-L6-v2"
        )
    return _embedding_model


def clean_text(text: str) -> str:
    """Clean text for embedding generation."""
    return text.replace("\n", " ")


async def generate_batch_embeddings(texts: List[str]) -> List[List[float]]:
    """Generate embeddings and return as float arrays for vector64()."""
    try:
        # Filter empty texts and clean
        cleaned_texts = [clean_text(text) for text in texts if text]

        if not cleaned_texts:
            return []

        # Generate 384-dimensional embeddings
        embedding_model = get_embedding_model()
        embeddings = list(embedding_model.embed(cleaned_texts))

        # Return as lists of floats
        return [embedding.tolist() for embedding in embeddings]
    except Exception as e:
        logger.error(f"Error generating batch embeddings: {e}")
        raise


@lru_cache(maxsize=1024)  # Cache up to 1024 recent embeddings
def generate_embeddings(text: str) -> list[float]:
    """Generate embedding and return as float array for vector64()."""
    try:
        cleaned_text = clean_text(text)
        embedding_model = get_embedding_model()
        embeddings = list(embedding_model.embed([cleaned_text]))
        embedding = embeddings[0]

        # Convert to 64-bit float precision
        float64_embedding = np.array(embedding, dtype=np.float64)
        return float64_embedding.tolist()
    except Exception as e:
        logger.error(f"Error generating embedding: {e}")
        raise
