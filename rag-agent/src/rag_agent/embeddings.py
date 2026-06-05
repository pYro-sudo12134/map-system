import logging
from sentence_transformers import SentenceTransformer
from typing import List, Optional

logger = logging.getLogger(__name__)


class EmbeddingGenerator:
    def __init__(self, model_name: str = "all-MiniLM-L6-v2"):
        """
        Модель all-MiniLM-L6-v2:
        - Размер: 80MB
        - Размерность вектора: 384
        - Быстрая, хороша для семантического поиска
        """
        self.model_name = model_name
        self.model: Optional[SentenceTransformer] = None

    def load(self):
        if self.model is None:
            logger.info(f"Loading embedding model: {self.model_name}")
            self.model = SentenceTransformer(self.model_name)
            logger.info("Model is loaded")

    def encode(self, text: str) -> List[float]:
        self.load()
        vector = self.model.encode(text, normalize_embeddings=True)
        return vector.tolist()

    def encode_batch(self, texts: List[str]) -> List[List[float]]:
        self.load()
        vectors = self.model.encode(texts, normalize_embeddings=True)
        return vectors.tolist()


embedding_generator = EmbeddingGenerator()
