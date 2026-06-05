import logging
from opensearchpy import OpenSearch, exceptions
from typing import Optional, Dict, Any
from ..config import settings
from ..embeddings import embedding_generator
from ..models import StoredQuery

logger = logging.getLogger(__name__)

class OpenSearchClient:
    def __init__(self):
        self.client: Optional[OpenSearch] = None
        self.index_name = settings.opensearch_index

    def connect(self):
        self.client = OpenSearch(
            hosts=[settings.opensearch_url],
            http_compress=True,
            use_ssl=False,
            verify_certs=False,
            timeout=30
        )
        logger.info(f"Connected to OpenSearch: {settings.opensearch_url}")
        self._create_index_if_not_exists()

    def _create_index_if_not_exists(self):
        if not self.client.indices.exists(index=self.index_name):
            index_settings = {
                "settings": {
                    "index": {
                        "knn": True,
                        "knn.algo_param.ef_search": 100
                    }
                },
                "mappings": {
                    "properties": {
                        "text_vector": {
                            "type": "knn_vector",
                            "dimension": 384,
                            "method": {
                                "name": "hnsw",
                                "space_type": "cosinesimil",
                                "engine": "lucene",
                                "parameters": {
                                    "ef_construction": 128,
                                    "m": 24
                                }
                            }
                        },
                        "text": {"type": "text"},
                        "request_id": {"type": "keyword"},
                        "parsed_query": {"type": "object"},
                        "success": {"type": "boolean"},
                        "user_feedback": {"type": "boolean"},
                        "language": {"type": "keyword"},
                        "created_at": {"type": "date"}
                    }
                }
            }
            self.client.indices.create(index=self.index_name, body=index_settings)
            logger.info(f"Created index {self.index_name} with KNN")

    def find_similar(self, text: str, threshold: float = None) -> Optional[Dict[str, Any]]:
        if threshold is None:
            threshold = settings.similarity_threshold

        vector = embedding_generator.encode(text)

        knn_query = {
            "size": 1,
            "query": {
                "knn": {
                    "text_vector": {
                        "vector": vector,
                        "k": 1
                    }
                }
            },
            "_source": ["text", "parsed_query", "success", "user_feedback"]
        }

        try:
            response = self.client.search(index=self.index_name, body=knn_query)
            hits = response.get("hits", {}).get("hits", [])

            if hits:
                hit = hits[0]
                score = hit.get("_score", 0)

                if score >= threshold:
                    logger.info(f"Found similar query. Score: {score}")
                    source = hit.get("_source", {})
                    return {
                        "text": source.get("text"),
                        "parsed_query": source.get("parsed_query"),
                        "score": score
                    }
            return None
        except Exception as e:
            logger.error(f"KNN search error: {e}")
            return None

    def save_query(self, stored: StoredQuery) -> bool:
        try:
            vector = embedding_generator.encode(stored.text)

            doc = stored.dict()
            doc["text_vector"] = vector

            self.client.index(
                index=self.index_name,
                id=stored.request_id,
                body=doc,
                refresh=True
            )
            logger.info(f"Query saved: {stored.request_id}")
            return True
        except Exception as e:
            logger.error(f"Failed to save query: {e}")
            return False

os_client = OpenSearchClient()