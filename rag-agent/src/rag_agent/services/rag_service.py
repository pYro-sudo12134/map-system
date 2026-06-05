import logging

from ..clients import os_client, llm_client
from ..models import QueryParams

logger = logging.getLogger(__name__)

class RAGService:
    @staticmethod
    async def process(request):
        similar = os_client.find_similar(request.text)

        if similar and similar.get('score', 0) >= 0.85:
            logger.info(f"Found similar query. Score: {similar['score']}")
            return QueryParams(
                query_type=similar['parsed_query'].get('query_type', 'unknown'),
                params=similar['parsed_query'].get('params', {})
            )

        return await llm_client.process(request.text)