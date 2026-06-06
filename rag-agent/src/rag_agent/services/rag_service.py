import logging
from ..models import QueryParams
from ..clients import os_client, llm_client

logger = logging.getLogger(__name__)

class RAGService:
    @staticmethod
    async def process(request):
        similar = os_client.find_similar(
            text=request.text,
            language=request.language
        )

        if similar and similar.get('score', 0) >= 0.85:
            logger.info(f"Found similar query. Score: {similar['score']}, Language: {request.language}")
            return QueryParams(
                query_type=similar['parsed_query'].get('query_type', 'unknown'),
                params=similar['parsed_query'].get('params', {})
            )

        return await llm_client.process(request.text)