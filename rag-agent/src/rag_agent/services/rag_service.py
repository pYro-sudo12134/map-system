import logging

from ..config import settings
from ..models import QueryParams
from ..clients import os_client, llm_client

logger = logging.getLogger(__name__)


class RAGService:
    def __init__(self, config=settings, os_client_=os_client, llm_client_=llm_client):
        self.config = config
        self.os_client = os_client_
        self.llm_client = llm_client_

    async def process(self, request):
        similar = self.os_client.find_similar(
            text=request.text,
            language=request.language,
            threshold=self.config.similarity_threshold,
        )

        if similar and similar.get("score", 0) >= self.config.similarity_threshold:
            logger.info(
                f"Found similar query. Score: {similar['score']}, "
                f"Language: {request.language}"
            )
            return QueryParams(
                query_type=similar["parsed_query"].get("query_type", "unknown"),
                params=similar["parsed_query"].get("params", {}),
            )

        return await self.llm_client.process(request.text)


rag_service = RAGService()