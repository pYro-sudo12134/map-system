import pytest
from unittest.mock import MagicMock, AsyncMock
from rag_agent.services.rag_service import RAGService
from rag_agent.models import QueryParams, QueryType


class TestRAGService:

    @pytest.fixture
    def service(self):
        config = MagicMock()
        config.similarity_threshold = 0.85
        os_client = MagicMock()
        llm_client = MagicMock()
        llm_client.process = AsyncMock()
        return RAGService(config=config, os_client_=os_client, llm_client_=llm_client)

    @pytest.mark.asyncio
    async def test_process_uses_cache_when_similar_found(self, service, sample_rag_request):
        similar_query = {
            "score": 0.92,
            "parsed_query": {
                "query_type": "proximity",
                "params": {"location_ref": "city center", "radius_km": 5}
            }
        }
        service.os_client.find_similar.return_value = similar_query

        result = await service.process(sample_rag_request)

        assert result.query_type == QueryType.PROXIMITY
        assert result.params["location_ref"] == "city center"
        service.llm_client.process.assert_not_called()

    @pytest.mark.asyncio
    async def test_process_calls_llm_when_no_similar(self, service, sample_rag_request):
        expected_result = QueryParams(
            query_type=QueryType.PROXIMITY,
            params={"location_ref": "center", "radius_km": 5}
        )
        service.os_client.find_similar.return_value = None
        service.llm_client.process.return_value = expected_result

        result = await service.process(sample_rag_request)

        assert result == expected_result
        service.llm_client.process.assert_called_once_with(sample_rag_request.text)

    @pytest.mark.asyncio
    async def test_process_passes_threshold_from_config(self, service, sample_rag_request):
        """Порог similarity должен приходить из конфига, а не хардкодиться."""
        service.config.similarity_threshold = 0.7
        service.os_client.find_similar.return_value = None
        service.llm_client.process.return_value = QueryParams(
            query_type=QueryType.UNKNOWN, params={}
        )

        await service.process(sample_rag_request)

        call_kwargs = service.os_client.find_similar.call_args[1]
        assert call_kwargs["threshold"] == 0.7

    @pytest.mark.asyncio
    async def test_process_uses_cache_with_language_filter(self, service, sample_rag_request_ru):
        similar_query = {
            "score": 0.88,
            "parsed_query": {
                "query_type": "proximity",
                "params": {"location_ref": "центр", "radius_km": 3}
            }
        }
        service.os_client.find_similar.return_value = similar_query

        result = await service.process(sample_rag_request_ru)

        call_kwargs = service.os_client.find_similar.call_args[1]
        assert call_kwargs["language"] == "ru"
        assert result.query_type == QueryType.PROXIMITY

    @pytest.mark.asyncio
    async def test_process_low_score_not_used(self, service, sample_rag_request):
        similar_query = {
            "score": 0.75,
            "parsed_query": {"query_type": "proximity", "params": {}}
        }
        expected_result = QueryParams(
            query_type=QueryType.NEAREST,
            params={"poi_type": "gas station"}
        )
        service.os_client.find_similar.return_value = similar_query
        service.llm_client.process.return_value = expected_result

        result = await service.process(sample_rag_request)

        assert result == expected_result
        service.llm_client.process.assert_called_once()

    @pytest.mark.asyncio
    async def test_process_uses_custom_threshold(self, sample_rag_request):
        config = MagicMock()
        config.similarity_threshold = 0.6
        os_client = MagicMock()
        llm_client = MagicMock()
        llm_client.process = AsyncMock()

        os_client.find_similar.return_value = {
            "score": 0.8,
            "parsed_query": {"query_type": "nearest", "params": {"poi_type": "cafe"}},
        }
        service = RAGService(config=config, os_client_=os_client, llm_client_=llm_client)

        result = await service.process(sample_rag_request)

        assert result.query_type == QueryType.NEAREST
        llm_client.process.assert_not_called()