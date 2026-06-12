import pytest
from unittest.mock import patch, AsyncMock
from rag_agent.services.rag_service import RAGService
from rag_agent.models import QueryParams, QueryType


class TestRAGService:

    @pytest.mark.asyncio
    async def test_process_uses_cache_when_similar_found(self, sample_rag_request):
        similar_query = {
            "score": 0.92,
            "parsed_query": {
                "query_type": "proximity",
                "params": {"location_ref": "city center", "radius_km": 5}
            }
        }

        with patch('rag_agent.services.rag_service.os_client.find_similar', return_value=similar_query):
            result = await RAGService.process(sample_rag_request)

            assert result.query_type == QueryType.PROXIMITY
            assert result.params["location_ref"] == "city center"

    @pytest.mark.asyncio
    async def test_process_calls_llm_when_no_similar(self, sample_rag_request):
        expected_result = QueryParams(
            query_type=QueryType.PROXIMITY,
            params={"location_ref": "center", "radius_km": 5}
        )

        with patch('rag_agent.services.rag_service.os_client.find_similar', return_value=None):
            with patch('rag_agent.services.rag_service.llm_client.process', new_callable=AsyncMock) as mock_llm:
                mock_llm.return_value = expected_result

                result = await RAGService.process(sample_rag_request)

                assert result == expected_result
                mock_llm.assert_called_once_with(sample_rag_request.text)

    @pytest.mark.asyncio
    async def test_process_uses_cache_with_language_filter(self, sample_rag_request_ru):
        similar_query = {
            "score": 0.88,
            "parsed_query": {
                "query_type": "proximity",
                "params": {"location_ref": "центр", "radius_km": 3}
            }
        }

        with patch('rag_agent.services.rag_service.os_client.find_similar') as mock_find:
            mock_find.return_value = similar_query

            result = await RAGService.process(sample_rag_request_ru)

            call_args = mock_find.call_args[1]
            assert call_args['language'] == 'ru'
            assert result.query_type == QueryType.PROXIMITY

    @pytest.mark.asyncio
    async def test_process_low_score_not_used(self, sample_rag_request):
        similar_query = {
            "score": 0.75,
            "parsed_query": {"query_type": "proximity", "params": {}}
        }

        expected_result = QueryParams(
            query_type=QueryType.NEAREST,
            params={"poi_type": "gas station"}
        )

        with patch('rag_agent.services.rag_service.os_client.find_similar', return_value=similar_query):
            with patch('rag_agent.services.rag_service.llm_client.process', new_callable=AsyncMock) as mock_llm:
                mock_llm.return_value = expected_result

                result = await RAGService.process(sample_rag_request)

                assert result == expected_result
                mock_llm.assert_called_once()
