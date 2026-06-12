import pytest
from unittest.mock import patch, AsyncMock, MagicMock
from rag_agent.clients.llm_client import LLMClient, SYSTEM_PROMPT
from rag_agent.models import QueryType


class TestLLMClient:

    @pytest.fixture
    def llm_client(self):
        with patch('ollama.Client') as mock_ollama:
            client = LLMClient()
            client.client = MagicMock()
            yield client

    def test_parse_response_valid_json(self, llm_client):
        response = '{"query_type": "proximity", "params": {"location": "center", "radius": 5}}'

        result = llm_client._parse_response(response)

        assert result.query_type == QueryType.PROXIMITY
        assert result.params["location"] == "center"

    def test_parse_response_with_json_markdown(self, llm_client):
        response = '```json\n{"query_type": "shortest_path", "params": {"from": "A", "to": "B"}}\n```'

        result = llm_client._parse_response(response)

        assert result.query_type == QueryType.SHORTEST_PATH
        assert result.params["from"] == "A"

    def test_parse_response_invalid_json(self, llm_client):
        response = 'not a json'

        result = llm_client._parse_response(response)

        assert result.query_type == QueryType.UNKNOWN
        assert "original_text" in result.params

    def test_call_ollama_success(self, llm_client):
        mock_response = {"response": '{"query_type": "nearest", "params": {"poi": "gas station"}}'}

        with patch('ollama.Client') as mock_client_class:
            mock_client = MagicMock()
            mock_client.generate.return_value = mock_response
            mock_client_class.return_value = mock_client

            result = llm_client._call_ollama("find gas station")

            assert result == mock_response["response"]

    def test_call_ollama_error(self, llm_client):
        with patch('ollama.Client') as mock_client_class:
            mock_client = MagicMock()
            mock_client.generate.side_effect = Exception("Connection refused")
            mock_client_class.return_value = mock_client

            with pytest.raises(Exception):
                llm_client._call_ollama("test query")

    @pytest.mark.asyncio
    async def test_process_success(self, llm_client):
        mock_response = '{"query_type": "shortest_path", "params": {"from_ref": "station", "to_ref": "airport"}}'

        with patch.object(llm_client, '_call_ollama', return_value=mock_response):
            result = await llm_client.process("how to get from station to airport")

            assert result.query_type == QueryType.SHORTEST_PATH
            assert result.params["from_ref"] == "station"

    def test_system_prompt_contains_all_types(self):
        assert "proximity" in SYSTEM_PROMPT
        assert "shortest_path" in SYSTEM_PROMPT
        assert "nearest" in SYSTEM_PROMPT
        assert "filter_by" in SYSTEM_PROMPT
        assert "route_details" in SYSTEM_PROMPT
        assert "unknown" in SYSTEM_PROMPT
