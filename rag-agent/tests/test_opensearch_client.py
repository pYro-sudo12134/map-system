import pytest
from unittest.mock import MagicMock, patch
from rag_agent.clients.opensearch_client import OpenSearchClient
from rag_agent.models import StoredQuery


class TestOpenSearchClient:

    @pytest.fixture
    def os_client(self):
        with patch('opensearchpy.OpenSearch') as mock_opensearch:
            client = OpenSearchClient()
            mock_instance = MagicMock()
            mock_opensearch.return_value = mock_instance
            mock_instance.indices.exists.return_value = True
            client.client = mock_instance
            yield client

    def test_connect_creates_index_if_not_exists(self):
        with patch('opensearchpy.OpenSearch') as mock_opensearch:
            client = OpenSearchClient()
            mock_instance = MagicMock()
            mock_opensearch.return_value = mock_instance
            mock_instance.indices.exists.return_value = False

            client.client = mock_instance

            with patch('rag_agent.clients.opensearch_client.settings.opensearch_url', 'http://localhost:9200'):
                with patch.object(client, '_create_index_if_not_exists') as mock_create:
                    client.connect()
                    mock_create.assert_called_once()

    def test_find_similar_no_results(self, os_client):
        os_client.client.search.return_value = {"hits": {"hits": []}}

        with patch('rag_agent.clients.opensearch_client.embedding_generator.encode') as mock_encode:
            mock_encode.return_value = [0.1] * 384

            result = os_client.find_similar("test text", "en")

            assert result is None

    def test_find_similar_below_threshold(self, os_client):
        os_client.client.search.return_value = {
            "hits": {
                "hits": [{
                    "_score": 0.5,
                    "_source": {"text": "test", "parsed_query": {}, "language": "en"}
                }]
            }
        }

        with patch('rag_agent.clients.opensearch_client.embedding_generator.encode') as mock_encode:
            mock_encode.return_value = [0.1] * 384

            result = os_client.find_similar("test text", "en")

            assert result is None

    def test_find_similar_above_threshold(self, os_client):
        os_client.client.search.return_value = {
            "hits": {
                "hits": [{
                    "_score": 0.95,
                    "_source": {"text": "test", "parsed_query": {"type": "test"}, "language": "en"}
                }]
            }
        }

        with patch('rag_agent.clients.opensearch_client.embedding_generator.encode') as mock_encode:
            mock_encode.return_value = [0.1] * 384

            result = os_client.find_similar("test text", "en")

            assert result is not None
            assert result["score"] == 0.95

    def test_save_query_success(self, os_client):
        stored = StoredQuery(
            request_id="test-123",
            text="test query",
            parsed_query={"type": "test"},
            language="en"
        )

        with patch('rag_agent.clients.opensearch_client.embedding_generator.encode') as mock_encode:
            mock_encode.return_value = [0.1] * 384
            os_client.client.index.return_value = {"_id": "test-123"}

            result = os_client.save_query(stored)

            assert result is True
            os_client.client.index.assert_called_once()

    def test_save_query_failure(self, os_client):
        stored = StoredQuery(
            request_id="test-123",
            text="test query",
            parsed_query={"type": "test"},
            language="en"
        )

        with patch('rag_agent.clients.opensearch_client.embedding_generator.encode') as mock_encode:
            mock_encode.return_value = [0.1] * 384
            os_client.client.index.side_effect = Exception("Connection error")

            result = os_client.save_query(stored)

            assert result is False
