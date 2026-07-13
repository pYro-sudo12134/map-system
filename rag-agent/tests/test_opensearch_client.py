import pytest
from unittest.mock import MagicMock, patch
from rag_agent.clients.opensearch_client import OpenSearchClient
from rag_agent.models import StoredQuery


class TestOpenSearchClient:

    @pytest.fixture
    def os_client(self):
        with patch('rag_agent.clients.opensearch_client.OpenSearch') as mock_opensearch:
            client = OpenSearchClient()
            mock_instance = MagicMock()
            mock_opensearch.return_value = mock_instance
            mock_instance.indices.exists.return_value = True
            client.client = mock_instance
            yield client

    def test_connect_creates_index_if_not_exists(self):
        with patch('rag_agent.clients.opensearch_client.OpenSearch') as mock_opensearch:
            client = OpenSearchClient()
            mock_instance = MagicMock()
            mock_opensearch.return_value = mock_instance
            mock_instance.indices.exists.return_value = False

            client.client = mock_instance

            with patch('rag_agent.clients.opensearch_client.settings.opensearch_url', 'http://localhost:9200'):
                with patch.object(client, '_create_index_if_not_exists') as mock_create:
                    client.connect()
                    mock_create.assert_called_once()

    def test_connect_without_auth(self):
        with patch('rag_agent.clients.opensearch_client.OpenSearch') as mock_opensearch:
            with patch('rag_agent.clients.opensearch_client.settings.opensearch_password', ''):
                with patch('rag_agent.clients.opensearch_client.settings.opensearch_user', 'admin'):
                    with patch('rag_agent.clients.opensearch_client.settings.opensearch_url', 'http://localhost:9200'):
                        with patch('rag_agent.clients.opensearch_client.settings.opensearch_use_ssl', False):
                            with patch('rag_agent.clients.opensearch_client.settings.opensearch_verify_certs', False):
                                client = OpenSearchClient()
                                mock_instance = MagicMock()
                                mock_opensearch.return_value = mock_instance
                                mock_instance.indices.exists.return_value = True

                                with patch.object(client, '_create_index_if_not_exists'):
                                    client.connect()
                                    mock_opensearch.assert_called_once()
                                    call_kwargs = mock_opensearch.call_args[1]
                                    assert 'http_auth' not in call_kwargs
                                    assert call_kwargs['hosts'] == ['http://localhost:9200']
                                    assert call_kwargs['use_ssl'] is False
                                    assert call_kwargs['verify_certs'] is False

    def test_connect_with_auth(self):
        with patch('rag_agent.clients.opensearch_client.OpenSearch') as mock_opensearch:
            with patch('rag_agent.clients.opensearch_client.settings.opensearch_password', 'test-password'):
                with patch('rag_agent.clients.opensearch_client.settings.opensearch_user', 'admin'):
                    with patch('rag_agent.clients.opensearch_client.settings.opensearch_url', 'http://localhost:9200'):
                        with patch('rag_agent.clients.opensearch_client.settings.opensearch_use_ssl', False):
                            with patch('rag_agent.clients.opensearch_client.settings.opensearch_verify_certs', False):
                                client = OpenSearchClient()
                                mock_instance = MagicMock()
                                mock_opensearch.return_value = mock_instance
                                mock_instance.indices.exists.return_value = True

                                with patch.object(client, '_create_index_if_not_exists'):
                                    client.connect()
                                    mock_opensearch.assert_called_once()
                                    call_kwargs = mock_opensearch.call_args[1]
                                    assert call_kwargs['http_auth'] == ('admin', 'test-password')
                                    assert call_kwargs['hosts'] == ['http://localhost:9200']
                                    assert call_kwargs['use_ssl'] is False
                                    assert call_kwargs['verify_certs'] is False

    def test_connect_with_ssl_disabled(self):
        with patch('rag_agent.clients.opensearch_client.OpenSearch') as mock_opensearch:
            with patch('rag_agent.clients.opensearch_client.settings.opensearch_password', 'test-password'):
                with patch('rag_agent.clients.opensearch_client.settings.opensearch_user', 'admin'):
                    with patch('rag_agent.clients.opensearch_client.settings.opensearch_url', 'http://localhost:9200'):
                        with patch('rag_agent.clients.opensearch_client.settings.opensearch_use_ssl', False):
                            with patch('rag_agent.clients.opensearch_client.settings.opensearch_verify_certs', False):
                                client = OpenSearchClient()
                                mock_instance = MagicMock()
                                mock_opensearch.return_value = mock_instance
                                mock_instance.indices.exists.return_value = True

                                with patch.object(client, '_create_index_if_not_exists'):
                                    client.connect()
                                    mock_opensearch.assert_called_once()
                                    call_kwargs = mock_opensearch.call_args[1]
                                    assert call_kwargs['use_ssl'] is False
                                    assert call_kwargs['verify_certs'] is False

    def test_connect_with_ssl_enabled(self):
        with patch('rag_agent.clients.opensearch_client.OpenSearch') as mock_opensearch:
            with patch('rag_agent.clients.opensearch_client.settings.opensearch_password', 'test-password'):
                with patch('rag_agent.clients.opensearch_client.settings.opensearch_user', 'admin'):
                    with patch('rag_agent.clients.opensearch_client.settings.opensearch_url', 'https://localhost:9200'):
                        with patch('rag_agent.clients.opensearch_client.settings.opensearch_use_ssl', True):
                            with patch('rag_agent.clients.opensearch_client.settings.opensearch_verify_certs', True):
                                client = OpenSearchClient()
                                mock_instance = MagicMock()
                                mock_opensearch.return_value = mock_instance
                                mock_instance.indices.exists.return_value = True

                                with patch.object(client, '_create_index_if_not_exists'):
                                    client.connect()
                                    mock_opensearch.assert_called_once()
                                    call_kwargs = mock_opensearch.call_args[1]
                                    assert call_kwargs['use_ssl'] is True
                                    assert call_kwargs['verify_certs'] is True
                                    assert call_kwargs['hosts'] == ['https://localhost:9200']

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