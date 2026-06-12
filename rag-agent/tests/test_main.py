import pytest
from unittest.mock import patch, AsyncMock, MagicMock
from rag_agent.main import process_message, process_with_llm_and_save


class TestMainFunctions:

    @pytest.mark.asyncio
    async def test_process_message_success(self, sqs_message, sample_rag_request, sample_parsed_query):
        with patch('rag_agent.main.sqs_client.parse_message', return_value=sample_rag_request):
            with patch('rag_agent.main.process_with_llm_and_save', new_callable=AsyncMock) as mock_process:
                mock_process.return_value = sample_parsed_query
                with patch('rag_agent.main.sqs_client.send_result') as mock_send:
                    with patch('rag_agent.main.sqs_client.delete_message', return_value=True):
                        result = await process_message(sqs_message)

                        assert result is True
                        mock_send.assert_called_once()
                        mock_process.assert_called_once()

    @pytest.mark.asyncio
    async def test_process_message_max_retries_exceeded(self, sqs_message):
        sqs_message['Attributes']['ApproximateReceiveCount'] = '5'

        with patch('rag_agent.main.sqs_client.parse_message', return_value=MagicMock()):
            with patch('rag_agent.main.settings.max_retries', 3):
                with patch('rag_agent.main.sqs_client.send_to_dlq') as mock_dlq:
                    with patch('rag_agent.main.sqs_client.delete_message', return_value=True):
                        result = await process_message(sqs_message)

                        assert result is False
                        mock_dlq.assert_called_once()

    @pytest.mark.asyncio
    async def test_process_message_parse_error(self, sqs_message):
        with patch('rag_agent.main.sqs_client.parse_message', return_value=None):
            with patch('rag_agent.main.sqs_client.delete_message', return_value=True):
                result = await process_message(sqs_message)

                assert result is False

    @pytest.mark.asyncio
    async def test_process_message_exception(self, sqs_message, sample_rag_request):
        with patch('rag_agent.main.sqs_client.parse_message', return_value=sample_rag_request):
            with patch('rag_agent.main.process_with_llm_and_save', new_callable=AsyncMock) as mock_process:
                mock_process.side_effect = Exception("Processing error")

                result = await process_message(sqs_message)

                assert result is False

    @pytest.mark.asyncio
    async def test_process_with_llm_and_save(self, sample_rag_request, sample_parsed_query):
        with patch('rag_agent.main.RAGService.process', new_callable=AsyncMock) as mock_rag:
            mock_rag.return_value = sample_parsed_query
            with patch('rag_agent.main.os_client.save_query') as mock_save:

                result = await process_with_llm_and_save(sample_rag_request)

                assert result == sample_parsed_query
                mock_save.assert_called_once()
                saved_query = mock_save.call_args[0][0]
                assert saved_query.request_id == sample_rag_request.request_id
                assert saved_query.text == sample_rag_request.text
                assert saved_query.success is True
