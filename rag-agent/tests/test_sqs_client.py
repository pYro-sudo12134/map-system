import pytest
import json
from unittest.mock import MagicMock, patch
from botocore.exceptions import ClientError
from rag_agent.clients.sqs_client import SQSClient


class TestSQSClient:

    @pytest.fixture
    def sqs_client(self):
        with patch('boto3.client') as mock_boto:
            mock_sqs = MagicMock()
            mock_boto.return_value = mock_sqs
            client = SQSClient()
            client.sqs = mock_sqs
            yield client

    def test_receive_messages(self, sqs_client):
        mock_response = {
            'Messages': [
                {'Body': 'test1', 'ReceiptHandle': 'handle1'},
                {'Body': 'test2', 'ReceiptHandle': 'handle2'}
            ]
        }
        sqs_client.sqs.receive_message.return_value = mock_response

        messages = sqs_client.receive_messages(max_messages=5)

        assert len(messages) == 2
        sqs_client.sqs.receive_message.assert_called_once()

    def test_receive_messages_empty(self, sqs_client):
        sqs_client.sqs.receive_message.return_value = {}

        messages = sqs_client.receive_messages()

        assert messages == []

    def test_delete_message_success(self, sqs_client):
        sqs_client.sqs.delete_message.return_value = {'ResponseMetadata': {'HTTPStatusCode': 200}}

        result = sqs_client.delete_message('handle-123')

        assert result is True

    def test_delete_message_failure(self, sqs_client):
        error_response = {'Error': {'Code': 'InvalidParameterValue', 'Message': 'Invalid handle'}}
        sqs_client.sqs.delete_message.side_effect = ClientError(error_response, 'DeleteMessage')

        result = sqs_client.delete_message('invalid-handle')

        assert result is False

    def test_send_to_dlq(self, sqs_client):
        sqs_client.send_to_dlq('test message', 'test error reason')

        sqs_client.sqs.send_message.assert_called_once()

    def test_send_result_success(self, sqs_client):
        sqs_client.sqs.send_message.return_value = {'MessageId': 'msg-123'}

        result = sqs_client.send_result(
            request_id='test-123',
            parsed_query={'type': 'test'}
        )

        assert result == 'msg-123'

    def test_parse_message_valid(self, sqs_client):
        message = {
            'Body': json.dumps({
                'request_id': 'test-123',
                'text': 'test query',
                'user_id': 'user-456',
                'language': 'en',
                'user_location': {'lat': 55.7558, 'lon': 37.6173}
            })
        }

        request = sqs_client.parse_message(message)

        assert request is not None
        assert request.request_id == 'test-123'
        assert request.text == 'test query'
        assert request.user_location.lat == 55.7558

    def test_parse_message_invalid_json(self, sqs_client):
        message = {'Body': 'invalid json{'}

        request = sqs_client.parse_message(message)

        assert request is None

    def test_get_retry_count(self, sqs_client):
        message = {'Attributes': {'ApproximateReceiveCount': '3'}}

        retry_count = sqs_client.get_retry_count(message)

        assert retry_count == 3

    def test_get_retry_count_default(self, sqs_client):
        message = {}

        retry_count = sqs_client.get_retry_count(message)

        assert retry_count == 0
