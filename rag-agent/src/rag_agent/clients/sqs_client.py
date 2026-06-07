import logging
import json
import boto3
from typing import Optional, Dict, Any
from botocore.config import Config
from botocore.exceptions import ClientError
from ..config import settings
from ..models import RagRequest, UserLocation

logger = logging.getLogger(__name__)

class SQSClient:
    def __init__(self):
        self.sqs = boto3.client(
            'sqs',
            endpoint_url=settings.aws_endpoint_url,
            region_name=settings.aws_region,
            aws_access_key_id=settings.aws_access_key_id,
            aws_secret_access_key=settings.aws_secret_access_key,
            config=Config(
                retries={'max_attempts': 3},
                connect_timeout=60,
                read_timeout=120
            )
        )
        self.queue_rag_url = settings.queue_rag_url
        self.queue_go_url = settings.queue_go_url
        self.dlq_url = settings.dlq_url

    def receive_messages(self, max_messages: int = 10, wait_time: int = 20) -> list:
        try:
            response = self.sqs.receive_message(
                QueueUrl=self.queue_rag_url,
                MaxNumberOfMessages=max_messages,
                WaitTimeSeconds=wait_time,
                VisibilityTimeout=settings.visibility_timeout,
                AttributeNames=['ApproximateReceiveCount']
            )
            return response.get('Messages', [])
        except ClientError as e:
            logger.error(f"Failed to receive messages: {e}")
            return []

    def delete_message(self, receipt_handle: str) -> bool:
        try:
            response = self.sqs.delete_message(
                QueueUrl=self.queue_rag_url,
                ReceiptHandle=receipt_handle
            )
            logger.info(f"Delete successful: {response}")
            return True
        except ClientError as e:
            logger.error(f"Delete failed: {e.response['Error']['Code']} - {e.response['Error']['Message']}")
            return False

    def send_to_dlq(self, message_body: str, reason: str):
        try:
            self.sqs.send_message(
                QueueUrl=self.dlq_url,
                MessageBody=message_body,
                MessageAttributes={
                    'ErrorReason': {
                        'DataType': 'String',
                        'StringValue': reason
                    }
                }
            )
            logger.warning(f"Sent to DLQ: {reason}")
        except ClientError as e:
            logger.error(f"Failed to send to DLQ: {e}")

    def send_result(self, request_id: str, parsed_query: Dict[str, Any], user_location: Optional[UserLocation] = None):
        message = {
            'request_id': request_id,
            'parsed_query': parsed_query
        }
        if user_location:
            message['user_location'] = user_location.dict()

        try:
            response = self.sqs.send_message(
                QueueUrl=self.queue_go_url,
                MessageBody=json.dumps(message)
            )
            logger.info(f"Result sent to SQS_GO: {request_id}")
            return response.get('MessageId')
        except ClientError as e:
            logger.error(f"Failed to send to SQS_GO: {e}")
            raise

    def parse_message(self, message: Dict) -> Optional[RagRequest]:
        try:
            body = json.loads(message['Body'])

            user_location = None
            if body.get('user_location'):
                user_location = UserLocation(
                    lat=body['user_location'].get('lat'),
                    lon=body['user_location'].get('lon')
                )

            return RagRequest(
                request_id=body.get('request_id'),
                text=body.get('text'),
                user_id=body.get('user_id'),
                language=body.get('language', 'ru'),
                user_location=user_location
            )
        except (json.JSONDecodeError, KeyError) as e:
            logger.error(f"Failed to parse message: {e}")
            return None

    def get_retry_count(self, message: Dict) -> int:
        attrs = message.get('Attributes', {})
        receive_count = attrs.get('ApproximateReceiveCount', '0')
        return int(receive_count)

sqs_client = SQSClient()