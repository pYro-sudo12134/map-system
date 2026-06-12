import logging
import asyncio
import signal
import sys

from .config import settings
from .models import StoredQuery
from .embeddings import embedding_generator
from .clients import os_client, sqs_client
from .services import RAGService

logger = logging.getLogger(__name__)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)

async def process_with_llm_and_save(request):
    parsed = await RAGService.process(request)

    stored = StoredQuery(
        request_id=request.request_id,
        text=request.text,
        parsed_query=parsed.model_dump(),
        success=True,
        language=request.language
    )
    os_client.save_query(stored)

    return parsed

async def process_message(message: dict) -> bool:
    request = sqs_client.parse_message(message)
    if not request:
        sqs_client.delete_message(message['ReceiptHandle'])
        return False

    logger.info(f"Processing {request.request_id}: {request.text[:50]}...")

    retry_count = sqs_client.get_retry_count(message)
    if retry_count > settings.max_retries:
        logger.error(f"Max retries exceeded ({retry_count}), sending to DLQ")
        sqs_client.send_to_dlq(message['Body'], f"Max retries exceeded: {retry_count}")
        sqs_client.delete_message(message['ReceiptHandle'])
        return False

    try:
        parsed_query = await process_with_llm_and_save(request)

        sqs_client.send_result(
            request_id=request.request_id,
            parsed_query=parsed_query.model_dump(),
            user_location=request.user_location
        )

        logger.info(f"Attempting to delete with handle: {message['ReceiptHandle'][:100]}...")
        delete_success = sqs_client.delete_message(message['ReceiptHandle'])
        if delete_success:
            logger.info(f"Message {request.request_id} deleted from queue")
        else:
            logger.error(f"Failed to delete message {request.request_id}")

        return True

    except Exception as e:
        logger.error(f"Error processing {request.request_id}: {e}", exc_info=True)
        return False


class RAGAgent:
    def __init__(self):
        self.running = True
        self.setup_signal_handlers()

    def setup_signal_handlers(self):
        signal.signal(signal.SIGINT, self.shutdown)
        signal.signal(signal.SIGTERM, self.shutdown)

    def shutdown(self, signum, frame):
        logger.info("Received shutdown signal, stopping...")
        self.running = False

    async def run(self):
        logger.info("RAG Agent started")

        embedding_generator.load()
        os_client.connect()

        logger.info(f"Waiting for messages from {settings.queue_rag_url}")

        while self.running:
            try:
                messages = sqs_client.receive_messages(max_messages=5)

                for message in messages:
                    if not self.running:
                        break
                    await process_message(message)

                if not messages:
                    await asyncio.sleep(0.5)

            except Exception as e:
                logger.error(f"Critical error in main loop: {e}", exc_info=True)
                await asyncio.sleep(5)

        logger.info("RAG Agent stopped")

async def main():
    agent = RAGAgent()
    await agent.run()

if __name__ == "__main__":
    asyncio.run(main())