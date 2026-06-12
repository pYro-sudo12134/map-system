import pytest
import json
from unittest.mock import AsyncMock, MagicMock, patch

from rag_agent.models import RagRequest, UserLocation, QueryParams, QueryType


@pytest.fixture
def sample_rag_request():
    return RagRequest(
        request_id="test-123",
        text="show routes near city center within 5 km",
        user_id="user-456",
        language="en",
        user_location=UserLocation(lat=55.7558, lon=37.6173)
    )


@pytest.fixture
def sample_rag_request_ru():
    return RagRequest(
        request_id="test-456",
        text="покажи маршруты рядом с центром, до 3 км",
        user_id="user-789",
        language="ru"
    )


@pytest.fixture
def sample_parsed_query():
    return QueryParams(
        query_type=QueryType.PROXIMITY,
        params={"location_ref": "city center", "radius_km": 5}
    )


@pytest.fixture
def sample_opensearch_doc():
    return {
        "text": "show routes near city center within 5 km",
        "parsed_query": {
            "query_type": "proximity",
            "params": {"location_ref": "city center", "radius_km": 5}
        },
        "score": 0.92,
        "language": "en"
    }


@pytest.fixture
def mock_llm_response():
    return {
        "query_type": "proximity",
        "params": {"location_ref": "city center", "radius_km": 5}
    }


@pytest.fixture
def sqs_message():
    return {
        'Body': json.dumps({
            'request_id': 'test-123',
            'text': 'show routes near city center',
            'user_id': 'user-456',
            'language': 'en',
            'user_location': {'lat': 55.7558, 'lon': 37.6173}
        }),
        'ReceiptHandle': 'handle-123',
        'Attributes': {'ApproximateReceiveCount': '1'}
    }


@pytest.fixture
def mock_os_client():
    with patch('rag_agent.clients.opensearch_client.os_client') as mock:
        yield mock


@pytest.fixture
def mock_sqs_client():
    with patch('rag_agent.clients.sqs_client.sqs_client') as mock:
        yield mock


@pytest.fixture
def mock_llm_client():
    with patch('rag_agent.clients.llm_client.llm_client') as mock:
        mock.process = AsyncMock()
        yield mock
