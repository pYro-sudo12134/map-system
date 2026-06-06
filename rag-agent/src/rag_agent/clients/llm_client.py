import json
import logging
import ollama

from ..config import settings
from ..models import QueryParams, QueryType

logger = logging.getLogger(__name__)

SYSTEM_PROMPT = """You are a routing system. Convert user query to JSON.

Output ONLY JSON, no explanations, no markdown.

IMPORTANT: Answer in the SAME LANGUAGE as the user's query.

Query types:
- proximity: "routes near [place] within N km"
- shortest_path: "how to get from [A] to [B]"
- nearest: "find nearest [POI]"
- filter_by: "show roads with conditions"
- route_details: "info about [route name]"
- unknown: if unclear

Examples:

Query (en): "show routes near city center, up to 3 km"
Output: {"query_type": "proximity", "params": {"location_ref": "city center", "radius_km": 3}}

Query (ru): "покажи маршруты рядом с центром, до 3 км"
Output: {"query_type": "proximity", "params": {"location_ref": "центр", "radius_km": 3}}

Query (en): "how to get from station to airport"
Output: {"query_type": "shortest_path", "params": {"from_ref": "station", "to_ref": "airport"}}

Query (ru): "как доехать от вокзала до аэропорта"
Output: {"query_type": "shortest_path", "params": {"from_ref": "вокзал", "to_ref": "аэропорт"}}

Query (en): "find nearest gas station"
Output: {"query_type": "nearest", "params": {"poi_type": "gas station", "location_ref": "my location"}}

Query (ru): "найди ближайшую заправку"
Output: {"query_type": "nearest", "params": {"poi_type": "заправка", "location_ref": "моё местоположение"}}

Query: "what is the weather today"
Output: {"query_type": "unknown", "params": {"original_text": "what is the weather today"}}"""

class LLMClient:
    def __init__(self):
        self.model = settings.ollama_model
        self.temperature = settings.ollama_temperature
        ollama.Client(host=settings.ollama_url)

    def _call_ollama(self, user_query: str) -> str:
        try:
            client = ollama.Client(host=settings.ollama_url)
            response = client.generate(
                model=self.model,
                prompt=user_query,
                system=SYSTEM_PROMPT,
                options={
                    "temperature": self.temperature,
                    "num_predict": settings.ollama_max_tokens,
                    "top_p": 0.9,
                }
            )
            return response.get("response", "").strip()
        except Exception as e:
            logger.error(f"Ollama error: {e}")
            raise

    def _parse_response(self, raw_response: str) -> QueryParams:
        raw_response = raw_response.strip()
        if raw_response.startswith("```json"):
            raw_response = raw_response[7:]
        if raw_response.startswith("```"):
            raw_response = raw_response[3:]
        if raw_response.endswith("```"):
            raw_response = raw_response[:-3]
        raw_response = raw_response.strip()

        try:
            data = json.loads(raw_response)
            return QueryParams(
                query_type=QueryType(data.get("query_type", "unknown")),
                params=data.get("params", {})
            )
        except json.JSONDecodeError as e:
            logger.error(f"JSON parse error: {e}\nResponse: {raw_response}")
            return QueryParams(
                query_type=QueryType.UNKNOWN,
                params={"original_text": raw_response[:200]}
            )

    async def process(self, text: str) -> QueryParams:
        logger.info(f"LLM request: {text[:100]}...")
        raw_response = self._call_ollama(text)
        parsed = self._parse_response(raw_response)
        logger.info(f"LLM result: {parsed.query_type} - {parsed.params}")
        return parsed

llm_client = LLMClient()