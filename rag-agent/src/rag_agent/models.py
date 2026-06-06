from pydantic import BaseModel, Field
from typing import Optional, Dict, Any
from enum import Enum
from datetime import datetime


class QueryType(str, Enum):
    PROXIMITY = "proximity"
    SHORTEST_PATH = "shortest_path"
    NEAREST = "nearest"
    FILTER_BY = "filter_by"
    ROUTE_DETAILS = "route_details"
    UNKNOWN = "unknown"


class QueryParams(BaseModel):
    query_type: QueryType
    params: Dict[str, Any] = Field(default_factory=dict)


class RagRequest(BaseModel):
    request_id: str
    text: str
    user_id: Optional[str] = None
    language: str = "ru"


class RagResponse(BaseModel):
    request_id: str
    original_text: str
    parsed_query: QueryParams
    from_cache: bool = False
    confidence: float = 1.0
    timestamp: datetime = Field(default_factory=datetime.utcnow)


class StoredQuery(BaseModel):
    request_id: str
    text: str
    text_vector: Optional[list[float]] = None
    parsed_query: Dict[str, Any]
    language: str
    success: bool = True
    user_feedback: Optional[bool] = None
    created_at: datetime = Field(default_factory=datetime.utcnow)
