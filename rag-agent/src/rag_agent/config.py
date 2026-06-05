from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    aws_access_key_id: str = "test"
    aws_secret_access_key: str = "test"
    aws_region: str = "us-east-1"
    aws_endpoint_url: str = "http://localstack:4566"

    queue_rag_url: str = "http://localstack:4566/000000000000/SQS_RAG"
    queue_go_url: str = "http://localstack:4566/000000000000/SQS_GO"
    dlq_url: str = "http://localstack:4566/000000000000/DLQ"

    ollama_url: str = "http://ollama:11434"
    ollama_model: str = "llama3.2:3b"
    ollama_temperature: float = 0.1
    ollama_max_tokens: int = 500

    opensearch_url: str = "http://opensearch:9200"
    opensearch_index: str = "rag_queries"

    similarity_threshold: float = 0.85
    knn_size: int = 5

    max_retries: int = 3
    visibility_timeout: int = 60


settings = Settings()
