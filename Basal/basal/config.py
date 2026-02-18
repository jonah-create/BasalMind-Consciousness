"""
Basal Configuration - Environment-driven configuration management.
"""

import os
from dotenv import load_dotenv

load_dotenv()


class BasalConfig:
    """Central configuration for Basal orchestrator."""

    # Service identity
    SERVICE_NAME = "Basal"
    VERSION = "1.0.0"
    HOST = os.getenv("BASAL_HOST", "0.0.0.0")
    PORT = int(os.getenv("BASAL_PORT", 5004))

    # Redis
    REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
    REDIS_PORT = int(os.getenv("REDIS_PORT", 6390))
    REDIS_PASSWORD = os.getenv("REDIS_PASSWORD", "_t9iV2e(p9voC34HpkvxirRoy%8cSYcf")
    REDIS_DB = int(os.getenv("REDIS_DB", 0))
    REDIS_SESSION_TTL = int(os.getenv("REDIS_SESSION_TTL", 3600))  # 1 hour

    # NATS
    NATS_URL = os.getenv("NATS_URL", "nats://localhost:4222")
    NATS_STREAM = os.getenv("NATS_STREAM", "BASALMIND_EVENTS")
    NATS_CONSUMER_NAME = "basal-orchestrator"
    NATS_DURABLE_NAME = "basal-durable"

    # TimescaleDB (read-only)
    TIMESCALE_HOST = os.getenv("TIMESCALE_HOST", "localhost")
    TIMESCALE_PORT = int(os.getenv("TIMESCALE_PORT", 5432))
    TIMESCALE_DB = os.getenv("TIMESCALE_DB", "basalmind_events")
    TIMESCALE_USER = os.getenv("TIMESCALE_USER", "basalmind")
    TIMESCALE_PASSWORD = os.getenv("TIMESCALE_PASSWORD", "basalmind_secure_2024")

    # PostgreSQL + pgvector (Interpreter semantic store, read-only)
    POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
    POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", 5432))
    POSTGRES_DB = os.getenv("POSTGRES_DB", "basalmind_interpreter_postgresql")
    POSTGRES_USER = os.getenv("POSTGRES_USER", "basalmind")
    POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "basalmind_secure_2024")

    # Neo4j
    NEO4J_URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
    NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
    NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "shadowcaster_secure_2024")
    NEO4J_DATABASE = os.getenv("NEO4J_DATABASE", "neo4j")

    # OpenAI
    OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
    EMBEDDING_MODEL = os.getenv("EMBEDDING_MODEL", "text-embedding-ada-002")
    LLM_MODEL = os.getenv("LLM_MODEL", "gpt-4o-mini")

    # Langfuse
    LANGFUSE_HOST = os.getenv("LANGFUSE_HOST", "http://104.236.99.69:3000")
    LANGFUSE_PUBLIC_KEY = os.getenv("LANGFUSE_PUBLIC_KEY", "pk-lf-27c16cd0-3677-4d01-821e-aff1b414403a")
    LANGFUSE_SECRET_KEY = os.getenv("LANGFUSE_SECRET_KEY", "sk-lf-74bde768-3e31-4a0e-8af0-e03bba3cdf2d")

    # Entity endpoints
    CARTOGRAPHER_URL = os.getenv("CARTOGRAPHER_URL", "http://localhost:5005")
    SENTINEL_URL = os.getenv("SENTINEL_URL", "http://localhost:5006")
    ASSEMBLER_URL = os.getenv("ASSEMBLER_URL", "http://localhost:5007")
    PHILOSOPHER_URL = os.getenv("PHILOSOPHER_URL", "http://localhost:5008")

    # MCP Server
    MCP_SERVER_URL = os.getenv("MCP_SERVER_URL", "http://localhost:8000")

    # Processing
    LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
    ENTITY_TIMEOUT_SECONDS = float(os.getenv("ENTITY_TIMEOUT_SECONDS", 10.0))
    MAX_CONCURRENT_REQUESTS = int(os.getenv("MAX_CONCURRENT_REQUESTS", 10))


config = BasalConfig()
