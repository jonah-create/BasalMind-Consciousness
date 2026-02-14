"""
BasalMind Interpreter Module - Phase 1

Transforms raw time-series events into structured semantic knowledge.

Architecture:
- Reads from TimescaleDB (Observer's raw data)
- Writes to PostgreSQL + pgvector (structured semantic records)
- Writes to Neo4j (semantic graph relationships)
- Generates embeddings for similarity search

Handles ALL event sources: Slack, nginx, Cloudflare, enterprise apps, etc.

Based on Claude Opus design specification.
"""

__version__ = "1.0.0-phase1"
__author__ = "BasalMind Team"

from .engine import InterpreterEngine
from .timescale_reader import TimescaleReader
from .intent_extractor import IntentExtractor
from .postgres_writer import PostgresWriter
from .neo4j_writer import Neo4jWriter
from .embedding_generator import EmbeddingGenerator

__all__ = [
    "InterpreterEngine",
    "TimescaleReader",
    "IntentExtractor",
    "PostgresWriter",
    "Neo4jWriter",
    "EmbeddingGenerator",
]
