"""
Embedding Generator - Phase 1

Generates semantic embeddings for similarity search.
Phase 1: Single embedding per thread using OpenAI.

Handles threads from ALL sources: Slack, nginx logs, Cloudflare events, etc.

Based on Claude Opus design specification.
"""

import logging
import asyncio
from typing import List, Dict, Any, Optional
from datetime import datetime
import openai
import asyncpg

logger = logging.getLogger(__name__)


class EmbeddingGenerator:
    """
    Generate semantic embeddings for knowledge entities.
    
    Phase 1: Single semantic embedding per thread
    Phase 2: Multi-embedding strategy (semantic, intent, topical, temporal)
    
    Responsibilities:
    - Generate embeddings for threads/conversations
    - Store embeddings in pgvector with lineage
    - Support multi-source content (Slack, logs, etc.)
    """
    
    def __init__(
        self,
        openai_api_key: str,
        model_name: str = "text-embedding-ada-002",
        postgres_pool: Optional[asyncpg.Pool] = None
    ):
        """
        Initialize embedding generator.
        
        Args:
            openai_api_key: OpenAI API key
            model_name: Embedding model to use
            postgres_pool: PostgreSQL connection pool (for storing embeddings)
        """
        self.model_name = model_name
        self.postgres_pool = postgres_pool
        
        # Initialize OpenAI client
        openai.api_key = openai_api_key
        
        # Embedding configuration
        self.max_tokens = 8000  # ada-002 limit
        self.embedding_dim = 1536  # ada-002 dimension
        
    async def generate_thread_embedding(
        self,
        thread_id: str,
        events: List[Dict[str, Any]],
        interpretation_id: Optional[str] = None
    ) -> Optional[List[float]]:
        """
        Generate semantic embedding for a thread/conversation.
        
        Works for:
        - Slack conversation threads
        - nginx request sequences
        - Cloudflare traffic patterns
        - Any event sequence
        
        Args:
            thread_id: Unique thread identifier
            events: List of events in this thread
            interpretation_id: Lineage to interpretation record
            
        Returns:
            Embedding vector (1536 dimensions) or None if failed
        """
        # Aggregate text from all events
        text_content = self._aggregate_thread_text(events)
        
        if not text_content:
            logger.warning(f"No text content for thread {thread_id}, skipping embedding")
            return None
            
        # Truncate to token limit (rough estimate: 1 token ≈ 4 chars)
        max_chars = self.max_tokens * 4
        if len(text_content) > max_chars:
            text_content = text_content[:max_chars]
            logger.debug(f"Truncated thread {thread_id} text to {max_chars} chars")
            
        try:
            # Generate embedding via OpenAI
            response = await asyncio.to_thread(
                openai.Embedding.create,
                input=text_content,
                model=self.model_name
            )
            
            embedding = response['data'][0]['embedding']
            
            logger.info(
                f"🧠 Generated embedding for thread {thread_id} "
                f"({len(text_content)} chars → {len(embedding)} dims)"
            )
            
            # Store in database if pool provided
            if self.postgres_pool:
                await self._store_embedding(
                    entity_type="thread",
                    entity_id=thread_id,
                    embedding_type="semantic",
                    embedding=embedding,
                    text_content=text_content,
                    source_event_ids=[e["event_id"] for e in events],
                    interpretation_id=interpretation_id
                )
                
            return embedding
            
        except Exception as e:
            logger.error(f"Failed to generate embedding for thread {thread_id}: {e}")
            return None
            
    def _aggregate_thread_text(self, events: List[Dict[str, Any]]) -> str:
        """
        Aggregate text from events into a single string for embedding.
        
        Handles:
        - Slack messages (text field)
        - nginx logs (request path, user agent, etc.)
        - Cloudflare events (metadata)
        - Any structured event
        """
        text_parts = []
        
        for event in events:
            source_system = event.get("source_system", "unknown")
            
            # Slack: use message text
            if source_system == "slack":
                text = event.get("text", "")
                if text:
                    text_parts.append(text)
                    
            # nginx/Cloudflare: aggregate meaningful fields
            elif source_system in ("nginx", "cloudflare"):
                metadata = event.get("metadata", {})
                if isinstance(metadata, dict):
                    # Extract meaningful fields
                    parts = []
                    if "path" in metadata:
                        parts.append(f"Path: {metadata['path']}")
                    if "method" in metadata:
                        parts.append(f"Method: {metadata['method']}")
                    if "status" in metadata:
                        parts.append(f"Status: {metadata['status']}")
                    if "user_agent" in metadata:
                        parts.append(f"UA: {metadata['user_agent']}")
                    if parts:
                        text_parts.append(" | ".join(parts))
                        
            # Generic: try to find any text field
            else:
                text = event.get("text") or event.get("message") or event.get("description")
                if text:
                    text_parts.append(text)
                    
        # Join with newlines
        return "\n".join(text_parts)
        
    async def _store_embedding(
        self,
        entity_type: str,
        entity_id: str,
        embedding_type: str,
        embedding: List[float],
        text_content: str,
        source_event_ids: List[str],
        interpretation_id: Optional[str] = None
    ):
        """
        Store embedding in pgvector table.
        
        Args:
            entity_type: Type of entity (thread, channel_summary, decision, etc.)
            entity_id: Entity identifier
            embedding_type: Type of embedding (semantic, intent, topical, temporal)
            embedding: Vector embedding
            text_content: Original text that was embedded
            source_event_ids: Event IDs for lineage
            interpretation_id: Reference to interpretation record
        """
        if not self.postgres_pool:
            return
            
        query = """
            INSERT INTO embeddings (
                entity_type,
                entity_id,
                embedding_type,
                model_name,
                embedding,
                text_content,
                token_count,
                source_interpretation_id,
                source_event_ids
            ) VALUES ($1, $2, $3, $4, $5::vector, $6, $7, $8, $9)
            ON CONFLICT (entity_type, entity_id, embedding_type) DO UPDATE
            SET embedding = EXCLUDED.embedding,
                text_content = EXCLUDED.text_content,
                token_count = EXCLUDED.token_count,
                source_event_ids = EXCLUDED.source_event_ids,
                created_at = NOW()
        """
        
        # Estimate token count (rough: 1 token ≈ 4 chars)
        token_count = len(text_content) // 4
        
        async with self.postgres_pool.acquire() as conn:
            await conn.execute(
                query,
                entity_type,
                entity_id,
                embedding_type,
                self.model_name,
                embedding,
                text_content[:1000],  # Store first 1000 chars
                token_count,
                interpretation_id,
                source_event_ids
            )
            
        logger.debug(f"💾 Stored embedding: {entity_type}/{entity_id}/{embedding_type}")
        
    async def search_similar(
        self,
        query_embedding: List[float],
        entity_type: Optional[str] = None,
        limit: int = 10
    ) -> List[Dict[str, Any]]:
        """
        Search for similar entities using cosine similarity.
        
        Args:
            query_embedding: Query vector
            entity_type: Filter by entity type (optional)
            limit: Maximum results
            
        Returns:
            List of {entity_id, entity_type, similarity, text_content}
        """
        if not self.postgres_pool:
            raise RuntimeError("PostgreSQL pool not provided")
            
        type_filter = "AND entity_type = $2" if entity_type else ""
        params = [query_embedding]
        if entity_type:
            params.append(entity_type)
        params.append(limit)
        
        query = f"""
            SELECT 
                entity_id,
                entity_type,
                embedding_type,
                text_content,
                1 - (embedding <=> $1::vector) as similarity
            FROM embeddings
            {type_filter}
            ORDER BY embedding <=> $1::vector
            LIMIT ${len(params)}
        """
        
        async with self.postgres_pool.acquire() as conn:
            rows = await conn.fetch(query, *params)
            
        results = []
        for row in rows:
            results.append({
                "entity_id": row["entity_id"],
                "entity_type": row["entity_type"],
                "embedding_type": row["embedding_type"],
                "similarity": float(row["similarity"]),
                "text_content": row["text_content"]
            })
            
        logger.info(f"🔍 Found {len(results)} similar entities")
        return results


# Example usage
async def test_generator():
    """Test the EmbeddingGenerator."""
    import os
    from dotenv import load_dotenv
    
    load_dotenv()
    
    # NOTE: Requires valid OpenAI API key
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key or api_key == "sk-your-key-here":
        print("⚠️ No valid OpenAI API key found, skipping test")
        return
        
    generator = EmbeddingGenerator(
        openai_api_key=api_key,
        model_name="text-embedding-ada-002"
    )
    
    # Sample thread events
    events = [
        {
            "event_id": "evt-1",
            "source_system": "slack",
            "text": "How do we configure authentication in this system?"
        },
        {
            "event_id": "evt-2",
            "source_system": "slack",
            "text": "You need to set up OAuth 2.0 with the identity provider"
        },
        {
            "event_id": "evt-3",
            "source_system": "slack",
            "text": "Got it, I'll use the oauth2 library"
        }
    ]
    
    embedding = await generator.generate_thread_embedding(
        thread_id="thread_123",
        events=events
    )
    
    if embedding:
        print(f"✅ Generated embedding: {len(embedding)} dimensions")
        print(f"   First 5 values: {embedding[:5]}")


if __name__ == "__main__":
    asyncio.run(test_generator())
