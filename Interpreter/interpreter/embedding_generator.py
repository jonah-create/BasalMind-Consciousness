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

        # Initialize OpenAI client (openai>=1.0.0 style)
        self.client = openai.OpenAI(api_key=openai_api_key)

        # Embedding configuration
        self.max_tokens = 8000  # ada-002 limit
        self.embedding_dim = 1536  # ada-002 dimension
        
    async def _call_openai(self, text: str) -> Optional[List[float]]:
        """Call OpenAI embeddings API and return the vector."""
        try:
            response = await asyncio.to_thread(
                self.client.embeddings.create,
                input=text,
                model=self.model_name
            )
            return response.data[0].embedding
        except Exception as e:
            logger.error(f"OpenAI embedding call failed: {e}")
            return None

    async def generate_turn_embedding(
        self,
        event_id: str,
        text: str,
        thread_id: str,
        interpretation_id: Optional[str] = None
    ) -> Optional[List[float]]:
        """
        Generate and store an embedding for a single turn (message).

        Args:
            event_id: The event UUID for this turn
            text: The message text
            thread_id: Parent thread this turn belongs to
            interpretation_id: Lineage to interpretation record

        Returns:
            Embedding vector or None if failed
        """
        if not text or not text.strip():
            return None

        max_chars = self.max_tokens * 4
        text = text[:max_chars]

        embedding = await self._call_openai(text)
        if embedding is None:
            return None

        logger.info(f"🧠 Turn embedding: event={event_id[:8]}... ({len(text)} chars → {len(embedding)} dims)")

        if self.postgres_pool:
            await self._store_embedding(
                entity_type="turn",
                entity_id=event_id,
                embedding_type="semantic",
                embedding=embedding,
                text_content=text,
                source_event_ids=[event_id],
                interpretation_id=interpretation_id
            )

        return embedding

    async def generate_thread_embedding(
        self,
        thread_id: str,
        events: List[Dict[str, Any]],
        interpretation_id: Optional[str] = None,
        # Optional context from thread_analyzer for richer history snapshots
        channel_id: Optional[str] = None,
        phase: Optional[str] = None,
        pattern: Optional[str] = None,
        participant_count: Optional[int] = None,
        message_count: Optional[int] = None,
    ) -> Optional[List[float]]:
        """
        Generate and store a composite embedding for a thread.

        Concatenates all turn texts in the thread (chronological order)
        and embeds the result. Always overwrites the previous thread embedding
        so it reflects the current full conversation context.

        Also appends an immutable snapshot to thread_embedding_history so the
        full semantic trajectory of the thread is preserved for trend analysis.

        Args:
            thread_id: Unique thread identifier
            events: All events in this thread (current batch)
            interpretation_id: Lineage to interpretation record
            channel_id: Slack channel (for history index)
            phase: Thread lifecycle phase at time of snapshot
            pattern: Conversation pattern at time of snapshot
            participant_count: Unique speakers so far
            message_count: Total messages when snapshot was taken

        Returns:
            Embedding vector or None if failed
        """
        text_content = self._aggregate_thread_text(events)
        if not text_content:
            return None

        max_chars = self.max_tokens * 4
        text_content = text_content[:max_chars]

        embedding = await self._call_openai(text_content)
        if embedding is None:
            return None

        logger.info(
            f"🧠 Thread embedding: thread={thread_id[:20]}... "
            f"({len(events)} turns, {len(text_content)} chars → {len(embedding)} dims)"
        )

        if self.postgres_pool:
            # 1. Overwrite current embedding in embeddings table
            await self._store_embedding(
                entity_type="thread",
                entity_id=thread_id,
                embedding_type="semantic",
                embedding=embedding,
                text_content=text_content,
                source_event_ids=[e["event_id"] for e in events],
                interpretation_id=interpretation_id
            )

            # 2. Append immutable history snapshot
            await self._store_thread_history_snapshot(
                thread_id=thread_id,
                channel_id=channel_id or (events[0].get("channel_id") if events else None),
                embedding=embedding,
                text_content=text_content,
                model_name=self.model_name,
                phase=phase,
                pattern=pattern,
                participant_count=participant_count,
                message_count=message_count if message_count is not None else len(events),
                interpretation_id=interpretation_id
            )

        return embedding

    async def _store_thread_history_snapshot(
        self,
        thread_id: str,
        channel_id: Optional[str],
        embedding: List[float],
        text_content: str,
        model_name: str,
        phase: Optional[str],
        pattern: Optional[str],
        participant_count: Optional[int],
        message_count: int,
        interpretation_id: Optional[str] = None,
    ):
        """
        Append an immutable snapshot row to thread_embedding_history.

        Automatically:
        - Increments snapshot_index by querying the current max for this thread
        - Computes drift_from_prev as cosine distance from the previous snapshot's vector
        """
        if not self.postgres_pool:
            return

        import json as _json
        import math as _math

        embedding_str = _json.dumps(embedding)

        async with self.postgres_pool.acquire() as conn:
            # Get the previous snapshot index and vector in one query
            prev_row = await conn.fetchrow(
                """
                SELECT snapshot_index, embedding::text AS emb_str
                FROM thread_embedding_history
                WHERE thread_id = $1
                ORDER BY snapshot_index DESC
                LIMIT 1
                """,
                thread_id
            )

            if prev_row:
                next_index = prev_row["snapshot_index"] + 1
                # Compute cosine distance from previous embedding
                try:
                    prev_vec = _json.loads(prev_row["emb_str"])
                    dot = sum(a * b for a, b in zip(embedding, prev_vec))
                    mag_a = _math.sqrt(sum(x * x for x in embedding))
                    mag_b = _math.sqrt(sum(x * x for x in prev_vec))
                    cosine_sim = dot / (mag_a * mag_b) if mag_a and mag_b else 0.0
                    drift = 1.0 - cosine_sim  # distance: 0=same, 1=orthogonal
                except Exception:
                    drift = None
            else:
                next_index = 1
                drift = None  # first snapshot — no previous to compare

            await conn.execute(
                """
                INSERT INTO thread_embedding_history (
                    thread_id, channel_id,
                    snapshot_index, message_count,
                    phase, pattern, participant_count,
                    model_name, embedding,
                    text_content, drift_from_prev,
                    source_interpretation_id
                ) VALUES (
                    $1, $2, $3, $4,
                    $5, $6, $7,
                    $8, $9::vector,
                    $10, $11, $12
                )
                """,
                thread_id, channel_id,
                next_index, message_count,
                phase, pattern, participant_count,
                model_name, embedding_str,
                text_content[:2000],  # store more than embeddings table (trend context)
                drift,
                interpretation_id if interpretation_id else None
            )

        drift_str = f"{drift:.4f}" if drift is not None else "N/A"
        logger.debug(
            f"📜 Thread history snapshot: thread={thread_id[:20]}... "
            f"index={next_index}, drift={drift_str}"
        )
            
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
        
        # pgvector requires the embedding as a string "[x, y, z, ...]" for ::vector cast
        import json as _json
        embedding_str = _json.dumps(embedding)

        async with self.postgres_pool.acquire() as conn:
            await conn.execute(
                query,
                entity_type,
                entity_id,
                embedding_type,
                self.model_name,
                embedding_str,
                text_content[:1000],  # Store first 1000 chars
                token_count,
                interpretation_id,
                source_event_ids
            )
            
        logger.debug(f"💾 Stored embedding: {entity_type}/{entity_id}/{embedding_type}")
        
    async def generate_channel_embedding(
        self,
        channel_id: str,
        thread_ids: List[str],
        interpretation_id: Optional[str] = None
    ) -> Optional[List[float]]:
        """
        Generate and store a composite embedding for a channel.

        Mean-pools all thread embedding vectors stored in the embeddings table
        for the given channel. Always overwrites the previous channel embedding.

        Args:
            channel_id: Slack channel identifier
            thread_ids: Thread IDs active in this channel (used for lineage only)
            interpretation_id: Lineage to interpretation record

        Returns:
            Mean-pooled embedding vector or None if no thread embeddings found
        """
        if not self.postgres_pool:
            return None

        # Fetch all thread embedding vectors for this channel
        query = """
            SELECT embedding::text
            FROM embeddings
            WHERE entity_type = 'thread'
              AND entity_id = ANY($1::text[])
              AND embedding_type = 'semantic'
        """

        import json as _json
        import numpy as _np

        async with self.postgres_pool.acquire() as conn:
            rows = await conn.fetch(query, thread_ids)

        if not rows:
            logger.debug(f"No thread embeddings found for channel {channel_id}, skipping channel embedding")
            return None

        # Parse the pgvector string representations back to lists
        vectors = []
        for row in rows:
            try:
                # pgvector returns "[x, y, z, ...]" as text
                vec_str = row["embedding"]
                vec = _json.loads(vec_str)
                vectors.append(vec)
            except Exception:
                pass

        if not vectors:
            return None

        # Mean-pool: average each dimension across all thread vectors
        arr = _np.array(vectors, dtype=_np.float32)
        mean_vec = arr.mean(axis=0).tolist()

        thread_count = len(vectors)
        logger.info(
            f"🧠 Channel embedding: channel={channel_id} "
            f"(mean-pooled {thread_count} thread vectors → {len(mean_vec)} dims)"
        )

        await self._store_embedding(
            entity_type="channel",
            entity_id=channel_id,
            embedding_type="semantic",
            embedding=mean_vec,
            text_content=f"Channel composite from {thread_count} threads",
            source_event_ids=[],
            interpretation_id=interpretation_id
        )

        return mean_vec

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
