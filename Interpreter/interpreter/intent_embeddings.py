"""
Intent Embeddings - Metadata-Only Approach

Per Claude Opus design: Track intent relationships and metadata without
generating actual embedding vectors for every intent.

Instead of embedding each intent individually:
- Track intent co-occurrence patterns
- Link intents to their thread/channel embeddings
- Store intent metadata for retrieval augmentation

This avoids the cost of generating thousands of embeddings while still
enabling intent-based semantic search via thread embeddings.
"""

import logging
from typing import List, Dict, Any, Optional
from datetime import datetime
import asyncpg
import json

logger = logging.getLogger(__name__)


class IntentEmbeddingTracker:
    """
    Track intent embeddings metadata (no vector generation).

    Purpose:
    - Link intents to thread/channel embeddings
    - Track intent co-occurrence for pattern detection
    - Store intent metadata for RAG (Retrieval Augmented Generation)

    Design Philosophy:
    - Intents inherit embeddings from their parent thread/channel
    - No need to embed individual intents (cost-prohibitive)
    - Metadata tracking enables intent-based filtering on existing embeddings
    """

    def __init__(self, postgres_pool: asyncpg.Pool):
        self.pool = postgres_pool

    async def link_intents_to_embeddings(
        self,
        intents: List[Dict[str, Any]],
        thread_embedding_id: Optional[str] = None,
        channel_embedding_id: Optional[str] = None
    ) -> int:
        """
        Link extracted intents to their parent thread/channel embeddings.

        This creates the metadata layer that allows intent-based semantic search
        without generating individual intent embeddings.

        Args:
            intents: List of extracted intents
            thread_embedding_id: ID of thread's embedding (if exists)
            channel_embedding_id: ID of channel's embedding (if exists)

        Returns:
            Number of intents linked
        """
        if not intents:
            return 0

        linked_count = 0

        for intent in intents:
            try:
                await self._link_intent_metadata(
                    intent=intent,
                    thread_embedding_id=thread_embedding_id,
                    channel_embedding_id=channel_embedding_id
                )
                linked_count += 1
            except Exception as e:
                logger.error(f"Error linking intent {intent.get('intent_id')}: {e}")

        logger.debug(f"🔗 Linked {linked_count} intents to embeddings")
        return linked_count

    async def _link_intent_metadata(
        self,
        intent: Dict[str, Any],
        thread_embedding_id: Optional[str],
        channel_embedding_id: Optional[str]
    ):
        """Store intent metadata linking to embeddings."""
        intent_id = intent.get("intent_id")
        if not intent_id:
            return

        # Build metadata object
        metadata = {
            "intent_type": intent.get("type"),
            "category": intent.get("category"),
            "confidence": intent.get("confidence"),
            "entities": intent.get("entities", []),
            "sentiment": intent.get("sentiment"),
            "thread_embedding_id": thread_embedding_id,
            "channel_embedding_id": channel_embedding_id,
            "indexed_at": datetime.utcnow().isoformat()
        }

        # Update intent record with embedding metadata
        query = """
            UPDATE intents
            SET metadata = metadata || $2::jsonb
            WHERE intent_id = $1
        """

        async with self.pool.acquire() as conn:
            await conn.execute(query, intent_id, json.dumps(metadata))

    async def track_intent_cooccurrence(
        self,
        intents: List[Dict[str, Any]],
        context_id: str,
        context_type: str = "thread"
    ):
        """
        Track which intents appear together in threads/channels.

        This helps identify intent patterns without needing embeddings:
        - "asking_question" + "requesting_help" often together
        - "reporting_bug" + "expressing_frustration" pattern
        - "making_decision" + "delegating_task" workflow

        Args:
            intents: Intents from same context
            context_id: Thread/channel ID
            context_type: Type of context
        """
        if len(intents) < 2:
            return  # No co-occurrence with single intent

        # Extract intent types
        intent_types = [i.get("type") for i in intents if i.get("type")]

        if len(intent_types) < 2:
            return

        # Store co-occurrence pattern
        pattern = {
            "intent_types": sorted(intent_types),
            "context_id": context_id,
            "context_type": context_type,
            "count": len(intent_types),
            "timestamp": datetime.utcnow().isoformat()
        }

        # Store in metadata table or log for analysis
        logger.debug(f"📊 Intent co-occurrence: {intent_types} in {context_type} {context_id}")

    async def get_intents_for_embedding(
        self,
        embedding_id: str,
        embedding_type: str = "thread"
    ) -> List[Dict[str, Any]]:
        """
        Retrieve all intents linked to a specific embedding.

        Enables intent-based filtering of semantic search results.

        Args:
            embedding_id: Embedding identifier
            embedding_type: thread or channel

        Returns:
            List of intents with metadata
        """
        metadata_key = f"{embedding_type}_embedding_id"

        query = f"""
            SELECT
                intent_id,
                type,
                category,
                confidence,
                entities,
                sentiment,
                metadata
            FROM intents
            WHERE metadata->>{metadata_key!r} = $1
            ORDER BY created_at DESC
        """

        async with self.pool.acquire() as conn:
            rows = await conn.fetch(query, embedding_id)

        intents = []
        for row in rows:
            intents.append({
                "intent_id": str(row["intent_id"]),
                "type": row["type"],
                "category": row["category"],
                "confidence": float(row["confidence"]) if row["confidence"] else None,
                "entities": row["entities"],
                "sentiment": row["sentiment"],
                "metadata": row["metadata"]
            })

        return intents

    async def search_by_intent_type(
        self,
        intent_types: List[str],
        limit: int = 50
    ) -> List[Dict[str, Any]]:
        """
        Find threads/channels containing specific intent types.

        This enables intent-based search WITHOUT individual intent embeddings.
        Instead, we:
        1. Find intents of desired types
        2. Get their linked thread/channel embeddings
        3. Return those embeddings for semantic search

        Args:
            intent_types: Intent types to search for
            limit: Max results

        Returns:
            List of embedding IDs and metadata
        """
        query = """
            SELECT DISTINCT
                metadata->>'thread_embedding_id' as thread_embedding_id,
                metadata->>'channel_embedding_id' as channel_embedding_id,
                type,
                category,
                COUNT(*) OVER (PARTITION BY metadata->>'thread_embedding_id') as intent_count
            FROM intents
            WHERE type = ANY($1)
            AND (
                metadata->>'thread_embedding_id' IS NOT NULL
                OR metadata->>'channel_embedding_id' IS NOT NULL
            )
            ORDER BY intent_count DESC
            LIMIT $2
        """

        async with self.pool.acquire() as conn:
            rows = await conn.fetch(query, intent_types, limit)

        results = []
        for row in rows:
            results.append({
                "thread_embedding_id": row["thread_embedding_id"],
                "channel_embedding_id": row["channel_embedding_id"],
                "intent_type": row["type"],
                "intent_category": row["category"],
                "intent_count": row["intent_count"]
            })

        logger.info(f"🔍 Found {len(results)} embeddings with intent types: {intent_types}")
        return results

    async def get_intent_statistics(self) -> Dict[str, Any]:
        """
        Get statistics about intent-embedding linkage.

        Returns:
            Dict with stats on how many intents are linked to embeddings
        """
        query = """
            SELECT
                COUNT(*) as total_intents,
                COUNT(CASE WHEN metadata->>'thread_embedding_id' IS NOT NULL THEN 1 END) as thread_linked,
                COUNT(CASE WHEN metadata->>'channel_embedding_id' IS NOT NULL THEN 1 END) as channel_linked,
                COUNT(DISTINCT type) as unique_intent_types
            FROM intents
        """

        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(query)

        stats = {
            "total_intents": row["total_intents"],
            "thread_linked": row["thread_linked"],
            "channel_linked": row["channel_linked"],
            "unique_intent_types": row["unique_intent_types"],
            "linkage_rate": (row["thread_linked"] / row["total_intents"] * 100) if row["total_intents"] > 0 else 0
        }

        logger.info(f"📊 Intent-Embedding Stats: {stats['linkage_rate']:.1f}% linked ({stats['thread_linked']}/{stats['total_intents']})")
        return stats


# Test the tracker
async def test_tracker():
    """Test intent embedding tracker."""
    import os
    from dotenv import load_dotenv

    load_dotenv()

    pool = await asyncpg.create_pool(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", 5432)),
        database=os.getenv("POSTGRES_DB", "basalmind_interpreter_postgresql"),
        user=os.getenv("POSTGRES_USER", "basalmind"),
        password=os.getenv("POSTGRES_PASSWORD")
    )

    tracker = IntentEmbeddingTracker(pool)

    # Sample intents
    intents = [
        {
            "intent_id": "intent-1",
            "type": "asking_question",
            "category": "information_seeking",
            "confidence": 0.95
        },
        {
            "intent_id": "intent-2",
            "type": "requesting_help",
            "category": "assistance",
            "confidence": 0.88
        }
    ]

    # Link to hypothetical thread embedding
    await tracker.link_intents_to_embeddings(
        intents=intents,
        thread_embedding_id="emb-thread-123"
    )

    # Get stats
    stats = await tracker.get_intent_statistics()
    print(f"Stats: {stats}")

    await pool.close()


if __name__ == "__main__":
    import asyncio
    logging.basicConfig(level=logging.INFO)
    asyncio.run(test_tracker())
