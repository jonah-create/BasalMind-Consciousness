"""
PostgreSQL Storage Layer - Phase 1

Writes interpreted knowledge to PostgreSQL for downstream analysis.
Handles ALL source systems: Slack, nginx, Cloudflare, enterprise apps, etc.

Stores:
- Interpretations (cross-source patterns, anomalies, intents)
- Thread contexts (conversations across any channel/system)
- User sessions (activity across all systems)
- Embeddings (semantic vectors for similarity search)

Based on Claude Opus design specification.
"""

import asyncio
import asyncpg
import logging
import json
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
from uuid import UUID

logger = logging.getLogger(__name__)


class PostgresWriter:
    """
    Write interpreted semantic knowledge to PostgreSQL.
    
    Responsibilities:
    - Store interpretation records with lineage
    - Track cross-source correlations and anomalies
    - Manage thread contexts (any conversation-like sequence)
    - Track user sessions (activity across all systems)
    - Store embeddings with lineage
    """
    
    def __init__(
        self,
        host: str,
        port: int,
        database: str,
        user: str,
        password: str
    ):
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password
        
        self.pool: Optional[asyncpg.Pool] = None
        
    async def connect(self):
        """Initialize connection pool to PostgreSQL."""
        self.pool = await asyncpg.create_pool(
            host=self.host,
            port=self.port,
            database=self.database,
            user=self.user,
            password=self.password,
            min_size=2,
            max_size=10,
            command_timeout=60
        )
        logger.info(
            f"✅ PostgreSQL writer connected: {self.host}:{self.port}/{self.database}"
        )
        
    async def close(self):
        """Close connection pool."""
        if self.pool:
            await self.pool.close()
            logger.info("PostgreSQL writer closed")
            
    async def write_interpretation(
        self,
        window_start: datetime,
        window_end: datetime,
        events: List[Dict[str, Any]],
        intents: Dict[str, List[Dict[str, Any]]],
        correlations: Optional[List[Dict[str, Any]]] = None,
        anomalies: Optional[List[Dict[str, Any]]] = None,
        processing_duration_ms: Optional[int] = None,
        graph_node_ids: Optional[List[str]] = None
    ) -> str:
        """
        Write an interpretation record for a processing window.
        
        Args:
            window_start: Start of time window
            window_end: End of time window
            events: List of source events processed
            intents: Dict mapping event_id to extracted intents
            correlations: Cross-source patterns detected (e.g., session without auth)
            anomalies: Unusual patterns (e.g., latency spikes, volume anomalies)
            processing_duration_ms: Time taken to process this window
            
        Returns:
            interpretation_id (UUID as string)
        """
        if not self.pool:
            raise RuntimeError("Writer not connected. Call connect() first.")
            
        # Extract source event IDs and times for lineage
        source_event_ids = [e["event_id"] for e in events]
        event_times = [
            e.get("event_time") or e.get("observed_at") for e in events
        ]
        earliest_event = min(event_times) if event_times else window_start
        latest_event = max(event_times) if event_times else window_end
        
        # Build intents JSONB
        all_intents = []
        for event_id, intent_list in intents.items():
            all_intents.extend(intent_list)
            
        # Collect deduplication metadata from events
        dedup_metadata = self._collect_dedup_metadata(events)

        # Build entities summary (users, channels, systems across all sources)
        entities = self._extract_entities(events)
        
        query = """
            INSERT INTO interpretations (
                window_start,
                window_end,
                event_count,
                processing_duration_ms,
                source_event_ids,
                earliest_event_time,
                latest_event_time,
                intents,
                correlations,
                anomalies,
                entities,
                deduplication_metadata,
                graph_node_ids
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
            RETURNING interpretation_id
        """
        
        async with self.pool.acquire() as conn:
            result = await conn.fetchval(
                query,
                window_start,
                window_end,
                len(events),
                processing_duration_ms,
                source_event_ids,
                earliest_event,
                latest_event,
                json.dumps(all_intents) if all_intents else None,
                json.dumps(correlations) if correlations else None,
                json.dumps(anomalies) if anomalies else None,
                json.dumps(entities) if entities else None,
                json.dumps(dedup_metadata) if dedup_metadata else None,
                graph_node_ids or []
            )
            
        interpretation_id = str(result)
        logger.info(
            f"📝 Wrote interpretation {interpretation_id[:8]}... "
            f"({len(events)} events, {len(all_intents)} intents)"
        )
        
        return interpretation_id
        
    def _extract_entities(self, events: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Extract entity summary from events (users, systems, channels, IPs, etc.).
        
        This captures entities from ALL sources: Slack, nginx, Cloudflare, etc.
        """
        entities = {}
        
        for event in events:
            source_system = event.get("source_system", "unknown")
            
            # User entities (from any system)
            user_id = event.get("user_id")
            if user_id:
                key = f"user_{user_id}"
                if key not in entities:
                    entities[key] = {
                        "type": "user",
                        "id": user_id,
                        "sources": set(),
                        "count": 0
                    }
                entities[key]["sources"].add(source_system)
                entities[key]["count"] += 1
                
            # Channel/conversation entities,
            channel_id = event.get("channel_id")
            if channel_id:
                key = f"channel_{channel_id}"
                if key not in entities:
                    entities[key] = {
                        "type": "channel",
                        "id": channel_id,
                        "sources": set(),
                        "count": 0
                    }
                entities[key]["sources"].add(source_system)
                entities[key]["count"] += 1
                
            # IP addresses (from nginx, Cloudflare, etc.)
            metadata = event.get("metadata", {})
            if isinstance(metadata, dict):
                client_ip = metadata.get("client_ip") or metadata.get("ip")
                if client_ip:
                    key = f"ip_{client_ip}"
                    if key not in entities:
                        entities[key] = {
                            "type": "ip_address",
                            "id": client_ip,
                            "sources": set(),
                            "count": 0
                        }
                    entities[key]["sources"].add(source_system)
                    entities[key]["count"] += 1
                    
            # System entities (which systems generated events)
            if source_system != "unknown":
                key = f"system_{source_system}"
                if key not in entities:
                    entities[key] = {
                        "type": "system",
                        "id": source_system,
                        "count": 0
                    }
                entities[key]["count"] += 1
                
        # Convert sets to lists for JSON serialization
        result = []
        for entity in entities.values():
            if "sources" in entity:
                entity["sources"] = list(entity["sources"])
            result.append(entity)
            
        return result
        
    async def upsert_thread_context(
        self,
        thread_id: str,
        channel_id: str,
        events: List[Dict[str, Any]],
        topic: Optional[str] = None,
        phase: Optional[str] = None,
        pattern: Optional[str] = None
    ) -> str:
        """
        Create or update a thread context.
        
        Threads can be:
        - Slack conversation threads
        - nginx request sequences (same session_id)
        - Cloudflare traffic patterns
        - Any conversation-like sequence
        
        Args:
            thread_id: Unique thread identifier
            channel_id: Container ID (Slack channel, nginx host, etc.)
            events: Events in this thread
            topic: Extracted topic (optional)
            phase: initiation|exploration|discussion|resolution
            pattern: Q&A|brainstorm|decision|troubleshooting
            
        Returns:
            context_id (UUID as string)
        """
        if not self.pool:
            raise RuntimeError("Writer not connected. Call connect() first.")
            
        # Extract temporal bounds
        event_times = [
            e.get("event_time") or e.get("observed_at") for e in events
        ]
        started_at = min(event_times) if event_times else datetime.utcnow()
        last_activity = max(event_times) if event_times else datetime.utcnow()
        
        # Extract participants (works for users, IPs, systems)
        participants = self._extract_participants(events)
        
        # Source event IDs for lineage
        source_event_ids = [e["event_id"] for e in events]
        
        query = """
            INSERT INTO thread_contexts (
                thread_id,
                channel_id,
                topic,
                phase,
                pattern,
                participants,
                started_at,
                last_activity,
                message_count,
                source_event_ids
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
            ON CONFLICT (thread_id) DO UPDATE SET
                last_activity = EXCLUDED.last_activity,
                message_count = EXCLUDED.message_count,
                participants = EXCLUDED.participants,
                source_event_ids = thread_contexts.source_event_ids || EXCLUDED.source_event_ids,
                updated_at = NOW()
            RETURNING context_id
        """
        
        async with self.pool.acquire() as conn:
            result = await conn.fetchval(
                query,
                thread_id,
                channel_id,
                topic,
                phase,
                pattern,
                json.dumps(participants),
                started_at,
                last_activity,
                len(events),
                source_event_ids
            )
            
        context_id = str(result)
        logger.debug(f"📝 Upserted thread context {thread_id}")
        
        return context_id
        

    def _collect_dedup_metadata(self, events: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        """
        Collect deduplication metadata from events.
        
        Returns aggregated dedup metadata if any events were deduplicated.
        """
        all_culled = []
        total_original = 0
        total_deduplicated = len(events)
        
        for event in events:
            if "deduplication_metadata" in event:
                dedup_meta = event["deduplication_metadata"]
                total_original += dedup_meta.get("original_event_count", 1)
                all_culled.extend(dedup_meta.get("culled_events", []))
        
        if not all_culled:
            # No deduplication occurred
            return None
        
        return {
            "total_original_events": total_original,
            "total_deduplicated_events": total_deduplicated,
            "reduction_percentage": round((1 - total_deduplicated / total_original) * 100, 1),
            "culled_events": all_culled
        }
    def _extract_participants(self, events: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Extract participants from events (users, IPs, systems)."""
        participants = {}
        
        for event in events:
            # User participants
            user_id = event.get("user_id")
            if user_id:
                if user_id not in participants:
                    participants[user_id] = {
                        "id": user_id,
                        "type": "user",
                        "message_count": 0
                    }
                participants[user_id]["message_count"] += 1
                
            # IP participants (for nginx/Cloudflare threads)
            metadata = event.get("metadata", {})
            if isinstance(metadata, dict):
                client_ip = metadata.get("client_ip") or metadata.get("ip")
                if client_ip:
                    if client_ip not in participants:
                        participants[client_ip] = {
                            "id": client_ip,
                            "type": "ip_address",
                            "message_count": 0
                        }
                    participants[client_ip]["message_count"] += 1
                    
        return list(participants.values())
        
    async def update_checkpoint(
        self,
        last_processed_time: datetime,
        last_event_id: str,
        records_processed: int
    ):
        """
        Update processing checkpoint for recovery.
        
        Args:
            last_processed_time: Timestamp of last processed event
            last_event_id: ID of last processed event
            records_processed: Total records processed in this batch
        """
        if not self.pool:
            raise RuntimeError("Writer not connected. Call connect() first.")
            
        query = """
            INSERT INTO interpreter_checkpoints (
                last_processed_time,
                last_event_id,
                records_processed
            ) VALUES ($1, $2, $3)
        """
        
        async with self.pool.acquire() as conn:
            await conn.execute(
                query,
                last_processed_time,
                UUID(last_event_id),
                records_processed
            )
            
        logger.debug(
            f"✅ Checkpoint updated: {last_processed_time.isoformat()}, "
            f"{records_processed} records"
        )
        
    async def get_latest_checkpoint(self) -> Optional[Dict[str, Any]]:
        """
        Get the latest processing checkpoint.
        
        Returns:
            Dict with {last_processed_time, last_event_id, records_processed}
            or None if no checkpoints exist
        """
        if not self.pool:
            raise RuntimeError("Writer not connected. Call connect() first.")
            
        query = """
            SELECT last_processed_time, last_event_id, records_processed
            FROM interpreter_checkpoints
            ORDER BY created_at DESC
            LIMIT 1
        """
        
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(query)
            
        if row:
            return {
                "last_processed_time": row["last_processed_time"],
                "last_event_id": str(row["last_event_id"]),
                "records_processed": row["records_processed"]
            }
        return None


# Example usage
async def test_writer():
    """Test the PostgresWriter with sample data."""
    import os
    from dotenv import load_dotenv
    
    load_dotenv()
    
    writer = PostgresWriter(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", 5432)),
        database=os.getenv("POSTGRES_DB", "basalmind_interpreter_postgresql"),
        user=os.getenv("POSTGRES_USER", "basalmind"),
        password=os.getenv("POSTGRES_PASSWORD")
    )
    
    await writer.connect()
    
    try:
        # Sample events from multiple sources
        events = [
            {
                "event_id": "evt-1",
                "event_time": datetime.utcnow(),
                "source_system": "slack",
                "user_id": "U123",
                "channel_id": "C456",
                "text": "How do we configure auth?"
            },
            {
                "event_id": "evt-2",
                "event_time": datetime.utcnow(),
                "source_system": "nginx",
                "metadata": {"client_ip": "192.168.1.100", "path": "/api/auth"}
            }
        ]
        
        # Sample intents
        intents = {
            "evt-1": [{
                "type": "asking_question",
                "confidence": 0.85,
                "actor_id": "U123"
            }]
        }
        
        # Write interpretation
        interpretation_id = await writer.write_interpretation(
            window_start=datetime.utcnow() - timedelta(seconds=30),
            window_end=datetime.utcnow(),
            events=events,
            intents=intents,
            processing_duration_ms=150
        )
        print(f"✅ Created interpretation: {interpretation_id}")
        
        # Update checkpoint
        await writer.update_checkpoint(
            last_processed_time=datetime.utcnow(),
            last_event_id="evt-2",
            records_processed=2
        )
        
        # Retrieve checkpoint
        checkpoint = await writer.get_latest_checkpoint()
        print(f"✅ Latest checkpoint: {checkpoint}")
        
    finally:
        await writer.close()


if __name__ == "__main__":
    asyncio.run(test_writer())
