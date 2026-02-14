"""
Interpreter Engine - Phase 1

Main orchestrator for the Interpreter module.
Coordinates batch processing of events from TimescaleDB.

Processes ALL sources: Slack, nginx, Cloudflare, enterprise apps, etc.
Detects cross-source patterns, anomalies, and semantic knowledge.

Based on Claude Opus design specification.
"""

import asyncio
import logging
import os
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Any, Optional
from dotenv import load_dotenv

from interpreter.timescale_reader import TimescaleReader
from interpreter.intent_extractor import IntentExtractor
from interpreter.event_deduplicator import EventDeduplicator
from interpreter.postgres_writer import PostgresWriter
from interpreter.neo4j_writer import Neo4jWriter
from interpreter.embedding_generator import EmbeddingGenerator

logger = logging.getLogger(__name__)


class InterpreterEngine:
    """
    Main Interpreter processing engine.
    
    Orchestrates the complete interpretation pipeline:
    1. Read events from TimescaleDB (30-second windows)
    2. Extract intents and patterns
    3. Detect cross-source correlations
    4. Identify anomalies
    5. Write to PostgreSQL
    6. Create Neo4j graph nodes
    7. Generate embeddings
    8. Update checkpoint
    
    Handles events from ALL sources in a unified pipeline.
    """
    
    def __init__(
        self,
        batch_window_seconds: int = 30,
        batch_size: int = 1000,
        processing_interval: int = 30,
        min_thread_messages: int = 5  # Min messages before generating embedding
    ):
        self.batch_window_seconds = batch_window_seconds
        self.batch_size = batch_size
        self.processing_interval = processing_interval
        self.min_thread_messages = min_thread_messages
        
        # Components (initialized in setup())
        self.timescale_reader: Optional[TimescaleReader] = None
        self.intent_extractor: Optional[IntentExtractor] = None
        self.postgres_writer: Optional[PostgresWriter] = None
        self.neo4j_writer: Optional[Neo4jWriter] = None
        self.embedding_generator: Optional[EmbeddingGenerator] = None
        
        # State
        self.running = False
        self.last_checkpoint: Optional[datetime] = None
        
    async def setup(self):
        """Initialize all components."""
        load_dotenv()
        
        # TimescaleDB reader (read-only access to Observer's data)
        self.timescale_reader = TimescaleReader(
            host=os.getenv("TIMESCALE_HOST", "localhost"),
            port=int(os.getenv("TIMESCALE_PORT", 5432)),
            database=os.getenv("TIMESCALE_DB", "basalmind_events_timescaledb"),
            user=os.getenv("TIMESCALE_USER", "basalmind"),
            password=os.getenv("TIMESCALE_PASSWORD"),
            batch_size=self.batch_size,
            window_seconds=self.batch_window_seconds
        )
        await self.timescale_reader.connect()
        
        # Intent extractor (keyword-based for Phase 1)
        self.intent_extractor = IntentExtractor(confidence_threshold=0.5)
        self.event_deduplicator = EventDeduplicator()
        
        # PostgreSQL writer (Interpreter's database)
        self.postgres_writer = PostgresWriter(
            host=os.getenv("POSTGRES_HOST", "localhost"),
            port=int(os.getenv("POSTGRES_PORT", 5432)),
            database=os.getenv("POSTGRES_DB", "basalmind_interpreter_postgresql"),
            user=os.getenv("POSTGRES_USER", "basalmind"),
            password=os.getenv("POSTGRES_PASSWORD")
        )
        await self.postgres_writer.connect()
        
        # Neo4j writer (shared graph with namespace)
        self.neo4j_writer = Neo4jWriter(
            uri=os.getenv("NEO4J_URI", "bolt://localhost:7687"),
            user=os.getenv("NEO4J_USER", "neo4j"),
            password=os.getenv("NEO4J_PASSWORD"),
            database=os.getenv("NEO4J_DATABASE", "neo4j"),
            namespace=os.getenv("NEO4J_NAMESPACE", "Interpreter")
        )
        self.neo4j_writer.connect()
        
        # Embedding generator (OpenAI)
        openai_key = os.getenv("OPENAI_API_KEY")
        if openai_key and openai_key != "sk-your-key-here":
            self.embedding_generator = EmbeddingGenerator(
                openai_api_key=openai_key,
                model_name=os.getenv("EMBEDDING_MODEL", "text-embedding-ada-002"),
                postgres_pool=self.postgres_writer.pool
            )
        else:
            logger.warning("⚠️ No OpenAI API key configured, embeddings disabled")
            
        # Load checkpoint
        checkpoint = await self.postgres_writer.get_latest_checkpoint()
        if checkpoint:
            self.last_checkpoint = checkpoint["last_processed_time"]
            logger.info(f"📍 Resuming from checkpoint: {self.last_checkpoint.isoformat()}")
        else:
            # Start from 1 hour ago if no checkpoint
            self.last_checkpoint = datetime.now(timezone.utc) - timedelta(minutes=5)
            logger.info(f"📍 Starting fresh from: {self.last_checkpoint.isoformat()}")
            
        logger.info("✅ Interpreter engine initialized")
        
    async def shutdown(self):
        """Clean shutdown of all components."""
        logger.info("🛑 Shutting down Interpreter engine...")
        
        if self.timescale_reader:
            await self.timescale_reader.close()
        if self.postgres_writer:
            await self.postgres_writer.close()
        if self.neo4j_writer:
            self.neo4j_writer.close()
            
        logger.info("✅ Interpreter engine shutdown complete")
        
    async def process_window(
        self,
        start_time: datetime,
        end_time: datetime
    ) -> Optional[str]:
        """
        Process a single time window of events.
        
        Args:
            start_time: Window start
            end_time: Window end
            
        Returns:
            interpretation_id if processing succeeded, None if no events
        """
        start_ms = datetime.now(timezone.utc)
        
        # 1. Fetch events from TimescaleDB
        events = await self.timescale_reader.get_batch(start_time, end_time)
        
        if not events:
            logger.debug(f"No events in window {start_time.isoformat()} - {end_time.isoformat()}")
            return None
            
        
        # 1.5. Deduplicate events (consolidate duplicates)
        events = self.event_deduplicator.deduplicate(events)
        if not events:
            logger.debug(f"No events after deduplication")
            return None
        logger.info(
            f"\n{'='*60}\n"
            f"Processing window: {start_time.isoformat()} - {end_time.isoformat()}\n"
            f"Events: {len(events)} from {len(set(e['source_system'] for e in events))} sources\n"
            f"{'='*60}"
        )
        
        # 2. Extract intents
        intents = self.intent_extractor.extract_from_batch(events)
        
        # 3. Detect correlations (Phase 1: basic, Phase 2: advanced)
        correlations = self._detect_correlations(events)
        
        # 4. Detect anomalies (Phase 1: basic, Phase 2: ML-based)
        anomalies = self._detect_anomalies(events)
        
        # Calculate processing time
        processing_ms = int((datetime.now(timezone.utc) - start_ms).total_seconds() * 1000)
        
        # 5. Write interpretation to PostgreSQL
        interpretation_id = await self.postgres_writer.write_interpretation(
            window_start=start_time,
            window_end=end_time,
            events=events,
            intents=intents,
            correlations=correlations,
            anomalies=anomalies,
            processing_duration_ms=processing_ms
        )
        
        # 6. Create Neo4j graph nodes
        await self._create_graph_nodes(events, intents)
        
        # 7. Generate embeddings for threads (if enough messages)
        await self._generate_embeddings(events, interpretation_id)
        
        # 8. Update checkpoint
        last_event = events[-1]
        await self.postgres_writer.update_checkpoint(
            last_processed_time=end_time,
            last_event_id=last_event["event_id"],
            records_processed=len(events)
        )
        
        logger.info(
            f"✅ Window complete: {interpretation_id[:8]}... "
            f"({processing_ms}ms, {len(intents)} intents extracted)"
        )
        
        return interpretation_id
        
    def _detect_correlations(self, events: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Detect cross-source correlations.
        
        Phase 1 examples:
        - Same user_id appearing in Slack and nginx logs
        - Same IP appearing in nginx and Cloudflare
        - Temporal proximity of related events
        
        Phase 2: More sophisticated ML-based correlation
        """
        correlations = []
        
        # Group events by correlation keys
        by_user = {}
        by_ip = {}
        by_session = {}
        
        for event in events:
            # User correlations
            user_id = event.get("user_id")
            if user_id:
                if user_id not in by_user:
                    by_user[user_id] = []
                by_user[user_id].append(event)
                
            # IP correlations
            metadata = event.get("metadata", {})
            if isinstance(metadata, dict):
                ip = metadata.get("client_ip") or metadata.get("ip")
                if ip:
                    if ip not in by_ip:
                        by_ip[ip] = []
                    by_ip[ip].append(event)
                    
            # Session correlations
            session_id = event.get("session_id")
            if session_id:
                if session_id not in by_session:
                    by_session[session_id] = []
                by_session[session_id].append(event)
                
        # Find cross-source correlations
        for user_id, user_events in by_user.items():
            sources = set(e["source_system"] for e in user_events)
            if len(sources) > 1:
                correlations.append({
                    "type": "cross_source_user",
                    "user_id": user_id,
                    "sources": list(sources),
                    "event_count": len(user_events),
                    "confidence": 0.9
                })
                
        for ip, ip_events in by_ip.items():
            sources = set(e["source_system"] for e in ip_events)
            if len(sources) > 1:
                correlations.append({
                    "type": "cross_source_ip",
                    "ip_address": ip,
                    "sources": list(sources),
                    "event_count": len(ip_events),
                    "confidence": 0.8
                })
                
        if correlations:
            logger.info(f"🔗 Detected {len(correlations)} cross-source correlations")
            
        return correlations
        
    def _detect_anomalies(self, events: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Detect anomalies in event patterns.
        
        Phase 1 examples:
        - High volume from single IP
        - Unusual latency patterns
        - Missing expected events
        
        Phase 2: Statistical and ML-based anomaly detection
        """
        anomalies = []
        
        # Volume anomaly: too many events from single source
        by_source = {}
        for event in events:
            source = event.get("source_system", "unknown")
            by_source[source] = by_source.get(source, 0) + 1
            
        for source, count in by_source.items():
            if count > 100:  # Simple threshold
                anomalies.append({
                    "type": "high_volume",
                    "source_system": source,
                    "event_count": count,
                    "severity": "medium",
                    "confidence": 0.7
                })
                
        if anomalies:
            logger.warning(f"⚠️ Detected {len(anomalies)} anomalies")
            
        return anomalies
        
    async def _create_graph_nodes(
        self,
        events: List[Dict[str, Any]],
        intents: Dict[str, List[Dict[str, Any]]]
    ):
        """Create Neo4j graph nodes for this batch."""
        # Create user/actor nodes
        users = {}
        for event in events:
            user_id = event.get("user_id")
            if user_id:
                if user_id not in users:
                    users[user_id] = {
                        "first_seen": event.get("event_time") or event.get("observed_at"),
                        "last_active": event.get("event_time") or event.get("observed_at"),
                        "sources": set(),
                        "count": 0
                    }
                users[user_id]["sources"].add(event.get("source_system"))
                users[user_id]["count"] += 1
                users[user_id]["last_active"] = max(
                    users[user_id]["last_active"],
                    event.get("event_time") or event.get("observed_at")
                )
                
        for user_id, data in users.items():
            for source_system in data["sources"]:
                self.neo4j_writer.create_or_update_user(
                    user_id=user_id,
                    source_system=source_system,
                    first_seen=data["first_seen"],
                    last_active=data["last_active"],
                    interaction_count=data["count"]
                )
                
        # Create intent nodes
        for event_id, intent_list in intents.items():
            for intent in intent_list:
                intent_id = f"intent_{event_id}_{intent['type']}"
                self.neo4j_writer.create_intent_node(
                    intent_id=intent_id,
                    intent_type=intent["type"],
                    text=intent["text"],
                    confidence=intent["confidence"],
                    actor_id=intent.get("actor_id"),
                    source_event_ids=[event_id],
                    timestamp=intent["timestamp"]
                )
                
    async def _generate_embeddings(
        self,
        events: List[Dict[str, Any]],
        interpretation_id: str
    ):
        """Generate embeddings for threads with enough messages."""
        if not self.embedding_generator:
            return
            
        # Group events by thread_id
        threads = {}
        for event in events:
            # Only Slack has explicit thread_id for now
            if event.get("source_system") == "slack":
                metadata = event.get("metadata", {})
                if isinstance(metadata, str):
                    import json
                    metadata = json.loads(metadata)
                thread_id = metadata.get("thread_ts") or event.get("channel_id")
                if thread_id:
                    if thread_id not in threads:
                        threads[thread_id] = []
                    threads[thread_id].append(event)
                    
        # Generate embeddings for threads with enough messages
        for thread_id, thread_events in threads.items():
            if len(thread_events) >= self.min_thread_messages:
                await self.embedding_generator.generate_thread_embedding(
                    thread_id=thread_id,
                    events=thread_events,
                    interpretation_id=interpretation_id
                )
                
    async def run(self):
        """Main processing loop."""
        self.running = True
        logger.info("🚀 Interpreter engine started")
        
        try:
            while self.running:
                # Calculate next window
                window_start = self.last_checkpoint
                window_end = window_start + timedelta(seconds=self.batch_window_seconds)
                
                # Don't process future windows
                if window_end > datetime.now(timezone.utc):
                    logger.debug("Caught up to present, waiting...")
                    await asyncio.sleep(self.processing_interval)
                    continue
                    
                # Process window
                interpretation_id = await self.process_window(window_start, window_end)
                
                # Move checkpoint forward
                self.last_checkpoint = window_end
                
                # Brief pause between windows
                await asyncio.sleep(1)
                
        except KeyboardInterrupt:
            logger.info("⚠️ Received shutdown signal")
        except Exception as e:
            logger.error(f"❌ Engine error: {e}", exc_info=True)
        finally:
            await self.shutdown()


# Main entry point
async def main():
    """Run the Interpreter engine."""
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO"),
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    
    engine = InterpreterEngine(
        batch_window_seconds=int(os.getenv("BATCH_WINDOW_SECONDS", 30)),
        batch_size=int(os.getenv("BATCH_SIZE", 1000)),
        processing_interval=int(os.getenv("PROCESSING_INTERVAL_SECONDS", 30))
    )
    
    await engine.setup()
    await engine.run()


if __name__ == "__main__":
    asyncio.run(main())
