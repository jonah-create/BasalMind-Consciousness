"""
Interpreter Engine - Event-Driven Real-Time Processing

Reads events from TimescaleDB as they arrive (cursor-based, no artificial time windows).
Each poll fetches all events observed after the last checkpoint.
Processing is immediate — events get their real timestamps, no 30s delay.

Pipeline per batch:
  1. Fetch new events (observed_at > checkpoint)
  2. Deduplicate
  3. Extract intents
  4. Detect correlations & anomalies
  5. Write interpretation to PostgreSQL
  6. Create Neo4j graph nodes
  7. Analyze threads
  8. Track sessions
  9. Generate channel summaries (event-driven threshold)
 10. Track intent embeddings
 11. Generate thread embeddings
 12. Advance checkpoint to last event's observed_at
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
from interpreter.intent_embeddings import IntentEmbeddingTracker
from interpreter.channel_summarizer import ChannelSummarizer
from interpreter.session_tracker import SessionTracker
from interpreter.thread_analyzer import ThreadAnalyzer

logger = logging.getLogger(__name__)


class InterpreterEngine:
    """
    Event-driven interpreter engine.

    Polls TimescaleDB for new events after a cursor position.
    No fixed time windows — events are processed as they arrive.
    """

    def __init__(
        self,
        poll_interval_seconds: int = 2,
        batch_size: int = 200,
        # kept for compat, not used
        batch_window_seconds: int = 30,
        processing_interval: int = 2,
    ):
        self.poll_interval_seconds = poll_interval_seconds
        self.batch_size = batch_size

        # Components (initialized in setup())
        self.timescale_reader: Optional[TimescaleReader] = None
        self.intent_extractor: Optional[IntentExtractor] = None
        self.event_deduplicator: Optional[EventDeduplicator] = None
        self.postgres_writer: Optional[PostgresWriter] = None
        self.neo4j_writer: Optional[Neo4jWriter] = None
        self.embedding_generator: Optional[EmbeddingGenerator] = None
        self.intent_embedding_tracker: Optional[IntentEmbeddingTracker] = None
        self.channel_summarizer: Optional[ChannelSummarizer] = None
        self.session_tracker: Optional[SessionTracker] = None
        self.thread_analyzer: Optional[ThreadAnalyzer] = None

        # Cursor: last observed_at we've fully processed
        self.last_checkpoint: Optional[datetime] = None
        self.running = False

    async def setup(self):
        """Initialize all components."""
        load_dotenv()

        # TimescaleDB reader (cursor-based, no window ceiling)
        self.timescale_reader = TimescaleReader(
            host=os.getenv("TIMESCALE_HOST", "localhost"),
            port=int(os.getenv("TIMESCALE_PORT", 5432)),
            database=os.getenv("TIMESCALE_DB", "basalmind_events_timescaledb"),
            user=os.getenv("TIMESCALE_USER", "basalmind"),
            password=os.getenv("TIMESCALE_PASSWORD"),
            batch_size=self.batch_size,
            lag_seconds=int(os.getenv("READER_LAG_SECONDS", 2))
        )
        await self.timescale_reader.connect()

        # Intent extractor
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

        # Neo4j writer
        self.neo4j_writer = Neo4jWriter(
            uri=os.getenv("NEO4J_URI", "bolt://localhost:7687"),
            user=os.getenv("NEO4J_USER", "neo4j"),
            password=os.getenv("NEO4J_PASSWORD"),
            database=os.getenv("NEO4J_DATABASE", "neo4j"),
            namespace=os.getenv("NEO4J_NAMESPACE", "Interpreter")
        )
        self.neo4j_writer.connect()

        # Embedding generator (optional)
        openai_key = os.getenv("OPENAI_API_KEY")
        if openai_key and openai_key != "sk-your-key-here":
            self.embedding_generator = EmbeddingGenerator(
                openai_api_key=openai_key,
                model_name=os.getenv("EMBEDDING_MODEL", "text-embedding-ada-002"),
                postgres_pool=self.postgres_writer.pool
            )
        else:
            logger.warning("No OpenAI API key configured, embeddings disabled")

        # Intent embedding tracker
        self.intent_embedding_tracker = IntentEmbeddingTracker(
            postgres_pool=self.postgres_writer.pool
        )

        # Channel summarizer (DB-backed event-driven counters)
        self.channel_summarizer = ChannelSummarizer(
            postgres_pool=self.postgres_writer.pool,
            drift_threshold=float(os.getenv("SUMMARY_DRIFT_THRESHOLD", 0.3))
        )
        await self.channel_summarizer.initialize()

        # Session tracker
        self.session_tracker = SessionTracker(
            postgres_pool=self.postgres_writer.pool,
            session_timeout_minutes=int(os.getenv("SESSION_TIMEOUT_MINUTES", 30))
        )

        # Thread analyzer
        self.thread_analyzer = ThreadAnalyzer(
            postgres_pool=self.postgres_writer.pool
        )

        # Load checkpoint — resume from last processed event
        checkpoint = await self.postgres_writer.get_latest_checkpoint()
        if checkpoint:
            self.last_checkpoint = checkpoint["last_processed_time"]
            logger.info(f"Resuming from checkpoint: {self.last_checkpoint.isoformat()}")
        else:
            self.last_checkpoint = datetime.now(timezone.utc) - timedelta(minutes=5)
            logger.info(f"Starting fresh from: {self.last_checkpoint.isoformat()}")

        logger.info("Interpreter engine initialized (event-driven mode)")

    async def shutdown(self):
        """Clean shutdown."""
        logger.info("Shutting down Interpreter engine...")
        if self.timescale_reader:
            await self.timescale_reader.close()
        if self.postgres_writer:
            await self.postgres_writer.close()
        if self.neo4j_writer:
            self.neo4j_writer.close()
        logger.info("Interpreter engine shutdown complete")

    async def process_batch(self, events: List[Dict[str, Any]]) -> Optional[str]:
        """
        Run the full interpretation pipeline on a batch of events.

        Args:
            events: List of event dicts from TimescaleDB (already deduped)

        Returns:
            interpretation_id
        """
        start_ms = datetime.now(timezone.utc)

        logger.info(
            f"\n{'='*60}\n"
            f"Processing {len(events)} events "
            f"(sources: {set(e['source_system'] for e in events)})\n"
            f"{'='*60}"
        )

        # Extract intents
        intents = self.intent_extractor.extract_from_batch(events)

        # Detect correlations & anomalies
        correlations = self._detect_correlations(events)
        anomalies = self._detect_anomalies(events)

        processing_ms = int(
            (datetime.now(timezone.utc) - start_ms).total_seconds() * 1000
        )

        # Write interpretation
        interpretation_id = await self.postgres_writer.write_interpretation(
            window_start=events[0]["observed_at"],
            window_end=events[-1]["observed_at"],
            events=events,
            intents=intents,
            correlations=correlations,
            anomalies=anomalies,
            processing_duration_ms=processing_ms
        )

        # Parallel processing of downstream consumers
        await self._create_graph_nodes(events, intents)
        await self._analyze_threads(events, intents)
        await self._track_user_sessions(events, intents)
        await self._generate_channel_summaries(events)
        await self._track_intent_embeddings(events, intents)
        await self._generate_embeddings(events, interpretation_id)

        logger.info(
            f"Batch complete: {interpretation_id[:8]}... "
            f"({processing_ms}ms, {len(intents)} events with intents)"
        )

        return interpretation_id

    async def run(self):
        """
        Main event-driven processing loop.

        Polls TimescaleDB for new events every poll_interval_seconds.
        Advances the cursor to the last event's observed_at after each batch.
        """
        self.running = True
        logger.info(
            f"Interpreter engine started "
            f"(poll_interval={self.poll_interval_seconds}s, "
            f"batch_size={self.batch_size})"
        )

        try:
            while self.running:
                # Fetch all events after cursor
                events = await self.timescale_reader.get_batch(
                    after_time=self.last_checkpoint
                )

                if events:
                    # Deduplicate
                    events = self.event_deduplicator.deduplicate(events)

                    if events:
                        await self.process_batch(events)

                        # Advance cursor to last event's observed_at
                        new_cursor = events[-1]["observed_at"]
                        self.last_checkpoint = new_cursor

                        # Persist checkpoint
                        await self.postgres_writer.update_checkpoint(
                            last_processed_time=new_cursor,
                            last_event_id=events[-1]["event_id"],
                            records_processed=len(events)
                        )

                # Always sleep between polls
                await asyncio.sleep(self.poll_interval_seconds)

        except KeyboardInterrupt:
            logger.info("Received shutdown signal")
        except Exception as e:
            logger.error(f"Engine error: {e}", exc_info=True)
        finally:
            await self.shutdown()

    # ── Correlation & anomaly detection ──────────────────────────────────────

    def _detect_correlations(self, events: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Detect cross-source correlations (Phase 1: user/IP based)."""
        correlations = []
        by_user: Dict[str, List] = {}
        by_ip: Dict[str, List] = {}

        for event in events:
            user_id = event.get("user_id")
            if user_id:
                by_user.setdefault(user_id, []).append(event)

            metadata = event.get("metadata", {})
            if isinstance(metadata, dict):
                ip = metadata.get("client_ip") or metadata.get("ip")
                if ip:
                    by_ip.setdefault(ip, []).append(event)

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
            logger.info(f"Detected {len(correlations)} cross-source correlations")

        return correlations

    def _detect_anomalies(self, events: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Detect anomalies (Phase 1: volume-based)."""
        anomalies = []
        by_source: Dict[str, int] = {}
        for event in events:
            src = event.get("source_system", "unknown")
            by_source[src] = by_source.get(src, 0) + 1

        for source, count in by_source.items():
            if count > 100:
                anomalies.append({
                    "type": "high_volume",
                    "source_system": source,
                    "event_count": count,
                    "severity": "medium",
                    "confidence": 0.7
                })

        if anomalies:
            logger.warning(f"Detected {len(anomalies)} anomalies")

        return anomalies

    # ── Downstream consumers ──────────────────────────────────────────────────

    async def _create_graph_nodes(
        self,
        events: List[Dict[str, Any]],
        intents: Dict[str, List[Dict[str, Any]]]
    ):
        """Create Neo4j graph nodes for this batch."""
        users: Dict[str, Any] = {}
        for event in events:
            user_id = event.get("user_id")
            if not user_id:
                continue
            ts = event.get("event_time") or event.get("observed_at")
            if user_id not in users:
                users[user_id] = {
                    "first_seen": ts,
                    "last_active": ts,
                    "sources": set(),
                    "count": 0
                }
            users[user_id]["sources"].add(event.get("source_system"))
            users[user_id]["count"] += 1
            if ts and ts > users[user_id]["last_active"]:
                users[user_id]["last_active"] = ts

        for user_id, data in users.items():
            for source_system in data["sources"]:
                try:
                    self.neo4j_writer.create_or_update_user(
                        user_id=user_id,
                        source_system=source_system,
                        first_seen=data["first_seen"],
                        last_active=data["last_active"],
                        interaction_count=data["count"]
                    )
                except Exception as e:
                    logger.error(f"Neo4j user node error for {user_id}: {e}")

        for event_id, intent_list in intents.items():
            for intent in intent_list:
                intent_id = f"intent_{event_id}_{intent['type']}"
                try:
                    self.neo4j_writer.create_intent_node(
                        intent_id=intent_id,
                        intent_type=intent["type"],
                        category=intent["category"],
                        text=intent["text"],
                        confidence=intent["confidence"],
                        actor_id=intent.get("actor_id"),
                        source_event_ids=[event_id],
                        timestamp=intent["timestamp"]
                    )
                except Exception as e:
                    logger.error(f"Neo4j intent node error: {e}")

    async def _analyze_threads(
        self,
        events: List[Dict[str, Any]],
        intents: Dict[str, List[Dict[str, Any]]]
    ):
        """Analyze conversation threads."""
        try:
            threads = await self.thread_analyzer.process_events(events, intents)
            if threads:
                logger.debug(f"Analyzed {len(threads)} threads")
        except Exception as e:
            logger.error(f"Error analyzing threads: {e}", exc_info=True)

    async def _track_user_sessions(
        self,
        events: List[Dict[str, Any]],
        intents: Dict[str, List[Dict[str, Any]]]
    ):
        """Track user sessions and journeys."""
        try:
            sessions = await self.session_tracker.process_events(events, intents)
            if sessions:
                logger.debug(f"Updated {len(sessions)} user sessions")
        except Exception as e:
            logger.error(f"Error tracking sessions: {e}", exc_info=True)

    async def _generate_channel_summaries(self, events: List[Dict[str, Any]]):
        """Generate channel summaries when event-driven thresholds are met."""
        try:
            summaries = await self.channel_summarizer.process_events(events)
            if summaries:
                logger.info(f"Created {len(summaries)} channel summaries")
        except Exception as e:
            logger.error(f"Error generating summaries: {e}", exc_info=True)

    async def _track_intent_embeddings(
        self,
        events: List[Dict[str, Any]],
        intents: Dict[str, List[Dict[str, Any]]]
    ):
        """Track intent co-occurrence metadata."""
        if not self.intent_embedding_tracker:
            return
        try:
            thread_intents: Dict[str, list] = {}
            for event_id, event_intents in intents.items():
                event = next((e for e in events if e.get("event_id") == event_id), None)
                if not event:
                    continue
                if event.get("source_system") == "slack":
                    import json as _json
                    metadata = event.get("metadata", {})
                    if isinstance(metadata, str):
                        metadata = _json.loads(metadata)
                    thread_id = metadata.get("thread_ts") or event.get("thread_id") or event.get("channel_id")
                    if thread_id:
                        thread_intents.setdefault(thread_id, []).extend(event_intents)

            for thread_id, t_intents in thread_intents.items():
                if len(t_intents) >= 2:
                    await self.intent_embedding_tracker.track_intent_cooccurrence(
                        intents=t_intents,
                        context_id=thread_id,
                        context_type="thread"
                    )
        except Exception as e:
            logger.error(f"Error tracking intent embeddings: {e}", exc_info=True)

    async def _generate_embeddings(
        self,
        events: List[Dict[str, Any]],
        interpretation_id: str
    ):
        """
        Generate embeddings for every Slack turn and update each thread's composite embedding.

        For each Slack event in the batch:
          1. Embed the individual turn (message) immediately — no threshold.
          2. Group events by thread_id, then update the thread's composite embedding
             using all turns seen in this batch.

        Thread embedding is always overwritten to reflect the latest context.
        """
        if not self.embedding_generator:
            return
        try:
            import json as _json
            threads: Dict[str, list] = {}

            for event in events:
                if event.get("source_system") != "slack":
                    continue

                event_id = event.get("event_id")
                text = event.get("text", "")
                metadata = event.get("metadata", {})
                if isinstance(metadata, str):
                    metadata = _json.loads(metadata)

                thread_id = (
                    metadata.get("thread_ts")
                    or event.get("thread_id")
                    or event.get("channel_id")
                )

                # 1. Embed this individual turn
                if event_id and text:
                    await self.embedding_generator.generate_turn_embedding(
                        event_id=event_id,
                        text=text,
                        thread_id=thread_id or event_id,
                        interpretation_id=interpretation_id
                    )

                # 2. Accumulate into thread group for composite embedding
                if thread_id:
                    threads.setdefault(thread_id, []).append(event)

            # Update composite thread embedding for each thread touched this batch
            # Also track which channels had threads updated (for channel composite)
            channels: Dict[str, list] = {}  # channel_id -> [thread_ids]

            for thread_id, thread_events in threads.items():
                # Fetch thread context for richer history snapshots
                thread_ctx = None
                try:
                    async with self.postgres_writer.pool.acquire() as _conn:
                        thread_ctx = await _conn.fetchrow(
                            """
                            SELECT channel_id, phase, pattern,
                                   message_count,
                                   jsonb_array_length(
                                       COALESCE(participants, '[]'::jsonb)
                                   ) AS participant_count
                            FROM thread_contexts
                            WHERE thread_id = $1
                            """,
                            thread_id
                        )
                except Exception as _ctx_err:
                    logger.debug(f"Could not fetch thread context for {thread_id}: {_ctx_err}")

                await self.embedding_generator.generate_thread_embedding(
                    thread_id=thread_id,
                    events=thread_events,
                    interpretation_id=interpretation_id,
                    channel_id=thread_ctx["channel_id"] if thread_ctx else None,
                    phase=thread_ctx["phase"] if thread_ctx else None,
                    pattern=thread_ctx["pattern"] if thread_ctx else None,
                    participant_count=thread_ctx["participant_count"] if thread_ctx else None,
                    message_count=thread_ctx["message_count"] if thread_ctx else len(thread_events),
                )

                # Collect channel → thread mapping for channel composite
                for event in thread_events:
                    channel_id = event.get("channel_id")
                    if channel_id:
                        if channel_id not in channels:
                            channels[channel_id] = []
                        if thread_id not in channels[channel_id]:
                            channels[channel_id].append(thread_id)

            # Update composite channel embedding for each channel touched this batch
            for channel_id, channel_thread_ids in channels.items():
                await self.embedding_generator.generate_channel_embedding(
                    channel_id=channel_id,
                    thread_ids=channel_thread_ids,
                    interpretation_id=interpretation_id
                )

        except Exception as e:
            logger.error(f"Error generating embeddings: {e}", exc_info=True)

    # ── process_window kept for backward compat with tests ────────────────────

    async def process_window(
        self,
        start_time: datetime,
        end_time: datetime
    ) -> Optional[str]:
        """Backward-compat wrapper — fetches events in [start, end] and processes them."""
        events = await self.timescale_reader.get_batch(
            after_time=start_time,
            limit=self.batch_size
        )
        # Filter to window end
        events = [e for e in events if e["observed_at"] <= end_time]
        if not events:
            return None
        events = self.event_deduplicator.deduplicate(events)
        if not events:
            return None
        return await self.process_batch(events)


# ── Entry point ───────────────────────────────────────────────────────────────

async def main():
    """Run the Interpreter engine."""
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO"),
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )

    engine = InterpreterEngine(
        poll_interval_seconds=int(os.getenv("POLL_INTERVAL_SECONDS", 2)),
        batch_size=int(os.getenv("BATCH_SIZE", 200)),
    )

    await engine.setup()
    await engine.run()


if __name__ == "__main__":
    asyncio.run(main())
