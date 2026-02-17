"""
Interpreter Engine - Event-Driven Real-Time Processing

Hardening additions vs original:
  - Redis checkpoint: survives restarts without reprocessing old events
  - Health endpoint: lightweight HTTP on HEALTH_PORT (default 5003)
  - Langfuse tracing: one trace per batch with duration + counts
"""

import asyncio
import logging
import os
import json
import threading
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer
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

# ── Health endpoint ───────────────────────────────────────────────────────────

class _HealthState:
    status: str = "starting"
    checkpoint: Optional[str] = None
    batches_processed: int = 0
    events_processed: int = 0
    last_batch_at: Optional[str] = None
    last_error: Optional[str] = None

_health = _HealthState()


class _HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path in ("/health", "/"):
            body = json.dumps({
                "status": _health.status,
                "checkpoint": _health.checkpoint,
                "batches_processed": _health.batches_processed,
                "events_processed": _health.events_processed,
                "last_batch_at": _health.last_batch_at,
                "last_error": _health.last_error,
            }).encode()
            code = 200 if _health.status == "ok" else 503
            self.send_response(code)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(body)
        else:
            self.send_response(404)
            self.end_headers()

    def log_message(self, format, *args):
        pass  # suppress access logs


def _start_health_server(port: int):
    server = HTTPServer(("0.0.0.0", port), _HealthHandler)
    t = threading.Thread(target=server.serve_forever, daemon=True)
    t.start()
    logger.info(f"Health endpoint: http://0.0.0.0:{port}/health")


# ── Redis checkpoint ──────────────────────────────────────────────────────────

REDIS_CHECKPOINT_KEY = "interpreter:checkpoint"


def _redis_client():
    try:
        import redis as _redis
        r = _redis.Redis(
            host=os.getenv("REDIS_HOST", "localhost"),
            port=int(os.getenv("REDIS_PORT", 6390)),
            password=os.getenv("REDIS_PASSWORD"),
            decode_responses=True,
            socket_connect_timeout=2,
        )
        r.ping()
        logger.info("Redis connected — checkpoint will persist across restarts")
        return r
    except Exception as e:
        logger.warning(f"Redis unavailable — checkpoint won't persist: {e}")
        return None


def _load_redis_checkpoint(r) -> Optional[datetime]:
    if not r:
        return None
    try:
        val = r.get(REDIS_CHECKPOINT_KEY)
        if val:
            return datetime.fromisoformat(val)
    except Exception as e:
        logger.warning(f"Could not read Redis checkpoint: {e}")
    return None


def _save_redis_checkpoint(r, ts: datetime):
    if not r:
        return
    try:
        r.set(REDIS_CHECKPOINT_KEY, ts.isoformat())
    except Exception as e:
        logger.warning(f"Could not save Redis checkpoint: {e}")


# ── Langfuse ──────────────────────────────────────────────────────────────────

def _init_langfuse():
    try:
        from langfuse import Langfuse
        lf = Langfuse(
            host=os.getenv("LANGFUSE_HOST"),
            public_key=os.getenv("LANGFUSE_PUBLIC_KEY"),
            secret_key=os.getenv("LANGFUSE_SECRET_KEY"),
        )
        logger.info("Langfuse tracing enabled")
        return lf
    except Exception as e:
        logger.warning(f"Langfuse not available — tracing disabled: {e}")
        return None


# ── Engine ────────────────────────────────────────────────────────────────────

class InterpreterEngine:
    def __init__(
        self,
        poll_interval_seconds: int = 2,
        batch_size: int = 200,
        batch_window_seconds: int = 30,  # kept for compat
        processing_interval: int = 2,    # kept for compat
    ):
        self.poll_interval_seconds = poll_interval_seconds
        self.batch_size = batch_size
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
        self.last_checkpoint: Optional[datetime] = None
        self.running = False
        self._redis = None
        self._langfuse = None

    async def setup(self):
        load_dotenv()

        self._redis = _redis_client()
        self._langfuse = _init_langfuse()

        health_port = int(os.getenv("HEALTH_PORT", 5003))
        _start_health_server(health_port)
        _health.status = "starting"

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

        self.intent_extractor = IntentExtractor(confidence_threshold=0.5)
        self.event_deduplicator = EventDeduplicator()

        self.postgres_writer = PostgresWriter(
            host=os.getenv("POSTGRES_HOST", "localhost"),
            port=int(os.getenv("POSTGRES_PORT", 5432)),
            database=os.getenv("POSTGRES_DB", "basalmind_interpreter_postgresql"),
            user=os.getenv("POSTGRES_USER", "basalmind"),
            password=os.getenv("POSTGRES_PASSWORD")
        )
        await self.postgres_writer.connect()

        self.neo4j_writer = Neo4jWriter(
            uri=os.getenv("NEO4J_URI", "bolt://localhost:7687"),
            user=os.getenv("NEO4J_USER", "neo4j"),
            password=os.getenv("NEO4J_PASSWORD"),
            database=os.getenv("NEO4J_DATABASE", "neo4j"),
            namespace=os.getenv("NEO4J_NAMESPACE", "interpreter")
        )
        self.neo4j_writer.connect()

        openai_key = os.getenv("OPENAI_API_KEY")
        if openai_key and openai_key != "sk-your-key-here":
            self.embedding_generator = EmbeddingGenerator(
                openai_api_key=openai_key,
                model_name=os.getenv("EMBEDDING_MODEL", "text-embedding-ada-002"),
                postgres_pool=self.postgres_writer.pool
            )
        else:
            logger.warning("No OpenAI API key — embeddings disabled")

        self.intent_embedding_tracker = IntentEmbeddingTracker(
            postgres_pool=self.postgres_writer.pool
        )

        self.channel_summarizer = ChannelSummarizer(
            postgres_pool=self.postgres_writer.pool,
            drift_threshold=float(os.getenv("SUMMARY_DRIFT_THRESHOLD", 0.3)),
            openai_api_key=os.getenv("OPENAI_API_KEY"),
            openai_model=os.getenv("SUMMARY_LLM_MODEL", "gpt-4o-mini"),
        )
        await self.channel_summarizer.initialize()

        self.session_tracker = SessionTracker(
            postgres_pool=self.postgres_writer.pool,
            session_timeout_minutes=int(os.getenv("SESSION_TIMEOUT_MINUTES", 30))
        )

        self.thread_analyzer = ThreadAnalyzer(
            postgres_pool=self.postgres_writer.pool
        )

        # Checkpoint: Redis (fastest) → Postgres → fresh 5-min lookback
        redis_ckpt = _load_redis_checkpoint(self._redis)
        if redis_ckpt:
            self.last_checkpoint = redis_ckpt
            logger.info(f"Resuming from Redis checkpoint: {self.last_checkpoint.isoformat()}")
        else:
            pg_ckpt = await self.postgres_writer.get_latest_checkpoint()
            if pg_ckpt:
                self.last_checkpoint = pg_ckpt["last_processed_time"]
                logger.info(f"Resuming from Postgres checkpoint: {self.last_checkpoint.isoformat()}")
            else:
                self.last_checkpoint = datetime.now(timezone.utc) - timedelta(minutes=5)
                logger.info(f"Starting fresh from: {self.last_checkpoint.isoformat()}")

        _health.status = "ok"
        _health.checkpoint = self.last_checkpoint.isoformat()
        logger.info("Interpreter engine initialized (event-driven mode)")

    async def shutdown(self):
        _health.status = "stopping"
        logger.info("Shutting down Interpreter engine...")
        if self._langfuse:
            try:
                self._langfuse.flush()
            except Exception:
                pass
        if self.timescale_reader:
            await self.timescale_reader.close()
        if self.postgres_writer:
            await self.postgres_writer.close()
        if self.neo4j_writer:
            self.neo4j_writer.close()
        logger.info("Interpreter engine shutdown complete")

    async def process_batch(self, events: List[Dict[str, Any]]) -> Optional[str]:
        start_time = datetime.now(timezone.utc)
        trace = None

        if self._langfuse:
            try:
                sources = list({e.get("source_system", "unknown") for e in events})
                trace = self._langfuse.trace(
                    name="interpreter.process_batch",
                    input={"event_count": len(events), "sources": sources},
                    tags=["interpreter", "batch"],
                )
            except Exception as lf_err:
                logger.debug(f"Langfuse trace start failed: {lf_err}")

        logger.info(
            f"\n{'='*60}\n"
            f"Processing {len(events)} events "
            f"(sources: {set(e['source_system'] for e in events)})\n"
            f"{'='*60}"
        )

        try:
            intents = self.intent_extractor.extract_from_batch(events)
            correlations = self._detect_correlations(events)
            anomalies = self._detect_anomalies(events)

            processing_ms = int(
                (datetime.now(timezone.utc) - start_time).total_seconds() * 1000
            )

            thread_data = await self._analyze_threads(events, intents)
            graph_node_ids = await self._create_graph_nodes(events, intents, thread_data)

            interpretation_id = await self.postgres_writer.write_interpretation(
                window_start=events[0]["observed_at"],
                window_end=events[-1]["observed_at"],
                events=events,
                intents=intents,
                correlations=correlations,
                anomalies=anomalies,
                processing_duration_ms=processing_ms,
                graph_node_ids=graph_node_ids
            )
            await self._track_user_sessions(events, intents)
            await self._generate_channel_summaries(events)
            await self._track_intent_embeddings(events, intents)
            await self._generate_embeddings(events, interpretation_id)

            total_ms = int(
                (datetime.now(timezone.utc) - start_time).total_seconds() * 1000
            )
            intent_count = sum(len(v) for v in intents.values())

            logger.info(
                f"Batch complete: {interpretation_id[:8]}... "
                f"({total_ms}ms, {intent_count} intents)"
            )

            if trace:
                try:
                    trace.update(output={
                        "interpretation_id": interpretation_id,
                        "intent_count": intent_count,
                        "correlations": len(correlations),
                        "anomalies": len(anomalies),
                        "duration_ms": total_ms,
                    })
                except Exception:
                    pass

            return interpretation_id

        except Exception as e:
            _health.last_error = str(e)
            if trace:
                try:
                    trace.update(output={"error": str(e)}, level="ERROR")
                except Exception:
                    pass
            raise

    async def run(self):
        self.running = True
        logger.info(
            f"Interpreter engine started "
            f"(poll_interval={self.poll_interval_seconds}s, "
            f"batch_size={self.batch_size})"
        )

        try:
            while self.running:
                events = await self.timescale_reader.get_batch(
                    after_time=self.last_checkpoint
                )

                if events:
                    events = self.event_deduplicator.deduplicate(events)
                    if events:
                        await self.process_batch(events)

                        new_cursor = events[-1]["observed_at"]
                        self.last_checkpoint = new_cursor

                        # Persist to both stores
                        await self.postgres_writer.update_checkpoint(
                            last_processed_time=new_cursor,
                            last_event_id=events[-1]["event_id"],
                            records_processed=len(events)
                        )
                        _save_redis_checkpoint(self._redis, new_cursor)

                        # Update health
                        _health.checkpoint = new_cursor.isoformat()
                        _health.batches_processed += 1
                        _health.events_processed += len(events)
                        _health.last_batch_at = datetime.now(timezone.utc).isoformat()
                        _health.last_error = None

                await asyncio.sleep(self.poll_interval_seconds)

        except KeyboardInterrupt:
            logger.info("Received shutdown signal")
        except Exception as e:
            _health.status = "error"
            _health.last_error = str(e)
            logger.error(f"Engine error: {e}", exc_info=True)
        finally:
            await self.shutdown()

    # ── Correlation & anomaly detection ──────────────────────────────────────

    def _detect_correlations(self, events):
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
        for user_id, ue in by_user.items():
            if len({e["source_system"] for e in ue}) > 1:
                correlations.append({"type": "cross_source_user", "user_id": user_id,
                    "sources": list({e["source_system"] for e in ue}), "event_count": len(ue), "confidence": 0.9})
        for ip, ie in by_ip.items():
            if len({e["source_system"] for e in ie}) > 1:
                correlations.append({"type": "cross_source_ip", "ip_address": ip,
                    "sources": list({e["source_system"] for e in ie}), "event_count": len(ie), "confidence": 0.8})
        if correlations:
            logger.info(f"Detected {len(correlations)} cross-source correlations")
        return correlations

    def _detect_anomalies(self, events):
        anomalies = []
        by_source: Dict[str, int] = {}
        for event in events:
            src = event.get("source_system", "unknown")
            by_source[src] = by_source.get(src, 0) + 1
        for source, count in by_source.items():
            if count > 100:
                anomalies.append({"type": "high_volume", "source_system": source,
                    "event_count": count, "severity": "medium", "confidence": 0.7})
        if anomalies:
            logger.warning(f"Detected {len(anomalies)} anomalies")
        return anomalies

    # ── Downstream consumers ──────────────────────────────────────────────────

    async def _create_graph_nodes(
        self,
        events,
        intents,
        thread_data: Optional[List[Dict[str, Any]]] = None,
    ) -> List[str]:
        """Create Neo4j nodes and return all node IDs created."""
        node_ids: List[str] = []
        users: Dict[str, Any] = {}
        for event in events:
            user_id = event.get("user_id")
            if not user_id:
                continue
            ts = event.get("event_time") or event.get("observed_at")
            if user_id not in users:
                users[user_id] = {"first_seen": ts, "last_active": ts, "sources": set(), "count": 0}
            users[user_id]["sources"].add(event.get("source_system"))
            users[user_id]["count"] += 1
            if ts and ts > users[user_id]["last_active"]:
                users[user_id]["last_active"] = ts
        for user_id, data in users.items():
            for source_system in data["sources"]:
                try:
                    nid = self.neo4j_writer.create_or_update_user(
                        user_id=user_id, source_system=source_system,
                        first_seen=data["first_seen"], last_active=data["last_active"],
                        interaction_count=data["count"])
                    if nid and nid not in node_ids:
                        node_ids.append(nid)
                except Exception as e:
                    logger.error(f"Neo4j user node error for {user_id}: {e}")
        for event_id, intent_list in intents.items():
            for intent in intent_list:
                try:
                    nid = self.neo4j_writer.create_intent_node(
                        intent_id=f"intent_{event_id}_{intent['type']}",
                        intent_type=intent["type"], category=intent["category"],
                        text=intent["text"], confidence=intent["confidence"],
                        actor_id=intent.get("actor_id"), source_event_ids=[event_id],
                        timestamp=intent["timestamp"])
                    if nid and nid not in node_ids:
                        node_ids.append(nid)
                except Exception as e:
                    logger.error(f"Neo4j intent node error: {e}")
        # ── Thread nodes ──
        for td in (thread_data or []):
            try:
                nid = self.neo4j_writer.create_thread_node(
                    thread_id=td["thread_id"],
                    topic=td.get("topic"),
                    phase=td.get("phase"),
                    started_at=td["started_at"],
                    last_activity=td["last_activity"],
                    source_event_ids=[eid for eid in td.get("event_ids", []) if eid],
                )
                if nid and nid not in node_ids:
                    node_ids.append(nid)
            except Exception as e:
                logger.error(f"Neo4j thread node error for {td.get('thread_id')}: {e}")
        return node_ids

    async def _analyze_threads(self, events, intents) -> List[Dict[str, Any]]:
        """Analyze threads and return thread data for Neo4j node creation."""
        try:
            context_ids = await self.thread_analyzer.process_events(events, intents)
            if context_ids:
                logger.debug(f"Analyzed {len(context_ids)} threads")
            # Return the in-memory thread data for Neo4j wiring
            thread_data = []
            for thread_id, td in self.thread_analyzer.active_threads.items():
                thread_data.append({
                    "thread_id": thread_id,
                    "channel_id": td.get("channel_id"),
                    "topic": td.get("topic"),
                    "phase": td.get("phase"),
                    "started_at": td.get("started_at"),
                    "last_activity": td.get("last_activity"),
                    "event_ids": [eid for eid in td.get("event_ids", []) if eid],
                })
            return thread_data
        except Exception as e:
            logger.error(f"Error analyzing threads: {e}", exc_info=True)
            return []

    async def _track_user_sessions(self, events, intents):
        try:
            sessions = await self.session_tracker.process_events(events, intents)
            if sessions:
                logger.debug(f"Updated {len(sessions)} user sessions")
        except Exception as e:
            logger.error(f"Error tracking sessions: {e}", exc_info=True)

    async def _generate_channel_summaries(self, events):
        try:
            summaries = await self.channel_summarizer.process_events(events)
            if summaries:
                logger.info(f"Created {len(summaries)} channel summaries")
        except Exception as e:
            logger.error(f"Error generating summaries: {e}", exc_info=True)

    async def _track_intent_embeddings(self, events, intents):
        if not self.intent_embedding_tracker:
            return
        try:
            thread_intents: Dict[str, list] = {}
            for event_id, event_intents in intents.items():
                event = next((e for e in events if e.get("event_id") == event_id), None)
                if not event or event.get("source_system") != "slack":
                    continue
                metadata = event.get("metadata", {})
                if isinstance(metadata, str):
                    metadata = json.loads(metadata)
                thread_id = metadata.get("thread_ts") or event.get("thread_id") or event.get("channel_id")
                if thread_id:
                    thread_intents.setdefault(thread_id, []).extend(event_intents)
            for thread_id, t_intents in thread_intents.items():
                if len(t_intents) >= 2:
                    await self.intent_embedding_tracker.track_intent_cooccurrence(
                        intents=t_intents, context_id=thread_id, context_type="thread")
        except Exception as e:
            logger.error(f"Error tracking intent embeddings: {e}", exc_info=True)

    async def _generate_embeddings(self, events, interpretation_id):
        if not self.embedding_generator:
            return
        try:
            threads: Dict[str, list] = {}
            for event in events:
                if event.get("source_system") != "slack":
                    continue
                event_id = event.get("event_id")
                text = event.get("text", "")
                metadata = event.get("metadata", {})
                if isinstance(metadata, str):
                    metadata = json.loads(metadata)
                thread_id = (metadata.get("thread_ts") or event.get("thread_id")
                             or event.get("channel_id"))
                if event_id and text:
                    await self.embedding_generator.generate_turn_embedding(
                        event_id=event_id, text=text,
                        thread_id=thread_id or event_id,
                        interpretation_id=interpretation_id)
                if thread_id:
                    threads.setdefault(thread_id, []).append(event)

            channels: Dict[str, list] = {}
            for thread_id, thread_events in threads.items():
                thread_ctx = None
                try:
                    async with self.postgres_writer.pool.acquire() as _conn:
                        thread_ctx = await _conn.fetchrow(
                            """SELECT channel_id, phase, pattern, message_count,
                                      jsonb_array_length(COALESCE(participants, '[]'::jsonb))
                                      AS participant_count
                               FROM thread_contexts WHERE thread_id = $1""",
                            thread_id)
                except Exception as _ctx_err:
                    logger.debug(f"Could not fetch thread context for {thread_id}: {_ctx_err}")

                await self.embedding_generator.generate_thread_embedding(
                    thread_id=thread_id, events=thread_events,
                    interpretation_id=interpretation_id,
                    channel_id=thread_ctx["channel_id"] if thread_ctx else None,
                    phase=thread_ctx["phase"] if thread_ctx else None,
                    pattern=thread_ctx["pattern"] if thread_ctx else None,
                    participant_count=thread_ctx["participant_count"] if thread_ctx else None,
                    message_count=thread_ctx["message_count"] if thread_ctx else len(thread_events),
                )
                for event in thread_events:
                    channel_id = event.get("channel_id")
                    if channel_id:
                        channels.setdefault(channel_id, [])
                        if thread_id not in channels[channel_id]:
                            channels[channel_id].append(thread_id)

            for channel_id, channel_thread_ids in channels.items():
                await self.embedding_generator.generate_channel_embedding(
                    channel_id=channel_id, thread_ids=channel_thread_ids,
                    interpretation_id=interpretation_id)

        except Exception as e:
            logger.error(f"Error generating embeddings: {e}", exc_info=True)

    # ── Backward-compat wrapper ───────────────────────────────────────────────

    async def process_window(self, start_time, end_time):
        events = await self.timescale_reader.get_batch(after_time=start_time, limit=self.batch_size)
        events = [e for e in events if e["observed_at"] <= end_time]
        if not events:
            return None
        events = self.event_deduplicator.deduplicate(events)
        return await self.process_batch(events) if events else None


# ── Entry point ───────────────────────────────────────────────────────────────

async def main():
    load_dotenv()
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
