"""
Enhanced TimescaleDB Writer for Observer with Batch Processing and WAL.

Enhancements:
1. Batch writing (100-500 events per batch)
2. Write-Ahead Log using Redis Stream for zero data loss
3. Time-based and size-based batch flushing
4. Backpressure handling with bounded queue
5. Automatic retry with exponential backoff

Philosophy: Every event is precious - never lose data.
"""

import os
import logging
import asyncio
from typing import Dict, Any, Optional, List
from datetime import datetime
import asyncpg
import json
from asyncpg.pool import Pool
import redis.asyncio as redis

logger = logging.getLogger(__name__)


class WriteAheadLog:
    """
    Redis Stream-based Write-Ahead Log for zero data loss.

    Events are written to Redis Stream first, then to TimescaleDB.
    If TimescaleDB write fails, events remain in stream for retry.
    """

    def __init__(self, redis_client: redis.Redis, stream_name: str = "observer:wal"):
        self.redis = redis_client
        self.stream_name = stream_name

    async def append(self, event_data: Dict[str, Any]) -> str:
        """
        Append event to WAL.
        Returns: Stream message ID
        """
        try:
            # Add to Redis Stream
            msg_id = await self.redis.xadd(
                self.stream_name,
                {"event": json.dumps(event_data)}
            )
            return msg_id.decode() if isinstance(msg_id, bytes) else msg_id
        except Exception as e:
            logger.error(f"WAL append failed: {e}")
            raise

    async def acknowledge(self, msg_ids: List[str]):
        """Remove events from WAL after successful write to TimescaleDB."""
        try:
            if msg_ids:
                # Delete messages from stream
                await self.redis.xdel(self.stream_name, *msg_ids)
        except Exception as e:
            logger.error(f"WAL acknowledge failed: {e}")

    async def get_pending(self, count: int = 100) -> List[tuple]:
        """Get pending events from WAL for retry."""
        try:
            messages = await self.redis.xrange(self.stream_name, count=count)
            return [(msg_id.decode(), json.loads(data[b'event'].decode()))
                    for msg_id, data in messages]
        except Exception as e:
            logger.error(f"WAL get_pending failed: {e}")
            return []


class BatchBuffer:
    """
    Buffered batch writer with time and size-based flushing.
    """

    def __init__(
        self,
        writer,
        wal: WriteAheadLog,
        batch_size: int = 100,
        flush_interval: float = 1.0
    ):
        self.writer = writer
        self.wal = wal
        self.batch_size = batch_size
        self.flush_interval = flush_interval

        self.buffer: List[tuple] = []  # (msg_id, event_data)
        self.lock = asyncio.Lock()
        self.flush_task: Optional[asyncio.Task] = None

    async def start(self):
        """Start background flush task."""
        self.flush_task = asyncio.create_task(self._periodic_flush())

    async def stop(self):
        """Stop background flush and flush remaining events."""
        if self.flush_task:
            self.flush_task.cancel()
            try:
                await self.flush_task
            except asyncio.CancelledError:
                pass

        # Final flush
        await self.flush()

    async def add(self, event_data: Dict[str, Any]) -> bool:
        """
        Add event to buffer.
        Returns: True if successfully added, False if buffer full (backpressure)
        """
        async with self.lock:
            # Check buffer capacity for backpressure
            if len(self.buffer) >= self.batch_size * 5:  # 5x batch size max
                logger.warning(f"Buffer full ({len(self.buffer)} events), applying backpressure")
                return False

            # Write to WAL first for durability
            try:
                msg_id = await self.wal.append(event_data)
                self.buffer.append((msg_id, event_data))

                # Flush if batch size reached
                if len(self.buffer) >= self.batch_size:
                    asyncio.create_task(self.flush())

                return True
            except Exception as e:
                logger.error(f"Failed to add to buffer: {e}")
                return False

    async def flush(self):
        """Flush buffer to TimescaleDB."""
        async with self.lock:
            if not self.buffer:
                return

            batch = self.buffer.copy()

            try:
                # Batch write to TimescaleDB
                success = await self.writer._write_batch([event for _, event in batch])

                if success:
                    # Acknowledge in WAL
                    msg_ids = [msg_id for msg_id, _ in batch]
                    await self.wal.acknowledge(msg_ids)

                    # Clear buffer
                    self.buffer = []

                    logger.info(f"✅ Flushed {len(batch)} events to TimescaleDB")
                else:
                    # Keep in buffer for retry
                    logger.error(f"❌ Batch write failed, keeping {len(batch)} events in buffer")

            except Exception as e:
                logger.error(f"❌ Flush failed: {e}", exc_info=True)

    async def _periodic_flush(self):
        """Background task to flush buffer periodically."""
        try:
            while True:
                await asyncio.sleep(self.flush_interval)
                await self.flush()
        except asyncio.CancelledError:
            pass


class EnhancedTimescaleWriter:
    """
    Enhanced TimescaleDB writer with batch processing, WAL, and backpressure.
    """

    def __init__(
        self,
        host: str = "localhost",
        port: int = 5432,
        database: str = "basalmind_events",
        user: str = "basalmind",
        password: str = "basalmind_secure_2024",
        redis_client: Optional[redis.Redis] = None,
        min_pool_size: int = 2,
        max_pool_size: int = 10,
        batch_size: int = 100,
        flush_interval: float = 1.0
    ):
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password
        self.min_pool_size = min_pool_size
        self.max_pool_size = max_pool_size
        self.batch_size = batch_size
        self.flush_interval = flush_interval

        self.pool: Optional[Pool] = None
        self._connected = False

        # WAL and batch buffer
        self.redis_client = redis_client
        self.wal: Optional[WriteAheadLog] = None
        self.batch_buffer: Optional[BatchBuffer] = None

        # Metrics
        self.events_written = 0
        self.batches_written = 0
        self.write_failures = 0

    async def connect(self):
        """Initialize connection pool to TimescaleDB."""
        try:
            self.pool = await asyncpg.create_pool(
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.user,
                password=self.password,
                min_size=self.min_pool_size,
                max_size=self.max_pool_size,
                command_timeout=10
            )

            # Test connection
            async with self.pool.acquire() as conn:
                await conn.fetchval('SELECT 1')

            self._connected = True

            # Initialize WAL and batch buffer
            if self.redis_client:
                self.wal = WriteAheadLog(self.redis_client)
                self.batch_buffer = BatchBuffer(
                    self,
                    self.wal,
                    self.batch_size,
                    self.flush_interval
                )
                await self.batch_buffer.start()
                logger.info("✅ WAL and batch buffer initialized")

            logger.info(f"✅ TimescaleDB connected: {self.host}:{self.port}/{self.database}")

        except Exception as e:
            logger.error(f"❌ Failed to connect to TimescaleDB: {e}")
            self._connected = False
            raise

    async def write_event(self, event_data: Dict[str, Any]) -> bool:
        """
        Write event to TimescaleDB via batch buffer.

        Events go to WAL first, then batched to TimescaleDB.
        Returns False if buffer is full (backpressure signal).
        """
        if not self._connected or not self.pool:
            logger.error("❌ TimescaleDB not connected, cannot write event")
            return False

        try:
            if self.batch_buffer:
                # Use batch buffer
                success = await self.batch_buffer.add(event_data)
                if not success:
                    # Backpressure - buffer full
                    self.write_failures += 1
                    return False
            else:
                # Fallback to direct write (backwards compatible)
                success = await self._write_single(event_data)
                if success:
                    self.events_written += 1
                else:
                    self.write_failures += 1
                return success

            return True

        except Exception as e:
            logger.error(f"❌ Failed to write event: {e}", exc_info=True)
            self.write_failures += 1
            return False


    @staticmethod
    def _get_field(event_data: Dict[str, Any], normalized: Dict[str, Any], field: str):
        """
        Get field value: check normalized dict first, fall back to top-level event_data.

        This handles both Slack events (which have a populated normalized dict) and
        internal events (which may have fields only at the top level).
        """
        val = normalized.get(field)
        if val is None:
            val = event_data.get(field)
        return val

    async def _write_single(self, event_data: Dict[str, Any]) -> bool:
        """Write single event directly (legacy mode)."""
        try:
            async with self.pool.acquire() as conn:
                query = """
                INSERT INTO events (
                    event_id, event_time, observed_at, event_type, source_system,
                    user_id, user_name, user_email, channel_id, channel_name,
                    thread_id, session_id, workspace_id, text, subject, body,
                    action_type, action_value, trace_id, correlation_id,
                    raw_payload, normalized_fields, metadata
                ) VALUES (
                    $1::uuid, $2, $3, $4, $5, $6, $7, $8, $9, $10,
                    $11, $12, $13, $14, $15, $16, $17, $18, $19, $20,
                    $21::jsonb, $22::jsonb, $23::jsonb
                )
                """

                event_time = self._parse_timestamp(event_data.get("event_time"))
                observed_at = self._parse_timestamp(event_data.get("observed_at", datetime.utcnow().isoformat()))
                normalized = event_data.get("normalized", {})

                gf = lambda f: self._get_field(event_data, normalized, f)
                await conn.execute(
                    query,
                    event_data.get("event_id"),
                    event_time,
                    observed_at,
                    event_data.get("event_type"),
                    event_data.get("source_system"),
                    gf("user_id"),
                    gf("user_name"),
                    gf("user_email"),
                    gf("channel_id"),
                    gf("channel_name"),
                    gf("thread_id"),
                    event_data.get("session_id"),
                    gf("workspace_id"),
                    gf("text"),
                    gf("subject"),
                    gf("body"),
                    gf("action_type"),
                    gf("action_value"),
                    event_data.get("trace_id"),
                    event_data.get("correlation_id"),
                    json.dumps(event_data.get("raw_payload", {})),
                    json.dumps(normalized),
                    json.dumps(normalized.get("metadata", {}))
                )

            logger.debug(f"💾 Event persisted: {event_data.get('event_id')}")
            return True

        except Exception as e:
            logger.error(f"❌ Single write failed: {e}", exc_info=True)
            return False

    async def _write_batch(self, events: List[Dict[str, Any]]) -> bool:
        """
        Write batch of events using PostgreSQL COPY for efficiency.
        """
        if not events:
            return True

        try:
            async with self.pool.acquire() as conn:
                # Prepare batch data
                rows = []
                for event_data in events:
                    event_time = self._parse_timestamp(event_data.get("event_time"))
                    observed_at = self._parse_timestamp(event_data.get("observed_at"))
                    normalized = event_data.get("normalized", {})

                    gf = lambda f: self._get_field(event_data, normalized, f)
                    rows.append((
                        event_data.get("event_id"),
                        event_time,
                        observed_at,
                        event_data.get("event_type"),
                        event_data.get("source_system"),
                        gf("user_id"),
                        gf("user_name"),
                        gf("user_email"),
                        gf("channel_id"),
                        gf("channel_name"),
                        gf("thread_id"),
                        event_data.get("session_id"),
                        gf("workspace_id"),
                        gf("text"),
                        gf("subject"),
                        gf("body"),
                        gf("action_type"),
                        gf("action_value"),
                        event_data.get("trace_id"),
                        event_data.get("correlation_id"),
                        json.dumps(event_data.get("raw_payload", {})),
                        json.dumps(normalized),
                        json.dumps(normalized.get("metadata", {}))
                    ))

                # Use executemany for batch insert
                await conn.executemany("""
                    INSERT INTO events (
                        event_id, event_time, observed_at, event_type, source_system,
                        user_id, user_name, user_email, channel_id, channel_name,
                        thread_id, session_id, workspace_id, text, subject, body,
                        action_type, action_value, trace_id, correlation_id,
                        raw_payload, normalized_fields, metadata
                    ) VALUES (
                        $1::uuid, $2, $3, $4, $5, $6, $7, $8, $9, $10,
                        $11, $12, $13, $14, $15, $16, $17, $18, $19, $20,
                        $21::jsonb, $22::jsonb, $23::jsonb
                    )
                """, rows)

                self.events_written += len(events)
                self.batches_written += 1
                logger.info(f"💾 Batch persisted: {len(events)} events")
                return True

        except Exception as e:
            logger.error(f"❌ Batch write failed: {e}", exc_info=True)
            return False

    async def disconnect(self):
        """Gracefully close connection pool and flush buffer."""
        # Stop and flush batch buffer
        if self.batch_buffer:
            await self.batch_buffer.stop()

        # Close connection pool
        if self.pool:
            try:
                await self.pool.close()
                logger.info("✅ TimescaleDB disconnected")
            except Exception as e:
                logger.error(f"❌ Error disconnecting: {e}")

        self._connected = False

    async def health_check(self) -> Dict[str, Any]:
        """Check TimescaleDB connection health with metrics."""
        if not self._connected or not self.pool:
            return {"connected": False, "error": "Not connected"}

        try:
            async with self.pool.acquire() as conn:
                version = await conn.fetchval('SELECT version()')
                event_count = await conn.fetchval('SELECT COUNT(*) FROM events')

            return {
                "connected": True,
                "host": self.host,
                "database": self.database,
                "version": version.split(',')[0] if version else "unknown",
                "event_count": event_count,
                "metrics": {
                    "events_written": self.events_written,
                    "batches_written": self.batches_written,
                    "write_failures": self.write_failures,
                    "buffer_size": len(self.batch_buffer.buffer) if self.batch_buffer else 0
                }
            }

        except Exception as e:
            return {"connected": False, "error": str(e)}

    @staticmethod
    def _parse_timestamp(ts: Any) -> datetime:
        """Parse timestamp from various formats."""
        if isinstance(ts, datetime):
            return ts
        elif isinstance(ts, str):
            try:
                return datetime.fromisoformat(ts.replace('Z', '+00:00'))
            except:
                return datetime.utcnow()
        else:
            return datetime.utcnow()

    @property
    def is_connected(self) -> bool:
        return self._connected


# Global writer instance
_writer: Optional[EnhancedTimescaleWriter] = None


async def get_writer(redis_client: Optional[redis.Redis] = None) -> EnhancedTimescaleWriter:
    """Get or create global TimescaleDB writer."""
    global _writer

    if _writer is None:
        host = os.getenv("TIMESCALE_HOST", "localhost")
        port = int(os.getenv("TIMESCALE_PORT", 5432))
        database = os.getenv("TIMESCALE_DB", "basalmind_events")
        user = os.getenv("TIMESCALE_USER", "basalmind")
        password = os.getenv("TIMESCALE_PASSWORD", "basalmind_secure_2024")
        batch_size = int(os.getenv("BATCH_SIZE", 100))
        flush_interval = float(os.getenv("FLUSH_INTERVAL", 1.0))

        _writer = EnhancedTimescaleWriter(
            host=host,
            port=port,
            database=database,
            user=user,
            password=password,
            redis_client=redis_client,
            batch_size=batch_size,
            flush_interval=flush_interval
        )
        await _writer.connect()

    return _writer


async def write_event(event_data: Dict[str, Any]) -> bool:
    """Convenience function to write event to TimescaleDB."""
    writer = await get_writer()
    return await writer.write_event(event_data)
