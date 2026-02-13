"""
TimescaleDB Writer for Observer.

Purpose: Permanent audit trail of ALL observed events.
Strategy: Write-through pattern - every event goes to TimescaleDB immediately.
No caching, no temporary storage - this is the source of truth.
"""

import os
import logging
from typing import Dict, Any, Optional
from datetime import datetime
import asyncpg
import json
from asyncpg.pool import Pool

logger = logging.getLogger(__name__)


class TimescaleWriter:
    """
    Writes events to TimescaleDB for permanent storage.

    Philosophy:
    - Every event is written (no filtering, no judgment)
    - Write failures are critical (logged as errors)
    - Async writes for performance
    - Connection pooling for efficiency
    """

    def __init__(
        self,
        host: str = "localhost",
        port: int = 5432,
        database: str = "basalmind_events",
        user: str = "basalmind",
        password: str = "basalmind_secure_2024",
        min_pool_size: int = 2,
        max_pool_size: int = 10
    ):
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password
        self.min_pool_size = min_pool_size
        self.max_pool_size = max_pool_size

        self.pool: Optional[Pool] = None
        self._connected = False

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
            logger.info(f"✅ TimescaleDB connected: {self.host}:{self.port}/{self.database}")

        except Exception as e:
            logger.error(f"❌ Failed to connect to TimescaleDB: {e}")
            self._connected = False
            raise

    async def write_event(self, event_data: Dict[str, Any]) -> bool:
        """
        Write event to TimescaleDB.

        This is the PERMANENT record. Write failures are critical.

        Args:
            event_data: Complete canonical event data

        Returns:
            bool: True if written successfully, False otherwise
        """
        if not self._connected or not self.pool:
            logger.error("❌ TimescaleDB not connected, cannot write event")
            return False

        try:
            async with self.pool.acquire() as conn:
                # Extract fields from canonical event
                query = """
                INSERT INTO events (
                    event_id,
                    event_time,
                    observed_at,
                    event_type,
                    source_system,
                    user_id,
                    user_name,
                    user_email,
                    channel_id,
                    channel_name,
                    thread_id,
                    session_id,
                    workspace_id,
                    text,
                    subject,
                    body,
                    action_type,
                    action_value,
                    trace_id,
                    correlation_id,
                    raw_payload,
                    normalized_fields,
                    metadata
                ) VALUES (
                    $1::uuid, $2, $3, $4, $5, $6, $7, $8, $9, $10,
                    $11, $12, $13, $14, $15, $16, $17, $18, $19, $20,
                    $21::jsonb, $22::jsonb, $23::jsonb
                )
                -- Deduplication handled by Redis at Stage 2
                """

                # Parse timestamps
                event_time = self._parse_timestamp(event_data.get("event_time"))
                observed_at = self._parse_timestamp(event_data.get("observed_at", datetime.utcnow().isoformat()))

                # Get normalized fields
                normalized = event_data.get("normalized", {})

                await conn.execute(
                    query,
                    event_data.get("event_id"),
                    event_time,
                    observed_at,
                    event_data.get("event_type"),
                    event_data.get("source_system"),
                    normalized.get("user_id"),
                    normalized.get("user_name"),
                    normalized.get("user_email"),
                    normalized.get("channel_id"),
                    normalized.get("channel_name"),
                    normalized.get("thread_id"),
                    event_data.get("session_id"),
                    normalized.get("workspace_id"),
                    normalized.get("text"),
                    normalized.get("subject"),
                    normalized.get("body"),
                    normalized.get("action_type"),
                    normalized.get("action_value"),
                    event_data.get("trace_id"),
                    event_data.get("correlation_id"),
                    json.dumps(event_data.get("raw_payload", {})),
                    json.dumps(normalized),
                    json.dumps(normalized.get("metadata", {}))
                )

            logger.debug(f"💾 Event persisted to TimescaleDB: {event_data.get('event_id')}")
            return True

        except Exception as e:
            logger.error(f"❌ Failed to write event to TimescaleDB: {e}", exc_info=True)
            return False

    async def disconnect(self):
        """Gracefully close connection pool."""
        if self.pool:
            try:
                await self.pool.close()
                logger.info("✅ TimescaleDB disconnected")
            except Exception as e:
                logger.error(f"❌ Error disconnecting from TimescaleDB: {e}")

        self._connected = False

    async def health_check(self) -> Dict[str, Any]:
        """Check TimescaleDB connection health."""
        if not self._connected or not self.pool:
            return {
                "connected": False,
                "error": "Not connected"
            }

        try:
            async with self.pool.acquire() as conn:
                version = await conn.fetchval('SELECT version()')
                event_count = await conn.fetchval('SELECT COUNT(*) FROM events')

            return {
                "connected": True,
                "host": self.host,
                "port": self.port,
                "database": self.database,
                "version": version.split(',')[0] if version else "unknown",
                "event_count": event_count
            }

        except Exception as e:
            return {
                "connected": False,
                "error": str(e)
            }

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
        """Check if TimescaleDB is connected."""
        return self._connected


# Global writer instance
_writer: Optional[TimescaleWriter] = None


async def get_writer() -> TimescaleWriter:
    """Get or create global TimescaleDB writer."""
    global _writer

    if _writer is None:
        host = os.getenv("TIMESCALE_HOST", "localhost")
        port = int(os.getenv("TIMESCALE_PORT", 5432))
        database = os.getenv("TIMESCALE_DB", "basalmind_events")
        user = os.getenv("TIMESCALE_USER", "basalmind")
        password = os.getenv("TIMESCALE_PASSWORD", "basalmind_secure_2024")

        _writer = TimescaleWriter(
            host=host,
            port=port,
            database=database,
            user=user,
            password=password
        )
        await _writer.connect()

    return _writer


async def write_event(event_data: Dict[str, Any]) -> bool:
    """
    Convenience function to write event to TimescaleDB.

    Usage:
        await write_event(canonical_event.__dict__)
    """
    writer = await get_writer()
    return await writer.write_event(event_data)
