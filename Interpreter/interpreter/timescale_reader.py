"""
TimescaleDB Event Tail Reader

Reads raw events from Observer's TimescaleDB using a cursor-based approach.
Events are read in real-time order using observed_at > last_checkpoint.
No artificial upper time bound - processes events as they arrive with their real timestamps.
"""

import asyncpg
import logging
from datetime import datetime
from typing import List, Dict, Any, Optional
from uuid import UUID

logger = logging.getLogger(__name__)


class TimescaleReader:
    """
    Cursor-based tail reader for TimescaleDB events.

    Reads events with observed_at > last_checkpoint, ordered by observed_at.
    Uses a small lag (e.g. 2s) to avoid reading events still being written.
    """

    def __init__(
        self,
        host: str,
        port: int,
        database: str,
        user: str,
        password: str,
        batch_size: int = 200,
        lag_seconds: int = 2,
        # kept for backward compat with engine setup, not used
        window_seconds: int = 30
    ):
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password
        self.batch_size = batch_size
        self.lag_seconds = lag_seconds

        self.pool: Optional[asyncpg.Pool] = None

    async def connect(self):
        """Initialize connection pool to TimescaleDB."""
        self.pool = await asyncpg.create_pool(
            host=self.host,
            port=self.port,
            database=self.database,
            user=self.user,
            password=self.password,
            min_size=2,
            max_size=5,
            command_timeout=60
        )
        logger.info(
            f"TimescaleDB reader connected: {self.host}:{self.port}/{self.database}"
        )

    async def close(self):
        """Close connection pool."""
        if self.pool:
            await self.pool.close()
            logger.info("TimescaleDB reader closed")

    async def get_batch(
        self,
        after_time: datetime,
        end_time: Optional[datetime] = None,  # ignored, kept for compat
        limit: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """
        Fetch events with observed_at > after_time, up to NOW() - lag_seconds.

        Args:
            after_time: Cursor position — only return events observed after this.
            end_time:   Ignored (kept for backward compat with old windowed API).
            limit:      Max events to return.

        Returns:
            List of event dicts ordered by observed_at ASC.
        """
        if not self.pool:
            raise RuntimeError("Reader not connected. Call connect() first.")

        if limit is None:
            limit = self.batch_size

        query = """
            SELECT
                event_id,
                observed_at,
                event_time,
                event_type,
                source_system,
                user_id,
                channel_id,
                thread_id,
                channel_name,
                session_id,
                text,
                normalized_fields,
                raw_payload,
                metadata
            FROM events
            WHERE observed_at > $1
              AND observed_at <= NOW() - ($2 * INTERVAL '1 second')
            ORDER BY observed_at ASC
            LIMIT $3
        """

        async with self.pool.acquire() as conn:
            rows = await conn.fetch(query, after_time, self.lag_seconds, limit)

        events = []
        for row in rows:
            event = dict(row)
            event["event_id"] = str(event["event_id"])
            events.append(event)

        if events:
            latest_ts = events[-1]["observed_at"].isoformat()
            logger.info(
                f"Fetched {len(events)} events after {after_time.isoformat()} "
                f"(latest: {latest_ts})"
            )
        else:
            logger.debug(f"No new events after {after_time.isoformat()}")

        return events

    async def get_events_by_ids(self, event_ids: List[str]) -> List[Dict[str, Any]]:
        """Fetch specific events by their IDs (for lineage queries)."""
        if not self.pool:
            raise RuntimeError("Reader not connected. Call connect() first.")

        uuids = [UUID(eid) for eid in event_ids]

        query = """
            SELECT
                event_id, observed_at, event_time, event_type, source_system,
                user_id, channel_id, thread_id, channel_name, session_id,
                text, normalized_fields, raw_payload, metadata
            FROM events
            WHERE event_id = ANY($1::uuid[])
            ORDER BY observed_at ASC
        """

        async with self.pool.acquire() as conn:
            rows = await conn.fetch(query, uuids)

        events = []
        for row in rows:
            event = dict(row)
            event["event_id"] = str(event["event_id"])
            events.append(event)

        logger.debug(f"Fetched {len(events)} events by ID")
        return events

    async def get_latest_event_time(self) -> Optional[datetime]:
        """Get the timestamp of the most recent event in TimescaleDB."""
        if not self.pool:
            raise RuntimeError("Reader not connected. Call connect() first.")
        async with self.pool.acquire() as conn:
            return await conn.fetchval("SELECT MAX(observed_at) FROM events")

    # kept for backward compat
    async def load_checkpoint(self) -> Optional[datetime]:
        return None
