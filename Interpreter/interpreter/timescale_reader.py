"""
TimescaleDB Batch Reader - Phase 1

Reads raw time-series events from Observer's TimescaleDB in batches.
Implements windowing strategy for temporal correlation.

Based on Claude Opus design specification.
"""

import asyncio
import asyncpg
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
from uuid import UUID

logger = logging.getLogger(__name__)


class TimescaleReader:
    """
    Batch reader for TimescaleDB events.
    
    Responsibilities:
    - Read events in time-ordered batches
    - Maintain checkpoint for resumability
    - Provide lineage tracking (event IDs)
    """
    
    def __init__(
        self,
        host: str,
        port: int,
        database: str,
        user: str,
        password: str,
        batch_size: int = 1000,
        window_seconds: int = 30
    ):
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password
        self.batch_size = batch_size
        self.window_seconds = window_seconds
        
        self.pool: Optional[asyncpg.Pool] = None
        self.last_checkpoint: Optional[datetime] = None
        
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
        logger.info(f"✅ TimescaleDB reader connected: {self.host}:{self.port}/{self.database}")
        
    async def close(self):
        """Close connection pool."""
        if self.pool:
            await self.pool.close()
            logger.info("TimescaleDB reader closed")
            
    async def load_checkpoint(self) -> Optional[datetime]:
        """
        Load the last processed timestamp from checkpoint.
        
        Returns:
            datetime of last processed event, or None if starting fresh
        """
        if not self.pool:
            raise RuntimeError("Reader not connected. Call connect() first.")
            
        # For Phase 1, we'll query interpretations checkpoint from the Interpreter's database
        # But this reader only accesses TimescaleDB, so we return None for now
        # The main engine will manage checkpoints
        return None
        
    async def get_batch(
        self,
        start_time: datetime,
        end_time: Optional[datetime] = None,
        limit: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """
        Fetch a batch of events within the time window.
        
        Args:
            start_time: Start of time window
            end_time: End of time window (defaults to start_time + window_seconds)
            limit: Maximum events to return (defaults to batch_size)
            
        Returns:
            List of event dicts with all fields from TimescaleDB
        """
        if not self.pool:
            raise RuntimeError("Reader not connected. Call connect() first.")
            
        if end_time is None:
            end_time = start_time + timedelta(seconds=self.window_seconds)
            
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
                session_id,
                text,
                normalized_fields,
                raw_payload,
                metadata
            FROM events
            WHERE observed_at > $1
              AND observed_at <= $2
            ORDER BY observed_at ASC
            LIMIT $3
        """
        
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(query, start_time, end_time, limit)
            
        # Convert records to dicts
        events = []
        for row in rows:
            event = dict(row)
            # Convert UUID to string for JSON serialization
            event['event_id'] = str(event['event_id'])
            events.append(event)
            
        logger.info(
            f"📥 Fetched {len(events)} events from {start_time.isoformat()} "
            f"to {end_time.isoformat()}"
        )
        
        return events
        
    async def get_events_by_ids(self, event_ids: List[str]) -> List[Dict[str, Any]]:
        """
        Fetch specific events by their IDs (for lineage queries).
        
        Args:
            event_ids: List of event ID strings
            
        Returns:
            List of event dicts
        """
        if not self.pool:
            raise RuntimeError("Reader not connected. Call connect() first.")
            
        # Convert string IDs to UUIDs
        uuids = [UUID(eid) for eid in event_ids]
        
        query = """
            SELECT 
                event_id,
                observed_at,
                event_time,
                event_type,
                source_system,
                user_id,
                channel_id,
                session_id,
                text,
                normalized_fields,
                raw_payload,
                metadata
            FROM events
            WHERE event_id = ANY($1::uuid[])
            ORDER BY observed_at ASC
        """
        
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(query, uuids)
            
        events = []
        for row in rows:
            event = dict(row)
            event['event_id'] = str(event['event_id'])
            events.append(event)
            
        logger.debug(f"📥 Fetched {len(events)} events by ID")
        
        return events
        
    async def get_latest_event_time(self) -> Optional[datetime]:
        """
        Get the timestamp of the most recent event in TimescaleDB.
        
        Returns:
            datetime of most recent event, or None if no events
        """
        if not self.pool:
            raise RuntimeError("Reader not connected. Call connect() first.")
            
        query = "SELECT MAX(observed_at) FROM events"
        
        async with self.pool.acquire() as conn:
            result = await conn.fetchval(query)
            
        return result


# Example usage
async def test_reader():
    """Test the TimescaleReader with real data."""
    import os
    from dotenv import load_dotenv
    
    load_dotenv()
    
    reader = TimescaleReader(
        host=os.getenv("TIMESCALE_HOST", "localhost"),
        port=int(os.getenv("TIMESCALE_PORT", 5432)),
        database=os.getenv("TIMESCALE_DB", "basalmind_events"),
        user=os.getenv("TIMESCALE_USER", "basalmind"),
        password=os.getenv("TIMESCALE_PASSWORD"),
        batch_size=100,
        window_seconds=30
    )
    
    await reader.connect()
    
    try:
        # Get latest event time
        latest = await reader.get_latest_event_time()
        print(f"Latest event time: {latest}")
        
        if latest:
            # Fetch last 30 seconds of events
            start_time = latest - timedelta(seconds=30)
            events = await reader.get_batch(start_time, latest)
            print(f"\nFetched {len(events)} events:")
            for event in events[:5]:  # Show first 5
                print(f"  - {event['event_time']}: {event['event_type']} from {event['source_system']}")
                
    finally:
        await reader.close()


if __name__ == "__main__":
    asyncio.run(test_reader())
