"""
Session Tracker for Interpreter

Tracks user activity sessions and behavior journeys.
Helps BasalMind understand user patterns and context.

Based on Claude Opus design specification.
"""

import logging
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta
from collections import defaultdict
import asyncpg
import json

logger = logging.getLogger(__name__)


class SessionTracker:
    """
    Track user sessions and activity journeys.
    
    Features:
    - Automatic session detection (30 min inactivity = session end)
    - Journey tracking (what users do during sessions)
    - Intent aggregation per session
    - Channel/thread participation tracking
    
    Helps BasalMind:
    - Understand user behavior patterns
    - Provide better context-aware responses
    - Identify user goals across multiple interactions
    """
    
    def __init__(
        self,
        postgres_pool: asyncpg.Pool,
        session_timeout_minutes: int = 30
    ):
        self.pool = postgres_pool
        self.session_timeout = timedelta(minutes=session_timeout_minutes)
        
        # In-memory tracking of active sessions
        self.active_sessions = {}  # (user_id, source) -> session_data
        
    async def process_events(
        self,
        events: List[Dict[str, Any]],
        intents: Dict[str, List[Dict[str, Any]]]
    ) -> List[str]:
        """
        Process events and track user sessions.
        
        Args:
            events: Batch of events
            intents: Extracted intents keyed by event_id
            
        Returns:
            List of session_ids updated
        """
        sessions_updated = []
        
        # Group events by user
        by_user = defaultdict(list)
        for event in events:
            user_id = event.get("user_id")
            source = event.get("source_system")
            if user_id and source:
                by_user[(user_id, source)].append(event)
                
        # Process each user's events
        for (user_id, source), user_events in by_user.items():
            try:
                session_id = await self._process_user_events(
                    user_id=user_id,
                    source_system=source,
                    events=user_events,
                    intents=intents
                )
                if session_id:
                    sessions_updated.append(session_id)
            except Exception as e:
                logger.error(f"Error tracking session for {user_id}: {e}", exc_info=True)
                
        # Close inactive sessions
        await self._close_inactive_sessions()
        
        return sessions_updated
        
    async def _process_user_events(
        self,
        user_id: str,
        source_system: str,
        events: List[Dict[str, Any]],
        intents: Dict[str, List[Dict[str, Any]]]
    ) -> Optional[str]:
        """Process events for a single user."""
        if not events:
            return None
            
        # Sort by time
        events = sorted(events, key=lambda e: e.get("observed_at"))
        
        session_key = (user_id, source_system)
        
        # Get or create session
        session_data = self.active_sessions.get(session_key)
        
        if not session_data:
            # Start new session
            session_data = await self._start_session(
                user_id=user_id,
                source_system=source_system,
                first_event=events[0]
            )
            self.active_sessions[session_key] = session_data
            logger.info(f"📍 Session started: {user_id} on {source_system}")
        else:
            # Check if session timed out
            last_activity = session_data["last_activity"]
            first_event_time = events[0].get("observed_at")
            
            if first_event_time - last_activity > self.session_timeout:
                # Close old session and start new one
                await self._end_session(session_data)
                session_data = await self._start_session(
                    user_id=user_id,
                    source_system=source_system,
                    first_event=events[0]
                )
                self.active_sessions[session_key] = session_data
                logger.info(f"📍 Session restarted: {user_id} (timeout)")
                
        # Update session with events
        await self._update_session(session_data, events, intents)
        
        return session_data["session_id"]
        
    async def _start_session(
        self,
        user_id: str,
        source_system: str,
        first_event: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Start a new user session."""
        started_at = first_event.get("observed_at")
        entry_point = self._get_entry_point(first_event)
        
        query = """
            INSERT INTO user_sessions (
                user_id, source_system, started_at, last_activity,
                entry_point, event_count
            ) VALUES ($1, $2, $3, $3, $4, 0)
            RETURNING session_id
        """
        
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                query,
                user_id,
                source_system,
                started_at,
                entry_point
            )
            
        session_id = str(row["session_id"])
        
        return {
            "session_id": session_id,
            "user_id": user_id,
            "source_system": source_system,
            "started_at": started_at,
            "last_activity": started_at,
            "channels": set(),
            "threads": set(),
            "intents": defaultdict(int),
            "journey": [],
            "event_ids": []
        }
        
    async def _update_session(
        self,
        session_data: Dict[str, Any],
        events: List[Dict[str, Any]],
        intents: Dict[str, List[Dict[str, Any]]]
    ):
        """Update session with new events."""
        for event in events:
            event_id = event.get("event_id")
            observed_at = event.get("observed_at")
            
            # Track event
            session_data["event_ids"].append(event_id)
            session_data["last_activity"] = observed_at
            
            # Track channels/threads
            channel_id = event.get("channel_id")
            if channel_id:
                session_data["channels"].add(channel_id)
                
            thread_id = event.get("thread_id")
            if thread_id:
                session_data["threads"].add(thread_id)
                
            # Track intents
            if event_id in intents:
                for intent in intents[event_id]:
                    intent_type = intent["type"]
                    session_data["intents"][intent_type] += 1
                    
            # Add journey step
            session_data["journey"].append({
                "timestamp": observed_at.isoformat(),
                "action": event.get("event_type"),
                "context": {
                    "channel": event.get("channel_name"),
                    "thread": thread_id
                }
            })
            
        # Write to database
        await self._persist_session(session_data)
        
    async def _persist_session(self, session_data: Dict[str, Any]):
        """Write session updates to database."""
        query = """
            UPDATE user_sessions
            SET last_activity = $2,
                event_count = $3,
                channels_visited = $4,
                threads_participated = $5,
                intents_expressed = $6,
                journey_steps = $7,
                event_ids = $8,
                updated_at = NOW()
            WHERE session_id = $1
        """
        
        async with self.pool.acquire() as conn:
            await conn.execute(
                query,
                session_data["session_id"],
                session_data["last_activity"],
                len(session_data["event_ids"]),
                list(session_data["channels"]),
                list(session_data["threads"]),
                json.dumps(dict(session_data["intents"])),  # Convert to JSON string
                json.dumps(session_data["journey"]),  # Convert to JSON string
                session_data["event_ids"]
            )
            
    async def _end_session(self, session_data: Dict[str, Any]):
        """Mark session as ended."""
        query = """
            UPDATE user_sessions
            SET ended_at = $2,
                exit_point = $3
            WHERE session_id = $1
        """
        
        last_event = session_data["journey"][-1] if session_data["journey"] else {}
        exit_point = last_event.get("context", {}).get("channel") or "unknown"
        
        async with self.pool.acquire() as conn:
            await conn.execute(
                query,
                session_data["session_id"],
                session_data["last_activity"],
                exit_point
            )
            
        logger.info(
            f"📍 Session ended: {session_data['user_id']} "
            f"({len(session_data['event_ids'])} events, "
            f"{len(session_data['channels'])} channels)"
        )
        
    async def _close_inactive_sessions(self):
        """Close sessions that have timed out."""
        from datetime import timezone
        cutoff_time = datetime.now(timezone.utc) - self.session_timeout
        
        to_remove = []
        for key, session_data in self.active_sessions.items():
            if session_data["last_activity"] < cutoff_time:
                await self._end_session(session_data)
                to_remove.append(key)
                
        for key in to_remove:
            del self.active_sessions[key]
            
    def _get_entry_point(self, event: Dict[str, Any]) -> str:
        """Determine session entry point from first event."""
        channel_name = event.get("channel_name")
        if channel_name:
            return f"channel:{channel_name}"
            
        event_type = event.get("event_type", "unknown")
        return f"event:{event_type}"


# Test the tracker
async def test_tracker():
    """Test session tracker."""
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
    
    tracker = SessionTracker(pool)
    
    # Test with sample events
    from datetime import timezone
    
    events = [
        {
            "event_id": "evt-1",
            "user_id": "U123",
            "source_system": "slack",
            "channel_id": "C001",
            "channel_name": "general",
            "observed_at": datetime.now(timezone.utc)
        }
    ]
    
    intents = {}
    
    sessions = await tracker.process_events(events, intents)
    print(f"Tracked {len(sessions)} sessions")
    
    await pool.close()


if __name__ == "__main__":
    import asyncio
    logging.basicConfig(level=logging.INFO)
    asyncio.run(test_tracker())
