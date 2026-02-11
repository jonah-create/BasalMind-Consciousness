"""
Redis cache module for Observer.

Responsibilities:
- Deduplication checks (5 min TTL)
- Hot cache for recent events (1 hour TTL)
- Session tracking (2 hour TTL)

NOT responsible for:
- Permanent storage (that is TimescaleDB job)
- Aggregation (that is Interpreter job)
"""

import redis
import hashlib
import json
from typing import Optional, Dict, Any
from datetime import datetime, timedelta
import logging

logger = logging.getLogger("observer.redis_cache")


class RedisCache:
    """
    Redis cache for Observer deduplication and hot cache.
    
    Lightweight, ephemeral storage with TTL-based expiration.
    """
    
    def __init__(
        self,
        host: str = "127.0.0.1",
        port: int = 6379,
        db: int = 0,
        password: Optional[str] = None
    ):
        self.client = redis.Redis(
            host=host,
            port=port,
            db=db,
            password=password,
            decode_responses=True
        )
        
        # TTL configuration (in seconds)
        self.DEDUP_TTL = 300  # 5 minutes
        self.CACHE_TTL = 3600  # 1 hour
        self.SESSION_TTL = 7200  # 2 hours
        
        logger.info(f"✅ Redis cache initialized (host={host}, port={port})")
    
    def _generate_dedup_key(self, event_data: Dict[str, Any]) -> str:
        """
        Generate dedup key from event data.
        
        Uses hash of critical fields to identify duplicates.
        DETERMINISTIC: Same event → same key.
        """
        # Extract critical fields for dedup
        critical_fields = {
            "source": event_data.get("source_system"),
            "type": event_data.get("event_type"),
            "user": event_data.get("user_id"),
            "timestamp": event_data.get("source_timestamp"),
            "text": event_data.get("text", "")[:100]  # First 100 chars
        }
        
        # Hash to create compact key
        data_str = json.dumps(critical_fields, sort_keys=True)
        hash_digest = hashlib.sha256(data_str.encode()).hexdigest()[:16]
        
        return f"dedup:{hash_digest}"
    
    async def is_duplicate(self, event_data: Dict[str, Any]) -> bool:
        """
        Check if event is a duplicate.
        
        Returns:
            True if event was seen recently (within DEDUP_TTL)
            False if event is new
        
        Performance target: <1ms
        """
        dedup_key = self._generate_dedup_key(event_data)
        
        try:
            # Check if key exists
            exists = self.client.exists(dedup_key)
            
            if exists:
                logger.debug(f"[DEDUP] Duplicate detected: {dedup_key}")
                return True
            else:
                # Mark as seen (set with TTL)
                self.client.setex(
                    dedup_key,
                    self.DEDUP_TTL,
                    datetime.utcnow().isoformat()
                )
                logger.debug(f"[DEDUP] New event marked: {dedup_key}")
                return False
                
        except Exception as e:
            logger.error(f"[DEDUP] Redis error: {e}")
            # On error, assume NOT duplicate (fail open)
            return False
    
    async def cache_event(
        self,
        event_id: str,
        event_data: Dict[str, Any]
    ) -> None:
        """
        Cache event for hot retrieval.
        
        Stores recent events with 1-hour TTL for fast access.
        """
        cache_key = f"event:{event_id}"
        
        try:
            self.client.setex(
                cache_key,
                self.CACHE_TTL,
                json.dumps(event_data)
            )
            logger.debug(f"[CACHE] Event cached: {event_id}")
            
        except Exception as e:
            logger.error(f"[CACHE] Failed to cache event {event_id}: {e}")
    
    async def get_cached_event(self, event_id: str) -> Optional[Dict[str, Any]]:
        """Retrieve cached event by ID."""
        cache_key = f"event:{event_id}"
        
        try:
            data = self.client.get(cache_key)
            if data:
                logger.debug(f"[CACHE] Cache hit: {event_id}")
                return json.loads(data)
            else:
                logger.debug(f"[CACHE] Cache miss: {event_id}")
                return None
                
        except Exception as e:
            logger.error(f"[CACHE] Failed to retrieve {event_id}: {e}")
            return None
    
    async def track_session(
        self,
        session_id: str,
        event_id: str
    ) -> None:
        """
        Track session activity.
        
        Maintains list of recent events per session with 2-hour TTL.
        """
        session_key = f"session:{session_id}"
        
        try:
            # Add event to session list
            self.client.lpush(session_key, event_id)
            
            # Trim to last 100 events
            self.client.ltrim(session_key, 0, 99)
            
            # Set/refresh TTL
            self.client.expire(session_key, self.SESSION_TTL)
            
            logger.debug(f"[SESSION] Tracked: {session_id} -> {event_id}")
            
        except Exception as e:
            logger.error(f"[SESSION] Failed to track {session_id}: {e}")
    
    async def get_session_events(
        self,
        session_id: str,
        limit: int = 10
    ) -> list:
        """Retrieve recent events for a session."""
        session_key = f"session:{session_id}"
        
        try:
            event_ids = self.client.lrange(session_key, 0, limit - 1)
            logger.debug(f"[SESSION] Retrieved {len(event_ids)} events for {session_id}")
            return event_ids
            
        except Exception as e:
            logger.error(f"[SESSION] Failed to retrieve session {session_id}: {e}")
            return []
    
    def health_check(self) -> Dict[str, Any]:
        """Check Redis connection health."""
        try:
            self.client.ping()
            return {
                "connected": True,
                "host": self.client.connection_pool.connection_kwargs.get("host"),
                "port": self.client.connection_pool.connection_kwargs.get("port")
            }
        except Exception as e:
            return {
                "connected": False,
                "error": str(e)
            }
