"""
Basal Redis Session Manager - Session state with TTL for orchestration context.
Follows Interpreter's Redis checkpoint pattern.
"""

import json
import logging
import uuid
from datetime import datetime
from typing import Any, Dict, Optional

import redis

from basal.config import config

logger = logging.getLogger("basal.session")

# Redis key namespaces
SESSION_PREFIX = "basal:session:"
SKILL_REGISTRY_PREFIX = "basal:skill_registry"
ORCHESTRATION_PREFIX = "basal:orchestration:"


class RedisSessionManager:
    """
    Manages ephemeral session state in Redis with TTL.
    
    Key patterns:
    - basal:session:{session_id} → orchestration context
    - basal:skill_registry → registered MCP skills
    - basal:orchestration:{request_id} → in-flight orchestration state
    """

    def __init__(self):
        self._client: Optional[redis.Redis] = None
        self._connected = False

    def connect(self) -> bool:
        """Establish Redis connection."""
        try:
            self._client = redis.Redis(
                host=config.REDIS_HOST,
                port=config.REDIS_PORT,
                password=config.REDIS_PASSWORD,
                db=config.REDIS_DB,
                decode_responses=True,
                socket_connect_timeout=3,
            )
            self._client.ping()
            self._connected = True
            logger.info(f"✅ Redis connected: {config.REDIS_HOST}:{config.REDIS_PORT}")
            return True
        except Exception as e:
            logger.warning(f"⚠️ Redis connection failed (non-fatal): {e}")
            self._connected = False
            return False

    @property
    def is_connected(self) -> bool:
        return self._connected

    def health_check(self) -> Dict[str, Any]:
        """Return Redis health status."""
        if not self._client:
            return {"connected": False, "error": "Not initialized"}
        try:
            self._client.ping()
            return {
                "connected": True,
                "host": config.REDIS_HOST,
                "port": config.REDIS_PORT,
            }
        except Exception as e:
            return {"connected": False, "error": str(e)}

    # ── Session management ─────────────────────────────────────────────────────

    def create_session(self, context: Dict[str, Any], ttl: int = None) -> str:
        """Create a new orchestration session. Returns session_id."""
        session_id = str(uuid.uuid4())
        session_data = {
            "session_id": session_id,
            "created_at": datetime.utcnow().isoformat(),
            "context": context,
        }
        if self._client:
            try:
                self._client.setex(
                    f"{SESSION_PREFIX}{session_id}",
                    ttl or config.REDIS_SESSION_TTL,
                    json.dumps(session_data)
                )
            except Exception as e:
                logger.warning(f"⚠️ Could not persist session to Redis: {e}")
        return session_id

    def get_session(self, session_id: str) -> Optional[Dict[str, Any]]:
        """Retrieve session by ID."""
        if not self._client:
            return None
        try:
            raw = self._client.get(f"{SESSION_PREFIX}{session_id}")
            if raw:
                return json.loads(raw)
        except Exception as e:
            logger.warning(f"⚠️ Could not retrieve session {session_id}: {e}")
        return None

    def update_session(self, session_id: str, updates: Dict[str, Any]) -> bool:
        """Update session context fields."""
        session = self.get_session(session_id)
        if session:
            session["context"].update(updates)
            session["updated_at"] = datetime.utcnow().isoformat()
            try:
                self._client.setex(
                    f"{SESSION_PREFIX}{session_id}",
                    config.REDIS_SESSION_TTL,
                    json.dumps(session)
                )
                return True
            except Exception as e:
                logger.warning(f"⚠️ Could not update session {session_id}: {e}")
        return False

    # ── Skill registry ─────────────────────────────────────────────────────────

    def register_skills(self, skills: Dict[str, Any]) -> bool:
        """Register available MCP skills in Redis."""
        if not self._client:
            return False
        try:
            self._client.set(
                SKILL_REGISTRY_PREFIX,
                json.dumps({
                    "registered_at": datetime.utcnow().isoformat(),
                    "skills": skills,
                })
            )
            return True
        except Exception as e:
            logger.warning(f"⚠️ Could not register skills: {e}")
            return False

    def get_skill_registry(self) -> Optional[Dict[str, Any]]:
        """Retrieve registered skills."""
        if not self._client:
            return None
        try:
            raw = self._client.get(SKILL_REGISTRY_PREFIX)
            if raw:
                return json.loads(raw)
        except Exception as e:
            logger.warning(f"⚠️ Could not retrieve skill registry: {e}")
        return None

    # ── Orchestration state ────────────────────────────────────────────────────

    def save_orchestration(self, request_id: str, state: Dict[str, Any], ttl: int = 300) -> bool:
        """Persist in-flight orchestration state."""
        if not self._client:
            return False
        try:
            self._client.setex(
                f"{ORCHESTRATION_PREFIX}{request_id}",
                ttl,
                json.dumps(state)
            )
            return True
        except Exception as e:
            logger.warning(f"⚠️ Could not save orchestration {request_id}: {e}")
            return False

    def get_orchestration(self, request_id: str) -> Optional[Dict[str, Any]]:
        """Retrieve orchestration state."""
        if not self._client:
            return None
        try:
            raw = self._client.get(f"{ORCHESTRATION_PREFIX}{request_id}")
            if raw:
                return json.loads(raw)
        except Exception as e:
            logger.warning(f"⚠️ Could not retrieve orchestration {request_id}: {e}")
        return None
