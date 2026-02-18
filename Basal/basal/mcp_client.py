"""
Basal MCP Client - Invokes MCP skills through the BasalMind MCP Server.

MCP is the ONLY path to side effects. 
No entity should directly perform external actions.

Architecture:
- Connects to basalmind_mcp_server_v3.py at localhost:3100
- Provides typed skill invocation with audit logging
- Implements graceful degradation when MCP is unavailable
- Registers available skills in Redis on startup
"""

import logging
import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional

import httpx

from basal.config import config
from basal.redis_session import RedisSessionManager

logger = logging.getLogger("basal.mcp")

# MCP Server endpoint (discovered from environment)
MCP_SERVER_URL = "http://localhost:3100"
MCP_CALL_ENDPOINT = f"{MCP_SERVER_URL}/mcp/call-tool"
MCP_HEALTH_ENDPOINT = f"{MCP_SERVER_URL}/health"


# ── Skill definitions ──────────────────────────────────────────────────────────

SKILL_REGISTRY = {
    # Health monitoring
    "check_service_health": {
        "description": "Check health status of a service",
        "category": "monitoring",
        "required_args": ["app_name"],
        "optional_args": ["include_metrics"],
        "side_effects": False,  # Read-only
    },
    # Database access
    "execute_database_query": {
        "description": "Execute a read-only database query",
        "category": "data",
        "required_args": ["database", "query"],
        "optional_args": ["read_only"],
        "side_effects": False,  # Read-only
    },
    # Log access
    "read_application_logs": {
        "description": "Read application logs",
        "category": "monitoring",
        "required_args": ["app_name"],
        "optional_args": ["lines", "level", "since"],
        "side_effects": False,  # Read-only
    },
    # Task management
    "manage_tasks": {
        "description": "Create, update, or query tasks",
        "category": "execution",
        "required_args": ["action"],
        "optional_args": ["task_id", "title", "description", "status"],
        "side_effects": True,
    },
    # Project management
    "manage_project": {
        "description": "Manage project lifecycle",
        "category": "execution",
        "required_args": ["action"],
        "optional_args": ["project_id", "name", "description"],
        "side_effects": True,
    },
    # User stories
    "create_user_story": {
        "description": "Create a user story",
        "category": "execution",
        "required_args": ["feature_id", "title", "story_text", "acceptance_criteria"],
        "optional_args": ["story_points", "priority"],
        "side_effects": True,
    },
    # Deployment tracking
    "create_deployment": {
        "description": "Track a deployment",
        "category": "execution",
        "required_args": ["story_ids", "environment", "version", "commit_sha"],
        "optional_args": ["deployed_by"],
        "side_effects": True,
    },
}


# ── Skill invocation result ────────────────────────────────────────────────────

class SkillResult:
    """Result of an MCP skill invocation."""

    def __init__(
        self,
        skill_name: str,
        success: bool,
        content: Any,
        invocation_id: str,
        duration_ms: float,
        error: Optional[str] = None,
    ):
        self.skill_name = skill_name
        self.success = success
        self.content = content
        self.invocation_id = invocation_id
        self.duration_ms = duration_ms
        self.error = error

    def to_dict(self) -> Dict[str, Any]:
        return {
            "skill": self.skill_name,
            "success": self.success,
            "content": self.content,
            "invocation_id": self.invocation_id,
            "duration_ms": self.duration_ms,
            "error": self.error,
        }


# ── MCP Client ─────────────────────────────────────────────────────────────────

class MCPClient:
    """
    Basal's MCP skill invocation client.
    
    All side effects go through here. Never bypass.
    """

    def __init__(self, session_manager: Optional[RedisSessionManager] = None):
        self._sessions = session_manager
        self._http: Optional[httpx.AsyncClient] = None
        self._available = False
        self._invocation_count = 0
        self._audit_log: List[Dict[str, Any]] = []  # In-memory audit (Stage 5: move to DB)

    async def initialize(self) -> bool:
        """Initialize HTTP client and verify MCP server availability."""
        self._http = httpx.AsyncClient(timeout=15.0)

        # Check MCP server health
        try:
            resp = await self._http.get(MCP_HEALTH_ENDPOINT)
            if resp.status_code == 200:
                self._available = True
                logger.info(f"✅ MCP Server connected: {MCP_SERVER_URL}")
                # Register skills in Redis
                if self._sessions:
                    self._sessions.register_skills(SKILL_REGISTRY)
                    logger.info(f"✅ Registered {len(SKILL_REGISTRY)} skills in Redis")
            else:
                logger.warning(f"⚠️ MCP Server returned {resp.status_code}")
        except Exception as e:
            logger.warning(f"⚠️ MCP Server unavailable (non-fatal): {e}")
            self._available = False

        return self._available

    async def shutdown(self):
        """Clean shutdown."""
        if self._http:
            await self._http.aclose()

    @property
    def is_available(self) -> bool:
        return self._available

    async def invoke(
        self,
        skill_name: str,
        arguments: Dict[str, Any],
        correlation_id: Optional[str] = None,
        require_approval: bool = False,
    ) -> SkillResult:
        """
        Invoke an MCP skill.
        
        Args:
            skill_name: Name of the skill from SKILL_REGISTRY
            arguments: Skill-specific arguments
            correlation_id: For audit trail linkage
            require_approval: If True, Sentinel must approve first (Stage 4+)
            
        Returns: SkillResult with success/failure details
        """
        invocation_id = str(uuid.uuid4())[:8]
        started_at = datetime.utcnow()

        logger.info(
            f"[MCP][{invocation_id}] Invoking skill: {skill_name} "
            f"(correlation={correlation_id})"
        )

        # Validate skill exists
        if skill_name not in SKILL_REGISTRY:
            return SkillResult(
                skill_name=skill_name,
                success=False,
                content=None,
                invocation_id=invocation_id,
                duration_ms=0.0,
                error=f"Unknown skill: {skill_name}. Available: {list(SKILL_REGISTRY.keys())}",
            )

        # Check MCP availability
        if not self._available or not self._http:
            logger.warning(f"[MCP][{invocation_id}] MCP Server unavailable, skill degraded")
            return SkillResult(
                skill_name=skill_name,
                success=False,
                content=None,
                invocation_id=invocation_id,
                duration_ms=0.0,
                error="MCP Server unavailable",
            )

        # Invoke via MCP server
        try:
            resp = await self._http.post(
                MCP_CALL_ENDPOINT,
                json={"name": skill_name, "arguments": arguments},
            )

            duration_ms = (datetime.utcnow() - started_at).total_seconds() * 1000

            if resp.status_code == 200:
                data = resp.json()
                content_list = data.get("content", [])
                # Extract text content
                content = "\n".join(
                    item.get("text", "") for item in content_list if item.get("type") == "text"
                )
                self._invocation_count += 1

                result = SkillResult(
                    skill_name=skill_name,
                    success=True,
                    content=content,
                    invocation_id=invocation_id,
                    duration_ms=duration_ms,
                )
                logger.info(f"[MCP][{invocation_id}] ✅ {skill_name} succeeded ({duration_ms:.0f}ms)")
            else:
                result = SkillResult(
                    skill_name=skill_name,
                    success=False,
                    content=None,
                    invocation_id=invocation_id,
                    duration_ms=duration_ms,
                    error=f"HTTP {resp.status_code}: {resp.text[:200]}",
                )
                logger.warning(f"[MCP][{invocation_id}] ⚠️ {skill_name} failed: HTTP {resp.status_code}")

        except Exception as e:
            duration_ms = (datetime.utcnow() - started_at).total_seconds() * 1000
            result = SkillResult(
                skill_name=skill_name,
                success=False,
                content=None,
                invocation_id=invocation_id,
                duration_ms=duration_ms,
                error=str(e),
            )
            logger.error(f"[MCP][{invocation_id}] ❌ {skill_name} error: {e}")

        # Audit log
        self._audit_log.append({
            "invocation_id": invocation_id,
            "skill": skill_name,
            "correlation_id": correlation_id,
            "success": result.success,
            "duration_ms": result.duration_ms,
            "error": result.error,
            "timestamp": started_at.isoformat(),
        })

        # Keep audit log bounded (last 1000 entries in memory)
        if len(self._audit_log) > 1000:
            self._audit_log = self._audit_log[-1000:]

        return result

    def get_audit_log(self, limit: int = 50) -> List[Dict[str, Any]]:
        """Return recent audit log entries."""
        return self._audit_log[-limit:]

    def get_metrics(self) -> Dict[str, Any]:
        """Return MCP invocation metrics."""
        return {
            "mcp_available": self._available,
            "total_invocations": self._invocation_count,
            "audit_log_size": len(self._audit_log),
            "registered_skills": list(SKILL_REGISTRY.keys()),
        }
