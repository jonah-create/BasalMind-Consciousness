"""
Basal Service - BasalMind Orchestration Layer

Every advisory exchange is auditable.
Every decision is traceable.
All side effects go through MCP.
"""

import logging
import os
from datetime import datetime
from typing import Any, Dict, Optional

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
import uvicorn
from dotenv import load_dotenv

load_dotenv()

from basal.config import config
from basal.redis_session import RedisSessionManager
from basal.nats_subscriber import NATSSubscriber
from basal.intent_classifier import IntentClassifier
from basal.orchestrator import Orchestrator
from basal.mcp_client import MCPClient
from basal.advisory_audit import AdvisoryAuditStore
from basal.langfuse_tracer import BasalTracer
from basal.decision_publisher import DecisionPublisher

logging.basicConfig(
    level=getattr(logging, config.LOG_LEVEL, logging.INFO),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger("basal")

app = FastAPI(
    title="Basal - BasalMind Orchestration Layer",
    description="Basal reasons. Entities inform. MCP executes.",
    version=config.VERSION,
)

# Global components
session_manager: Optional[RedisSessionManager] = None
nats_subscriber: Optional[NATSSubscriber] = None
intent_classifier: Optional[IntentClassifier] = None
mcp_client: Optional[MCPClient] = None
audit_store: Optional[AdvisoryAuditStore] = None
tracer: Optional[BasalTracer] = None
orchestrator: Optional[Orchestrator] = None
decision_publisher: Optional[DecisionPublisher] = None


@app.on_event("startup")
async def startup_event():
    global session_manager, nats_subscriber, intent_classifier
    global mcp_client, audit_store, tracer, orchestrator, decision_publisher

    logger.info("🧠 Starting Basal — orchestration layer")

    # 1. Redis session manager
    session_manager = RedisSessionManager()
    session_manager.connect()

    # 2. Advisory audit store (wraps same Redis client)
    audit_store = AdvisoryAuditStore(
        redis_client=session_manager._client if session_manager.is_connected else None
    )
    logger.info("✅ Advisory audit store ready")

    # 3. Intent classifier
    intent_classifier = IntentClassifier()

    # 4. MCP client
    mcp_client = MCPClient(session_manager=session_manager)
    await mcp_client.initialize()

    # 5. Langfuse tracer (gracefully disabled if unavailable)
    tracer = BasalTracer()
    logger.info("✅ Langfuse tracer initialized")

    # 6. Decision publisher (Conductor bridge)
    decision_publisher = DecisionPublisher()
    dp_connected = await decision_publisher.connect()
    logger.info(f"{'✅' if dp_connected else '⚠️'} Decision publisher {'connected' if dp_connected else 'unavailable (non-fatal)'}")

    # 7. Orchestrator (wired with audit store + tracer + decision publisher)
    orchestrator = Orchestrator(
        session_manager=session_manager,
        intent_classifier=intent_classifier,
        mcp_client=mcp_client,
        audit_store=audit_store,
        tracer=tracer,
        nats_publisher=decision_publisher if dp_connected else None,
    )
    await orchestrator.initialize()

    # 8. NATS subscriber
    nats_subscriber = NATSSubscriber()
    nats_connected = await nats_subscriber.connect()
    if nats_connected:
        await nats_subscriber.subscribe(handler=orchestrator.handle_event)

    logger.info(
        f"✅ Basal ready — "
        f"Redis: {'✅' if session_manager.is_connected else '⚠️'} | "
        f"NATS: {'✅' if nats_subscriber.is_connected else '⚠️'} | "
        f"MCP: {'✅' if mcp_client.is_available else '⚠️'} | "
        f"Langfuse: {'✅' if tracer and tracer.is_enabled else '⚠️'} | "
        f"Conductor bridge: {'✅' if dp_connected else '⚠️'}"
    )


@app.on_event("shutdown")
async def shutdown_event():
    logger.info("🛑 Shutting down Basal")
    if nats_subscriber:
        await nats_subscriber.disconnect()
    if decision_publisher:
        await decision_publisher.disconnect()
    if orchestrator:
        await orchestrator.shutdown()
    if mcp_client:
        await mcp_client.shutdown()
    logger.info("✅ Basal shutdown complete")


# ── Core endpoints ─────────────────────────────────────────────────────────────

@app.get("/")
async def root():
    return {
        "service": "Basal",
        "version": config.VERSION,
        "philosophy": "Basal reasons. Entities inform. MCP executes.",
        "port": config.PORT,
    }


@app.get("/health")
async def health():
    redis_health = session_manager.health_check() if session_manager else {"connected": False}
    orch_health = orchestrator.get_health() if orchestrator else {}
    return {
        "status": "healthy",
        "timestamp": datetime.utcnow().isoformat(),
        "service": "Basal",
        "version": config.VERSION,
        "components": {
            "redis": redis_health,
            "nats": {"connected": nats_subscriber.is_connected if nats_subscriber else False},
            "mcp": {"available": mcp_client.is_available if mcp_client else False},
            "orchestrator": {"ready": orchestrator is not None, **orch_health},
            "intent_classifier": {"ready": intent_classifier is not None, "mode": "rule_based"},
            "advisory_audit": {"active": audit_store is not None},
            "langfuse": {"active": tracer.is_enabled if tracer else False},
        },
    }


@app.post("/orchestrate")
async def orchestrate_event(request: Request):
    """Manually submit an event for orchestration (testing / direct injection)."""
    if not orchestrator:
        return JSONResponse(status_code=503, content={"error": "Orchestrator not initialized"})
    try:
        event_data = await request.json()
        if "event_type" not in event_data:
            return JSONResponse(status_code=400, content={"error": "Missing event_type"})
        event_data.setdefault("source_system", "direct_api")
        event_data.setdefault("observed_at", datetime.utcnow().isoformat())
        return await orchestrator.handle_event(event_data)
    except Exception as e:
        logger.error(f"Orchestration failed: {e}", exc_info=True)
        return JSONResponse(status_code=500, content={"error": str(e)})


@app.get("/session/{session_id}")
async def get_session(session_id: str):
    if not session_manager:
        return JSONResponse(status_code=503, content={"error": "Session manager unavailable"})
    session = session_manager.get_session(session_id)
    if not session:
        return JSONResponse(status_code=404, content={"error": f"Session {session_id} not found"})
    return session


@app.get("/orchestration/{request_id}")
async def get_orchestration(request_id: str):
    """Retrieve complete orchestration record by request_id."""
    if not session_manager:
        return JSONResponse(status_code=503, content={"error": "Session manager unavailable"})
    record = session_manager.get_orchestration(request_id)
    if not record:
        return JSONResponse(status_code=404, content={"error": f"Orchestration {request_id} not found"})
    return record


# ── Audit endpoints ────────────────────────────────────────────────────────────

@app.get("/audit/recent")
async def audit_recent(limit: int = 50):
    """
    Recent advisory exchanges — fully auditable process trail.
    Shows what Basal asked each entity and what they returned.
    """
    if not audit_store:
        return JSONResponse(status_code=503, content={"error": "Audit store unavailable"})
    return {
        "exchanges": audit_store.get_recent(limit=limit),
        "stats": audit_store.get_stats(),
    }


@app.get("/audit/request/{request_id}")
async def audit_request(request_id: str):
    """
    Full audit trail for one orchestration request.
    Returns: advisory exchanges + decision record.
    """
    if not audit_store:
        return JSONResponse(status_code=503, content={"error": "Audit store unavailable"})
    advisories = audit_store.get_all_advisories_for_request(request_id)
    decision = audit_store.get_decision(request_id)
    if not advisories and not decision:
        return JSONResponse(status_code=404, content={"error": f"No audit data for {request_id}"})
    return {
        "request_id": request_id,
        "advisories": advisories,
        "decision": decision,
    }


@app.get("/audit/advisory/{request_id}/{entity}")
async def audit_advisory(request_id: str, entity: str):
    """Retrieve one specific advisory exchange."""
    if not audit_store:
        return JSONResponse(status_code=503, content={"error": "Audit store unavailable"})
    record = audit_store.get_advisory(request_id, entity)
    if not record:
        return JSONResponse(
            status_code=404,
            content={"error": f"Advisory {request_id}:{entity} not found"}
        )
    return record


# ── MCP endpoints ──────────────────────────────────────────────────────────────

@app.get("/mcp/skills")
async def list_mcp_skills():
    if not mcp_client:
        return JSONResponse(status_code=503, content={"error": "MCP unavailable"})
    return {
        "available": mcp_client.is_available,
        "skills": mcp_client.get_metrics().get("registered_skills", []),
    }


@app.get("/mcp/audit")
async def mcp_audit(limit: int = 50):
    if not mcp_client:
        return JSONResponse(status_code=503, content={"error": "MCP unavailable"})
    return {
        "entries": mcp_client.get_audit_log(limit=limit),
        "total": mcp_client.get_metrics().get("total_invocations", 0),
    }


@app.post("/mcp/invoke")
async def invoke_mcp_skill(request: Request):
    """Directly invoke an MCP skill (admin/testing only)."""
    if not mcp_client:
        return JSONResponse(status_code=503, content={"error": "MCP unavailable"})
    try:
        body = await request.json()
        skill = body.get("skill")
        if not skill:
            return JSONResponse(status_code=400, content={"error": "Missing 'skill'"})
        result = await mcp_client.invoke(
            skill_name=skill,
            arguments=body.get("arguments", {}),
            correlation_id="admin_direct",
        )
        return result.to_dict()
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})


@app.get("/audit/analytics")
async def audit_analytics():
    """Advisory learning analytics — entity utilization, decision patterns, calibration."""
    if not audit_store:
        return JSONResponse(status_code=503, content={"error": "Audit store unavailable"})
    return audit_store.get_analytics()


@app.get("/metrics")
async def get_metrics():
    orch = orchestrator.get_health() if orchestrator else {}
    return {
        "service": "Basal",
        "timestamp": datetime.utcnow().isoformat(),
        **orch,
        "nats_connected": nats_subscriber.is_connected if nats_subscriber else False,
        "redis_connected": session_manager.is_connected if session_manager else False,
        "mcp_available": mcp_client.is_available if mcp_client else False,
    }


@app.post("/consult")
async def consult(request: Request):
    """Intent classification without side effects (used by other entities)."""
    if not intent_classifier:
        return JSONResponse(status_code=503, content={"error": "Not initialized"})
    try:
        body = await request.json()
        event = body.get("event", body)
        result = intent_classifier.classify_event(event)
        return {
            "entity": "Basal",
            "intent": result.intent,
            "confidence": result.confidence,
            "priority": result.priority,
            "requires_entities": result.requires_entities,
        }
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})


def main():
    uvicorn.run(
        "basal.main:app",
        host=config.HOST,
        port=config.PORT,
        log_level=config.LOG_LEVEL.lower(),
        reload=False,
    )


if __name__ == "__main__":
    main()
