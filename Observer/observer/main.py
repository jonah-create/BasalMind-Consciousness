"""
Observer Service - Pure Sensory Layer

Philosophy: Sense, dont think. Observe, dont judge.

This is a FastAPI service that receives signals from all sources,
normalizes them to canonical events, and persists to storage.

NO LLMs. NO reasoning. NO decision making.
Just pure passive observation.
"""

import os
import logging
from typing import Dict, Any
from datetime import datetime

from fastapi import FastAPI, Request, Response
from fastapi.responses import JSONResponse
import uvicorn
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Import Observer components
from observer.event_schema import create_event, EventSource, NormalizedFields
from observer.adapters.slack_adapter import (
    normalize_slack_message,
    normalize_slack_button_click,
    normalize_slack_app_mention
)
from observer.storage.redis_cache import RedisCache
from observer.metrics import ObserverMetrics

# Configure logging
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("observer")

# Initialize FastAPI app
app = FastAPI(
    title="Observer - BasalMind Sensory Layer",
    description="Pure passive observation without judgment or reasoning",
    version="0.1.0"
)

# Global state (initialized on startup)
redis_cache: RedisCache = None
metrics: ObserverMetrics = None


@app.on_event("startup")
async def startup_event():
    """Initialize Observer components on startup."""
    global redis_cache, metrics
    
    logger.info("🚀 Starting Observer service...")
    
    # Initialize Redis cache
    redis_cache = RedisCache(
        host=os.getenv("REDIS_HOST", "localhost"),
        port=int(os.getenv("REDIS_PORT", 6390)),
        password=os.getenv("REDIS_PASSWORD"),
        db=int(os.getenv("REDIS_DB", 0))
    )
    
    # Initialize metrics
    metrics = ObserverMetrics()
    
    # Health check
    # Health check
    health = redis_cache.health_check()
    if health.get("connected"):
        host = health.get("host", "unknown")
        port = health.get("port", "unknown")
        logger.info(f"✅ Redis connected: {host}:{port}")
    else:
        error = health.get("error", "unknown error")
        logger.error(f"❌ Redis connection failed: {error}")
    
    logger.info("✅ Observer service ready")


@app.get("/")
async def root():
    """Root endpoint - service info."""
    return {
        "service": "Observer",
        "version": "0.1.0",
        "philosophy": "Sense, dont think. Observe, dont judge.",
        "status": "observing",
        "endpoints": {
            "health": "/health",
            "metrics": "/metrics",
            "slack_events": "/observe/slack/events",
            "slack_interactions": "/observe/slack/interactions"
        }
    }


@app.get("/health")
async def health():
    """Health check endpoint."""
    redis_health = redis_cache.health_check() if redis_cache else {"connected": False}
    
    return {
        "status": "healthy" if redis_health.get("connected") else "degraded",
        "timestamp": datetime.utcnow().isoformat(),
        "components": {
            "redis": redis_health,
            "metrics": {
                "events_observed": metrics.events_observed if metrics else 0
            }
        }
    }


@app.get("/metrics")
async def get_metrics():
    """Get Observer metrics."""
    if not metrics:
        return {"error": "Metrics not initialized"}
    
    return metrics.get_summary()


@app.post("/observe/slack/events")
async def observe_slack_event(request: Request):
    """
    Observe Slack event webhook.
    
    Responsibilities:
    1. Receive raw Slack event
    2. Normalize to canonical format
    3. Deduplicate via Redis
    4. Cache event
    5. Return 200 OK
    
    NOT responsible for:
    - Interpreting the event
    - Deciding what to do
    - Generating responses
    - Calling LLMs
    """
    try:
        raw_data = await request.json()
        
        # Slack URL verification challenge
        if raw_data.get("type") == "url_verification":
            logger.info("[SLACK] URL verification challenge received")
            return {"challenge": raw_data.get("challenge")}
        
        # Extract event
        slack_event = raw_data.get("event", {})
        event_type = slack_event.get("type")
        
        logger.info(f"[OBSERVE] Slack event: {event_type}")
        
        # Record observation
        if metrics:
            metrics.record_event_observed("slack")
        
        # Normalize to canonical format
        if event_type == "message":
            # Skip bot messages
            if slack_event.get("subtype") == "bot_message":
                logger.debug("[OBSERVE] Skipping bot message")
                return {"status": "ignored", "reason": "bot_message"}
            
            canonical_event = normalize_slack_message(slack_event)
            
        elif event_type == "app_mention":
            canonical_event = normalize_slack_app_mention(slack_event)
            
        else:
            # Unknown type - create generic canonical event
            logger.warning(f"[OBSERVE] Unknown Slack event type: {event_type}")
            canonical_event = create_event(
                event_type=f"slack.{event_type}",
                source_system=EventSource.SLACK,
                raw_payload=slack_event,
                normalized=NormalizedFields()
            )
        
        # Dedup check
        start_dedup = datetime.utcnow()
        event_data = {
            "source_system": canonical_event.source_system.value,
            "event_type": canonical_event.event_type,
            "user_id": canonical_event.normalized.user_id,
            "timestamp": canonical_event.normalized.source_timestamp,
            "text": canonical_event.normalized.text or ""
        }
        
        is_duplicate = await redis_cache.is_duplicate(event_data)
        dedup_duration_ms = (datetime.utcnow() - start_dedup).total_seconds() * 1000
        
        if metrics:
            metrics.record_dedup_check(dedup_duration_ms, is_duplicate)
        
        if is_duplicate:
            logger.debug(f"[OBSERVE] Duplicate event: {canonical_event.event_id}")
            return {"status": "duplicate", "event_id": canonical_event.event_id}
        
        # Cache the event
        await redis_cache.cache_event(
            canonical_event.event_id,
            {
                "event_type": canonical_event.event_type,
                "source_system": canonical_event.source_system.value,
                "normalized": {
                    k: v for k, v in canonical_event.normalized.__dict__.items()
                    if v is not None
                },
                "raw_payload": canonical_event.raw_payload
            }
        )
        
        # Track session
        if canonical_event.session_id:
            await redis_cache.track_session(
                canonical_event.session_id,
                canonical_event.event_id
            )
        
        logger.info(f"[OBSERVE] Event observed: {canonical_event.event_id}")
        
        # Observer job is done
        return {
            "status": "observed",
            "event_id": canonical_event.event_id,
            "event_type": canonical_event.event_type,
            "dedup_ms": round(dedup_duration_ms, 2)
        }
        
    except Exception as e:
        logger.error(f"[ERROR] Failed to observe event: {e}", exc_info=True)
        return JSONResponse(
            status_code=500,
            content={"status": "error", "message": str(e)}
        )


@app.post("/observe/slack/interactions")
async def observe_slack_interaction(request: Request):
    """Observe Slack button clicks and other interactions."""
    try:
        form_data = await request.form()
        import json
        payload = json.loads(form_data.get("payload", "{}"))
        
        logger.info(f"[OBSERVE] Slack interaction: {payload.get(type)}")
        
        # Record observation
        if metrics:
            metrics.record_event_observed("slack_interaction")
        
        # Normalize
        canonical_event = normalize_slack_button_click(payload)
        
        # Dedup check
        event_data = {
            "source_system": canonical_event.source_system.value,
            "event_type": canonical_event.event_type,
            "user_id": canonical_event.normalized.user_id,
            "action_id": canonical_event.normalized.action_type,
            "timestamp": canonical_event.event_time.isoformat()
        }
        
        is_duplicate = await redis_cache.is_duplicate(event_data)
        
        if is_duplicate:
            logger.debug(f"[OBSERVE] Duplicate interaction: {canonical_event.event_id}")
            return {"status": "duplicate"}
        
        # Cache
        await redis_cache.cache_event(canonical_event.event_id, event_data)
        
        logger.info(f"[OBSERVE] Interaction observed: {canonical_event.event_id}")
        
        return {
            "status": "observed",
            "event_id": canonical_event.event_id
        }
        
    except Exception as e:
        logger.error(f"[ERROR] Failed to observe interaction: {e}", exc_info=True)
        return JSONResponse(
            status_code=500,
            content={"status": "error", "message": str(e)}
        )


def main():
    """Run the Observer service."""
    host = os.getenv("OBSERVER_HOST", "0.0.0.0")
    port = int(os.getenv("OBSERVER_PORT", 5000))
    
    logger.info(f"👁️  Starting Observer on {host}:{port}")
    
    uvicorn.run(
        "observer.main:app",
        host=host,
        port=port,
        log_level=os.getenv("LOG_LEVEL", "info").lower(),
        reload=False
    )


if __name__ == "__main__":
    main()
