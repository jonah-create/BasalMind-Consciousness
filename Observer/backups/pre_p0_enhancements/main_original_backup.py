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
from observer.nats_publisher import NATSPublisher
from observer.timescale_writer import TimescaleWriter

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
nats_publisher: NATSPublisher = None
timescale_writer: TimescaleWriter = None


@app.on_event("startup")
async def startup_event():
    """Initialize Observer components on startup."""
    global redis_cache, metrics, nats_publisher, timescale_writer

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

    # Initialize NATS publisher
    try:
        nats_url = os.getenv("NATS_URL", "nats://localhost:4222")
        stream_name = os.getenv("NATS_STREAM", "BASALMIND_EVENTS")

        nats_publisher = NATSPublisher(nats_url=nats_url, stream_name=stream_name)
        await nats_publisher.connect()
        logger.info(f"✅ NATS connected: {nats_url}")
    except Exception as e:
        logger.warning(f"⚠️ NATS connection failed (non-fatal): {e}")
        nats_publisher = None

    # Initialize TimescaleDB writer
    try:
        host = os.getenv("TIMESCALE_HOST", "localhost")
        port = int(os.getenv("TIMESCALE_PORT", 5432))
        database = os.getenv("TIMESCALE_DB", "basalmind_events")
        user = os.getenv("TIMESCALE_USER", "basalmind")
        password = os.getenv("TIMESCALE_PASSWORD", "basalmind_secure_2024")

        timescale_writer = TimescaleWriter(
            host=host,
            port=port,
            database=database,
            user=user,
            password=password
        )
        await timescale_writer.connect()
        logger.info(f"✅ TimescaleDB connected: {host}:{port}/{database}")
    except Exception as e:
        logger.error(f"❌ TimescaleDB connection failed (CRITICAL): {e}")
        timescale_writer = None

    # Health check Redis
    health = redis_cache.health_check()
    if health.get("connected"):
        host = health.get("host", "unknown")
        port = health.get("port", "unknown")
        logger.info(f"✅ Redis connected: {host}:{port}")
    else:
        error = health.get("error", "unknown error")
        logger.error(f"❌ Redis connection failed: {error}")

    logger.info("✅ Observer service ready")


@app.on_event("shutdown")
async def shutdown_event():
    """Cleanup on shutdown."""
    global nats_publisher

    logger.info("🛑 Shutting down Observer...")

    if nats_publisher:
        await nats_publisher.disconnect()

    if timescale_writer:
        await timescale_writer.disconnect()

    logger.info("✅ Observer shutdown complete")


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
        
        # Write to TimescaleDB (PERMANENT - source of truth)
        if timescale_writer and timescale_writer.is_connected:
            event_for_db = {
                "event_id": canonical_event.event_id,
                "event_time": canonical_event.event_time.isoformat(),
                "observed_at": canonical_event.observed_at.isoformat(),
                "event_type": canonical_event.event_type,
                "source_system": canonical_event.source_system.value,
                "normalized": {
                    k: v for k, v in canonical_event.normalized.__dict__.items()
                    if v is not None
                },
                "session_id": canonical_event.session_id,
                "trace_id": canonical_event.trace_id,
                "correlation_id": canonical_event.correlation_id,
                "raw_payload": canonical_event.raw_payload
            }
            await timescale_writer.write_event(event_for_db)
        else:
            logger.error("❌ CRITICAL: TimescaleDB not available, event NOT persisted!")
        
        logger.info(f"[OBSERVE] Event observed: {canonical_event.event_id}")

        # Publish to NATS for downstream consumers (fire and forget)
        if nats_publisher and nats_publisher.is_connected:
            event_for_nats = {
                "event_id": canonical_event.event_id,
                "event_time": canonical_event.event_time.isoformat(),
                "observed_at": canonical_event.observed_at.isoformat(),
                "event_type": canonical_event.event_type,
                "source_system": canonical_event.source_system.value,
                "normalized": {
                    k: v for k, v in canonical_event.normalized.__dict__.items()
                    if v is not None
                },
                "session_id": canonical_event.session_id,
                "trace_id": canonical_event.trace_id
            }
            await nats_publisher.publish(event_for_nats)

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
