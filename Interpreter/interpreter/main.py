"""
Enhanced Interpreter - Stage 2 Data Enrichment

Philosophy: Capture raw information for downstream analysis WITHOUT doing the analysis itself.

New Capabilities:
1. Session tracking (generate/maintain session IDs)
2. Timing/latency capture (processing time, delays)
3. Trace correlation (link related events)
4. Context enrichment (IP geo, user agent parsing, etc.)
5. Sequence tracking (event ordering within sessions)

NO LLM analysis. NO reasoning. Just structured data enrichment.
"""

import os
import logging
import json
import asyncio
import hashlib
import redis.asyncio as redis
from datetime import datetime, timedelta
from typing import Dict, Any, Optional
from uuid import uuid4

from fastapi import FastAPI
import uvicorn
from dotenv import load_dotenv
import nats
from nats.js import JetStreamContext

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("interpreter")

# Initialize FastAPI app
app = FastAPI(
    title="Interpreter - BasalMind Stage 2",
    description="Data enrichment and context capture",
    version="2.0.0"
)

# Global connections
nc: nats.NATS = None
js: JetStreamContext = None
redis_client: redis.Redis = None
events_processed = 0
enrichment_stats = {
    "sessions_created": 0,
    "traces_linked": 0,
    "timing_captured": 0
}


class DataEnricher:
    """Enrich events with additional raw information for downstream consumers."""
    
    def __init__(self, redis_client: redis.Redis):
        self.redis = redis_client
    
    async def enrich_event(self, event_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Enrich event with additional context and tracking data.
        
        Returns enriched event with new fields in _interpreter_enrichment.
        """
        enriched = event_data.copy()
        interpreter_data = {}
        
        # 1. TIMING: Capture processing timestamps
        interpreter_data["received_at"] = datetime.utcnow().isoformat()
        interpreter_data["interpreter_processing_ms"] = 0  # Will be updated at end
        
        # Calculate Observer->Interpreter latency
        if "observed_at" in event_data:
            try:
                observed_time = datetime.fromisoformat(event_data["observed_at"].replace("Z", "+00:00"))
                received_time = datetime.utcnow()
                interpreter_data["pipeline_latency_ms"] = (received_time - observed_time).total_seconds() * 1000
            except Exception as e:
                logger.debug(f"Could not calculate latency: {e}")
        
        # 2. SESSION TRACKING: Generate or retrieve session ID
        session_data = await self._get_or_create_session(event_data)
        if session_data:
            interpreter_data["session"] = session_data
        
        # 3. TRACE CORRELATION: Link related events
        trace_info = await self._manage_trace_correlation(event_data)
        if trace_info:
            interpreter_data["trace"] = trace_info
        
        # 4. SEQUENCE TRACKING: Event ordering within session
        if session_data:
            sequence_num = await self._increment_session_sequence(session_data["session_id"])
            interpreter_data["sequence_in_session"] = sequence_num
        
        # 5. CONTEXT CAPTURE: Extract additional raw context
        context = self._extract_context(event_data)
        if context:
            interpreter_data["context"] = context
        
        # 6. EVENT CHAIN: Track if this event spawned from another
        if "trace_id" in event_data or "correlation_id" in event_data:
            interpreter_data["is_chained_event"] = True
            interpreter_data["parent_trace_id"] = event_data.get("trace_id")
        
        # Add all enrichments to event
        enriched["_interpreter_enrichment"] = interpreter_data
        enriched["_stage"] = 2
        enriched["_interpreter_version"] = "2.0.0"
        
        return enriched
    
    async def _get_or_create_session(self, event_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Generate or retrieve session ID for user.
        Sessions expire after 2 hours of inactivity.
        """
        try:
            # Extract user identifier (could be user_id, IP, device_id, etc.)
            user_id = event_data.get("user_id")
            if not user_id:
                normalized = event_data.get("normalized", {})
                user_id = normalized.get("user_id") if isinstance(normalized, dict) else None
            
            if not user_id:
                return None
            
            # Check for existing session
            session_key = f"session:{user_id}"
            existing_session = await self.redis.get(session_key)
            
            if existing_session:
                session_data = json.loads(existing_session)
                # Extend session TTL
                await self.redis.expire(session_key, 7200)  # 2 hours
                
                # Update session metrics
                session_data["events_in_session"] = session_data.get("events_in_session", 0) + 1
                session_data["last_activity"] = datetime.utcnow().isoformat()
                await self.redis.setex(session_key, 7200, json.dumps(session_data))
                
                return session_data
            else:
                # Create new session
                session_id = str(uuid4())
                session_data = {
                    "session_id": session_id,
                    "user_id": user_id,
                    "started_at": datetime.utcnow().isoformat(),
                    "last_activity": datetime.utcnow().isoformat(),
                    "events_in_session": 1,
                    "source_system": event_data.get("source_system"),
                    "first_event_type": event_data.get("event_type")
                }
                
                await self.redis.setex(session_key, 7200, json.dumps(session_data))
                
                global enrichment_stats
                enrichment_stats["sessions_created"] += 1
                
                return session_data
        
        except Exception as e:
            logger.error(f"Session tracking error: {e}")
            return None
    
    async def _manage_trace_correlation(self, event_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Manage trace correlation for event chains.
        Traces track causally-related events.
        """
        try:
            # If event already has trace_id, use it
            existing_trace = event_data.get("trace_id")
            if existing_trace:
                trace_data = {"trace_id": existing_trace, "is_new_trace": False}
            else:
                # Generate new trace for this event
                trace_id = f"trace_{str(uuid4())[:8]}_{int(datetime.utcnow().timestamp())}"
                trace_data = {"trace_id": trace_id, "is_new_trace": True}
                
                global enrichment_stats
                enrichment_stats["traces_linked"] += 1
            
            # Store trace origin information
            trace_key = f"trace:{trace_data['trace_id']}"
            trace_info = {
                "trace_id": trace_data["trace_id"],
                "origin_event_id": event_data.get("event_id"),
                "origin_event_type": event_data.get("event_type"),
                "origin_source": event_data.get("source_system"),
                "created_at": datetime.utcnow().isoformat()
            }
            
            # Store trace info for 24 hours
            await self.redis.setex(trace_key, 86400, json.dumps(trace_info))
            
            return trace_data
        
        except Exception as e:
            logger.error(f"Trace correlation error: {e}")
            return None
    
    async def _increment_session_sequence(self, session_id: str) -> int:
        """Increment and return sequence number for session."""
        try:
            seq_key = f"session:seq:{session_id}"
            sequence = await self.redis.incr(seq_key)
            await self.redis.expire(seq_key, 7200)  # Match session TTL
            return sequence
        except Exception as e:
            logger.error(f"Sequence tracking error: {e}")
            return 0
    
    def _extract_context(self, event_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Extract additional contextual information from event.
        This is RAW data extraction, not analysis.
        """
        context = {}
        
        # Extract source-specific context
        source = event_data.get("source_system")
        event_type = event_data.get("event_type", "")
        
        # Nginx/HTTP events
        if "nginx" in event_type or "http" in event_type.lower():
            normalized = event_data.get("normalized", {})
            if isinstance(normalized, dict):
                metadata = normalized.get("metadata", {})
                if isinstance(metadata, dict):
                    context["http"] = {
                        "method": metadata.get("method"),
                        "path": metadata.get("path"),
                        "status": metadata.get("status"),
                        "user_agent": metadata.get("user_agent"),
                        "ip": metadata.get("ip")
                    }
        
        # Slack events
        elif source == "slack":
            normalized = event_data.get("normalized", {})
            if isinstance(normalized, dict):
                context["slack"] = {
                    "channel_id": normalized.get("channel_id"),
                    "workspace_id": normalized.get("workspace_id"),
                    "has_thread": bool(normalized.get("thread_id"))
                }
        
        # Cloudflare events
        elif source == "cloudflare":
            normalized = event_data.get("normalized", {})
            if isinstance(normalized, dict):
                metadata = normalized.get("metadata", {})
                if isinstance(metadata, dict):
                    context["cloudflare"] = {
                        "country": metadata.get("country"),
                        "latency_ms": metadata.get("latency_ms"),
                        "method": metadata.get("method")
                    }
        
        return context if context else None


async def process_event(event_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Process event from Observer and enrich with tracking data.
    
    Stage 2 Processing:
    1. Receive canonical event from NATS
    2. Add timing/latency metrics
    3. Generate/retrieve session ID
    4. Add trace correlation
    5. Capture sequence information
    6. Forward enriched event to next stage
    """
    global events_processed
    start_time = datetime.utcnow()
    
    logger.info(f"[INTERPRETER] Processing event: {event_data.get('event_id')} - {event_data.get('event_type')}")
    
    # Enrich event with tracking data
    enricher = DataEnricher(redis_client)
    enriched_event = await enricher.enrich_event(event_data)
    
    # Update processing time
    processing_ms = (datetime.utcnow() - start_time).total_seconds() * 1000
    if "_interpreter_enrichment" in enriched_event:
        enriched_event["_interpreter_enrichment"]["interpreter_processing_ms"] = processing_ms
    
    global enrichment_stats
    enrichment_stats["timing_captured"] += 1
    
    events_processed += 1
    
    session_preview = enriched_event.get("_interpreter_enrichment", {}).get("session", {}).get("session_id", "N/A")
    if session_preview != "N/A":
        session_preview = session_preview[:8] + "..."
    
    logger.info(f"[INTERPRETER] Event #{events_processed} enriched (session: {session_preview})")
    
    # TODO: Publish to next stage (Philosopher) via NATS
    
    return {
        "status": "interpreted",
        "event_id": event_data.get("event_id"),
        "events_processed": events_processed,
        "enrichments_added": list(enriched_event.get("_interpreter_enrichment", {}).keys())
    }


async def message_handler(msg):
    """Handle incoming NATS messages."""
    try:
        data = json.loads(msg.data.decode())
        logger.debug(f"[NATS] Received message on {msg.subject}")
        
        result = await process_event(data)
        
        # TODO: Publish enriched event to philosopher.events.{type}
        
        await msg.ack()
        
    except Exception as e:
        logger.error(f"[ERROR] Failed to process message: {e}", exc_info=True)


@app.on_event("startup")
async def startup_event():
    """Initialize Interpreter on startup."""
    global nc, js, redis_client
    
    logger.info("🚀 Starting Interpreter service (Stage 2 - Enhanced)...")
    
    # Connect to Redis
    try:
        redis_host = os.getenv("REDIS_HOST", "localhost")
        redis_port = int(os.getenv("REDIS_PORT", "6390"))
        redis_password = os.getenv("REDIS_PASSWORD", "_t9iV2e(p9voC34HpkvxirRoy%8cSYcf")
        
        redis_client = redis.Redis(
            host=redis_host,
            port=redis_port,
            password=redis_password,
            decode_responses=False
        )
        
        await redis_client.ping()
        logger.info(f"✅ Redis connected: {redis_host}:{redis_port}")
        
    except Exception as e:
        logger.error(f"❌ Failed to connect to Redis: {e}")
        redis_client = None
    
    # Connect to NATS
    try:
        nats_url = os.getenv("NATS_URL", "nats://localhost:4222")
        nc = await nats.connect(nats_url)
        js = nc.jetstream()
        
        # Clean up old consumer if it exists
        try:
            await js.delete_consumer("BASALMIND_EVENTS", "interpreter-consumer")
            logger.info("🗑️  Deleted existing consumer")
        except Exception as e:
            logger.debug(f"No existing consumer to delete: {e}")
        
        # Subscribe to Observer events
        await js.subscribe(
            "events.>",  # All events from Observer
            stream="BASALMIND_EVENTS",
            durable="interpreter-consumer",
            cb=message_handler
        )
        
        logger.info(f"✅ Interpreter subscribed to NATS: events.>")
        logger.info("✅ Interpreter service ready (Enhanced Data Capture Mode)")
        
    except Exception as e:
        logger.error(f"❌ Failed to connect to NATS: {e}")
        raise


@app.on_event("shutdown")
async def shutdown_event():
    """Cleanup on shutdown."""
    global nc, redis_client
    
    logger.info("🛑 Shutting down Interpreter...")
    
    if nc:
        await nc.drain()
    
    if redis_client:
        await redis_client.close()
    
    logger.info("✅ Interpreter shutdown complete")


@app.get("/")
async def root():
    """Root endpoint - service info."""
    return {
        "service": "Interpreter",
        "stage": 2,
        "version": "2.0.0",
        "mode": "enhanced_data_capture",
        "philosophy": "Capture raw information for downstream analysis",
        "status": "interpreting",
        "events_processed": events_processed,
        "enrichment_stats": enrichment_stats
    }


@app.get("/health")
async def health():
    """Health check endpoint."""
    return {
        "status": "healthy",
        "timestamp": datetime.utcnow().isoformat(),
        "events_processed": events_processed,
        "nats_connected": nc is not None and nc.is_connected,
        "redis_connected": redis_client is not None,
        "enrichment_stats": enrichment_stats
    }


@app.get("/metrics")
async def metrics():
    """Get Interpreter metrics."""
    return {
        "events_processed": events_processed,
        "nats_connected": nc is not None and nc.is_connected,
        "redis_connected": redis_client is not None,
        "enrichment_stats": enrichment_stats
    }


def main():
    """Run the Interpreter service."""
    host = os.getenv("INTERPRETER_HOST", "0.0.0.0")
    port = int(os.getenv("INTERPRETER_PORT", 5003))
    
    logger.info(f"🧠 Starting Enhanced Interpreter on {host}:{port}")
    
    uvicorn.run(
        "interpreter.main:app",
        host=host,
        port=port,
        log_level=os.getenv("LOG_LEVEL", "info").lower(),
        reload=False
    )


if __name__ == "__main__":
    main()
