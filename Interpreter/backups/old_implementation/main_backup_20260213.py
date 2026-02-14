"""
Interpreter Service - Stage 2 of the Consciousness Pipeline

Philosophy: Extract meaning from raw observations.

Pipeline Stage 2:
Observer → **Interpreter** → Philosopher → Cartographer → Sentinel → Assembler → Basal

Responsibilities:
- Subscribe to NATS events from Observer
- Extract semantic meaning (intent, entities, sentiment)
- Enrich events with interpretations
- Forward to next stage

NO decision making. NO response generation. Just interpretation.
"""

import os
import logging
import json
import asyncio
from datetime import datetime
from typing import Dict, Any

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
    description="Semantic analysis and meaning extraction",
    version="1.0.0"
)

# Global NATS connection
nc: nats.NATS = None
js: JetStreamContext = None
events_processed = 0


async def process_event(event_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Process event from Observer and extract meaning.
    
    Stage 2 Processing:
    1. Receive canonical event from NATS
    2. Extract intent, entities, sentiment
    3. Add interpretation metadata
    4. Forward to next stage (Philosopher)
    """
    global events_processed
    
    logger.info(f"[INTERPRETER] Processing event: {event_data.get('event_id')} - {event_data.get('event_type')}")
    
    # For now, just log and count
    events_processed += 1
    
    # TODO: Add LLM-based interpretation here
    # interpretation = {
    #     "intent": "unknown",
    #     "entities": [],
    #     "sentiment": "neutral"
    # }
    
    logger.info(f"[INTERPRETER] Event #{events_processed} interpreted successfully")
    
    return {
        "status": "interpreted",
        "event_id": event_data.get("event_id"),
        "events_processed": events_processed
    }


async def message_handler(msg):
    """Handle incoming NATS messages."""
    try:
        data = json.loads(msg.data.decode())
        logger.debug(f"[NATS] Received message on {msg.subject}")
        
        result = await process_event(data)
        
        # TODO: Publish to next stage (Philosopher)
        
        await msg.ack()
        
    except Exception as e:
        logger.error(f"[ERROR] Failed to process message: {e}", exc_info=True)


@app.on_event("startup")
async def startup_event():
    """Initialize Interpreter on startup."""
    global nc, js
    
    logger.info("🚀 Starting Interpreter service (Stage 2)...")
    
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
        logger.info("✅ Interpreter service ready")
        
    except Exception as e:
        logger.error(f"❌ Failed to connect to NATS: {e}")
        raise


@app.on_event("shutdown")
async def shutdown_event():
    """Cleanup on shutdown."""
    global nc
    
    logger.info("🛑 Shutting down Interpreter...")
    
    if nc:
        await nc.drain()
    
    logger.info("✅ Interpreter shutdown complete")


@app.get("/")
async def root():
    """Root endpoint - service info."""
    return {
        "service": "Interpreter",
        "stage": 2,
        "version": "1.0.0",
        "philosophy": "Extract meaning from raw observations",
        "status": "interpreting",
        "events_processed": events_processed
    }


@app.get("/health")
async def health():
    """Health check endpoint."""
    return {
        "status": "healthy",
        "timestamp": datetime.utcnow().isoformat(),
        "events_processed": events_processed,
        "nats_connected": nc is not None and nc.is_connected
    }


@app.get("/metrics")
async def metrics():
    """Get Interpreter metrics."""
    return {
        "events_processed": events_processed,
        "nats_connected": nc is not None and nc.is_connected
    }


def main():
    """Run the Interpreter service."""
    host = os.getenv("INTERPRETER_HOST", "0.0.0.0")
    port = int(os.getenv("INTERPRETER_PORT", 5003))
    
    logger.info(f"🧠 Starting Interpreter on {host}:{port}")
    
    uvicorn.run(
        "interpreter.main:app",
        host=host,
        port=port,
        log_level=os.getenv("LOG_LEVEL", "info").lower(),
        reload=False
    )


if __name__ == "__main__":
    main()
