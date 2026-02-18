"""
Basal NATS Subscriber - Consumes events from BASALMIND_EVENTS stream.

Subject pattern: events.{source}.{event_type}
(as published by Observer's NATSPublisher)
"""

import asyncio
import json
import logging
from typing import Any, Callable, Dict, Optional

import nats
from nats.aio.client import Client as NATSClient

from basal.config import config

logger = logging.getLogger("basal.nats")

# Observer publishes to: events.{source}.{event_type}
# Basal subscribes to all events: events.>
SUBSCRIBE_SUBJECT = "events.>"


class NATSSubscriber:
    """
    JetStream consumer for BASALMIND_EVENTS stream.
    
    Subscribes to all event subjects: events.>
    This catches everything Observer publishes.
    """

    def __init__(self):
        self._nc: Optional[NATSClient] = None
        self._js = None
        self._sub = None
        self._connected = False
        self._handler: Optional[Callable] = None

    async def connect(self) -> bool:
        """Connect to NATS."""
        try:
            self._nc = await nats.connect(
                config.NATS_URL,
                name=config.NATS_CONSUMER_NAME,
                max_reconnect_attempts=5,
                reconnect_time_wait=2,
            )
            self._js = self._nc.jetstream()
            self._connected = True
            logger.info(f"✅ NATS connected: {config.NATS_URL}")
            return True
        except Exception as e:
            logger.warning(f"⚠️ NATS connection failed (non-fatal): {e}")
            self._connected = False
            return False

    async def subscribe(self, handler: Callable) -> bool:
        """
        Subscribe to all events from BASALMIND_EVENTS stream.
        Uses JetStream push consumer with durable name for exactly-once processing.
        
        Args:
            handler: Async callable that receives (event_dict: dict)
        """
        if not self._connected or not self._js:
            logger.warning("⚠️ Cannot subscribe: NATS not connected")
            return False

        self._handler = handler

        # Try JetStream durable consumer first
        try:
            self._sub = await self._js.subscribe(
                SUBSCRIBE_SUBJECT,
                stream=config.NATS_STREAM,
                durable=config.NATS_DURABLE_NAME,
                cb=self._jetstream_handler,
                manual_ack=True,
            )
            logger.info(
                f"✅ JetStream subscribe: {SUBSCRIBE_SUBJECT} "
                f"(stream={config.NATS_STREAM}, durable={config.NATS_DURABLE_NAME})"
            )
            return True
        except Exception as e:
            logger.warning(f"⚠️ JetStream durable subscribe failed: {e}")

        # Fallback: core NATS subscribe (no persistence)
        try:
            self._sub = await self._nc.subscribe(
                SUBSCRIBE_SUBJECT,
                cb=self._core_handler,
            )
            logger.info(f"✅ Core NATS subscribe: {SUBSCRIBE_SUBJECT} (no persistence)")
            return True
        except Exception as e2:
            logger.error(f"❌ NATS subscribe failed: {e2}")
            return False

    async def _jetstream_handler(self, msg):
        """JetStream message handler with acknowledgment."""
        try:
            event_data = json.loads(msg.data.decode())
            logger.debug(f"📨 NATS event received: {event_data.get('event_type', 'unknown')} via {msg.subject}")
            if self._handler:
                await self._handler(event_data)
            await msg.ack()
        except Exception as e:
            logger.error(f"❌ Error processing NATS JetStream message: {e}", exc_info=True)
            try:
                await msg.nak()
            except Exception:
                pass

    async def _core_handler(self, msg):
        """Core NATS message handler (no ack needed)."""
        try:
            event_data = json.loads(msg.data.decode())
            logger.debug(f"📨 NATS event received: {event_data.get('event_type', 'unknown')} via {msg.subject}")
            if self._handler:
                await self._handler(event_data)
        except Exception as e:
            logger.error(f"❌ Error processing NATS core message: {e}", exc_info=True)

    async def disconnect(self):
        """Clean shutdown."""
        if self._sub:
            try:
                await self._sub.unsubscribe()
            except Exception:
                pass
        if self._nc:
            try:
                await self._nc.drain()
                await self._nc.close()
            except Exception:
                pass
        self._connected = False
        logger.info("NATS subscriber disconnected")

    @property
    def is_connected(self) -> bool:
        return self._connected
