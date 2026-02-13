"""
NATS Publisher for Observer.

Publishes canonical events to NATS JetStream for downstream consumers.
Pure fire-and-forget pattern - Observer doesn't wait for acknowledgments.
"""

import os
import json
import logging
from typing import Dict, Any, Optional
from datetime import datetime
import asyncio

import nats
from nats.js import JetStreamContext
from nats.js.api import StreamConfig, RetentionPolicy

logger = logging.getLogger(__name__)


class NATSPublisher:
    """
    Publishes events to NATS JetStream.

    Philosophy:
    - Fire and forget (async, non-blocking)
    - Observer doesn't wait for downstream consumers
    - Failed publishes are logged but don't block observation
    """

    def __init__(
        self,
        nats_url: str = "nats://localhost:4222",
        stream_name: str = "BASALMIND_EVENTS",
        max_retries: int = 2
    ):
        self.nats_url = nats_url
        self.stream_name = stream_name
        self.max_retries = max_retries

        self.nc: Optional[nats.NATS] = None
        self.js: Optional[JetStreamContext] = None
        self._connected = False

    async def connect(self):
        """Initialize NATS connection and create stream if needed."""
        try:
            self.nc = await nats.connect(self.nats_url)
            self.js = self.nc.jetstream()

            # Create stream if it doesn't exist
            try:
                await self.js.stream_info(self.stream_name)
                logger.info(f"✅ NATS stream '{self.stream_name}' exists")
            except:
                # Stream doesn't exist, create it
                stream_config = StreamConfig(
                    name=self.stream_name,
                    subjects=[
                        "events.slack.*",
                        "events.github.*",
                        "events.postgres.*",
                        "events.salesforce.*",
                        "events.api.*",
                        "events.internal.*"
                    ],
                    retention=RetentionPolicy.LIMITS,
                    max_age=86400 * 7,  # 7 days
                    max_bytes=1024 * 1024 * 1024,  # 1GB
                    storage=nats.js.api.StorageType.FILE
                )
                await self.js.add_stream(stream_config)
                logger.info(f"✅ Created NATS stream '{self.stream_name}'")

            self._connected = True
            logger.info(f"✅ NATS publisher connected: {self.nats_url}")

        except Exception as e:
            logger.error(f"❌ Failed to connect to NATS: {e}")
            self._connected = False
            raise

    async def publish(self, event_data: Dict[str, Any]) -> bool:
        """
        Publish event to NATS.

        Subject pattern: events.{source_system}.{event_type}
        Example: events.slack.message

        Returns:
            bool: True if published successfully, False otherwise
        """
        if not self._connected:
            logger.warning("⚠️ NATS not connected, skipping publish")
            return False

        try:
            # Build subject from event data
            source = event_data.get("source_system", "unknown")
            event_type = event_data.get("event_type", "unknown").split(".")[-1]
            subject = f"events.{source}.{event_type}"

            # Serialize event
            payload = json.dumps(event_data).encode('utf-8')

            # Fire-and-forget publish
            await self.js.publish(subject, payload)

            logger.debug(f"📤 Published to NATS: {subject} ({len(payload)} bytes)")
            return True

        except Exception as e:
            logger.error(f"❌ Failed to publish to NATS: {e}")
            return False

    async def disconnect(self):
        """Gracefully disconnect from NATS."""
        if self.nc:
            try:
                await self.nc.drain()
                logger.info("✅ NATS publisher disconnected")
            except Exception as e:
                logger.error(f"❌ Error disconnecting from NATS: {e}")

        self._connected = False

    @property
    def is_connected(self) -> bool:
        """Check if NATS is connected."""
        return self._connected


# Global publisher instance
_publisher: Optional[NATSPublisher] = None


async def get_publisher() -> NATSPublisher:
    """Get or create global NATS publisher."""
    global _publisher

    if _publisher is None:
        nats_url = os.getenv("NATS_URL", "nats://localhost:4222")
        stream_name = os.getenv("NATS_STREAM", "BASALMIND_EVENTS")

        _publisher = NATSPublisher(nats_url=nats_url, stream_name=stream_name)
        await _publisher.connect()

    return _publisher


async def publish_event(event_data: Dict[str, Any]) -> bool:
    """
    Convenience function to publish event.

    Usage:
        await publish_event(canonical_event.__dict__)
    """
    publisher = await get_publisher()
    return await publisher.publish(event_data)
