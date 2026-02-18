"""
Basal Decision Publisher - Publishes decisions to NATS for Conductor.

Publishes to the BASALMIND_DECISIONS stream.
Subject pattern: decisions.{channel_id}

Conductor subscribes to decisions.> and acts on actionable decisions.
This is a one-way, fire-and-forget publication — Basal does not wait
for Conductor's response.
"""

import json
import logging
from typing import Any, Dict, Optional

import nats
from nats.aio.client import Client as NATSClient

from basal.config import config

logger = logging.getLogger("basal.decisions")

DECISIONS_STREAM = "BASALMIND_DECISIONS"
DECISIONS_SUBJECT = "decisions.>"


class DecisionPublisher:
    """
    Publishes Basal decisions to NATS for downstream consumers (primarily Conductor).

    Fire-and-forget: Basal publishes and moves on.
    Conductor consumes asynchronously.
    """

    def __init__(self):
        self._nc: Optional[NATSClient] = None
        self._js = None
        self._connected = False

    async def connect(self) -> bool:
        """Connect to NATS and ensure BASALMIND_DECISIONS stream exists."""
        try:
            self._nc = await nats.connect(
                config.NATS_URL,
                name="basal-decision-publisher",
                max_reconnect_attempts=3,
                reconnect_time_wait=1,
            )
            self._js = self._nc.jetstream()

            # Ensure decisions stream exists
            try:
                await self._js.stream_info(DECISIONS_STREAM)
                logger.info(f"✅ NATS decisions stream '{DECISIONS_STREAM}' exists")
            except Exception:
                from nats.js.api import StreamConfig, RetentionPolicy
                stream_config = StreamConfig(
                    name=DECISIONS_STREAM,
                    subjects=["decisions.>"],
                    retention=RetentionPolicy.LIMITS,
                    max_age=86400 * 7,   # 7 days
                    max_bytes=512 * 1024 * 1024,  # 512MB
                )
                await self._js.add_stream(stream_config)
                logger.info(f"✅ Created NATS decisions stream '{DECISIONS_STREAM}'")

            self._connected = True
            logger.info("✅ Decision publisher connected")
            return True

        except Exception as e:
            logger.warning(f"⚠️ Decision publisher connection failed (non-fatal): {e}")
            self._connected = False
            return False

    async def publish_decision(self, subject: str, payload: Dict[str, Any]) -> bool:
        """
        Publish a decision payload to NATS.

        Args:
            subject: NATS subject, e.g. 'decisions.C0123ABC'
            payload: Decision data dict (will be JSON-encoded)

        Returns:
            True if published, False if unavailable (non-fatal)
        """
        if not self._connected or not self._js:
            return False
        try:
            data = json.dumps(payload, default=str).encode()
            await self._js.publish(subject, data)
            logger.debug(f"📤 Decision published → {subject} ({len(data)} bytes)")
            return True
        except Exception as e:
            logger.warning(f"⚠️ Decision publish failed (non-fatal): {e}")
            return False

    async def disconnect(self):
        """Graceful shutdown."""
        if self._nc:
            try:
                await self._nc.drain()
                await self._nc.close()
            except Exception:
                pass
        self._connected = False
        logger.info("Decision publisher disconnected")

    @property
    def is_connected(self) -> bool:
        return self._connected
