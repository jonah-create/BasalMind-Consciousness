"""
Basal Structured Logging - Correlation ID tracking and stage logging.
Follows Observer/Interpreter patterns.
"""

import logging
import os
import uuid
from datetime import datetime
from typing import Optional


def setup_logging(level: str = "INFO") -> logging.Logger:
    """Configure structured logging for Basal."""
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    return logging.getLogger("basal")


logger = setup_logging(os.getenv("LOG_LEVEL", "INFO"))


class CorrelatedLogger:
    """Logger with correlation ID for distributed tracing."""

    def __init__(self, correlation_id: Optional[str] = None, entity: str = "Basal"):
        self.correlation_id = correlation_id or str(uuid.uuid4())[:8]
        self.entity = entity
        self._logger = logging.getLogger(f"basal.{entity.lower()}")

    def _prefix(self, stage: Optional[str] = None) -> str:
        parts = [f"[{self.correlation_id}]"]
        if stage:
            parts.append(f"[{stage}]")
        return " ".join(parts)

    def info(self, msg: str, stage: Optional[str] = None):
        self._logger.info(f"{self._prefix(stage)} {msg}")

    def warning(self, msg: str, stage: Optional[str] = None):
        self._logger.warning(f"{self._prefix(stage)} {msg}")

    def error(self, msg: str, stage: Optional[str] = None, exc_info: bool = False):
        self._logger.error(f"{self._prefix(stage)} {msg}", exc_info=exc_info)

    def debug(self, msg: str, stage: Optional[str] = None):
        self._logger.debug(f"{self._prefix(stage)} {msg}")
