"""
Canonical Event Schema - Pure Data Structures

This module defines the canonical event format for all observed signals.
NO REASONING. NO LLMS. Just data normalization.
"""

from dataclasses import dataclass, asdict
from datetime import datetime
from typing import Dict, Any, Optional, Union
from enum import Enum
import uuid
import json


class EventSource(str, Enum):
    """Known signal sources (deterministic classification only)."""
    SLACK = "slack"
    GITHUB = "github"
    POSTGRES = "postgres"
    SALESFORCE = "salesforce"
    API = "api"
    INTERNAL = "internal"


@dataclass
class NormalizedFields:
    """
    Typed normalized fields (common across all sources).
    
    Type-safe with clear schema for enterprise applications.
    """
    
    # Content
    text: Optional[str] = None
    subject: Optional[str] = None
    body: Optional[str] = None
    
    # Identity
    user_id: Optional[str] = None
    user_name: Optional[str] = None
    user_email: Optional[str] = None
    
    # Location/Context
    channel_id: Optional[str] = None
    channel_name: Optional[str] = None
    thread_id: Optional[str] = None
    workspace_id: Optional[str] = None
    
    # Timing
    source_timestamp: Optional[str] = None
    
    # Action/Interaction
    action_type: Optional[str] = None
    action_value: Optional[str] = None
    trigger_id: Optional[str] = None
    
    # Response handling
    response_url: Optional[str] = None
    callback_id: Optional[str] = None
    
    # Escape hatch for source-specific fields
    metadata: Optional[Dict[str, Any]] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary, excluding None values."""
        return {k: v for k, v in asdict(self).items() if v is not None}


@dataclass
class CanonicalEvent:
    """Canonical event structure for all observed signals."""
    
    # Core identity
    event_id: str
    event_time: datetime
    observed_at: datetime
    
    # Classification
    event_type: str
    source_system: EventSource
    
    # Raw payload
    raw_payload: Dict[str, Any]
    
    # Normalized fields
    normalized: NormalizedFields
    
    # Metadata
    trace_id: Optional[str] = None
    correlation_id: Optional[str] = None
    session_id: Optional[str] = None
    user_id: Optional[str] = None
    
    # Observability
    persisted_at: Optional[datetime] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            'event_id': self.event_id,
            'event_time': self.event_time.isoformat(),
            'observed_at': self.observed_at.isoformat(),
            'event_type': self.event_type,
            'source_system': self.source_system.value,
            'raw_payload': self.raw_payload,
            'normalized': self.normalized.to_dict(),
            'trace_id': self.trace_id,
            'correlation_id': self.correlation_id,
            'session_id': self.session_id,
            'user_id': self.user_id,
            'persisted_at': self.persisted_at.isoformat() if self.persisted_at else None,
        }


def create_event(
    event_type: str,
    source_system: EventSource,
    raw_payload: Dict[str, Any],
    normalized: NormalizedFields,
    **metadata
) -> CanonicalEvent:
    """
    Create a canonical event.
    
    DETERMINISTIC - No LLMs, no reasoning.
    """
    now = datetime.utcnow()
    
    return CanonicalEvent(
        event_id=str(uuid.uuid4()),
        event_time=now,
        observed_at=now,
        event_type=event_type,
        source_system=source_system,
        raw_payload=raw_payload,
        normalized=normalized,
        **metadata
    )
