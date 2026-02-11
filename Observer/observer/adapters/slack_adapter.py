"""
Slack Adapter - Normalize Slack events to CanonicalEvent.

PURE FUNCTIONS ONLY:
- No side effects
- No LLM calls
- No reasoning
- Just structural transformation (simple key lookups)
"""

import sys
sys.path.insert(0, '/opt/basalmind')

from typing import Dict, Any
from Consciousness.observer.event_schema import (
    CanonicalEvent,
    NormalizedFields,
    EventSource,
    create_event
)


def normalize_slack_message(slack_event: Dict[str, Any]) -> CanonicalEvent:
    """
    Normalize Slack message event to canonical format.
    
    PURE FUNCTION:
    - Input: Raw Slack event JSON
    - Output: CanonicalEvent
    - No LLMs, no reasoning, just key lookups
    """
    
    event_type = f"slack.{slack_event.get('type', 'unknown')}"
    source_system = EventSource.SLACK
    
    # Extract normalized fields (simple key lookups only)
    normalized = NormalizedFields(
        text=slack_event.get("text", "").strip(),
        user_id=slack_event.get("user"),
        user_name=slack_event.get("username"),  # If available
        channel_id=slack_event.get("channel"),
        thread_id=slack_event.get("thread_ts"),
        source_timestamp=slack_event.get("ts"),
        workspace_id=slack_event.get("team"),
        
        # Escape hatch for Slack-specific fields
        metadata={
            "subtype": slack_event.get("subtype"),
            "client_msg_id": slack_event.get("client_msg_id"),
            "event_ts": slack_event.get("event_ts")
        }
    )
    
    # Generate session ID (deterministic)
    session_id = f"slack_{normalized.channel_id}_{normalized.thread_id or normalized.source_timestamp}"
    
    # Create canonical event
    return create_event(
        event_type=event_type,
        source_system=source_system,
        raw_payload=slack_event,  # Preserve original
        normalized=normalized,
        user_id=normalized.user_id,
        session_id=session_id
    )


def normalize_slack_button_click(interaction: Dict[str, Any]) -> CanonicalEvent:
    """
    Normalize Slack button interaction to canonical format.
    
    PURE FUNCTION - no side effects.
    """
    
    event_type = "slack.button_click"
    source_system = EventSource.SLACK
    
    # Extract action (first action in list)
    actions = interaction.get("actions", [])
    action = actions[0] if actions else {}
    
    # Extract user info
    user = interaction.get("user", {})
    channel = interaction.get("channel", {})
    
    normalized = NormalizedFields(
        action_type=action.get("type"),
        action_value=action.get("value"),
        user_id=user.get("id"),
        user_name=user.get("name"),
        channel_id=channel.get("id"),
        channel_name=channel.get("name"),
        trigger_id=interaction.get("trigger_id"),
        response_url=interaction.get("response_url"),
        callback_id=interaction.get("callback_id"),
        
        # Slack-specific
        metadata={
            "action_id": action.get("action_id"),
            "block_id": action.get("block_id"),
            "action_ts": action.get("action_ts")
        }
    )
    
    # Create canonical event
    return create_event(
        event_type=event_type,
        source_system=source_system,
        raw_payload=interaction,
        normalized=normalized,
        user_id=normalized.user_id,
        session_id=f"slack_{normalized.channel_id}_{interaction.get('message_ts')}"
    )


def normalize_slack_app_mention(slack_event: Dict[str, Any]) -> CanonicalEvent:
    """
    Normalize Slack app_mention event.
    
    App mentions are just messages, so reuse message normalization.
    """
    
    # App mention is essentially a message
    canonical = normalize_slack_message(slack_event)
    
    # Update event type
    canonical.event_type = "slack.app_mention"
    
    return canonical
