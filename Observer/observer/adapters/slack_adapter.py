"""
Slack Adapter - Normalize Slack events to CanonicalEvent.

PURE FUNCTIONS ONLY:
- No side effects
- No LLM calls
- No reasoning
- Just structural transformation (simple key lookups)
"""

from typing import Dict, Any
from observer.event_schema import (
    CanonicalEvent,
    NormalizedFields,
    EventSource,
    create_event
)


def normalize_slack_message(slack_event: Dict[str, Any]) -> CanonicalEvent:
    """
    Normalize Slack message event to canonical format.
    
    PURE FUNCTION:
    - No side effects
    - No LLM calls
    - No reasoning
    - Just structural transformation
    """
    event_type = f"slack.{slack_event.get('type', 'unknown')}"
    source_system = EventSource.SLACK
    
    normalized = NormalizedFields(
        text=slack_event.get("text", "").strip(),
        user_id=slack_event.get("user"),
        channel_id=slack_event.get("channel"),
        thread_id=slack_event.get("thread_ts"),
        workspace_id=slack_event.get("team"),
        source_timestamp=slack_event.get("ts"),
        metadata={
            "subtype": slack_event.get("subtype"),
            "client_msg_id": slack_event.get("client_msg_id"),
            "event_ts": slack_event.get("event_ts"),
        }
    )
    
    return create_event(
        event_type=event_type,
        source_system=source_system,
        raw_payload=slack_event,
        normalized=normalized,
        user_id=normalized.user_id,
        session_id=f"slack_{normalized.channel_id}_{normalized.thread_id or normalized.source_timestamp}"
    )


def normalize_slack_button_click(interaction: Dict[str, Any]) -> CanonicalEvent:
    """
    Normalize Slack button click interaction to canonical format.
    
    PURE FUNCTION - no interpretation, just key extraction.
    """
    event_type = "slack.interaction.button_click"
    source_system = EventSource.SLACK
    
    user = interaction.get("user", {})
    channel = interaction.get("channel", {})
    message = interaction.get("message", {})
    
    # Extract action value (which button was clicked)
    actions = interaction.get("actions", [])
    action_value = actions[0].get("value") if actions else None
    action_text = actions[0].get("text", {}).get("text") if actions else None
    
    normalized = NormalizedFields(
        text=action_text,
        user_id=user.get("id"),
        user_name=user.get("username"),
        channel_id=channel.get("id"),
        channel_name=channel.get("name"),
        thread_id=message.get("thread_ts") or message.get("ts"),
        workspace_id=interaction.get("team", {}).get("id"),
        source_timestamp=interaction.get("action_ts"),
        action_type="button_click",
        action_value=action_value,
        trigger_id=interaction.get("trigger_id"),
        response_url=interaction.get("response_url"),
        callback_id=interaction.get("callback_id"),
        metadata={
            "message_ts": message.get("ts"),
            "action_id": actions[0].get("action_id") if actions else None,
            "block_id": actions[0].get("block_id") if actions else None,
        }
    )
    
    return create_event(
        event_type=event_type,
        source_system=source_system,
        raw_payload=interaction,
        normalized=normalized,
        user_id=normalized.user_id,
        session_id=f"slack_{normalized.channel_id}_{normalized.thread_id}"
    )


def normalize_slack_app_mention(slack_event: Dict[str, Any]) -> CanonicalEvent:
    """
    Normalize Slack app_mention event to canonical format.
    
    PURE FUNCTION - just extracts @mention specific fields.
    """
    event_type = "slack.app_mention"
    source_system = EventSource.SLACK
    
    normalized = NormalizedFields(
        text=slack_event.get("text", "").strip(),
        user_id=slack_event.get("user"),
        channel_id=slack_event.get("channel"),
        thread_id=slack_event.get("thread_ts"),
        workspace_id=slack_event.get("team"),
        source_timestamp=slack_event.get("ts"),
        metadata={
            "event_ts": slack_event.get("event_ts"),
            "client_msg_id": slack_event.get("client_msg_id"),
        }
    )
    
    return create_event(
        event_type=event_type,
        source_system=source_system,
        raw_payload=slack_event,
        normalized=normalized,
        user_id=normalized.user_id,
        session_id=f"slack_{normalized.channel_id}_{normalized.thread_id or normalized.source_timestamp}"
    )
