"""
Event Deduplication Module

Consolidates duplicate events using client_msg_id grouping and poker hand ranking.
Preserves full audit trail in deduplication_metadata.
"""

from typing import List, Dict, Any
from collections import defaultdict
import hashlib
import logging

logger = logging.getLogger(__name__)


class EventDeduplicator:
    """
    Deduplicate events using semantic content and metadata hints.
    
    Strategy:
    - Group events by client_msg_id (Slack) or content hash (other sources)
    - Apply poker hand ranking to select canonical event
    - Preserve full audit trail for culled duplicates
    """
    
    # Event type rankings (higher = more signal)
    EVENT_RANKINGS = {
        "slack.app_mention": 100,  # Royal flush - explicit bot interaction
        "slack.message": 50,       # Flush - generic message
        "slack.interaction.button_click": 75,  # Straight - user action
        "slack.user_change": 10,   # Pair - metadata only
    }
    
    def __init__(self):
        self.dedup_stats = {
            "total_events": 0,
            "deduplicated_events": 0,
            "duplicate_groups": 0
        }
    
    def deduplicate(self, events: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Deduplicate a batch of events.
        
        Args:
            events: List of event dicts from TimescaleDB
            
        Returns:
            List of canonical events with deduplication_metadata
        """
        if not events:
            return []
        
        self.dedup_stats["total_events"] = len(events)
        
        # Group events by dedup key
        groups = self._group_events(events)
        
        # Select canonical event from each group
        canonical_events = []
        for dedup_key, group in groups.items():
            if len(group) == 1:
                # No duplicates - pass through
                canonical_events.append(group[0])
            else:
                # Multiple events - select canonical and mark duplicates
                self.dedup_stats["duplicate_groups"] += 1
                canonical = self._select_canonical(group)
                canonical_events.append(canonical)
        
        self.dedup_stats["deduplicated_events"] = len(canonical_events)
        
        logger.info(
            f"🔗 Deduplicated {self.dedup_stats['total_events']} events → "
            f"{self.dedup_stats['deduplicated_events']} canonical "
            f"({self.dedup_stats['duplicate_groups']} duplicate groups)"
        )
        
        return canonical_events
    
    def _group_events(self, events: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
        """
        Group events by deduplication key.
        
        For Slack: use client_msg_id
        For others: use content hash
        """
        groups = defaultdict(list)
        
        for event in events:
            source_system = event.get("source_system", "unknown")
            
            if source_system == "slack":
                metadata = event.get("metadata", {})
                if isinstance(metadata, str):
                    import json
                    metadata = json.loads(metadata)
                client_msg_id = metadata.get("client_msg_id") if isinstance(metadata, dict) else None
                metadata = event.get("metadata", {})
                if isinstance(metadata, str):
                    import json
                    metadata = json.loads(metadata)
                client_msg_id = metadata.get("client_msg_id") if isinstance(metadata, dict) else None
                if client_msg_id:
                    dedup_key = f"slack:{client_msg_id}"
                else:
                    # No client_msg_id - treat as unique
                    dedup_key = f"unique:{event.get('event_id')}"
            else:
                # Use content hash for other sources
                text = event.get("text", "")
                user_id = event.get("user_id", "")
                timestamp = event.get("event_time") or event.get("observed_at")
                content = f"{text}:{user_id}:{timestamp}"
                content_hash = hashlib.sha256(content.encode()).hexdigest()[:16]
                dedup_key = f"{source_system}:{content_hash}"
            
            groups[dedup_key].append(event)
        
        return groups
    
    def _select_canonical(self, group: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Select canonical event from duplicate group using poker hand ranking.
        
        Args:
            group: List of duplicate events
            
        Returns:
            Canonical event with deduplication_metadata
        """
        # Sort by event type ranking (descending)
        sorted_events = sorted(
            group,
            key=lambda e: self.EVENT_RANKINGS.get(e.get("event_type", ""), 0),
            reverse=True
        )
        
        canonical = sorted_events[0]
        culled = sorted_events[1:]
        
        # Build deduplication metadata
        canonical["deduplication_metadata"] = {
            "original_event_count": len(group),
            "deduplicated_event_count": 1,
            "consolidation_strategy": "poker_hand_ranking",
            "culled_events": [
                {
                    "event_id": e["event_id"],
                    "event_type": e["event_type"],
                    "reason": f"duplicate_of_{canonical['event_type']}",
                    "survivor_id": canonical["event_id"],
                    "ranking_diff": (
                        self.EVENT_RANKINGS.get(canonical.get("event_type", ""), 0) -
                        self.EVENT_RANKINGS.get(e.get("event_type", ""), 0)
                    )
                }
                for e in culled
            ]
        }
        
        logger.debug(
            f"📌 Selected canonical: {canonical['event_type']} ({canonical['event_id']}) "
            f"over {len(culled)} duplicates"
        )
        
        return canonical
    
    def get_stats(self) -> Dict[str, int]:
        """Get deduplication statistics."""
        return self.dedup_stats.copy()
