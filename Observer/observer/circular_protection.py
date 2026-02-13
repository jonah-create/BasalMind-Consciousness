"""
Circular Reference Protection for BasalMind Observer
Prevents infinite loops when processing internal system events
"""

from typing import Dict, Any, Optional
import logging

logger = logging.getLogger(__name__)


class CircularReferenceProtector:
    """
    Protects against circular references and infinite loops in event processing.

    Rules:
    1. Observer cannot process its own operational events
    2. Events have a maximum recursion depth of 3
    3. Events are tagged with their originating entity
    """

    # Event types that Observer should never ingest
    EXCLUDED_EVENT_PATTERNS = [
        "internal.observer_",           # All Observer operational events
        "internal.event_processing_",   # Event processing stats
        "internal.redis_cache_",        # Cache operations
        "internal.timescale_write_",    # Database writes
        "internal.neo4j_query_"         # Neo4j operations
    ]

    # Maximum event processing depth
    MAX_DEPTH = 3

    # Entities that can generate internal events
    INTERNAL_ENTITIES = [
        "Observer", "Interpreter", "Philosopher",
        "Cartographer", "Sentinel", "Assembler", "Basal"
    ]

    @classmethod
    def should_process_event(cls, event: Dict[str, Any]) -> tuple[bool, Optional[str]]:
        """
        Determine if an event should be processed or skipped.

        Returns:
            (should_process: bool, reason: Optional[str])
        """

        # 1. Check if event is from excluded pattern
        event_type = event.get("event_type", "")
        for pattern in cls.EXCLUDED_EVENT_PATTERNS:
            if event_type.startswith(pattern):
                return False, f"Excluded pattern: {pattern}"

        # 2. Check recursion depth
        depth = event.get("_depth", 0)
        if depth >= cls.MAX_DEPTH:
            logger.warning(f"Event depth limit reached ({depth}): {event_type}")
            return False, f"Depth limit exceeded: {depth} >= {cls.MAX_DEPTH}"

        # 3. Check if Observer is trying to process its own event
        originated_from = event.get("_originated_from")
        processing_entity = event.get("_processing_entity", "Observer")

        if originated_from == processing_entity == "Observer":
            return False, "Observer cannot process its own events"

        # 4. Check for rapid event loops (same event type from same entity)
        # This would require keeping a sliding window of recent events
        # Implemented in should_check_loop_detection()

        return True, None

    @classmethod
    def tag_event_for_processing(
        cls,
        event: Dict[str, Any],
        processing_entity: str,
        parent_event: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Add metadata to event to track processing chain and prevent loops.

        Args:
            event: The event to tag
            processing_entity: Which entity is processing this event
            parent_event: The event that triggered this one (if any)

        Returns:
            Tagged event with loop prevention metadata
        """

        # Add processing metadata
        event["_processing_entity"] = processing_entity
        event["_originated_from"] = event.get("_originated_from", processing_entity)

        # Increment depth from parent
        if parent_event:
            event["_depth"] = parent_event.get("_depth", 0) + 1
            event["_parent_event_id"] = parent_event.get("event_id")
            event["_processing_chain"] = parent_event.get("_processing_chain", []) + [processing_entity]
        else:
            event["_depth"] = 0
            event["_processing_chain"] = [processing_entity]

        return event

    @classmethod
    def is_safe_internal_event(cls, event: Dict[str, Any]) -> bool:
        """
        Check if an internal event is safe to process.

        Safe internal events:
        - Security alerts (critical)
        - User-facing errors (not Observer's own errors)
        - Business logic events (not infrastructure events)
        """

        event_type = event.get("event_type", "")

        # Always process security alerts
        if event_type == "internal.security_alert":
            return True

        # Process application errors that are NOT from Observer
        if event_type == "internal.application_error":
            originated_from = event.get("_originated_from")
            if originated_from and originated_from != "Observer":
                return True
            return False

        # Process business events
        if event_type.startswith("internal.business_"):
            return True

        # Block all other internal events by default
        return False


class LoopDetector:
    """
    Detects rapid event loops using a sliding window.
    """

    def __init__(self, window_size: int = 10, threshold: int = 5):
        """
        Args:
            window_size: Number of recent events to track
            threshold: Max occurrences of same event type in window
        """
        self.window_size = window_size
        self.threshold = threshold
        self.recent_events = []

    def check_for_loop(self, event: Dict[str, Any]) -> tuple[bool, Optional[str]]:
        """
        Check if this event indicates a loop.

        Returns:
            (is_loop: bool, reason: Optional[str])
        """

        event_type = event.get("event_type", "")
        originated_from = event.get("_originated_from", "")

        # Add to window
        self.recent_events.append((event_type, originated_from))

        # Keep only last N events
        if len(self.recent_events) > self.window_size:
            self.recent_events.pop(0)

        # Count occurrences of this event type + origin
        count = sum(1 for et, of in self.recent_events
                   if et == event_type and of == originated_from)

        if count >= self.threshold:
            return True, f"Loop detected: {event_type} from {originated_from} occurred {count} times in last {self.window_size} events"

        return False, None


# Example usage in Observer
if __name__ == "__main__":
    print("🔒 Circular Reference Protection Demo")
    print("=" * 60)

    # Test 1: Observer's own event (should be blocked)
    observer_event = {
        "event_type": "internal.observer_heartbeat",
        "source_system": "internal",
        "_originated_from": "Observer",
        "_processing_entity": "Observer"
    }

    should_process, reason = CircularReferenceProtector.should_process_event(observer_event)
    print(f"\n1. Observer's own event:")
    print(f"   Event: {observer_event['event_type']}")
    print(f"   Process: {should_process}")
    print(f"   Reason: {reason}")

    # Test 2: Security alert (should be allowed)
    security_event = {
        "event_type": "internal.security_alert",
        "source_system": "internal",
        "_originated_from": "Sentinel",
        "_depth": 1
    }

    should_process, reason = CircularReferenceProtector.should_process_event(security_event)
    print(f"\n2. Security alert from Sentinel:")
    print(f"   Event: {security_event['event_type']}")
    print(f"   Process: {should_process}")
    print(f"   Reason: {reason or 'Event is safe to process'}")

    # Test 3: Deep recursion (should be blocked)
    deep_event = {
        "event_type": "internal.application_error",
        "source_system": "internal",
        "_depth": 5
    }

    should_process, reason = CircularReferenceProtector.should_process_event(deep_event)
    print(f"\n3. Event with depth=5:")
    print(f"   Event: {deep_event['event_type']}")
    print(f"   Process: {should_process}")
    print(f"   Reason: {reason}")

    # Test 4: Tag event for processing
    print(f"\n4. Tagging event for processing:")
    new_event = {
        "event_type": "slack.message",
        "source_system": "slack"
    }

    tagged = CircularReferenceProtector.tag_event_for_processing(
        new_event,
        processing_entity="Observer"
    )

    print(f"   Original: {new_event}")
    print(f"   Tagged: {tagged}")

    # Test 5: Loop detection
    print(f"\n5. Loop detection:")
    detector = LoopDetector(window_size=5, threshold=3)

    for i in range(6):
        test_event = {
            "event_type": "internal.test_event",
            "_originated_from": "TestEntity"
        }

        is_loop, reason = detector.check_for_loop(test_event)
        print(f"   Iteration {i+1}: Loop={is_loop}, Reason={reason or 'OK'}")

    print(f"\n✅ Circular reference protection demo complete")
