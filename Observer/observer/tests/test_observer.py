"""
Observer Test Suite

Tests all core Observer components without requiring live external services.
Uses mocks for Neo4j, TimescaleDB, and NATS connections.
"""

import pytest
import asyncio
from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch, AsyncMock
import sys
import os

# Add Observer to path
sys.path.insert(0, '/opt/basalmind/BasalMind_Consciousness/Observer')


# ─────────────────────────────────────────────────────────────────────────────
# Event Schema
# ─────────────────────────────────────────────────────────────────────────────

class TestCanonicalEvent:
    """Test CanonicalEvent data structure and serialization."""

    def test_create_event_has_uuid(self):
        from observer.event_schema import create_event, EventSource, NormalizedFields
        event = create_event(
            event_type="slack.message",
            source_system=EventSource.SLACK,
            raw_payload={"text": "hello"},
            normalized=NormalizedFields(text="hello")
        )
        assert event.event_id
        assert len(event.event_id) == 36  # UUID4 format

    def test_create_event_timestamps(self):
        from observer.event_schema import create_event, EventSource, NormalizedFields
        before = datetime.utcnow()
        event = create_event(
            event_type="slack.message",
            source_system=EventSource.SLACK,
            raw_payload={},
            normalized=NormalizedFields()
        )
        after = datetime.utcnow()
        assert before <= event.observed_at <= after

    def test_to_dict_excludes_none(self):
        from observer.event_schema import NormalizedFields
        fields = NormalizedFields(text="hello", user_id="U123")
        d = fields.to_dict()
        assert "text" in d
        assert "user_id" in d
        # Fields that are None should be excluded
        assert "user_email" not in d
        assert "subject" not in d

    def test_canonical_event_to_dict_serializable(self):
        from observer.event_schema import create_event, EventSource, NormalizedFields
        import json
        event = create_event(
            event_type="slack.message",
            source_system=EventSource.SLACK,
            raw_payload={"key": "val"},
            normalized=NormalizedFields(text="hi", user_id="U1", channel_id="C1")
        )
        d = event.to_dict()
        # Must be JSON-serializable
        json.dumps(d)
        assert d["event_type"] == "slack.message"
        assert d["source_system"] == "slack"

    def test_all_event_sources_defined(self):
        from observer.event_schema import EventSource
        assert EventSource.SLACK == "slack"
        assert EventSource.INTERNAL == "internal"
        assert EventSource.API == "api"

    def test_normalized_fields_metadata(self):
        from observer.event_schema import NormalizedFields
        fields = NormalizedFields(
            text="test",
            metadata={"custom": "data", "count": 42}
        )
        d = fields.to_dict()
        assert d["metadata"]["custom"] == "data"
        assert d["metadata"]["count"] == 42


# ─────────────────────────────────────────────────────────────────────────────
# Slack Adapter
# ─────────────────────────────────────────────────────────────────────────────

class TestSlackAdapter:
    """Test Slack event normalization (pure functions)."""

    def _slack_message(self, **overrides):
        base = {
            "type": "message",
            "text": "Hello team, how is authentication configured?",
            "user": "U099BM321LH",
            "channel": "C0ADWKHD4SJ",
            "ts": "1771346978.310559",
            "team": "T12345",
            "thread_ts": "1771346978.310559",
        }
        base.update(overrides)
        return base

    def test_normalize_message_type(self):
        from observer.adapters.slack_adapter import normalize_slack_message
        event = normalize_slack_message(self._slack_message())
        assert event.event_type == "slack.message"
        assert event.source_system.value == "slack"

    def test_normalize_message_fields(self):
        from observer.adapters.slack_adapter import normalize_slack_message
        event = normalize_slack_message(self._slack_message())
        assert event.normalized.user_id == "U099BM321LH"
        assert event.normalized.channel_id == "C0ADWKHD4SJ"
        assert event.normalized.thread_id == "1771346978.310559"
        assert event.normalized.text == "Hello team, how is authentication configured?"

    def test_normalize_message_missing_thread(self):
        from observer.adapters.slack_adapter import normalize_slack_message
        event = normalize_slack_message(self._slack_message(thread_ts=None))
        assert event.normalized.thread_id is None

    def test_normalize_message_strips_text_whitespace(self):
        from observer.adapters.slack_adapter import normalize_slack_message
        event = normalize_slack_message(self._slack_message(text="  hello world  "))
        assert event.normalized.text == "hello world"

    def test_normalize_button_click(self):
        from observer.adapters.slack_adapter import normalize_slack_button_click
        interaction = {
            "user": {"id": "U123", "username": "alice"},
            "channel": {"id": "C456", "name": "general"},
            "message": {"ts": "1234567890.000001", "thread_ts": None},
            "actions": [{"value": "approve", "text": {"text": "Approve"}, "action_id": "btn_approve", "block_id": "b1"}],
            "trigger_id": "trig_123",
            "response_url": "https://hooks.slack.com/...",
            "team": {"id": "T789"},
            "action_ts": "1234567890.100000"
        }
        event = normalize_slack_button_click(interaction)
        assert event.event_type == "slack.interaction.button_click"
        assert event.normalized.action_type == "button_click"
        assert event.normalized.action_value == "approve"
        assert event.normalized.user_id == "U123"

    def test_normalize_app_mention(self):
        from observer.adapters.slack_adapter import normalize_slack_app_mention
        slack_event = {
            "type": "app_mention",
            "text": "<@UBOT> what is the status?",
            "user": "U123",
            "channel": "C456",
            "ts": "1234567890.000001",
            "team": "T789",
        }
        event = normalize_slack_app_mention(slack_event)
        assert event.event_type == "slack.app_mention"
        assert event.normalized.user_id == "U123"
        assert "<@UBOT>" in event.normalized.text

    def test_session_id_format(self):
        from observer.adapters.slack_adapter import normalize_slack_message
        event = normalize_slack_message(self._slack_message())
        # session_id must be a string and not empty
        assert event.session_id
        assert "C0ADWKHD4SJ" in event.session_id

    def test_raw_payload_preserved(self):
        from observer.adapters.slack_adapter import normalize_slack_message
        raw = self._slack_message(custom_field="preserved")
        event = normalize_slack_message(raw)
        assert event.raw_payload["custom_field"] == "preserved"


# ─────────────────────────────────────────────────────────────────────────────
# Circuit Breaker
# ─────────────────────────────────────────────────────────────────────────────

class TestCircuitBreaker:
    """Test circuit breaker state machine."""

    def test_initial_state_closed(self):
        from observer.circuit_breaker import CircuitBreaker, CircuitState
        cb = CircuitBreaker("test", failure_threshold=3, recovery_timeout=60)
        assert cb.state == CircuitState.CLOSED

    @pytest.mark.asyncio
    async def test_successful_call_stays_closed(self):
        from observer.circuit_breaker import CircuitBreaker, CircuitState
        cb = CircuitBreaker("test", failure_threshold=3, recovery_timeout=60)
        async with cb:
            pass  # success
        assert cb.state == CircuitState.CLOSED
        assert cb.failure_count == 0

    @pytest.mark.asyncio
    async def test_failures_open_circuit(self):
        from observer.circuit_breaker import CircuitBreaker, CircuitState
        cb = CircuitBreaker("test", failure_threshold=3, recovery_timeout=60)
        for _ in range(3):
            try:
                async with cb:
                    raise Exception("service down")
            except Exception:
                pass
        assert cb.state == CircuitState.OPEN

    @pytest.mark.asyncio
    async def test_open_circuit_rejects_calls(self):
        from observer.circuit_breaker import CircuitBreaker, CircuitBreakerOpen
        cb = CircuitBreaker("test", failure_threshold=2, recovery_timeout=60)
        for _ in range(2):
            try:
                async with cb:
                    raise Exception("down")
            except Exception:
                pass
        with pytest.raises(CircuitBreakerOpen):
            async with cb:
                pass

    @pytest.mark.asyncio
    async def test_recovery_to_half_open(self):
        from observer.circuit_breaker import CircuitBreaker, CircuitState
        cb = CircuitBreaker("test", failure_threshold=1, recovery_timeout=0)
        try:
            async with cb:
                raise Exception("fail")
        except Exception:
            pass
        assert cb.state == CircuitState.OPEN
        # With recovery_timeout=0, next check should go HALF_OPEN
        import asyncio
        await asyncio.sleep(0.01)
        cb._check_state()
        assert cb.state == CircuitState.HALF_OPEN

    @pytest.mark.asyncio
    async def test_half_open_success_closes_circuit(self):
        from observer.circuit_breaker import CircuitBreaker, CircuitState
        cb = CircuitBreaker("test", failure_threshold=1, recovery_timeout=0)
        try:
            async with cb:
                raise Exception("fail")
        except Exception:
            pass
        await asyncio.sleep(0.01)
        cb._check_state()
        # Successful call in HALF_OPEN should close
        async with cb:
            pass
        assert cb.state == CircuitState.CLOSED

    def test_stats_tracking(self):
        from observer.circuit_breaker import CircuitBreaker
        cb = CircuitBreaker("test", failure_threshold=5, recovery_timeout=60)
        stats = cb.get_stats()
        assert stats["name"] == "test"
        assert stats["state"] == "closed"
        assert stats["total_calls"] == 0
        assert stats["success_rate"] == 100.0

    def test_reset_clears_state(self):
        from observer.circuit_breaker import CircuitBreaker, CircuitState
        cb = CircuitBreaker("test", failure_threshold=1, recovery_timeout=60)
        cb.failure_count = 5
        cb.state = CircuitState.OPEN
        cb.reset()
        assert cb.state == CircuitState.CLOSED
        assert cb.failure_count == 0

    @pytest.mark.asyncio
    async def test_call_method(self):
        from observer.circuit_breaker import CircuitBreaker
        cb = CircuitBreaker("test", failure_threshold=3, recovery_timeout=60)
        async def my_func(x):
            return x * 2
        result = await cb.call(my_func, 5)
        assert result == 10


# ─────────────────────────────────────────────────────────────────────────────
# Circular Reference Protection
# ─────────────────────────────────────────────────────────────────────────────

class TestCircularProtection:
    """Test circular reference and loop detection."""

    def test_blocks_observer_own_events(self):
        from observer.circular_protection import CircularReferenceProtector
        event = {"event_type": "internal.observer_heartbeat", "_originated_from": "Observer"}
        should, reason = CircularReferenceProtector.should_process_event(event)
        assert should is False
        assert reason is not None

    def test_blocks_excluded_patterns(self):
        from observer.circular_protection import CircularReferenceProtector
        for pattern in [
            "internal.observer_status",
            "internal.event_processing_stats",
            "internal.redis_cache_miss",
            "internal.timescale_write_ok",
            "internal.neo4j_query_slow",
        ]:
            event = {"event_type": pattern}
            should, _ = CircularReferenceProtector.should_process_event(event)
            assert should is False, f"Should block: {pattern}"

    def test_blocks_depth_exceeded(self):
        from observer.circular_protection import CircularReferenceProtector
        event = {"event_type": "slack.message", "_depth": 5}
        should, reason = CircularReferenceProtector.should_process_event(event)
        assert should is False
        assert "Depth" in reason

    def test_allows_normal_slack_events(self):
        from observer.circular_protection import CircularReferenceProtector
        event = {"event_type": "slack.message", "source_system": "slack"}
        should, reason = CircularReferenceProtector.should_process_event(event)
        assert should is True
        assert reason is None

    def test_allows_security_alerts(self):
        from observer.circular_protection import CircularReferenceProtector
        event = {"event_type": "internal.security_alert", "_depth": 1}
        should, reason = CircularReferenceProtector.should_process_event(event)
        assert should is True

    def test_tag_event_sets_depth_zero(self):
        from observer.circular_protection import CircularReferenceProtector
        event = {"event_type": "slack.message"}
        tagged = CircularReferenceProtector.tag_event_for_processing(event, "Observer")
        assert tagged["_depth"] == 0
        assert tagged["_processing_entity"] == "Observer"
        assert tagged["_originated_from"] == "Observer"

    def test_tag_event_increments_depth_from_parent(self):
        from observer.circular_protection import CircularReferenceProtector
        parent = {"event_type": "slack.message", "_depth": 1, "_processing_chain": ["Observer"]}
        child = {"event_type": "internal.business_follow_up"}
        tagged = CircularReferenceProtector.tag_event_for_processing(child, "Interpreter", parent)
        assert tagged["_depth"] == 2
        assert "Observer" in tagged["_processing_chain"]
        assert "Interpreter" in tagged["_processing_chain"]

    def test_loop_detector_triggers_on_threshold(self):
        from observer.circular_protection import LoopDetector
        detector = LoopDetector(window_size=10, threshold=3)
        event = {"event_type": "internal.test", "_originated_from": "TestEntity"}
        results = [detector.check_for_loop(event) for _ in range(5)]
        # First 2 should be fine, 3rd+ should trigger
        assert results[0][0] is False
        assert results[1][0] is False
        assert results[2][0] is True  # threshold hit
        assert "Loop detected" in results[2][1]

    def test_is_safe_internal_security_alert(self):
        from observer.circular_protection import CircularReferenceProtector
        event = {"event_type": "internal.security_alert"}
        assert CircularReferenceProtector.is_safe_internal_event(event) is True

    def test_is_safe_blocks_infrastructure(self):
        from observer.circular_protection import CircularReferenceProtector
        event = {"event_type": "internal.cache_flush"}
        assert CircularReferenceProtector.is_safe_internal_event(event) is False

    def test_business_events_safe(self):
        from observer.circular_protection import CircularReferenceProtector
        event = {"event_type": "internal.business_order_created"}
        assert CircularReferenceProtector.is_safe_internal_event(event) is True


# ─────────────────────────────────────────────────────────────────────────────
# Event Correlator (unit — no live Neo4j)
# ─────────────────────────────────────────────────────────────────────────────

class TestEventCorrelator:
    """Test event correlator logic with mocked Neo4j."""

    def _make_correlator(self):
        from observer.event_correlator import EventCorrelator
        with patch("observer.event_correlator.GraphDatabase.driver") as mock_driver:
            correlator = EventCorrelator(
                neo4j_uri="bolt://localhost:7687",
                neo4j_user="neo4j",
                neo4j_password="test"
            )
            # Inject a fake schema directly
            correlator._schema_cache = {
                "sources": {
                    "slack": {
                        "primary_key": "ts",
                        "keys": [
                            {"name": "user_id", "extraction_path": "normalized.user_id", "required": True},
                            {"name": "channel_id", "extraction_path": "normalized.channel_id", "required": True},
                            {"name": "thread_id", "extraction_path": "normalized.thread_id", "required": False},
                        ]
                    }
                },
                "rules": []
            }
            correlator._cache_time = datetime.utcnow()
            return correlator

    def test_extract_value_dot_path(self):
        from observer.event_correlator import EventCorrelator
        correlator = self._make_correlator()
        data = {"normalized": {"user_id": "U123", "channel_id": "C456"}}
        assert correlator.extract_value(data, "normalized.user_id") == "U123"
        assert correlator.extract_value(data, "normalized.channel_id") == "C456"

    def test_extract_value_missing_path(self):
        correlator = self._make_correlator()
        data = {"normalized": {}}
        assert correlator.extract_value(data, "normalized.user_id") is None
        assert correlator.extract_value(data, "does.not.exist") is None

    def test_enrich_slack_event(self):
        correlator = self._make_correlator()
        event = {
            "source_system": "slack",
            "normalized": {"user_id": "U123", "channel_id": "C456", "thread_id": "1234.5678"}
        }
        result = correlator.enrich_event(event)
        assert result["correlatable"] is True
        assert result["correlation_keys"]["user_id"] == "U123"
        assert result["correlation_keys"]["channel_id"] == "C456"
        assert result["correlation_keys"]["thread_id"] == "1234.5678"

    def test_enrich_unknown_source(self):
        correlator = self._make_correlator()
        event = {"source_system": "unknown_system", "normalized": {}}
        result = correlator.enrich_event(event)
        assert result == {}

    def test_enrich_missing_source_system(self):
        correlator = self._make_correlator()
        event = {"normalized": {"user_id": "U123"}}
        result = correlator.enrich_event(event)
        assert result == {}

    def test_enrich_missing_required_key_still_returns(self):
        correlator = self._make_correlator()
        # Missing user_id (required) — should still return with what's available
        event = {
            "source_system": "slack",
            "normalized": {"channel_id": "C456"}  # no user_id
        }
        result = correlator.enrich_event(event)
        assert "channel_id" in result["correlation_keys"]
        assert "user_id" not in result["correlation_keys"]


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
