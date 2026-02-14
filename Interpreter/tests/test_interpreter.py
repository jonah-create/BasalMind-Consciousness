"""
Interpreter Module - Basic Tests

Tests core functionality of all Interpreter components.
"""

import pytest
import asyncio
from datetime import datetime, timedelta
from interpreter.intent_extractor import IntentExtractor


class TestIntentExtractor:
    """Test intent extraction functionality."""
    
    def setup_method(self):
        """Initialize extractor before each test."""
        self.extractor = IntentExtractor(confidence_threshold=0.3)
        
    def test_question_detection(self):
        """Test detection of asking_question intent."""
        event = {
            "event_id": "test-1",
            "text": "How do we configure authentication in this system?",
            "user_id": "U123",
            "event_time": datetime.utcnow()
        }
        
        intents = self.extractor.extract_from_event(event)
        
        assert len(intents) > 0
        assert any(i["type"] == "asking_question" for i in intents)
        assert all(i["confidence"] >= 0.3 for i in intents)
        
    def test_decision_detection(self):
        """Test detection of making_decision intent."""
        event = {
            "event_id": "test-2",
            "text": "Let's use PostgreSQL for the database",
            "user_id": "U123",
            "event_time": datetime.utcnow()
        }
        
        intents = self.extractor.extract_from_event(event)
        
        assert len(intents) > 0
        assert any(i["type"] == "making_decision" for i in intents)
        
    def test_troubleshooting_detection(self):
        """Test detection of troubleshooting intent."""
        event = {
            "event_id": "test-3",
            "text": "Error: Connection timeout when connecting to Redis",
            "user_id": "U124",
            "event_time": datetime.utcnow()
        }
        
        intents = self.extractor.extract_from_event(event)
        
        assert len(intents) > 0
        assert any(i["type"] == "troubleshooting" for i in intents)
        
    def test_batch_extraction(self):
        """Test batch processing of multiple events."""
        events = [
            {
                "event_id": "evt-1",
                "text": "How does this work?",
                "user_id": "U123",
                "event_time": datetime.utcnow()
            },
            {
                "event_id": "evt-2",
                "text": "Thanks for the help!",
                "user_id": "U123",
                "event_time": datetime.utcnow()
            },
            {
                "event_id": "evt-3",
                "text": "We should deploy this tomorrow",
                "user_id": "U124",
                "event_time": datetime.utcnow()
            }
        ]
        
        results = self.extractor.extract_from_batch(events)
        
        assert len(results) > 0
        assert all(isinstance(intents, list) for intents in results.values())
        
    def test_multi_source_events(self):
        """Test extraction from different source systems."""
        events = [
            {
                "event_id": "slack-1",
                "source_system": "slack",
                "text": "How do we handle errors?",
                "user_id": "U123",
                "event_time": datetime.utcnow()
            },
            {
                "event_id": "nginx-1",
                "source_system": "nginx",
                "text": "",  # nginx logs don't have text
                "metadata": {"path": "/api/auth", "status": 500},
                "event_time": datetime.utcnow()
            }
        ]
        
        results = self.extractor.extract_from_batch(events)
        
        # Slack event should have intents
        assert "slack-1" in results
        
        # nginx event might not have text-based intents
        # This is expected - not all sources have extractable intents
        
    def test_confidence_threshold(self):
        """Test confidence thresholding."""
        low_threshold = IntentExtractor(confidence_threshold=0.1)
        high_threshold = IntentExtractor(confidence_threshold=0.8)
        
        event = {
            "event_id": "test-4",
            "text": "ok",  # Minimal text, low confidence
            "user_id": "U123",
            "event_time": datetime.utcnow()
        }
        
        low_results = low_threshold.extract_from_event(event)
        high_results = high_threshold.extract_from_event(event)
        
        # Low threshold should capture more intents
        assert len(low_results) >= len(high_results)


# Run tests
if __name__ == "__main__":
    pytest.main([__file__, "-v"])
