"""
Intent Extractor - Phase 1 (Simple Keyword-Based)

Extracts user intents from event text using pattern matching.
Phase 1 uses simple keyword detection with confidence scoring.

Based on Claude Opus design specification.
"""

import logging
import re
from typing import List, Dict, Any, Optional
from datetime import datetime

logger = logging.getLogger(__name__)


# Intent type definitions from BasalMind architecture
INTENT_TYPES = {
    "asking_question": {
        "keywords": ["?", "how", "what", "why", "when", "where", "who", "can you", "could you", "would you"],
        "confidence_boost": 0.2
    },
    "requesting_help": {
        "keywords": ["help", "assist", "support", "need", "stuck", "problem", "issue", "error"],
        "confidence_boost": 0.15
    },
    "providing_information": {
        "keywords": ["here is", "this is", "fyi", "btw", "note that", "update:", "status:"],
        "confidence_boost": 0.1
    },
    "making_decision": {
        "keywords": ["decide", "let's use", "we should", "go with", "choose", "pick", "select"],
        "confidence_boost": 0.25
    },
    "expressing_opinion": {
        "keywords": ["i think", "i believe", "in my opinion", "imo", "imho", "seems like", "looks like"],
        "confidence_boost": 0.1
    },
    "requesting_action": {
        "keywords": ["please", "can you", "could you", "would you mind", "need you to", "deploy", "fix", "implement"],
        "confidence_boost": 0.15
    },
    "acknowledging": {
        "keywords": ["thanks", "thank you", "got it", "ok", "okay", "understood", "makes sense", "👍"],
        "confidence_boost": 0.1
    },
    "disagreeing": {
        "keywords": ["no", "disagree", "but", "however", "actually", "not sure", "i don't think"],
        "confidence_boost": 0.15
    },
    "brainstorming": {
        "keywords": ["what if", "maybe", "how about", "consider", "alternative", "idea", "thought"],
        "confidence_boost": 0.12
    },
    "troubleshooting": {
        "keywords": ["error", "bug", "broken", "not working", "failing", "crash", "issue", "debug"],
        "confidence_boost": 0.18
    },
    "planning": {
        "keywords": ["plan", "schedule", "roadmap", "next", "TODO", "task", "milestone"],
        "confidence_boost": 0.12
    },
    "general_conversation": {
        "keywords": [],  # Catch-all with low confidence
        "confidence_boost": 0.0
    }
}


class IntentExtractor:
    """
    Extract user intents from text using pattern matching.
    
    Phase 1: Simple keyword-based detection with confidence scoring.
    Phase 2: Will add LLM-based classification and multi-intent detection.
    """
    
    def __init__(self, confidence_threshold: float = 0.5):
        """
        Initialize intent extractor.
        
        Args:
            confidence_threshold: Minimum confidence to include an intent (0.0-1.0)
        """
        self.confidence_threshold = confidence_threshold
        self.intent_types = INTENT_TYPES
        
    def extract_from_event(self, event: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Extract intents from a single event.
        
        Args:
            event: Event dict from TimescaleDB
            
        Returns:
            List of intent dicts with {type, confidence, text, actor_id, timestamp}
        """
        text = event.get("text", "")
        if not text or not isinstance(text, str):
            return []
            
        # Normalize text for matching
        text_lower = text.lower()
        
        # Score all intent types
        intent_scores = {}
        for intent_type, config in self.intent_types.items():
            score = self._score_intent(text_lower, config["keywords"])
            if score > 0:
                # Add confidence boost
                score = min(1.0, score + config["confidence_boost"])
                intent_scores[intent_type] = score
                
        # If no intents detected, default to general_conversation
        if not intent_scores:
            intent_scores["general_conversation"] = 0.3
            
        # Build intent list
        intents = []
        for intent_type, confidence in intent_scores.items():
            if confidence >= self.confidence_threshold:
                intents.append({
                    "type": intent_type,
                    "confidence": round(confidence, 2),
                    "text": text[:200],  # First 200 chars
                    "actor_id": event.get("user_id"),
                    "timestamp": (event.get("event_time") or event.get("observed_at")).isoformat(),
                    "source_event_id": event.get("event_id")
                })
                
        # Sort by confidence descending
        intents.sort(key=lambda x: x["confidence"], reverse=True)
        
        return intents
        
    def _score_intent(self, text: str, keywords: List[str]) -> float:
        """
        Score an intent based on keyword presence.
        
        Args:
            text: Normalized text to analyze
            keywords: List of keywords to match
            
        Returns:
            Base confidence score (0.0-1.0)
        """
        if not keywords:
            return 0.0
            
        matches = 0
        for keyword in keywords:
            if keyword.lower() in text:
                matches += 1
                
        # Score is proportion of keywords matched, with diminishing returns
        if matches == 0:
            return 0.0
        elif matches == 1:
            return 0.3
        elif matches == 2:
            return 0.5
        else:
            return 0.7  # Cap at 0.7 for base score (boosted by confidence_boost)
            
    def extract_from_batch(self, events: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
        """
        Extract intents from a batch of events.
        
        Args:
            events: List of event dicts from TimescaleDB
            
        Returns:
            Dict mapping event_id to list of intents
        """
        results = {}
        
        for event in events:
            event_id = event.get("event_id")
            if not event_id:
                continue
                
            intents = self.extract_from_event(event)
            if intents:
                results[event_id] = intents
                
        logger.info(
            f"🎯 Extracted intents from {len(results)}/{len(events)} events "
            f"(avg {sum(len(v) for v in results.values()) / max(len(results), 1):.1f} intents/event)"
        )
        
        return results


# Example usage
def test_extractor():
    """Test the IntentExtractor with sample events."""
    extractor = IntentExtractor(confidence_threshold=0.3)
    
    sample_events = [
        {
            "event_id": "123",
            "text": "How do we configure authentication in this system?",
            "user_id": "U123",
            "event_time": datetime.now()
        },
        {
            "event_id": "124",
            "text": "I think we should use PostgreSQL for the database",
            "user_id": "U123",
            "event_time": datetime.now()
        },
        {
            "event_id": "125",
            "text": "Error: Connection timeout when connecting to Redis",
            "user_id": "U124",
            "event_time": datetime.now()
        },
        {
            "event_id": "126",
            "text": "Thanks! That makes sense 👍",
            "user_id": "U123",
            "event_time": datetime.now()
        }
    ]
    
    results = extractor.extract_from_batch(sample_events)
    
    print("\n🎯 Intent Extraction Results:")
    print("=" * 60)
    for event_id, intents in results.items():
        print(f"\nEvent {event_id}:")
        for intent in intents:
            print(f"  - {intent['type']}: {intent['confidence']} confidence")
            print(f"    Text: {intent['text']}")


if __name__ == "__main__":
    test_extractor()
