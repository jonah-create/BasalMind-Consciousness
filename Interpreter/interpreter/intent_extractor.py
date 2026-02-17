"""
Intent Extractor - Phase 2 (Action-Oriented with Categorical Taxonomy)

Extracts user intents for BasalMind - an AI that:
- Builds software applications and systems
- Executes projects and implements solutions  
- Acts as a life coach and supportive buddy
- Designs processes and manages teams
- Patches, deploys, and maintains systems

Based on Claude Opus design + Phase 2 enhancements.
"""

import logging
from typing import List, Dict, Any, Optional
from datetime import datetime
from .intent_taxonomy import BASALMIND_INTENT_TAXONOMY

logger = logging.getLogger(__name__)


class IntentExtractor:
    """
    Extract user intents using BasalMind's comprehensive action-oriented taxonomy.
    
    Phase 2: 42 intent types across 11 categories
    - Captures building, executing, coaching, and managing intents
    - Categorical structure prevents synonym proliferation
    - Optimized for action-oriented AI interactions
    """
    
    def __init__(self, confidence_threshold: float = 0.5):
        """
        Initialize intent extractor with BasalMind taxonomy.
        
        Args:
            confidence_threshold: Minimum confidence to include an intent (0.0-1.0)
        """
        self.confidence_threshold = confidence_threshold
        self.taxonomy = BASALMIND_INTENT_TAXONOMY
        
        # Build flat lookup for category resolution
        self.intent_to_category = {}
        self.total_intent_types = 0
        for category, config in self.taxonomy.items():
            for intent_type in config["intents"].keys():
                self.intent_to_category[intent_type] = category
                self.total_intent_types += 1
        
        logger.info(
            f"🎯 Intent Extractor initialized: {self.total_intent_types} intent types "
            f"across {len(self.taxonomy)} categories"
        )
        
    def extract_from_event(self, event: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Extract intents from a single event with categorical structure.
        
        Args:
            event: Event dict from TimescaleDB
            
        Returns:
            List of intent dicts with {category, type, confidence, text, actor_id, timestamp}
        """
        text = event.get("text", "")
        if not text or not isinstance(text, str):
            return []
            
        # Normalize text for matching
        text_lower = text.lower()
        
        # Score all intent types across categories
        intent_scores = []  # List of (category, intent_type, score)
        
        for category, category_config in self.taxonomy.items():
            for intent_type, intent_config in category_config["intents"].items():
                score = self._score_intent(text_lower, intent_config["keywords"])
                if score > 0:
                    # Add confidence boost
                    score = min(1.0, score + intent_config["confidence_boost"])
                    intent_scores.append((category, intent_type, score))
        
        # Filter by threshold and deduplicate
        intents = []
        seen_intents = set()  # Prevent duplicate intent types
        
        for category, intent_type, confidence in sorted(intent_scores, key=lambda x: x[2], reverse=True):
            if confidence >= self.confidence_threshold and intent_type not in seen_intents:
                intents.append({
                    "category": category,
                    "type": intent_type,
                    "confidence": round(confidence, 2),
                    "text": text[:200],  # First 200 chars
                    "actor_id": event.get("user_id"),
                    "timestamp": (event.get("event_time") or event.get("observed_at")).isoformat()
                    if hasattr((event.get("event_time") or event.get("observed_at")), "isoformat")
                    else str(event.get("event_time") or event.get("observed_at")),
                    "source_event_id": str(event.get("event_id"))
                })
                seen_intents.add(intent_type)
        
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
        category_counts = {}
        
        for event in events:
            event_id = event.get("event_id")
            if not event_id:
                continue
                
            intents = self.extract_from_event(event)
            if intents:
                results[str(event_id)] = intents
                
                # Count categories for stats
                for intent in intents:
                    category = intent["category"]
                    category_counts[category] = category_counts.get(category, 0) + 1
        
        if results:
            logger.info(
                f"🎯 Extracted intents from {len(results)}/{len(events)} events "
                f"(avg {sum(len(v) for v in results.values()) / len(results):.1f} intents/event)"
            )
            logger.info(f"   Category breakdown: {dict(sorted(category_counts.items()))}")
        
        return results
    
    def get_category_for_intent(self, intent_type: str) -> Optional[str]:
        """
        Get the category for a given intent type.
        
        Args:
            intent_type: Intent type (e.g., "asking_question")
            
        Returns:
            Category name or None if not found
        """
        return self.intent_to_category.get(intent_type)
    
    def get_intents_in_category(self, category: str) -> List[str]:
        """
        Get all intent types in a category.
        
        Args:
            category: Category name (e.g., "building_creating")
            
        Returns:
            List of intent types in the category
        """
        if category in self.taxonomy:
            return list(self.taxonomy[category]["intents"].keys())
        return []
    
    def get_taxonomy_summary(self) -> Dict[str, Any]:
        """
        Get a summary of the intent taxonomy.
        
        Returns:
            Dict with taxonomy statistics
        """
        summary = {
            "total_categories": len(self.taxonomy),
            "total_intent_types": self.total_intent_types,
            "categories": {}
        }
        
        for category, config in self.taxonomy.items():
            summary["categories"][category] = {
                "description": config["description"],
                "intent_count": len(config["intents"]),
                "intents": list(config["intents"].keys())
            }
        
        return summary


# Example usage
def test_extractor():
    """Test the Intent Extractor with BasalMind-specific examples."""
    extractor = IntentExtractor(confidence_threshold=0.3)
    
    sample_events = [
        {
            "event_id": "1",
            "text": "Can you build me an app that tracks my daily tasks?",
            "user_id": "U123",
            "event_time": datetime.now()
        },
        {
            "event_id": "2",
            "text": "Deploy the new authentication service to production",
            "user_id": "U123",
            "event_time": datetime.now()
        },
        {
            "event_id": "3",
            "text": "I'm feeling stressed about this project deadline",
            "user_id": "U124",
            "event_time": datetime.now()
        },
        {
            "event_id": "4",
            "text": "Fix the bug in the payment module - it's crashing on checkout",
            "user_id": "U125",
            "event_time": datetime.now()
        },
        {
            "event_id": "5",
            "text": "What's the status on the database migration?",
            "user_id": "U123",
            "event_time": datetime.now()
        }
    ]
    
    results = extractor.extract_from_batch(sample_events)
    
    print("\n🎯 BasalMind Intent Extraction Results:")
    print("=" * 80)
    for event_id, intents in results.items():
        print(f"\nEvent {event_id}:")
        for intent in intents:
            print(f"  - [{intent['category']!s:25}] {intent['type']!s:30} ({intent['confidence']})")
            print(f"    Text: {intent['text']}")
    
    print("\n📊 Taxonomy Summary:")
    print("=" * 80)
    summary = extractor.get_taxonomy_summary()
    print(f"Total: {summary['total_intent_types']} intent types across {summary['total_categories']} categories\n")
    for category, info in summary["categories"].items():
        print(f"{category}: {info['intent_count']} intents")
        print(f"  Description: {info['description']}")


if __name__ == "__main__":
    test_extractor()
