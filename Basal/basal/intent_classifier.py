"""
Basal Intent Classifier - Classify incoming events without LLM on critical path.

Uses keyword/rule-based classification first, with optional embedding-based
classification when OpenAI is available. Reads from Interpreter's semantic
store for context (read-only).
"""

import logging
import re
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

from basal.config import config

logger = logging.getLogger("basal.intent")


@dataclass
class IntentResult:
    """Result of intent classification."""
    intent: str
    confidence: float
    sub_intents: List[str] = field(default_factory=list)
    requires_entities: List[str] = field(default_factory=list)
    priority: str = "medium"  # low, medium, high, urgent
    metadata: Dict[str, Any] = field(default_factory=dict)


# Intent taxonomy aligned with system capabilities
INTENT_TAXONOMY = {
    "query.status": {
        "patterns": [r"\b(status|health|how is|what is the state|running)\b"],
        "priority": "medium",
        "entities": [],
        "description": "User querying system status"
    },
    "query.information": {
        "patterns": [r"\b(what|who|when|where|why|how|tell me|show me|explain|describe)\b"],
        "priority": "medium",
        "entities": [],
        "description": "General information request"
    },
    "request.feature": {
        "patterns": [r"\b(add|build|create|implement|develop|make|new feature|can you add)\b"],
        "priority": "medium",
        "weight": 1.0,
        "entities": ["cartographer", "assembler", "sentinel"],
        "description": "User requesting new functionality"
    },
    "request.bug_fix": {
        "patterns": [r"\b(bug|fix|broken|error|not working|issue|problem|crash|fail)\b"],
        "priority": "high",
        "entities": ["assembler", "sentinel"],
        "description": "User reporting a bug or requesting fix"
    },
    "request.analysis": {
        "patterns": [r"\b(analyze|analyse|review|audit|assess|evaluate|check|inspect)\b"],
        "priority": "medium",
        "entities": ["philosopher"],
        "description": "Request for system or data analysis"
    },
    "request.deployment": {
        "patterns": [r"\b(deploy|ship|release|push|go live|production|prod|rollout|to prod|to production)\b"],
        "priority": "high",
        "weight": 1.5,
        "entities": ["sentinel", "assembler"],
        "description": "Deployment-related request"
    },
    "request.configuration": {
        "patterns": [r"\b(configure|config|setting|setup|env|environment)\b"],
        "priority": "medium",
        "entities": ["sentinel"],
        "description": "Configuration change request"
    },
    "conversation.greeting": {
        "patterns": [r"\b(hello|hi|hey|good morning|good afternoon|good evening|sup)\b"],
        "priority": "low",
        "entities": [],
        "description": "User greeting"
    },
    "conversation.acknowledgment": {
        "patterns": [r"\b(thanks|thank you|great|perfect|awesome|got it|ok|okay|sure)\b"],
        "priority": "low",
        "entities": [],
        "description": "User acknowledgment"
    },
    "system.alert": {
        "patterns": [r"\b(alert|warning|critical|urgent|emergency|down|outage)\b"],
        "priority": "urgent",
        "entities": ["sentinel"],
        "description": "System alert or emergency"
    },
    "unknown": {
        "patterns": [],
        "priority": "low",
        "entities": [],
        "description": "Unclassified intent"
    }
}


class RuleBasedClassifier:
    """Fast keyword/regex-based classifier - no LLM dependency."""

    def __init__(self):
        # Pre-compile all patterns
        self._compiled = {}
        for intent, spec in INTENT_TAXONOMY.items():
            if spec["patterns"]:
                self._compiled[intent] = [
                    re.compile(p, re.IGNORECASE) for p in spec["patterns"]
                ]

    def classify(self, text: str) -> Tuple[str, float]:
        """
        Classify text using regex patterns.
        
        Returns: (intent_name, confidence_score)
        """
        if not text or not text.strip():
            return "unknown", 0.0

        text_lower = text.lower()
        scores = {}

        for intent, patterns in self._compiled.items():
            matches = sum(1 for p in patterns if p.search(text_lower))
            if matches > 0:
                # Confidence based on match ratio, scaled by intent specificity weight
                weight = INTENT_TAXONOMY.get(intent, {}).get("weight", 1.0)
                base = min(matches / len(patterns) * 0.8 + 0.2, 0.95)
                scores[intent] = min(base * weight, 0.98)

        if not scores:
            return "unknown", 0.1

        best_intent = max(scores, key=scores.get)
        return best_intent, scores[best_intent]


class IntentClassifier:
    """
    Basal's intent classifier - rule-based with optional embedding enhancement.
    
    Stage 1: Pure rule-based (no LLM).
    Stage 5+: Enhanced with semantic similarity from Interpreter's store.
    """

    def __init__(self):
        self._rule_classifier = RuleBasedClassifier()
        logger.info("Intent classifier initialized (rule-based mode)")

    def classify_event(self, event: Dict[str, Any]) -> IntentResult:
        """
        Classify an event from the NATS stream.
        
        Args:
            event: Canonical event dict from Observer/NATS
            
        Returns: IntentResult with classification details
        """
        # Extract text from event
        text = self._extract_text(event)
        event_type = event.get("event_type", "")
        source = event.get("source_system", "")

        # Check if event already has intent from Interpreter
        normalized = event.get("normalized", {})
        existing_intent = normalized.get("intent")
        if existing_intent:
            logger.debug(f"Using Interpreter's intent: {existing_intent}")
            return IntentResult(
                intent=existing_intent,
                confidence=0.9,
                metadata={"source": "interpreter", "text_preview": text[:100]}
            )

        # Rule-based classification
        intent_name, confidence = self._rule_classifier.classify(text)

        # Override with event_type context
        if "slack.app_mention" in event_type:
            # App mentions are always direct requests
            if confidence < 0.5:
                intent_name = "query.information"
                confidence = 0.6

        spec = INTENT_TAXONOMY.get(intent_name, INTENT_TAXONOMY["unknown"])

        return IntentResult(
            intent=intent_name,
            confidence=confidence,
            requires_entities=spec.get("entities", []),
            priority=spec.get("priority", "medium"),
            metadata={
                "source": "rule_based",
                "event_type": event_type,
                "source_system": source,
                "text_preview": text[:100] if text else "",
            }
        )

    def _extract_text(self, event: Dict[str, Any]) -> str:
        """Extract meaningful text from event for classification."""
        # Try normalized fields first
        normalized = event.get("normalized", {})
        if isinstance(normalized, dict):
            text = normalized.get("text", "") or normalized.get("message", "")
            if text:
                return str(text)

        # Fall back to top-level text
        text = event.get("text", "") or event.get("message", "")
        if text:
            return str(text)

        # Last resort: event_type as signal
        return event.get("event_type", "")
