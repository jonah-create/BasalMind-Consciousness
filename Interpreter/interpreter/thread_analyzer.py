"""
Thread Analyzer for Interpreter

Tracks conversation threads and analyzes their context, patterns, and outcomes.
Helps BasalMind understand multi-message conversations.

Based on Claude Opus design specification.
"""

import logging
from typing import List, Dict, Any, Optional
from datetime import datetime
from collections import defaultdict
import asyncpg
import json

logger = logging.getLogger(__name__)


class ThreadAnalyzer:
    """
    Analyze and track conversation threads.

    Features:
    - Detect thread topics and conversation patterns
    - Track participants and their roles
    - Extract decisions, facts, and answered questions
    - Monitor thread lifecycle (starting → active → resolved)

    Helps BasalMind:
    - Understand conversation context
    - Identify when issues are resolved
    - Track decision-making processes
    - Recognize Q&A patterns
    """

    def __init__(
        self,
        postgres_pool: asyncpg.Pool
    ):
        self.pool = postgres_pool

        # In-memory tracking of active threads
        self.active_threads = {}  # thread_id -> thread_data

        # Conversation pattern keywords
        self.question_keywords = {"how", "what", "why", "when", "where", "who", "can", "could", "should", "?"}
        self.decision_keywords = {"decide", "decision", "choose", "going with", "will use", "agreed", "consensus"}
        self.troubleshoot_keywords = {"error", "issue", "problem", "bug", "broken", "not working", "failed"}
        self.brainstorm_keywords = {"idea", "suggest", "what if", "consider", "alternative", "option"}

    async def process_events(
        self,
        events: List[Dict[str, Any]],
        intents: Dict[str, List[Dict[str, Any]]]
    ) -> List[str]:
        """
        Process events and analyze threads.

        Args:
            events: Batch of events
            intents: Extracted intents keyed by event_id

        Returns:
            List of thread_ids updated
        """
        threads_updated = []

        # Fix #3: Only analyze Slack events (they have thread/channel context)
        slack_events = [e for e in events if e.get("source_system") == "slack"]
        if not slack_events:
            return threads_updated

        # Fix #2: Group events by their conversation context
        # - Events with thread_id -> threaded conversation (group by thread_id)
        # - Events without thread_id -> top-level channel message (group by channel_id)
        by_thread = defaultdict(list)
        for event in slack_events:
            thread_id = event.get("thread_id")   # Only set for threaded replies
            channel_id = event.get("channel_id")  # Always set for Slack events

            if thread_id:
                # This is a threaded reply - group by the thread
                by_thread[thread_id].append(event)
            elif channel_id:
                # Top-level channel message - group by channel
                by_thread[channel_id].append(event)
            # If neither, skip (should not happen for Slack)
        # Process each thread
        for thread_id, thread_events in by_thread.items():
            try:
                context_id = await self._process_thread_events(
                    thread_id=thread_id,
                    events=thread_events,
                    intents=intents
                )
                if context_id:
                    threads_updated.append(context_id)
            except Exception as e:
                logger.error(f"Error analyzing thread {thread_id}: {e}", exc_info=True)

        return threads_updated

    async def _process_thread_events(
        self,
        thread_id: str,
        events: List[Dict[str, Any]],
        intents: Dict[str, List[Dict[str, Any]]]
    ) -> Optional[str]:
        """Process events for a single thread."""
        if not events:
            return None

        # Sort by time
        events = sorted(events, key=lambda e: e.get("observed_at"))

        # Get or create thread context
        thread_data = self.active_threads.get(thread_id)

        if not thread_data:
            # Create new thread context
            thread_data = await self._start_thread(
                thread_id=thread_id,
                first_event=events[0]
            )
            self.active_threads[thread_id] = thread_data
            logger.info(f"🧵 Thread started: {thread_id}")

        # Update thread with new events
        await self._update_thread(thread_data, events, intents)

        # Check if thread is resolved
        if self._is_thread_resolved(thread_data, events):
            await self._resolve_thread(thread_data)

        return thread_data["context_id"]

    async def _start_thread(
        self,
        thread_id: str,
        first_event: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Start tracking a new thread."""
        started_at = first_event.get("observed_at")
        channel_id = first_event.get("channel_id")
        initiator_id = first_event.get("user_id")

        # Detect initial pattern and topic
        text = first_event.get("text", "")
        pattern = self._detect_pattern(text)
        topic = self._extract_topic(text)

        # Use upsert to handle restarts gracefully (thread_id has UNIQUE constraint)
        query = """
            INSERT INTO thread_contexts (
                thread_id, channel_id, topic, phase, pattern,
                participants, initiator_id, started_at, last_activity,
                message_count, decisions_made, facts_established,
                questions_answered, source_event_ids
            ) VALUES ($1, $2, $3, 'initiation', $4, '[]'::jsonb, $5, $6, $6, 0, '{}', '{}', '[]'::jsonb, '{}')
            ON CONFLICT (thread_id) DO UPDATE
                SET last_activity = EXCLUDED.last_activity,
                    channel_id = COALESCE(thread_contexts.channel_id, EXCLUDED.channel_id)
            RETURNING context_id, phase, pattern, message_count
        """

        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                query,
                thread_id,
                channel_id,
                topic,
                pattern,
                initiator_id,
                started_at
            )

        context_id = str(row["context_id"])
        # Restore state from DB if thread already existed (interpreter restart)
        existing_phase = row["phase"] or "initiation"
        existing_pattern = row["pattern"] or pattern
        existing_msg_count = row["message_count"] or 0

        return {
            "context_id": context_id,
            "thread_id": thread_id,
            "channel_id": channel_id,
            "topic": topic,
            "pattern": existing_pattern,
            "phase": existing_phase,
            "started_at": started_at,
            "last_activity": started_at,
            "participants": {},  # user_id -> {message_count, first_seen, last_seen}
            "decisions": [],
            "facts": [],
            "questions": [],
            # Seed event_ids with existing_msg_count placeholders so that
            # len(event_ids) reflects the true cumulative count after restart
            "event_ids": [None] * existing_msg_count,
            "intent_ids": [],
            "_new_event_count": 0,  # tracks only events added in this session
        }

    async def _update_thread(
        self,
        thread_data: Dict[str, Any],
        events: List[Dict[str, Any]],
        intents: Dict[str, List[Dict[str, Any]]]
    ):
        """Update thread context with new events."""
        for event in events:
            event_id = event.get("event_id")
            observed_at = event.get("observed_at")
            user_id = event.get("user_id")
            text = event.get("text", "")

            # Track event
            thread_data["event_ids"].append(event_id)
            thread_data["_new_event_count"] = thread_data.get("_new_event_count", 0) + 1
            thread_data["last_activity"] = observed_at

            # Track participant
            if user_id:
                if user_id not in thread_data["participants"]:
                    thread_data["participants"][user_id] = {
                        "message_count": 0,
                        "first_seen": observed_at.isoformat(),
                        "last_seen": observed_at.isoformat()
                    }
                thread_data["participants"][user_id]["message_count"] += 1
                thread_data["participants"][user_id]["last_seen"] = observed_at.isoformat()

            # Extract decisions
            if any(kw in text.lower() for kw in self.decision_keywords):
                thread_data["decisions"].append({
                    "decision": text[:200],
                    "timestamp": observed_at.isoformat(),
                    "participant": user_id
                })

            # Extract facts (look for definitive statements)
            if any(indicator in text.lower() for indicator in ["is", "are", "will", "must"]):
                if len(text.split()) > 5:  # Ignore short statements
                    thread_data["facts"].append({
                        "fact": text[:200],
                        "source_event_id": event_id
                    })

            # Track Q&A
            if "?" in text:
                thread_data["questions"].append({
                    "question": text[:200],
                    "asked_by": user_id,
                    "timestamp": observed_at.isoformat(),
                    "answered": False
                })
            else:
                # Check if this answers a recent question
                for q in reversed(thread_data["questions"][-5:]):  # Check last 5 questions
                    if not q.get("answered") and user_id != q.get("asked_by"):
                        q["answered"] = True
                        q["answer"] = text[:200]
                        q["answered_by"] = user_id
                        break

            # Track intents
            if event_id in intents:
                for intent in intents[event_id]:
                    intent_id = intent.get("intent_id")
                    if intent_id:
                        thread_data["intent_ids"].append(intent_id)

        # Update phase based on activity
        message_count = len(thread_data["event_ids"])
        if thread_data["phase"] == "initiation" and message_count >= 3:
            thread_data["phase"] = "exploration"
        elif thread_data["phase"] == "exploration" and len(thread_data["participants"]) >= 2:
            thread_data["phase"] = "discussion"

        # Refine pattern based on accumulated data
        thread_data["pattern"] = self._refine_pattern(thread_data)

        # Persist to database
        await self._persist_thread(thread_data)

    async def _persist_thread(self, thread_data: Dict[str, Any]):
        """Write thread updates to database."""
        # message_count is cumulative — add only the new events processed this batch
        new_events = thread_data.get("_new_event_count", 0)
        query = """
            UPDATE thread_contexts
            SET last_activity = $2,
                message_count = thread_contexts.message_count + $3,
                phase = $4,
                pattern = $5,
                participants = $6,
                decisions_made = $7,
                facts_established = $8,
                questions_answered = $9,
                source_event_ids = $10,
                updated_at = NOW()
            WHERE context_id = $1
        """

        # decisions_made is text[] - extract text strings only
        decisions_text = [d.get("decision", "")[:200] for d in thread_data["decisions"][:10]]

        # facts_established is text[] - extract text strings only
        facts_text = [f.get("fact", "")[:200] for f in thread_data["facts"][:20]]

        # participants is jsonb - convert to JSON string
        participants_json = [
            {
                "user_id": user_id,
                **data
            }
            for user_id, data in thread_data["participants"].items()
        ]

        # source_event_ids is uuid[] - convert string UUIDs, skip None placeholders
        from uuid import UUID as UUID_type
        event_uuids = []
        for eid in thread_data["event_ids"]:
            if eid is None:
                continue  # skip restart-seeded placeholders
            try:
                event_uuids.append(UUID_type(str(eid)))
            except (ValueError, AttributeError):
                pass

        async with self.pool.acquire() as conn:
            await conn.execute(
                query,
                thread_data["context_id"],
                thread_data["last_activity"],
                new_events,  # only new events added this batch (DB increments cumulatively)
                thread_data["phase"],
                thread_data["pattern"],
                json.dumps(participants_json),  # jsonb column
                decisions_text,                 # text[] column
                facts_text,                     # text[] column
                json.dumps([q for q in thread_data["questions"][-10:]]),  # jsonb column
                event_uuids                     # uuid[] column
            )

    def _detect_pattern(self, text: str) -> str:
        """Detect conversation pattern from text."""
        text_lower = text.lower()

        # Count pattern indicators
        scores = {
            "Q&A": sum(1 for kw in self.question_keywords if kw in text_lower),
            "troubleshooting": sum(1 for kw in self.troubleshoot_keywords if kw in text_lower),
            "brainstorm": sum(1 for kw in self.brainstorm_keywords if kw in text_lower),
            "decision": sum(1 for kw in self.decision_keywords if kw in text_lower)
        }

        # Return pattern with highest score (default to Q&A if no indicators)
        if max(scores.values()) == 0:
            return "Q&A"  # Default pattern

        return max(scores, key=scores.get)

    def _refine_pattern(self, thread_data: dict) -> str:
        """Refine pattern based on accumulated thread data."""
        # If multiple decisions made, it is a decision thread
        if len(thread_data["decisions"]) >= 2:
            return "decision"

        # If lots of questions, it is Q&A
        if len(thread_data["questions"]) >= 3:
            answered = sum(1 for q in thread_data["questions"] if q.get("answered"))
            if answered > 0:
                return "Q&A"

        # Keep current pattern
        return thread_data["pattern"]

    def _extract_topic(self, text: str) -> str:
        """Extract topic from text (simplified)."""
        # Remove common words
        words = text.lower().split()
        stopwords = {"the", "a", "an", "and", "or", "but", "in", "on", "at", "to", "for", "of", "with"}
        keywords = [w for w in words if len(w) > 3 and w not in stopwords]

        if keywords:
            return " ".join(keywords[:3])  # First 3 keywords
        return "general topic"

    def _is_thread_resolved(
        self,
        thread_data: dict,
        events: list
    ) -> bool:
        """Check if thread appears to be resolved."""
        # Check for resolution indicators in latest messages
        latest_texts = [e.get("text", "").lower() for e in events[-3:]]

        resolution_keywords = {
            "thanks", "thank you", "solved", "fixed", "resolved",
            "got it", "makes sense", "perfect", "awesome", "great"
        }

        for text in latest_texts:
            if any(kw in text for kw in resolution_keywords):
                return True

        # If all questions answered
        unanswered = sum(1 for q in thread_data["questions"] if not q.get("answered"))
        if len(thread_data["questions"]) > 0 and unanswered == 0:
            return True

        return False

    async def _resolve_thread(self, thread_data: dict):
        """Mark thread as resolved."""
        query = """
            UPDATE thread_contexts
            SET phase = 'resolution'
            WHERE context_id = $1 AND phase != 'resolution'
        """

        async with self.pool.acquire() as conn:
            result = await conn.execute(query, thread_data["context_id"])

        if result != "UPDATE 0":
            thread_data["phase"] = "resolution"
            import logging
            logger = logging.getLogger(__name__)
            logger.info(
                f"Thread resolved: {thread_data['thread_id']} "
                f"({len(thread_data['event_ids'])} messages, "
                f"{len(thread_data['participants'])} participants, "
                f"pattern={thread_data['pattern']})"
            )
            # Remove from active tracking
            if thread_data["thread_id"] in self.active_threads:
                del self.active_threads[thread_data["thread_id"]]

