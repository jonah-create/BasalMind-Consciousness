"""
Channel Summarizer - Event-Driven with DB-Persisted Turn Counters

Each Slack message to a channel increments a persistent DB counter.
Summaries fire on event-driven thresholds:
  - First summary: at 5 messages
  - Updates: every 10 messages thereafter (15, 25, 35, ...)

Counter survives interpreter restarts because it lives in the DB.
"""

import json
import logging
from typing import List, Dict, Any, Optional
from datetime import datetime
from collections import Counter
import asyncpg

logger = logging.getLogger(__name__)

# Threshold constants
INITIAL_SUMMARY_THRESHOLD = 5
UPDATE_SUMMARY_INTERVAL = 10


class ChannelSummarizer:
    """
    Event-driven channel summarizer with DB-persisted per-channel message counters.

    - On each call to process_events(), Slack messages increment the channel counter
      in the channel_turn_counts table.
    - A summary is triggered when the counter crosses 5 (first) or every 10 thereafter.
    - The in-memory buffer collects recent messages per channel for summarization text.
    """

    def __init__(
        self,
        postgres_pool: asyncpg.Pool,
        # kept for compat but now unused (thresholds are constants above)
        summary_interval_messages: int = INITIAL_SUMMARY_THRESHOLD,
        summary_update_messages: int = UPDATE_SUMMARY_INTERVAL,
        drift_threshold: float = 0.3
    ):
        self.pool = postgres_pool
        self.drift_threshold = drift_threshold

        # In-memory buffer of recent events per channel (for summary text generation)
        # This is ephemeral — only used for the current process batch
        self._recent_events: Dict[str, List[Dict[str, Any]]] = {}

    async def initialize(self):
        """
        No-op — counter state is fully in the DB.
        Kept for backward compat with engine.setup().
        """
        logger.info("ChannelSummarizer initialized (DB-backed counters)")

    async def process_events(
        self,
        events: List[Dict[str, Any]]
    ) -> List[str]:
        """
        Process a batch of events.

        For each Slack message:
          1. Increment the persistent channel counter in channel_turn_counts.
          2. Buffer the event for summary text generation.
          3. If counter crosses a threshold, create a summary.

        Returns:
            List of summary_ids created.
        """
        summaries_created = []

        # Only process Slack messages
        slack_events = [
            e for e in events
            if e.get("source_system") == "slack" and e.get("channel_id")
        ]

        if not slack_events:
            return summaries_created

        # Group by channel
        by_channel: Dict[str, List[Dict[str, Any]]] = {}
        for event in slack_events:
            ch = event["channel_id"]
            by_channel.setdefault(ch, []).append(event)

        for channel_id, ch_events in by_channel.items():
            # Buffer events for this channel
            self._recent_events.setdefault(channel_id, [])
            self._recent_events[channel_id].extend(ch_events)
            # Keep at most 50 recent messages in buffer
            self._recent_events[channel_id] = self._recent_events[channel_id][-50:]

            # Atomically increment counter and check threshold
            new_count, prev_count = await self._increment_counter(
                channel_id, len(ch_events)
            )

            # Check if we crossed a summary threshold
            if self._crossed_threshold(prev_count, new_count):
                try:
                    summary_id = await self._create_summary(
                        channel_id=channel_id,
                        channel_events=self._recent_events[channel_id],
                        total_count=new_count
                    )
                    if summary_id:
                        summaries_created.append(summary_id)
                        # Don't reset counter — it keeps growing so we trigger on 5, 15, 25...
                except Exception as e:
                    logger.error(
                        f"Failed to create summary for {channel_id}: {e}",
                        exc_info=True
                    )

        return summaries_created

    def _crossed_threshold(self, prev: int, new: int) -> bool:
        """
        Returns True if the counter crossed a summary threshold between prev and new.

        Thresholds: 5, 15, 25, 35, ... (first=5, then every 10)
        """
        for count in range(prev + 1, new + 1):
            if count == INITIAL_SUMMARY_THRESHOLD:
                return True
            if count > INITIAL_SUMMARY_THRESHOLD and (count - INITIAL_SUMMARY_THRESHOLD) % UPDATE_SUMMARY_INTERVAL == 0:
                return True
        return False

    async def _increment_counter(
        self, channel_id: str, delta: int
    ) -> tuple[int, int]:
        """
        Atomically increment the channel message counter in the DB.

        Returns:
            (new_count, prev_count)
        """
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow("""
                INSERT INTO channel_turn_counts (channel_id, message_count, last_updated)
                VALUES ($1, $2, NOW())
                ON CONFLICT (channel_id) DO UPDATE
                    SET message_count = channel_turn_counts.message_count + $2,
                        last_updated = NOW()
                RETURNING
                    message_count as new_count,
                    message_count - $2 as prev_count
            """, channel_id, delta)
        return row["new_count"], row["prev_count"]

    async def _create_summary(
        self,
        channel_id: str,
        channel_events: List[Dict[str, Any]],
        total_count: int
    ) -> Optional[str]:
        """Create a summary from buffered events."""
        if not channel_events:
            return None

        summary_data = self._analyze_events(channel_events)
        channel_name = channel_events[-1].get("channel_name") or channel_id

        times = [e.get("observed_at") for e in channel_events if e.get("observed_at")]
        if not times:
            return None

        period_start = min(times)
        period_end = max(times)

        previous_summary = await self._get_previous_summary(channel_id)
        drift_score = None
        if previous_summary:
            drift_score = self._calculate_drift(
                summary_data["key_topics"],
                previous_summary.get("key_topics", [])
            )

        summary_id = await self._write_summary(
            channel_id=channel_id,
            channel_name=channel_name,
            period_start=period_start,
            period_end=period_end,
            summary_data=summary_data,
            drift_score=drift_score,
            previous_summary_id=(
                previous_summary.get("summary_id") if previous_summary else None
            )
        )

        drift_str = f"{drift_score:.2f}" if drift_score is not None else "N/A"
        logger.info(
            f"Summary created: {channel_name} "
            f"({summary_data['message_count']} msgs buffered, "
            f"{total_count} total, {summary_data['participant_count']} users) "
            f"drift={drift_str}"
        )

        if drift_score and drift_score >= self.drift_threshold:
            old_topics = previous_summary.get("key_topics", [])[:3] if previous_summary else []
            new_topics = summary_data["key_topics"][:3]
            logger.info(f"  TOPIC SHIFT: {old_topics} -> {new_topics}")

        return summary_id

    def _analyze_events(self, events: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Extract key information from events."""
        participants = set()
        all_words = []
        decision_indicators = []

        for event in events:
            user_id = event.get("user_id")
            if user_id:
                participants.add(user_id)

            text = event.get("text", "")
            if text:
                words = self._extract_keywords(text)
                all_words.extend(words)

                if any(w in text.lower() for w in ["decide", "decision", "choose", "will use", "going with"]):
                    decision_indicators.append(text[:100])

        word_counts = Counter(all_words)
        key_topics = [word for word, count in word_counts.most_common(10)]

        summary_text = self._generate_summary_text(
            message_count=len(events),
            participant_count=len(participants),
            key_topics=key_topics[:5],
            decision_count=len(decision_indicators)
        )

        return {
            "summary_text": summary_text,
            "key_topics": key_topics,
            "active_decisions": decision_indicators[:5],
            "participant_count": len(participants),
            "message_count": len(events),
            "source_event_ids": [e.get("event_id") for e in events if e.get("event_id")]
        }

    def _extract_keywords(self, text: str) -> List[str]:
        """Extract meaningful keywords from text."""
        stopwords = {
            "the", "a", "an", "and", "or", "but", "in", "on", "at", "to", "for",
            "of", "with", "by", "from", "is", "are", "was", "were", "be", "been",
            "do", "does", "did", "will", "would", "could", "should", "can",
            "this", "that", "these", "those", "i", "you", "we", "they"
        }
        words = text.lower().split()
        keywords = []
        for word in words:
            word = "".join(c for c in word if c.isalnum() or c == "-")
            if len(word) > 3 and word not in stopwords:
                keywords.append(word)
        return keywords

    def _generate_summary_text(
        self,
        message_count: int,
        participant_count: int,
        key_topics: List[str],
        decision_count: int
    ) -> str:
        """Generate human-readable summary."""
        parts = [f"{message_count} messages from {participant_count} participants"]
        if key_topics:
            parts.append(f"discussing {', '.join(key_topics[:3])}")
        if decision_count > 0:
            parts.append(f"with {decision_count} decision points")
        return ". ".join(parts) + "."

    def _calculate_drift(
        self,
        current_topics: List[str],
        previous_topics: List[str]
    ) -> float:
        """Calculate topic drift (0.0 = same, 1.0 = completely different)."""
        if not previous_topics:
            return 0.0
        current_set = set(current_topics[:10])
        previous_set = set(previous_topics[:10])
        intersection = len(current_set & previous_set)
        union = len(current_set | previous_set)
        if union == 0:
            return 0.0
        return 1.0 - (intersection / union)

    async def _get_previous_summary(
        self, channel_id: str
    ) -> Optional[Dict[str, Any]]:
        """Get the most recent summary for this channel."""
        query = """
            SELECT summary_id, key_topics
            FROM channel_summaries
            WHERE channel_id = $1
            ORDER BY period_end DESC
            LIMIT 1
        """
        try:
            async with self.pool.acquire() as conn:
                row = await conn.fetchrow(query, channel_id)
            if row:
                return dict(row)
        except Exception as e:
            logger.error(f"Error getting previous summary: {e}")
        return None

    async def _write_summary(
        self,
        channel_id: str,
        channel_name: str,
        period_start: datetime,
        period_end: datetime,
        summary_data: Dict[str, Any],
        drift_score: Optional[float],
        previous_summary_id: Optional[str]
    ) -> str:
        """Write summary to database."""
        query = """
            INSERT INTO channel_summaries (
                channel_id, channel_name, summary_text, key_topics,
                active_decisions, participant_count, period_start,
                period_end, message_count, previous_summary_id,
                drift_score, source_event_ids
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12
            )
            ON CONFLICT (channel_id, period_end)
            DO UPDATE SET
                summary_text = EXCLUDED.summary_text,
                key_topics = EXCLUDED.key_topics,
                message_count = EXCLUDED.message_count
            RETURNING summary_id
        """
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                query,
                channel_id,
                channel_name,
                summary_data["summary_text"],
                summary_data["key_topics"],
                summary_data["active_decisions"],
                summary_data["participant_count"],
                period_start,
                period_end,
                summary_data["message_count"],
                previous_summary_id,
                drift_score,
                summary_data["source_event_ids"]
            )
        return str(row["summary_id"])
