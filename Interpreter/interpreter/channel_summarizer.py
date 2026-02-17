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
import os
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
        drift_threshold: float = 0.3,
        openai_api_key: Optional[str] = None,
        openai_model: str = "gpt-4o-mini",
    ):
        self.pool = postgres_pool
        self.drift_threshold = drift_threshold
        self._openai_model = openai_model

        # OpenAI client (lazy init — only if key available)
        self._openai_client = None
        _key = openai_api_key or os.getenv("OPENAI_API_KEY")
        if _key and _key != "sk-your-key-here":
            try:
                import openai as _openai
                self._openai_client = _openai.OpenAI(api_key=_key)
                logger.info("ChannelSummarizer: OpenAI LLM enabled for thread summaries")
            except Exception as e:
                logger.warning(f"ChannelSummarizer: OpenAI unavailable — falling back to keyword summary: {e}")

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
        """Create a summary from thread summaries (LLM) or events (fallback)."""
        if not channel_events:
            return None

        channel_name = channel_events[-1].get("channel_name") or channel_id

        times = [e.get("observed_at") for e in channel_events if e.get("observed_at")]
        if not times:
            return None

        period_start = min(times)
        period_end = max(times)

        # ── Try LLM summary of thread summaries first ──────────────────────
        thread_summaries = await self._get_channel_thread_summaries(channel_id)
        if thread_summaries and self._openai_client:
            summary_data = await self._llm_summarize_threads(
                channel_name=channel_name,
                thread_summaries=thread_summaries,
                channel_events=channel_events,
            )
        else:
            # Fallback: keyword extraction from raw events
            summary_data = self._analyze_events(channel_events)
            if thread_summaries and not self._openai_client:
                logger.debug(
                    "No OpenAI client — using keyword fallback for channel summary"
                )

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
            f"{total_count} total, {summary_data['participant_count']} users, "
            f"{len(thread_summaries)} threads) drift={drift_str}"
        )

        if drift_score and drift_score >= self.drift_threshold:
            old_topics = previous_summary.get("key_topics", [])[:3] if previous_summary else []
            new_topics = summary_data["key_topics"][:3]
            logger.info(f"  TOPIC SHIFT: {old_topics} -> {new_topics}")

        return summary_id

    async def _get_channel_thread_summaries(
        self, channel_id: str
    ) -> List[Dict[str, Any]]:
        """
        Fetch thread-level summaries for this channel from thread_contexts.
        Returns a list of dicts with topic, phase, pattern, decisions_made,
        message_count, and participant count.
        """
        query = """
            SELECT
                thread_id,
                topic,
                phase,
                pattern,
                decisions_made,
                message_count,
                jsonb_array_length(
                    CASE jsonb_typeof(participants)
                        WHEN 'array' THEN participants
                        ELSE '[]'::jsonb
                    END
                ) AS participant_count,
                last_activity
            FROM thread_contexts
            WHERE channel_id = $1
            ORDER BY last_activity DESC
            LIMIT 20
        """
        try:
            async with self.pool.acquire() as conn:
                rows = await conn.fetch(query, channel_id)
            return [dict(r) for r in rows]
        except Exception as e:
            logger.error(f"Error fetching thread summaries for {channel_id}: {e}")
            return []

    def _analyze_events(self, events: list) -> dict:
        """Extract key information from events (keyword-based fallback summary)."""
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

        from collections import Counter
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

    def _extract_keywords(self, text: str) -> list:
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
        key_topics: list,
        decision_count: int
    ) -> str:
        """Generate human-readable summary (keyword fallback)."""
        parts = [f"{message_count} messages from {participant_count} participants"]
        if key_topics:
            parts.append(f"discussing {', '.join(key_topics[:3])}")
        if decision_count > 0:
            parts.append(f"with {decision_count} decision points")
        return ". ".join(parts) + "."

    def _calculate_drift(
        self,
        current_topics: list,
        previous_topics: list
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

    async def _llm_summarize_threads(
        self,
        channel_name: str,
        thread_summaries: List[Dict[str, Any]],
        channel_events: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        """
        Use OpenAI to generate a natural language summary of thread summaries.
        Falls back to keyword extraction if the LLM call fails.
        """
        participants = set()
        for e in channel_events:
            if e.get("user_id"):
                participants.add(e["user_id"])

        # Build the thread context block for the prompt
        thread_lines = []
        all_decisions = []
        for i, t in enumerate(thread_summaries, 1):
            topic = t.get("topic") or "general discussion"
            phase = t.get("phase") or "unknown"
            pattern = t.get("pattern") or "conversation"
            msgs = t.get("message_count") or 0
            pcount = t.get("participant_count") or 0
            decisions = t.get("decisions_made") or []
            decisions_str = "; ".join(decisions[:3])
            line = (
                f"Thread {i}: topic='{topic}', phase={phase}, "
                f"pattern={pattern}, {msgs} messages, {pcount} participants"
            )
            if decisions:
                line += f", decisions: {decisions_str}"
                all_decisions.extend(decisions[:3])
            thread_lines.append(line)

        thread_block = "\n".join(thread_lines)
        prompt = (
            f"You are summarizing activity in a Slack channel called '{channel_name}'.\n"
            f"Below are summaries of the active conversation threads:\n\n"
            f"{thread_block}\n\n"
            "Write a concise 2-3 sentence summary of what this channel has been discussing. "
            "Focus on topics, decisions made, and the overall direction of conversations. "
            "Do not mention thread numbers — write naturally as if describing the channel."
        )

        try:
            import asyncio
            loop = asyncio.get_event_loop()
            response = await loop.run_in_executor(
                None,
                lambda: self._openai_client.chat.completions.create(
                    model=self._openai_model,
                    messages=[{"role": "user", "content": prompt}],
                    max_tokens=200,
                    temperature=0.3,
                )
            )
            summary_text = response.choices[0].message.content.strip()
            logger.info(f"LLM channel summary for {channel_name}: {summary_text[:80]}...")
        except Exception as e:
            logger.warning(f"LLM channel summary failed — using fallback: {e}")
            # Keyword fallback
            return self._analyze_events(channel_events)

        # Extract topics from thread data for drift tracking
        from collections import Counter as _Counter
        topic_words = []
        for t in thread_summaries:
            topic = t.get("topic") or ""
            topic_words.extend(topic.lower().split()[:3])
        word_counts = _Counter(topic_words)
        key_topics = [w for w, _ in word_counts.most_common(10) if len(w) > 3]

        return {
            "summary_text": summary_text,
            "key_topics": key_topics,
            "active_decisions": all_decisions[:5],
            "participant_count": len(participants),
            "message_count": len(channel_events),
            "source_event_ids": [e.get("event_id") for e in channel_events if e.get("event_id")],
        }

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
