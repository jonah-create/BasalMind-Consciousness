"""
Interpreter Test Suite - Full Coverage

Tests all Interpreter components without requiring live DB/OpenAI connections.
Uses mocks for asyncpg, OpenAI, and Neo4j.
"""

import pytest
import asyncio
import json
import math
import uuid
from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch, AsyncMock, call
import sys

sys.path.insert(0, '/opt/basalmind/BasalMind_Consciousness/Interpreter')


def _make_async_pool(mock_conn):
    """
    Build a mock asyncpg pool whose acquire() works correctly as:
        async with pool.acquire() as conn:
            ...

    asyncpg.Pool.acquire() is a synchronous call that returns an
    *async context manager* (not a coroutine).  If we make acquire()
    itself an AsyncMock, the call returns a coroutine object which
    does NOT support __aenter__/__aexit__, producing:
        TypeError: 'coroutine' object does not support the
                   asynchronous context manager protocol

    The fix: make acquire() a plain MagicMock whose return value
    has async __aenter__/__aexit__ attributes.
    """
    ctx = MagicMock()
    ctx.__aenter__ = AsyncMock(return_value=mock_conn)
    ctx.__aexit__ = AsyncMock(return_value=False)

    mock_pool = MagicMock()
    mock_pool.acquire = MagicMock(return_value=ctx)
    return mock_pool


# ─────────────────────────────────────────────────────────────────────────────
# Intent Extractor
# ─────────────────────────────────────────────────────────────────────────────

class TestIntentExtractor:
    def setup_method(self):
        from interpreter.intent_extractor import IntentExtractor
        self.ex = IntentExtractor(confidence_threshold=0.3)

    def test_asking_question_detected(self):
        intents = self.ex.extract_from_event({
            "event_id": "e1", "text": "How do we configure authentication?",
            "user_id": "U1", "event_time": datetime.utcnow()
        })
        assert any(i["type"] == "asking_question" for i in intents)

    def test_making_decision_detected(self):
        intents = self.ex.extract_from_event({
            "event_id": "e2", "text": "Let's use PostgreSQL for the database",
            "user_id": "U1", "event_time": datetime.utcnow()
        })
        assert any(i["type"] == "making_decision" for i in intents)

    def test_troubleshooting_detected(self):
        # The intent_extractor maps bug/error reports to "reporting_bug" in the
        # "fixing_maintaining" category, not a literal "troubleshooting" type.
        intents = self.ex.extract_from_event({
            "event_id": "e3", "text": "Error: Connection timeout when connecting to Redis",
            "user_id": "U1", "event_time": datetime.utcnow()
        })
        fixing_types = {"reporting_bug", "troubleshooting", "debugging", "fixing"}
        assert any(
            i["type"] in fixing_types or i.get("category") == "fixing_maintaining"
            for i in intents
        ), f"Expected a fixing/troubleshooting intent, got: {intents}"

    def test_confidence_above_threshold(self):
        intents = self.ex.extract_from_event({
            "event_id": "e4", "text": "What is the best approach for caching?",
            "user_id": "U1", "event_time": datetime.utcnow()
        })
        assert all(i["confidence"] >= 0.3 for i in intents)

    def test_empty_text_returns_no_intents(self):
        intents = self.ex.extract_from_event({
            "event_id": "e5", "text": "", "user_id": "U1", "event_time": datetime.utcnow()
        })
        assert intents == []

    def test_batch_extraction_returns_dict(self):
        events = [
            {"event_id": "e1", "text": "How does this work?", "user_id": "U1", "event_time": datetime.utcnow()},
            {"event_id": "e2", "text": "Let's deploy this now", "user_id": "U2", "event_time": datetime.utcnow()},
        ]
        results = self.ex.extract_from_batch(events)
        assert isinstance(results, dict)
        assert "e1" in results

    def test_high_threshold_filters_more(self):
        from interpreter.intent_extractor import IntentExtractor
        low = IntentExtractor(confidence_threshold=0.1)
        high = IntentExtractor(confidence_threshold=0.9)
        event = {"event_id": "e1", "text": "ok", "user_id": "U1", "event_time": datetime.utcnow()}
        assert len(low.extract_from_event(event)) >= len(high.extract_from_event(event))

    def test_intent_has_required_fields(self):
        intents = self.ex.extract_from_event({
            "event_id": "e1", "text": "Why is the build failing?",
            "user_id": "U1", "event_time": datetime.utcnow()
        })
        for intent in intents:
            assert "type" in intent
            assert "confidence" in intent
            assert "category" in intent

    def test_nginx_event_no_text_intents(self):
        results = self.ex.extract_from_batch([{
            "event_id": "nginx-1", "source_system": "nginx", "text": "",
            "metadata": {"path": "/api/auth", "status": 500},
            "event_time": datetime.utcnow()
        }])
        # nginx events without text should have no text-based intents
        if "nginx-1" in results:
            assert isinstance(results["nginx-1"], list)


# ─────────────────────────────────────────────────────────────────────────────
# Thread Analyzer
# ─────────────────────────────────────────────────────────────────────────────

class TestThreadAnalyzer:
    def _make_event(self, text, thread_id="thread_1", channel_id="C001",
                    user_id="U1", event_id=None, offset_seconds=0):
        return {
            "event_id": event_id or f"evt_{text[:5]}",
            "source_system": "slack",
            "text": text,
            "thread_id": thread_id,
            "channel_id": channel_id,
            "user_id": user_id,
            "observed_at": datetime.utcnow() + timedelta(seconds=offset_seconds)
        }

    def _make_analyzer(self):
        from interpreter.thread_analyzer import ThreadAnalyzer
        mock_conn = MagicMock()
        mock_conn.fetchrow = AsyncMock(return_value={
            "context_id": "ctx-uuid-1234",
            "phase": "initiation",
            "pattern": "Q&A",
            "message_count": 0
        })
        mock_conn.execute = AsyncMock()
        pool = _make_async_pool(mock_conn)
        return ThreadAnalyzer(postgres_pool=pool)

    def test_detect_question_pattern(self):
        from interpreter.thread_analyzer import ThreadAnalyzer
        analyzer = ThreadAnalyzer.__new__(ThreadAnalyzer)
        analyzer.question_keywords = {"how", "what", "why", "when", "where", "who", "can", "?"}
        analyzer.decision_keywords = {"decide", "decision", "going with"}
        analyzer.troubleshoot_keywords = {"error", "issue", "problem", "bug"}
        analyzer.brainstorm_keywords = {"idea", "suggest", "what if"}
        pattern = analyzer._detect_pattern("How do we configure auth?")
        assert pattern == "Q&A"

    def test_detect_troubleshooting_pattern(self):
        from interpreter.thread_analyzer import ThreadAnalyzer
        analyzer = ThreadAnalyzer.__new__(ThreadAnalyzer)
        analyzer.question_keywords = {"how", "what", "?"}
        analyzer.decision_keywords = {"decide"}
        analyzer.troubleshoot_keywords = {"error", "issue", "problem", "bug"}
        analyzer.brainstorm_keywords = {"idea"}
        pattern = analyzer._detect_pattern("There is an error and a bug and a problem here")
        assert pattern == "troubleshooting"

    def test_extract_topic_removes_stopwords(self):
        from interpreter.thread_analyzer import ThreadAnalyzer
        analyzer = ThreadAnalyzer.__new__(ThreadAnalyzer)
        topic = analyzer._extract_topic("the quick brown fox jumps over the lazy dog")
        words = topic.split()
        stopwords = {"the", "a", "an", "and", "or", "but", "in", "on", "at", "to", "for", "of", "with"}
        assert not any(w in stopwords for w in words)

    def test_extract_topic_max_3_keywords(self):
        from interpreter.thread_analyzer import ThreadAnalyzer
        analyzer = ThreadAnalyzer.__new__(ThreadAnalyzer)
        topic = analyzer._extract_topic("authentication configuration deployment monitoring scaling performance")
        assert len(topic.split()) <= 3

    def test_is_thread_resolved_thank_you(self):
        from interpreter.thread_analyzer import ThreadAnalyzer
        analyzer = ThreadAnalyzer.__new__(ThreadAnalyzer)
        thread_data = {"questions": []}
        events = [{"text": "thanks, that solved it!"}]
        assert analyzer._is_thread_resolved(thread_data, events) is True

    def test_is_thread_resolved_all_questions_answered(self):
        from interpreter.thread_analyzer import ThreadAnalyzer
        analyzer = ThreadAnalyzer.__new__(ThreadAnalyzer)
        thread_data = {
            "questions": [{"question": "how?", "answered": True}]
        }
        events = [{"text": "here is some code"}]
        assert analyzer._is_thread_resolved(thread_data, events) is True

    def test_is_thread_not_resolved_unanswered_questions(self):
        from interpreter.thread_analyzer import ThreadAnalyzer
        analyzer = ThreadAnalyzer.__new__(ThreadAnalyzer)
        thread_data = {
            "questions": [{"question": "how?", "answered": False}]
        }
        events = [{"text": "let me think about this"}]
        assert analyzer._is_thread_resolved(thread_data, events) is False

    def test_refine_pattern_decision_on_multiple_decisions(self):
        from interpreter.thread_analyzer import ThreadAnalyzer
        analyzer = ThreadAnalyzer.__new__(ThreadAnalyzer)
        thread_data = {
            "decisions": [{"decision": "use postgres"}, {"decision": "use redis"}],
            "questions": [],
            "pattern": "Q&A"
        }
        assert analyzer._refine_pattern(thread_data) == "decision"

    def test_only_slack_events_processed(self):
        from interpreter.thread_analyzer import ThreadAnalyzer
        analyzer = ThreadAnalyzer.__new__(ThreadAnalyzer)
        analyzer.active_threads = {}
        analyzer.pool = MagicMock()
        # Non-slack events should return empty immediately
        result = asyncio.get_event_loop().run_until_complete(
            analyzer.process_events(
                [{"event_id": "e1", "source_system": "nginx", "text": "GET /api"}],
                {}
            )
        )
        assert result == []


# ─────────────────────────────────────────────────────────────────────────────
# Embedding Generator
# ─────────────────────────────────────────────────────────────────────────────

class TestEmbeddingGenerator:
    def _make_generator(self, mock_embedding=None, mock_conn=None):
        from interpreter.embedding_generator import EmbeddingGenerator
        if mock_embedding is None:
            mock_embedding = [0.1] * 1536
        pool = _make_async_pool(mock_conn) if mock_conn else None
        with patch("interpreter.embedding_generator.openai.OpenAI"):
            gen = EmbeddingGenerator(openai_api_key="sk-test", postgres_pool=pool)
            gen._call_openai = AsyncMock(return_value=mock_embedding)
        return gen

    @pytest.mark.asyncio
    async def test_generate_turn_embedding_returns_vector(self):
        gen = self._make_generator()
        result = await gen.generate_turn_embedding(
            event_id="evt-1", text="hello world", thread_id="thread-1"
        )
        assert result is not None
        assert len(result) == 1536

    @pytest.mark.asyncio
    async def test_generate_turn_embedding_empty_text_returns_none(self):
        gen = self._make_generator()
        result = await gen.generate_turn_embedding(
            event_id="evt-1", text="", thread_id="thread-1"
        )
        assert result is None

    @pytest.mark.asyncio
    async def test_generate_turn_embedding_whitespace_returns_none(self):
        gen = self._make_generator()
        result = await gen.generate_turn_embedding(
            event_id="evt-1", text="   ", thread_id="thread-1"
        )
        assert result is None

    @pytest.mark.asyncio
    async def test_generate_thread_embedding_returns_vector(self):
        gen = self._make_generator()
        events = [
            {"event_id": "e1", "source_system": "slack", "text": "question 1"},
            {"event_id": "e2", "source_system": "slack", "text": "answer 1"},
        ]
        result = await gen.generate_thread_embedding(thread_id="t1", events=events)
        assert result is not None
        assert len(result) == 1536

    @pytest.mark.asyncio
    async def test_generate_thread_embedding_empty_events(self):
        gen = self._make_generator()
        result = await gen.generate_thread_embedding(thread_id="t1", events=[])
        assert result is None

    @pytest.mark.asyncio
    async def test_aggregate_thread_text_slack(self):
        gen = self._make_generator()
        events = [
            {"source_system": "slack", "text": "first message"},
            {"source_system": "slack", "text": "second message"},
        ]
        text = gen._aggregate_thread_text(events)
        assert "first message" in text
        assert "second message" in text

    @pytest.mark.asyncio
    async def test_aggregate_thread_text_nginx(self):
        gen = self._make_generator()
        events = [{
            "source_system": "nginx",
            "metadata": {"path": "/api/auth", "method": "GET", "status": 200}
        }]
        text = gen._aggregate_thread_text(events)
        assert "Path: /api/auth" in text
        assert "Method: GET" in text
        assert "Status: 200" in text

    @pytest.mark.asyncio
    async def test_aggregate_thread_text_skips_empty(self):
        gen = self._make_generator()
        events = [
            {"source_system": "slack", "text": ""},
            {"source_system": "slack", "text": "real message"},
        ]
        text = gen._aggregate_thread_text(events)
        assert text.strip() == "real message"

    @pytest.mark.asyncio
    async def test_openai_failure_returns_none(self):
        gen = self._make_generator()
        gen._call_openai = AsyncMock(return_value=None)
        result = await gen.generate_turn_embedding(
            event_id="evt-1", text="test", thread_id="t1"
        )
        assert result is None

    @pytest.mark.asyncio
    async def test_store_embedding_uses_json_string(self):
        """Verify embedding is serialized as JSON string for pgvector."""
        from interpreter.embedding_generator import EmbeddingGenerator
        mock_conn = MagicMock()
        mock_conn.execute = AsyncMock()
        mock_conn.fetchrow = AsyncMock(return_value=None)
        mock_pool = _make_async_pool(mock_conn)
        with patch("interpreter.embedding_generator.openai.OpenAI"):
            gen = EmbeddingGenerator(openai_api_key="sk-test", postgres_pool=mock_pool)
        embedding = [0.1, 0.2, 0.3]
        await gen._store_embedding(
            entity_type="turn", entity_id="e1", embedding_type="semantic",
            embedding=embedding, text_content="test", source_event_ids=["e1"]
        )
        call_args = mock_conn.execute.call_args[0]
        # arg layout: [0]=query, [1]=entity_type, [2]=entity_id, [3]=embedding_type,
        #             [4]=model_name, [5]=embedding_str, [6]=text_content, ...
        embedding_arg = call_args[5]
        assert isinstance(embedding_arg, str)
        assert json.loads(embedding_arg) == embedding

    @pytest.mark.asyncio
    async def test_generate_channel_embedding_mean_pools(self):
        """Channel embedding is mean of thread vectors."""
        from interpreter.embedding_generator import EmbeddingGenerator
        # Two thread vectors
        vec_a = [1.0] * 1536
        vec_b = [3.0] * 1536

        mock_conn = MagicMock()
        mock_conn.fetch = AsyncMock(return_value=[
            {"embedding": json.dumps(vec_a)},
            {"embedding": json.dumps(vec_b)},
        ])
        mock_conn.execute = AsyncMock()
        mock_pool = _make_async_pool(mock_conn)

        with patch("interpreter.embedding_generator.openai.OpenAI"):
            gen = EmbeddingGenerator(openai_api_key="sk-test", postgres_pool=mock_pool)
        result = await gen.generate_channel_embedding(
            channel_id="C001", thread_ids=["t1", "t2"]
        )
        assert result is not None
        assert len(result) == 1536
        assert abs(result[0] - 2.0) < 0.0001  # mean of 1.0 and 3.0

    @pytest.mark.asyncio
    async def test_generate_channel_embedding_no_threads_returns_none(self):
        from interpreter.embedding_generator import EmbeddingGenerator
        mock_conn = MagicMock()
        mock_conn.fetch = AsyncMock(return_value=[])
        mock_pool = _make_async_pool(mock_conn)
        with patch("interpreter.embedding_generator.openai.OpenAI"):
            gen = EmbeddingGenerator(openai_api_key="sk-test", postgres_pool=mock_pool)
        result = await gen.generate_channel_embedding(channel_id="C001", thread_ids=[])
        assert result is None

    @pytest.mark.asyncio
    async def test_history_snapshot_drift_computed(self):
        """Second snapshot has computed cosine distance from first."""
        from interpreter.embedding_generator import EmbeddingGenerator

        vec_prev = [1.0] + [0.0] * 1535
        mock_conn = MagicMock()
        mock_conn.fetchrow = AsyncMock(return_value={
            "snapshot_index": 1,
            "emb_str": json.dumps(vec_prev)
        })
        mock_conn.execute = AsyncMock()
        mock_pool = _make_async_pool(mock_conn)

        with patch("interpreter.embedding_generator.openai.OpenAI"):
            gen = EmbeddingGenerator(openai_api_key="sk-test", postgres_pool=mock_pool)

        # New embedding orthogonal to previous (drift should be ~1.0)
        vec_new = [0.0, 1.0] + [0.0] * 1534
        await gen._store_thread_history_snapshot(
            thread_id="t1", channel_id="C1", embedding=vec_new,
            text_content="new topic", model_name="text-embedding-ada-002",
            phase="discussion", pattern="Q&A", participant_count=2, message_count=5
        )
        execute_args = mock_conn.execute.call_args[0]
        # arg layout: [0]=query, [1]=thread_id, [2]=channel_id, [3]=snapshot_index,
        #             [4]=message_count, [5]=phase, [6]=pattern, [7]=participant_count,
        #             [8]=model_name, [9]=embedding_str, [10]=text_content,
        #             [11]=drift_from_prev, [12]=interpretation_id
        drift = execute_args[11]
        assert drift is not None
        assert abs(drift - 1.0) < 0.01  # orthogonal vectors = cosine distance 1.0

    @pytest.mark.asyncio
    async def test_history_snapshot_no_previous_drift_is_none(self):
        """First snapshot has drift=None."""
        from interpreter.embedding_generator import EmbeddingGenerator
        mock_conn = MagicMock()
        mock_conn.fetchrow = AsyncMock(return_value=None)  # No previous snapshot
        mock_conn.execute = AsyncMock()
        mock_pool = _make_async_pool(mock_conn)

        with patch("interpreter.embedding_generator.openai.OpenAI"):
            gen = EmbeddingGenerator(openai_api_key="sk-test", postgres_pool=mock_pool)
        await gen._store_thread_history_snapshot(
            thread_id="t1", channel_id="C1", embedding=[0.1] * 1536,
            text_content="first message", model_name="text-embedding-ada-002",
            phase="initiation", pattern="Q&A", participant_count=1, message_count=1
        )
        execute_args = mock_conn.execute.call_args[0]
        drift = execute_args[11]
        assert drift is None

    @pytest.mark.asyncio
    async def test_history_snapshot_index_increments(self):
        """snapshot_index is prev + 1."""
        from interpreter.embedding_generator import EmbeddingGenerator
        mock_conn = MagicMock()
        mock_conn.fetchrow = AsyncMock(return_value={
            "snapshot_index": 5,
            "emb_str": json.dumps([0.1] * 1536)
        })
        mock_conn.execute = AsyncMock()
        mock_pool = _make_async_pool(mock_conn)

        with patch("interpreter.embedding_generator.openai.OpenAI"):
            gen = EmbeddingGenerator(openai_api_key="sk-test", postgres_pool=mock_pool)
        await gen._store_thread_history_snapshot(
            thread_id="t1", channel_id="C1", embedding=[0.1] * 1536,
            text_content="text", model_name="text-embedding-ada-002",
            phase="exploration", pattern="Q&A", participant_count=2, message_count=6
        )
        execute_args = mock_conn.execute.call_args[0]
        # arg layout: [0]=query, [1]=thread_id, [2]=channel_id, [3]=snapshot_index, ...
        snapshot_index = execute_args[3]
        assert snapshot_index == 6  # 5 + 1


# ─────────────────────────────────────────────────────────────────────────────
# Channel Summarizer
# ─────────────────────────────────────────────────────────────────────────────

class TestChannelSummarizer:
    def _make_events(self, channel_id="C001", count=5):
        return [
            {
                "event_id": f"evt-{i}",
                "source_system": "slack",
                "channel_id": channel_id,
                "user_id": f"U{i % 3}",
                "text": f"message {i}",
                "observed_at": datetime.utcnow() + timedelta(seconds=i)
            }
            for i in range(count)
        ]

    def _make_summarizer(self, new_count=6, prev_count=5):
        """
        Build a ChannelSummarizer with a mocked pool.

        _increment_counter does:
            row = await conn.fetchrow(INSERT ... RETURNING new_count, prev_count)
            return row["new_count"], row["prev_count"]

        We control threshold behaviour via new_count/prev_count.
        """
        from interpreter.channel_summarizer import ChannelSummarizer
        mock_conn = MagicMock()
        mock_conn.fetchrow = AsyncMock(return_value={
            "new_count": new_count,
            "prev_count": prev_count
        })
        mock_conn.execute = AsyncMock()
        mock_pool = _make_async_pool(mock_conn)
        summarizer = ChannelSummarizer(postgres_pool=mock_pool)
        return summarizer, mock_conn

    @pytest.mark.asyncio
    async def test_initialize_no_error(self):
        summarizer, _ = self._make_summarizer()
        await summarizer.initialize()  # should not raise

    @pytest.mark.asyncio
    async def test_skips_non_slack_events(self):
        summarizer, mock_conn = self._make_summarizer()
        nginx_events = [{
            "event_id": "n1", "source_system": "nginx",
            "channel_id": None, "text": "GET /api"
        }]
        result = await summarizer.process_events(nginx_events)
        # No channel_id -> skipped, no DB increment
        assert result == []

    @pytest.mark.asyncio
    async def test_buffer_accumulates_events(self):
        # Use high counts (200/199) so no summary threshold is crossed —
        # we just want to verify the in-memory buffer is populated.
        summarizer, _ = self._make_summarizer(new_count=200, prev_count=199)
        events = self._make_events(count=3)
        await summarizer.process_events(events)
        assert "C001" in summarizer._recent_events
        assert len(summarizer._recent_events["C001"]) >= 1


# ─────────────────────────────────────────────────────────────────────────────
# Session Tracker
# ─────────────────────────────────────────────────────────────────────────────

class TestSessionTracker:
    def _make_tracker(self):
        from interpreter.session_tracker import SessionTracker
        mock_conn = MagicMock()
        mock_conn.fetchrow = AsyncMock(return_value=None)  # No existing session
        mock_conn.execute = AsyncMock()
        mock_conn.fetchval = AsyncMock(return_value="session-uuid-1234")
        mock_pool = _make_async_pool(mock_conn)
        return SessionTracker(postgres_pool=mock_pool, session_timeout_minutes=30)

    def _make_event(self, user_id="U1", source="slack", offset_minutes=0):
        return {
            "event_id": f"evt-{user_id}",
            "source_system": source,
            "user_id": user_id,
            "channel_id": "C001",
            "observed_at": datetime.utcnow() + timedelta(minutes=offset_minutes)
        }

    @pytest.mark.asyncio
    async def test_process_events_returns_list(self):
        tracker = self._make_tracker()
        events = [self._make_event()]
        result = await tracker.process_events(events, {})
        assert isinstance(result, list)

    @pytest.mark.asyncio
    async def test_skips_events_without_user_id(self):
        tracker = self._make_tracker()
        event = {"event_id": "e1", "source_system": "slack", "observed_at": datetime.utcnow()}
        result = await tracker.process_events([event], {})
        # No user_id, should not crash and result is empty or empty list
        assert isinstance(result, list)

    def test_session_timeout_attribute(self):
        tracker = self._make_tracker()
        assert tracker.session_timeout == timedelta(minutes=30)


# ─────────────────────────────────────────────────────────────────────────────
# Engine Integration (smoke tests — no live DB)
# ─────────────────────────────────────────────────────────────────────────────

class TestEngineIntegration:
    """Smoke tests for engine class existence and EventDeduplicator."""

    def test_engine_class_exists(self):
        """InterpreterEngine is importable without env vars."""
        # engine.py does NOT import asyncpg/neo4j/openai at module level,
        # so no patching needed — the class itself is always importable.
        import interpreter.engine as eng
        assert hasattr(eng, "InterpreterEngine")

    def test_engine_has_run_method(self):
        import interpreter.engine as eng
        assert hasattr(eng.InterpreterEngine, "run")

    def test_event_deduplicator_filters_duplicates(self):
        """EventDeduplicator collapses Slack events with same client_msg_id."""
        from interpreter.event_deduplicator import EventDeduplicator
        dedup = EventDeduplicator()
        # Slack deduplication is keyed on client_msg_id, not event_id.
        # Both events share a client_msg_id so they form one group.
        # Each event also needs event_type for the culled_events metadata.
        events = [
            {
                "event_id": "e1",
                "event_type": "slack.message",
                "source_system": "slack",
                "text": "hello",
                "user_id": "U1",
                "metadata": {"client_msg_id": "msg-abc"},
            },
            {
                "event_id": "e2",
                "event_type": "slack.app_mention",
                "source_system": "slack",
                "text": "hello",
                "user_id": "U1",
                "metadata": {"client_msg_id": "msg-abc"},  # same → duplicate group
            },
            {
                "event_id": "e3",
                "event_type": "slack.message",
                "source_system": "slack",
                "text": "world",
                "user_id": "U2",
                "metadata": {},  # no client_msg_id → unique
            },
        ]
        result = dedup.deduplicate(events)
        # deduplicate() returns List[Dict], not a tuple
        assert isinstance(result, list)
        ids = [e["event_id"] for e in result]
        # Group 1 (msg-abc) → 1 canonical; Group 2 (unique e3) → 1 canonical → total 2
        assert len(result) == 2
        assert len(ids) == len(set(ids))  # no duplicate IDs in output


# ─────────────────────────────────────────────────────────────────────────────
# Drift Computation (pure math — no mocks)
# ─────────────────────────────────────────────────────────────────────────────

class TestDriftComputation:
    """Verify cosine distance math used in history snapshots."""

    def _cosine_distance(self, a, b):
        dot = sum(x * y for x, y in zip(a, b))
        mag_a = math.sqrt(sum(x * x for x in a))
        mag_b = math.sqrt(sum(x * x for x in b))
        return 1.0 - dot / (mag_a * mag_b)

    def test_identical_vectors_zero_drift(self):
        v = [0.5] * 1536
        assert abs(self._cosine_distance(v, v)) < 1e-6

    def test_orthogonal_vectors_drift_one(self):
        a = [1.0] + [0.0] * 1535
        b = [0.0, 1.0] + [0.0] * 1534
        assert abs(self._cosine_distance(a, b) - 1.0) < 1e-6

    def test_opposite_vectors_drift_two(self):
        a = [1.0] + [0.0] * 1535
        b = [-1.0] + [0.0] * 1535
        assert abs(self._cosine_distance(a, b) - 2.0) < 1e-6

    def test_similar_vectors_low_drift(self):
        import random
        random.seed(42)
        base = [random.gauss(0, 1) for _ in range(1536)]
        # Small perturbation
        perturbed = [x + random.gauss(0, 0.01) for x in base]
        drift = self._cosine_distance(base, perturbed)
        assert drift < 0.01  # similar vectors should have low drift

# ─────────────────────────────────────────────────────────────────────────────
# Graph Node IDs Linkage (Priority 1: Postgres ↔ Neo4j lineage)
# ─────────────────────────────────────────────────────────────────────────────

class TestGraphNodeIdsLinkage:
    """
    Verify that Neo4j node IDs created during a batch are stored on the
    interpretations record in PostgreSQL, enabling Postgres→Neo4j traversal.
    """

    def _make_async_pool(self, mock_conn):
        ctx = MagicMock()
        ctx.__aenter__ = AsyncMock(return_value=mock_conn)
        ctx.__aexit__ = AsyncMock(return_value=False)
        mock_pool = MagicMock()
        mock_pool.acquire = MagicMock(return_value=ctx)
        return mock_pool

    # ── PostgresWriter: write_interpretation accepts graph_node_ids ──────────

    @pytest.mark.asyncio
    async def test_write_interpretation_accepts_graph_node_ids(self):
        """write_interpretation must accept graph_node_ids without error."""
        from interpreter.postgres_writer import PostgresWriter

        mock_conn = AsyncMock()
        mock_conn.fetchval = AsyncMock(return_value=uuid.UUID("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"))

        writer = PostgresWriter.__new__(PostgresWriter)
        writer.pool = self._make_async_pool(mock_conn)

        events = [{
            "event_id": str(uuid.uuid4()),
            "observed_at": datetime(2026, 2, 17, 12, 0, 0),
            "source_system": "slack",
            "text": "test",
            "user_id": "U1",
            "metadata": {},
        }]
        intents = {}

        # Must not raise even with graph_node_ids supplied
        result = await writer.write_interpretation(
            window_start=datetime(2026, 2, 17, 12, 0, 0),
            window_end=datetime(2026, 2, 17, 12, 0, 1),
            events=events,
            intents=intents,
            graph_node_ids=["U1", "intent_evt1_asking_question"],
        )
        assert result == "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"

    @pytest.mark.asyncio
    async def test_write_interpretation_stores_graph_node_ids_in_query(self):
        """graph_node_ids must be passed as the 13th positional arg to fetchval."""
        from interpreter.postgres_writer import PostgresWriter

        mock_conn = AsyncMock()
        mock_conn.fetchval = AsyncMock(return_value=uuid.UUID("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"))

        writer = PostgresWriter.__new__(PostgresWriter)
        writer.pool = self._make_async_pool(mock_conn)

        node_ids = ["U99", "intent_e1_making_decision"]
        events = [{
            "event_id": str(uuid.uuid4()),
            "observed_at": datetime(2026, 2, 17, 12, 0, 0),
            "source_system": "slack",
            "text": "let's ship it",
            "user_id": "U99",
            "metadata": {},
        }]

        await writer.write_interpretation(
            window_start=datetime(2026, 2, 17, 12, 0, 0),
            window_end=datetime(2026, 2, 17, 12, 0, 1),
            events=events,
            intents={},
            graph_node_ids=node_ids,
        )

        call_args = mock_conn.fetchval.call_args[0]
        # arg[0] is the SQL string; args[1..12] are positional params; [13] is graph_node_ids
        stored_ids = call_args[13]
        assert stored_ids == node_ids

    @pytest.mark.asyncio
    async def test_write_interpretation_defaults_to_empty_list(self):
        """Omitting graph_node_ids should default to empty list, not None."""
        from interpreter.postgres_writer import PostgresWriter

        mock_conn = AsyncMock()
        mock_conn.fetchval = AsyncMock(return_value=uuid.UUID("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"))

        writer = PostgresWriter.__new__(PostgresWriter)
        writer.pool = self._make_async_pool(mock_conn)

        events = [{
            "event_id": str(uuid.uuid4()),
            "observed_at": datetime(2026, 2, 17, 12, 0, 0),
            "source_system": "slack",
            "text": "hello",
            "user_id": "U1",
            "metadata": {},
        }]

        await writer.write_interpretation(
            window_start=datetime(2026, 2, 17, 12, 0, 0),
            window_end=datetime(2026, 2, 17, 12, 0, 1),
            events=events,
            intents={},
            # graph_node_ids omitted
        )

        call_args = mock_conn.fetchval.call_args[0]
        stored_ids = call_args[13]
        assert stored_ids == []  # not None

    # ── Neo4jWriter: return values ───────────────────────────────────────────

    def test_neo4j_writer_create_or_update_user_returns_string(self):
        """create_or_update_user must return a non-empty string node ID."""
        from interpreter.neo4j_writer import Neo4jWriter

        mock_session = MagicMock()
        mock_result = MagicMock()
        mock_result.single.return_value = {"node_id": "U42"}
        mock_session.run.return_value = mock_result
        mock_session.__enter__ = MagicMock(return_value=mock_session)
        mock_session.__exit__ = MagicMock(return_value=False)

        mock_driver = MagicMock()
        mock_driver.session.return_value = mock_session

        writer = Neo4jWriter.__new__(Neo4jWriter)
        writer.driver = mock_driver
        writer.database = "neo4j"
        writer.namespace = "interpreter"

        result = writer.create_or_update_user(
            user_id="U42",
            source_system="slack",
            first_seen=datetime(2026, 2, 17, 10, 0, 0),
            last_active=datetime(2026, 2, 17, 12, 0, 0),
            interaction_count=5,
        )
        assert isinstance(result, str)
        assert result == "U42"

    def test_neo4j_writer_create_intent_node_returns_string(self):
        """create_intent_node must return a non-empty string node ID."""
        from interpreter.neo4j_writer import Neo4jWriter

        mock_session = MagicMock()
        mock_result = MagicMock()
        intent_id = "intent_evt1_asking_question"
        mock_result.single.return_value = {"node_id": intent_id}
        mock_session.run.return_value = mock_result
        mock_session.__enter__ = MagicMock(return_value=mock_session)
        mock_session.__exit__ = MagicMock(return_value=False)

        mock_driver = MagicMock()
        mock_driver.session.return_value = mock_session

        writer = Neo4jWriter.__new__(Neo4jWriter)
        writer.driver = mock_driver
        writer.database = "neo4j"
        writer.namespace = "interpreter"

        result = writer.create_intent_node(
            intent_id=intent_id,
            intent_type="asking_question",
            category="information_seeking",
            text="How does this work?",
            confidence=0.85,
            actor_id="U42",
            source_event_ids=["evt-1"],
            timestamp=datetime(2026, 2, 17, 12, 0, 0),
        )
        assert isinstance(result, str)
        assert result == intent_id

    def test_neo4j_writer_returns_fallback_when_no_record(self):
        """If Neo4j returns no record, the node ID input is used as fallback."""
        from interpreter.neo4j_writer import Neo4jWriter

        mock_session = MagicMock()
        mock_result = MagicMock()
        mock_result.single.return_value = None   # ← no record returned
        mock_session.run.return_value = mock_result
        mock_session.__enter__ = MagicMock(return_value=mock_session)
        mock_session.__exit__ = MagicMock(return_value=False)

        mock_driver = MagicMock()
        mock_driver.session.return_value = mock_session

        writer = Neo4jWriter.__new__(Neo4jWriter)
        writer.driver = mock_driver
        writer.database = "neo4j"
        writer.namespace = "interpreter"

        result = writer.create_or_update_user(
            user_id="U_fallback",
            source_system="slack",
            first_seen=datetime(2026, 2, 17, 10, 0, 0),
            last_active=datetime(2026, 2, 17, 12, 0, 0),
        )
        assert result == "U_fallback"

    # ── Engine: _create_graph_nodes returns list ─────────────────────────────

    @pytest.mark.asyncio
    async def test_engine_create_graph_nodes_returns_list(self):
        """_create_graph_nodes must return a list (possibly empty)."""
        import interpreter.engine as eng

        engine = eng.InterpreterEngine.__new__(eng.InterpreterEngine)

        # neo4j_writer that returns deterministic IDs
        mock_neo4j = MagicMock()
        mock_neo4j.create_or_update_user.return_value = "U1"
        mock_neo4j.create_intent_node.return_value = "intent_e1_asking_question"
        engine.neo4j_writer = mock_neo4j

        events = [{
            "event_id": "e1",
            "observed_at": datetime(2026, 2, 17, 12, 0, 0),
            "source_system": "slack",
            "user_id": "U1",
            "text": "hello",
            "metadata": {},
        }]
        intents = {
            "e1": [{
                "type": "asking_question",
                "category": "information_seeking",
                "text": "hello",
                "confidence": 0.8,
                "actor_id": "U1",
                "timestamp": datetime(2026, 2, 17, 12, 0, 0),
            }]
        }

        result = await engine._create_graph_nodes(events, intents)
        assert isinstance(result, list)
        assert "U1" in result
        assert "intent_e1_asking_question" in result

    @pytest.mark.asyncio
    async def test_engine_create_graph_nodes_deduplicates_node_ids(self):
        """Same user appearing in multiple events should appear once in node_ids."""
        import interpreter.engine as eng

        engine = eng.InterpreterEngine.__new__(eng.InterpreterEngine)

        mock_neo4j = MagicMock()
        mock_neo4j.create_or_update_user.return_value = "U1"  # same ID each call
        mock_neo4j.create_intent_node.return_value = "intent_e1_asking_question"
        engine.neo4j_writer = mock_neo4j

        events = [
            {"event_id": "e1", "observed_at": datetime(2026, 2, 17, 12, 0, 0),
             "source_system": "slack", "user_id": "U1", "text": "msg1", "metadata": {}},
            {"event_id": "e2", "observed_at": datetime(2026, 2, 17, 12, 0, 1),
             "source_system": "slack", "user_id": "U1", "text": "msg2", "metadata": {}},
        ]
        intents = {"e1": [{"type": "asking_question", "category": "information_seeking",
                            "text": "msg1", "confidence": 0.8, "actor_id": "U1",
                            "timestamp": datetime(2026, 2, 17, 12, 0, 0)}]}

        result = await engine._create_graph_nodes(events, intents)
        # U1 appears twice from two events but should only be in list once
        assert result.count("U1") == 1

    @pytest.mark.asyncio
    async def test_engine_create_graph_nodes_empty_events(self):
        """Empty events and intents should return an empty list, not raise."""
        import interpreter.engine as eng

        engine = eng.InterpreterEngine.__new__(eng.InterpreterEngine)
        engine.neo4j_writer = MagicMock()

        result = await engine._create_graph_nodes([], {})
        assert result == []


# ─────────────────────────────────────────────────────────────────────────────
# Thread Neo4j Wiring (Priority: engine._create_graph_nodes with thread_data)
# ─────────────────────────────────────────────────────────────────────────────

class TestThreadNeo4jWiring:
    """
    Verify that interpreter_Thread nodes are created in Neo4j when
    thread_data is passed to _create_graph_nodes.
    """

    @pytest.mark.asyncio
    async def test_create_graph_nodes_creates_thread_node(self):
        """Thread data passed to _create_graph_nodes must call create_thread_node."""
        import interpreter.engine as eng

        engine = eng.InterpreterEngine.__new__(eng.InterpreterEngine)

        mock_neo4j = MagicMock()
        mock_neo4j.create_or_update_user.return_value = "U1"
        mock_neo4j.create_intent_node.return_value = "intent_e1_asking_question"
        mock_neo4j.create_thread_node.return_value = "T_thread1"
        engine.neo4j_writer = mock_neo4j

        thread_data = [{
            "thread_id": "thread1",
            "channel_id": "C1",
            "topic": "deployment pipeline",
            "phase": "discussion",
            "started_at": datetime(2026, 2, 17, 10, 0, 0),
            "last_activity": datetime(2026, 2, 17, 12, 0, 0),
            "event_ids": ["e1", "e2"],
        }]

        result = await engine._create_graph_nodes([], {}, thread_data)

        mock_neo4j.create_thread_node.assert_called_once_with(
            thread_id="thread1",
            topic="deployment pipeline",
            phase="discussion",
            started_at=datetime(2026, 2, 17, 10, 0, 0),
            last_activity=datetime(2026, 2, 17, 12, 0, 0),
            source_event_ids=["e1", "e2"],
        )
        assert "T_thread1" in result

    @pytest.mark.asyncio
    async def test_create_graph_nodes_no_thread_data_still_works(self):
        """Omitting thread_data (default None) must not raise."""
        import interpreter.engine as eng

        engine = eng.InterpreterEngine.__new__(eng.InterpreterEngine)
        mock_neo4j = MagicMock()
        mock_neo4j.create_or_update_user.return_value = "U1"
        engine.neo4j_writer = mock_neo4j

        events = [{"event_id": "e1", "observed_at": datetime(2026, 2, 17, 12, 0, 0),
                   "source_system": "slack", "user_id": "U1", "text": "hi", "metadata": {}}]

        # No thread_data argument — should default to [] and not call create_thread_node
        result = await engine._create_graph_nodes(events, {})
        mock_neo4j.create_thread_node.assert_not_called()
        assert "U1" in result

    @pytest.mark.asyncio
    async def test_create_graph_nodes_thread_ids_deduplicated(self):
        """Same thread_id from two thread dicts should appear once in node_ids."""
        import interpreter.engine as eng

        engine = eng.InterpreterEngine.__new__(eng.InterpreterEngine)
        mock_neo4j = MagicMock()
        mock_neo4j.create_thread_node.return_value = "T_same"
        engine.neo4j_writer = mock_neo4j

        thread_data = [
            {"thread_id": "t1", "topic": "a", "phase": "initiation",
             "started_at": datetime(2026, 2, 17, 10, 0, 0),
             "last_activity": datetime(2026, 2, 17, 10, 0, 0),
             "event_ids": []},
            {"thread_id": "t2", "topic": "b", "phase": "discussion",
             "started_at": datetime(2026, 2, 17, 11, 0, 0),
             "last_activity": datetime(2026, 2, 17, 11, 0, 0),
             "event_ids": []},
        ]

        result = await engine._create_graph_nodes([], {}, thread_data)
        # Both calls return "T_same" — should only be in list once
        assert result.count("T_same") == 1

    @pytest.mark.asyncio
    async def test_analyze_threads_returns_list_of_dicts(self):
        """_analyze_threads must return a list (possibly empty), not None."""
        import interpreter.engine as eng

        engine = eng.InterpreterEngine.__new__(eng.InterpreterEngine)

        mock_analyzer = MagicMock()
        mock_analyzer.process_events = AsyncMock(return_value=[])
        mock_analyzer.active_threads = {}
        engine.thread_analyzer = mock_analyzer

        result = await engine._analyze_threads([], {})
        assert isinstance(result, list)

    @pytest.mark.asyncio
    async def test_analyze_threads_exposes_active_thread_data(self):
        """_analyze_threads must return data from active_threads for graph wiring."""
        import interpreter.engine as eng

        engine = eng.InterpreterEngine.__new__(eng.InterpreterEngine)

        mock_analyzer = MagicMock()
        mock_analyzer.process_events = AsyncMock(return_value=["ctx-1"])
        mock_analyzer.active_threads = {
            "T001": {
                "context_id": "ctx-1",
                "thread_id": "T001",
                "channel_id": "C1",
                "topic": "deployment",
                "phase": "discussion",
                "started_at": datetime(2026, 2, 17, 10, 0, 0),
                "last_activity": datetime(2026, 2, 17, 12, 0, 0),
                "event_ids": ["e1", "e2", None],  # None placeholder should be filtered
            }
        }
        engine.thread_analyzer = mock_analyzer

        result = await engine._analyze_threads([], {})
        assert len(result) == 1
        td = result[0]
        assert td["thread_id"] == "T001"
        assert td["channel_id"] == "C1"
        assert td["topic"] == "deployment"
        # None should be stripped from event_ids
        assert None not in td["event_ids"]


# ─────────────────────────────────────────────────────────────────────────────
# Channel Summary LLM (Priority: LLM summarizes thread summaries)
# ─────────────────────────────────────────────────────────────────────────────

class TestChannelSummaryLLM:
    """
    Verify that ChannelSummarizer uses the LLM when thread summaries exist,
    and falls back to keyword extraction when OpenAI is unavailable.
    """

    def _make_pool(self, rows=None):
        """Return a mock asyncpg pool whose acquire() returns rows on fetch."""
        mock_conn = AsyncMock()
        mock_conn.fetch = AsyncMock(return_value=rows or [])
        mock_conn.fetchrow = AsyncMock(return_value=None)
        mock_conn.execute = AsyncMock()

        ctx = MagicMock()
        ctx.__aenter__ = AsyncMock(return_value=mock_conn)
        ctx.__aexit__ = AsyncMock(return_value=False)

        mock_pool = MagicMock()
        mock_pool.acquire = MagicMock(return_value=ctx)
        return mock_pool, mock_conn

    @pytest.mark.asyncio
    async def test_get_channel_thread_summaries_queries_db(self):
        """_get_channel_thread_summaries must query thread_contexts for the channel."""
        from interpreter.channel_summarizer import ChannelSummarizer

        pool, mock_conn = self._make_pool(rows=[
            {"thread_id": "T1", "topic": "deploy", "phase": "discussion",
             "pattern": "Q&A", "decisions_made": ["use docker"],
             "message_count": 10, "participant_count": 2,
             "last_activity": datetime(2026, 2, 17, 12, 0, 0)},
        ])

        cs = ChannelSummarizer.__new__(ChannelSummarizer)
        cs.pool = pool
        cs._openai_client = None

        result = await cs._get_channel_thread_summaries("C_test")
        mock_conn.fetch.assert_called_once()
        call_sql = mock_conn.fetch.call_args[0][0]
        assert "thread_contexts" in call_sql
        assert len(result) == 1
        assert result[0]["topic"] == "deploy"

    @pytest.mark.asyncio
    async def test_llm_summarize_threads_calls_openai(self):
        """_llm_summarize_threads must call OpenAI and return summary_text."""
        from interpreter.channel_summarizer import ChannelSummarizer

        pool, _ = self._make_pool()
        cs = ChannelSummarizer.__new__(ChannelSummarizer)
        cs.pool = pool
        cs._openai_model = "gpt-4o-mini"

        # Mock OpenAI client
        mock_response = MagicMock()
        mock_response.choices[0].message.content = "The channel discussed deployment pipeline and Docker."
        mock_client = MagicMock()
        mock_client.chat.completions.create.return_value = mock_response
        cs._openai_client = mock_client

        thread_summaries = [{
            "topic": "deployment pipeline",
            "phase": "discussion",
            "pattern": "Q&A",
            "decisions_made": ["use docker"],
            "message_count": 8,
            "participant_count": 3,
        }]
        channel_events = [
            {"event_id": "e1", "user_id": "U1",
             "observed_at": datetime(2026, 2, 17, 12, 0, 0), "text": "hello"},
        ]

        result = await cs._llm_summarize_threads(
            channel_name="general",
            thread_summaries=thread_summaries,
            channel_events=channel_events,
        )

        mock_client.chat.completions.create.assert_called_once()
        assert "summary_text" in result
        assert "The channel" in result["summary_text"]
        assert result["participant_count"] == 1  # 1 unique user

    @pytest.mark.asyncio
    async def test_llm_fallback_when_openai_unavailable(self):
        """When _openai_client is None, channel summary falls back to keyword extraction."""
        from interpreter.channel_summarizer import ChannelSummarizer

        # thread_contexts returns empty (no threads yet)
        pool, mock_conn = self._make_pool(rows=[])
        mock_conn.fetchrow = AsyncMock(return_value=None)  # no previous summary

        cs = ChannelSummarizer.__new__(ChannelSummarizer)
        cs.pool = pool
        cs._openai_client = None  # no LLM
        cs._openai_model = "gpt-4o-mini"
        cs.drift_threshold = 0.3
        cs._recent_events = {}

        channel_events = [
            {"event_id": "e1", "user_id": "U1", "channel_id": "C1",
             "channel_name": "general", "source_system": "slack",
             "observed_at": datetime(2026, 2, 17, 12, 0, 0),
             "text": "let us deploy the docker container"},
            {"event_id": "e2", "user_id": "U2", "channel_id": "C1",
             "channel_name": "general", "source_system": "slack",
             "observed_at": datetime(2026, 2, 17, 12, 0, 1),
             "text": "agreed sounds good"},
        ]

        # _write_summary needs fetchrow to return a summary_id
        mock_write_row = MagicMock()
        mock_write_row.__getitem__ = MagicMock(
            side_effect=lambda k: "sum-uuid-1234" if k == "summary_id" else None
        )
        mock_conn.fetchrow = AsyncMock(return_value=mock_write_row)

        result = await cs._create_summary(
            channel_id="C1",
            channel_events=channel_events,
            total_count=2,
        )
        # Should not raise; result is a summary_id string
        assert result is not None

    @pytest.mark.asyncio
    async def test_llm_fallback_on_openai_error(self):
        """If OpenAI raises during _llm_summarize_threads, fallback to keyword extraction."""
        from interpreter.channel_summarizer import ChannelSummarizer

        pool, _ = self._make_pool()
        cs = ChannelSummarizer.__new__(ChannelSummarizer)
        cs.pool = pool
        cs._openai_model = "gpt-4o-mini"

        # Client that raises on call
        mock_client = MagicMock()
        mock_client.chat.completions.create.side_effect = RuntimeError("rate limit")
        cs._openai_client = mock_client

        thread_summaries = [{"topic": "testing", "phase": "initiation",
                              "pattern": "Q&A", "decisions_made": [],
                              "message_count": 3, "participant_count": 1}]
        channel_events = [
            {"event_id": "e1", "user_id": "U1",
             "observed_at": datetime(2026, 2, 17, 12, 0, 0),
             "text": "how does this work"},
        ]

        result = await cs._llm_summarize_threads(
            channel_name="general",
            thread_summaries=thread_summaries,
            channel_events=channel_events,
        )
        # Should fall back to keyword analysis dict (has summary_text key)
        assert "summary_text" in result
        # summary_text from keyword fallback uses _generate_summary_text pattern
        assert "messages" in result["summary_text"]


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
