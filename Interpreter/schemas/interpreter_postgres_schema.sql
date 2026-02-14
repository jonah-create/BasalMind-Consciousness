-- Interpreter PostgreSQL Schema - Phase 1
-- Based on Claude Opus design specification

-- Enable required extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "vector";

-- =============================================================================
-- Core Tables
-- =============================================================================

-- Interpretation Records (Main Table)
CREATE TABLE IF NOT EXISTS interpretations (
    interpretation_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    -- Processing Metadata
    window_start TIMESTAMPTZ NOT NULL,
    window_end TIMESTAMPTZ NOT NULL,
    event_count INTEGER NOT NULL,
    processing_duration_ms INTEGER,

    -- Source Events (Lineage)
    source_event_ids UUID[] NOT NULL,  -- Array of TimescaleDB event IDs
    earliest_event_time TIMESTAMPTZ NOT NULL,
    latest_event_time TIMESTAMPTZ NOT NULL,

    -- Extracted Knowledge
    intents JSONB,      -- Array of {type, confidence, actor_id}
    decisions JSONB,    -- Array of {text, confidence, neo4j_id}
    facts JSONB,        -- Array of {statement, domain, confidence}
    entities JSONB,     -- Array of {name, type, count}

    -- Correlation Results
    correlations JSONB, -- Cross-source patterns found
    anomalies JSONB     -- Unexpected patterns
);

-- Create indexes for interpretations
CREATE INDEX IF NOT EXISTS idx_interpretations_window_time 
    ON interpretations (window_start, window_end);
CREATE INDEX IF NOT EXISTS idx_interpretations_source_events 
    ON interpretations USING GIN (source_event_ids);
CREATE INDEX IF NOT EXISTS idx_interpretations_intents 
    ON interpretations USING GIN (intents);
CREATE INDEX IF NOT EXISTS idx_interpretations_decisions 
    ON interpretations USING GIN (decisions);

-- Thread Contexts
CREATE TABLE IF NOT EXISTS thread_contexts (
    context_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    thread_id TEXT NOT NULL,
    channel_id TEXT NOT NULL,

    -- Thread Analysis
    topic TEXT,
    phase TEXT CHECK (phase IN ('initiation', 'exploration', 'discussion', 'resolution')),
    pattern TEXT CHECK (pattern IN ('Q&A', 'brainstorm', 'decision', 'troubleshooting')),

    -- Participants
    participants JSONB,  -- Array of {user_id, message_count, role}
    initiator_id TEXT,
    resolver_id TEXT,

    -- Temporal
    started_at TIMESTAMPTZ NOT NULL,
    last_activity TIMESTAMPTZ NOT NULL,
    message_count INTEGER DEFAULT 0,

    -- Key Extractions
    decisions_made TEXT[],
    facts_established TEXT[],
    questions_answered JSONB,

    -- Lineage
    source_event_ids UUID[],

    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),

    UNIQUE(thread_id)
);

CREATE INDEX IF NOT EXISTS idx_thread_contexts_channel 
    ON thread_contexts (channel_id, started_at);
CREATE INDEX IF NOT EXISTS idx_thread_contexts_source_events 
    ON thread_contexts USING GIN (source_event_ids);

-- User Sessions
CREATE TABLE IF NOT EXISTS user_sessions (
    session_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id TEXT NOT NULL,
    source_system TEXT NOT NULL,

    -- Session Bounds
    started_at TIMESTAMPTZ NOT NULL,
    ended_at TIMESTAMPTZ,
    last_activity TIMESTAMPTZ NOT NULL,

    -- Activity Summary
    event_count INTEGER DEFAULT 0,
    channels_visited TEXT[],
    threads_participated TEXT[],
    intents_expressed JSONB,

    -- Journey Tracking
    entry_point TEXT,
    exit_point TEXT,
    journey_steps JSONB,  -- Array of {timestamp, action, context}

    -- Lineage
    event_ids UUID[]
);

CREATE INDEX IF NOT EXISTS idx_user_sessions_user 
    ON user_sessions (user_id, started_at);
CREATE INDEX IF NOT EXISTS idx_user_sessions_active 
    ON user_sessions (ended_at) WHERE ended_at IS NULL;

-- Embeddings Table (Phase 1: Single embedding per thread)
CREATE TABLE IF NOT EXISTS embeddings (
    embedding_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),

    -- Entity Reference
    entity_type TEXT NOT NULL,  -- 'thread', 'channel_summary', 'decision', 'user_session'
    entity_id TEXT NOT NULL,

    -- Embedding Data
    embedding_type TEXT NOT NULL,  -- 'semantic', 'intent', 'topical', 'temporal'
    model_name TEXT NOT NULL,      -- 'text-embedding-ada-002'
    embedding vector(1536) NOT NULL,

    -- Context
    text_content TEXT,  -- Original text that was embedded
    token_count INTEGER,

    -- Temporal
    created_at TIMESTAMPTZ DEFAULT NOW(),
    valid_until TIMESTAMPTZ,  -- For time-sensitive embeddings

    -- Metadata
    metadata JSONB,  -- Additional context

    -- Lineage
    source_interpretation_id UUID REFERENCES interpretations(interpretation_id),
    source_event_ids UUID[]
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_embeddings_entity 
    ON embeddings (entity_type, entity_id, embedding_type);
CREATE INDEX IF NOT EXISTS idx_embeddings_vector 
    ON embeddings USING ivfflat (embedding vector_cosine_ops);

-- Interpreter Checkpoints
CREATE TABLE IF NOT EXISTS interpreter_checkpoints (
    checkpoint_id SERIAL PRIMARY KEY,
    last_processed_time TIMESTAMPTZ NOT NULL,
    last_event_id UUID NOT NULL,
    records_processed BIGINT,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- =============================================================================
-- Helper Functions
-- =============================================================================

-- Function to update thread context timestamps
CREATE OR REPLACE FUNCTION update_thread_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER thread_contexts_updated_at
    BEFORE UPDATE ON thread_contexts
    FOR EACH ROW
    EXECUTE FUNCTION update_thread_updated_at();

-- =============================================================================
-- Views for Common Queries
-- =============================================================================

-- Active threads view
CREATE OR REPLACE VIEW active_threads AS
SELECT 
    thread_id,
    channel_id,
    topic,
    phase,
    pattern,
    started_at,
    last_activity,
    message_count,
    AGE(NOW(), last_activity) as idle_time
FROM thread_contexts
WHERE last_activity > NOW() - INTERVAL '24 hours'
ORDER BY last_activity DESC;

-- Recent interpretations view
CREATE OR REPLACE VIEW recent_interpretations AS
SELECT 
    interpretation_id,
    window_start,
    window_end,
    event_count,
    processing_duration_ms,
    array_length(source_event_ids, 1) as source_event_count,
    jsonb_array_length(COALESCE(intents, '[]'::jsonb)) as intent_count,
    jsonb_array_length(COALESCE(decisions, '[]'::jsonb)) as decision_count,
    created_at
FROM interpretations
ORDER BY created_at DESC
LIMIT 100;

COMMENT ON TABLE interpretations IS 'Main table for Interpreter processing windows and extracted knowledge';
COMMENT ON TABLE thread_contexts IS 'Conversation thread analysis and tracking';
COMMENT ON TABLE user_sessions IS 'User activity sessions across channels and threads';
COMMENT ON TABLE embeddings IS 'Vector embeddings for semantic similarity search';
COMMENT ON TABLE interpreter_checkpoints IS 'Processing checkpoints for recovery';
