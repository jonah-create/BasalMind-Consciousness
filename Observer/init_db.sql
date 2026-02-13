-- BasalMind Observer - TimescaleDB Initialization
-- Purpose: Permanent audit trail of all observed events
-- Date: February 11, 2026

-- Enable TimescaleDB extension
CREATE EXTENSION IF NOT EXISTS timescaledb;

-- Create events table (will become hypertable)
CREATE TABLE IF NOT EXISTS events (
    -- Primary identifiers
    event_id UUID PRIMARY KEY,
    event_time TIMESTAMPTZ NOT NULL,
    observed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    -- Event classification
    event_type VARCHAR(255) NOT NULL,
    source_system VARCHAR(50) NOT NULL,

    -- Normalized fields (indexed for fast queries)
    user_id VARCHAR(255),
    user_name VARCHAR(255),
    user_email VARCHAR(255),
    channel_id VARCHAR(255),
    channel_name VARCHAR(255),
    thread_id VARCHAR(255),
    session_id VARCHAR(255),
    workspace_id VARCHAR(255),

    -- Content (searchable)
    text TEXT,
    subject TEXT,
    body TEXT,

    -- Action fields
    action_type VARCHAR(255),
    action_value TEXT,

    -- Tracing
    trace_id VARCHAR(255),
    correlation_id VARCHAR(255),

    -- Raw payload (JSONB for flexible queries)
    raw_payload JSONB NOT NULL,
    normalized_fields JSONB,

    -- Metadata
    metadata JSONB,

    -- Audit
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Convert to hypertable (TimescaleDB magic for time-series)
SELECT create_hypertable('events', 'event_time', if_not_exists => TRUE);

-- Create indexes for fast queries
CREATE INDEX IF NOT EXISTS idx_events_source_system ON events(source_system, event_time DESC);
CREATE INDEX IF NOT EXISTS idx_events_event_type ON events(event_type, event_time DESC);
CREATE INDEX IF NOT EXISTS idx_events_user_id ON events(user_id, event_time DESC);
CREATE INDEX IF NOT EXISTS idx_events_session_id ON events(session_id, event_time DESC);
CREATE INDEX IF NOT EXISTS idx_events_channel_id ON events(channel_id, event_time DESC);
CREATE INDEX IF NOT EXISTS idx_events_observed_at ON events(observed_at DESC);

-- Full-text search on text fields
CREATE INDEX IF NOT EXISTS idx_events_text_search ON events USING gin(to_tsvector('english', coalesce(text, '') || ' ' || coalesce(subject, '') || ' ' || coalesce(body, '')));

-- JSONB indexes for flexible queries
CREATE INDEX IF NOT EXISTS idx_events_raw_payload ON events USING gin(raw_payload);
CREATE INDEX IF NOT EXISTS idx_events_normalized ON events USING gin(normalized_fields);

-- Create sessions view for easy session queries
CREATE OR REPLACE VIEW sessions AS
SELECT
    session_id,
    MIN(event_time) as session_start,
    MAX(event_time) as session_end,
    COUNT(*) as event_count,
    array_agg(DISTINCT source_system) as sources,
    array_agg(DISTINCT event_type) as event_types,
    MAX(user_id) as user_id,
    MAX(user_name) as user_name,
    MAX(channel_id) as channel_id,
    MAX(workspace_id) as workspace_id
FROM events
WHERE session_id IS NOT NULL
GROUP BY session_id;

-- Create compression policy (compress data older than 7 days)
SELECT add_compression_policy('events', INTERVAL '7 days', if_not_exists => TRUE);

-- Create retention policy (keep data for 2 years, then archive or drop)
SELECT add_retention_policy('events', INTERVAL '2 years', if_not_exists => TRUE);

-- Create continuous aggregate for hourly stats
CREATE MATERIALIZED VIEW IF NOT EXISTS events_hourly
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 hour', event_time) as hour,
    source_system,
    event_type,
    COUNT(*) as event_count,
    COUNT(DISTINCT user_id) as unique_users,
    COUNT(DISTINCT session_id) as unique_sessions
FROM events
GROUP BY hour, source_system, event_type
WITH NO DATA;

-- Refresh policy for continuous aggregate (refresh every hour)
SELECT add_continuous_aggregate_policy('events_hourly',
    start_offset => INTERVAL '3 hours',
    end_offset => INTERVAL '1 hour',
    schedule_interval => INTERVAL '1 hour',
    if_not_exists => TRUE
);

-- Grant permissions
GRANT ALL PRIVILEGES ON events TO basalmind;
GRANT ALL PRIVILEGES ON sessions TO basalmind;
GRANT ALL PRIVILEGES ON events_hourly TO basalmind;

-- Success message
DO $$
BEGIN
    RAISE NOTICE '✅ TimescaleDB initialized successfully';
    RAISE NOTICE '   - Hypertable: events';
    RAISE NOTICE '   - Retention: 2 years';
    RAISE NOTICE '   - Compression: After 7 days';
    RAISE NOTICE '   - Indexes: Full-text, JSONB, time-series';
    RAISE NOTICE '   - Views: sessions, events_hourly';
END $$;
