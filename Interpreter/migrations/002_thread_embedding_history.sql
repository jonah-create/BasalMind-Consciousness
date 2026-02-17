-- Migration 002: Thread embedding history for trend detection
--
-- Creates an append-only table that stores a snapshot of the thread embedding
-- each time generate_thread_embedding() is called. Unlike the embeddings table
-- (which is overwritten), this table accumulates all historical states so an
-- LLM can observe the semantic trajectory of a conversation over time.
--
-- Key field for trend detection:
--   drift_from_prev: cosine distance from previous snapshot (pre-computed).
--   0.0 = identical topic, ~0.25 = meaningful shift, >0.5 = major pivot.
--   NULL on snapshot_index=1 (no prior state to compare).

CREATE TABLE IF NOT EXISTS thread_embedding_history (
    history_id          uuid PRIMARY KEY DEFAULT gen_random_uuid(),

    -- Identity
    thread_id           text        NOT NULL,
    channel_id          text,

    -- Snapshot position (the x-axis for trend detection)
    snapshot_index      integer     NOT NULL,   -- 1, 2, 3 ... monotonically per thread
    message_count       integer     NOT NULL,   -- messages in thread at time of snapshot

    -- Thread state at time of snapshot
    phase               text,                   -- initiation|exploration|discussion|resolution
    pattern             text,                   -- Q&A|brainstorm|decision|troubleshooting
    participant_count   integer,                -- unique speakers at time of snapshot

    -- The embedding at this moment
    model_name          text        NOT NULL,
    embedding           vector(1536) NOT NULL,

    -- The concatenated text that was embedded (LLM can read each chapter)
    text_content        text,

    -- Pre-computed cosine distance from previous snapshot.
    -- NULL for snapshot_index=1. High value = topic shift.
    drift_from_prev     float,

    -- Lineage
    source_interpretation_id uuid,

    captured_at         timestamp with time zone NOT NULL DEFAULT now()
);

-- Fast ordered retrieval of all snapshots for a thread
CREATE INDEX IF NOT EXISTS idx_teh_thread_order
    ON thread_embedding_history (thread_id, snapshot_index);

-- Fetch latest snapshots across all threads in a channel
CREATE INDEX IF NOT EXISTS idx_teh_channel_time
    ON thread_embedding_history (channel_id, captured_at DESC);

-- Vector similarity search across historical snapshots
CREATE INDEX IF NOT EXISTS idx_teh_vector
    ON thread_embedding_history USING ivfflat (embedding vector_cosine_ops)
    WITH (lists = 100);
