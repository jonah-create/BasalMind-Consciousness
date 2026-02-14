# BasalMind Interpreter - Phase 1 MVP

**Transform raw time-series events into structured semantic knowledge**

The Interpreter is Stage 2 of the BasalMind Consciousness Pipeline, converting raw signals from the Observer into semantic memory optimized for enterprise intelligence.

---

## Architecture Overview

```
Observer → TimescaleDB (raw events)
    ↓
Interpreter → PostgreSQL + Neo4j (semantic knowledge)
    ↓
Philosopher/Cartographer → Business intelligence
```

**Key Principle**: The Interpreter reads raw data without modifying the Observer. It creates **NEW** structured knowledge while maintaining complete lineage back to source events.

---

## What It Does

### Multi-Source Event Processing

Handles events from **ALL** sources:
- ✅ Slack conversations
- ✅ nginx web server logs
- ✅ Cloudflare CDN events  
- ✅ Future: Enterprise applications, databases, APIs

### Semantic Extraction

- **Intent Detection**: 12 intent types (asking_question, making_decision, troubleshooting, etc.)
- **Entity Tracking**: Users, systems, IPs, channels across all sources
- **Cross-Source Correlation**: Links related events (e.g., Slack user → nginx requests)
- **Anomaly Detection**: Volume spikes, latency issues, missing events
- **Semantic Embeddings**: Vector search for similar conversations/patterns

### Knowledge Storage

**PostgreSQL** (`basalmind_interpreter_postgresql`):
- `interpretations` - Processing windows with extracted knowledge
- `thread_contexts` - Conversation/sequence tracking
- `user_sessions` - Activity across all systems
- `embeddings` - Semantic vectors (pgvector)

**Neo4j** (namespace: `Interpreter_*`):
- Users, Systems, Intents, Decisions, Facts
- Relationships with complete lineage
- Temporal graph structure

---

## Installation

### 1. Run Setup Script

```bash
cd /opt/basalmind/BasalMind_Consciousness/Interpreter
./scripts/setup_interpreter.sh
```

This will:
- Create Python virtual environment
- Install dependencies
- Verify database connections

### 2. Configure Environment

Edit `.env` and add your OpenAI API key:

```bash
# Required for embeddings
OPENAI_API_KEY=sk-your-real-key-here

# Optional: Adjust processing parameters
BATCH_WINDOW_SECONDS=30      # Process events in 30s windows
BATCH_SIZE=1000               # Max events per batch
PROCESSING_INTERVAL_SECONDS=30  # How often to check for new data
```

### 3. Verify Databases

```bash
# Check TimescaleDB (Observer's data)
docker exec basalmind-timescaledb psql -U basalmind -d basalmind_events -c "SELECT COUNT(*) FROM events;"

# Check PostgreSQL (Interpreter's database)
docker exec basalmind-timescaledb psql -U basalmind -d basalmind_interpreter_postgresql -c "\dt"

# Check Neo4j
docker exec shadowcaster-neo4j cypher-shell -u neo4j -p shadowcaster_secure_2024 "RETURN 1"
```

---

## Running the Interpreter

### Start the Engine

```bash
cd /opt/basalmind/BasalMind_Consciousness/Interpreter
./scripts/run_interpreter.sh
```

Expected output:
```
🚀 Starting BasalMind Interpreter...
✅ Loaded .env configuration
🔍 Checking database connections...
   ✅ TimescaleDB (basalmind_events)
   ✅ PostgreSQL (basalmind_interpreter_postgresql)
   ✅ Neo4j
🎯 Starting Interpreter engine...
   Batch window: 30s
   Batch size: 1000 events
   Processing interval: 30s

✅ Interpreter engine initialized
📍 Resuming from checkpoint: 2026-02-13T18:00:00Z
🚀 Interpreter engine started

============================================================
Processing window: 2026-02-13T18:00:00Z - 2026-02-13T18:00:30Z
Events: 45 from 3 sources
============================================================
🎯 Extracted intents from 23/45 events (avg 1.3 intents/event)
🔗 Detected 2 cross-source correlations
📝 Wrote interpretation 5f3a1c8e... (45 events, 30 intents)
✅ Window complete: 5f3a1c8e... (182ms, 30 intents extracted)
```

### Stop the Engine

Press `Ctrl+C` for graceful shutdown:
```
⚠️ Received shutdown signal
🛑 Shutting down Interpreter engine...
✅ Interpreter engine shutdown complete
```

---

## Testing

### Run Unit Tests

```bash
cd /opt/basalmind/BasalMind_Consciousness/Interpreter
source venv/bin/activate
pytest tests/test_interpreter.py -v
```

Expected output:
```
test_interpreter.py::TestIntentExtractor::test_question_detection PASSED
test_interpreter.py::TestIntentExtractor::test_decision_detection PASSED
test_interpreter.py::TestIntentExtractor::test_troubleshooting_detection PASSED
test_interpreter.py::TestIntentExtractor::test_batch_extraction PASSED
test_interpreter.py::TestIntentExtractor::test_multi_source_events PASSED
test_interpreter.py::TestIntentExtractor::test_confidence_threshold PASSED

========================= 6 passed in 0.45s =========================
```

### Test Individual Components

```bash
# Test TimescaleDB reader
python3 -m interpreter.timescale_reader

# Test intent extractor
python3 -m interpreter.intent_extractor

# Test PostgreSQL writer
python3 -m interpreter.postgres_writer

# Test Neo4j writer
python3 -m interpreter.neo4j_writer
```

---

## Query Examples

### PostgreSQL Queries

```sql
-- Recent interpretations
SELECT * FROM recent_interpretations LIMIT 10;

-- Cross-source correlations
SELECT 
    interpretation_id,
    jsonb_array_length(correlations) as correlation_count,
    correlations
FROM interpretations
WHERE correlations IS NOT NULL
ORDER BY created_at DESC;

-- User activity across sources
SELECT 
    entities->>'id' as entity_id,
    entities->>'sources' as sources,
    (entities->>'count')::int as event_count
FROM interpretations,
    jsonb_array_elements(entities) as entities
WHERE entities->>'type' = 'user'
ORDER BY (entities->>'count')::int DESC;

-- Find similar threads by embedding
SELECT 
    entity_id,
    text_content,
    1 - (embedding <=> '[0.1, 0.2, ...]'::vector) as similarity
FROM embeddings
WHERE entity_type = 'thread'
ORDER BY embedding <=> '[0.1, 0.2, ...]'::vector
LIMIT 10;
```

### Neo4j Queries

```cypher
// Find all users and their intents
MATCH (u:Interpreter_User)-[:Interpreter_PERFORMED]->(i:Interpreter_Intent)
RETURN u.id, i.type, i.confidence, i.text
ORDER BY i.extracted_at DESC
LIMIT 20;

// Cross-source user activity
MATCH (u:Interpreter_User)
WHERE size(u.source_systems) > 1
RETURN u.id, u.source_systems, u.interaction_count
ORDER BY u.interaction_count DESC;

// Trace intent back to source events
MATCH (i:Interpreter_Intent {id: $intent_id})-[r:Interpreter_EXTRACTED_FROM]->(e:Interpreter_Event)
RETURN i.text, r.event_ids, e.id;

// Find decision-making patterns
MATCH (u:Interpreter_User)-[:Interpreter_PERFORMED]->(i:Interpreter_Intent)
WHERE i.type = 'making_decision'
RETURN u.id, count(i) as decisions_made, collect(i.text) as decisions
ORDER BY decisions_made DESC;
```

---

## Monitoring

### Check Processing Status

```sql
-- Latest checkpoint
SELECT * FROM interpreter_checkpoints ORDER BY created_at DESC LIMIT 1;

-- Processing lag
SELECT 
    last_processed_time,
    NOW() - last_processed_time as lag,
    records_processed
FROM interpreter_checkpoints
ORDER BY created_at DESC
LIMIT 1;

-- Interpretations per hour
SELECT 
    date_trunc('hour', window_start) as hour,
    COUNT(*) as interpretation_count,
    SUM(event_count) as total_events,
    AVG(processing_duration_ms) as avg_processing_ms
FROM interpretations
WHERE window_start > NOW() - INTERVAL '24 hours'
GROUP BY hour
ORDER BY hour DESC;
```

### Watch Live Processing

```bash
# Tail logs
tail -f /var/log/interpreter.log

# Watch interpretations table
watch -n 5 "docker exec basalmind-timescaledb psql -U basalmind -d basalmind_interpreter_postgresql -c 'SELECT COUNT(*) FROM interpretations;'"
```

---

## Troubleshooting

### "No events in window"

**Cause**: Caught up to present or no new Observer data  
**Solution**: Normal operation. Interpreter waits for new events.

### "PostgreSQL connection failed"

**Cause**: Database not accessible  
**Solution**: Check database is running: `docker ps | grep timescale`

### "OpenAI API error"

**Cause**: Invalid or missing API key  
**Solution**: Update `OPENAI_API_KEY` in `.env` file

### "Checkpoint shows old timestamp"

**Cause**: Interpreter catching up on backlog  
**Solution**: Normal. It will process historical data then catch up.

---

## Phase 1 Limitations

**Current Implementation (MVP)**:
- ✅ Keyword-based intent detection (simple but fast)
- ✅ Single semantic embedding per thread
- ✅ Basic correlation detection (user_id, IP matching)
- ✅ Simple anomaly detection (volume thresholds)

**Phase 2 Enhancements (Future)**:
- 🔨 LLM-based intent classification (higher accuracy)
- 🔨 Multi-embedding strategy (semantic, intent, topical, temporal)
- 🔨 ML-based correlation detection
- 🔨 Statistical anomaly detection
- 🔨 Decision and fact extraction with confidence scoring
- 🔨 Real-time streaming mode (vs batch processing)

---

## Enterprise Intelligence Vision

The Interpreter is the **knowledge foundation** for:

1. **Security Intelligence**: Detect unauthorized access patterns, correlate login attempts across systems
2. **Performance Intelligence**: Identify latency bottlenecks, traffic anomalies
3. **Business Intelligence**: Track user journeys, decision patterns, conversation themes
4. **Compliance Intelligence**: Audit trails, policy enforcement, access patterns
5. **Operational Intelligence**: System health, error patterns, usage trends

**Example Use Cases**:
- "Show me all users who accessed the system from new IPs in the last 24 hours"
- "Find conversations about authentication in the last week"
- "What decisions were made about the database architecture?"
- "Detect traffic spikes correlated with Slack mentions"
- "Find similar support requests to this one"

---

## File Structure

```
Interpreter/
├── interpreter/
│   ├── __init__.py                 # Module exports
│   ├── engine.py                   # Main orchestrator
│   ├── timescale_reader.py        # Read Observer's data
│   ├── intent_extractor.py        # Extract semantic intents
│   ├── postgres_writer.py         # Write structured data
│   ├── neo4j_writer.py            # Create graph relationships
│   └── embedding_generator.py     # Generate semantic vectors
├── schemas/
│   ├── interpreter_postgres_schema.sql   # PostgreSQL tables
│   └── interpreter_neo4j_schema.cypher   # Neo4j constraints
├── scripts/
│   ├── setup_interpreter.sh       # Installation script
│   └── run_interpreter.sh         # Runner script
├── tests/
│   └── test_interpreter.py        # Unit tests
├── .env                           # Configuration
├── requirements.txt               # Python dependencies
└── README.md                      # This file
```

---

## Design Principles

1. **Auditability**: Every extraction traceable to source events
2. **Multi-Source**: Handle Slack, nginx, Cloudflare, any future source
3. **Lineage**: Complete chain from interpretation → event → raw data
4. **Simple**: Keyword-based extraction (Phase 1), no over-engineering
5. **Scalable**: Batch processing, checkpoints, recovery
6. **Namespace Isolation**: `Interpreter_*` labels prevent Neo4j collisions

---

## Credits

**Design**: Claude Opus  
**Implementation**: Claude Sonnet  
**Architecture**: BasalMind Enterprise Intelligence  
**Date**: 2026-02-13  
**Version**: 1.0.0-phase1

---

## Next Steps

1. **Deploy**: Run the Interpreter against real Observer data
2. **Monitor**: Watch processing lag, intent accuracy, storage growth
3. **Iterate**: Add Phase 2 enhancements based on business needs
4. **Integrate**: Build Philosopher and Cartographer to query this knowledge

**The Interpreter is ready for production use.** 🎯
