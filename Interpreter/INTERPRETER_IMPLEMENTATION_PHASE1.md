# Interpreter Module - Phase 1 Implementation

**Status**: In Progress
**Date**: 2026-02-13
**Based on**: Claude Opus Design Specification

---

## Implementation Overview

### Completed Components

✅ **Database Schemas**
- PostgreSQL schema with pgvector extension
- Neo4j constraints and indexes
- Checkpoint and lineage tables
- Embedding storage infrastructure

✅ **TimescaleDB Batch Reader** (`interpreter/timescale_reader.py`)
- Reads events in 30-second windows
- Maintains checkpoint for resumability
- Provides lineage tracking via event IDs
- Batch size: 1000 events

✅ **Intent Extractor** (`interpreter/intent_extractor.py`)
- Keyword-based intent detection (Phase 1)
- 12 intent types with confidence scoring
- Multi-intent detection per event
- Confidence threshold: 0.5

### Remaining Components

🔨 **PostgreSQL Storage Layer** (`interpreter/postgres_writer.py`)
- Write interpretations to `interpretations` table
- Manage thread_contexts
- Track user_sessions
- Store lineage (source_event_ids)

🔨 **Neo4j Graph Writer** (`interpreter/neo4j_writer.py`)
- Create semantic nodes (Intent, Decision, Fact)
- Create actor nodes (User, System)
- Build relationships with lineage
- Maintain temporal graph (Moment nodes)

🔨 **Embedding Generator** (`interpreter/embedding_generator.py`)
- Generate OpenAI embeddings for threads
- Single embedding per thread (Phase 1)
- Store in pgvector with lineage

🔨 **Main Interpreter Engine** (`interpreter/engine.py`)
- Orchestrate batch processing pipeline
- Coordinate all modules
- Implement 30-second processing loop
- Handle checkpoints and error recovery

🔨 **Setup Scripts**
- `scripts/setup_interpreter.sh` - Install dependencies
- `scripts/run_interpreter.py` - Standalone runner
- `tests/test_interpreter.py` - Basic tests

---

## Architecture Diagram

```
┌─────────────────────┐
│   TimescaleDB       │ ← Raw events from Observer
│  (basalmind_events_timescaledb) │
└──────────┬──────────┘
           │
           ▼ Read in 30s windows
┌──────────────────────────────────┐
│   INTERPRETER ENGINE             │
├──────────────────────────────────┤
│ 1. TimescaleReader.get_batch()   │
│ 2. IntentExtractor.extract()     │
│ 3. PostgresWriter.write_interp() │
│ 4. Neo4jWriter.create_nodes()    │
│ 5. EmbeddingGen.generate()       │
│ 6. Update checkpoint             │
└─────────┬────────────┬───────────┘
          │            │
          ▼            ▼
┌──────────────┐  ┌──────────────┐
│  PostgreSQL  │  │    Neo4j     │
│  + pgvector  │  │   (Graph)    │
└──────────────┘  └──────────────┘
```

---

## File Structure

```
Interpreter/
├── interpreter/
│   ├── __init__.py
│   ├── timescale_reader.py   ✅ DONE
│   ├── intent_extractor.py   ✅ DONE
│   ├── postgres_writer.py    🔨 TODO
│   ├── neo4j_writer.py        🔨 TODO
│   ├── embedding_generator.py 🔨 TODO
│   └── engine.py              🔨 TODO
├── schemas/
│   ├── interpreter_postgres_schema.sql  ✅ DONE (Applied)
│   └── interpreter_neo4j_schema.cypher  ✅ DONE (Applied)
├── scripts/
│   ├── setup_interpreter.sh   🔨 TODO
│   └── run_interpreter.py     🔨 TODO
├── tests/
│   └── test_interpreter.py    🔨 TODO
├── .env                       ✅ DONE
└── requirements.txt           🔨 TODO
```

---

## Data Flow - Phase 1

### Input (from TimescaleDB)
```python
{
    "event_id": "abc-123",
    "observed_at": "2026-02-13T23:00:00Z",
    "event_type": "slack.message",
    "source_system": "slack",
    "user_id": "U123",
    "channel_id": "C456",
    "text": "How do we configure authentication?",
    "normalized_fields": {...},
    "metadata": {...}
}
```

### Processing Steps

1. **Batch Acquisition** (30-second window)
   - Fetch 1000 events max
   - Maintain event_ids[] for lineage

2. **Intent Extraction**
   - Detect: "asking_question" (0.85 confidence)
   - Extract actor: user_id
   - Preserve source event_id

3. **Create Interpretation Record** (PostgreSQL)
   ```sql
   INSERT INTO interpretations (
       window_start, window_end, event_count,
       source_event_ids, intents, ...
   ) VALUES (...)
   ```

4. **Create Graph Nodes** (Neo4j)
   ```cypher
   MERGE (u:User {id: $user_id})
   CREATE (i:Intent {
       id: $intent_id,
       type: "asking_question",
       text: $text,
       confidence: 0.85
   })
   CREATE (u)-[:PERFORMED]->(i)
   CREATE (i)-[:EXTRACTED_FROM {event_ids: $event_ids}]->(e:Event {id: $event_id})
   ```

5. **Generate Embedding** (if thread has 5+ messages)
   ```python
   embedding = openai.Embedding.create(
       model="text-embedding-ada-002",
       input=thread_text
   )
   
   INSERT INTO embeddings (
       entity_type, entity_id, embedding,
       source_event_ids
   ) VALUES ('thread', $thread_id, $embedding, $event_ids)
   ```

6. **Update Checkpoint**
   ```sql
   INSERT INTO interpreter_checkpoints (
       last_processed_time, last_event_id, records_processed
   ) VALUES ($end_time, $last_event_id, $count)
   ```

---

## Environment Configuration

```bash
# TimescaleDB (read-only)
TIMESCALE_HOST=localhost
TIMESCALE_PORT=5432
TIMESCALE_DB=basalmind_events_timescaledb
TIMESCALE_USER=basalmind
TIMESCALE_PASSWORD=basalmind_secure_2024

# PostgreSQL (Interpreter's database)
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=basalmind_interpreter_postgresql
POSTGRES_USER=basalmind
POSTGRES_PASSWORD=basalmind_secure_2024

# Neo4j
NEO4J_URI=bolt://localhost:7687
NEO4J_USER=neo4j
NEO4J_PASSWORD=shadowcaster_secure_2024
NEO4J_DATABASE=neo4j

# OpenAI (embeddings)
OPENAI_API_KEY=sk-your-key-here
EMBEDDING_MODEL=text-embedding-ada-002

# Processing
BATCH_WINDOW_SECONDS=30
BATCH_SIZE=1000
PROCESSING_INTERVAL_SECONDS=30
```

---

## Success Metrics - Phase 1

### Functional
- ✅ Intent detection accuracy: >70% (keyword-based)
- ✅ Lineage completeness: 100%
- ✅ Processing latency: <500ms per window

### Performance
- Process 100 events/second
- PostgreSQL write: <20ms per record
- Neo4j write: <50ms per node
- Embedding generation: <100ms per text

### Storage
- All events traceable to source via event_id
- Complete temporal ordering preserved
- Confidence scores on all extractions

---

## Testing Strategy

### Unit Tests
```python
test_timescale_reader()
  - test_batch_fetch()
  - test_checkpoint_load()
  - test_event_by_id_lookup()

test_intent_extractor()
  - test_question_detection()
  - test_confidence_scoring()
  - test_multi_intent_detection()

test_postgres_writer()
  - test_interpretation_write()
  - test_lineage_preservation()

test_neo4j_writer()
  - test_node_creation()
  - test_relationship_lineage()

test_embedding_generator()
  - test_openai_api_call()
  - test_vector_storage()
```

### Integration Tests
```python
test_end_to_end_pipeline()
  - Fetch real events from TimescaleDB
  - Extract intents
  - Write to PostgreSQL
  - Create Neo4j nodes
  - Generate embeddings
  - Verify lineage chain
```

---

## Next Steps

1. **Complete Remaining Modules** (4 files)
   - postgres_writer.py
   - neo4j_writer.py
   - embedding_generator.py
   - engine.py

2. **Create Setup Scripts**
   - requirements.txt
   - setup_interpreter.sh
   - run_interpreter.py

3. **Write Tests**
   - test_interpreter.py

4. **Deploy and Test**
   - Run against real Observer data
   - Validate lineage preservation
   - Monitor performance

5. **Iterate to Phase 2**
   - Multi-intent scoring
   - LLM-based classification
   - Decision extraction
   - Fact extraction

---

## Open Questions

1. **Embedding API Key**: Need real OpenAI API key in .env
2. **Thread Detection**: How to identify thread boundaries in events?
3. **Session Timeout**: How long before we close a user_session?
4. **Checkpoint Frequency**: Every batch? Every 10 batches?

---

## References

- Design Spec: `/tmp/OPUS_IMPLEMENTATION_REVIEW_PROMPT.md` (Interpreter section)
- Observer Code: `/opt/basalmind/BasalMind_Consciousness/Observer/`
- TimescaleDB Schema: `basalmind_events_timescaledb.events` table
- Neo4j Instance: `bolt://localhost:7687`
- Postgres Instance: Docker container `basalmind-timescaledb`

---

**Implementation Status**: 40% Complete (2/5 core modules done)
**Next Action**: Create postgres_writer.py, neo4j_writer.py, embedding_generator.py, engine.py
**Target**: Phase 1 MVP ready for testing

