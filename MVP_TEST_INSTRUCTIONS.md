# Observer + Interpreter MVP Test Instructions

**Date**: 2026-02-14  
**Status**: Ready for manual Slack test  
**Objective**: Verify end-to-end data flow from Slack → Observer → Interpreter → Databases

---

## Pre-Test Status

### ✅ Cleanup Complete

**Interpreter**:
- ✅ Removed old `main.py` implementation (moved to backups/)
- ✅ Clean module directory with only Phase 1 files
- ✅ `engine.py` is the clear entry point

**Observer**:
- ✅ Removed 7 backup files (moved to backups/)
- ✅ Clean module directory with only active P0 files
- ✅ All imports verified working

**Backups Preserved**:
```
Observer/backups/pre_p0_enhancements/
├── main.py.backup
├── main.py.enhanced
├── main_enhanced.py
├── main_original_backup.py
├── timescale_writer.py
├── timescale_writer.py.backup
└── neo4j_reader.py.backup

Interpreter/backups/old_implementation/
├── main.py
└── main_backup_20260213.py
```

---

## System Status

### Observer (Currently Running)
```bash
Port: 5002
Status: ✅ Healthy
Components:
  - Redis: Connected
  - TimescaleDB: Connected  
  - Neo4j: Connected
  - NATS: Connected
Events observed: 226
```

### Interpreter (Not Started Yet)
```bash
Port: N/A (will run as standalone batch processor)
Status: ⏸️ Not running (ready to start)
Database: basalmind_interpreter_postgresql (ready)
```

---

## Test Plan

### Phase 1: Verify Observer is Working

**Action**: Send a Slack message to test channel

**Expected Observer Behavior**:
1. Receives webhook from Slack
2. Normalizes event data
3. Enriches with correlation keys
4. Writes to TimescaleDB with WAL
5. Publishes to NATS
6. Creates Neo4j nodes (with `Observer_*` namespace)

**Verification**:
```bash
# Check latest event in TimescaleDB
ssh root@104.236.99.69 'docker exec basalmind-timescaledb psql -U basalmind -d basalmind_events -c "SELECT event_id, event_type, source_system, text FROM events ORDER BY observed_at DESC LIMIT 1;"'

# Check Observer health
curl http://104.236.99.69:5002/health

# Check Observer logs
ssh root@104.236.99.69 'tail -50 /var/log/basalmind-observer-error.log | grep -E "(✅|❌|Stage)"'
```

---

### Phase 2: Start Interpreter and Process Events

**Action**: Start the Interpreter engine

```bash
ssh root@104.236.99.69

# Navigate to Interpreter directory
cd /opt/basalmind/BasalMind_Consciousness/Interpreter

# Run setup (if not done already)
./scripts/setup_interpreter.sh

# Check that OpenAI API key is configured
grep OPENAI_API_KEY .env

# Start the Interpreter
./scripts/run_interpreter.sh
```

**Expected Interpreter Behavior**:
1. Connects to all databases
2. Loads checkpoint (or starts from 1 hour ago if fresh)
3. Begins processing 30-second windows
4. Extracts intents from events
5. Detects cross-source correlations
6. Creates Neo4j graph nodes (with `Interpreter_*` namespace)
7. Generates embeddings for threads (if 5+ messages)
8. Updates checkpoint after each window

**Verification**:
```bash
# Watch Interpreter output
# Should see:
# ✅ Interpreter engine initialized
# 📍 Resuming from checkpoint: ...
# 🚀 Interpreter engine started
# Processing window: ...
# 🎯 Extracted intents from X/Y events
# 📝 Wrote interpretation ...
# ✅ Window complete: ...

# In another terminal, check database
ssh root@104.236.99.69

# Check interpretations table
docker exec basalmind-timescaledb psql -U basalmind -d basalmind_interpreter_postgresql -c "SELECT interpretation_id, window_start, event_count, jsonb_array_length(intents) as intent_count FROM interpretations ORDER BY created_at DESC LIMIT 5;"

# Check thread contexts
docker exec basalmind-timescaledb psql -U basalmind -d basalmind_interpreter_postgresql -c "SELECT thread_id, channel_id, message_count, phase FROM thread_contexts ORDER BY last_activity DESC LIMIT 5;"

# Check embeddings (if any threads have 5+ messages)
docker exec basalmind-timescaledb psql -U basalmind -d basalmind_interpreter_postgresql -c "SELECT entity_type, entity_id, embedding_type, created_at FROM embeddings ORDER BY created_at DESC LIMIT 5;"
```

---

### Phase 3: Verify Neo4j Graph Integration

**Action**: Check that both Observer and Interpreter are creating namespaced nodes

```bash
ssh root@104.236.99.69

# Check Observer nodes (Observer_* namespace)
echo 'MATCH (n) WHERE labels(n)[0] STARTS WITH "Observer" RETURN labels(n) as label, count(n) as count ORDER BY count DESC LIMIT 10;' | docker exec -i shadowcaster-neo4j cypher-shell -u neo4j -p shadowcaster_secure_2024 --format plain

# Check Interpreter nodes (Interpreter_* namespace)
echo 'MATCH (n) WHERE labels(n)[0] STARTS WITH "Interpreter" RETURN labels(n) as label, count(n) as count ORDER BY count DESC LIMIT 10;' | docker exec -i shadowcaster-neo4j cypher-shell -u neo4j -p shadowcaster_secure_2024 --format plain

# Find a specific user's intents
echo 'MATCH (u:Interpreter_User)-[:Interpreter_PERFORMED]->(i:Interpreter_Intent) RETURN u.id, i.type, i.confidence, i.text LIMIT 5;' | docker exec -i shadowcaster-neo4j cypher-shell -u neo4j -p shadowcaster_secure_2024 --format plain
```

---

### Phase 4: End-to-End Data Flow Verification

**Action**: Send a test Slack message and trace it through the entire system

**Test Message**:
```
"How do we configure authentication in the BasalMind system?"
```

**Tracing Steps**:

1. **Observer receives event**:
```bash
# Check latest event
ssh root@104.236.99.69 'docker exec basalmind-timescaledb psql -U basalmind -d basalmind_events -c "SELECT event_id, event_type, text FROM events ORDER BY observed_at DESC LIMIT 1;"'
# Copy the event_id for next steps
```

2. **Interpreter processes event**:
```bash
# Wait for Interpreter to process (up to 30 seconds for next window)
# Check if event was interpreted
ssh root@104.236.99.69 'docker exec basalmind-timescaledb psql -U basalmind -d basalmind_interpreter_postgresql -c "SELECT interpretation_id, event_count, intents FROM interpretations WHERE '\''EVENT_ID_HERE'\'' = ANY(source_event_ids);"'
```

3. **Intent was extracted**:
```bash
# Should see "asking_question" intent with high confidence
# Check Neo4j for intent node
echo 'MATCH (i:Interpreter_Intent) WHERE i.text CONTAINS "authentication" RETURN i.id, i.type, i.confidence, i.text LIMIT 1;' | docker exec -i shadowcaster-neo4j cypher-shell -u neo4j -p shadowcaster_secure_2024 --format plain
```

4. **Lineage is preserved**:
```bash
# Trace from interpretation → event
ssh root@104.236.99.69 'docker exec basalmind-timescaledb psql -U basalmind -d basalmind_interpreter_postgresql -c "SELECT source_event_ids FROM interpretations ORDER BY created_at DESC LIMIT 1;"'

# Trace from Neo4j intent → TimescaleDB event
echo 'MATCH (i:Interpreter_Intent)-[r:Interpreter_EXTRACTED_FROM]->(e:Interpreter_Event) RETURN i.text, r.event_ids, e.id LIMIT 5;' | docker exec -i shadowcaster-neo4j cypher-shell -u neo4j -p shadowcaster_secure_2024 --format plain
```

---

## Success Criteria

### ✅ Observer Working
- [ ] Slack webhook receives messages
- [ ] Events written to TimescaleDB
- [ ] WAL empty after processing (events acknowledged)
- [ ] Neo4j Observer nodes created
- [ ] Health endpoint returns healthy

### ✅ Interpreter Working
- [ ] Connects to all databases successfully
- [ ] Processes events in 30-second windows
- [ ] Extracts intents with confidence scores
- [ ] Writes interpretations to PostgreSQL
- [ ] Creates Neo4j Interpreter nodes with lineage
- [ ] Checkpoints update after each window
- [ ] No errors in processing loop

### ✅ Integration Working
- [ ] Complete lineage chain: Interpretation → event_id → TimescaleDB
- [ ] Neo4j namespaces isolated (Observer_* vs Interpreter_*)
- [ ] Cross-source correlation detected (if multiple sources active)
- [ ] Embeddings generated (if thread has 5+ messages)

---

## Troubleshooting

### Interpreter won't start

**Check 1**: Database connections
```bash
# Test TimescaleDB
docker exec basalmind-timescaledb psql -U basalmind -d basalmind_events -c "SELECT 1"

# Test PostgreSQL
docker exec basalmind-timescaledb psql -U basalmind -d basalmind_interpreter_postgresql -c "SELECT 1"

# Test Neo4j
echo "RETURN 1" | docker exec -i shadowcaster-neo4j cypher-shell -u neo4j -p shadowcaster_secure_2024
```

**Check 2**: Python dependencies
```bash
cd /opt/basalmind/BasalMind_Consciousness/Interpreter
source venv/bin/activate
python3 -c "import asyncpg, neo4j, openai; print('✅ Dependencies OK')"
```

**Check 3**: Configuration
```bash
cat .env | grep -E "(TIMESCALE|POSTGRES|NEO4J|OPENAI)"
```

### No intents extracted

**Possible Causes**:
- Events don't have text (nginx logs, for example)
- Confidence threshold too high (default 0.5)
- Event type not supported

**Check**:
```bash
# Lower confidence threshold in code if needed
# Or check events table for text content
docker exec basalmind-timescaledb psql -U basalmind -d basalmind_events -c "SELECT event_type, text FROM events WHERE text IS NOT NULL LIMIT 10;"
```

### Embeddings not generating

**Possible Causes**:
- OpenAI API key missing or invalid
- Thread doesn't have 5+ messages
- Text content too short

**Check**:
```bash
# Verify API key
grep OPENAI_API_KEY /opt/basalmind/BasalMind_Consciousness/Interpreter/.env

# Check thread message counts
docker exec basalmind-timescaledb psql -U basalmind -d basalmind_interpreter_postgresql -c "SELECT thread_id, message_count FROM thread_contexts ORDER BY message_count DESC LIMIT 10;"
```

---

## After MVP Test

Once you confirm everything is working:

1. **Stop Interpreter** (Ctrl+C in the terminal)
2. **Review results** (check databases, Neo4j graph)
3. **Confirm**: "MVP working - ready for Phase 2"
4. **Then we'll discuss Phase 2 enhancements**

---

## Quick Reference Commands

```bash
# Observer health
curl http://104.236.99.69:5002/health

# Latest events
ssh root@104.236.99.69 'docker exec basalmind-timescaledb psql -U basalmind -d basalmind_events -c "SELECT * FROM events ORDER BY observed_at DESC LIMIT 5;"'

# Latest interpretations
ssh root@104.236.99.69 'docker exec basalmind-timescaledb psql -U basalmind -d basalmind_interpreter_postgresql -c "SELECT * FROM recent_interpretations LIMIT 5;"'

# Interpreter checkpoint
ssh root@104.236.99.69 'docker exec basalmind-timescaledb psql -U basalmind -d basalmind_interpreter_postgresql -c "SELECT * FROM interpreter_checkpoints ORDER BY created_at DESC LIMIT 1;"'

# Neo4j node counts
echo 'MATCH (n) RETURN labels(n)[0] as label, count(n) as count ORDER BY count DESC;' | docker exec -i shadowcaster-neo4j cypher-shell -u neo4j -p shadowcaster_secure_2024 --format plain
```

---

**Ready to test!** 🚀

Send a Slack message and I'll help you verify each step.
