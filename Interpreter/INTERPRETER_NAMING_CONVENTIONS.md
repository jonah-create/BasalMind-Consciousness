# Interpreter Module - Naming Conventions

**Date**: 2026-02-13
**Purpose**: Consistent, descriptive naming for all Interpreter databases and resources

---

## Database Naming

### TimescaleDB (Observer's Raw Data - Read Only)
```
Host: localhost:5432
Database: basalmind_events_timescaledb
User: basalmind
Purpose: Raw time-series events from Observer
Access: Read-only for Interpreter
```

**Naming Rationale**: 
- `basalmind_events` - Core identifier
- `_timescaledb` - Technology suffix for clarity

### PostgreSQL (Interpreter's Structured Data - Read/Write)
```
Host: localhost:5432
Database: basalmind_interpreter_postgresql
User: basalmind
Purpose: Interpreted semantic records, embeddings, sessions
Access: Full read/write by Interpreter
```

**Naming Rationale**:
- `basalmind_interpreter` - Module identifier
- `_postgresql` - Technology suffix for consistency

**Tables**:
- `interpretations` - Main processing records
- `thread_contexts` - Conversation tracking
- `user_sessions` - User journey tracking
- `embeddings` - Semantic vectors (pgvector)
- `interpreter_checkpoints` - Recovery checkpoints

### Neo4j (Semantic Graph - Read/Write)
```
URI: bolt://localhost:7687
Database: neo4j (Community Edition - single database)
Namespace: interpreter
User: neo4j
Purpose: Semantic graph relationships and entities
Access: Full read/write by Interpreter
```

**Naming Rationale**:
- Cannot rename database in Community Edition
- Use `NEO4J_NAMESPACE=interpreter` to prefix all nodes/relationships
- Example: `(u:Interpreter_User)` instead of `(u:User)`
- Prevents collision with other BasalMind modules using same Neo4j instance

**Node Labels with Namespace**:
- `Interpreter_User` (Actor)
- `Interpreter_System` (Actor)
- `Interpreter_Intent` (Semantic)
- `Interpreter_Decision` (Semantic)
- `Interpreter_Fact` (Semantic)
- `Interpreter_Thread` (Conversation)
- `Interpreter_Channel` (Conversation)
- `Interpreter_Session` (Temporal)
- `Interpreter_Moment` (Temporal)
- `Interpreter_Event` (Reference to TimescaleDB)

---

## Comparison with Observer

| Component | Observer | Interpreter |
|-----------|----------|-------------|
| **TimescaleDB** | `basalmind_events_timescaledb` (write) | `basalmind_events_timescaledb` (read-only) |
| **PostgreSQL** | N/A | `basalmind_interpreter_postgresql` |
| **Neo4j** | Uses namespace `observer` | Uses namespace `interpreter` |
| **Redis** | localhost:6390 | localhost:6390 (shared) |
| **NATS** | Publishes to `BASALMIND_EVENTS` | Subscribes to `BASALMIND_EVENTS` |

---

## Environment Variable Naming

```bash
# TimescaleDB (Observer's data store)
TIMESCALE_HOST=localhost
TIMESCALE_PORT=5432
TIMESCALE_DB=basalmind_events_timescaledb
TIMESCALE_USER=basalmind
TIMESCALE_PASSWORD=basalmind_secure_2024

# PostgreSQL (Interpreter's data store)
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=basalmind_interpreter_postgresql
POSTGRES_USER=basalmind
POSTGRES_PASSWORD=basalmind_secure_2024

# Neo4j (Shared graph with namespace isolation)
NEO4J_URI=bolt://localhost:7687
NEO4J_USER=neo4j
NEO4J_PASSWORD=shadowcaster_secure_2024
NEO4J_DATABASE=neo4j
NEO4J_NAMESPACE=interpreter
```

---

## Benefits of This Naming Convention

1. **Technology Clarity**: Suffix immediately identifies the database technology
   - `_timescaledb` - Time-series database
   - `_postgresql` - Relational database with pgvector
   - `_neo4j` - Graph database (via namespace)

2. **Module Isolation**: Clear ownership boundaries
   - Observer owns `basalmind_events_timescaledb`
   - Interpreter owns `basalmind_interpreter_postgresql`
   - Both share Neo4j with namespace prefixes

3. **Consistency**: All databases follow same pattern
   - `basalmind_{module}_{technology}`

4. **Auditability**: Easy to trace data flow
   - Raw events: `basalmind_events_timescaledb`
   - Interpreted data: `basalmind_interpreter_postgresql`
   - Graph relationships: Neo4j with `Interpreter_*` labels

5. **Scalability**: Pattern extends to future modules
   - `basalmind_philosopher_postgresql`
   - `basalmind_cartographer_postgresql`
   - All using same Neo4j instance with different namespaces

---

## Migration Notes

**What Changed**:
- ✅ Renamed `basalmind_interpreter` → `basalmind_interpreter_postgresql`
- ✅ Updated `.env` file with consistent naming
- ✅ Added `NEO4J_NAMESPACE=interpreter` for node label prefixing
- ✅ Updated documentation to reflect new names

**What Stayed Same**:
- TimescaleDB database name remains `basalmind_events` in container
  - Environment variable now explicitly states `_timescaledb` suffix for clarity
- Neo4j database remains `neo4j` (Community Edition limitation)
- All connection details unchanged

**Code Impact**:
- All Python modules will use environment variables
- Neo4j writer will automatically prefix node labels with namespace
- No hardcoded database names in code

---

## Future Modules Pattern

When adding new modules (Philosopher, Cartographer, etc.):

```bash
# PostgreSQL database
{module}_postgresql
Example: basalmind_philosopher_postgresql

# Neo4j namespace
NEO4J_NAMESPACE={module_name}
Example: NEO4J_NAMESPACE=philosopher

# Node labels
{Namespace}_{EntityType}
Example: Philosopher_Hypothesis, Philosopher_Theory
```

This ensures clean separation and prevents naming collisions.

