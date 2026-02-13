# Observer - Stage 1: Sensory Layer

**Version**: 1.0.0  
**Role**: Pure observation without judgment or reasoning  
**Philosophy**: Sense, don't think. Observe, don't judge.

---

## Table of Contents

1. [Purpose](#purpose)
2. [Architecture](#architecture)
3. [Pipeline Stages](#pipeline-stages)
4. [Data Sources](#data-sources)
5. [Event Schema](#event-schema)
6. [Storage Strategy](#storage-strategy)
7. [Quick Start](#quick-start)
8. [Configuration](#configuration)
9. [Performance](#performance)
10. [Testing](#testing)
11. [Extending](#extending)

---

## Purpose

The Observer is **Stage 1** of the 6-stage Consciousness Pipeline:

```
Observer → Interpreter → Philosopher → Cartographer → Sentinel → Assembler → Basal
  (1)         (2)           (3)            (4)          (5)         (6)      (Output)
```

### Observer Responsibilities

✅ **DOES**:
- Receive events from all data sources (Slack, HTTP, Cloudflare, internal)
- Normalize events to canonical schema
- Deduplicate events (Redis, 5-min window)
- Detect circular references (prevent infinite loops)
- Enrich with Neo4j dimensional context (priority, category, retention)
- Persist to permanent storage (TimescaleDB)
- Distribute to downstream stages (NATS JetStream)
- Track performance metrics

❌ **DOES NOT**:
- Call LLMs or perform semantic analysis
- Detect user intent or sentiment
- Make business decisions
- Filter based on interpretation
- Generate responses
- Perform complex reasoning

**Key Principle**: The Observer is a **pure data pipeline**. All intelligence happens in downstream stages.

---

## Architecture

### High-Level Flow

```
┌─────────────┐
│ Data Sources│
│  - Slack    │
│  - HTTP     │
│  - Cloudflare│
│  - Internal │
└──────┬──────┘
       │
       ▼
┌─────────────────────────────────────────────────┐
│             OBSERVER (6-Stage Pipeline)          │
├─────────────────────────────────────────────────┤
│ 1. Ingestion      → Receive & normalize         │
│ 2. Deduplication  → Redis check (<1ms)          │
│ 3. Loop Check     → Circular reference protect  │
│ 4. Enrichment     → Neo4j dimensional context   │
│ 5. Storage        → TimescaleDB persistence     │
│ 6. Distribution   → NATS JetStream publish      │
└──────┬──────────────────────────────────────────┘
       │
       ▼
┌─────────────┐
│    NATS     │
│ JetStream   │
│   events.>  │
└──────┬──────┘
       │
       ▼
┌─────────────┐
│ Interpreter │
│  (Stage 2)  │
└─────────────┘
```

### Component Architecture

```
observer/
├── main.py                  # FastAPI service, 6-stage pipeline
├── event_schema.py          # CanonicalEvent data classes
├── adapters/                # Data source adapters
│   └── slack_adapter.py     # Slack → CanonicalEvent
├── storage/
│   └── redis_cache.py       # Dedup + caching logic
├── timescale_writer.py      # TimescaleDB persistence
├── neo4j_reader.py          # Context enrichment
├── nats_publisher.py        # NATS distribution
├── circular_protection.py   # Loop detection
├── metrics.py               # Performance tracking
└── tests/                   # Test suite
```

---

## Pipeline Stages

The Observer processes every event through 6 stages:

### Stage 1: Ingestion

**Purpose**: Receive raw events and normalize to canonical schema

**Input**: Source-specific event format (Slack JSON, HTTP request, etc.)  
**Output**: `CanonicalEvent` with standardized fields  
**Processing Time**: <1ms

**Adapters**:
- `slack_adapter.py` - Slack events → CanonicalEvent
- Internal events - Direct CanonicalEvent creation
- Cloudflare - Edge HTTP requests → CanonicalEvent
- Nginx - Access logs → CanonicalEvent (via streamer)

**Key Function**: `process_event_pipeline(canonical_event)`

---

### Stage 2: Deduplication

**Purpose**: Prevent duplicate event processing

**Strategy**: Redis-based content hashing  
**TTL**: 5 minutes  
**Processing Time**: <1ms

**Hash Components**:
- `source_system`
- `event_type`
- `user_id`
- `timestamp`
- `text` content

**Result**: Returns `status: "duplicate"` if event seen recently

**Code**: `storage/redis_cache.py::is_duplicate()`

---

### Stage 3: Loop Check

**Purpose**: Detect circular references to prevent infinite event loops

**Strategy**: Depth tracking + origin tracking  
**Max Depth**: 10 levels  
**Processing Time**: <1ms

**Tracks**:
- `_depth` - How many times this event has spawned children
- `_originated_from` - Original source system
- `_processing_entity` - Current handler

**Result**: Rejects events that exceed depth limits or form loops

**Code**: `circular_protection.py::CircularReferenceProtector`

---

### Stage 4: Enrichment

**Purpose**: Add dimensional context from Neo4j knowledge graph

**Data Sources**: Neo4j graph database  
**Processing Time**: 5-15ms (cached lookups faster)

**Enrichment Data**:
- **Priority**: low, medium, high, critical
- **Category**: messaging, user_interaction, security_alert, etc.
- **Retention Policy**: Days to retain event
- **Consumed By**: Which cognitive layers need this event
- **Data Classifications**: public, pii, sensitive

**Example**:
```json
{
  "_enrichment": {
    "priority": "high",
    "category": "messaging",
    "retention_days": 90,
    "consumed_by": ["Interpreter", "Philosopher"],
    "data_class": ["pii", "public"]
  }
}
```

**Code**: `neo4j_reader.py::get_event_type_metadata()`

---

### Stage 5: Storage

**Purpose**: Persist events permanently to TimescaleDB

**Database**: TimescaleDB (PostgreSQL + time-series)  
**Table**: `events` (hypertable partitioned by `event_time`)  
**Processing Time**: 3-5ms

**Schema**:
```sql
CREATE TABLE events (
    event_id UUID,
    event_time TIMESTAMPTZ,
    observed_at TIMESTAMPTZ,
    event_type VARCHAR(255),
    source_system VARCHAR(50),
    user_id VARCHAR(255),
    text TEXT,
    raw_payload JSONB,
    normalized_fields JSONB,
    metadata JSONB,
    -- ... 20+ normalized fields
    PRIMARY KEY (event_id, event_time)
);
```

**Indexes**:
- `event_time DESC` - Time-series queries
- `source_system, event_type` - Source filtering
- `user_id, event_time` - User activity
- GIN indexes on JSONB fields - Full-text search

**Code**: `timescale_writer.py::write_event()`

---

### Stage 6: Distribution

**Purpose**: Publish events to NATS for downstream consumers

**Transport**: NATS JetStream  
**Stream**: `BASALMIND_EVENTS`  
**Subject Pattern**: `events.{source}.{type}`  
**Processing Time**: 2-3ms

**Example Subjects**:
- `events.slack.message`
- `events.internal.test`
- `events.cloudflare.http_request`

**Subscribers**:
- **Interpreter** (Stage 2) - Subscribes to `events.>`
- **Future stages** - Can subscribe to specific patterns

**Durability**: JetStream ensures at-least-once delivery

**Code**: `nats_publisher.py::publish()`

---

## Data Sources

### 1. Slack

**Endpoint**: `/observe/slack/events`  
**Method**: POST  
**Authentication**: Slack signing secret validation  
**Event Types**: `slack.message`, `slack.app_mention`, `slack.user_change`

**Adapter**: `adapters/slack_adapter.py::SlackAdapter`

**Normalized Fields**:
- `user_id` - Slack user ID
- `channel_id` - Slack channel
- `workspace_id` - Slack team
- `text` - Message content
- `thread_id` - Thread parent (if threaded)

---

### 2. Internal Events

**Endpoint**: `/observe/internal/event`  
**Method**: POST  
**Authentication**: None (internal only)  
**Event Types**: `test.*`, custom event types

**Usage**:
```bash
curl -X POST http://localhost:5002/observe/internal/event \
  -H "Content-Type: application/json" \
  -d '{
    "event_type": "user.signup",
    "user_id": "user_123",
    "text": "User signed up",
    "source_system": "internal"
  }'
```

---

### 3. Cloudflare

**Endpoint**: `/observe/cloudflare/event`  
**Method**: POST  
**Authentication**: None (can add token)  
**Event Types**: `cloudflare.http_request`, `cloudflare.firewall_event`

**Cloudflare Worker**: Deployed at edge to capture HTTP requests  
**Location**: `/opt/basalmind/cloudflare-workers/observer-edge-logger.js`

**Captured Data**:
- Request method, path, status
- Edge latency (ms)
- Country/region
- User agent

---

### 4. Nginx Access Logs

**Source**: `/var/log/nginx/basalcorp_access.log`  
**Streamer**: `nginx-observer-streamer.service`  
**Event Type**: `nginx.http_request` (sent as `internal` source)

**Captured Data**:
- IP address
- HTTP method, path, status
- Response size
- User agent, referer

---

## Event Schema

### CanonicalEvent Structure

```python
@dataclass
class CanonicalEvent:
    # Core Identity
    event_id: str              # UUID
    event_time: datetime       # When event occurred
    observed_at: datetime      # When Observer saw it
    
    # Classification
    event_type: str            # e.g., "slack.message"
    source_system: str         # e.g., "slack"
    
    # Raw Data
    raw_payload: Dict[str, Any]   # Original event JSON
    
    # Normalized Fields
    normalized: NormalizedFields  # Standardized structure
    
    # Tracking
    trace_id: Optional[str]       # Distributed tracing
    correlation_id: Optional[str] # Related events
    session_id: Optional[str]     # User session (added by Interpreter)
```

### NormalizedFields

```python
@dataclass
class NormalizedFields:
    # Content
    text: Optional[str]
    subject: Optional[str]
    body: Optional[str]
    
    # Identity
    user_id: Optional[str]
    user_name: Optional[str]
    user_email: Optional[str]
    
    # Context
    channel_id: Optional[str]
    workspace_id: Optional[str]
    thread_id: Optional[str]
    
    # Action
    action_type: Optional[str]
    action_value: Optional[str]
    
    # Metadata
    metadata: Optional[Dict[str, Any]]
```

---

## Storage Strategy

### Redis (Ephemeral)

**Purpose**: Fast lookups, temporary caching  
**Connection**: `localhost:6390`

**Use Cases**:
1. **Deduplication** (5-min TTL)
   - Key: `dedup:{hash}`
   - Prevents duplicate processing

2. **Session Tracking** (2-hour TTL) - *Added by Interpreter*
   - Key: `session:{user_id}`
   - Groups related events

3. **Trace Metadata** (24-hour TTL) - *Added by Interpreter*
   - Key: `trace:{trace_id}`
   - Distributed tracing

**No Overlap**: Redis never duplicates TimescaleDB data

---

### TimescaleDB (Permanent)

**Purpose**: Long-term storage, analytics, audit trails  
**Connection**: `localhost:5432`  
**Database**: `basalmind_events`

**Retention**:
- Events stored forever (until retention policy triggers)
- Hypertable automatically chunks by time
- Compression after 7 days

**Query Performance**:
- Recent events: <10ms
- Historical aggregations: <100ms (with proper indexes)
- Full-text search: <50ms (GIN indexes)

---

### Neo4j (Context)

**Purpose**: Dimensional enrichment, knowledge graph  
**Connection**: `bolt://localhost:7687`

**Data Stored**:
- Event type definitions
- Categories and priorities
- Retention policies
- Cognitive layer relationships
- Data classification rules

**Not Stored**: Individual events (only metadata)

---

### NATS JetStream (Distribution)

**Purpose**: Event streaming to downstream stages  
**Connection**: `nats://localhost:4222`  
**Stream**: `BASALMIND_EVENTS`

**Retention**: 24 hours or 10GB (whichever first)  
**Delivery**: At-least-once guaranteed

---

## Quick Start

### Prerequisites

```bash
# Required services
docker ps | grep -E "redis|timescale|neo4j|nats"

# Should show:
# - basalmind-redis (port 6390)
# - basalmind-timescaledb (port 5432)
# - shadowcaster-neo4j (port 7687)
# - NATS (port 4222)
```

### Installation

```bash
cd /opt/basalmind/BasalMind_Consciousness/Observer

# Install dependencies
pip install -r requirements.txt

# Configure environment
cp .env.example .env
# Edit .env with your credentials

# Run Observer
python -m uvicorn observer.main:app --host 0.0.0.0 --port 5002
```

### Verify Running

```bash
curl http://localhost:5002/health

# Expected:
# {
#   "status": "healthy",
#   "components": {
#     "redis": {"connected": true},
#     "timescaledb": {"connected": true},
#     "neo4j": {"connected": true},
#     "nats": {"connected": true}
#   }
# }
```

### Send Test Event

```bash
curl -X POST http://localhost:5002/observe/internal/event \
  -H "Content-Type: application/json" \
  -d '{
    "event_type": "test.quickstart",
    "user_id": "user_test",
    "text": "Testing Observer",
    "source_system": "internal"
  }'

# Expected:
# {
#   "status": "observed",
#   "event_id": "...",
#   "stage_reached": 6
# }
```

---

## Configuration

### Environment Variables

See `.env` file:

```bash
# Observer Service
OBSERVER_HOST=0.0.0.0
OBSERVER_PORT=5002
LOG_LEVEL=INFO

# Redis
REDIS_HOST=localhost
REDIS_PORT=6390
REDIS_PASSWORD=<password>
REDIS_DB=0

# TimescaleDB
TIMESCALE_HOST=localhost
TIMESCALE_PORT=5432
TIMESCALE_DB=basalmind_events
TIMESCALE_USER=basalmind
TIMESCALE_PASSWORD=<password>

# Neo4j
NEO4J_URI=bolt://localhost:7687
NEO4J_USER=neo4j
NEO4J_PASSWORD=<password>
NEO4J_DATABASE=neo4j

# NATS
NATS_URL=nats://localhost:4222
NATS_STREAM=BASALMIND_EVENTS

# Slack
SLACK_SIGNING_SECRET=<secret>
```

---

## Performance

### Benchmarks (Per Event)

| Stage | Average | 95th Percentile |
|-------|---------|-----------------|
| 1. Ingestion | <1ms | 2ms |
| 2. Deduplication | <1ms | 2ms |
| 3. Loop Check | <1ms | 1ms |
| 4. Enrichment | 5-10ms | 20ms |
| 5. Storage | 3-5ms | 10ms |
| 6. Distribution | 2-3ms | 5ms |
| **Total** | **10-20ms** | **40ms** |

### Throughput

- **Sustained**: 500-1000 events/sec
- **Burst**: 2000+ events/sec
- **Bottleneck**: TimescaleDB writes (can be batched)

### Optimization Tips

1. **Batch writes** - Group TimescaleDB inserts
2. **Connection pooling** - Reuse DB connections
3. **Async operations** - All I/O is non-blocking
4. **Redis caching** - Enrichment results cached

---

## Testing

### Run Test Suite

```bash
cd /opt/basalmind/BasalMind_Consciousness/Observer
pytest observer/tests/ -v
```

### Test Coverage

Currently: **Minimal** (needs expansion)

**Recommended Tests**:
- [ ] Unit tests for each adapter
- [ ] Pipeline stage integration tests
- [ ] Deduplication logic tests
- [ ] Circular reference detection tests
- [ ] Performance/load tests
- [ ] Error handling tests

---

## Extending

### Adding a New Data Source

1. **Create Adapter** in `observer/adapters/`

```python
# observer/adapters/github_adapter.py

from observer.event_schema import CanonicalEvent, NormalizedFields, EventSource
from datetime import datetime

class GitHubAdapter:
    @staticmethod
    def adapt_push_event(github_payload: dict) -> CanonicalEvent:
        """Convert GitHub push event to CanonicalEvent."""
        
        normalized = NormalizedFields(
            user_id=github_payload["sender"]["id"],
            user_name=github_payload["sender"]["login"],
            text=github_payload["head_commit"]["message"],
            action_type="git_push",
            metadata={
                "repo": github_payload["repository"]["full_name"],
                "branch": github_payload["ref"],
                "commits": len(github_payload["commits"])
            }
        )
        
        return CanonicalEvent(
            event_id=github_payload["head_commit"]["id"],
            event_time=datetime.fromisoformat(github_payload["head_commit"]["timestamp"]),
            observed_at=datetime.utcnow(),
            event_type="github.push",
            source_system=EventSource.GITHUB,
            raw_payload=github_payload,
            normalized=normalized
        )
```

2. **Add Endpoint** in `observer/main.py`

```python
from observer.adapters.github_adapter import GitHubAdapter

@app.post("/observe/github/webhook")
async def observe_github_webhook(request: Request):
    """Receive GitHub webhook events."""
    try:
        payload = await request.json()
        
        # Verify GitHub signature (recommended)
        # verify_github_signature(request.headers, payload)
        
        # Adapt to canonical event
        canonical_event = GitHubAdapter.adapt_push_event(payload)
        
        # Process through pipeline
        result = await process_event_pipeline(canonical_event.to_dict())
        
        return result
    
    except Exception as e:
        logger.error(f"GitHub webhook error: {e}")
        return JSONResponse(status_code=500, content={"error": str(e)})
```

3. **Add Event Types to Neo4j**

```cypher
CREATE (et:EventType {
    name: "github.push",
    description: "Code pushed to repository",
    priority: "medium",
    category: "development"
})
```

4. **Test**

```bash
curl -X POST http://localhost:5002/observe/github/webhook \
  -H "Content-Type: application/json" \
  -d @github_push_payload.json
```

---

## Future Roadmap

### Planned Data Sources

- [ ] **GitHub** - Issues, PRs, commits
- [ ] **PostgreSQL** - Database change events (via triggers/logical replication)
- [ ] **Salesforce** - CRM events (leads, opportunities, cases)
- [ ] **Stripe** - Payment events
- [ ] **SendGrid** - Email events
- [ ] **Twilio** - SMS/call events

### Planned Features

- [ ] **Batch processing** - Group TimescaleDB writes
- [ ] **Event replay** - Reprocess historical events
- [ ] **Sampling** - Process subset of high-volume events
- [ ] **Schema validation** - Enforce event structure
- [ ] **Rate limiting** - Protect against event floods
- [ ] **Event transformation** - Map custom fields
- [ ] **Dead letter queue** - Handle failed events

---

## Troubleshooting

### Observer Not Starting

```bash
# Check dependencies
docker ps | grep -E "redis|timescale|neo4j"

# Check logs
tail -f /var/log/basalmind-observer-error.log

# Test connections
redis-cli -h localhost -p 6390 ping
psql -h localhost -U basalmind basalmind_events -c "SELECT 1;"
```

### Events Not Being Stored

```bash
# Check TimescaleDB
psql -h localhost -U basalmind basalmind_events -c \
  "SELECT COUNT(*) FROM events WHERE observed_at > NOW() - INTERVAL '5 minutes';"

# Check Observer health
curl http://localhost:5002/health

# Check for errors
grep ERROR /var/log/basalmind-observer-error.log | tail -20
```

### High Latency

```bash
# Check Redis
redis-cli -h localhost -p 6390 --latency

# Check TimescaleDB
psql -h localhost -U basalmind basalmind_events -c \
  "SELECT * FROM pg_stat_activity WHERE state = 'active';"

# Check Observer metrics
curl http://localhost:5002/metrics
```

---

## Support

**Documentation**: `/opt/basalmind/BasalMind_Consciousness/Observer/docs/`  
**Issues**: Report via GitHub or internal tracker  
**Logs**: `/var/log/basalmind-observer-error.log`

---

## License

Internal use only - BasalMind Consciousness Project
