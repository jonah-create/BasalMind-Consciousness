# Observer - Pure Sensory Layer

**Role**: Passive observation without judgment or reasoning
**Philosophy**: Sense, dont think. Observe, dont judge.

## Purpose

The Observer is Stage 1 of the three-stage cognitive pipeline:
1. **Observer** (this) - Pure passive sensing
2. **Interpreter** - Semantic analysis
3. **Actor** - Response generation

## Responsibilities

✅ **DOES**:
- Receive signals from all sources (Slack, GitHub, Postgres, etc.)
- Normalize to canonical event structure
- Persist to permanent storage (TimescaleDB)
- Deduplicate via Redis cache
- Track performance metrics

❌ **DOES NOT**:
- Call LLMs
- Detect intent
- Make decisions
- Filter based on interpretation
- Generate responses

## Architecture

```
Signal → Adapter → Canonical Event → Storage (Redis + TimescaleDB) → Done
```

## Quick Start

```bash
# Install dependencies
cd Observer
pip install -r requirements.txt

# Configure secrets
cp .env.example .env
# Edit .env with your credentials

# Run Observer
python -m observer.main
```

## Event Flow

1. **Receive** - Webhook/event arrives
2. **Normalize** - Adapter converts to CanonicalEvent (pure function)
3. **Dedup** - Redis check (<1ms)
4. **Persist** - TimescaleDB write (~3ms)
5. **Cache** - Redis hot cache (1hr TTL)
6. **Done** - Observer job complete

**Total**: <10ms per event

## Configuration

See `.env` for:
- Redis connection (dedup + cache)
- TimescaleDB connection (permanent storage)
- Service port (default: 5000)

## Testing

```bash
pytest observer/tests/
```

## Performance

- Dedup check: <1ms ✅ Tested
- TimescaleDB write: ~3ms
- Total latency: <10ms
- Throughput: >1000 events/sec

## Storage Strategy

**Redis** (ephemeral):
- Deduplication: 5 min TTL
- Event cache: 1 hour TTL
- Session tracking: 2 hours TTL

**TimescaleDB** (permanent):
- All events forever
- Historical analysis
- Audit trails

**No overlap** - Complementary strengths
