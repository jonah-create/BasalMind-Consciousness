# Observer Architecture

**Last Updated**: 2026-02-13
**Version**: 1.0.0

---

## System Context

The Observer is the first stage in the BasalMind Consciousness Pipeline, serving as the sensory layer that ingests all external signals.

### Position in Pipeline

```
External Events → OBSERVER (Stage 1) → Interpreter (Stage 2) → Philosopher (Stage 3)
                      ↓
                 TimescaleDB (Permanent Storage)
                      ↓
                   Neo4j (Knowledge Graph for Enrichment)
```

## 6-Stage Pipeline

Every event flows through these stages:

1. **Ingestion** - Receive & normalize to canonical schema
2. **Deduplication** - Redis hash check (<1ms)
3. **Loop Check** - Prevent circular references
4. **Enrichment** - Neo4j dimensional context
5. **Storage** - TimescaleDB persistence
6. **Distribution** - NATS JetStream publish

## Critical Success Factors

### Must Always Work
- **Stage 5 (Storage)** - Permanent record, cannot lose events
- All other stages can gracefully degrade

### Performance Targets
- Total latency: <20ms per event (currently achieving 10-20ms)
- Throughput: 500+ events/sec sustained
- Deduplication: <1ms
- Storage write: 3-5ms

## Current Adapters

1. **Slack** - Event API webhook
2. **Internal** - Direct API calls
3. **Cloudflare** - Edge Worker events
4. **Nginx** - Access log streaming

## Future-Proofing

The adapter pattern allows easy addition of new sources:
- GitHub (issues, PRs, commits)
- PostgreSQL (database triggers)
- Salesforce (CRM events)
- Any webhook-capable system

Each adapter converts source-specific format to CanonicalEvent schema.
